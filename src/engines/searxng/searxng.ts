import axios from 'axios';
import type { AxiosRequestConfig, AxiosResponse } from 'axios';
import { config } from '../../config.js';
import { SearchResult } from '../../types.js';
import type { SearchExecutionContext } from '../../core/search/searchService.js';
import { SearxngScheduler, QueueTimeoutError, AllPairsCoolingError } from './scheduler.js';
import type { Clock, BackoffChange } from './scheduler.js';

export { QueueTimeoutError, AllPairsCoolingError };

const HARD_MAX_PAGES = 5;
const CACHE_MAX_ENTRIES = 1000;
const QUERY_LOG_MAX = 80;

type HttpGet = (url: string, options: AxiosRequestConfig) => Promise<AxiosResponse>;

const realClock: Clock = {
    now: () => Date.now(),
    sleep: (ms, signal) => new Promise((resolve) => {
        const timer = setTimeout(resolve, ms);
        // a cancelled sleep must not keep a timer pending for the whole queue timeout
        signal?.addEventListener('abort', () => { clearTimeout(timer); resolve(); }, { once: true });
    })
};
let httpGet: HttpGet = (url, options) => axios.get(url, options);
let clock: Clock = realClock;

// Module state: LRU cache and the pair scheduler (concurrency, intervals, global ceiling, backoff).
const cache = new Map<string, { expires: number; results: SearchResult[] }>();
let cacheHits = 0;
const scheduler = new SearxngScheduler(
    () => ({
        maxConcurrency: config.searxngMaxConcurrency ?? 2,
        minIntervalMs: config.searxngMinIntervalMs ?? 1000,
        pairMinIntervalMs: config.searxngPairMinIntervalMs ?? 10000,
        globalMaxPerMin: config.searxngGlobalMaxPerMin ?? 12,
        backoffBaseMs: config.searxngBackoffBaseMs ?? 60000,
        backoffMaxMs: config.searxngBackoffMaxMs ?? 3600000,
        queueTimeoutMs: config.searxngQueueTimeoutMs ?? 45000
    }),
    () => clock
);

export function __setSearxngHttpGetForTests(impl?: HttpGet): void {
    httpGet = impl ?? ((url, options) => axios.get(url, options));
}

export function __setSearxngClockForTests(impl?: Clock): void {
    clock = impl ?? realClock;
}

export function __resetSearxngStateForTests(): void {
    cache.clear();
    cacheHits = 0;
    scheduler.reset();
}

export function __getSearxngSchedulerForTests(): SearxngScheduler {
    return scheduler;
}

export type SearxngMeta = {
    queued_ms: number;
    upstream_ms: number;
    egress: Record<string, string>;
    cache: 'hit' | 'miss';
    unresponsive: string[];
    queue_depth: number;
};

export type SearxngPartialFailure = {
    engine: string;
    code: 'engine_degraded';
    message: string;
};

export interface SearxngDetailedResult {
    results: SearchResult[];
    meta: SearxngMeta;
    partialFailures: SearxngPartialFailure[];
    /** raw `unresponsive_engines` entries of the last upstream response, SearXNG-shaped */
    rawUnresponsive: unknown[];
}

export interface SearxngSearchOptions {
    /** fetch exactly this single page (compat endpoint); default is the configured multi-page loop */
    pageno?: number;
    language?: string;
    categories?: string[];
}

interface SearxngRawResult {
    title?: string;
    url?: string;
    content?: string;
    engine?: string;
    engines?: string[];
}

interface SearxngBody {
    results?: SearxngRawResult[];
    unresponsive_engines?: unknown[];
}

function cacheGet(key: string): SearchResult[] | undefined {
    const entry = cache.get(key);
    if (!entry) return undefined;
    if (entry.expires <= clock.now()) {
        cache.delete(key);
        return undefined;
    }
    cache.delete(key); // refresh LRU position
    cache.set(key, entry);
    return entry.results;
}

function cacheSet(key: string, results: SearchResult[], partial: boolean): void {
    let ttl = config.searxngCacheTtlMs ?? 86400000;
    // a response where a requested engine was unresponsive/skipped is incomplete: keep it only briefly
    if (partial) ttl = Math.min(ttl, config.searxngPartialCacheTtlMs ?? 600000);
    if (ttl <= 0 || results.length === 0) return;
    cache.delete(key);
    cache.set(key, { expires: clock.now() + ttl, results });
    while (cache.size > CACHE_MAX_ENTRIES) {
        cache.delete(cache.keys().next().value as string);
    }
}

function unresponsiveNames(list: unknown[]): string[] {
    return list.map((item) => String(Array.isArray(item) ? item[0] : item));
}

function contributed(results: SearxngRawResult[], pairName: string): boolean {
    return results.some((r) => r.engine === pairName || r.engines?.includes(pairName));
}

/**
 * Query a self-hosted SearXNG instance via its JSON API.
 * Always plain HTTP (no proxy, no browser) - independent of SEARCH_MODE and USE_PROXY.
 * Every upstream call goes through the scheduler (FIFO queue, per-pair interval, global ceiling, backoff).
 */
export async function searchSearxngDetailed(query: string, limit: number, options: SearxngSearchOptions = {}): Promise<SearxngDetailedResult> {
    const baseUrl = config.searxngUrl?.replace(/\/+$/, '');
    if (!baseUrl) {
        throw new Error('SearXNG engine is unavailable: SEARXNG_URL is not set');
    }

    const plainEngines = config.searxngEngines ?? [];
    const categories = options.categories ?? config.searxngCategories ?? [];
    const language = options.language ?? config.searxngLanguage;
    const egresses = config.searxngEgresses ?? [];
    const rotated = egresses.length > 0 ? (config.searxngRotateEngines ?? []) : [];
    const extras = config.searxngExtraEngines ?? [];
    const rotationOn = rotated.length > 0;

    // Engine set without egress: what the cache key and label are based on.
    const baseEngines = rotationOn ? [...rotated, ...extras] : plainEngines;
    const engineLabel = baseEngines.length > 0 ? `searxng:${baseEngines.join(',')}` : 'searxng';
    const keyEngines = [...baseEngines].sort().join(',');

    const singlePage = options.pageno !== undefined;
    const firstPage = singlePage ? Math.max(1, options.pageno!) : 1;
    const maxPages = singlePage ? 1 : Math.min(HARD_MAX_PAGES, Math.max(1, config.searxngMaxPages ?? 1));
    const timeoutMs = config.searxngTimeoutMs ?? 10000;
    const queueTimeoutMs = config.searxngQueueTimeoutMs ?? 45000;
    // Split deadlines: the queue budget is spent only while waiting for a slot, the upstream budget only
    // while an HTTP call is in flight. Both are shared across pages and the retry of one search call.
    let queueBudget = queueTimeoutMs;
    let upstreamBudget = timeoutMs;

    const seen = new Set<string>();
    const collected: SearchResult[] = [];
    const partialFailures: SearxngPartialFailure[] = [];
    const backoffChanges: BackoffChange[] = [];
    const meta: SearxngMeta = { queued_ms: 0, upstream_ms: 0, egress: {}, cache: 'miss', unresponsive: [], queue_depth: 0 };
    let rawUnresponsiveLast: unknown[] = [];
    let cachePages = 0;
    let fetchedPages = 0;
    let failure: unknown;

    const upstream = async (pageno: number, enginesParam: string | undefined): Promise<SearxngBody> => {
        const params: Record<string, string | number> = { q: query, format: 'json', pageno };
        if (enginesParam) params.engines = enginesParam;
        if (categories.length > 0) params.categories = categories.join(',');
        if (language) params.language = language;

        if (upstreamBudget <= 0) {
            throw new Error(`SearXNG request failed: overall timeout of ${timeoutMs}ms exceeded`);
        }
        const startedAt = clock.now();
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), upstreamBudget);
        let response: AxiosResponse;
        try {
            response = await httpGet(`${baseUrl}/search`, {
                params,
                proxy: false,
                timeout: upstreamBudget,
                signal: controller.signal,
                maxRedirects: 0,
                headers: { Accept: 'application/json' },
                // parse JSON ourselves so parse failures give a clear message
                responseType: 'text',
                transformResponse: (data) => data,
                validateStatus: () => true
            });
        } catch (error) {
            throw new Error(`SearXNG request failed: ${error instanceof Error ? error.message : String(error)}`);
        } finally {
            clearTimeout(timer);
            const spent = Math.max(0, clock.now() - startedAt);
            upstreamBudget -= spent;
            meta.upstream_ms += spent;
        }

        if (response.status !== 200) {
            throw new Error(`SearXNG returned HTTP ${response.status}`);
        }
        try {
            return typeof response.data === 'string' ? JSON.parse(response.data) : response.data;
        } catch (error) {
            throw new Error(`SearXNG returned invalid JSON (is the json format enabled in settings.yml?): ${error instanceof Error ? error.message : String(error)}`);
        }
    };

    interface CallOutcome {
        body: SearxngBody;
        picks: Map<string, string>;
        failed: string[]; // rotated engines whose pair was sick in this call
        unresponsive: string[];
        rawUnresponsive: unknown[];
    }

    // One scheduled upstream call for the given rotated engines (plus extras when requested).
    const scheduledCall = async (pageno: number, rotatedSubset: string[], withExtras: boolean): Promise<CallOutcome> => {
        const grant = await scheduler.acquire({
            rotated: rotatedSubset,
            egresses,
            hasExtras: withExtras && extras.length > 0,
            timeoutMs: Math.max(0, queueBudget)
        });
        queueBudget -= grant.queuedMs;
        meta.queued_ms += grant.queuedMs;
        meta.queue_depth = Math.max(meta.queue_depth, grant.queueDepth);
        try {
            for (const skipped of grant.skipped) {
                partialFailures.push({
                    engine: skipped.engine,
                    code: 'engine_degraded',
                    message: `${skipped.engine}: all egress pairs are cooling down (earliest recovery in ${Math.max(1, Math.ceil((skipped.retryAt - clock.now()) / 1000))}s); request sent without it`
                });
            }
            const parts = [...[...grant.picks].map(([engine, egress]) => `${engine} ${egress}`), ...(withExtras ? extras : [])];
            const body = await upstream(pageno, parts.length > 0 ? parts.join(',') : undefined);
            const rawResults = Array.isArray(body?.results) ? body.results : [];
            const rawUnresponsive = Array.isArray(body?.unresponsive_engines) ? body.unresponsive_engines! : [];
            const unresponsive = unresponsiveNames(rawUnresponsive);

            // Judge each pair: unresponsive (even if other engines delivered) or zero contribution while
            // the request had results is sick; a contributing pair is healthy again.
            const failed: string[] = [];
            for (const [engine, egress] of grant.picks) {
                const name = `${engine} ${egress}`;
                const sick = unresponsive.includes(name) || (rawResults.length > 0 && !contributed(rawResults, name));
                if (sick) {
                    failed.push(engine);
                    backoffChanges.push(scheduler.reportFailure(engine, egress));
                } else if (contributed(rawResults, name)) {
                    scheduler.reportSuccess(engine, egress);
                }
            }
            return { body, picks: grant.picks, failed, unresponsive, rawUnresponsive };
        } finally {
            grant.release();
        }
    };

    const fetchPage = async (pageno: number): Promise<SearchResult[]> => {
        const cacheKey = JSON.stringify([query, language ?? '', [...categories].sort(), keyEngines, pageno]);
        const cached = cacheGet(cacheKey);
        if (cached) {
            cacheHits++;
            cachePages++;
            console.error(`[local-search] cache hit (hits=${cacheHits}) query="${query.slice(0, QUERY_LOG_MAX)}" page=${pageno}`);
            return cached;
        }
        fetchedPages++;

        let body: SearxngBody;
        let unresponsive: string[];
        let rawUnresponsive: unknown[];
        let partial = false;
        if (rotationOn) {
            const first = await scheduledCall(pageno, rotated, true);
            first.picks.forEach((egress, engine) => { meta.egress[engine] = egress; });
            body = first.body;
            unresponsive = first.unresponsive;
            rawUnresponsive = first.rawUnresponsive;
            partial = first.failed.length > 0 || partialFailures.length > 0 || first.unresponsive.length > 0;
            const rawCount = Array.isArray(body?.results) ? body.results.length : 0;
            if (rawCount === 0 && first.failed.length > 0) {
                // At most one retry, for the failed engines only, through the scheduler with a different
                // healthy pair (it respects pair interval and global limits). If nothing can be scheduled
                // within the remaining budget we fall through to the readable "unresponsive" error.
                try {
                    const retry = await scheduledCall(pageno, first.failed, false);
                    retry.picks.forEach((egress, engine) => { meta.egress[engine] = egress; });
                    body = retry.body;
                    unresponsive = [...first.unresponsive, ...retry.unresponsive];
                    rawUnresponsive = [...first.rawUnresponsive, ...retry.rawUnresponsive];
                } catch (error) {
                    if (!(error instanceof QueueTimeoutError || error instanceof AllPairsCoolingError)) throw error;
                    console.error(`[local-search] retry not possible: ${error.message}`);
                }
            }
        } else {
            const grant = await scheduler.acquire({ rotated: [], egresses: [], hasExtras: true, timeoutMs: Math.max(0, queueBudget) });
            queueBudget -= grant.queuedMs;
            meta.queued_ms += grant.queuedMs;
            meta.queue_depth = Math.max(meta.queue_depth, grant.queueDepth);
            try {
                body = await upstream(pageno, plainEngines.length > 0 ? plainEngines.join(',') : undefined);
            } finally {
                grant.release();
            }
            rawUnresponsive = Array.isArray(body?.unresponsive_engines) ? body.unresponsive_engines! : [];
            unresponsive = unresponsiveNames(rawUnresponsive);
            partial = unresponsive.length > 0;
        }
        rawUnresponsiveLast = rawUnresponsive;
        for (const name of unresponsive) if (!meta.unresponsive.includes(name)) meta.unresponsive.push(name);

        const rawResults = Array.isArray(body?.results) ? body.results : [];
        if (rawResults.length === 0 && unresponsive.length > 0 && pageno === firstPage) {
            throw new Error(`SearXNG: no results - upstream engines rate-limited/unresponsive (${JSON.stringify(rawUnresponsive)}); back off and retry later, this does NOT mean no information exists`);
        }

        const mapped: SearchResult[] = [];
        for (const raw of rawResults) {
            if (!raw.url || !raw.title) continue;
            const src = raw.engines?.length ? raw.engines.join(',') : raw.engine;
            mapped.push({ title: raw.title, url: raw.url, description: raw.content ?? '', source: src ?? '', engine: engineLabel });
        }
        cacheSet(cacheKey, mapped, partial);
        return mapped;
    };

    try {
        for (let pageno = firstPage; pageno < firstPage + maxPages && collected.length < limit; pageno++) {
            const page = await fetchPage(pageno);
            if (page.length === 0) {
                console.error('⚠️ No more results, ending early....');
                break;
            }
            const before = collected.length;
            for (const result of page) {
                if (seen.has(result.url)) continue;
                seen.add(result.url);
                collected.push(result);
            }
            if (collected.length === before) break; // page added nothing new
        }
    } catch (error) {
        failure = error;
        throw error;
    } finally {
        meta.cache = fetchedPages === 0 && cachePages > 0 ? 'hit' : 'miss';
        console.error(`[local-search] search ${JSON.stringify({
            query: query.length > QUERY_LOG_MAX ? `${query.slice(0, QUERY_LOG_MAX)}...` : query,
            cache: meta.cache,
            queued_ms: meta.queued_ms,
            upstream_ms: meta.upstream_ms,
            queue_depth: meta.queue_depth,
            pairs: Object.entries(meta.egress).map(([engine, egress]) => `${engine}:${egress}`).join(','),
            unresponsive: meta.unresponsive,
            backoff: backoffChanges.map((c) => ({ pair: c.pair, level: c.level, until: new Date(c.until).toISOString() })),
            ...(failure ? { error: failure instanceof Error ? failure.message : String(failure) } : {})
        })}`);
    }

    return { results: collected.slice(0, limit), meta, partialFailures, rawUnresponsive: rawUnresponsiveLast };
}

export async function searchSearxng(
    query: string,
    limit: number,
    context?: SearchExecutionContext
): Promise<SearchResult[]> {
    const detailed = await searchSearxngDetailed(query, limit);
    context?.report?.({ meta: detailed.meta, partialFailures: detailed.partialFailures });
    return detailed.results;
}
