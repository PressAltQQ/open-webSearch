import axios from 'axios';
import type { AxiosRequestConfig, AxiosResponse } from 'axios';
import { config } from '../../config.js';
import { SearchResult } from '../../types.js';
import type { SearchExecutionContext } from '../../core/search/searchService.js';
import { SearxngScheduler, SchedulerBusyError, QueueTimeoutError, QueueFullError, AllPairsCoolingError, QueueAbortedError, NoCapacityError } from './scheduler.js';
import type { Clock, BackoffChange } from './scheduler.js';

export { SchedulerBusyError, QueueTimeoutError, QueueFullError, AllPairsCoolingError, QueueAbortedError, NoCapacityError };

const HARD_MAX_PAGES = 5;
const CACHE_MAX_ENTRIES = 1000;
const QUERY_LOG_MAX = 80;
// A result set where the primary engine was missing is kept only this long, whatever the configured TTL.
const DEGRADED_CACHE_TTL_MS = 60000;

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
const cache = new Map<string, { expires: number; page: PageData }>();
let cacheHits = 0;
const scheduler = new SearxngScheduler(
    () => ({
        maxConcurrency: config.searxngMaxConcurrency ?? 2,
        minIntervalMs: config.searxngMinIntervalMs ?? 1000,
        pairMinIntervalMs: config.searxngPairMinIntervalMs ?? 10000,
        globalMaxPerMin: config.searxngGlobalMaxPerMin ?? 12,
        backoffBaseMs: config.searxngBackoffBaseMs ?? 60000,
        backoffMaxMs: config.searxngBackoffMaxMs ?? 3600000,
        queueTimeoutMs: config.searxngQueueTimeoutMs ?? 45000,
        maxQueueDepth: config.searxngMaxQueueDepth ?? 30
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

export type SearxngExtras = {
    answers: unknown[];
    infoboxes: unknown[];
    suggestions: unknown[];
    corrections: unknown[];
    number_of_results?: number;
};

export interface SearxngDetailedResult {
    results: SearchResult[];
    /** upstream result objects as SearXNG returned them (all fields kept) */
    rawResults: SearxngRawResult[];
    extras: SearxngExtras;
    meta: SearxngMeta;
    partialFailures: SearxngPartialFailure[];
    /** engines skipped because all their pairs were cooling */
    skipped: string[];
    /** raw `unresponsive_engines` entries, SearXNG-shaped */
    rawUnresponsive: unknown[];
    /** set when a rotated primary engine did not contribute: results may be missing key sources */
    degraded?: SearxngDegraded;
}

export type SearxngDegraded = { engines: string[]; reasons: string[] };

export interface SearxngSearchOptions {
    /** fetch exactly this single page (compat endpoint); default is the configured multi-page loop */
    pageno?: number;
    language?: string;
    categories?: string[];
    timeRange?: string;
    safesearch?: string;
    /** client-chosen engines: forwarded as-is, no rotation (global limits still apply) */
    engines?: string[];
    /** aborts the queue wait (client disconnected) */
    signal?: AbortSignal;
}

/** Upstream answered but every engine was unresponsive and nothing was found. */
export class UpstreamUnresponsiveError extends Error {
    constructor(message: string, readonly rawUnresponsive: unknown[], readonly retryAfterSec: number) {
        super(message);
        this.name = 'UpstreamUnresponsiveError';
    }
}

export interface SearxngRawResult {
    title?: string;
    url?: string;
    content?: string;
    engine?: string;
    engines?: string[];
    [key: string]: unknown;
}

interface SearxngBody {
    results?: SearxngRawResult[];
    unresponsive_engines?: unknown[];
    answers?: unknown[];
    infoboxes?: unknown[];
    suggestions?: unknown[];
    corrections?: unknown[];
    number_of_results?: number;
}

// Everything one result page needs to be answered again from the cache, partial state included.
interface PageData {
    mapped: SearchResult[];
    raw: SearxngRawResult[];
    extras: SearxngExtras;
    partial: boolean;
    partialFailures: SearxngPartialFailure[];
    unresponsive: string[];
    rawUnresponsive: unknown[];
    egress: Record<string, string>;
    degraded?: SearxngDegraded;
    /** rotated engines that failed first and answered on the retry */
    recovered?: string[];
}

function cacheGet(key: string): PageData | undefined {
    const entry = cache.get(key);
    if (!entry) return undefined;
    if (entry.expires <= clock.now()) {
        cache.delete(key);
        return undefined;
    }
    cache.delete(key); // refresh LRU position
    cache.set(key, entry);
    return entry.page;
}

function cacheSet(key: string, page: PageData): void {
    let ttl = config.searxngCacheTtlMs ?? 86400000;
    // a response where a requested engine was unresponsive/skipped is incomplete: keep it only briefly
    if (page.partial) ttl = Math.min(ttl, config.searxngPartialCacheTtlMs ?? 600000);
    if (page.degraded) ttl = Math.min(ttl, DEGRADED_CACHE_TTL_MS);
    if (ttl <= 0 || page.mapped.length === 0) return;
    // a degraded answer must not displace a healthy, still valid one
    const existing = cache.get(key);
    if (page.degraded && existing && !existing.page.degraded && existing.expires > clock.now()) return;
    cache.delete(key);
    cache.set(key, { expires: clock.now() + ttl, page });
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

function unresponsiveReason(raw: unknown[], engine: string): string | undefined {
    const hit = raw.find((item) => String(Array.isArray(item) ? item[0] : item).startsWith(`${engine} `));
    const msg = Array.isArray(hit) ? hit[1] : undefined;
    return typeof msg === 'string' && msg ? msg : undefined;
}

function mergeBodies(first: SearxngBody, retry: SearxngBody): SearxngBody {
    const results = (asArray(first?.results) as SearxngRawResult[]).map((r) => ({ ...r }));
    const extra: SearxngRawResult[] = [];
    for (const r of asArray(retry?.results) as SearxngRawResult[]) {
        const same = results.find((x) => x.url === r.url);
        if (!same) { extra.push(r); continue; }
        // same URL from the rotated engine: keep one copy but credit both engines (health is judged on source)
        const names = (x: SearxngRawResult) => x.engines?.length ? x.engines : (x.engine ? [x.engine] : []);
        same.engines = [...new Set([...names(r), ...names(same)])];
    }
    // rotated-engine results first: the final limit must not cut them while the answer is flagged healthy
    return { ...first, results: [...extra, ...results] };
}

const asArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);

// The zero-contribution rule is noisy for sparse queries: only trust it with a decent sample.
const MIN_RESULTS_FOR_ZERO_CONTRIBUTION = 5;

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
    const timeRange = options.timeRange;
    const safesearch = options.safesearch;
    const clientEngines = options.engines ?? [];
    const egresses = config.searxngEgresses ?? [];
    const rotated = egresses.length > 0 ? (config.searxngRotateEngines ?? []) : [];
    const extras = config.searxngExtraEngines ?? [];
    // Rotation only makes sense for plain general web searches. Anything else (other categories, engines
    // picked by the client) is forwarded as-is: it still passes the global ceiling/concurrency/queue, but
    // uses no pairs, so no pair interval or backoff applies to it.
    const forwardRaw = clientEngines.length > 0 || categories.some((c) => c.toLowerCase() !== 'general');
    const rotationOn = rotated.length > 0 && !forwardRaw;
    const fixedEngines = rotationOn ? extras : (clientEngines.length > 0 ? clientEngines : (forwardRaw ? [] : plainEngines));

    // Engine set without egress: what the cache key and label are based on.
    const baseEngines = rotationOn ? [...rotated, ...extras] : fixedEngines;
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
    const rawCollected: SearxngRawResult[] = [];
    const partialFailures: SearxngPartialFailure[] = [];
    const backoffChanges: BackoffChange[] = [];
    const meta: SearxngMeta = { queued_ms: 0, upstream_ms: 0, egress: {}, cache: 'miss', unresponsive: [], queue_depth: 0 };
    let extrasOut: SearxngExtras = { answers: [], infoboxes: [], suggestions: [], corrections: [] };
    let rawUnresponsiveLast: unknown[] = [];
    let cachePages = 0;
    let fetchedPages = 0;
    let upstreamCalls = 0;
    let failure: unknown;
    const degradedAll: SearxngDegraded = { engines: [], reasons: [] };
    const recoveredAll = new Set<string>();

    const upstream = async (pageno: number, enginesParam: string | undefined): Promise<SearxngBody> => {
        const params: Record<string, string | number> = { q: query, format: 'json', pageno };
        if (enginesParam) params.engines = enginesParam;
        if (categories.length > 0) params.categories = categories.join(',');
        if (language) params.language = language;
        if (timeRange) params.time_range = timeRange;
        if (safesearch) params.safesearch = safesearch;

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

    // One scheduled upstream call: the given rotated engines (one pair each) plus `fixed` engines as-is.
    const scheduledCall = async (pageno: number, rotatedSubset: string[], fixed: string[], attempt: 'first' | 'retry', pageFailures: SearxngPartialFailure[], noWait = false): Promise<CallOutcome> => {
        let grant;
        try {
            grant = await scheduler.acquire({
                rotated: rotatedSubset,
                egresses,
                hasExtras: fixed.length > 0,
                timeoutMs: Math.max(0, queueBudget),
                signal: options.signal,
                noWait
            });
        } catch (error) {
            // account for the time really spent waiting, also when the wait ended in an error
            const waited = (error as { queuedMs?: number })?.queuedMs;
            if (typeof waited === 'number') {
                queueBudget -= waited;
                meta.queued_ms += waited;
            }
            throw error;
        }
        queueBudget -= grant.queuedMs;
        meta.queued_ms += grant.queuedMs;
        meta.queue_depth = Math.max(meta.queue_depth, grant.queueDepth);
        upstreamCalls++;
        // One line per upstream call at grant time: per-pair intervals are provable from these timestamps.
        console.error(`[local-search] upstream ${JSON.stringify({
            started_at: new Date(clock.now()).toISOString(),
            pairs: [...grant.picks].map(([engine, egress]) => `${engine}:${egress}`),
            attempt,
            pageno,
            queued_ms: grant.queuedMs,
            queue_depth: grant.queueDepth
        })}`);
        try {
            for (const skipped of grant.skipped) {
                pageFailures.push({
                    engine: skipped.engine,
                    code: 'engine_degraded',
                    message: `${skipped.engine}: all egress pairs are cooling down (earliest recovery in ${Math.max(1, Math.ceil((skipped.retryAt - clock.now()) / 1000))}s); request sent without it`
                });
            }
            const parts = [...[...grant.picks].map(([engine, egress]) => `${engine} ${egress}`), ...fixed];
            const body = await upstream(pageno, parts.length > 0 ? parts.join(',') : undefined);
            const rawResults = asArray(body?.results) as SearxngRawResult[];
            const rawUnresponsive = asArray(body?.unresponsive_engines);
            const unresponsive = unresponsiveNames(rawUnresponsive);

            // Judge each pair: unresponsive (even if other engines delivered) is sick; with a decent sample on
            // the first page, contributing nothing is sick too; a contributing pair is healthy again.
            const trustZero = pageno === 1 && rawResults.length >= MIN_RESULTS_FOR_ZERO_CONTRIBUTION;
            const failed: string[] = [];
            for (const [engine, egress] of grant.picks) {
                const name = `${engine} ${egress}`;
                // on the retry (rotated engines only) a pair that returned nothing is sick whatever the sample size
                const sick = unresponsive.includes(name) || ((trustZero || attempt === 'retry') && !contributed(rawResults, name));
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

    const fetchPage = async (pageno: number): Promise<{ page: PageData; hit: boolean }> => {
        const cacheKey = JSON.stringify([query, language ?? '', [...categories].sort(), keyEngines, pageno, timeRange ?? '', safesearch ?? '']);
        const cached = cacheGet(cacheKey);
        if (cached) {
            cacheHits++;
            console.error(`[local-search] cache hit (hits=${cacheHits}) query="${query.slice(0, QUERY_LOG_MAX)}" page=${pageno}`);
            return { page: cached, hit: true };
        }

        const pageFailures: SearxngPartialFailure[] = [];
        const egress: Record<string, string> = {};
        let body: SearxngBody;
        let unresponsive: string[];
        let rawUnresponsive: unknown[];
        let partial: boolean;
        let degraded: SearxngDegraded | undefined;
        let recovered: string[] = [];
        if (rotationOn) {
            const first = await scheduledCall(pageno, rotated, extras, 'first', pageFailures);
            first.picks.forEach((e, engine) => { egress[engine] = e; });
            body = first.body;
            unresponsive = first.unresponsive;
            rawUnresponsive = first.rawUnresponsive;
            partial = first.failed.length > 0 || pageFailures.length > 0 || first.unresponsive.length > 0;
            let stillFailed = first.failed;
            let retryError: string | undefined;
            const hasResults = asArray(body?.results).length > 0;
            // 0 results: one full-wait retry for the failed engines (readable error otherwise). With results
            // already in hand the retry is optional completeness work (extras-only answers miss the primary
            // engine): first page only, never for the single-page compat path, only while another healthy pair
            // would remain, and it fails fast instead of queueing at the head of the line.
            const retryEngines = hasResults
                ? (pageno === firstPage && !singlePage ? first.failed.filter((e) => scheduler.healthyPairCount(e, egresses) >= 2) : [])
                : first.failed;
            if (retryEngines.length > 0) {
                try {
                    const retry = await scheduledCall(pageno, retryEngines, [], 'retry', pageFailures, hasResults);
                    retry.picks.forEach((e, engine) => { egress[engine] = e; });
                    body = hasResults ? mergeBodies(body, retry.body) : retry.body;
                    stillFailed = first.failed.filter((e) => !retryEngines.includes(e) || retry.failed.includes(e));
                    recovered = retryEngines.filter((e) => !retry.failed.includes(e));
                    unresponsive = [...first.unresponsive, ...retry.unresponsive];
                    rawUnresponsive = [...first.rawUnresponsive, ...retry.rawUnresponsive];
                } catch (error) {
                    if (error instanceof QueueAbortedError) throw error;
                    if (!hasResults && !(error instanceof SchedulerBusyError)) throw error;
                    // the first call's results are kept; say why the retry did not complete
                    retryError = error instanceof Error ? error.message : String(error);
                    console.error(`[local-search] retry not possible: ${retryError}`);
                }
            }
            const cooling = pageFailures.map((f) => f.engine).filter((e) => rotated.includes(e));
            const engines = [...new Set([...stillFailed, ...cooling])];
            if (engines.length > 0) {
                degraded = {
                    engines,
                    reasons: engines.map((e) => `${e}: ${cooling.includes(e) && !stillFailed.includes(e)
                        ? 'all egress pairs cooling down'
                        : (unresponsiveReason(rawUnresponsive, e) ?? 'returned no results')}${retryError && stillFailed.includes(e) ? ` (retry failed: ${retryError})` : ''}`)
                };
            }
        } else {
            const call = await scheduledCall(pageno, [], fixedEngines, 'first', pageFailures);
            body = call.body;
            rawUnresponsive = call.rawUnresponsive;
            unresponsive = call.unresponsive;
            partial = unresponsive.length > 0;
        }

        const rawResults = asArray(body?.results) as SearxngRawResult[];
        if (rawResults.length === 0 && unresponsive.length > 0 && pageno === firstPage) {
            const recoveryMs = rotationOn ? scheduler.earliestRecoveryMs(rotated, egresses) : 0;
            throw new UpstreamUnresponsiveError(
                `SearXNG: no results - upstream engines rate-limited/unresponsive (${JSON.stringify(rawUnresponsive)}); back off and retry later, this does NOT mean no information exists`,
                rawUnresponsive,
                recoveryMs > 0 ? Math.ceil(recoveryMs / 1000) : 60
            );
        }

        const mapped: SearchResult[] = [];
        for (const raw of rawResults) {
            if (!raw.url || !raw.title) continue;
            const src = raw.engines?.length ? raw.engines.join(',') : raw.engine;
            mapped.push({ title: raw.title, url: raw.url, description: raw.content ?? '', source: src ?? '', engine: engineLabel });
        }
        const page: PageData = {
            mapped,
            raw: rawResults,
            extras: {
                answers: asArray(body?.answers),
                infoboxes: asArray(body?.infoboxes),
                suggestions: asArray(body?.suggestions),
                corrections: asArray(body?.corrections),
                ...(typeof body?.number_of_results === 'number' ? { number_of_results: body.number_of_results } : {})
            },
            partial,
            partialFailures: pageFailures,
            unresponsive,
            rawUnresponsive,
            egress,
            ...(degraded ? { degraded } : {}),
            ...(recovered.length > 0 ? { recovered } : {})
        };
        cacheSet(cacheKey, page);
        return { page, hit: false };
    };

    try {
        for (let pageno = firstPage; pageno < firstPage + maxPages && collected.length < limit; pageno++) {
            let fetched: { page: PageData; hit: boolean };
            try {
                fetched = await fetchPage(pageno);
            } catch (error) {
                if (collected.length > 0 && error instanceof SchedulerBusyError) {
                    // keep what the earlier pages delivered, but say the answer is incomplete
                    partialFailures.push({ engine: 'searxng', code: 'engine_degraded', message: `page ${pageno} not fetched: ${error.message}` });
                    break;
                }
                throw error;
            }
            const { page, hit } = fetched;
            if (hit) cachePages++; else fetchedPages++;
            Object.assign(meta.egress, page.egress);
            for (const name of page.unresponsive) if (!meta.unresponsive.includes(name)) meta.unresponsive.push(name);
            partialFailures.push(...page.partialFailures);
            rawUnresponsiveLast = page.rawUnresponsive;
            for (const e of page.recovered ?? []) recoveredAll.add(e);
            for (const e of page.degraded?.engines ?? []) if (!degradedAll.engines.includes(e)) degradedAll.engines.push(e);
            for (const r of page.degraded?.reasons ?? []) if (!degradedAll.reasons.includes(r)) degradedAll.reasons.push(r);
            if (pageno === firstPage) extrasOut = page.extras;
            rawCollected.push(...page.raw);

            if (page.mapped.length === 0) {
                console.error('⚠️ No more results, ending early....');
                break;
            }
            const before = collected.length;
            for (const result of page.mapped) {
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
            calls: upstreamCalls,
            pairs: Object.entries(meta.egress).map(([engine, e]) => `${engine}:${e}`).join(','),
            skipped: partialFailures.filter((f) => f.engine !== 'searxng').map((f) => f.engine),
            unresponsive: meta.unresponsive,
            backoff: backoffChanges.map((c) => ({ pair: c.pair, level: c.level, until: new Date(c.until).toISOString() })),
            ...(failure ? { error: failure instanceof Error ? failure.message : String(failure) } : {})
        })}`);
    }

    const finalResults = collected.slice(0, limit);
    // a recovered engine only counts as healthy if one of its results survived the limit
    for (const e of recoveredAll) {
        if (finalResults.some((r) => r.source.split(',').some((src) => src.startsWith(`${e} `)))) continue;
        if (!degradedAll.engines.includes(e)) degradedAll.engines.push(e);
        degradedAll.reasons.push(`${e}: no results from the primary engine within the limit`);
    }

    return {
        results: finalResults,
        rawResults: rawCollected,
        extras: extrasOut,
        meta,
        partialFailures,
        skipped: partialFailures.filter((f) => f.engine !== 'searxng').map((f) => f.engine),
        rawUnresponsive: rawUnresponsiveLast,
        ...(degradedAll.engines.length > 0 ? { degraded: degradedAll } : {})
    };
}

export async function searchSearxng(
    query: string,
    limit: number,
    context?: SearchExecutionContext
): Promise<SearchResult[]> {
    const detailed = await searchSearxngDetailed(query, limit);
    context?.report?.({ meta: detailed.meta, partialFailures: detailed.partialFailures, degraded: detailed.degraded });
    return detailed.results;
}
