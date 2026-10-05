import axios from 'axios';
import type { AxiosRequestConfig, AxiosResponse } from 'axios';
import { config } from '../../config.js';
import { SearchResult } from '../../types.js';

const HARD_MAX_PAGES = 5;
const CACHE_MAX_ENTRIES = 1000;

type HttpGet = (url: string, options: AxiosRequestConfig) => Promise<AxiosResponse>;
type Clock = { now: () => number; sleep: (ms: number) => Promise<void> };

const realClock: Clock = { now: () => Date.now(), sleep: (ms) => new Promise((r) => setTimeout(r, ms)) };
let httpGet: HttpGet = (url, options) => axios.get(url, options);
let clock: Clock = realClock;

// Module state: rotation counters, LRU cache, concurrency gate.
const rotationCounters = new Map<string, number>();
const cache = new Map<string, { expires: number; results: SearchResult[] }>();
let active = 0;
const waiters: Array<() => void> = [];
let nextSlotAt = 0;

export function __setSearxngHttpGetForTests(impl?: HttpGet): void {
    httpGet = impl ?? ((url, options) => axios.get(url, options));
}

export function __setSearxngClockForTests(impl?: Clock): void {
    clock = impl ?? realClock;
}

export function __resetSearxngStateForTests(): void {
    rotationCounters.clear();
    cache.clear();
    waiters.length = 0;
    active = 0;
    nextSlotAt = 0;
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

async function acquireSlot(): Promise<void> {
    const max = Math.max(1, config.searxngMaxConcurrency ?? 2);
    if (active >= max) {
        await new Promise<void>((resolve) => waiters.push(resolve));
    } else {
        active++;
    }
    // Reserve a start time synchronously so concurrent callers are spaced apart.
    const interval = config.searxngMinIntervalMs ?? 1000;
    const startAt = Math.max(clock.now(), nextSlotAt);
    nextSlotAt = startAt + interval;
    const wait = startAt - clock.now();
    if (wait > 0) await clock.sleep(wait);
}

function releaseSlot(): void {
    const next = waiters.shift();
    if (next) next(); // hand the slot over, active stays the same
    else active--;
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

function cacheSet(key: string, results: SearchResult[]): void {
    const ttl = config.searxngCacheTtlMs ?? 86400000;
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

/**
 * Query a self-hosted SearXNG instance via its JSON API.
 * Always plain HTTP (no proxy, no browser) - independent of SEARCH_MODE.
 */
export async function searchSearxng(query: string, limit: number): Promise<SearchResult[]> {
    const baseUrl = config.searxngUrl?.replace(/\/+$/, '');
    if (!baseUrl) {
        throw new Error('SearXNG engine is unavailable: SEARXNG_URL is not set');
    }

    const plainEngines = config.searxngEngines ?? [];
    const categories = config.searxngCategories ?? [];
    const egresses = config.searxngEgresses ?? [];
    const rotated = egresses.length > 0 ? (config.searxngRotateEngines ?? []) : [];
    const extras = config.searxngExtraEngines ?? [];
    const rotationOn = rotated.length > 0;

    // Engine set without egress: what the cache key and label are based on.
    const baseEngines = rotationOn ? [...rotated, ...extras] : plainEngines;
    const engineLabel = baseEngines.length > 0 ? `searxng:${baseEngines.join(',')}` : 'searxng';
    const keyEngines = [...baseEngines].sort().join(',');

    const maxPages = Math.min(HARD_MAX_PAGES, Math.max(1, config.searxngMaxPages ?? 1));
    const timeoutMs = config.searxngTimeoutMs ?? 10000;
    const deadline = clock.now() + timeoutMs;
    const seen = new Set<string>();
    const collected: SearchResult[] = [];

    // Round-robin: one egress per rotated engine, independent counters.
    const pickEgresses = (shift: Map<string, number>): Map<string, string> => {
        const picked = new Map<string, string>();
        for (const engine of rotated) {
            const idx = (rotationCounters.get(engine) ?? 0) + (shift.get(engine) ?? 0);
            picked.set(engine, egresses[idx % egresses.length]);
        }
        return picked;
    };
    const advance = (engine: string, by = 1) => rotationCounters.set(engine, (rotationCounters.get(engine) ?? 0) + by);

    const upstream = async (pageno: number, enginesParam: string | undefined): Promise<SearxngBody> => {
        const params: Record<string, string | number> = { q: query, format: 'json', pageno };
        if (enginesParam) params.engines = enginesParam;
        if (categories.length > 0) params.categories = categories.join(',');
        if (config.searxngLanguage) params.language = config.searxngLanguage;

        await acquireSlot();
        try {
            const remaining = deadline - clock.now();
            if (remaining <= 0) {
                throw new Error(`SearXNG request failed: overall timeout of ${timeoutMs}ms exceeded`);
            }
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), remaining);
            let response: AxiosResponse;
            try {
                response = await httpGet(`${baseUrl}/search`, {
                    params,
                    proxy: false,
                    timeout: remaining,
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
            }

            if (response.status !== 200) {
                throw new Error(`SearXNG returned HTTP ${response.status}`);
            }
            try {
                return typeof response.data === 'string' ? JSON.parse(response.data) : response.data;
            } catch (error) {
                throw new Error(`SearXNG returned invalid JSON (is the json format enabled in settings.yml?): ${error instanceof Error ? error.message : String(error)}`);
            }
        } finally {
            releaseSlot();
        }
    };

    const fetchPage = async (pageno: number): Promise<SearchResult[]> => {
        const cacheKey = JSON.stringify([query, config.searxngLanguage ?? '', [...categories].sort(), keyEngines, pageno]);
        const cached = cacheGet(cacheKey);
        if (cached) return cached;

        const build = (picked: Map<string, string>) =>
            [...[...picked].map(([engine, egress]) => `${engine} ${egress}`), ...extras].join(',');

        let body: SearxngBody;
        let unresponsive: string[] = [];
        let rawUnresponsive: unknown[] = [];
        if (rotationOn) {
            const picked = pickEgresses(new Map());
            rotated.forEach((engine) => advance(engine));
            body = await upstream(pageno, build(picked));
            let rawCount = Array.isArray(body?.results) ? body.results.length : 0;
            rawUnresponsive = Array.isArray(body?.unresponsive_engines) ? body.unresponsive_engines! : [];
            unresponsive = unresponsiveNames(rawUnresponsive);
            const failed = rotated.filter((engine) => unresponsive.includes(`${engine} ${picked.get(engine)}`));
            if (rawCount === 0 && failed.length > 0) {
                // Retry once with the next egress for the engines that failed.
                const next = pickEgresses(new Map());
                const retryPicked = new Map(picked);
                for (const engine of failed) {
                    retryPicked.set(engine, next.get(engine)!);
                    advance(engine);
                }
                body = await upstream(pageno, build(retryPicked));
                rawCount = Array.isArray(body?.results) ? body.results.length : 0;
                rawUnresponsive = Array.isArray(body?.unresponsive_engines) ? body.unresponsive_engines! : [];
            unresponsive = unresponsiveNames(rawUnresponsive);
            }
        } else {
            body = await upstream(pageno, plainEngines.length > 0 ? plainEngines.join(',') : undefined);
            rawUnresponsive = Array.isArray(body?.unresponsive_engines) ? body.unresponsive_engines! : [];
            unresponsive = unresponsiveNames(rawUnresponsive);
        }

        const rawResults = Array.isArray(body?.results) ? body.results : [];
        if (rawResults.length === 0 && unresponsive.length > 0 && pageno === 1) {
            throw new Error(`SearXNG: no results - upstream engines rate-limited/unresponsive (${JSON.stringify(rawUnresponsive)}); back off and retry later, this does NOT mean no information exists`);
        }

        const mapped: SearchResult[] = [];
        for (const raw of rawResults) {
            if (!raw.url || !raw.title) continue;
            const src = raw.engines?.length ? raw.engines.join(',') : raw.engine;
            mapped.push({ title: raw.title, url: raw.url, description: raw.content ?? '', source: src ?? '', engine: engineLabel });
        }
        cacheSet(cacheKey, mapped);
        return mapped;
    };

    for (let pageno = 1; pageno <= maxPages && collected.length < limit; pageno++) {
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

    return collected.slice(0, limit);
}
