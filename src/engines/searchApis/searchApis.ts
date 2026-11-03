import axios from 'axios';
import { closeSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, writeSync } from 'node:fs';
import { homedir } from 'node:os';
import path from 'node:path';
import { config } from '../../config.js';
import type { SearchResult } from '../../types.js';

/**
 * Key-based search APIs (Brave Search API, Exa) used as paid fallbacks when the SearXNG path is degraded,
 * plus the explicit searchExa tool. Keys come from env only; every request is counted against a persisted
 * calendar-month (UTC) budget before it is sent.
 */
export type ApiProvider = 'brave' | 'exa';
export type HttpRequest = (options: { method: 'GET' | 'POST'; url: string; headers: Record<string, string>; params?: Record<string, unknown>; data?: unknown; timeout: number }) => Promise<{ status: number; data: any }>;
export type ApiClock = { now: () => number; sleep: (ms: number) => Promise<void> };
export type FallbackOutcome = { provider?: ApiProvider; results: SearchResult[]; failures: string[] };
export type ApiUsage = {
    month: string;
    providers: Record<ApiProvider, { enabled: boolean; used: number; limit: number }>;
    /** the explicit searchExa tool: its own cap, also counted in providers.exa */
    exaTool: { used: number; limit: number };
    /** set when the counter file cannot be trusted; paid calls are refused until it is fixed */
    error?: string;
};

const PROVIDERS: ApiProvider[] = ['brave', 'exa'];
const BRAVE_URL = 'https://api.search.brave.com/res/v1/web/search';
const EXA_URL = 'https://api.exa.ai/search';
const REQUEST_TIMEOUT_MS = 10000;
const CACHE_MAX_ENTRIES = 500;
const DEFAULT_LIMIT: Record<ApiProvider, number> = { brave: 900, exa: 1400 };
const DEFAULT_EXA_TOOL_LIMIT = 300;
const MIN_INTERVAL_MS: Record<ApiProvider, number> = { brave: 1000, exa: 200 };
const QUERY_LOG_MAX = 80;

const realClock: ApiClock = { now: () => Date.now(), sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)) };
const realHttp: HttpRequest = async ({ method, url, headers, params, data, timeout }) => {
    const response = await axios.request({ method, url, headers, params, data, timeout, proxy: false, maxRedirects: 0, validateStatus: () => true });
    return { status: response.status, data: response.data };
};
let http: HttpRequest = realHttp;
let clock: ApiClock = realClock;
const lastCallAt: Partial<Record<ApiProvider, number>> = {};
const cache = new Map<string, { expires: number; value: unknown }>();
const inflight = new Map<string, Promise<unknown>>();
const loggedOnce = new Set<string>();

export function __setSearchApiHttpForTests(impl?: HttpRequest): void { http = impl ?? realHttp; }
export function __setSearchApiClockForTests(impl?: ApiClock): void { clock = impl ?? realClock; }
export function __resetSearchApiStateForTests(): void {
    cache.clear();
    delete lastCallAt.brave;
    delete lastCallAt.exa;
    inflight.clear();
    loggedOnce.clear();
}

function logOnce(id: string, line: string): void {
    if (loggedOnce.has(id)) return;
    loggedOnce.add(id);
    console.error(line);
}

class BudgetError extends Error {}

const keyOf = (provider: ApiProvider): string | undefined =>
    (provider === 'brave' ? process.env.BRAVE_SEARCH_API_KEY : process.env.EXA_API_KEY)?.trim() || undefined;

export const providerEnabled = (provider: ApiProvider): boolean => keyOf(provider) !== undefined;
export const exaEnabled = (): boolean => providerEnabled('exa');

function parseLimit(name: string, fallback: number): number {
    const raw = process.env[name];
    const value = raw === undefined || raw.trim() === '' ? NaN : Number(raw);
    return Number.isInteger(value) && value >= 0 ? value : fallback;
}

const limitOf = (provider: ApiProvider): number => parseLimit(provider === 'brave' ? 'BRAVE_MONTHLY_LIMIT' : 'EXA_MONTHLY_LIMIT', DEFAULT_LIMIT[provider]);
const exaToolLimit = (): number => parseLimit('EXA_TOOL_MONTHLY_LIMIT', DEFAULT_EXA_TOOL_LIMIT);

/** Removes configured API keys from any text that might reach logs or tool output. */
export function redactSecrets(text: string): string {
    let out = text;
    for (const provider of PROVIDERS) {
        const key = keyOf(provider);
        if (key && key.length >= 6) out = out.split(key).join('[REDACTED]');
    }
    return out;
}

const errorText = (error: unknown): string => redactSecrets(error instanceof Error ? error.message : String(error));

// ---- persisted monthly budget -------------------------------------------------------------------
// Single process only: the counter file has no lock. Run one open-websearch instance per state dir.

type UsageFile = { month: string; usage: { brave: number; exa: number; exaTool: number } };

const usagePath = (): string => path.join(process.env.XDG_STATE_HOME || path.join(homedir(), '.local', 'state'), 'open-websearch', 'api-usage.json');
const currentMonth = (): string => new Date(clock.now()).toISOString().slice(0, 7);
const validCount = (v: unknown): v is number => typeof v === 'number' && Number.isInteger(v) && v >= 0;

/** Fails closed: an unreadable or implausible counter file refuses paid calls instead of resetting to zero. */
function loadUsage(): UsageFile {
    const month = currentMonth();
    const file = usagePath();
    const unreadable = (): BudgetError => {
        logOnce(`unreadable:${month}:${file}`, `[local-search] api usage file unreadable, paid calls refused until fixed: ${file}`);
        return new BudgetError(`usage file unreadable: ${file}`);
    };
    let text: string;
    try {
        text = readFileSync(file, 'utf8');
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { month, usage: { brave: 0, exa: 0, exaTool: 0 } };
        throw unreadable();
    }
    let stored: any;
    try { stored = JSON.parse(text); } catch { throw unreadable(); }
    if (typeof stored?.month !== 'string' || typeof stored?.usage !== 'object' || stored.usage === null) throw unreadable();
    const u = stored.usage;
    for (const k of ['brave', 'exa', 'exaTool']) {
        if (u[k] !== undefined && !validCount(u[k])) throw unreadable();
    }
    if (stored.month !== month) return { month, usage: { brave: 0, exa: 0, exaTool: 0 } };
    return { month, usage: { brave: u.brave ?? 0, exa: u.exa ?? 0, exaTool: u.exaTool ?? 0 } };
}

function saveUsage(file: UsageFile): void {
    const target = usagePath();
    mkdirSync(path.dirname(target), { recursive: true });
    const tmp = `${target}.${process.pid}.tmp`;
    const fd = openSync(tmp, 'w');
    try {
        writeSync(fd, JSON.stringify(file));
        fsyncSync(fd);
    } finally {
        closeSync(fd);
    }
    renameSync(tmp, target);
}

/** Counts one request before it is sent; throws when the month's budget is used up. */
function reserve(provider: ApiProvider, tool: boolean): { used: number; limit: number; month: string } {
    const file = loadUsage();
    const limit = limitOf(provider);
    if (file.usage[provider] >= limit) {
        throw new BudgetError(`monthly budget exhausted (${file.usage[provider]}/${limit})`);
    }
    if (tool && file.usage.exaTool >= exaToolLimit()) {
        throw new BudgetError(`searchExa monthly budget exhausted (${file.usage.exaTool}/${exaToolLimit()})`);
    }
    file.usage[provider]++;
    if (tool) file.usage.exaTool++;
    saveUsage(file);
    return { used: file.usage[provider], limit, month: file.month };
}

export function getApiUsage(): ApiUsage {
    const entry = (p: ApiProvider, used: number) => ({ enabled: providerEnabled(p), used, limit: limitOf(p) });
    try {
        const file = loadUsage();
        return { month: file.month, providers: { brave: entry('brave', file.usage.brave), exa: entry('exa', file.usage.exa) }, exaTool: { used: file.usage.exaTool, limit: exaToolLimit() } };
    } catch (error) {
        return { month: currentMonth(), providers: { brave: entry('brave', 0), exa: entry('exa', 0) }, exaTool: { used: 0, limit: exaToolLimit() }, error: errorText(error) };
    }
}

// ---- provider calls -----------------------------------------------------------------------------

function hostOf(url: string): string {
    try { return new URL(url).hostname; } catch { return ''; }
}

const clip = (query: string): string => (query.length > QUERY_LOG_MAX ? `${query.slice(0, QUERY_LOG_MAX)}...` : query);

async function paidCall(provider: ApiProvider, query: string, request: Omit<Parameters<HttpRequest>[0], 'timeout'>, tool = false): Promise<any> {
    let reserved: { used: number; limit: number; month: string };
    try {
        reserved = reserve(provider, tool);
    } catch (error) {
        if (error instanceof BudgetError) {
            const { month } = getApiUsage();
            logOnce(`exhausted:${month}:${provider}${tool ? ':tool' : ''}`, `[local-search] api budget exhausted for ${provider}${tool ? ' (searchExa tool cap)' : ''} in ${month}: ${errorText(error)}`);
            console.error(`[local-search] api ${redactSecrets(JSON.stringify({ provider, query: clip(query), status: 'over_budget', ms: 0, used: getApiUsage().providers[provider].used, limit: limitOf(provider) }))}`);
        }
        throw error;
    }
    const { used, limit } = reserved;
    // Claim the rate-limit slot synchronously (before any await) so concurrent callers queue up behind each other.
    const slot = Math.max(clock.now(), (lastCallAt[provider] ?? -Infinity) + MIN_INTERVAL_MS[provider]);
    lastCallAt[provider] = slot;
    const wait = slot - clock.now();
    if (wait > 0) await clock.sleep(wait);
    const startedAt = clock.now();
    let status: number | string = 'error';
    try {
        const response = await http({ ...request, timeout: REQUEST_TIMEOUT_MS });
        status = response.status;
        if (response.status === 429) throw new Error('HTTP 429 (rate limited)');
        if (response.status !== 200) throw new Error(`HTTP ${response.status}`);
        return response.data;
    } catch (error) {
        throw new Error(errorText(error));
    } finally {
        console.error(`[local-search] api ${redactSecrets(JSON.stringify({ provider, query: clip(query), status, ms: Math.max(0, clock.now() - startedAt), used, limit }))}`);
    }
}

/** Cache (healthy TTL) + in-flight dedupe: identical concurrent or repeated queries pay once. */
async function cachedCall<T>(key: string, run: () => Promise<T>, cacheable: (value: T) => boolean): Promise<T> {
    const hit = cache.get(key);
    if (hit && hit.expires > clock.now()) return hit.value as T;
    cache.delete(key);
    const pending = inflight.get(key);
    if (pending) return pending as Promise<T>;
    const promise = (async () => {
        const value = await run();
        const ttl = config.searxngCacheTtlMs ?? 86400000;
        if (ttl > 0 && cacheable(value)) {
            cache.set(key, { expires: clock.now() + ttl, value });
            while (cache.size > CACHE_MAX_ENTRIES) cache.delete(cache.keys().next().value as string);
        }
        return value;
    })().finally(() => inflight.delete(key));
    inflight.set(key, promise);
    return promise;
}

async function braveSearch(query: string, limit: number): Promise<SearchResult[]> {
    const data = await paidCall('brave', query, {
        method: 'GET',
        url: BRAVE_URL,
        headers: { Accept: 'application/json', 'X-Subscription-Token': keyOf('brave')! },
        params: { q: query, count: Math.min(20, Math.max(1, limit)) }
    });
    const items: any[] = Array.isArray(data?.web?.results) ? data.web.results : [];
    return items.filter((r) => r?.url && r?.title).slice(0, limit).map((r) => ({
        title: String(r.title), url: String(r.url), description: String(r.description ?? ''), source: hostOf(String(r.url)), engine: 'brave-api'
    }));
}

async function exaSearch(query: string, numResults: number, type: 'lite' | 'auto', extra: { includeDomains?: string[]; startPublishedDate?: string } = {}, tool = false): Promise<SearchResult[]> {
    const data = await paidCall('exa', query, {
        method: 'POST',
        url: EXA_URL,
        headers: { 'Content-Type': 'application/json', 'x-api-key': keyOf('exa')! },
        data: {
            query,
            numResults: Math.min(10, Math.max(1, numResults)),
            type,
            ...(extra.includeDomains?.length ? { includeDomains: extra.includeDomains } : {}),
            ...(extra.startPublishedDate ? { startPublishedDate: extra.startPublishedDate } : {})
        }
    }, tool);
    const items: any[] = Array.isArray(data?.results) ? data.results : [];
    return items.filter((r) => r?.url && r?.title).map((r) => ({
        title: String(r.title),
        url: String(r.url),
        description: typeof r.text === 'string' ? r.text.slice(0, 500) : (Array.isArray(r.highlights) ? r.highlights.join(' ') : ''),
        source: hostOf(String(r.url)),
        engine: 'exa-api',
        ...(r.publishedDate ? { publishedDate: String(r.publishedDate) } : {})
    } as SearchResult));
}

const HOSTNAME = /^(\*\.)?[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$/i;

/** Explicit Exa search (the searchExa tool): neural `auto` type, bound by its own cap and the shared Exa cap. */
export async function searchExaTool(query: string, opts: { numResults?: number; includeDomains?: string[]; startPublishedDate?: string }): Promise<SearchResult[]> {
    if (!exaEnabled()) throw new Error('EXA_API_KEY is not set');
    // validate before spending anything
    if (opts.startPublishedDate !== undefined && Number.isNaN(Date.parse(opts.startPublishedDate))) {
        throw new Error(`invalid startPublishedDate "${opts.startPublishedDate}": expected an ISO 8601 date`);
    }
    for (const d of opts.includeDomains ?? []) {
        if (!HOSTNAME.test(d)) throw new Error(`invalid includeDomains entry "${d}": expected a bare hostname like example.com (no scheme, path or spaces)`);
    }
    const numResults = Math.min(10, Math.max(1, opts.numResults ?? 8));
    const key = JSON.stringify(['exaTool', query, numResults, [...(opts.includeDomains ?? [])].sort(), opts.startPublishedDate ?? '']);
    return cachedCall(key, () => exaSearch(query, numResults, 'auto', opts, true), (r) => r.length > 0);
}

/**
 * Brave first, then Exa (unless `braveOnly`). Disabled providers are skipped silently; a provider that fails,
 * is rate limited, returns nothing or is over budget adds a reason and the next one is tried.
 */
export async function searchWithFallback(query: string, limit: number, options: { braveOnly?: boolean } = {}): Promise<FallbackOutcome> {
    const key = JSON.stringify(['fallback', query, limit, options.braveOnly === true]);
    return cachedCall<FallbackOutcome>(key, async () => {
        const failures: string[] = [];
        for (const provider of PROVIDERS) {
            if (provider === 'exa' && options.braveOnly) continue;
            if (!providerEnabled(provider)) continue;
            try {
                const results = provider === 'brave' ? await braveSearch(query, limit) : await exaSearch(query, Math.min(10, limit), 'lite');
                if (results.length === 0) { failures.push(`${provider}: returned no results`); continue; }
                return { provider, results, failures };
            } catch (error) {
                failures.push(`${provider}: ${errorText(error)}`);
            }
        }
        return { results: [], failures };
    }, (o) => o.provider !== undefined);
}
