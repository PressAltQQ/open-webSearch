import axios from 'axios';
import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
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
export type ApiUsage = { month: string; providers: Record<ApiProvider, { enabled: boolean; used: number; limit: number }> };

const PROVIDERS: ApiProvider[] = ['brave', 'exa'];
const BRAVE_URL = 'https://api.search.brave.com/res/v1/web/search';
const EXA_URL = 'https://api.exa.ai/search';
const REQUEST_TIMEOUT_MS = 10000;
const CACHE_MAX_ENTRIES = 500;
const DEFAULT_LIMIT: Record<ApiProvider, number> = { brave: 900, exa: 1500 };
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
const cache = new Map<string, { expires: number; provider: ApiProvider; results: SearchResult[] }>();

export function __setSearchApiHttpForTests(impl?: HttpRequest): void { http = impl ?? realHttp; }
export function __setSearchApiClockForTests(impl?: ApiClock): void { clock = impl ?? realClock; }
export function __resetSearchApiStateForTests(): void {
    cache.clear();
    delete lastCallAt.brave;
    delete lastCallAt.exa;
}

class BudgetError extends Error {}

const keyOf = (provider: ApiProvider): string | undefined =>
    (provider === 'brave' ? process.env.BRAVE_SEARCH_API_KEY : process.env.EXA_API_KEY)?.trim() || undefined;

export const providerEnabled = (provider: ApiProvider): boolean => keyOf(provider) !== undefined;
export const exaEnabled = (): boolean => providerEnabled('exa');

function limitOf(provider: ApiProvider): number {
    const raw = process.env[provider === 'brave' ? 'BRAVE_MONTHLY_LIMIT' : 'EXA_MONTHLY_LIMIT'];
    const value = raw === undefined || raw.trim() === '' ? NaN : Number(raw);
    return Number.isInteger(value) && value >= 0 ? value : DEFAULT_LIMIT[provider];
}

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

type UsageFile = { month: string; usage: Record<ApiProvider, number> };

const usagePath = (): string => path.join(process.env.XDG_STATE_HOME || path.join(homedir(), '.local', 'state'), 'open-websearch', 'api-usage.json');
const currentMonth = (): string => new Date(clock.now()).toISOString().slice(0, 7);

function loadUsage(): UsageFile {
    const month = currentMonth();
    try {
        const stored = JSON.parse(readFileSync(usagePath(), 'utf8')) as Partial<UsageFile>;
        if (stored.month === month) {
            return { month, usage: { brave: Number(stored.usage?.brave) || 0, exa: Number(stored.usage?.exa) || 0 } };
        }
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
            console.error(`[local-search] api usage file unreadable, starting from zero: ${errorText(error)}`);
        }
    }
    return { month, usage: { brave: 0, exa: 0 } };
}

function saveUsage(file: UsageFile): void {
    const target = usagePath();
    mkdirSync(path.dirname(target), { recursive: true });
    const tmp = `${target}.${process.pid}.tmp`;
    writeFileSync(tmp, JSON.stringify(file));
    renameSync(tmp, target);
}

/** Counts one request before it is sent; throws when the month's budget is used up. */
function reserve(provider: ApiProvider): { used: number; limit: number } {
    const file = loadUsage();
    const limit = limitOf(provider);
    if (file.usage[provider] >= limit) {
        throw new BudgetError(`monthly budget exhausted (${file.usage[provider]}/${limit})`);
    }
    file.usage[provider]++;
    saveUsage(file);
    return { used: file.usage[provider], limit };
}

export function getApiUsage(): ApiUsage {
    const file = loadUsage();
    const entry = (p: ApiProvider) => ({ enabled: providerEnabled(p), used: file.usage[p], limit: limitOf(p) });
    return { month: file.month, providers: { brave: entry('brave'), exa: entry('exa') } };
}

// ---- provider calls -----------------------------------------------------------------------------

function hostOf(url: string): string {
    try { return new URL(url).hostname; } catch { return ''; }
}

async function paidCall(provider: ApiProvider, query: string, request: Omit<Parameters<HttpRequest>[0], 'timeout'>): Promise<any> {
    const { used, limit } = reserve(provider);
    const wait = (lastCallAt[provider] ?? -Infinity) + MIN_INTERVAL_MS[provider] - clock.now();
    if (wait > 0) await clock.sleep(wait);
    lastCallAt[provider] = clock.now();
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
        console.error(`[local-search] api ${redactSecrets(JSON.stringify({
            provider,
            query: query.length > QUERY_LOG_MAX ? `${query.slice(0, QUERY_LOG_MAX)}...` : query,
            status,
            ms: Math.max(0, clock.now() - startedAt),
            used,
            limit
        }))}`);
    }
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

async function exaSearch(query: string, numResults: number, type: 'lite' | 'auto', extra: { includeDomains?: string[]; startPublishedDate?: string } = {}): Promise<SearchResult[]> {
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
    });
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

/** Explicit Exa search (the searchExa tool): neural `auto` type, still bound by the monthly budget. */
export async function searchExaTool(query: string, opts: { numResults?: number; includeDomains?: string[]; startPublishedDate?: string }): Promise<SearchResult[]> {
    if (!exaEnabled()) throw new Error('EXA_API_KEY is not set');
    return exaSearch(query, opts.numResults ?? 8, 'auto', opts);
}

/**
 * Brave first, then Exa. Disabled providers are skipped silently; a provider that fails, is rate limited,
 * returns nothing or is over budget adds a reason and the next one is tried. Cached per query+limit so
 * repeated queries do not spend budget.
 */
export async function searchWithFallback(query: string, limit: number): Promise<FallbackOutcome> {
    const key = JSON.stringify([query, limit]);
    const hit = cache.get(key);
    if (hit && hit.expires > clock.now()) return { provider: hit.provider, results: hit.results, failures: [] };
    cache.delete(key);

    const failures: string[] = [];
    for (const provider of PROVIDERS) {
        if (!providerEnabled(provider)) continue;
        try {
            const results = provider === 'brave' ? await braveSearch(query, limit) : await exaSearch(query, Math.min(10, limit), 'lite');
            if (results.length === 0) { failures.push(`${provider}: returned no results`); continue; }
            const ttl = config.searxngCacheTtlMs ?? 86400000;
            if (ttl > 0) {
                cache.set(key, { expires: clock.now() + ttl, provider, results });
                while (cache.size > CACHE_MAX_ENTRIES) cache.delete(cache.keys().next().value as string);
            }
            return { provider, results, failures };
        } catch (error) {
            failures.push(`${provider}: ${errorText(error)}`);
        }
    }
    return { results: [], failures };
}
