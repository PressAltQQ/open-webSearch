import axios from 'axios';
import type { AxiosRequestConfig, AxiosResponse } from 'axios';
import { config } from '../../config.js';
import { SearchResult } from '../../types.js';

const MAX_PAGES = 5;

type HttpGet = (url: string, options: AxiosRequestConfig) => Promise<AxiosResponse>;

let httpGet: HttpGet = (url, options) => axios.get(url, options);

export function __setSearxngHttpGetForTests(impl?: HttpGet): void {
    httpGet = impl ?? ((url, options) => axios.get(url, options));
}

interface SearxngRawResult {
    title?: string;
    url?: string;
    content?: string;
    engine?: string;
    engines?: string[];
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

    const engines = config.searxngEngines ?? [];
    const categories = config.searxngCategories ?? [];
    const engineLabel = engines.length > 0
        ? `searxng:${engines.join(',')}`
        : 'searxng';

    const timeoutMs = config.searxngTimeoutMs ?? 10000;
    const deadline = Date.now() + timeoutMs;
    const seen = new Set<string>();
    const collected: SearchResult[] = [];

    for (let pageno = 1; pageno <= MAX_PAGES && collected.length < limit; pageno++) {
        const params: Record<string, string | number> = { q: query, format: 'json', pageno };
        if (engines.length > 0) params.engines = engines.join(',');
        if (categories.length > 0) params.categories = categories.join(',');
        if (config.searxngLanguage) params.language = config.searxngLanguage;

        const remaining = deadline - Date.now();
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

        let body: { results?: SearxngRawResult[]; unresponsive_engines?: unknown[] };
        try {
            body = typeof response.data === 'string' ? JSON.parse(response.data) : response.data;
        } catch (error) {
            throw new Error(`SearXNG returned invalid JSON (is the json format enabled in settings.yml?): ${error instanceof Error ? error.message : String(error)}`);
        }

        const rawResults = Array.isArray(body?.results) ? body.results : [];
        if (rawResults.length === 0) {
            const unresponsive = Array.isArray(body?.unresponsive_engines) ? body.unresponsive_engines : [];
            if (unresponsive.length > 0 && collected.length === 0) {
                throw new Error(`SearXNG returned no results; unresponsive engines: ${JSON.stringify(unresponsive)}`);
            }
            console.error('⚠️ No more results, ending early....');
            break;
        }

        const before = collected.length;
        for (const raw of rawResults) {
            if (!raw.url || !raw.title || seen.has(raw.url)) continue;
            seen.add(raw.url);
            const upstream = raw.engines?.length ? raw.engines.join(',') : raw.engine;
            collected.push({
                title: raw.title,
                url: raw.url,
                description: raw.content ?? '',
                source: upstream ?? '',
                engine: engineLabel
            });
        }
        if (collected.length === before) {
            break; // page added nothing new
        }
    }

    return collected.slice(0, limit);
}
