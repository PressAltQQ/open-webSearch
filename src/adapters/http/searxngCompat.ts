// SearXNG-compatible JSON endpoint served from inside the MCP process.
// Goal: every consumer (MCP tools and plain `curl /search?format=json`) goes through the SAME cache,
// scheduler, rotation and backoff, so nobody can bypass the rate limits by talking to SearXNG directly.
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { searchSearxngDetailed, QueueTimeoutError, AllPairsCoolingError } from '../../engines/searxng/searxng.js';

const MAX_BODY_BYTES = 64 * 1024;
const COMPAT_RESULTS_PER_PAGE = 1000; // a page is whatever SearXNG returned; do not truncate it

export function parseCompatListen(value: string): { host: string; port: number } {
    const match = /^(?:\[([^\]]+)\]|([^:]+)):(\d{1,5})$/.exec(value.trim());
    if (!match) throw new Error(`invalid SEARXNG_COMPAT_LISTEN "${value}", expected host:port (e.g. 127.0.0.1:8888)`);
    const host = (match[1] ?? match[2]).toLowerCase();
    const port = Number(match[3]);
    if (port > 65535) throw new Error(`invalid SEARXNG_COMPAT_LISTEN port in "${value}"`);
    const loopback = host === 'localhost' || host === '::1' || /^127(\.\d{1,3}){3}$/.test(host);
    if (!loopback) throw new Error(`SEARXNG_COMPAT_LISTEN must be a loopback address (127.x.x.x, ::1, localhost), got "${host}"`);
    return { host, port };
}

function sendJson(res: http.ServerResponse, status: number, body: unknown, headers: Record<string, string> = {}): void {
    const payload = JSON.stringify(body);
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(payload), ...headers });
    res.end(payload);
}

function readBody(req: http.IncomingMessage): Promise<string> {
    return new Promise((resolve, reject) => {
        let size = 0;
        const chunks: Buffer[] = [];
        req.on('data', (chunk: Buffer) => {
            size += chunk.length;
            if (size > MAX_BODY_BYTES) {
                reject(new Error('request body too large'));
                req.destroy();
                return;
            }
            chunks.push(chunk);
        });
        req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
        req.on('error', reject);
    });
}

export function createSearxngCompatServer(): http.Server {
    return http.createServer(async (req, res) => {
        try {
            const url = new URL(req.url ?? '/', 'http://localhost');
            if (url.pathname !== '/search') return sendJson(res, 404, { error: 'not found' });
            if (req.method !== 'GET' && req.method !== 'POST') return sendJson(res, 405, { error: 'method not allowed' }, { Allow: 'GET, POST' });

            const params = new URLSearchParams(url.search);
            if (req.method === 'POST') {
                for (const [key, value] of new URLSearchParams(await readBody(req))) params.set(key, value);
            }

            const q = params.get('q')?.trim();
            if (!q) return sendJson(res, 400, { error: 'missing q parameter' });
            if (params.get('format') !== 'json') return sendJson(res, 400, { error: 'only format=json is supported' });
            const pageno = params.has('pageno') ? Number(params.get('pageno')) : 1;
            if (!Number.isInteger(pageno) || pageno < 1) return sendJson(res, 400, { error: 'invalid pageno' });
            const categories = params.get('categories')?.split(',').map((c) => c.trim()).filter(Boolean);

            try {
                const found = await searchSearxngDetailed(q, COMPAT_RESULTS_PER_PAGE, {
                    pageno,
                    language: params.get('language') || undefined,
                    categories: categories && categories.length > 0 ? categories : undefined
                });
                return sendJson(res, 200, {
                    query: q,
                    number_of_results: found.results.length,
                    results: found.results.map((r) => ({ title: r.title, url: r.url, content: r.description, engine: r.source })),
                    unresponsive_engines: found.rawUnresponsive,
                    meta: found.meta
                });
            } catch (error) {
                if (error instanceof QueueTimeoutError || error instanceof AllPairsCoolingError) {
                    return sendJson(res, 429, { error: error.message }, { 'Retry-After': String(error.retryAfterSec) });
                }
                return sendJson(res, 502, { error: error instanceof Error ? error.message : String(error) });
            }
        } catch (error) {
            sendJson(res, 400, { error: error instanceof Error ? error.message : String(error) });
        }
    });
}

/** Start the compat listener; resolves once it is bound (port 0 picks an ephemeral port). */
export function startSearxngCompatServer(listen: string): Promise<http.Server> {
    const { host, port } = parseCompatListen(listen);
    const server = createSearxngCompatServer();
    return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(port, host, () => {
            server.off('error', reject);
            const address = server.address() as AddressInfo;
            console.error(`✅ SearXNG-compatible endpoint listening on http://${host}:${address.port}/search (same cache/scheduler as the MCP tools)`);
            resolve(server);
        });
    });
}
