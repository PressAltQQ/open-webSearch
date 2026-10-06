import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { config } from '../config.js';
import { parseCompatListen, startSearxngCompatServer } from '../adapters/http/searxngCompat.js';
import { __setSearxngHttpGetForTests, __resetSearxngStateForTests } from '../engines/searxng/searxng.js';

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

function request(port: number, method: string, path: string, body?: string): Promise<{ status: number; headers: http.IncomingHttpHeaders; json: any }> {
    return new Promise((resolve, reject) => {
        const req = http.request({ host: '127.0.0.1', port, method, path, headers: body ? { 'Content-Type': 'application/x-www-form-urlencoded' } : {} }, (res) => {
            let data = '';
            res.on('data', (c) => { data += c; });
            res.on('end', () => resolve({ status: res.statusCode ?? 0, headers: res.headers, json: data ? JSON.parse(data) : undefined }));
        });
        req.on('error', reject);
        req.end(body);
    });
}

function testListenValidation(): void {
    assert(parseCompatListen('127.0.0.1:8888').port === 8888, 'parses loopback v4');
    assert(parseCompatListen('[::1]:8888').host === '::1', 'parses loopback v6');
    assert(parseCompatListen('localhost:1').host === 'localhost', 'localhost ok');
    for (const bad of ['0.0.0.0:8888', '192.168.1.5:8888', 'example.com:80', '8888', '127.0.0.1', '127.0.0.1:99999']) {
        let failed = false;
        try { parseCompatListen(bad); } catch { failed = true; }
        assert(failed, `rejects "${bad}"`);
    }
    console.log('✅ compat: listen address validation (loopback only)');
}

async function main(): Promise<void> {
    testListenValidation();

    __resetSearxngStateForTests();
    Object.assign(config, {
        searxngUrl: 'http://127.0.0.1:8889', searxngEngines: [], searxngCategories: [], searxngLanguage: undefined,
        searxngMaxPages: 1, searxngMinIntervalMs: 0, searxngCacheTtlMs: 60_000, searxngMaxConcurrency: 2,
        searxngTimeoutMs: 5000, searxngRotateEngines: [], searxngEgresses: [], searxngExtraEngines: [],
        searxngPairMinIntervalMs: 0, searxngGlobalMaxPerMin: 1000, searxngQueueTimeoutMs: 5000
    });
    const upstreamCalls: Array<Record<string, unknown>> = [];
    __setSearxngHttpGetForTests(async (_url, options) => {
        upstreamCalls.push(options.params as Record<string, unknown>);
        return { status: 200, data: { results: [{ title: 'T', url: 'https://e.com/1', content: 'C', engine: 'duckduckgo', engines: ['duckduckgo', 'bing'] }], unresponsive_engines: [['yahoo', 'timeout']] } } as any;
    });

    const server = await startSearxngCompatServer('127.0.0.1:0');
    const port = (server.address() as AddressInfo).port;
    try {
        const get = await request(port, 'GET', '/search?q=hello&format=json&pageno=2&language=en&categories=general');
        assert(get.status === 200, `GET status ${get.status}`);
        assert(get.json.query === 'hello' && get.json.number_of_results === 1, 'shape');
        assert(get.json.results[0].title === 'T' && get.json.results[0].url === 'https://e.com/1' && get.json.results[0].content === 'C' && get.json.results[0].engine === 'duckduckgo,bing', `result: ${JSON.stringify(get.json.results[0])}`);
        assert(JSON.stringify(get.json.unresponsive_engines) === '[["yahoo","timeout"]]', 'unresponsive passthrough');
        assert(get.json.meta && get.json.meta.cache === 'miss', 'meta present');
        assert(upstreamCalls.length === 1 && upstreamCalls[0].pageno === 2 && upstreamCalls[0].language === 'en' && upstreamCalls[0].categories === 'general', `upstream params ${JSON.stringify(upstreamCalls[0])}`);

        // same query again: served by the shared cache (no second upstream call)
        const again = await request(port, 'GET', '/search?q=hello&format=json&pageno=2&language=en&categories=general');
        assert(again.status === 200 && again.json.meta.cache === 'hit' && upstreamCalls.length === 1, 'cache shared with the scheduler path');

        const post = await request(port, 'POST', '/search', 'q=posted&format=json');
        assert(post.status === 200 && post.json.query === 'posted' && (upstreamCalls.length as number) === 2, 'POST form works');

        assert((await request(port, 'GET', '/search?q=x&format=html')).status === 400, 'non-json -> 400');
        assert((await request(port, 'GET', '/search?q=x')).status === 400, 'missing format -> 400');
        assert((await request(port, 'GET', '/search?format=json')).status === 400, 'missing q -> 400');
        assert((await request(port, 'GET', '/nope')).status === 404, '404');
        assert((upstreamCalls.length as number) === 2, 'rejected requests never reach upstream');

        // saturated queue -> 429 + Retry-After
        config.searxngGlobalMaxPerMin = 1;
        config.searxngQueueTimeoutMs = 100;
        __resetSearxngStateForTests();
        await request(port, 'GET', '/search?q=first&format=json'); // takes the only slot of the minute
        const limited = await request(port, 'GET', '/search?q=second&format=json');
        assert(limited.status === 429, `429, got ${limited.status}`);
        assert(/queue wait exceeded/.test(limited.json.error) && limited.headers['retry-after'] !== undefined, `429 body/headers: ${JSON.stringify(limited.json)} ${limited.headers['retry-after']}`);
        console.log('✅ compat: GET/POST shape, shared cache, 400/404, 429 with Retry-After');
    } finally {
        await new Promise<void>((resolve) => server.close(() => resolve()));
        __setSearxngHttpGetForTests();
    }
    console.log('\nSearXNG compat endpoint tests passed.');
}

main().catch((error) => { console.error(error); process.exit(1); });
