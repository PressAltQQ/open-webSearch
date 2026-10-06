import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { config } from '../config.js';
import { parseCompatListen, startSearxngCompatServer } from '../adapters/http/searxngCompat.js';
import { __setSearxngHttpGetForTests, __resetSearxngStateForTests, __getSearxngSchedulerForTests } from '../engines/searxng/searxng.js';

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
        if (String((options.params as any).q) === 'allbad') {
            return { status: 200, data: { results: [], unresponsive_engines: [['duckduckgo', 'CAPTCHA'], ['bing', 'timeout']] } } as any;
        }
        return { status: 200, data: {
            results: [{ title: 'T', url: 'https://e.com/1', content: 'C', engine: 'duckduckgo', engines: ['duckduckgo', 'bing'], score: 2.5, publishedDate: '2026-01-01', category: 'general' }],
            answers: [{ answer: 'a1' }], infoboxes: [], suggestions: ['s1'], corrections: ['c1'], number_of_results: 1234,
            unresponsive_engines: [['yahoo', 'timeout']] } } as any;
    });

    const server = await startSearxngCompatServer('127.0.0.1:0');
    const port = (server.address() as AddressInfo).port;
    try {
        const get = await request(port, 'GET', '/search?q=hello&format=json&pageno=2&language=en&categories=general');
        assert(get.status === 200, `GET status ${get.status}`);
        assert(get.json.query === 'hello' && get.json.results.length === 1, 'shape');
        // raw upstream objects pass through untouched, plus the top-level SearXNG fields
        assert(JSON.stringify(get.json.results[0]) === JSON.stringify({ title: 'T', url: 'https://e.com/1', content: 'C', engine: 'duckduckgo', engines: ['duckduckgo', 'bing'], score: 2.5, publishedDate: '2026-01-01', category: 'general' }), `result: ${JSON.stringify(get.json.results[0])}`);
        assert(get.json.number_of_results === 1234 && JSON.stringify(get.json.answers) === '[{"answer":"a1"}]' && JSON.stringify(get.json.suggestions) === '["s1"]' && JSON.stringify(get.json.corrections) === '["c1"]' && Array.isArray(get.json.infoboxes), `top-level fields: ${JSON.stringify(get.json)}`);
        assert(JSON.stringify(get.json.unresponsive_engines) === '[["yahoo","timeout"]]', 'unresponsive passthrough');
        assert(get.json.meta && get.json.meta.cache === 'miss', 'meta present');
        assert(upstreamCalls.length === 1 && upstreamCalls[0].pageno === 2 && upstreamCalls[0].language === 'en' && upstreamCalls[0].categories === 'general', `upstream params ${JSON.stringify(upstreamCalls[0])}`);

        // same query again: served by the shared cache (no second upstream call)
        const again = await request(port, 'GET', '/search?q=hello&format=json&pageno=2&language=en&categories=general');
        assert(again.status === 200 && again.json.meta.cache === 'hit' && upstreamCalls.length === 1, 'cache shared with the scheduler path');

        const post = await request(port, 'POST', '/search', 'q=posted&format=json');
        assert(post.status === 200 && post.json.query === 'posted' && (upstreamCalls.length as number) === 2, 'POST form works');

        // time_range / safesearch / engines / categories are forwarded; the cache keeps them apart
        const fwd = await request(port, 'GET', '/search?q=fwd&format=json&time_range=week&safesearch=2&engines=brave,qwant&categories=images');
        const fp = upstreamCalls[upstreamCalls.length - 1];
        assert(fwd.status === 200 && fp.time_range === 'week' && fp.safesearch === '2' && fp.engines === 'brave,qwant' && fp.categories === 'images', `forwarded params: ${JSON.stringify(fp)}`);
        // a cache hit also returns the answers/infoboxes of the original response
        const fwdAgain = await request(port, 'GET', '/search?q=fwd&format=json&time_range=week&safesearch=2&engines=brave,qwant&categories=images');
        assert(fwdAgain.json.meta.cache === 'hit' && fwdAgain.json.answers.length === 1, 'hit keeps extras');
        const before = upstreamCalls.length;
        await request(port, 'GET', '/search?q=fwd&format=json&time_range=year&safesearch=2&engines=brave,qwant&categories=images');
        assert(upstreamCalls.length === before + 1, 'time_range is part of the cache key');

        // every engine unresponsive and nothing found -> 503 with Retry-After (not 502)
        const bad = await request(port, 'GET', '/search?q=allbad&format=json');
        assert(bad.status === 503 && Number(bad.headers['retry-after']) >= 1 && /rate-limited\/unresponsive/.test(bad.json.error), `503: ${bad.status} ${JSON.stringify(bad.json)}`);

        // body > 64KB -> 413
        const huge = await request(port, 'POST', '/search', `q=${'a'.repeat(70 * 1024)}&format=json`);
        assert(huge.status === 413, `413, got ${huge.status}`);

        assert((await request(port, 'GET', '/search?q=x&format=html')).status === 400, 'non-json -> 400');
        assert((await request(port, 'GET', '/search?q=x')).status === 400, 'missing format -> 400');
        assert((await request(port, 'GET', '/search?format=json')).status === 400, 'missing q -> 400');
        assert((await request(port, 'GET', '/nope')).status === 404, '404');
        assert((upstreamCalls.length as number) === before + 2, 'rejected requests never reach upstream');

        // saturated queue -> 429 + Retry-After
        config.searxngGlobalMaxPerMin = 1;
        config.searxngQueueTimeoutMs = 100;
        __resetSearxngStateForTests();
        await request(port, 'GET', '/search?q=first&format=json'); // takes the only slot of the minute
        const limited = await request(port, 'GET', '/search?q=second&format=json');
        assert(limited.status === 429, `429, got ${limited.status}`);
        assert(/queue wait exceeded.*retry in ~\d+s, do not retry immediately/.test(limited.json.error) && Number(limited.headers['retry-after']) >= 1 && limited.json.error.includes(`~${limited.headers['retry-after']}s`), `429 body/headers: ${JSON.stringify(limited.json)} ${limited.headers['retry-after']}`);
        console.log('✅ compat: GET/POST shape, raw passthrough, forwarding, shared cache, 400/404/413/503, 429 with Retry-After');

        // cooling engines show up in unresponsive_engines as "cooling down"
        __resetSearxngStateForTests();
        Object.assign(config, { searxngRotateEngines: ['duckduckgo'], searxngEgresses: ['a', 'b'], searxngExtraEngines: ['wikipedia'], searxngGlobalMaxPerMin: 1000, searxngQueueTimeoutMs: 5000 });
        __getSearxngSchedulerForTests().reportFailure('duckduckgo', 'a');
        __getSearxngSchedulerForTests().reportFailure('duckduckgo', 'b');
        const cooling = await request(port, 'GET', '/search?q=cool&format=json');
        assert(cooling.status === 200 && JSON.stringify(cooling.json.unresponsive_engines).includes('["duckduckgo","cooling down"]'), `cooling: ${JSON.stringify(cooling.json.unresponsive_engines)}`);
        console.log('✅ compat: cooling engines reported in unresponsive_engines');

        // a client that disconnects while queued frees its queue place
        __resetSearxngStateForTests();
        Object.assign(config, { searxngRotateEngines: [], searxngEgresses: [], searxngExtraEngines: [], searxngGlobalMaxPerMin: 1, searxngQueueTimeoutMs: 20_000 });
        await request(port, 'GET', '/search?q=takes-the-slot&format=json');
        await new Promise<void>((resolve, reject) => {
            const req = http.request({ host: '127.0.0.1', port, path: '/search?q=queued&format=json' });
            req.on('error', () => undefined);
            req.end();
            setTimeout(async () => {
                try {
                    assert(__getSearxngSchedulerForTests().queueLength === 1, 'request is queued');
                    req.destroy(); // client goes away
                    for (let i = 0; i < 50 && __getSearxngSchedulerForTests().queueLength > 0; i++) await new Promise((r) => setTimeout(r, 10));
                    assert(__getSearxngSchedulerForTests().queueLength === 0, 'queue ticket released after disconnect');
                    resolve();
                } catch (e) { reject(e); }
            }, 100);
        });
        console.log('✅ compat: client disconnect aborts the queued request');
    } finally {
        await new Promise<void>((resolve) => server.close(() => resolve()));
        __setSearxngHttpGetForTests();
    }
    console.log('\nSearXNG compat endpoint tests passed.');
}

main().catch((error) => { console.error(error); process.exit(1); });
