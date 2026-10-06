import { config } from '../config.js';
import { searchSearxng, __setSearxngHttpGetForTests, __setSearxngClockForTests, __resetSearxngStateForTests } from '../engines/searxng/searxng.js';

type Call = { url: string; params: Record<string, unknown>; proxy: unknown };
const calls: Call[] = [];

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

function mockPages(pages: Array<{ status?: number; data: unknown }>): void {
    calls.length = 0;
    __setSearxngHttpGetForTests(async (url, options) => {
        calls.push({ url, params: options.params as Record<string, unknown>, proxy: options.proxy });
        const page = pages[calls.length - 1] ?? { data: { results: [] } };
        return { status: page.status ?? 200, data: page.data } as any;
    });
}

const item = (n: number, extra: object = {}) => ({
    title: `T${n}`, url: `https://example.com/${n}`, content: `C${n}`, engine: 'duckduckgo', engines: ['duckduckgo', 'brave'], ...extra
});

async function expectReject(fn: () => Promise<unknown>, pattern: RegExp, label: string): Promise<void> {
    try {
        await fn();
    } catch (error) {
        assert(pattern.test((error as Error).message), `${label}: unexpected message "${(error as Error).message}"`);
        return;
    }
    throw new Error(`${label}: expected rejection`);
}

function resetRotationConfig(): void {
    __resetSearxngStateForTests();
    config.searxngUrl = 'http://127.0.0.1:8888';
    config.searxngEngines = [];
    config.searxngCategories = [];
    config.searxngLanguage = undefined;
    config.searxngMaxPages = 1;
    config.searxngMinIntervalMs = 0;
    config.searxngCacheTtlMs = 0;
    config.searxngMaxConcurrency = 2;
    config.searxngTimeoutMs = 5000;
    config.searxngRotateEngines = ['duckduckgo', 'bing'];
    config.searxngEgresses = ['deck', 'vlabs', 'madrid'];
    config.searxngExtraEngines = ['wikipedia'];
    config.searxngPairMinIntervalMs = 0;
    config.searxngGlobalMaxPerMin = 1000;
    config.searxngQueueTimeoutMs = 5000;
}

// Results as SearXNG returns them for a rotated request: tagged with the per-egress engine copies requested.
function rotatedItems(params: Record<string, unknown>, n: number): object[] {
    const names = String(params.engines ?? '').split(',').filter((e) => e.includes(' '));
    return names.map((name, i) => item(n * 10 + i, { engine: name, engines: [name] }));
}

function mockRotated(): void {
    calls.length = 0;
    __setSearxngHttpGetForTests(async (url, options) => {
        const params = options.params as Record<string, unknown>;
        calls.push({ url, params, proxy: options.proxy });
        return { status: 200, data: { results: rotatedItems(params, calls.length) } } as any;
    });
}

async function testRotation(): Promise<void> {
    resetRotationConfig();
    mockRotated();
    for (let i = 0; i < 4; i++) await searchSearxng(`q${i}`, 1);
    const engines = calls.map(c => c.params.engines);
    assert(engines[0] === 'duckduckgo deck,bing deck,wikipedia', `first: ${engines[0]}`);
    assert(engines[1] === 'duckduckgo vlabs,bing vlabs,wikipedia', `second: ${engines[1]}`);
    assert(engines[2] === 'duckduckgo madrid,bing madrid,wikipedia', `third: ${engines[2]}`);
    assert(engines[3] === 'duckduckgo deck,bing deck,wikipedia', `wraps (least recently used): ${engines[3]}`);
    const r = await searchSearxng('label', 1);
    assert(r[0].engine === 'searxng:duckduckgo,bing,wikipedia', `label: ${r[0].engine}`);
    console.log('✅ rotation round-robin');

    // retry with next egress for the failed copy only; counters independent
    resetRotationConfig();
    mockPages([
        { data: { results: [], unresponsive_engines: [['duckduckgo deck', 'Suspended: too many requests']] } },
        { data: { results: [item(1, { engine: 'duckduckgo vlabs', engines: ['duckduckgo vlabs'] })] } },
        { data: { results: [item(2, { engine: 'duckduckgo madrid', engines: ['duckduckgo madrid'] }), item(3, { engine: 'bing vlabs', engines: ['bing vlabs'] })] } }
    ]);
    const res = await searchSearxng('retry', 1);
    assert(res.length === 1 && (calls.length as number) === 2, `retried once, calls=${calls.length}`);
    // the retry asks only the failed engine, on a different healthy pair
    assert(calls[1].params.engines === 'duckduckgo vlabs', `retry engines: ${calls[1].params.engines}`);
    await searchSearxng('next', 1);
    // deck is cooling for duckduckgo (sick pair skipped); bing deck was used last -> bing prefers vlabs
    assert(calls[2].params.engines === 'duckduckgo madrid,bing vlabs,wikipedia', `sick pair skipped, LRU for the rest: ${calls[2].params.engines}`);
    console.log('✅ rotation retry with next egress');

    // retry exhausted -> explicit error
    resetRotationConfig();
    mockPages([
        { data: { results: [], unresponsive_engines: [['duckduckgo deck', 'x']] } },
        { data: { results: [], unresponsive_engines: [['duckduckgo vlabs', 'x']] } }
    ]);
    await expectReject(() => searchSearxng('q', 1), /back off and retry later/, 'retry exhausted');
    assert(calls.length === 2, 'only one retry');
    console.log('✅ rotation retry is bounded');
}

async function testCache(): Promise<void> {
    resetRotationConfig();
    config.searxngCacheTtlMs = 1000;
    let t = 1_000_000;
    __setSearxngClockForTests({ now: () => t, sleep: async () => {} });
    mockPages(Array.from({ length: 10 }, (_, i) => ({ data: { results: [item(i)] } })));
    config.searxngRotateEngines = [];
    await searchSearxng('cached', 1);
    await searchSearxng('cached', 1);
    assert(calls.length === 1, `second call served from cache, calls=${calls.length}`);
    await searchSearxng('other', 1);
    assert((calls.length as number) === 2, 'different query misses cache');
    t += 1001;
    await searchSearxng('cached', 1);
    assert((calls.length as number) === 3, 'expired entry refetched');
    // empty results are not cached
    mockPages([{ data: { results: [] } }, { data: { results: [] } }]);
    await searchSearxng('empty', 1);
    await searchSearxng('empty', 1);
    assert((calls.length as number) === 2, 'empty results not cached');
    __setSearxngClockForTests();
    console.log('✅ cache hit, expiry, empty not cached');
}

async function testConcurrencyAndInterval(): Promise<void> {
    resetRotationConfig();
    config.searxngRotateEngines = [];
    config.searxngMaxConcurrency = 2;
    let inFlight = 0, maxInFlight = 0;
    const releases: Array<() => void> = [];
    __setSearxngHttpGetForTests(async () => {
        inFlight++; maxInFlight = Math.max(maxInFlight, inFlight);
        await new Promise<void>((r) => releases.push(r));
        inFlight--;
        return { status: 200, data: { results: [item(Math.random())] } } as any;
    });
    const all = [1, 2, 3, 4].map((n) => searchSearxng(`c${n}`, 1));
    await new Promise((r) => setTimeout(r, 20));
    assert(inFlight === 2, `only 2 in flight, got ${inFlight}`);
    while (releases.length) { releases.shift()!(); await new Promise((r) => setTimeout(r, 10)); }
    await Promise.all(all);
    assert(maxInFlight === 2, `max in flight ${maxInFlight}`);
    console.log('✅ concurrency limit');

    // min interval with fake clock
    resetRotationConfig();
    config.searxngRotateEngines = [];
    config.searxngMinIntervalMs = 1000;
    let now = 0;
    const sleeps: number[] = [];
    __setSearxngClockForTests({ now: () => now, sleep: async (ms) => { sleeps.push(ms); now += ms; } });
    mockPages(Array.from({ length: 5 }, (_, i) => ({ data: { results: [item(i)] } })));
    await searchSearxng('i1', 1);
    await searchSearxng('i2', 1);
    assert(sleeps.length === 1 && sleeps[0] === 1000, `min interval enforced: ${JSON.stringify(sleeps)}`);
    __setSearxngClockForTests();
    console.log('✅ min interval');
}

async function testMaxPages(): Promise<void> {
    resetRotationConfig();
    config.searxngRotateEngines = [];
    mockPages(Array.from({ length: 5 }, (_, i) => ({ data: { results: [item(i)] } })));
    await searchSearxng('p', 10);
    assert(calls.length === 1, `default 1 page, got ${calls.length}`);
    config.searxngMaxPages = 99;
    mockPages(Array.from({ length: 9 }, (_, i) => ({ data: { results: [item(i + 100)] } })));
    await searchSearxng('p2', 50);
    assert((calls.length as number) === 5, `capped at 5 pages, got ${calls.length}`);
    console.log('✅ max pages');
}

async function main(): Promise<void> {
    config.searxngUrl = 'http://127.0.0.1:8888/';
    config.searxngEngines = ['duckduckgo', 'brave'];
    config.searxngCategories = ['general'];
    config.searxngLanguage = 'en';
    config.searxngTimeoutMs = 5000;
    config.searxngMaxPages = 5;
    config.searxngMinIntervalMs = 0;
    config.searxngCacheTtlMs = 0; // cache off except in the cache tests
    config.searxngMaxConcurrency = 2;
    config.searxngPairMinIntervalMs = 0;
    config.searxngGlobalMaxPerMin = 1000;
    config.searxngQueueTimeoutMs = 5000;

    // request shape + normalization
    mockPages([{ data: { results: [item(1), item(2)] } }]);
    let results = await searchSearxng('hello world', 2);
    assert(results.length === 2, 'returns 2 results');
    assert(calls[0].url === 'http://127.0.0.1:8888/search', `url: ${calls[0].url}`);
    assert(calls[0].params.q === 'hello world' && calls[0].params.format === 'json' && calls[0].params.pageno === 1, 'q/format/pageno params');
    assert(calls[0].params.engines === 'duckduckgo,brave', 'engines param');
    assert(calls[0].params.categories === 'general' && calls[0].params.language === 'en', 'categories/language params');
    assert(calls[0].proxy === false, 'proxy disabled');
    assert(results[0].title === 'T1' && results[0].url === 'https://example.com/1' && results[0].description === 'C1', 'field mapping');
    assert(results[0].engine === 'searxng:duckduckgo,brave', `engine label: ${results[0].engine}`);
    assert(results[0].source === 'duckduckgo,brave', 'source from upstream engines');
    console.log('✅ request shape and normalization');

    // pagination + dedupe
    mockPages([
        { data: { results: [item(1), item(2), item(2)] } },
        { data: { results: [item(2), item(3), item(4)] } }
    ]);
    results = await searchSearxng('q', 3);
    assert(calls.length === 2, `fetches 2 pages, got ${calls.length}`);
    assert(calls[1].params.pageno === 2, 'second page requested');
    assert(results.map(r => r.url).join() === [1, 2, 3].map(n => `https://example.com/${n}`).join(), 'deduped and limited');
    console.log('✅ pagination and dedupe');

    // stops on empty page, caps pages
    mockPages([{ data: { results: [item(1)] } }, { data: { results: [] } }]);
    results = await searchSearxng('q', 10);
    assert(results.length === 1 && (calls.length as number) === 2, 'stops when no more results');
    mockPages(Array.from({ length: 10 }, (_, i) => ({ data: { results: [item(i)] } })));
    await searchSearxng('q', 100);
    assert((calls.length as number) === 5, `max 5 pages, got ${calls.length}`);
    console.log('✅ pagination bounds');

    // stops when a page adds no new unique results
    mockPages(Array.from({ length: 5 }, () => ({ data: { results: [item(1)] } })));
    results = await searchSearxng('q', 10);
    assert(results.length === 1 && (calls.length as number) === 2, `stops on all-duplicate page, calls=${calls.length}`);
    console.log('✅ stops on duplicate-only page');

    // overall deadline across pages
    config.searxngTimeoutMs = 60;
    let seenSignal = false;
    __setSearxngHttpGetForTests(async (_u, options) => {
        seenSignal = options.signal instanceof AbortSignal && typeof options.timeout === 'number' && options.timeout <= 60;
        await new Promise((r) => setTimeout(r, 40));
        return { status: 200, data: { results: [item(Math.random())] } } as any;
    });
    await expectReject(() => searchSearxng('q', 50), /overall timeout|request failed/, 'overall deadline');
    assert(seenSignal, 'abort signal and remaining timeout passed');
    config.searxngTimeoutMs = 5000;
    console.log('✅ overall deadline');

    // optional params omitted
    config.searxngEngines = []; config.searxngCategories = []; config.searxngLanguage = undefined;
    mockPages([{ data: { results: [item(1)] } }]);
    results = await searchSearxng('q', 1);
    assert(!('engines' in calls[0].params) && !('categories' in calls[0].params) && !('language' in calls[0].params), 'optional params omitted');
    assert(results[0].engine === 'searxng', 'plain engine label without engines list');
    console.log('✅ optional params omitted');

    // errors
    mockPages([{ status: 403, data: 'Forbidden' }]);
    await expectReject(() => searchSearxng('q', 1), /HTTP 403/, 'non-200');
    mockPages([{ data: '<html>not json</html>' }]);
    await expectReject(() => searchSearxng('q', 1), /invalid JSON/, 'bad json');
    mockPages([{ data: { results: [], unresponsive_engines: [['google', 'timeout']] } }]);
    await expectReject(() => searchSearxng('q', 1), /rate-limited\/unresponsive.*google.*timeout.*does NOT mean no information/, 'unresponsive engines');
    __setSearxngHttpGetForTests(async () => { throw new Error('ECONNREFUSED'); });
    await expectReject(() => searchSearxng('q', 1), /request failed: ECONNREFUSED/, 'network error');
    console.log('✅ error handling');

    config.searxngUrl = undefined;
    await expectReject(() => searchSearxng('q', 1), /SEARXNG_URL is not set/, 'unset url');
    console.log('✅ unavailable without SEARXNG_URL');

    await testRotation();
    await testCache();
    await testConcurrencyAndInterval();
    await testMaxPages();

    __setSearxngHttpGetForTests();
    __setSearxngClockForTests();
    console.log('\nSearXNG tests passed.');
}

main().catch((error) => { console.error(error); process.exit(1); });
