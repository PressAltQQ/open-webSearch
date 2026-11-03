import { config } from '../config.js';
import { searchSearxng, searchSearxngDetailed, __getSearxngSchedulerForTests, __setSearxngHttpGetForTests, __setSearxngClockForTests, __resetSearxngStateForTests } from '../engines/searxng/searxng.js';

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


const pairItem = (n: number, pair: string) => item(n, { engine: pair, engines: [pair] });
const wikiItem = (n: number) => item(n, { engine: 'wikipedia', engines: ['wikipedia'] });

async function testLoudDegradation(): Promise<void> {
    const baseCfg = () => { resetRotationConfig(); config.searxngRotateEngines = ['duckduckgo']; };

    // mode 1: DDG captcha, extras deliver -> one retry on another egress; recovered -> not degraded
    baseCfg();
    mockPages([
        { data: { results: [wikiItem(1)], unresponsive_engines: [['duckduckgo deck', 'CAPTCHA']] } },
        { data: { results: [pairItem(2, 'duckduckgo vlabs')] } }
    ]);
    let d = await searchSearxngDetailed('m1', 10);
    assert(calls.length === 2 && calls[1].params.engines === 'duckduckgo vlabs', `retried on another egress even with extras: ${calls.map(c => c.params.engines)}`);
    assert(d.results.length === 2 && !d.degraded, `merged and healthy: ${d.results.length} ${JSON.stringify(d.degraded)}`);

    // mode 1: retry fails too -> degraded with reason, results kept
    baseCfg();
    mockPages([
        { data: { results: [wikiItem(1)], unresponsive_engines: [['duckduckgo deck', 'CAPTCHA']] } },
        { data: { results: [], unresponsive_engines: [['duckduckgo vlabs', 'CAPTCHA']] } }
    ]);
    d = await searchSearxngDetailed('m1b', 10);
    assert((calls.length as number) === 2 && d.results.length === 1, `one retry, extras kept: ${calls.length}`);
    assert(d.degraded?.engines.join() === 'duckduckgo' && /duckduckgo.*CAPTCHA/.test(d.degraded.reasons.join()), `degraded: ${JSON.stringify(d.degraded)}`);

    // degraded results are cached only briefly; healthy ones keep the full TTL
    baseCfg();
    config.searxngCacheTtlMs = 3_600_000;
    let t = 1_000_000;
    __setSearxngClockForTests({ now: () => t, sleep: async () => {} });
    calls.length = 0;
    __setSearxngHttpGetForTests(async (url, options) => {
        const params = options.params as Record<string, unknown>;
        calls.push({ url, params, proxy: options.proxy });
        const pair = String(params.engines).split(',').find((e) => e.startsWith('duckduckgo '));
        return { status: 200, data: { results: [wikiItem(1)], unresponsive_engines: [[pair, 'CAPTCHA']] } } as any;
    });
    await searchSearxngDetailed('cache-deg', 10);
    const n = calls.length;
    const hit = await searchSearxngDetailed('cache-deg', 10);
    assert(calls.length === n && hit.degraded, 'degraded result served from cache within a minute, still flagged');
    t += 61_000;
    await searchSearxngDetailed('cache-deg', 10);
    assert(calls.length > n, 'degraded cache entry expires after ~60s');
    baseCfg();
    config.searxngCacheTtlMs = 3_600_000;
    mockPages(Array.from({ length: 4 }, (_, i) => ({ data: { results: [pairItem(i, 'duckduckgo deck')] } })));
    await searchSearxngDetailed('cache-ok', 10);
    t += 1_800_000;
    await searchSearxngDetailed('cache-ok', 10);
    assert((calls.length as number) === 1, 'healthy result keeps the full TTL');
    __setSearxngClockForTests();

    // mode 2: every DDG pair cooling, extras still deliver -> degraded, cooling reason
    baseCfg();
    const sched = __getSearxngSchedulerForTests();
    for (const e of ['deck', 'vlabs', 'madrid']) sched.reportFailure('duckduckgo', e);
    mockPages([{ data: { results: [wikiItem(1)] } }]);
    d = await searchSearxngDetailed('m2', 10);
    assert(d.results.length === 1 && d.degraded?.engines.join() === 'duckduckgo' && /cooling/.test(d.degraded.reasons.join()), `cooling degraded: ${JSON.stringify(d.degraded)}`);

    // mode 3: >=5 results but DDG contributed none -> degraded
    baseCfg();
    // DDG silently contributes nothing (not reported unresponsive); the DDG-only retry realistically returns nothing too
    mockPages([{ data: { results: [1, 2, 3, 4, 5].map(wikiItem) } }, { data: { results: [] } }]);
    d = await searchSearxngDetailed('m3', 10);
    assert(d.results.length === 5 && d.degraded?.engines.join() === 'duckduckgo' && /no results/.test(d.degraded.reasons.join()), `zero contribution degraded: ${JSON.stringify(d.degraded)}`);

    // healthy + genuine zero results stay unflagged
    baseCfg();
    mockPages([{ data: { results: [pairItem(1, 'duckduckgo deck'), wikiItem(2)] } }]);
    d = await searchSearxngDetailed('ok', 10);
    assert(!d.degraded && d.partialFailures.length === 0, 'healthy is not degraded');
    baseCfg();
    mockPages([{ data: { results: [] } }]);
    d = await searchSearxngDetailed('none', 10);
    assert(!d.degraded && d.partialFailures.length === 0 && d.results.length === 0, 'genuine empty stays a plain empty result');

    // compat-style single page: no extra retry when results exist
    baseCfg();
    mockPages([{ data: { results: [wikiItem(1)], unresponsive_engines: [['duckduckgo deck', 'CAPTCHA']] } }]);
    await searchSearxngDetailed('compat', 10, { pageno: 1 });
    assert((calls.length as number) === 1, 'single-page (compat) path keeps its retry-only-on-empty behaviour');
    console.log('✅ loud degradation: retry, flag, short cache, no false alarms');
}

async function testLoudRetryHardening(): Promise<void> {
    const baseCfg = () => { resetRotationConfig(); config.searxngRotateEngines = ['duckduckgo']; };
    const captcha = (pair = 'duckduckgo deck') => ({ results: [wikiItem(1), wikiItem(2)], unresponsive_engines: [[pair, 'CAPTCHA']] });

    // T1: a failing completeness retry keeps the first call's results and reports why
    baseCfg();
    mockPages([{ data: captcha() }, { status: 502, data: '' }]);
    let d = await searchSearxngDetailed('t1a', 10);
    assert(d.results.length === 2 && d.degraded && /retry failed.*HTTP 502/.test(d.degraded.reasons.join()), `retry 502 keeps results: ${JSON.stringify(d.degraded)}`);
    baseCfg();
    calls.length = 0;
    __setSearxngHttpGetForTests(async () => {
        if (calls.push({ url: '', params: {}, proxy: false }) === 1) return { status: 200, data: captcha() } as any;
        throw new Error('timeout of 3000ms exceeded');
    });
    d = await searchSearxngDetailed('t1b', 10);
    assert(d.results.length === 2 && d.degraded && /retry failed.*timeout/.test(d.degraded.reasons.join()), `retry timeout keeps results: ${JSON.stringify(d.degraded)}`);

    // T2: with no free pair the completeness retry fails fast instead of waiting at the head of the queue
    baseCfg();
    config.searxngPairMinIntervalMs = 10000;
    config.searxngQueueTimeoutMs = 45000;
    let t = 1_000_000;
    const sleeps: number[] = [];
    __setSearxngClockForTests({ now: () => t, sleep: async (ms) => { sleeps.push(ms); t += ms; } });
    const pairOf = (o: any) => String(o.params.engines).split(',').find((e: string) => e.startsWith('duckduckgo '))!;
    __setSearxngHttpGetForTests(async (_u, o) => ({ status: 200, data: { results: [pairItem(1, pairOf(o)), wikiItem(1)] } } as any));
    await searchSearxngDetailed('o1', 10);
    await searchSearxngDetailed('o2', 10);
    t += 1000;
    sleeps.length = 0;
    let n = 0;
    __setSearxngHttpGetForTests(async (_u, o) => {
        n++;
        return { status: 200, data: n === 1 ? captcha(pairOf(o)) : { results: [pairItem(5, pairOf(o))] } } as any;
    });
    d = await searchSearxngDetailed('t2', 10);
    assert(n === 1 && sleeps.length === 0 && d.meta.queued_ms === 0, `retry did not wait: calls=${n} sleeps=${JSON.stringify(sleeps)} queued=${d.meta.queued_ms}`);
    assert(d.results.length === 2 && d.degraded, 'degraded results returned immediately');
    __setSearxngClockForTests();

    // T3: completeness retry only on the first page
    baseCfg();
    config.searxngMaxPages = 3;
    n = 0;
    __setSearxngHttpGetForTests(async (_u, o) => {
        n++;
        const params = o.params as Record<string, any>;
        const pair = String(params.engines).split(',').find((e) => e.startsWith('duckduckgo '))!;
        const ext = String(params.engines).includes('wikipedia');
        return { status: 200, data: { results: ext ? [1, 2, 3, 4, 5].map((i) => wikiItem(params.pageno * 10 + i)) : [], unresponsive_engines: [[pair, 'CAPTCHA']] } } as any;
    });
    d = await searchSearxngDetailed('t3', 30);
    assert((n as number) === 4, `1 retry (page 1 only) + 3 first calls, got ${n}`);

    // T4: retry results come first, so the limit cannot cut them and still report healthy
    baseCfg();
    mockPages([
        { data: { results: [1, 2, 3, 4, 5].map(wikiItem), unresponsive_engines: [['duckduckgo deck', 'CAPTCHA']] } },
        { data: { results: [1, 2, 3].map((i) => pairItem(i + 100, 'duckduckgo vlabs')) } }
    ]);
    d = await searchSearxngDetailed('t4', 5);
    assert(d.results.slice(0, 3).every((r) => r.source.startsWith('duckduckgo')) && !d.degraded, `rotated results first: ${d.results.map((r) => r.title)}`);
    console.log('✅ loud degradation: retry errors, fail-fast, first page only, ordering');
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
    await testLoudDegradation();
    await testLoudRetryHardening();

    __setSearxngHttpGetForTests();
    __setSearxngClockForTests();
    console.log('\nSearXNG tests passed.');
}

main().catch((error) => { console.error(error); process.exit(1); });
