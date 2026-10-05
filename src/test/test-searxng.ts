import { config } from '../config.js';
import { searchSearxng, __setSearxngHttpGetForTests } from '../engines/searxng/searxng.js';

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

async function main(): Promise<void> {
    config.searxngUrl = 'http://127.0.0.1:8888/';
    config.searxngEngines = ['duckduckgo', 'brave'];
    config.searxngCategories = ['general'];
    config.searxngLanguage = 'en';
    config.searxngTimeoutMs = 5000;

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
    await expectReject(() => searchSearxng('q', 1), /unresponsive engines.*google.*timeout/, 'unresponsive engines');
    __setSearxngHttpGetForTests(async () => { throw new Error('ECONNREFUSED'); });
    await expectReject(() => searchSearxng('q', 1), /request failed: ECONNREFUSED/, 'network error');
    console.log('✅ error handling');

    config.searxngUrl = undefined;
    await expectReject(() => searchSearxng('q', 1), /SEARXNG_URL is not set/, 'unset url');
    console.log('✅ unavailable without SEARXNG_URL');

    __setSearxngHttpGetForTests();
    console.log('\nSearXNG tests passed.');
}

main().catch((error) => { console.error(error); process.exit(1); });
