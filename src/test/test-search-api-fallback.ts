import { mkdtempSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { AppConfig, config } from '../config.js';
import { createOpenWebSearchRuntime } from '../runtime/createRuntime.js';
import { setupTools } from '../tools/setupTools.js';
import { startLocalDaemon } from '../adapters/http/localDaemon.js';
import {
    __resetSearchApiStateForTests, __setSearchApiClockForTests, __setSearchApiHttpForTests,
    getApiUsage, redactSecrets, type HttpRequest
} from '../engines/searchApis/searchApis.js';

const BRAVE_KEY = 'brave-secret-key-123';
const EXA_KEY = 'exa-secret-key-456';
const stateRoot = mkdtempSync(path.join(tmpdir(), 'owsearch-api-'));
process.env.XDG_STATE_HOME = stateRoot;
const usageFile = () => path.join(stateRoot, 'open-websearch', 'api-usage.json');

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

type Req = Parameters<HttpRequest>[0];
const calls: Req[] = [];
let handlers: { brave: (req: Req) => any; exa: (req: Req) => any };

const braveOk = (n: number[] = [1, 2]) => ({ status: 200, data: { web: { results: n.map((i) => ({ title: `B${i}`, url: `https://brave.example/${i}`, description: `bd${i}` })) } } });
const exaOk = (n: number[] = [1]) => ({ status: 200, data: { results: n.map((i) => ({ title: `E${i}`, url: `https://exa.example/${i}`, highlights: [`eh${i}`], publishedDate: '2026-01-01' })) } });

function installHttp(): void {
    calls.length = 0;
    __setSearchApiHttpForTests(async (req) => {
        calls.push(req);
        return req.url.includes('brave') ? handlers.brave(req) : handlers.exa(req);
    });
}

let now = Date.UTC(2026, 10, 3, 12, 0, 0);
const sleeps: number[] = [];
function installClock(): void {
    __setSearchApiClockForTests({ now: () => now, sleep: async (ms) => { sleeps.push(ms); now += ms; } });
}

function setKeys(brave: boolean, exa: boolean): void {
    if (brave) process.env.BRAVE_SEARCH_API_KEY = BRAVE_KEY; else delete process.env.BRAVE_SEARCH_API_KEY;
    if (exa) process.env.EXA_API_KEY = EXA_KEY; else delete process.env.EXA_API_KEY;
    delete process.env.BRAVE_MONTHLY_LIMIT;
    delete process.env.EXA_MONTHLY_LIMIT;
}

function fresh(brave = true, exa = true): void {
    rmSync(path.join(stateRoot, 'open-websearch'), { recursive: true, force: true });
    __resetSearchApiStateForTests();
    setKeys(brave, exa);
    handlers = { brave: () => braveOk(), exa: () => exaOk() };
    now = Date.UTC(2026, 10, 3, 12, 0, 0);
    sleeps.length = 0;
    config.searxngCacheTtlMs = 86400000;
    installHttp();
    installClock();
}

const wiki = { title: 'Wiki', url: 'https://wiki.example/1', description: 'w', source: 'wikipedia', engine: 'searxng' };
const dup = { title: 'Dup', url: 'https://brave.example/1', description: 'dupe', source: 'wikipedia', engine: 'searxng' };
type Mode = 'healthy' | 'degraded' | 'failure';
let mode: Mode = 'degraded';
let primaryCalls = 0;

function makeTools() {
    const runtime = createOpenWebSearchRuntime({
        config: { defaultSearchEngine: 'searxng', allowedSearchEngines: [], searchMode: 'request', proxyUrl: '', useProxy: false, fakeIpCidrs: [], fetchWebAllowInsecureTls: false, playwrightPackage: 'auto', playwrightHeadless: true, playwrightNavigationTimeoutMs: 20000, enableCors: false, corsOrigin: '*', enableHttpServer: true } as AppConfig,
        dependencies: {
            searchExecutors: {
                searxng: async (_q, _l, context) => {
                    primaryCalls++;
                    if (mode === 'failure') throw new Error('SearXNG: no results - upstream engines rate-limited/unresponsive');
                    if (mode === 'degraded') context?.report?.({ degraded: { engines: ['duckduckgo'], reasons: ['duckduckgo: returned no results'] } });
                    return [wiki, dup];
                }
            }
        }
    });
    const server = new McpServer({ name: 'test', version: '1.0.0' });
    setupTools(server, runtime);
    return { runtime, tools: (server as unknown as { _registeredTools: Record<string, { description: string; handler: (input: unknown) => Promise<{ content: Array<{ text: string }>; isError?: boolean }> }> })._registeredTools };
}

async function search(query: string, limit = 10) {
    const { tools } = makeTools();
    const res = await tools.search.handler({ query, limit, engines: ['searxng'] });
    return { text: res.content[0].text, isError: res.isError === true };
}
const jsonOf = (text: string) => JSON.parse(text.slice(text.indexOf('{')));

async function testDisabledWithoutKeys(): Promise<void> {
    fresh(false, false);
    mode = 'degraded';
    const r = await search('q-disabled');
    assert(r.text.startsWith('WARNING: primary web search engine unavailable'), `disabled: expected PR#3 WARNING, got ${r.text.slice(0, 80)}`);
    assert(!r.text.includes('fallback'), 'disabled: no mention of fallback');
    assert(calls.length === 0, 'disabled: no HTTP calls');
    assert(jsonOf(r.text).degraded === true && jsonOf(r.text).fallback === undefined, 'disabled: degraded json unchanged');
    mode = 'failure';
    const f = await search('q-disabled-fail');
    assert(f.isError && f.text.startsWith('Search FAILED:') && !f.text.includes('fallback'), `disabled: failure unchanged: ${f.text}`);
    assert(calls.length === 0, 'disabled failure: no HTTP calls');
    assert(!('searchExa' in makeTools().tools), 'searchExa must not be registered without EXA_API_KEY');
    assert(!getApiUsage().providers.brave.enabled && !getApiUsage().providers.exa.enabled, 'usage shows disabled');
    console.log('✅ disabled without keys');
}

async function testHealthyNeverCallsApis(): Promise<void> {
    fresh();
    mode = 'healthy';
    const r = await search('q-healthy');
    assert(calls.length === 0, 'healthy search must not call paid APIs');
    assert(!r.text.startsWith('NOTE') && !r.text.startsWith('WARNING'), 'healthy: no prefix');
    assert(jsonOf(r.text).fallback === undefined, 'healthy: no fallback field');
    console.log('✅ healthy search never calls paid APIs');
}

async function testBraveFallbackOnDegraded(): Promise<void> {
    fresh();
    mode = 'degraded';
    const r = await search('q-brave');
    assert(calls.length === 1 && calls[0].url === 'https://api.search.brave.com/res/v1/web/search', `brave url: ${calls[0]?.url}`);
    assert(calls[0].headers['X-Subscription-Token'] === BRAVE_KEY, 'brave auth header');
    assert(calls[0].params?.q === 'q-brave' && calls[0].params?.count === 10, 'brave params');
    assert(r.text.split('\n')[0] === 'NOTE: primary engine unavailable; results from brave fallback.', `NOTE line: ${r.text.split('\n')[0]}`);
    assert(!r.text.includes('WARNING'), 'no WARNING when fallback delivered');
    const j = jsonOf(r.text);
    assert(j.fallback === 'brave', 'fallback field');
    assert(j.degradedReasons[0].includes('duckduckgo'), 'degradedReasons kept');
    const urls = j.results.map((x: any) => x.url);
    assert(JSON.stringify(urls) === JSON.stringify(['https://brave.example/1', 'https://brave.example/2', 'https://wiki.example/1']), `brave first, deduped: ${urls}`);
    assert(!r.isError, 'not an error');
    console.log('✅ brave fallback on degraded (NOTE text, order, dedupe)');
}

async function testBraveThenExa(): Promise<void> {
    fresh();
    mode = 'degraded';
    handlers.brave = () => ({ status: 429, data: 'rate limited' });
    const r = await search('q-exa');
    assert(calls.length === 2 && calls[0].url.includes('brave') && calls[1].url === 'https://api.exa.ai/search', 'brave then exa order');
    assert(calls[1].headers['x-api-key'] === EXA_KEY && calls[1].method === 'POST', 'exa auth/method');
    const body = calls[1].data as any;
    assert(body.query === 'q-exa' && body.numResults <= 10 && body.type === 'lite', `exa body ${JSON.stringify(body)}`);
    assert(r.text.split('\n')[0] === 'NOTE: primary engine unavailable; results from exa fallback.', 'exa NOTE');
    assert(jsonOf(r.text).fallback === 'exa' && jsonOf(r.text).results[0].url === 'https://exa.example/1', 'exa first');
    // Brave returning zero results counts as a failed fallback too
    fresh();
    handlers.brave = () => braveOk([]);
    const r2 = await search('q-exa-empty');
    assert(jsonOf(r2.text).fallback === 'exa', 'empty brave -> exa');
    console.log('✅ brave -> exa order, 429 handling');
}

async function testAllFail(): Promise<void> {
    fresh();
    mode = 'degraded';
    handlers.brave = () => ({ status: 429, data: '' });
    handlers.exa = () => ({ status: 500, data: '' });
    const r = await search('q-allfail');
    assert(r.text.startsWith('WARNING: primary web search engine unavailable'), 'WARNING kept');
    assert(/brave: .*429/.test(r.text) && /exa: .*500/.test(r.text), `failure reasons appended: ${r.text.slice(0, 400)}`);
    assert(jsonOf(r.text).degraded === true && jsonOf(r.text).fallback === undefined, 'still degraded, no fallback');
    mode = 'failure';
    const f = await search('q-allfail2');
    assert(f.isError && f.text.startsWith('Search FAILED:') && /brave: .*429/.test(f.text) && /exa: .*500/.test(f.text), `isError with reasons: ${f.text}`);
    console.log('✅ all fallbacks fail -> PR#3 behaviour + reasons');
}

async function testZeroResultFailureFallback(): Promise<void> {
    fresh();
    mode = 'failure';
    const r = await search('q-zero');
    assert(!r.isError && r.text.startsWith('NOTE: primary engine unavailable; results from brave fallback.'), `zero-result failure -> fallback: ${r.text.slice(0, 100)}`);
    assert(jsonOf(r.text).results.length === 2, 'brave results only');
    console.log('✅ zero-result failure falls back');
}

async function testBudgetAndPersistence(): Promise<void> {
    fresh();
    mode = 'degraded';
    process.env.BRAVE_MONTHLY_LIMIT = '2';
    await search('b1'); await search('b2');
    assert(calls.filter((c) => c.url.includes('brave')).length === 2, 'two brave calls');
    // simulate restart: in-memory state dropped, file survives
    __resetSearchApiStateForTests(); installClock();
    const stored = JSON.parse(readFileSync(usageFile(), 'utf8'));
    assert(stored.month === '2026-11' && stored.usage.brave === 2, `persisted: ${JSON.stringify(stored)}`);
    assert(readdirSync(path.dirname(usageFile())).every((f) => !f.includes('.tmp')), 'no temp file left');
    const r = await search('b3');
    assert(calls.filter((c) => c.url.includes('brave')).length === 2, 'third brave call blocked after restart');
    assert(jsonOf(r.text).fallback === 'exa', 'budget exhausted -> exa');
    handlers.exa = () => ({ status: 500, data: '' });
    const r2 = await search('b4');
    assert(r2.text.includes('brave: monthly budget exhausted (2/2)'), `budget reason: ${r2.text.slice(0, 300)}`);
    assert(getApiUsage().providers.brave.used === 2 && getApiUsage().providers.brave.limit === 2, 'usage exposed');
    console.log('✅ budget exhaustion + persistence across restart');
}

async function testExaBudgetDefaultsAndCountBeforeSend(): Promise<void> {
    fresh();
    assert(getApiUsage().providers.brave.limit === 900 && getApiUsage().providers.exa.limit === 1500, 'default limits 900/1500');
    mode = 'degraded';
    handlers.brave = () => { throw new Error('boom'); };
    await search('cb');
    assert(getApiUsage().providers.brave.used === 1, 'request counted even when it throws');
    console.log('✅ default limits, counted before send');
}

async function testMonthRollover(): Promise<void> {
    fresh();
    mode = 'degraded';
    process.env.BRAVE_MONTHLY_LIMIT = '1';
    await search('m1');
    assert(getApiUsage().providers.brave.used === 1, 'used 1 in Nov');
    now = Date.UTC(2026, 11, 1, 0, 0, 1);
    assert(getApiUsage().providers.brave.used === 0 && getApiUsage().month === '2026-12', 'rollover shows 0 in Dec');
    await search('m2');
    assert(calls.filter((c) => c.url.includes('brave')).length === 2, 'brave callable again after rollover');
    assert(JSON.parse(readFileSync(usageFile(), 'utf8')).month === '2026-12', 'file month updated');
    console.log('✅ month rollover (UTC)');
}

async function testCaching(): Promise<void> {
    fresh();
    mode = 'degraded';
    await search('same'); await search('same');
    assert(calls.length === 1, `repeat query served from fallback cache, calls=${calls.length}`);
    assert(getApiUsage().providers.brave.used === 1, 'budget spent once');
    config.searxngCacheTtlMs = 0;
    await search('same2'); await search('same2');
    assert((calls as Req[]).length === 3, 'TTL 0 disables fallback cache');
    config.searxngCacheTtlMs = 86400000;
    console.log('✅ fallback results cached');
}

async function testRateLimiter(): Promise<void> {
    fresh();
    mode = 'degraded';
    await search('r1'); await search('r2');
    assert(sleeps.length === 1 && sleeps[0] > 0 && sleeps[0] <= 1000, `brave spaced >=1s: ${sleeps}`);
    fresh(false, true);
    await search('r3'); await search('r4');
    assert(sleeps.some((s) => s > 0 && s <= 200), `exa spaced >=200ms: ${sleeps}`);
    console.log('✅ per-provider rate limiter');
}

async function testRedactionAndLogging(): Promise<void> {
    fresh();
    mode = 'degraded';
    const logs: string[] = [];
    const orig = console.error;
    console.error = (...a: unknown[]) => { logs.push(a.map(String).join(' ')); };
    try {
        handlers.brave = () => { throw new Error(`connect failed for X-Subscription-Token: ${BRAVE_KEY}`); };
        handlers.exa = () => { throw new Error(`bad key ${EXA_KEY} in url`); };
        const r = await search('red');
        assert(!r.text.includes(BRAVE_KEY) && !r.text.includes(EXA_KEY), 'keys leaked into tool output');
        assert(r.text.includes('[REDACTED]'), 'redaction marker present');
        assert(!logs.join('\n').includes(BRAVE_KEY) && !logs.join('\n').includes(EXA_KEY), 'keys leaked into logs');
        assert(!readFileSync(usageFile(), 'utf8').includes(BRAVE_KEY), 'keys not in state file');
        fresh();
        handlers.brave = () => braveOk();
        await search('log');
        assert(logs.some((l) => l.startsWith('[local-search] api ') && l.includes('"provider":"brave"') && l.includes('"used":1') && l.includes('"limit":900') && l.includes('"ms"')), `paid call log line: ${logs.slice(-3).join(' | ')}`);
    } finally {
        console.error = orig;
    }
    assert(redactSecrets(`a ${BRAVE_KEY} b`) === 'a [REDACTED] b', 'redactSecrets direct');
    console.log('✅ key redaction + log line');
}

async function testSearchExaTool(): Promise<void> {
    fresh(false, true);
    const { tools } = makeTools();
    assert(tools.searchExa, 'searchExa registered with EXA_API_KEY');
    assert(/primary sources/.test(tools.searchExa.description) && /costs budget/i.test(tools.searchExa.description), 'description');
    const res = await tools.searchExa.handler({ query: 'acme 10-K', numResults: 3, includeDomains: ['sec.gov'], startPublishedDate: '2026-01-01T00:00:00.000Z' });
    const body = calls[0].data as any;
    assert(body.numResults === 3 && body.includeDomains[0] === 'sec.gov' && body.startPublishedDate === '2026-01-01T00:00:00.000Z' && body.type === 'auto', `body ${JSON.stringify(body)}`);
    const j = JSON.parse(res.content[0].text);
    assert(j.results[0].url === 'https://exa.example/1' && j.results[0].description === 'eh1', 'results mapped');
    assert(getApiUsage().providers.exa.used === 1, 'explicit tool spends budget');
    process.env.EXA_MONTHLY_LIMIT = '1';
    const over = await tools.searchExa.handler({ query: 'again' });
    assert(over.isError && over.content[0].text.includes('monthly budget exhausted (1/1)'), `over budget: ${over.content[0].text}`);
    assert(calls.length === 1, 'no HTTP when over budget');
    fresh(true, false);
    assert(!makeTools().tools.searchExa, 'not registered when only Brave key set');
    console.log('✅ searchExa tool');
}

async function testStatusExposesUsage(): Promise<void> {
    fresh();
    const { runtime } = makeTools();
    const daemon = await startLocalDaemon(runtime, { port: 0, version: 't' });
    try {
        const payload = await (await fetch(`${daemon.baseUrl}/status`)).json() as any;
        assert(payload.data.searchApis.providers.brave.limit === 900 && payload.data.searchApis.providers.exa.enabled === true, `status: ${JSON.stringify(payload.data.searchApis)}`);
        assert(!JSON.stringify(payload).includes(BRAVE_KEY), 'status has no key');
    } finally {
        await daemon.close();
    }
    console.log('✅ status exposes usage');
}

async function main(): Promise<void> {
    await testDisabledWithoutKeys();
    await testHealthyNeverCallsApis();
    await testBraveFallbackOnDegraded();
    await testBraveThenExa();
    await testAllFail();
    await testZeroResultFailureFallback();
    await testBudgetAndPersistence();
    await testExaBudgetDefaultsAndCountBeforeSend();
    await testMonthRollover();
    await testCaching();
    await testRateLimiter();
    await testRedactionAndLogging();
    await testSearchExaTool();
    await testStatusExposesUsage();
    __setSearchApiHttpForTests();
    __setSearchApiClockForTests();
    rmSync(stateRoot, { recursive: true, force: true });
    console.log('\nSearch API fallback tests passed.');
}

main().catch((error) => { console.error(error); process.exit(1); });
