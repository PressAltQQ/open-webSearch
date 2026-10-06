import { config } from '../config.js';
import { SearxngScheduler, QueueTimeoutError, QueueFullError, QueueAbortedError, AllPairsCoolingError } from '../engines/searxng/scheduler.js';
import type { Clock, SchedulerOptions, Grant } from '../engines/searxng/scheduler.js';
import { createSearchService } from '../core/search/searchService.js';
import {
    searchSearxng,
    SchedulerBusyError,
    searchSearxngDetailed,
    __setSearxngHttpGetForTests,
    __setSearxngClockForTests,
    __resetSearxngStateForTests,
    __getSearxngSchedulerForTests
} from '../engines/searxng/searxng.js';

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

// Deterministic clock: sleep() parks until the test advances time.
class FakeClock implements Clock {
    t = 1_000_000;
    private timers: Array<{ at: number; resolve: () => void }> = [];
    now = () => this.t;
    sleep = (ms: number, signal?: AbortSignal): Promise<void> => new Promise<void>((resolve) => {
        const timer = { at: this.t + ms, resolve };
        this.timers.push(timer);
        signal?.addEventListener('abort', () => {
            this.timers = this.timers.filter((x) => x !== timer);
            resolve();
        }, { once: true });
    });
    private async settle(): Promise<void> {
        for (let i = 0; i < 10; i++) await new Promise((r) => setImmediate(r));
    }
    /** Drive time forward until every promise settled; never touches the real clock. */
    async run<T>(promises: Array<Promise<T>>): Promise<Array<PromiseSettledResult<T>>> {
        let done = false;
        const all = Promise.allSettled(promises).then((r) => { done = true; return r; });
        for (let guard = 0; guard < 10_000 && !done; guard++) {
            await this.settle();
            if (done) break;
            if (this.timers.length === 0) throw new Error('deadlock: pending work but no timers');
            const next = Math.min(...this.timers.map((x) => x.at));
            this.t = Math.max(this.t, next);
            const due = this.timers.filter((x) => x.at <= this.t);
            this.timers = this.timers.filter((x) => x.at > this.t);
            due.forEach((x) => x.resolve());
        }
        return all;
    }
}

const baseOptions: SchedulerOptions = {
    maxConcurrency: 10,
    minIntervalMs: 0,
    pairMinIntervalMs: 10_000,
    globalMaxPerMin: 1000,
    backoffBaseMs: 60_000,
    backoffMaxMs: 3_600_000,
    queueTimeoutMs: 45_000,
    maxQueueDepth: 30
};

function makeScheduler(clock: FakeClock, overrides: Partial<SchedulerOptions> = {}): SearxngScheduler {
    return new SearxngScheduler(() => ({ ...baseOptions, ...overrides }), () => clock);
}

const req = (overrides: object = {}) => ({ rotated: ['duckduckgo'], egresses: ['a', 'b'], hasExtras: false, timeoutMs: 45_000, ...overrides });

async function testPairIntervalAndFifo(): Promise<void> {
    const clock = new FakeClock();
    const s = makeScheduler(clock);
    const starts: Array<{ pair: string; at: number; n: number }> = [];
    const order: number[] = [];
    const jobs = Array.from({ length: 12 }, (_, n) => (async () => {
        const grant = await s.acquire(req({ timeoutMs: 600_000 }));
        order.push(n);
        for (const [engine, egress] of grant.picks) starts.push({ pair: `${engine} ${egress}`, at: clock.now(), n });
        grant.release();
    })());
    const results = await clock.run(jobs);
    assert(results.every((r) => r.status === 'fulfilled'), `all granted: ${JSON.stringify(results.map((r) => r.status))}`);
    assert(order.join() === Array.from({ length: 12 }, (_, i) => i).join(), `FIFO order: ${order.join()}`);
    for (const pair of ['duckduckgo a', 'duckduckgo b']) {
        const times = starts.filter((x) => x.pair === pair).map((x) => x.at);
        assert(times.length > 0, `pair ${pair} used`);
        for (let i = 1; i < times.length; i++) {
            assert(times[i] - times[i - 1] >= 10_000, `pair ${pair} interval violated: ${times[i] - times[i - 1]}ms`);
        }
    }
    // LRU: pairs alternate instead of a blind counter
    assert(starts[0].pair === 'duckduckgo a' && starts[1].pair === 'duckduckgo b' && starts[2].pair === 'duckduckgo a', 'least-recently-used alternation');
    console.log('✅ scheduler: per-pair interval under burst, FIFO, LRU pair choice');
}

async function testGlobalLimits(): Promise<void> {
    // global ceiling: sliding 60s window
    let clock = new FakeClock();
    let s = makeScheduler(clock, { globalMaxPerMin: 3, pairMinIntervalMs: 0 });
    const t0 = clock.now();
    const at: number[] = [];
    await clock.run(Array.from({ length: 5 }, async () => {
        const g = await s.acquire(req({ timeoutMs: 600_000 }));
        at.push(clock.now() - t0);
        g.release();
    }));
    assert(at.slice(0, 3).every((x) => x === 0), `first 3 immediate: ${at}`);
    assert(at[3] === 60_000 && at[4] === 60_000, `4th and 5th wait for the window: ${at}`);

    // global min interval keeps working
    clock = new FakeClock();
    s = makeScheduler(clock, { minIntervalMs: 1000, pairMinIntervalMs: 0 });
    const t1 = clock.now();
    const gaps: number[] = [];
    await clock.run([1, 2, 3].map(async () => { const g = await s.acquire(req()); gaps.push(clock.now() - t1); g.release(); }));
    assert(gaps.join() === '0,1000,2000', `min interval: ${gaps}`);

    // concurrency
    clock = new FakeClock();
    s = makeScheduler(clock, { maxConcurrency: 1, pairMinIntervalMs: 0 });
    const first = await s.acquire(req());
    let secondGranted = false;
    const second = s.acquire(req()).then((g) => { secondGranted = true; return g; });
    await clock.run([Promise.resolve()]);
    for (let i = 0; i < 5; i++) await new Promise((r) => setImmediate(r));
    assert(!secondGranted, 'second waits for the concurrency slot');
    first.release();
    const g2 = await second;
    assert(secondGranted, 'granted after release');
    g2.release();
    console.log('✅ scheduler: global ceiling, global min interval, concurrency');
}

async function testQueueTimeout(): Promise<void> {
    const clock = new FakeClock();
    const s = makeScheduler(clock, { maxConcurrency: 1, pairMinIntervalMs: 0 });
    const holder = await s.acquire(req());
    const waiting = s.acquire(req({ timeoutMs: 45_000 }));
    const behind = s.acquire(req({ timeoutMs: 10_000 }));
    const [w, b] = await clock.run([waiting, behind]);
    assert(b.status === 'rejected' && b.reason instanceof QueueTimeoutError, 'request behind times out first');
    // the text reports the time actually waited (10s here, not the configured 45s), an ETA and a do-not-retry hint
    assert(/queue wait exceeded 10s \(1 ahead\); retry in ~\d+s, do not retry immediately/.test((b as PromiseRejectedResult).reason.message), `text: ${(b as PromiseRejectedResult).reason.message}`);
    // nobody ahead: no contradictory "(0 requests ahead)", it says it waited on the global rate window
    const headMsg = (w as PromiseRejectedResult).reason.message as string;
    assert(w.status === 'rejected' && /queue wait exceeded 45s waiting on the global rate window \(limit 1000\/min\); retry in ~\d+s, do not retry immediately/.test(headMsg) && !/0 ahead|0 requests/.test(headMsg), `head text: ${headMsg}`);
    assert(((w as PromiseRejectedResult).reason as QueueTimeoutError).retryAfterSec >= 1 && ((w as PromiseRejectedResult).reason as QueueTimeoutError).queuedMs === 45_000, 'retryAfterSec and queuedMs (actual wait) on the error');
    holder.release();
    // a timed-out waiter must not poison the queue
    const g = await s.acquire(req());
    g.release();
    console.log('✅ scheduler: queue timeout error text, queue stays healthy');
}

async function testEtaQueueDepthAndAbort(): Promise<void> {
    // ETA follows the global rate: (ahead+1) * 60000 / max-per-minute
    let clock = new FakeClock();
    let s = makeScheduler(clock, { maxConcurrency: 1, pairMinIntervalMs: 0, globalMaxPerMin: 12, maxQueueDepth: 2 });
    const holder = await s.acquire(req());
    const q1 = s.acquire(req({ timeoutMs: 600_000 }));
    const q2 = s.acquire(req({ timeoutMs: 600_000 }));
    let full: unknown;
    try { await s.acquire(req()); } catch (e) { full = e; }
    assert(full instanceof QueueFullError && /queue full \(2 waiting, max 2\); retry in ~15s, do not retry immediately/.test(full.message) && full.retryAfterSec === 15, `queue full: ${(full as Error)?.message}`);
    // a queued request can be aborted (client disconnected): it leaves, the others are still served
    const ac = new AbortController();
    clock = new FakeClock();
    s = makeScheduler(clock, { maxConcurrency: 1, pairMinIntervalMs: 0 });
    const h2 = await s.acquire(req());
    const aborted = s.acquire(req({ timeoutMs: 600_000, signal: ac.signal }));
    const after = s.acquire(req({ timeoutMs: 600_000 }));
    await new Promise((r) => setImmediate(r));
    ac.abort();
    let abortError: unknown;
    try { await aborted; } catch (e) { abortError = e; }
    assert(abortError instanceof QueueAbortedError, 'aborted waiter rejects with QueueAbortedError');
    h2.release();
    const [served] = await clock.run([after]);
    assert(served.status === 'fulfilled', 'request behind the aborted one is served');
    (served as PromiseFulfilledResult<Grant>).value.release();
    holder.release();
    void q1; void q2;
    console.log('✅ scheduler: ETA, max queue depth, abort frees the queue ticket');
}

async function testBackoff(): Promise<void> {
    const clock = new FakeClock();
    const s = makeScheduler(clock, { backoffBaseMs: 60_000, backoffMaxMs: 300_000 });
    const t = clock.now();
    const levels = [1, 2, 3, 4].map(() => s.reportFailure('duckduckgo', 'a'));
    assert(levels.map((l) => l.level).join() === '1,2,3,4', 'levels grow');
    assert(s.pairState('duckduckgo', 'a')!.cooldownUntil === t + 300_000, 'capped at max');
    const first = s.reportFailure('bing', 'a');
    assert(first.until === t + 60_000, 'base cooldown');
    const second = s.reportFailure('bing', 'a');
    assert(second.until === t + 120_000, `exponential: ${second.until - t}`);
    s.reportSuccess('bing', 'a');
    const afterReset = s.reportFailure('bing', 'a');
    assert(afterReset.level === 1 && afterReset.until === t + 60_000, 'success resets the level');

    // sick pair is skipped by the scheduler
    const g = await s.acquire(req({ rotated: ['bing'], egresses: ['a', 'b'] }));
    assert(g.picks.get('bing') === 'b', `skips sick pair: ${g.picks.get('bing')}`);
    g.release();
    // all pairs of an engine cooling + extras -> engine skipped, request still granted
    s.reportFailure('bing', 'b');
    const g2 = await s.acquire(req({ rotated: ['bing'], egresses: ['a', 'b'], hasExtras: true }));
    assert(g2.picks.size === 0 && g2.skipped.length === 1 && g2.skipped[0].engine === 'bing', 'degraded grant');
    g2.release();
    // ... without extras and recovery beyond the deadline -> readable error
    let cooling: unknown;
    try { await s.acquire(req({ rotated: ['bing'], egresses: ['a', 'b'], timeoutMs: 1000 })); } catch (e) { cooling = e; }
    assert(cooling instanceof AllPairsCoolingError && /cooling down/.test(cooling.message), 'cooling error');
    // ... but waits when recovery is within the deadline
    const waited = s.acquire(req({ rotated: ['bing'], egresses: ['a', 'b'], timeoutMs: 600_000 }));
    const [r] = await clock.run([waited]);
    assert(r.status === 'fulfilled' && clock.now() - t >= 60_000, 'waits for earliest recovery');
    (r as PromiseFulfilledResult<Grant>).value.release();
    console.log('✅ scheduler: backoff exponential/cap/reset, sick pair skipped, degraded grant, cooling error');
}

// ---- integration through searchSearxng with mocked HTTP ----

function resetConfig(): void {
    __resetSearxngStateForTests();
    Object.assign(config, {
        searxngUrl: 'http://127.0.0.1:8889',
        searxngEngines: [],
        searxngCategories: [],
        searxngLanguage: undefined,
        searxngMaxPages: 1,
        searxngMinIntervalMs: 0,
        searxngCacheTtlMs: 0,
        searxngMaxConcurrency: 4,
        searxngTimeoutMs: 10_000,
        searxngRotateEngines: ['duckduckgo'],
        searxngEgresses: ['a', 'b'],
        searxngExtraEngines: [],
        searxngPairMinIntervalMs: 10_000,
        searxngGlobalMaxPerMin: 1000,
        searxngQueueTimeoutMs: 45_000,
        searxngMaxQueueDepth: 30,
        searxngBackoffBaseMs: 60_000,
        searxngBackoffMaxMs: 3_600_000,
        searxngPartialCacheTtlMs: 600_000
    });
}

type Call = { engines: string; at: number; timeout: unknown };
const rec = (name: string) => ({ title: name, url: `https://e.com/${encodeURIComponent(name)}-${Math.random()}`, content: 'c', engine: name, engines: [name] });

function mockUpstream(clock: FakeClock, handler: (engines: string, n: number) => { results: object[]; unresponsive?: unknown[] }, durationMs = 0): Call[] {
    const calls: Call[] = [];
    __setSearxngHttpGetForTests(async (_url, options) => {
        const engines = String((options.params as any).engines ?? '');
        calls.push({ engines, at: clock.now(), timeout: options.timeout });
        if (durationMs > 0) await clock.sleep(durationMs);
        const r = handler(engines, calls.length);
        return { status: 200, data: { results: r.results, unresponsive_engines: r.unresponsive ?? [] } } as any;
    });
    return calls;
}

async function testBackoffThroughSearch(): Promise<void> {
    resetConfig();
    const clock = new FakeClock();
    __setSearxngClockForTests(clock);
    // a is unresponsive -> retried once on b (through the scheduler); b delivers
    const calls = mockUpstream(clock, (engines) => engines === 'duckduckgo a'
        ? { results: [], unresponsive: [['duckduckgo a', 'CAPTCHA']] }
        : { results: [rec(engines)] });
    const [r1] = await clock.run([searchSearxngDetailed('q1', 5)]);
    assert(r1.status === 'fulfilled', `search 1: ${r1.status === 'rejected' ? r1.reason : ''}`);
    assert(calls.map((c) => c.engines).join('|') === 'duckduckgo a|duckduckgo b', `retried once on other pair: ${calls.map((c) => c.engines)}`);
    assert(__getSearxngSchedulerForTests().pairState('duckduckgo', 'a')!.failures === 1, 'a is sick');

    // next search: a skipped while cooling; b must respect its 10s pair interval
    const tBefore = clock.now();
    const [r2] = await clock.run([searchSearxngDetailed('q2', 5)]);
    assert(r2.status === 'fulfilled', 'search 2');
    assert(calls[2].engines === 'duckduckgo b', `sick pair skipped: ${calls[2].engines}`);
    assert(calls[2].at - calls[1].at >= 10_000, `pair interval respected: ${calls[2].at - calls[1].at}`);
    assert(calls[2].at >= tBefore, 'time moved');

    // unresponsive engine is a failure EVEN when results are non-empty
    resetConfig();
    mockUpstream(clock, (engines) => ({ results: [rec(engines)], unresponsive: [[engines, 'timeout']] }));
    await clock.run([searchSearxngDetailed('q3', 5)]);
    assert(__getSearxngSchedulerForTests().pairState('duckduckgo', 'a')!.failures === 1, 'non-empty but unresponsive -> sick');
    // zero contribution while the request had results -> sick
    resetConfig();
    config.searxngRotateEngines = ['duckduckgo', 'bing'];
    mockUpstream(clock, () => ({ results: Array.from({ length: 5 }, () => rec('duckduckgo a')) }));
    await clock.run([searchSearxngDetailed('q4', 5)]);
    const sch = __getSearxngSchedulerForTests();
    assert(sch.pairState('bing', 'a')!.failures === 1 && sch.pairState('duckduckgo', 'a')!.failures === 0, 'zero-contribution engine is sick, contributing one is not');
    // ... but not with a small sample (< 5 results) and not beyond page 1
    resetConfig();
    config.searxngRotateEngines = ['duckduckgo', 'bing'];
    mockUpstream(clock, () => ({ results: [rec('duckduckgo a'), rec('duckduckgo a')] }));
    await clock.run([searchSearxngDetailed('q4b', 5)]);
    assert((__getSearxngSchedulerForTests().pairState('bing', 'a')?.failures ?? 0) === 0, 'small sample: no zero-contribution verdict');
    mockUpstream(clock, () => ({ results: Array.from({ length: 6 }, () => rec('duckduckgo a')) }));
    await clock.run([searchSearxngDetailed('q4c', 5, { pageno: 2 })]);
    assert((__getSearxngSchedulerForTests().pairState('bing', 'a')?.failures ?? 0) === 0, 'only page 1 is judged');
    resetConfig();
    config.searxngRotateEngines = ['duckduckgo', 'bing'];
    mockUpstream(clock, () => ({ results: Array.from({ length: 5 }, () => rec('duckduckgo a')) }));
    await clock.run([searchSearxngDetailed('q4', 5)]);
    // success resets
    sch.reportSuccess('bing', 'a');
    assert(sch.pairState('bing', 'a')!.failures === 0, 'success resets');

    // all pairs of the rotated engine cooling + extras: sent without it, reported in partialFailures
    resetConfig();
    config.searxngExtraEngines = ['wikipedia'];
    const s2 = __getSearxngSchedulerForTests();
    s2.reportFailure('duckduckgo', 'a');
    s2.reportFailure('duckduckgo', 'b');
    const calls2 = mockUpstream(clock, (engines) => ({ results: [rec(engines)] }));
    const [r5] = await clock.run([searchSearxngDetailed('q5', 5)]);
    assert(r5.status === 'fulfilled', 'degraded search ok');
    const d = (r5 as PromiseFulfilledResult<Awaited<ReturnType<typeof searchSearxngDetailed>>>).value;
    assert(calls2[0].engines === 'wikipedia', `extras only: ${calls2[0].engines}`);
    assert(d.partialFailures.length === 1 && d.partialFailures[0].engine === 'duckduckgo' && d.partialFailures[0].code === 'engine_degraded', 'partialFailures reported');

    // no extras and everything cooling beyond the queue timeout -> readable error, no upstream call
    resetConfig();
    const s3 = __getSearxngSchedulerForTests();
    s3.reportFailure('duckduckgo', 'a');
    s3.reportFailure('duckduckgo', 'b');
    const calls3 = mockUpstream(clock, () => ({ results: [] }));
    const [r6] = await clock.run([searchSearxngDetailed('q6', 5)]);
    assert(r6.status === 'rejected' && /cooling down.*do not retry immediately/.test((r6 as PromiseRejectedResult).reason.message) && calls3.length === 0, 'readable error without upstream call');
    __setSearxngClockForTests();
    console.log('✅ search: backoff, retry on other pair via scheduler, degraded request, readable error');
}

async function testSplitDeadlines(): Promise<void> {
    resetConfig();
    config.searxngRotateEngines = [];
    config.searxngMinIntervalMs = 5000;
    config.searxngTimeoutMs = 10_000;
    const clock = new FakeClock();
    __setSearxngClockForTests(clock);
    const calls = mockUpstream(clock, () => ({ results: [rec('x')] }), 8000);
    const [a, b] = await clock.run([searchSearxngDetailed('s1', 3), searchSearxngDetailed('s2', 3)]);
    assert(a.status === 'fulfilled' && b.status === 'fulfilled', `both ok: ${[a, b].map((x) => x.status === 'rejected' ? x.reason : 'ok')}`);
    const meta = (b as PromiseFulfilledResult<Awaited<ReturnType<typeof searchSearxngDetailed>>>).value.meta;
    assert(meta.queued_ms === 5000, `queued ${meta.queued_ms}`);
    assert(meta.upstream_ms === 8000, `upstream ${meta.upstream_ms}`);
    assert(calls[1].timeout === 10_000, `queue wait is not charged to the upstream deadline: ${calls[1].timeout}`);

    // upstream deadline still enforced independently
    resetConfig();
    config.searxngRotateEngines = [];
    config.searxngTimeoutMs = 5000;
    config.searxngMaxPages = 3;
    __setSearxngHttpGetForTests(async () => { await clock.sleep(6000); return { status: 200, data: { results: [rec('y')] } } as any; });
    const [c] = await clock.run([searchSearxngDetailed('s3', 50)]);
    assert(c.status === 'rejected' && /overall timeout of 5000ms/.test((c as PromiseRejectedResult).reason.message), 'upstream deadline');
    __setSearxngClockForTests();
    console.log('✅ search: split queue/upstream deadlines');
}

async function testCacheBypassesScheduler(): Promise<void> {
    resetConfig();
    config.searxngRotateEngines = [];
    config.searxngCacheTtlMs = 3_600_000;
    config.searxngGlobalMaxPerMin = 1;
    const clock = new FakeClock();
    __setSearxngClockForTests(clock);
    const calls = mockUpstream(clock, () => ({ results: [rec('x')] }));
    await clock.run([searchSearxngDetailed('cached', 3)]);
    // the scheduler is now saturated (1 per minute): a miss would wait 60s, a hit must not
    const t = clock.now();
    let reported: any;
    const results = await searchSearxng('cached', 3, { report: (info) => { reported = info; } });
    assert(results.length === 1 && calls.length === 1, 'served from cache');
    assert(clock.now() === t, 'no waiting');
    assert(reported.meta.cache === 'hit' && reported.meta.queued_ms === 0, `meta: ${JSON.stringify(reported)}`);

    // partial responses are cached only briefly
    resetConfig();
    config.searxngCacheTtlMs = 3_600_000;
    config.searxngPairMinIntervalMs = 0;
    mockUpstream(clock, (engines) => ({ results: [rec(engines)], unresponsive: [['wikipedia', 'timeout']] }));
    await clock.run([searchSearxngDetailed('partial', 3)]);
    const callsAfter = mockUpstream(clock, (engines) => ({ results: [rec(engines)] }));
    await clock.run([searchSearxngDetailed('partial', 3)]);
    assert(callsAfter.length === 0, 'partial response cached within short TTL');
    clock.t += 600_001;
    await clock.run([searchSearxngDetailed('partial', 3)]);
    assert((callsAfter.length as number) === 1, 'partial response expired after the short TTL, well before the full TTL');
    __setSearxngClockForTests();
    console.log('✅ search: cache hit bypasses scheduler, partial responses use short TTL');
}

function captureLogs<T>(fn: () => Promise<T>): Promise<{ lines: string[]; result: PromiseSettledResult<T> }> {
    const original = console.error;
    const lines: string[] = [];
    console.error = (...args: unknown[]) => { lines.push(args.map(String).join(' ')); };
    return fn().then(
        (value) => ({ lines, result: { status: 'fulfilled' as const, value } }),
        (reason) => ({ lines, result: { status: 'rejected' as const, reason } })
    ).finally(() => { console.error = original; });
}
const jsonOf = (lines: string[], prefix: string) => lines.filter((l) => l.startsWith(prefix)).map((l) => JSON.parse(l.slice(prefix.length)));

async function testObservability(): Promise<void> {
    resetConfig();
    config.searxngRotateEngines = ['duckduckgo', 'bing'];
    config.searxngExtraEngines = ['wikipedia'];
    const clock = new FakeClock();
    __setSearxngClockForTests(clock);
    // A: one line per upstream call at grant time (pairs, attempt, pageno) + summary with calls and skipped
    mockUpstream(clock, (engines) => engines.startsWith('duckduckgo a')
        ? { results: [], unresponsive: [['duckduckgo a', 'CAPTCHA']] }
        : { results: Array.from({ length: 5 }, () => rec('duckduckgo b')).concat(Array.from({ length: 5 }, () => rec('bing a'))) });
    __getSearxngSchedulerForTests().reportFailure('bing', 'b');
    const logged = await captureLogs(async () => (await clock.run([searchSearxngDetailed('observed', 5)]))[0]);
    const ups = jsonOf(logged.lines, '[local-search] upstream ');
    assert(ups.length === 2 && ups[0].attempt === 'first' && ups[1].attempt === 'retry', `two upstream lines: ${JSON.stringify(ups)}`);
    assert(JSON.stringify(ups[0].pairs) === '["duckduckgo:a","bing:a"]' && ups[0].pageno === 1 && typeof ups[0].started_at === 'string' && 'queued_ms' in ups[0] && 'queue_depth' in ups[0], `first line: ${JSON.stringify(ups[0])}`);
    assert(Date.parse(ups[1].started_at) - Date.parse(ups[0].started_at) >= 0 && JSON.stringify(ups[1].pairs) === '["duckduckgo:b"]', 'retry on the other pair');
    const summary = jsonOf(logged.lines, '[local-search] search ')[0];
    assert(summary.calls === 2 && Array.isArray(summary.skipped), `summary: ${JSON.stringify(summary)}`);

    // C/L: a cooling engine is skipped, listed in the log and in partialFailures; a cached partial response
    // gives the partial state back on a hit
    resetConfig();
    config.searxngCacheTtlMs = 3_600_000;
    config.searxngPairMinIntervalMs = 0;
    config.searxngExtraEngines = ['wikipedia'];
    config.searxngRotateEngines = ['duckduckgo', 'bing'];
    __getSearxngSchedulerForTests().reportFailure('bing', 'a');
    __getSearxngSchedulerForTests().reportFailure('bing', 'b');
    mockUpstream(clock, (engines) => ({ results: [rec(engines)], unresponsive: [['wikipedia', 'timeout']] }));
    const first = await captureLogs(async () => (await clock.run([searchSearxngDetailed('partial-state', 5)]))[0]);
    assert(jsonOf(first.lines, '[local-search] search ')[0].skipped.join() === 'bing', 'skipped engine in the summary line');
    const hitCalls = mockUpstream(clock, () => ({ results: [] }));
    const [hit] = await clock.run([searchSearxngDetailed('partial-state', 5)]);
    const hv = (hit as PromiseFulfilledResult<Awaited<ReturnType<typeof searchSearxngDetailed>>>).value;
    assert(hitCalls.length === 0 && hv.meta.cache === 'hit', 'served from cache');
    assert(hv.partialFailures.some((f) => f.engine === 'bing') && hv.meta.unresponsive.includes('wikipedia') && hv.skipped.join() === 'bing', `partial state restored on hit: ${JSON.stringify(hv.partialFailures)} ${hv.meta.unresponsive}`);

    // B: queued_ms reflects the real wait also when the wait ends in an error
    resetConfig();
    config.searxngRotateEngines = [];
    config.searxngGlobalMaxPerMin = 1;
    config.searxngQueueTimeoutMs = 5000;
    mockUpstream(clock, () => ({ results: [rec('x')] }));
    await clock.run([searchSearxngDetailed('b1', 5)]);
    const timedOut = await captureLogs(async () => (await clock.run([searchSearxngDetailed('b2', 5)]))[0]);
    const fail = jsonOf(timedOut.lines, '[local-search] search ')[0];
    assert(fail.queued_ms === 5000 && /queue wait exceeded 5s/.test(fail.error), `queued_ms on error: ${JSON.stringify(fail)}`);
    __setSearxngClockForTests();
    console.log('✅ search: per-call upstream log, skipped engines, cached partial state, queued_ms on errors');
}

async function testForwardingAndPages(): Promise<void> {
    const clock = new FakeClock();
    __setSearxngClockForTests(clock);
    // D: other categories / client engines are forwarded as-is, no rotation, no pairs; time_range/safesearch pass through
    resetConfig();
    config.searxngCacheTtlMs = 3_600_000;
    config.searxngRotateEngines = ['duckduckgo'];
    const seen: Array<Record<string, unknown>> = [];
    __setSearxngHttpGetForTests(async (_u, options) => {
        seen.push(options.params as Record<string, unknown>);
        return { status: 200, data: { results: [rec('whatever')] } } as any;
    });
    await clock.run([searchSearxngDetailed('pics', 5, { categories: ['images'] })]);
    assert(seen[0].categories === 'images' && !('engines' in seen[0]), `images forwarded without rotation: ${JSON.stringify(seen[0])}`);
    await clock.run([searchSearxngDetailed('pics', 5, { engines: ['brave', 'qwant'], timeRange: 'week', safesearch: '1' })]);
    assert(seen[1].engines === 'brave,qwant' && seen[1].time_range === 'week' && seen[1].safesearch === '1', `client engines/time_range/safesearch: ${JSON.stringify(seen[1])}`);
    assert(__getSearxngSchedulerForTests().pairState('duckduckgo', 'a') === undefined, 'no pair touched in forwarded mode');
    // general web search is rotated as before, and time_range is part of the cache key
    await clock.run([searchSearxngDetailed('web', 5, { categories: ['general'], timeRange: 'day' })]);
    assert(seen[2].engines === 'duckduckgo a', `general is rotated: ${seen[2].engines}`);
    await clock.run([searchSearxngDetailed('web', 5, { categories: ['general'], timeRange: 'day' })]);
    assert(seen.length === 3, 'same query+time_range is a cache hit');
    await clock.run([searchSearxngDetailed('web', 5, { categories: ['general'], timeRange: 'year' })]);
    assert((seen.length as number) === 4 && seen[3].time_range === 'year', 'time_range is part of the cache key');

    // J: a later page hitting the queue timeout keeps the earlier results and reports the gap
    resetConfig();
    config.searxngRotateEngines = [];
    config.searxngMaxPages = 3;
    config.searxngGlobalMaxPerMin = 1;
    config.searxngQueueTimeoutMs = 3000;
    mockUpstream(clock, () => ({ results: [rec('p1')] }));
    const [pg] = await clock.run([searchSearxngDetailed('pages', 10)]);
    assert(pg.status === 'fulfilled', `partial multi-page is not an error: ${pg.status === 'rejected' ? pg.reason : ''}`);
    const pv = (pg as PromiseFulfilledResult<Awaited<ReturnType<typeof searchSearxngDetailed>>>).value;
    assert(pv.results.length === 1 && pv.partialFailures.some((f) => /page 2 not fetched.*queue wait exceeded/.test(f.message)), `kept page 1: ${JSON.stringify(pv.partialFailures)}`);
    __setSearxngClockForTests();
    console.log('✅ search: forwarded categories/engines, time_range/safesearch cache key, later-page queue timeout keeps results');
}

async function testSearchServiceBusyFlag(): Promise<void> {
    const service = createSearchService({
        searxng: async () => { throw new SchedulerBusyError('local-search: queue wait exceeded 45s (1 ahead); retry in ~40s, do not retry immediately', 40, 45000); }
    });
    const r = await service.execute({ query: 'q', engines: ['searxng'], limit: 5 });
    assert(r.totalResults === 0 && r.partialFailures[0].retryAfterSec === 40 && r.partialFailures[0].code === 'engine_error', 'busy failure carries retryAfterSec so the tool can answer isError');
    console.log('✅ search service: busy failures carry retryAfterSec');
}

async function main(): Promise<void> {
    await testPairIntervalAndFifo();
    await testGlobalLimits();
    await testQueueTimeout();
    await testEtaQueueDepthAndAbort();
    await testBackoff();
    await testBackoffThroughSearch();
    await testObservability();
    await testForwardingAndPages();
    await testSearchServiceBusyFlag();
    await testSplitDeadlines();
    await testCacheBypassesScheduler();
    __setSearxngHttpGetForTests();
    __setSearxngClockForTests();
    console.log('\nSearXNG scheduler tests passed.');
}

main().catch((error) => { console.error(error); process.exit(1); });
