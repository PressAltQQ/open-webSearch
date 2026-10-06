import { config } from '../config.js';
import { SearxngScheduler, QueueTimeoutError, AllPairsCoolingError } from '../engines/searxng/scheduler.js';
import type { Clock, SchedulerOptions, Grant } from '../engines/searxng/scheduler.js';
import {
    searchSearxng,
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
    queueTimeoutMs: 45_000
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
    assert(/queue wait exceeded 45s \(1 requests ahead\), retry later/.test((b as PromiseRejectedResult).reason.message), `text: ${(b as PromiseRejectedResult).reason.message}`);
    assert(w.status === 'rejected' && /queue wait exceeded 45s \(0 requests ahead\), retry later/.test((w as PromiseRejectedResult).reason.message), 'head of queue times out too');
    assert(((w as PromiseRejectedResult).reason as QueueTimeoutError).retryAfterSec === 45, 'retryAfterSec');
    holder.release();
    // a timed-out waiter must not poison the queue
    const g = await s.acquire(req());
    g.release();
    console.log('✅ scheduler: queue timeout error text, queue stays healthy');
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
    mockUpstream(clock, () => ({ results: [rec('duckduckgo a')] }));
    await clock.run([searchSearxngDetailed('q4', 5)]);
    const sch = __getSearxngSchedulerForTests();
    assert(sch.pairState('bing', 'a')!.failures === 1 && sch.pairState('duckduckgo', 'a')!.failures === 0, 'zero-contribution engine is sick, contributing one is not');
    // success resets
    mockUpstream(clock, () => ({ results: [rec('duckduckgo b'), rec('bing b')] }));
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
    assert(r6.status === 'rejected' && /cooling down.*retry later/.test((r6 as PromiseRejectedResult).reason.message) && calls3.length === 0, 'readable error without upstream call');
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

async function main(): Promise<void> {
    await testPairIntervalAndFifo();
    await testGlobalLimits();
    await testQueueTimeout();
    await testBackoff();
    await testBackoffThroughSearch();
    await testSplitDeadlines();
    await testCacheBypassesScheduler();
    __setSearxngHttpGetForTests();
    __setSearxngClockForTests();
    console.log('\nSearXNG scheduler tests passed.');
}

main().catch((error) => { console.error(error); process.exit(1); });
