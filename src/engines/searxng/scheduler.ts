// Pure scheduling logic for SearXNG upstream calls (no network, injectable clock).
//
// A "pair" is one rotated engine on one egress (e.g. `duckduckgo`@`madrid`). Every upstream call
// reserves, atomically, a global slot (concurrency, global per-minute ceiling, global min interval)
// and one healthy pair per rotated engine (per-pair min interval, least-recently-used first).
// Waiters are served strictly FIFO. Sick pairs back off exponentially and are skipped.

export type Clock = { now: () => number; sleep: (ms: number, signal?: AbortSignal) => Promise<void> };

export interface SchedulerOptions {
    maxConcurrency: number;
    minIntervalMs: number;
    pairMinIntervalMs: number;
    globalMaxPerMin: number;
    backoffBaseMs: number;
    backoffMaxMs: number;
    /** full configured queue timeout; only used for the error text */
    queueTimeoutMs: number;
}

export interface AcquireRequest {
    rotated: string[];
    egresses: string[];
    /** extra engines are sent with the request, so it is still useful with zero healthy pairs */
    hasExtras: boolean;
    /** remaining queue-wait budget for this caller */
    timeoutMs: number;
}

export interface SkippedEngine {
    engine: string;
    retryAt: number;
}

export interface Grant {
    picks: Map<string, string>;
    skipped: SkippedEngine[];
    queuedMs: number;
    /** requests in the queue (including the one being served) when this one was enqueued */
    queueDepth: number;
    release: () => void;
}

export interface BackoffChange {
    pair: string;
    level: number;
    until: number;
}

export class QueueTimeoutError extends Error {
    readonly retryAfterSec: number;
    constructor(timeoutMs: number, ahead: number) {
        const sec = Math.round(timeoutMs / 1000);
        super(`local-search: queue wait exceeded ${sec}s (${ahead} requests ahead), retry later`);
        this.name = 'QueueTimeoutError';
        this.retryAfterSec = Math.max(1, sec);
    }
}

export class AllPairsCoolingError extends Error {
    readonly retryAfterSec: number;
    constructor(retryInMs: number) {
        const sec = Math.max(1, Math.ceil(retryInMs / 1000));
        super(`local-search: all upstream engine/egress pairs are cooling down after errors (earliest recovery in ${sec}s), retry later`);
        this.name = 'AllPairsCoolingError';
        this.retryAfterSec = sec;
    }
}

interface PairState {
    lastStart: number;
    failures: number;
    cooldownUntil: number;
}

interface Ticket {
    turn: Promise<void>;
    resolveTurn: () => void;
}

const WINDOW_MS = 60_000;

export class SearxngScheduler {
    private pairs = new Map<string, PairState>();
    private queue: Ticket[] = [];
    private active = 0;
    private lastStart = -Infinity;
    private window: number[] = [];
    private wake: (() => void) | null = null;

    constructor(private options: () => SchedulerOptions, private clock: () => Clock) {}

    reset(): void {
        this.pairs.clear();
        this.queue.length = 0;
        this.active = 0;
        this.lastStart = -Infinity;
        this.window.length = 0;
        this.wake = null;
    }

    pairKey(engine: string, egress: string): string {
        return `${engine} ${egress}`;
    }

    pairState(engine: string, egress: string): Readonly<PairState> | undefined {
        return this.pairs.get(this.pairKey(engine, egress));
    }

    private state(key: string): PairState {
        let s = this.pairs.get(key);
        if (!s) {
            s = { lastStart: -Infinity, failures: 0, cooldownUntil: 0 };
            this.pairs.set(key, s);
        }
        return s;
    }

    /** Successful use of a pair resets its backoff. */
    reportSuccess(engine: string, egress: string): void {
        const s = this.pairs.get(this.pairKey(engine, egress));
        if (s) {
            s.failures = 0;
            s.cooldownUntil = 0;
        }
    }

    /** Failure of a pair: cooldown = base * 2^(n-1), capped. */
    reportFailure(engine: string, egress: string): BackoffChange {
        const o = this.options();
        const key = this.pairKey(engine, egress);
        const s = this.state(key);
        s.failures++;
        const cooldown = Math.min(o.backoffMaxMs, o.backoffBaseMs * 2 ** (s.failures - 1));
        s.cooldownUntil = this.clock().now() + cooldown;
        return { pair: key, level: s.failures, until: s.cooldownUntil };
    }

    private notify(): void {
        const wake = this.wake;
        this.wake = null;
        wake?.();
    }

    private plan(req: AcquireRequest, now: number):
        | { kind: 'ready' | 'wait'; waitMs: number; picks: Map<string, string>; skipped: SkippedEngine[] }
        | { kind: 'cooling'; retryAt: number } {
        const o = this.options();
        let waitMs = 0;

        while (this.window.length > 0 && this.window[0] <= now - WINDOW_MS) this.window.shift();
        if (this.window.length >= Math.max(1, o.globalMaxPerMin)) {
            waitMs = Math.max(waitMs, this.window[0] + WINDOW_MS - now);
        }
        waitMs = Math.max(waitMs, this.lastStart + o.minIntervalMs - now);

        const picks = new Map<string, string>();
        const skipped: SkippedEngine[] = [];
        for (const engine of req.rotated) {
            const candidates = req.egresses.map((egress, index) => {
                const s = this.pairs.get(this.pairKey(engine, egress));
                return { egress, index, lastStart: s?.lastStart ?? -Infinity, cooldownUntil: s?.cooldownUntil ?? 0 };
            });
            const healthy = candidates.filter((c) => c.cooldownUntil <= now);
            if (healthy.length === 0) {
                skipped.push({ engine, retryAt: Math.min(...candidates.map((c) => c.cooldownUntil)) });
                continue;
            }
            // least recently used == the pair that became available earliest
            healthy.sort((a, b) => a.lastStart - b.lastStart || a.index - b.index);
            const best = healthy[0];
            waitMs = Math.max(waitMs, best.lastStart + o.pairMinIntervalMs - now);
            picks.set(engine, best.egress);
        }

        if (req.rotated.length > 0 && picks.size === 0 && !req.hasExtras) {
            return { kind: 'cooling', retryAt: Math.min(...skipped.map((s) => s.retryAt)) };
        }
        const slotFree = this.active < Math.max(1, o.maxConcurrency);
        return { kind: slotFree && waitMs <= 0 ? 'ready' : 'wait', waitMs: Math.max(0, waitMs), picks, skipped };
    }

    async acquire(req: AcquireRequest): Promise<Grant> {
        const clock = this.clock();
        const enqueuedAt = clock.now();
        const deadline = enqueuedAt + req.timeoutMs;
        const queueDepth = this.queue.length;

        let resolveTurn!: () => void;
        const ticket: Ticket = { turn: new Promise<void>((r) => { resolveTurn = r; }), resolveTurn };
        this.queue.push(ticket);
        if (this.queue.length === 1) ticket.resolveTurn();

        const leave = () => {
            const idx = this.queue.indexOf(ticket);
            if (idx >= 0) this.queue.splice(idx, 1);
            if (idx === 0 && this.queue.length > 0) this.queue[0].resolveTurn();
        };
        const timeout = () => new QueueTimeoutError(this.options().queueTimeoutMs, Math.max(0, this.queue.indexOf(ticket)));

        try {
            // 1. wait for our turn (strict FIFO)
            if (this.queue[0] !== ticket) {
                const turnTimer = new AbortController();
                const turnResult = await Promise.race([
                    ticket.turn.then(() => 'turn' as const),
                    clock.sleep(Math.max(0, deadline - clock.now()), turnTimer.signal).then(() => 'timeout' as const)
                ]);
                turnTimer.abort();
                if (turnResult === 'timeout') throw timeout();
            }

            // 2. head of the queue: wait until a global slot and healthy pairs are available
            for (;;) {
                const now = clock.now();
                const plan = this.plan(req, now);
                if (plan.kind === 'cooling') {
                    if (plan.retryAt > deadline) throw new AllPairsCoolingError(plan.retryAt - now);
                } else if (plan.kind === 'ready') {
                    this.window.push(now);
                    this.lastStart = now;
                    this.active++;
                    for (const [engine, egress] of plan.picks) {
                        this.state(this.pairKey(engine, egress)).lastStart = now;
                    }
                    let released = false;
                    leave();
                    return {
                        picks: plan.picks,
                        skipped: plan.skipped,
                        queuedMs: now - enqueuedAt,
                        queueDepth,
                        release: () => {
                            if (released) return;
                            released = true;
                            this.active--;
                            this.notify();
                        }
                    };
                }
                if (now >= deadline) throw timeout();

                const remaining = deadline - now;
                const waitMs = plan.kind === 'cooling' ? plan.retryAt - now : plan.waitMs;
                const sleeper = new AbortController();
                const woken = new Promise<void>((r) => { this.wake = r; });
                await Promise.race([
                    clock.sleep(Math.min(remaining, waitMs > 0 ? waitMs : remaining), sleeper.signal),
                    woken
                ]);
                sleeper.abort();
            }
        } catch (error) {
            leave();
            throw error;
        }
    }
}
