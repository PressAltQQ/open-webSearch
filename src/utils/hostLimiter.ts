// Per-host + global limiter for outgoing page fetches (fetchWebContent, deepresearch).
// Hosts are independent: a slow or throttled host never blocks others (FIFO within a host).

export interface HostLimiterOptions {
    perHostConcurrency: number;
    perHostMinIntervalMs: number;
    maxConcurrency: number;
    queueTimeoutMs: number;
}

export class FetchQueueTimeoutError extends Error {
    constructor(host: string, timeoutMs: number, ahead: number) {
        super(`local-search: fetch queue wait exceeded ${Math.round(timeoutMs / 1000)}s for host ${host} (${ahead} requests ahead), retry later`);
        this.name = 'FetchQueueTimeoutError';
    }
}

interface Waiter {
    host: string;
    enqueuedAt: number;
    start: (queuedMs: number) => void;
    timer: ReturnType<typeof setTimeout>;
}

export class HostLimiter {
    private waiters: Waiter[] = [];
    private active = 0;
    private hosts = new Map<string, { active: number; lastStart: number }>();
    private pumpTimer: ReturnType<typeof setTimeout> | undefined;

    constructor(private options: () => HostLimiterOptions, private now: () => number = () => Date.now()) {}

    /** Run `fn` once a global and a per-host slot are available. Resolves with fn's result. */
    async run<T>(host: string, fn: (info: { queuedMs: number }) => Promise<T>): Promise<T> {
        const queuedMs = await this.acquire(host);
        try {
            return await fn({ queuedMs });
        } finally {
            this.active--;
            this.hosts.get(host)!.active--;
            this.pump();
        }
    }

    private acquire(host: string): Promise<number> {
        const o = this.options();
        return new Promise<number>((resolve, reject) => {
            const waiter: Waiter = {
                host,
                enqueuedAt: this.now(),
                start: resolve,
                timer: setTimeout(() => {
                    const idx = this.waiters.indexOf(waiter);
                    if (idx < 0) return;
                    this.waiters.splice(idx, 1);
                    reject(new FetchQueueTimeoutError(host, o.queueTimeoutMs, idx));
                }, o.queueTimeoutMs)
            };
            this.waiters.push(waiter);
            this.pump();
        });
    }

    private pump(): void {
        if (this.pumpTimer) {
            clearTimeout(this.pumpTimer);
            this.pumpTimer = undefined;
        }
        const o = this.options();
        const now = this.now();
        let nextWake = Infinity;
        for (let i = 0; i < this.waiters.length;) {
            const w = this.waiters[i];
            const state = this.hosts.get(w.host) ?? { active: 0, lastStart: -Infinity };
            const intervalAt = state.lastStart + o.perHostMinIntervalMs;
            const slotFree = this.active < Math.max(1, o.maxConcurrency) && state.active < Math.max(1, o.perHostConcurrency);
            if (slotFree && intervalAt <= now) {
                this.waiters.splice(i, 1);
                clearTimeout(w.timer);
                state.active++;
                state.lastStart = now;
                this.active++;
                this.hosts.set(w.host, state);
                w.start(now - w.enqueuedAt);
                continue;
            }
            if (slotFree && intervalAt > now) nextWake = Math.min(nextWake, intervalAt);
            i++;
        }
        if (nextWake !== Infinity) {
            this.pumpTimer = setTimeout(() => this.pump(), Math.max(1, nextWake - now));
        }
    }
}
