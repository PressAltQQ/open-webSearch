import { HostLimiter, FetchQueueTimeoutError } from '../utils/hostLimiter.js';
import { createWebFetchService } from '../core/fetch/fetchServices.js';

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const opts = (o: Partial<ConstructorParameters<typeof HostLimiter>[0] extends () => infer R ? R : never> = {}) => () => ({
    perHostConcurrency: 2, perHostMinIntervalMs: 0, maxConcurrency: 4, queueTimeoutMs: 2000, ...o
});

async function testPerHostConcurrency(): Promise<void> {
    const limiter = new HostLimiter(opts());
    let inFlight = 0, maxInFlight = 0;
    const job = () => limiter.run('a.example', async () => {
        inFlight++; maxInFlight = Math.max(maxInFlight, inFlight);
        await sleep(30);
        inFlight--;
    });
    await Promise.all(Array.from({ length: 6 }, job));
    assert(maxInFlight === 2, `per-host concurrency 2, saw ${maxInFlight}`);
    console.log('✅ fetch limiter: per-host concurrency');
}

async function testGlobalConcurrencyAcrossHosts(): Promise<void> {
    const limiter = new HostLimiter(opts({ maxConcurrency: 3 }));
    let inFlight = 0, maxInFlight = 0;
    await Promise.all(Array.from({ length: 9 }, (_, i) => limiter.run(`h${i}.example`, async () => {
        inFlight++; maxInFlight = Math.max(maxInFlight, inFlight);
        await sleep(20);
        inFlight--;
    })));
    assert(maxInFlight === 3, `global concurrency 3, saw ${maxInFlight}`);
    console.log('✅ fetch limiter: global concurrency across hosts');
}

async function testPerHostInterval(): Promise<void> {
    const limiter = new HostLimiter(opts({ perHostMinIntervalMs: 60 }));
    const starts: Record<string, number[]> = { a: [], b: [] };
    const t0 = Date.now();
    await Promise.all(['a', 'a', 'a', 'b'].map((h) => limiter.run(h, async () => { starts[h].push(Date.now() - t0); })));
    for (let i = 1; i < starts.a.length; i++) {
        assert(starts.a[i] - starts.a[i - 1] >= 55, `host a interval: ${starts.a}`);
    }
    assert(starts.b[0] < 40, `other host not delayed by a's interval: ${starts.b}`);
    console.log('✅ fetch limiter: per-host min interval, hosts independent');
}

async function testQueueTimeout(): Promise<void> {
    const limiter = new HostLimiter(opts({ perHostConcurrency: 1, queueTimeoutMs: 50 }));
    const slow = limiter.run('slow.example', () => sleep(200));
    let error: unknown;
    try { await limiter.run('slow.example', async () => 'never'); } catch (e) { error = e; }
    assert(error instanceof FetchQueueTimeoutError, 'queue timeout error');
    assert(/fetch queue wait exceeded .*s for host slow\.example \(\d+ requests ahead\), retry later/.test(error.message), `text: ${error.message}`);
    await slow;
    // slot is released and the limiter keeps working
    assert(await limiter.run('slow.example', async () => 'ok') === 'ok', 'recovers');
    console.log('✅ fetch limiter: queue timeout');
}

async function testServiceUsesLimiter(): Promise<void> {
    const limiter = new HostLimiter(opts({ perHostConcurrency: 1 }));
    let inFlight = 0, maxInFlight = 0;
    const service = createWebFetchService(async () => {
        inFlight++; maxInFlight = Math.max(maxInFlight, inFlight);
        await sleep(20);
        inFlight--;
        return { url: '', finalUrl: '', contentType: 'text/plain', title: '', truncated: false, content: '' } as any;
    }, limiter);
    // deepresearch-style fan-out: Promise.all over results of the same host
    await Promise.all(['https://x.example/1', 'https://x.example/2', 'https://x.example/3']
        .map((url) => service.execute({ url, maxChars: 1000 })));
    assert(maxInFlight === 1, `service fan-out limited per host, saw ${maxInFlight}`);
    console.log('✅ fetch limiter: web fetch service goes through the limiter');
}

async function main(): Promise<void> {
    await testPerHostConcurrency();
    await testGlobalConcurrencyAcrossHosts();
    await testPerHostInterval();
    await testQueueTimeout();
    await testServiceUsesLimiter();
    console.log('\nFetch limiter tests passed.');
}

main().catch((error) => { console.error(error); process.exit(1); });
