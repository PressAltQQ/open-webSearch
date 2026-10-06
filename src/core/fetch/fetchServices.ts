import { FetchWebContentOptions, FetchWebContentResult } from '../../engines/web/fetchWebContent.js';
import {
    validateArticleUrl,
    validateGithubRepositoryUrl,
    validatePublicWebUrl
} from '../validation/targetValidation.js';
import { config } from '../../config.js';
import { HostLimiter } from '../../utils/hostLimiter.js';

// One limiter for every web fetch of this process (fetchWebContent tool and deepresearch fan-out).
export const webFetchLimiter = new HostLimiter(() => ({
    perHostConcurrency: config.fetchPerHostConcurrency ?? 2,
    perHostMinIntervalMs: config.fetchPerHostMinIntervalMs ?? 1000,
    maxConcurrency: config.fetchMaxConcurrency ?? 4,
    queueTimeoutMs: config.fetchQueueTimeoutMs ?? 30000
}));

export type ArticleFetcher = (url: string) => Promise<{ content: string }>;
export type GithubReadmeFetcher = (url: string) => Promise<string | null>;
export type WebFetcher = (url: string, maxChars: number, options?: FetchWebContentOptions) => Promise<FetchWebContentResult>;

export function createArticleFetchService(
    type: 'linuxdo' | 'csdn' | 'juejin',
    fetcher: ArticleFetcher
) {
    return {
        async execute({ url }: { url: string }): Promise<{ content: string }> {
            if (!validateArticleUrl(url, type)) {
                throw new Error(`Invalid ${type} article URL`);
            }

            return fetcher(url);
        }
    };
}

export function createGithubReadmeService(fetcher: GithubReadmeFetcher) {
    return {
        async execute({ url }: { url: string }): Promise<string | null> {
            if (!validateGithubRepositoryUrl(url)) {
                throw new Error('Invalid GitHub repository URL');
            }

            return fetcher(url);
        }
    };
}

export function createWebFetchService(fetcher: WebFetcher, limiter: HostLimiter = webFetchLimiter) {
    return {
        async execute({
            url,
            maxChars,
            readability,
            includeLinks,
            deadlineMs
        }: {
            url: string;
            maxChars: number;
            readability?: boolean;
            includeLinks?: boolean;
            /** absolute time (Date.now()-style) after which waiting in the fetch queue is pointless */
            deadlineMs?: number;
        }): Promise<FetchWebContentResult> {
            if (!validatePublicWebUrl(url)) {
                throw new Error('Invalid public HTTP(S) URL');
            }

            const host = new URL(url).hostname.toLowerCase();
            return limiter.run(host, ({ queuedMs }) => {
                console.error(`[local-search] fetch ${JSON.stringify({ host, queued_ms: queuedMs })}`);
                return fetcher(url, maxChars, { readability, includeLinks });
            }, { deadlineMs });
        }
    };
}
