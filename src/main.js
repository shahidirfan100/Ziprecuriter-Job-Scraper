/**
 * ZipRecruiter Jobs Scraper - Fast & Reliable
 * Uses URL-based pagination (?page=X) for reliability
 */

import { PlaywrightCrawler, RequestQueue } from '@crawlee/playwright';
import { Actor, log } from 'apify';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';

await Actor.init();

// ============================================================================
// CONFIGURATION
// ============================================================================
const CONFIG = {
    CLOUDFLARE_WAIT: 4000,
    CONTENT_WAIT: 1500,
    MAX_PAGES: 50,
    JOBS_PER_PAGE: 20,
};

// ============================================================================
// STATISTICS
// ============================================================================
const stats = {
    pagesProcessed: 0,
    jobsExtracted: 0,
    startTime: Date.now(),
};

// Track seen job IDs globally
const seenJobIds = new Set();

// ============================================================================
// URL BUILDER
// ============================================================================

function buildSearchUrl(input, pageNum = 1) {
    let baseUrl;

    if (input.searchUrl?.trim()) {
        baseUrl = input.searchUrl.trim();
    } else {
        const params = new URLSearchParams();
        if (input.searchQuery?.trim()) params.append('search', input.searchQuery.trim());
        if (input.location?.trim()) params.append('location', input.location.trim());
        if (input.radius && input.location?.trim()) params.append('radius', input.radius);
        if (input.daysBack && input.daysBack !== 'any') params.append('days', input.daysBack);
        baseUrl = `https://www.ziprecruiter.com/jobs-search?${params.toString()}`;
    }

    // Add page parameter for pagination
    if (pageNum > 1) {
        const url = new URL(baseUrl);
        url.searchParams.set('page', pageNum.toString());
        return url.toString();
    }

    return baseUrl;
}

// ============================================================================
// JOB EXTRACTION
// ============================================================================

async function extractJobs(page) {
    return page.evaluate(() => {
        const jobs = [];
        document.querySelectorAll('.job_result_two_pane_v2').forEach(card => {
            try {
                const article = card.querySelector('article');
                const jobIdMatch = (article?.id || '').match(/job-card-(.+)/);
                const jobId = jobIdMatch?.[1] || '';

                const titleEl = card.querySelector('h2');
                const title = titleEl?.getAttribute('aria-label')?.trim() || titleEl?.textContent?.trim() || '';

                const companyEl = card.querySelector('[data-testid="job-card-company"]');
                const company = companyEl?.textContent?.trim() || '';
                let companyUrl = companyEl?.getAttribute('href') || '';
                if (companyUrl?.startsWith('/')) companyUrl = `https://www.ziprecruiter.com${companyUrl}`;

                const locationEl = card.querySelector('[data-testid="job-card-location"]');
                let location = locationEl?.textContent?.trim() || '';
                locationEl?.parentElement?.querySelectorAll('span').forEach(s => {
                    if (s.textContent.toLowerCase().includes('remote')) location += ' (Remote)';
                });

                let salary = 'Not specified';
                card.querySelectorAll('p').forEach(p => {
                    if (p.textContent?.includes('$')) salary = p.textContent.trim();
                });

                const postedMatch = (card.textContent || '').match(/Posted\s+(\d+\s+\w+\s+ago|today|yesterday)/i);

                if (title || company) {
                    jobs.push({
                        title: title || 'Unknown Title',
                        company,
                        companyUrl,
                        location,
                        salary,
                        jobType: 'Not specified',
                        postedDate: postedMatch?.[1] || '',
                        url: jobId ? `https://www.ziprecruiter.com/jobs/${jobId}` : '',
                        jobId,
                        scrapedAt: new Date().toISOString(),
                    });
                }
            } catch { /* skip */ }
        });
        return jobs;
    });
}

async function dismissPopups(page) {
    try {
        const btn = await page.$('button[aria-label="Close"]');
        if (btn) await btn.click();
    } catch { /* ignore */ }
}

// ============================================================================
// MAIN
// ============================================================================

try {
    const input = await Actor.getInput() || {};

    log.info('═══════════════════════════════════════════════════════════════');
    log.info('🚀 ZipRecruiter Jobs Scraper');
    log.info('═══════════════════════════════════════════════════════════════');

    if (!input.searchUrl?.trim() && !input.searchQuery?.trim()) {
        throw new Error('Provide "searchUrl" OR "searchQuery"');
    }

    const maxJobs = input.maxJobs ?? 50;
    const totalPagesNeeded = Math.ceil(maxJobs / CONFIG.JOBS_PER_PAGE);

    log.info(`🔍 Query: ${input.searchQuery || 'N/A'}`);
    log.info(`📍 Location: ${input.location || 'Auto'}`);
    log.info(`🎯 Max Jobs: ${maxJobs} (${totalPagesNeeded} pages)`);
    log.info('───────────────────────────────────────────────────────────────');

    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] }
    );
    const proxyUrl = await proxyConfiguration.newUrl();

    let totalScraped = 0;
    let currentPage = 1;
    let consecutiveEmpty = 0;

    // Create crawler
    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: Math.min(totalPagesNeeded + 5, CONFIG.MAX_PAGES),
        maxConcurrency: 1,
        navigationTimeoutSecs: 45,
        requestHandlerTimeoutSecs: 120,

        launchContext: {
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: true,
                proxy: proxyUrl,
                geoip: true,
                os: 'windows',
                locale: 'en-US',
                screen: { minWidth: 1280, maxWidth: 1920, minHeight: 720, maxHeight: 1080 },
            }),
        },

        preNavigationHooks: [async ({ page }) => {
            await page.setExtraHTTPHeaders({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'DNT': '1',
            });
        }],

        async requestHandler({ page, request }) {
            const pageNum = request.userData.pageNum || 1;

            log.info(`📄 Loading page ${pageNum}...`);
            await page.waitForTimeout(CONFIG.CLOUDFLARE_WAIT);

            const title = await page.title();
            if (title.includes('Just a moment')) {
                await page.waitForTimeout(4000);
            }

            // Wait for job container
            try {
                await page.waitForSelector('.job_result_two_pane_v2', { timeout: 20000 });
            } catch {
                log.warning(`Page ${pageNum}: No jobs found`);
                consecutiveEmpty++;

                if (pageNum === 1) {
                    await Actor.setValue('DEBUG', await page.content(), { contentType: 'text/html' });
                }
                return;
            }

            await dismissPopups(page);
            await page.waitForTimeout(CONFIG.CONTENT_WAIT);

            // Extract jobs
            const jobs = await extractJobs(page);
            stats.pagesProcessed++;

            // Deduplicate
            const newJobs = jobs.filter(job => {
                const key = job.jobId || `${job.title}-${job.company}`;
                if (seenJobIds.has(key)) return false;
                seenJobIds.add(key);
                return true;
            });

            if (newJobs.length === 0) {
                log.info(`📄 Page ${pageNum}: No new jobs`);
                consecutiveEmpty++;
            } else {
                consecutiveEmpty = 0;
                const toSave = newJobs.slice(0, maxJobs - totalScraped);
                await Actor.pushData(toSave);
                totalScraped += toSave.length;
                stats.jobsExtracted += toSave.length;
                log.info(`📄 Page ${pageNum}: +${toSave.length} jobs | Total: ${totalScraped}/${maxJobs}`);
            }

            // Check if we should continue
            if (totalScraped >= maxJobs) {
                log.info('🎯 Target reached!');
                return;
            }

            if (consecutiveEmpty >= 2) {
                log.info('📭 No more results');
                return;
            }

            // Add next page to queue if needed
            const nextPage = pageNum + 1;
            if (nextPage <= CONFIG.MAX_PAGES && totalScraped < maxJobs) {
                const nextUrl = buildSearchUrl(input, nextPage);
                await crawler.addRequests([{
                    url: nextUrl,
                    userData: { pageNum: nextPage },
                }]);
            }
        },

        failedRequestHandler({ request, error }) {
            log.error(`❌ Page ${request.userData.pageNum || 1} failed: ${error.message}`);
        },
    });

    // Start with page 1
    const startUrl = buildSearchUrl(input, 1);
    await crawler.run([{
        url: startUrl,
        userData: { pageNum: 1 },
    }]);

    // Final stats
    const duration = Math.round((Date.now() - stats.startTime) / 1000);
    const speed = totalScraped > 0 ? (duration / totalScraped).toFixed(2) : 'N/A';

    await Actor.setValue('STATISTICS', {
        jobs: totalScraped,
        pages: stats.pagesProcessed,
        duration: `${duration}s`,
        speed: `${speed}s/job`,
    });

    log.info('');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info(`🎉 DONE! ${totalScraped} jobs | ${stats.pagesProcessed} pages | ${duration}s`);
    if (totalScraped > 0) log.info(`⚡ Speed: ${speed}s/job`);
    log.info('═══════════════════════════════════════════════════════════════');

} catch (error) {
    log.exception(error, '❌ Failed');
    throw error;
}

await Actor.exit();
