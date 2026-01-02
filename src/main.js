/**
 * ZipRecruiter Jobs Scraper - Production Ready
 * Uses infinite scroll pagination (not button clicks)
 */

import { PlaywrightCrawler } from '@crawlee/playwright';
import { Actor, log } from 'apify';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';

await Actor.init();

// ============================================================================
// CONFIGURATION
// ============================================================================
const CONFIG = {
    CLOUDFLARE_WAIT: 5000,
    SCROLL_WAIT: 800,           // Wait after each scroll for new jobs to load
    SCROLL_BATCH_SIZE: 20,      // Approx jobs per scroll batch
    MAX_SCROLLS: 100,           // Safety limit
    MAX_STALE_SCROLLS: 3,       // Stop after N scrolls with no new jobs
};

// ============================================================================
// STATISTICS
// ============================================================================
const stats = {
    scrollCount: 0,
    jobsExtracted: 0,
    duplicates: 0,
    startTime: Date.now(),
};

// ============================================================================
// URL BUILDER
// ============================================================================

function buildSearchUrl(input) {
    if (input.searchUrl?.trim()) {
        return input.searchUrl.trim();
    }

    const params = new URLSearchParams();

    if (input.searchQuery?.trim()) {
        params.append('search', input.searchQuery.trim());
    }

    // Use "United States" for nationwide if no location specified
    if (input.location?.trim()) {
        params.append('location', input.location.trim());
    }

    if (input.radius && input.location?.trim()) {
        params.append('radius', input.radius);
    }

    if (input.daysBack && input.daysBack !== 'any') {
        params.append('days', input.daysBack);
    }

    const queryString = params.toString();
    return queryString
        ? `https://www.ziprecruiter.com/jobs-search?${queryString}`
        : 'https://www.ziprecruiter.com/jobs-search';
}

// ============================================================================
// JOB EXTRACTION
// ============================================================================

async function extractAllJobs(page) {
    return page.evaluate(() => {
        const jobs = [];
        const cards = document.querySelectorAll('.job_result_two_pane_v2');

        cards.forEach(card => {
            try {
                const article = card.querySelector('article');
                const jobIdMatch = (article?.id || '').match(/job-card-(.+)/);
                const jobId = jobIdMatch?.[1] || '';

                const titleEl = card.querySelector('h2');
                const title = titleEl?.getAttribute('aria-label')?.trim() ||
                    titleEl?.textContent?.trim() || '';

                const companyEl = card.querySelector('[data-testid="job-card-company"]');
                const company = companyEl?.textContent?.trim() || '';
                let companyUrl = companyEl?.getAttribute('href') || '';
                if (companyUrl?.startsWith('/')) {
                    companyUrl = `https://www.ziprecruiter.com${companyUrl}`;
                }

                const locationEl = card.querySelector('[data-testid="job-card-location"]');
                let location = locationEl?.textContent?.trim() || '';
                const parent = locationEl?.parentElement;
                if (parent) {
                    parent.querySelectorAll('span').forEach(s => {
                        if (s.textContent.toLowerCase().includes('remote')) {
                            location += ' (Remote)';
                        }
                    });
                }

                let salary = 'Not specified';
                card.querySelectorAll('p').forEach(p => {
                    const text = p.textContent || '';
                    if (text.includes('$') && text.match(/\$[\d,]+/)) {
                        salary = text.trim();
                    }
                });

                const postedMatch = (card.textContent || '').match(
                    /Posted\s+(\d+\s+\w+\s+ago|today|yesterday|just now)/i
                );
                const postedDate = postedMatch?.[1] || '';

                if (title || company) {
                    jobs.push({
                        title: title || 'Unknown Title',
                        company,
                        companyUrl,
                        location,
                        salary,
                        jobType: 'Not specified',
                        postedDate,
                        url: jobId ? `https://www.ziprecruiter.com/jobs/${jobId}` : '',
                        jobId,
                        scrapedAt: new Date().toISOString(),
                    });
                }
            } catch (e) { /* skip */ }
        });

        return jobs;
    });
}

// ============================================================================
// INFINITE SCROLL PAGINATION
// ============================================================================

async function scrollToLoadMore(page) {
    return page.evaluate(async () => {
        const container = document.querySelector('section.job_results_two_pane');
        if (!container) return false;

        const previousHeight = container.scrollHeight;
        container.scrollTop = container.scrollHeight;

        // Wait for new content
        await new Promise(r => setTimeout(r, 500));

        // Check if more content loaded
        return container.scrollHeight > previousHeight;
    });
}

async function getJobCount(page) {
    return page.evaluate(() =>
        document.querySelectorAll('.job_result_two_pane_v2').length
    );
}

// ============================================================================
// HELPERS
// ============================================================================

async function dismissPopups(page) {
    const selectors = ['button[aria-label="Close"]', 'button[aria-label="close"]', '.modal-close'];
    for (const sel of selectors) {
        try {
            const btn = await page.$(sel);
            if (btn) await btn.click();
        } catch { /* ignore */ }
    }
}

// ============================================================================
// MAIN
// ============================================================================

try {
    const input = await Actor.getInput() || {};

    log.info('═══════════════════════════════════════════════════════════════');
    log.info('🚀 ZipRecruiter Jobs Scraper');
    log.info('═══════════════════════════════════════════════════════════════');

    // Validate input
    if (!input.searchUrl?.trim() && !input.searchQuery?.trim()) {
        throw new Error('Please provide "searchUrl" OR "searchQuery"');
    }

    const maxJobs = input.maxJobs ?? 50;
    const searchUrl = buildSearchUrl(input);

    log.info(`🔍 Query: ${input.searchQuery || 'N/A'}`);
    log.info(`📍 Location: ${input.location || 'Auto-detect'}`);
    log.info(`🎯 Max Jobs: ${maxJobs}`);
    log.info(`🔗 URL: ${searchUrl}`);
    log.info('───────────────────────────────────────────────────────────────');

    // Proxy
    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] }
    );
    const proxyUrl = await proxyConfiguration.newUrl();

    // State
    const seenIds = new Set();
    let totalScraped = 0;

    // Crawler
    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: 1,
        maxConcurrency: 1,
        navigationTimeoutSecs: 60,
        requestHandlerTimeoutSecs: 600,

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

        async requestHandler({ page }) {
            log.info('✓ Page loaded, waiting for Cloudflare...');
            await page.waitForTimeout(CONFIG.CLOUDFLARE_WAIT);

            const title = await page.title();

            if (title.includes('Just a moment') || title.includes('Cloudflare')) {
                log.warning('⚠️  Cloudflare detected, waiting more...');
                await page.waitForTimeout(5000);
            }

            // Wait for jobs container
            try {
                await page.waitForSelector('section.job_results_two_pane', { timeout: 30000 });
                await page.waitForSelector('.job_result_two_pane_v2', { timeout: 10000 });
                log.info(`✓ ${title}`);
            } catch {
                log.error('❌ No jobs found on page');
                await Actor.setValue('DEBUG_HTML', await page.content(), { contentType: 'text/html' });
                await Actor.setValue('DEBUG_SCREENSHOT', await page.screenshot(), { contentType: 'image/png' });
                return;
            }

            await dismissPopups(page);

            // Initial job count
            let lastJobCount = await getJobCount(page);
            log.info(`📄 Initial: ${lastJobCount} jobs visible`);

            // Infinite scroll loop
            let staleScrolls = 0;
            let scrollNum = 0;

            while (totalScraped < maxJobs && scrollNum < CONFIG.MAX_SCROLLS) {
                scrollNum++;
                stats.scrollCount++;

                // Extract current batch
                const allJobs = await extractAllJobs(page);

                // Dedupe and get new ones only
                const newJobs = allJobs.filter(job => {
                    const key = job.jobId || `${job.title}-${job.company}`;
                    if (seenIds.has(key)) {
                        return false;
                    }
                    seenIds.add(key);
                    return true;
                });

                if (newJobs.length > 0) {
                    const toSave = newJobs.slice(0, maxJobs - totalScraped);
                    await Actor.pushData(toSave);
                    totalScraped += toSave.length;
                    stats.jobsExtracted += toSave.length;

                    log.info(`📄 Scroll ${scrollNum}: +${toSave.length} new jobs | Total: ${totalScraped}/${maxJobs}`);
                    staleScrolls = 0;
                } else {
                    staleScrolls++;
                    log.debug(`Scroll ${scrollNum}: No new jobs (stale: ${staleScrolls})`);
                }

                // Check if we've reached the limit
                if (totalScraped >= maxJobs) {
                    log.info('🎯 Target reached!');
                    break;
                }

                // Check if we're stuck
                if (staleScrolls >= CONFIG.MAX_STALE_SCROLLS) {
                    log.info('📭 No more jobs loading');
                    break;
                }

                // Scroll to load more
                const scrolled = await scrollToLoadMore(page);
                await page.waitForTimeout(CONFIG.SCROLL_WAIT);

                // Check if more jobs loaded
                const currentCount = await getJobCount(page);
                if (currentCount === lastJobCount && !scrolled) {
                    staleScrolls++;
                }
                lastJobCount = currentCount;
            }

            await dismissPopups(page);
        },

        failedRequestHandler({ request, error }) {
            log.error(`❌ Request failed: ${error.message}`);
        },
    });

    await crawler.run([searchUrl]);

    // Final stats
    const duration = Math.round((Date.now() - stats.startTime) / 1000);
    const avgPerJob = totalScraped > 0 ? (duration / totalScraped).toFixed(2) : 'N/A';

    await Actor.setValue('STATISTICS', {
        totalJobsScraped: totalScraped,
        scrollsPerformed: stats.scrollCount,
        durationSeconds: duration,
        avgSecondsPerJob: avgPerJob,
    });

    log.info('');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info(`🎉 DONE! ${totalScraped} jobs | ${stats.scrollCount} scrolls | ${duration}s`);
    if (totalScraped > 0) {
        log.info(`⚡ Speed: ${avgPerJob}s per job`);
    }
    log.info('═══════════════════════════════════════════════════════════════');

} catch (error) {
    log.exception(error, '❌ Actor failed');
    await Actor.setValue('ERROR', {
        message: error.message,
        stack: error.stack,
    });
    throw error;
}

await Actor.exit();
