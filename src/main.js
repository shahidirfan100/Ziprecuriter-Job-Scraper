/**
 * ZipRecruiter Jobs Scraper - Production Ready
 * Fast listing-only extraction with reliable pagination
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
    PAGE_LOAD_WAIT: 2500,
    SCROLL_WAIT: 300,
    BETWEEN_PAGES_MIN: 500,
    BETWEEN_PAGES_MAX: 1000,
    MAX_PAGES: 100,
    MAX_EMPTY_PAGES: 2,
};

// ============================================================================
// STATISTICS
// ============================================================================
const stats = {
    pagesProcessed: 0,
    jobsExtracted: 0,
    duplicates: 0,
    startTime: Date.now(),
};

// ============================================================================
// URL BUILDER
// ============================================================================

function buildSearchUrl(input) {
    // Priority 1: Direct URL
    if (input.searchUrl?.trim()) {
        return input.searchUrl.trim();
    }

    // Priority 2: Build from parameters
    const params = new URLSearchParams();

    if (input.searchQuery?.trim()) {
        params.append('search', input.searchQuery.trim());
    }

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

async function extractJobs(page) {
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
// PAGINATION - Multiple button selectors
// ============================================================================

async function clickNextPage(page) {
    // Try multiple selectors for Next Page button
    const selectors = [
        'button[title="Next Page"]',
        'button[aria-label="Next Page"]',
        'button:has-text("Next")',
        '[data-testid="next-page"]',
        '.pagination-next',
        'a[title="Next Page"]',
    ];

    for (const selector of selectors) {
        try {
            const button = await page.$(selector);
            if (!button) continue;

            // Check if button is visible and enabled
            const isVisible = await button.isVisible();
            const isDisabled = await button.evaluate(el =>
                el.disabled || el.hasAttribute('disabled') || el.classList.contains('disabled')
            );

            if (!isVisible || isDisabled) {
                log.debug(`Button ${selector} is not clickable`);
                continue;
            }

            // Scroll into view and click
            await button.scrollIntoViewIfNeeded();
            await page.waitForTimeout(200);
            await button.click();

            log.debug(`Clicked: ${selector}`);

            // Wait for content to load
            await page.waitForTimeout(CONFIG.PAGE_LOAD_WAIT);

            // Verify new content loaded (jobs should be present)
            const hasJobs = await page.$('.job_result_two_pane_v2');
            if (hasJobs) {
                return true;
            }
        } catch (e) {
            log.debug(`Selector ${selector} failed: ${e.message}`);
        }
    }

    // Fallback: Try clicking via JavaScript
    try {
        const clicked = await page.evaluate(() => {
            const buttons = document.querySelectorAll('button');
            for (const btn of buttons) {
                const title = btn.getAttribute('title') || '';
                const text = btn.textContent || '';
                if (title.includes('Next') || text.includes('Next')) {
                    if (!btn.disabled) {
                        btn.click();
                        return true;
                    }
                }
            }
            return false;
        });

        if (clicked) {
            await page.waitForTimeout(CONFIG.PAGE_LOAD_WAIT);
            return true;
        }
    } catch (e) {
        log.debug(`JS click failed: ${e.message}`);
    }

    return false;
}

// ============================================================================
// HELPERS
// ============================================================================

async function scrollToLoadAll(page) {
    await page.evaluate(async () => {
        const container = document.querySelector('.job_results_two_pane');
        if (!container) return;

        for (let i = 0; i < 3; i++) {
            container.scrollTop = container.scrollHeight;
            await new Promise(r => setTimeout(r, 200));
        }
        container.scrollTop = 0;
    }).catch(() => { });
}

async function dismissPopups(page) {
    const selectors = ['button[aria-label="Close"]', 'button[aria-label="close"]', '.modal-close'];
    for (const sel of selectors) {
        try {
            const btn = await page.$(sel);
            if (btn) await btn.click();
        } catch { /* ignore */ }
    }
}

const delay = (min, max) => new Promise(r =>
    setTimeout(r, min + Math.random() * (max - min))
);

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
    log.info(`📍 Location: ${input.location || 'Nationwide'}`);
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

            // Wait for jobs
            try {
                await page.waitForSelector('.job_result_two_pane_v2', { timeout: 30000 });
                log.info(`✓ ${title}`);
            } catch {
                log.error('❌ No jobs found on page');
                await Actor.setValue('DEBUG_HTML', await page.content(), { contentType: 'text/html' });
                await Actor.setValue('DEBUG_SCREENSHOT', await page.screenshot(), { contentType: 'image/png' });
                return;
            }

            await dismissPopups(page);

            // Pagination loop
            let pageNum = 1;
            let emptyPages = 0;

            while (totalScraped < maxJobs && pageNum <= CONFIG.MAX_PAGES) {
                // Scroll to load all jobs
                await scrollToLoadAll(page);

                // Extract jobs
                const jobs = await extractJobs(page);

                // Dedupe
                const unique = jobs.filter(job => {
                    const key = job.jobId || `${job.title}-${job.company}`;
                    if (seenIds.has(key)) {
                        stats.duplicates++;
                        return false;
                    }
                    seenIds.add(key);
                    return true;
                });

                stats.pagesProcessed++;

                if (unique.length === 0) {
                    emptyPages++;
                    log.info(`📄 Page ${pageNum}: No new jobs (empty: ${emptyPages}/${CONFIG.MAX_EMPTY_PAGES})`);
                    if (emptyPages >= CONFIG.MAX_EMPTY_PAGES) {
                        log.info('📭 No more results');
                        break;
                    }
                } else {
                    emptyPages = 0;
                    const toSave = unique.slice(0, maxJobs - totalScraped);
                    await Actor.pushData(toSave);
                    totalScraped += toSave.length;
                    stats.jobsExtracted += toSave.length;

                    log.info(`📄 Page ${pageNum}: +${toSave.length} jobs | Total: ${totalScraped}/${maxJobs}`);
                }

                // Check limit
                if (totalScraped >= maxJobs) {
                    log.info('🎯 Target reached!');
                    break;
                }

                // Go to next page
                log.debug('Attempting to go to next page...');
                const hasNext = await clickNextPage(page);

                if (!hasNext) {
                    log.info('📭 No more pages available');
                    break;
                }

                await dismissPopups(page);
                await delay(CONFIG.BETWEEN_PAGES_MIN, CONFIG.BETWEEN_PAGES_MAX);
                pageNum++;
            }
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
        pagesProcessed: stats.pagesProcessed,
        duplicatesRemoved: stats.duplicates,
        durationSeconds: duration,
        avgSecondsPerJob: avgPerJob,
    });

    log.info('');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info(`🎉 DONE! ${totalScraped} jobs | ${stats.pagesProcessed} pages | ${duration}s`);
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
