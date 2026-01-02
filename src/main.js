/**
 * ZipRecruiter Jobs Scraper - Production Ready
 * 
 * Uses PlaywrightCrawler + Camoufox for Cloudflare bypass
 * Browser-based pagination via button clicks (ZipRecruiter uses client-side nav)
 */

import { PlaywrightCrawler } from '@crawlee/playwright';
import { Actor, log } from 'apify';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';

// Initialize the Apify SDK
await Actor.init();

// ============================================================================
// CONFIGURATION
// ============================================================================
const CONFIG = {
    // Timing (ms)
    CLOUDFLARE_BYPASS_WAIT: 8000,
    PAGE_LOAD_WAIT: 3000,
    BETWEEN_PAGES_DELAY: 1500,
    SCROLL_DELAY: 300,

    // Pagination
    MAX_PAGES: 50,
    MAX_CONSECUTIVE_EMPTY: 2,

    // User Agents Pool
    USER_AGENTS: [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:128.0) Gecko/20100101 Firefox/128.0',
    ],
};

// ============================================================================
// STATISTICS
// ============================================================================
const stats = {
    pagesProcessed: 0,
    jobsExtracted: 0,
    jobsSaved: 0,
    duplicatesRemoved: 0,
    errors: [],
    startTime: Date.now(),
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function getRandomUserAgent() {
    return CONFIG.USER_AGENTS[Math.floor(Math.random() * CONFIG.USER_AGENTS.length)];
}

async function randomDelay(minMs, maxMs) {
    const delay = Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
    await new Promise(resolve => setTimeout(resolve, delay));
}

function normalizeUrl(url) {
    if (!url) return '';
    if (url.startsWith('http')) return url;
    if (url.startsWith('/')) return `https://www.ziprecruiter.com${url}`;
    return url;
}

function extractJobId(articleId) {
    if (!articleId) return null;
    const match = articleId.match(/job-card-(.+)/);
    return match ? match[1] : null;
}

function buildSearchUrl(input) {
    if (input.searchUrl?.trim()) {
        return input.searchUrl.trim();
    }

    const baseUrl = 'https://www.ziprecruiter.com/jobs-search';
    const params = new URLSearchParams();

    if (input.searchQuery) params.append('search', input.searchQuery);
    if (input.location) params.append('location', input.location);
    if (input.radius) params.append('radius', input.radius);
    if (input.daysBack && input.daysBack !== 'any') params.append('days', input.daysBack);
    if (input.employmentType?.length > 0) {
        input.employmentType.forEach(type => params.append('employment_type', type));
    }
    if (input.salaryMin) params.append('salary_min', input.salaryMin.toString());
    if (input.remoteOnly) params.append('remote', '1');

    return `${baseUrl}?${params.toString()}`;
}

// ============================================================================
// JOB EXTRACTION - Via page.evaluate (Browser)
// ============================================================================

async function extractJobsFromPage(page) {
    try {
        const jobs = await page.evaluate(() => {
            const results = [];
            const jobCards = document.querySelectorAll('.job_result_two_pane_v2');

            jobCards.forEach((card) => {
                try {
                    const article = card.querySelector('article');
                    const articleId = article?.id || '';
                    const jobIdMatch = articleId.match(/job-card-(.+)/);
                    const jobId = jobIdMatch ? jobIdMatch[1] : null;

                    const titleEl = card.querySelector('h2');
                    const title = titleEl?.getAttribute('aria-label')?.trim() ||
                        titleEl?.textContent?.trim() || '';

                    const companyEl = card.querySelector('[data-testid="job-card-company"]');
                    const company = companyEl?.textContent?.trim() || '';
                    const companyUrl = companyEl?.getAttribute('href') || '';

                    const locationEl = card.querySelector('[data-testid="job-card-location"]');
                    let location = locationEl?.textContent?.trim() || '';
                    const spans = locationEl?.parentElement?.querySelectorAll('span') || [];
                    spans.forEach(span => {
                        if (span.textContent.toLowerCase().includes('remote')) {
                            location += ' (Remote)';
                        }
                    });

                    let salary = 'Not specified';
                    const paragraphs = card.querySelectorAll('p');
                    for (const p of paragraphs) {
                        if (p.textContent.includes('$')) {
                            salary = p.textContent.trim();
                            break;
                        }
                    }

                    let postedDate = '';
                    const cardText = card.textContent || '';
                    const postedMatch = cardText.match(/Posted\s+(\d+\s+\w+\s+ago|today|yesterday|just now)/i);
                    if (postedMatch) postedDate = postedMatch[1];

                    if (title || company) {
                        results.push({
                            title: title || 'Unknown Title',
                            company,
                            companyUrl: companyUrl?.startsWith('/') ?
                                `https://www.ziprecruiter.com${companyUrl}` : (companyUrl || ''),
                            location,
                            salary,
                            jobType: 'Not specified',
                            postedDate,
                            descriptionText: '',
                            descriptionHtml: '',
                            url: jobId ? `https://www.ziprecruiter.com/jobs/${jobId}` : '',
                            applyUrl: jobId ? `https://www.ziprecruiter.com/jobs/${jobId}` : '',
                            jobId: jobId || '',
                            scrapedAt: new Date().toISOString(),
                        });
                    }
                } catch (e) { /* skip */ }
            });

            return results;
        });

        return jobs;
    } catch (error) {
        log.warning(`Browser extraction failed: ${error.message}`);
        return [];
    }
}

// ============================================================================
// PAGINATION - Click "Next Page" Button
// ============================================================================

async function clickNextPage(page) {
    try {
        // Wait for any loading to complete
        await page.waitForTimeout(500);

        // Find and check the Next Page button
        const nextButton = await page.$('button[title="Next Page"]');

        if (!nextButton) {
            log.debug('   Next Page button not found');
            return false;
        }

        // Check if button is disabled
        const isDisabled = await nextButton.evaluate(btn => btn.disabled || btn.hasAttribute('disabled'));
        if (isDisabled) {
            log.debug('   Next Page button is disabled');
            return false;
        }

        // Scroll button into view
        await nextButton.scrollIntoViewIfNeeded();
        await page.waitForTimeout(300);

        // Click the button
        await nextButton.click();
        log.debug('   Clicked Next Page button');

        // Wait for new content to load
        await page.waitForTimeout(CONFIG.PAGE_LOAD_WAIT);

        return true;
    } catch (error) {
        log.warning(`   Failed to click Next Page: ${error.message}`);
        return false;
    }
}

// ============================================================================
// SCROLL JOB LIST
// ============================================================================

async function scrollJobList(page) {
    try {
        await page.evaluate(async () => {
            const container = document.querySelector('.job_results_two_pane');
            if (!container) return;

            // Scroll down incrementally
            for (let i = 0; i < 5; i++) {
                container.scrollTop = container.scrollHeight;
                await new Promise(r => setTimeout(r, 200));
            }
            // Scroll back to top
            container.scrollTop = 0;
        });
    } catch (error) {
        log.debug(`Scroll failed: ${error.message}`);
    }
}

// ============================================================================
// DISMISS POPUPS
// ============================================================================

async function dismissPopups(page) {
    try {
        const closeSelectors = [
            'button[aria-label="Close"]',
            'button[aria-label="close"]',
            '[data-testid="close-button"]',
            '.modal-close',
            'button.close',
        ];

        for (const selector of closeSelectors) {
            const closeBtn = await page.$(selector);
            if (closeBtn) {
                await closeBtn.click();
                await page.waitForTimeout(500);
                log.debug('   Dismissed popup');
            }
        }
    } catch (error) {
        // Ignore popup errors
    }
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

try {
    const input = await Actor.getInput() || {};

    log.info('═══════════════════════════════════════════════════════════════');
    log.info('🚀 ZipRecruiter Jobs Scraper - Browser-Based Pagination');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info('📋 Strategy: Camoufox Browser + Button Click Pagination');
    log.info('───────────────────────────────────────────────────────────────');

    // Validate input
    if (!input.searchUrl?.trim() && !input.searchQuery?.trim() && !input.location?.trim()) {
        throw new Error('Either "searchUrl" OR "searchQuery"/"location" must be provided');
    }

    const maxJobs = input.maxJobs ?? 50;
    if (maxJobs < 0 || maxJobs > 10000) {
        throw new Error('maxJobs must be between 0 and 10000');
    }

    const searchUrl = buildSearchUrl(input);

    log.info(`🔍 Search Query: ${input.searchQuery || 'N/A'}`);
    log.info(`📍 Location: ${input.location || 'N/A'}`);
    log.info(`🎯 Max Jobs: ${maxJobs}`);
    log.info(`🔗 URL: ${searchUrl}`);
    log.info('───────────────────────────────────────────────────────────────');

    // Create proxy configuration
    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || {
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL']
        }
    );

    const proxyUrl = await proxyConfiguration.newUrl();

    // State variables
    const seenJobIds = new Set();
    let totalJobsScraped = 0;

    // ========================================================================
    // PlaywrightCrawler with Camoufox
    // ========================================================================
    log.info('');
    log.info('🌐 Launching Camoufox browser...');
    log.info('───────────────────────────────────────────────────────────────');

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: 1, // We handle pagination manually via button clicks
        maxConcurrency: 1,
        navigationTimeoutSecs: 60,
        requestHandlerTimeoutSecs: 600, // Long timeout for pagination

        // Camoufox configuration with proxy for Cloudflare bypass
        launchContext: {
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: true,
                proxy: proxyUrl,
                geoip: true,
                os: 'windows',
                locale: 'en-US',
                screen: {
                    minWidth: 1280,
                    maxWidth: 1920,
                    minHeight: 720,
                    maxHeight: 1080,
                },
            }),
        },

        preNavigationHooks: [
            async ({ page }) => {
                await page.setExtraHTTPHeaders({
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Upgrade-Insecure-Requests': '1',
                });
            },
        ],

        async requestHandler({ page, request }) {
            log.info(`✓ Browser navigated to: ${request.url}`);

            // Wait for Cloudflare bypass
            log.info(`⏳ Waiting ${CONFIG.CLOUDFLARE_BYPASS_WAIT / 1000}s for Cloudflare bypass...`);
            await page.waitForTimeout(CONFIG.CLOUDFLARE_BYPASS_WAIT);

            // Check page title
            const pageTitle = await page.title();
            log.info(`📝 Page title: ${pageTitle}`);

            if (pageTitle.includes('Just a moment') || pageTitle.includes('Cloudflare')) {
                log.warning('⚠️  Still on Cloudflare page, waiting additional 5s...');
                await page.waitForTimeout(5000);
            }

            // Wait for job results container
            try {
                await page.waitForSelector('.job_result_two_pane_v2', { timeout: 30000 });
                log.info('✓ Job listings container loaded');
            } catch {
                log.warning('⚠️  Job container not found, saving debug info...');
                const html = await page.content();
                await Actor.setValue('DEBUG_PAGE_HTML', html, { contentType: 'text/html' });
                const screenshot = await page.screenshot();
                await Actor.setValue('DEBUG_SCREENSHOT', screenshot, { contentType: 'image/png' });
                return;
            }

            // Dismiss any popups
            await dismissPopups(page);

            // ================================================================
            // PAGINATION LOOP - Using Button Clicks
            // ================================================================
            let pageNumber = 1;
            let consecutiveEmpty = 0;

            while (totalJobsScraped < maxJobs && pageNumber <= CONFIG.MAX_PAGES) {
                log.info('');
                log.info(`📄 Page ${pageNumber}: Extracting jobs...`);

                // Scroll to load all jobs
                await scrollJobList(page);

                // Extract jobs from current page
                const pageJobs = await extractJobsFromPage(page);
                stats.pagesProcessed++;

                log.info(`   Found ${pageJobs.length} jobs`);

                if (pageJobs.length === 0) {
                    consecutiveEmpty++;
                    log.info(`   No jobs on page (consecutive empty: ${consecutiveEmpty})`);

                    if (consecutiveEmpty >= CONFIG.MAX_CONSECUTIVE_EMPTY) {
                        log.info('   Reached end of results');
                        break;
                    }
                } else {
                    consecutiveEmpty = 0;

                    // Deduplicate
                    const uniqueJobs = pageJobs.filter(job => {
                        const key = job.jobId || job.url || `${job.title}-${job.company}`;
                        if (seenJobIds.has(key)) {
                            stats.duplicatesRemoved++;
                            return false;
                        }
                        seenJobIds.add(key);
                        return true;
                    });

                    if (uniqueJobs.length < pageJobs.length) {
                        log.info(`   Removed ${pageJobs.length - uniqueJobs.length} duplicates`);
                    }

                    // Limit to maxJobs
                    const toSave = uniqueJobs.slice(0, Math.max(0, maxJobs - totalJobsScraped));

                    if (toSave.length > 0) {
                        await Actor.pushData(toSave);
                        totalJobsScraped += toSave.length;
                        stats.jobsSaved = totalJobsScraped;
                        log.info(`   ✅ Saved ${toSave.length} jobs | Total: ${totalJobsScraped}/${maxJobs}`);
                    }
                }

                // Check if we've reached the limit
                if (totalJobsScraped >= maxJobs) {
                    log.info(`🎯 Reached target: ${maxJobs} jobs`);
                    break;
                }

                // Try to go to next page via button click
                log.info(`   ➡️  Clicking Next Page...`);
                const hasNextPage = await clickNextPage(page);

                if (!hasNextPage) {
                    log.info('   📭 No more pages available');
                    break;
                }

                // Dismiss any popups that appeared
                await dismissPopups(page);

                // Add delay between pages
                await randomDelay(CONFIG.BETWEEN_PAGES_DELAY, CONFIG.BETWEEN_PAGES_DELAY + 1000);

                pageNumber++;
            }
        },

        failedRequestHandler({ request, error }) {
            log.error(`Browser request failed: ${request.url}`, { error: error.message });
            stats.errors.push({ url: request.url, error: error.message });
        },
    });

    // Run the crawler
    await crawler.run([searchUrl]);

    // ========================================================================
    // FINAL STATISTICS
    // ========================================================================
    const endTime = Date.now();
    const durationSecs = Math.round((endTime - stats.startTime) / 1000);
    const avgTimePerJob = totalJobsScraped > 0 ? (durationSecs / totalJobsScraped).toFixed(2) : 'N/A';

    const finalStats = {
        totalJobsScraped,
        pagesProcessed: stats.pagesProcessed,
        duplicatesRemoved: stats.duplicatesRemoved,
        duration: `${durationSecs} seconds`,
        averageTimePerJob: `${avgTimePerJob}s`,
        errorsCount: stats.errors.length,
        timestamp: new Date().toISOString(),
    };

    await Actor.setValue('STATISTICS', finalStats);

    log.info('');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info('🎉 SCRAPING COMPLETED SUCCESSFULLY!');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info(`📊 Total Jobs Scraped:    ${totalJobsScraped}`);
    log.info(`📄 Pages Processed:       ${stats.pagesProcessed}`);
    log.info(`🔄 Duplicates Removed:    ${stats.duplicatesRemoved}`);
    log.info(`🕐 Duration:              ${durationSecs} seconds`);
    log.info(`⏱️  Avg Time/Job:          ${avgTimePerJob}s`);
    log.info(`❌ Errors:                ${stats.errors.length}`);
    log.info('═══════════════════════════════════════════════════════════════');

    if (totalJobsScraped === 0) {
        log.warning('');
        log.warning('⚠️  No jobs were scraped. Please check:');
        log.warning('   • Search parameters are correct');
        log.warning('   • Check DEBUG_PAGE_HTML in key-value store');
        log.warning('   • Proxy configuration may need adjustment');
    }

} catch (error) {
    log.exception(error, '❌ Actor failed with error');

    await Actor.setValue('ERROR_DETAILS', {
        message: error.message,
        stack: error.stack,
        stats,
        timestamp: new Date().toISOString(),
    });

    throw error;
}

// Exit successfully
await Actor.exit();
