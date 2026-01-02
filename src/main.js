/**
 * ZipRecruiter Jobs Scraper - Production Ready
 * 
 * Uses PlaywrightCrawler + Camoufox for Cloudflare bypass
 * Intercepts internal API responses for fast, reliable data extraction
 * Falls back to DOM extraction if API interception fails
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

    // Pagination
    MAX_PAGES: 50,
    MAX_CONSECUTIVE_EMPTY: 2,

    // API Interception patterns
    API_PATTERNS: {
        HYDRATE_JOB_CARDS: '/job_services.job_card.api_public.public.api.v1.API/HydrateJobCards',
        LIST_JOB_KEYS: '/job_services.job_card.api_public.public.api.v1.API/ListJobKeys',
    },
};

// ============================================================================
// STATISTICS
// ============================================================================
const stats = {
    pagesProcessed: 0,
    jobsFromAPI: 0,
    jobsFromDOM: 0,
    apiResponsesCaptured: 0,
    duplicatesRemoved: 0,
    errors: [],
    startTime: Date.now(),
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

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

async function randomDelay(minMs, maxMs) {
    const delay = Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
    await new Promise(resolve => setTimeout(resolve, delay));
}

// ============================================================================
// JOB DATA PARSING FROM API RESPONSE
// ============================================================================

function parseJobsFromAPIResponse(data) {
    const jobs = [];

    try {
        // The API response structure may vary, try common patterns
        let jobCards = [];

        if (data.jobCards) {
            jobCards = data.jobCards;
        } else if (data.jobs) {
            jobCards = data.jobs;
        } else if (data.results) {
            jobCards = data.results;
        } else if (Array.isArray(data)) {
            jobCards = data;
        } else if (data.data?.jobCards) {
            jobCards = data.data.jobCards;
        }

        for (const card of jobCards) {
            try {
                const job = {
                    title: card.title || card.jobTitle || card.name || 'Unknown Title',
                    company: card.hiringCompany?.name || card.company || card.companyName || '',
                    companyUrl: normalizeUrl(card.hiringCompany?.url || card.companyUrl || ''),
                    location: card.location || card.formattedLocation || card.city || '',
                    salary: card.salary || card.compensation || card.salaryText || 'Not specified',
                    jobType: card.employmentType || card.jobType || 'Not specified',
                    postedDate: card.postedTime || card.datePosted || card.posted || '',
                    descriptionText: card.snippet || card.description || '',
                    descriptionHtml: card.descriptionHtml || '',
                    url: normalizeUrl(card.jobUrl || card.url || card.saveJobUrl || ''),
                    applyUrl: normalizeUrl(card.applyUrl || card.url || ''),
                    jobId: card.jobId || card.id || card.encryptedId || '',
                    scrapedAt: new Date().toISOString(),
                };

                // Generate URL from jobId if not present
                if (!job.url && job.jobId) {
                    job.url = `https://www.ziprecruiter.com/jobs/${job.jobId}`;
                    job.applyUrl = job.url;
                }

                if (job.title || job.company) {
                    jobs.push(job);
                }
            } catch (e) {
                log.debug(`Failed to parse job card: ${e.message}`);
            }
        }
    } catch (error) {
        log.debug(`Failed to parse API response: ${error.message}`);
    }

    return jobs;
}

// ============================================================================
// DOM-BASED EXTRACTION (Fallback)
// ============================================================================

async function extractJobsFromDOM(page) {
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
        log.warning(`DOM extraction failed: ${error.message}`);
        return [];
    }
}

// ============================================================================
// PAGINATION - Click "Next Page" Button
// ============================================================================

async function clickNextPage(page) {
    try {
        await page.waitForTimeout(500);

        const nextButton = await page.$('button[title="Next Page"]');

        if (!nextButton) {
            log.debug('   Next Page button not found');
            return false;
        }

        const isDisabled = await nextButton.evaluate(btn => btn.disabled || btn.hasAttribute('disabled'));
        if (isDisabled) {
            log.debug('   Next Page button is disabled');
            return false;
        }

        await nextButton.scrollIntoViewIfNeeded();
        await page.waitForTimeout(300);
        await nextButton.click();

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

            for (let i = 0; i < 5; i++) {
                container.scrollTop = container.scrollHeight;
                await new Promise(r => setTimeout(r, 200));
            }
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
            }
        }
    } catch (error) {
        // Ignore
    }
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

try {
    const input = await Actor.getInput() || {};

    log.info('═══════════════════════════════════════════════════════════════');
    log.info('🚀 ZipRecruiter Jobs Scraper - API Interception Mode');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info('📋 Strategy: Camoufox + API Response Interception + DOM Fallback');
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

    // Store for intercepted API data
    const interceptedJobs = [];

    // ========================================================================
    // PlaywrightCrawler with API Interception
    // ========================================================================
    log.info('');
    log.info('🌐 Launching Camoufox browser with API interception...');
    log.info('───────────────────────────────────────────────────────────────');

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
                // Set up API response interception BEFORE navigation
                page.on('response', async (response) => {
                    const url = response.url();

                    // Check if this is a job data API response
                    if (url.includes(CONFIG.API_PATTERNS.HYDRATE_JOB_CARDS) ||
                        url.includes('HydrateJobCards') ||
                        url.includes('jobCards') ||
                        url.includes('/api/jobs')) {

                        try {
                            const contentType = response.headers()['content-type'] || '';
                            if (contentType.includes('application/json') ||
                                contentType.includes('application/grpc-web') ||
                                contentType.includes('text/plain')) {

                                const body = await response.text();

                                // Try to parse as JSON
                                try {
                                    const data = JSON.parse(body);
                                    const jobs = parseJobsFromAPIResponse(data);

                                    if (jobs.length > 0) {
                                        stats.apiResponsesCaptured++;
                                        interceptedJobs.push(...jobs);
                                        log.info(`   📡 API Intercepted: ${jobs.length} jobs from ${url.split('/').pop()}`);
                                    }
                                } catch (jsonError) {
                                    // Not valid JSON, might be protobuf - skip
                                }
                            }
                        } catch (e) {
                            // Response might be already consumed, skip
                        }
                    }
                });

                await page.setExtraHTTPHeaders({
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'DNT': '1',
                });
            },
        ],

        async requestHandler({ page, request }) {
            log.info(`✓ Browser navigated to: ${request.url}`);

            // Wait for Cloudflare bypass
            log.info(`⏳ Waiting ${CONFIG.CLOUDFLARE_BYPASS_WAIT / 1000}s for Cloudflare bypass...`);
            await page.waitForTimeout(CONFIG.CLOUDFLARE_BYPASS_WAIT);

            const pageTitle = await page.title();
            log.info(`📝 Page title: ${pageTitle}`);

            if (pageTitle.includes('Just a moment') || pageTitle.includes('Cloudflare')) {
                log.warning('⚠️  Still on Cloudflare page, waiting additional 5s...');
                await page.waitForTimeout(5000);
            }

            // Wait for job results
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

            await dismissPopups(page);

            // ================================================================
            // PAGINATION LOOP
            // ================================================================
            let pageNumber = 1;
            let consecutiveEmpty = 0;

            while (totalJobsScraped < maxJobs && pageNumber <= CONFIG.MAX_PAGES) {
                log.info('');
                log.info(`📄 Page ${pageNumber}: Extracting jobs...`);

                // Clear intercepted jobs buffer for this page
                interceptedJobs.length = 0;

                // Scroll to trigger any lazy loading / API calls
                await scrollJobList(page);

                // Wait a bit for API responses to be intercepted
                await page.waitForTimeout(1500);

                // Get jobs - prefer API intercepted data, fallback to DOM
                let pageJobs = [];

                if (interceptedJobs.length > 0) {
                    pageJobs = [...interceptedJobs];
                    stats.jobsFromAPI += pageJobs.length;
                    log.info(`   📡 Got ${pageJobs.length} jobs from API interception`);
                } else {
                    pageJobs = await extractJobsFromDOM(page);
                    stats.jobsFromDOM += pageJobs.length;
                    log.info(`   🔍 Got ${pageJobs.length} jobs from DOM extraction`);
                }

                stats.pagesProcessed++;

                if (pageJobs.length === 0) {
                    consecutiveEmpty++;
                    log.info(`   No jobs found (consecutive empty: ${consecutiveEmpty})`);

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
                        log.info(`   ✅ Saved ${toSave.length} jobs | Total: ${totalJobsScraped}/${maxJobs}`);
                    }
                }

                if (totalJobsScraped >= maxJobs) {
                    log.info(`🎯 Reached target: ${maxJobs} jobs`);
                    break;
                }

                // Try to go to next page
                log.info(`   ➡️  Clicking Next Page...`);
                const hasNextPage = await clickNextPage(page);

                if (!hasNextPage) {
                    log.info('   📭 No more pages available');
                    break;
                }

                await dismissPopups(page);
                await randomDelay(CONFIG.BETWEEN_PAGES_DELAY, CONFIG.BETWEEN_PAGES_DELAY + 1000);
                pageNumber++;
            }
        },

        failedRequestHandler({ request, error }) {
            log.error(`Browser request failed: ${request.url}`, { error: error.message });
            stats.errors.push({ url: request.url, error: error.message });
        },
    });

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
        jobsFromAPI: stats.jobsFromAPI,
        jobsFromDOM: stats.jobsFromDOM,
        apiResponsesCaptured: stats.apiResponsesCaptured,
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
    log.info(`📡 Jobs from API:         ${stats.jobsFromAPI}`);
    log.info(`🔍 Jobs from DOM:         ${stats.jobsFromDOM}`);
    log.info(`📦 API Responses:         ${stats.apiResponsesCaptured}`);
    log.info(`🔄 Duplicates Removed:    ${stats.duplicatesRemoved}`);
    log.info(`🕐 Duration:              ${durationSecs} seconds`);
    log.info(`⏱️  Avg Time/Job:          ${avgTimePerJob}s`);
    log.info('═══════════════════════════════════════════════════════════════');

    if (totalJobsScraped === 0) {
        log.warning('');
        log.warning('⚠️  No jobs were scraped. Please check:');
        log.warning('   • Search parameters are correct');
        log.warning('   • Check DEBUG_PAGE_HTML in key-value store');
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

await Actor.exit();
