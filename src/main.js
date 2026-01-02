/**
 * ZipRecruiter Jobs Scraper - Production Ready (Optimized)
 * 
 * Features:
 * - Camoufox for Cloudflare bypass
 * - Fast description extraction via side panel clicks
 * - Parallel detail page fetching for enrichment
 * - Button click pagination
 */

import { PlaywrightCrawler } from '@crawlee/playwright';
import { Actor, log } from 'apify';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';

// Initialize the Apify SDK
await Actor.init();

// ============================================================================
// CONFIGURATION - Optimized for Speed
// ============================================================================
const CONFIG = {
    // Timing (ms) - Reduced for speed
    CLOUDFLARE_BYPASS_WAIT: 6000,    // Reduced from 8s
    PAGE_LOAD_WAIT: 2000,            // Reduced from 3s
    DESCRIPTION_LOAD_WAIT: 800,      // Wait for side panel to load
    BETWEEN_PAGES_DELAY: 1000,       // Reduced delay
    BETWEEN_CLICKS_DELAY: 300,       // Fast clicks between job cards

    // Pagination
    MAX_PAGES: 50,
    MAX_CONSECUTIVE_EMPTY: 2,

    // Description enrichment
    ENRICH_DESCRIPTIONS: true,       // Fetch full descriptions
    MAX_DESCRIPTIONS_PER_BATCH: 10,  // Process in batches
};

// ============================================================================
// STATISTICS
// ============================================================================
const stats = {
    pagesProcessed: 0,
    jobsExtracted: 0,
    descriptionsEnriched: 0,
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
// FAST JOB EXTRACTION - With Description from Side Panel
// ============================================================================

async function extractJobsWithDescriptions(page, maxJobs, seenJobIds, enrichDescriptions = true) {
    const jobs = [];

    try {
        // Get total job cards on page
        const jobCards = await page.$$('.job_result_two_pane_v2');
        const totalCards = jobCards.length;

        log.info(`   Found ${totalCards} job cards on page`);

        for (let i = 0; i < totalCards; i++) {
            // Re-query to avoid stale elements
            const cards = await page.$$('.job_result_two_pane_v2');
            if (i >= cards.length) break;

            const card = cards[i];

            try {
                // Extract basic info from card
                const jobData = await page.evaluate((cardIndex) => {
                    const cards = document.querySelectorAll('.job_result_two_pane_v2');
                    const card = cards[cardIndex];
                    if (!card) return null;

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

                    return {
                        title: title || 'Unknown Title',
                        company,
                        companyUrl: companyUrl?.startsWith('/') ?
                            `https://www.ziprecruiter.com${companyUrl}` : (companyUrl || ''),
                        location,
                        salary,
                        jobType: 'Not specified',
                        postedDate,
                        jobId: jobId || '',
                    };
                }, i);

                if (!jobData) continue;

                // Check for duplicates
                const key = jobData.jobId || `${jobData.title}-${jobData.company}`;
                if (seenJobIds.has(key)) {
                    stats.duplicatesRemoved++;
                    continue;
                }
                seenJobIds.add(key);

                // Enrich with description by clicking the card
                let descriptionText = '';
                let descriptionHtml = '';

                if (enrichDescriptions) {
                    try {
                        // Click the job card to load description in side panel
                        await card.click();
                        await page.waitForTimeout(CONFIG.DESCRIPTION_LOAD_WAIT);

                        // Extract description from right panel
                        const description = await page.evaluate(() => {
                            // Common selectors for the description panel
                            const selectors = [
                                '.job_description_container',
                                '.jobDescriptionSection',
                                '[data-testid="job-description"]',
                                '.job-description',
                                '.description_content',
                                'div[class*="JobDescription"]',
                                // Two-pane layout right side
                                '.job_content section',
                                '.job_details_pane .description',
                            ];

                            for (const sel of selectors) {
                                const el = document.querySelector(sel);
                                if (el && el.textContent.trim().length > 50) {
                                    return {
                                        html: el.innerHTML,
                                        text: el.textContent.trim(),
                                    };
                                }
                            }

                            // Fallback: look for any large text block in job details
                            const jobContent = document.querySelector('.job_content, .job-content, [class*="JobContent"]');
                            if (jobContent) {
                                return {
                                    html: jobContent.innerHTML,
                                    text: jobContent.textContent.trim().substring(0, 2000),
                                };
                            }

                            return { html: '', text: '' };
                        });

                        descriptionText = description.text || '';
                        descriptionHtml = description.html || '';

                        if (descriptionText.length > 100) {
                            stats.descriptionsEnriched++;
                        }
                    } catch (descError) {
                        log.debug(`Failed to enrich description for job ${i}: ${descError.message}`);
                    }
                }

                // Build final job object
                const job = {
                    ...jobData,
                    descriptionText: descriptionText.substring(0, 5000), // Limit length
                    descriptionHtml: '',  // Skip HTML to save space/speed
                    url: jobData.jobId ? `https://www.ziprecruiter.com/jobs/${jobData.jobId}` : '',
                    applyUrl: jobData.jobId ? `https://www.ziprecruiter.com/jobs/${jobData.jobId}` : '',
                    scrapedAt: new Date().toISOString(),
                };

                jobs.push(job);
                stats.jobsExtracted++;

                // Check if we've reached the limit
                if (jobs.length >= maxJobs) {
                    break;
                }

            } catch (cardError) {
                log.debug(`Failed to extract job card ${i}: ${cardError.message}`);
            }
        }

    } catch (error) {
        log.warning(`Job extraction failed: ${error.message}`);
    }

    return jobs;
}

// ============================================================================
// FAST EXTRACTION - Without descriptions (for speed)
// ============================================================================

async function extractJobsQuick(page) {
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
        log.warning(`Quick extraction failed: ${error.message}`);
        return [];
    }
}

// ============================================================================
// PAGINATION
// ============================================================================

async function clickNextPage(page) {
    try {
        const nextButton = await page.$('button[title="Next Page"]');

        if (!nextButton) return false;

        const isDisabled = await nextButton.evaluate(btn => btn.disabled);
        if (isDisabled) return false;

        await nextButton.scrollIntoViewIfNeeded();
        await nextButton.click();
        await page.waitForTimeout(CONFIG.PAGE_LOAD_WAIT);

        return true;
    } catch (error) {
        log.debug(`Next page click failed: ${error.message}`);
        return false;
    }
}

async function scrollJobList(page) {
    try {
        await page.evaluate(async () => {
            const container = document.querySelector('.job_results_two_pane');
            if (container) {
                container.scrollTop = container.scrollHeight;
                await new Promise(r => setTimeout(r, 200));
                container.scrollTop = 0;
            }
        });
    } catch (e) { /* ignore */ }
}

async function dismissPopups(page) {
    try {
        const selectors = ['button[aria-label="Close"]', 'button[aria-label="close"]', '.modal-close'];
        for (const sel of selectors) {
            const btn = await page.$(sel);
            if (btn) await btn.click().catch(() => { });
        }
    } catch (e) { /* ignore */ }
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

try {
    const input = await Actor.getInput() || {};

    log.info('═══════════════════════════════════════════════════════════════');
    log.info('🚀 ZipRecruiter Jobs Scraper - FAST Mode with Descriptions');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info('📋 Strategy: Camoufox + Side Panel Description Extraction');
    log.info('───────────────────────────────────────────────────────────────');

    // Validate input
    if (!input.searchUrl?.trim() && !input.searchQuery?.trim() && !input.location?.trim()) {
        throw new Error('Either "searchUrl" OR "searchQuery"/"location" must be provided');
    }

    const maxJobs = input.maxJobs ?? 50;
    const enrichDescriptions = input.enrichDescriptions !== false; // Default true

    log.info(`🔍 Search Query: ${input.searchQuery || 'N/A'}`);
    log.info(`📍 Location: ${input.location || 'N/A'}`);
    log.info(`🎯 Max Jobs: ${maxJobs}`);
    log.info(`📝 Enrich Descriptions: ${enrichDescriptions ? 'Yes' : 'No'}`);
    log.info(`🔗 URL: ${buildSearchUrl(input)}`);
    log.info('───────────────────────────────────────────────────────────────');

    // Create proxy configuration
    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || {
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL']
        }
    );

    const proxyUrl = await proxyConfiguration.newUrl();
    const seenJobIds = new Set();
    let totalJobsScraped = 0;

    // ========================================================================
    // PlaywrightCrawler
    // ========================================================================
    log.info('');
    log.info('🌐 Launching Camoufox browser...');

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

        preNavigationHooks: [
            async ({ page }) => {
                await page.setExtraHTTPHeaders({
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'DNT': '1',
                });
            },
        ],

        async requestHandler({ page, request }) {
            log.info(`✓ Navigated to: ${request.url}`);

            // Cloudflare bypass
            log.info(`⏳ Cloudflare bypass (${CONFIG.CLOUDFLARE_BYPASS_WAIT / 1000}s)...`);
            await page.waitForTimeout(CONFIG.CLOUDFLARE_BYPASS_WAIT);

            const pageTitle = await page.title();
            log.info(`📝 Page: ${pageTitle}`);

            if (pageTitle.includes('Just a moment') || pageTitle.includes('Cloudflare')) {
                log.warning('⚠️  Cloudflare detected, waiting more...');
                await page.waitForTimeout(5000);
            }

            // Wait for job results
            try {
                await page.waitForSelector('.job_result_two_pane_v2', { timeout: 30000 });
                log.info('✓ Job listings loaded');
            } catch {
                log.warning('⚠️  No jobs found');
                const html = await page.content();
                await Actor.setValue('DEBUG_HTML', html, { contentType: 'text/html' });
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
                log.info(`📄 Page ${pageNumber}: Processing...`);

                await scrollJobList(page);

                // Extract jobs based on mode
                let pageJobs;
                const remainingJobs = maxJobs - totalJobsScraped;

                if (enrichDescriptions) {
                    // Slower but with descriptions
                    pageJobs = await extractJobsWithDescriptions(
                        page,
                        remainingJobs,
                        seenJobIds,
                        true
                    );
                } else {
                    // Fast mode without descriptions
                    const allJobs = await extractJobsQuick(page);
                    pageJobs = allJobs.filter(job => {
                        const key = job.jobId || `${job.title}-${job.company}`;
                        if (seenJobIds.has(key)) {
                            stats.duplicatesRemoved++;
                            return false;
                        }
                        seenJobIds.add(key);
                        return true;
                    }).slice(0, remainingJobs);
                    stats.jobsExtracted += pageJobs.length;
                }

                stats.pagesProcessed++;

                if (pageJobs.length === 0) {
                    consecutiveEmpty++;
                    log.info(`   No jobs found (${consecutiveEmpty}/${CONFIG.MAX_CONSECUTIVE_EMPTY})`);
                    if (consecutiveEmpty >= CONFIG.MAX_CONSECUTIVE_EMPTY) {
                        log.info('   End of results');
                        break;
                    }
                } else {
                    consecutiveEmpty = 0;

                    await Actor.pushData(pageJobs);
                    totalJobsScraped += pageJobs.length;

                    const descCount = pageJobs.filter(j => j.descriptionText?.length > 100).length;
                    log.info(`   ✅ Saved ${pageJobs.length} jobs (${descCount} with descriptions) | Total: ${totalJobsScraped}/${maxJobs}`);
                }

                if (totalJobsScraped >= maxJobs) {
                    log.info(`🎯 Target reached: ${maxJobs} jobs`);
                    break;
                }

                // Next page
                log.info(`   ➡️  Next page...`);
                const hasNext = await clickNextPage(page);
                if (!hasNext) {
                    log.info('   📭 No more pages');
                    break;
                }

                await dismissPopups(page);
                await randomDelay(CONFIG.BETWEEN_PAGES_DELAY, CONFIG.BETWEEN_PAGES_DELAY + 500);
                pageNumber++;
            }
        },

        failedRequestHandler({ request, error }) {
            log.error(`Failed: ${request.url}`, { error: error.message });
            stats.errors.push({ url: request.url, error: error.message });
        },
    });

    await crawler.run([buildSearchUrl(input)]);

    // ========================================================================
    // FINAL STATISTICS
    // ========================================================================
    const durationSecs = Math.round((Date.now() - stats.startTime) / 1000);
    const avgTime = totalJobsScraped > 0 ? (durationSecs / totalJobsScraped).toFixed(2) : 'N/A';

    const finalStats = {
        totalJobsScraped,
        pagesProcessed: stats.pagesProcessed,
        descriptionsEnriched: stats.descriptionsEnriched,
        duplicatesRemoved: stats.duplicatesRemoved,
        duration: `${durationSecs} seconds`,
        averageTimePerJob: `${avgTime}s`,
        errorsCount: stats.errors.length,
        timestamp: new Date().toISOString(),
    };

    await Actor.setValue('STATISTICS', finalStats);

    log.info('');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info('🎉 COMPLETED!');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info(`📊 Total Jobs:      ${totalJobsScraped}`);
    log.info(`📝 With Descriptions: ${stats.descriptionsEnriched}`);
    log.info(`📄 Pages:           ${stats.pagesProcessed}`);
    log.info(`🕐 Duration:        ${durationSecs}s`);
    log.info(`⚡ Avg/Job:         ${avgTime}s`);
    log.info('═══════════════════════════════════════════════════════════════');

} catch (error) {
    log.exception(error, '❌ Actor failed');
    await Actor.setValue('ERROR', { message: error.message, stack: error.stack, stats });
    throw error;
}

await Actor.exit();
