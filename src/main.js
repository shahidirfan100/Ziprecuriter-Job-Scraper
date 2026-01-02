import { Actor, log } from 'apify';
import { PlaywrightCrawler } from 'crawlee';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';
import * as cheerio from 'cheerio';

await Actor.init();

// ============================================================================
// CONFIGURATION
// ============================================================================
const CONFIG = {
    // Timing
    CLOUDFLARE_WAIT_MS: 8000,
    PAGE_LOAD_WAIT_MS: 3000,
    BETWEEN_PAGES_DELAY_MS: 2000,
    SCROLL_DELAY_MS: 500,

    // Concurrency
    MAX_CONCURRENCY: 1,
    MAX_PAGES: 50,

    // Retry
    MAX_RETRIES: 3,

    // User Agents (rotation pool)
    USER_AGENTS: [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0',
    ],
};

// Statistics tracking
const stats = {
    pagesProcessed: 0,
    jobsExtracted: 0,
    jobsSaved: 0,
    errors: [],
    startTime: Date.now(),
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Get a random user agent from the pool
 */
function getRandomUserAgent() {
    return CONFIG.USER_AGENTS[Math.floor(Math.random() * CONFIG.USER_AGENTS.length)];
}

/**
 * Random delay between min and max milliseconds
 */
async function randomDelay(minMs, maxMs) {
    const delay = Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
    await new Promise(resolve => setTimeout(resolve, delay));
}

/**
 * Build ZipRecruiter search URL from input parameters
 */
function buildSearchUrl(input) {
    if (input.searchUrl?.trim()) {
        return input.searchUrl.trim();
    }

    const baseUrl = 'https://www.ziprecruiter.com/jobs-search';
    const params = new URLSearchParams();

    if (input.searchQuery) {
        params.append('search', input.searchQuery);
    }

    if (input.location) {
        params.append('location', input.location);
    }

    if (input.radius) {
        params.append('radius', input.radius);
    }

    if (input.daysBack && input.daysBack !== 'any') {
        params.append('days', input.daysBack);
    }

    if (input.employmentType && Array.isArray(input.employmentType) && input.employmentType.length > 0) {
        input.employmentType.forEach(type => {
            params.append('employment_type', type);
        });
    }

    if (input.salaryMin) {
        params.append('salary_min', input.salaryMin.toString());
    }

    if (input.remoteOnly) {
        params.append('remote', '1');
    }

    return `${baseUrl}?${params.toString()}`;
}

/**
 * Normalize URL to absolute
 */
function normalizeUrl(url) {
    if (!url) return '';
    if (url.startsWith('http')) return url;
    if (url.startsWith('/')) return `https://www.ziprecruiter.com${url}`;
    return url;
}

/**
 * Strip HTML tags from text
 */
function stripHtml(html) {
    if (!html) return '';
    return html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

/**
 * Extract job ID from article element ID (format: job-card-{id})
 */
function extractJobId(articleId) {
    if (!articleId) return null;
    const match = articleId.match(/job-card-(.+)/);
    return match ? match[1] : null;
}

// ============================================================================
// JOB EXTRACTION FUNCTIONS
// ============================================================================

/**
 * Extract jobs from page HTML using correct ZipRecruiter selectors
 * Based on browser analysis findings
 */
function extractJobsFromHTML(html) {
    const $ = cheerio.load(html);
    const jobs = [];

    // Primary selector: .job_result_two_pane_v2 (discovered via browser analysis)
    const jobCards = $('.job_result_two_pane_v2');

    log.info(`Found ${jobCards.length} job cards with primary selector`);

    jobCards.each((index, element) => {
        try {
            const $card = $(element);
            const $article = $card.find('article').first();

            // Extract job ID from article ID attribute
            const articleId = $article.attr('id') || '';
            const jobId = extractJobId(articleId);

            // Title: h2[aria-label] or h2 text
            const $title = $card.find('h2').first();
            const title = $title.attr('aria-label')?.trim() || $title.text().trim() || '';

            // Company: a[data-testid="job-card-company"]
            const $company = $card.find('[data-testid="job-card-company"]').first();
            const company = $company.text().trim() || '';
            const companyUrl = normalizeUrl($company.attr('href') || '');

            // Location: a[data-testid="job-card-location"] and check for Remote
            const $location = $card.find('[data-testid="job-card-location"]').first();
            let location = $location.text().trim() || '';

            // Check for Remote indicator (span after location)
            const locationParent = $location.parent();
            const remoteSpan = locationParent.find('span').text().trim();
            if (remoteSpan && remoteSpan.toLowerCase().includes('remote')) {
                location = `${location} (Remote)`;
            }

            // Salary: p tag inside div.break-all that contains $ sign
            const $salaryContainer = $card.find('div.break-all p');
            let salary = 'Not specified';
            $salaryContainer.each((_, el) => {
                const text = $(el).text().trim();
                if (text.includes('$')) {
                    salary = text;
                    return false; // break
                }
            });

            // Job URL: button aria-label contains "View {title}"
            const $viewButton = $card.find('button[aria-label^="View "]').first();
            let url = '';
            if (jobId) {
                // Construct URL from job ID
                url = `https://www.ziprecruiter.com/jobs/${jobId}`;
            }

            // Posted date: look for text like "Posted X days ago" or similar
            let postedDate = '';
            const textContent = $card.text();
            const postedMatch = textContent.match(/Posted\s+(\d+\s+\w+\s+ago|today|yesterday)/i);
            if (postedMatch) {
                postedDate = postedMatch[1];
            }

            // Description snippet: look for job snippet/summary text
            const $snippet = $card.find('.job-snippet, .snippet, p[class*="text-"]').first();
            const descriptionText = $snippet.text().trim() || '';

            if (title || company) {
                jobs.push({
                    title: title || 'Unknown Title',
                    company,
                    companyUrl,
                    location,
                    salary,
                    jobType: 'Not specified',
                    postedDate,
                    descriptionText,
                    descriptionHtml: '',
                    url,
                    applyUrl: url,
                    jobId,
                    scrapedAt: new Date().toISOString(),
                });
            }
        } catch (error) {
            log.debug(`Failed to extract job at index ${index}: ${error.message}`);
        }
    });

    return jobs;
}

/**
 * Extract jobs directly from page using JavaScript execution
 * More reliable than HTML parsing for dynamic content
 */
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

                    // Title
                    const titleEl = card.querySelector('h2');
                    const title = titleEl?.getAttribute('aria-label')?.trim() ||
                        titleEl?.textContent?.trim() || '';

                    // Company
                    const companyEl = card.querySelector('[data-testid="job-card-company"]');
                    const company = companyEl?.textContent?.trim() || '';
                    const companyUrl = companyEl?.getAttribute('href') || '';

                    // Location
                    const locationEl = card.querySelector('[data-testid="job-card-location"]');
                    let location = locationEl?.textContent?.trim() || '';

                    // Check for Remote
                    const locationParent = locationEl?.parentElement;
                    const spans = locationParent?.querySelectorAll('span') || [];
                    spans.forEach(span => {
                        if (span.textContent.toLowerCase().includes('remote')) {
                            location += ' (Remote)';
                        }
                    });

                    // Salary - look for p tag with $ sign
                    let salary = 'Not specified';
                    const paragraphs = card.querySelectorAll('p');
                    for (const p of paragraphs) {
                        if (p.textContent.includes('$')) {
                            salary = p.textContent.trim();
                            break;
                        }
                    }

                    // Posted date
                    let postedDate = '';
                    const cardText = card.textContent || '';
                    const postedMatch = cardText.match(/Posted\s+(\d+\s+\w+\s+ago|today|yesterday)/i);
                    if (postedMatch) {
                        postedDate = postedMatch[1];
                    }

                    if (title || company) {
                        results.push({
                            title: title || 'Unknown Title',
                            company,
                            companyUrl: companyUrl.startsWith('/') ?
                                `https://www.ziprecruiter.com${companyUrl}` : companyUrl,
                            location,
                            salary,
                            jobType: 'Not specified',
                            postedDate,
                            descriptionText: '',
                            descriptionHtml: '',
                            url: jobId ? `https://www.ziprecruiter.com/jobs/${jobId}` : '',
                            applyUrl: jobId ? `https://www.ziprecruiter.com/jobs/${jobId}` : '',
                            jobId,
                            scrapedAt: new Date().toISOString(),
                        });
                    }
                } catch (e) {
                    // Skip failed cards
                }
            });

            return results;
        });

        return jobs;
    } catch (error) {
        log.warning(`Failed to extract jobs via page.evaluate: ${error.message}`);
        return [];
    }
}

/**
 * Check if page has next page button and click it
 */
async function goToNextPage(page) {
    try {
        const nextButton = await page.$('button[title="Next Page"]');
        if (!nextButton) {
            log.info('No next page button found');
            return false;
        }

        const isDisabled = await nextButton.getAttribute('disabled');
        if (isDisabled !== null) {
            log.info('Next page button is disabled');
            return false;
        }

        // Scroll to button first
        await nextButton.scrollIntoViewIfNeeded();
        await randomDelay(500, 1000);

        // Click and wait for navigation/content update
        await nextButton.click();
        await randomDelay(CONFIG.PAGE_LOAD_WAIT_MS, CONFIG.PAGE_LOAD_WAIT_MS + 1500);

        log.info('Successfully navigated to next page');
        return true;
    } catch (error) {
        log.warning(`Failed to go to next page: ${error.message}`);
        return false;
    }
}

/**
 * Scroll through the job list to load all visible jobs
 */
async function scrollJobList(page) {
    try {
        await page.evaluate(async () => {
            const container = document.querySelector('.job_results_two_pane');
            if (!container) return;

            // Scroll incrementally
            const scrollStep = 500;
            let currentScroll = 0;
            const maxScroll = container.scrollHeight;

            while (currentScroll < maxScroll) {
                container.scrollTop = currentScroll;
                currentScroll += scrollStep;
                await new Promise(r => setTimeout(r, 200));
            }

            // Scroll to top
            container.scrollTop = 0;
        });
    } catch (error) {
        log.debug(`Scroll failed: ${error.message}`);
    }
}

/**
 * Dismiss any popups that might appear (login, newsletter, etc.)
 */
async function dismissPopups(page) {
    try {
        // Common popup close button selectors
        const closeSelectors = [
            'button[aria-label="Close"]',
            'button[aria-label="close"]',
            '[data-testid="close-button"]',
            '.modal-close',
            '.popup-close',
            'button.close',
        ];

        for (const selector of closeSelectors) {
            const closeBtn = await page.$(selector);
            if (closeBtn) {
                await closeBtn.click();
                await randomDelay(500, 1000);
                log.info('Dismissed popup');
            }
        }
    } catch (error) {
        // Ignore popup dismiss errors
    }
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

try {
    const input = await Actor.getInput() || {};

    log.info('🚀 Starting ZipRecruiter Jobs Scraper (Browser-First Optimized)', {
        searchUrl: input.searchUrl,
        searchQuery: input.searchQuery,
        location: input.location,
        maxJobs: input.maxJobs
    });

    // Validate input
    if (!input.searchUrl?.trim() && !input.searchQuery?.trim() && !input.location?.trim()) {
        throw new Error('Either "searchUrl" OR "searchQuery"/"location" must be provided');
    }

    const maxJobs = input.maxJobs ?? 50;
    if (maxJobs < 0 || maxJobs > 10000) {
        throw new Error('maxJobs must be between 0 and 10000');
    }

    const searchUrl = buildSearchUrl(input);
    log.info(`🔍 Search URL: ${searchUrl}`);

    // Create proxy configuration
    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || {
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL']
        }
    );

    // Deduplication set
    const seenJobIds = new Set();
    let totalJobsScraped = 0;

    // Main crawler using Camoufox for Cloudflare bypass
    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxConcurrency: CONFIG.MAX_CONCURRENCY,
        maxRequestsPerCrawl: CONFIG.MAX_PAGES,
        navigationTimeoutSecs: 60,
        requestHandlerTimeoutSecs: 300,

        launchContext: {
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: true,
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
                // Set random user agent
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
            stats.pagesProcessed++;
            log.info(`\n📄 Processing: ${request.url}`);

            // Wait for Cloudflare challenge to pass
            log.info('⏳ Waiting for Cloudflare challenge to pass...');
            await page.waitForTimeout(CONFIG.CLOUDFLARE_WAIT_MS);

            // Check if Cloudflare is still showing
            const title = await page.title();
            if (title.includes('Just a moment') || title.includes('Cloudflare')) {
                log.warning('⚠️  Still on Cloudflare challenge page, waiting more...');
                await page.waitForTimeout(5000);
            }

            const finalTitle = await page.title();
            log.info(`📝 Page title: ${finalTitle}`);

            // Wait for job results to appear
            try {
                await page.waitForSelector('.job_result_two_pane_v2', { timeout: 30000 });
                log.info('✓ Job results container loaded');
            } catch (error) {
                log.warning('Job results container not found, checking page content...');
                const pageContent = await page.content();
                await Actor.setValue('DEBUG_PAGE_HTML', pageContent, { contentType: 'text/html' });

                // Take screenshot for debugging
                const screenshot = await page.screenshot();
                await Actor.setValue('DEBUG_SCREENSHOT', screenshot, { contentType: 'image/png' });

                throw new Error('Failed to find job results on page');
            }

            // Dismiss any popups
            await dismissPopups(page);

            // Scroll to load all jobs in the list
            await scrollJobList(page);

            // Process pages with pagination
            let pageNumber = 1;
            let consecutiveEmptyPages = 0;

            while (totalJobsScraped < maxJobs) {
                log.info(`\n📑 Processing page ${pageNumber}...`);

                // Extract jobs using JavaScript evaluation (more reliable)
                let pageJobs = await extractJobsFromPage(page);

                // Fallback to HTML parsing if JS extraction failed
                if (pageJobs.length === 0) {
                    log.info('JS extraction returned no jobs, trying HTML parsing...');
                    const html = await page.content();
                    pageJobs = extractJobsFromHTML(html);
                }

                log.info(`Found ${pageJobs.length} jobs on page ${pageNumber}`);

                if (pageJobs.length === 0) {
                    consecutiveEmptyPages++;
                    if (consecutiveEmptyPages >= 2) {
                        log.info('No jobs found on consecutive pages, stopping pagination');
                        break;
                    }
                } else {
                    consecutiveEmptyPages = 0;
                }

                // Deduplicate and limit
                const uniqueJobs = pageJobs.filter(job => {
                    const key = job.jobId || job.url || `${job.title}-${job.company}`;
                    if (seenJobIds.has(key)) {
                        return false;
                    }
                    seenJobIds.add(key);
                    return true;
                });

                if (uniqueJobs.length < pageJobs.length) {
                    log.info(`🔍 Removed ${pageJobs.length - uniqueJobs.length} duplicate jobs`);
                }

                // Limit to maxJobs
                const jobsToSave = uniqueJobs.slice(0, Math.max(0, maxJobs - totalJobsScraped));

                // Save to dataset
                if (jobsToSave.length > 0) {
                    await Actor.pushData(jobsToSave);
                    totalJobsScraped += jobsToSave.length;
                    stats.jobsSaved = totalJobsScraped;
                    log.info(`✅ Saved ${jobsToSave.length} jobs. Total: ${totalJobsScraped}/${maxJobs}`);
                }

                // Check if we've reached the limit
                if (totalJobsScraped >= maxJobs) {
                    log.info(`🎯 Reached maximum jobs limit: ${maxJobs}`);
                    break;
                }

                // Try to go to next page
                const hasNextPage = await goToNextPage(page);
                if (!hasNextPage) {
                    log.info('📭 No more pages to scrape');
                    break;
                }

                pageNumber++;
                await dismissPopups(page);
                await scrollJobList(page);
            }

            stats.jobsExtracted = totalJobsScraped;
        },

        failedRequestHandler({ request, error }) {
            log.error(`Request failed: ${request.url}`, { error: error.message });
            stats.errors.push({
                url: request.url,
                error: error.message,
            });
        },
    });

    // Run the crawler
    await crawler.run([searchUrl]);

    // Calculate final statistics
    const endTime = Date.now();
    const duration = Math.round((endTime - stats.startTime) / 1000);

    const finalStats = {
        totalJobsScraped,
        pagesProcessed: stats.pagesProcessed,
        duration: `${duration} seconds`,
        averageTimePerJob: totalJobsScraped > 0 ? `${(duration / totalJobsScraped).toFixed(2)}s` : 'N/A',
        errorsCount: stats.errors.length,
        timestamp: new Date().toISOString(),
    };

    await Actor.setValue('STATISTICS', finalStats);

    log.info('\n🎉 ============================================');
    log.info('✅ SCRAPING COMPLETED SUCCESSFULLY!');
    log.info('============================================');
    log.info(`📊 Total Jobs Scraped: ${totalJobsScraped}`);
    log.info(`📄 Pages Processed: ${stats.pagesProcessed}`);
    log.info(`🕐 Duration: ${duration} seconds`);
    log.info(`⚡ Average: ${finalStats.averageTimePerJob} per job`);
    log.info('============================================\n');

    if (totalJobsScraped === 0) {
        log.warning('⚠️  No jobs were scraped. Please check:');
        log.warning('  - Search parameters are correct');
        log.warning('  - Check DEBUG_PAGE_HTML and DEBUG_SCREENSHOT in key-value store');
        log.warning('  - Proxy configuration may need adjustment');
    }

} catch (error) {
    log.exception(error, '❌ Actor failed with error');

    await Actor.setValue('ERROR_DETAILS', {
        message: error.message,
        stack: error.stack,
        stats: stats,
        timestamp: new Date().toISOString(),
    });

    throw error;
}

await Actor.exit();
