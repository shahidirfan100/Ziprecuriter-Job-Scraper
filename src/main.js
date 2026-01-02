/**
 * ZipRecruiter Jobs Scraper - Production Ready
 * 
 * Hybrid approach for maximum speed and stealth:
 * - Phase 1: PlaywrightCrawler + Camoufox for Cloudflare bypass
 * - Phase 2: got-scraping/cheerio for fast HTTP pagination
 * - Phase 3: Browser fallback if HTTP gets blocked
 */

import { PlaywrightCrawler } from '@crawlee/playwright';
import { Actor, log } from 'apify';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

// Initialize the Apify SDK
await Actor.init();

// ============================================================================
// CONFIGURATION
// ============================================================================
const CONFIG = {
    // Timing (ms)
    CLOUDFLARE_BYPASS_WAIT: 8000,
    PAGE_CONTENT_WAIT: 3000,
    BETWEEN_REQUESTS_MIN: 800,
    BETWEEN_REQUESTS_MAX: 1500,
    HTTP_REQUEST_TIMEOUT: 20000,

    // Pagination
    JOBS_PER_PAGE: 20,
    MAX_PAGES: 100,
    MAX_CONSECUTIVE_EMPTY: 2,

    // User Agents Pool
    USER_AGENTS: [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0',
    ],
};

// ============================================================================
// STATISTICS
// ============================================================================
const stats = {
    browserPagesLoaded: 0,
    httpRequestsMade: 0,
    jobsExtracted: 0,
    jobsSaved: 0,
    pagesProcessed: 0,
    duplicatesRemoved: 0,
    errors: [],
    extractionMethod: 'HYBRID',
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

function buildPageUrl(baseUrl, pageNumber) {
    const url = new URL(baseUrl);
    if (pageNumber > 1) {
        url.searchParams.set('page', pageNumber.toString());
    }
    return url.toString();
}

// ============================================================================
// JOB EXTRACTION - Cheerio-based (Fast HTML Parsing)
// ============================================================================

function extractJobsFromHTML(html) {
    const $ = cheerio.load(html);
    const jobs = [];

    // Check for Cloudflare block
    const title = $('title').text();
    if (title.includes('Just a moment') || title.includes('Cloudflare') || title.includes('Access denied')) {
        log.warning('⚠️  Cloudflare/Access block detected in HTML');
        return { jobs: [], blocked: true };
    }

    // Primary selector: .job_result_two_pane_v2
    const jobCards = $('.job_result_two_pane_v2');

    if (jobCards.length === 0) {
        const fallbackSelectors = ['article[id^="job-card-"]', '.job_result', '[data-job-id]', '.jobCard'];
        for (const sel of fallbackSelectors) {
            const cards = $(sel);
            if (cards.length > 0) {
                log.debug(`Using fallback selector: ${sel}`);
                cards.each((_, el) => {
                    const job = extractJobFromElement($, $(el));
                    if (job) jobs.push(job);
                });
                break;
            }
        }
    } else {
        jobCards.each((_, element) => {
            const job = extractJobFromElement($, $(element));
            if (job) jobs.push(job);
        });
    }

    return { jobs, blocked: false };
}

function extractJobFromElement($, $card) {
    try {
        const $article = $card.find('article').first();
        const articleId = $article.attr('id') || $card.attr('id') || '';
        const jobId = extractJobId(articleId);

        // Title
        const $title = $card.find('h2').first();
        const title = $title.attr('aria-label')?.trim() || $title.text().trim() || '';

        // Company
        const $company = $card.find('[data-testid="job-card-company"]').first();
        const company = $company.text().trim() || '';
        const companyUrl = normalizeUrl($company.attr('href') || '');

        // Location
        const $location = $card.find('[data-testid="job-card-location"]').first();
        let location = $location.text().trim() || '';
        const locationParent = $location.parent();
        const remoteText = locationParent.find('span').text().trim();
        if (remoteText.toLowerCase().includes('remote')) {
            location = location ? `${location} (Remote)` : 'Remote';
        }

        // Salary
        let salary = 'Not specified';
        const salaryContainers = $card.find('div.break-all p, p:contains("$")');
        salaryContainers.each((_, el) => {
            const text = $(el).text().trim();
            if (text.includes('$') && text.match(/\$[\d,]+/)) {
                salary = text;
                return false;
            }
        });

        // Posted date
        let postedDate = '';
        const cardText = $card.text();
        const postedMatch = cardText.match(/Posted\s+(\d+\s+\w+\s+ago|today|yesterday|just now)/i);
        if (postedMatch) postedDate = postedMatch[1];

        // Description snippet
        let descriptionText = '';
        const snippetSelectors = ['.job-snippet', '.snippet', 'p[class*="text-gray"]'];
        for (const sel of snippetSelectors) {
            const $snippet = $card.find(sel).first();
            if ($snippet.length) {
                descriptionText = $snippet.text().trim();
                if (descriptionText.length > 50) break;
            }
        }

        if (!title && !company) return null;

        return {
            title: title || 'Unknown Title',
            company,
            companyUrl,
            location,
            salary,
            jobType: 'Not specified',
            postedDate,
            descriptionText,
            descriptionHtml: '',
            url: jobId ? `https://www.ziprecruiter.com/jobs/${jobId}` : '',
            applyUrl: jobId ? `https://www.ziprecruiter.com/jobs/${jobId}` : '',
            jobId: jobId || '',
            scrapedAt: new Date().toISOString(),
        };
    } catch (error) {
        log.debug(`Error extracting job: ${error.message}`);
        return null;
    }
}

// ============================================================================
// BROWSER EXTRACTION - Via page.evaluate
// ============================================================================

async function extractJobsViaBrowser(page) {
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

async function getCookiesFromPage(page) {
    try {
        const cookies = await page.context().cookies();
        return cookies.map(c => `${c.name}=${c.value}`).join('; ');
    } catch (error) {
        log.warning(`Failed to get cookies: ${error.message}`);
        return '';
    }
}

// ============================================================================
// HTTP PAGINATION - Fast with got-scraping
// ============================================================================

async function fetchPageViaHTTP(url, cookies, proxyUrl, userAgent) {
    stats.httpRequestsMade++;

    try {
        const response = await gotScraping({
            url,
            proxyUrl,
            timeout: { request: CONFIG.HTTP_REQUEST_TIMEOUT },
            retry: { limit: 2 },
            headers: {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'DNT': '1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': userAgent,
                'Cookie': cookies,
                'Referer': 'https://www.ziprecruiter.com/',
            },
        });

        if (response.statusCode === 403 || response.statusCode === 503) {
            log.warning(`⚠️  HTTP blocked with status ${response.statusCode}`);
            return { html: null, blocked: true };
        }

        if (response.statusCode !== 200) {
            log.warning(`HTTP request returned status ${response.statusCode}`);
            return { html: null, blocked: false };
        }

        return { html: response.body, blocked: false };
    } catch (error) {
        log.warning(`HTTP request failed: ${error.message}`);
        stats.errors.push({ type: 'HTTP', error: error.message, url });
        return { html: null, blocked: false };
    }
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

try {
    const input = await Actor.getInput() || {};

    log.info('═══════════════════════════════════════════════════════════════');
    log.info('🚀 ZipRecruiter Jobs Scraper - HYBRID MODE (Fast & Stealthy)');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info('📋 Strategy: PlaywrightCrawler + Camoufox → HTTP Pagination');
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
    const userAgent = getRandomUserAgent();

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
    let sessionCookies = '';
    let httpBlocked = false;
    let browserPageNumber = 1;

    // ========================================================================
    // PHASE 1: PlaywrightCrawler with Camoufox for Cloudflare Bypass
    // ========================================================================
    log.info('');
    log.info('🌐 PHASE 1: PlaywrightCrawler + Camoufox (Cloudflare Bypass)');
    log.info('───────────────────────────────────────────────────────────────');

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: 1, // Only use browser for first page
        maxConcurrency: 1,
        navigationTimeoutSecs: 60,
        requestHandlerTimeoutSecs: 120,

        // Camoufox launch options with proxy for Cloudflare bypass
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
            stats.browserPagesLoaded++;
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
            }

            // Scroll to load all jobs in viewport
            await page.evaluate(async () => {
                const container = document.querySelector('.job_results_two_pane');
                if (container) {
                    for (let i = 0; i < 5; i++) {
                        container.scrollTop = container.scrollHeight;
                        await new Promise(r => setTimeout(r, 300));
                    }
                    container.scrollTop = 0;
                }
            });

            // Capture session cookies for HTTP pagination
            sessionCookies = await getCookiesFromPage(page);
            log.info(`✓ Session cookies captured (${sessionCookies.length} chars)`);

            // Extract jobs from page 1
            log.info('');
            log.info(`📄 Extracting jobs from page ${browserPageNumber} (browser)...`);

            let pageJobs = await extractJobsViaBrowser(page);

            if (pageJobs.length === 0) {
                log.info('   Browser JS extraction empty, trying HTML parsing...');
                const html = await page.content();
                const result = extractJobsFromHTML(html);
                pageJobs = result.jobs;
            }

            stats.pagesProcessed++;
            log.info(`   ✓ Found ${pageJobs.length} jobs on page ${browserPageNumber}`);

            // Deduplicate and save
            const uniqueJobs = pageJobs.filter(job => {
                const key = job.jobId || job.url || `${job.title}-${job.company}`;
                if (seenJobIds.has(key)) {
                    stats.duplicatesRemoved++;
                    return false;
                }
                seenJobIds.add(key);
                return true;
            });

            const toSave = uniqueJobs.slice(0, Math.max(0, maxJobs - totalJobsScraped));
            if (toSave.length > 0) {
                await Actor.pushData(toSave);
                totalJobsScraped += toSave.length;
                stats.jobsSaved = totalJobsScraped;
                log.info(`   ✅ Saved ${toSave.length} jobs | Total: ${totalJobsScraped}/${maxJobs}`);
            }

            browserPageNumber++;
        },

        failedRequestHandler({ request, error }) {
            log.error(`Browser request failed: ${request.url}`, { error: error.message });
            stats.errors.push({ type: 'BROWSER', url: request.url, error: error.message });
        },
    });

    // Run crawler for first page only
    await crawler.run([searchUrl]);
    log.info('✓ Browser phase completed (switching to HTTP mode)');

    // ========================================================================
    // PHASE 2: HTTP Pagination with got-scraping (Fast)
    // ========================================================================
    if (totalJobsScraped < maxJobs && sessionCookies) {
        log.info('');
        log.info('⚡ PHASE 2: Fast HTTP pagination with captured cookies...');
        log.info('───────────────────────────────────────────────────────────────');

        let pageNumber = 2;
        let consecutiveEmpty = 0;

        while (totalJobsScraped < maxJobs && pageNumber <= CONFIG.MAX_PAGES) {
            const pageUrl = buildPageUrl(searchUrl, pageNumber);
            log.info(`📄 Page ${pageNumber}: Fetching via HTTP...`);

            // Random delay between requests
            await randomDelay(CONFIG.BETWEEN_REQUESTS_MIN, CONFIG.BETWEEN_REQUESTS_MAX);

            const { html, blocked } = await fetchPageViaHTTP(pageUrl, sessionCookies, proxyUrl, userAgent);

            if (blocked) {
                log.warning('⚠️  HTTP requests blocked, session may have expired');
                httpBlocked = true;
                break;
            }

            if (!html) {
                consecutiveEmpty++;
                if (consecutiveEmpty >= CONFIG.MAX_CONSECUTIVE_EMPTY) {
                    log.info(`   No more pages (${consecutiveEmpty} consecutive empty)`);
                    break;
                }
                pageNumber++;
                continue;
            }

            const { jobs, blocked: htmlBlocked } = extractJobsFromHTML(html);

            if (htmlBlocked) {
                log.warning('⚠️  Cloudflare block detected in HTML response');
                httpBlocked = true;
                break;
            }

            stats.pagesProcessed++;

            if (jobs.length === 0) {
                consecutiveEmpty++;
                log.info(`   Page ${pageNumber}: No jobs found (consecutive empty: ${consecutiveEmpty})`);
                if (consecutiveEmpty >= CONFIG.MAX_CONSECUTIVE_EMPTY) {
                    log.info('   Reached end of results');
                    break;
                }
            } else {
                consecutiveEmpty = 0;

                const uniqueJobs = jobs.filter(job => {
                    const key = job.jobId || job.url || `${job.title}-${job.company}`;
                    if (seenJobIds.has(key)) {
                        stats.duplicatesRemoved++;
                        return false;
                    }
                    seenJobIds.add(key);
                    return true;
                });

                const toSave = uniqueJobs.slice(0, Math.max(0, maxJobs - totalJobsScraped));

                if (toSave.length > 0) {
                    await Actor.pushData(toSave);
                    totalJobsScraped += toSave.length;
                    stats.jobsSaved = totalJobsScraped;

                    const dupes = jobs.length - uniqueJobs.length;
                    log.info(`   ✅ Page ${pageNumber}: +${toSave.length} jobs${dupes > 0 ? ` (${dupes} dupes)` : ''} | Total: ${totalJobsScraped}/${maxJobs}`);
                }
            }

            if (totalJobsScraped >= maxJobs) {
                log.info(`🎯 Reached target: ${maxJobs} jobs`);
                break;
            }

            pageNumber++;
        }
    }

    // ========================================================================
    // PHASE 3: Browser Fallback (if HTTP was blocked mid-scrape)
    // ========================================================================
    if (httpBlocked && totalJobsScraped < maxJobs) {
        log.info('');
        log.info('🔄 PHASE 3: Fallback - Resuming with PlaywrightCrawler...');
        log.info('───────────────────────────────────────────────────────────────');

        const startPage = stats.pagesProcessed + 1;
        const remainingUrls = [];

        for (let p = startPage; p <= Math.min(startPage + 10, CONFIG.MAX_PAGES); p++) {
            if (totalJobsScraped + (p - startPage) * CONFIG.JOBS_PER_PAGE >= maxJobs) break;
            remainingUrls.push(buildPageUrl(searchUrl, p));
        }

        if (remainingUrls.length > 0) {
            const fallbackCrawler = new PlaywrightCrawler({
                proxyConfiguration,
                maxConcurrency: 1,
                navigationTimeoutSecs: 45,
                requestHandlerTimeoutSecs: 90,

                launchContext: {
                    launcher: firefox,
                    launchOptions: await camoufoxLaunchOptions({
                        headless: true,
                        proxy: proxyUrl,
                        geoip: true,
                        os: 'windows',
                        locale: 'en-US',
                    }),
                },

                async requestHandler({ page, request }) {
                    if (totalJobsScraped >= maxJobs) return;

                    stats.browserPagesLoaded++;
                    const pageNum = new URL(request.url).searchParams.get('page') || '1';
                    log.info(`📄 Page ${pageNum}: Loading via browser fallback...`);

                    await page.waitForTimeout(CONFIG.PAGE_CONTENT_WAIT);

                    const jobs = await extractJobsViaBrowser(page);
                    stats.pagesProcessed++;

                    if (jobs.length === 0) {
                        log.info(`   Page ${pageNum}: No jobs found`);
                        return;
                    }

                    const uniqueJobs = jobs.filter(job => {
                        const key = job.jobId || job.url || `${job.title}-${job.company}`;
                        if (seenJobIds.has(key)) return false;
                        seenJobIds.add(key);
                        return true;
                    });

                    const toSave = uniqueJobs.slice(0, Math.max(0, maxJobs - totalJobsScraped));
                    if (toSave.length > 0) {
                        await Actor.pushData(toSave);
                        totalJobsScraped += toSave.length;
                        stats.jobsSaved = totalJobsScraped;
                        log.info(`   ✅ Page ${pageNum}: +${toSave.length} jobs | Total: ${totalJobsScraped}/${maxJobs}`);
                    }
                },
            });

            await fallbackCrawler.run(remainingUrls);
        }
    }

    // ========================================================================
    // FINAL STATISTICS
    // ========================================================================
    const endTime = Date.now();
    const durationSecs = Math.round((endTime - stats.startTime) / 1000);
    const avgTimePerJob = totalJobsScraped > 0 ? (durationSecs / totalJobsScraped).toFixed(2) : 'N/A';

    const finalStats = {
        totalJobsScraped,
        pagesProcessed: stats.pagesProcessed,
        browserPagesLoaded: stats.browserPagesLoaded,
        httpRequestsMade: stats.httpRequestsMade,
        duplicatesRemoved: stats.duplicatesRemoved,
        extractionMethod: stats.extractionMethod,
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
    log.info(`🌐 Browser Pages:         ${stats.browserPagesLoaded}`);
    log.info(`⚡ HTTP Requests:         ${stats.httpRequestsMade}`);
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
