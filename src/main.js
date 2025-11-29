// src/main.js
import { Actor, log } from 'apify';
import { Dataset } from 'crawlee';
import { chromium } from 'playwright';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

const BASE_URL = 'https://www.ziprecruiter.com';
const DEFAULT_RESULTS_WANTED = 20;
const DEFAULT_MAX_PAGES = 5;
const DEFAULT_MAX_DETAIL_CONCURRENCY = 3;

// --- URL BUILDING / PAGINATION ------------------------------------------------

/**
 * Fallback builder when no startUrl is provided.
 * ZipRecruiter does support /jobs-search with search/location params.
 * We still strongly prefer startUrl to keep behaviour aligned with browser.
 */
const buildSearchUrl = (keyword, location, page = 1) => {
    const url = new URL('/jobs-search', BASE_URL);
    if (keyword) url.searchParams.set('search', keyword);
    if (location) url.searchParams.set('location', location);
    if (page > 1) url.searchParams.set('page', String(page));
    return url.href;
};

/**
 * Generic pagination builder: if base URL already has ?page=, overwrite it.
 * Otherwise, add page=<page>.
 */
const buildPageUrlFromStart = (startUrl, page) => {
    if (page === 1) return startUrl;

    const url = new URL(startUrl);
    url.searchParams.set('page', String(page));
    return url.href;
};

// --- FINGERPRINT & STEALTH ----------------------------------------------------

const randomFingerprint = () => {
    const mobile = Math.random() < 0.3;
    if (mobile) {
        return {
            ua: 'Mozilla/5.0 (Linux; Android 14; Pixel 7 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
            viewport: { width: 390, height: 844 },
        };
    }
    return {
        ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        viewport: { width: 1366, height: 768 },
    };
};

const looksBlockedHtml = (html = '') => {
    const lower = html.toLowerCase();
    const markers = [
        'unusual traffic',
        'verify you are a human',
        'are you a robot',
        'access denied',
        'temporary blocked',
        '/captcha/',
    ];
    return markers.some((m) => lower.includes(m));
};

const buildCookieHeaderFromPlaywrightCookies = (cookies) => {
    if (!Array.isArray(cookies) || !cookies.length) return '';
    return cookies
        .map((c) => `${c.name}=${c.value}`)
        .join('; ');
};

// --- PLAYWRIGHT HANDSHAKE (ONE PAGE ONLY) -------------------------------------

const dismissPopupsPlaywright = async (page) => {
    const selectors = [
        'button:has-text("Accept all")',
        'button:has-text("Accept All")',
        'button:has-text("I Accept")',
        '[id*="accept"][type="button"]',
        '[data-testid*="accept"]',
        '.js-accept-consent',
    ];

    for (const selector of selectors) {
        try {
            const btn = page.locator(selector).first();
            if (await btn.isVisible({ timeout: 1000 }).catch(() => false)) {
                await btn.click().catch(() => {});
                await page.waitForTimeout(500);
                break;
            }
        } catch {
            // ignore per selector
        }
    }
};

/**
 * Playwright handshake:
 * - gets HTML for the first page
 * - grabs cookies
 * - sets a realistic UA & viewport
 * This is used once; all subsequent requests are got-scraping.
 */
const doPlaywrightHandshake = async (url) => {
    const fp = randomFingerprint();
    let browser;
    try {
        log.info('Starting Playwright handshake', { url });

        browser = await chromium.launch({
            headless: true,
        });

        const context = await browser.newContext({
            userAgent: fp.ua,
            viewport: fp.viewport,
        });

        // Basic navigator hardening
        await context.addInitScript(() => {
            try {
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
                window.chrome = window.chrome || { runtime: {} };
            } catch {
                // ignore
            }
        });

        const page = await context.newPage();

        // Block heavy resources
        await page.route('**/*', (route) => {
            const type = route.request().resourceType();
            if (['image', 'media', 'font', 'stylesheet'].includes(type)) {
                return route.abort();
            }
            return route.continue();
        });

        await page.goto(url, { waitUntil: 'networkidle', timeout: 45000 }).catch((err) => {
            log.warning('Handshake goto() issue', { message: err.message ?? String(err) });
        });

        await dismissPopupsPlaywright(page);

        // small human-ish activity
        await page.waitForTimeout(1000 + Math.round(Math.random() * 1000));
        try {
            await page.mouse.wheel(0, 400);
        } catch {
            // ignore
        }

        const html = await page.content();
        const cookies = await context.cookies();

        const cookieHeader = buildCookieHeaderFromPlaywrightCookies(cookies);
        const ua = fp.ua;

        await browser.close().catch(() => {});

        log.info('Playwright handshake done', {
            cookieBytes: cookieHeader.length,
            htmlBytes: html.length,
        });

        return {
            html,
            cookieHeader,
            userAgent: ua,
        };
    } catch (err) {
        log.warning('Playwright handshake failed', { error: err.message ?? String(err) });
        if (browser) await browser.close().catch(() => {});
        return {
            html: '',
            cookieHeader: '',
            userAgent: randomFingerprint().ua,
        };
    }
};

// --- HTTP SCRAPING ------------------------------------------------------------

const httpFetchHtml = async ({ url, userAgent, cookieHeader, proxyUrl, timeoutMs = 25000 }) => {
    log.debug('HTTP fetch', { url });

    const res = await gotScraping({
        url,
        proxyUrl,
        timeout: { request: timeoutMs },
        http2: true,
        decompress: true,
        retry: { limit: 1 },
        headers: {
            'user-agent': userAgent,
            accept:
                'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            referer: BASE_URL + '/',
            ...(cookieHeader ? { cookie: cookieHeader } : {}),
        },
    });

    const { statusCode } = res;
    if (statusCode >= 400) {
        throw new Error(`HTTP ${statusCode} for ${url}`);
    }

    const body = res.body?.toString() ?? '';
    if (!body) {
        throw new Error(`Empty body for ${url}`);
    }

    if (looksBlockedHtml(body)) {
        throw new Error(`Blocked on HTTP (captcha/blocked page) for ${url}`);
    }

    return body;
};

// --- LISTING PARSING ----------------------------------------------------------

const normalizeUrl = (href) => {
    if (!href) return null;
    try {
        return new URL(href, BASE_URL).href;
    } catch {
        return null;
    }
};

/**
 * DOM-based listing parser for ZipRecruiter.
 * We try specific card selectors, then fallback to generic h2-based parsing.
 */
const extractJobsFromDom = (html, pageUrl) => {
    const $ = cheerio.load(html);
    $('script,noscript,style').remove();

    const urlObj = new URL(pageUrl);
    const seen = new Set();
    const jobs = [];

    const cardSelectors = [
        'article[data-job-id]',
        'div[data-job-id]',
        '[data-testid="job-card"]',
        '.job_result',
        '.job_result_card',
    ];

    let cards = [];
    for (const sel of cardSelectors) {
        const found = $(sel).toArray();
        if (found.length > 0) {
            cards = found;
            break;
        }
    }

    const pushJob = (jobUrl, title, company, location, salary) => {
        if (!jobUrl || seen.has(jobUrl)) return;
        if (!title || title.length < 2 || title.length > 180) return;

        jobs.push({
            source: 'ziprecruiter',
            listing_page_url: urlObj.href,
            title: title || null,
            company: company || null,
            location: location || null,
            salary: salary || null,
            date_posted: null,
            url: jobUrl,
        });

        seen.add(jobUrl);
    };

    // Prefer real job cards
    if (cards.length) {
        for (const card of cards) {
            const c = $(card);

            const titleEl =
                c.find('a[data-testid="job-title"]').first() ||
                c.find('a[href*="/jobs/"]').first() ||
                c.find('h2 a, h2').first();

            const title = titleEl.text().replace(/\s+/g, ' ').trim();

            let company =
                c.find('[data-testid="company-name"]').first().text().replace(/\s+/g, ' ').trim() ||
                c.find('.job_result_company, .company_name').first().text().replace(/\s+/g, ' ').trim();

            let location =
                c.find('[data-testid="location"]').first().text().replace(/\s+/g, ' ').trim() ||
                c.find('.job_result_location, .location').first().text().replace(/\s+/g, ' ').trim();

            let salary =
                c.find('[data-testid="salary"]').first().text().replace(/\s+/g, ' ').trim() ||
                c.find('.job_result_salary, .salary').first().text().replace(/\s+/g, ' ').trim();

            const href =
                titleEl.attr('href') ||
                c.find('a[href*="/jobs/"], a[href*="/job/"]').first().attr('href');

            const jobUrl = normalizeUrl(href);

            pushJob(jobUrl, title, company, location, salary);
        }
    }

    // Fallback: generic h2 scanning
    if (!jobs.length) {
        $('h2').each((_, el) => {
            const title = $(el).text().replace(/\s+/g, ' ').trim();
            if (!title || title.length < 2 || title.length > 180) return;

            const container = $(el).closest('article,div').length
                ? $(el).closest('article,div')
                : $(el).parent();

            let company =
                container
                    .find('[data-testid="company-name"]')
                    .first()
                    .text()
                    .replace(/\s+/g, ' ')
                    .trim() ||
                container.find('.company, .company_name').first().text().replace(/\s+/g, ' ').trim();

            const text = container.text().replace(/\s+/g, ' ').trim();
            let location = '';
            let salary = '';

            const locMatch = text.match(/in\s+([^·|]+)/i);
            if (locMatch) location = locMatch[1].trim();

            const salMatch = text.match(/\$\s?[\d,.]+[^·|]*/);
            if (salMatch) salary = salMatch[0].trim();

            const href =
                container.find('a[href*="/jobs/"], a[href*="/job/"]').first().attr('href') ||
                $(el).find('a').first().attr('href');

            const jobUrl = normalizeUrl(href);
            pushJob(jobUrl, title, company, location, salary);
        });
    }

    return jobs;
};

// --- DETAIL PAGE PARSING ------------------------------------------------------

const extractJobDetail = (html) => {
    const result = {};

    const $ = cheerio.load(html);

    // JSON-LD pass
    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const jsonText = $(el).text();
            const parsed = JSON.parse(jsonText || '{}');
            const items = Array.isArray(parsed) ? parsed : [parsed];
            for (const item of items) {
                if (item['@type'] === 'JobPosting') {
                    if (item.title && !result.title) {
                        result.title = String(item.title);
                    }
                    if (item.description) {
                        result.description_html = String(item.description);
                        const tmp = cheerio.load(result.description_html);
                        result.description_text = tmp('body').text().replace(/\s+/g, ' ').trim();
                    }
                    if (item.hiringOrganization?.name && !result.company) {
                        result.company = String(item.hiringOrganization.name);
                    }
                    if (item.jobLocation?.address?.addressLocality && !result.location) {
                        const address = item.jobLocation.address;
                        result.location = [address.addressLocality, address.addressRegion, address.addressCountry]
                            .filter(Boolean)
                            .join(', ');
                    }
                    if (item.datePosted) {
                        result.date_posted = new Date(item.datePosted).toISOString();
                    }
                }
            }
        } catch {
            // ignore bad JSON blocks
        }
    });

    // DOM fallbacks
    if (!result.title) {
        const title =
            $('[data-testid="job-title"]').first().text().trim() ||
            $('h1').first().text().trim();
        if (title) result.title = title;
    }

    if (!result.description_html) {
        const descEl =
            $('[data-testid="job-description"]').first() ||
            $('#job_description') ||
            $('.job_description').first() ||
            $('section[role="main"]').first();

        if (descEl && descEl.length) {
            const htmlBody = descEl.html() || '';
            result.description_html = htmlBody;
            const txt = descEl.text().replace(/\s+/g, ' ').trim();
            result.description_text = txt || null;
        }
    }

    if (!result.company) {
        const company =
            $('[data-testid="company-name"]').first().text().trim() ||
            $('.company_name').first().text().trim();
        if (company) result.company = company;
    }

    if (!result.location) {
        const loc =
            $('[data-testid="location"]').first().text().trim() ||
            $('.location').first().text().trim();
        if (loc) result.location = loc;
    }

    return result;
};

// --- CONCURRENCY UTIL ---------------------------------------------------------

const processWithConcurrency = async (items, limit, handler) => {
    const results = new Array(items.length);
    let index = 0;

    const worker = async () => {
        while (true) {
            const i = index++;
            if (i >= items.length) break;
            results[i] = await handler(items[i], i);
        }
    };

    const workers = [];
    const workerCount = Math.min(limit, items.length || 1);
    for (let i = 0; i < workerCount; i++) {
        workers.push(worker());
    }

    await Promise.all(workers);
    return results;
};

// --- MAIN ACTOR ---------------------------------------------------------------

Actor.main(async () => {
    const input = (await Actor.getInput()) ?? {};

    const {
        keyword = '',
        location = '',
        startUrl = '',
        results_wanted = DEFAULT_RESULTS_WANTED,
        max_pages = DEFAULT_MAX_PAGES,
        max_detail_concurrency = DEFAULT_MAX_DETAIL_CONCURRENCY,
        detail_mode = 'full', // 'none' | 'basic' | 'full'
        proxyConfiguration: proxyFromInput,
    } = input;

    const detailMode = ['none', 'basic', 'full'].includes(detail_mode) ? detail_mode : 'full';
    const resultsWanted = Math.max(1, Number(results_wanted) || DEFAULT_RESULTS_WANTED);
    const pagesLimit = Math.max(1, Math.min(Number(max_pages) || DEFAULT_MAX_PAGES, 50));
    const detailConcurrency = Math.max(
        1,
        Math.min(Number(max_detail_concurrency) || DEFAULT_MAX_DETAIL_CONCURRENCY, 8),
    );

    const startUrlClean = (startUrl || '').trim();
    const startUrlToUse =
        startUrlClean || buildSearchUrl(keyword.trim(), location.trim(), 1);

    const stats = {
        startedAt: new Date().toISOString(),
        keyword,
        location,
        startUrl: startUrlClean || null,
        resultsWanted,
        pagesLimit,
        detailMode,
        detailConcurrency,
        handshakeAttempted: false,
        handshakeSucceeded: false,
        pagesFetched: 0,
        pagesFailed: 0,
        pagesBlockedOrCaptcha: 0,
        jobsFromDomFallback: 0,
        jobsSaved: 0,
        detailRequests: 0,
        detailSuccess: 0,
        detailFailed: 0,
        stoppedReason: null,
    };

    log.info('Starting ZipRecruiter job actor', {
        keyword,
        location,
        startUrl: startUrlClean || null,
        resultsWanted,
        pagesLimit,
        detailMode,
        detailConcurrency,
    });

    // Proxy config (for HTTP scraping only)
    const proxyConfiguration = await Actor.createProxyConfiguration(
        proxyFromInput || {
            useApifyProxy: true,
            groups: ['RESIDENTIAL'],
            countryCode: 'GB',
        },
    );

    const handshakeUrl = startUrlToUse;
    stats.handshakeAttempted = true;
    let handshakeHtml = '';
    let handshakeCookieHeader = '';
    let handshakeUserAgent = randomFingerprint().ua;

    const handshakeResult = await doPlaywrightHandshake(handshakeUrl);
    handshakeHtml = handshakeResult.html || '';
    handshakeCookieHeader = handshakeResult.cookieHeader || '';
    handshakeUserAgent = handshakeResult.userAgent || handshakeUserAgent;
    stats.handshakeSucceeded = Boolean(handshakeHtml && handshakeCookieHeader);

    if (!stats.handshakeSucceeded) {
        log.warning('Continuing with HTTP-only mode (handshake did not succeed)');
    }

    let savedCount = 0;

    for (let pageNum = 1; pageNum <= pagesLimit; pageNum++) {
        if (savedCount >= resultsWanted) break;

        const listingUrl = startUrlClean
            ? buildPageUrlFromStart(startUrlToUse, pageNum)
            : buildSearchUrl(keyword.trim(), location.trim(), pageNum);

        let html;
        try {
            // Use handshake HTML for the first page if available
            if (pageNum === 1 && handshakeHtml) {
                html = handshakeHtml;
                log.info('Using HTML from Playwright handshake for page 1');
            } else {
                const proxyUrl = proxyConfiguration ? await proxyConfiguration.newUrl() : undefined;
                html = await httpFetchHtml({
                    url: listingUrl,
                    userAgent: handshakeUserAgent,
                    cookieHeader: handshakeCookieHeader,
                    proxyUrl,
                });
            }
        } catch (err) {
            stats.pagesFailed++;
            const msg = err.message ?? String(err);
            if (msg.includes('Blocked on HTTP')) {
                stats.pagesBlockedOrCaptcha++;
                stats.stoppedReason = 'BLOCKED_ON_LISTING';
                log.warning('Blocked while fetching listing page, stopping pagination', {
                    url: listingUrl,
                    error: msg,
                });
                break;
            }

            log.warning('Error fetching listing page, stopping pagination', {
                url: listingUrl,
                error: msg,
            });
            stats.stoppedReason = 'LISTING_FETCH_ERROR';
            break;
        }

        if (looksBlockedHtml(html)) {
            stats.pagesBlockedOrCaptcha++;
            stats.stoppedReason = 'BLOCKED_HTML_LISTING';
            log.warning('Blocked page HTML detected, stopping pagination', { url: listingUrl });
            break;
        }

        stats.pagesFetched++;

        const jobs = extractJobsFromDom(html, listingUrl);
        stats.jobsFromDomFallback += jobs.length;

        if (!jobs.length) {
            log.warning('No jobs parsed from listing page; stopping pagination.', {
                url: listingUrl,
                pageNum,
            });
            stats.stoppedReason = pageNum === 1 ? 'NO_JOBS_ON_FIRST_PAGE' : 'NO_JOBS_ON_PAGE';
            break;
        }

        log.info(`Parsed ${jobs.length} jobs from listing page`, {
            pageNum,
            url: listingUrl,
        });

        // Decide which jobs get detail fetch based on detailMode
        const jobsToProcess = jobs.slice(0, resultsWanted - savedCount); // cap by remaining target

        const shouldFetchDetail = (index) => {
            if (detailMode === 'none') return false;
            if (detailMode === 'basic') return index < 5; // first few per page
            return true; // full mode
        };

        const proxyUrlForDetails = proxyConfiguration ? await proxyConfiguration.newUrl() : undefined;

        const enrichedJobs = await processWithConcurrency(
            jobsToProcess,
            detailMode === 'none' ? 1 : detailConcurrency,
            async (baseJob, index) => {
                if (!shouldFetchDetail(index)) {
                    return {
                        ...baseJob,
                        description_html: null,
                        description_text: null,
                        keyword_search: keyword || null,
                        location_search: location || null,
                        extracted_at: new Date().toISOString(),
                    };
                }

                if (!baseJob.url) {
                    return {
                        ...baseJob,
                        description_html: null,
                        description_text: null,
                        keyword_search: keyword || null,
                        location_search: location || null,
                        extracted_at: new Date().toISOString(),
                    };
                }

                // slight jitter between detail requests
                await Actor.sleep(50 + Math.floor(Math.random() * 120));

                stats.detailRequests++;
                try {
                    const htmlDetail = await httpFetchHtml({
                        url: baseJob.url,
                        userAgent: handshakeUserAgent,
                        cookieHeader: handshakeCookieHeader,
                        proxyUrl: proxyUrlForDetails,
                        timeoutMs: 30000,
                    });

                    const detail = extractJobDetail(htmlDetail);
                    stats.detailSuccess++;

                    const finalJob = {
                        ...baseJob,
                        title:
                            detail.title &&
                            detail.title.length >= 2 &&
                            detail.title.length <= 200
                                ? detail.title
                                : baseJob.title,
                        company: detail.company || baseJob.company,
                        location: detail.location || baseJob.location,
                        salary: baseJob.salary || null,
                        date_posted: detail.date_posted || baseJob.date_posted || null,
                        description_html: detail.description_html || null,
                        description_text: detail.description_text || null,
                        keyword_search: keyword || null,
                        location_search: location || null,
                        extracted_at: new Date().toISOString(),
                    };

                    return finalJob;
                } catch (err) {
                    stats.detailFailed++;
                    log.debug('Detail fetch failed, keeping listing-only job', {
                        url: baseJob.url,
                        error: err.message ?? String(err),
                    });

                    return {
                        ...baseJob,
                        description_html: null,
                        description_text: null,
                        keyword_search: keyword || null,
                        location_search: location || null,
                        extracted_at: new Date().toISOString(),
                    };
                }
            },
        );

        // Save jobs
        for (const finalJob of enrichedJobs) {
            if (savedCount >= resultsWanted) break;
            await Dataset.pushData(finalJob);
            savedCount++;
            stats.jobsSaved = savedCount;

            log.info(`Saved job ${savedCount}/${resultsWanted}`, {
                title: finalJob.title,
                url: finalJob.url,
            });
        }

        if (savedCount >= resultsWanted) {
            stats.stoppedReason = 'TARGET_RESULTS_REACHED';
            break;
        }

        // Jitter between listing pages
        if (pageNum < pagesLimit && savedCount < resultsWanted) {
            const waitMs = 300 + Math.floor(Math.random() * 400);
            await Actor.sleep(waitMs);
        }
    }

    if (!stats.stoppedReason) {
        stats.stoppedReason = 'PAGES_LIMIT_REACHED';
    }

    log.info('Scraping completed', {
        jobsSaved: savedCount,
        target: resultsWanted,
        stats,
    });

    await Actor.setValue('STATS', stats);
});
