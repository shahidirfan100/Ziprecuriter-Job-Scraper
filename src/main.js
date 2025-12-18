// main.js
import { Actor, log } from 'apify';
import { Dataset, playwright } from 'crawlee';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

const { chromium } = playwright;

const BASE_URL = 'https://www.ziprecruiter.com';
const DEFAULT_RESULTS_WANTED = 20;
const DEFAULT_MAX_PAGES = 5;
const DEFAULT_MAX_DETAIL_CONCURRENCY = 3;

// --- URL BUILDING / PAGINATION ------------------------------------------------

/**
 * Fallback builder when no startUrl is provided.
 * ZipRecruiter supports /jobs-search with search/location params.
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

const buildJsonPageUrlFromSource = (sourceUrl, page) => {
    try {
        const url = new URL(sourceUrl);
        const pageKeys = ['page', 'pageNum', 'page_number', 'pageNumber', 'pn', 'p'];
        let foundKey = null;
        for (const key of pageKeys) {
            if (url.searchParams.has(key)) {
                foundKey = key;
                break;
            }
        }
        if (!foundKey) {
            // If no page param exists, add standard page
            foundKey = 'page';
        }
        url.searchParams.set(foundKey, String(page));
        return url.href;
    } catch {
        return null;
    }
};

// --- FINGERPRINT & STEALTH ----------------------------------------------------

const randomFingerprint = () => {
    const mobile = Math.random() < 0.2; // Lower mobile probability
    if (mobile) {
        return {
            ua: 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1',
            viewport: { width: 390, height: 844 },
        };
    }
    // Use recent Chrome versions on Windows
    const chromeVersions = ['120.0.0.0', '121.0.0.0', '122.0.0.0', '123.0.0.0', '124.0.0.0'];
    const version = chromeVersions[Math.floor(Math.random() * chromeVersions.length)];
    return {
        ua: `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${version} Safari/537.36`,
        viewport: { width: 1920, height: 1080 },
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
        'just a moment',
        'checking your browser',
        'enable javascript and cookies',
        'cloudflare',
        'cf-browser-verification',
        'challenge-running',
    ];
    return markers.some((m) => lower.includes(m));
};

const retryWithBackoff = async (fn, { attempts = 3, startMs = 600, factor = 1.6 } = {}) => {
    let lastErr;
    for (let i = 0; i < attempts; i++) {
        try {
            return await fn();
        } catch (err) {
            lastErr = err;
            if (i === attempts - 1) break;
            const wait = Math.round(startMs * Math.pow(factor, i) * (1 + Math.random() * 0.3));
            await Actor.sleep(wait);
        }
    }
    throw lastErr;
};

const normalizeUrl = (href) => {
    if (!href) return null;
    try {
        return new URL(href, BASE_URL).href;
    } catch {
        return null;
    }
};

const safeParseJson = (text) => {
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
};

const extractJobsFromJsonPayload = (payload, pageUrl) => {
    if (!payload) return [];
    const jobs = [];
    const seen = new Set();
    const pushJob = (jobUrl, title, company, location, salary, datePosted) => {
        const normalized = normalizeUrl(jobUrl);
        if (!normalized || seen.has(normalized)) return;
        if (!title || title.length < 2 || title.length > 200) return;

        jobs.push({
            source: 'ziprecruiter',
            listing_page_url: pageUrl,
            title,
            company: company || null,
            location: location || null,
            salary: salary || null,
            date_posted: datePosted || null,
            url: normalized,
        });
        seen.add(normalized);
    };

    const walk = (node) => {
        if (!node) return;
        if (Array.isArray(node)) {
            // If array looks like jobs, try to extract directly
            if (node.length && node.every((item) => typeof item === 'object')) {
                for (const item of node) {
                    const title = item.title || item.job_title || item.name || item.position;
                    const company =
                        item.company ||
                        item.company_name ||
                        item.employer ||
                        item.hiringOrganization?.name;
                    const location =
                        item.location ||
                        item.city ||
                        item.region ||
                        item.jobLocation?.address?.addressLocality ||
                        item.jobLocation?.address?.addressRegion;
                    const salary =
                        item.salary ||
                        item.compensation ||
                        item.pay ||
                        item.estimated_salary ||
                        item.salary_range;
                    const datePosted = item.date_posted || item.datePosted || item.posted_at;
                    const url =
                        item.url ||
                        item.job_url ||
                        item.jobUrl ||
                        item.href ||
                        item.absolute_url ||
                        item.detailUrl;

                    if (url && title) {
                        pushJob(url, String(title), company ? String(company) : null, location ? String(location) : null, salary ? String(salary) : null, datePosted ? new Date(datePosted).toISOString() : null);
                        continue;
                    }
                }
            }
            for (const child of node) walk(child);
        } else if (typeof node === 'object') {
            for (const value of Object.values(node)) {
                walk(value);
            }
        }
    };

    walk(payload);
    return jobs;
};

const extractJobsFromEmbeddedJson = (html, pageUrl) => {
    const $ = cheerio.load(html);
    const jobs = [];
    const seen = new Set();

    const pushJob = (job) => {
        const url = normalizeUrl(job.url);
        if (!url || seen.has(url)) return;
        if (!job.title || job.title.length < 2 || job.title.length > 200) return;
        jobs.push({
            source: 'ziprecruiter',
            listing_page_url: pageUrl,
            title: job.title,
            company: job.company || null,
            location: job.location || null,
            salary: job.salary || null,
            date_posted: job.date_posted || null,
            url,
        });
        seen.add(url);
    };

    // JSON-LD JobPosting blocks
    $('script[type="application/ld+json"]').each((_, el) => {
        const parsed = safeParseJson($(el).text());
        const items = Array.isArray(parsed) ? parsed : [parsed];
        for (const item of items) {
            if (item && item['@type'] === 'JobPosting') {
                pushJob({
                    title: item.title,
                    company: item.hiringOrganization?.name,
                    location:
                        item.jobLocation?.address?.addressLocality ||
                        item.jobLocation?.address?.addressRegion ||
                        item.jobLocation?.address?.addressCountry,
                    salary: item.baseSalary?.value?.value || item.baseSalary?.value?.minValue,
                    date_posted: item.datePosted ? new Date(item.datePosted).toISOString() : null,
                    url: item.url || item.title?.url || item.directApplyLink,
                });
            }
        }
    });

    // Generic embedded JSON (Next.js / app state)
    const scriptCandidates = $(
        'script[id="__NEXT_DATA__"], script[data-json], script[type="application/json"], script:not([src])',
    ).toArray();

    for (const el of scriptCandidates) {
        const text = $(el).text();
        if (!text || text.length < 20) continue;
        const parsed = safeParseJson(text);
        if (!parsed) continue;
        const extracted = extractJobsFromJsonPayload(parsed, pageUrl);
        for (const job of extracted) pushJob(job);
        if (jobs.length) break; // stop early if we already found jobs
    }

    return jobs;
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

// Try to close/skip opt-in / overlays / modals that block job list.
const handleOptInFlow = async (page) => {
    const skipSelectors = [
        'button:has-text("Skip")',
        'button:has-text("No thanks")',
        'button:has-text("Not now")',
        'button:has-text("Maybe later")',
        'button:has-text("Continue without")',
        'button:has-text("Continue as guest")',
        '[data-testid*="skip"]',
        '[data-testid*="dismiss"]',
        '[data-testid*="close"]',
        '[aria-label*="close"]',
        '.modal-close',
        '.close',
        '.close-button',
    ];

    for (const selector of skipSelectors) {
        try {
            const btn = page.locator(selector).first();
            if (await btn.isVisible({ timeout: 1000 }).catch(() => false)) {
                await btn.click().catch(() => {});
                await page.waitForTimeout(800);
            }
        } catch {
            // ignore individual selector failures
        }
    }
};

/**
 * Playwright handshake:
 * - gets HTML for the first page
 * - grabs cookies
 * - sets a realistic UA & viewport
 * - extracts initial job anchors (URL + title) from live DOM
 * - captures JSON API responses if present (preferred)
 * This is used once; all subsequent listing + detail pages are got-scraping.
 */
const doPlaywrightHandshake = async (url, proxyConfiguration, sessionId) => {
    const fp = randomFingerprint();
    let browser;
    const jsonApiJobs = [];
    const jsonApiSources = [];
    try {
        log.info('Starting Playwright handshake', { url });

        const proxyUrl = proxyConfiguration
            ? await proxyConfiguration.newUrl({ session: sessionId })
            : undefined;

        browser = await chromium.launch({
            headless: true,
            proxy: proxyUrl ? { server: proxyUrl } : undefined,
            args: [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-features=BlockInsecurePrivateNetworkRequests',
                '--disable-infobars',
            ],
        });

        const context = await browser.newContext({
            userAgent: fp.ua,
            viewport: fp.viewport,
            locale: 'en-US',
            timezoneId: 'America/New_York',
            permissions: ['geolocation'],
            geolocation: { longitude: -122.4194, latitude: 37.7749 },
            bypassCSP: true,
            proxy: proxyUrl ? { server: proxyUrl } : undefined,
        });

        // Enhanced navigator hardening to bypass Cloudflare
        await context.addInitScript(() => {
            try {
                // Hide webdriver
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                
                Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
                Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
                Object.defineProperty(navigator, 'maxTouchPoints', { get: () => 0 });
                Object.defineProperty(navigator, 'doNotTrack', { get: () => '1' });

                // Fake plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [
                        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                        { name: 'Native Client', filename: 'internal-nacl-plugin' },
                    ],
                });
                
                // Fake languages
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
                Object.defineProperty(navigator, 'connection', {
                    get: () => ({ effectiveType: '4g', rtt: 50, downlink: 10 }),
                });
                
                // Fake chrome runtime
                window.chrome = {
                    runtime: {},
                    loadTimes: () => ({}),
                    csi: () => ({}),
                    app: {},
                };
                
                // Override permissions query
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) =>
                    parameters.name === 'notifications'
                        ? Promise.resolve({ state: Notification.permission })
                        : originalQuery(parameters);
            } catch {
                // ignore
            }
        });

        const page = await context.newPage();

        // Capture JSON API responses early (preferred data source)
        page.on('response', async (response) => {
            try {
                const headers = response.headers() || {};
                const ct = (headers['content-type'] || headers['Content-Type'] || '').toLowerCase();
                if (!ct.includes('json')) return;
                if (jsonApiSources.length >= 4) return; // keep it light

                const bodyText = await response.text();
                if (!bodyText || bodyText.length > 900000) return; // avoid very large payloads
                const parsed = safeParseJson(bodyText);
                if (!parsed) return;

                const urlFromResponse = response.url();
                const extracted = extractJobsFromJsonPayload(parsed, urlFromResponse);
                if (extracted.length) {
                    jsonApiJobs.push(...extracted);
                    jsonApiSources.push({ url: urlFromResponse });
                }
            } catch {
                // ignore noisy errors from response parsing
            }
        });

        // Block heavy resources but allow scripts (needed for Cloudflare)
        await page.route('**/*', (route) => {
            const type = route.request().resourceType();
            if (['image', 'media', 'font'].includes(type)) {
                return route.abort();
            }
            return route.continue();
        });

        // Navigate and wait for network to settle (important for Cloudflare)
        await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 }).catch((err) => {
            log.warning('Handshake goto() issue', { message: err.message ?? String(err) });
        });

        // Check if we're on Cloudflare challenge page and wait for it to resolve
        let attempts = 0;
        const maxAttempts = 10;
        while (attempts < maxAttempts) {
            const pageTitle = await page.title();
            const pageContent = await page.content();
            
            if (pageTitle.toLowerCase().includes('just a moment') || 
                pageContent.toLowerCase().includes('checking your browser') ||
                pageContent.toLowerCase().includes('cf-browser-verification')) {
                log.info('Cloudflare challenge detected, waiting...', { attempt: attempts + 1 });
                await page.waitForTimeout(3000);
                attempts++;
            } else {
                break;
            }
        }
        
        if (attempts >= maxAttempts) {
            log.warning('Cloudflare challenge did not resolve after maximum attempts');
        }

        await dismissPopupsPlaywright(page);
        await handleOptInFlow(page);

        // Wait explicitly for something that looks like job results or job links
        try {
            await page.waitForSelector(
                [
                    'article[data-job-id]',
                    'div[data-job-id]',
                    '[data-testid="job-card"]',
                    '.job_result',
                    '.job_result_card',
                    'section[role="list"] article',
                    'a[href*="/jobs/"]',
                    'a[href*="/job/"]',
                ].join(','),
                { timeout: 20000 },
            );
        } catch {
            log.debug('Handshake: job selector not found before timeout');
        }

        // small human-ish activity
        await page.waitForTimeout(1000 + Math.round(Math.random() * 1000));
        try {
            await page.mouse.wheel(0, 400);
        } catch {
            // ignore
        }

        // Extract job links directly from DOM (URL + title only; rest from detail pages)
        const initialJobs = await page
            .evaluate(() => {
                const anchors = Array.from(
                    document.querySelectorAll('a[href*="/jobs/"], a[href*="/job/"]'),
                );
                const seen = new Set();
                const jobs = [];

                for (const a of anchors) {
                    const hrefRaw = a.getAttribute('href') || '';
                    if (!hrefRaw) continue;

                    // Ignore search / filters / nav links
                    if (hrefRaw.includes('/jobs-search')) continue;
                    if (hrefRaw.startsWith('#')) continue;

                    let fullUrl;
                    try {
                        fullUrl = new URL(hrefRaw, window.location.origin).href;
                    } catch {
                        continue;
                    }

                    if (seen.has(fullUrl)) continue;
                    const title = (a.textContent || '').replace(/\s+/g, ' ').trim();
                    if (!title || title.length < 2 || title.length > 160) continue;

                    // avoid obvious nav/header/footer links
                    const inNavOrFooter = !!a.closest('header,nav,footer');
                    if (inNavOrFooter) continue;

                    jobs.push({
                        source: 'ziprecruiter',
                        listing_page_url: window.location.href,
                        title,
                        company: null,
                        location: null,
                        salary: null,
                        date_posted: null,
                        url: fullUrl,
                    });
                    seen.add(fullUrl);
                }

                return jobs;
            })
            .catch(() => []);

        const html = await page.content();
        const cookies = await context.cookies();
        const cookieHeader = buildCookieHeaderFromPlaywrightCookies(cookies);
        const ua = fp.ua;
        const storageState = await context.storageState().catch(() => null);

        await browser.close().catch(() => {});

        const initialJobsCombined = jsonApiJobs.length ? jsonApiJobs : initialJobs;

        log.info('Playwright handshake done', {
            cookieBytes: cookieHeader.length,
            htmlBytes: html.length,
            initialJobs: initialJobsCombined.length,
            jsonApiHits: jsonApiJobs.length,
        });

        return {
            html,
            cookieHeader,
            userAgent: ua,
            initialJobs: initialJobsCombined,
            jsonApiSources,
            storageState,
        };
    } catch (err) {
        log.warning('Playwright handshake failed', { error: err.message ?? String(err) });
        if (browser) await browser.close().catch(() => {});
        return {
            html: '',
            cookieHeader: '',
            userAgent: randomFingerprint().ua,
            initialJobs: [],
            jsonApiSources: [],
            storageState: null,
        };
    }
};

const playwrightFetchPageHtml = async ({ url, userAgent, proxyUrl, storageState }) => {
    let browser;
    const fp = randomFingerprint();
    try {
        browser = await chromium.launch({
            headless: true,
            proxy: proxyUrl ? { server: proxyUrl } : undefined,
            args: [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-infobars',
            ],
        });

        const context = await browser.newContext({
            userAgent: userAgent || fp.ua,
            viewport: fp.viewport,
            locale: 'en-US',
            timezoneId: 'America/New_York',
            bypassCSP: true,
            proxy: proxyUrl ? { server: proxyUrl } : undefined,
            storageState: storageState || undefined,
        });

        await context.addInitScript(() => {
            try {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
                Object.defineProperty(navigator, 'maxTouchPoints', { get: () => 0 });
                Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
                window.chrome = { runtime: {}, app: {} };
            } catch {
                // ignore
            }
        });

        const page = await context.newPage();
        await page.route('**/*', (route) => {
            const type = route.request().resourceType();
            if (['image', 'media', 'font'].includes(type)) return route.abort();
            return route.continue();
        });

        await page.goto(url, { waitUntil: 'networkidle', timeout: 45000 });
        await dismissPopupsPlaywright(page);
        await handleOptInFlow(page);

        const html = await page.content();
        if (looksBlockedHtml(html)) {
            throw new Error('Blocked HTML detected during Playwright detail fetch');
        }
        await browser.close().catch(() => {});
        return html;
    } catch (err) {
        if (browser) await browser.close().catch(() => {});
        throw err;
    }
};

// --- HTTP SCRAPING ------------------------------------------------------------

const httpFetchHtml = async ({
    url,
    userAgent,
    cookieHeader,
    proxyUrl,
    timeoutMs = 25000,
    retries = 2,
    extraHeaders = {},
}) => {
    log.debug('HTTP fetch', { url });

    const res = await retryWithBackoff(
        async () =>
            gotScraping({
                url,
                proxyUrl,
                timeout: { request: timeoutMs },
                http2: true,
                decompress: true,
                retry: { limit: 0 },
                headers: {
                    'user-agent': userAgent,
                    accept:
                        'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'accept-language': 'en-US,en;q=0.9',
                    referer: BASE_URL + '/',
                    ...(cookieHeader ? { cookie: cookieHeader } : {}),
                    ...extraHeaders,
                },
            }),
        { attempts: Math.max(1, retries), startMs: 700 },
    );

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

const httpFetchJson = async ({
    url,
    userAgent,
    cookieHeader,
    proxyUrl,
    timeoutMs = 20000,
    retries = 2,
    extraHeaders = {},
}) => {
    log.debug('HTTP fetch JSON', { url });

    const res = await retryWithBackoff(
        async () =>
            gotScraping({
                url,
                proxyUrl,
                timeout: { request: timeoutMs },
                http2: true,
                decompress: true,
                retry: { limit: 0 },
                headers: {
                    'user-agent': userAgent,
                    accept: 'application/json,text/plain,*/*',
                    'accept-language': 'en-US,en;q=0.9',
                    referer: BASE_URL + '/',
                    ...(cookieHeader ? { cookie: cookieHeader } : {}),
                    ...extraHeaders,
                },
            }),
        { attempts: Math.max(1, retries), startMs: 600 },
    );

    const { statusCode } = res;
    if (statusCode >= 400) {
        throw new Error(`HTTP ${statusCode} for ${url}`);
    }

    const body = res.body?.toString() ?? '';
    if (!body) {
        throw new Error(`Empty JSON body for ${url}`);
    }

    const parsed = safeParseJson(body);
    if (!parsed) {
        throw new Error(`Failed to parse JSON for ${url}`);
    }

    return parsed;
};

// --- LISTING PARSING ----------------------------------------------------------

/** 
 * DOM-based listing parser for ZipRecruiter using Cheerio.
 * We try specific card selectors, then fallback to generic h2/h3-based parsing.
 */
const extractJobsFromDom = (html, pageUrl) => {
    const $ = cheerio.load(html);
    $('script,noscript,style').remove();

    const urlObj = new URL(pageUrl);
    const seen = new Set();
    const jobs = [];

    // ZipRecruiter uses various card structures
    const cardSelectors = [
        'article[data-job-id]',
        'div[data-job-id]',
        '[data-testid="job-card"]',
        '.job_result',
        '.job_result_card',
        'section[role="list"] article',
        '.job-listing',
        '.jobList article',
        '[class*="JobCard"]',
        '[class*="job-card"]',
        '[class*="job_card"]',
    ];

    let cards = [];
    for (const sel of cardSelectors) {
        const found = $(sel).toArray();
        if (found.length > 0) {
            cards = found;
            log.debug('Found job cards with selector', { selector: sel, count: found.length });
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
                c.find('h2 a').first() ||
                c.find('a[href*="/jobs/"]').first() ||
                c.find('a[href*="/job/"]').first() ||
                c.find('h2, h3').first();

            const title = titleEl.text().replace(/\s+/g, ' ').trim();

            let company =
                c.find('[data-testid="company-name"]').first().text().replace(/\s+/g, ' ').trim() ||
                c.find('.job_result_company, .company_name, .company').first().text().replace(/\s+/g, ' ').trim() ||
                c.find('a[href*="/co/"]').first().text().replace(/\s+/g, ' ').trim();

            let location =
                c.find('[data-testid="location"]').first().text().replace(/\s+/g, ' ').trim() ||
                c.find('.job_result_location, .location').first().text().replace(/\s+/g, ' ').trim();

            let salary =
                c.find('[data-testid="salary"]').first().text().replace(/\s+/g, ' ').trim() ||
                c.find('.job_result_salary, .salary').first().text().replace(/\s+/g, ' ').trim();

            // Try to extract salary from card text if not found
            if (!salary) {
                const cardText = c.text();
                const salaryMatch = cardText.match(/\$[\d,]+(?:\s*-\s*\$[\d,]+)?(?:\/(?:hr|year|yr|month|mo))?/i);
                if (salaryMatch) {
                    salary = salaryMatch[0].trim();
                }
            }

            const href =
                titleEl.attr('href') ||
                c.find('a[href*="/jobs/"]').first().attr('href') ||
                c.find('a[href*="/job/"]').first().attr('href') ||
                c.find('a[href*="/c/"]').first().attr('href');

            const jobUrl = normalizeUrl(href);

            pushJob(jobUrl, title, company, location, salary);
        }
    }

    // Fallback: generic h2/h3 scanning for job titles
    if (!jobs.length) {
        log.debug('No cards found, trying h2/h3 fallback');
        
        $('h2, h3').each((_, el) => {
            const $el = $(el);
            const title = $el.text().replace(/\s+/g, ' ').trim();
            if (!title || title.length < 2 || title.length > 180) return;
            
            // Skip titles that look like section headers
            if (title.toLowerCase().includes('similar jobs') || 
                title.toLowerCase().includes('related') ||
                title.toLowerCase().includes('search')) return;

            const container = $el.closest('article,li,div[class]').length
                ? $el.closest('article,li,div[class]')
                : $el.parent();

            let company =
                container
                    .find('[data-testid="company-name"]')
                    .first()
                    .text()
                    .replace(/\s+/g, ' ')
                    .trim() ||
                container.find('.company, .company_name').first().text().replace(/\s+/g, ' ').trim() ||
                container.find('a[href*="/co/"]').first().text().replace(/\s+/g, ' ').trim();

            const text = container.text().replace(/\s+/g, ' ').trim();
            let location = '';
            let salary = '';

            // Look for location patterns
            const locMatch = text.match(/(?:in|·)\s*([A-Za-z\s,]+(?:,\s*[A-Z]{2})?)/i);
            if (locMatch) location = locMatch[1].trim();

            // Look for salary patterns (including CA$, $, etc.)
            const salMatch = text.match(/(?:CA)?\$[\d,]+(?:\s*[-–]\s*(?:CA)?\$[\d,]+)?(?:\/(?:hr|hour|year|yr|month|mo|week|wk))?/i);
            if (salMatch) salary = salMatch[0].trim();

            // Find job URL
            const href =
                container.find('a[href*="/jobs/"]').first().attr('href') ||
                container.find('a[href*="/job/"]').first().attr('href') ||
                container.find('a[href*="/c/"]').first().attr('href') ||
                $el.find('a').first().attr('href') ||
                $el.closest('a').attr('href');

            const jobUrl = normalizeUrl(href);
            pushJob(jobUrl, title, company, location, salary);
        });
    }

    // Last resort: scan for any job-like links
    if (!jobs.length) {
        log.debug('No h2/h3 jobs found, trying link scanning');
        
        $('a[href*="/jobs/"], a[href*="/job/"], a[href*="/c/"]').each((_, el) => {
            const $a = $(el);
            const href = $a.attr('href');
            
            // Skip navigation links
            if (href.includes('/jobs-search') || href.includes('/post-job')) return;
            
            const title = $a.text().replace(/\s+/g, ' ').trim();
            if (!title || title.length < 3 || title.length > 160) return;
            
            // Skip if in header/footer/nav
            if ($a.closest('header,footer,nav').length) return;
            
            const jobUrl = normalizeUrl(href);
            pushJob(jobUrl, title, null, null, null);
        });
    }

    log.debug('extractJobsFromDom result', { jobCount: jobs.length });
    return jobs;
};

// --- DETAIL PAGE PARSING ------------------------------------------------------

const extractDetailFromJsonStructure = (payload) => {
    if (!payload) return null;
    const detail = {};
    const visited = new Set();

    const visit = (node) => {
        if (!node || visited.has(node)) return;
        if (typeof node === 'object') visited.add(node);

        if (Array.isArray(node)) {
            for (const child of node) visit(child);
            return;
        }

        if (typeof node === 'object') {
            const title = node.title || node.job_title || node.name || node.position;
            const company =
                node.company ||
                node.company_name ||
                node.employer ||
                node.hiringOrganization?.name;
            const location =
                node.location ||
                node.city ||
                node.region ||
                node.jobLocation?.address?.addressLocality ||
                node.jobLocation?.address?.addressRegion ||
                node.jobLocation?.address?.addressCountry;
            const descriptionHtml = node.description || node.job_description || node.body;
            const datePosted = node.date_posted || node.datePosted || node.posted_at;

            if (title && !detail.title) detail.title = String(title);
            if (company && !detail.company) detail.company = String(company);
            if (location && !detail.location) detail.location = String(location);
            if (descriptionHtml && !detail.description_html) {
                detail.description_html = String(descriptionHtml);
                const tmp = cheerio.load(detail.description_html);
                detail.description_text = tmp('body').text().replace(/\s+/g, ' ').trim();
            }
            if (datePosted && !detail.date_posted) {
                detail.date_posted = new Date(datePosted).toISOString();
            }

            for (const value of Object.values(node)) {
                visit(value);
            }
        }
    };

    visit(payload);
    return Object.keys(detail).length ? detail : null;
};

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

    if (!result.title || !result.description_html) {
        const scriptCandidates = $(
            'script[id="__NEXT_DATA__"], script[type="application/json"], script[data-json]',
        ).toArray();
        for (const el of scriptCandidates) {
            const parsed = safeParseJson($(el).text());
            if (!parsed) continue;
            const detailFromJson = extractDetailFromJsonStructure(parsed);
            if (detailFromJson) {
                Object.assign(
                    result,
                    {
                        title: detailFromJson.title || result.title,
                        company: detailFromJson.company || result.company,
                        location: detailFromJson.location || result.location,
                        description_html: detailFromJson.description_html || result.description_html,
                        description_text: detailFromJson.description_text || result.description_text,
                        date_posted: detailFromJson.date_posted || result.date_posted,
                    },
                );
            }
            if (result.title && result.description_html) break;
        }
    }

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
        detail_playwright_fallback = false,
        listing_fetch_retries = 2,
        detail_fetch_retries = 2,
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
        handshakeInitialJobs: 0,
        handshakeJsonSources: 0,
        pagesFetched: 0,
        pagesFailed: 0,
        pagesBlockedOrCaptcha: 0,
        listingJsonHits: 0,
        listingJsonFailures: 0,
        listingEmbeddedJsonHits: 0,
        listingDomHits: 0,
        jobsFromDomFallback: 0,
        jobsSaved: 0,
        detailRequests: 0,
        detailSuccess: 0,
        detailFailed: 0,
        detailPlaywrightAttempts: 0,
        detailPlaywrightSuccess: 0,
        detailPlaywrightFailed: 0,
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
        detail_playwright_fallback,
        listing_fetch_retries,
        detail_fetch_retries,
    });

    // Proxy config (for HTTP scraping only)
    const proxyConfiguration = await Actor.createProxyConfiguration(
        proxyFromInput || {
            useApifyProxy: true,
            groups: ['RESIDENTIAL'],
            countryCode: 'GB',
        },
    );

    const sessionId = `sess-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
    const getProxyUrl = async (session = sessionId) =>
        proxyConfiguration ? proxyConfiguration.newUrl({ session }) : undefined;

    const handshakeUrl = startUrlToUse;
    stats.handshakeAttempted = true;
    let handshakeHtml = '';
    let handshakeCookieHeader = '';
    let handshakeUserAgent = randomFingerprint().ua;
    let handshakeInitialJobs = [];

    const handshakeResult = await doPlaywrightHandshake(handshakeUrl, proxyConfiguration, sessionId);
    handshakeHtml = handshakeResult.html || '';
    handshakeCookieHeader = handshakeResult.cookieHeader || '';
    handshakeUserAgent = handshakeResult.userAgent || handshakeUserAgent;
    handshakeInitialJobs = handshakeResult.initialJobs || [];
    const handshakeJsonSources = handshakeResult.jsonApiSources || [];
    const handshakeStorageState = handshakeResult.storageState || null;
    stats.handshakeSucceeded = Boolean(handshakeHtml && handshakeCookieHeader);
    stats.handshakeInitialJobs = handshakeInitialJobs.length;
    stats.handshakeJsonSources = (handshakeResult.jsonApiSources || []).length;

    if (!stats.handshakeSucceeded) {
        log.warning('Continuing with HTTP-only mode (handshake did not fully succeed)');
    }

    const deriveJsonUrlForPage = (pageNum) => {
        for (const src of handshakeJsonSources) {
            if (!src?.url) continue;
            const nextUrl = buildJsonPageUrlFromSource(src.url, pageNum);
            if (nextUrl) return nextUrl;
        }
        return null;
    };

    const fetchListingViaJson = async (pageNum) => {
        const jsonUrl = deriveJsonUrlForPage(pageNum);
        if (!jsonUrl) return null;
        try {
            const proxyUrl = await getProxyUrl(`${sessionId}-json-${pageNum}`);
            const json = await httpFetchJson({
                url: jsonUrl,
                userAgent: handshakeUserAgent,
                cookieHeader: handshakeCookieHeader,
                proxyUrl,
                retries: listing_fetch_retries,
            });
            const jobs = extractJobsFromJsonPayload(json, jsonUrl);
            if (jobs.length) {
                stats.listingJsonHits += jobs.length;
                return { jobs, jsonUrl };
            }
            stats.listingJsonFailures++;
        } catch (err) {
            stats.listingJsonFailures++;
            log.debug('JSON listing fetch failed', { url: jsonUrl, error: err.message ?? String(err) });
        }
        return null;
    };

    let savedCount = 0;

    for (let pageNum = 1; pageNum <= pagesLimit; pageNum++) {
        if (savedCount >= resultsWanted) break;

        const listingUrl = startUrlClean
            ? buildPageUrlFromStart(startUrlToUse, pageNum)
            : buildSearchUrl(keyword.trim(), location.trim(), pageNum);

        let html = '';
        let jobs = [];
        let listingSource = 'unknown';

        try {
            const jsonResult = await fetchListingViaJson(pageNum);
            if (jsonResult?.jobs?.length) {
                jobs = jsonResult.jobs;
                listingSource = 'json-api';
            }

            // PAGE 1: Prefer jobs extracted directly from Playwright DOM/JSON if we have them
            if (!jobs.length && pageNum === 1 && handshakeInitialJobs.length > 0) {
                html = handshakeHtml;
                jobs = handshakeInitialJobs;
                listingSource = 'handshake-initial';
                log.info('Using jobs extracted from Playwright handshake for page 1', {
                    count: jobs.length,
                });
            }

            // HTML path
            if (!jobs.length) {
                if (pageNum === 1 && handshakeHtml) {
                    html = handshakeHtml;
                    listingSource = listingSource === 'unknown' ? 'handshake-html' : listingSource;
                    log.info('Using HTML from Playwright handshake for page 1 (parse flow)');
                } else {
                    const proxyUrl = await getProxyUrl(
                        `${sessionId}-list-${pageNum}-${Math.floor(Math.random() * 10)}`,
                    );
                    html = await httpFetchHtml({
                        url: listingUrl,
                        userAgent: handshakeUserAgent,
                        cookieHeader: handshakeCookieHeader,
                        proxyUrl,
                        retries: listing_fetch_retries,
                    });
                }

                if (looksBlockedHtml(html)) {
                    stats.pagesBlockedOrCaptcha++;
                    stats.stoppedReason = 'BLOCKED_HTML_LISTING';
                    log.warning('Blocked page HTML detected, stopping pagination', { url: listingUrl });
                    break;
                }

                const embeddedJobs = extractJobsFromEmbeddedJson(html, listingUrl);
                if (embeddedJobs.length) {
                    jobs = embeddedJobs;
                    listingSource = 'embedded-json';
                    stats.listingEmbeddedJsonHits += jobs.length;
                }

                if (!jobs.length) {
                    jobs = extractJobsFromDom(html, listingUrl);
                    listingSource = 'dom';
                    stats.listingDomHits += jobs.length;
                }
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

        if (!jobs || !jobs.length) {
            // Extra debug only on first page to avoid huge logs
            if (pageNum === 1 && html) {
                const snippet = html.slice(0, 1000).replace(/\s+/g, ' ');
                log.info('First page HTML snippet (no jobs parsed):', { snippet });
            }

            log.warning('No jobs parsed from listing page; stopping pagination.', {
                url: listingUrl,
                pageNum,
            });
            stats.stoppedReason = pageNum === 1 ? 'NO_JOBS_ON_FIRST_PAGE' : 'NO_JOBS_ON_PAGE';
            break;
        }

        stats.pagesFetched++;
        if (listingSource === 'dom') {
            stats.jobsFromDomFallback += jobs.length;
        }

        log.info(`Parsed ${jobs.length} jobs from listing page`, {
            pageNum,
            url: listingUrl,
            source: listingSource,
        });

        // Decide which jobs get detail fetch based on detailMode
        const jobsToProcess = jobs.slice(0, resultsWanted - savedCount); // cap by remaining target

        const shouldFetchDetail = (index) => {
            if (detailMode === 'none') return false;
            if (detailMode === 'basic') return index < 5; // first few per page
            return true; // full mode
        };

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
                    const proxyUrlForDetails = await getProxyUrl(
                        `${sessionId}-detail-${index % Math.max(1, detailConcurrency)}`,
                    );
                    const htmlDetail = await httpFetchHtml({
                        url: baseJob.url,
                        userAgent: handshakeUserAgent,
                        cookieHeader: handshakeCookieHeader,
                        proxyUrl: proxyUrlForDetails,
                        timeoutMs: 30000,
                        retries: detail_fetch_retries,
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

                    if (detail_playwright_fallback) {
                        stats.detailPlaywrightAttempts++;
                        try {
                            const proxyUrlForPw = await getProxyUrl(
                                `${sessionId}-detail-pw-${index % Math.max(2, detailConcurrency)}`,
                            );
                            const pwHtml = await playwrightFetchPageHtml({
                                url: baseJob.url,
                                userAgent: handshakeUserAgent,
                                proxyUrl: proxyUrlForPw,
                                storageState: handshakeStorageState,
                            });
                            const detail = extractJobDetail(pwHtml);
                            stats.detailPlaywrightSuccess++;

                            return {
                                ...baseJob,
                                title:
                                    detail.title && detail.title.length >= 2 && detail.title.length <= 200
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
                        } catch (pwErr) {
                            stats.detailPlaywrightFailed++;
                            log.debug('Playwright detail fallback failed', {
                                url: baseJob.url,
                                error: pwErr.message ?? String(pwErr),
                            });
                        }
                    }

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
