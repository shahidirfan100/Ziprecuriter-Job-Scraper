// src/main.js
// ESM + Node 22 + Apify SDK v3 + Crawlee v3 (CheerioCrawler)
// Prefers startUrl (ZipRecruiter search/list URL). Falls back to keyword/location only if startUrl missing.
// Robust pagination + optional detail scraping + proxy + anti-bot headers.

import { Actor, log } from 'apify';
import {
    CheerioCrawler,
    Dataset,
    createPlaywrightRouter, // not used, just keeping imports clean if you expand later
} from 'crawlee';

// ---------------------------
// Utilities
// ---------------------------

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
const ABS = (href, base) => {
    try { return new URL(href, base).href; } catch { return null; }
};

const STR = (x) => (x ?? '').toString().trim();
const CLEAN = (s) => STR(s).replace(/\s+/g, ' ').trim();

const parseSalary = (txt) => {
    // Examples on ZR: "$162K - $215K / yr" or "$60 - $80 / hr" or "$300000 - $320000 Yearly"
    const s = CLEAN(txt || '');
    if (!s) return null;
    const m = s.match(/\$?\s*([\d,.]+)\s*(K|M)?\s*-\s*\$?\s*([\d,.]+)\s*(K|M)?\s*\/?\s*(yr|hr|year|hour|annually|monthly)?/i);
    if (!m) return { raw: s };
    const toNum = (num, mult) => {
        let n = Number(num.replace(/,/g, ''));
        if (mult?.toUpperCase() === 'K') n *= 1e3;
        if (mult?.toUpperCase() === 'M') n *= 1e6;
        return n;
    };
    return {
        raw: s,
        min: toNum(m[1], m[2]),
        max: toNum(m[3], m[4]),
        period: (m[5] || '').toLowerCase() || null,
    };
};

const guessPosted = (txt) => {
    // ZR shows "8 days ago", "Posted 1 day ago", or a relative snippet
    const s = CLEAN(txt || '');
    const m = s.match(/(\d+)\s+(day|days|hour|hours|minute|minutes|week|weeks|month|months)\s+ago/i);
    if (!m) return { raw: s };
    return { raw: s, relative: `${m[1]} ${m[2]} ago` };
};

// Extract JSON-LD if present for richer detail
const extractJsonLd = ($) => {
    try {
        const blocks = $('script[type="application/ld+json"]')
            .map((_, el) => $(el).contents().text())
            .get();
        for (const b of blocks) {
            try {
                const data = JSON.parse(b);
                // Could be array or object; look for JobPosting
                const arr = Array.isArray(data) ? data : [data];
                const jp = arr.find((x) => x['@type'] === 'JobPosting') || null;
                if (jp) return jp;
            } catch { /* continue */ }
        }
    } catch { /* ignore */ }
    return null;
};

// Attempt to find "next page" on listing
const findNextPage = ($, baseUrl) => {
    // Strategy:
    // 1) <link rel="next" href="...">
    // 2) <a rel="next"> or <a aria-label="Next"> or visible "Next" control
    // 3) any anchor containing "?p=" or "&p=" greater than current page
    let href = $('link[rel="next"]').attr('href');
    if (href) return ABS(href, baseUrl);

    // aria-label / rel on anchors
    const a1 = $('a[rel="next"], a[aria-label="Next"], a[aria-label="next"]').attr('href');
    if (a1) return ABS(a1, baseUrl);

    // numeric pagination; pick the largest page link greater than current "?p="
    const cur = new URL(baseUrl);
    const curP = Number(cur.searchParams.get('p') || '1') || 1;

    let best = null;
    $('a[href*="?p="], a[href*="&p="]').each((_, a) => {
        const h = $(a).attr('href');
        const url = ABS(h, baseUrl);
        if (!url) return;
        try {
            const u = new URL(url);
            const p = Number(u.searchParams.get('p') || '0');
            if (p > curP && (!best || p < best.p)) best = { p, url };
        } catch { /* ignore */ }
    });
    if (best) return best.url;

    // Heuristic: if no explicit links, try incrementing "?p="
    if (!cur.searchParams.has('p')) cur.searchParams.set('p', (curP + 1).toString());
    else cur.searchParams.set('p', (curP + 1).toString());
    return cur.href;
};

// Scrape job cards on listing page (conservative selectors)
const scrapeCards = ($, baseUrl) => {
    const jobs = [];

    // ZR markup varies; target anchors that look like job detail links.
    const LINK_SEL = [
        'a.job_link',
        'a[href*="/c/"][href*="/Job/"]',
        'a[href*="/job/"]',
        'a[href*="/jobs/"][href*="jid="]',
    ].join(',');

    // Each card may be a block with text siblings holding company/location/salary.
    $(LINK_SEL).each((_, el) => {
        const $a = $(el);
        const href = ABS($a.attr('href'), baseUrl);
        if (!href) return;

        // Avoid pagination/other links by quick sanity check
        if (!/\/(c|job|jobs)\//i.test(href)) return;

        // Title is often the anchor text or nested heading
        let title = CLEAN($a.text());
        if (!title) title = CLEAN($a.find('h2,h3').first().text());

        // Neighborhood text: look at parent/next siblings for company/location/salary
        const $card = $a.closest('article, li, div').first();
        const textBlob = CLEAN(($card.text() || '').slice(0, 800));

        // Try to extract company & location heuristically (ZR shows "Company  City, ST" near the title)
        let company = null;
        let location = null;
        let employmentType = null;
        let postedText = null;
        let salaryRaw = null;

        // Company often appears before a city comma state pattern or as capitalized words
        const locMatch = textBlob.match(/[A-Za-z .'-]+,\s*[A-Z]{2}\b/);
        if (locMatch) location = locMatch[0];

        // Posted "X days ago"
        const postMatch = textBlob.match(/\b\d+\s+(?:day|days|hour|hours|week|weeks|month|months)\s+ago\b/i);
        if (postMatch) postedText = postMatch[0];

        // Salary pattern
        const salMatch = textBlob.match(/\$\s?[\d.,]+(?:\s*[KM])?\s*-\s*\$\s?[\d.,]+(?:\s*[KM])?.{0,20}?(?:yr|hr|year|hour|annually)/i);
        if (salMatch) salaryRaw = salMatch[0];

        // Try dedicated spans if present
        if (!company) {
            company = CLEAN($card.find('a.t_org_link, .t_org_link, .company, [data-company], .job_empresa').first().text());
        }
        if (!location) {
            location = CLEAN($card.find('.job_location, .location, [data-location]').first().text()) || location;
        }
        if (!employmentType) {
            employmentType = CLEAN($card.find('.employment-type, [data-employment-type]').first().text()) || null;
        }

        jobs.push({
            url: href,
            title: title || null,
            company: company || null,
            location: location || null,
            posted_text: postedText || null,
            posted_guess: postedText ? guessPosted(postedText) : null,
            salary: salaryRaw ? parseSalary(salaryRaw) : null,
        });
    });

    // De-dupe by URL
    const seen = new Set();
    return jobs.filter((j) => (j.url && !seen.has(j.url) && seen.add(j.url)));
};

// Parse job detail page for richer fields
const scrapeDetail = ($, loadedUrl) => {
    const out = {};

    // Title, company, location
    out.title = CLEAN($('h1, h1[itemprop="title"], h1[data-job-title]').first().text()) || null;

    const companyCandidates = [
        'a[href*="/co/"]',
        '.company, [itemprop="hiringOrganization"]',
        'h2:contains("Company") ~ *',
    ].join(',');
    out.company = CLEAN($(companyCandidates).first().text()) || null;

    const locCandidates = [
        '.location, .job_location, [itemprop="jobLocation"]',
        'h2:contains("Address") ~ *, h3:contains("Address") ~ *',
    ].join(',');
    out.location = CLEAN($(locCandidates).first().text()) || null;

    // Description: prefer a specific container, else fall back to main content section
    const descNode = $('section.job_description, #job_description, .job_description, [data-testid="jobDescription"], article').first();
    out.description_html = STR(descNode.html()) || null;
    out.description_text = CLEAN(descNode.text()) || null;

    // Relative posted text & employment type
    out.posted_text = CLEAN($('time[datetime], .posted, .posted-date, .t_posted').first().text()) || null;
    out.employment_type = CLEAN($('.employment-type, [data-employment-type]').first().text()) || null;

    // Try JSON-LD JobPosting for structured fields
    const jp = extractJsonLd($);
    if (jp) {
        out.jsonld = jp;
        if (!out.title && jp.title) out.title = CLEAN(jp.title);
        if (!out.company && jp.hiringOrganization?.name) out.company = CLEAN(jp.hiringOrganization.name);
        if (!out.description_html && jp.description) out.description_html = jp.description;
        if (!out.employment_type && jp.employmentType) out.employment_type = jp.employmentType;
        if (!out.location && jp.jobLocation?.address) {
            const a = jp.jobLocation.address;
            out.location = CLEAN([a.addressLocality, a.addressRegion, a.addressCountry].filter(Boolean).join(', '));
        }
        if (jp.datePosted) out.date_posted_iso = jp.datePosted;
        if (jp.validThrough) out.valid_through_iso = jp.validThrough;
        if (jp.baseSalary) out.base_salary = jp.baseSalary;
    }

    out.detail_url = loadedUrl;
    return out;
};

// ---------------------------
// Main
// ---------------------------

await Actor.init();

const input = (await Actor.getInput()) || {};
// Primary: startUrl. Optional: results_wanted, collectDetails, maxConcurrency, proxyConfiguration.
const {
    startUrl,                           // Prefer this (ZipRecruiter search/list page)
    // Back-compat fallbacks (used only if startUrl missing):
    keyword = 'Software Engineer',
    location = '',
    posted_date = 'anytime',            // not used for URL build unless falling back
    results_wanted = 100,
    collect_details = true,
    maxConcurrency = 8,
    maxRequestRetries = 3,
    proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] },
    requestTimeoutSecs = 45,
    downloadIntervalMs = 0,             // small delay between listing pages if needed
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// Resolve start URL
const buildStartUrl = (kw, loc) => {
    const u = new URL('https://www.ziprecruiter.com/Jobs/');
    // The /Jobs/<Title>/-in-<City,ST> pattern is common; a general fallback is:
    // https://www.ziprecruiter.com/candidate/search?search=kw&location=loc
    // But the Jobs/* pages are SEO-friendly and SSR.
    const searchSlug = kw ? kw.trim().replace(/\s+/g, '-').replace(/[^A-Za-z0-9-]/g, '') : 'Jobs';
    let path = `/${encodeURIComponent(searchSlug)}`;
    if (loc && loc.trim()) {
        path += `/-in-${encodeURIComponent(loc.trim())}`;
    }
    return `https://www.ziprecruiter.com/Jobs${path}`;
};

const START_URL = startUrl?.trim()
    ? startUrl.trim()
    : buildStartUrl(keyword, location);

log.info(`ZipRecruiter scraper starting from: ${START_URL}`);
log.info(`Details collection: ${collect_details ? 'ON' : 'OFF'} | Target jobs: ${results_wanted}`);

let pushed = 0;

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxConcurrency,
    maxRequestRetries,
    requestHandlerTimeoutSecs: requestTimeoutSecs,
    // Enable sessions/cookies: ZR occasionally shows sign-in overlays, but SSR HTML still contains the job content.
    useSessionPool: true,
    persistCookiesPerSession: true,
    // Pre-navigation tweaks to reduce 403s
    preNavigationHooks: [
        async ({ request, session, gotOptions }) => {
            gotOptions.headers = {
                ...(gotOptions.headers || {}),
                // Realistic desktop headers
                'user-agent': session?.userData?.ua || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.9',
                'upgrade-insecure-requests': '1',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'dnt': '1',
            };
            // Small jitter to avoid hammering
            if (downloadIntervalMs) await sleep(downloadIntervalMs);
        },
    ],
    // Handle pages
    requestHandler: async ({ request, $, enqueueLinks, response }) => {
        const { label } = request.userData;

        if (!label || label === 'LIST') {
            const baseUrl = request.loadedUrl ?? request.url;

            const cards = scrapeCards($, baseUrl);
            log.debug(`Found ${cards.length} job cards on ${baseUrl}`);

            for (const card of cards) {
                if (pushed >= results_wanted) break;

                if (collect_details) {
                    await enqueueLinks({
                        urls: [card.url],
                        userData: { label: 'DETAIL', card },
                    });
                } else {
                    // Push as-is without details
                    await Dataset.pushData({
                        source: 'ziprecruiter',
                        scraped_at: new Date().toISOString(),
                        search_url: START_URL,
                        ...card,
                    });
                    pushed++;
                }
            }

            if (pushed < results_wanted) {
                const nextUrl = findNextPage($, baseUrl);
                if (nextUrl && nextUrl !== baseUrl) {
                    log.info(`Enqueue next list page: ${nextUrl}`);
                    await enqueueLinks({ urls: [nextUrl], userData: { label: 'LIST' } });
                } else {
                    log.info('No next page detected.');
                }
            }
            return;
        }

        if (label === 'DETAIL') {
            const base = request.loadedUrl ?? request.url;

            // Some ZR pages show a sign-in overlay ("You Already Have an Account") but HTML still contains usable SSR content.
            // We parse below; if description missing, we still output the card-level info.
            const detail = scrapeDetail($, base);
            const record = {
                source: 'ziprecruiter',
                scraped_at: new Date().toISOString(),
                search_url: START_URL,
                ...request.userData.card,
                ...detail,
            };

            await Dataset.pushData(record);
            pushed++;

            return;
        }
    },
    failedRequestHandler: async ({ request, error }) => {
        log.warning(`Request failed ${request.url}: ${error?.message || error}`);
        await Dataset.pushData({
            type: 'error',
            url: request.url,
            message: String(error?.message || error),
            label: request.userData?.label || null,
            at: new Date().toISOString(),
        });
    },
});

// Seed
await crawler.run([
    { url: START_URL, userData: { label: 'LIST' } },
]);

log.info(`Done. Total jobs pushed: ${pushed}`);
await Actor.exit();

/*
Notes:

- ZipRecruiter can show a login modal ("You Already Have an Account") on detail pages, but content is still server-rendered and parsable with Cheerio (see live example text on a detail page showing that modal language). This scraper pulls the SSR HTML, not the interactive overlay.

- Input fields expected (input_schema can be kept minimal):
  {
    "startUrl": "https://www.ziprecruiter.com/Jobs/Software-Engineer/-in-San-Francisco,CA",
    "results_wanted": 50,
    "collect_details": true,
    "proxyConfiguration": { "useApifyProxy": true, "apifyProxyGroups": ["RESIDENTIAL"] }
  }

- If you must support keyword/location fallback, leave "startUrl" empty and set "keyword" and "location" in input.

- Anti-blocking:
  * Residential proxy group is recommended.
  * Sessions + cookie persistence enabled.
  * Realistic headers + small optional delay (downloadIntervalMs).

- Pagination:
  * Prefers <link rel="next">, falls back to common "Next" anchors, then increments ?p=.
  * Stops naturally when no next page, or when results_wanted reached.

- Schema of dataset item:
  {
    source, scraped_at, search_url,
    url, title, company, location,
    posted_text, posted_guess, salary,
    // if details enabled:
    detail_url, description_text, description_html,
    employment_type, date_posted_iso, valid_through_iso, base_salary, jsonld
  }
*/
