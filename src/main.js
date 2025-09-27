// src/main.js
// ESM + Node 22 + Apify SDK v3 + Crawlee v3 (CheerioCrawler)
// ZipRecruiter scraper that prefers a direct listing URL (startUrl), supports pagination,
// optional detail scraping, proxy, sessions, and defensive anti-bot headers (no invalid options).

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

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
    const s = CLEAN(txt || '');
    const m = s.match(/(\d+)\s+(day|days|hour|hours|minute|minutes|week|weeks|month|months)\s+ago/i);
    if (!m) return { raw: s };
    return { raw: s, relative: `${m[1]} ${m[2]} ago` };
};

const extractJsonLd = ($) => {
    try {
        const blocks = $('script[type="application/ld+json"]')
            .map((_, el) => $(el).contents().text())
            .get();
        for (const b of blocks) {
            try {
                const data = JSON.parse(b);
                const arr = Array.isArray(data) ? data : [data];
                const jp = arr.find((x) => x['@type'] === 'JobPosting') || null;
                if (jp) return jp;
            } catch { /* continue */ }
        }
    } catch { /* ignore */ }
    return null;
};

const findNextPage = ($, baseUrl) => {
    let href = $('link[rel="next"]').attr('href');
    if (href) return ABS(href, baseUrl);

    const a1 = $('a[rel="next"], a[aria-label="Next"], a[aria-label="next"]').attr('href');
    if (a1) return ABS(a1, baseUrl);

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

    // Heuristic increment; will naturally stop when page stops changing
    if (!cur.searchParams.has('p')) cur.searchParams.set('p', (curP + 1).toString());
    else cur.searchParams.set('p', (curP + 1).toString());
    return cur.href;
};

const scrapeCards = ($, baseUrl) => {
    const jobs = [];
    const LINK_SEL = [
        'a.job_link',
        'a[href*="/c/"][href*="/Job/"]',
        'a[href*="/job/"]',
        'a[href*="/jobs/"][href*="jid="]',
    ].join(',');

    $(LINK_SEL).each((_, el) => {
        const $a = $(el);
        const href = ABS($a.attr('href'), baseUrl);
        if (!href) return;
        if (!/\/(c|job|jobs)\//i.test(href)) return;

        let title = CLEAN($a.text());
        if (!title) title = CLEAN($a.find('h2,h3').first().text());

        const $card = $a.closest('article, li, div').first();
        const textBlob = CLEAN(($card.text() || '').slice(0, 800));

        let company = CLEAN($card.find('a.t_org_link, .t_org_link, .company, [data-company], .job_empresa').first().text()) || null;
        let location = CLEAN($card.find('.job_location, .location, [data-location]').first().text()) || null;
        let employmentType = CLEAN($card.find('.employment-type, [data-employment-type]').first().text()) || null;

        if (!location) {
            const locMatch = textBlob.match(/[A-Za-z .'-]+,\s*[A-Z]{2}\b/);
            if (locMatch) location = locMatch[0];
        }

        let postedText = null;
        const postMatch = textBlob.match(/\b\d+\s+(?:day|days|hour|hours|week|weeks|month|months)\s+ago\b/i);
        if (postMatch) postedText = postMatch[0];

        let salaryRaw = null;
        const salMatch = textBlob.match(/\$\s?[\d.,]+(?:\s*[KM])?\s*-\s*\$\s?[\d.,]+(?:\s*[KM])?.{0,20}?(?:yr|hr|year|hour|annually)/i);
        if (salMatch) salaryRaw = salMatch[0];

        jobs.push({
            url: href,
            title: title || null,
            company: company || null,
            location: location || null,
            posted_text: postedText || null,
            posted_guess: postedText ? guessPosted(postedText) : null,
            salary: salaryRaw ? parseSalary(salaryRaw) : null,
            employment_type: employmentType || null,
        });
    });

    const seen = new Set();
    return jobs.filter((j) => (j.url && !seen.has(j.url) && seen.add(j.url)));
};

const scrapeDetail = ($, loadedUrl) => {
    const out = {};
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

    const descNode = $('section.job_description, #job_description, .job_description, [data-testid="jobDescription"], article').first();
    out.description_html = STR(descNode.html()) || null;
    out.description_text = CLEAN(descNode.text()) || null;

    out.posted_text = CLEAN($('time[datetime], .posted, .posted-date, .t_posted').first().text()) || null;
    out.employment_type = CLEAN($('.employment-type, [data-employment-type]').first().text()) || null;

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
const {
    startUrl,
    keyword = 'Software Engineer',
    location = '',
    results_wanted = 100,
    collect_details = true,
    maxConcurrency = 8,
    maxRequestRetries = 3,
    proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] },
    requestHandlerTimeoutSecs = 45,
    downloadIntervalMs = 0,
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

const buildStartUrl = (kw, loc) => {
    // SEO-friendly listing; fallback works well with SSR.
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
    requestHandlerTimeoutSecs,
    useSessionPool: true,
    persistCookiesPerSession: true,

    // Defensive preNavigation hook: set realistic headers no matter which options object Crawlee uses
    preNavigationHooks: [
        async (ctx) => {
            const { request, session } = ctx;

            const ua = session?.userData?.ua || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36';
            if (session && !session.userData.ua) session.userData.ua = ua;

            const headers = {
                'user-agent': ua,
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.9',
                'upgrade-insecure-requests': '1',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'dnt': '1',
            };

            if (ctx.gotOptions) {
                ctx.gotOptions.headers = { ...(ctx.gotOptions.headers || {}), ...headers };
            }
            if (ctx.requestOptions) {
                ctx.requestOptions.headers = { ...(ctx.requestOptions.headers || {}), ...headers };
            }
            request.headers = { ...(request.headers || {}), ...headers };

            if (downloadIntervalMs) await sleep(downloadIntervalMs);
        },
    ],

    requestHandler: async ({ request, $, enqueueLinks }) => {
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
await crawler.run([{ url: START_URL, userData: { label: 'LIST' } }]);

log.info(`Done. Total jobs pushed: ${pushed}`);
await Actor.exit();

/*
Input example:
{
  "startUrl": "https://www.ziprecruiter.com/Jobs/Software-Engineer/-in-San-Francisco,CA",
  "results_wanted": 50,
  "collect_details": true,
  "proxyConfiguration": { "useApifyProxy": true, "apifyProxyGroups": ["RESIDENTIAL"] },
  "downloadIntervalMs": 250
}
*/
