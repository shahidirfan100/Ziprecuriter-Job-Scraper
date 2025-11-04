// src/main.js
// ZipRecruiter — Apify SDK + Crawlee (CheerioCrawler)
// COMPLETE FILE with targeted patches to prevent: endless requests with few/no results,
// circular pagination, and mid-run stalls. No new dependencies; output schema unchanged.

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

// =============== Small utilities ===============
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const STR = (x) => (x ?? '').toString().trim();
const CLEAN = (s) => STR(s).replace(/\s+/g, ' ').trim();
const htmlToText = (html) => {
    const raw = STR(html);
    if (!raw) return null;
    return CLEAN(raw.replace(/<[^>]*>/g, ' '));
};
const toNumber = (v) => {
    if (v === undefined || v === null) return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
};
const pickValue = (...values) => {
    for (const v of values) {
        if (v === undefined || v === null) continue;
        if (typeof v === 'string') {
            const s = CLEAN(v);
            if (s) return s;
        } else if (typeof v === 'number' || typeof v === 'boolean') {
            return v;
        }
    }
    return null;
};
const ABS = (href, base) => { try { return new URL(href, base).href; } catch { return null; } };

// =============== Domain guard (typo fix only; no behavior change) ===============
const ensureCorrectDomain = (url) => {
    if (!url) return url;
    try {
        const u = new URL(url);
        if (u.hostname.replace(/^www\./, '') === 'ziprecuriters.com') {
            u.hostname = 'www.ziprecruiter.com';
            return u.href;
        }
        return url;
    } catch {
        return url;
    }
};

// =============== Output record (keep schema) ===============
const extractJobId = (url) => {
    try {
        return new URL(url).searchParams.get('jid') || null;
    } catch {
        return null;
    }
};

const buildOutputRecord = ({ searchUrl, scrapedAt, url, referer, card = {}, detail = {} }) => {
    const rawDescriptionHtml = pickValue(detail.description_html, card.description_html);
    const descriptionTextCandidate = pickValue(detail.description_text, card.description_text);
    const descriptionText = descriptionTextCandidate || (rawDescriptionHtml ? htmlToText(rawDescriptionHtml) : null);

    const company = sanitizeCompanyName(pickValue(detail.company, card.company));
    const title = pickValue(detail.title, card.title);
    const location = pickValue(detail.location, card.location);
    const employmentType = pickValue(detail.employment_type, card.employment_type);
    const postedText = pickValue(detail.posted_text, card.posted_text);
    const postedRelative = pickValue(detail.posted_relative, card.posted_relative);
    const salaryRaw = pickValue(detail.salary_raw, card.salary_raw);
    const salaryMin = pickValue(detail.salary_min, card.salary_min);
    const salaryMax = pickValue(detail.salary_max, card.salary_max);
    const salaryPeriod = pickValue(detail.salary_period, card.salary_period);
    const salaryCurrency = pickValue(detail.salary_currency, card.salary_currency);

    return {
        source: 'ziprecruiter',
        scraped_at: scrapedAt,
        search_url: searchUrl,
        url,
        job_id: extractJobId(url),
        referer: referer || null,
        title: title || null,
        company: company || null,
        location: location || null,
        description_text: descriptionText || null,
        description_html: rawDescriptionHtml || null,
        employment_type: employmentType || null,
        salary_raw: salaryRaw || null,
        salary_min: salaryMin ?? null,
        salary_max: salaryMax ?? null,
        salary_period: salaryPeriod || null,
        salary_currency: salaryCurrency || null,
        posted_text: postedText || null,
        posted_relative: postedRelative || null,
        date_posted_iso: detail.date_posted_iso ?? null,
        valid_through_iso: detail.valid_through_iso ?? null,
        direct_apply: detail.direct_apply ?? null,
        detail_url: detail.detail_url || null,
    };
};

// =============== Config helpers ===============
const POSTED_WITHIN_MAP = new Map([
    ['any', null],
    ['1', 1], ['1d', 1], ['24', 1], ['24h', 1],
    ['7', 7], ['7d', 7],
    ['30', 30], ['30d', 30],
]);
const resolvePostedWithin = (value) => {
    if (value === undefined || value === null) return null;
    const key = STR(value).toLowerCase();
    return POSTED_WITHIN_MAP.has(key) ? POSTED_WITHIN_MAP.get(key) : null;
};

// =============== Company helpers ===============
const BAD_COMPANY_PATTERNS = [
    /about\s+us/i, /careers?/i, /investors?/i, /blog/i, /press/i, /engineering/i,
    /ziprecruiter\.org/i, /ziprecruiter\s+uk/i, /contact\s+us/i, /privacy/i, /terms/i,
    /^(home|jobs|search|help)$/i,
];
const sanitizeCompanyName = (raw) => {
    const str = CLEAN(raw);
    if (!str) return null;
    if (BAD_COMPANY_PATTERNS.some((p) => p.test(str))) return null;
    let cleaned = str.replace(/\s*\[[^\]]+\]\s*$/, '').replace(/^company[:\-\s]+/i, '');
    const delimiters = [/\s*\|\s/, /\s*\/\s/, /\s+[-–—]\s+/, /\s*•\s*/, /\s+at\s+/i];
    for (const d of delimiters) {
        if (d.test(cleaned)) {
            cleaned = CLEAN(cleaned.split(d)[0]);
            break;
        }
    }
    if (!cleaned) return null;
    if (BAD_COMPANY_PATTERNS.some((p) => p.test(cleaned.toLowerCase()))) return null;
    return cleaned.length < 2 ? null : cleaned;
};

const extractCompanyFromCard = ($card) => {
    if (!$card || $card.length === 0) return null;
    const dataAttrs = ['data-company-name', 'data-company'];
    for (const a of dataAttrs) {
        const val = sanitizeCompanyName($card.attr(a));
        if (val) return val;
    }
    const sels = [
        'a.company_name', 'a[data-company-name]', 'a[href*="/company/"]',
        'span.company_name', 'div.company_name', '[data-testid*="company"]',
        '[class*="company"]', '.job_org_name', '.t_org_link',
    ];
    for (const sel of sels) {
        const $n = $card.find(sel).first();
        if ($n?.length) {
            const v = sanitizeCompanyName($n.text()) || sanitizeCompanyName($n.attr('title')) || sanitizeCompanyName($n.attr('aria-label'));
            if (v) return v;
        }
    }
    const m = ($card.text() || '').match(/(?:hiring\s+)?company:\s*([^\n\r|]+)/i);
    if (m) return sanitizeCompanyName(m[1]) || null;
    return null;
};

const extractCompanyFromPage = ($) => {
    const jp = extractJsonLd($);
    if (jp?.hiringOrganization?.name) {
        const v = sanitizeCompanyName(jp.hiringOrganization.name);
        if (v) return v;
    }
    const hero = [
        '[data-testid="hero-company-name"]', '[data-testid="companyName"]',
        'h2.company-name', 'h3.company-name', 'div.company-header',
    ];
    for (const sel of hero) {
        const $n = $(sel).first();
        if ($n?.length) {
            const v = sanitizeCompanyName($n.text());
            if (v) return v;
        }
    }
    const structured = [
        '[itemprop="hiringOrganization"] [itemprop="name"]',
        '[itemprop="hiringOrganization"]',
        'a.company_link', 'a[href*="/company/"]',
    ];
    for (const sel of structured) {
        const $n = $(sel).first();
        if ($n?.length) {
            const v = sanitizeCompanyName($n.text()) || sanitizeCompanyName($n.attr('content'));
            if (v) return v;
        }
    }
    const metas = ['meta[property="og:site_name"]', 'meta[name="twitter:data1"]'];
    for (const sel of metas) {
        const c = $(sel).attr('content');
        if (c) {
            const v = sanitizeCompanyName(c.split('•')[0]);
            if (v) return v;
        }
    }
    return null;
};

// =============== Parsing helpers ===============
const extractNodeHtml = ($node) => {
    if (!$node || $node.length === 0) return null;
    const clone = $node.clone();
    clone.find('script, style, noscript').remove();
    const html = STR(clone.html());
    return html || null;
};
// =============== Random helpers ===============
const randomInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const randomFloat = (min, max) => Math.random() * (max - min) + min;
const pickOne = (arr) => arr[Math.floor(Math.random() * arr.length)];

// =============== Enhanced User-Agent Profiles (Oct 2025) ===============
const UA_PROFILES = [
    {
        ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.91 Safari/537.36',
        ch: {
            'sec-ch-ua': '"Not.A/Brand";v="99", "Chromium";v="130", "Google Chrome";v="130"',
            'sec-ch-ua-full-version-list': '"Not.A/Brand";v="99.0.0.0", "Chromium";v="130.0.6723.91", "Google Chrome";v="130.0.6723.91"',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"15.0.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-model': '""',
        },
    },
    {
        ua: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.70 Safari/537.36',
        ch: {
            'sec-ch-ua': '"Not.A/Brand";v="99", "Chromium";v="130", "Google Chrome";v="130"',
            'sec-ch-ua-full-version-list': '"Not.A/Brand";v="99.0.0.0", "Chromium";v="130.0.6723.70", "Google Chrome";v="130.0.6723.70"',
            'sec-ch-ua-platform': '"macOS"',
            'sec-ch-ua-platform-version': '"14.5.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-model': '""',
        },
    },
    {
        ua: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15',
        ch: {
            'sec-ch-ua': '"Not.A/Brand";v="99", "Safari";v="18"',
            'sec-ch-ua-full-version-list': '"Not.A/Brand";v="99.0.0.0", "Safari";v="18.0.0.0"',
            'sec-ch-ua-platform': '"macOS"',
            'sec-ch-ua-platform-version': '"14.5.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-model': '""',
        },
    },
    {
        ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
        ch: {
            'sec-ch-ua': '"Not.A/Brand";v="99", "Firefox";v="130"',
            'sec-ch-ua-full-version-list': '"Not.A/Brand";v="99.0.0.0", "Firefox";v="130.0.0.0"',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"15.0.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-model': '""',
        },
    },
    {
        ua: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.63 Safari/537.36',
        ch: {
            'sec-ch-ua': '"Not.A/Brand";v="99", "Chromium";v="130", "Google Chrome";v="130"',
            'sec-ch-ua-full-version-list': '"Not.A/Brand";v="99.0.0.0", "Chromium";v="130.0.6723.63", "Google Chrome";v="130.0.6723.63"',
            'sec-ch-ua-platform': '"Linux"',
            'sec-ch-ua-platform-version': '"6.9.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-model': '""',
        },
    },
];
const pickUAProfile = () => pickOne(UA_PROFILES);

const ACCEPT_LANGUAGE_POOL = [
    'en-US,en;q=0.9',
    'en-US,en;q=0.9,en-GB;q=0.8',
    'en-GB,en;q=0.9,en-US;q=0.7',
    'en-CA,en;q=0.9,en-US;q=0.8',
    'en-AU,en;q=0.9,en-US;q=0.8',
];

const SEARCH_QUERIES = [
    'ziprecruiter job listings',
    'latest tech jobs ziprecruiter',
    'remote software engineer openings',
    'ziprecruiter hiring today',
    'marketing coordinator roles ziprecruiter',
    'sales manager postings ziprecruiter',
    'data analyst positions ziprecruiter',
];

const SEARCH_ENGINES = [
    (q) => `https://www.google.com/search?q=${encodeURIComponent(q)}`,
    (q) => `https://www.bing.com/search?q=${encodeURIComponent(q)}`,
    (q) => `https://duckduckgo.com/?q=${encodeURIComponent(q)}`,
];

const BROWSING_REFERERS = [
    'https://www.linkedin.com/jobs/',
    'https://news.google.com/',
    'https://www.facebook.com/',
    'https://www.reddit.com/r/jobs/',
    'https://www.youtube.com/',
];

const buildSearchReferer = () => {
    const query = pickOne(SEARCH_QUERIES);
    const engine = pickOne(SEARCH_ENGINES);
    return engine(query);
};

const ensureRefererChain = (request) => {
    const existing = request.userData?.referer;
    if (existing) return existing;
    const label = request.userData?.label ?? 'LIST';
    let referer = null;
    if (label === 'DETAIL') {
        referer = request.userData?.originListUrl || buildSearchReferer();
    } else if (label === 'LIST') {
        referer = buildSearchReferer();
    } else {
        referer = pickOne([buildSearchReferer(), pickOne(BROWSING_REFERERS)]);
    }
    request.userData = { ...(request.userData || {}), referer };
    return referer;
};

const computeFetchSite = (targetUrl, referer) => {
    if (!referer) return 'none';
    try {
        const targetHost = new URL(targetUrl).hostname.replace(/^www\./, '');
        const refererHost = new URL(referer).hostname.replace(/^www\./, '');
        if (targetHost === refererHost) return 'same-origin';
        return 'cross-site';
    } catch {
        return 'cross-site';
    }
};

const TIMING_PROFILES = {
    LIST: { base: 240, networkRange: [70, 150], humanPauseChance: 0.12, humanPauseRange: [420, 860] },
    DETAIL: { base: 360, networkRange: [90, 210], humanPauseChance: 0.3, humanPauseRange: [720, 1500] },
    DEFAULT: { base: 280, networkRange: [70, 180], humanPauseChance: 0.18, humanPauseRange: [520, 1100] },
};

const computeDelayMs = (label = 'DEFAULT', configuredInterval, retryCount = 0) => {
    const profile = TIMING_PROFILES[label] || TIMING_PROFILES.DEFAULT;
    const baseTarget = configuredInterval
        ? randomFloat(configuredInterval * 0.85, configuredInterval * 1.3)
        : randomFloat(profile.base * 0.7, profile.base * 1.35);
    const networkLatency = randomInt(profile.networkRange[0], profile.networkRange[1]);
    const humanPause = Math.random() < profile.humanPauseChance
        ? randomInt(profile.humanPauseRange[0], profile.humanPauseRange[1])
        : 0;
    const cappedRetry = Math.min(retryCount, 5);
    const retryBackoff = retryCount > 0
        ? Math.min(4500, Math.pow(2, cappedRetry) * 110 + randomInt(60, 220))
        : 0;
    return Math.round(baseTarget + networkLatency + humanPause + retryBackoff);
};

const flushPendingCardsToDataset = async ({
    collectDetails,
    targetCount,
    searchUrl,
    currentCount,
}) => {
    if (!collectDetails) return { emitted: 0 };
    const remaining = targetCount - currentCount;
    if (remaining <= 0) return { emitted: 0 };
    if (CARD_CACHE.size === 0) return { emitted: 0 };

    const fallbackEntries = [];
    for (const [url, info] of CARD_CACHE.entries()) {
        if (!info || !info.card) continue;
        fallbackEntries.push({ url, info });
        if (fallbackEntries.length >= remaining) break;
    }

    if (fallbackEntries.length === 0) return { emitted: 0 };

    let emitted = 0;
    for (const { url, info } of fallbackEntries) {
        const record = buildOutputRecord({
            searchUrl,
            scrapedAt: info.discoveredAt || new Date().toISOString(),
            url,
            referer: info.referer,
            card: info.card,
            detail: { detail_url: url },
        });
        await Dataset.pushData(record);
        CARD_CACHE.delete(url);
        emitted++;
    }

    return { emitted };
};
const normalizeJobUrl = (u) => {
    try {
        const url = new URL(u);
        url.hash = '';
        const keep = new Set(['jid', 'mid']);
        const kept = new URLSearchParams();
        for (const [k, v] of url.searchParams.entries()) if (keep.has(k)) kept.set(k, v);
        url.search = kept.toString();
        return url.href;
    } catch { return u; }
};
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
    return { raw: s, min: toNum(m[1], m[2]), max: toNum(m[3], m[4]), period: (m[5] || '').toLowerCase() || null };
};
const guessPosted = (txt) => {
    const s = CLEAN(txt || '');
    const m = s.match(/(\d+)\s+(day|days|hour|hours|minute|minutes|week|weeks|month|months)\s+ago/i);
    if (!m) return { raw: s };
    return { raw: s, relative: `${m[1]} ${m[2]} ago` };
};
const extractJsonLd = ($) => {
    try {
        const blocks = $('script[type="application/ld+json"]').map((_, el) => $(el).contents().text()).get();
        for (const b of blocks) {
            try {
                const data = JSON.parse(b);
                const arr = Array.isArray(data) ? data : [data];
                const jp = arr.find((x) => x['@type'] === 'JobPosting');
                if (jp) return jp;
            } catch {}
        }
    } catch {}
    return null;
};

// =============== Enhanced Pagination helpers ===============
const findNextPage = ($, baseUrl) => {
    let href = $('link[rel="next"]').attr('href');
    if (href) {
        const nextUrl = ABS(href, baseUrl);
        if (nextUrl && nextUrl !== baseUrl) return nextUrl;
    }
    const sels = [
        'a[rel="next"]',
        'a[aria-label="Next"]', 'a[aria-label="next"]', 'a[aria-label*="Next"]', 'a[aria-label*="next"]',
        'a.next', 'a[class*="next"]', 'a[class*="Next"]', 'li.next a', 'li[class*="next"] a',
        '.pagination__next[href]', '[data-testid*="pagination-next"][href]', 'a[data-testid*="PaginationNext"][href]',
        'a[data-test*="next"]', 'button[aria-label*="Next"] ~ a', 'nav a[aria-label*="Next"]',
        'a:contains("Next")', 'a:contains("next")', 'a:contains("›")', 'a:contains("»")', 'a:contains("→")',
    ];
    for (const sel of sels) {
        const $next = $(sel);
        if ($next.length) {
            const aNext = $next.attr('href');
            if (aNext) {
                const nextUrl = ABS(aNext, baseUrl);
                if (nextUrl && nextUrl !== baseUrl) return nextUrl;
            }
        }
    }
    const btns = ['button[aria-label*="Next"]', 'button[class*="next"]', '[role="button"][aria-label*="Next"]'];
    for (const sel of btns) {
        const $btn = $(sel).first();
        if ($btn?.length) {
            for (const a of ['data-href', 'data-url', 'formaction', 'onclick']) {
                const v = $btn.attr(a);
                if (v) {
                    const urlMatch = v.match(/https?:\/\/[^\s'"]+/);
                    if (urlMatch) {
                        const nextUrl = ABS(urlMatch[0], baseUrl);
                        if (nextUrl && nextUrl !== baseUrl) return nextUrl;
                    }
                    const nextUrl = ABS(v, baseUrl);
                    if (nextUrl && nextUrl !== baseUrl) return nextUrl;
                }
            }
        }
    }
    const cur = new URL(baseUrl);
    const keys = ['page', 'p', 'page_number', 'start'];
    for (const k of keys) {
        if (cur.searchParams.has(k)) {
            if (k === 'start') {
                const start = parseInt(cur.searchParams.get('start'), 10) || 0;
                const pageSize = 20;
                cur.searchParams.set('start', String(start + pageSize));
                return cur.href;
            }
            const val = Number(cur.searchParams.get(k));
            if (Number.isFinite(val)) {
                cur.searchParams.set(k, String(val + 1));
                return cur.href;
            }
        }
    }
    if (/\/(jobs|search|candidate)/i.test(baseUrl)) {
        cur.searchParams.set('page', '2');
        return cur.href;
    }
    return null;
};

const findNextPageByUrlOnly = (baseUrl) => {
    try {
        const cur = new URL(baseUrl);
        if (cur.searchParams.has('start')) {
            const start = parseInt(cur.searchParams.get('start'), 10) || 0;
            const pageSize = 20;
            cur.searchParams.set('start', String(start + pageSize));
            return cur.href;
        }
        for (const k of ['page', 'p', 'page_number']) {
            const v = parseInt(cur.searchParams.get(k), 10);
            if (!isNaN(v)) { cur.searchParams.set(k, String(v + 1)); return cur.href; }
        }
        cur.searchParams.set('page', '2');
        return cur.href;
    } catch { return null; }
};

const tryAlternativePagination = (currentUrl, currentPageNum) => {
    try {
        const url = new URL(currentUrl);
        const nextPageNum = currentPageNum + 1;
        if (!url.searchParams.has('page')) { url.searchParams.set('page', String(nextPageNum)); return url.href; }
        if (!url.searchParams.has('p'))    { url.searchParams.delete('page'); url.searchParams.set('p', String(nextPageNum)); return url.href; }
        if (!url.searchParams.has('start')){ const pageSize = 20; url.searchParams.set('start', String((nextPageNum - 1) * pageSize)); return url.href; }
        if (!url.searchParams.has('page_number')) { url.searchParams.set('page_number', String(nextPageNum)); return url.href; }
        return null;
    } catch { return null; }
};

// ======== PATCH: list URL normalizer & loop guards ========
const normalizeListUrl = (url) => {
    try {
        const u = new URL(url);
        u.hash = '';
        const drop = new Set(['utm_source','utm_medium','utm_campaign','utm_term','utm_content','ref','fbclid','gclid']);
        const keepSorted = Array.from(u.searchParams.entries())
            .filter(([k]) => !drop.has(k))
            .sort(([a], [b]) => a.localeCompare(b));
        u.search = new URLSearchParams(keepSorted).toString();
        return u.href;
    } catch {
        return url;
    }
};
const STALL_LIMIT = 3; // consecutive list pages with no new jobs
let noProgressPages = 0;
const PAGINATION_URLS_SEEN = new Set();

// =============== Card & Detail scraping ===============
const scrapeCards = ($, baseUrl) => {
    const jobs = [];
    const LINK_SEL = [
        'a.job_link', 'a.job-link', 'a.t_job_link',
        'a[data-job-id]',
        'a[href*="/c/"][href*="/Job/"]',
        'a[href*="/job/"]',
        'a[href*="/jobs/"][href*="jid="]',
        'article a[href*="/job"]',
        '.job-card a', '.job_card a', '[class*="jobCard"] a',
    ].join(',');

    const $links = $(LINK_SEL);
    $links.each((_, el) => {
        const $a = $(el);
        const href0 = $a.attr('href');
        if (!href0) return;
        const href = ABS(href0, baseUrl);
        if (!href) return;
        if (!/\/(c|job|jobs)\//i.test(href) && !/jid=/i.test(href)) return;

        const title = CLEAN($a.text()) || CLEAN($a.find('h2,h3,h4').first().text());
        let $card = $a.closest('article, li, div[class*="job"]').first();
        if (!$card?.length) $card = $a.parent().closest('article, li, div').first();

        const textBlob = CLEAN(($card.text() || '').slice(0, 800));
        let company = extractCompanyFromCard($card) || sanitizeCompanyName($a.attr('data-company-name') || $a.attr('data-company'));
        let location = CLEAN($card.find('.job_location, .location, [data-location], [class*="location"]').first().text()) || null;
        let employmentType = CLEAN($card.find('.employment-type, [data-employment-type]').first().text()) || null;

        if (!location) {
            const m = textBlob.match(/[A-Za-z .'-]+,\s*[A-Z]{2}\b/);
            if (m) location = m[0];
        }

        let postedText = null;
        const postMatch = textBlob.match(/\b\d+\s+(?:day|days|hour|hours|week|weeks|month|months)\s+ago\b/i);
        if (postMatch) postedText = postMatch[0];

        let salaryRaw = null;
        const salMatch = textBlob.match(/\$\s?[\d.,]+(?:\s*[KM])?\s*-\s*\$\s?[\d.,]+(?:\s*[KM])?.{0,20}?(?:yr|hr|year|hour|annually)/i);
        if (salMatch) salaryRaw = salMatch[0];

        const postedGuess = postedText ? guessPosted(postedText) : null;
        const salaryParsed = salaryRaw ? parseSalary(salaryRaw) : null;

        jobs.push({
            url: href,
            title: title || null,
            company: company || null,
            location: location || null,
            employment_type: employmentType || null,
            posted_text: postedText || null,
            posted_relative: postedGuess?.relative || null,
            salary_raw: salaryParsed?.raw || salaryRaw || null,
            salary_min: salaryParsed?.min ?? null,
            salary_max: salaryParsed?.max ?? null,
            salary_period: salaryParsed?.period || null,
        });
    });

    const seen = new Set();
    const unique = jobs.filter((job) => {
        const norm = normalizeJobUrl(job.url);
        if (seen.has(norm)) return false;
        seen.add(norm);
        return true;
    });

    return unique;
};

const scrapeDetail = ($, loadedUrl) => {
    const out = {};
    out.title = CLEAN($('h1, h1[itemprop="title"], h1[data-job-title]').first().text()) || null;

    out.company = extractCompanyFromPage($) || null;

    const locCandidates = [
        '.location, .job_location, [itemprop="jobLocation"]',
        'h2:contains("Address") ~ *, h3:contains("Address") ~ *',
    ].join(',');
    out.location = CLEAN($(locCandidates).first().text()) || null;

    const descriptionSelectors = [
        '[data-testid="jobDescriptionSection"]',
        '[data-testid="jobDescription"]',
        'div[class*="jobDescriptionSection"]',
        'section[class*="jobDescription"]',
        '[data-job-description]',
        'div.job_description',
        'section.job_description',
        'div.jobDescription',
        '#job_description',
        '[itemprop="description"]',
        'article[itemprop="description"]',
        'article.job-details',
        'div.job-details',
    ];
    let descNode = null, bestScore = 0;
    for (const sel of descriptionSelectors) {
        const cand = $(sel).first();
        if (cand?.length) {
            const text = CLEAN(cand.text());
            const len = text.length;
            let score = len;
            const lower = text.toLowerCase();
            if (lower.includes('responsibilities') || lower.includes('requirements') || lower.includes('qualifications')) score += 1000;
            if (lower.includes('ziprecruiter uk') || lower.includes('ziprecruiter.org') || lower.includes('about us careers')) score -= 5000;
            if (len >= 100 && score > bestScore) { bestScore = score; descNode = cand; }
        }
    }
    let descriptionHtml = null, descriptionText = null;
    if (descNode && bestScore > 0) {
        descriptionHtml = extractNodeHtml(descNode);
        descriptionText = CLEAN(descNode.text()) || null;
        if (descriptionText) {
            const lower = descriptionText.toLowerCase();
            if (lower.includes('ziprecruiter uk') || lower.includes('ziprecruiter.org') || lower.includes('about us careers investors')) {
                const sections = descriptionText.split(/\n\n+/);
                const kept = sections.filter((s) => s.length > 50 && !/ziprecruiter uk|ziprecruiter\.org|about us careers/i.test(s));
                if (kept.join('\n\n').trim().length > 100) {
                    descriptionText = CLEAN(kept.join('\n\n'));
                    descriptionHtml = kept.map((p) => `<p>${CLEAN(p)}</p>`).join('');
                } else {
                    descriptionText = null; descriptionHtml = null;
                }
            }
        }
    }
    if (descriptionHtml) out.description_html = descriptionHtml;
    if (descriptionText) out.description_text = descriptionText;
    if (!out.description_text && out.description_html) out.description_text = htmlToText(out.description_html);

    out.posted_text = CLEAN($('time[datetime], .posted, .posted-date, .t_posted').first().text()) || null;
    out.employment_type = CLEAN($('.employment-type, [data-employment-type]').first().text()) || null;

    const salaryDetailText = CLEAN($('.salary, .compensation, .pay, [data-salary], .job_salary, .job-salary').first().text()) || null;
    if (salaryDetailText) {
        const parsed = parseSalary(salaryDetailText);
        if (parsed) {
            if (!out.salary_raw) out.salary_raw = parsed.raw;
            if (toNumber(parsed.min) !== null) out.salary_min = parsed.min;
            if (toNumber(parsed.max) !== null) out.salary_max = parsed.max;
            if (parsed.period && !out.salary_period) out.salary_period = parsed.period;
        }
    }
    if (out.posted_text) {
        const g = guessPosted(out.posted_text);
        if (g?.relative) out.posted_relative = g.relative;
    }

    const jp = extractJsonLd($);
    if (jp) {
        if (!out.title && jp.title) out.title = CLEAN(jp.title);
        const jpCompany = sanitizeCompanyName(jp.hiringOrganization?.name);
        if (jpCompany && !out.company) out.company = jpCompany;
        if (!out.employment_type && jp.employmentType) out.employment_type = CLEAN(jp.employmentType);
        if (!out.location && jp.jobLocation?.address) {
            const a = jp.jobLocation.address;
            out.location = CLEAN([a.addressLocality, a.addressRegion, a.addressCountry].filter(Boolean).join(', '));
        }
        const jsonDescRaw = STR(jp.description);
        if (jsonDescRaw && !out.description_text) {
            const text = htmlToText(jsonDescRaw);
            if (text && text.length > 100) {
                out.description_text = text;
                if (!out.description_html) {
                    const hasTags = /<[^>]+>/.test(jsonDescRaw);
                    if (hasTags) out.description_html = jsonDescRaw;
                    else {
                        const parts = jsonDescRaw.split(/\r?\n{2,}/).map((seg) => seg.trim()).filter(Boolean);
                        out.description_html = parts.length ? `<p>${parts.join('</p><p>')}</p>` : `<p>${jsonDescRaw}</p>`;
                    }
                }
            }
        }
        if (jp.datePosted) out.date_posted_iso = jp.datePosted;
        if (jp.directApply !== undefined) out.direct_apply = Boolean(jp.directApply);
        if (jp.validThrough) out.valid_through_iso = jp.validThrough;

        if (jp.baseSalary) {
            const base = jp.baseSalary;
            const value = base && typeof base === 'object' ? base.value ?? base : {};
            const salaryText = value && typeof value === 'object' ? (value.text ?? base.text ?? null) : (base.text ?? null);
            if (salaryText && !out.salary_raw) out.salary_raw = CLEAN(salaryText);
            const minCandidate = value && typeof value === 'object' ? (value.minValue ?? value.value ?? null) : null;
            const maxCandidate = value && typeof value === 'object' ? (value.maxValue ?? null) : null;
            const minNumber = toNumber(minCandidate);
            const maxNumber = toNumber(maxCandidate);
            if (minNumber !== null) out.salary_min = minNumber;
            if (maxNumber !== null) out.salary_max = maxNumber;
            const currency = value && typeof value === 'object' ? (value.currency ?? base.currency ?? null) : (base.currency ?? null);
            if (currency) out.salary_currency = CLEAN(currency);
            const unit = value && typeof value === 'object' ? (value.unitText ?? base.unitText ?? null) : (base.unitText ?? null);
            if (unit) out.salary_period = CLEAN(unit).toLowerCase();
        }
    }

    if (out.company) out.company = sanitizeCompanyName(out.company);
    if (!out.description_text && out.description_html) out.description_text = htmlToText(out.description_html);
    if (!out.description_html && out.description_text && out.description_text.length > 50) {
        out.description_html = `<p>${out.description_text}</p>`;
    }

    out.detail_url = loadedUrl;
    return out;
};

// =============== URL builders ===============
const buildJobsUrl = (kw, loc, postedWithinDays) => {
    const slug = (kw || 'Jobs').trim().replace(/\s+/g, '-').replace(/[^A-Za-z0-9-]/g, '');
    let path = `/${encodeURIComponent(slug)}`;
    if (loc && loc.trim()) path += `/-in-${encodeURIComponent(loc.trim())}`;
    const url = new URL(`https://www.ziprecruiter.com/Jobs${path}`);
    if (postedWithinDays) url.searchParams.set('days', String(postedWithinDays));
    return url.href;
};
const buildCandidateSearchUrl = (kw, loc, postedWithinDays) => {
    const u = new URL('https://www.ziprecruiter.com/candidate/search');
    if (kw && kw.trim()) u.searchParams.set('search', kw.trim());
    if (loc && loc.trim()) u.searchParams.set('location', loc.trim());
    if (postedWithinDays) u.searchParams.set('days', String(postedWithinDays));
    return u.href;
};

// =============== Actor main ===============
await Actor.init();

const input = (await Actor.getInput()) || {};
let {
    startUrl,
    keyword = 'Administrative Assistant',
    location = '',
    postedWithin = 'any',
    results_wanted = 100,
    collect_details = true,
    maxConcurrency = 10,
    maxRequestRetries = 2,
    proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'], countryCode: 'US' },
    requestHandlerTimeoutSecs = 35,
    downloadIntervalMs: downloadIntervalMsInput = null,
    preferCandidateSearch = false,
    // optional caps / aliases (no schema change)
    maxJobs,
    max_jobs,
    // optional: allow user to set cap; otherwise we derive a safe ceiling
    maxRequestsPerCrawl = null,
} = input;

// normalize cap aliases
const aliasCap = toNumber(maxJobs ?? max_jobs);
if (aliasCap && aliasCap > 0) results_wanted = aliasCap;

const postedWithinDays = resolvePostedWithin(postedWithin);

if (startUrl) startUrl = ensureCorrectDomain(startUrl);
const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);
const downloadIntervalMs = downloadIntervalMsInput ?? (collect_details ? 200 : 100);

let START_URL = startUrl?.trim()
    || (preferCandidateSearch ? buildCandidateSearchUrl(keyword, location, postedWithinDays) : buildJobsUrl(keyword, location, postedWithinDays));

const postedLabel = postedWithinDays ? `<=${postedWithinDays}d` : 'any';
log.info(`ZipRecruiter: ${START_URL} | details: ${collect_details ? 'ON' : 'OFF'} | target: ${results_wanted} | posted: ${postedLabel}`);

const STEALTH_CONCURRENCY_CAP = collect_details ? 6 : 8;
const effectiveMaxConcurrency = Math.max(2, Math.min(maxConcurrency, STEALTH_CONCURRENCY_CAP));
if (effectiveMaxConcurrency !== maxConcurrency) {
    log.info(`Stealth tuning: capping maxConcurrency ${maxConcurrency} -> ${effectiveMaxConcurrency}`);
}
maxConcurrency = effectiveMaxConcurrency;

// ===== run state =====
let pushed = 0;
const SEEN_URLS = new Set();
const QUEUED_DETAILS = new Set();
let pagesProcessed = 0;
let listPagesQueued = 1;
const MAX_PAGES = 50;
const MAX_STALL_RECOVERY = 4;
let stallRecoveryAttempts = 0;
let lastListUrlNorm = null;
const CARD_CACHE = new Map();

// PATCH: derived ceiling to prevent "requests storm" when site loops
const MAX_REQS = Number.isFinite(maxRequestsPerCrawl) && maxRequestsPerCrawl > 0
    ? maxRequestsPerCrawl
    : Math.max(200, results_wanted * 3);

// reset loop trackers
noProgressPages = 0;
PAGINATION_URLS_SEEN.clear();

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxConcurrency,
    maxRequestRetries: Math.max(3, maxRequestRetries + 2), // Add headroom for stealth backoff
    maxRequestsPerCrawl: MAX_REQS,
    requestHandlerTimeoutSecs,
    navigationTimeoutSecs: requestHandlerTimeoutSecs + 10,
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: {
        maxPoolSize: Math.max(48, effectiveMaxConcurrency * 14),
        sessionOptions: {
            maxUsageCount: 12,
            maxAgeSecs: 420,
            errorScoreDecrement: 0.5,
            maxErrorScore: 2.2,
        },
    },
    autoscaledPoolOptions: { maybeRunIntervalSecs: 0.3, minConcurrency: 2 },

    preNavigationHooks: [
        async (ctx) => {
            const { request, session, proxyInfo } = ctx;

            // honor cap on details
            if (request.userData?.label === 'DETAIL' && pushed >= results_wanted) {
                request.skipNavigation = true;
                return;
            }

            if (proxyInfo?.isApifyProxy && session?.id) {
                request.proxy = { ...(request.proxy || {}), session: session.id };
            }

            // Enhanced session-based UA rotation
            if (session) {
                const usageCount = session.usageCount ?? 0;
                session.userData = session.userData || {};
                if (!session.userData.uaProfile || usageCount > 8) {
                    session.userData.uaProfile = pickUAProfile();
                }
                if (!session.userData.acceptLanguage) {
                    session.userData.acceptLanguage = pickOne(ACCEPT_LANGUAGE_POOL);
                }
                if (usageCount > 10 && Math.random() < 0.35) {
                    session.retire();
                }
            }

            const profile = session?.userData?.uaProfile || pickUAProfile();
            const ua = profile.ua;
            const acceptLang = session?.userData?.acceptLanguage || pickOne(ACCEPT_LANGUAGE_POOL);
            const referer = ensureRefererChain(request);
            const fetchSite = computeFetchSite(request.url, referer);

            const headers = {
                'user-agent': ua,
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'accept-language': acceptLang,
                'accept-encoding': 'gzip, deflate, br',
                'upgrade-insecure-requests': '1',
                'cache-control': 'max-age=0',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': fetchSite,
                'sec-fetch-user': '?1',
                'referer': referer,
            };

            for (const [key, value] of Object.entries(profile.ch || {})) {
                if (value !== undefined && value !== null) headers[key] = value;
            }

            // Remove undefined headers
            Object.keys(headers).forEach(key => headers[key] === undefined && delete headers[key]);

            if (ctx.gotOptions) ctx.gotOptions.headers = { ...(ctx.gotOptions.headers || {}), ...headers };
            if (ctx.requestOptions) ctx.requestOptions.headers = { ...(ctx.requestOptions.headers || {}), ...headers };
            request.headers = { ...(request.headers || {}), ...headers };

            const label = request.userData?.label || 'DEFAULT';
            const retryCount = request.retryCount || 0;
            const delayMs = computeDelayMs(label, downloadIntervalMs, retryCount);
            await sleep(delayMs);
        },
    ],

    requestHandler: async (ctx) => {
        const { request, $, enqueueLinks, session, response } = ctx;
        const { label } = request.userData;

        // Enhanced block detection with session rotation
        if (response?.statusCode === 403 || response?.statusCode === 429) {
            log.warning(`${response.statusCode} on ${request.url} — rotating session ${session?.id}`);
            if (session) {
                session.retire();
                session.markBad();
            }
            throw new Error(`Blocked (${response.statusCode})`);
        }
        
        const bodyText = ($('title').text() + ' ' + $('.error, .captcha, #challenge-running, .cf-error-details').text()).toLowerCase();
        if (bodyText.includes('blocked') || bodyText.includes('access denied') || bodyText.includes('verify you are a human') || bodyText.includes('cloudflare')) {
            log.warning(`Bot detection on ${request.url} — rotating session ${session?.id}`);
            if (session) {
                session.retire();
                session.markBad();
            }
            throw new Error('Blocked (bot detection)');
        }

        if (!label || label === 'LIST') {
            const baseUrl = request.loadedUrl ?? request.url;
            pagesProcessed++;

            // normalize and mark this list page as seen for loop prevention
            const baseUrlNorm = normalizeListUrl(baseUrl);
            PAGINATION_URLS_SEEN.add(baseUrlNorm);
            lastListUrlNorm = baseUrlNorm;

            // parse cards
            const cards = scrapeCards($, baseUrl);

            // soft warn only; do NOT abort crawl
            if (cards.length === 0) {
                log.warning(`⚠ No job cards parsed on page ${pagesProcessed}. URL: ${baseUrl}`);
            }

            let newAdded = 0;

            for (const card of cards) {
                const norm = normalizeJobUrl(card.url);
                if (SEEN_URLS.has(norm)) continue;
                SEEN_URLS.add(norm);
                newAdded++;

                if (!collect_details) {
                    const record = buildOutputRecord({
                        searchUrl: START_URL,
                        scrapedAt: new Date().toISOString(),
                        url: norm,
                        referer: baseUrl,
                        card,
                    });
                    await Dataset.pushData(record);
                    pushed++;
                } else {
                    if (!QUEUED_DETAILS.has(norm)) {
                        QUEUED_DETAILS.add(norm);
                        CARD_CACHE.set(norm, {
                            card,
                            referer: baseUrl,
                            discoveredAt: new Date().toISOString(),
                        });
                        await enqueueLinks({
                            urls: [norm],
                            userData: { label: 'DETAIL', card, referer: baseUrl, originListUrl: baseUrl },
                        });
                    }
                }
            }

            log.info(`📄 LIST page ${pagesProcessed}: found ${cards.length} cards, new ${newAdded} → SEEN=${SEEN_URLS.size}, scraped=${pushed}/${results_wanted}`);

            // === PATCH: stall detector ===
            if (newAdded === 0) {
                noProgressPages += 1;
            } else {
                noProgressPages = 0;
                stallRecoveryAttempts = 0;
            }
            const stalled = noProgressPages >= STALL_LIMIT;

            // Enhanced pagination planning with higher lookahead
            const estimatedJobsPerPage = cards.length > 0 ? cards.length : 20;
            const desiredSeen = collect_details
                ? Math.max(results_wanted + 20, Math.ceil(results_wanted * 1.3))
                : results_wanted;
            const remainingNeeded = desiredSeen - SEEN_URLS.size;
            const pagesNeeded = Math.ceil(Math.max(0, remainingNeeded) / Math.max(1, estimatedJobsPerPage));
            const pagesToQueue = Math.min(6, Math.max(0, pagesNeeded)); // wider lookahead for pagination stability

            const stillNeedJobs = pushed < results_wanted;
            const coverageShort = SEEN_URLS.size < desiredSeen;
            const shouldContinue = (coverageShort || stillNeedJobs) &&
                                   pagesProcessed < MAX_PAGES &&
                                   listPagesQueued < MAX_PAGES * 2 &&
                                   !stalled;

            if (shouldContinue && pagesToQueue > 0) {
                let currentUrl = baseUrl;
                let successfulQueues = 0;
                let lastFoundNextUrl = null;

                for (let i = 0; i < pagesToQueue; i++) {
                    const nextUrlRaw = i === 0 ? findNextPage($, currentUrl) : findNextPageByUrlOnly(currentUrl);
                    const nextUrl = nextUrlRaw ? normalizeListUrl(nextUrlRaw) : null;
                    lastFoundNextUrl = nextUrl;

                    const currentNorm = normalizeListUrl(currentUrl);
                    const isLoop = !nextUrl || nextUrl === currentNorm || PAGINATION_URLS_SEEN.has(nextUrl);

                    if (!isLoop) {
                        listPagesQueued++;
                        PAGINATION_URLS_SEEN.add(nextUrl);

                        await enqueueLinks({
                            urls: [nextUrl],
                            userData: { label: 'LIST', referer: currentUrl },
                            forefront: i < 2, // Prioritize first 2 pages
                            transformRequestFunction: (req) => {
                                // stable dedupe key for list pages
                                req.uniqueKey = `${normalizeListUrl(req.url)}#LIST`;
                                return req;
                            },
                        });

                        if (i === 0) {
                            log.info(`Next page queued (#${listPagesQueued}): ${nextUrl.substring(0, 120)}...`);
                        }

                        currentUrl = nextUrl;
                        successfulQueues++;
                    } else {
                        break;
                    }
                }

                if (successfulQueues === 0) {
                    if (stalled) {
                        log.warning(`Pagination stalled (no new jobs for ${noProgressPages} pages).`);
                    } else if (lastFoundNextUrl && PAGINATION_URLS_SEEN.has(lastFoundNextUrl)) {
                        log.warning('Pagination loop detected - URL already seen');
                    } else {
                        log.warning(`No next page found after page ${pagesProcessed}. SEEN=${SEEN_URLS.size}, target=${results_wanted}`);
                    }

                    const alt = tryAlternativePagination(baseUrl, pagesProcessed);
                    const altNorm = alt ? normalizeListUrl(alt) : null;
                    if (altNorm && !PAGINATION_URLS_SEEN.has(altNorm) && !stalled && pushed < results_wanted) {
                        listPagesQueued++;
                        PAGINATION_URLS_SEEN.add(altNorm);
                        log.info(`Alternative page ${listPagesQueued}: ${altNorm.substring(0, 120)}...`);
                        await enqueueLinks({
                            urls: [altNorm],
                            userData: { label: 'LIST', referer: baseUrl },
                            forefront: true,
                            transformRequestFunction: (req) => { req.uniqueKey = `${normalizeListUrl(req.url)}#LIST`; return req; },
                        });
                    }
                }
            } else {
                if (stalled && (coverageShort || stillNeedJobs)) {
                    if (stallRecoveryAttempts < MAX_STALL_RECOVERY) {
                        stallRecoveryAttempts++;
                        const shortfall = Math.max(results_wanted - pushed, desiredSeen - SEEN_URLS.size);
                        const forcePages = Math.min(4, Math.max(1, Math.ceil(shortfall / Math.max(1, estimatedJobsPerPage))));
                        let forcedCurrent = lastListUrlNorm || baseUrlNorm;
                        let forcedQueued = 0;
                        for (let i = 0; i < forcePages; i++) {
                            const forcedRaw = findNextPageByUrlOnly(forcedCurrent);
                            if (!forcedRaw) break;
                            const forcedNorm = normalizeListUrl(forcedRaw);
                            forcedCurrent = forcedNorm;
                            if (PAGINATION_URLS_SEEN.has(forcedNorm)) continue;

                            listPagesQueued++;
                            PAGINATION_URLS_SEEN.add(forcedNorm);
                            await enqueueLinks({
                                urls: [forcedNorm],
                                userData: { label: 'LIST', referer: baseUrl },
                                forefront: i === 0,
                                transformRequestFunction: (req) => {
                                    req.uniqueKey = `${normalizeListUrl(req.url)}#LIST`;
                                    return req;
                                },
                            });
                            forcedQueued++;
                        }

                        if (forcedQueued > 0) {
                            log.info(`Stall recovery attempt ${stallRecoveryAttempts}/${MAX_STALL_RECOVERY}: queued ${forcedQueued} fallback page(s).`);
                            noProgressPages = Math.max(0, Math.floor(STALL_LIMIT / 2));
                            return;
                        }
                    }
                    log.info(`Stopping pagination after ${noProgressPages} empty pages (stall).`);
                    noProgressPages = 0;
                } else if (pagesProcessed >= MAX_PAGES) {
                    log.warning(`Reached MAX_PAGES limit (${MAX_PAGES}).`);
                } else if (pagesToQueue <= 0) {
                    log.info('Enough jobs collected/queued for target buffer.');
                } else if (pushed >= results_wanted) {
                    log.info(`Job cap reached (${pushed}/${results_wanted}). Not queuing more pages.`);
                } else {
                    log.info(`Target buffer reached: SEEN=${SEEN_URLS.size} (target ${results_wanted}).`);
                }
            }

            return;
        }

        if (label === 'DETAIL') {
            if (pushed >= results_wanted) return;

            const base = request.loadedUrl ?? request.url;
            const detail = scrapeDetail($, base);
            const normalizedUrl = normalizeJobUrl(base);
            const record = buildOutputRecord({
                searchUrl: START_URL,
                scrapedAt: new Date().toISOString(),
                url: normalizedUrl,
                referer: request.userData.referer,
                card: request.userData.card || {},
                detail,
            });
            CARD_CACHE.delete(normalizedUrl);
            await Dataset.pushData(record);
            pushed++;

            if (pushed % 25 === 0 || pushed === results_wanted) {
                log.info(`📊 Progress: ${pushed}/${results_wanted} jobs scraped (${((pushed / results_wanted) * 100).toFixed(1)}%)`);
            }
            return;
        }
    },

    failedRequestHandler: async ({ request, error, session }) => {
        const statusCode = error?.statusCode || error?.response?.statusCode;
        const is403or429 = statusCode === 403 || statusCode === 429;
        
        if (is403or429 && session) {
            log.warning(`Blocking error ${statusCode} for ${request.url} - session ${session.id} retired`);
            session.retire();
        } else {
            log.warning(`FAILED ${request.url}: ${error?.message || error}`);
            if (session) session.markBad();
        }
        
        await Dataset.pushData({
            type: 'error',
            url: request.url,
            message: String(error?.message || error),
            statusCode: statusCode || null,
            label: request.userData?.label || null,
            at: new Date().toISOString(),
        });
    },

});

log.info(`🚀 Starting crawler with target: ${results_wanted} jobs`);
log.info(`📍 Start URL: ${START_URL}`);

await crawler.run([{ url: START_URL, userData: { label: 'LIST', referer: buildSearchReferer() } }]);

const { emitted: fallbackEmitted } = await flushPendingCardsToDataset({
    collectDetails: collect_details,
    targetCount: results_wanted,
    searchUrl: START_URL,
    currentCount: pushed,
});
if (fallbackEmitted > 0) {
    pushed += fallbackEmitted;
    log.info(`Fallback emitted ${fallbackEmitted} card-only record(s) to approach requested total.`);
}

// =============== Final report ===============
const finalCount = pushed;
const successRate = SEEN_URLS.size > 0 ? ((finalCount / SEEN_URLS.size) * 100).toFixed(1) : '0.0';

log.info(`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ SCRAPING COMPLETED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  🎯 Target:          ${results_wanted} jobs
  ✅ Scraped:         ${finalCount} jobs
  🔗 Unique URLs:     ${SEEN_URLS.size}
  📄 Pages Crawled:   ${pagesProcessed}
  📋 Pages Queued:    ${listPagesQueued}
  📊 Success Rate:    ${successRate}%
  ⚙️  Details Mode:    ${collect_details ? 'ON' : 'OFF'}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);

if (finalCount < results_wanted) {
    const ratio = (finalCount / results_wanted * 100).toFixed(0);
    log.warning(`⚠️  Only scraped ${finalCount}/${results_wanted} jobs (${ratio}%)`);
    log.warning(`Possible reasons:`);
    log.warning(`  • Not enough jobs available for this query`);
    log.warning(`  • Pagination stopped (loop/stall guarded)`);
    log.warning(`  • Some detail pages failed (see error logs)`);
    log.warning(`  • Site structure may have changed`);
}

await Actor.exit();

