// src/main.js
// ZipRecruiter — HTTP-only (CheerioCrawler) with robust pagination to reach target counts.
// - Global de-dup across pages
// - Paginate until SEEN >= results_wanted (not only pushed)
// - Works for /Jobs/* (?p=) and /candidate/search (?page=)
// - Cookie warmup, sticky residential proxy, per-session UA, referer chaining
// - JSON-LD surfaced (date_posted_iso, description_text, direct_apply)

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const STR = (x) => (x ?? '').toString().trim();
const CLEAN = (s) => STR(s).replace(/\s+/g, ' ').trim();
const htmlToText = (html) => {
  const raw = STR(html);
  if (!raw) return null;
  return CLEAN(raw.replace(/<[^>]*>/g, ' '));
};
const pickValue = (...values) => {
  for (const val of values) {
    if (val === undefined || val === null) continue;
    if (typeof val === 'string') {
      const str = CLEAN(val);
      if (str) return str;
    } else if (typeof val === 'number') {
      return val;
    } else if (typeof val === 'boolean') {
      return val;
    }
  }
  return null;
};
const toNumber = (value) => {
  if (value === undefined || value === null) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};
const extractJobId = (url) => {
  try {
    return new URL(url).searchParams.get('jid') || null;
  } catch {
    return null;
  }
};
const buildOutputRecord = ({ searchUrl, scrapedAt, url, referer, card = {}, detail = {} }) => {
  const title = pickValue(detail.title, card.title);
  const company = pickValue(detail.company, card.company);
  const location = pickValue(detail.location, card.location);
  const employmentType = pickValue(detail.employment_type, card.employment_type);
  const description = pickValue(detail.description_text, card.description_text);
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
    description: description || null,
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

const UA_POOL = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
];
const pickUA = () => UA_POOL[Math.floor(Math.random() * UA_POOL.length)];

// ---------- URL helpers ----------
const ABS = (href, base) => { try { return new URL(href, base).href; } catch { return null; } };

// Make the same job URL canonical even if params order differs.
// Keep important params like jid; drop tracking and fragments.
const normalizeJobUrl = (u) => {
  try {
    const url = new URL(u);
    url.hash = '';
    // keep only a conservative allowlist of params
    const keep = new Set(['jid', 'mid']);
    const kept = new URLSearchParams();
    for (const [k, v] of url.searchParams.entries()) if (keep.has(k)) kept.set(k, v);
    url.search = kept.toString();
    return url.href;
  } catch { return u; }
};

// -------- extraction helpers --------
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

// -------- pagination detection --------
const findNextPage = ($, baseUrl) => {
  // 1) rel=next
  let href = $('link[rel="next"]').attr('href');
  if (href) return ABS(href, baseUrl);

  // 2) a[rel/aria-label=Next]
  const aNext = $('a[rel="next"], a[aria-label="Next"], a[aria-label="next"]').attr('href');
  if (aNext) return ABS(aNext, baseUrl);

  // 3) numeric controls; handle both ?p= and ?page=
  const cur = new URL(baseUrl);
  const pageKeys = ['p', 'page'];
  const getCur = () => {
    for (const k of pageKeys) {
      const n = Number(cur.searchParams.get(k) || '1');
      if (n) return { key: k, val: n };
    }
    return { key: 'p', val: 1 };
  };
  const curInfo = getCur();

  // pick the smallest page greater than current among anchors
  let best = null;
  $('a[href]').each((_, a) => {
    const url = ABS($(a).attr('href'), baseUrl);
    if (!url) return;
    try {
      const u = new URL(url);
      for (const key of pageKeys) {
        const p = Number(u.searchParams.get(key) || '0');
        if (p > curInfo.val && (!best || p < best.p)) best = { p, url };
      }
    } catch {}
  });
  if (best) return best.url;

  // 4) heuristic increment (try current key, then the other one)
  const tryInc = (key) => { const u = new URL(baseUrl); u.searchParams.set(key, String((Number(u.searchParams.get(key) || '1') || 1) + 1)); return u.href; };
  return cur.searchParams.has(curInfo.key) ? tryInc(curInfo.key) : tryInc(curInfo.key === 'p' ? 'page' : 'p');
};

// -------- card/detail scraping --------
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
    const href0 = $a.attr('href');
    const href = ABS(href0, baseUrl);
    if (!href) return;
    if (!/\/(c|job|jobs)\//i.test(href)) return;

    let title = CLEAN($a.text()) || CLEAN($a.find('h2,h3').first().text());
    const $card = $a.closest('article, li, div').first();
    const textBlob = CLEAN(($card.text() || '').slice(0, 800));

    let company = CLEAN($card.find('a.t_org_link, .t_org_link, .company, [data-company]').first().text()) || null;
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

  return jobs;
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
  const descriptionHtml = STR(descNode.html()) || null;
  out.description_text = CLEAN(descNode.text()) || htmlToText(descriptionHtml);

  out.posted_text = CLEAN($('time[datetime], .posted, .posted-date, .t_posted').first().text()) || null;
  out.employment_type = CLEAN($('.employment-type, [data-employment-type]').first().text()) || null;

  const salaryDetailText = CLEAN($('.salary, .compensation, .pay, [data-salary], .job_salary, .job-salary').first().text()) || null;
  if (salaryDetailText) {
    const parsed = parseSalary(salaryDetailText);
    if (parsed) {
      if (!out.salary_raw) out.salary_raw = parsed.raw;
      if (parsed.min !== undefined && parsed.min !== null && toNumber(parsed.min) !== null) out.salary_min = parsed.min;
      if (parsed.max !== undefined && parsed.max !== null && toNumber(parsed.max) !== null) out.salary_max = parsed.max;
      if (parsed.period && !out.salary_period) out.salary_period = parsed.period;
    }
  }

  if (out.posted_text) {
    const detailPostedGuess = guessPosted(out.posted_text);
    if (detailPostedGuess?.relative) out.posted_relative = detailPostedGuess.relative;
  }

  const jp = extractJsonLd($);
  if (jp) {
    if (!out.title && jp.title) out.title = CLEAN(jp.title);
    if (!out.company && jp.hiringOrganization?.name) out.company = CLEAN(jp.hiringOrganization.name);
    if (!out.description_text && jp.description) out.description_text = htmlToText(jp.description);
    if (!out.employment_type && jp.employmentType) out.employment_type = CLEAN(jp.employmentType);
    if (!out.location && jp.jobLocation?.address) {
      const a = jp.jobLocation.address;
      out.location = CLEAN([a.addressLocality, a.addressRegion, a.addressCountry].filter(Boolean).join(', '));
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

  out.detail_url = loadedUrl;
  return out;
};

// -------- fallback URL builders --------
const buildJobsUrl = (kw, loc) => {
  const slug = (kw || 'Jobs').trim().replace(/\s+/g, '-').replace(/[^A-Za-z0-9-]/g, '');
  let path = `/${encodeURIComponent(slug)}`;
  if (loc && loc.trim()) path += `/-in-${encodeURIComponent(loc.trim())}`;
  return `https://www.ziprecruiter.com/Jobs${path}`;
};
const buildCandidateSearchUrl = (kw, loc) => {
  const u = new URL('https://www.ziprecruiter.com/candidate/search');
  if (kw && kw.trim()) u.searchParams.set('search', kw.trim());
  if (loc && loc.trim()) u.searchParams.set('location', loc.trim());
  return u.href;
};

// ---------------------------
// Main
// ---------------------------

await Actor.init();

const input = (await Actor.getInput()) || {};
const {
  startUrl,
  keyword = 'Administrative Assistant',
  location = '',
  results_wanted = 100,
  collect_details = true,
  maxConcurrency = 2,
  maxRequestRetries = 2,
  proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'], countryCode: 'US' },
  requestHandlerTimeoutSecs = 35,
  downloadIntervalMs: downloadIntervalMsInput = null,
  preferCandidateSearch = false,     // set true to always use /candidate/search
} = input;

const downloadIntervalMs = downloadIntervalMsInput ?? (collect_details ? 600 : 320);

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// Decide effective start URL
let START_URL = startUrl?.trim()
  || (preferCandidateSearch ? buildCandidateSearchUrl(keyword, location) : buildJobsUrl(keyword, location));

log.info(`ZipRecruiter start: ${START_URL} | details: ${collect_details ? 'ON' : 'OFF'} | target: ${results_wanted}`);

let pushed = 0;
const SEEN_URLS = new Set();     // global de-dup across pages
const QUEUED_DETAILS = new Set();// avoid re-enqueueing same detail

const crawler = new CheerioCrawler({
  proxyConfiguration: proxyConfig,
  maxConcurrency,
  maxRequestRetries,
  requestHandlerTimeoutSecs,
  navigationTimeoutSecs: requestHandlerTimeoutSecs,
  useSessionPool: true,
  persistCookiesPerSession: true,
  sessionPoolOptions: {
    maxPoolSize: 30,
    sessionOptions: { maxUsageCount: 20 },
  },

  preNavigationHooks: [
    async (ctx) => {
      const { request, session, proxyInfo } = ctx;
      if (proxyInfo?.isApifyProxy && session?.id) {
        request.proxy = { ...(request.proxy || {}), session: session.id };
      }
      if (session && !session.userData.ua) session.userData.ua = pickUA();
      const ua = session?.userData?.ua || pickUA();
      const referer = request.userData?.referer || 'https://www.google.com/';
      const headers = {
        'user-agent': ua,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'upgrade-insecure-requests': '1',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'dnt': '1',
        'referer': referer,
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
      };
      if (ctx.gotOptions) ctx.gotOptions.headers = { ...(ctx.gotOptions.headers || {}), ...headers };
      if (ctx.requestOptions) ctx.requestOptions.headers = { ...(ctx.requestOptions.headers || {}), ...headers };
      request.headers = { ...(request.headers || {}), ...headers };
      if (downloadIntervalMs) await sleep(downloadIntervalMs + Math.floor(Math.random() * 200));
    },
  ],

  requestHandler: async (ctx) => {
    const { request, $, enqueueLinks, session, response } = ctx;
    const { label } = request.userData;

    // basic block detection
    if (response?.statusCode === 403) {
      log.warning(`403 on ${request.url} — retire session ${session?.id}`);
      if (session) session.markBad();
      throw new Error('Blocked (403)');
    }
    const bodyText = ($('body').text() || '').toLowerCase();
    if (bodyText.includes('request blocked') || bodyText.includes('access denied') || bodyText.includes('verify you are a human')) {
      log.warning(`Bot page on ${request.url} — retire session ${session?.id}`);
      if (session) session.markBad();
      throw new Error('Blocked (bot page)');
    }

    // WARMUP just sets cookies then moves on
    if (label === 'WARMUP') {
      await enqueueLinks({ urls: [START_URL], userData: { label: 'LIST', referer: request.url } });
      return;
    }

    if (!label || label === 'LIST') {
      const baseUrl = request.loadedUrl ?? request.url;

      // SCRAPE cards and register globally
      const cards = scrapeCards($, baseUrl);
      let newAdded = 0;

      for (const card of cards) {
        const norm = normalizeJobUrl(card.url);
        if (SEEN_URLS.has(norm)) continue; // global dedupe
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
            await enqueueLinks({
              urls: [norm],
              userData: { label: 'DETAIL', card, referer: baseUrl },
            });
          }
        }

        if (SEEN_URLS.size >= results_wanted) break; // drive by SEEN count
      }

      log.info(`LIST ${baseUrl} -> cards=${cards.length}, new=${newAdded}, SEEN=${SEEN_URLS.size}, pushed=${pushed}`);

      // PAGINATION: continue while SEEN < target
      if (SEEN_URLS.size < results_wanted) {
        const nextUrl = findNextPage($, baseUrl);
        if (nextUrl && nextUrl !== baseUrl) {
          await enqueueLinks({ urls: [nextUrl], userData: { label: 'LIST', referer: baseUrl } });
        } else {
          log.info('No next page detected.');
        }
      }
      return;
    }

    if (label === 'DETAIL') {
      const base = request.loadedUrl ?? request.url;
      const detail = scrapeDetail($, base);
      const record = buildOutputRecord({
        searchUrl: START_URL,
        scrapedAt: new Date().toISOString(),
        url: normalizeJobUrl(base),
        referer: request.userData.referer,
        card: request.userData.card || {},
        detail,
      });
      await Dataset.pushData(record);
      pushed++;
      return;
    }
  },

  failedRequestHandler: async ({ request, error, session }) => {
    log.warning(`FAILED ${request.url}: ${error?.message || error}`);
    if (session) session.markBad();
    await Dataset.pushData({
      type: 'error',
      url: request.url,
      message: String(error?.message || error),
      label: request.userData?.label || null,
      at: new Date().toISOString(),
    });
  },
});

// Seed with cookie warmup, then actual start (same session/proxy)
await crawler.run([
  { url: 'https://www.ziprecruiter.com/', userData: { label: 'WARMUP', referer: 'https://www.google.com/' } },
]);

log.info(`Done. SEEN=${SEEN_URLS.size} pushed=${pushed}`);
await Actor.exit();

/*
Tips to guarantee 100:
- Keep "collect_details": false for volume-first (then re-run with details if needed).
- If a run still stalls near 50, set maxConcurrency=1 and downloadIntervalMs=900 temporarily to build the SEEN set.
- Prefer "preferCandidateSearch": true for friendlier SSR pagination (uses ?page=).
Input example:
{
  "keyword": "Automotive",
  "location": "Virginia, US",
  "results_wanted": 100,
  "collect_details": false,
  "preferCandidateSearch": true,
  "maxConcurrency": 1,
  "downloadIntervalMs": 900,
  "proxyConfiguration": { "useApifyProxy": true, "apifyProxyGroups": ["RESIDENTIAL"], "countryCode": "US" }
}
*/

