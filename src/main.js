// src/main.js
// ZipRecruiter — HTTP-only (CheerioCrawler) with strong anti-block hardening.
// - Per-session UA + sticky residential proxy
// - Retire session on 403 and bot pages
// - Referer chaining (list -> list, list -> detail)
// - Optional detail skipping to reduce block risk
// - Conservative concurrency + jitter

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ABS = (href, base) => { try { return new URL(href, base).href; } catch { return null; } };
const STR = (x) => (x ?? '').toString().trim();
const CLEAN = (s) => STR(s).replace(/\s+/g, ' ').trim();

const UA_POOL = [
  // modern, plausible desktop UAs; rotated per session
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
];

const pickUA = () => UA_POOL[Math.floor(Math.random() * UA_POOL.length)];

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
      } catch { /* ignore */ }
    }
  } catch {}
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
    } catch {}
  });
  if (best) return best.url;

  // heuristic increment
  cur.searchParams.set('p', String(curP + 1));
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
  return jobs.filter((j) => j.url && !seen.has(j.url) && seen.add(j.url));
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

await Actor.init();

const input = (await Actor.getInput()) || {};
const {
  startUrl,
  keyword = 'Admin',
  location = '',
  results_wanted = 100,
  collect_details = true,        // set false to avoid detail-page 403s if needed
  maxConcurrency = 4,            // conservative
  maxRequestRetries = 2,         // retire sessions quickly
  proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] },
  requestHandlerTimeoutSecs = 35,
  downloadIntervalMs = 350,      // jitter to reduce 403s
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

const buildStartUrl = (kw, loc) => {
  const searchSlug = kw ? kw.trim().replace(/\s+/g, '-').replace(/[^A-Za-z0-9-]/g, '') : 'Jobs';
  let path = `/${encodeURIComponent(searchSlug)}`;
  if (loc && loc.trim()) path += `/-in-${encodeURIComponent(loc.trim())}`;
  return `https://www.ziprecruiter.com/Jobs${path}`;
};

const START_URL = startUrl?.trim() ? startUrl.trim() : buildStartUrl(keyword, location);
log.info(`ZipRecruiter from: ${START_URL} | details: ${collect_details ? 'ON' : 'OFF'} | target: ${results_wanted}`);

let pushed = 0;

const crawler = new CheerioCrawler({
  proxyConfiguration: proxyConfig,
  maxConcurrency,
  maxRequestRetries,
  requestHandlerTimeoutSecs,
  navigationTimeoutSecs: requestHandlerTimeoutSecs,
  useSessionPool: true,
  persistCookiesPerSession: true,
  sessionPoolOptions: {
    maxPoolSize: 40,
    sessionOptions: { maxUsageCount: 25 }, // rotate even if not blocked
  },

  // 403/bot-page detector BEFORE handler runs
  postResponseHooks: [
    async ({ session, response, body, request }) => {
      // Hard 403 from server
      if (response?.statusCode === 403) {
        log.warning(`403 on ${request.url} — retiring session ${session?.id}`);
        if (session) session.markBad();
        throw new Error('Blocked (403)');
      }
      // Soft bot pages (HTML says “Request blocked” / “Access Denied” / “verify you are human”)
      const text = typeof body === 'string' ? body : '';
      if (text) {
        const lc = text.toLowerCase();
        if (lc.includes('request blocked') || lc.includes('access denied') || lc.includes('verify you are a human')) {
          log.warning(`Bot page on ${request.url} — retiring session ${session?.id}`);
          if (session) session.markBad();
          throw new Error('Blocked (bot page)');
        }
      }
    },
  ],

  // Set realistic headers & sticky proxy session per Crawlee session; add referer chaining; pace requests
  preNavigationHooks: [
    async (ctx) => {
      const { request, session, proxyInfo } = ctx;

      // Attach sticky Apify proxy session per Crawlee session id
      if (proxyInfo?.isApifyProxy && session?.id) {
        request.proxy = { ...(request.proxy || {}), session: session.id };
      }

      // One UA per session
      if (session && !session.userData.ua) session.userData.ua = pickUA();
      const ua = session?.userData?.ua || pickUA();

      // Referer chaining: detail pages refer to their list; next list refers to previous page
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
      };

      // Apply headers defensively on any options object Crawlee may use
      if (ctx.gotOptions) ctx.gotOptions.headers = { ...(ctx.gotOptions.headers || {}), ...headers };
      if (ctx.requestOptions) ctx.requestOptions.headers = { ...(ctx.requestOptions.headers || {}), ...headers };
      request.headers = { ...(request.headers || {}), ...headers };

      // gentle pacing
      if (downloadIntervalMs) await sleep(downloadIntervalMs + Math.floor(Math.random() * 200));
    },
  ],

  requestHandler: async ({ request, $, enqueueLinks, session }) => {
    const { label } = request.userData;

    if (!label || label === 'LIST') {
      const baseUrl = request.loadedUrl ?? request.url;

      // SCRAPE CARDS
      const cards = scrapeCards($, baseUrl);
      log.info(`LIST ${baseUrl} -> ${cards.length} cards`);

      for (const card of cards) {
        if (pushed >= results_wanted) break;

        if (collect_details) {
          await enqueueLinks({
            urls: [card.url],
            userData: { label: 'DETAIL', card, referer: baseUrl },
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

      // PAGINATION
      if (pushed < results_wanted) {
        const nextUrl = findNextPage($, baseUrl);
        if (nextUrl && nextUrl !== baseUrl) {
          log.info(`NEXT -> ${nextUrl}`);
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

      await Dataset.pushData({
        source: 'ziprecruiter',
        scraped_at: new Date().toISOString(),
        search_url: START_URL,
        ...request.userData.card,
        ...detail,
      });
      pushed++;
      return;
    }
  },

  failedRequestHandler: async ({ request, error, session }) => {
    log.warning(`FAILED ${request.url}: ${error?.message || error}`);
    if (session) session.markBad(); // ensure bad identities are retired
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
await crawler.run([{ url: START_URL, userData: { label: 'LIST', referer: 'https://www.ziprecruiter.com/' } }]);

log.info(`Done. Total jobs pushed: ${pushed}`);
await Actor.exit();

/*
Tips if you still see some 403s (normal on ZR, but recoverable with rotation):
- Lower maxConcurrency to 2–3 and raise downloadIntervalMs to 500–800.
- Ensure Apify residential proxy is enabled; avoid datacenter IPs.
- If a specific geo blocks, set your Apify proxy country to US and keep sticky sessions on.

Input example:
{
  "startUrl": "https://www.ziprecruiter.com/Jobs/Admin",
  "results_wanted": 60,
  "collect_details": false,
  "proxyConfiguration": { "useApifyProxy": true, "apifyProxyGroups": ["RESIDENTIAL"] },
  "maxConcurrency": 3,
  "downloadIntervalMs": 500
}
*/
