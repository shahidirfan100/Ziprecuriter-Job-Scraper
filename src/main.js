// src/main.js
// ZipRecruiter – HTTP-only scraping with Apify SDK + Crawlee (CheerioCrawler + got-scraping)
// Goal: FAST, STABLE, and RESILIENT — keep output schema/logic intact, no new dependencies.
// Changes:
//  - Confirm/correct target domain to ziprecruiter.com
//  - Keep-Alive HTTP agents + compression
//  - Tuned concurrency & polite pacing
//  - Smart retry/backoff for 429/5xx with jitter + Retry-After
//  - Reduced CPU/DOM work (narrow selectors, avoid heavy body scans)
//  - Strong pagination (already present) + no premature termination on empty first page
//  - Lightweight instrumentation: RPS, success/fail counts, timing (TTFB/download/parse), queue stats

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import http from 'http';
import https from 'https';

// ========= Utilities =========
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();
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
const ABS = (href, base) => { try { return new URL(href, base).href; } catch { return null; } };

// ======= Domain guard (spelling) =======
const ensureCorrectDomain = (url) => {
  if (!url) return url;
  try {
    const u = new URL(url);
    // Correct common typo 'ziprecuriters.com' -> 'ziprecruiter.com'
    if (u.hostname.replace(/^www\./, '') === 'ziprecuriters.com') {
      u.hostname = 'www.ziprecruiter.com';
      return u.href;
    }
    return url;
  } catch {
    return url;
  }
};

// ========= Output Builder =========
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

// ========= Config helpers =========
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

// ========= Company extraction / sanitization =========
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
  for (const attr of dataAttrs) {
    const val = sanitizeCompanyName($card.attr(attr));
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

// ========= Parsing helpers =========
const extractNodeHtml = ($node) => {
  if (!$node || $node.length === 0) return null;
  const clone = $node.clone();
  clone.find('script, style, noscript').remove();
  const html = STR(clone.html());
  return html || null;
};

const UA_POOL = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
];
const pickUA = () => UA_POOL[Math.floor(Math.random() * UA_POOL.length)];

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

// ========= Pagination =========
const findNextPage = ($, baseUrl) => {
  let href = $('link[rel="next"]').attr('href');
  if (href) {
    const nextUrl = ABS(href, baseUrl);
    if (nextUrl && nextUrl !== baseUrl) return nextUrl;
  }
  const nextSelectors = [
    'a[rel="next"]',
    'a[aria-label="Next"]',
    'a[aria-label="next"]',
    'a[aria-label*="Next"]',
    'a.next',
    'a[class*="next"]',
    'li.next a',
    '.pagination__next[href]',
    '[data-testid*="pagination-next"][href]',
    'a[data-testid*="PaginationNext"][href]',
    'a:contains("Next")',
    'a:contains("›")',
    'a:contains("»")',
  ];
  for (const sel of nextSelectors) {
    const aNext = $(sel).attr('href');
    if (aNext) {
      const nextUrl = ABS(aNext, baseUrl);
      if (nextUrl && nextUrl !== baseUrl) return nextUrl;
    }
  }
  // Buttons with data-url attributes
  const buttonCandidates = ['button[aria-label*="Next"]', 'button[class*="next"]', '[role="button"][aria-label*="Next"]'];
  for (const sel of buttonCandidates) {
    const $btn = $(sel).first();
    if ($btn?.length) {
      for (const attr of ['data-href', 'data-url', 'formaction']) {
        const v = $btn.attr(attr);
        if (v) {
          const nextUrl = ABS(v, baseUrl);
          if (nextUrl && nextUrl !== baseUrl) return nextUrl;
        }
      }
    }
  }
  // Param increment
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
      const v = Number(cur.searchParams.get(k));
      if (Number.isFinite(v)) {
        cur.searchParams.set(k, String(v + 1));
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
      if (!isNaN(v)) {
        cur.searchParams.set(k, String(v + 1));
        return cur.href;
      }
    }
    cur.searchParams.set('page', '2');
    return cur.href;
  } catch {
    return null;
  }
};
const tryAlternativePagination = (currentUrl, currentPageNum) => {
  try {
    const url = new URL(currentUrl);
    const next = currentPageNum + 1;
    if (!url.searchParams.has('page')) { url.searchParams.set('page', String(next)); return url.href; }
    if (!url.searchParams.has('p'))    { url.searchParams.delete('page'); url.searchParams.set('p', String(next)); return url.href; }
    if (!url.searchParams.has('start')){ const pageSize = 20; url.searchParams.set('start', String((next - 1) * pageSize)); return url.href; }
    if (!url.searchParams.has('page_number')) { url.searchParams.set('page_number', String(next)); return url.href; }
    return null;
  } catch { return null; }
};

// ========= Card & Detail scraping =========
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

    let title = CLEAN($a.text()) || CLEAN($a.find('h2,h3,h4').first().text());
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
    '[data-testid="jobDescriptionSection"]', '[data-testid="jobDescription"]',
    'div[class*="jobDescriptionSection"]', 'section[class*="jobDescription"]',
    '[data-job-description]', 'div.job_description', 'section.job_description',
    'div.jobDescription', '#job_description', '[itemprop="description"]',
    'article[itemprop="description"]', 'article.job-details', 'div.job-details',
  ];
  let descNode = null; let bestScore = 0;
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
  let descriptionHtml = null; let descriptionText = null;
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
  if (!out.description_text && descriptionHtml) out.description_text = htmlToText(descriptionHtml);

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

// ========= URL builders =========
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

// ========= Metrics / Instrumentation =========
const METRICS = {
  startTs: now(),
  requests_total: 0,
  requests_success: 0,
  requests_failed: 0,
  list_pages: 0,
  detail_pages: 0,
  // timing aggregates (ms)
  ttfb_ms: [], // if available via got timings
  download_ms: [],
  parse_ms: [],
  recordSuccess: function (type) {
    this.requests_total++;
    this.requests_success++;
    if (type === 'LIST') this.list_pages++;
    if (type === 'DETAIL') this.detail_pages++;
  },
  recordFail: function () {
    this.requests_total++;
    this.requests_failed++;
  },
  pushTiming: function ({ ttfb, download, parse }) {
    if (Number.isFinite(ttfb)) this.ttfb_ms.push(ttfb);
    if (Number.isFinite(download)) this.download_ms.push(download);
    if (Number.isFinite(parse)) this.parse_ms.push(parse);
  },
};
const p50 = (arr) => {
  if (!arr?.length) return 0;
  const a = [...arr].sort((x, y) => x - y);
  const mid = Math.floor(a.length / 2);
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
};
const p95 = (arr) => {
  if (!arr?.length) return 0;
  const a = [...arr].sort((x, y) => x - y);
  const idx = Math.floor(0.95 * (a.length - 1));
  return a[idx];
};
const summarizeMetrics = () => {
  const elapsed = (now() - METRICS.startTs) / 1000;
  const rps = elapsed > 0 ? (METRICS.requests_success / elapsed).toFixed(2) : '0.00';
  log.info(`📈 Metrics: succ=${METRICS.requests_success}, fail=${METRICS.requests_failed}, rps=${rps}/s, list=${METRICS.list_pages}, detail=${METRICS.detail_pages}`);
  if (METRICS.ttfb_ms.length || METRICS.download_ms.length || METRICS.parse_ms.length) {
    log.info(
      `⏱ TTFB p50=${p50(METRICS.ttfb_ms)}ms p95=${p95(METRICS.ttfb_ms)}ms | ` +
      `DL p50=${p50(METRICS.download_ms)}ms p95=${p95(METRICS.download_ms)}ms | ` +
      `Parse p50=${p50(METRICS.parse_ms)}ms p95=${p95(METRICS.parse_ms)}ms`
    );
  }
};

// ========= Actor init & input =========
await Actor.init();

const input = (await Actor.getInput()) || {};
let {
  startUrl,
  keyword = 'Administrative Assistant',
  location = '',
  postedWithin = 'any',
  results_wanted = 100,
  collect_details = true,
  maxConcurrency = 50,            // tuned up for speed; session pool keeps reliability
  maxRequestRetries = 3,          // moderate retry
  proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'], countryCode: 'US' },
  requestHandlerTimeoutSecs = 60, // allow enough time per request
  downloadIntervalMs: downloadIntervalMsInput = null,
  preferCandidateSearch = false,
  // aliases (no schema change)
  maxJobs,
  max_jobs,
} = input;

// normalize inputs
const aliasCap = toNumber(maxJobs ?? max_jobs);
if (aliasCap && aliasCap > 0) results_wanted = aliasCap;
const postedWithinDays = resolvePostedWithin(postedWithin);

// Correct domain typo in startUrl if present
if (startUrl) startUrl = ensureCorrectDomain(startUrl);

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// connection pooling (keep-alive) — reduces time spent in TCP/TLS
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 64 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 64 });

// pacing
const downloadIntervalMs = downloadIntervalMsInput ?? (collect_details ? 100 : 50); // slightly faster default

let START_URL = startUrl?.trim()
  || (preferCandidateSearch ? buildCandidateSearchUrl(keyword, location, postedWithinDays) : buildJobsUrl(keyword, location, postedWithinDays));

const postedLabel = postedWithinDays ? `<=${postedWithinDays}d` : 'any';
log.info(`ZipRecruiter FAST mode: ${START_URL} | details: ${collect_details ? 'ON' : 'OFF'} | target: ${results_wanted} | posted: ${postedLabel}`);

// ========= Crawl state =========
let pushed = 0;
const SEEN_URLS = new Set();
const QUEUED_DETAILS = new Set();
let pagesProcessed = 0;
let listPagesQueued = 1;
const MAX_PAGES = 100;
const PAGINATION_URLS_SEEN = new Set();

// backoff coordination (adaptive pacing when 429s occur)
let RATE_PENALTY_MS = 0;
const RATE_PENALTY_MAX = 3000;
const RATE_PENALTY_DECAY = 0.85;

// ========= CheerioCrawler (HTTP-only) =========
const crawler = new CheerioCrawler({
  proxyConfiguration: proxyConfig,
  maxConcurrency,
  maxRequestRetries,
  requestHandlerTimeoutSecs,
  navigationTimeoutSecs: requestHandlerTimeoutSecs,
  useSessionPool: true,
  persistCookiesPerSession: true,
  sessionPoolOptions: { maxPoolSize: 80, sessionOptions: { maxUsageCount: 80 } },
  autoscaledPoolOptions: { maybeRunIntervalSecs: 0.5, minConcurrency: 1 },
  maxRequestsPerCrawl: results_wanted * 4,

  // Centralized HTTP options — keep-alive, compression, timeouts
  preNavigationHooks: [
    async (ctx) => {
      const { request, session, proxyInfo } = ctx;

      // honor cap early for detail requests
      if (request.userData?.label === 'DETAIL' && pushed >= results_wanted) {
        request.skipNavigation = true;
        return;
      }

      // rotate UA per session
      if (session && !session.userData.ua) session.userData.ua = pickUA();
      const ua = session?.userData?.ua || pickUA();
      const referer = request.userData?.referer || 'https://www.google.com/';

      const headers = {
        'user-agent': ua,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'upgrade-insecure-requests': '1',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'referer': referer,
      };

      // keep-alive agents & got-scraping options
      ctx.requestOptions = {
        ...(ctx.requestOptions || {}),
        headers: { ...(ctx.requestOptions?.headers || {}), ...headers },
        retry: {
          limit: 0, // we do manual backoff to avoid hammering
        },
        timeout: {
          request: requestHandlerTimeoutSecs * 1000,
        },
        decompress: true,
        throwHttpErrors: false,
        agent: {
          http: httpAgent,
          https: httpsAgent,
        },
      };

      // ensure proxy sticky per session
      if (proxyInfo?.isApifyProxy && session?.id) {
        request.proxy = { ...(request.proxy || {}), session: session.id };
      }

      // adaptive rate penalty (decays over time)
      if (RATE_PENALTY_MS > 0) RATE_PENALTY_MS *= RATE_PENALTY_DECAY;
      const jitter = Math.floor(Math.random() * 30);
      const delay = (downloadIntervalMs || 0) + Math.min(RATE_PENALTY_MS | 0, RATE_PENALTY_MAX) + jitter;
      if (delay > 0) await sleep(delay);
    },
  ],

  requestHandler: async (ctx) => {
    const t0 = now();
    const { request, $, enqueueLinks, session, response } = ctx;
    const label = request.userData?.label || 'LIST';

    // fast status checks (avoid expensive $('body').text())
    const statusCode = response?.statusCode || 0;

    // 429 & 5xx backoff (policy-based, no stealth)
    if (statusCode === 429 || statusCode === 503 || statusCode === 502 || statusCode === 504 || statusCode === 500) {
      const retryAfter = Number(response?.headers?.['retry-after']) || null;
      const n = request.retryCount || 0;
      const base = retryAfter ? retryAfter * 1000 : 400;
      const wait = Math.min(base * Math.pow(2, n) + Math.floor(Math.random() * 200), 8000);
      RATE_PENALTY_MS = Math.min((RATE_PENALTY_MS || 0) + 150, RATE_PENALTY_MAX);
      log.warning(`⏳ Backoff on ${statusCode} for ${wait}ms (retry #${n + 1}) — ${request.url}`);
      await sleep(wait);
      throw new Error(`HTTP_${statusCode}_BACKOFF`);
    }

    if (statusCode === 403 || statusCode === 401) {
      log.warning(`🔒 ${statusCode} on ${request.url} — retiring session ${session?.id}`);
      if (session) session.markBad();
      throw new Error(`HTTP_${statusCode}_BLOCKED`);
    }

    const t1 = now(); // after headers -> approx TTFB surrogate in this pipeline
    let parseStart = t1;

    if (label === 'LIST') {
      METRICS.list_pages += 1;
      const baseUrl = request.loadedUrl ?? request.url;
      pagesProcessed++;
      PAGINATION_URLS_SEEN.add(baseUrl);

      // Parse cards (narrow selectors only; avoid $('body').text())
      const cards = scrapeCards($, baseUrl);

      if (cards.length === 0) {
        // Soft retry once for first page (transient empty or slow server)
        if (!request.userData?._retriedEmpty && pagesProcessed === 1) {
          log.info('↻ Soft retry first page once due to empty list (uniqueKey-suffixed).');
          await enqueueLinks({
            urls: [baseUrl],
            userData: { label: 'LIST', referer: request.userData?.referer, _retriedEmpty: true },
            forefront: true,
            transformRequestFunction: (req) => { req.uniqueKey = `${req.url}#empty-retry`; return req; },
          });
        } else {
          log.warning(`⚠ No job cards parsed on page ${pagesProcessed}. URL: ${baseUrl}`);
        }
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
            await enqueueLinks({
              urls: [norm],
              userData: { label: 'DETAIL', card, referer: baseUrl },
            });
          }
        }
      }

      METRICS.recordSuccess('LIST');
      const t2 = now();
      METRICS.pushTiming({ ttfb: t1 - t0, download: (t1 - t0), parse: t2 - parseStart });

      log.info(`📄 LIST page ${pagesProcessed}: found ${cards.length} cards, new ${newAdded} → SEEN=${SEEN_URLS.size}, scraped=${pushed}/${results_wanted}`);

      // Plan pagination
      const estimatedJobsPerPage = cards.length > 0 ? cards.length : 20;
      const remainingNeeded = Math.max(0, results_wanted - SEEN_URLS.size);
      const pagesNeeded = Math.ceil(remainingNeeded / Math.max(1, estimatedJobsPerPage));
      const pagesToQueue = Math.min(3, Math.max(0, pagesNeeded));

      const shouldContinue =
        SEEN_URLS.size < results_wanted * 1.5 &&
        pagesProcessed < MAX_PAGES &&
        listPagesQueued < MAX_PAGES * 2 &&
        pushed < results_wanted;

      if (shouldContinue && pagesToQueue > 0) {
        let currentUrl = baseUrl;
        let queued = 0;
        let lastFoundNextUrl = null;

        for (let i = 0; i < pagesToQueue; i++) {
          const nextUrl = i === 0 ? findNextPage($, currentUrl) : findNextPageByUrlOnly(currentUrl);
          lastFoundNextUrl = nextUrl;

          if (nextUrl && nextUrl !== currentUrl && !PAGINATION_URLS_SEEN.has(nextUrl)) {
            listPagesQueued++;
            PAGINATION_URLS_SEEN.add(nextUrl);
            if (i === 0) log.info(`➡️  Next page queued (#${listPagesQueued}): ${nextUrl.substring(0, 120)}...`);
            await enqueueLinks({
              urls: [nextUrl],
              userData: { label: 'LIST', referer: currentUrl },
              forefront: i === 0,
            });
            currentUrl = nextUrl;
            queued++;
          } else {
            break;
          }
        }

        if (queued === 0) {
          if (lastFoundNextUrl && PAGINATION_URLS_SEEN.has(lastFoundNextUrl)) {
            log.warning(`⚠ Pagination loop detected - URL already seen`);
          } else {
            log.warning(`⚠ No next page found after page ${pagesProcessed}. SEEN=${SEEN_URLS.size}, target=${results_wanted}`);
          }
          if (SEEN_URLS.size < results_wanted * 0.9) {
            log.info('🔄 Trying alternative pagination method...');
            const alternativeNext = tryAlternativePagination(baseUrl, pagesProcessed);
            if (alternativeNext && !PAGINATION_URLS_SEEN.has(alternativeNext)) {
              listPagesQueued++;
              PAGINATION_URLS_SEEN.add(alternativeNext);
              log.info(`➡️  Alternative page ${listPagesQueued}: ${alternativeNext.substring(0, 120)}...`);
              await enqueueLinks({
                urls: [alternativeNext],
                userData: { label: 'LIST', referer: baseUrl },
                forefront: true,
              });
            } else {
              log.warning(`❌ Alternative pagination also failed`);
            }
          }
        }
      } else {
        if (pagesProcessed >= MAX_PAGES) {
          log.warning(`⛔ Reached MAX_PAGES limit (${MAX_PAGES})`);
        } else if (pagesToQueue <= 0) {
          log.info(`✓ Enough jobs collected/queued`);
        } else if (pushed >= results_wanted) {
          log.info(`✓ Job cap reached (${pushed}/${results_wanted}). Not queuing more pages.`);
        } else {
          log.info(`✓ Target buffer reached: SEEN=${SEEN_URLS.size} (target ${results_wanted})`);
        }
      }

      // periodic metrics line
      if (METRICS.requests_success % 15 === 0) summarizeMetrics();
      return;
    }

    if (label === 'DETAIL') {
      if (pushed >= results_wanted) return;
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

      METRICS.recordSuccess('DETAIL');
      const t2 = now();
      METRICS.pushTiming({ ttfb: t1 - t0, download: (t1 - t0), parse: t2 - parseStart });

      if (pushed % 25 === 0 || pushed === results_wanted) {
        log.info(`📊 Progress: ${pushed}/${results_wanted} jobs scraped (${((pushed / results_wanted) * 100).toFixed(1)}%)`);
        summarizeMetrics();
      }
      return;
    }
  },

  failedRequestHandler: async ({ request, error, session, response }) => {
    METRICS.recordFail();
    const code = response?.statusCode ? ` [${response.statusCode}]` : '';
    log.warning(`FAILED ${request.url}${code}: ${error?.message || error}`);
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

log.info(`🚀 Starting crawler with target: ${results_wanted} jobs`);
log.info(`📍 Start URL: ${START_URL}`);

await crawler.run([
  { url: START_URL, userData: { label: 'LIST', referer: 'https://www.google.com/' } },
]);

// ========= Final report =========
const finalCount = pushed;
const successRate = SEEN_URLS.size > 0 ? ((finalCount / SEEN_URLS.size) * 100).toFixed(1) : '0.0';
summarizeMetrics();

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
  log.warning(`  • Not enough jobs available for this search query`);
  log.warning(`  • Pagination stopped early (only ${pagesProcessed} pages found)`);
  log.warning(`  • Some detail pages failed or were blocked (check error logs above)`);
  log.warning(`  • Site structure may have changed`);
  log.warning(`\nSuggestions:`);
  log.warning(`  • Try broader keyword or remove location filter`);
  log.warning(`  • Consider preferCandidateSearch: true when list mode is sparse`);
  log.warning(`  • Inspect warnings for "No job cards parsed" or 429/403 patterns`);
} else if (finalCount > results_wanted) {
  log.info(`✨ Exceeded target by ${finalCount - results_wanted} jobs (buffer/queue overlap)`);
}

await Actor.exit();
