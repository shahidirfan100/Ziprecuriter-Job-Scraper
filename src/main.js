// src/main.js
// ZipRecruiter – Optimized for SPEED with HTTP-only (CheerioCrawler)
// Improvements: Parallel detail scraping, optimized intervals, better concurrency

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, RequestQueue } from 'crawlee';

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

const POSTED_WITHIN_MAP = new Map([
  ['any', null],
  ['1', 1],
  ['1d', 1],
  ['24', 1],
  ['24h', 1],
  ['7', 7],
  ['7d', 7],
  ['30', 30],
  ['30d', 30],
]);
const resolvePostedWithin = (value) => {
  if (value === undefined || value === null) return null;
  const key = STR(value).toLowerCase();
  return POSTED_WITHIN_MAP.has(key) ? POSTED_WITHIN_MAP.get(key) : null;
};

const BAD_COMPANY_PATTERNS = [
  /about\s+us/i,
  /careers?/i,
  /investors?/i,
  /blog/i,
  /press/i,
  /engineering/i,
  /ziprecruiter\.org/i,
  /ziprecruiter\s+uk/i,
  /contact\s+us/i,
  /privacy/i,
  /terms/i,
  /^(home|jobs|search|help)$/i,
];

const sanitizeCompanyName = (raw) => {
  const str = CLEAN(raw);
  if (!str) return null;
  
  if (BAD_COMPANY_PATTERNS.some(pattern => pattern.test(str))) {
    return null;
  }
  
  let cleaned = str.replace(/\s*\[[^\]]+\]\s*$/, '');
  cleaned = cleaned.replace(/^company[:\-\s]+/i, '');
  
  const delimiters = [
    /\s*\|\s/,
    /\s*\/\s/,
    /\s+[-–—]\s+/,
    /\s*•\s*/,
    /\s+at\s+/i,
  ];
  
  for (const delimiter of delimiters) {
    if (delimiter.test(cleaned)) {
      const parts = cleaned.split(delimiter);
      cleaned = CLEAN(parts[0]);
      break;
    }
  }
  
  if (!cleaned) return null;
  
  const lower = cleaned.toLowerCase();
  if (BAD_COMPANY_PATTERNS.some(pattern => pattern.test(lower))) {
    return null;
  }
  
  if (cleaned.length < 2) return null;
  
  return cleaned;
};

const extractCompanyFromCard = ($card) => {
  if (!$card || $card.length === 0) return null;
  
  const dataAttrs = ['data-company-name', 'data-company'];
  for (const attr of dataAttrs) {
    const val = sanitizeCompanyName($card.attr(attr));
    if (val) return val;
  }
  
  const companySelectors = [
    'a.company_name',
    'a[data-company-name]',
    'a[href*="/company/"]',
    'span.company_name',
    'div.company_name',
    '[data-testid*="company"]',
    '[class*="company"]',
    '.job_org_name',
    '.t_org_link',
  ];
  
  for (const sel of companySelectors) {
    const $node = $card.find(sel).first();
    if ($node && $node.length) {
      const text = sanitizeCompanyName($node.text());
      if (text) return text;
      
      const titleAttr = sanitizeCompanyName($node.attr('title'));
      if (titleAttr) return titleAttr;
      
      const ariaLabel = sanitizeCompanyName($node.attr('aria-label'));
      if (ariaLabel) return ariaLabel;
    }
  }
  
  const cardText = $card.text();
  const companyMatch = cardText.match(/(?:hiring\s+)?company:\s*([^\n\r|]+)/i);
  if (companyMatch) {
    const candidate = sanitizeCompanyName(companyMatch[1]);
    if (candidate) return candidate;
  }
  
  return null;
};

const extractCompanyFromPage = ($) => {
  const jp = extractJsonLd($);
  if (jp?.hiringOrganization?.name) {
    const candidate = sanitizeCompanyName(jp.hiringOrganization.name);
    if (candidate) return candidate;
  }
  
  const heroSelectors = [
    '[data-testid="hero-company-name"]',
    '[data-testid="companyName"]',
    'h2.company-name',
    'h3.company-name',
    'div.company-header',
  ];
  
  for (const sel of heroSelectors) {
    const $node = $(sel).first();
    if ($node && $node.length) {
      const text = sanitizeCompanyName($node.text());
      if (text) return text;
    }
  }
  
  const structuredSelectors = [
    '[itemprop="hiringOrganization"] [itemprop="name"]',
    '[itemprop="hiringOrganization"]',
    'a.company_link',
    'a[href*="/company/"]',
  ];
  
  for (const sel of structuredSelectors) {
    const $node = $(sel).first();
    if ($node && $node.length) {
      const text = sanitizeCompanyName($node.text());
      if (text) return text;
      
      const contentAttr = sanitizeCompanyName($node.attr('content'));
      if (contentAttr) return contentAttr;
    }
  }
  
  const metaSelectors = [
    'meta[property="og:site_name"]',
    'meta[name="twitter:data1"]',
  ];
  
  for (const sel of metaSelectors) {
    const content = $(sel).attr('content');
    if (content) {
      const parts = content.split('•');
      const candidate = sanitizeCompanyName(parts[0]);
      if (candidate) return candidate;
    }
  }
  
  return null;
};

const extractNodeHtml = ($node) => {
  if (!$node || $node.length === 0) return null;
  const clone = $node.clone();
  clone.find('script, style, noscript').remove();
  const html = STR(clone.html());
  return html || null;
};

const UA_POOL = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
];
const pickUA = () => UA_POOL[Math.floor(Math.random() * UA_POOL.length)];

const ABS = (href, base) => { try { return new URL(href, base).href; } catch { return null; } };

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

const findNextPage = ($, baseUrl) => {
  const ABS = (href, base) => { try { return new URL(href, base).href; } catch { return null; } };
  const pickNum = (s) => {
    const n = Number(String(s || '').replace(/[^\d]/g, ''));
    return Number.isFinite(n) ? n : null;
  };

  // 1) <link rel="next">
  let href = $('link[rel="next"]').attr('href');
  if (href) return ABS(href, baseUrl);

  // 2) Explicit Next controls (aria or visible text)
  const nextCandSel = [
    'a[rel="next"]',
    'a[aria-label]',
    'button[aria-label]',
    'nav[aria-label] a',
    'a:contains("Next")',
    'a:contains("›")',
    'a:contains("»")',
  ].join(',');

  const $next = $(nextCandSel).filter((i, el) => {
    const txt = ($(el).text() || '').trim().toLowerCase();
    const aria = ($(el).attr('aria-label') || '').trim().toLowerCase();
    return /next|›|»/.test(txt) || /next/.test(aria) || ($(el).attr('rel') || '').toLowerCase() === 'next';
  }).first();

  if ($next.length) {
    const h = $next.attr('href');
    if (h) return ABS(h, baseUrl);
  }

  // 3) Numeric pager: find current page number and follow the next higher one
  const pagerScopes = $('nav[aria-label], .pagination, [class*="pager"], body');
  let curPage = null;

  // try to find "current/active" page element
  pagerScopes.find('a, span, button').each((_, el) => {
    const $el = $(el);
    const isActive =
      /active|selected|current/i.test($el.attr('class') || '') ||
      ($el.is('span') && /\d+/.test($el.text()));
    const num = pickNum($el.text());
    if (isActive && num && num >= 1) curPage = num;
  });

  // infer from URL if still unknown
  if (curPage == null) {
    try {
      const cur = new URL(baseUrl);
      const cands = ['page', 'p', 'pg', 'start', 'offset'];
      for (const k of cands) {
        const n = pickNum(cur.searchParams.get(k));
        if (n && n >= 1) { curPage = n; break; }
      }
    } catch {}
  }

  // collect numbered links
  const numLinks = [];
  pagerScopes.find('a[href]').each((_, a) => {
    const $a = $(a);
    const txtNum = pickNum($a.text());
    if (!txtNum || txtNum < 1) return;
    const h = $a.attr('href');
    const abs = ABS(h, baseUrl);
    if (abs) numLinks.push({ n: txtNum, url: abs });
  });

  if (numLinks.length) {
    if (curPage == null) {
      const min = Math.min(...numLinks.map(x => x.n));
      curPage = (Number.isFinite(min) && min >= 1) ? min : 1;
    }
    const candidates = numLinks.filter(x => x.n > curPage).sort((a, b) => a.n - b.n);
    if (candidates[0]?.url) return candidates[0].url;
  }

  // 4) Scan for page-like params across anchors
  let best = null;
  try {
    const cur = new URL(baseUrl);
    const keys = ['page', 'p', 'pg', 'start', 'offset'];
    const curInfo = (() => {
      for (const k of keys) {
        const n = pickNum(cur.searchParams.get(k));
        if (n && n >= 1) return { key: k, val: n };
      }
      return { key: 'page', val: 1 };
    })();

    $('a[href]').each((_, a) => {
      const url = ABS($(a).attr('href'), baseUrl);
      if (!url) return;
      try {
        const u = new URL(url);
        for (const key of keys) {
          const p = pickNum(u.searchParams.get(key));
          if (p && p > curInfo.val && (!best || p < best.p)) best = { p, url };
        }
      } catch {}
    });

    if (best?.url) return best.url;

    // 5) Fallback: synthetically increment observed param
    const inc = (key) => {
      const u = new URL(baseUrl);
      const curVal = pickNum(u.searchParams.get(key)) || 1;
      u.searchParams.set(key, String(curVal + 1));
      return u.href;
    };
    if (cur.searchParams.has(curInfo.key)) return inc(curInfo.key);
    return inc(curInfo.key === 'page' ? 'p' : 'page');
  } catch {}

  return null;
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
    const href0 = $a.attr('href');
    const href = ABS(href0, baseUrl);
    if (!href) return;
    if (!/\/(c|job|jobs)\//i.test(href)) return;

    let title = CLEAN($a.text()) || CLEAN($a.find('h2,h3').first().text());
    const $card = $a.closest('article, li, div').first();
    const textBlob = CLEAN(($card.text() || '').slice(0, 800));

    let company = extractCompanyFromCard($card) || sanitizeCompanyName($a.attr('data-company-name') || $a.attr('data-company'));
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
    'div[class*="job-content"]',
    'section[class*="job-content"]',
  ];
  
  let descNode = null;
  let bestScore = 0;
  
  for (const sel of descriptionSelectors) {
    const candidate = $(sel).first();
    if (candidate && candidate.length) {
      const text = CLEAN(candidate.text());
      const textLength = text.length;
      
      let score = textLength;
      
      if (text.toLowerCase().includes('responsibilities') || 
          text.toLowerCase().includes('requirements') ||
          text.toLowerCase().includes('qualifications')) {
        score += 1000;
      }
      
      if (text.toLowerCase().includes('ziprecruiter uk') ||
          text.toLowerCase().includes('ziprecruiter.org') ||
          text.toLowerCase().includes('about us careers')) {
        score -= 5000;
      }
      
      if (textLength >= 100 && score > bestScore) {
        bestScore = score;
        descNode = candidate;
      }
    }
  }
  
  let descriptionHtml = null;
  let descriptionText = null;
  
  if (descNode && descNode.length && bestScore > 0) {
    descriptionHtml = extractNodeHtml(descNode);
    descriptionText = CLEAN(descNode.text()) || null;
    
    if (descriptionText) {
      const lower = descriptionText.toLowerCase();
      
      if (lower.includes('ziprecruiter uk') || 
          lower.includes('ziprecruiter.org') ||
          lower.includes('about us careers investors')) {
        
        const sections = descriptionText.split(/\n\n+/);
        let cleanedText = '';
        
        for (const section of sections) {
          const sectionLower = section.toLowerCase();
          if (section.length > 50 && 
              !sectionLower.includes('ziprecruiter uk') &&
              !sectionLower.includes('ziprecruiter.org') &&
              !sectionLower.includes('about us careers')) {
            cleanedText += section + '\n\n';
          }
        }
        
        if (cleanedText.trim().length > 100) {
          descriptionText = CLEAN(cleanedText);
          const paragraphs = cleanedText.split(/\n\n+/).filter(p => p.trim());
          descriptionHtml = paragraphs.map(p => `<p>${CLEAN(p)}</p>`).join('');
        } else {
          descriptionText = null;
          descriptionHtml = null;
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

    const jpCompany = sanitizeCompanyName(jp.hiringOrganization?.name);
    if (jpCompany && !out.company) out.company = jpCompany;

    if (!out.employment_type && jp.employmentType) out.employment_type = CLEAN(jp.employmentType);
    if (!out.location && jp.jobLocation?.address) {
      const a = jp.jobLocation.address;
      out.location = CLEAN([a.addressLocality, a.addressRegion, a.addressCountry].filter(Boolean).join(', '));
    }

    const jsonDescRaw = STR(jp.description);
    if (jsonDescRaw && !out.description_text) {
      const jsonDescText = htmlToText(jsonDescRaw);
      if (jsonDescText && jsonDescText.length > 100) {
        out.description_text = jsonDescText;
        
        if (!out.description_html) {
          const hasTags = /<[^>]+>/.test(jsonDescRaw);
          if (hasTags) {
            out.description_html = jsonDescRaw;
          } else {
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

await Actor.init();

const input = (await Actor.getInput()) || {};
const {
  startUrl,
  keyword = 'Administrative Assistant',
  location = '',
  postedWithin = 'any',
  results_wanted = 100,
  collect_details = true,
  maxConcurrency = 10, // INCREASED from 2
  maxRequestRetries = 2,
  proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'], countryCode: 'US' },
  requestHandlerTimeoutSecs = 35,
  downloadIntervalMs: downloadIntervalMsInput = null,
  preferCandidateSearch = false,
} = input;

// OPTIMIZED: Reduced intervals significantly
const downloadIntervalMs = downloadIntervalMsInput ?? (collect_details ? 200 : 100);
const postedWithinDays = resolvePostedWithin(postedWithin);

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

let START_URL = startUrl?.trim()
  || (preferCandidateSearch ? buildCandidateSearchUrl(keyword, location, postedWithinDays) : buildJobsUrl(keyword, location, postedWithinDays));

const postedLabel = postedWithinDays ? `<=${postedWithinDays}d` : 'any';
log.info(`ZipRecruiter FAST mode: ${START_URL} | details: ${collect_details ? 'ON' : 'OFF'} | target: ${results_wanted} | posted: ${postedLabel}`);

let pushed = 0;
const SEEN_URLS = new Set();
const QUEUED_DETAILS = new Set();
let pagesProcessed = 0;
const MAX_PAGES = 100; // Safety limit to prevent infinite loops

/* CHANGED: open and seed the RequestQueue BEFORE creating the crawler */
const requestQueue = await RequestQueue.open();
await requestQueue.addRequest({ url: START_URL, userData: { label: 'LIST', referer: 'https://www.google.com/' } });

const crawler = new CheerioCrawler({
  proxyConfiguration: proxyConfig,
  maxConcurrency, // Higher concurrency
  maxRequestRetries,
  requestHandlerTimeoutSecs,
  navigationTimeoutSecs: requestHandlerTimeoutSecs,
  useSessionPool: true,
  persistCookiesPerSession: true,
  sessionPoolOptions: {
    maxPoolSize: 50, // INCREASED from 30
    sessionOptions: { maxUsageCount: 50 }, // INCREASED from 20
  },
  /* provide the already opened requestQueue */
  requestQueue,

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
      
      // OPTIMIZED: Reduced sleep time with random jitter
      if (downloadIntervalMs) {
        const jitter = Math.floor(Math.random() * 100);
        await sleep(downloadIntervalMs + jitter);
      }
    },
  ],

  requestHandler: async (ctx) => {
    const { request, $, enqueueLinks, session, response } = ctx;
    const { label } = request.userData;

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

    // REMOVED WARMUP - Start directly with LIST
    if (!label || label === 'LIST') {
      const baseUrl = request.loadedUrl ?? request.url;
      pagesProcessed++;

      const cards = scrapeCards($, baseUrl);
      let newAdded = 0;

      // Process ALL cards from this page (don't break early)
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

      log.info(`LIST page ${pagesProcessed}: cards=${cards.length}, new=${newAdded}, SEEN=${SEEN_URLS.size}, pushed=${pushed}, target=${results_wanted}`);

      // FIXED: Continue pagination until we've SEEN enough jobs to meet target
      // For details mode: we need to see MORE than target because some might fail
      // For non-details mode: pushed count should match target
      const bufferMultiplier = collect_details ? 1.2 : 1.0; // Queue 20% extra in details mode
      const targetWithBuffer = Math.ceil(results_wanted * bufferMultiplier);
      // When collecting details, prefer to stop only when we've queued enough detail pages (QUEUED_DETAILS),
      // otherwise use seen URLs for non-detail mode.
      let needMore;
      if (collect_details) {
        needMore = QUEUED_DETAILS.size < targetWithBuffer && pagesProcessed < MAX_PAGES;
      } else {
        needMore = SEEN_URLS.size < targetWithBuffer && pagesProcessed < MAX_PAGES;
      }
      
      if (needMore) {
        const nextUrl = findNextPage($, baseUrl);
        if (nextUrl && nextUrl !== baseUrl) {
          const remaining = collect_details
            ? Math.max(0, results_wanted - QUEUED_DETAILS.size)
            : Math.max(0, results_wanted - SEEN_URLS.size);
          log.info(`✓ Queuing next page (need ~${remaining} more jobs, seen ${SEEN_URLS.size}/${targetWithBuffer}, queuedDetails=${QUEUED_DETAILS.size})`);
          await enqueueLinks({ 
            urls: [nextUrl], 
            userData: { label: 'LIST', referer: baseUrl },
            forefront: true // Keep pagination at high priority
          });
        } else {
          log.info(`⚠ No more pages found. Final: SEEN=${SEEN_URLS.size}, pushed=${pushed}/${results_wanted}`);
        }
      } else {
        if (pagesProcessed >= MAX_PAGES) {
          log.warning(`⚠ Reached MAX_PAGES limit (${MAX_PAGES}). Stopping pagination.`);
        } else {
          log.info(`✓ Target reached! SEEN=${SEEN_URLS.size}, pushed=${pushed}/${results_wanted}. Stopping pagination.`);
        }
      }
      return;
    }

    if (label === 'DETAIL') {
      // Check if we've already hit target (skip if over limit)
      if (pushed >= results_wanted) {
        log.info(`Skipping detail page (already reached target: ${pushed}/${results_wanted})`);
        return;
      }
      
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
      
      // Log progress every 10 jobs
      if (pushed % 10 === 0) {
        log.info(`📊 Progress: ${pushed}/${results_wanted} jobs scraped`);
      }
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

await crawler.run();

const finalCount = pushed;
const successRate = SEEN_URLS.size > 0 ? ((finalCount / SEEN_URLS.size) * 100).toFixed(1) : 0;

log.info(`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ SCRAPING COMPLETED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Target:        ${results_wanted} jobs
  Scraped:       ${finalCount} jobs
  Seen URLs:     ${SEEN_URLS.size}
  Pages Crawled: ${pagesProcessed}
  Success Rate:  ${successRate}%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);

if (finalCount < results_wanted) {
  log.warning(`⚠ Only scraped ${finalCount}/${results_wanted} jobs. Possible reasons:
  - Not enough jobs available for this search
  - Pagination ended (no more pages found)
  - Some detail pages failed to load
  - Try: broader keyword, remove location filter, or check logs for errors`);
}

await Actor.exit();