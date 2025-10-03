// src/main.js
// ZipRecruiter – HTTP-only (CheerioCrawler) with robust pagination to reach target counts.
// FIXED: Company name and job description extraction

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

// IMPROVED: More comprehensive bad company pattern detection
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
  
  // Check if it's just navigation/footer links
  if (BAD_COMPANY_PATTERNS.some(pattern => pattern.test(str))) {
    return null;
  }
  
  // Remove bracketed content at the end
  let cleaned = str.replace(/\s*\[[^\]]+\]\s*$/, '');
  
  // Remove "Company:" prefix
  cleaned = cleaned.replace(/^company[:\-\s]+/i, '');
  
  // Split on common delimiters and take first part
  const delimiters = [
    /\s*\|\s/,           // pipe
    /\s*\/\s/,           // forward slash  
    /\s+[-–—]\s+/,       // dash variants
    /\s*•\s*/,           // bullet
    /\s+at\s+/i,         // " at "
  ];
  
  for (const delimiter of delimiters) {
    if (delimiter.test(cleaned)) {
      const parts = cleaned.split(delimiter);
      cleaned = CLEAN(parts[0]);
      break;
    }
  }
  
  if (!cleaned) return null;
  
  // Final check for bad patterns
  const lower = cleaned.toLowerCase();
  if (BAD_COMPANY_PATTERNS.some(pattern => pattern.test(lower))) {
    return null;
  }
  
  // Check if it's too short (likely not a real company name)
  if (cleaned.length < 2) return null;
  
  return cleaned;
};

// IMPROVED: Better company extraction from card
const extractCompanyFromCard = ($card) => {
  if (!$card || $card.length === 0) return null;
  
  // Priority 1: Specific data attributes
  const dataAttrs = ['data-company-name', 'data-company'];
  for (const attr of dataAttrs) {
    const val = sanitizeCompanyName($card.attr(attr));
    if (val) return val;
  }
  
  // Priority 2: Look for company-specific elements (improved selectors)
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
      
      // Try attributes
      const titleAttr = sanitizeCompanyName($node.attr('title'));
      if (titleAttr) return titleAttr;
      
      const ariaLabel = sanitizeCompanyName($node.attr('aria-label'));
      if (ariaLabel) return ariaLabel;
    }
  }
  
  // Priority 3: Try to find company in structured text (looking for hiring company pattern)
  const cardText = $card.text();
  // Look for "Hiring Company: XYZ" or "Company: XYZ" patterns
  const companyMatch = cardText.match(/(?:hiring\s+)?company:\s*([^\n\r|]+)/i);
  if (companyMatch) {
    const candidate = sanitizeCompanyName(companyMatch[1]);
    if (candidate) return candidate;
  }
  
  return null;
};

// IMPROVED: Better company extraction from detail page
const extractCompanyFromPage = ($) => {
  // Priority 1: JSON-LD structured data (most reliable)
  const jp = extractJsonLd($);
  if (jp?.hiringOrganization?.name) {
    const candidate = sanitizeCompanyName(jp.hiringOrganization.name);
    if (candidate) return candidate;
  }
  
  // Priority 2: Specific hero/header elements
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
  
  // Priority 3: Structured data attributes
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
  
  // Priority 4: Meta tags
  const metaSelectors = [
    'meta[property="og:site_name"]',
    'meta[name="twitter:data1"]',
  ];
  
  for (const sel of metaSelectors) {
    const content = $(sel).attr('content');
    if (content) {
      // For twitter:data1, split on bullet and take first part
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
  let href = $('link[rel="next"]').attr('href');
  if (href) return ABS(href, baseUrl);

  const aNext = $('a[rel="next"], a[aria-label="Next"], a[aria-label="next"]').attr('href');
  if (aNext) return ABS(aNext, baseUrl);

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

  const tryInc = (key) => { const u = new URL(baseUrl); u.searchParams.set(key, String((Number(u.searchParams.get(key) || '1') || 1) + 1)); return u.href; };
  return cur.searchParams.has(curInfo.key) ? tryInc(curInfo.key) : tryInc(curInfo.key === 'p' ? 'page' : 'p');
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

// IMPROVED: Better description extraction with fallback logic
const scrapeDetail = ($, loadedUrl) => {
  const out = {};
  out.title = CLEAN($('h1, h1[itemprop="title"], h1[data-job-title]').first().text()) || null;

  out.company = extractCompanyFromPage($) || null;

  const locCandidates = [
    '.location, .job_location, [itemprop="jobLocation"]',
    'h2:contains("Address") ~ *, h3:contains("Address") ~ *',
  ].join(',');
  out.location = CLEAN($(locCandidates).first().text()) || null;

  // IMPROVED: More comprehensive description selectors with better priority
  const descriptionSelectors = [
    // Primary selectors (most specific)
    '[data-testid="jobDescriptionSection"]',
    '[data-testid="jobDescription"]',
    'div[class*="jobDescriptionSection"]',
    'section[class*="jobDescription"]',
    
    // Secondary selectors
    '[data-job-description]',
    'div.job_description',
    'section.job_description',
    'div.jobDescription',
    '#job_description',
    
    // Tertiary (schema.org)
    '[itemprop="description"]',
    'article[itemprop="description"]',
    
    // Fallback - broader containers
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
      
      // Score based on content length and quality indicators
      let score = textLength;
      
      // Bonus points for good indicators
      if (text.toLowerCase().includes('responsibilities') || 
          text.toLowerCase().includes('requirements') ||
          text.toLowerCase().includes('qualifications')) {
        score += 1000;
      }
      
      // Penalty for bad indicators (navigation/footer content)
      if (text.toLowerCase().includes('ziprecruiter uk') ||
          text.toLowerCase().includes('ziprecruiter.org') ||
          text.toLowerCase().includes('about us careers')) {
        score -= 5000;
      }
      
      // Must have reasonable length (at least 100 chars)
      if (textLength >= 100 && score > bestScore) {
        bestScore = score;
        descNode = candidate;
      }
    }
  }
  
  // Extract description if found
  let descriptionHtml = null;
  let descriptionText = null;
  
  if (descNode && descNode.length && bestScore > 0) {
    descriptionHtml = extractNodeHtml(descNode);
    descriptionText = CLEAN(descNode.text()) || null;
    
    // Validate the extracted description
    if (descriptionText) {
      const lower = descriptionText.toLowerCase();
      
      // If it contains navigation elements, try to clean it
      if (lower.includes('ziprecruiter uk') || 
          lower.includes('ziprecruiter.org') ||
          lower.includes('about us careers investors')) {
        
        // Try to find the actual job content by looking for common job section markers
        const sections = descriptionText.split(/\n\n+/);
        let cleanedText = '';
        
        for (const section of sections) {
          const sectionLower = section.toLowerCase();
          // Keep sections that look like job content
          if (section.length > 50 && 
              !sectionLower.includes('ziprecruiter uk') &&
              !sectionLower.includes('ziprecruiter.org') &&
              !sectionLower.includes('about us careers')) {
            cleanedText += section + '\n\n';
          }
        }
        
        if (cleanedText.trim().length > 100) {
          descriptionText = CLEAN(cleanedText);
          // Rebuild HTML from cleaned text
          const paragraphs = cleanedText.split(/\n\n+/).filter(p => p.trim());
          descriptionHtml = paragraphs.map(p => `<p>${CLEAN(p)}</p>`).join('');
        } else {
          // If cleaning didn't help, nullify the description
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
      // Only use JSON-LD description if it's substantial
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
  maxConcurrency = 2,
  maxRequestRetries = 2,
  proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'], countryCode: 'US' },
  requestHandlerTimeoutSecs = 35,
  downloadIntervalMs: downloadIntervalMsInput = null,
  preferCandidateSearch = false,
} = input;

const downloadIntervalMs = downloadIntervalMsInput ?? (collect_details ? 600 : 320);
const postedWithinDays = resolvePostedWithin(postedWithin);

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

let START_URL = startUrl?.trim()
  || (preferCandidateSearch ? buildCandidateSearchUrl(keyword, location, postedWithinDays) : buildJobsUrl(keyword, location, postedWithinDays));

const postedLabel = postedWithinDays ? `<=${postedWithinDays}d` : 'any';
log.info(`ZipRecruiter start: ${START_URL} | details: ${collect_details ? 'ON' : 'OFF'} | target: ${results_wanted} | posted: ${postedLabel}`);

let pushed = 0;
const SEEN_URLS = new Set();
const QUEUED_DETAILS = new Set();

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

    if (response?.statusCode === 403) {
      log.warning(`403 on ${request.url} – retire session ${session?.id}`);
      if (session) session.markBad();
      throw new Error('Blocked (403)');
    }
    const bodyText = ($('body').text() || '').toLowerCase();
    if (bodyText.includes('request blocked') || bodyText.includes('access denied') || bodyText.includes('verify you are a human')) {
      log.warning(`Bot page on ${request.url} – retire session ${session?.id}`);
      if (session) session.markBad();
      throw new Error('Blocked (bot page)');
    }

    if (label === 'WARMUP') {
      await enqueueLinks({ urls: [START_URL], userData: { label: 'LIST', referer: request.url } });
      return;
    }

    if (!label || label === 'LIST') {
      const baseUrl = request.loadedUrl ?? request.url;

      const cards = scrapeCards($, baseUrl);
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

        if (SEEN_URLS.size >= results_wanted) break;
      }

      log.info(`LIST ${baseUrl} -> cards=${cards.length}, new=${newAdded}, SEEN=${SEEN_URLS.size}, pushed=${pushed}`);

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

await crawler.run([
  { url: 'https://www.ziprecruiter.com/', userData: { label: 'WARMUP', referer: 'https://www.google.com/' } },
]);

log.info(`Done. SEEN=${SEEN_URLS.size} pushed=${pushed}`);
await Actor.exit();