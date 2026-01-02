import { Actor, log } from 'apify';
import { PlaywrightCrawler } from 'crawlee';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';
import * as cheerio from 'cheerio';
import { gotScraping } from 'got-scraping';

await Actor.init();

// Statistics tracking
const stats = {
    httpJsonSuccess: 0,
    httpHtmlSuccess: 0,
    browserSuccess: 0,
    totalPages: 0,
    totalJobs: 0,
    errors: [],
};

/**
 * PRIORITY 1: HTTP-based JSON extraction from embedded data
 * Fast, lightweight, no browser needed
 */
async function extractJobsViaHTTPJSON(url, proxyUrl = null) {
    log.info('🔥 PRIORITY 1: Attempting HTTP JSON extraction');

    try {
        const requestOptions = {
            url,
            headers: {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
            },
            timeout: { request: 30000 },
            retry: { limit: 2 },
        };

        if (proxyUrl) {
            requestOptions.proxyUrl = proxyUrl;
        }

        const response = await gotScraping(requestOptions);

        // Check for Cloudflare block
        if (response.statusCode === 403 || response.statusCode === 503) {
            log.warning('⚠️  Cloudflare block detected on HTTP request');
            return { blocked: true, jobs: [] };
        }

        if (response.statusCode !== 200) {
            log.warning(`HTTP request returned status ${response.statusCode}`);
            return { jobs: [] };
        }

        const html = response.body;
        const $ = cheerio.load(html);

        // Check if we got Cloudflare challenge page
        const title = $('title').text();
        if (title.includes('Just a moment') || title.includes('Cloudflare') || html.includes('cf-browser-verification')) {
            log.warning('⚠️  Cloudflare challenge page detected');
            return { blocked: true, jobs: [] };
        }

        const jobs = [];

        // Method 1: Extract from <script> tags with embedded JSON
        const scriptTags = $('script:not([src])');
        log.info(`Found ${scriptTags.length} inline script tags to analyze`);

        scriptTags.each((_, element) => {
            const scriptContent = $(element).html() || '';

            // Look for common ZipRecruiter data patterns
            const patterns = [
                /window\.__PRELOADED_STATE__\s*=\s*(\{[\s\S]*?\});/,
                /window\.__INITIAL_STATE__\s*=\s*(\{[\s\S]*?\});/,
                /window\.INITIAL_DATA\s*=\s*(\{[\s\S]*?\});/,
                /"job_results"\s*:\s*(\[[\s\S]*?\])/,
                /"jobs"\s*:\s*(\[[\s\S]*?\])/,
                /jobResults\s*=\s*(\[[\s\S]*?\])/,
            ];

            for (const pattern of patterns) {
                const match = scriptContent.match(pattern);
                if (match) {
                    try {
                        const jsonStr = match[1];
                        const data = JSON.parse(jsonStr);
                        
                        log.info(`✓ Found embedded JSON data with pattern: ${pattern.source.substring(0, 50)}...`);
                        
                        const extractedJobs = extractJobsFromData(data);
                        if (extractedJobs.length > 0) {
                            jobs.push(...extractedJobs);
                            log.info(`✓ Extracted ${extractedJobs.length} jobs from embedded JSON`);
                        }
                    } catch (parseError) {
                        log.debug(`Failed to parse JSON match: ${parseError.message}`);
                    }
                }
            }
        });

        // Method 2: Look for JSON-LD structured data
        const jsonLdScripts = $('script[type="application/ld+json"]');
        log.info(`Found ${jsonLdScripts.length} JSON-LD script tags`);

        jsonLdScripts.each((_, element) => {
            try {
                const jsonContent = $(element).html() || '';
                const data = JSON.parse(jsonContent);
                
                const ldJobs = extractJobsFromJSONLD(data);
                if (ldJobs.length > 0) {
                    jobs.push(...ldJobs);
                    log.info(`✓ Extracted ${ldJobs.length} jobs from JSON-LD`);
                }
            } catch (error) {
                log.debug(`Failed to parse JSON-LD: ${error.message}`);
            }
        });

        if (jobs.length > 0) {
            stats.httpJsonSuccess++;
            log.info(`🎉 HTTP JSON extraction successful: ${jobs.length} jobs`);
            return { jobs, method: 'HTTP_JSON' };
        }

        log.info('No jobs found via HTTP JSON extraction');
        return { jobs: [] };

    } catch (error) {
        log.warning(`HTTP JSON extraction failed: ${error.message}`);
        stats.errors.push({ method: 'HTTP_JSON', error: error.message });
        return { jobs: [], error: error.message };
    }
}

/**
 * PRIORITY 2: HTTP-based HTML parsing with CSS selectors
 * Lightweight fallback when JSON not available
 */
async function extractJobsViaHTTPHTML(url, proxyUrl = null) {
    log.info('🔥 PRIORITY 2: Attempting HTTP HTML parsing');

    try {
        const requestOptions = {
            url,
            headers: {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
                'Cache-Control': 'no-cache',
            },
            timeout: { request: 30000 },
            retry: { limit: 2 },
        };

        if (proxyUrl) {
            requestOptions.proxyUrl = proxyUrl;
        }

        const response = await gotScraping(requestOptions);

        if (response.statusCode !== 200) {
            log.warning(`HTTP request returned status ${response.statusCode}`);
            return { jobs: [] };
        }

        const $ = cheerio.load(response.body);
        const jobs = [];

        // ZipRecruiter common selectors (try multiple patterns)
        const jobSelectors = [
            'article.job_result',
            '.job_result',
            '[data-job-id]',
            'article[data-job]',
            '.job-card-container',
            'li.job-listing',
            '.job-list-item',
        ];

        let jobElements = $([]);
        let usedSelector = '';

        // Try each selector until we find jobs
        for (const selector of jobSelectors) {
            const elements = $(selector);
            if (elements.length > 0) {
                jobElements = elements;
                usedSelector = selector;
                log.info(`✓ Found ${elements.length} job elements with selector: ${selector}`);
                break;
            }
        }

        if (jobElements.length === 0) {
            log.warning('No job elements found with any known selector');
            // Save HTML for debugging
            await Actor.setValue('DEBUG_NO_JOBS_HTML', response.body, { contentType: 'text/html' });
            return { jobs: [] };
        }

        // Extract data from each job element
        jobElements.each((index, element) => {
            try {
                const job = extractJobFromElement($, $(element));
                if (job && job.title) {
                    jobs.push(job);
                }
            } catch (error) {
                log.debug(`Failed to extract job ${index}: ${error.message}`);
            }
        });

        if (jobs.length > 0) {
            stats.httpHtmlSuccess++;
            log.info(`🎉 HTTP HTML parsing successful: ${jobs.length} jobs`);
            return { jobs, method: 'HTTP_HTML', selector: usedSelector };
        }

        log.info('No jobs extracted from HTML elements');
        return { jobs: [] };

    } catch (error) {
        log.warning(`HTTP HTML parsing failed: ${error.message}`);
        stats.errors.push({ method: 'HTTP_HTML', error: error.message });
        return { jobs: [], error: error.message };
    }
}

/**
 * LAST RESORT: Browser-based extraction with Camoufox
 * Only used if HTTP methods fail or Cloudflare blocks
 */
async function extractJobsViaBrowser(url, proxyConfiguration) {
    log.info('🔥 LAST RESORT: Attempting browser-based extraction');

    try {
        const jobs = [];
        const proxyUrl = await proxyConfiguration.newUrl();

        const crawler = new PlaywrightCrawler({
            proxyConfiguration,
            maxRequestsPerCrawl: 1,
            maxConcurrency: 1,
            navigationTimeoutSecs: 45,
            requestHandlerTimeoutSecs: 120,
            launchContext: {
                launcher: firefox,
                launchOptions: await camoufoxLaunchOptions({
                    headless: true,
                    proxy: proxyUrl,
                    geoip: true,
                    os: 'windows',
                    locale: 'en-US',
                    screen: {
                        minWidth: 1280,
                        maxWidth: 1920,
                        minHeight: 720,
                        maxHeight: 1080,
                    },
                }),
            },

            async requestHandler({ page, request }) {
                log.info('Browser loaded, extracting data...');

                await page.goto(request.url, {
                    waitUntil: 'domcontentloaded',
                    timeout: 45000
                });

                // Wait for content to load
                await page.waitForTimeout(3000);

                // Check for Cloudflare
                const title = await page.title();
                if (title.includes('Just a moment') || title.includes('Cloudflare')) {
                    log.warning('Cloudflare challenge detected, waiting...');
                    await page.waitForTimeout(8000);
                }

                // Try to extract embedded JSON
                const embeddedData = await page.evaluate(() => {
                    const patterns = [
                        'window.__PRELOADED_STATE__',
                        'window.__INITIAL_STATE__',
                        'window.INITIAL_DATA',
                    ];

                    for (const pattern of patterns) {
                        try {
                            const data = eval(pattern);
                            if (data) return data;
                        } catch (e) {
                            // Continue
                        }
                    }
                    return null;
                });

                if (embeddedData) {
                    log.info('✓ Found embedded data in browser');
                    const extractedJobs = extractJobsFromData(embeddedData);
                    jobs.push(...extractedJobs);
                }

                // Fallback: Parse HTML
                if (jobs.length === 0) {
                    const html = await page.content();
                    const $ = cheerio.load(html);

                    const jobElements = $('article.job_result, .job_result, [data-job-id]');
                    log.info(`Found ${jobElements.length} job elements in browser HTML`);

                    jobElements.each((_, element) => {
                        const job = extractJobFromElement($, $(element));
                        if (job && job.title) {
                            jobs.push(job);
                        }
                    });
                }
            },
        });

        await crawler.run([url]);

        if (jobs.length > 0) {
            stats.browserSuccess++;
            log.info(`🎉 Browser extraction successful: ${jobs.length} jobs`);
            return { jobs, method: 'BROWSER' };
        }

        return { jobs: [] };

    } catch (error) {
        log.error(`Browser extraction failed: ${error.message}`);
        stats.errors.push({ method: 'BROWSER', error: error.message });
        return { jobs: [], error: error.message };
    }
}

/**
 * Extract jobs from various data structures (API responses, embedded data)
 */
function extractJobsFromData(data) {
    const jobs = [];

    try {
        // Try common paths where job arrays might be
        const possiblePaths = [
            data.jobs,
            data.job_results,
            data.results,
            data.data?.jobs,
            data.data?.job_results,
            data.data?.results,
            data.searchResults,
            data.jobResults,
        ];

        let jobArray = null;

        for (const path of possiblePaths) {
            if (Array.isArray(path) && path.length > 0) {
                jobArray = path;
                log.debug(`Found job array at path with ${path.length} items`);
                break;
            }
        }

        // If no direct array found, search recursively
        if (!jobArray && typeof data === 'object') {
            jobArray = findJobArray(data);
        }

        if (jobArray && Array.isArray(jobArray)) {
            for (const item of jobArray) {
                const job = parseJobObject(item);
                if (job) {
                    jobs.push(job);
                }
            }
        }

    } catch (error) {
        log.debug(`Error extracting jobs from data: ${error.message}`);
    }

    return jobs;
}

/**
 * Recursively find job arrays in nested objects
 */
function findJobArray(obj, depth = 0, maxDepth = 5) {
    if (depth > maxDepth) return null;

    if (Array.isArray(obj)) {
        // Check if this looks like a job array
        if (obj.length > 0 && obj[0]) {
            const first = obj[0];
            if (first.title || first.jobTitle || first.job_title || first.name) {
                return obj;
            }
        }
        return null;
    }

    if (typeof obj === 'object' && obj !== null) {
        for (const key in obj) {
            const result = findJobArray(obj[key], depth + 1, maxDepth);
            if (result) return result;
        }
    }

    return null;
}

/**
 * Parse job object from API/embedded data
 */
function parseJobObject(job) {
    try {
        const title = job.title || job.jobTitle || job.job_title || job.name || '';
        const company = job.company || job.companyName || job.employer || 
                       job.hiring_company?.name || job.hiringCompany?.name || '';
        const location = job.location || job.city || job.jobLocation || 
                        job.location_name || job.formatted_location || '';
        const salary = job.salary || job.compensation || job.salary_text || 
                      job.posted_salary || job.salaryText || 'Not specified';
        const jobType = job.employment_type || job.employmentType || job.type || 
                       job.job_type || job.jobType || 'Not specified';
        const postedDate = job.posted_time || job.postedDate || job.datePosted || 
                          job.posted_date || job.date_posted || '';
        const description = job.description || job.snippet || job.job_description || '';
        const url = job.url || job.link || job.job_url || job.jobUrl || job.save_job_url || '';

        if (!title && !url) return null;

        return {
            title: title || 'Unknown Title',
            company,
            location,
            salary,
            jobType,
            postedDate,
            descriptionHtml: description,
            descriptionText: stripHtml(description),
            url: normalizeUrl(url),
            applyUrl: normalizeUrl(job.apply_url || job.applyUrl || url),
            companyUrl: normalizeUrl(job.company_url || job.companyUrl || ''),
            benefits: Array.isArray(job.benefits) ? job.benefits.join(', ') : (job.benefits || ''),
            scrapedAt: new Date().toISOString(),
        };
    } catch (error) {
        log.debug(`Error parsing job object: ${error.message}`);
        return null;
    }
}

/**
 * Extract jobs from JSON-LD structured data
 */
function extractJobsFromJSONLD(data) {
    const jobs = [];

    try {
        const processJobPosting = (item) => {
            if (item['@type'] === 'JobPosting') {
                const hiringOrg = item.hiringOrganization || {};
                const jobLocation = item.jobLocation || {};
                const address = jobLocation.address || {};

                let location = '';
                if (typeof address === 'string') {
                    location = address;
                } else {
                    location = [
                        address.addressLocality,
                        address.addressRegion,
                        address.addressCountry
                    ].filter(Boolean).join(', ');
                }

                let salary = 'Not specified';
                if (item.baseSalary) {
                    const baseSalary = item.baseSalary;
                    if (baseSalary.value) {
                        const value = baseSalary.value;
                        if (typeof value === 'object') {
                            salary = `${value.minValue || ''} - ${value.maxValue || ''} ${baseSalary.currency || ''}`.trim();
                        } else {
                            salary = `${value} ${baseSalary.currency || ''}`.trim();
                        }
                    }
                }

                jobs.push({
                    title: item.title || '',
                    company: hiringOrg.name || '',
                    location: location,
                    salary: salary,
                    jobType: item.employmentType || 'Not specified',
                    postedDate: item.datePosted || '',
                    descriptionHtml: item.description || '',
                    descriptionText: stripHtml(item.description || ''),
                    url: normalizeUrl(item.url || ''),
                    applyUrl: normalizeUrl(item.url || ''),
                    companyUrl: normalizeUrl(hiringOrg.url || ''),
                    benefits: '',
                    scrapedAt: new Date().toISOString(),
                });
            }
        };

        if (Array.isArray(data)) {
            data.forEach(processJobPosting);
        } else if (data['@type'] === 'JobPosting') {
            processJobPosting(data);
        } else if (data['@graph']) {
            data['@graph'].forEach(processJobPosting);
        } else if (data['@type'] === 'ItemList' && data.itemListElement) {
            data.itemListElement.forEach(listItem => {
                const item = listItem.item || listItem;
                processJobPosting(item);
            });
        }

    } catch (error) {
        log.debug(`Error extracting JSON-LD: ${error.message}`);
    }

    return jobs;
}

/**
 * Extract job from HTML element using CSS selectors
 */
function extractJobFromElement($, $el) {
    try {
        // Title and URL
        const titleSelectors = ['h2 a', '.job_link', '[data-job-title]', '.job-title a', 'a.job-title-link', 'h3 a', 'a[href*="/job/"]'];
        let title = '';
        let url = '';

        for (const sel of titleSelectors) {
            const el = $el.find(sel).first();
            if (el.length) {
                title = el.text().trim();
                url = el.attr('href') || '';
                if (title) break;
            }
        }

        // Company
        const companySelectors = ['.hiring_company a', '.company_name', '[data-company-name]', '.job-company-name', 'a.company', '.company'];
        let company = '';
        let companyUrl = '';

        for (const sel of companySelectors) {
            const el = $el.find(sel).first();
            if (el.length) {
                company = el.text().trim();
                companyUrl = el.attr('href') || '';
                if (company) break;
            }
        }

        // Location
        const locationSelectors = ['.location', '.job_location', '[data-location]', '.job-location', '.job-location-text'];
        let location = '';
        for (const sel of locationSelectors) {
            const el = $el.find(sel).first();
            if (el.length) {
                location = el.text().trim();
                if (location) break;
            }
        }

        // Salary
        const salarySelectors = ['.salary', '.job_salary', '[data-salary]', '.compensation', '.job-salary'];
        let salary = 'Not specified';
        for (const sel of salarySelectors) {
            const el = $el.find(sel).first();
            if (el.length && el.text().trim()) {
                salary = el.text().trim();
                break;
            }
        }

        // Job Type
        const jobTypeSelectors = ['.employment-type', '.job-type', '[data-job-type]', '.job_type'];
        let jobType = 'Not specified';
        for (const sel of jobTypeSelectors) {
            const el = $el.find(sel).first();
            if (el.length && el.text().trim()) {
                jobType = el.text().trim();
                break;
            }
        }

        // Posted Date
        const dateSelectors = ['.posted_time', '.job-posted', '[data-posted-date]', 'time', '.post-time'];
        let postedDate = '';
        for (const sel of dateSelectors) {
            const el = $el.find(sel).first();
            if (el.length) {
                postedDate = el.text().trim();
                if (postedDate) break;
            }
        }

        // Description snippet
        const descSelectors = ['.job_snippet', '.job-description', '.snippet', '[data-description]', '.job-snippet'];
        let snippet = '';
        for (const sel of descSelectors) {
            const el = $el.find(sel).first();
            if (el.length) {
                snippet = el.text().trim();
                if (snippet) break;
            }
        }

        if (!title && !url) return null;

        return {
            title: title || 'Unknown Title',
            company,
            location,
            salary,
            jobType,
            postedDate,
            descriptionHtml: snippet,
            descriptionText: snippet,
            url: normalizeUrl(url),
            applyUrl: normalizeUrl(url),
            companyUrl: normalizeUrl(companyUrl),
            benefits: '',
            scrapedAt: new Date().toISOString(),
        };

    } catch (error) {
        log.debug(`Error extracting from element: ${error.message}`);
        return null;
    }
}

/**
 * Build ZipRecruiter search URL
 */
function buildSearchUrl(input) {
    if (input.searchUrl && input.searchUrl.trim()) {
        return input.searchUrl.trim();
    }

    const baseUrl = 'https://www.ziprecruiter.com/jobs-search';
    const params = new URLSearchParams();

    if (input.searchQuery) {
        params.append('search', input.searchQuery);
    }

    if (input.location) {
        params.append('location', input.location);
    }

    if (input.radius) {
        params.append('radius', input.radius);
    }

    if (input.daysBack && input.daysBack !== 'any') {
        params.append('days', input.daysBack);
    }

    return `${baseUrl}?${params.toString()}`;
}

/**
 * Enrich jobs with full descriptions (optional, only if needed)
 */
async function enrichJobsWithDescriptions(jobs, maxToEnrich = 10, proxyUrl = null) {
    if (jobs.length === 0) return jobs;

    log.info(`Enriching up to ${Math.min(jobs.length, maxToEnrich)} jobs with full descriptions...`);

    const enrichedJobs = [];
    let enrichedCount = 0;

    for (const job of jobs) {
        if (enrichedCount >= maxToEnrich) {
            enrichedJobs.push(job);
            continue;
        }

        // Only enrich if description is very short or missing
        if (!job.descriptionText || job.descriptionText.length < 100) {
            if (job.url) {
                try {
                    const requestOptions = {
                        url: job.url,
                        headers: {
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'Accept-Language': 'en-US,en;q=0.9',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
                        },
                        timeout: { request: 15000 },
                        retry: { limit: 1 },
                    };

                    if (proxyUrl) {
                        requestOptions.proxyUrl = proxyUrl;
                    }

                    const response = await gotScraping(requestOptions);
                    
                    if (response.statusCode === 200) {
                        const $ = cheerio.load(response.body);
                        
                        const descSelectors = ['.job_description', '.jobDescriptionSection', '[data-test="job-description"]', '.job-description-container', '#job-description'];
                        
                        for (const selector of descSelectors) {
                            const descEl = $(selector).first();
                            if (descEl.length && descEl.text().trim().length > 100) {
                                job.descriptionHtml = descEl.html()?.trim() || '';
                                job.descriptionText = descEl.text().trim();
                                enrichedCount++;
                                break;
                            }
                        }
                    }

                    // Rate limiting
                    await new Promise(resolve => setTimeout(resolve, 500));

                } catch (error) {
                    log.debug(`Failed to enrich job ${job.url}: ${error.message}`);
                }
            }
        }

        enrichedJobs.push(job);
    }

    if (enrichedCount > 0) {
        log.info(`✓ Enriched ${enrichedCount} jobs with full descriptions`);
    }

    return enrichedJobs;
}

/**
 * Find next page URL
 */
async function findNextPageUrl(currentUrl, html) {
    try {
        const $ = cheerio.load(html);
        
        // Try multiple pagination selectors
        const nextSelectors = [
            'a[rel="next"]',
            'a.next-page',
            '[aria-label="Next page"]',
            'a:contains("Next")',
            '.pagination a.next',
        ];

        for (const selector of nextSelectors) {
            const nextLink = $(selector).first();
            if (nextLink.length) {
                let href = nextLink.attr('href');
                if (href) {
                    href = normalizeUrl(href);
                    if (href !== currentUrl) {
                        return href;
                    }
                }
            }
        }

        // Fallback: Try incrementing page parameter
        const url = new URL(currentUrl);
        const currentPage = parseInt(url.searchParams.get('page') || '1');
        url.searchParams.set('page', (currentPage + 1).toString());
        
        return url.toString();

    } catch (error) {
        log.debug(`Error finding next page: ${error.message}`);
        return null;
    }
}

/**
 * Utility functions
 */
function stripHtml(html) {
    if (!html) return '';
    return html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

function normalizeUrl(url) {
    if (!url) return '';
    if (url.startsWith('http')) return url;
    if (url.startsWith('/')) return `https://www.ziprecruiter.com${url}`;
    return url;
}

/**
 * Main Actor execution
 */
try {
    const input = await Actor.getInput() || {};

    log.info('🚀 Starting ZipRecruiter Jobs Scraper (HTTP-First Optimized)', {
        searchUrl: input.searchUrl,
        searchQuery: input.searchQuery,
        location: input.location,
        maxJobs: input.maxJobs
    });

    // Validate input
    if (!input.searchUrl?.trim() && !input.searchQuery?.trim() && !input.location?.trim()) {
        throw new Error('Either "searchUrl" OR "searchQuery" and "location" must be provided');
    }

    const maxJobs = input.maxJobs ?? 50;
    if (maxJobs < 0 || maxJobs > 10000) {
        throw new Error('maxJobs must be between 0 and 10000');
    }

    const searchUrl = buildSearchUrl(input);
    log.info(`🔍 Search URL: ${searchUrl}`);

    // Create proxy configuration
    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || { 
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL']
        }
    );

    const proxyUrl = await proxyConfiguration.newUrl();

    // Statistics
    const startTime = Date.now();
    const seenJobUrls = new Set();
    let totalJobsScraped = 0;
    let pagesProcessed = 0;
    let extractionMethod = 'None';
    let useBrowser = false;

    // Process pages with pagination
    let currentUrl = searchUrl;
    let consecutiveFailures = 0;
    const maxConsecutiveFailures = 3;

    while (currentUrl && totalJobsScraped < maxJobs && pagesProcessed < 20) {
        pagesProcessed++;
        log.info(`\n📄 Processing page ${pagesProcessed}: ${currentUrl}`);
        stats.totalPages++;

        let pageJobs = [];
        let result = null;

        // STRATEGY 1: Try HTTP JSON extraction first (fastest)
        if (!useBrowser) {
            log.info('🔥 Attempting STRATEGY 1: HTTP JSON extraction');
            result = await extractJobsViaHTTPJSON(currentUrl, proxyUrl);
            
            if (result.blocked) {
                log.warning('⚠️  Cloudflare blocked HTTP requests, switching to browser mode');
                useBrowser = true;
            } else if (result.jobs && result.jobs.length > 0) {
                pageJobs = result.jobs;
                extractionMethod = 'HTTP_JSON';
                consecutiveFailures = 0;
            }
        }

        // STRATEGY 2: Try HTTP HTML parsing (if JSON failed)
        if (pageJobs.length === 0 && !useBrowser) {
            log.info('🔥 Attempting STRATEGY 2: HTTP HTML parsing');
            result = await extractJobsViaHTTPHTML(currentUrl, proxyUrl);
            
            if (result.jobs && result.jobs.length > 0) {
                pageJobs = result.jobs;
                extractionMethod = 'HTTP_HTML';
                consecutiveFailures = 0;
            } else {
                consecutiveFailures++;
            }
        }

        // STRATEGY 3: Use browser (last resort or if Cloudflare detected)
        if ((pageJobs.length === 0 && consecutiveFailures >= 2) || useBrowser) {
            log.info('🔥 Attempting STRATEGY 3: Browser-based extraction');
            result = await extractJobsViaBrowser(currentUrl, proxyConfiguration);
            
            if (result.jobs && result.jobs.length > 0) {
                pageJobs = result.jobs;
                extractionMethod = 'BROWSER';
                consecutiveFailures = 0;
            } else {
                consecutiveFailures++;
            }
        }

        if (pageJobs.length === 0) {
            log.warning(`⚠️  No jobs found on page ${pagesProcessed} (consecutive failures: ${consecutiveFailures})`);
            
            if (consecutiveFailures >= maxConsecutiveFailures) {
                log.error(`❌ Too many consecutive failures (${consecutiveFailures}), stopping pagination`);
                break;
            }
        }

        // Remove duplicates
        const uniqueJobs = pageJobs.filter(job => {
            if (!job.url) return true;
            if (seenJobUrls.has(job.url)) {
                return false;
            }
            seenJobUrls.add(job.url);
            return true;
        });

        if (uniqueJobs.length < pageJobs.length) {
            log.info(`🔍 Removed ${pageJobs.length - uniqueJobs.length} duplicate jobs`);
        }

        // Limit jobs to maxJobs
        let jobsToSave = maxJobs > 0 
            ? uniqueJobs.slice(0, Math.max(0, maxJobs - totalJobsScraped))
            : uniqueJobs;

        // Optional: Enrich descriptions (only for first few jobs)
        if (jobsToSave.length > 0 && input.enrichDescriptions !== false) {
            const enrichCount = Math.min(5, jobsToSave.length);
            log.info(`💎 Enriching ${enrichCount} jobs with full descriptions...`);
            jobsToSave = await enrichJobsWithDescriptions(jobsToSave, enrichCount, proxyUrl);
        }

        // Save to dataset
        if (jobsToSave.length > 0) {
            await Actor.pushData(jobsToSave);
            totalJobsScraped += jobsToSave.length;
            stats.totalJobs = totalJobsScraped;
            log.info(`✅ Saved ${jobsToSave.length} jobs. Total: ${totalJobsScraped}/${maxJobs || '∞'}`);
        }

        // Check if we've reached the limit
        if (maxJobs > 0 && totalJobsScraped >= maxJobs) {
            log.info(`🎯 Reached maximum jobs limit: ${maxJobs}`);
            break;
        }

        // Find next page
        if (result && result.jobs && result.jobs.length > 0) {
            // Get HTML for pagination (if we have it)
            let html = null;
            
            if (!useBrowser) {
                try {
                    const response = await gotScraping({
                        url: currentUrl,
                        headers: {
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
                        },
                        timeout: { request: 15000 },
                        proxyUrl: proxyUrl,
                    });
                    html = response.body;
                } catch (error) {
                    log.debug(`Failed to fetch HTML for pagination: ${error.message}`);
                }
            }

            if (html) {
                const nextUrl = await findNextPageUrl(currentUrl, html);
                if (nextUrl && nextUrl !== currentUrl) {
                    currentUrl = nextUrl;
                    log.info(`➡️  Found next page: ${currentUrl}`);
                    
                    // Rate limiting between pages
                    await new Promise(resolve => setTimeout(resolve, 2000));
                    continue;
                }
            }
        }

        // No more pages
        log.info('📭 No more pages to scrape');
        break;
    }

    // Calculate statistics
    const endTime = Date.now();
    const duration = Math.round((endTime - startTime) / 1000);

    const finalStats = {
        totalJobsScraped,
        pagesProcessed,
        primaryExtractionMethod: extractionMethod,
        methodsUsed: {
            httpJson: stats.httpJsonSuccess,
            httpHtml: stats.httpHtmlSuccess,
            browser: stats.browserSuccess,
        },
        duration: `${duration} seconds`,
        averageTimePerJob: totalJobsScraped > 0 ? `${(duration / totalJobsScraped).toFixed(2)}s` : 'N/A',
        errors: stats.errors.length,
        timestamp: new Date().toISOString(),
    };

    await Actor.setValue('STATISTICS', finalStats);

    log.info('\n🎉 ============================================');
    log.info('✅ SCRAPING COMPLETED SUCCESSFULLY!');
    log.info('============================================');
    log.info(`📊 Total Jobs Scraped: ${totalJobsScraped}`);
    log.info(`📄 Pages Processed: ${pagesProcessed}`);
    log.info(`⚡ Primary Method: ${extractionMethod}`);
    log.info(`🕐 Duration: ${duration} seconds`);
    log.info(`⚡ Average: ${finalStats.averageTimePerJob} per job`);
    log.info('============================================\n');

    if (totalJobsScraped === 0) {
        log.warning('⚠️  No jobs were scraped. Please check:');
        log.warning('  - Search parameters are correct');
        log.warning('  - URL returns results in browser');
        log.warning('  - Proxy configuration is working');
        log.warning('  - Check DEBUG_NO_JOBS_HTML in key-value store');
    }

} catch (error) {
    log.exception(error, '❌ Actor failed with error');
    
    // Save error details
    await Actor.setValue('ERROR_DETAILS', {
        message: error.message,
        stack: error.stack,
        stats: stats,
        timestamp: new Date().toISOString(),
    });
    
    throw error;
}

await Actor.exit();
