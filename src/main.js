/**
 * ZipRecruiter Jobs Scraper - Production Ready (Fast & Stealthy)
 * 
 * Extracts job listing data only (no descriptions) for maximum speed.
 * Uses Camoufox for Cloudflare bypass with advanced stealth features.
 */

import { PlaywrightCrawler } from '@crawlee/playwright';
import { Actor, log } from 'apify';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';

await Actor.init();

// ============================================================================
// CONFIGURATION - Optimized for Speed & Stealth
// ============================================================================
const CONFIG = {
    // Timing (ms) - Minimal waits
    CLOUDFLARE_WAIT: 5000,
    PAGE_LOAD_WAIT: 1500,
    SCROLL_WAIT: 200,
    BETWEEN_PAGES_MIN: 800,
    BETWEEN_PAGES_MAX: 1500,

    // Limits
    MAX_PAGES: 50,
    MAX_CONSECUTIVE_EMPTY: 2,

    // Stealth - User Agent rotation
    USER_AGENTS: [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0',
    ],
};

// ============================================================================
// STATISTICS
// ============================================================================
const stats = {
    pagesProcessed: 0,
    jobsExtracted: 0,
    duplicatesRemoved: 0,
    startTime: Date.now(),
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

const getRandomUA = () => CONFIG.USER_AGENTS[Math.floor(Math.random() * CONFIG.USER_AGENTS.length)];
const randomDelay = (min, max) => new Promise(r => setTimeout(r, min + Math.random() * (max - min)));

function buildSearchUrl(input) {
    if (input.searchUrl?.trim()) return input.searchUrl.trim();

    const params = new URLSearchParams();
    if (input.searchQuery) params.append('search', input.searchQuery);
    if (input.location) params.append('location', input.location);
    if (input.radius) params.append('radius', input.radius);
    if (input.daysBack && input.daysBack !== 'any') params.append('days', input.daysBack);
    if (input.employmentType?.length) input.employmentType.forEach(t => params.append('employment_type', t));
    if (input.salaryMin) params.append('salary_min', input.salaryMin.toString());
    if (input.remoteOnly) params.append('remote', '1');

    return `https://www.ziprecruiter.com/jobs-search?${params.toString()}`;
}

// ============================================================================
// FAST JOB EXTRACTION - Listing Data Only
// ============================================================================

async function extractJobs(page) {
    return page.evaluate(() => {
        const jobs = [];
        document.querySelectorAll('.job_result_two_pane_v2').forEach(card => {
            try {
                const article = card.querySelector('article');
                const jobIdMatch = (article?.id || '').match(/job-card-(.+)/);
                const jobId = jobIdMatch?.[1] || '';

                const titleEl = card.querySelector('h2');
                const title = titleEl?.getAttribute('aria-label')?.trim() || titleEl?.textContent?.trim() || '';

                const companyEl = card.querySelector('[data-testid="job-card-company"]');
                const company = companyEl?.textContent?.trim() || '';
                let companyUrl = companyEl?.getAttribute('href') || '';
                if (companyUrl?.startsWith('/')) companyUrl = `https://www.ziprecruiter.com${companyUrl}`;

                const locationEl = card.querySelector('[data-testid="job-card-location"]');
                let location = locationEl?.textContent?.trim() || '';
                locationEl?.parentElement?.querySelectorAll('span').forEach(s => {
                    if (s.textContent.toLowerCase().includes('remote')) location += ' (Remote)';
                });

                let salary = 'Not specified';
                card.querySelectorAll('p').forEach(p => {
                    if (p.textContent.includes('$')) { salary = p.textContent.trim(); }
                });

                const postedMatch = (card.textContent || '').match(/Posted\s+(\d+\s+\w+\s+ago|today|yesterday|just now)/i);
                const postedDate = postedMatch?.[1] || '';

                if (title || company) {
                    jobs.push({
                        title: title || 'Unknown Title',
                        company,
                        companyUrl,
                        location,
                        salary,
                        jobType: 'Not specified',
                        postedDate,
                        url: jobId ? `https://www.ziprecruiter.com/jobs/${jobId}` : '',
                        jobId,
                        scrapedAt: new Date().toISOString(),
                    });
                }
            } catch (e) { /* skip */ }
        });
        return jobs;
    });
}

// ============================================================================
// PAGINATION
// ============================================================================

async function goToNextPage(page) {
    try {
        const btn = await page.$('button[title="Next Page"]');
        if (!btn || await btn.evaluate(b => b.disabled)) return false;
        await btn.scrollIntoViewIfNeeded();
        await btn.click();
        await page.waitForTimeout(CONFIG.PAGE_LOAD_WAIT);
        return true;
    } catch { return false; }
}

async function scrollList(page) {
    await page.evaluate(() => {
        const c = document.querySelector('.job_results_two_pane');
        if (c) { c.scrollTop = c.scrollHeight; }
    }).catch(() => { });
    await page.waitForTimeout(CONFIG.SCROLL_WAIT);
}

async function dismissPopups(page) {
    for (const sel of ['button[aria-label="Close"]', '.modal-close', 'button.close']) {
        await page.$(sel).then(b => b?.click()).catch(() => { });
    }
}

// ============================================================================
// MAIN
// ============================================================================

try {
    const input = await Actor.getInput() || {};

    log.info('═══════════════════════════════════════════════════════════════');
    log.info('🚀 ZipRecruiter Jobs Scraper - FAST Listing Mode');
    log.info('═══════════════════════════════════════════════════════════════');

    if (!input.searchUrl?.trim() && !input.searchQuery?.trim() && !input.location?.trim()) {
        throw new Error('Provide "searchUrl" OR "searchQuery"/"location"');
    }

    const maxJobs = input.maxJobs ?? 50;
    const searchUrl = buildSearchUrl(input);

    log.info(`🔍 Query: ${input.searchQuery || 'N/A'} | 📍 Location: ${input.location || 'N/A'}`);
    log.info(`🎯 Max: ${maxJobs} jobs | 🔗 ${searchUrl}`);
    log.info('───────────────────────────────────────────────────────────────');

    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] }
    );
    const proxyUrl = await proxyConfiguration.newUrl();
    const userAgent = getRandomUA();

    const seenIds = new Set();
    let totalScraped = 0;

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: 1,
        maxConcurrency: 1,
        navigationTimeoutSecs: 45,
        requestHandlerTimeoutSecs: 300,

        launchContext: {
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: true,
                proxy: proxyUrl,
                geoip: true,
                os: 'windows',
                locale: 'en-US',
                screen: { minWidth: 1280, maxWidth: 1920, minHeight: 720, maxHeight: 1080 },
            }),
        },

        preNavigationHooks: [async ({ page }) => {
            await page.setExtraHTTPHeaders({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            });
        }],

        async requestHandler({ page }) {
            log.info(`✓ Loaded page`);
            log.info(`⏳ Cloudflare bypass...`);
            await page.waitForTimeout(CONFIG.CLOUDFLARE_WAIT);

            const title = await page.title();
            if (title.includes('Just a moment') || title.includes('Cloudflare')) {
                log.warning('⚠️  Extra Cloudflare wait...');
                await page.waitForTimeout(4000);
            }

            try {
                await page.waitForSelector('.job_result_two_pane_v2', { timeout: 25000 });
                log.info(`✓ Jobs loaded: ${title}`);
            } catch {
                log.error('❌ No jobs found');
                await Actor.setValue('DEBUG_HTML', await page.content(), { contentType: 'text/html' });
                return;
            }

            await dismissPopups(page);

            let pageNum = 1;
            let emptyCount = 0;

            while (totalScraped < maxJobs && pageNum <= CONFIG.MAX_PAGES) {
                await scrollList(page);

                const jobs = await extractJobs(page);
                const unique = jobs.filter(j => {
                    const key = j.jobId || `${j.title}-${j.company}`;
                    if (seenIds.has(key)) { stats.duplicatesRemoved++; return false; }
                    seenIds.add(key);
                    return true;
                });

                stats.pagesProcessed++;

                if (unique.length === 0) {
                    if (++emptyCount >= CONFIG.MAX_CONSECUTIVE_EMPTY) {
                        log.info('📭 End of results');
                        break;
                    }
                } else {
                    emptyCount = 0;
                    const toSave = unique.slice(0, maxJobs - totalScraped);
                    await Actor.pushData(toSave);
                    totalScraped += toSave.length;
                    stats.jobsExtracted += toSave.length;
                    log.info(`📄 Page ${pageNum}: +${toSave.length} jobs | Total: ${totalScraped}/${maxJobs}`);
                }

                if (totalScraped >= maxJobs) {
                    log.info(`🎯 Target reached!`);
                    break;
                }

                if (!await goToNextPage(page)) {
                    log.info('📭 No more pages');
                    break;
                }

                await dismissPopups(page);
                await randomDelay(CONFIG.BETWEEN_PAGES_MIN, CONFIG.BETWEEN_PAGES_MAX);
                pageNum++;
            }
        },

        failedRequestHandler({ error }) {
            log.error(`❌ Failed: ${error.message}`);
        },
    });

    await crawler.run([searchUrl]);

    // Stats
    const duration = Math.round((Date.now() - stats.startTime) / 1000);
    const avg = totalScraped > 0 ? (duration / totalScraped).toFixed(2) : 'N/A';

    await Actor.setValue('STATISTICS', {
        totalJobsScraped: totalScraped,
        pagesProcessed: stats.pagesProcessed,
        duplicatesRemoved: stats.duplicatesRemoved,
        duration: `${duration}s`,
        avgPerJob: `${avg}s`,
    });

    log.info('');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info(`🎉 DONE! ${totalScraped} jobs | ${stats.pagesProcessed} pages | ${duration}s (${avg}s/job)`);
    log.info('═══════════════════════════════════════════════════════════════');

} catch (error) {
    log.exception(error, '❌ Failed');
    throw error;
}

await Actor.exit();
