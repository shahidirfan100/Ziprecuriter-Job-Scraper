/**
 * ZipRecruiter Jobs Scraper - Fast & Stealthy
 * Hybrid pagination: Button clicks + Scroll fallback
 */

import { PlaywrightCrawler } from '@crawlee/playwright';
import { Actor, log } from 'apify';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';

await Actor.init();

// ============================================================================
// CONFIGURATION - Optimized for Speed
// ============================================================================
const CONFIG = {
    CLOUDFLARE_WAIT: 4000,      // Reduced for speed
    PAGE_WAIT: 1200,            // Wait after pagination
    SCROLL_WAIT: 600,           // Quick scroll wait
    MAX_PAGES: 50,
    MAX_EMPTY: 2,
};

// ============================================================================
// STATISTICS
// ============================================================================
const stats = {
    pages: 0,
    jobs: 0,
    startTime: Date.now(),
};

// ============================================================================
// URL BUILDER
// ============================================================================

function buildSearchUrl(input) {
    if (input.searchUrl?.trim()) return input.searchUrl.trim();

    const params = new URLSearchParams();
    if (input.searchQuery?.trim()) params.append('search', input.searchQuery.trim());
    if (input.location?.trim()) params.append('location', input.location.trim());
    if (input.radius && input.location?.trim()) params.append('radius', input.radius);
    if (input.daysBack && input.daysBack !== 'any') params.append('days', input.daysBack);

    return `https://www.ziprecruiter.com/jobs-search?${params.toString()}`;
}

// ============================================================================
// JOB EXTRACTION - Fast single pass
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
                    if (p.textContent?.includes('$')) salary = p.textContent.trim();
                });

                const postedMatch = (card.textContent || '').match(/Posted\s+(\d+\s+\w+\s+ago|today|yesterday)/i);

                if (title || company) {
                    jobs.push({
                        title: title || 'Unknown Title',
                        company,
                        companyUrl,
                        location,
                        salary,
                        jobType: 'Not specified',
                        postedDate: postedMatch?.[1] || '',
                        url: jobId ? `https://www.ziprecruiter.com/jobs/${jobId}` : '',
                        jobId,
                        scrapedAt: new Date().toISOString(),
                    });
                }
            } catch { /* skip */ }
        });
        return jobs;
    });
}

// ============================================================================
// PAGINATION - Hybrid: Button + Scroll
// ============================================================================

async function goNextPage(page) {
    // Method 1: Try Next button (works on many layouts)
    try {
        const nextBtn = await page.$('button[title="Next Page"]:not([disabled])');
        if (nextBtn) {
            const isVisible = await nextBtn.isVisible();
            if (isVisible) {
                await nextBtn.scrollIntoViewIfNeeded();
                await nextBtn.click();
                await page.waitForTimeout(CONFIG.PAGE_WAIT);
                return true;
            }
        }
    } catch { /* fallback to scroll */ }

    // Method 2: Try pagination links
    try {
        const nextLink = await page.$('a[title="Next Page"], a.next-page, [data-testid="next-page"]');
        if (nextLink) {
            await nextLink.click();
            await page.waitForTimeout(CONFIG.PAGE_WAIT);
            return true;
        }
    } catch { /* fallback to scroll */ }

    // Method 3: Scroll the job container to load more
    try {
        const countBefore = await page.evaluate(() =>
            document.querySelectorAll('.job_result_two_pane_v2').length
        );

        // Scroll the job list container
        await page.evaluate(() => {
            const container = document.querySelector('section.job_results_two_pane, .job_results_two_pane');
            if (container) {
                container.scrollTop = container.scrollHeight;
            } else {
                // Fallback: scroll the page
                window.scrollTo(0, document.body.scrollHeight);
            }
        });

        await page.waitForTimeout(CONFIG.SCROLL_WAIT);

        const countAfter = await page.evaluate(() =>
            document.querySelectorAll('.job_result_two_pane_v2').length
        );

        // More jobs loaded?
        return countAfter > countBefore;
    } catch {
        return false;
    }
}

async function scrollContainer(page) {
    await page.evaluate(() => {
        const container = document.querySelector('section.job_results_two_pane');
        if (container) {
            container.scrollTop = container.scrollHeight;
        }
    }).catch(() => { });
}

async function dismissPopups(page) {
    try {
        const btn = await page.$('button[aria-label="Close"]');
        if (btn) await btn.click();
    } catch { /* ignore */ }
}

// ============================================================================
// MAIN
// ============================================================================

try {
    const input = await Actor.getInput() || {};

    log.info('═══════════════════════════════════════════════════════════════');
    log.info('🚀 ZipRecruiter Jobs Scraper');
    log.info('═══════════════════════════════════════════════════════════════');

    if (!input.searchUrl?.trim() && !input.searchQuery?.trim()) {
        throw new Error('Provide "searchUrl" OR "searchQuery"');
    }

    const maxJobs = input.maxJobs ?? 50;
    const searchUrl = buildSearchUrl(input);

    log.info(`🔍 Query: ${input.searchQuery || 'N/A'}`);
    log.info(`📍 Location: ${input.location || 'Auto'}`);
    log.info(`🎯 Max Jobs: ${maxJobs}`);
    log.info(`🔗 ${searchUrl}`);
    log.info('───────────────────────────────────────────────────────────────');

    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] }
    );
    const proxyUrl = await proxyConfiguration.newUrl();

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
                'DNT': '1',
            });
        }],

        async requestHandler({ page }) {
            log.info('✓ Page loaded');
            await page.waitForTimeout(CONFIG.CLOUDFLARE_WAIT);

            const title = await page.title();
            if (title.includes('Just a moment')) {
                await page.waitForTimeout(4000);
            }

            try {
                await page.waitForSelector('.job_result_two_pane_v2', { timeout: 20000 });
                log.info(`✓ ${title}`);
            } catch {
                log.error('❌ No jobs found');
                await Actor.setValue('DEBUG', await page.content(), { contentType: 'text/html' });
                return;
            }

            await dismissPopups(page);
            await scrollContainer(page);

            let pageNum = 1;
            let emptyCount = 0;

            while (totalScraped < maxJobs && pageNum <= CONFIG.MAX_PAGES) {
                // Extract all visible jobs
                const jobs = await extractJobs(page);

                // Filter to new ones only
                const newJobs = jobs.filter(j => {
                    const key = j.jobId || `${j.title}-${j.company}`;
                    if (seenIds.has(key)) return false;
                    seenIds.add(key);
                    return true;
                });

                stats.pages++;

                if (newJobs.length === 0) {
                    emptyCount++;
                    if (emptyCount >= CONFIG.MAX_EMPTY) {
                        log.info('📭 End of results');
                        break;
                    }
                } else {
                    emptyCount = 0;
                    const toSave = newJobs.slice(0, maxJobs - totalScraped);
                    await Actor.pushData(toSave);
                    totalScraped += toSave.length;
                    stats.jobs += toSave.length;
                    log.info(`📄 Page ${pageNum}: +${toSave.length} | Total: ${totalScraped}/${maxJobs}`);
                }

                if (totalScraped >= maxJobs) {
                    log.info('🎯 Target reached!');
                    break;
                }

                // Try to get more jobs
                const hasMore = await goNextPage(page);
                if (!hasMore) {
                    // Try one more scroll
                    await scrollContainer(page);
                    await page.waitForTimeout(CONFIG.SCROLL_WAIT);

                    const moreJobs = await extractJobs(page);
                    const newAfterScroll = moreJobs.filter(j => !seenIds.has(j.jobId || `${j.title}-${j.company}`));

                    if (newAfterScroll.length === 0) {
                        log.info('📭 No more pages');
                        break;
                    }
                }

                await dismissPopups(page);
                pageNum++;
            }
        },

        failedRequestHandler({ error }) {
            log.error(`❌ ${error.message}`);
        },
    });

    await crawler.run([searchUrl]);

    const duration = Math.round((Date.now() - stats.startTime) / 1000);
    const speed = totalScraped > 0 ? (duration / totalScraped).toFixed(2) : 'N/A';

    await Actor.setValue('STATISTICS', {
        jobs: totalScraped,
        pages: stats.pages,
        duration: `${duration}s`,
        speed: `${speed}s/job`,
    });

    log.info('');
    log.info('═══════════════════════════════════════════════════════════════');
    log.info(`🎉 DONE! ${totalScraped} jobs | ${stats.pages} pages | ${duration}s (${speed}s/job)`);
    log.info('═══════════════════════════════════════════════════════════════');

} catch (error) {
    log.exception(error, '❌ Failed');
    throw error;
}

await Actor.exit();
