import { PlaywrightCrawler } from '@crawlee/playwright';
import { Actor, log } from 'apify';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';

await Actor.init();

const CONFIG = {
    CLOUDFLARE_WAIT_MS: 2200,
    CONTENT_WAIT_MS: 300,
    JOBS_PER_PAGE: 20,
    DEFAULT_MAX_PAGES: 50,
    MAX_PAGES_HARD_LIMIT: 200,
    DETAIL_CONCURRENCY: 3,
};

const stats = {
    pagesProcessed: 0,
    apiPagesProcessed: 0,
    domFallbackPages: 0,
    jobsExtracted: 0,
    detailCalls: 0,
    detailFailures: 0,
    startTime: Date.now(),
    apiEndpoints: new Set(),
};

const seenJobIds = new Set();

const asArray = (value) => (Array.isArray(value) ? value : []);

const safeJsonParse = (value, fallback = null) => {
    try {
        return JSON.parse(value);
    } catch {
        return fallback;
    }
};

function normalizeUrl(value) {
    if (!value || typeof value !== 'string') return '';
    const raw = value.startsWith('/') ? `https://www.ziprecruiter.com${value}` : value;
    if (!raw.startsWith('http://') && !raw.startsWith('https://')) return '';
    try {
        return new URL(raw).toString();
    } catch {
        return '';
    }
}

function stripHtml(html) {
    if (!html || typeof html !== 'string') return '';
    return html
        .replace(/<style[\s\S]*?<\/style>/gi, ' ')
        .replace(/<script[\s\S]*?<\/script>/gi, ' ')
        .replace(/<[^>]+>/g, ' ')
        .replace(/&nbsp;/gi, ' ')
        .replace(/&amp;/gi, '&')
        .replace(/\s+/g, ' ')
        .trim();
}

function normalizePayInterval(interval) {
    if (!interval || typeof interval !== 'string') return '';
    return interval.replace(/^PAY_INTERVAL_/i, '').toLowerCase();
}

function formatCurrency(value, currency = 'USD') {
    if (typeof value !== 'number' || Number.isNaN(value)) return '';
    try {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency,
            maximumFractionDigits: 0,
        }).format(value);
    } catch {
        return `$${value.toLocaleString('en-US')}`;
    }
}

function formatSalary(pay = {}) {
    const min = typeof pay.min === 'number' ? pay.min : null;
    const max = typeof pay.max === 'number' ? pay.max : null;
    const minAnnual = typeof pay.minAnnual === 'number' ? pay.minAnnual : null;
    const maxAnnual = typeof pay.maxAnnual === 'number' ? pay.maxAnnual : null;
    const currency = typeof pay.currency === 'string' ? pay.currency.replace(/^PAY_CURRENCY_/i, '') : 'USD';
    const interval = normalizePayInterval(pay.interval);

    let salary = '';
    if (min !== null && max !== null) {
        salary = `${formatCurrency(min, currency)} - ${formatCurrency(max, currency)}`;
    } else if (min !== null) {
        salary = `${formatCurrency(min, currency)}+`;
    } else if (max !== null) {
        salary = `Up to ${formatCurrency(max, currency)}`;
    } else if (minAnnual !== null && maxAnnual !== null) {
        salary = `${formatCurrency(minAnnual, currency)} - ${formatCurrency(maxAnnual, currency)} / year`;
    }

    if (salary && interval) salary = `${salary} / ${interval}`;

    return {
        salary: salary || 'Not specified',
        salaryMin: min,
        salaryMax: max,
        salaryMinAnnual: minAnnual,
        salaryMaxAnnual: maxAnnual,
        salaryInterval: interval,
        salaryCurrency: currency,
    };
}

function normalizeTypedNames(items) {
    return asArray(items)
        .map((item) => item?.name)
        .filter((name) => typeof name === 'string' && name.trim())
        .map((name) => name.trim());
}

function buildSearchUrl(input, pageNum = 1) {
    let baseUrl;

    if (input.searchUrl?.trim()) {
        baseUrl = input.searchUrl.trim();
    } else {
        const params = new URLSearchParams();
        if (input.searchQuery?.trim()) params.append('search', input.searchQuery.trim());
        if (input.location?.trim()) params.append('location', input.location.trim());
        if (input.daysBack && input.daysBack !== 'any') params.append('days', input.daysBack);
        baseUrl = `https://www.ziprecruiter.com/jobs-search?${params.toString()}`;
    }

    const url = new URL(baseUrl);
    url.searchParams.set('page', String(pageNum));
    return url.toString();
}

async function dismissPopups(page) {
    try {
        const selectors = [
            'button[aria-label="Close"]',
            'button:has-text("Not now")',
            'button:has-text("No Thanks")',
        ];
        for (const selector of selectors) {
            const button = page.locator(selector).first();
            if (await button.count()) {
                await button.click({ timeout: 3000 }).catch(() => {});
            }
        }
        await page.keyboard.press('Escape').catch(() => {});
    } catch {
        // Ignore popup failures.
    }
}

async function extractModelFromJsVariables(page) {
    const raw = await page.evaluate(() => {
        const el = document.querySelector('#js_variables');
        return el?.textContent || '';
    });

    if (!raw) return null;
    return safeJsonParse(raw, null);
}

function parseApiDataFromModel(model) {
    if (!model || typeof model !== 'object') {
        return {
            pageNum: null,
            maxPages: null,
            placementId: null,
            impressionLotId: '',
            jobKeys: [],
            jobCards: [],
        };
    }

    const impressionLotId = model.impressionSetId
        || model.suggestedSearchData?.impressionSetId
        || model.suggestedSearchData?.impressionSupersetId
        || '';

    const placementId = Number(model.placementID || model.placementId || 0) || null;

    const jobKeys = asArray(model.listJobKeysResponse?.jobKeys)
        .filter((jobKey) => jobKey?.listingKey && jobKey?.matchId)
        .map((jobKey) => ({
            listingKey: jobKey.listingKey,
            matchId: jobKey.matchId,
            bidTrackingData: jobKey.bidTrackingData || '',
        }));

    const jobCards = asArray(model.hydrateJobCardsResponse?.jobCards)
        .filter((jobCard) => jobCard?.listingKey);

    return {
        pageNum: Number(model.page || 0) || null,
        maxPages: Number(model.maxPages || 0) || null,
        placementId,
        impressionLotId,
        jobKeys,
        jobCards,
    };
}

async function hydrateJobCardsViaApi(page, jobKeys) {
    if (!jobKeys.length) return [];

    const result = await page.evaluate(async ({ keys }) => {
        try {
            const response = await fetch('/job_services.job_card.api_public.public.api.v1.API/HydrateJobCards', {
                method: 'POST',
                headers: {
                    'content-type': 'application/json',
                    accept: 'application/json',
                },
                credentials: 'include',
                body: JSON.stringify({ jobKeys: keys }),
            });

            if (!response.ok) {
                return { ok: false, status: response.status, jobCards: [] };
            }

            const data = await response.json().catch(() => ({}));
            return { ok: true, status: response.status, jobCards: Array.isArray(data?.jobCards) ? data.jobCards : [] };
        } catch (error) {
            return { ok: false, status: 0, error: String(error), jobCards: [] };
        }
    }, { keys: jobKeys });

    return result.jobCards || [];
}

async function fetchListingDetailsBatchViaApi(page, options) {
    const {
        jobKeys,
        placementId,
        impressionLotId,
    } = options;

    const validJobKeys = jobKeys
        .filter((jobKey) => jobKey?.listingKey && jobKey?.matchId)
        .map((jobKey) => ({
            listingKey: jobKey.listingKey,
            matchId: jobKey.matchId,
            bidTrackingData: jobKey.bidTrackingData || '',
        }));

    if (!validJobKeys.length || !placementId || !impressionLotId) {
        return { detailsByListing: new Map(), failed: 0 };
    }

    const results = await page.evaluate(async ({ keys, placementIdValue, impressionLotIdValue, maxConcurrency }) => {
        const endpoint = '/job_services.job_card.api_public.public.api.v1.API/GetJobDetails';
        const collected = [];
        let cursor = 0;

        async function runWorker() {
            while (cursor < keys.length) {
                const index = cursor;
                cursor += 1;

                const key = keys[index];
                const payload = {
                    jobKey: {
                        listingKey: key.listingKey,
                        matchId: key.matchId,
                        bidTrackingData: key.bidTrackingData || '',
                    },
                    placementId: placementIdValue,
                    impressionLotId: impressionLotIdValue,
                };

                try {
                    const response = await fetch(endpoint, {
                        method: 'POST',
                        headers: {
                            'content-type': 'application/json',
                            accept: 'application/json',
                        },
                        credentials: 'include',
                        body: JSON.stringify(payload),
                    });

                    if (!response.ok) {
                        const text = await response.text().catch(() => '');
                        collected.push({
                            listingKey: key.listingKey,
                            ok: false,
                            status: response.status,
                            error: text.slice(0, 300),
                        });
                        continue;
                    }

                    const data = await response.json().catch(() => ({}));
                    collected.push({
                        listingKey: key.listingKey,
                        ok: true,
                        status: response.status,
                        jobDetails: data?.jobDetails || null,
                    });
                    await new Promise((resolve) => {
                        setTimeout(resolve, 40 + Math.floor(Math.random() * 80));
                    });
                } catch (error) {
                    collected.push({
                        listingKey: key.listingKey,
                        ok: false,
                        status: 0,
                        error: String(error),
                    });
                }
            }
        }

        await Promise.all(Array.from({ length: maxConcurrency }, () => runWorker()));
        return collected;
    }, {
        keys: validJobKeys,
        placementIdValue: placementId,
        impressionLotIdValue: impressionLotId,
        maxConcurrency: CONFIG.DETAIL_CONCURRENCY,
    });

    const detailsByListing = new Map();
    let failed = 0;

    for (const result of results) {
        if (result?.ok && result.jobDetails) {
            detailsByListing.set(result.listingKey, result.jobDetails);
        } else {
            failed += 1;
        }
    }

    return { detailsByListing, failed };
}

function normalizeJobRecord(card, detail, context) {
    const status = detail?.status || card?.status || {};
    const company = detail?.company || card?.company || {};
    const companyWidget = detail?.companyWidget || {};
    const location = detail?.location || card?.location || {};
    const pay = detail?.pay || card?.pay || {};
    const applyButton = detail?.applyButtonConfig || card?.applyButtonConfig || {};

    const employmentTypes = normalizeTypedNames(detail?.employmentTypes || card?.employmentTypes);
    const locationTypes = normalizeTypedNames(detail?.locationTypes || card?.locationTypes);

    const salaryInfo = formatSalary(pay);

    const canonicalJobUrl = normalizeUrl(detail?.rawCanonicalZipJobPageUrl || card?.rawCanonicalZipJobPageUrl || '');
    const redirectJobUrl = normalizeUrl(card?.jobRedirectPageUrl || '');
    const jobUrl = canonicalJobUrl || redirectJobUrl;

    const externalApplyUrl = normalizeUrl(applyButton.externalApplyUrl || '');
    const companyUrl = normalizeUrl(detail?.companyUrl || card?.companyUrl || companyWidget.companyPageLink || '');
    const locationUrl = normalizeUrl(detail?.locationUrl || card?.locationUrl || '');

    const shortDescription = card?.shortDescription || '';
    const htmlDescription = detail?.htmlFullDescription || '';
    const textDescription = stripHtml(htmlDescription) || shortDescription;

    const locationName = location.displayName || card?.location?.displayName || '';
    const remoteByType = locationTypes.some((type) => /remote/i.test(type));
    const remoteByLocation = /remote/i.test(locationName);

    return {
        title: detail?.title || card?.title || 'Unknown Title',
        company: company.canonicalDisplayName || company.name || companyWidget.displayName || '',
        companyCanonicalName: company.canonicalDisplayName || '',
        companyId: company.id || '',
        companyUrl: companyUrl || undefined,
        companyLogoUrl: detail?.companyLogoUrl || card?.companyLogo?.logoUrl || '',
        companyWidget,

        location: locationName,
        locationUrl: locationUrl || undefined,
        locationCity: location.city || '',
        locationState: location.state || '',
        locationStateCode: location.stateCode || '',
        locationCountry: location.country || '',
        locationCountryCode: location.countryCode || '',
        locationTypes,
        locationType: locationTypes.join(', '),
        isRemote: remoteByType || remoteByLocation,

        employmentTypes,
        jobType: employmentTypes.join(', ') || 'Not specified',

        salary: salaryInfo.salary,
        salaryMin: salaryInfo.salaryMin,
        salaryMax: salaryInfo.salaryMax,
        salaryMinAnnual: salaryInfo.salaryMinAnnual,
        salaryMaxAnnual: salaryInfo.salaryMaxAnnual,
        salaryInterval: salaryInfo.salaryInterval,
        salaryCurrency: salaryInfo.salaryCurrency,

        postedDate: status.postedAtUtc || '',
        postedAtUtc: status.postedAtUtc || '',
        rollingPostedAtUtc: status.rollingPostedAtUtc || '',
        isActive: typeof status.isActive === 'boolean' ? status.isActive : null,

        url: jobUrl || undefined,
        externalApplyUrl: externalApplyUrl || undefined,
        applyButtonType: applyButton.applyButtonType || '',
        applyDestination: applyButton.destination || '',
        applyStatus: applyButton.currentApplicationStatus || null,

        listingKey: card?.listingKey || detail?.listingKey || '',
        matchId: card?.matchId || '',
        jobId: card?.listingKey || detail?.listingKey || '',
        openSeatId: card?.openSeatId || '',

        description: textDescription,
        shortDescription,
        htmlDescription,

        searchQuery: context.searchQuery || '',
        searchLocation: context.searchLocation || '',
        page: context.page,

        rawCard: card || null,
        rawDetails: detail || null,
        scrapedAt: new Date().toISOString(),
    };
}

async function extractJobsByDomFallback(page) {
    return page.evaluate(() => {
        const cards = Array.from(document.querySelectorAll('.job_result_two_pane_v2'));

        return cards.map((card) => {
            const article = card.querySelector('article');
            const articleId = article?.id || '';
            const listingKey = articleId.replace('job-card-', '');

            const titleEl = card.querySelector('h2');
            const title = titleEl?.getAttribute('aria-label')?.trim() || titleEl?.textContent?.trim() || '';

            const companyEl = card.querySelector('[data-testid="job-card-company"]');
            const company = companyEl?.textContent?.trim() || '';
            const companyUrl = companyEl?.getAttribute('href') || '';

            const locationEl = card.querySelector('[data-testid="job-card-location"]');
            const location = locationEl?.textContent?.trim() || '';

            let salary = 'Not specified';
            card.querySelectorAll('p').forEach((paragraph) => {
                const text = paragraph.textContent?.trim() || '';
                if (text.includes('$')) salary = text;
            });

            const text = card.textContent || '';
            const postedMatch = text.match(/Posted\s+(\d+\s+\w+\s+ago|today|yesterday)/i);

            return {
                title: title || 'Unknown Title',
                company,
                companyUrl,
                location,
                salary,
                postedDate: postedMatch?.[1] || '',
                listingKey,
                jobId: listingKey,
                url: listingKey ? `https://www.ziprecruiter.com/jobs/${listingKey}` : '',
                scrapedAt: new Date().toISOString(),
            };
        }).filter((job) => job.title || job.company);
    });
}

try {
    const input = (await Actor.getInput()) || {};

    if (!input.searchUrl?.trim() && !input.searchQuery?.trim()) {
        throw new Error('Provide "searchUrl" OR "searchQuery"');
    }

    const maxJobsInput = Number(input.maxJobs ?? 20);
    const targetJobs = maxJobsInput > 0 ? maxJobsInput : Number.POSITIVE_INFINITY;

    const inferredPagesFromTarget = Number.isFinite(targetJobs)
        ? Math.ceil(targetJobs / CONFIG.JOBS_PER_PAGE) + 2
        : CONFIG.DEFAULT_MAX_PAGES;

    const maxPages = Math.min(
        Math.max(Number(input.maxPages) || inferredPagesFromTarget, 1),
        CONFIG.MAX_PAGES_HARD_LIMIT,
    );

    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] },
    );

    const proxyUrl = await proxyConfiguration?.newUrl();

    log.info('Starting ZipRecruiter actor in API-first mode', {
        searchQuery: input.searchQuery || null,
        location: input.location || null,
        maxJobs: Number.isFinite(targetJobs) ? targetJobs : 0,
        maxPages,
    });

    let totalScraped = 0;
    let consecutiveEmpty = 0;

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: maxPages,
        maxConcurrency: 1,
        maxRequestRetries: 2,
        navigationTimeoutSecs: 90,
        requestHandlerTimeoutSecs: 210,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 20,
        },

        launchContext: {
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: true,
                geoip: true,
                os: 'windows',
                locale: 'en-US',
                ...(proxyUrl ? { proxy: proxyUrl } : {}),
                screen: {
                    minWidth: 1280,
                    maxWidth: 1920,
                    minHeight: 720,
                    maxHeight: 1080,
                },
            }),
        },

        preNavigationHooks: [
            async ({ page }) => {
                await page.route('**/*', async (route) => {
                    const request = route.request();
                    const resourceType = request.resourceType();
                    const url = request.url();

                    if (resourceType === 'image' || resourceType === 'font' || resourceType === 'media') {
                        await route.abort();
                        return;
                    }

                    if (
                        url.includes('google-analytics.com')
                        || url.includes('googletagmanager.com')
                        || url.includes('doubleclick.net')
                        || url.includes('hotjar.com')
                        || url.includes('sentry.io')
                        || url.includes('ketchcdn.com')
                        || url.includes('featureassets.org')
                        || url.includes('prodregistryv2.org')
                    ) {
                        await route.abort();
                        return;
                    }

                    await route.continue();
                });

                await page.setExtraHTTPHeaders({
                    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    DNT: '1',
                });
            },
        ],

        async requestHandler({ page, request }) {
            const pageNum = Number(request.userData.pageNum) || 1;
            const remainingSlots = Number.isFinite(targetJobs) ? Math.max(targetJobs - totalScraped, 0) : Number.POSITIVE_INFINITY;
            if (remainingSlots === 0) return;

            const apiCapture = {
                hydratedCards: [],
            };

            const onResponse = async (response) => {
                try {
                    const url = response.url();
                    const contentType = response.headers()['content-type'] || '';
                    if (!url.includes('/job_services.job_card.api_public.public.api.v1.API/')) return;

                    stats.apiEndpoints.add(url);

                    if (!contentType.includes('application/json')) return;

                    const json = await response.json().catch(() => null);
                    if (!json || typeof json !== 'object') return;
                    if (Array.isArray(json.jobCards) && json.jobCards.length) {
                        apiCapture.hydratedCards.push(...json.jobCards);
                    }
                } catch {
                    // Ignore capture errors.
                }
            };

            page.on('response', onResponse);

            try {
                await page.waitForLoadState('domcontentloaded', { timeout: 20000 }).catch(() => {});
                await page.waitForTimeout(CONFIG.CONTENT_WAIT_MS);

                const pageTitle = await page.title();
                if (pageTitle.includes('Just a moment')) {
                    await page.waitForTimeout(CONFIG.CLOUDFLARE_WAIT_MS);
                }

                await dismissPopups(page);
                await page.waitForSelector('#js_variables', { timeout: 12000 }).catch(() => {});

                let records = [];
                let usedDomFallback = false;

                const model = await extractModelFromJsVariables(page);
                const modelData = parseApiDataFromModel(model);

                let { jobCards } = modelData;
                const keyMap = new Map(modelData.jobKeys.map((jobKey) => [jobKey.listingKey, jobKey]));

                if (!jobCards.length && modelData.jobKeys.length) {
                    const hydrated = await hydrateJobCardsViaApi(page, modelData.jobKeys);
                    if (hydrated.length) {
                        jobCards = hydrated;
                    }
                }

                if (!jobCards.length && apiCapture.hydratedCards.length) {
                    jobCards = apiCapture.hydratedCards;
                }

                if (jobCards.length) {
                    stats.apiPagesProcessed += 1;

                    const pageCards = jobCards
                        .filter((card) => card?.listingKey)
                        .slice(0, Number.isFinite(remainingSlots) ? remainingSlots : undefined);

                    const jobKeysForDetails = pageCards
                        .map((card) => ({
                            listingKey: card.listingKey,
                            matchId: card.matchId || keyMap.get(card.listingKey)?.matchId || '',
                            bidTrackingData: keyMap.get(card.listingKey)?.bidTrackingData || card.bidTrackingData || '',
                        }))
                        .filter((jobKey) => jobKey.listingKey && jobKey.matchId);

                    stats.detailCalls += jobKeysForDetails.length;
                    const detailResult = await fetchListingDetailsBatchViaApi(page, {
                        jobKeys: jobKeysForDetails,
                        placementId: modelData.placementId,
                        impressionLotId: modelData.impressionLotId,
                    });
                    const { detailsByListing, failed } = detailResult;
                    stats.detailFailures += failed;

                    records = pageCards.map((card) => {
                        const detail = detailsByListing.get(card.listingKey) || null;
                        return normalizeJobRecord(card, detail, {
                            searchQuery: input.searchQuery || '',
                            searchLocation: input.location || '',
                            page: pageNum,
                        });
                    });
                }

                if (!records.length) {
                    usedDomFallback = true;
                    stats.domFallbackPages += 1;
                    log.warning(`API extraction returned no jobs on page ${pageNum}. Falling back to DOM.`);
                    records = await extractJobsByDomFallback(page);
                }

                stats.pagesProcessed += 1;

                const uniqueRecords = [];
                for (const record of records) {
                    const dedupeKey = record.listingKey
                        || record.jobId
                        || `${record.title || ''}|${record.company || ''}|${record.location || ''}`;

                    if (!dedupeKey || seenJobIds.has(dedupeKey)) continue;
                    seenJobIds.add(dedupeKey);
                    uniqueRecords.push(record);
                }

                if (!uniqueRecords.length) {
                    consecutiveEmpty += 1;
                    log.info(`Page ${pageNum}: no new jobs`);
                } else {
                    consecutiveEmpty = 0;
                    const limitedRecords = Number.isFinite(remainingSlots)
                        ? uniqueRecords.slice(0, remainingSlots)
                        : uniqueRecords;

                    await Actor.pushData(limitedRecords);
                    totalScraped += limitedRecords.length;
                    stats.jobsExtracted += limitedRecords.length;

                    log.info(`Page ${pageNum}: extracted ${limitedRecords.length} jobs`, {
                        total: totalScraped,
                        mode: usedDomFallback ? 'dom-fallback' : 'api-first',
                    });
                }

                const reachedTarget = Number.isFinite(targetJobs) && totalScraped >= targetJobs;
                if (reachedTarget) {
                    log.info(`Target reached on page ${pageNum}`);
                    return;
                }

                if (consecutiveEmpty >= 2) {
                    log.info(`Stopping after ${consecutiveEmpty} empty pages`);
                    return;
                }

                const nextPage = pageNum + 1;
                if (nextPage <= maxPages) {
                    await crawler.addRequests([{
                        url: buildSearchUrl(input, nextPage),
                        userData: { pageNum: nextPage },
                    }]);
                }
            } finally {
                page.off('response', onResponse);
            }
        },

        failedRequestHandler({ request, error }) {
            log.error(`Request failed for page ${request.userData.pageNum || 1}: ${error.message}`);
        },
    });

    await crawler.run([{
        url: buildSearchUrl(input, 1),
        userData: { pageNum: 1 },
    }]);

    const durationSeconds = Math.round((Date.now() - stats.startTime) / 1000);
    const jobsPerSecond = durationSeconds > 0 ? Number((stats.jobsExtracted / durationSeconds).toFixed(3)) : 0;

    await Actor.setValue('statistics', {
        jobs: totalScraped,
        pagesProcessed: stats.pagesProcessed,
        apiPagesProcessed: stats.apiPagesProcessed,
        domFallbackPages: stats.domFallbackPages,
        detailCalls: stats.detailCalls,
        detailFailures: stats.detailFailures,
        durationSeconds,
        jobsPerSecond,
        apiEndpoints: Array.from(stats.apiEndpoints),
        finishedAt: new Date().toISOString(),
    });

    log.info('Run finished', {
        jobs: totalScraped,
        pagesProcessed: stats.pagesProcessed,
        apiPagesProcessed: stats.apiPagesProcessed,
        domFallbackPages: stats.domFallbackPages,
        detailCalls: stats.detailCalls,
        detailFailures: stats.detailFailures,
        durationSeconds,
    });
} catch (error) {
    log.exception(error, 'Actor failed');
    throw error;
}

await Actor.exit();
