import { PlaywrightCrawler } from '@crawlee/playwright';
import { Actor, log } from 'apify';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';

await Actor.init();

const CONFIG = {
    CLOUDFLARE_WAIT_MS: 2200,
    CONTENT_WAIT_MS: 180,
    JOBS_PER_PAGE: 20,
    DEFAULT_MAX_PAGES: 50,
    MAX_PAGES_HARD_LIMIT: 200,
    DETAIL_CONCURRENCY: 4,
};
const DEFAULT_SEARCH_QUERY = 'software engineer';

const stats = {
    pagesProcessed: 0,
    apiPagesProcessed: 0,
    jobsExtracted: 0,
    challengeRetries: 0,
    emptyApiPages: 0,
    detailCalls: 0,
    detailFailures: 0,
    totalPageMs: 0,
    slowPages: 0,
    startTime: Date.now(),
    apiEndpoints: new Set(),
};

const seenJobIds = new Set();

const asArray = (value) => (Array.isArray(value) ? value : []);

const NULLABLE_NUMBER_FIELDS = [
    'salaryMin',
    'salaryMax',
    'salaryMinAnnual',
    'salaryMaxAnnual',
];

const safeJsonParse = (value, fallback = null) => {
    try {
        return JSON.parse(value);
    } catch {
        return fallback;
    }
};

const NEXT_FLIGHT_PUSH_REGEX = /self\.__next_f\.push\(\[1,\\"([\s\S]*?)\\"\]\)/g;

function decodeNextFlightPayloadFromHtml(html) {
    if (!html || typeof html !== 'string') return '';

    const chunks = [];
    for (const match of html.matchAll(NEXT_FLIGHT_PUSH_REGEX)) {
        const rawChunk = match[1];
        try {
            chunks.push(JSON.parse(`"${rawChunk}"`));
        } catch {
            // Ignore malformed chunks and continue.
        }
    }

    return chunks.join('\n');
}

function extractJsonObjectByMarker(source, marker) {
    if (!source || typeof source !== 'string') return null;
    const markerIndex = source.indexOf(marker);
    if (markerIndex < 0) return null;

    const start = source.indexOf('{', markerIndex + marker.length - 1);
    if (start < 0) return null;

    let depth = 0;
    let inString = false;
    let escaped = false;

    for (let i = start; i < source.length; i += 1) {
        const ch = source[i];

        if (inString) {
            if (escaped) {
                escaped = false;
            } else if (ch === '\\') {
                escaped = true;
            } else if (ch === '"') {
                inString = false;
            }
            continue;
        }

        if (ch === '"') {
            inString = true;
            continue;
        }

        if (ch === '{') depth += 1;
        if (ch === '}') {
            depth -= 1;
            if (depth === 0) {
                return source.slice(start, i + 1);
            }
        }
    }

    return null;
}

function extractJsonObjectByKey(source, key) {
    return extractJsonObjectByMarker(source, `"${key}":{`);
}

function extractLooseObjectFromMarker(source, markerText) {
    if (!source || typeof source !== 'string') return null;

    const markerIndex = source.indexOf(markerText);
    if (markerIndex < 0) return null;

    const start = source.lastIndexOf('{', markerIndex);
    if (start < 0) return null;

    let depth = 0;
    for (let i = start; i < source.length; i += 1) {
        const ch = source[i];
        if (ch === '{') depth += 1;
        if (ch === '}') {
            depth -= 1;
            if (depth === 0) {
                return source.slice(start, i + 1);
            }
        }
    }

    return null;
}

function extractSearchPayloadFromHtml(html) {
    const findCardsNode = (value, depth = 0) => {
        if (!value || typeof value !== 'object' || depth > 8) return null;

        if (
            Array.isArray(value.jobKeys)
            || (value.jobKeysMap && typeof value.jobKeysMap === 'object')
        ) {
            return value;
        }

        if (Array.isArray(value)) {
            for (const item of value) {
                const foundInItem = findCardsNode(item, depth + 1);
                if (foundInItem) return foundInItem;
            }
            return null;
        }

        for (const nestedValue of Object.values(value)) {
            const found = findCardsNode(nestedValue, depth + 1);
            if (found) return found;
        }

        return null;
    };

    const mergedPayload = decodeNextFlightPayloadFromHtml(html);
    let cardsData = null;

    if (mergedPayload) {
        const cardsText = extractJsonObjectByKey(mergedPayload, 'serializedJobCardsData');
        cardsData = safeJsonParse(cardsText, null);
    }

    if (!cardsData) {
        const escapedObject = extractLooseObjectFromMarker(html, 'serializedJobCardsData');
        if (escapedObject) {
            const decoded = escapedObject
                .replace(/\\\\/g, '\\')
                .replace(/\\"/g, '"');
            cardsData = safeJsonParse(decoded, null);
        }
    }

    if (!cardsData) {
        const plainObject = extractJsonObjectByMarker(html, '"serializedJobCardsData":{');
        cardsData = safeJsonParse(plainObject, null);
    }

    if (!cardsData) return null;

    cardsData = findCardsNode(cardsData);
    if (!cardsData) return null;

    const jobKeys = asArray(cardsData.jobKeys)
        .filter((jobKey) => jobKey?.listingKey && jobKey?.matchId)
        .map((jobKey) => ({
            listingKey: jobKey.listingKey,
            matchId: jobKey.matchId,
            bidTrackingData: jobKey.bidTrackingData || '',
        }));

    const rawMap = cardsData.jobKeysMap && typeof cardsData.jobKeysMap === 'object'
        ? cardsData.jobKeysMap
        : {};

    const jobKeysMap = Object.fromEntries(
        Object.entries(rawMap).filter(([, value]) => value && typeof value === 'object' && !Array.isArray(value)),
    );

    return {
        jobKeys,
        jobKeysMap,
        totalListings: Number(cardsData.totalListings || 0) || null,
        placementId: Number(cardsData.placementId || 0) || null,
        impressionLotId: typeof cardsData.impressionLotId === 'string' ? cardsData.impressionLotId : '',
    };
}

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
    const text = html
        .replace(/<style[\s\S]*?<\/style>/gi, ' ')
        .replace(/<script[\s\S]*?<\/script>/gi, ' ')
        .replace(/<[^>]+>/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();

    return decodeHtmlEntities(text);
}

function decodeHtmlEntities(text) {
    if (!text || typeof text !== 'string') return '';

    return text
        .replace(/&#x([0-9a-f]+);/gi, (_, hex) => {
            const code = Number.parseInt(hex, 16);
            return Number.isFinite(code) ? String.fromCodePoint(code) : _;
        })
        .replace(/&#(\d+);/g, (_, num) => {
            const code = Number.parseInt(num, 10);
            return Number.isFinite(code) ? String.fromCodePoint(code) : _;
        })
        .replace(/&nbsp;/gi, ' ')
        .replace(/&quot;/gi, '"')
        .replace(/&apos;|&#39;/gi, "'")
        .replace(/&lt;/gi, '<')
        .replace(/&gt;/gi, '>')
        .replace(/&amp;/gi, '&')
        .replace(/\s+/g, ' ')
        .trim();
}

const DESCRIPTION_ALLOWED_TAGS = new Set(['p', 'br', 'strong', 'em', 'ul', 'ol', 'li']);

function sanitizeDescriptionHtml(html) {
    if (!html || typeof html !== 'string') return '';

    const normalized = html
        .replace(/<style[\s\S]*?<\/style>/gi, ' ')
        .replace(/<script[\s\S]*?<\/script>/gi, ' ')
        .replace(/<!--[\s\S]*?-->/g, ' ')
        .replace(/<\/?b(\s[^>]*)?>/gi, (tag) => (tag.startsWith('</') ? '</strong>' : '<strong>'));

    const sanitized = normalized.replace(/<\/?([a-z0-9]+)(\s[^>]*)?>/gi, (fullTag, tagName) => {
        const tag = String(tagName || '').toLowerCase();
        const isClosing = fullTag.startsWith('</');

        if (!DESCRIPTION_ALLOWED_TAGS.has(tag)) return ' ';
        if (tag === 'br') return '<br>';
        return isClosing ? `</${tag}>` : `<${tag}>`;
    });

    return sanitized
        .replace(/\s{2,}/g, ' ')
        .replace(/>\s+</g, '><')
        .trim();
}

function toReadableEnumName(value) {
    if (!value || typeof value !== 'string') return '';

    return value
        .replace(/^(LOCATION_TYPE_NAME_|EMPLOYMENT_TYPE_NAME_)/i, '')
        .toLowerCase()
        .split('_')
        .filter(Boolean)
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ')
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

    function normalizeTypedLabels(items) {
        return normalizeTypedNames(items)
        .map((name) => toReadableEnumName(name))
        .filter(Boolean);
    }

function normalizeInputString(value) {
    if (typeof value !== 'string') return '';
    return value.trim();
}

function recordPageTiming(pageNum, pageStartedAt) {
    const pageDurationMs = Date.now() - pageStartedAt;
    stats.totalPageMs += pageDurationMs;

    if (pageDurationMs > 3500) {
        stats.slowPages += 1;
        log.warning(`Slow page detected: ${pageDurationMs}ms on page ${pageNum}`);
    }
}

function sanitizeRecordForDataset(record) {
    if (!record || typeof record !== 'object') return null;

    const sanitized = { ...record };

    for (const field of NULLABLE_NUMBER_FIELDS) {
        if (sanitized[field] === null || typeof sanitized[field] !== 'number') {
            delete sanitized[field];
        }
    }

    if (typeof sanitized.isActive !== 'boolean') {
        delete sanitized.isActive;
    }

    for (const [key, value] of Object.entries(sanitized)) {
        if (Array.isArray(value)) {
            const primitiveItems = value
                .filter((item) => ['string', 'number', 'boolean'].includes(typeof item))
                .map((item) => String(item).trim())
                .filter(Boolean);

            if (!primitiveItems.length) {
                delete sanitized[key];
            } else {
                sanitized[key] = primitiveItems.join(', ');
            }
            continue;
        }

        if (value && typeof value === 'object') {
            delete sanitized[key];
            continue;
        }

        if (value === null || value === undefined || value === '') {
            delete sanitized[key];
        }
    }

    if (sanitized.jobId && sanitized.listingKey && sanitized.jobId === sanitized.listingKey) {
        delete sanitized.jobId;
    }

    if (sanitized.postedDate && sanitized.postedAtUtc && sanitized.postedDate === sanitized.postedAtUtc) {
        delete sanitized.postedDate;
    }

    if (sanitized.locationType && sanitized.locationTypes && sanitized.locationType === sanitized.locationTypes) {
        delete sanitized.locationTypes;
    }

    if (sanitized.jobType && sanitized.employmentTypes && sanitized.jobType === sanitized.employmentTypes) {
        delete sanitized.employmentTypes;
    }

    return sanitized;
}

async function pushRecordsSafely(records) {
    if (!records.length) return 0;

    const sanitizedRecords = records
        .map((record) => sanitizeRecordForDataset(record))
        .filter(Boolean);

    if (!sanitizedRecords.length) return 0;

    try {
        await Actor.pushData(sanitizedRecords);
        return sanitizedRecords.length;
    } catch (error) {
        log.warning(`Bulk push failed (${error.message}). Retrying item-by-item.`);
    }

    let pushed = 0;
    for (const record of sanitizedRecords) {
        try {
            await Actor.pushData(record);
            pushed += 1;
        } catch (error) {
            log.warning(`Skipping invalid record during push: ${error.message}`);
        }
    }

    return pushed;
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
        return { detailsByListing: new Map(), failed: validJobKeys.length };
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
                        collected.push({
                            listingKey: key.listingKey,
                            ok: false,
                            status: response.status,
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
                } catch {
                    collected.push({
                        listingKey: key.listingKey,
                        ok: false,
                        status: 0,
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
    const location = detail?.location || card?.location || {};
    const pay = detail?.pay || card?.pay || {};
    const applyButton = detail?.applyButtonConfig || card?.applyButtonConfig || {};

    const employmentTypes = normalizeTypedLabels(card?.employmentTypes);
    const locationTypes = normalizeTypedLabels(card?.locationTypes);

    const salaryInfo = formatSalary(pay);

    const canonicalJobUrl = normalizeUrl(detail?.rawCanonicalZipJobPageUrl || card?.rawCanonicalZipJobPageUrl || '');
    const redirectJobUrl = normalizeUrl(card?.jobRedirectPageUrl || '');
    const jobUrl = canonicalJobUrl || redirectJobUrl;

    const externalApplyUrl = normalizeUrl(applyButton.externalApplyUrl || '');
    const companyUrl = normalizeUrl(detail?.companyUrl || card?.companyUrl || '');
    const locationUrl = normalizeUrl(detail?.locationUrl || card?.locationUrl || '');

    const sourceDescriptionHtml = detail?.htmlFullDescription || card?.htmlFullDescription || card?.shortDescription || '';
    const descriptionHtml = sanitizeDescriptionHtml(sourceDescriptionHtml) || sanitizeDescriptionHtml(card?.shortDescription || '');
    const descriptionText = stripHtml(descriptionHtml) || stripHtml(card?.shortDescription || '');

    const locationName = location.displayName || card?.location?.displayName || '';
    const remoteByType = locationTypes.some((type) => /remote/i.test(type));
    const remoteByLocation = /remote/i.test(locationName);

    return {
        title: detail?.title || card?.title || 'Unknown Title',
        company: company.canonicalDisplayName || company.name || '',
        companyCanonicalName: company.canonicalDisplayName || '',
        companyId: company.id || '',
        companyUrl: companyUrl || undefined,
        companyLogoUrl: detail?.companyLogoUrl || card?.companyLogo?.logoUrl || '',

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

        listingKey: card?.listingKey || '',
        matchId: card?.matchId || '',
        jobId: card?.listingKey || '',
        openSeatId: card?.openSeatId || '',

        description_text: descriptionText,
        description_html: descriptionHtml,

        searchQuery: context.searchQuery || '',
        searchLocation: context.searchLocation || '',
        page: context.page,

        scrapedAt: new Date().toISOString(),
    };
}

try {
    const input = (await Actor.getInput()) || {};

    const rawSearchUrl = normalizeInputString(input.searchUrl);
    const rawSearchQuery = normalizeInputString(input.searchQuery);
    const rawLocation = normalizeInputString(input.location);

    const hasExplicitFilters = Boolean(rawSearchQuery || rawLocation);
    const hasSearchUrl = Boolean(rawSearchUrl);
    const useSearchUrl = hasSearchUrl && !hasExplicitFilters;
    const useDefaultQuery = !useSearchUrl && !rawSearchQuery && !rawLocation;

    const effectiveSearch = {
        searchUrl: useSearchUrl ? rawSearchUrl : '',
        searchQuery: rawSearchQuery || (useDefaultQuery ? DEFAULT_SEARCH_QUERY : ''),
        location: rawLocation,
        daysBack: input.daysBack,
    };

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

    log.info('Starting ZipRecruiter actor in API-only search payload mode', {
        searchQuery: effectiveSearch.searchQuery || null,
        location: effectiveSearch.location || null,
        usingSearchUrl: Boolean(effectiveSearch.searchUrl),
        maxJobs: Number.isFinite(targetJobs) ? targetJobs : 0,
        maxPages,
    });

    let totalScraped = 0;
    let consecutiveEmpty = 0;

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: 1,
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

        async requestHandler({ page, response }) {
            if (Number.isFinite(targetJobs) && totalScraped >= targetJobs) return;

            await page.waitForLoadState('domcontentloaded', { timeout: 20000 }).catch(() => {});
            await page.waitForTimeout(CONFIG.CONTENT_WAIT_MS);

            let discoveredMaxPages = maxPages;

            for (let pageNum = 1; pageNum <= discoveredMaxPages; pageNum += 1) {
                const pageStartedAt = Date.now();
                const remainingSlots = Number.isFinite(targetJobs)
                    ? Math.max(targetJobs - totalScraped, 0)
                    : Number.POSITIVE_INFINITY;

                if (remainingSlots === 0) {
                    log.info(`Target reached on page ${pageNum - 1}`);
                    return;
                }

                let records = [];
                let usedChallengeRetry = false;

                try {
                    const currentPageUrl = buildSearchUrl(effectiveSearch, pageNum);
                    let pageHtml = '';

                    if (pageNum > 1) {
                        const navigationResponse = await page.goto(currentPageUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
                        pageHtml = await navigationResponse?.text().catch(() => '');
                        await page.waitForTimeout(CONFIG.CONTENT_WAIT_MS);
                    } else {
                        pageHtml = await response?.text().catch(() => '');
                    }

                    if (!pageHtml) {
                        pageHtml = await page.content();
                    }

                    let pagePayload = extractSearchPayloadFromHtml(pageHtml);
                    let challengeDetected = /just a moment|cloudflare/i.test(pageHtml);

                    if ((!pagePayload || !pagePayload.jobKeys.length) && challengeDetected) {
                        usedChallengeRetry = true;
                        stats.challengeRetries += 1;
                        log.warning(`Challenge page detected on page ${pageNum}. Retrying after wait.`);

                        await page.waitForTimeout(CONFIG.CLOUDFLARE_WAIT_MS + 1200);
                        const retryResponse = await page.goto(currentPageUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
                        await page.waitForTimeout(CONFIG.CONTENT_WAIT_MS + 200);

                        let retryHtml = await retryResponse?.text().catch(() => '');
                        if (!retryHtml) {
                            retryHtml = await page.content();
                        }

                        pagePayload = extractSearchPayloadFromHtml(retryHtml);
                        challengeDetected = /just a moment|cloudflare/i.test(retryHtml);
                    }

                    if (pagePayload?.totalListings) {
                        const pagesFromTotal = Math.ceil(pagePayload.totalListings / CONFIG.JOBS_PER_PAGE);
                        if (Number.isFinite(pagesFromTotal) && pagesFromTotal > 0) {
                            discoveredMaxPages = Math.min(discoveredMaxPages, pagesFromTotal);
                        }
                    }

                    if (pagePayload?.jobKeys.length && Object.keys(pagePayload.jobKeysMap || {}).length) {
                        stats.apiPagesProcessed += 1;
                        stats.apiEndpoints.add('/jobs-search:serializedJobCardsData');

                        const pageKeys = pagePayload.jobKeys
                            .filter((jobKey) => pagePayload.jobKeysMap[jobKey.listingKey])
                            .slice(0, Number.isFinite(remainingSlots) ? remainingSlots : undefined);

                        const pageCards = pageKeys
                            .map((jobKey) => pagePayload.jobKeysMap[jobKey.listingKey])
                            .filter((jobCard) => jobCard?.listingKey);

                        const jobKeysForDetails = pageKeys
                            .map((jobKey) => ({
                                listingKey: jobKey.listingKey,
                                matchId: jobKey.matchId,
                                bidTrackingData: jobKey.bidTrackingData || '',
                            }))
                            .filter((jobKey) => jobKey.listingKey && jobKey.matchId);

                        stats.detailCalls += jobKeysForDetails.length;
                        stats.apiEndpoints.add('/job_services.job_card.api_public.public.api.v1.API/GetJobDetails');

                        const detailResult = await fetchListingDetailsBatchViaApi(page, {
                            jobKeys: jobKeysForDetails,
                            placementId: pagePayload.placementId,
                            impressionLotId: pagePayload.impressionLotId,
                        });
                        const { detailsByListing, failed } = detailResult;
                        stats.detailFailures += failed;

                        records = pageCards.map((card) => {
                            const detail = detailsByListing.get(card.listingKey) || null;
                            return normalizeJobRecord(card, detail, {
                                searchQuery: effectiveSearch.searchQuery || '',
                                searchLocation: effectiveSearch.location || '',
                                page: pageNum,
                            });
                        });
                    }

                    if (!records.length) {
                        stats.emptyApiPages += 1;
                        log.warning(`No API jobs extracted on page ${pageNum}`, {
                            challenge: challengeDetected,
                            usedChallengeRetry,
                        });
                    }
                } catch (pageError) {
                    stats.pagesProcessed += 1;
                    consecutiveEmpty += 1;
                    recordPageTiming(pageNum, pageStartedAt);
                    log.warning(`Page ${pageNum} processing failed`, {
                        error: pageError.message,
                        consecutiveEmpty,
                    });

                    if (consecutiveEmpty >= 2) {
                        log.info(`Stopping after ${consecutiveEmpty} empty pages`);
                        return;
                    }

                    continue;
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

                    const pushedCount = await pushRecordsSafely(limitedRecords);
                    totalScraped += pushedCount;
                    stats.jobsExtracted += pushedCount;
                    let extractionMode = 'next-flight-only';
                    if (usedChallengeRetry) {
                        extractionMode = 'next-flight+challenge-retry';
                    }

                    log.info(`Page ${pageNum}: extracted ${pushedCount} jobs`, {
                        total: totalScraped,
                        mode: extractionMode,
                    });
                }

                recordPageTiming(pageNum, pageStartedAt);

                if (Number.isFinite(targetJobs) && totalScraped >= targetJobs) {
                    log.info(`Target reached on page ${pageNum}`);
                    return;
                }

                if (consecutiveEmpty >= 2) {
                    log.info(`Stopping after ${consecutiveEmpty} empty pages`);
                    return;
                }

                await page.waitForTimeout(40 + Math.floor(Math.random() * 80));
            }
        },

        failedRequestHandler({ request, error }) {
            log.error(`Request failed for page ${request.userData.pageNum || 1}: ${error.message}`);
        },
    });

    await crawler.run([{
        url: buildSearchUrl(effectiveSearch, 1),
    }]);

    const durationSeconds = Math.round((Date.now() - stats.startTime) / 1000);
    const jobsPerSecond = durationSeconds > 0 ? Number((stats.jobsExtracted / durationSeconds).toFixed(3)) : 0;
    const avgPageMs = stats.pagesProcessed > 0
        ? Math.round(stats.totalPageMs / stats.pagesProcessed)
        : 0;

    await Actor.setValue('statistics', {
        jobs: totalScraped,
        pagesProcessed: stats.pagesProcessed,
        apiPagesProcessed: stats.apiPagesProcessed,
        challengeRetries: stats.challengeRetries,
        emptyApiPages: stats.emptyApiPages,
        detailCalls: stats.detailCalls,
        detailFailures: stats.detailFailures,
        slowPages: stats.slowPages,
        avgPageMs,
        durationSeconds,
        jobsPerSecond,
        apiEndpoints: Array.from(stats.apiEndpoints),
        finishedAt: new Date().toISOString(),
    });

    log.info('Run finished', {
        jobs: totalScraped,
        pagesProcessed: stats.pagesProcessed,
        apiPagesProcessed: stats.apiPagesProcessed,
        challengeRetries: stats.challengeRetries,
        emptyApiPages: stats.emptyApiPages,
        detailCalls: stats.detailCalls,
        detailFailures: stats.detailFailures,
        slowPages: stats.slowPages,
        avgPageMs,
        durationSeconds,
    });
} catch (error) {
    log.exception(error, 'Actor failed');
    throw error;
}

await Actor.exit();
