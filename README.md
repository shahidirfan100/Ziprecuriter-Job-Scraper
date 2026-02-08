# ZipRecruiter Jobs Scraper

Extract and collect ZipRecruiter job listings into structured datasets for analysis, research, and automation. Gather job titles, companies, locations, salary information, full descriptions, and apply links at scale. This actor is built for fast, reliable job data collection workflows.

## Features

- **Comprehensive Job Data** — Collect detailed listing records including job content, compensation fields, and metadata.
- **Flexible Search Inputs** — Start from a direct search URL or use keywords and location.
- **Automatic Pagination** — Continue across result pages until you hit your limits.
- **Freshness Filtering** — Restrict results to recent postings using a day-range filter.
- **Structured Exports** — Use JSON, CSV, Excel, and other dataset export formats.
- **Automation Ready** — Run manually, on schedule, or through API integrations.

## Use Cases

### Recruitment Pipeline Enrichment
Collect targeted job datasets by role and location to enrich recruiting workflows. Track openings from specific regions or categories and sync data downstream.

### Job Market Intelligence
Analyze hiring demand by title, location, compensation range, and posting recency. Use the data for reporting, trend analysis, and competitive monitoring.

### Lead Generation for B2B Services
Find companies actively hiring in your service niche and create prospect lists. Use job activity as an intent signal for outreach campaigns.

### Career Research and Content
Gather current job trends for salary guides, skills reports, and job-seeker resources. Build data-backed insights from recent posting activity.

### Internal BI and Dashboards
Feed datasets into warehouse or BI tools for recurring reporting. Monitor role distribution, market shifts, and hiring spikes over time.

---

## Input Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `searchUrl` | String | No | — | Direct ZipRecruiter search URL. When provided, it overrides search query and location fields. |
| `searchQuery` | String | No | `"software engineer"` | Keywords or job title to search for. Required when `searchUrl` is not provided. |
| `location` | String | No | `"New York, NY"` | Target location for search results. |
| `maxJobs` | Integer | No | `20` | Maximum number of jobs to collect. Use `0` for unlimited until other limits are reached. |
| `maxPages` | Integer | No | `50` | Maximum number of result pages to process. |
| `daysBack` | String | No | `"any"` | Posting recency filter. Allowed values: `any`, `1`, `3`, `7`, `14`, `30`. |
| `proxyConfiguration` | Object | No | Residential proxy preset | Proxy configuration for reliable collection. |

---

## Output Data

Each dataset item contains job listing data like:

| Field | Type | Description |
|-------|------|-------------|
| `title` | String | Job title. |
| `company` | String | Company display name. |
| `companyCanonicalName` | String | Canonical company name when available. |
| `companyId` | String | Company identifier when available. |
| `companyUrl` | String | Company page URL. |
| `companyLogoUrl` | String | Company logo URL. |
| `location` | String | Display location text. |
| `locationCity` | String | City value. |
| `locationState` | String | State value. |
| `locationStateCode` | String | State code value. |
| `locationCountry` | String | Country value. |
| `locationCountryCode` | String | Country code value. |
| `isRemote` | Boolean | Indicates remote classification. |
| `locationTypes` | Array | Location type labels. |
| `employmentTypes` | Array | Employment type labels. |
| `jobType` | String | Combined employment type string. |
| `salary` | String | Human-readable salary summary. |
| `salaryMin` | Number | Minimum salary when available. |
| `salaryMax` | Number | Maximum salary when available. |
| `salaryMinAnnual` | Number | Annualized minimum salary when available. |
| `salaryMaxAnnual` | Number | Annualized maximum salary when available. |
| `salaryInterval` | String | Salary interval value. |
| `salaryCurrency` | String | Salary currency code. |
| `postedDate` | String | Posting date value. |
| `postedAtUtc` | String | UTC posting timestamp. |
| `rollingPostedAtUtc` | String | Rolling UTC posting timestamp. |
| `isActive` | Boolean | Listing active flag when available. |
| `url` | String | Job URL. |
| `externalApplyUrl` | String | External apply URL when available. |
| `applyButtonType` | String | Apply button type value. |
| `applyDestination` | String | Apply destination value. |
| `applyStatus` | String/Null | Apply status value when available. |
| `listingKey` | String | Listing key identifier. |
| `matchId` | String | Match identifier. |
| `jobId` | String | Job identifier. |
| `openSeatId` | String | Open seat identifier when available. |
| `description` | String | Full job description text. |
| `shortDescription` | String | Short listing summary text. |
| `htmlDescription` | String | Rich description content when available. |
| `searchQuery` | String | Input keyword used in the run. |
| `searchLocation` | String | Input location used in the run. |
| `page` | Number | Results page number. |
| `scrapedAt` | String | Extraction timestamp. |
| `rawCard` | Object | Raw listing payload. |
| `rawDetails` | Object | Raw detailed payload. |

---

## Usage Examples

### Basic Keyword Search

Collect software engineering jobs in New York:

```json
{
    "searchQuery": "software engineer",
    "location": "New York, NY",
    "maxJobs": 50
}
```

### Direct Search URL

Use a ZipRecruiter search URL directly:

```json
{
    "searchUrl": "https://www.ziprecruiter.com/jobs-search?search=data+analyst&location=Remote",
    "maxJobs": 100,
    "maxPages": 20
}
```

### Recent Jobs with Proxy Configuration

Focus on fresh jobs and use proxy settings:

```json
{
    "searchQuery": "product manager",
    "location": "California",
    "daysBack": "7",
    "maxJobs": 200,
    "maxPages": 50,
    "proxyConfiguration": {
        "useApifyProxy": true,
        "apifyProxyGroups": ["RESIDENTIAL"]
    }
}
```

---

## Sample Output

```json
{
    "title": "Software Engineer, Observability",
    "company": "CoreWeave",
    "location": "New York, NY US",
    "jobType": "Full-time",
    "salary": "$109,000 - $145,000 / year",
    "postedAtUtc": "2026-02-05T17:18:00Z",
    "url": "https://www.ziprecruiter.com/c/CoreWeave/Job/Software-Engineer,-Observability/-in-New-York,NY?jid=example",
    "externalApplyUrl": "https://coreweave.com/careers/jobs/example",
    "listingKey": "exampleListingKey",
    "matchId": "exampleMatchId",
    "isRemote": false,
    "description": "We are looking for a software engineer to build and scale observability services...",
    "searchQuery": "software engineer",
    "searchLocation": "New York, NY",
    "page": 1,
    "scrapedAt": "2026-02-08T14:15:28.000Z"
}
```

---

## Tips for Best Results

### Start with Tight Searches
- Use specific keywords to reduce irrelevant results.
- Narrow by location when possible for cleaner datasets.

### Control Volume Intentionally
- Start with `maxJobs` 20-100 for test runs.
- Increase `maxPages` and `maxJobs` for production runs.

### Use Recency Filters
- Set `daysBack` to `1`, `3`, or `7` for fresher monitoring workflows.
- Use `any` when building historical-style snapshots.

### Keep Proxy Enabled
- Use residential proxy settings for stronger reliability.
- Keep proxy configuration consistent for scheduled runs.

### Schedule for Monitoring
- Run daily or hourly schedules to detect new listings quickly.
- Connect runs to notifications or downstream automations.

---

## Integrations

Connect extracted data to:

- **Google Sheets** — Share and analyze job datasets quickly.
- **Airtable** — Build searchable recruiting and market tables.
- **Slack** — Send run notifications to your team.
- **Webhooks** — Trigger custom backend workflows after each run.
- **Make** — Build no-code pipelines and enrichment flows.
- **Zapier** — Connect jobs data to business apps automatically.

### Export Formats

- **JSON** — API and engineering workflows.
- **CSV** — Spreadsheet analysis and reporting.
- **Excel** — Business-ready reports.
- **XML** — System-to-system integrations.

---

## Frequently Asked Questions

### Do I need both `searchUrl` and `searchQuery`?
No. Use either `searchUrl` or `searchQuery` (with optional `location`).

### How many jobs can I collect?
Set `maxJobs` to your target value. If set to `0`, collection continues until page limits or result availability stops it.

### Can I collect only recent jobs?
Yes. Use `daysBack` with values like `1`, `3`, `7`, `14`, or `30`.

### Why are some fields empty?
Some listings do not expose every field. Missing values are returned as empty or null depending on field type.

### Can I automate this actor?
Yes. You can run it on schedules, call it through API, and send results to downstream tools.

### Is output available as CSV and Excel?
Yes. You can export dataset items in CSV, Excel, JSON, and additional formats from Apify.

---

## Support

For issues or feature requests, use the Apify Console issue/support channels for this actor.

### Resources

- [Apify Documentation](https://docs.apify.com/)
- [Apify API Reference](https://docs.apify.com/api/v2)
- [Apify Schedules](https://docs.apify.com/platform/schedules)

---

## Legal Notice

This actor is intended for legitimate data collection and research use. You are responsible for complying with applicable laws, platform terms, and data usage policies in your jurisdiction.
