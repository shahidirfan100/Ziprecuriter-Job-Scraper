# ZipRecruiter Job Scraper

Extract comprehensive job listings from ZipRecruiter.com - the leading global job search platform. This powerful scraper collects detailed employment data including job titles, companies, locations, salaries, and descriptions with intelligent pagination and error handling.

## Features

- **JSON-first, then HTML**: Captures in-page JSON/API data during a stealth Playwright handshake, falling back to HTML parsing only when needed
- **Stealth + anti-blocking**: Hardened navigator spoofing, resource blocking, per-session proxies, and jittered concurrency to avoid Cloudflare/captcha walls
- **Fast detail enrichment**: Concurrency-controlled detail fetches with retries/backoff and optional Playwright fallback for blocked pages
- **Structured parsing**: JSON-LD/Next.js state parsing plus resilient DOM selectors for titles, companies, salaries, locations, and descriptions
- **Rich telemetry**: Saves `STATS` with JSON/HTML hit counts, block counters, and detail success/failure numbers for quick debugging
- **Configurable**: Tune pages, results, detail depth, retries, and proxy settings for cost/speed balance

## Use Cases

### For Job Seekers
Find relevant job opportunities across industries and locations. Stay updated with new positions matching your skills and career goals.

### For Recruiters and Agencies
Monitor job market trends, track competitor hiring patterns, and identify talent pools in specific sectors.

### For Market Researchers
Analyze salary trends, job posting frequencies, and demand for various roles across different regions.

### For Businesses
Track industry hiring patterns and benchmark recruitment strategies against market standards.

## Input Configuration

Configure your job search with these flexible input options:

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `keyword` | string | Job title, role, or skill keywords | `""` |
| `location` | string | City, region, or "Remote" | `""` |
| `startUrl` | string | Direct ZipRecruiter search URL (overrides keyword/location) | `""` |
| `results_wanted` | number | Maximum jobs to collect | `20` |
| `max_pages` | number | Max listing pages to walk (hard limit 50) | `5` |
| `detail_mode` | enum | `none` (skip), `basic` (first few per page), `full` (all) | `full` |
| `max_detail_concurrency` | number | Parallel detail fetches (1-8) | `3` |
| `listing_fetch_retries` | number | Retries with backoff for listing fetch | `2` |
| `detail_fetch_retries` | number | Retries with backoff for detail fetch | `2` |
| `detail_playwright_fallback` | boolean | Use stealth Playwright when detail HTML is blocked | `false` |
| `proxyConfiguration` | object | Apify Proxy settings (RESIDENTIAL recommended) | `{ useApifyProxy: true, groups: ["RESIDENTIAL"], countryCode: "GB" }` |

## Output Data Structure

Each job listing is saved as a structured JSON object:

```json
{
  "title": "Senior Software Engineer",
  "company": "Tech Innovations Inc.",
  "location": "San Francisco, CA",
  "salary": "$120,000 - $150,000 per year",
  "job_type": "Full-time",
  "date_posted": "2024-01-15",
  "description_html": "<p>Join our innovative team...</p>",
  "description_text": "Join our innovative team...",
  "url": "https://www.ziprecruiter.com/job/senior-software-engineer/job12345",
  "keyword_search": "software engineer",
  "location_search": "san francisco",
  "extracted_at": "2024-01-15T10:30:00.000Z"
}
```

### Field Descriptions

- **`title`**: Job position title
- **`company`**: Hiring organization name
- **`location`**: Job location (city, region, or remote)
- **`salary`**: Salary information when available
- **`job_type`**: Employment type (Full-time, Part-time, Contract, etc.)
- **`date_posted`**: Job posting date (YYYY-MM-DD format)
- **`description_html`**: Full job description with HTML formatting
- **`description_text`**: Plain text job description
- **`url`**: Direct link to the job posting on ZipRecruiter
- **`keyword_search`**: The keyword used for the search
- **`location_search`**: The location used for the search
- **`extracted_at`**: Timestamp when the data was extracted

## Usage Examples

### Example 1: Search for Software Engineer Jobs in San Francisco

```json
{
  "keyword": "Software Engineer",
  "location": "San Francisco",
  "results_wanted": 50,
  "detail_mode": "full"
}
```

*Collects up to 50 software engineer positions in San Francisco with full descriptions.*

### Example 2: Data Analyst Positions in New York

```json
{
  "keyword": "Data Analyst",
  "location": "New York",
  "results_wanted": 25,
  "max_pages": 5,
  "detail_mode": "basic"
}
```

*Finds data analyst jobs in New York, limiting to 25 results and 5 pages.*

### Example 3: Custom Search URL

```json
{
  "startUrl": "https://www.ziprecruiter.com/jobs-search?search=project+manager&location=remote",
  "detail_mode": "none",
  "results_wanted": 100
}
```

*Uses a specific ZipRecruiter search URL for remote project manager positions.*

## Configuration Options

### Proxy Settings
For optimal performance and reliability, configure Apify Proxy with residential IPs:

```json
{
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

### Result Limits
Control the scope of your scraping:

- `results_wanted`: Maximum number of job listings to collect
- `max_pages`: Safety limit on search result pages to process
- `detail_mode`: Choose `none` to skip details, `basic` for the first few per page, or `full` for every job (more thorough but slower)

## Limits and Considerations

- **Rate Limiting**: Respects ZipRecruiter.com's servers with adaptive delays
- **Data Freshness**: Job listings are extracted in real-time from the current search results
- **Geographic Coverage**: Supports global job searches with location filtering
- **Data Accuracy**: Extracts data as displayed on ZipRecruiter.com at the time of scraping

## Data Source

**ZipRecruiter.com** - One of the largest job search platforms globally, connecting millions of job seekers with employers across all industries and locations.

## Keywords

job scraper, ZipRecruiter, job listings, employment data, job search, career opportunities, recruitment data, salary information, job market analysis, hiring trends
