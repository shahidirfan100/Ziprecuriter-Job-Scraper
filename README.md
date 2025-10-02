# ZipRecruiter Jobs Scraper

A fast and robust scraper for extracting job listings from ZipRecruiter. It can perform a keyword-based search or start from any ZipRecruiter jobs URL. The actor is designed to be resilient, bypass blocking, and retrieve detailed information for each job, including full descriptions, salary data, and application details.

## Key Features

*   **Comprehensive Data Extraction**: Scrapes all key job details, including:
    *   Full Job Description (in both HTML and plain text)
    *   Salary Range (min, max, period)
    *   Company Name and Location
    *   Date Posted and Employment Type
    *   Direct Apply Flag (when available)
    *   Structured JSON-LD data for technical users.

*   **Flexible Starting Options**:
    *   Use the `keyword` and `location` inputs to perform a new search.
    *   Provide a specific `startUrl` to scrape from a pre-defined search results page, category, or company page.

*   **Robust Pagination & Deduplication**:
    *   Intelligently navigates through all pages until the desired number of jobs (`results_wanted`) is found.
    *   Handles multiple URL structures used by ZipRecruiter for maximum compatibility.
    *   Ensures no duplicate jobs are collected, even if they appear on multiple pages.

*   **Two Scraping Modes**:
    *   **Full Detail Mode** (`collect_details: true`): Clicks into each job to scrape the full description and all available data. (Default)
    *   **Fast List Mode** (`collect_details: false`): Scrapes only the information available on the search results pages for a much faster run.

*   **Anti-Blocking & Stealth**:
    *   Uses a pool of browser-like user agents.
    *   Manages cookies and sessions to appear as a legitimate user.
    *   Built-in support for Apify's residential proxies to avoid IP-based blocks.
    *   Configurable request intervals to be polite to the server.

## Input Configuration

The actor can be configured using the following input fields:

| Field | Type | Description |
|---|---|---|
| `startUrl` | `string` | A specific ZipRecruiter URL to start from. Overrides `keyword` and `location`. |
| `keyword` | `string` | The job title, skill, or keyword to search for (e.g., "Software Engineer"). |
| `location` | `string` | The city, state, or country to search in (e.g., "New York, NY", "Remote"). |
| `results_wanted` | `integer` | The total number of unique job listings you want to scrape. |
| `collect_details` | `boolean` | If `true`, the actor will visit each job's detail page for more data. |
| `preferCandidateSearch` | `boolean` | If `true`, uses an alternative URL structure (`/candidate/search`) which can be more stable. |
| `maxConcurrency` | `integer` | The number of parallel requests. Keep this low (1-3) to avoid blocks. |
| `downloadIntervalMs` | `integer` | Milliseconds to wait between requests. Increase this if you encounter blocks. |
| `proxyConfiguration` | `object` | Proxy settings. Residential proxies are highly recommended. |

### Example Input

```json
{
  "keyword": "Data Analyst",
  "location": "Remote",
  "results_wanted": 150,
  "collect_details": true,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```