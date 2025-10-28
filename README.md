# ZipRecruiter Jobs Scraper

## Description

This Apify actor provides a fast and robust solution for extracting job listings from ZipRecruiter. It supports keyword-based searches or starting from any ZipRecruiter jobs URL, ensuring resilience against blocking while retrieving detailed job information, including descriptions, salaries, and application details.

## Key Features

- **Comprehensive Data Extraction**: Captures all essential job details, such as:
  - Full job description (HTML and plain text formats).
  - Salary range (minimum, maximum, and period).
  - Company name, location, date posted, and employment type.
  - Direct apply flag and structured JSON-LD data.

- **Flexible Starting Options**:
  - Perform searches using keywords and locations.
  - Start from a specific ZipRecruiter URL for targeted scraping.

- **Robust Pagination and Deduplication**:
  - Navigates all pages to reach the desired number of results.
  - Handles various URL structures for compatibility.
  - Prevents duplicate entries.

- **Scraping Modes**:
  - **Full Detail Mode**: Visits each job page for complete data (default).
  - **Fast List Mode**: Extracts data from search results only for speed.

- **Anti-Blocking Measures**:
  - Rotates user agents and manages sessions.
  - Supports residential proxies.
  - Configurable request intervals for politeness.

## Input

Configure the actor with the following fields:

| Field | Type | Description |
|---|---|---|
| `startUrl` | `string` | Optional. A specific ZipRecruiter URL to begin scraping from. Overrides `keyword` and `location`. |
| `keyword` | `string` | Optional. Job title, skill, or keyword for search (e.g., "Software Engineer"). |
| `location` | `string` | Optional. City, state, or region (e.g., "New York, NY" or "Remote"). |
| `results_wanted` | `integer` | Required. Total unique jobs to scrape. |
| `collect_details` | `boolean` | Optional. Set to `true` for full details (default); `false` for fast mode. |
| `preferCandidateSearch` | `boolean` | Optional. Use alternative URL structure for stability. |
| `maxConcurrency` | `integer` | Optional. Number of parallel requests (1-3 recommended). |
| `downloadIntervalMs` | `integer` | Optional. Delay between requests in milliseconds. |
| `proxyConfiguration` | `object` | Optional. Proxy settings (residential proxies advised). |

## Output

The actor outputs job data in JSON format, with each item containing fields like `title`, `company`, `location`, `description`, `salary`, and more. Results are deduplicated and paginated.

## Usage

1. **Basic Search**: Provide `keyword` and `location` to start a new search.
2. **Custom URL**: Use `startUrl` for specific pages.
3. **Mode Selection**: Enable `collect_details` for in-depth data or disable for quick lists.
4. **Run the Actor**: Input your configuration and execute on Apify.

### Configuration Tips

- For large result sets, increase `downloadIntervalMs` to avoid rate limits.
- Use residential proxies in `proxyConfiguration` to bypass blocks.
- Set `maxConcurrency` low for stability.

## Examples

### Example 1: Keyword Search for Remote Data Analyst Jobs

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

### Example 2: Fast Mode from a Specific URL

```json
{
  "startUrl": "https://www.ziprecruiter.com/jobs-search?search=engineer&location=San+Francisco%2C+CA",
  "results_wanted": 50,
  "collect_details": false,
  "maxConcurrency": 2,
  "downloadIntervalMs": 1000
}
```

## Limitations

- Results depend on ZipRecruiter's availability and changes.
- High concurrency may trigger blocks; adjust settings accordingly.

## Support

For issues or questions, refer to Apify's documentation or community forums.