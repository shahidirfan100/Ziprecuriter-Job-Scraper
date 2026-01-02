# ZipRecruiter Jobs Scraper

Extract comprehensive job listings from ZipRecruiter with advanced search capabilities and intelligent data extraction. Get structured, high-quality job data including titles, companies, locations, salaries, descriptions, and application links.

## Overview

The ZipRecruiter Jobs Scraper is a powerful tool designed to extract job listings from ZipRecruiter.com with multiple extraction strategies for maximum reliability. This scraper automatically adapts to extract data using the most efficient method available.

### Key Features

<ul>
<li><strong>Multiple Extraction Strategies</strong> - Automatically uses the best available method</li>
<li><strong>Advanced Search Filters</strong> - Filter by location, keywords, salary, job type, and posting date</li>
<li><strong>Complete Job Data</strong> - Titles, companies, locations, salaries, descriptions, and benefits</li>
<li><strong>Intelligent Pagination</strong> - Automatically follows search result pages</li>
<li><strong>Duplicate Detection</strong> - Prevents duplicate job entries</li>
<li><strong>Full Description Enrichment</strong> - Fetches complete job descriptions from detail pages</li>
<li><strong>Export Flexibility</strong> - Download data in JSON, CSV, Excel, XML, RSS, HTML formats</li>
<li><strong>Residential Proxy Support</strong> - Built-in support for reliable scraping</li>
</ul>

### Perfect For

<ul>
<li><strong>Recruitment Agencies</strong> - Automate candidate sourcing and market intelligence</li>
<li><strong>Job Aggregation Platforms</strong> - Build comprehensive job databases</li>
<li><strong>HR Analytics</strong> - Track hiring trends and compensation patterns</li>
<li><strong>Market Research</strong> - Analyze employment markets and industry demands</li>
<li><strong>Career Services</strong> - Provide fresh opportunities to job seekers</li>
<li><strong>Competitive Intelligence</strong> - Monitor competitor hiring activities</li>
</ul>

## Quick Start

### Running on Apify Platform

<ol>
<li>Open the Actor in the Apify Console</li>
<li>Configure your search parameters (see Input Parameters below)</li>
<li>Click <strong>Start</strong> and wait for the results</li>
<li>Download your data in your preferred format</li>
</ol>

### Basic Input Example

```json
{
  "searchQuery": "software engineer",
  "location": "New York, NY",
  "maxJobs": 100,
  "radius": "25",
  "daysBack": "7"
}
```

### Using Search URL

```json
{
  "searchUrl": "https://www.ziprecruiter.com/jobs-search?location=New+York%2C+NY&search=software+engineer",
  "maxJobs": 50
}
```

## Input Parameters

Configure the scraper with these parameters to customize your search:

<table>
<thead>
<tr>
<th>Parameter</th>
<th>Type</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>

<tr>
<td><code>searchUrl</code></td>
<td>String</td>
<td>No</td>
<td>Direct ZipRecruiter search URL. If provided, other search parameters are ignored</td>
</tr>

<tr>
<td><code>searchQuery</code></td>
<td>String</td>
<td>No*</td>
<td>Job title or keywords (e.g., "software engineer", "data analyst")</td>
</tr>

<tr>
<td><code>location</code></td>
<td>String</td>
<td>No*</td>
<td>Location to search (e.g., "New York, NY", "San Francisco, CA", "Remote")</td>
</tr>

<tr>
<td><code>maxJobs</code></td>
<td>Integer</td>
<td>No</td>
<td>Maximum number of jobs to scrape (default: 50, 0 = unlimited)</td>
</tr>

<tr>
<td><code>radius</code></td>
<td>String</td>
<td>No</td>
<td>Search radius in miles: 5, 10, 15, 25, 50, 100 (default: 25)</td>
</tr>

<tr>
<td><code>daysBack</code></td>
<td>String</td>
<td>No</td>
<td>Posted within: any, 1, 3, 7, 14, 30 days (default: any)</td>
</tr>

<tr>
<td><code>employmentType</code></td>
<td>Array</td>
<td>No</td>
<td>Filter by type: full_time, part_time, contract, temporary, internship</td>
</tr>

<tr>
<td><code>salaryMin</code></td>
<td>Integer</td>
<td>No</td>
<td>Minimum annual salary filter</td>
</tr>

<tr>
<td><code>remoteOnly</code></td>
<td>Boolean</td>
<td>No</td>
<td>Show only remote positions (default: false)</td>
</tr>

<tr>
<td><code>proxyConfiguration</code></td>
<td>Object</td>
<td>No</td>
<td>Proxy settings (residential proxies recommended)</td>
</tr>

</tbody>
</table>

<p><em>* Either <code>searchUrl</code> OR both <code>searchQuery</code> and <code>location</code> must be provided</em></p>

## Output Data

Each job listing contains comprehensive structured data:

```json
{
  "title": "Senior Software Engineer",
  "company": "Tech Solutions Inc.",
  "location": "New York, NY",
  "salary": "$120,000 - $160,000 per year",
  "jobType": "Full-time",
  "postedDate": "2 days ago",
  "descriptionHtml": "<p>We are seeking an experienced software engineer...</p>",
  "descriptionText": "We are seeking an experienced software engineer...",
  "url": "https://www.ziprecruiter.com/c/...",
  "applyUrl": "https://www.ziprecruiter.com/c/.../apply",
  "companyUrl": "https://www.ziprecruiter.com/co/...",
  "benefits": "Health insurance, 401(k), Remote work",
  "scrapedAt": "2026-01-02T10:30:00.000Z"
}
```

### Data Fields

<table>
<thead>
<tr>
<th>Field</th>
<th>Type</th>
<th>Description</th>
</tr>
</thead>
<tbody>

<tr>
<td><code>title</code></td>
<td>String</td>
<td>Job position title</td>
</tr>

<tr>
<td><code>company</code></td>
<td>String</td>
<td>Hiring company name</td>
</tr>

<tr>
<td><code>location</code></td>
<td>String</td>
<td>Job location (city, state)</td>
</tr>

<tr>
<td><code>salary</code></td>
<td>String</td>
<td>Salary range or "Not specified"</td>
</tr>

<tr>
<td><code>jobType</code></td>
<td>String</td>
<td>Employment type (Full-time, Part-time, Contract, etc.)</td>
</tr>

<tr>
<td><code>postedDate</code></td>
<td>String</td>
<td>When the job was posted</td>
</tr>

<tr>
<td><code>descriptionHtml</code></td>
<td>String</td>
<td>Full job description with HTML formatting</td>
</tr>

<tr>
<td><code>descriptionText</code></td>
<td>String</td>
<td>Plain text version of job description</td>
</tr>

<tr>
<td><code>url</code></td>
<td>String</td>
<td>Direct link to job posting</td>
</tr>

<tr>
<td><code>applyUrl</code></td>
<td>String</td>
<td>Job application URL</td>
</tr>

<tr>
<td><code>companyUrl</code></td>
<td>String</td>
<td>Company profile URL</td>
</tr>

<tr>
<td><code>benefits</code></td>
<td>String</td>
<td>Job benefits and perks</td>
</tr>

<tr>
<td><code>scrapedAt</code></td>
<td>String</td>
<td>ISO timestamp of extraction</td>
</tr>

</tbody>
</table>

## Export Formats

Download your scraped data in multiple formats for different use cases:

<ul>
<li><strong>JSON</strong> - Structured data for API integration and applications</li>
<li><strong>CSV</strong> - Compatible with spreadsheet software</li>
<li><strong>Excel (XLSX)</strong> - Advanced data analysis and reporting</li>
<li><strong>XML</strong> - Enterprise system integration</li>
<li><strong>RSS</strong> - Feed subscriptions and updates</li>
<li><strong>HTML</strong> - Web display and preview</li>
</ul>

## Usage Examples

### Example 1: Tech Jobs in San Francisco

```json
{
  "searchQuery": "software developer",
  "location": "San Francisco, CA",
  "maxJobs": 100,
  "radius": "25",
  "daysBack": "7",
  "salaryMin": 100000,
  "employmentType": ["full_time"]
}
```

### Example 2: Remote Data Analyst Positions

```json
{
  "searchQuery": "data analyst",
  "location": "United States",
  "maxJobs": 50,
  "remoteOnly": true,
  "daysBack": "14",
  "salaryMin": 70000
}
```

### Example 3: Entry-Level Marketing Jobs

```json
{
  "searchQuery": "marketing coordinator",
  "location": "New York, NY",
  "maxJobs": 75,
  "radius": "50",
  "employmentType": ["full_time", "part_time"],
  "daysBack": "30"
}
```

### Example 4: Contract Positions

```json
{
  "searchQuery": "project manager",
  "location": "Chicago, IL",
  "maxJobs": 50,
  "employmentType": ["contract", "temporary"],
  "radius": "25"
}
```

## How It Works

### Intelligent Extraction Strategy

The scraper uses a three-tier extraction strategy, automatically selecting the most efficient method:

<ol>
<li><strong>JSON API Extraction (Priority 1)</strong>
<ul>
<li>Intercepts network requests to capture structured JSON responses</li>
<li>Extracts data from embedded JSON in page source</li>
<li>Fastest and most reliable method</li>
<li>Returns complete structured data</li>
</ul>
</li>

<li><strong>JSON-LD Structured Data (Priority 2)</strong>
<ul>
<li>Parses schema.org JobPosting markup</li>
<li>Standard format with consistent structure</li>
<li>High data quality and completeness</li>
</ul>
</li>

<li><strong>HTML Parsing (Priority 3)</strong>
<ul>
<li>Fallback method using CSS selectors</li>
<li>Intelligent selector fallback chains</li>
<li>Ensures data extraction even when other methods fail</li>
</ul>
</li>
</ol>

### Process Flow

<ol>
<li><strong>Search Configuration</strong> - Builds search URL from your parameters</li>
<li><strong>Page Navigation</strong> - Loads search results page</li>
<li><strong>Data Extraction</strong> - Applies extraction strategy (JSON API → JSON-LD → HTML)</li>
<li><strong>Description Enrichment</strong> - Fetches full descriptions from detail pages</li>
<li><strong>Deduplication</strong> - Removes duplicate job listings</li>
<li><strong>Pagination</strong> - Automatically follows next page links</li>
<li><strong>Data Storage</strong> - Saves structured data to dataset</li>
</ol>

### Performance Characteristics

<ul>
<li><strong>Small Runs</strong> (&lt; 50 jobs): 1-2 minutes</li>
<li><strong>Medium Runs</strong> (50-200 jobs): 3-6 minutes</li>
<li><strong>Large Runs</strong> (200-500 jobs): 8-15 minutes</li>
<li><strong>Very Large Runs</strong> (500+ jobs): 15-30 minutes</li>
</ul>

## Proxy Configuration

For reliable scraping, residential proxies are recommended:

```json
{
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

### Proxy Options

<ul>
<li><strong>Residential Proxies</strong> - Best for ZipRecruiter (recommended)</li>
<li><strong>Datacenter Proxies</strong> - Faster but may be detected</li>
<li><strong>Custom Proxies</strong> - Use your own proxy service</li>
</ul>

## Best Practices

### Optimize Your Searches

<ul>
<li>Use specific keywords for more relevant results</li>
<li>Set appropriate location and radius for your needs</li>
<li>Use <code>daysBack</code> to focus on recent postings</li>
<li>Set <code>maxJobs</code> to reasonable limits for faster runs</li>
<li>Filter by <code>employmentType</code> to narrow results</li>
</ul>

### Reliable Data Collection

<ul>
<li>Use residential proxies for best results</li>
<li>Start with smaller <code>maxJobs</code> values to test</li>
<li>Review the statistics output for extraction method used</li>
<li>Check the debug output if no jobs are found</li>
</ul>

### Data Quality

<ul>
<li>Full descriptions are automatically fetched from detail pages</li>
<li>Duplicates are automatically removed</li>
<li>All data is validated before storage</li>
<li>Empty fields are marked as "Not specified"</li>
</ul>

## Troubleshooting

### No Jobs Found

<ul>
<li>Verify your search parameters are correct</li>
<li>Try broadening your search (increase radius, remove filters)</li>
<li>Check if the search URL returns results in a browser</li>
<li>Review the DEBUG_PAGE_HTML in key-value store</li>
</ul>

### Incomplete Data

<ul>
<li>Some fields may not be available for all jobs</li>
<li>Enable full description enrichment (enabled by default)</li>
<li>Check the extraction method in statistics output</li>
</ul>

### Rate Limiting

<ul>
<li>Use residential proxies for better reliability</li>
<li>Reduce <code>maxConcurrency</code> if experiencing issues</li>
<li>Set reasonable <code>maxJobs</code> limits</li>
</ul>

## Output Analysis

### Statistics

After each run, check the statistics in the key-value store:

```json
{
  "totalJobsScraped": 150,
  "pagesProcessed": 6,
  "extractionMethod": "JSON API",
  "duration": "180 seconds",
  "timestamp": "2026-01-02T10:30:00.000Z"
}
```

### Extraction Methods

<ul>
<li><strong>JSON API</strong> - Best performance and data quality</li>
<li><strong>JSON-LD</strong> - Good structured data quality</li>
<li><strong>HTML Parsing</strong> - Fallback method, may have incomplete data</li>
</ul>

## Integration Examples

### Use with Apify API

```javascript
const ApifyClient = require('apify-client');

const client = new ApifyClient({
    token: 'YOUR_API_TOKEN',
});

const input = {
    searchQuery: 'data scientist',
    location: 'Boston, MA',
    maxJobs: 100,
};

const run = await client.actor('YOUR_ACTOR_ID').call(input);
const { items } = await client.dataset(run.defaultDatasetId).listItems();

console.log(items);
```

### Webhook Integration

Set up webhooks to receive notifications when scraping completes:

<ol>
<li>Configure webhook in Actor settings</li>
<li>Receive POST request with run details</li>
<li>Download dataset via provided URLs</li>
<li>Process data in your application</li>
</ol>

### Scheduled Runs

Automate job data collection with scheduled runs:

<ol>
<li>Create a schedule in Apify Console</li>
<li>Set frequency (hourly, daily, weekly)</li>
<li>Configure input parameters</li>
<li>Receive fresh job data automatically</li>
</ol>

## Advanced Features

### Custom Search URLs

Provide your own ZipRecruiter search URL for complete control:

```json
{
  "searchUrl": "https://www.ziprecruiter.com/jobs-search?location=Remote&search=react+developer&radius=100",
  "maxJobs": 100
}
```

### Multiple Employment Types

Filter by multiple employment types simultaneously:

```json
{
  "employmentType": ["full_time", "contract", "part_time"]
}
```

### Salary Filtering

Set minimum salary requirements:

```json
{
  "salaryMin": 80000
}
```

### Remote-Only Positions

Focus on remote work opportunities:

```json
{
  "remoteOnly": true,
  "location": "United States"
}
```

## Data Privacy and Compliance

<ul>
<li>This scraper only collects publicly available job listing information</li>
<li>No personal data or user accounts are accessed</li>
<li>Complies with ZipRecruiter's public data access</li>
<li>Use responsibly and respect rate limits</li>
<li>Review ZipRecruiter's Terms of Service for your use case</li>
</ul>

## Technical Details

### Technology Stack

<ul>
<li>Built on Apify SDK for Actor infrastructure</li>
<li>Uses intelligent extraction strategies</li>
<li>Employs residential proxy rotation</li>
<li>Implements comprehensive error handling</li>
</ul>

### Concurrency Settings

<ul>
<li>Optimized for balance between speed and reliability</li>
<li>Automatic retry on failures</li>
<li>Intelligent timeout management</li>
</ul>

### Data Validation

<ul>
<li>All URLs are normalized and validated</li>
<li>HTML content is sanitized</li>
<li>Duplicate detection by URL</li>
<li>Empty fields handled gracefully</li>
</ul>

## Support and Feedback

### Get Help

<ul>
<li>Check the troubleshooting section above</li>
<li>Review the statistics output for insights</li>
<li>Examine DEBUG_PAGE_HTML in key-value store</li>
<li>Contact support through Apify Console</li>
</ul>

### Report Issues

If you encounter problems:

<ol>
<li>Note your input parameters</li>
<li>Check the Actor run logs</li>
<li>Review the statistics output</li>
<li>Provide run ID when requesting support</li>
</ol>

## License

Apache License 2.0

---

<p align="center">
Made with ❤️ for the Apify platform
</p>
