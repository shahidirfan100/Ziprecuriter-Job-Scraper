# ZipRecruiter Jobs Scraper (HTTP + Cheerio)

A lightweight and fast ZipRecruiter jobs scraper designed for the Apify platform.

- **HTTP-based**: Uses `CheerioCrawler` for efficient, low-overhead scraping without a full browser.
- **Crawlee Powered**: Built on the latest version of Crawlee.
- **Lightweight**: Optimized for cloud execution on Apify's Alpine Linux image.
- **Uses `gotScraping`**: Avoids potential compatibility issues with Crawlee's request mechanisms.

This actor scrapes job listings from ZipRecruiter by searching for a given keyword and location. It extracts key job details, including the full description in both HTML and plain text formats.