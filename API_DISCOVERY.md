## Selected API

- Endpoint (cards payload source): `GET https://www.ziprecruiter.com/jobs-search?...` (parse `serializedJobCardsData` from embedded Next payload)
- Endpoint (details): `POST /job_services.job_card.api_public.public.api.v1.API/GetJobDetails`
- Method: GET + POST
- Auth: No explicit auth token required; relies on browser session/cookies from the loaded search page
- Pagination: URL `page` parameter on search URL (`...&page=1..N`)
- Fields available (cards): `listingKey`, `matchId`, `bidTrackingData`, `title`, `status`, `pay`, `employmentTypes`, `locationTypes`, `company`, `shortDescription`, `location`, `companyUrl`, `applyButtonConfig`, `rawCanonicalZipJobPageUrl`, `openSeatId`
- Fields available (details): `title`, `status`, `pay`, `company`, `htmlFullDescription`, `location`, `locationUrl`, `companyUrl`, `applyButtonConfig`, `rawCanonicalZipJobPageUrl`, `companyLogoUrl`, `companyWidget`

### Fields currently missing in previous actor path

- Full multi-page card payload was not extracted from current Next payload format
- Detail enrichment was not consistently applied because page parsing failed before details hydration
- Many records were falling back to DOM-only extraction, causing weak descriptions and inconsistent metadata

### Field Count Comparison

- Previous effective extraction on blocked runs: mostly DOM fallback (~10-12 useful fields)
- Selected API-based path: 30+ normalized output fields with detail enrichment fallback

### API Scoring

- Returns JSON directly: +30 (embedded serialized JSON + JSON details endpoint)
- Has >15 unique fields: +25
- No auth required: +20
- Has pagination support: +15
- Matches/extends existing fields: +10
- Total: **100 / 100**
