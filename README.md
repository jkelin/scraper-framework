# ez-scraper-framework

My little library for writing scrapers and managing their data.

![NPM Version](https://img.shields.io/npm/v/ez-scraper-framework)

## AI Description

The framework provides a structured approach to building web scrapers with job management, retry logic, and concurrent processing.

### Core Components

- **ScraperProcessor**: Processes URLs with configurable workers and retry logic
- **Database**: Tracks job state (version, attempts, timestamps) - supports SQLite via `BunSqlDatabase` or in-memory via `InMemoryDatabase`
- **Downloader**: Handles URL downloads with concurrency control and caching - default `FetchDownloader` uses fetch API

### Key Features

- **Version-based re-processing**: Incrementing the processor version triggers re-processing of all jobs
- **Automatic retries**: Failed jobs are retried up to `maxAttempts` times
- **Concurrent processing**: Configurable worker count for parallel job processing
- **Job persistence**: Jobs are tracked in a database with state management
- **Flexible data processing**: Supports raw buffers, JSON, or string processing

## Creating a Scraper Processor

1. **Set up dependencies**:

```typescript
import {
  BunSqlDatabase,
  FetchDownloader,
  ScraperProcessor,
} from "scraper-framework";

const downloader = new FetchDownloader({ maxConcurrentDownloads: 2 });
const database = new BunSqlDatabase("sqlite://./jobs.db");
```

2. **Create a processor** with your processing logic:

```typescript
const processor = new ScraperProcessor({
  name: "my-scraper",
  database,
  downloader,
  version: 1, // Increment to re-process all jobs
  processJson: async (data, url) => {
    // Your processing logic here
    console.log("Processing:", url, data);
  },
});
```

3. **Add URLs and run**:

```typescript
processor.add("https://example.com/api/data");
await processor.run({ rethrow: true });
```

### Processor Options

- `name`: Unique identifier for the processor
- `version`: Version number (incrementing triggers re-processing)
- `intervalMs`: Minimum time between re-processing the same URL (default: 100 years)
- `maxAttempts`: Maximum retry attempts for failed jobs (default: 3)
- `workers`: Number of concurrent workers (default: 10)
- `processJson`: Function to process JSON responses
- `processString`: Function to process text responses
- `processRaw`: Function to process raw Buffer data

### Chaining Processors

Processors can add URLs to other processors, enabling multi-stage scraping pipelines:

```typescript
const listProcessor = new ScraperProcessor({
  name: "listings",
  processJson: async (ids, url) => {
    const itemUrls = ids.map((id) => `${API_BASE}/item/${id}.json`);
    itemProcessor.add(...itemUrls);
  },
});

const itemProcessor = new ScraperProcessor({
  name: "items",
  processJson: async (item, url) => {
    // Process individual items
  },
});
```
