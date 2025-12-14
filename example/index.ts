import SqliteCache from "cache-sqlite-lru-ttl";
import { BunSqlDatabase, FetchDownloader, ScraperProcessor } from "../src";

const database = new BunSqlDatabase("sqlite://./jobs.db");

// Initialize SQLite cache for downloaded content
const sqliteCacheInstance = new SqliteCache({
	database: "./jobs.db",
	defaultTtlMs: 1000 * 60 * 60 * 24 * 7, // 7 days TTL
	maxItems: 100000, // LRU eviction after 10k items
	compress: false,
});

// Create downloader with SQLite cache
const downloader = new FetchDownloader({
	maxConcurrentDownloads: 2,
	cache: sqliteCacheInstance,
});

// Hacker News API base URL
const HN_API_BASE = "https://hacker-news.firebaseio.com/v0";

// Type definitions for Hacker News API responses
interface HNItem {
	id: number;
	deleted?: boolean;
	type: "story" | "comment" | "job" | "poll" | "pollopt";
	by?: string;
	time?: number;
	text?: string;
	dead?: boolean;
	parent?: number;
	poll?: number;
	kids?: number[];
	url?: string;
	score?: number;
	title?: string;
	parts?: number[];
	descendants?: number;
}

// Processor for fetching top stories list
const listProcessor = new ScraperProcessor({
	name: "listings",
	database,
	version: 4, // increasing version triggers re-processing of all jobs
	downloader,
	processJson: async (storyIds: number[], url: string) => {
		if (!Array.isArray(storyIds)) {
			throw new Error("Invalid story IDs");
		}

		console.log("processing listings", url, `Found ${storyIds.length} stories`);

		// Convert story IDs to API URLs for individual items
		const itemUrls = storyIds.map((id) => `${HN_API_BASE}/item/${id}.json`);

		threadProcessor.add(...itemUrls);
	},
}).add(`${HN_API_BASE}/topstories.json`);

// Processor for fetching individual story details
const threadProcessor = new ScraperProcessor({
	name: "threads",
	database,
	downloader,
	version: 2,
	processJson: async (item: HNItem, url: string) => {
		console.log("processing thread", url);

		// Skip deleted items or non-story items
		if (!item || item.deleted || item.type !== "story") {
			throw new Error("Invalid item");
		}

		const title = item.title;
		const articleUrl =
			item.url || `https://news.ycombinator.com/item?id=${item.id}`;
		const points = item.score || 0;
		const comments = item.descendants || 0;
		const author = item.by;
		// Convert Unix timestamp to ISO date string
		const date = item.time ? new Date(item.time * 1000).toISOString() : null;

		if (!title || !author || !date) {
			throw new Error("Missing required data");
		}

		// save data you care about into a database
		console.log({ title, articleUrl, points, comments, author, date });
	},
});

await Promise.all([
	listProcessor.run({
		rethrow: true, // to ease up debugging by throwing on errors instead of just logging
	}),
	threadProcessor.run({
		rethrow: true,
	}),
]);
