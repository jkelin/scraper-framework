import { EventEmitter } from "node:events";
import { debounce } from "./helpers";
import { scraperLogger } from "./logger";
import type { ScraperProcessor } from "./processor";

export interface ProcessorJob {
	url: string;
	version: number;
	attempt: number;
	date: string;
	processor: string;
	error?: string;
}

const logger = scraperLogger.child({
	database: "Database",
});

export enum DatabaseEvents {
	NEW_JOBS = "newJobs",
}

export abstract class Database extends EventEmitter<{
	[DatabaseEvents.NEW_JOBS]: [processor: ScraperProcessor];
}> {
	private pendingUrls: Map<ScraperProcessor, string[]> = new Map();

	abstract query(
		processor: ScraperProcessor,
		currentlyProcessingUrls: Set<string>,
	): Promise<ProcessorJob[]>;

	protected abstract batchInsertUrls(
		urls: string[],
		processor: ScraperProcessor,
	): Promise<void>;

	private insertManyUrlsWrapperDebounced = debounce(
		() => {
			for (const [processor, urls] of this.pendingUrls.entries()) {
				if (urls.length === 0) continue;
				logger.info(
					{ processor: processor.name, count: urls.length },
					"Inserting URLs",
				);

				this.batchInsertUrls(urls, processor).then(() => {
					this.emit(DatabaseEvents.NEW_JOBS, processor);
				});
			}

			this.pendingUrls.clear();
		},
		100,
		200,
	);

	public add(urls: string | string[], processor: ScraperProcessor) {
		logger.debug({ processor: processor.name, urls }, "Adding URLs");

		let pendingUrls = this.pendingUrls.get(processor);
		if (!pendingUrls) {
			pendingUrls = [];
			this.pendingUrls.set(processor, pendingUrls);
		}

		if (Array.isArray(urls)) {
			pendingUrls.push(...urls);
		} else {
			pendingUrls.push(urls);
		}

		this.insertManyUrlsWrapperDebounced();
	}

	public async close() {
		logger.info("Closing database");
		for (const [processor, urls] of this.pendingUrls.entries()) {
			if (urls.length === 0) continue;
			await this.batchInsertUrls(urls, processor);
		}

		this.pendingUrls.clear();
	}

	public abstract update(job: ProcessorJob): Promise<void>;
}

export class InMemoryDatabase extends Database {
	private jobs: ProcessorJob[] = [];

	public async query(
		processor: ScraperProcessor,
		currentlyProcessingUrls: Set<string>,
	): Promise<ProcessorJob[]> {
		return this.jobs
			.filter((job) => job.processor === processor.name)
			.filter(
				(job) =>
					job.attempt === 0 ||
					job.version !== processor.version ||
					(job.error && job.attempt < processor.maxAttempts) ||
					new Date(job.date) < new Date(Date.now() - processor.intervalMs),
			)
			.filter((job) => !currentlyProcessingUrls.has(job.url));
	}

	protected async batchInsertUrls(
		urls: string[],
		processor: ScraperProcessor,
	): Promise<void> {
		const newUrls = urls.filter(
			(url) =>
				!this.jobs.some(
					(job) => job.url === url && job.processor === processor.name,
				),
		);

		this.jobs.push(
			...newUrls.map((url) => ({
				url,
				version: processor.version,
				attempt: 0,
				date: new Date().toISOString(),
				processor: processor.name,
			})),
		);
	}

	public async update(job: ProcessorJob): Promise<void> {
		const existing = this.jobs.find(
			(j) => j.url === job.url && j.processor === job.processor,
		);
		if (existing) {
			existing.version = job.version;
			existing.attempt = job.attempt;
			existing.date = job.date;
			existing.error = job.error;
		} else {
			throw new Error(
				`Job for URL ${job.url} and processor ${job.processor} not found`,
			);
		}
	}
}

export const inMemoryDatabase: Database = new InMemoryDatabase();
