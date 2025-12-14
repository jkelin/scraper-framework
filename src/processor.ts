import { EventEmitter } from "node:events";
import type { Logger } from "pino";
import {
	type Database,
	DatabaseEvents,
	inMemoryDatabase,
	type ProcessorJob,
} from "./database";
import { type Downloader, fetchDownloader } from "./downloader";
import { Mutex } from "./helpers";
import { scraperLogger } from "./logger";

export enum ScraperProcessorEvents {
	JOB_PROCESSED = "jobProcessed",
}

export class ScraperProcessor extends EventEmitter<{
	[ScraperProcessorEvents.JOB_PROCESSED]: [job: ProcessorJob];
}> {
	public readonly name: string;

	public readonly version: number = 0;

	public readonly intervalMs: number = 100 * 365 * 24 * 60 * 60 * 1000; // 100 years

	public readonly maxAttempts: number = 3;

	protected readonly downloader: Downloader = fetchDownloader;

	protected readonly database: Database = inMemoryDatabase;

	protected readonly logger: Logger;

	private readonly pendingJobs: ProcessorJob[] = [];

	// HashSet of URLs currently being processed to prevent duplicate processing
	private readonly currentlyProcessingUrls: Set<string> = new Set();

	protected readonly processRaw?: (data: Buffer, url: string) => Promise<void>;

	protected readonly workers: number = 10;

	protected readonly processJson?: (json: any, url: string) => Promise<void>;

	protected readonly processString?: (
		text: string,
		url: string,
	) => Promise<void>;

	constructor({
		name,
		version = 0,
		intervalMs = 100 * 365 * 24 * 60 * 60 * 1000, // 100 years
		maxAttempts = 3,
		downloader = fetchDownloader,
		database = inMemoryDatabase,
		processRaw,
		processJson,
		processString,
		workers = 10,
	}: {
		name: string;
		version?: number;
		intervalMs?: number;
		maxAttempts?: number;
		downloader?: Downloader;
		database?: Database;
		processRaw?: (data: Buffer, url: string) => Promise<void>;
		processJson?: (json: any, url: string) => Promise<void>;
		processString?: (text: string, url: string) => Promise<void>;
		workers?: number;
	}) {
		super();

		this.name = name;
		this.version = version;
		this.intervalMs = intervalMs;
		this.maxAttempts = maxAttempts;
		this.downloader = downloader;
		this.database = database;
		this.processRaw = processRaw;
		this.processJson = processJson;
		this.processString = processString;
		this.workers = workers;

		this.logger = scraperLogger.child({
			processor: this.name,
		});
	}

	private readonly getNextPendingJobMutex = new Mutex();
	private async getNextPendingJob(): Promise<ProcessorJob> {
		return await this.getNextPendingJobMutex.with(async () => {
			while (this.pendingJobs.length === 0) {
				const jobs = await this.database.query(
					this as any,
					this.currentlyProcessingUrls,
				);

				if (jobs.length > 0) {
					this.pendingJobs.push(...jobs);
				} else {
					this.logger.info("No pending jobs, waiting for new jobs");

					await new Promise<void>((resolve) => {
						const timeout = setTimeout(() => {
							this.database.off(DatabaseEvents.NEW_JOBS, finish);
							resolve();
						}, 1000);

						const finish = (processor: ScraperProcessor) => {
							if (processor !== this) return;

							clearTimeout(timeout);
							this.database.off(DatabaseEvents.NEW_JOBS, finish);
							resolve();
						};

						this.database.on(DatabaseEvents.NEW_JOBS, finish);
					});
				}
			}

			const jobToProcess = this.pendingJobs.shift();
			if (!jobToProcess) {
				throw new Error("No pending jobs, this should never happen");
			}

			return jobToProcess;
		});
	}

	public add(...urls: string[]) {
		this.database.add(urls, this as any);

		return this;
	}

	public async run({ rethrow = false }: { rethrow?: boolean } = {}) {
		const workers = new Array(this.workers).fill(0).map(async () => {
			while (true) {
				const job = await this.getNextPendingJob();
				await this.processJob(job, rethrow);
			}
		});

		await Promise.all(workers);
	}

	private async processJob(job: ProcessorJob, rethrow: boolean) {
		// Add URL to currently processing set to prevent duplicate processing
		this.currentlyProcessingUrls.add(job.url);

		try {
			let data: Buffer;
			try {
				data = await this.downloader.download(job.url);
			} catch (error) {
				this.logger.error({ error }, `Failed to download URL ${job.url}`);

				if (rethrow) {
					throw error;
				}

				return;
			}

			try {
				await this.processData(job.url, data);

				await this.database.update({
					url: job.url,
					version: this.version,
					attempt: 1,
					date: new Date().toISOString(),
					processor: this.name,
				});
			} catch (error) {
				this.logger.error({ error }, `Failed to process URL ${job.url}`);

				if (rethrow) {
					throw error;
				}

				await this.database.update({
					url: job.url,
					version: this.version,
					attempt: job.attempt + 1,
					date: new Date().toISOString(),
					processor: this.name,
				});
			}
		} finally {
			// Remove URL from currently processing set when done
			this.currentlyProcessingUrls.delete(job.url);
			this.emit(ScraperProcessorEvents.JOB_PROCESSED, job);
		}
	}

	private async processData(url: string, data: Buffer) {
		this.logger.debug({ url }, "Processing data");

		if (this.processRaw) {
			await this.processRaw(data, url);
		}

		if (this.processString) {
			await this.processString(data.toString(), url);
		}

		if (this.processJson) {
			const json = JSON.parse(data.toString());
			await this.processJson(json, url);
		}
	}
}
