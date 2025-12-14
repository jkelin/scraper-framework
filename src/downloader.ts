import { type ICache, memoryCache } from "./cache";
import { Semaphore } from "./helpers";
import { scraperLogger } from "./logger";

const logger = scraperLogger.child({
	downloader: "Downloader",
});

export abstract class Downloader {
	protected cache: ICache = memoryCache;

	abstract download(url: string): Promise<Buffer>;
}

export class FetchDownloader extends Downloader {
	private readonly semaphore: Semaphore;
	private readonly headers: Record<string, string> | undefined;

	constructor({
		maxConcurrentDownloads = 10,
		headers,
		cache = memoryCache,
	}: {
		maxConcurrentDownloads?: number;
		headers?: Record<string, string>;
		cache?: ICache;
	} = {}) {
		super();
		this.semaphore = new Semaphore(maxConcurrentDownloads);
		this.headers = headers;
		this.cache = cache;
	}

	public async download(url: string): Promise<Buffer> {
		const cached = await this.cache.get(url);
		if (cached) {
			return cached;
		}

		return await this.semaphore.with(async () => {
			logger.info({ url }, "Downloading URL");

			const fetchOptions: RequestInit = {};
			if (this.headers) {
				fetchOptions.headers = this.headers;
			}

			const response = await fetch(url, fetchOptions);

			if (!response.ok) {
				throw new Error(
					`HTTP error! status: ${response.status} ${response.statusText} for URL: ${url}`,
				);
			}

			const arrayBuffer = await response.arrayBuffer();
			const buffer = Buffer.from(arrayBuffer);

			await this.cache.set(url, buffer);

			return buffer;
		});
	}
}

export const fetchDownloader: Downloader = new FetchDownloader();
