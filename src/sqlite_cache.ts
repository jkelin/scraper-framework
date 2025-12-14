import type { ICache } from "./cache";

/**
 * Minimal interface for cache-sqlite-lru-ttl instance
 * This matches the methods we use from the cache-sqlite-lru-ttl library
 */
interface ISqliteCache {
	get(key: string): Promise<unknown>;
	set(
		key: string,
		value: unknown,
		options?: { ttlMs?: number; compress?: boolean },
	): Promise<void>;
	close(): Promise<void>;
}

/**
 * SQLite cache implementation using cache-sqlite-lru-ttl
 * Provides persistent caching with LRU and TTL eviction
 */
export class SqliteLruTtlCache implements ICache {
	private cache: ISqliteCache;

	constructor(cache: ISqliteCache) {
		this.cache = cache;
	}

	public async get(url: string): Promise<Buffer | null | undefined> {
		const value = await this.cache.get(url);
		if (value === undefined || value === null) {
			return null;
		}
		// cache-sqlite-lru-ttl stores values as Buffer when they are Buffers
		// or converts them back from CBOR
		if (Buffer.isBuffer(value)) {
			return value;
		}
		// If it's not a Buffer, try to convert it
		// This shouldn't happen if we always store Buffers, but handle it just in case
		if (typeof value === "string") {
			return Buffer.from(value, "utf-8");
		}
		// For other types, serialize to JSON and convert to Buffer
		return Buffer.from(JSON.stringify(value), "utf-8");
	}

	public async set(url: string, data: Buffer): Promise<void> {
		// cache-sqlite-lru-ttl can store Buffers directly
		await this.cache.set(url, data);
	}

	/**
	 * Close the cache database connection
	 * Should be called during graceful shutdown
	 */
	public async close(): Promise<void> {
		await this.cache.close();
	}
}
