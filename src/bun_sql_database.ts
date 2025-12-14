import type { SQL } from "bun";
import { Database, type ProcessorJob } from "./database";
import type { ScraperProcessor } from "./processor";

async function initialize(sql: SQL, jobsTable: string) {
	await sql`
		CREATE TABLE IF NOT EXISTS ${sql(jobsTable)} (
			url TEXT,
			processor TEXT,
			version INTEGER,
			date TIMESTAMP,
			error TEXT,
			attempt INTEGER,
			PRIMARY KEY (url, processor)
		)
	`;
}

async function load(connectionString: string, jobsTable: string): Promise<SQL> {
	const { SQL } = await import("bun");
	const sql = new SQL(connectionString);
	await initialize(sql, jobsTable);
	return sql;
}

export class BunSqlDatabase extends Database {
	private readonly db: Promise<SQL>;

	constructor(
		connectionString: string,
		private readonly jobsTable: string = "jobs",
	) {
		super();
		this.db = load(connectionString, this.jobsTable);
	}

	public async query(processor: ScraperProcessor): Promise<ProcessorJob[]> {
		const sql = await this.db;

		const queryResult = await sql`
				SELECT url, version, attempt, date, processor, error FROM ${sql(this.jobsTable)}
				WHERE processor = ${processor.name}
				AND (
					attempt = 0
					OR (error IS NOT NULL AND attempt < ${processor.maxAttempts})
					OR version <> ${processor.version}
					OR date < ${new Date(Date.now() - processor.intervalMs).toISOString()}
				)
				ORDER BY date ASC
				LIMIT 20
			`;

		return queryResult.map((row: ProcessorJob & { date: Date | string }) => {
			return {
				url: row.url,
				version: row.version,
				attempt: row.attempt,
				date:
					(row.date as unknown) instanceof Date
						? (row.date as Date).toISOString()
						: String(row.date),
				processor: row.processor,
				error: row.error ?? undefined,
			};
		});
	}

	protected async batchInsertUrls(
		urls: string[],
		processor: ScraperProcessor,
	): Promise<void> {
		const data = urls.map((url) => ({
			url,
			processor: processor.name,
			version: processor.version,
			date: new Date().toISOString(),
			attempt: 0,
			error: null,
		}));

		const sql = await this.db;
		await sql`INSERT OR IGNORE INTO ${sql(this.jobsTable)} ${sql(data)}`;
	}

	override async close() {
		await super.close();
		const sql = await this.db;
		await sql.close();
	}

	public async update(job: ProcessorJob): Promise<void> {
		const sql = await this.db;
		await sql`
			UPDATE ${sql(this.jobsTable)}
			SET ${sql(job)}
			WHERE url = ${job.url} AND processor = ${job.processor}
		`;
	}
}
