/**
 * Adapter that wraps @tursodatabase/serverless Connection to match
 * the DatabasePromise interface used by AgentFS internals.
 *
 * The core challenge: DatabasePromise.prepare() is synchronous and returns
 * a Statement immediately, but serverless Connection.prepare() is async
 * (it needs to fetch column metadata over HTTP).
 *
 * Solution: return a LazyStatement that defers the actual prepare() call
 * until run()/get()/all() is called — those are already async, so the
 * deferral is invisible to callers.
 */

import type { Connection, Statement as ServerlessStatement } from "@tursodatabase/serverless";
import type { DatabasePromise } from "@tursodatabase/database-common";

/**
 * A statement that defers the async prepare() call until execution.
 *
 * DatabasePromise.prepare() must return synchronously, but the serverless
 * driver's prepare() is async. LazyStatement bridges this by storing the
 * SQL and only calling conn.prepare() when run/get/all is invoked.
 */
class LazyStatement {
  private conn: Connection;
  private sql: string;
  private _raw = false;
  private _pluck = false;
  private _safeIntegers = false;

  constructor(conn: Connection, sql: string) {
    this.conn = conn;
    this.sql = sql;
  }

  private async getStmt(): Promise<ServerlessStatement> {
    const stmt = await this.conn.prepare(this.sql);
    if (this._raw) stmt.raw(true);
    if (this._pluck) stmt.pluck(true);
    if (this._safeIntegers) stmt.safeIntegers(true);
    return stmt;
  }

  raw(raw?: boolean): this {
    this._raw = raw !== false;
    return this;
  }

  pluck(pluck?: boolean): this {
    this._pluck = pluck !== false;
    return this;
  }

  safeIntegers(toggle?: boolean): this {
    this._safeIntegers = toggle !== false;
    return this;
  }

  columns(): any[] {
    throw new Error("columns() requires an async prepare — not supported synchronously in serverless mode");
  }

  get source(): void {
    return undefined;
  }

  get reader(): void {
    return undefined;
  }

  get database(): any {
    return undefined;
  }

  async run(...args: any[]): Promise<{ changes: number; lastInsertRowid: number }> {
    const stmt = await this.getStmt();
    return stmt.run(args);
  }

  async get(...args: any[]): Promise<any> {
    const stmt = await this.getStmt();
    return stmt.get(args);
  }

  async all(...args: any[]): Promise<any[]> {
    const stmt = await this.getStmt();
    return stmt.all(args);
  }

  async *iterate(...args: any[]): AsyncGenerator<any> {
    const stmt = await this.getStmt();
    yield* stmt.iterate(args);
  }

  bind(..._args: any[]): this {
    throw new Error("bind() is not supported in serverless mode — pass parameters to run/get/all instead");
  }

  interrupt(): void {}

  close(): void {}
}

function notSupported(name: string): () => never {
  return () => {
    throw new Error(`${name}() is not supported in serverless mode`);
  };
}

/**
 * Wraps a @tursodatabase/serverless Connection to match
 * the DatabasePromise interface expected by AgentFS.openWith().
 *
 * @example
 * ```typescript
 * import { connect } from "@tursodatabase/serverless";
 * import { AgentFS } from "agentfs-sdk";
 * import { createServerlessAdapter } from "agentfs-sdk/serverless";
 *
 * const conn = connect({
 *   url: "http://localhost:8080",
 * });
 *
 * const db = createServerlessAdapter(conn);
 * const agent = await AgentFS.openWith(db);
 * ```
 */
export function createServerlessAdapter(conn: Connection): DatabasePromise {
  return {
    name: "serverless",
    readonly: false,
    open: true,
    memory: false,
    inTransaction: false,

    async connect(): Promise<void> {
      // Validate the connection by running a simple query
      await conn.execute("SELECT 1");
    },

    prepare(sql: string): LazyStatement {
      return new LazyStatement(conn, sql);
    },

    transaction(fn: (...args: any[]) => any) {
      return conn.transaction(fn);
    },

    async exec(sql: string): Promise<void> {
      await conn.exec(sql);
    },

    pragma(): Promise<any[]> {
      throw new Error("pragma() is not supported in serverless mode — pragmas are not available over HTTP");
    },

    backup: notSupported("backup"),
    serialize: notSupported("serialize"),
    function: notSupported("function"),
    aggregate: notSupported("aggregate"),
    table: notSupported("table"),
    loadExtension: notSupported("loadExtension"),
    maxWriteReplicationIndex: notSupported("maxWriteReplicationIndex"),
    interrupt: notSupported("interrupt"),

    defaultSafeIntegers(_toggle?: boolean) {},

    async close(): Promise<void> {
      await conn.close();
    },
  } as unknown as DatabasePromise;
}
