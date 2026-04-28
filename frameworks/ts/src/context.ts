/**
 * Context object — passed to every processor invocation.
 *
 * Mirrors the Python and Rust Context API:
 *   ctx.output(v, {id?, src?})
 *   ctx.emit(queue, v, {id?, src?})
 *   ctx.drain()
 *   ctx.meta(v)
 *   ctx.log(msg, {level?, ...extra})
 *   ctx.error(v, err)
 *   ctx.trace(k, v)
 *   ctx.stats(data)
 *   ctx.hash(str)
 *   ctx.hashFile(path)
 */

import type { JSONValue } from './envelope';
import {
  hashFile,
  hashString,
  writeData,
  writeError,
  writeLog,
  writeMeta,
  writeStats,
  writeTrace,
} from './envelope';

export interface QueueItem {
  queue: string;
  v: JSONValue;
  id: string;
  src: string;
}

/** Tiny runtime surface the Context needs — avoids circular imports with Runtime. */
export interface RuntimeLike {
  enqueue(item: QueueItem): void;
  drainQueue(): Promise<void>;
}

export class Context {
  readonly id: string;
  readonly src: string;
  readonly memory: Memory;
  private readonly _runtime: RuntimeLike;
  /** Labels accumulated by ctx.trace(k, v). Flushed (merged) as one
   *  {type:"trace"} stderr event before each ctx.output(), then cleared. */
  private _labels: Record<string, JSONValue> = {};

  constructor(id: string, src: string, memory: Memory, runtime: RuntimeLike) {
    this.id = id;
    this.src = src;
    this.memory = memory;
    this._runtime = runtime;
  }

  /** Accumulate a label on this invocation's next output envelope. */
  trace(key: string, value: JSONValue): void {
    this._labels[key] = value;
  }

  /** Emit a stats event to stderr. Example: ctx.stats({rows:1000, rps:250}). */
  stats(data: Record<string, JSONValue>): void {
    if (data && Object.keys(data).length > 0) {
      writeStats(data);
    }
  }

  output(v: JSONValue, opts: { id?: string; src?: string } = {}): void {
    const outId = opts.id ?? this.id;
    const outSrc = opts.src ?? this.src;
    // Emit merged trace first (always, even with empty labels), then data, then clear.
    writeTrace(outId, outSrc, this._labels);
    this._labels = {};
    writeData(v, outId, outSrc);
  }

  emit(queue: string, v: JSONValue, opts: { id?: string; src?: string } = {}): void {
    this._runtime.enqueue({
      queue,
      v,
      id: opts.id ?? this.id,
      src: opts.src ?? this.src,
    });
  }

  async drain(): Promise<void> {
    await this._runtime.drainQueue();
  }

  meta(v: JSONValue): void {
    writeMeta(v);
  }

  log(msg: string, opts: { level?: string } & Record<string, JSONValue> = {}): void {
    const { level = 'info', ...extra } = opts as { level?: string } & Record<string, JSONValue>;
    writeLog(msg, level, Object.keys(extra).length ? extra : undefined);
  }

  error(v: JSONValue, err: unknown): void {
    writeError(v, err, this.id, this.src);
  }

  hash(key: string): string {
    return hashString(key);
  }

  hashFile(filepath: string): Promise<string | undefined> {
    return hashFile(filepath);
  }
}

/**
 * Shared accumulator memory. Tools extend this by attaching their own
 * accumulator instances as properties. Named accumulator types will ship in a
 * later milestone — filter tool doesn't need them.
 */
export class Memory {
  [key: string]: unknown;
}
