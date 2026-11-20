/**
 * NDJSON envelope types + parse / write / hash helpers.
 *
 * The envelope protocol matches the Python and Rust frameworks:
 *   data:  {"t":"d","id":"<8B hex>","src":"<string>","v":<payload>}
 *   meta:  {"t":"m","v":<payload>}
 *   log:   {"type":"log","level":"...","msg":"...","...":"..."}  (stderr)
 *   error: {"type":"error","error":"...","input":..,"id":..,"src":..} (stderr)
 */

import { createHash } from 'node:crypto';

export type JSONValue = null | boolean | number | string | JSONValue[] | { [k: string]: JSONValue };

export interface DataEnvelope {
  t: 'd';
  id: string;
  src: string;
  v: JSONValue;
}

export interface MetaEnvelope {
  t: 'm';
  v: JSONValue;
}

export type Envelope = DataEnvelope | MetaEnvelope;

/** blake2b-64 hash of a string. Returns 16-char hex. Matches Python/Rust envelope ID. */
export function hashString(key: string): string {
  return createHash('blake2b512').update(key, 'utf8').digest('hex').slice(0, 16);
}

/** Stream-hash a file by path. Returns 16 hex chars. `undefined` if the file cannot be opened.
 *
 * Streams chunks through the hasher rather than loading the file into a single
 * Uint8Array — important for large inputs (> a few hundred MB).
 */
export async function hashFile(filepath: string): Promise<string | undefined> {
  try {
    const hasher = createHash('blake2b512');
    const stream = Bun.file(filepath).stream();
    for await (const chunk of stream as AsyncIterable<Uint8Array>) {
      hasher.update(chunk);
    }
    return hasher.digest('hex').slice(0, 16);
  } catch {
    return undefined;
  }
}

/** Deterministic id from (src | stage | sorted JSON of v). */
export function computeId(src: string, stage: string, v: JSONValue): string {
  const vStr = canonicalJson(v);
  return hashString(`${src}|${stage}|${vStr}`);
}

/** Parse one NDJSON line. Returns null on blank, invalid JSON, or any
 *  shape that isn't a well-formed envelope. Data envelopes must carry
 *  `id` and `src` strings; meta envelopes must carry a `v` payload. */
export function parseEnvelope(line: string): Envelope | null {
  const trimmed = line.trim();
  if (!trimmed) return null;
  try {
    const obj = JSON.parse(trimmed);
    if (!obj || typeof obj !== 'object') return null;
    return asEnvelope(obj);
  } catch {
    return null;
  }
}

/** The parsed object as a well-formed data / meta envelope, or null. */
function asEnvelope(obj: { t?: unknown; id?: unknown; src?: unknown }): Envelope | null {
  if (obj.t === 'd') {
    if (typeof obj.id !== 'string' || typeof obj.src !== 'string') return null;
    if (!('v' in obj)) return null;
    return obj as DataEnvelope;
  }
  if (obj.t === 'm') {
    if (!('v' in obj)) return null;
    return obj as MetaEnvelope;
  }
  return null;
}

/** What one stdin line is, for the runtime's input loop. */
export type InputLine =
  | { kind: 'empty' }
  | { kind: 'envelope'; env: Envelope }
  /** Valid JSON object with a `t` the runtime does not handle: skipped, as before. */
  | { kind: 'other' }
  /** Not an envelope: invalid JSON, not an object, or a data / meta envelope missing fields. */
  | { kind: 'bad'; reason: string };

/** `acceptMeta`: whether the tool takes meta envelopes; if not, a malformed one is skipped
 *  like any meta envelope rather than reported. */
export function parseInputLine(line: string, acceptMeta = false): InputLine {
  const trimmed = line.trim();
  if (!trimmed) return { kind: 'empty' };
  let obj: unknown;
  try {
    obj = JSON.parse(trimmed);
  } catch (e) {
    return { kind: 'bad', reason: `invalid JSON (${e instanceof Error ? e.message : String(e)})` };
  }
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
    return { kind: 'bad', reason: 'not a JSON object' };
  }
  const env = asEnvelope(obj as { t?: unknown });
  if (env) return { kind: 'envelope', env };
  const t = (obj as { t?: unknown }).t;
  if (t === 'd')
    return { kind: 'bad', reason: 'data envelope without string id / src or without v' };
  if (t === 'm' && acceptMeta) return { kind: 'bad', reason: 'meta envelope without v' };
  return { kind: 'other' };
}

/** Optional detail for `ctx.error(v, err, extra)`; the runner puts it on the stage's `errors` port. */
export interface ErrorExtra {
  /** Machine-readable error code; defaults to `err.code`, else the error's class name. */
  code?: string | number;
  /** Whether trying again may succeed (rate limit, timeout...). */
  retryable?: boolean;
  /** Anything else worth keeping with the error. */
  details?: JSONValue;
}

/** Longest start of an unparseable input line kept in its error event. */
export const RAW_LINE_LIMIT = 4096;

function errorCode(err: unknown): string | undefined {
  if (err && typeof err === 'object') {
    // DOMException's numeric `code` is a legacy constant (TimeoutError → 23); its name is
    // the meaningful part (fetch timeouts and aborts arrive as DOMException).
    if (typeof DOMException !== 'undefined' && err instanceof DOMException) return err.name;
    const code = (err as { code?: unknown }).code;
    if (typeof code === 'string' || typeof code === 'number') return String(code);
    if (err instanceof Error && err.name && err.name !== 'Error') return err.name;
  }
  return undefined;
}

export function writeData(v: JSONValue, id: string, src: string): void {
  _writeStdout({ t: 'd', id, src, v });
}

export function writeMeta(v: JSONValue): void {
  _writeStdout({ t: 'm', v });
}

export function writeLog(msg: string, level = 'info', extra?: Record<string, JSONValue>): void {
  _writeStderr({ type: 'log', level, msg, ...(extra ?? {}) });
}

export function writeError(
  v: JSONValue,
  err: unknown,
  id: string,
  src: string,
  extra?: ErrorExtra,
): void {
  const event: Record<string, JSONValue> = {
    type: 'error',
    error: err instanceof Error ? err.message : String(err),
    input: v,
    id,
    src,
  };
  const code = extra?.code !== undefined ? String(extra.code) : errorCode(err);
  if (code !== undefined) event.code = code;
  if (typeof extra?.retryable === 'boolean') event.retryable = extra.retryable;
  if (extra?.details !== undefined) event.details = extra.details;
  _writeStderr(event);
}

/** An input line that is not an envelope: reported instead of being skipped silently. */
export function writeBadInput(line: string, reason: string): void {
  _writeStderr({
    type: 'error',
    error: `unparseable input line: ${reason}`,
    // Cut by code points: a UTF-16 slice can split a surrogate pair, and a lone surrogate
    // makes the whole event unreadable for the runner. The first 2 x limit UTF-16 units always
    // hold the first `limit` code points, so a huge line is never expanded whole.
    raw: Array.from(line.slice(0, 2 * RAW_LINE_LIMIT))
      .slice(0, RAW_LINE_LIMIT)
      .join(''),
  });
}

/** Merged trace event flushed once per output envelope (even with empty labels).
 *
 *  `channel` distinguishes counter buckets at the runner side:
 *    - `"data"` from ctx.output  → rows_out
 *    - `"meta"` from ctx.meta    → meta
 *  Omit the field for back-compat (older runner treats unset as data).
 */
export function writeTrace(
  id: string,
  src: string,
  labels: Record<string, JSONValue>,
  channel?: 'data' | 'meta',
): void {
  const rec: Record<string, JSONValue> = { type: 'trace', id, src, labels };
  if (channel) rec.channel = channel;
  _writeStderr(rec);
}

/** Input event — emitted by the runtime per envelope read from stdin,
 *  BEFORE the processor runs. Lets the runner count rows_in for stages
 *  that never call ctx.output (pass-through, terminal sinks). */
export function writeInput(id: string, src: string): void {
  _writeStderr({ type: 'input', id, src });
}

/** Custom stats event. Shape: {type:"stats", ...data}. */
export function writeStats(data: Record<string, JSONValue>): void {
  _writeStderr({ type: 'stats', ...data });
}

/** Canonical JSON for hashing: sorted keys, compact, no spaces. */
function canonicalJson(v: JSONValue): string {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(',')}]`;
  const keys = Object.keys(v).sort();
  const parts = keys.map(
    (k) => `${JSON.stringify(k)}:${canonicalJson((v as Record<string, JSONValue>)[k]!)}`,
  );
  return `{${parts.join(',')}}`;
}

function _writeStdout(obj: Record<string, JSONValue>): void {
  process.stdout.write(`${JSON.stringify(obj)}\n`);
}

function _writeStderr(obj: Record<string, JSONValue>): void {
  process.stderr.write(`${JSON.stringify(obj)}\n`);
}
