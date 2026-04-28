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
  } catch {
    return null;
  }
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

export function writeError(v: JSONValue, err: unknown, id: string, src: string): void {
  _writeStderr({
    type: 'error',
    error: err instanceof Error ? err.message : String(err),
    input: v,
    id,
    src,
  });
}

/** Merged trace event flushed once per output envelope (even with empty labels). */
export function writeTrace(id: string, src: string, labels: Record<string, JSONValue>): void {
  _writeStderr({ type: 'trace', id, src, labels });
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
