// Cache helper used by `ctx.cached(...)`. Honors the runner's
// DPE_CACHE_MODE env (use / refresh / bypass / off) and stores
// JSON-serialized values at $DPE_STORAGE/<namespace>/<key>.json.
//
// Key derivation: blake2b(canonical-JSON(key)) → 32-hex chars.
// Canonical = sorted object keys, compact, no whitespace.
//
// Failure modes (cache-disabling, NOT errors propagated to user):
//   - DPE_STORAGE not set        → cache disabled, every call produces
//   - cache file unreadable      → treat as miss, log warn
//   - cache file unparseable     → treat as miss, log warn
//   - producer throws            → re-throw to caller (no cache write)
//
// All cache I/O is sync — these are short JSON files, async wouldn't
// buy anything and would complicate the call site.

import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { writeLog } from './envelope';

export type CacheMode = 'use' | 'refresh' | 'bypass' | 'off';

/** Read DPE_CACHE_MODE; default to "use". Anything unrecognized → "use". */
export function readCacheMode(): CacheMode {
  const v = process.env.DPE_CACHE_MODE ?? 'use';
  if (v === 'use' || v === 'refresh' || v === 'bypass' || v === 'off') return v;
  return 'use';
}

/** Compute the on-disk path for a (namespace, key) pair. Returns null
 *  when DPE_STORAGE isn't set — caller treats that as cache disabled. */
export function cachePath(namespace: string, key: unknown): string | null {
  const storage = process.env.DPE_STORAGE;
  if (!storage) return null;
  const k = canonicalJson(key);
  const hash = createHash('blake2b512').update(k, 'utf8').digest('hex').slice(0, 32);
  return join(storage, namespace, `${hash}.json`);
}

/** Helper used by Context.cached(). Implements the four-mode cache
 *  semantics around the produce callback. */
export async function cachedImpl<T>(
  namespace: string,
  key: unknown,
  produce: () => T | Promise<T>,
): Promise<T> {
  const mode = readCacheMode();
  const path = cachePath(namespace, key);

  // Mode "off"/"bypass" or no $storage → just produce, no cache I/O.
  const canRead = path !== null && (mode === 'use' || mode === 'refresh');
  const canWrite = path !== null && mode !== 'bypass' && mode !== 'off';
  const willRead = canRead && mode !== 'refresh';

  if (willRead && existsSync(path!)) {
    try {
      const raw = readFileSync(path!, 'utf8');
      const value = JSON.parse(raw) as T;
      writeLog(`cached: hit (${namespace})`, 'debug');
      return value;
    } catch (e) {
      writeLog(
        `cached: read failed (${namespace}) — ${e instanceof Error ? e.message : String(e)}`,
        'warn',
      );
      // Fall through to produce.
    }
  }

  const result = await produce();

  if (canWrite && path !== null) {
    try {
      mkdirSync(dirname(path), { recursive: true });
      writeFileSync(path, JSON.stringify(result));
    } catch (e) {
      writeLog(
        `cached: write failed (${namespace}) — ${e instanceof Error ? e.message : String(e)}`,
        'warn',
      );
      // Don't fail the caller — they got their value.
    }
  }

  return result;
}

/** Canonical JSON: stable across runs. Sorted object keys, compact,
 *  no whitespace. Same logic as envelope.canonicalJson but exposed
 *  here so cache is independent of envelope-id concerns. */
function canonicalJson(v: unknown): string {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${(v as unknown[]).map(canonicalJson).join(',')}]`;
  const keys = Object.keys(v as Record<string, unknown>).sort();
  const parts = keys.map(
    (k) => `${JSON.stringify(k)}:${canonicalJson((v as Record<string, unknown>)[k])}`,
  );
  return `{${parts.join(',')}}`;
}
