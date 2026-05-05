// Tests for the cache helper. Each test sets DPE_STORAGE + DPE_CACHE_MODE
// to a tempdir-scoped value, exercises a behavior, restores env.

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { cachedImpl, cachePath, readCacheMode } from '../src/cache';

let storage: string;
const PRESERVED: Record<string, string | undefined> = {};

beforeEach(() => {
  storage = mkdtempSync(join(tmpdir(), 'dpe-cache-test-'));
  PRESERVED.DPE_STORAGE = process.env.DPE_STORAGE;
  PRESERVED.DPE_CACHE_MODE = process.env.DPE_CACHE_MODE;
  process.env.DPE_STORAGE = storage;
  delete process.env.DPE_CACHE_MODE;
});

afterEach(() => {
  for (const k of Object.keys(PRESERVED)) {
    if (PRESERVED[k] === undefined) delete process.env[k];
    else process.env[k] = PRESERVED[k];
  }
  if (storage && existsSync(storage)) rmSync(storage, { recursive: true, force: true });
});

describe('readCacheMode', () => {
  test("default is 'use'", () => {
    delete process.env.DPE_CACHE_MODE;
    expect(readCacheMode()).toBe('use');
  });
  test('recognizes valid values', () => {
    for (const m of ['use', 'refresh', 'bypass', 'off'] as const) {
      process.env.DPE_CACHE_MODE = m;
      expect(readCacheMode()).toBe(m);
    }
  });
  test("falls back to 'use' on garbage", () => {
    process.env.DPE_CACHE_MODE = 'nonsense';
    expect(readCacheMode()).toBe('use');
  });
});

describe('cachePath', () => {
  test('returns null when DPE_STORAGE not set', () => {
    delete process.env.DPE_STORAGE;
    expect(cachePath('ns', { k: 1 })).toBeNull();
  });
  test('namespace-stable hash', () => {
    const a = cachePath('ns', { k: 1, m: 2 });
    const b = cachePath('ns', { m: 2, k: 1 }); // key order shouldn't matter
    expect(a).toBe(b);
  });
  test('different keys → different paths', () => {
    const a = cachePath('ns', { k: 1 });
    const b = cachePath('ns', { k: 2 });
    expect(a).not.toBe(b);
  });
  test('different namespaces → different paths', () => {
    const a = cachePath('ns1', { k: 1 });
    const b = cachePath('ns2', { k: 1 });
    expect(a).not.toBe(b);
  });
});

describe('cachedImpl', () => {
  test('miss → calls produce, writes file, returns produced', async () => {
    let calls = 0;
    const result = await cachedImpl('ns', { k: 1 }, () => {
      calls++;
      return { hello: 'world' };
    });
    expect(result).toEqual({ hello: 'world' });
    expect(calls).toBe(1);
    expect(existsSync(cachePath('ns', { k: 1 })!)).toBe(true);
  });

  test('hit → reads file, skips produce', async () => {
    // Prime via miss to create dir + initial cache file.
    await cachedImpl('ns', { k: 1 }, () => ({ initial: true }));
    // Overwrite with a seeded value, then verify hit returns it.
    writeFileSync(cachePath('ns', { k: 1 })!, JSON.stringify({ cached: true }));
    let calls = 0;
    const result = await cachedImpl<{ cached?: boolean; fresh?: boolean }>('ns', { k: 1 }, () => {
      calls++;
      return { fresh: true };
    });
    expect(result).toEqual({ cached: true });
    expect(calls).toBe(0);
  });

  test('refresh → always calls produce, overwrites file', async () => {
    await cachedImpl('ns', { k: 1 }, () => ({ first: true }));
    process.env.DPE_CACHE_MODE = 'refresh';
    let calls = 0;
    const result = await cachedImpl('ns', { k: 1 }, () => {
      calls++;
      return { fresh: true };
    });
    expect(result).toEqual({ fresh: true });
    expect(calls).toBe(1);
    const onDisk = JSON.parse(readFileSync(cachePath('ns', { k: 1 })!, 'utf8'));
    expect(onDisk).toEqual({ fresh: true });
  });

  test('bypass → calls produce, does NOT read or write', async () => {
    await cachedImpl('ns', { k: 1 }, () => ({ initial: true }));
    process.env.DPE_CACHE_MODE = 'bypass';
    let calls = 0;
    const result = await cachedImpl('ns', { k: 1 }, () => {
      calls++;
      return { fresh: true };
    });
    expect(result).toEqual({ fresh: true });
    expect(calls).toBe(1);
    // Original cache content unchanged.
    const onDisk = JSON.parse(readFileSync(cachePath('ns', { k: 1 })!, 'utf8'));
    expect(onDisk).toEqual({ initial: true });
  });

  test('off → same as bypass', async () => {
    await cachedImpl('ns', { k: 1 }, () => ({ initial: true }));
    process.env.DPE_CACHE_MODE = 'off';
    const result = await cachedImpl('ns', { k: 1 }, () => ({ fresh: true }));
    expect(result).toEqual({ fresh: true });
    const onDisk = JSON.parse(readFileSync(cachePath('ns', { k: 1 })!, 'utf8'));
    expect(onDisk).toEqual({ initial: true });
  });

  test('DPE_STORAGE missing → cache disabled, every call produces', async () => {
    delete process.env.DPE_STORAGE;
    let calls = 0;
    const r1 = await cachedImpl('ns', { k: 1 }, () => {
      calls++;
      return { n: calls };
    });
    const r2 = await cachedImpl('ns', { k: 1 }, () => {
      calls++;
      return { n: calls };
    });
    expect(r1).toEqual({ n: 1 });
    expect(r2).toEqual({ n: 2 });
  });

  test('producer error → propagates, no cache written', async () => {
    let attempts = 0;
    await expect(
      cachedImpl('ns', { k: 1 }, () => {
        attempts++;
        throw new Error('kaboom');
      }),
    ).rejects.toThrow('kaboom');
    expect(attempts).toBe(1);
    expect(existsSync(cachePath('ns', { k: 1 })!)).toBe(false);
  });

  test('malformed cache file → treated as miss, gets overwritten on produce', async () => {
    // Prime via miss to create dir.
    await cachedImpl('ns', { k: 1 }, () => ({ ok: true }));
    // Corrupt the file.
    writeFileSync(cachePath('ns', { k: 1 })!, 'this is not json');
    let calls = 0;
    const result = await cachedImpl('ns', { k: 1 }, () => {
      calls++;
      return { recovered: true };
    });
    expect(result).toEqual({ recovered: true });
    expect(calls).toBe(1);
  });

  test('async producer also works', async () => {
    const result = await cachedImpl('ns', { k: 1 }, async () => {
      await Promise.resolve();
      return { async: true };
    });
    expect(result).toEqual({ async: true });
  });
});
