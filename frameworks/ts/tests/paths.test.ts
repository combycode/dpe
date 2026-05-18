/**
 * Tests for EnvPaths -- $token resolve/tokenize round-trip.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { EnvPaths } from '../src/paths';

function paths(): EnvPaths {
  return new EnvPaths([
    ['input', '/abs/input'],
    ['output', '/abs/output'],
    ['storage', '/abs/storage'],
    // Intentionally shorter abs -- tests longest-match logic.
    ['session', '/abs/storage/session'],
  ]);
}

// --- resolveValue ---

describe('resolveValue', () => {
  test('top-level token', () => {
    expect(paths().resolveValue('$input')).toBe('/abs/input');
  });

  test('token with subpath', () => {
    expect(paths().resolveValue('$input/data/file.csv')).toBe('/abs/input/data/file.csv');
  });

  test('unknown token unchanged', () => {
    expect(paths().resolveValue('$set/field')).toBe('$set/field');
    expect(paths().resolveValue('$bogus')).toBe('$bogus');
  });

  test('recurses into object', () => {
    const v = { path: '$input/a.csv', n: 42, nested: { p: '$output/b' } };
    const r = paths().resolveValue(v) as Record<string, unknown>;
    expect(r.path).toBe('/abs/input/a.csv');
    expect(r.n).toBe(42);
    expect((r.nested as Record<string, unknown>).p).toBe('/abs/output/b');
  });

  test('recurses into array', () => {
    const v = ['$input/x', '$output/y', 'plain'];
    const r = paths().resolveValue(v) as string[];
    expect(r).toEqual(['/abs/input/x', '/abs/output/y', 'plain']);
  });

  test('no env is no-op', () => {
    const empty = new EnvPaths([]);
    const v = { p: '$input/foo' };
    expect(empty.resolveValue(v)).toEqual(v);
  });

  test('non-string types unchanged', () => {
    const v = { n: 42, b: true, nil: null, p: '$input' };
    const r = paths().resolveValue(v) as Record<string, unknown>;
    expect(r.n).toBe(42);
    expect(r.b).toBe(true);
    expect(r.nil).toBeNull();
    expect(r.p).toBe('/abs/input');
  });
});

// --- tokenizeValue ---

describe('tokenizeValue', () => {
  test('exact prefix', () => {
    expect(paths().tokenizeValue('/abs/input')).toBe('$input');
  });

  test('prefix with subpath', () => {
    expect(paths().tokenizeValue('/abs/output/results/out.csv')).toBe('$output/results/out.csv');
  });

  test('partial component not replaced', () => {
    // "/abs/inputXYZ" must NOT match "$input"
    expect(paths().tokenizeValue('/abs/inputXYZ')).toBe('/abs/inputXYZ');
  });

  test('longest prefix wins', () => {
    // "/abs/storage/session/..." -> $session (longer abs), not $storage
    expect(paths().tokenizeValue('/abs/storage/session/data.json')).toBe('$session/data.json');
  });

  test('non-path unchanged', () => {
    expect(paths().tokenizeValue('hello world')).toBe('hello world');
    expect(paths().tokenizeValue(42)).toBe(42);
    expect(paths().tokenizeValue(null)).toBeNull();
  });

  test('recurses into nested', () => {
    const v = { a: { b: '/abs/input/x' }, arr: ['/abs/output/y'] };
    const r = paths().tokenizeValue(v) as Record<string, unknown>;
    expect((r.a as Record<string, unknown>).b).toBe('$input/x');
    expect((r.arr as string[])[0]).toBe('$output/y');
  });
});

// --- round-trip ---

describe('round-trip', () => {
  test('resolve then tokenize restores original', () => {
    const p = new EnvPaths([
      ['input', '/data/in'],
      ['output', '/data/out'],
    ]);
    const original = { src: '$input/file.csv', dst: '$output/result.csv' };
    const resolved = p.resolveValue(original);
    expect(resolved).toEqual({ src: '/data/in/file.csv', dst: '/data/out/result.csv' });
    expect(p.tokenizeValue(resolved)).toEqual(original);
  });

  test('windows backslash in abs normalised', () => {
    const p = new EnvPaths([['data', 'C:\\Data\\proj']]);
    expect(p.resolveValue('$data/sub')).toBe('C:/Data/proj/sub');
  });
});

// --- from process.env ---

describe('from process.env', () => {
  const saved: Record<string, string | undefined> = {};
  const testVars = [
    'DPE_INPUT',
    'DPE_OUTPUT',
    'DPE_CONFIGS',
    'DPE_STORAGE',
    'DPE_TEMP',
    'DPE_SESSION',
  ];

  beforeAll(() => {
    for (const v of testVars) saved[v] = process.env[v];
  });

  afterAll(() => {
    for (const v of testVars) {
      if (saved[v] === undefined) delete process.env[v];
      else process.env[v] = saved[v];
    }
  });

  test('reads DPE_INPUT', () => {
    process.env.DPE_INPUT = '/mnt/input';
    delete process.env.DPE_OUTPUT;
    const p = new EnvPaths();
    expect(p.resolveValue('$input/file.csv')).toBe('/mnt/input/file.csv');
    expect(p.resolveValue('$output/x')).toBe('$output/x'); // not set
    delete process.env.DPE_INPUT;
  });

  test('missing vars produces empty', () => {
    for (const v of testVars) delete process.env[v];
    const p = new EnvPaths();
    expect(p.isEmpty()).toBe(true);
    expect(p.resolveValue('$input/foo')).toBe('$input/foo');
  });

  test('named factory: fromEnv() reads DPE_* vars', () => {
    process.env.DPE_INPUT = '/mnt/x';
    delete process.env.DPE_OUTPUT;
    const p = EnvPaths.fromEnv();
    expect(p.resolveValue('$input/file')).toBe('/mnt/x/file');
    delete process.env.DPE_INPUT;
  });

  test('named factory: fromPairs() bypasses env', () => {
    process.env.DPE_INPUT = '/ignored';
    const p = EnvPaths.fromPairs([['input', '/explicit']]);
    expect(p.resolveValue('$input/file')).toBe('/explicit/file');
    delete process.env.DPE_INPUT;
  });
});
