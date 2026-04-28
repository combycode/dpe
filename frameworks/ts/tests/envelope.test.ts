import { describe, expect, test } from 'bun:test';
import { computeId, hashString, parseEnvelope } from '../src/envelope';

describe('hashString', () => {
  test('returns 16 hex chars', () => {
    const h = hashString('hello');
    expect(h).toMatch(/^[0-9a-f]{16}$/);
  });

  test('deterministic', () => {
    expect(hashString('abc')).toBe(hashString('abc'));
  });

  test('unicode works', () => {
    const h = hashString('Итог:');
    expect(h).toMatch(/^[0-9a-f]{16}$/);
  });

  test('different inputs give different hashes', () => {
    expect(hashString('a')).not.toBe(hashString('b'));
  });
});

describe('computeId', () => {
  test('stable across key reorderings (canonical JSON)', () => {
    const a = computeId('src', 'stage', { b: 2, a: 1 });
    const b = computeId('src', 'stage', { a: 1, b: 2 });
    expect(a).toBe(b);
  });

  test('different src gives different id', () => {
    const a = computeId('s1', 'stage', { x: 1 });
    const b = computeId('s2', 'stage', { x: 1 });
    expect(a).not.toBe(b);
  });

  test('different stage gives different id', () => {
    const a = computeId('src', 'a', { x: 1 });
    const b = computeId('src', 'b', { x: 1 });
    expect(a).not.toBe(b);
  });

  test('nested objects are hashed canonically', () => {
    const a = computeId('s', 't', { x: { b: 2, a: 1 } });
    const b = computeId('s', 't', { x: { a: 1, b: 2 } });
    expect(a).toBe(b);
  });
});

describe('parseEnvelope', () => {
  test('parses data envelope', () => {
    const env = parseEnvelope('{"t":"d","id":"abc","src":"s","v":{"x":1}}');
    expect(env).toEqual({ t: 'd', id: 'abc', src: 's', v: { x: 1 } });
  });

  test('parses meta envelope', () => {
    const env = parseEnvelope('{"t":"m","v":{"rows":5}}');
    expect(env?.t).toBe('m');
  });

  test('returns null for blank', () => {
    expect(parseEnvelope('')).toBeNull();
    expect(parseEnvelope('   \n')).toBeNull();
  });

  test('returns null for invalid JSON', () => {
    expect(parseEnvelope('not json')).toBeNull();
  });

  test('returns null for missing t', () => {
    expect(parseEnvelope('{"v":{}}')).toBeNull();
  });

  test('returns null for unknown t value', () => {
    expect(parseEnvelope('{"t":"x","v":{}}')).toBeNull();
  });
});
