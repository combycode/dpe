/**
 * EnvPaths -- resolves $token path prefixes in envelope v on input;
 * reverse-tokenizes absolute paths back to $token form on output.
 *
 * Input side (runtime, before processor): $token/subpath -> absolute path
 * Output side (context.output / context.meta): absolute path -> $token/subpath
 *
 * Both sides are a no-op when the DPE_* env vars are not set.
 * `DPE_PATH_TOKENS=off` (also `0` / `false` / `no`) turns the output side off; the
 * runner sets it from the variant's / stage's `path_tokens`. Input resolution stays on.
 */

import type { JSONValue } from './envelope';

const TOKEN_MAP: [string, string][] = [
  ['input', 'DPE_INPUT'],
  ['output', 'DPE_OUTPUT'],
  ['configs', 'DPE_CONFIGS'],
  ['storage', 'DPE_STORAGE'],
  ['temp', 'DPE_TEMP'],
  ['session', 'DPE_SESSION'],
];

interface Entry {
  token: string;
  abs: string;
}

/** True unless `DPE_PATH_TOKENS` is `off`, `0`, `false` or `no` (any case). */
export function tokensEnabledFromEnv(): boolean {
  const v = process.env.DPE_PATH_TOKENS;
  if (v == null) return true;
  return !['off', '0', 'false', 'no'].includes(v.trim().toLowerCase());
}

/** Resolved env prefix map.  Build once at tool startup; cheap to clone. */
export class EnvPaths {
  private entries: Entry[];
  private noTokenize: boolean;

  /**
   * If `pairs` is provided, use those (token, abs_path) pairs directly.
   * Otherwise read the standard DPE_* env vars from process.env.
   *
   * Prefer the named factories `EnvPaths.fromEnv()` and
   * `EnvPaths.fromPairs(pairs)` -- they're consistent with the Rust SDK
   * (`from_env()` / `from_pairs(&[...])`) and read more clearly at call
   * sites. The polymorphic constructor is kept for backwards compat.
   */
  constructor(pairs?: [string, string][]) {
    const raw: Entry[] =
      pairs != null
        ? pairs.map(([token, abs]) => ({ token, abs: abs.replace(/\\/g, '/') }))
        : TOKEN_MAP.flatMap(([token, envVar]) => {
            const val = process.env[envVar];
            return val ? [{ token, abs: val.replace(/\\/g, '/') }] : [];
          });

    // Sort longest-abs-first for greedy tokenization.
    this.entries = raw.sort((a, b) => b.abs.length - a.abs.length);
    this.noTokenize = pairs == null ? !tokensEnabledFromEnv() : false;
  }

  /** Turn the output side (absolute path -> `$token`) on or off. */
  withTokenize(on: boolean): EnvPaths {
    this.noTokenize = !on;
    return this;
  }

  /** Whether `tokenizeValue` rewrites anything. */
  tokenizeEnabled(): boolean {
    return !this.noTokenize;
  }

  /** Read the standard DPE_* env vars; missing/empty vars are skipped. */
  static fromEnv(): EnvPaths {
    return new EnvPaths();
  }

  /** Construct from explicit (token, abs_path) pairs -- useful for tests. */
  static fromPairs(pairs: [string, string][]): EnvPaths {
    return new EnvPaths(pairs);
  }

  isEmpty(): boolean {
    return this.entries.length === 0;
  }

  /** Walk v, resolving $token[/subpath] strings to absolute paths. */
  resolveValue(v: JSONValue): JSONValue {
    if (this.isEmpty()) return v;
    return this.walk(v, (s) => this.resolveStr(s));
  }

  /** Walk v, tokenizing absolute paths back to $token[/subpath] form. */
  tokenizeValue(v: JSONValue): JSONValue {
    if (this.isEmpty() || this.noTokenize) return v;
    return this.walk(v, (s) => this.tokenizeStr(s));
  }

  private walk(v: JSONValue, f: (s: string) => string): JSONValue {
    if (typeof v === 'string') return f(v);
    if (Array.isArray(v)) return v.map((x) => this.walk(x, f));
    if (v !== null && typeof v === 'object') {
      const out: Record<string, JSONValue> = {};
      for (const [k, val] of Object.entries(v)) {
        out[k] = this.walk(val as JSONValue, f);
      }
      return out;
    }
    return v;
  }

  private resolveStr(s: string): string {
    if (!s.startsWith('$')) return s;
    const slashIdx = s.indexOf('/');
    const [name, tail] =
      slashIdx < 0 ? [s.slice(1), ''] : [s.slice(1, slashIdx), s.slice(slashIdx)];
    const entry = this.entries.find((e) => e.token === name);
    if (!entry) return s;
    return tail ? entry.abs + tail : entry.abs;
  }

  private tokenizeStr(s: string): string {
    const normalized = s.replace(/\\/g, '/');
    for (const entry of this.entries) {
      if (normalized.startsWith(entry.abs)) {
        const rest = normalized.slice(entry.abs.length);
        if (!rest) return `$${entry.token}`;
        if (rest.startsWith('/')) return `$${entry.token}${rest}`;
        // rest doesn't start with '/' -- partial component match, skip
      }
    }
    return s;
  }
}
