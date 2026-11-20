/**
 * Conformance test: every stderr event the TS framework emits must validate
 * against `runner/schemas/stderr-events.schema.json`. Cross-checked with the
 * Rust and Python conformance tests.
 *
 * Strategy: spawn a tiny tool subprocess that calls the framework's stderr
 * writers, capture stderr, parse each line, validate against the schema.
 */
import { describe, expect, test } from 'bun:test';
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import Ajv from 'ajv/dist/2020';

const FRAMEWORK_DIR = join(import.meta.dir, '..').replaceAll('\\', '/');
const SCHEMA_PATH = join(
  import.meta.dir,
  '..',
  '..',
  '..',
  'runner',
  'schemas',
  'stderr-events.schema.json',
);

function loadValidator() {
  const ajv = new Ajv({ allErrors: true, strict: false });
  const schema = JSON.parse(readFileSync(SCHEMA_PATH, 'utf-8'));
  return ajv.compile(schema);
}

function runTool(script: string): Promise<{ stdout: string; stderr: string; code: number }> {
  const dir = mkdtempSync(join(tmpdir(), 'dpe-stderr-conformance-'));
  const scriptPath = join(dir, 'tool.ts');
  writeFileSync(scriptPath, script);

  return new Promise((resolve) => {
    const child = spawn('bun', [scriptPath], {
      stdio: ['pipe', 'pipe', 'pipe'],
      shell: false,
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => {
      stdout += d.toString();
    });
    child.stderr.on('data', (d) => {
      stderr += d.toString();
    });
    child.on('close', (code) => {
      resolve({ stdout, stderr, code: code ?? 0 });
    });
    child.stdin.end();
  });
}

function parseLines(s: string): unknown[] {
  return s
    .split('\n')
    .filter((l) => l.trim())
    .map((l) => JSON.parse(l));
}

describe('stderr event schema conformance', () => {
  test('log event validates against schema', async () => {
    const script = `
      import { writeLog } from "${FRAMEWORK_DIR}/src/envelope.ts";
      writeLog("hello world", "info");
      writeLog("debug detail", "debug");
      writeLog("warn now", "warn");
      writeLog("hard fail", "error");
    `;
    const validate = loadValidator();
    const { stderr, code } = await runTool(script);
    expect(code).toBe(0);
    const events = parseLines(stderr);
    expect(events.length).toBe(4);
    for (const ev of events) {
      const ok = validate(ev);
      if (!ok)
        throw new Error(
          `schema fail: ${JSON.stringify(validate.errors)} for ${JSON.stringify(ev)}`,
        );
    }
  });

  test('error event validates against schema', async () => {
    const script = `
      import { writeError } from "${FRAMEWORK_DIR}/src/envelope.ts";
      writeError({ k: "v" }, new Error("boom"), "id1", "src1");
    `;
    const validate = loadValidator();
    const { stderr, code } = await runTool(script);
    expect(code).toBe(0);
    const events = parseLines(stderr);
    expect(events.length).toBe(1);
    const ok = validate(events[0]);
    if (!ok) throw new Error(`schema fail: ${JSON.stringify(validate.errors)}`);
    const ev = events[0] as Record<string, unknown>;
    expect(ev.type).toBe('error');
    expect(ev.error).toBe('boom');
    expect(ev.id).toBe('id1');
    expect(ev.src).toBe('src1');
  });

  test('trace event validates against schema', async () => {
    const script = `
      import { writeTrace } from "${FRAMEWORK_DIR}/src/envelope.ts";
      writeTrace("id1", "src1", { stage: "convert", tool: "doc-converter" });
      writeTrace("id2", "src2", {});
    `;
    const validate = loadValidator();
    const { stderr, code } = await runTool(script);
    expect(code).toBe(0);
    const events = parseLines(stderr);
    expect(events.length).toBe(2);
    for (const ev of events) {
      const ok = validate(ev);
      if (!ok)
        throw new Error(
          `schema fail: ${JSON.stringify(validate.errors)} for ${JSON.stringify(ev)}`,
        );
    }
  });

  test('stats event validates against schema', async () => {
    const script = `
      import { writeStats } from "${FRAMEWORK_DIR}/src/envelope.ts";
      writeStats({ rows_in: 100, rows_out: 95, skipped: 5 });
      writeStats({});
    `;
    const validate = loadValidator();
    const { stderr, code } = await runTool(script);
    expect(code).toBe(0);
    const events = parseLines(stderr);
    expect(events.length).toBe(2);
    for (const ev of events) {
      const ok = validate(ev);
      if (!ok)
        throw new Error(
          `schema fail: ${JSON.stringify(validate.errors)} for ${JSON.stringify(ev)}`,
        );
    }
  });

  test('error event with extra fields and bad-input event validate', async () => {
    const script = `
      import { writeError, writeBadInput } from "${FRAMEWORK_DIR}/src/envelope.ts";
      writeError({ k: 1 }, new Error("rate limited"), "id1", "src1", { code: 429, retryable: true, details: { after: 3 } });
      writeError({ k: 2 }, Object.assign(new Error("dup"), { code: 11000 }), "id2", "src2");
      class QuotaError extends Error { constructor(m: string) { super(m); this.name = "QuotaError"; } }
      writeError({ k: 3 }, new QuotaError("over"), "id3", "src3");
      writeError({ k: 4 }, "plain string", "id4", "src4");
      writeBadInput("{not json" + "x".repeat(5000), "invalid JSON");
    `;
    const validate = loadValidator();
    const { stderr, code } = await runTool(script);
    expect(code).toBe(0);
    const ev = parseLines(stderr) as Record<string, unknown>[];
    expect(ev.length).toBe(5);
    for (const e of ev) {
      if (!validate(e))
        throw new Error(`schema fail: ${JSON.stringify(validate.errors)} for ${JSON.stringify(e)}`);
    }
    expect([ev[0]!.code, ev[0]!.retryable, ev[0]!.details]).toEqual(['429', true, { after: 3 }]);
    expect(ev[1]!.code).toBe('11000'); // from err.code
    expect(ev[2]!.code).toBe('QuotaError'); // from the class name
    expect('code' in ev[3]!).toBe(false); // plain Error / string: no code
    expect((ev[4]!.raw as string).length).toBe(4096);
    expect(ev[4]!.error).toBe('unparseable input line: invalid JSON');
    expect('input' in ev[4]!).toBe(false);
  });

  test('schema rejects malformed events', () => {
    // Sanity-check the schema actually rejects bad shapes.
    const validate = loadValidator();
    expect(validate({ type: 'log', msg: 'no level' })).toBe(false);
    expect(validate({ type: 'error', error: 'x' })).toBe(false);
    expect(validate({ type: 'error', error: 'x', input: 1, id: 'a', src: 'b', code: 5 })).toBe(
      false,
    );
    expect(validate({ type: 'error', error: 'x', raw: 'y'.repeat(4097) })).toBe(false);
    expect(validate({ type: 'mystery' })).toBe(false);
  });
});
