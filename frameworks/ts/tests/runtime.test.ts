/**
 * Integration tests for the runtime: spawn a tiny tool that uses the framework
 * via `bun run <script>`, feed NDJSON on stdin, assert on stdout / stderr.
 */
import { describe, expect, test } from 'bun:test';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const FRAMEWORK_DIR = join(import.meta.dir, '..').replaceAll('\\', '/');

function runTool(
  script: string,
  settings: unknown,
  stdin: string,
): Promise<{ stdout: string; stderr: string; code: number }> {
  const dir = mkdtempSync(join(tmpdir(), 'dpe-framework-ts-'));
  const scriptPath = join(dir, 'tool.ts');
  writeFileSync(scriptPath, script);

  return new Promise((resolve) => {
    const child = spawn('bun', [scriptPath, JSON.stringify(settings)], {
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
    child.stdin.write(stdin);
    child.stdin.end();
  });
}

/** Permissive shape used in test assertions. Production code never returns
 *  these — the runtime emits typed envelopes. Tests need flexible field
 *  access (`out[0].v.got`, `e.type`, etc.) so we cast through this type.
 */
// biome-ignore lint/suspicious/noExplicitAny: tests need flexible JSON access
type TestLine = { [k: string]: any };

function parseLines(s: string): TestLine[] {
  return s
    .split('\n')
    .filter((l) => l.trim())
    .map((l) => JSON.parse(l) as TestLine);
}

describe('runtime stdin loop', () => {
  test('emits one output per input data envelope', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({
                input: (v, _s, ctx) => {
                    ctx.output({ got: v });
                },
            });
        `;
    const input = [
      '{"t":"d","id":"a","src":"s1","v":{"x":1}}',
      '{"t":"d","id":"b","src":"s2","v":{"x":2}}',
    ].join('\n');
    const { stdout } = await runTool(script, {}, input);
    const out = parseLines(stdout);
    expect(out.length).toBe(2);
    expect(out[0]!.v.got).toEqual({ x: 1 });
    expect(out[1]!.v.got).toEqual({ x: 2 });
  });

  test('skips meta envelopes (only process_input sees data)', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({
                input: (v, _s, ctx) => { ctx.output({ saw: v }); },
            });
        `;
    const input = [
      '{"t":"m","v":{"stats":1}}',
      '{"t":"d","id":"a","src":"s","v":{"x":1}}',
      '{"t":"m","v":{"stats":2}}',
    ].join('\n');
    const { stdout } = await runTool(script, {}, input);
    const dataOut = parseLines(stdout).filter((e) => e.t === 'd');
    expect(dataOut.length).toBe(1);
  });

  test('passes settings from argv[1]', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({
                input: (_v, settings, ctx) => { ctx.output(settings); },
            });
        `;
    const { stdout } = await runTool(
      script,
      { threshold: 42, name: 'x' },
      '{"t":"d","id":"a","src":"s","v":{}}\n',
    );
    const out = parseLines(stdout);
    expect(out[0]!.v).toEqual({ threshold: 42, name: 'x' });
  });

  test('preserves id and src when ctx.output called without overrides', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({
                input: (v, _s, ctx) => ctx.output(v),
            });
        `;
    const { stdout } = await runTool(
      script,
      {},
      '{"t":"d","id":"abc123","src":"upstream","v":{"x":1}}\n',
    );
    const out = parseLines(stdout)[0]!;
    expect(out.id).toBe('abc123');
    expect(out.src).toBe('upstream');
  });

  test('overrides id and src when provided to ctx.output', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({
                input: (v, _s, ctx) => ctx.output(v, { id: "custom", src: "mine" }),
            });
        `;
    const { stdout } = await runTool(script, {}, '{"t":"d","id":"abc","src":"s","v":{}}\n');
    const out = parseLines(stdout)[0]!;
    expect(out.id).toBe('custom');
    expect(out.src).toBe('mine');
  });

  test('errors thrown in processor go to stderr and stream continues', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({
                input: (v, _s, ctx) => {
                    if ((v as any).bad) throw new Error("boom");
                    ctx.output(v);
                },
            });
        `;
    const input = [
      '{"t":"d","id":"a","src":"s","v":{"ok":true}}',
      '{"t":"d","id":"b","src":"s","v":{"bad":true}}',
      '{"t":"d","id":"c","src":"s","v":{"ok":true}}',
    ].join('\n');
    const { stdout, stderr } = await runTool(script, {}, input);
    const data = parseLines(stdout).filter((e) => e.t === 'd');
    expect(data.length).toBe(2);
    const errors = parseLines(stderr).filter((e) => e.type === 'error');
    expect(errors.length).toBe(1);
    expect(errors[0]!.error).toContain('boom');
  });

  test('ctx.meta emits a meta envelope', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({
                input: (_v, _s, ctx) => ctx.meta({ stats: { n: 1 } }),
            });
        `;
    const { stdout } = await runTool(script, {}, '{"t":"d","id":"a","src":"s","v":{}}\n');
    const metas = parseLines(stdout).filter((e) => e.t === 'm');
    expect(metas.length).toBe(1);
    expect(metas[0]!.v.stats).toEqual({ n: 1 });
  });

  test('ctx.log emits to stderr', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({
                input: (_v, _s, ctx) => { ctx.log("hello", { level: "warn", extra: "yes" }); },
            });
        `;
    const { stderr } = await runTool(script, {}, '{"t":"d","id":"a","src":"s","v":{}}\n');
    const logs = parseLines(stderr).filter((e) => e.type === 'log');
    expect(logs.length).toBe(1);
    expect(logs[0]!.msg).toBe('hello');
    expect(logs[0]!.level).toBe('warn');
    expect(logs[0]!.extra).toBe('yes');
  });

  test('queue emit + drain triggers queue processor', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({
                input: (v, _s, ctx) => { ctx.emit("double", v); },
                queues: {
                    double: (v, _s, ctx) => {
                        const n = (v as any).x;
                        ctx.output({ result: n * 2 });
                    },
                },
            });
        `;
    const input = [
      '{"t":"d","id":"a","src":"s","v":{"x":3}}',
      '{"t":"d","id":"b","src":"s","v":{"x":5}}',
    ].join('\n');
    const { stdout } = await runTool(script, {}, input);
    const data = parseLines(stdout).filter((e) => e.t === 'd');
    expect(data.map((d) => d.v.result)).toEqual([6, 10]);
  });

  test('onShutdown runs after stdin EOF', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({
                input: (_v, _s, _ctx) => {},
                onShutdown: () => {
                    process.stdout.write(JSON.stringify({t:"m",v:{bye:true}}) + "\\n");
                },
            });
        `;
    const { stdout } = await runTool(script, {}, '{"t":"d","id":"a","src":"s","v":{}}\n');
    const metas = parseLines(stdout).filter((e) => e.t === 'm');
    expect(metas.some((m) => m.v.bye === true)).toBe(true);
  });

  test('blank stdin lines ignored', async () => {
    const script = `
            import { run } from "${FRAMEWORK_DIR}/src/index.ts";
            await run({ input: (v, _s, ctx) => ctx.output(v) });
        `;
    const input = '\n\n' + '{"t":"d","id":"a","src":"s","v":{"x":1}}' + '\n\n';
    const { stdout } = await runTool(script, {}, input);
    const data = parseLines(stdout).filter((e) => e.t === 'd');
    expect(data.length).toBe(1);
  });
});
