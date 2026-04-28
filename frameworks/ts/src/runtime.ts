/**
 * Runtime — stdin loop, settings parse, queue drain, processor dispatch.
 *
 * Tools register processors explicitly via `run({input, queues?})`.
 * Unlike Python (which auto-discovers process_* functions), TS is structurally
 * typed so explicit registration keeps types honest.
 */

import { Context, Memory, type QueueItem, type RuntimeLike } from './context';
import type { DataEnvelope, JSONValue } from './envelope';
import { parseEnvelope, writeLog } from './envelope';

export type Processor = (v: JSONValue, settings: JSONValue, ctx: Context) => void | Promise<void>;

export interface RunOptions {
  /** Main input processor — called for each data envelope on stdin. */
  input: Processor;
  /** Optional named queue processors — triggered by ctx.emit(queue, v). */
  queues?: Record<string, Processor>;
  /** Called once before the stdin loop starts (e.g. parse/compile settings). */
  onStart?: (settings: JSONValue) => void | Promise<void>;
  /** Called once after stdin EOF / SIGTERM (e.g. flush, emit final meta). */
  onShutdown?: () => void | Promise<void>;
  /** Max iterations per drain cycle — safety against infinite emit loops. */
  maxDrainIterations?: number;
}

class Runtime implements RuntimeLike {
  settings: JSONValue = {};
  memory = new Memory();
  private input!: Processor;
  private queues: Record<string, Processor> = {};
  private queue: QueueItem[] = [];
  private shutdown = false;
  private drainLimit = 100_000;

  enqueue(item: QueueItem): void {
    this.queue.push(item);
  }

  async drainQueue(): Promise<void> {
    let iterations = 0;
    while (this.queue.length > 0 && iterations < this.drainLimit) {
      const item = this.queue.shift()!;
      const proc = this.queues[item.queue];
      if (!proc) {
        writeLog(`No processor for queue '${item.queue}', dropping item`, 'warn');
        continue;
      }
      const ctx = new Context(item.id, item.src, this.memory, this);
      try {
        await proc(item.v, this.settings, ctx);
      } catch (e) {
        ctx.error(item.v, e);
      }
      iterations++;
    }
    if (iterations >= this.drainLimit) {
      writeLog(`Queue drain hit safety limit (${this.drainLimit})`, 'error');
    }
  }

  async run(opts: RunOptions): Promise<void> {
    this.input = opts.input;
    this.queues = opts.queues ?? {};
    if (opts.maxDrainIterations) this.drainLimit = opts.maxDrainIterations;

    this.settings = parseSettings();

    // SIGTERM / SIGINT → graceful shutdown (stop reading, drain, exit)
    const onSignal = () => {
      this.shutdown = true;
    };
    process.on('SIGTERM', onSignal);
    process.on('SIGINT', onSignal);

    if (opts.onStart) await opts.onStart(this.settings);

    for await (const line of readStdinLines()) {
      if (this.shutdown) break;

      const env = parseEnvelope(line);
      if (env === null) continue;
      if (env.t !== 'd') continue;

      const d = env as DataEnvelope;
      const ctx = new Context(d.id ?? '', d.src ?? '', this.memory, this);
      try {
        await this.input(d.v, this.settings, ctx);
      } catch (e) {
        ctx.error(d.v, e);
      }

      if (this.queue.length > 0) await this.drainQueue();
    }

    if (this.queue.length > 0) await this.drainQueue();
    if (opts.onShutdown) await opts.onShutdown();
  }
}

export async function run(opts: RunOptions): Promise<void> {
  const rt = new Runtime();
  await rt.run(opts);
}

function parseSettings(): JSONValue {
  if (process.argv.length < 3) return {};
  try {
    return JSON.parse(process.argv[2]!) as JSONValue;
  } catch {
    writeLog('Failed to parse settings from argv[1], using empty object', 'warn');
    return {};
  }
}

/** Async iterator over stdin lines (splits on \n, strips \r). */
async function* readStdinLines(): AsyncGenerator<string> {
  let buffer = '';
  const decoder = new TextDecoder('utf-8');
  for await (const chunk of process.stdin as AsyncIterable<Buffer>) {
    buffer += decoder.decode(chunk, { stream: true });
    while (true) {
      const idx = buffer.indexOf('\n');
      if (idx < 0) break;
      let line = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);
      if (line.endsWith('\r')) line = line.slice(0, -1);
      yield line;
    }
  }
  // flush decoder and any residual partial line
  buffer += decoder.decode();
  if (buffer.length > 0) yield buffer;
}
