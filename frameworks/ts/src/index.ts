/**
 * @combycode/dpe-framework-ts — DPE framework for Bun/TypeScript tools.
 *
 * Example:
 *   import { run } from "@combycode/dpe-framework-ts";
 *
 *   await run({
 *     input: (v, settings, ctx) => {
 *       ctx.output({ ok: true });
 *     },
 *   });
 */

export type { QueueItem, RuntimeLike } from './context';
export { Context, Memory } from './context';
export type { DataEnvelope, Envelope, JSONValue, MetaEnvelope } from './envelope';
export {
  computeId,
  hashFile,
  hashString,
  parseEnvelope,
  writeData,
  writeError,
  writeLog,
  writeMeta,
} from './envelope';
export type { Processor, RunOptions } from './runtime';
export { run } from './runtime';
