/**
 * {{tool_name_kebab}} — {{description}}
 *
 * Tool contract:
 *   - argv[2]: JSON settings (argv[0]=bun, argv[1]=script, argv[2]=settings)
 *             (the framework parses it; you receive `settings` in the processor)
 *   - stdin:   NDJSON envelopes (one per line)
 *   - stdout:  NDJSON envelopes (one per line)
 *   - stderr:  typed events (trace / log / error / stats)
 *
 * Replace the TODO in the `input` processor with your transformation.
 * See spec.yaml for the intended input / output / settings contract.
 */

import { type Context, type JSONValue, run } from '@combycode/dpe-framework-ts';

await run({
  // Rename `_settings` → `settings` once your processor reads tool config.
  input: (v: JSONValue, _settings: JSONValue, ctx: Context) => {
    // TODO: transform v per spec.yaml.
    ctx.output(v);
  },
});
