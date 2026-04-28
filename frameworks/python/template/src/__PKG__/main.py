"""{{tool_name_kebab}} — {{description}}.

Tool contract:
    - argv[1]: JSON settings (parsed once at startup by the framework)
    - stdin:   NDJSON envelopes (one per line)
    - stdout:  NDJSON envelopes (one per line)
    - stderr:  typed events (trace / log / error / stats)

Replace the TODO in `process_input` with your transformation.
See spec.yaml for the intended input / output / settings contract.
"""

import dpe


def process_input(v, settings, ctx):
    """Called once per input envelope.

    `v` is the payload (env.v); `settings` is the parsed argv[1];
    `ctx` provides output / error / log / trace / stats / emit / memory.
    """
    # TODO: transform v per spec.yaml.
    ctx.output(v)


if __name__ == "__main__":
    dpe.run()
