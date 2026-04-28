use combycode_dpe::prelude::*;
use combycode_dpe::dpe_run;
use std::sync::OnceLock;

use combycode_dpe_tool_normalize::engine::Engine;
use combycode_dpe_tool_normalize::settings::ToolSettings;

static ENGINE: OnceLock<Option<Engine>> = OnceLock::new();

fn load_engine(settings: &Value) -> Option<&Engine> {
    ENGINE.get_or_init(|| {
        let s: ToolSettings = match serde_json::from_value(settings.clone()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{{\"type\":\"error\",\"error\":\"invalid settings: {}\"}}", e);
                return None;
            }
        };
        match Engine::load(&s) {
            Ok(eng) => Some(eng),
            Err(e) => {
                eprintln!("{{\"type\":\"error\",\"error\":\"engine load: {}\"}}", e);
                None
            }
        }
    }).as_ref()
}

fn process_input(v: Value, settings: &Value, ctx: &mut Context<'_>) {
    let engine = match load_engine(settings) {
        Some(e) => e,
        None => {
            ctx.error(&v, "normalize engine not initialised");
            return;
        }
    };
    engine.apply(v, ctx);
}

fn main() {
    dpe_run! {
        input: process_input,
    };
}
