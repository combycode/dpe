//! `pipeline-cli monitor <session>` — ratatui TUI for a live session.
//!
//! Tabs:
//!   1. Stages    — per-stage counters (rows, errors, replicas, state)
//!   2. Pipeline  — overall state, duration, gates with progress bars
//!   3. Logs      — tail of `$session/log.ndjson`
//!
//! Data sources (polled every 500ms):
//!   - Live: `send_request(session, Status)` + `Progress` over control socket.
//!     Falls back to reading `journal.json` directly when the control socket
//!     isn't reachable (session already finished / killed).
//!   - Logs: tail of `$session/log.ndjson`.
//!
//! Keys: `q` or Esc — quit; `Tab` / `1`/`2`/`3` — switch tab.

use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tokio::io::AsyncSeekExt;

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table, Tabs},
};
use tokio::io::AsyncReadExt;

use crate::control::{GateProgress, PipelineState, ProgressReport, StageStatus, StatusReport};
use crate::session_proxy::{ControlSocketProxy, ProxyError, SessionProxy};

/// Default poll interval when the runner config can't be loaded — same value
/// as `RuntimeConfig::default().monitor_poll_ms`.
const DEFAULT_POLL_MS: u64 = 500;
const TAB_NAMES: &[&str] = &["Stages (1)", "Pipeline (2)", "Logs (3)"];
const MAX_LOG_LINES: usize = 500;

struct App<P: SessionProxy> {
    proxy: P,
    session_dir: PathBuf,
    tab: usize,
    status: StatusReport,
    progress: ProgressReport,
    status_stale: bool,
    log_lines: Vec<String>,
    log_pos: u64,
    last_error: Option<String>,
}

impl<P: SessionProxy> App<P> {
    fn new(proxy: P, session_dir: PathBuf) -> Self {
        Self {
            proxy,
            session_dir,
            tab: 0,
            status: StatusReport::default(),
            progress: ProgressReport::default(),
            status_stale: true,
            log_lines: Vec::new(),
            log_pos: 0,
            last_error: None,
        }
    }

    async fn poll(&mut self) {
        match self.proxy.status().await {
            Ok(s) => {
                self.status = s;
                self.status_stale = false;
                self.last_error = None;
            }
            Err(ProxyError::Refused(msg)) => {
                self.last_error = Some(msg);
                self.status_stale = true;
                self.rehydrate_from_disk();
            }
            Err(e) => {
                self.last_error = Some(format!("status: {}", e));
                self.status_stale = true;
                self.rehydrate_from_disk();
            }
        }
        if let Ok(p) = self.proxy.progress().await {
            self.progress = p;
        }
        self.tail_logs().await;
    }

    fn rehydrate_from_disk(&mut self) {
        // When the control server isn't answering, prefer journal.json for
        // a snapshot — it tells us the final counters + state.
        let jp = self.session_dir.join("journal.json");
        if let Ok(bytes) = std::fs::read(&jp) {
            if let Ok(j) = serde_json::from_slice::<crate::journal::Journal>(&bytes) {
                self.status.pipeline = j.pipeline.clone();
                self.status.variant  = j.variant.clone();
                self.status.session  = j.session_id.clone();
                self.status.started_at = j.started_at;
                self.status.state = match j.state {
                    crate::journal::JournalState::Running   => PipelineState::Running,
                    crate::journal::JournalState::Succeeded => PipelineState::Stopped,
                    crate::journal::JournalState::Partial   => PipelineState::Failed,
                    crate::journal::JournalState::Failed    => PipelineState::Failed,
                    crate::journal::JournalState::Killed    => PipelineState::Stopped,
                };
                self.status.stages = j.stages.iter().map(|(sid, c)| StageStatus {
                    sid: sid.clone(), tool: String::new(),
                    state: self.status.state,
                    rows: c.rows_out, errors: c.errors, replicas: 1,
                }).collect();
                self.progress.rows_total   = j.totals.envelopes_observed;
                self.progress.errors_total = j.totals.errors;
            }
        }
    }

    async fn tail_logs(&mut self) {
        let path = self.session_dir.join("log.ndjson");
        let Ok(mut f) = tokio::fs::File::open(&path).await else { return };
        let Ok(meta) = f.metadata().await else { return };
        if meta.len() < self.log_pos { self.log_pos = 0; }
        if meta.len() == self.log_pos { return; }
        let _ = f.seek(std::io::SeekFrom::Start(self.log_pos)).await;
        let mut buf = String::new();
        let _ = f.read_to_string(&mut buf).await;
        self.log_pos = meta.len();
        for line in buf.lines() {
            if line.trim().is_empty() { continue; }
            let display = match serde_json::from_str::<serde_json::Value>(line) {
                Ok(v) => format!("[{}] {}: {}",
                    v.get("sid").and_then(|x| x.as_str()).unwrap_or("?"),
                    v.get("level").and_then(|x| x.as_str()).unwrap_or("info"),
                    v.get("msg").and_then(|x| x.as_str()).unwrap_or("")),
                Err(_) => line.to_string(),
            };
            self.log_lines.push(display);
            if self.log_lines.len() > MAX_LOG_LINES {
                self.log_lines.drain(..(self.log_lines.len() - MAX_LOG_LINES));
            }
        }
    }
}

pub fn run(session_dir: PathBuf) -> anyhow::Result<()> {
    let poll_ms = crate::config::load(None).ok()
        .map(|c| c.runtime.effective_monitor_poll_ms())
        .unwrap_or(DEFAULT_POLL_MS);
    let proxy = ControlSocketProxy::new(session_dir.clone());
    run_with(proxy, session_dir, poll_ms)
}

/// Run the TUI driven by an explicit [`SessionProxy`]. Tests inject a
/// `MockSessionProxy` here to exercise the rendering loop without spawning
/// a real runner.
pub fn run_with<P: SessionProxy + 'static>(
    proxy: P,
    session_dir: PathBuf,
    poll_ms: u64,
) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let outcome = rt.block_on(app_loop(proxy, session_dir, &mut terminal, poll_ms));

    disable_raw_mode()?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    outcome
}

async fn app_loop<B, P>(
    proxy: P,
    session_dir: PathBuf,
    terminal: &mut Terminal<B>,
    poll_ms: u64,
) -> anyhow::Result<()>
where
    B: Backend,
    B::Error: Send + Sync + 'static,
    P: SessionProxy,
{
    let mut app = App::new(proxy, session_dir);
    let mut last_poll = Instant::now() - Duration::from_secs(1);

    loop {
        if last_poll.elapsed() >= Duration::from_millis(poll_ms) {
            app.poll().await;
            last_poll = Instant::now();
        }
        terminal.draw(|f| draw(f, &app))?;
        // Non-blocking key polling with small timeout.
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(k) = event::read()? {
                if k.kind != KeyEventKind::Press { continue; }
                match k.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Tab => app.tab = (app.tab + 1) % TAB_NAMES.len(),
                    KeyCode::Char('1') => app.tab = 0,
                    KeyCode::Char('2') => app.tab = 1,
                    KeyCode::Char('3') => app.tab = 2,
                    _ => {}
                }
            }
        }
    }
}

fn draw<P: SessionProxy>(f: &mut Frame<'_>, app: &App<P>) {
    let area = f.area();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),   // title / tabs
            Constraint::Min(0),      // body
            Constraint::Length(1),   // footer
        ]).split(area);

    // Title + tabs
    let titles: Vec<Line<'_>> = TAB_NAMES.iter().map(|t| Line::from(*t)).collect();
    let header_text = format!(
        " {}:{}  session={}  state={}{}",
        app.status.pipeline, app.status.variant, app.status.session,
        fmt_state(app.status.state),
        if app.status_stale { "  (stale — server unreachable)" } else { "" },
    );
    let header = Paragraph::new(header_text)
        .style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD));
    let tabs = Tabs::new(titles)
        .select(app.tab)
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
        .divider("│");
    let top = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(layout[0]);
    f.render_widget(header, top[0]);
    f.render_widget(tabs, top[1]);

    // Body
    match app.tab {
        0 => draw_stages(f, layout[1], app),
        1 => draw_pipeline(f, layout[1], app),
        2 => draw_logs(f, layout[1], app),
        _ => {}
    }

    // Footer
    let footer = Paragraph::new(" q quit • Tab next • 1/2/3 jump tab ")
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, layout[2]);
}

fn fmt_state(s: PipelineState) -> &'static str {
    match s {
        PipelineState::Idle     => "idle",
        PipelineState::Running  => "running",
        PipelineState::Paused   => "paused",
        PipelineState::Stopping => "stopping",
        PipelineState::Stopped  => "stopped",
        PipelineState::Failed   => "failed",
    }
}

fn draw_stages<P: SessionProxy>(f: &mut Frame<'_>, area: Rect, app: &App<P>) {
    let header = Row::new(["stage", "tool", "state", "rows", "errors", "replicas"])
        .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
    let rows: Vec<Row<'_>> = app.status.stages.iter().map(|s| {
        Row::new([
            Cell::from(s.sid.clone()),
            Cell::from(s.tool.clone()),
            Cell::from(fmt_state(s.state)),
            Cell::from(s.rows.to_string()),
            Cell::from(s.errors.to_string())
                .style(if s.errors > 0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default()
                }),
            Cell::from(s.replicas.to_string()),
        ])
    }).collect();
    let table = Table::new(rows, [
        Constraint::Percentage(25),
        Constraint::Percentage(25),
        Constraint::Length(9),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(8),
    ])
    .header(header)
    .block(Block::default().borders(Borders::ALL).title("Stages"));
    f.render_widget(table, area);
}

fn draw_pipeline<P: SessionProxy>(f: &mut Frame<'_>, area: Rect, app: &App<P>) {
    let block = Block::default().borders(Borders::ALL).title("Pipeline");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut constraints = vec![
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
    ];
    for _ in &app.progress.gates { constraints.push(Constraint::Length(2)); }
    constraints.push(Constraint::Min(0));
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(constraints)
        .split(inner);

    let duration_ms = app.status.started_at.saturating_sub(0);
    let now = crate::journal::now_ms();
    let elapsed = if app.status.started_at > 0 { now.saturating_sub(app.status.started_at) } else { duration_ms };
    let text1 = format!("State:    {}", fmt_state(app.status.state));
    let text2 = format!("Elapsed:  {}", fmt_duration(elapsed));
    let text3 = format!("Totals:   rows={}  errors={}",
        app.progress.rows_total, app.progress.errors_total);
    f.render_widget(Paragraph::new(text1), rows[0]);
    f.render_widget(Paragraph::new(text2), rows[1]);
    f.render_widget(Paragraph::new(text3), rows[2]);

    for (i, g) in app.progress.gates.iter().enumerate() {
        let idx = 3 + i;
        if idx >= rows.len() { break; }
        render_gate(f, rows[idx], g);
    }
}

fn render_gate(f: &mut Frame<'_>, area: Rect, g: &GateProgress) {
    // Simple label; if we knew expected total we'd render a Gauge. For now
    // display label + count + met flag.
    let marker = if g.predicate_met { "✓" } else { "…" };
    let label = format!(" {} gate[{}]  count={}", marker, g.name, g.count);
    let style = if g.predicate_met {
        Style::default().fg(Color::Green)
    } else {
        Style::default().fg(Color::Yellow)
    };
    let g_block = Gauge::default()
        .gauge_style(style)
        .ratio(if g.predicate_met { 1.0 } else { 0.0 })
        .label(label);
    f.render_widget(g_block, area);
}

fn draw_logs<P: SessionProxy>(f: &mut Frame<'_>, area: Rect, app: &App<P>) {
    let block = Block::default().borders(Borders::ALL).title("Logs (tail)");
    let inner = block.inner(area);
    f.render_widget(block, area);
    let take = app.log_lines.len().saturating_sub(inner.height as usize);
    let visible: Vec<Line<'_>> = app.log_lines[take..].iter()
        .map(|l| Line::from(l.as_str())).collect();
    let para = Paragraph::new(visible);
    f.render_widget(para, inner);
}

fn fmt_duration(ms: u64) -> String {
    let secs = ms / 1000;
    if secs < 60 { return format!("{}.{:03}s", secs, ms % 1000); }
    let mins = secs / 60;
    let secs = secs % 60;
    if mins < 60 { return format!("{}m{}s", mins, secs); }
    let hrs = mins / 60;
    let mins = mins % 60;
    format!("{}h{}m", hrs, mins)
}
