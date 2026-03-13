use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Json,
    },
    routing::{get, post},
    Router,
};
use chrono::Utc;
use parking_lot::RwLock;
use serde::Serialize;
use tokio_stream::StreamExt;
use tracing::{error, info};

use crate::dashboard::state::DashboardState;

pub type SharedDashboard = Arc<RwLock<DashboardState>>;

// ── Bot Control State ──

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BotStatus {
    Running,
    Paused,
    Stopping,
}

impl BotStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            BotStatus::Running => "running",
            BotStatus::Paused => "paused",
            BotStatus::Stopping => "stopping",
        }
    }
}

#[derive(Debug)]
pub struct BotControl {
    pub status: BotStatus,
}

impl Default for BotControl {
    fn default() -> Self {
        Self {
            status: BotStatus::Running,
        }
    }
}

pub type SharedBotControl = Arc<RwLock<BotControl>>;

// ── Web State (shared across all handlers) ──

#[derive(Clone)]
struct WebState {
    dashboards: HashMap<String, SharedDashboard>,
    control: SharedBotControl,
    shutdown_flag: Arc<AtomicBool>,
    prometheus_handle: metrics_exporter_prometheus::PrometheusHandle,
    latency_tracker: Arc<crate::latency::LatencyTracker>,
}

const DASHBOARD_HTML: &str = include_str!("dashboard.html");

pub async fn start_web_server(
    dashboards: HashMap<String, SharedDashboard>,
    control: SharedBotControl,
    shutdown_flag: Arc<AtomicBool>,
    port: u16,
    prometheus_handle: metrics_exporter_prometheus::PrometheusHandle,
    latency_tracker: Arc<crate::latency::LatencyTracker>,
) {
    let web_state = WebState {
        dashboards,
        control,
        shutdown_flag,
        prometheus_handle,
        latency_tracker,
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/health", get(health_handler))
        .route("/api/stream", get(sse_handler))
        .route("/api/start", post(start_handler))
        .route("/api/stop", post(stop_handler))
        .route("/api/terminate", post(terminate_handler))
        .route("/api/orders.csv", get(orders_csv_handler))
        .route("/api/latency", get(latency_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(web_state);

    let addr = format!("0.0.0.0:{port}");
    info!("[web] Dashboard available at http://localhost:{port}");
    eprintln!("\n  Dashboard: http://localhost:{port}\n");

    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("[web] Failed to bind on {addr}: {e} — dashboard disabled");
            return;
        }
    };
    if let Err(e) = axum::serve(listener, app).await {
        error!("[web] Server exited with error: {e}");
    }
}

async fn index_handler() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

/// SSE: sends a JSON map of { "BTC": DashboardState, "ETH": DashboardState, ... }
/// every 500ms. Each DashboardState includes its `asset` field.
async fn sse_handler(
    State(state): State<WebState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(
        Duration::from_millis(500),
    ))
    .map(move |_| {
        let mut payload: HashMap<String, DashboardState> = HashMap::new();
        for (name, dash) in &state.dashboards {
            payload.insert(name.clone(), dash.read().clone());
        }
        let json = serde_json::to_string(&payload).unwrap_or_default();
        Ok(Event::default().data(json))
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

async fn start_handler(State(state): State<WebState>) -> impl IntoResponse {
    let mut ctrl = state.control.write();
    info!("[web] START command received (was {:?})", ctrl.status);
    ctrl.status = BotStatus::Running;
    (StatusCode::OK, "ok")
}

async fn stop_handler(State(state): State<WebState>) -> impl IntoResponse {
    let mut ctrl = state.control.write();
    info!("[web] STOP command received (was {:?})", ctrl.status);
    ctrl.status = BotStatus::Stopping;
    (StatusCode::OK, "ok")
}

async fn terminate_handler(State(_state): State<WebState>) -> impl IntoResponse {
    // DISABLED: Unprotected terminate endpoint caused unexpected shutdowns.
    // Use `kill $(pgrep -f polymarket-arb)` from shell instead.
    info!("[web] TERMINATE command received but DISABLED");
    (
        StatusCode::FORBIDDEN,
        "terminate endpoint disabled — use shell kill",
    )
}

async fn orders_csv_handler(State(state): State<WebState>) -> impl IntoResponse {
    // Merge detailed_order_log from all asset dashboards
    let mut all_entries = Vec::new();
    for (name, dash) in &state.dashboards {
        let d = dash.read();
        for entry in &d.detailed_order_log {
            all_entries.push((name.clone(), entry.clone()));
        }
    }
    // Sort by time
    all_entries.sort_by_key(|(_, e)| e.time);

    let mut csv = String::with_capacity(all_entries.len() * 300);

    // Header (added asset column)
    csv.push_str("asset,time,market,condition_id,side,outcome,fill_price,size,status,");
    csv.push_str("btc_price_at_fill,btc_open,fv_up,fv_down,sigma,");
    csv.push_str("bid_yes,bid_no,combined_bid,");
    csv.push_str("best_ask_yes,best_ask_no,best_bid_yes,best_bid_no,");
    csv.push_str("pos_yes_qty,pos_no_qty,complete_pairs,locked_profit,");
    csv.push_str("secs_remaining,total_pnl,today_pnl\n");

    for (asset, e) in &all_entries {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},\
             {},{},{:.6},{:.6},{:.10},\
             {},{},{},\
             {},{},{},{},\
             {},{},{},{},\
             {:.1},{},{}\n",
            asset,
            e.time.to_rfc3339(),
            escape_csv(&e.market),
            e.condition_id,
            e.side,
            e.outcome,
            e.fill_price,
            e.size,
            e.status,
            e.btc_price_at_fill,
            e.btc_open,
            e.fv_up,
            e.fv_down,
            e.sigma,
            e.bid_yes,
            e.bid_no,
            e.combined_bid,
            e.best_ask_yes,
            e.best_ask_no,
            e.best_bid_yes,
            e.best_bid_no,
            e.pos_yes_qty,
            e.pos_no_qty,
            e.complete_pairs,
            e.locked_profit,
            e.secs_remaining,
            e.total_pnl,
            e.today_pnl,
        ));
    }

    (
        [
            (axum::http::header::CONTENT_TYPE, "text/csv"),
            (
                axum::http::header::CONTENT_DISPOSITION,
                "attachment; filename=\"orders.csv\"",
            ),
        ],
        csv,
    )
}

// ── Health Check ──

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    bot_status: String,
    uptime_secs: u64,
    active_markets: u32,
    last_update_secs_ago: f64,
    shutdown_pending: bool,
}

async fn health_handler(State(state): State<WebState>) -> impl IntoResponse {
    let shutdown_pending = state.shutdown_flag.load(Ordering::Relaxed);
    let bot_status = state.control.read().status;

    // Aggregate across all asset dashboards
    let mut max_uptime: u64 = 0;
    let mut total_active_markets: u32 = 0;
    let mut max_staleness_secs: f64 = 0.0;

    let now = Utc::now();
    for dash in state.dashboards.values() {
        let d = dash.read();
        max_uptime = max_uptime.max(d.uptime_secs);
        total_active_markets += d.active_market_count;
        let staleness = (now - d.last_update).num_milliseconds().max(0) as f64 / 1000.0;
        if staleness > max_staleness_secs {
            max_staleness_secs = staleness;
        }
    }

    let is_healthy =
        bot_status == BotStatus::Running && !shutdown_pending && max_staleness_secs < 30.0;
    let is_degraded = bot_status == BotStatus::Paused
        || (max_staleness_secs >= 30.0 && max_staleness_secs < 120.0);

    let status_label = if is_healthy {
        "healthy"
    } else if is_degraded {
        "degraded"
    } else {
        "unhealthy"
    };

    let http_status = if is_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = HealthResponse {
        status: status_label,
        bot_status: bot_status.as_str().to_string(),
        uptime_secs: max_uptime,
        active_markets: total_active_markets,
        last_update_secs_ago: max_staleness_secs,
        shutdown_pending,
    };

    (http_status, Json(body))
}

async fn latency_handler(State(state): State<WebState>) -> impl IntoResponse {
    Json(state.latency_tracker.snapshot())
}

async fn metrics_handler(State(state): State<WebState>) -> impl IntoResponse {
    state.prometheus_handle.render()
}

fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
