use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::risk::InventoryManager;
use crate::types::AlertMessage;

/// Emergency handler monitors critical conditions and triggers shutdown when needed.
pub struct EmergencyHandler {
    /// Global emergency flag — all modules check this before acting
    pub emergency_flag: Arc<AtomicBool>,
    inventory: Arc<InventoryManager>,
    alert_tx: mpsc::UnboundedSender<AlertMessage>,
    health_check_interval: Duration,
    /// Shared aggregate daily P&L in cents (updated by all per-asset inventories)
    aggregate_daily_pnl_cents: Arc<AtomicI64>,
    /// Daily loss limit in cents (positive value, e.g. 1500 for $15)
    daily_loss_limit_cents: i64,
    /// Session loss limit in cents, measured from process start. None disables it.
    session_loss_limit_cents: Option<i64>,
}

/// Trigger reasons for emergency shutdown
#[derive(Debug)]
pub enum EmergencyTrigger {
    DailyLossLimit,
    SessionLossLimit,
    ProlongedImbalance(String),
    LowUsdc,
    LowMatic,
    StrategyHang,
    CtrlC,
}

impl std::fmt::Display for EmergencyTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmergencyTrigger::DailyLossLimit => write!(f, "Daily loss limit breached"),
            EmergencyTrigger::SessionLossLimit => write!(f, "Session loss limit breached"),
            EmergencyTrigger::ProlongedImbalance(m) => {
                write!(f, "Prolonged imbalance in market {m}")
            }
            EmergencyTrigger::LowUsdc => write!(f, "USDC balance critically low"),
            EmergencyTrigger::LowMatic => write!(f, "MATIC balance too low for gas"),
            EmergencyTrigger::StrategyHang => write!(f, "Strategy engine not responding"),
            EmergencyTrigger::CtrlC => write!(f, "User interrupt (Ctrl+C)"),
        }
    }
}

impl EmergencyHandler {
    pub fn new(
        inventory: Arc<InventoryManager>,
        alert_tx: mpsc::UnboundedSender<AlertMessage>,
        health_check_interval_secs: u64,
        aggregate_daily_pnl_cents: Arc<AtomicI64>,
        daily_loss_limit: Decimal,
        session_loss_limit: Option<Decimal>,
    ) -> Self {
        let limit_cents = (daily_loss_limit * Decimal::from(100))
            .to_i64()
            .unwrap_or(1500); // default $15
        let session_limit_cents = session_loss_limit.and_then(|limit| {
            (limit * Decimal::from(100))
                .to_i64()
                .filter(|value| *value > 0)
        });
        Self {
            emergency_flag: Arc::new(AtomicBool::new(false)),
            inventory,
            alert_tx,
            health_check_interval: Duration::from_secs(health_check_interval_secs),
            aggregate_daily_pnl_cents,
            daily_loss_limit_cents: limit_cents,
            session_loss_limit_cents: session_limit_cents,
        }
    }

    /// Check if emergency flag is set.
    pub fn is_emergency(&self) -> bool {
        self.emergency_flag.load(Ordering::SeqCst)
    }

    /// Set the emergency flag and send alert.
    pub fn trigger_emergency(&self, reason: EmergencyTrigger) {
        if self
            .emergency_flag
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let msg = format!("EMERGENCY SHUTDOWN: {reason}");
            error!("{msg}");
            let _ = self.alert_tx.send(AlertMessage::Emergency(msg));
        }
    }

    /// Clear emergency flag after cooldown.
    pub fn clear_emergency(&self) {
        self.emergency_flag.store(false, Ordering::SeqCst);
        info!("Emergency flag cleared, resuming normal operation");
        let _ = self
            .alert_tx
            .send(AlertMessage::System("Emergency cleared, resuming".into()));
    }

    /// Run the monitoring loop. This should be spawned as a tokio task.
    /// `strategy_heartbeat_rx` receives pings from the strategy engine.
    /// `usdc_balance_fn` is a callback to check the proxy wallet's USDC balance.
    pub async fn run_monitor(
        self: Arc<Self>,
        mut strategy_heartbeat_rx: mpsc::Receiver<()>,
        usdc_balance: Arc<dyn Fn() -> Decimal + Send + Sync>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        shutdown_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) {
        let mut interval = tokio::time::interval(self.health_check_interval);
        let mut strategy_last_seen = tokio::time::Instant::now();
        let cooldown_duration = Duration::from_secs(300); // 5 minutes
                                                          // Use the same 5-minute cooldown for externally-triggered emergencies (canary loss,
                                                          // health check). Previously 60s was too short — a catastrophic canary loss could
                                                          // resume trading before any new period resolved to re-trigger the check.
        let external_cooldown = Duration::from_secs(300);
        // Track the current UTC date for daily P&L counter reset.
        let mut current_date = chrono::Utc::now().date_naive();
        // FIX: Use a baseline snapshot instead of resetting the atomic to 0.
        // The old swap(0) approach had a race: fills arriving after midnight but
        // before the reset tick would have their P&L contribution wiped.
        // With the baseline approach, daily P&L = current_value - baseline,
        // so fills between midnight and the snapshot are correctly attributed.
        let mut day_start_baseline = self.aggregate_daily_pnl_cents.load(Ordering::Relaxed);
        let session_start_baseline = day_start_baseline;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Snapshot the aggregate counter at UTC midnight for the new day's baseline.
                    // We do NOT reset the atomic to 0 — that would race with fills arriving
                    // between midnight and this tick, wiping their contribution.
                    // Instead: today's P&L = atomic.load() - day_start_baseline.
                    let today = chrono::Utc::now().date_naive();
                    if today != current_date {
                        let snapshot = self.aggregate_daily_pnl_cents.load(Ordering::Relaxed);
                        info!(
                            old_date = %current_date,
                            new_date = %today,
                            old_baseline = day_start_baseline,
                            snapshot_at_midnight = snapshot,
                            "Daily P&L baseline updated at UTC midnight"
                        );
                        day_start_baseline = snapshot;
                        current_date = today;
                    }

                    // Handle externally-triggered emergencies (e.g., canary loss, health check).
                    // Instead of skipping forever, enter a cooldown and clear.
                    if self.is_emergency() {
                        info!("Emergency flag set (externally triggered) — entering {}s cooldown",
                            external_cooldown.as_secs());
                        if self.start_cooldown(external_cooldown, &mut shutdown_rx).await {
                            break; // Shutdown requested during cooldown
                        }
                        // Reset heartbeat timer so we don't immediately trigger StrategyHang
                        // after clearing the emergency (orchestrators weren't sending heartbeats
                        // while in emergency mode).
                        strategy_last_seen = tokio::time::Instant::now();
                        continue;
                    }

                    // 1. Daily P&L check — uses shared aggregate across ALL assets
                    let agg_pnl_cents = self.aggregate_daily_pnl_cents.load(Ordering::Relaxed);
                    let today_pnl_cents = agg_pnl_cents - day_start_baseline;
                    let session_pnl_cents = agg_pnl_cents - session_start_baseline;
                    if today_pnl_cents < -self.daily_loss_limit_cents {
                        error!(
                            aggregate_pnl_cents = agg_pnl_cents,
                            today_pnl_cents,
                            day_start_baseline,
                            limit_cents = self.daily_loss_limit_cents,
                            "AGGREGATE daily loss limit breached — PERMANENT shutdown"
                        );
                        self.trigger_emergency(EmergencyTrigger::DailyLossLimit);
                        // DailyLossLimit is PERMANENT — break immediately, no cooldown.
                        // Requires manual restart after investigation.
                        break;
                    }
                    if let Some(limit_cents) = self.session_loss_limit_cents {
                        if session_pnl_cents < -limit_cents {
                            error!(
                                aggregate_pnl_cents = agg_pnl_cents,
                                session_pnl_cents,
                                session_start_baseline,
                                limit_cents,
                                "Session loss limit breached — shutting down this run"
                            );
                            self.trigger_emergency(EmergencyTrigger::SessionLossLimit);
                            break;
                        }
                    }

                    // 2. Check USDC balance
                    let usdc = usdc_balance();
                    if usdc <= Decimal::ZERO {
                        self.trigger_emergency(EmergencyTrigger::LowUsdc);
                        if self.start_cooldown(cooldown_duration, &mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }

                    // MATIC check removed: on-chain ops disabled for proxy wallets,
                    // so gas balance is not critical for CLOB-only trading.

                    // 4. Strategy heartbeat check — if the strategy engine hangs,
                    // trigger emergency and break the loop to force shutdown.
                    // A process manager (systemd, supervisor) should restart the bot.
                    if strategy_last_seen.elapsed() > Duration::from_secs(60) {
                        error!("Strategy engine has not sent heartbeat in 60s — forcing shutdown");
                        self.trigger_emergency(EmergencyTrigger::StrategyHang);
                        // Don't cooldown — break immediately to force process exit.
                        // This is safer than lingering in emergency state with a dead strategy.
                        break;
                    }
                }
                Some(()) = strategy_heartbeat_rx.recv() => {
                    strategy_last_seen = tokio::time::Instant::now();
                }
                _ = shutdown_rx.recv() => {
                    info!("Emergency handler received shutdown signal");
                    self.trigger_emergency(EmergencyTrigger::CtrlC);
                    break;
                }
            }
        }

        // Signal the orchestrator to shut down via the shared flag.
        // This covers StrategyHang and other break-out-of-loop paths.
        if let Some(flag) = &shutdown_flag {
            flag.store(true, Ordering::SeqCst);
            info!("Emergency handler set shutdown flag — orchestrator will exit");
        }
    }

    /// BUG #10 fix: Cooldown that remains responsive to shutdown signals.
    /// Returns true if shutdown was requested during cooldown.
    async fn start_cooldown(
        &self,
        duration: Duration,
        shutdown_rx: &mut tokio::sync::broadcast::Receiver<()>,
    ) -> bool {
        info!("Starting emergency cooldown for {}s", duration.as_secs());
        tokio::select! {
            _ = tokio::time::sleep(duration) => {
                // Cooldown completed — always clear.
                // The orchestrator-level checks (canary, health_check) will re-trigger
                // if the condition persists. Keeping the flag set permanently is worse
                // than briefly clearing and re-evaluating.
                self.clear_emergency();
                false
            }
            _ = shutdown_rx.recv() => {
                info!("Shutdown requested during emergency cooldown");
                self.trigger_emergency(EmergencyTrigger::CtrlC);
                true
            }
        }
    }
}
