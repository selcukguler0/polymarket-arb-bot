use std::collections::HashMap;
use std::time::Instant;

use tokio::sync::mpsc;
use tracing::{error, info};

use crate::types::AlertMessage;

/// Telegram alerting service running on a dedicated std::thread.
/// This ensures alerts work even if the tokio runtime is stressed.
pub struct AlertingService {
    bot_token: String,
    chat_id: String,
    max_alerts_per_5min: u32,
}

impl AlertingService {
    pub fn new(bot_token: String, chat_id: String, max_alerts_per_5min: u32) -> Self {
        Self {
            bot_token,
            chat_id,
            max_alerts_per_5min,
        }
    }

    /// Spawn the alerting thread. Returns an unbounded sender for queueing alerts.
    /// Falls back to a noop sender if thread spawn fails.
    pub fn spawn(self) -> mpsc::UnboundedSender<AlertMessage> {
        let (tx, rx) = mpsc::unbounded_channel();

        match std::thread::Builder::new()
            .name("alerting".into())
            .spawn(move || {
                self.run_blocking(rx);
            }) {
            Ok(_) => tx,
            Err(e) => {
                error!("Failed to spawn alerting thread: {e} — alerts disabled");
                // rx is dropped here, tx becomes orphaned; return a fresh noop sender
                noop_alert_sender()
            }
        }
    }

    fn run_blocking(self, mut rx: mpsc::UnboundedReceiver<AlertMessage>) {
        let mut rate_limits: HashMap<String, Vec<Instant>> = HashMap::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create alerting runtime");

        rt.block_on(async {
            while let Some(msg) = rx.recv().await {
                let category = msg.category().to_string();

                // Rate limiting per category
                let now = Instant::now();
                let timestamps = rate_limits.entry(category.clone()).or_default();

                // Remove entries older than 5 minutes
                timestamps.retain(|t| now.duration_since(*t).as_secs() < 300);

                if timestamps.len() >= self.max_alerts_per_5min as usize {
                    continue; // Rate limited
                }

                timestamps.push(now);

                // Send via Telegram
                let text = format!("[{}] {}", msg.category(), msg.text());
                if let Err(e) = self.send_telegram(&text) {
                    error!("Failed to send Telegram alert: {e}");
                }
            }
        });
    }

    fn send_telegram(&self, text: &str) -> std::result::Result<(), String> {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);

        let body = serde_json::json!({
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
        });

        let response = ureq::post(&url)
            .send_json(&body)
            .map_err(|e| format!("HTTP error: {e}"))?;

        let status = response.status();
        if status != 200 {
            return Err(format!("Telegram API returned status {status}"));
        }

        info!("Telegram alert sent: {text}");
        Ok(())
    }
}

/// Create a no-op alert sender for when alerting is disabled.
pub fn noop_alert_sender() -> mpsc::UnboundedSender<AlertMessage> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    // Spawn a drain task that discards all messages
    tokio::spawn(async move {
        while rx.recv().await.is_some() {
            // discard
        }
    });
    tx
}
