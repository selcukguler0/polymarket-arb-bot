use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use parking_lot::RwLock;
use serde::Serialize;

const MAX_SAMPLES: usize = 500;

pub struct LatencyTracker {
    ops: RwLock<HashMap<&'static str, OpWindow>>,
    boot: Instant,
}

struct OpWindow {
    samples: VecDeque<f64>,
    total_count: u64,
}

#[derive(Serialize)]
pub struct LatencyStats {
    pub avg_ms: f64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub count: u64,
}

#[derive(Serialize)]
pub struct LatencySnapshot {
    pub uptime_secs: u64,
    pub operations: HashMap<String, LatencyStats>,
}

impl LatencyTracker {
    pub fn new() -> Self {
        Self {
            ops: RwLock::new(HashMap::new()),
            boot: Instant::now(),
        }
    }

    pub fn record(&self, op: &'static str, elapsed_ms: f64) {
        let mut map = self.ops.write();
        let window = map.entry(op).or_insert_with(|| OpWindow {
            samples: VecDeque::with_capacity(MAX_SAMPLES),
            total_count: 0,
        });
        if window.samples.len() >= MAX_SAMPLES {
            window.samples.pop_front();
        }
        window.samples.push_back(elapsed_ms);
        window.total_count += 1;
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let map = self.ops.read();
        let mut operations = HashMap::new();
        for (&op, window) in map.iter() {
            if window.samples.is_empty() {
                continue;
            }
            let mut sorted: Vec<f64> = window.samples.iter().copied().collect();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let n = sorted.len();
            let sum: f64 = sorted.iter().sum();
            let p50_idx = (n - 1) / 2;
            let p99_idx = ((n as f64 * 0.99) as usize).min(n - 1);
            operations.insert(
                op.to_string(),
                LatencyStats {
                    avg_ms: sum / n as f64,
                    p50_ms: sorted[p50_idx],
                    p99_ms: sorted[p99_idx],
                    min_ms: sorted[0],
                    max_ms: sorted[n - 1],
                    count: window.total_count,
                },
            );
        }
        LatencySnapshot {
            uptime_secs: self.boot.elapsed().as_secs(),
            operations,
        }
    }
}
