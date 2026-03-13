//! VPIN (Volume-Synchronized Probability of Informed Trading) toxic flow detector.
//!
//! Easley, López de Prado & O'Hara (2012) — classifies trade flow imbalance
//! using volume buckets to detect periods of elevated informed trading.

use std::collections::VecDeque;

/// Configuration for the VPIN tracker.
#[derive(Debug, Clone)]
pub struct VpinConfig {
    /// Volume per bucket (in shares). When the accumulating bucket reaches this,
    /// it is sealed and pushed to the rolling window.
    pub bucket_volume: f64,
    /// Number of sealed buckets in the rolling window.
    pub n_buckets: usize,
    /// VPIN level above which spread is widened (0.0–1.0).
    pub widen_threshold: f64,
    /// VPIN level above which we pull back to level-0 only (0.0–1.0).
    pub pullback_threshold: f64,
    /// Maximum spread multiplier at VPIN = 1.0.
    pub max_spread_multiplier: f64,
}

impl Default for VpinConfig {
    fn default() -> Self {
        Self {
            bucket_volume: 100.0,
            n_buckets: 50,
            widen_threshold: 0.50,
            pullback_threshold: 0.70,
            max_spread_multiplier: 3.0,
        }
    }
}

/// A sealed volume bucket: total volume and buy-classified volume.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    buy_volume: f64,
    sell_volume: f64,
}

impl Bucket {
    fn total(&self) -> f64 {
        self.buy_volume + self.sell_volume
    }

    fn imbalance(&self) -> f64 {
        (self.buy_volume - self.sell_volume).abs()
    }
}

/// Rolling VPIN tracker that processes fills and computes the VPIN metric.
#[derive(Debug)]
pub struct VpinTracker {
    config: VpinConfig,
    /// Sealed buckets (most recent at back).
    sealed: VecDeque<Bucket>,
    /// Current accumulating bucket.
    current_buy: f64,
    current_sell: f64,
    current_total: f64,
}

impl VpinTracker {
    pub fn new(config: VpinConfig) -> Self {
        Self {
            sealed: VecDeque::with_capacity(config.n_buckets + 1),
            current_buy: 0.0,
            current_sell: 0.0,
            current_total: 0.0,
            config,
        }
    }

    /// Record a trade. `is_buy` uses bulk volume classification (BVC):
    /// the fill side as seen by our bot (buy fill = aggressor buying from us).
    pub fn record_trade(&mut self, volume: f64, is_buy: bool) {
        if volume <= 0.0 {
            return;
        }
        let mut remaining = volume;
        while remaining > 0.0 {
            let space = self.config.bucket_volume - self.current_total;
            let fill = remaining.min(space);
            if is_buy {
                self.current_buy += fill;
            } else {
                self.current_sell += fill;
            }
            self.current_total += fill;
            remaining -= fill;

            // Seal bucket when full
            if self.current_total >= self.config.bucket_volume {
                self.sealed.push_back(Bucket {
                    buy_volume: self.current_buy,
                    sell_volume: self.current_sell,
                });
                // Trim to window size
                while self.sealed.len() > self.config.n_buckets {
                    self.sealed.pop_front();
                }
                self.current_buy = 0.0;
                self.current_sell = 0.0;
                self.current_total = 0.0;
            }
        }
    }

    /// Compute VPIN: rolling mean of |buy-sell|/total across sealed buckets.
    /// Returns `None` if fewer than 2 sealed buckets (insufficient data).
    /// Range: 0.0 (balanced) to 1.0 (completely one-sided).
    pub fn vpin(&self) -> Option<f64> {
        if self.sealed.len() < 2 {
            return None;
        }
        let sum_imbalance: f64 = self.sealed.iter().map(|b| b.imbalance()).sum();
        let sum_total: f64 = self.sealed.iter().map(|b| b.total()).sum();
        if sum_total <= 0.0 {
            return None;
        }
        Some((sum_imbalance / sum_total).clamp(0.0, 1.0))
    }

    /// Spread multiplier: 1.0 below widen_threshold, linearly up to
    /// max_spread_multiplier at VPIN = 1.0.
    pub fn spread_multiplier(&self) -> f64 {
        let v = match self.vpin() {
            Some(v) => v,
            None => return 1.0,
        };
        if v <= self.config.widen_threshold {
            return 1.0;
        }
        let range = 1.0 - self.config.widen_threshold;
        if range <= 0.0 {
            return self.config.max_spread_multiplier;
        }
        let t = (v - self.config.widen_threshold) / range;
        1.0 + t * (self.config.max_spread_multiplier - 1.0)
    }

    /// Size reduction factor: 1.0 below widen_threshold, down to 0.25 at VPIN=1.0.
    pub fn size_factor(&self) -> f64 {
        let v = match self.vpin() {
            Some(v) => v,
            None => return 1.0,
        };
        if v <= self.config.widen_threshold {
            return 1.0;
        }
        let range = 1.0 - self.config.widen_threshold;
        if range <= 0.0 {
            return 0.25;
        }
        let t = (v - self.config.widen_threshold) / range;
        (1.0 - 0.75 * t).max(0.25)
    }

    /// True when VPIN exceeds the pullback threshold (truncate to level-0 only).
    pub fn should_pullback(&self) -> bool {
        self.vpin()
            .map(|v| v > self.config.pullback_threshold)
            .unwrap_or(false)
    }

    /// Reset all state (call on period boundaries).
    pub fn reset(&mut self) {
        self.sealed.clear();
        self.current_buy = 0.0;
        self.current_sell = 0.0;
        self.current_total = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> VpinConfig {
        VpinConfig {
            bucket_volume: 10.0,
            n_buckets: 5,
            widen_threshold: 0.50,
            pullback_threshold: 0.70,
            max_spread_multiplier: 3.0,
        }
    }

    #[test]
    fn balanced_flow_low_vpin() {
        let mut tracker = VpinTracker::new(test_config());
        // Alternate buy/sell in equal volumes — perfectly balanced
        for _ in 0..10 {
            tracker.record_trade(5.0, true);
            tracker.record_trade(5.0, false);
        }
        let v = tracker.vpin().expect("should have enough buckets");
        assert!(v < 0.1, "balanced flow should have low VPIN, got {v}");
        assert!((tracker.spread_multiplier() - 1.0).abs() < 0.01);
        assert!((tracker.size_factor() - 1.0).abs() < 0.01);
        assert!(!tracker.should_pullback());
    }

    #[test]
    fn one_sided_flow_high_vpin() {
        let mut tracker = VpinTracker::new(test_config());
        // All buys — completely toxic flow
        for _ in 0..60 {
            tracker.record_trade(10.0, true);
        }
        let v = tracker.vpin().expect("should have enough buckets");
        assert!(v > 0.9, "one-sided flow should have high VPIN, got {v}");
        assert!(tracker.spread_multiplier() > 2.5);
        assert!(tracker.size_factor() < 0.35);
        assert!(tracker.should_pullback());
    }

    #[test]
    fn multiplier_below_threshold() {
        let mut tracker = VpinTracker::new(test_config());
        // Slightly imbalanced but below threshold
        for _ in 0..10 {
            tracker.record_trade(6.0, true);
            tracker.record_trade(4.0, false);
        }
        let v = tracker.vpin().expect("should have enough buckets");
        // Imbalance per bucket: |6-4|/10 = 0.2 — well below 0.50
        assert!(
            v < 0.50,
            "mild imbalance should be below threshold, got {v}"
        );
        assert!((tracker.spread_multiplier() - 1.0).abs() < 0.01);
        assert!((tracker.size_factor() - 1.0).abs() < 0.01);
    }

    #[test]
    fn reset_clears_state() {
        let mut tracker = VpinTracker::new(test_config());
        for _ in 0..20 {
            tracker.record_trade(10.0, true);
        }
        assert!(tracker.vpin().is_some());
        tracker.reset();
        assert!(tracker.vpin().is_none());
    }

    #[test]
    fn no_data_returns_none() {
        let tracker = VpinTracker::new(test_config());
        assert!(tracker.vpin().is_none());
        assert!((tracker.spread_multiplier() - 1.0).abs() < f64::EPSILON);
        assert!((tracker.size_factor() - 1.0).abs() < f64::EPSILON);
        assert!(!tracker.should_pullback());
    }

    #[test]
    fn partial_bucket_not_counted() {
        let mut tracker = VpinTracker::new(test_config());
        // Record less than one bucket
        tracker.record_trade(5.0, true);
        assert!(tracker.vpin().is_none());
    }
}
