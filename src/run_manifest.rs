use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Context;
use chrono::Utc;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::config::TradingMode;

#[derive(Debug, Clone, Serialize)]
pub struct AssetRunProfile {
    pub asset: String,
    pub max_position_per_market: String,
    pub base_order_shares: String,
    pub target_combined: String,
    pub max_combined_avg_cost: String,
    pub light_side_max_combined: String,
    pub max_share_imbalance: String,
    pub one_sided_threshold: String,
    pub trading_window_start_pct: f64,
    pub trading_window_end_pct: f64,
    pub allowed_durations: Vec<u32>,
    pub ladder_levels: u32,
    pub ladder_levels_5m: Option<u32>,
    pub ladder_levels_15m: Option<u32>,
    pub ladder_levels_60m: Option<u32>,
    pub buy_level_activation_limit_5m: Option<u32>,
    pub merge_at_closing: bool,
    pub continuous_merge_enabled: bool,
    pub period_gross_buy_cap_usdc: String,
    pub single_order_notional_cap_usdc: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompleteSetRunProfile {
    pub asset: String,
    pub allowed_durations: Vec<u32>,
    pub trading_window_start_pct: f64,
    pub trading_window_end_pct: f64,
    pub poll_interval_ms: u64,
    pub discovery_interval_secs: u64,
    pub long_enabled: bool,
    pub short_enabled: bool,
    pub max_long_combined_ask: String,
    pub min_short_combined_bid: String,
    pub fee_buffer: String,
    pub cooldown_secs: u64,
    pub pairs_per_trade: String,
    pub max_trades_per_period: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunManifest {
    pub run_id: String,
    pub started_at: String,
    pub mode: String,
    pub wallet_address: String,
    pub config_path: String,
    pub config_hash: String,
    pub git_sha: Option<String>,
    pub eoa_mode: bool,
    pub enabled_assets: Vec<String>,
    pub enabled_durations: Vec<u32>,
    pub asset_profiles: Vec<AssetRunProfile>,
    pub strategy_label: Option<String>,
    pub complete_set_profile: Option<CompleteSetRunProfile>,
}

impl RunManifest {
    pub fn build(
        run_id: String,
        mode: TradingMode,
        wallet_address: String,
        config_path: &Path,
        eoa_mode: bool,
        asset_profiles: Vec<AssetRunProfile>,
    ) -> anyhow::Result<Self> {
        let config_bytes = fs::read(config_path).with_context(|| {
            format!(
                "Failed to read config for manifest: {}",
                config_path.display()
            )
        })?;
        let config_hash = sha256_hex(&config_bytes);

        let mut enabled_assets: Vec<String> =
            asset_profiles.iter().map(|p| p.asset.clone()).collect();
        enabled_assets.sort();

        let mut enabled_durations = BTreeSet::new();
        for profile in &asset_profiles {
            for duration in &profile.allowed_durations {
                enabled_durations.insert(*duration);
            }
        }

        Ok(Self {
            run_id,
            started_at: Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            mode: mode.as_str().to_string(),
            wallet_address,
            config_path: config_path.display().to_string(),
            config_hash,
            git_sha: git_sha(),
            eoa_mode,
            enabled_assets,
            enabled_durations: enabled_durations.into_iter().collect(),
            asset_profiles,
            strategy_label: None,
            complete_set_profile: None,
        })
    }

    pub fn build_complete_set(
        run_id: String,
        mode: TradingMode,
        wallet_address: String,
        config_path: &Path,
        eoa_mode: bool,
        profile: CompleteSetRunProfile,
    ) -> anyhow::Result<Self> {
        let config_bytes = fs::read(config_path).with_context(|| {
            format!(
                "Failed to read config for manifest: {}",
                config_path.display()
            )
        })?;
        let config_hash = sha256_hex(&config_bytes);

        Ok(Self {
            run_id,
            started_at: Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            mode: mode.as_str().to_string(),
            wallet_address,
            config_path: config_path.display().to_string(),
            config_hash,
            git_sha: git_sha(),
            eoa_mode,
            enabled_assets: vec![profile.asset.clone()],
            enabled_durations: profile.allowed_durations.clone(),
            asset_profiles: Vec::new(),
            strategy_label: Some("complete_set".to_string()),
            complete_set_profile: Some(profile),
        })
    }

    pub fn persist<P: AsRef<Path>>(&self, logs_dir: P) -> anyhow::Result<PathBuf> {
        let manifest_dir = logs_dir.as_ref().join("manifests");
        fs::create_dir_all(&manifest_dir).with_context(|| {
            format!("Failed to create manifest dir: {}", manifest_dir.display())
        })?;
        let path = manifest_dir.join(format!("{}.json", self.run_id));
        let body = serde_json::to_vec_pretty(self)?;
        fs::write(&path, body)
            .with_context(|| format!("Failed to write run manifest: {}", path.display()))?;
        Ok(path)
    }
}

fn git_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?;
    let sha = sha.trim();
    if sha.is_empty() {
        None
    } else {
        Some(sha.to_string())
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
