use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use alloy::primitives::{B256, U256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;
use parking_lot::Mutex;
use polymarket_client_sdk::clob::types::request::CancelMarketOrderRequest;
use polymarket_client_sdk::clob::types::response::CancelOrdersResponse;
use polymarket_client_sdk::clob::types::{Amount, OrderType, Side, SignatureType};
// WS response types used directly by orchestrator via sdk.ws.subscribe_*()
use polymarket_client_sdk::clob::{self, Config as ClobConfig};
use polymarket_client_sdk::ctf;
use polymarket_client_sdk::ctf::types::{
    MergePositionsRequest, RedeemPositionsRequest, SplitPositionRequest,
};
use polymarket_client_sdk::data;
use polymarket_client_sdk::gamma;
use polymarket_client_sdk::gamma::types::request::EventsRequest;
use polymarket_client_sdk::gamma::types::response::Event;
use polymarket_client_sdk::types::Address;
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{info, warn};

use crate::error::{BotError, Result};

// ── Constants ──

const CLOB_HOST: &str = "https://clob.polymarket.com";

/// Maximum retries for SDK API calls (FIX 9).
const MAX_RETRIES: u32 = 3;
/// Base delay for exponential backoff.
const RETRY_BASE_DELAY_MS: u64 = 500;
/// Exchange lot precision is 2 decimal places for share size.
const ORDER_SIZE_DECIMALS: u32 = 2;
/// Polymarket minimum order size in shares.
const MIN_ORDER_SHARES: Decimal = dec!(5); // Polymarket CLOB minimum is 5 shares
/// Minimum notional for marketable buys to avoid deterministic 400s.
const MIN_MARKETABLE_BUY_NOTIONAL: Decimal = dec!(1);
/// Cooldown after deterministic placement failures per token+side flow.
const DETERMINISTIC_ORDER_BACKOFF_SECS: u64 = 8;
/// Timeout for HTTP order submission / cancellation calls.
/// Without this, a hung CLOB endpoint can block the entire quote cycle for minutes
/// (reqwest has no default timeout). 5s is generous — p99 should be <1s.
const HTTP_SUBMIT_TIMEOUT: Duration = Duration::from_secs(5);

// ── Type aliases for authenticated client states ──
/// CLOB client uses Builder auth for order attribution + weekly USDC rewards.
type BuilderAuthState = polymarket_client_sdk::auth::state::Authenticated<
    polymarket_client_sdk::auth::builder::Builder,
>;
/// WS client uses Normal auth (no builder headers needed for subscriptions).
type WsAuthState =
    polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>;

// ── Authenticated SDK Client Wrapper ──

/// Callback type for the SDK-level sell position guard.
/// Takes (token_id, requested_size) and returns the available-to-sell quantity.
/// The SDK clamps the sell size to min(requested, available) and rejects if below minimum.
pub type SellPositionGuard = Arc<dyn Fn(&str, Decimal) -> Decimal + Send + Sync>;

#[derive(Debug, Clone)]
pub struct MarketOrderSpec {
    pub token_id: String,
    pub side: Side,
    pub shares: Decimal,
}

#[derive(Debug, Clone)]
pub struct MarketOrderResult {
    pub index: usize,
    pub token_id: String,
    pub side: Side,
    pub shares: Decimal,
    pub success: bool,
    pub order_id: Option<String>,
    pub error_msg: Option<String>,
}

/// Wraps all authenticated SDK clients for the bot.
/// Created once at startup, passed by `Arc` to modules that need it.
pub struct SdkClients {
    /// Authenticated CLOB client (Builder mode) for order placement / cancellation.
    pub clob: clob::Client<BuilderAuthState>,
    /// The signer needed for `client.sign(&signer, order)`.
    pub signer: PrivateKeySigner,
    /// WebSocket client (Normal auth) for orderbook + user events.
    pub ws: polymarket_client_sdk::clob::ws::Client<WsAuthState>,
    /// Gamma client for market discovery.
    pub gamma: gamma::Client,
    /// Data client for position reconciliation.
    pub data: data::Client,
    /// Wallet address.
    pub wallet_address: Address,
    /// USDC contract address (configurable for USDC.e -> native USDC migration).
    pub usdc_address: Address,
    /// Short-lived backoff by order flow key (`kind:token_id`) after deterministic failures.
    order_backoff_until: Mutex<HashMap<String, (Instant, String)>>,
    /// SDK-level sell position guard — last line of defense against naked sells.
    /// When set, every sell order must pass this check before submission to CLOB.
    /// Returns the max sellable quantity for a given token_id.
    sell_position_guard: parking_lot::RwLock<Option<SellPositionGuard>>,
    /// Mutex to serialize merge_positions() calls across concurrent orchestrators.
    /// Without this, concurrent merges from different markets race on the wallet nonce,
    /// causing nonce-too-low failures and wasted gas.
    merge_lock: tokio::sync::Mutex<()>,
    /// Mutex to serialize redeem_all_redeemable() calls across concurrent orchestrators.
    /// Without this, multiple asset orchestrators resolving simultaneously could race
    /// on the global wallet-wide sweep, wasting gas and creating nonce conflicts.
    redeem_lock: tokio::sync::Mutex<()>,
    /// Optional relayer client for gasless on-chain transactions via Gnosis Safe.
    /// When set, merge/redeem/split operations are routed through the relayer
    /// (Polymarket pays gas) instead of direct RPC (EOA pays POL).
    pub relayer: Option<crate::relayer::RelayerClient>,
}

impl SdkClients {
    /// Create and authenticate all SDK clients.
    ///
    /// When `eoa_mode` is true, uses `SignatureType::Eoa` (type 0) so the EOA
    /// is both signer and fund holder, enabling on-chain merge/split/redeem.
    /// When false, uses `SignatureType::GnosisSafe` (type 2) with CREATE2-derived
    /// Safe wallet — funds held in Safe, on-chain ops routed through relayer (gasless).
    pub async fn new(
        private_key: &str,
        wallet_address: &str,
        eoa_mode: bool,
        usdc_address: &str,
        builder_key: &str,
        builder_secret: &str,
        builder_passphrase: &str,
    ) -> Result<Self> {
        // Parse signer
        let signer = PrivateKeySigner::from_str(private_key)
            .map_err(|e| BotError::Sdk(format!("Invalid private key: {e}")))?
            .with_chain_id(Some(POLYGON));

        let wallet_addr = Address::from_str(wallet_address)
            .map_err(|e| BotError::Sdk(format!("Invalid wallet address: {e}")))?;

        let usdc_addr = Address::from_str(usdc_address)
            .map_err(|e| BotError::Sdk(format!("Invalid USDC address: {e}")))?;

        let builder_uuid = polymarket_client_sdk::auth::Uuid::parse_str(builder_key)
            .map_err(|e| BotError::Sdk(format!("Invalid POLY_BUILDER_KEY (must be UUID): {e}")))?;
        let sig_type = if eoa_mode {
            SignatureType::Eoa
        } else {
            SignatureType::GnosisSafe
        };

        // Full auth + builder promotion with retry.
        // promote_to_builder consumes the CLOB client, so on failure we must
        // re-authenticate from scratch. The SDK's internal sync lock can race
        // if a previous session wasn't fully torn down.
        let mut last_err = String::new();
        let mut attempt = 0u32;
        let (clob, ws) = loop {
            attempt += 1;
            info!(attempt, "Authenticating CLOB client...");
            let clob_normal = clob::Client::new(
                CLOB_HOST,
                ClobConfig::builder()
                    .heartbeat_interval(Duration::from_secs(10))
                    .build(),
            )
            .map_err(|e| BotError::Sdk(format!("Failed to create CLOB client: {e}")))?
            .authentication_builder(&signer)
            .signature_type(sig_type)
            .authenticate()
            .await
            .map_err(|e| BotError::Sdk(format!("CLOB authentication failed: {e}")))?;

            info!("CLOB client authenticated");

            // Clone credentials for WS BEFORE promoting (promotion consumes the client)
            let credentials = clob_normal.credentials().clone();
            let ws_unauth = polymarket_client_sdk::clob::ws::Client::default();
            let ws = ws_unauth
                .authenticate(credentials, wallet_addr)
                .map_err(|e| BotError::Sdk(format!("WS authentication failed: {e}")))?;

            info!("WebSocket client authenticated");

            // Promote to Builder for order attribution + weekly USDC rewards
            let builder_creds = polymarket_client_sdk::auth::Credentials::new(
                builder_uuid,
                builder_secret.to_string(),
                builder_passphrase.to_string(),
            );
            let builder_config = polymarket_client_sdk::auth::builder::Config::local(builder_creds);
            match clob_normal.promote_to_builder(builder_config).await {
                Ok(promoted) => {
                    info!("CLOB client promoted to Builder mode");
                    break (promoted, ws);
                }
                Err(e) => {
                    last_err = format!("{e}");
                    if attempt >= 3 {
                        return Err(BotError::Sdk(format!(
                            "Builder promotion failed after {attempt} attempts: {last_err}"
                        )));
                    }
                    warn!(
                        attempt,
                        error = %e,
                        "Builder promotion failed, re-authenticating after delay..."
                    );
                    tokio::time::sleep(Duration::from_secs(2 * attempt as u64)).await;
                }
            }
        };

        // Gamma client (no auth needed)
        let gamma_client = gamma::Client::default();

        // Data client (no auth needed)
        let data_client = data::Client::default();

        Ok(Self {
            clob,
            signer,
            ws,
            gamma: gamma_client,
            data: data_client,
            wallet_address: wallet_addr,
            usdc_address: usdc_addr,
            order_backoff_until: Mutex::new(HashMap::new()),
            sell_position_guard: parking_lot::RwLock::new(None),
            merge_lock: tokio::sync::Mutex::new(()),
            redeem_lock: tokio::sync::Mutex::new(()),
            relayer: None,
        })
    }

    /// Attach a relayer client for gasless on-chain operations.
    ///
    /// When set, `merge_positions`, `redeem_positions`, and `split_position`
    /// will route through Polymarket's relayer (gas-free) instead of direct RPC.
    pub fn set_relayer(&mut self, relayer: crate::relayer::RelayerClient) {
        info!(
            safe = %relayer.safe_address(),
            "Relayer attached — on-chain ops will be gasless"
        );
        self.relayer = Some(relayer);
    }

    /// Check if a relayer is configured for gasless operations.
    pub fn has_relayer(&self) -> bool {
        self.relayer.is_some()
    }

    fn flow_key(kind: &str, token_id: &str) -> String {
        format!("{kind}:{token_id}")
    }

    fn backoff_if_active(&self, kind: &str, token_id: &str) -> Option<String> {
        let key = Self::flow_key(kind, token_id);
        let now = Instant::now();
        let mut guard = self.order_backoff_until.lock();
        if let Some((until, reason)) = guard.get(&key) {
            if now < *until {
                let secs_left = until.saturating_duration_since(now).as_secs();
                return Some(format!(
                    "Suppressed {kind} for token {token_id} ({}s left): {reason}",
                    secs_left.max(1)
                ));
            }
            guard.remove(&key);
        }
        None
    }

    fn maybe_set_backoff(&self, kind: &str, token_id: &str, msg: &str) {
        if !is_deterministic_order_error(msg) {
            return;
        }
        let key = Self::flow_key(kind, token_id);
        self.order_backoff_until.lock().insert(
            key,
            (
                Instant::now() + Duration::from_secs(DETERMINISTIC_ORDER_BACKOFF_SECS),
                msg.to_string(),
            ),
        );
    }

    // ── Sell Position Guard ──

    /// Set the sell position guard callback. Must be called after InventoryManager is initialized.
    /// The guard takes (token_id, requested_size) and returns the available-to-sell quantity.
    /// Once set, ALL sell orders are validated against this guard before CLOB submission.
    pub fn set_sell_position_guard(&self, guard: SellPositionGuard) {
        *self.sell_position_guard.write() = Some(guard);
    }

    /// Validate a sell order against the position guard.
    /// Returns the clamped size (min of requested and available), or Err if:
    /// - Guard is set and available < MIN_ORDER_SHARES
    /// - Guard is set and available is zero
    /// If no guard is set, logs a warning and allows the sell (backwards compat during init).
    fn validate_sell_position(&self, token_id: &str, size: Decimal) -> Result<Decimal> {
        let guard = self.sell_position_guard.read();
        match guard.as_ref() {
            Some(check_fn) => {
                let available = check_fn(token_id, size);
                if available <= Decimal::ZERO {
                    let msg = format!(
                        "BLOCKED NAKED SELL: tried to sell {size} of token {token_id} but hold 0"
                    );
                    warn!("{}", msg);
                    return Err(BotError::Order(msg));
                }
                let clamped = size.min(available);
                if clamped < MIN_ORDER_SHARES {
                    let msg = format!(
                        "BLOCKED SELL: clamped size {clamped} (available {available}) below minimum {MIN_ORDER_SHARES} for token {token_id}"
                    );
                    warn!("{}", msg);
                    return Err(BotError::Order(msg));
                }
                if clamped < size {
                    warn!(
                        token_id = %token_id,
                        requested = %size,
                        available = %available,
                        clamped = %clamped,
                        "Sell size clamped to available position"
                    );
                }
                Ok(clamped)
            }
            None => {
                // Guard not yet set (startup race). Allow but warn loudly.
                warn!(
                    token_id = %token_id,
                    size = %size,
                    "Sell position guard not set — allowing sell without validation"
                );
                Ok(size)
            }
        }
    }

    // ── Order Placement (FIX 2 + FIX 3: postOnly enforced) ──

    /// Place a GTC limit buy order with `postOnly = true`.
    /// Returns the order ID from the CLOB on success.
    pub async fn place_limit_order(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
        expiration: Option<DateTime<Utc>>,
    ) -> Result<String> {
        if let Some(msg) = self.backoff_if_active("buy_gtc", token_id) {
            return Err(BotError::Order(msg));
        }

        let size = quantize_order_size(size);
        if size < MIN_ORDER_SHARES {
            let msg = format!(
                "Order skipped: size {} is below minimum {} after 2dp quantization",
                size, MIN_ORDER_SHARES
            );
            self.maybe_set_backoff("buy_gtc", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        // FIX 19: Round price to tick_size before submission
        let rounded_price = round_to_tick(price, tick_size);
        if rounded_price <= Decimal::ZERO {
            let msg = format!("Order skipped: non-positive rounded price {rounded_price}");
            self.maybe_set_backoff("buy_gtc", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        let token = U256::from_str(token_id)
            .map_err(|e| BotError::Order(format!("Invalid token_id: {e}")))?;

        // Build the order — FIX 3: postOnly(true) enforced
        let mut builder = self
            .clob
            .limit_order()
            .token_id(token.clone())
            .side(Side::Buy)
            .price(rounded_price)
            .size(size)
            .post_only(true);
        builder = if let Some(exp) = expiration {
            builder.order_type(OrderType::GTD).expiration(exp)
        } else {
            builder.order_type(OrderType::GTC)
        };
        let signable = match builder.build().await {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Order build failed: {e}");
                self.maybe_set_backoff("buy_gtc", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        // Sign the order
        let signed = match self.clob.sign(&self.signer, signable).await {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Order signing failed: {e}");
                self.maybe_set_backoff("buy_gtc", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        // Post order with timeout — prevents hung CLOB endpoint from blocking the quote cycle.
        // No retry: SignedOrder is not Clone, and signing is non-trivial to redo.
        let post_result =
            tokio::time::timeout(HTTP_SUBMIT_TIMEOUT, self.clob.post_order(signed)).await;

        let mut response = match post_result {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                let err_str = format!("{e}");
                let is_ambiguous = err_str.contains("timeout")
                    || err_str.contains("connection")
                    || err_str.contains("hyper");

                if is_ambiguous {
                    // FIX 21: Ambiguous error — order may have been created.
                    // Query open orders to check. Only reconcile if EXACTLY one match
                    // exists to avoid false-matching another identical ladder order.
                    warn!(
                        token_id,
                        "[sdk] Ambiguous post_order error ({err_str}) — checking open orders"
                    );
                    if let Ok(orders) = self.get_open_orders_for_token(token_id).await {
                        let now = Utc::now();
                        let matches: Vec<_> = orders
                            .iter()
                            .filter(|order| {
                                let age_secs = (now - order.created_at).num_seconds();
                                age_secs < 5 // tighter window (was 10s)
                                    && order.price == rounded_price
                                    && order.original_size == size
                                    && matches!(order.side, Side::Buy)
                            })
                            .collect();
                        if matches.len() == 1 {
                            let order = matches[0];
                            warn!(
                                order_id = %order.id,
                                "[sdk] Found exactly 1 matching order after ambiguous error — reconciled"
                            );
                            return Ok(order.id.clone());
                        } else if matches.len() > 1 {
                            warn!(
                                count = matches.len(),
                                "[sdk] Multiple matching orders found — cannot disambiguate, treating as failed"
                            );
                        }
                    }
                }

                let msg = format!("Post order failed: {e}");
                self.maybe_set_backoff("buy_gtc", token_id, &msg);
                return Err(BotError::Order(msg));
            }
            Err(_elapsed) => {
                // Timeout — order may have been created (ambiguous).
                warn!(
                    token_id,
                    timeout_ms = HTTP_SUBMIT_TIMEOUT.as_millis() as u64,
                    "[sdk] post_order timed out — checking open orders for reconciliation"
                );
                if let Ok(orders) = self.get_open_orders_for_token(token_id).await {
                    let now = Utc::now();
                    let matches: Vec<_> = orders
                        .iter()
                        .filter(|order| {
                            let age_secs = (now - order.created_at).num_seconds();
                            age_secs < 10
                                && order.price == rounded_price
                                && order.original_size == size
                                && matches!(order.side, Side::Buy)
                        })
                        .collect();
                    if matches.len() == 1 {
                        let order = matches[0];
                        warn!(
                            order_id = %order.id,
                            "[sdk] Found matching order after timeout — reconciled"
                        );
                        return Ok(order.id.clone());
                    }
                }
                let msg = format!(
                    "Post order timed out after {}ms",
                    HTTP_SUBMIT_TIMEOUT.as_millis()
                );
                self.maybe_set_backoff("buy_gtc", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        // Check for API-level errors (use `success` flag as primary check)
        if !response.success {
            let err_msg = response.error_msg.as_deref().unwrap_or("unknown error");
            // One reprice attempt for post-only crosses-book errors.
            if is_crosses_book_error(err_msg) && tick_size > Decimal::ZERO {
                let retry_price =
                    round_to_tick((rounded_price - tick_size).max(Decimal::ZERO), tick_size);
                if retry_price > Decimal::ZERO && retry_price < rounded_price {
                    let mut retry_builder = self
                        .clob
                        .limit_order()
                        .token_id(token.clone())
                        .side(Side::Buy)
                        .price(retry_price)
                        .size(size)
                        .post_only(true);
                    retry_builder = if let Some(exp) = expiration {
                        retry_builder.order_type(OrderType::GTD).expiration(exp)
                    } else {
                        retry_builder.order_type(OrderType::GTC)
                    };
                    let retry_signable = match retry_builder.build().await {
                        Ok(v) => v,
                        Err(e) => {
                            let msg = format!("Order retry build failed: {e}");
                            self.maybe_set_backoff("buy_gtc", token_id, &msg);
                            return Err(BotError::Order(msg));
                        }
                    };

                    let retry_signed = match self.clob.sign(&self.signer, retry_signable).await {
                        Ok(v) => v,
                        Err(e) => {
                            let msg = format!("Order retry signing failed: {e}");
                            self.maybe_set_backoff("buy_gtc", token_id, &msg);
                            return Err(BotError::Order(msg));
                        }
                    };

                    response = match tokio::time::timeout(
                        HTTP_SUBMIT_TIMEOUT,
                        self.clob.post_order(retry_signed),
                    )
                    .await
                    {
                        Ok(Ok(v)) => v,
                        Ok(Err(e)) => {
                            let msg = format!("Order retry post failed: {e}");
                            self.maybe_set_backoff("buy_gtc", token_id, &msg);
                            return Err(BotError::Order(msg));
                        }
                        Err(_) => {
                            let msg = format!(
                                "Order retry post timed out after {}ms",
                                HTTP_SUBMIT_TIMEOUT.as_millis()
                            );
                            self.maybe_set_backoff("buy_gtc", token_id, &msg);
                            return Err(BotError::Order(msg));
                        }
                    };

                    if !response.success {
                        let retry_err = response.error_msg.as_deref().unwrap_or("unknown error");
                        let msg = format!("Order rejected after reprice: {retry_err}");
                        self.maybe_set_backoff("buy_gtc", token_id, &msg);
                        return Err(BotError::Order(msg));
                    }
                } else {
                    let msg = format!("Order rejected: {err_msg}");
                    self.maybe_set_backoff("buy_gtc", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
            } else {
                let msg = format!("Order rejected: {err_msg}");
                self.maybe_set_backoff("buy_gtc", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        }

        // Return the real CLOB order ID (BUG #1 fix — was previously a synthetic ID)
        if response.order_id.is_empty() {
            let msg = "CLOB returned success but empty order_id".to_string();
            self.maybe_set_backoff("buy_gtc", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        Ok(response.order_id)
    }

    /// Place paired GTC limit buy orders atomically via the batch API (BUG #5 fix).
    /// If one order succeeds and the other fails, the successful order is cancelled
    /// to prevent one-sided exposure. Returns the order IDs of all successful orders.
    pub async fn place_paired_orders(
        &self,
        orders: Vec<(&str, Decimal, Decimal, Decimal)>, // (token_id, price, size, tick_size)
        expiration: Option<DateTime<Utc>>,
    ) -> Result<Vec<String>> {
        // Build and sign all orders
        let mut signed_orders = Vec::new();
        for (token_id, price, size, tick_size) in &orders {
            if let Some(msg) = self.backoff_if_active("buy_gtc_batch", token_id) {
                return Err(BotError::Order(msg));
            }

            let size = quantize_order_size(*size);
            if size < MIN_ORDER_SHARES {
                let msg = format!(
                    "Batch order skipped for token {token_id}: size {} is below minimum {}",
                    size, MIN_ORDER_SHARES
                );
                self.maybe_set_backoff("buy_gtc_batch", token_id, &msg);
                return Err(BotError::Order(msg));
            }

            let rounded_price = round_to_tick(*price, *tick_size);
            if rounded_price <= Decimal::ZERO {
                let msg =
                    format!("Batch order skipped for token {token_id}: non-positive rounded price");
                self.maybe_set_backoff("buy_gtc_batch", token_id, &msg);
                return Err(BotError::Order(msg));
            }
            let token = U256::from_str(token_id)
                .map_err(|e| BotError::Order(format!("Invalid token_id: {e}")))?;

            let mut builder = self
                .clob
                .limit_order()
                .token_id(token)
                .side(Side::Buy)
                .price(rounded_price)
                .size(size)
                .post_only(true);
            builder = if let Some(exp) = expiration {
                builder.order_type(OrderType::GTD).expiration(exp)
            } else {
                builder.order_type(OrderType::GTC)
            };
            let signable = match builder.build().await {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!("Batch order build failed: {e}");
                    self.maybe_set_backoff("buy_gtc_batch", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
            };

            let signed = match self.clob.sign(&self.signer, signable).await {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!("Batch order sign failed: {e}");
                    self.maybe_set_backoff("buy_gtc_batch", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
            };

            signed_orders.push(signed);
        }

        // Submit all at once via batch API (with timeout)
        let responses =
            match tokio::time::timeout(HTTP_SUBMIT_TIMEOUT, self.clob.post_orders(signed_orders))
                .await
            {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    let msg = format!("Batch post_orders failed: {e}");
                    for (token_id, _, _, _) in &orders {
                        self.maybe_set_backoff("buy_gtc_batch", token_id, &msg);
                    }
                    return Err(BotError::Order(msg));
                }
                Err(_) => {
                    let msg = format!(
                        "Batch post_orders timed out after {}ms",
                        HTTP_SUBMIT_TIMEOUT.as_millis()
                    );
                    for (token_id, _, _, _) in &orders {
                        self.maybe_set_backoff("buy_gtc_batch", token_id, &msg);
                    }
                    return Err(BotError::Order(msg));
                }
            };

        // Separate successes and failures
        let mut success_ids = Vec::new();
        let mut any_failed = false;

        for resp in &responses {
            if resp.success && !resp.order_id.is_empty() {
                success_ids.push(resp.order_id.clone());
            } else {
                any_failed = true;
            }
        }

        // If partial success: cancel successful orders to avoid one-sided exposure
        if any_failed && !success_ids.is_empty() {
            warn!(
                succeeded = success_ids.len(),
                total = responses.len(),
                "Batch order partial failure — cancelling successful orders to avoid one-sided exposure"
            );
            for oid in &success_ids {
                if let Err(e) = self.cancel_order(oid).await {
                    warn!(order_id = %oid, "Failed to cancel partial-success order: {e}");
                }
            }
            let err_msgs: Vec<String> = responses
                .iter()
                .filter(|r| !r.success)
                .map(|r| r.error_msg.clone().unwrap_or_else(|| "unknown".into()))
                .collect();
            let msg = format!(
                "Batch order partially failed (cancelled {} successful): {}",
                success_ids.len(),
                err_msgs.join("; ")
            );
            for (token_id, _, _, _) in &orders {
                self.maybe_set_backoff("buy_gtc_batch", token_id, &msg);
            }
            return Err(BotError::Order(msg));
        }

        if any_failed {
            let err_msgs: Vec<String> = responses
                .iter()
                .filter(|r| !r.success)
                .map(|r| r.error_msg.clone().unwrap_or_else(|| "unknown".into()))
                .collect();
            let msg = format!("All orders failed: {}", err_msgs.join("; "));
            for (token_id, _, _, _) in &orders {
                self.maybe_set_backoff("buy_gtc_batch", token_id, &msg);
            }
            return Err(BotError::Order(msg));
        }

        Ok(success_ids)
    }

    /// Place a batch of limit buy orders via the batch API, keeping successful orders
    /// on partial failure (unlike `place_paired_orders` which cancels all on partial failure).
    /// Returns a Vec of (original_index, order_id) for successfully placed orders.
    /// Logs warnings for any that fail but does NOT roll back the successful ones.
    pub async fn place_batch_orders(
        &self,
        orders: Vec<(&str, Decimal, Decimal, Decimal)>, // (token_id, price, size, tick_size)
        expiration: Option<DateTime<Utc>>,
    ) -> Result<Vec<(usize, String)>> {
        if orders.is_empty() {
            return Ok(Vec::new());
        }

        // Build and sign all orders
        let mut signed_orders = Vec::new();
        let mut valid_indices = Vec::new(); // maps signed_orders index → original orders index

        for (i, (token_id, price, size, tick_size)) in orders.iter().enumerate() {
            if let Some(msg) = self.backoff_if_active("buy_batch", token_id) {
                warn!("[sdk] Batch order {i} skipped (backoff): {msg}");
                continue;
            }

            let size = quantize_order_size(*size);
            if size < MIN_ORDER_SHARES {
                warn!(
                    "[sdk] Batch order {i} skipped: size {} below minimum {}",
                    size, MIN_ORDER_SHARES
                );
                continue;
            }

            let rounded_price = round_to_tick(*price, *tick_size);
            if rounded_price <= Decimal::ZERO {
                warn!("[sdk] Batch order {i} skipped: non-positive rounded price");
                continue;
            }

            let token = match U256::from_str(token_id) {
                Ok(v) => v,
                Err(e) => {
                    warn!("[sdk] Batch order {i} skipped: invalid token_id: {e}");
                    continue;
                }
            };

            let mut builder = self
                .clob
                .limit_order()
                .token_id(token)
                .side(Side::Buy)
                .price(rounded_price)
                .size(size)
                .post_only(true);
            builder = if let Some(exp) = expiration {
                builder.order_type(OrderType::GTD).expiration(exp)
            } else {
                builder.order_type(OrderType::GTC)
            };
            let signable = match builder.build().await {
                Ok(v) => v,
                Err(e) => {
                    warn!("[sdk] Batch order {i} build failed: {e}");
                    self.maybe_set_backoff("buy_batch", token_id, &format!("build failed: {e}"));
                    continue;
                }
            };

            let signed = match self.clob.sign(&self.signer, signable).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("[sdk] Batch order {i} sign failed: {e}");
                    self.maybe_set_backoff("buy_batch", token_id, &format!("sign failed: {e}"));
                    continue;
                }
            };

            signed_orders.push(signed);
            valid_indices.push(i);
        }

        if signed_orders.is_empty() {
            return Ok(Vec::new());
        }

        // Submit all at once via batch API (with timeout)
        let post_result =
            tokio::time::timeout(HTTP_SUBMIT_TIMEOUT, self.clob.post_orders(signed_orders)).await;

        let responses = match post_result {
            Ok(Ok(v)) => v,
            Err(_elapsed) => {
                // Timeout is always ambiguous — orders may have been created.
                let err_str = format!(
                    "batch post_orders timed out after {}ms",
                    HTTP_SUBMIT_TIMEOUT.as_millis()
                );

                warn!(
                    "[sdk] Ambiguous batch post_orders error ({err_str}) — checking open orders for reconciliation"
                );
                let now = Utc::now();
                let mut reconciled = Vec::new();
                let mut seen_order_ids = std::collections::HashSet::new();
                for &orig_idx in &valid_indices {
                    let (token_id, price, _size, tick_size) = &orders[orig_idx];
                    let rounded_price = round_to_tick(*price, *tick_size);
                    let quantized_size = quantize_order_size(*_size);
                    if let Ok(open) = self.get_open_orders_for_token(token_id).await {
                        for order in &open {
                            if seen_order_ids.contains(&order.id) {
                                continue;
                            }
                            let age_secs = (now - order.created_at).num_seconds();
                            if age_secs < 10
                                && order.price == rounded_price
                                && order.original_size == quantized_size
                                && matches!(order.side, Side::Buy)
                            {
                                warn!(
                                    order_id = %order.id,
                                    orig_idx,
                                    "[sdk] Batch timeout reconciliation: found matching order"
                                );
                                seen_order_ids.insert(order.id.clone());
                                reconciled.push((orig_idx, order.id.clone()));
                                break;
                            }
                        }
                    }
                }
                if !reconciled.is_empty() {
                    warn!(
                        found = reconciled.len(),
                        attempted = valid_indices.len(),
                        "[sdk] Batch timeout reconciliation recovered some orders"
                    );
                    return Ok(reconciled);
                }

                let msg = format!(
                    "Batch post_orders timed out after {}ms",
                    HTTP_SUBMIT_TIMEOUT.as_millis()
                );
                for (token_id, _, _, _) in &orders {
                    self.maybe_set_backoff("buy_batch", token_id, &msg);
                }
                return Err(BotError::Order(msg));
            }
            Ok(Err(e)) => {
                let err_str = format!("{e}");
                let is_ambiguous = err_str.contains("timeout")
                    || err_str.contains("connection")
                    || err_str.contains("hyper");

                if is_ambiguous {
                    // Ambiguous error — some or all orders may have been created.
                    // Query open orders for each token to reconcile.
                    // FIX: Only reconcile orders that were actually submitted (valid_indices),
                    // and compare against quantized size + rounded price to match what the
                    // exchange actually received.
                    warn!(
                        "[sdk] Ambiguous batch post_orders error ({err_str}) — checking open orders for reconciliation"
                    );
                    let now = Utc::now();
                    let mut reconciled = Vec::new();
                    let mut seen_order_ids = std::collections::HashSet::new();
                    for &orig_idx in &valid_indices {
                        let (token_id, price, _size, tick_size) = &orders[orig_idx];
                        let rounded_price = round_to_tick(*price, *tick_size);
                        let quantized_size = quantize_order_size(*_size);
                        if let Ok(open) = self.get_open_orders_for_token(token_id).await {
                            for order in &open {
                                if seen_order_ids.contains(&order.id) {
                                    continue;
                                }
                                let age_secs = (now - order.created_at).num_seconds();
                                if age_secs < 10
                                    && order.price == rounded_price
                                    && order.original_size == quantized_size
                                    && matches!(order.side, Side::Buy)
                                {
                                    warn!(
                                        order_id = %order.id,
                                        orig_idx,
                                        "[sdk] Batch reconciliation: found matching order after ambiguous error"
                                    );
                                    seen_order_ids.insert(order.id.clone());
                                    reconciled.push((orig_idx, order.id.clone()));
                                    break;
                                }
                            }
                        }
                    }
                    if !reconciled.is_empty() {
                        warn!(
                            found = reconciled.len(),
                            attempted = valid_indices.len(),
                            "[sdk] Batch reconciliation recovered some orders"
                        );
                        return Ok(reconciled);
                    }
                }

                let msg = format!("Batch post_orders failed: {e}");
                for (token_id, _, _, _) in &orders {
                    self.maybe_set_backoff("buy_batch", token_id, &msg);
                }
                return Err(BotError::Order(msg));
            }
        };

        // Collect successful order IDs with their original indices
        let mut results = Vec::new();
        for (resp_idx, resp) in responses.iter().enumerate() {
            let orig_idx = valid_indices[resp_idx];
            if resp.success && !resp.order_id.is_empty() {
                results.push((orig_idx, resp.order_id.clone()));
            } else {
                let err_msg = resp.error_msg.as_deref().unwrap_or("unknown");
                let token_id = orders[orig_idx].0;
                warn!("[sdk] Batch order {orig_idx} rejected: {err_msg}");
                self.maybe_set_backoff("buy_batch", token_id, &format!("rejected: {err_msg}"));
            }
        }

        Ok(results)
    }

    /// Place a GTC limit sell order (V2: proactive sell-back of overweight positions).
    /// Returns the order ID from the CLOB on success.
    pub async fn place_limit_sell(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
        expiration: Option<DateTime<Utc>>,
    ) -> Result<String> {
        // SDK-level naked sell guard — last line of defense before CLOB submission.
        let size = match self.validate_sell_position(token_id, size) {
            Ok(v) => v,
            Err(e) => {
                self.maybe_set_backoff("sell_gtc", token_id, &e.to_string());
                return Err(e);
            }
        };

        if let Some(msg) = self.backoff_if_active("sell_gtc", token_id) {
            return Err(BotError::Order(msg));
        }

        let size = quantize_order_size(size);
        if size < MIN_ORDER_SHARES {
            let msg = format!(
                "Sell order skipped: size {} is below minimum {} after 2dp quantization",
                size, MIN_ORDER_SHARES
            );
            self.maybe_set_backoff("sell_gtc", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        let rounded_price = round_to_tick(price, tick_size);
        if rounded_price <= Decimal::ZERO {
            let msg = format!("Sell order skipped: non-positive rounded price {rounded_price}");
            self.maybe_set_backoff("sell_gtc", token_id, &msg);
            return Err(BotError::Order(msg));
        }
        let token = U256::from_str(token_id)
            .map_err(|e| BotError::Order(format!("Invalid token_id: {e}")))?;

        let mut builder = self
            .clob
            .limit_order()
            .token_id(token)
            .side(Side::Sell)
            .price(rounded_price)
            .size(size)
            .post_only(true);
        builder = if let Some(exp) = expiration {
            builder.order_type(OrderType::GTD).expiration(exp)
        } else {
            builder.order_type(OrderType::GTC)
        };
        let signable = match builder.build().await {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Sell order build failed: {e}");
                self.maybe_set_backoff("sell_gtc", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        let signed = match self.clob.sign(&self.signer, signable).await {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Sell order sign failed: {e}");
                self.maybe_set_backoff("sell_gtc", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        let post_result =
            tokio::time::timeout(HTTP_SUBMIT_TIMEOUT, self.clob.post_order(signed)).await;

        let response = match post_result {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                let err_str = format!("{e}");
                let is_ambiguous = err_str.contains("timeout")
                    || err_str.contains("connection")
                    || err_str.contains("hyper");

                if is_ambiguous {
                    // Ambiguous error — sell order may have been created.
                    // Query open orders to check.
                    warn!(
                        token_id,
                        "[sdk] Ambiguous sell post_order error ({err_str}) — checking open orders"
                    );
                    if let Ok(orders) = self.get_open_orders_for_token(token_id).await {
                        let now = Utc::now();
                        for order in &orders {
                            let age_secs = (now - order.created_at).num_seconds();
                            if age_secs < 10
                                && order.price == rounded_price
                                && order.original_size == size
                                && matches!(order.side, Side::Sell)
                            {
                                warn!(
                                    order_id = %order.id,
                                    "[sdk] Found matching sell order after ambiguous error — reconciled"
                                );
                                return Ok(order.id.clone());
                            }
                        }
                    }
                }

                let msg = format!("Sell order post failed: {e}");
                self.maybe_set_backoff("sell_gtc", token_id, &msg);
                return Err(BotError::Order(msg));
            }
            Err(_elapsed) => {
                // Timeout — sell order may have been created (ambiguous).
                warn!(
                    token_id,
                    timeout_ms = HTTP_SUBMIT_TIMEOUT.as_millis() as u64,
                    "[sdk] sell post_order timed out — checking open orders"
                );
                if let Ok(orders) = self.get_open_orders_for_token(token_id).await {
                    let now = Utc::now();
                    for order in &orders {
                        let age_secs = (now - order.created_at).num_seconds();
                        if age_secs < 10
                            && order.price == rounded_price
                            && order.original_size == size
                            && matches!(order.side, Side::Sell)
                        {
                            warn!(
                                order_id = %order.id,
                                "[sdk] Found matching sell order after timeout — reconciled"
                            );
                            return Ok(order.id.clone());
                        }
                    }
                }
                let msg = format!(
                    "Sell post_order timed out after {}ms",
                    HTTP_SUBMIT_TIMEOUT.as_millis()
                );
                self.maybe_set_backoff("sell_gtc", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        if !response.success {
            let err_msg = response.error_msg.as_deref().unwrap_or("unknown error");
            let msg = format!("Sell order rejected: {err_msg}");
            self.maybe_set_backoff("sell_gtc", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        if response.order_id.is_empty() {
            let msg = "CLOB returned success but empty order_id".to_string();
            self.maybe_set_backoff("sell_gtc", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        Ok(response.order_id)
    }

    /// Place a FOK sell order for emergency inventory dumps (FIX 10).
    pub async fn place_emergency_sell(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
    ) -> Result<()> {
        // SDK-level naked sell guard — last line of defense before CLOB submission.
        let size = match self.validate_sell_position(token_id, size) {
            Ok(v) => v,
            Err(e) => {
                self.maybe_set_backoff("sell_fok", token_id, &e.to_string());
                return Err(e);
            }
        };

        if let Some(msg) = self.backoff_if_active("sell_fok", token_id) {
            return Err(BotError::Order(msg));
        }

        let size = quantize_order_size(size);
        if size < MIN_ORDER_SHARES {
            let msg = format!(
                "Emergency sell skipped: size {} is below minimum {} after 2dp quantization",
                size, MIN_ORDER_SHARES
            );
            self.maybe_set_backoff("sell_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        let rounded_price = round_to_tick(price, tick_size);
        if rounded_price <= Decimal::ZERO {
            let msg = format!("Emergency sell skipped: non-positive rounded price {rounded_price}");
            self.maybe_set_backoff("sell_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }
        let token = U256::from_str(token_id)
            .map_err(|e| BotError::Order(format!("Invalid token_id: {e}")))?;

        let signable = match self
            .clob
            .limit_order()
            .token_id(token)
            .side(Side::Sell)
            .price(rounded_price)
            .size(size)
            .order_type(OrderType::FOK)
            .build()
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Emergency sell build failed: {e}");
                self.maybe_set_backoff("sell_fok", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        let signed = match self.clob.sign(&self.signer, signable).await {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Emergency sell sign failed: {e}");
                self.maybe_set_backoff("sell_fok", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        let response =
            match tokio::time::timeout(HTTP_SUBMIT_TIMEOUT, self.clob.post_order(signed)).await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    let msg = format!("Emergency sell post failed: {e}");
                    self.maybe_set_backoff("sell_fok", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
                Err(_) => {
                    let msg = format!(
                        "Emergency sell timed out after {}ms",
                        HTTP_SUBMIT_TIMEOUT.as_millis()
                    );
                    self.maybe_set_backoff("sell_fok", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
            };

        if !response.success {
            let err_msg = response.error_msg.as_deref().unwrap_or("unknown error");
            let msg = format!("Emergency sell rejected: {err_msg}");
            self.maybe_set_backoff("sell_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        if response.order_id.is_empty() {
            let msg = "Emergency sell succeeded but returned empty order_id".to_string();
            self.maybe_set_backoff("sell_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        Ok(())
    }

    /// Place a FOK buy order for late-phase pair completion.
    /// Crosses the spread to fill immediately. Returns order_id on success, or
    /// Err if the FOK couldn't be filled (expected when liquidity is thin).
    pub async fn place_fok_buy(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        tick_size: Decimal,
    ) -> Result<String> {
        if let Some(msg) = self.backoff_if_active("buy_fok", token_id) {
            return Err(BotError::Order(msg));
        }

        let size = quantize_order_size(size);
        if size < MIN_ORDER_SHARES {
            let msg = format!(
                "FOK buy skipped: size {} is below minimum {} after 2dp quantization",
                size, MIN_ORDER_SHARES
            );
            self.maybe_set_backoff("buy_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        let rounded_price = round_to_tick(price, tick_size);
        if rounded_price <= Decimal::ZERO {
            let msg = format!("FOK buy skipped: non-positive rounded price {rounded_price}");
            self.maybe_set_backoff("buy_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }
        if rounded_price * size < MIN_MARKETABLE_BUY_NOTIONAL {
            let msg = format!(
                "FOK buy skipped: notional {} is below minimum {}",
                rounded_price * size,
                MIN_MARKETABLE_BUY_NOTIONAL
            );
            self.maybe_set_backoff("buy_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }
        let token = U256::from_str(token_id)
            .map_err(|e| BotError::Order(format!("Invalid token_id: {e}")))?;

        let signable = match self
            .clob
            .limit_order()
            .token_id(token)
            .side(Side::Buy)
            .price(rounded_price)
            .size(size)
            .order_type(OrderType::FOK)
            .build()
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("FOK buy build failed: {e}");
                self.maybe_set_backoff("buy_fok", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        let signed = match self.clob.sign(&self.signer, signable).await {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("FOK buy sign failed: {e}");
                self.maybe_set_backoff("buy_fok", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        let response =
            match tokio::time::timeout(HTTP_SUBMIT_TIMEOUT, self.clob.post_order(signed)).await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    let msg = format!("FOK buy post failed: {e}");
                    self.maybe_set_backoff("buy_fok", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
                Err(_) => {
                    let msg = format!(
                        "FOK buy timed out after {}ms",
                        HTTP_SUBMIT_TIMEOUT.as_millis()
                    );
                    self.maybe_set_backoff("buy_fok", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
            };

        if !response.success {
            let err_msg = response.error_msg.as_deref().unwrap_or("unknown error");
            let msg = format!("FOK buy rejected: {err_msg}");
            self.maybe_set_backoff("buy_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        if response.order_id.is_empty() {
            let msg = "FOK buy succeeded but returned empty order_id".to_string();
            self.maybe_set_backoff("buy_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        Ok(response.order_id)
    }

    /// Place a market-style FOK buy sized in shares using live book depth discovery.
    pub async fn place_market_buy_fok_shares(
        &self,
        token_id: &str,
        shares: Decimal,
    ) -> Result<String> {
        if let Some(msg) = self.backoff_if_active("buy_market_fok", token_id) {
            return Err(BotError::Order(msg));
        }

        let shares = quantize_order_size(shares);
        if shares < MIN_ORDER_SHARES {
            let msg = format!(
                "Market FOK buy skipped: size {} is below minimum {} after 2dp quantization",
                shares, MIN_ORDER_SHARES
            );
            self.maybe_set_backoff("buy_market_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        let token = U256::from_str(token_id)
            .map_err(|e| BotError::Order(format!("Invalid token_id: {e}")))?;
        let amount = Amount::shares(shares)
            .map_err(|e| BotError::Order(format!("Market FOK buy amount failed: {e}")))?;
        let signable = match self
            .clob
            .market_order()
            .token_id(token)
            .side(Side::Buy)
            .amount(amount)
            .order_type(OrderType::FOK)
            .build()
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Market FOK buy build failed: {e}");
                self.maybe_set_backoff("buy_market_fok", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        let signed = match self.clob.sign(&self.signer, signable).await {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Market FOK buy sign failed: {e}");
                self.maybe_set_backoff("buy_market_fok", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        let response =
            match tokio::time::timeout(HTTP_SUBMIT_TIMEOUT, self.clob.post_order(signed)).await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    let msg = format!("Market FOK buy post failed: {e}");
                    self.maybe_set_backoff("buy_market_fok", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
                Err(_) => {
                    let msg = format!(
                        "Market FOK buy timed out after {}ms",
                        HTTP_SUBMIT_TIMEOUT.as_millis()
                    );
                    self.maybe_set_backoff("buy_market_fok", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
            };

        if !response.success {
            let err_msg = response.error_msg.as_deref().unwrap_or("unknown error");
            let msg = format!("Market FOK buy rejected: {err_msg}");
            self.maybe_set_backoff("buy_market_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        if response.order_id.is_empty() {
            let msg = "Market FOK buy succeeded but returned empty order_id".to_string();
            self.maybe_set_backoff("buy_market_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        Ok(response.order_id)
    }

    /// Place a market-style FOK sell sized in shares using live book depth discovery.
    pub async fn place_market_sell_fok_shares(
        &self,
        token_id: &str,
        shares: Decimal,
    ) -> Result<String> {
        let shares = match self.validate_sell_position(token_id, shares) {
            Ok(v) => v,
            Err(e) => {
                self.maybe_set_backoff("sell_market_fok", token_id, &e.to_string());
                return Err(e);
            }
        };
        if let Some(msg) = self.backoff_if_active("sell_market_fok", token_id) {
            return Err(BotError::Order(msg));
        }

        let shares = quantize_order_size(shares);
        if shares < MIN_ORDER_SHARES {
            let msg = format!(
                "Market FOK sell skipped: size {} is below minimum {} after 2dp quantization",
                shares, MIN_ORDER_SHARES
            );
            self.maybe_set_backoff("sell_market_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        let token = U256::from_str(token_id)
            .map_err(|e| BotError::Order(format!("Invalid token_id: {e}")))?;
        let amount = Amount::shares(shares)
            .map_err(|e| BotError::Order(format!("Market FOK sell amount failed: {e}")))?;
        let signable = match self
            .clob
            .market_order()
            .token_id(token)
            .side(Side::Sell)
            .amount(amount)
            .order_type(OrderType::FOK)
            .build()
            .await
        {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Market FOK sell build failed: {e}");
                self.maybe_set_backoff("sell_market_fok", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        let signed = match self.clob.sign(&self.signer, signable).await {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Market FOK sell sign failed: {e}");
                self.maybe_set_backoff("sell_market_fok", token_id, &msg);
                return Err(BotError::Order(msg));
            }
        };

        let response =
            match tokio::time::timeout(HTTP_SUBMIT_TIMEOUT, self.clob.post_order(signed)).await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    let msg = format!("Market FOK sell post failed: {e}");
                    self.maybe_set_backoff("sell_market_fok", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
                Err(_) => {
                    let msg = format!(
                        "Market FOK sell timed out after {}ms",
                        HTTP_SUBMIT_TIMEOUT.as_millis()
                    );
                    self.maybe_set_backoff("sell_market_fok", token_id, &msg);
                    return Err(BotError::Order(msg));
                }
            };

        if !response.success {
            let err_msg = response.error_msg.as_deref().unwrap_or("unknown error");
            let msg = format!("Market FOK sell rejected: {err_msg}");
            self.maybe_set_backoff("sell_market_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        if response.order_id.is_empty() {
            let msg = "Market FOK sell succeeded but returned empty order_id".to_string();
            self.maybe_set_backoff("sell_market_fok", token_id, &msg);
            return Err(BotError::Order(msg));
        }

        Ok(response.order_id)
    }

    /// Submit multiple market FOK orders in one batch request.
    pub async fn place_batch_market_orders(
        &self,
        orders: Vec<MarketOrderSpec>,
    ) -> Result<Vec<MarketOrderResult>> {
        if orders.is_empty() {
            return Ok(Vec::new());
        }

        let mut signed_orders = Vec::with_capacity(orders.len());
        let mut submitted_specs = Vec::with_capacity(orders.len());

        for spec in orders {
            let mut shares = spec.shares;
            if matches!(spec.side, Side::Sell) {
                shares = match self.validate_sell_position(&spec.token_id, shares) {
                    Ok(v) => v,
                    Err(e) => {
                        self.maybe_set_backoff("market_batch", &spec.token_id, &e.to_string());
                        return Err(e);
                    }
                };
            }
            if let Some(msg) = self.backoff_if_active("market_batch", &spec.token_id) {
                return Err(BotError::Order(msg));
            }

            let shares = quantize_order_size(shares);
            if shares < MIN_ORDER_SHARES {
                let msg = format!(
                    "Batch market order skipped for token {}: size {} below minimum {}",
                    spec.token_id, shares, MIN_ORDER_SHARES
                );
                self.maybe_set_backoff("market_batch", &spec.token_id, &msg);
                return Err(BotError::Order(msg));
            }

            let token = U256::from_str(&spec.token_id)
                .map_err(|e| BotError::Order(format!("Invalid token_id: {e}")))?;
            let amount = Amount::shares(shares)
                .map_err(|e| BotError::Order(format!("Batch market amount failed: {e}")))?;
            let signable = match self
                .clob
                .market_order()
                .token_id(token)
                .side(spec.side)
                .amount(amount)
                .order_type(OrderType::FOK)
                .build()
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!("Batch market order build failed: {e}");
                    self.maybe_set_backoff("market_batch", &spec.token_id, &msg);
                    return Err(BotError::Order(msg));
                }
            };
            let signed = match self.clob.sign(&self.signer, signable).await {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!("Batch market order sign failed: {e}");
                    self.maybe_set_backoff("market_batch", &spec.token_id, &msg);
                    return Err(BotError::Order(msg));
                }
            };

            submitted_specs.push(MarketOrderSpec {
                token_id: spec.token_id,
                side: spec.side,
                shares,
            });
            signed_orders.push(signed);
        }

        let responses =
            match tokio::time::timeout(HTTP_SUBMIT_TIMEOUT, self.clob.post_orders(signed_orders))
                .await
            {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    let msg = format!("Batch market post_orders failed: {e}");
                    for spec in &submitted_specs {
                        self.maybe_set_backoff("market_batch", &spec.token_id, &msg);
                    }
                    return Err(BotError::Order(msg));
                }
                Err(_) => {
                    let msg = format!(
                        "Batch market post_orders timed out after {}ms",
                        HTTP_SUBMIT_TIMEOUT.as_millis()
                    );
                    for spec in &submitted_specs {
                        self.maybe_set_backoff("market_batch", &spec.token_id, &msg);
                    }
                    return Err(BotError::Order(msg));
                }
            };

        if responses.len() != submitted_specs.len() {
            let msg = format!(
                "Batch market response length mismatch: expected {}, got {}",
                submitted_specs.len(),
                responses.len()
            );
            for spec in &submitted_specs {
                self.maybe_set_backoff("market_batch", &spec.token_id, &msg);
            }
            return Err(BotError::Order(msg));
        }

        let mut results = Vec::with_capacity(submitted_specs.len());
        for (index, (resp, spec)) in responses
            .into_iter()
            .zip(submitted_specs.into_iter())
            .enumerate()
        {
            if !resp.success {
                let err_msg = resp
                    .error_msg
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string());
                self.maybe_set_backoff("market_batch", &spec.token_id, &err_msg);
                results.push(MarketOrderResult {
                    index,
                    token_id: spec.token_id,
                    side: spec.side,
                    shares: spec.shares,
                    success: false,
                    order_id: None,
                    error_msg: Some(err_msg),
                });
                continue;
            }

            if resp.order_id.is_empty() {
                let msg = "Batch market order succeeded but returned empty order_id".to_string();
                self.maybe_set_backoff("market_batch", &spec.token_id, &msg);
                results.push(MarketOrderResult {
                    index,
                    token_id: spec.token_id,
                    side: spec.side,
                    shares: spec.shares,
                    success: false,
                    order_id: None,
                    error_msg: Some(msg),
                });
                continue;
            }

            results.push(MarketOrderResult {
                index,
                token_id: spec.token_id,
                side: spec.side,
                shares: spec.shares,
                success: true,
                order_id: Some(resp.order_id),
                error_msg: None,
            });
        }

        Ok(results)
    }

    // ── Order Cancellation (FIX 4) ──

    /// Cancel a single order by ID with retry.
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let oid = order_id.to_string();
        retry_sdk_call(|| {
            let oid_ref = oid.as_str();
            async move { self.clob.cancel_order(oid_ref).await }
        })
        .await
        .map_err(|e| BotError::Order(format!("Cancel order failed: {e}")))?;

        Ok(())
    }

    /// Cancel all open orders (FIX 5: startup cancel-all).
    pub async fn cancel_all_orders(&self) -> Result<CancelOrdersResponse> {
        let response = retry_sdk_call(|| async { self.clob.cancel_all_orders().await })
            .await
            .map_err(|e| BotError::Order(format!("Cancel all orders failed: {e}")))?;

        info!(
            cancelled = response.canceled.len(),
            "Cancelled all open orders"
        );

        Ok(response)
    }

    /// Batch-cancel specific orders by ID list (single HTTP call via DELETE /orders).
    /// Returns the list of successfully cancelled order IDs.
    pub async fn cancel_orders(&self, order_ids: &[&str]) -> Result<Vec<String>> {
        if order_ids.is_empty() {
            return Ok(vec![]);
        }
        let ids: Vec<String> = order_ids.iter().map(|s| s.to_string()).collect();
        let response = retry_sdk_call(|| {
            let refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
            async move { self.clob.cancel_orders(&refs).await }
        })
        .await
        .map_err(|e| BotError::Order(format!("Batch cancel failed: {e}")))?;

        if !response.not_canceled.is_empty() {
            warn!(
                not_cancelled = ?response.not_canceled,
                "Some orders failed to cancel in batch"
            );
        }
        info!(
            requested = order_ids.len(),
            cancelled = response.canceled.len(),
            "Batch-cancelled orders"
        );

        Ok(response.canceled)
    }

    /// Cancel all orders for a specific market (condition_id).
    /// Single HTTP call via DELETE /cancel-market-orders.
    /// Returns the list of successfully cancelled order IDs.
    pub async fn cancel_market_orders_by_market(&self, condition_id: &str) -> Result<Vec<String>> {
        let b256 = B256::from_str(condition_id)
            .map_err(|e| BotError::Config(format!("Invalid condition_id for cancel: {e}")))?;
        let req = CancelMarketOrderRequest::builder().market(b256).build();
        let response = retry_sdk_call(|| async { self.clob.cancel_market_orders(&req).await })
            .await
            .map_err(|e| BotError::Order(format!("Cancel market orders failed: {e}")))?;

        info!(
            cancelled = response.canceled.len(),
            condition_id, "Cancelled market orders"
        );

        Ok(response.canceled)
    }

    // ── WebSocket Subscriptions (FIX 6) ──

    // NOTE: WebSocket subscribe methods (subscribe_orderbook, subscribe_trades, subscribe_orders)
    // are called directly via `sdk.ws.subscribe_*()` rather than through wrappers here.
    // This is because the SDK uses `use<>` capture to return `'static` streams, but wrapping
    // them in a method that takes `&self` obscures that and prevents spawning into tokio tasks.

    /// Unsubscribe from orderbook updates.
    pub fn unsubscribe_orderbook(&self, token_ids: &[U256]) -> Result<()> {
        self.ws
            .unsubscribe_orderbook(token_ids)
            .map_err(|e| BotError::Sdk(format!("Orderbook unsub failed: {e}")))
    }

    // ── Market Discovery (FIX 14) ──

    /// Discover active markets from the Gamma API.
    /// `tag` is the Gamma tag slug (e.g. "bitcoin" or "ethereum").
    pub async fn discover_events(&self, tag: &str) -> Result<Vec<Event>> {
        let now = chrono::Utc::now();
        let req = EventsRequest::builder()
            .limit(200)
            .active(true)
            .closed(false)
            .end_date_min(now)
            .tag_slug(tag.to_string())
            .build();

        retry_sdk_call(|| async { self.gamma.events(&req).await })
            .await
            .map_err(|e| BotError::Sdk(format!("Gamma events query failed: {e}")))
    }

    // ── Market Resolution Query (BUG #3 fix) ──

    /// Query the CLOB API for a market's resolution status.
    /// Returns the winning outcome if the market is resolved, None otherwise.
    pub async fn get_market_resolution(
        &self,
        condition_id: &str,
    ) -> Result<Option<crate::types::Outcome>> {
        use crate::types::Outcome;

        let cid = condition_id.to_string();
        let response = retry_sdk_call(|| {
            let cid_ref = cid.as_str();
            async move { self.clob.market(cid_ref).await }
        })
        .await
        .map_err(|e| BotError::Sdk(format!("Market query failed: {e}")))?;

        for token in &response.tokens {
            if token.winner {
                return Ok(match token.outcome.as_str() {
                    "Yes" | "Up" => Some(Outcome::Yes),
                    "No" | "Down" => Some(Outcome::No),
                    _ => None,
                });
            }
        }

        Ok(None) // No winner found (market not yet resolved or voided)
    }

    /// Check if a market is still active (not closed/resolved) via the CLOB API.
    /// Returns true if the market is active and accepting orders, false otherwise.
    pub async fn is_market_active(&self, condition_id: &str) -> Result<bool> {
        let cid = condition_id.to_string();
        let response = retry_sdk_call(|| {
            let cid_ref = cid.as_str();
            async move { self.clob.market(cid_ref).await }
        })
        .await
        .map_err(|e| BotError::Sdk(format!("Market query failed: {e}")))?;

        Ok(response.active && !response.closed)
    }

    // ── Market Parameters (fees + tick size) ──

    /// Fetch market parameters from the CLOB API in a single call.
    /// Returns `(maker_base_fee, taker_base_fee, minimum_tick_size)`.
    pub async fn get_market_params(
        &self,
        condition_id: &str,
    ) -> Result<(Decimal, Decimal, Decimal)> {
        let cid = condition_id.to_string();
        let response = retry_sdk_call(|| {
            let cid_ref = cid.as_str();
            async move { self.clob.market(cid_ref).await }
        })
        .await
        .map_err(|e| BotError::Sdk(format!("Market params query failed: {e}")))?;

        Ok((
            response.maker_base_fee,
            response.taker_base_fee,
            response.minimum_tick_size,
        ))
    }

    // ── Heartbeat Health Probes ──

    /// Check if the SDK's automatic heartbeat background task is still running.
    /// If heartbeats stop, Polymarket will auto-cancel all orders after ~15s.
    pub fn heartbeats_active(&self) -> bool {
        self.clob.heartbeats_active()
    }

    /// Check heartbeat health by verifying the background task is alive.
    ///
    /// IMPORTANT: We must NOT call `post_heartbeat(None)` here — sending a null
    /// heartbeat_id creates a NEW server-side session, which invalidates the ID
    /// held by the SDK's background task. This caused a cascading failure where
    /// every probe killed the background heartbeat, leading Polymarket to
    /// auto-cancel all resting orders.
    pub async fn probe_heartbeat(&self) -> Result<()> {
        if self.clob.heartbeats_active() {
            Ok(())
        } else {
            Err(BotError::Sdk(
                "SDK heartbeat background task is not running".to_string(),
            ))
        }
    }

    // ── Open Orders Query (FIX 21: idempotency reconciliation) ──

    /// Query open orders for a specific token, used to reconcile after ambiguous submission errors.
    /// Paginates through all pages using `next_cursor` (max 5 pages as safety limit).
    pub async fn get_open_orders_for_token(
        &self,
        asset_id: &str,
    ) -> Result<Vec<polymarket_client_sdk::clob::types::response::OpenOrderResponse>> {
        use polymarket_client_sdk::clob::types::request::OrdersRequest;
        let token_u256 = alloy::primitives::U256::from_str(asset_id)
            .map_err(|e| BotError::Sdk(format!("Invalid token ID: {e}")))?;
        let req = OrdersRequest::builder().asset_id(token_u256).build();

        let mut all_orders = Vec::new();
        let mut cursor: Option<String> = None;
        let max_pages = 5u32;

        for page_num in 0..max_pages {
            let page = retry_sdk_call(|| {
                let r = &req;
                let c = cursor.clone();
                async move { self.clob.orders(r, c).await }
            })
            .await
            .map_err(|e| BotError::Sdk(format!("Open orders query failed: {e}")))?;

            let has_more = !page.next_cursor.is_empty() && page.next_cursor != "LTE=";
            all_orders.extend(page.data);

            if has_more {
                cursor = Some(page.next_cursor);
                if page_num == max_pages - 1 {
                    warn!(
                        asset_id = %asset_id,
                        total_fetched = all_orders.len(),
                        max_pages,
                        "[sdk] Open orders query hit pagination cap — results may be incomplete"
                    );
                }
            } else {
                break;
            }
        }

        Ok(all_orders)
    }

    // ── Startup Reconciliation (FIX 8) ──

    /// Get current positions from the Data API for reconciliation.
    /// Uses explicit limit and logs if the result is at the limit boundary (possible truncation).
    pub async fn get_positions_from_api(
        &self,
    ) -> Result<Vec<polymarket_client_sdk::data::types::response::Position>> {
        let limit = 500i32;
        let req = polymarket_client_sdk::data::types::request::PositionsRequest::builder()
            .user(self.wallet_address)
            .limit(limit)
            .map_err(|e| BotError::Sdk(format!("Invalid limit for positions request: {e}")))?
            .build();

        let positions = retry_sdk_call(|| async { self.data.positions(&req).await })
            .await
            .map_err(|e| BotError::Sdk(format!("Data positions query failed: {e}")))?;

        if positions.len() as i32 >= limit {
            warn!(
                count = positions.len(),
                limit,
                "[sdk] Position query returned at limit — possible truncation, some positions may be missing"
            );
        }

        Ok(positions)
    }

    // ── CTF Operations (FIX 7 + FIX 12) ──

    /// Merge complete YES+NO pairs back into USDC on-chain.
    ///
    /// Routes through the relayer (gasless) when available, falls back to direct RPC.
    /// `amount` is in whole shares (the CTF contract uses USDC-scaled units internally,
    /// so we multiply by 10^6).
    ///
    /// Returns the transaction hash on success.
    pub async fn merge_positions(
        &self,
        rpc_url: &str,
        condition_id_hex: &str,
        amount: u64,
    ) -> Result<B256> {
        let condition_id = B256::from_str(condition_id_hex)
            .map_err(|e| BotError::OnChain(format!("Invalid condition_id: {e}")))?;

        // Use relayer if available (gasless)
        if let Some(relayer) = &self.relayer {
            info!(
                %condition_id_hex,
                shares = amount,
                "Executing gasless merge via relayer"
            );
            return relayer.merge_positions(condition_id, amount).await;
        }

        // Fallback: direct RPC (EOA pays gas)
        // Serialize merges across concurrent orchestrators to prevent wallet nonce races.
        let _merge_guard = self.merge_lock.lock().await;

        let url = rpc_url
            .parse()
            .map_err(|e| BotError::OnChain(format!("Invalid RPC URL: {e}")))?;

        let provider = ProviderBuilder::new()
            .wallet(self.signer.clone())
            .connect_http(url);

        let ctf_client =
            ctf::Client::new(provider, POLYGON).map_err(|e| BotError::OnChain(format!("{e}")))?;

        // CTF contract amounts are in USDC-scaled units (6 decimals).
        // 1 share = 1_000_000 on-chain units.
        let amount_scaled = U256::from(amount) * U256::from(1_000_000u64);

        let merge_req = MergePositionsRequest::for_binary_market(
            self.usdc_address,
            condition_id,
            amount_scaled,
        );

        info!(
            %condition_id_hex,
            shares = amount,
            raw_amount = %amount_scaled,
            "Executing on-chain merge (direct RPC)"
        );

        let resp = ctf_client
            .merge_positions(&merge_req)
            .await
            .map_err(|e| BotError::OnChain(format!("Merge failed: {e}")))?;

        Ok(resp.transaction_hash)
    }

    /// Redeem winning positions after market resolution.
    ///
    /// Routes through the relayer (gasless) when available, falls back to direct RPC.
    /// Returns the transaction hash on success.
    pub async fn redeem_positions(
        &self,
        rpc_url: &str,
        condition_id_hex: &str,
        neg_risk: bool,
        _neg_risk_amounts: Option<Vec<U256>>,
    ) -> Result<B256> {
        let condition_id = B256::from_str(condition_id_hex)
            .map_err(|e| BotError::OnChain(format!("Invalid condition_id: {e}")))?;

        // Use relayer if available (gasless)
        if let Some(relayer) = &self.relayer {
            info!(%condition_id_hex, neg_risk, "Executing gasless redeem via relayer");
            return relayer.redeem_positions(condition_id, neg_risk).await;
        }

        // Fallback: direct RPC (EOA pays gas)
        let url = rpc_url
            .parse()
            .map_err(|e| BotError::OnChain(format!("Invalid RPC URL: {e}")))?;

        let provider = ProviderBuilder::new()
            .wallet(self.signer.clone())
            .connect_http(url);

        let ctf_client = if neg_risk {
            ctf::Client::with_neg_risk(provider, POLYGON)
        } else {
            ctf::Client::new(provider, POLYGON)
        }
        .map_err(|e| BotError::OnChain(format!("{e}")))?;

        let redeem_req = RedeemPositionsRequest::for_binary_market(self.usdc_address, condition_id);

        info!(%condition_id_hex, neg_risk, "Executing on-chain redeem (direct RPC)");

        let resp = ctf_client
            .redeem_positions(&redeem_req)
            .await
            .map_err(|e| BotError::OnChain(format!("Redeem failed: {e}")))?;

        Ok(resp.transaction_hash)
    }

    /// Scan all wallet positions via Data API and redeem every resolved market on-chain.
    /// Serialized via `redeem_lock` to prevent concurrent sweeps from multiple orchestrators
    /// racing on the same wallet (wasting gas and causing nonce conflicts).
    ///
    /// Returns `(success_count, fail_count)`.
    pub async fn redeem_all_redeemable(&self, rpc_url: &str) -> Result<(u32, u32)> {
        let _guard = self.redeem_lock.lock().await;
        use polymarket_client_sdk::data::types::request::PositionsRequest;

        let req = PositionsRequest::builder()
            .user(self.wallet_address)
            .redeemable(true)
            .size_threshold(Decimal::new(1, 2)) // 0.01 minimum
            .limit(500)
            .map_err(|e| BotError::Sdk(format!("Invalid limit for positions request: {e}")))?
            .build();

        let positions = self
            .data
            .positions(&req)
            .await
            .map_err(|e| BotError::Sdk(format!("Data API redeemable query failed: {e}")))?;

        if positions.is_empty() {
            info!("[redeem_all] No redeemable positions found");
            return Ok((0, 0));
        }

        info!(
            count = positions.len(),
            "[redeem_all] Found redeemable positions"
        );

        // Group by condition_id, track neg_risk flag
        let mut markets: std::collections::HashMap<B256, (String, bool)> =
            std::collections::HashMap::new();
        for pos in &positions {
            if pos.size <= Decimal::ZERO {
                continue;
            }
            markets
                .entry(pos.condition_id)
                .or_insert_with(|| (pos.title.clone(), pos.negative_risk));
        }

        let mut success_count = 0u32;
        let mut fail_count = 0u32;

        for (condition_id, (title, neg_risk)) in &markets {
            info!(%condition_id, title, neg_risk, "[redeem_all] Redeeming");

            let result = if let Some(relayer) = &self.relayer {
                // Gasless path via relayer
                relayer.redeem_positions(*condition_id, *neg_risk).await
            } else {
                // Direct RPC fallback
                let url = rpc_url
                    .parse()
                    .map_err(|e| BotError::OnChain(format!("Invalid RPC URL: {e}")))?;

                let provider = ProviderBuilder::new()
                    .wallet(self.signer.clone())
                    .connect_http(url);

                let ctf_client = if *neg_risk {
                    ctf::Client::with_neg_risk(provider, POLYGON)
                } else {
                    ctf::Client::new(provider, POLYGON)
                }
                .map_err(|e| BotError::OnChain(format!("{e}")))?;

                let redeem_req =
                    RedeemPositionsRequest::for_binary_market(self.usdc_address, *condition_id);

                ctf_client
                    .redeem_positions(&redeem_req)
                    .await
                    .map(|resp| resp.transaction_hash)
                    .map_err(|e| BotError::OnChain(format!("Redeem failed: {e}")))
            };

            match result {
                Ok(tx) => {
                    info!(%condition_id, tx = %tx, "[redeem_all] OK");
                    success_count += 1;
                }
                Err(e) => {
                    warn!(%condition_id, title, "[redeem_all] FAILED: {e}");
                    fail_count += 1;
                }
            }
        }

        info!(success_count, fail_count, "[redeem_all] Sweep complete");
        Ok((success_count, fail_count))
    }

    /// Split USDC collateral into YES+NO outcome tokens on-chain.
    ///
    /// Routes through the relayer (gasless) when available, falls back to direct RPC.
    /// `amount_usdc_6` is in USDC raw units (6 decimals), e.g. 1_000_000 = $1.00.
    /// Returns the transaction hash on success.
    pub async fn split_position(
        &self,
        rpc_url: &str,
        condition_id_hex: &str,
        amount_usdc_6: u64,
    ) -> Result<B256> {
        let condition_id = B256::from_str(condition_id_hex)
            .map_err(|e| BotError::OnChain(format!("Invalid condition_id: {e}")))?;

        // Use relayer if available (gasless)
        if let Some(relayer) = &self.relayer {
            info!(%condition_id_hex, amount_usdc_6, "Executing gasless split via relayer");
            return relayer.split_position(condition_id, amount_usdc_6).await;
        }

        // Fallback: direct RPC (EOA pays gas)
        let url = rpc_url
            .parse()
            .map_err(|e| BotError::OnChain(format!("Invalid RPC URL: {e}")))?;

        let provider = ProviderBuilder::new()
            .wallet(self.signer.clone())
            .connect_http(url);

        let ctf_client =
            ctf::Client::new(provider, POLYGON).map_err(|e| BotError::OnChain(format!("{e}")))?;

        let split_req = SplitPositionRequest::for_binary_market(
            self.usdc_address,
            condition_id,
            U256::from(amount_usdc_6),
        );

        info!(%condition_id_hex, amount_usdc_6, "Executing on-chain split (direct RPC)");

        let resp = ctf_client
            .split_position(&split_req)
            .await
            .map_err(|e| BotError::OnChain(format!("Split failed: {e}")))?;

        Ok(resp.transaction_hash)
    }
}

// ── Retry Helper (FIX 9) ──

/// Retry an async SDK call with exponential backoff and per-attempt timeout.
/// Retries on transient errors (429, 5xx, timeout) and tokio timeout.
async fn retry_sdk_call<F, Fut, T>(f: F) -> polymarket_client_sdk::Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = polymarket_client_sdk::Result<T>>,
{
    let mut last_err = None;

    for attempt in 0..MAX_RETRIES {
        // Wrap each attempt with a timeout to prevent hung HTTP calls from blocking forever.
        let result = tokio::time::timeout(HTTP_SUBMIT_TIMEOUT, f()).await;

        match result {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(e)) => {
                let err_str = format!("{e}");
                let is_retryable = err_str.contains("429")
                    || err_str.contains("425")
                    || err_str.contains("500")
                    || err_str.contains("502")
                    || err_str.contains("503")
                    || err_str.contains("504")
                    || err_str.contains("timeout")
                    || err_str.contains("connection");

                // 425 (engine restart) should NOT be retried rapidly — return immediately
                // so the orchestrator can apply its 30s cooldown.
                let is_engine_restart = err_str.contains("425");
                if is_engine_restart {
                    return Err(e);
                }

                if !is_retryable || attempt == MAX_RETRIES - 1 {
                    return Err(e);
                }

                let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * 2u64.pow(attempt));
                warn!(
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis() as u64,
                    error = %err_str,
                    "SDK call failed, retrying"
                );
                tokio::time::sleep(delay).await;
                last_err = Some(e);
            }
            Err(_elapsed) => {
                // Timeout — always retryable
                let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * 2u64.pow(attempt));
                warn!(
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis() as u64,
                    timeout_ms = HTTP_SUBMIT_TIMEOUT.as_millis() as u64,
                    "SDK call timed out, retrying"
                );
                if attempt == MAX_RETRIES - 1 {
                    return Err(polymarket_client_sdk::error::Error::validation(format!(
                        "SDK call timed out after {}ms on final attempt",
                        HTTP_SUBMIT_TIMEOUT.as_millis()
                    )));
                }
                tokio::time::sleep(delay).await;
            }
        }
    }

    // The loop always executes at least once (MAX_RETRIES > 0) and non-retryable
    // errors return early, so last_err is always Some when we reach here.
    // Safety: if somehow None, we propagate the last error from the loop body.
    match last_err {
        Some(e) => Err(e),
        None => {
            // Structurally unreachable (loop runs ≥1 iteration, Ok returns early).
            // But we never panic on a live trading path.
            tracing::error!(
                "retry_sdk_call: loop completed with no error captured — this should not happen"
            );
            // Re-call once to get a real error
            f().await
        }
    }
}

// ── Tick Size Rounding (FIX 19) ──

/// Round a price down to the nearest tick_size.
fn round_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
    if tick_size.is_zero() {
        return price;
    }
    (price / tick_size).floor() * tick_size
}

fn quantize_order_size(size: Decimal) -> Decimal {
    if size <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let scale = Decimal::from(10u64.pow(ORDER_SIZE_DECIMALS));
    (size * scale).floor() / scale
}

fn is_crosses_book_error(msg: &str) -> bool {
    msg.contains("invalid post-only order: order crosses book")
}

fn is_deterministic_order_error(msg: &str) -> bool {
    let msg_lc = msg.to_ascii_lowercase();
    msg_lc.contains("not enough balance / allowance")
        || msg_lc.contains("decimal places")
        || msg_lc.contains("lower than the minimum: 5")
        || msg_lc.contains("invalid amount for a marketable buy order")
        || msg_lc.contains("invalid amounts")
        || msg_lc.contains("price 0 is too small or too large for the minimum tick size")
        || msg_lc.contains("blocked naked sell")
        || msg_lc.contains("invalid post-only order: order crosses book")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_round_to_tick() {
        assert_eq!(round_to_tick(dec!(0.437), dec!(0.01)), dec!(0.43));
        assert_eq!(round_to_tick(dec!(0.435), dec!(0.01)), dec!(0.43));
        assert_eq!(round_to_tick(dec!(0.44), dec!(0.01)), dec!(0.44));
        assert_eq!(round_to_tick(dec!(0.4567), dec!(0.001)), dec!(0.456));
        assert_eq!(round_to_tick(dec!(0.5), dec!(0.1)), dec!(0.5));
    }

    #[test]
    fn test_quantize_order_size() {
        assert_eq!(quantize_order_size(dec!(4.999999)), dec!(4.99));
        assert_eq!(quantize_order_size(dec!(5.001)), dec!(5.00));
        assert_eq!(quantize_order_size(dec!(0)), dec!(0));
        assert_eq!(quantize_order_size(dec!(-1.5)), dec!(0));
    }

    #[test]
    fn test_deterministic_error_classifier() {
        assert!(is_deterministic_order_error(
            "Order error: Sell order post failed: {\"error\":\"not enough balance / allowance\"}"
        ));
        assert!(is_deterministic_order_error(
            "Order error: Sell order build failed: Size 4.002482 has 6 decimal places. Maximum lot size is 2"
        ));
        assert!(is_deterministic_order_error(
            "Order error: invalid post-only order: order crosses book"
        ));
        assert!(is_deterministic_order_error(
            "Order error: BLOCKED NAKED SELL: tried to sell 5 of token abc but hold 0"
        ));
        assert!(!is_deterministic_order_error("timeout talking to API"));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn round_to_tick_is_multiple(
            price_cents in 1u64..10_000,
            tick_cents in 1u64..100,
        ) {
            let price = Decimal::from(price_cents) / dec!(100);
            let tick = Decimal::from(tick_cents) / dec!(100);
            let result = round_to_tick(price, tick);

            // Result is always a multiple of tick (within Decimal precision)
            let remainder = result % tick;
            prop_assert!(remainder.is_zero(),
                "round_to_tick({}, {}) = {} has remainder {}",
                price, tick, result, remainder);

            // Result <= price (rounds down)
            prop_assert!(result <= price,
                "round_to_tick({}, {}) = {} exceeds input",
                price, tick, result);

            // Distance < tick
            prop_assert!(price - result < tick,
                "round_to_tick({}, {}) = {} is more than one tick below",
                price, tick, result);
        }

        #[test]
        fn quantize_size_bounds(size_hundredths in 0u64..100_000) {
            let size = Decimal::from(size_hundredths) / dec!(100);
            let result = quantize_order_size(size);

            // Result <= input
            prop_assert!(result <= size,
                "quantize({}) = {} exceeds input", size, result);

            // Result has at most 2 decimal places
            let scaled = result * dec!(100);
            prop_assert!(scaled == scaled.floor(),
                "quantize({}) = {} has more than 2 decimal places", size, result);
        }
    }
}
