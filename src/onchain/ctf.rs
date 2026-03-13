use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::sol;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::str::FromStr;
use tracing::{info, warn};

use crate::error::{BotError, Result};

// ABI definitions for on-chain contracts
sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
    }
}

sol! {
    #[sol(rpc)]
    interface IERC1155 {
        function balanceOf(address account, uint256 id) external view returns (uint256);
    }
}

// Well-known Polygon mainnet addresses
// FIX 13: Use correct distinct addresses for standard vs NegRisk exchange.
// Source: polymarket-client-sdk lib.rs CONFIG / NEG_RISK_CONFIG for chain_id 137.
const USDC_E_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
/// Standard CTF Exchange (non-neg-risk markets)
const CTF_EXCHANGE_ADDRESS: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
/// NegRisk CTF Exchange (neg-risk markets)
const NEG_RISK_CTF_EXCHANGE_ADDRESS: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";
/// NegRisk adapter contract
const NEG_RISK_ADAPTER_ADDRESS: &str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";

/// Manages on-chain interactions with Polygon contracts.
/// Uses alloy for direct contract calls (balance checks, approvals).
/// The SDK's CTF module handles split/merge/redeem operations.
pub struct OnChainManager {
    wallet_address: Address,
    usdc_address: Address,
    ctf_address: Address,
    ctf_exchange_address: Address,
    neg_risk_exchange_address: Address,
    rpc_url: String,
    // Cache balances with timestamps to avoid excessive RPC calls
    cached_usdc_balance: parking_lot::RwLock<(Decimal, std::time::Instant)>,
    cached_matic_balance: parking_lot::RwLock<(Decimal, std::time::Instant)>,
}

const BALANCE_CACHE_TTL_SECS: u64 = 30;

impl OnChainManager {
    pub fn new(
        wallet_address: &str,
        rpc_url: &str,
        usdc_addr: &str,
        ctf_exchange_addr: &str,
        neg_risk_exchange_addr: &str,
    ) -> Result<Self> {
        let wallet_addr = Address::from_str(wallet_address)
            .map_err(|e| BotError::Config(format!("Invalid wallet address: {e}")))?;

        Ok(Self {
            wallet_address: wallet_addr,
            usdc_address: Address::from_str(usdc_addr)
                .map_err(|e| BotError::Config(format!("Invalid USDC address: {e}")))?,
            ctf_address: Address::from_str(CTF_ADDRESS).unwrap(),
            ctf_exchange_address: Address::from_str(ctf_exchange_addr)
                .map_err(|e| BotError::Config(format!("Invalid CTF exchange address: {e}")))?,
            neg_risk_exchange_address: Address::from_str(neg_risk_exchange_addr)
                .map_err(|e| BotError::Config(format!("Invalid NegRisk exchange address: {e}")))?,
            rpc_url: rpc_url.to_string(),
            // Initialize cache as expired so first read triggers an RPC call
            cached_usdc_balance: parking_lot::RwLock::new((
                Decimal::ZERO,
                std::time::Instant::now()
                    - std::time::Duration::from_secs(BALANCE_CACHE_TTL_SECS + 1),
            )),
            cached_matic_balance: parking_lot::RwLock::new((
                Decimal::ZERO,
                std::time::Instant::now()
                    - std::time::Duration::from_secs(BALANCE_CACHE_TTL_SECS + 1),
            )),
        })
    }

    fn provider(&self) -> Result<impl Provider> {
        let url = self
            .rpc_url
            .parse()
            .map_err(|e| BotError::OnChain(format!("Invalid RPC URL: {e}")))?;
        let provider = ProviderBuilder::new().connect_http(url);
        Ok(provider)
    }

    /// Convert U256 with 6 decimals (USDC) to Decimal
    fn u256_to_decimal_6(value: U256) -> Decimal {
        let s = value.to_string();
        match Decimal::from_str(&s) {
            Ok(d) => d / dec!(1_000_000),
            Err(_) => {
                // U256::MAX overflows Decimal — treat as unlimited approval
                Decimal::MAX
            }
        }
    }

    /// Convert U256 with 18 decimals (MATIC) to Decimal
    fn u256_to_decimal_18(value: U256) -> Decimal {
        let s = value.to_string();
        match Decimal::from_str(&s) {
            Ok(d) => d / dec!(1_000_000_000_000_000_000),
            Err(_) => {
                // U256::MAX overflows Decimal — treat as unlimited
                Decimal::MAX
            }
        }
    }

    /// Get USDC.e balance (with caching)
    pub async fn get_usdc_balance(&self) -> Result<Decimal> {
        // Check cache
        {
            let cache = self.cached_usdc_balance.read();
            if cache.1.elapsed().as_secs() < BALANCE_CACHE_TTL_SECS {
                return Ok(cache.0);
            }
        }

        let provider = self.provider()?;
        let contract = IERC20::new(self.usdc_address, &provider);
        let result = contract
            .balanceOf(self.wallet_address)
            .call()
            .await
            .map_err(|e| BotError::OnChain(format!("USDC balanceOf failed: {e}")))?;

        let balance = Self::u256_to_decimal_6(result);

        // Update cache
        {
            let mut cache = self.cached_usdc_balance.write();
            *cache = (balance, std::time::Instant::now());
        }

        Ok(balance)
    }

    /// Get MATIC balance (native token, with caching)
    pub async fn get_matic_balance(&self) -> Result<Decimal> {
        // Check cache
        {
            let cache = self.cached_matic_balance.read();
            if cache.1.elapsed().as_secs() < BALANCE_CACHE_TTL_SECS {
                return Ok(cache.0);
            }
        }

        let provider = self.provider()?;
        let raw = provider
            .get_balance(self.wallet_address)
            .await
            .map_err(|e| BotError::OnChain(format!("MATIC balance failed: {e}")))?;

        let balance = Self::u256_to_decimal_18(raw);

        // Update cache
        {
            let mut cache = self.cached_matic_balance.write();
            *cache = (balance, std::time::Instant::now());
        }

        Ok(balance)
    }

    /// Get ERC1155 token balance (CTF outcome tokens)
    pub async fn get_token_balance(&self, token_id: &str) -> Result<Decimal> {
        let provider = self.provider()?;
        let contract = IERC1155::new(self.ctf_address, &provider);

        let id = U256::from_str(token_id)
            .map_err(|e| BotError::OnChain(format!("Invalid token ID: {e}")))?;

        let result = contract
            .balanceOf(self.wallet_address, id)
            .call()
            .await
            .map_err(|e| BotError::OnChain(format!("Token balanceOf failed: {e}")))?;

        // CTF tokens don't have decimals (they represent whole outcome shares)
        let s = result.to_string();
        Ok(Decimal::from_str(&s).unwrap_or_default())
    }

    /// Check USDC allowance on the CTF Exchange contract
    pub async fn check_usdc_allowance(&self, neg_risk: bool) -> Result<Decimal> {
        let provider = self.provider()?;
        let contract = IERC20::new(self.usdc_address, &provider);

        let spender = if neg_risk {
            self.neg_risk_exchange_address
        } else {
            self.ctf_exchange_address
        };

        let result = contract
            .allowance(self.wallet_address, spender)
            .call()
            .await
            .map_err(|e| BotError::OnChain(format!("Allowance check failed: {e}")))?;

        Ok(Self::u256_to_decimal_6(result))
    }

    /// Verify on-chain setup: sufficient balances and approvals
    pub async fn verify_setup(&self) -> Result<()> {
        let usdc = self.get_usdc_balance().await?;
        let matic = self.get_matic_balance().await?;

        info!(%usdc, %matic, "On-chain balances");

        if usdc <= Decimal::ZERO {
            warn!("USDC.e balance is zero — cannot trade");
        }

        if matic < dec!(0.1) {
            warn!(%matic, "MATIC balance low — need at least 0.1 MATIC for gas");
        }

        // Check allowances
        let allowance_standard = self.check_usdc_allowance(false).await?;
        let allowance_neg_risk = self.check_usdc_allowance(true).await?;

        info!(
            %allowance_standard,
            %allowance_neg_risk,
            "USDC allowances on exchange contracts"
        );

        let mut missing_approvals = Vec::new();
        if allowance_standard <= Decimal::ZERO {
            missing_approvals.push("CTF Exchange");
        }
        if allowance_neg_risk <= Decimal::ZERO {
            missing_approvals.push("NegRisk CTF Exchange");
        }

        if !missing_approvals.is_empty() {
            let msg = format!(
                "USDC not approved on: {}. Run approval before starting in live mode.",
                missing_approvals.join(", ")
            );
            return Err(BotError::OnChain(msg));
        }

        Ok(())
    }

    /// Get the exchange contract address for a market (depends on neg_risk flag)
    pub fn exchange_address(&self, neg_risk: bool) -> Address {
        if neg_risk {
            self.neg_risk_exchange_address
        } else {
            self.ctf_exchange_address
        }
    }

    /// Invalidate cached balances (e.g., after a transaction)
    pub fn invalidate_balance_cache(&self) {
        {
            let mut cache = self.cached_usdc_balance.write();
            cache.1 = std::time::Instant::now()
                - std::time::Duration::from_secs(BALANCE_CACHE_TTL_SECS + 1);
        }
        {
            let mut cache = self.cached_matic_balance.write();
            cache.1 = std::time::Instant::now()
                - std::time::Duration::from_secs(BALANCE_CACHE_TTL_SECS + 1);
        }
    }

    /// Get cached USDC balance (non-async, for use in synchronous closures)
    pub fn cached_usdc_balance_value(&self) -> Decimal {
        self.cached_usdc_balance.read().0
    }

    /// Get cached MATIC balance (non-async, for use in synchronous closures)
    pub fn cached_matic_balance_value(&self) -> Decimal {
        self.cached_matic_balance.read().0
    }

    /// Get the wallet address
    pub fn wallet_address(&self) -> Address {
        self.wallet_address
    }

    /// Get the RPC URL (for passing to SDK CTF operations)
    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }
}
