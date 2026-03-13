//! Polymarket Relayer Client — gasless on-chain transactions via Gnosis Safe.
//!
//! Routes merge/redeem/split through Polymarket's relayer instead of direct RPC,
//! so Polymarket pays gas fees. Requires Builder API credentials and a deployed
//! Gnosis Safe wallet (CREATE2-derived from the EOA signer).
//!
//! Reference: @polymarket/builder-relayer-client (TypeScript) and
//! py-builder-relayer-client (Python).

use std::str::FromStr;
use std::time::Duration;

use alloy::hex;
use alloy::primitives::{address, b256, keccak256, Address, Bytes, FixedBytes, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use alloy::sol;
use alloy::sol_types::{SolCall, SolValue};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use tracing::{debug, info, warn};

use crate::error::{BotError, Result};

// ── Constants ──

const RELAYER_URL: &str = "https://relayer-v2.polymarket.com";

/// Gnosis Safe factory on Polygon mainnet.
const SAFE_FACTORY: Address = address!("aacFeEa03eb1561C4e67d661e40682Bd20E3541b");

/// Gnosis Safe MultiSend contract on Polygon mainnet.
const SAFE_MULTISEND: Address = address!("A238CBeb142c10Ef7Ad8442C6D1f9E89e07e7761");

/// CREATE2 init code hash for Safe proxy deployment.
const SAFE_INIT_CODE_HASH: B256 =
    b256!("0x2bce2127ff07fb632d16c8347c4ebf501f4841168bed00d9e6ef715ddb6fcecf");

/// CTF contract (ERC-1155 outcome tokens) on Polygon mainnet.
const CTF_ADDRESS: Address = address!("4D97DCd97eC945f40cF65F87097ACe5EA0476045");

/// NegRisk adapter contract on Polygon mainnet.
const NEG_RISK_ADAPTER: Address = address!("d91E80cF2E7be2e162c6513ceD06f1dD0dA35296");

/// Polygon chain ID.
const CHAIN_ID: u64 = 137;

/// Poll interval for transaction status.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum poll attempts (2s × 100 = 200s timeout).
const MAX_POLL_ATTEMPTS: u32 = 100;

/// HTTP timeout for relayer requests.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

// ── Alloy ABI definitions ──

// We use `sol!` macros to get proper ABI encoding for the CTF calls.

sol! {
    /// CTF mergePositions(address,bytes32,uint256[],uint256)
    function mergePositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] partition,
        uint256 amount
    );

    /// CTF redeemPositions(address,bytes32,bytes32,uint256[])
    function redeemPositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] indexSets
    );

    /// CTF splitPosition(address,bytes32,bytes32,uint256[],uint256)
    function splitPosition(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] partition,
        uint256 amount
    );

    /// ERC20 approve(address,uint256)
    function approve(address spender, uint256 amount);

    /// ERC1155 setApprovalForAll(address,bool)
    function setApprovalForAll(address operator, bool approved);

    /// Safe multiSend(bytes)
    function multiSend(bytes transactions);
}

// ── EIP-712 types for Safe signing ──

// SafeTx EIP-712 struct — we compute the struct hash manually because alloy's
// eip712 derive doesn't support all the fields cleanly.

/// Gnosis Safe transaction struct hash computation.
/// typehash = keccak256("SafeTx(address to,uint256 value,bytes data,uint8 operation,uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,address refundReceiver,uint256 nonce)")
const SAFE_TX_TYPEHASH: B256 =
    b256!("0xbb8310d486368db6bd6f849402fdd73ad53d316b5a4b2644ad6efe0f941286d8");

/// CreateProxy EIP-712 struct for Safe deployment.
/// typehash = keccak256("CreateProxy(address paymentToken,uint256 payment,address paymentReceiver)")
const CREATE_PROXY_TYPEHASH: B256 =
    b256!("0x7e05acff72e33fbd18bc7df7ba5e5e100d2b9e0b4baf3c90ab6a3eb29e667507");

/// EIP-712 domain separator name for the Safe factory.
const FACTORY_DOMAIN_NAME: &str = "Polymarket Contract Proxy Factory";

// ── Types ──

/// A single transaction to execute via the Safe.
#[derive(Debug, Clone)]
pub struct RelayerTransaction {
    /// Target contract address.
    pub to: Address,
    /// ABI-encoded calldata.
    pub data: Bytes,
    /// Value in wei (usually 0).
    pub value: U256,
}

/// Transaction state returned by the relayer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    New,
    Executed,
    Mined,
    Confirmed,
    Failed,
    Invalid,
    Unknown(String),
}

impl From<&str> for TransactionState {
    fn from(s: &str) -> Self {
        match s {
            "STATE_NEW" => Self::New,
            "STATE_EXECUTED" => Self::Executed,
            "STATE_MINED" => Self::Mined,
            "STATE_CONFIRMED" => Self::Confirmed,
            "STATE_FAILED" => Self::Failed,
            "STATE_INVALID" => Self::Invalid,
            other => Self::Unknown(other.to_string()),
        }
    }
}

/// Relayer submit response.
#[derive(Debug, Clone, Deserialize)]
pub struct SubmitResponse {
    #[serde(alias = "transactionId", alias = "transactionID")]
    pub transaction_id: String,
    #[serde(default, alias = "transactionHash")]
    pub transaction_hash: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
}

/// Relayer transaction status response.
#[derive(Debug, Clone, Deserialize)]
pub struct TransactionStatus {
    #[serde(alias = "transactionId", alias = "transactionID")]
    pub transaction_id: String,
    #[serde(default, alias = "transactionHash")]
    pub transaction_hash: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default, alias = "proxyAddress")]
    pub proxy_address: Option<String>,
}

/// Nonce response.
#[derive(Debug, Clone, Deserialize)]
pub struct NonceResponse {
    pub nonce: String,
}

/// Deployed check response.
#[derive(Debug, Clone, Deserialize)]
pub struct DeployedResponse {
    pub deployed: bool,
}

// ── Relayer Client ──

/// Gasless transaction client using Polymarket's relayer infrastructure.
///
/// Deploys and manages a Gnosis Safe wallet, submits transactions via the
/// relayer (Polymarket pays gas), and polls for confirmation.
pub struct RelayerClient {
    /// EOA signer (same private key as CLOB trading).
    signer: PrivateKeySigner,
    /// EOA address.
    eoa_address: Address,
    /// Deterministically derived Safe address.
    safe_address: Address,
    /// USDC.e contract address.
    usdc_address: Address,
    /// Builder API key (UUID string).
    builder_key: String,
    /// Builder API secret (base64-encoded).
    builder_secret: String,
    /// Builder passphrase.
    builder_passphrase: String,
    /// HTTP client.
    http: reqwest::Client,
    /// Base URL for the relayer.
    base_url: String,
    /// Mutex to serialize execute calls (prevents nonce races).
    execute_lock: tokio::sync::Mutex<()>,
}

impl RelayerClient {
    /// Create a new relayer client.
    ///
    /// Derives the Safe address deterministically from the EOA signer.
    pub fn new(
        signer: PrivateKeySigner,
        usdc_address: Address,
        builder_key: String,
        builder_secret: String,
        builder_passphrase: String,
    ) -> Result<Self> {
        let eoa_address = signer.address();
        let safe_address = Self::derive_safe_address(eoa_address);

        info!(
            eoa = %eoa_address,
            safe = %safe_address,
            "Relayer client initialized"
        );

        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|e| BotError::OnChain(format!("Failed to create HTTP client: {e}")))?;

        Ok(Self {
            signer,
            eoa_address,
            safe_address,
            usdc_address,
            builder_key,
            builder_secret,
            builder_passphrase,
            http,
            base_url: RELAYER_URL.to_string(),
            execute_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Get the Safe wallet address.
    pub fn safe_address(&self) -> Address {
        self.safe_address
    }

    /// Get the EOA address.
    pub fn eoa_address(&self) -> Address {
        self.eoa_address
    }

    // ── Safe Address Derivation ──

    /// Derive the deterministic Safe address via CREATE2.
    ///
    /// `address = keccak256(0xff ++ factory ++ salt ++ initCodeHash)[12..]`
    /// where `salt = keccak256(abi.encode(eoa_address))`
    fn derive_safe_address(eoa: Address) -> Address {
        // salt = keccak256(abi.encode(address))
        // abi.encode(address) = left-pad to 32 bytes
        let encoded = eoa.abi_encode();
        let salt = keccak256(&encoded);

        // CREATE2: keccak256(0xff ++ factory ++ salt ++ initCodeHash)
        let mut data = Vec::with_capacity(1 + 20 + 32 + 32);
        data.push(0xff);
        data.extend_from_slice(SAFE_FACTORY.as_slice());
        data.extend_from_slice(salt.as_slice());
        data.extend_from_slice(SAFE_INIT_CODE_HASH.as_slice());

        let hash = keccak256(&data);
        Address::from_slice(&hash[12..])
    }

    // ── HMAC Authentication ──

    /// Build HMAC-SHA256 signature for builder authentication.
    ///
    /// message = timestamp + method + path [+ body]
    fn build_hmac_signature(
        &self,
        timestamp: u64,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<String> {
        let mut message = format!("{timestamp}{method}{path}");
        if let Some(b) = body {
            message.push_str(b);
        }

        let secret_bytes = base64::engine::general_purpose::URL_SAFE
            .decode(&self.builder_secret)
            .map_err(|e| BotError::OnChain(format!("Invalid builder secret (base64): {e}")))?;

        let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)
            .map_err(|e| BotError::OnChain(format!("HMAC init failed: {e}")))?;
        mac.update(message.as_bytes());

        let result = mac.finalize();
        let sig = base64::engine::general_purpose::URL_SAFE.encode(result.into_bytes());

        Ok(sig)
    }

    /// Add builder auth headers to a request.
    fn auth_headers(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<Vec<(String, String)>> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let signature = self.build_hmac_signature(timestamp, method, path, body)?;

        Ok(vec![
            ("POLY_BUILDER_API_KEY".to_string(), self.builder_key.clone()),
            ("POLY_BUILDER_TIMESTAMP".to_string(), timestamp.to_string()),
            (
                "POLY_BUILDER_PASSPHRASE".to_string(),
                self.builder_passphrase.clone(),
            ),
            ("POLY_BUILDER_SIGNATURE".to_string(), signature),
        ])
    }

    // ── HTTP Helpers ──

    async fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .http
            .get(&url)
            .query(query)
            .send()
            .await
            .map_err(|e| BotError::OnChain(format!("Relayer GET {path} failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(BotError::OnChain(format!(
                "Relayer GET {path} returned {status}: {body}"
            )));
        }

        Ok(resp)
    }

    async fn post_authenticated(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.base_url, path);
        let body_str = serde_json::to_string(body)
            .map_err(|e| BotError::OnChain(format!("JSON serialize failed: {e}")))?;

        let headers = self.auth_headers("POST", path, Some(&body_str))?;

        let mut req = self
            .http
            .post(&url)
            .body(body_str.clone())
            .header("Content-Type", "application/json");

        for (key, value) in &headers {
            req = req.header(key, value);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| BotError::OnChain(format!("Relayer POST {path} failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let resp_body = resp.text().await.unwrap_or_default();
            // Log the request body for debugging (truncate data field to avoid noise)
            let debug_body = if body_str.len() > 500 {
                format!("{}...(truncated)", &body_str[..500])
            } else {
                body_str.clone()
            };
            warn!(
                status = %status,
                response = %resp_body,
                request = %debug_body,
                "Relayer POST {path} failed"
            );
            return Err(BotError::OnChain(format!(
                "Relayer POST {path} returned {status}: {resp_body}"
            )));
        }

        Ok(resp)
    }

    // ── Relayer API Methods ──

    /// Check if the Safe wallet is deployed.
    pub async fn is_deployed(&self) -> Result<bool> {
        let resp = self
            .get(
                "/deployed",
                &[("address", &format!("{:?}", self.safe_address))],
            )
            .await?;

        let data: DeployedResponse = resp
            .json()
            .await
            .map_err(|e| BotError::OnChain(format!("Parse deployed response: {e}")))?;

        Ok(data.deployed)
    }

    /// Get the current nonce for our Safe.
    pub async fn get_nonce(&self) -> Result<String> {
        let resp = self
            .get(
                "/nonce",
                &[
                    ("address", &format!("{:?}", self.eoa_address)),
                    ("type", "SAFE"),
                ],
            )
            .await?;

        let data: NonceResponse = resp
            .json()
            .await
            .map_err(|e| BotError::OnChain(format!("Parse nonce response: {e}")))?;

        Ok(data.nonce)
    }

    /// Deploy the Safe wallet via the relayer.
    ///
    /// This is a one-time operation. The Safe is deployed at the deterministic
    /// CREATE2 address derived from the EOA.
    pub async fn deploy_safe(&self) -> Result<B256> {
        // Check if already deployed
        if self.is_deployed().await? {
            info!(safe = %self.safe_address, "Safe already deployed");
            return Err(BotError::OnChain("Safe already deployed".into()));
        }

        info!(safe = %self.safe_address, "Deploying Safe wallet via relayer...");

        // EIP-712 domain for the factory
        let domain_separator = self.factory_domain_separator();

        // CreateProxy struct hash
        let struct_hash = {
            let mut data = Vec::with_capacity(4 * 32);
            data.extend_from_slice(CREATE_PROXY_TYPEHASH.as_slice());
            data.extend_from_slice(&Address::ZERO.abi_encode()); // paymentToken
            data.extend_from_slice(&U256::ZERO.abi_encode()); // payment
            data.extend_from_slice(&Address::ZERO.abi_encode()); // paymentReceiver
            keccak256(&data)
        };

        // EIP-712 hash: keccak256("\x19\x01" ++ domainSeparator ++ structHash)
        let signing_hash = {
            let mut data = Vec::with_capacity(2 + 32 + 32);
            data.extend_from_slice(b"\x19\x01");
            data.extend_from_slice(domain_separator.as_slice());
            data.extend_from_slice(struct_hash.as_slice());
            keccak256(&data)
        };

        // Sign the hash directly (no EIP-191 prefix for SAFE-CREATE)
        let sig = self
            .signer
            .sign_hash(&signing_hash)
            .await
            .map_err(|e| BotError::OnChain(format!("Signing failed: {e}")))?;

        let signature_hex = format!("0x{}", hex::encode(sig.as_bytes()));

        let body = serde_json::json!({
            "type": "SAFE-CREATE",
            "from": format!("{:?}", self.eoa_address),
            "to": format!("{:?}", SAFE_FACTORY),
            "data": "0x",
            "signature": signature_hex,
            "value": "0",
            "signatureParams": {
                "paymentToken": format!("{:?}", Address::ZERO),
                "payment": "0",
                "paymentReceiver": format!("{:?}", Address::ZERO)
            },
            "metadata": "Deploy Safe wallet"
        });

        let resp = self.post_authenticated("/submit", &body).await?;
        let submit: SubmitResponse = resp
            .json()
            .await
            .map_err(|e| BotError::OnChain(format!("Parse submit response: {e}")))?;

        info!(tx_id = %submit.transaction_id, "Safe deploy submitted, waiting for confirmation...");

        let result = self.wait_for_transaction(&submit.transaction_id).await?;

        let tx_hash = result
            .transaction_hash
            .ok_or_else(|| BotError::OnChain("No tx hash in deploy response".into()))?;

        let hash = B256::from_str(&tx_hash)
            .map_err(|e| BotError::OnChain(format!("Invalid tx hash: {e}")))?;

        info!(safe = %self.safe_address, tx = %hash, "Safe deployed successfully");

        Ok(hash)
    }

    /// Execute one or more transactions through the Safe via the relayer.
    ///
    /// If multiple transactions are provided, they are batched via MultiSend.
    /// Polymarket pays gas for the execution.
    pub async fn execute(&self, txns: &[RelayerTransaction], metadata: &str) -> Result<B256> {
        if txns.is_empty() {
            return Err(BotError::OnChain("No transactions to execute".into()));
        }

        // Serialize to prevent nonce races
        let _guard = self.execute_lock.lock().await;

        let nonce = self.get_nonce().await?;

        // Determine target and data based on single vs multi tx
        let (to, data, operation) = if txns.len() == 1 {
            let tx = &txns[0];
            (tx.to, tx.data.clone(), 0u8) // 0 = Call
        } else {
            // MultiSend: pack all transactions
            let packed = self.encode_multi_send(txns);
            let call = multiSendCall {
                transactions: packed,
            };
            let encoded = Bytes::from(call.abi_encode());
            (SAFE_MULTISEND, encoded, 1u8) // 1 = DelegateCall
        };

        // Build SafeTx struct hash
        let struct_hash = self.safe_tx_struct_hash(to, U256::ZERO, &data, operation, &nonce);

        // EIP-712 domain separator for the Safe itself
        let domain_separator = self.safe_domain_separator();

        // EIP-712 signing hash
        let signing_hash = {
            let mut buf = Vec::with_capacity(2 + 32 + 32);
            buf.extend_from_slice(b"\x19\x01");
            buf.extend_from_slice(domain_separator.as_slice());
            buf.extend_from_slice(struct_hash.as_slice());
            keccak256(&buf)
        };

        // Sign with eth_sign prefix (EIP-191 personal message).
        // Both TS and Python Polymarket SDKs use signMessage/encode_defunct which
        // adds "\x19Ethereum Signed Message:\n32" before signing.
        // The Safe contract interprets v=31/32 as "eth_sign signature".
        let sig = self
            .signer
            .sign_message(signing_hash.as_slice())
            .await
            .map_err(|e| BotError::OnChain(format!("Signing failed: {e}")))?;

        // Adjust v value per Gnosis Safe eth_sign convention:
        // v=0/1 (recovery id) → +31, v=27/28 (legacy) → +4
        let sig_bytes = sig.as_bytes();
        let mut adjusted = [0u8; 65];
        adjusted[..64].copy_from_slice(&sig_bytes[..64]); // r, s
        let v = sig_bytes[64];
        adjusted[64] = if v < 27 { v + 31 } else { v + 4 };

        let signature_hex = format!("0x{}", hex::encode(adjusted));

        let body = serde_json::json!({
            "type": "SAFE",
            "from": format!("{:?}", self.eoa_address),
            "to": format!("{:?}", to),
            "proxyWallet": format!("{:?}", self.safe_address),
            "data": format!("0x{}", hex::encode(&data)),
            "signature": signature_hex,
            "value": "0",
            "nonce": nonce,
            "metadata": metadata,
            "signatureParams": {
                "gasPrice": "0",
                "operation": operation.to_string(),
                "safeTxnGas": "0",
                "baseGas": "0",
                "gasToken": format!("{:?}", Address::ZERO),
                "refundReceiver": format!("{:?}", Address::ZERO)
            }
        });

        debug!(
            metadata,
            nonce,
            tx_count = txns.len(),
            to = %to,
            data_len = data.len(),
            operation,
            "Submitting transaction to relayer"
        );

        let resp = self.post_authenticated("/submit", &body).await?;
        let resp_text = resp
            .text()
            .await
            .map_err(|e| BotError::OnChain(format!("Read submit response: {e}")))?;

        debug!(response = %resp_text, "Relayer submit raw response");

        let submit: SubmitResponse = serde_json::from_str(&resp_text).map_err(|e| {
            BotError::OnChain(format!(
                "Parse submit response: {e}\nRaw response: {resp_text}"
            ))
        })?;

        info!(
            tx_id = %submit.transaction_id,
            metadata,
            "Relayer tx submitted, polling for confirmation..."
        );

        let result = self.wait_for_transaction(&submit.transaction_id).await?;

        let tx_hash = result
            .transaction_hash
            .ok_or_else(|| BotError::OnChain("No tx hash in relayer response".into()))?;

        let hash = B256::from_str(&tx_hash)
            .map_err(|e| BotError::OnChain(format!("Invalid tx hash: {e}")))?;

        info!(tx = %hash, metadata, "Relayer transaction confirmed");

        Ok(hash)
    }

    /// Poll for transaction completion.
    async fn wait_for_transaction(&self, tx_id: &str) -> Result<TransactionStatus> {
        for attempt in 0..MAX_POLL_ATTEMPTS {
            tokio::time::sleep(POLL_INTERVAL).await;

            let resp = self.get("/transaction", &[("id", tx_id)]).await?;

            // Response is an array of transactions
            let statuses: Vec<TransactionStatus> = resp
                .json()
                .await
                .map_err(|e| BotError::OnChain(format!("Parse tx status: {e}")))?;

            if let Some(status) = statuses.first() {
                let state = status
                    .state
                    .as_deref()
                    .map(TransactionState::from)
                    .unwrap_or(TransactionState::Unknown("none".into()));

                match state {
                    TransactionState::Mined | TransactionState::Confirmed => {
                        debug!(tx_id, attempt, ?state, "Transaction finalized");
                        return Ok(status.clone());
                    }
                    TransactionState::Failed | TransactionState::Invalid => {
                        return Err(BotError::OnChain(format!(
                            "Relayer transaction {tx_id} failed with state: {state:?}"
                        )));
                    }
                    _ => {
                        if attempt % 5 == 0 {
                            debug!(tx_id, attempt, ?state, "Waiting for tx...");
                        }
                    }
                }
            }
        }

        Err(BotError::OnChain(format!(
            "Relayer transaction {tx_id} timed out after {}s",
            MAX_POLL_ATTEMPTS * 2
        )))
    }

    // ── EIP-712 Helpers ──

    /// EIP-712 domain separator for the Safe factory (used for deploy).
    fn factory_domain_separator(&self) -> B256 {
        // keccak256(abi.encode(
        //   keccak256("EIP712Domain(string name,uint256 chainId,address verifyingContract)"),
        //   keccak256(name),
        //   chainId,
        //   factory
        // ))
        let domain_typehash =
            keccak256("EIP712Domain(string name,uint256 chainId,address verifyingContract)");
        let name_hash = keccak256(FACTORY_DOMAIN_NAME.as_bytes());

        let mut data = Vec::with_capacity(4 * 32);
        data.extend_from_slice(domain_typehash.as_slice());
        data.extend_from_slice(name_hash.as_slice());
        data.extend_from_slice(&U256::from(CHAIN_ID).abi_encode());
        data.extend_from_slice(&SAFE_FACTORY.abi_encode());

        keccak256(&data)
    }

    /// EIP-712 domain separator for the Safe itself (used for execute).
    fn safe_domain_separator(&self) -> B256 {
        // keccak256(abi.encode(
        //   keccak256("EIP712Domain(uint256 chainId,address verifyingContract)"),
        //   chainId,
        //   safeAddress
        // ))
        let domain_typehash = keccak256("EIP712Domain(uint256 chainId,address verifyingContract)");

        let mut data = Vec::with_capacity(3 * 32);
        data.extend_from_slice(domain_typehash.as_slice());
        data.extend_from_slice(&U256::from(CHAIN_ID).abi_encode());
        data.extend_from_slice(&self.safe_address.abi_encode());

        keccak256(&data)
    }

    /// Compute the SafeTx struct hash.
    fn safe_tx_struct_hash(
        &self,
        to: Address,
        value: U256,
        data: &[u8],
        operation: u8,
        nonce: &str,
    ) -> B256 {
        let nonce_u256 = U256::from_str(nonce).unwrap_or(U256::ZERO);
        let data_hash = keccak256(data);

        let mut buf = Vec::with_capacity(11 * 32);
        buf.extend_from_slice(SAFE_TX_TYPEHASH.as_slice());
        buf.extend_from_slice(&to.abi_encode());
        buf.extend_from_slice(&value.abi_encode());
        buf.extend_from_slice(data_hash.as_slice());
        buf.extend_from_slice(&U256::from(operation).abi_encode()); // operation
        buf.extend_from_slice(&U256::ZERO.abi_encode()); // safeTxGas
        buf.extend_from_slice(&U256::ZERO.abi_encode()); // baseGas
        buf.extend_from_slice(&U256::ZERO.abi_encode()); // gasPrice
        buf.extend_from_slice(&Address::ZERO.abi_encode()); // gasToken
        buf.extend_from_slice(&Address::ZERO.abi_encode()); // refundReceiver
        buf.extend_from_slice(&nonce_u256.abi_encode()); // nonce

        keccak256(&buf)
    }

    /// Encode multiple transactions for Gnosis Safe MultiSend.
    ///
    /// Format per transaction: operation (1 byte) + to (20 bytes) + value (32 bytes)
    /// + data_length (32 bytes) + data (variable)
    fn encode_multi_send(&self, txns: &[RelayerTransaction]) -> Bytes {
        let mut packed = Vec::new();

        for tx in txns {
            packed.push(0u8); // operation = Call
            packed.extend_from_slice(tx.to.as_slice()); // to (20 bytes)
            packed.extend_from_slice(&tx.value.to_be_bytes::<32>()); // value (32 bytes, big-endian)
            let data_len = tx.data.len();
            packed.extend_from_slice(&U256::from(data_len).to_be_bytes::<32>()); // data length (32 bytes, big-endian)
            packed.extend_from_slice(&tx.data); // data (variable)
        }

        Bytes::from(packed)
    }

    // ── High-Level CTF Operations ──

    /// Merge YES+NO pairs into USDC via the relayer (gasless).
    ///
    /// `amount` is in whole shares (will be scaled by 10^6 for USDC decimals).
    pub async fn merge_positions(&self, condition_id: B256, amount: u64) -> Result<B256> {
        let amount_scaled = U256::from(amount) * U256::from(1_000_000u64);

        let call = mergePositionsCall {
            collateralToken: self.usdc_address,
            parentCollectionId: FixedBytes::ZERO,
            conditionId: condition_id,
            partition: vec![U256::from(1), U256::from(2)],
            amount: amount_scaled,
        };

        let tx = RelayerTransaction {
            to: CTF_ADDRESS,
            data: Bytes::from(call.abi_encode()),
            value: U256::ZERO,
        };

        info!(
            %condition_id,
            shares = amount,
            "Executing gasless merge via relayer"
        );

        self.execute(&[tx], &format!("merge {amount} shares")).await
    }

    /// Redeem winning positions after resolution via the relayer (gasless).
    pub async fn redeem_positions(&self, condition_id: B256, neg_risk: bool) -> Result<B256> {
        let target = if neg_risk {
            NEG_RISK_ADAPTER
        } else {
            CTF_ADDRESS
        };

        let call = redeemPositionsCall {
            collateralToken: self.usdc_address,
            parentCollectionId: FixedBytes::ZERO,
            conditionId: condition_id,
            indexSets: vec![U256::from(1), U256::from(2)],
        };

        let tx = RelayerTransaction {
            to: target,
            data: Bytes::from(call.abi_encode()),
            value: U256::ZERO,
        };

        info!(
            %condition_id,
            neg_risk,
            "Executing gasless redeem via relayer"
        );

        self.execute(&[tx], "redeem positions").await
    }

    /// Split USDC into YES+NO tokens via the relayer (gasless).
    ///
    /// `amount_usdc_6` is in raw USDC units (6 decimals), e.g. 1_000_000 = $1.00.
    pub async fn split_position(&self, condition_id: B256, amount_usdc_6: u64) -> Result<B256> {
        let call = splitPositionCall {
            collateralToken: self.usdc_address,
            parentCollectionId: FixedBytes::ZERO,
            conditionId: condition_id,
            partition: vec![U256::from(1), U256::from(2)],
            amount: U256::from(amount_usdc_6),
        };

        let tx = RelayerTransaction {
            to: CTF_ADDRESS,
            data: Bytes::from(call.abi_encode()),
            value: U256::ZERO,
        };

        info!(
            %condition_id,
            amount_usdc_6,
            "Executing gasless split via relayer"
        );

        self.execute(&[tx], "split position").await
    }

    /// Approve all required contracts to spend USDC and outcome tokens from the Safe.
    ///
    /// This is a one-time setup after deploying the Safe. Batches all approvals
    /// into a single MultiSend transaction.
    pub async fn approve_all_contracts(
        &self,
        ctf_exchange: Address,
        neg_risk_exchange: Address,
    ) -> Result<B256> {
        let max_uint = U256::MAX;

        // 1. Approve USDC on CTF Exchange
        let approve_usdc_ctf = RelayerTransaction {
            to: self.usdc_address,
            data: Bytes::from(
                approveCall {
                    spender: ctf_exchange,
                    amount: max_uint,
                }
                .abi_encode(),
            ),
            value: U256::ZERO,
        };

        // 2. Approve USDC on NegRisk Exchange
        let approve_usdc_neg = RelayerTransaction {
            to: self.usdc_address,
            data: Bytes::from(
                approveCall {
                    spender: neg_risk_exchange,
                    amount: max_uint,
                }
                .abi_encode(),
            ),
            value: U256::ZERO,
        };

        // 3. Approve USDC on CTF contract (for merge/split)
        let approve_usdc_ctf_contract = RelayerTransaction {
            to: self.usdc_address,
            data: Bytes::from(
                approveCall {
                    spender: CTF_ADDRESS,
                    amount: max_uint,
                }
                .abi_encode(),
            ),
            value: U256::ZERO,
        };

        // 4. Approve USDC on NegRisk adapter (for neg-risk merge/split)
        let approve_usdc_neg_adapter = RelayerTransaction {
            to: self.usdc_address,
            data: Bytes::from(
                approveCall {
                    spender: NEG_RISK_ADAPTER,
                    amount: max_uint,
                }
                .abi_encode(),
            ),
            value: U256::ZERO,
        };

        // 5. Approve CTF tokens (ERC1155) for CTF Exchange
        let approve_1155_ctf = RelayerTransaction {
            to: CTF_ADDRESS,
            data: Bytes::from(
                setApprovalForAllCall {
                    operator: ctf_exchange,
                    approved: true,
                }
                .abi_encode(),
            ),
            value: U256::ZERO,
        };

        // 6. Approve CTF tokens for NegRisk Exchange
        let approve_1155_neg = RelayerTransaction {
            to: CTF_ADDRESS,
            data: Bytes::from(
                setApprovalForAllCall {
                    operator: neg_risk_exchange,
                    approved: true,
                }
                .abi_encode(),
            ),
            value: U256::ZERO,
        };

        // 7. Approve CTF tokens for NegRisk adapter
        let approve_1155_adapter = RelayerTransaction {
            to: CTF_ADDRESS,
            data: Bytes::from(
                setApprovalForAllCall {
                    operator: NEG_RISK_ADAPTER,
                    approved: true,
                }
                .abi_encode(),
            ),
            value: U256::ZERO,
        };

        let txns = vec![
            approve_usdc_ctf,
            approve_usdc_neg,
            approve_usdc_ctf_contract,
            approve_usdc_neg_adapter,
            approve_1155_ctf,
            approve_1155_neg,
            approve_1155_adapter,
        ];

        info!("Approving all contracts via relayer (batched MultiSend)...");

        self.execute(&txns, "approve all contracts").await
    }
}
