use thiserror::Error;

#[derive(Error, Debug)]
pub enum BotError {
    #[error("SDK error: {0}")]
    Sdk(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("On-chain error: {0}")]
    OnChain(String),

    #[error("Strategy error: {0}")]
    Strategy(String),

    #[error("Risk limit breached: {0}")]
    RiskLimit(String),

    #[error("Market not found: {0}")]
    MarketNotFound(String),

    #[error("Order error: {0}")]
    Order(String),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Emergency shutdown: {0}")]
    Emergency(String),

    #[error("Channel closed")]
    ChannelClosed,
}

pub type Result<T> = std::result::Result<T, BotError>;
