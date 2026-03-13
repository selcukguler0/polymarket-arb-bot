use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config/complete_set_shadow.toml"));
    let out_path = polymarket_arb::complete_set::capture_live_snapshot(&config_path).await?;
    println!("{}", out_path.display());
    Ok(())
}
