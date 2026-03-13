use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mut config_path: Option<PathBuf> = None;
    let mut runtime_secs: Option<u64> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--runtime-secs" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--runtime-secs requires a value"))?;
                runtime_secs = Some(
                    value
                        .parse::<u64>()
                        .map_err(|e| anyhow::anyhow!("Invalid --runtime-secs value: {e}"))?,
                );
            }
            other if other.starts_with("--runtime-secs=") => {
                let value = other.trim_start_matches("--runtime-secs=");
                runtime_secs = Some(
                    value
                        .parse::<u64>()
                        .map_err(|e| anyhow::anyhow!("Invalid --runtime-secs value: {e}"))?,
                );
            }
            other if other.starts_with('-') => {
                return Err(anyhow::anyhow!("Unknown flag: {other}"));
            }
            other => {
                if config_path.is_some() {
                    return Err(anyhow::anyhow!("Unexpected extra argument: {other}"));
                }
                config_path = Some(PathBuf::from(other));
            }
        }
    }

    let config_path =
        config_path.unwrap_or_else(|| PathBuf::from("config/complete_set_shadow.toml"));
    polymarket_arb::complete_set::run(&config_path, runtime_secs.map(Duration::from_secs)).await
}
