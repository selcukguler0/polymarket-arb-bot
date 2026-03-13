use alloy::signers::local::PrivateKeySigner;
use polymarket_client_sdk::POLYGON;
use std::str::FromStr;

fn main() {
    dotenvy::dotenv().ok();

    let private_key =
        std::env::var("POLYMARKET_PRIVATE_KEY").expect("POLYMARKET_PRIVATE_KEY not set");
    let wallet_address = std::env::var("WALLET_ADDRESS").expect("WALLET_ADDRESS not set");

    let signer = PrivateKeySigner::from_str(&private_key).expect("Invalid private key");

    let eoa = signer.address();
    let derived = polymarket_client_sdk::derive_proxy_wallet(eoa, POLYGON);

    println!("EOA signer:           {eoa}");
    println!("Proxy (env):          {wallet_address}");
    println!("Derived proxy (SDK):  {derived:?}");
    println!(
        "Derived matches env?  {}",
        derived
            .map(|d| format!("{d:?}").to_lowercase() == wallet_address.to_lowercase())
            .unwrap_or(false)
    );
}
