// Library crate: expose modules needed by auxiliary binaries.
// main.rs keeps its own `mod` declarations for the full module tree.

pub mod complete_set;
pub mod config;
pub mod error;
pub mod paper_sim;
pub mod relayer;
pub mod run_manifest;
pub mod sdk;
pub mod strategies;
pub mod types;
