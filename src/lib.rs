#![forbid(unsafe_code)]

use anyhow::Result;

use crate::{common::shutdown::Shutdown, config::config::load_config};

pub mod common;
pub mod config;
pub mod core;
pub mod network;

pub async fn start() -> Result<()> {
    start_with(Shutdown::new()).await
}

pub async fn start_with(shutdown: Shutdown) -> Result<()> {
    let app = load_config()?;
    let addr = format!("{}:{}", app.config.host, app.config.port);
    let listener = network::net::bind(&addr).await?;
    network::net::serve(listener, app, shutdown).await
}
