use std::fs;

use anyhow::{Context, Ok, Result};
use serde::{Deserialize, Serialize};

use crate::common::utils::{MAX_TOKEN_LEN, token_len_is_valid};

const CONFIG_PATH: &str = "./config/stan.yml";
const TOKEN_ENV_VAR: &str = "STAN_AUTH_TOKEN";

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Config {
    pub server_id: String,
    pub version: String,
    pub runtime: String,
    pub host: String,
    pub port: u32,
    pub max_payload: u64,
    pub max_control_line: u32,
    pub tls_required: bool,
    pub auth_required: bool,
}

impl Config {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.server_id.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(self.version.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(self.runtime.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(self.host.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.port.to_be_bytes());
        buf.extend_from_slice(&self.max_payload.to_be_bytes());
        buf.extend_from_slice(&self.max_control_line.to_be_bytes());
        buf.push(self.tls_required.into());
        buf.push(self.auth_required.into());
        buf.extend_from_slice(self.server_id.as_bytes());
        buf.extend_from_slice(self.version.as_bytes());
        buf.extend_from_slice(self.runtime.as_bytes());
        buf.extend_from_slice(self.host.as_bytes());
        buf
    }
}

pub struct AppConfig {
    pub config: Config,
    pub token: Option<String>,
}

pub fn load_config() -> Result<AppConfig> {
    let content = fs::read_to_string(CONFIG_PATH)
        .with_context(|| format!("Failed to read file:{}", CONFIG_PATH))?;
    let config: Config =
        serde_yaml::from_str(&content).with_context(|| "Failed to parse config file")?;
    let mut token = None;
    if config.auth_required {
        let env = std::env::var(TOKEN_ENV_VAR).with_context(||format!("The config field 'auth_required was set to true but no token was found in env. Ensure it is set with the name {}",TOKEN_ENV_VAR))?;
        anyhow::ensure!(
            token_len_is_valid(env.len()),
            "{} is {} bytes, which exceeds the {MAX_TOKEN_LEN} byte limit",
            TOKEN_ENV_VAR,
            env.len()
        );
        token = Some(env);
    }
    let app = AppConfig { config, token };
    Ok(app)
}
