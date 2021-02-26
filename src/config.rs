use std::{env, fs, time::Duration};

use super::{base::credential::Credential, download::RangeReaderBuilder};
use log::{error, warn};
use once_cell::sync::Lazy;
use reqwest::blocking::Client as HTTPClient;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(alias = "ak")]
    access_key: String,
    #[serde(alias = "sk")]
    secret_key: String,

    bucket: String,

    #[serde(alias = "io_hosts")]
    io_urls: Vec<String>,

    #[serde(alias = "uc_hosts")]
    uc_urls: Option<Vec<String>>,

    sim: Option<bool>,
    private: Option<bool>,
    retry: Option<usize>,
    punish_time_s: Option<u64>,
    base_timeout_ms: Option<u64>,
    dial_timeout_ms: Option<u64>,
}
static QINIU_CONFIG: Lazy<Option<Config>> = Lazy::new(load_config);
pub(super) static HTTP_CLIENT: Lazy<HTTPClient> = Lazy::new(build_http_client);

/// 判断当前是否已经启用七牛环境
///
/// 如果当前没有设置 QINIU 环境变量，或加载该环境变量出现错误，则返回 false
#[inline]
pub fn is_qiniu_enabled() -> bool {
    QINIU_CONFIG.is_some()
}

fn build_http_client() -> HTTPClient {
    let mut base_timeout_ms = 500u64;
    let mut dial_timeout_ms = 500u64;
    if let Some(config) = QINIU_CONFIG.as_ref() {
        if let Some(value) = config.base_timeout_ms {
            if value > 0 {
                base_timeout_ms = value;
            }
        }
        if let Some(value) = config.dial_timeout_ms {
            if value > 0 {
                dial_timeout_ms = value;
            }
        }
    }
    let user_agent = format!("QiniuRustDownload/{}", env!("CARGO_PKG_VERSION"));
    HTTPClient::builder()
        .user_agent(user_agent)
        .connect_timeout(Duration::from_millis(dial_timeout_ms))
        .timeout(Duration::from_millis(base_timeout_ms))
        .pool_max_idle_per_host(5)
        .connection_verbose(true)
        .build()
        .expect("Failed to build Reqwest Client")
}

fn load_config() -> Option<Config> {
    if let Ok(qiniu_config_path) = env::var("QINIU") {
        if let Ok(qiniu_config) = fs::read(&qiniu_config_path) {
            let qiniu_config: Option<Config> = if qiniu_config_path.ends_with(".toml") {
                toml::from_slice(&qiniu_config).ok()
            } else {
                serde_json::from_slice(&qiniu_config).ok()
            };
            if let Some(qiniu_config) = qiniu_config {
                Some(qiniu_config)
            } else {
                error!(
                    "Qiniu config file cannot be deserialized: {}",
                    qiniu_config_path
                );
                None
            }
        } else {
            error!("Qiniu config file cannot be open: {}", qiniu_config_path);
            None
        }
    } else {
        warn!("QINIU Env IS NOT ENABLED");
        None
    }
}

pub(super) fn build_range_reader_builder_from_config(
    key: String,
    config: &Config,
) -> RangeReaderBuilder {
    let mut builder = RangeReaderBuilder::new(
        config.bucket.to_owned(),
        key,
        Credential::new(&config.access_key, &config.secret_key),
        config.io_urls.to_owned(),
    );

    if let Some(uc_urls) = &config.uc_urls {
        if !uc_urls.is_empty() {
            builder = builder.uc_urls(uc_urls.to_owned());
        }
    }
    if let Some(retry) = config.retry {
        if retry > 0 {
            builder = builder.io_tries(retry);
        }
    }

    if let Some(punish_time_s) = config.punish_time_s {
        if punish_time_s > 0 {
            builder = builder.punish_duration(Duration::from_secs(punish_time_s));
        }
    }

    if let Some(base_timeout_ms) = config.base_timeout_ms {
        if base_timeout_ms > 0 {
            builder = builder.base_timeout(Duration::from_millis(base_timeout_ms));
        }
    }

    if let Some(sim) = config.sim {
        builder = builder.use_getfile_api(!sim);
    }

    builder
}

pub(super) fn build_range_reader_builder_from_env(key: String) -> Option<RangeReaderBuilder> {
    QINIU_CONFIG
        .as_ref()
        .map(|qiniu_config| build_range_reader_builder_from_config(key, qiniu_config))
}
