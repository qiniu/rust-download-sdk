use std::{env, fs, time::Duration};

use super::{base::credential::Credential, download::RangeReaderBuilder};
use log::{error, warn};
use once_cell::sync::Lazy;
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
}
static QINIU_CONFIG: Lazy<Option<Config>> = Lazy::new(load_config);

/// 判断当前是否已经启用七牛环境
///
/// 如果当前没有设置 QINIU 环境变量，或加载该环境变量出现错误，则返回 false
#[inline]
pub fn is_qiniu_enabled() -> bool {
    QINIU_CONFIG.is_some()
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
    key: &str,
    config: &Config,
) -> RangeReaderBuilder {
    RangeReaderBuilder::new(
        &config.bucket,
        key,
        config.uc_urls.as_deref(),
        config.io_urls.as_ref(),
        &Credential::new(&config.access_key, &config.secret_key),
        !config.sim.unwrap_or(false),
        config
            .private
            .unwrap_or(false)
            .then(|| Duration::from_secs(3600 * 24)),
        5,
    )
}

pub(super) fn build_range_reader_builder_from_env(key: &str) -> Option<RangeReaderBuilder> {
    QINIU_CONFIG
        .as_ref()
        .map(|qiniu_config| build_range_reader_builder_from_config(key, qiniu_config))
}
