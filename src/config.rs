use super::{base::credential::Credential, download::RangeReaderBuilder};
use isahc::{config::Configurable, http::header::USER_AGENT, HttpClient};
use log::{error, warn};
use num_cpus::get as get_num_cpus;
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::{convert::TryInto, env, fs, time::Duration, u64};

/// 七牛配置信息
#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(alias = "ak")]
    access_key: String,
    #[serde(alias = "sk")]
    secret_key: String,

    bucket: String,

    #[serde(alias = "io_hosts")]
    io_urls: Option<Vec<String>>,

    #[serde(alias = "uc_hosts")]
    uc_urls: Option<Vec<String>>,

    sim: Option<bool>,
    normalize_key: Option<bool>,
    private: Option<bool>,
    retry: Option<usize>,
    punish_time_s: Option<u64>,
    dial_timeout_ms: Option<u64>,
    low_speed_time_s: Option<u64>,
    base_low_speed_limit: Option<u32>,
}
static QINIU_CONFIG: Lazy<Option<Config>> = Lazy::new(load_config);
pub(super) static HTTP_CLIENT: Lazy<HttpClient> = Lazy::new(build_http_client);

/// 判断当前是否已经启用七牛环境
///
/// 如果当前没有设置 QINIU 环境变量，或加载该环境变量出现错误，则返回 false
#[inline]
pub fn is_qiniu_enabled() -> bool {
    QINIU_CONFIG.is_some()
}

fn build_http_client() -> HttpClient {
    let mut dial_timeout_ms = 500u64;
    if let Some(config) = QINIU_CONFIG.as_ref() {
        if let Some(value) = config.dial_timeout_ms {
            if value > 0 {
                dial_timeout_ms = value;
            }
        }
    }
    let user_agent = format!("QiniuRustDownload/{}", env!("CARGO_PKG_VERSION"));
    HttpClient::builder()
        .connection_cache_size(get_num_cpus() * 2)
        .default_header(USER_AGENT, user_agent)
        .connect_timeout(Duration::from_millis(dial_timeout_ms))
        .tcp_keepalive(Duration::from_secs(60))
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
        config
            .io_urls
            .as_ref()
            .map(|urls| urls.to_owned())
            .unwrap_or_default(),
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
    if let Some(low_speed_time_s) = config.low_speed_time_s {
        if low_speed_time_s > 0 {
            builder = builder.low_speed_duration(Duration::from_secs(low_speed_time_s));
        }
    }
    if let Some(base_low_speed_limit) = config.base_low_speed_limit {
        if base_low_speed_limit > 0 {
            builder = builder.base_low_speed_limit(base_low_speed_limit);
        }
    }

    if let Some(sim) = config.sim {
        builder = builder.use_getfile_api(!sim);
    }

    if let Some(normalize_key) = config.normalize_key {
        builder = builder.normalize_key(normalize_key);
    }

    builder
}

pub(super) fn build_range_reader_builder_from_env(key: String) -> Option<RangeReaderBuilder> {
    QINIU_CONFIG
        .as_ref()
        .map(|qiniu_config| build_range_reader_builder_from_config(key, qiniu_config))
}

impl Config {
    /// 创建七牛配置信息构建器
    pub fn builder(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        bucket: impl Into<String>,
        io_urls: Option<Vec<String>>,
    ) -> ConfigBuilder {
        ConfigBuilder::new(access_key, secret_key, bucket, io_urls)
    }
}

/// 七牛配置信息构建器
#[derive(Debug)]
pub struct ConfigBuilder {
    inner: Config,
}

impl ConfigBuilder {
    /// 创建七牛配置信息构建器
    pub fn new(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        bucket: impl Into<String>,
        io_urls: Option<Vec<String>>,
    ) -> Self {
        Self {
            inner: Config {
                access_key: access_key.into(),
                secret_key: secret_key.into(),
                bucket: bucket.into(),
                io_urls,
                uc_urls: None,
                sim: None,
                normalize_key: None,
                private: None,
                retry: None,
                punish_time_s: None,
                dial_timeout_ms: None,
                low_speed_time_s: None,
                base_low_speed_limit: None,
            },
        }
    }

    /// 构建七牛配置信息
    #[inline]
    pub fn build(self) -> Config {
        self.inner
    }

    /// 配置 UC 服务器域名列表
    #[inline]
    pub fn uc_urls(mut self, uc_urls: Option<Vec<String>>) -> Self {
        self.inner.uc_urls = uc_urls;
        self
    }

    /// 是否使用 Getfile API，默认为 true
    #[inline]
    pub fn use_getfile_api(mut self, use_getfile_api: Option<bool>) -> Self {
        self.inner.sim = use_getfile_api.map(|b| !b);
        self
    }

    /// 是否对 key 进行格式化，默认为 false
    #[inline]
    pub fn normalize_key(mut self, normalize_key: Option<bool>) -> Self {
        self.inner.normalize_key = normalize_key;
        self
    }

    /// 是否使用私有存储空间，默认不使用
    #[inline]
    pub fn private(mut self, private: Option<bool>) -> Self {
        self.inner.private = private;
        self
    }

    /// 配置 IO 和 UC 服务器访问重试次数，默认为 10
    #[inline]
    pub fn retry(mut self, retry: Option<usize>) -> Self {
        self.inner.retry = retry;
        self
    }

    /// 配置域名访问失败后的惩罚时长，默认为 30 分钟
    #[inline]
    pub fn punish_duration(mut self, punish_duration: Option<Duration>) -> Self {
        self.inner.punish_time_s = punish_duration.map(|d| d.as_secs());
        self
    }

    /// 配置域名连接的超时时长，默认为 500 毫秒
    #[inline]
    pub fn connect_timeout(mut self, connect_timeout: Option<Duration>) -> Self {
        self.inner.dial_timeout_ms =
            connect_timeout.map(|d| d.as_millis().try_into().unwrap_or(u64::MAX));
        self
    }

    /// 配置低速网络基础阈值，默认为最低 1 MB/秒，如果持续三秒则终止
    #[inline]
    pub fn base_low_speed(mut self, duration: Duration, base_limit_bytes_per_second: u32) -> Self {
        self.inner.low_speed_time_s = Some(duration.as_secs());
        self.inner.base_low_speed_limit = Some(base_limit_bytes_per_second);
        self
    }
}
