use super::{base::credential::Credential, download::RangeReaderBuilder};
use log::{error, info, warn};
use notify::{watcher, DebouncedEvent, RecursiveMode, Result as NotifyResult, Watcher};
use once_cell::sync::{Lazy, OnceCell};
use reqwest::blocking::Client as HTTPClient;
use serde::{Deserialize, Serialize};
use std::{
    convert::TryInto,
    env, fs,
    path::{Path, PathBuf},
    sync::{mpsc::channel, RwLock},
    thread::{Builder as ThreadBuilder, JoinHandle},
    time::Duration,
    u64,
};
use tap::prelude::*;

/// 七牛配置信息
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
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
    base_timeout_ms: Option<u64>,
    dial_timeout_ms: Option<u64>,
}
static QINIU_CONFIG: Lazy<RwLock<Option<Config>>> = Lazy::new(|| {
    RwLock::new(load_config()).tap(|_| {
        on_config_updated(|| {
            if let Some(config) = load_config() {
                *QINIU_CONFIG.write().unwrap() = Some(config);
            }
            info!("QINIU_CONFIG reloaded: {:?}", QINIU_CONFIG);
        })
    })
});
pub(super) static HTTP_CLIENT: Lazy<RwLock<HTTPClient>> = Lazy::new(|| {
    RwLock::new(build_http_client()).tap(|_| {
        on_config_updated(|| {
            *HTTP_CLIENT.write().unwrap() = build_http_client();
            info!("HTTP_CLIENT reloaded: {:?}", HTTP_CLIENT);
        })
    })
});

/// 判断当前是否已经启用七牛环境
///
/// 如果当前没有设置 QINIU 环境变量，或加载该环境变量出现错误，则返回 false
#[inline]
pub fn is_qiniu_enabled() -> bool {
    QINIU_CONFIG.read().unwrap().is_some()
}

fn build_http_client() -> HTTPClient {
    let mut base_timeout_ms = 3000u64;
    let mut dial_timeout_ms = 500u64;
    if let Some(config) = QINIU_CONFIG.read().unwrap().as_ref() {
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

const QINIU_ENV: &str = "QINIU";

fn load_config() -> Option<Config> {
    if let Ok(qiniu_config_path) = env::var(QINIU_ENV) {
        if let Ok(qiniu_config) = fs::read(&qiniu_config_path) {
            let qiniu_config: Option<Config> = if qiniu_config_path.ends_with(".toml") {
                toml::from_slice(&qiniu_config).ok()
            } else {
                serde_json::from_slice(&qiniu_config).ok()
            };
            if let Some(qiniu_config) = qiniu_config {
                setup_config_watcher(&qiniu_config_path);
                return Some(qiniu_config);
            } else {
                error!(
                    "Qiniu config file cannot be deserialized: {}",
                    qiniu_config_path
                );
                return None;
            }
        } else {
            error!("Qiniu config file cannot be open: {}", qiniu_config_path);
            return None;
        }
    } else {
        warn!("QINIU Env IS NOT ENABLED");
        return None;
    }

    fn setup_config_watcher(config_path: impl Into<PathBuf>) {
        let config_path = config_path.into();
        static UNIQUE_THREAD: OnceCell<JoinHandle<()>> = OnceCell::new();

        if let Err(err) = UNIQUE_THREAD.get_or_try_init(|| {
            ThreadBuilder::new()
                .name("qiniu-config-watcher".into())
                .spawn(move || {
                    if let Err(err) = setup_config_watcher_inner(&config_path) {
                        error!("Qiniu config file watcher was setup failed: {:?}", err);
                    }
                })
        }) {
            error!(
                "Failed to start thread to watch Qiniu config file: {:?}",
                err
            );
        }

        fn setup_config_watcher_inner(config_path: &Path) -> NotifyResult<()> {
            let (tx, rx) = channel();
            let mut watcher = watcher(tx, Duration::from_millis(500))?;
            watcher.watch(config_path, RecursiveMode::NonRecursive)?;

            info!("Qiniu config file watcher was setup");

            loop {
                match rx.recv() {
                    Ok(event) => match event {
                        DebouncedEvent::Create(_) | DebouncedEvent::Write(_) => {
                            info!("Received event {:?} from Qiniu config file watcher", event);
                            for handle in CONFIG_UPDATE_HANDLERS.read().unwrap().iter() {
                                handle();
                            }
                        }
                        DebouncedEvent::Error(err, _) => {
                            error!(
                                "Received error event from Qiniu config file watcher: {:?}",
                                err
                            );
                        }
                        _ => {}
                    },
                    Err(err) => {
                        error!(
                            "Failed to receive event from Qiniu config file watcher: {:?}",
                            err
                        );
                    }
                }
            }
        }
    }
}

type ConfigUpdateHandler = fn();
type ConfigUpdateHandlers = Vec<ConfigUpdateHandler>;
static CONFIG_UPDATE_HANDLERS: Lazy<RwLock<ConfigUpdateHandlers>> = Lazy::new(Default::default);

pub(super) fn on_config_updated(handle: fn()) {
    CONFIG_UPDATE_HANDLERS.write().unwrap().push(handle);
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

    if let Some(base_timeout_ms) = config.base_timeout_ms {
        if base_timeout_ms > 0 {
            builder = builder.base_timeout(Duration::from_millis(base_timeout_ms));
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
        .read()
        .unwrap()
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
                base_timeout_ms: None,
                dial_timeout_ms: None,
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

    /// 配置域名访问的基础超时时长，默认为 3000 毫秒
    #[inline]
    pub fn base_timeout(mut self, base_timeout: Option<Duration>) -> Self {
        self.inner.base_timeout_ms =
            base_timeout.map(|d| d.as_millis().try_into().unwrap_or(u64::MAX));
        self
    }

    /// 配置域名连接的超时时长，默认为 500 毫秒
    #[inline]
    pub fn connect_timeout(mut self, connect_timeout: Option<Duration>) -> Self {
        self.inner.dial_timeout_ms =
            connect_timeout.map(|d| d.as_millis().try_into().unwrap_or(u64::MAX));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        error::Error,
        fs::{remove_file, OpenOptions},
        io::Write,
        sync::atomic::{AtomicUsize, Ordering::Relaxed},
        thread::sleep,
    };
    use tempfile::Builder as TempFileBuilder;

    #[test]
    fn test_load_config() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        let mut config = Config {
            access_key: "test-ak-1".into(),
            secret_key: "test-sk-1".into(),
            bucket: "test-bucket-1".into(),
            io_urls: Some(vec!["http://io1.com".into(), "http://io2.com".into()]),
            uc_urls: Default::default(),
            sim: Default::default(),
            normalize_key: Default::default(),
            private: Default::default(),
            retry: Default::default(),
            punish_time_s: Default::default(),
            base_timeout_ms: Default::default(),
            dial_timeout_ms: Default::default(),
        };
        let tempfile_path = {
            let mut tempfile = TempFileBuilder::new().suffix(".toml").tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            env::set_var(QINIU_ENV, tempfile.path().as_os_str());
            tempfile.into_temp_path()
        };

        static UPDATED: AtomicUsize = AtomicUsize::new(0);
        UPDATED.store(0, Relaxed);

        let loaded = load_config().unwrap();
        assert_eq!(loaded, config);

        on_config_updated(|| {
            UPDATED.fetch_add(1, Relaxed);
        });
        on_config_updated(|| {
            UPDATED.fetch_add(1, Relaxed);
        });
        on_config_updated(|| {
            UPDATED.fetch_add(1, Relaxed);
        });

        sleep(Duration::from_secs(1));

        config.access_key = "test-ak-2".into();
        config.secret_key = "test-sk-2".into();
        config.bucket = "test-bucket-2".into();

        {
            let mut tempfile = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&tempfile_path)?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
        }

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 3);

        remove_file(tempfile_path)?;

        Ok(())
    }
}
