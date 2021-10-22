use super::{base::credential::Credential, download::RangeReaderBuilder};
use log::{error, info, warn};
use notify::{watcher, DebouncedEvent, RecursiveMode, Result as NotifyResult, Watcher};
use once_cell::sync::{Lazy, OnceCell};
use reqwest::blocking::Client as HTTPClient;
use serde::{Deserialize, Serialize};
use std::{
    convert::TryInto,
    env, fs,
    io::Result as IOResult,
    path::{Path, PathBuf},
    sync::{mpsc::channel, RwLock},
    thread::{Builder as ThreadBuilder, JoinHandle},
    time::Duration,
    u64,
};
use tap::prelude::*;

/// 七牛配置信息
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug, Default)]
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

    #[serde(alias = "monitor_hosts")]
    monitor_urls: Option<Vec<String>>,

    sim: Option<bool>,
    normalize_key: Option<bool>,
    private: Option<bool>,
    retry: Option<usize>,
    dot_interval_s: Option<u64>,
    max_dot_buffer_size: Option<u64>,
    punish_time_s: Option<u64>,
    base_timeout_ms: Option<u64>,
    dial_timeout_ms: Option<u64>,
}
static QINIU_CONFIG: Lazy<RwLock<Option<Config>>> = Lazy::new(|| RwLock::new(load_config()));

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

/// 手动设置七牛环境配置
#[inline]
pub fn set_qiniu_config(config: Config) {
    set_config_and_reload(config)
}

fn build_http_client() -> HTTPClient {
    let mut base_timeout_ms = 3000u64;
    let mut dial_timeout_ms = 50u64;
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
const QINIU_DISABLE_CONFIG_HOT_RELOADING_ENV: &str = "QINIU_DISABLE_CONFIG_HOT_RELOADING";

fn load_config() -> Option<Config> {
    if let Ok(qiniu_config_path) = env::var(QINIU_ENV) {
        if let Ok(qiniu_config) = fs::read(&qiniu_config_path) {
            let qiniu_config: Option<Config> = if qiniu_config_path.ends_with(".toml") {
                toml::from_slice(&qiniu_config).ok()
            } else {
                serde_json::from_slice(&qiniu_config).ok()
            };
            if let Some(qiniu_config) = qiniu_config {
                if env::var_os(QINIU_DISABLE_CONFIG_HOT_RELOADING_ENV).is_none() {
                    setup_config_watcher(&qiniu_config_path).ok();
                }
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

fn setup_config_watcher(config_path: impl Into<PathBuf>) -> IOResult<()> {
    let config_path = config_path
        .into()
        .canonicalize()
        .tap_err(|err| warn!("Failed to canonicalize config path: {:?}", err))?;

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

    return Ok(());

    fn setup_config_watcher_inner(config_path: &Path) -> NotifyResult<()> {
        let (tx, rx) = channel();
        let mut watcher = watcher(tx, Duration::from_millis(500))?;
        watcher.watch(
            config_path.parent().unwrap_or_else(|| Path::new("/")),
            RecursiveMode::NonRecursive,
        )?;

        info!("Qiniu config file watcher was setup");

        loop {
            match rx.recv() {
                Ok(event) => match event {
                    DebouncedEvent::Create(ref path) if path == config_path => {
                        event_received(event);
                    }
                    DebouncedEvent::Write(ref path) if path == config_path => {
                        event_received(event);
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

    #[inline]
    fn event_received(event: DebouncedEvent) {
        info!("Received event {:?} from Qiniu config file watcher", event);
        reload_config();
    }
}

#[inline]
fn reload_config() {
    if let Some(config) = load_config() {
        set_config_and_reload(config);
    }
}

#[inline]
fn set_config_and_reload(config: Config) {
    *QINIU_CONFIG.write().unwrap() = Some(config);
    info!("QINIU_CONFIG reloaded: {:?}", QINIU_CONFIG);
    for handle in CONFIG_UPDATE_HANDLERS.read().unwrap().iter() {
        handle();
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

    if let Some(monitor_urls) = &config.monitor_urls {
        if !monitor_urls.is_empty() {
            builder = builder.monitor_urls(monitor_urls.to_owned());
        }
    }

    if let Some(retry) = config.retry {
        if retry > 0 {
            builder = builder.io_tries(retry).dot_tries(retry);
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

    if let Some(dot_interval_s) = config.dot_interval_s {
        if dot_interval_s > 0 {
            builder = builder.dot_interval(Duration::from_secs(dot_interval_s));
        }
    }

    if let Some(max_dot_buffer_size) = config.max_dot_buffer_size {
        if max_dot_buffer_size > 0 {
            builder = builder.max_dot_buffer_size(max_dot_buffer_size);
        }
    }

    if let Some(true) = config.private {
        builder = builder.private_url_lifetime(Some(Duration::from_secs(3600)));
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
                ..Default::default()
            },
        }
    }

    /// 配置七牛 Access Key
    #[inline]
    pub fn access_key(mut self, access_key: impl Into<String>) -> Self {
        self.inner.access_key = access_key.into();
        self
    }

    /// 配置七牛 Secret Key
    #[inline]
    pub fn secret_key(mut self, secret_key: impl Into<String>) -> Self {
        self.inner.secret_key = secret_key.into();
        self
    }

    /// 配置七牛存储空间
    #[inline]
    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.inner.bucket = bucket.into();
        self
    }

    /// 配置 IO 服务器 URL 列表
    #[inline]
    pub fn io_urls(mut self, io_urls: Option<Vec<String>>) -> Self {
        self.inner.io_urls = io_urls;
        self
    }

    /// 构建七牛配置信息
    #[inline]
    pub fn build(self) -> Config {
        self.inner
    }

    /// 配置 UC 服务器 URL 列表
    #[inline]
    pub fn uc_urls(mut self, uc_urls: Option<Vec<String>>) -> Self {
        self.inner.uc_urls = uc_urls;
        self
    }

    /// 配置监控服务器 URL 列表，如果不配置或配置为空，则不会启用打点功能
    #[inline]
    pub fn monitor_urls(mut self, monitor_urls: Option<Vec<String>>) -> Self {
        self.inner.monitor_urls = monitor_urls;
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

    /// 配置域名连接的超时时长，默认为 50 毫秒
    #[inline]
    pub fn connect_timeout(mut self, connect_timeout: Option<Duration>) -> Self {
        self.inner.dial_timeout_ms =
            connect_timeout.map(|d| d.as_millis().try_into().unwrap_or(u64::MAX));
        self
    }

    /// 设置打点记录上传频率，默认为 10 秒
    #[inline]
    pub fn dot_interval(mut self, dot_interval: Option<Duration>) -> Self {
        self.inner.dot_interval_s = dot_interval.map(|d| d.as_secs());
        self
    }

    /// 设置打点记录本地缓存文件尺寸上限，默认为 1 MB
    #[inline]
    pub fn max_dot_buffer_size(mut self, max_dot_buffer_size: Option<u64>) -> Self {
        self.inner.max_dot_buffer_size = max_dot_buffer_size;
        self
    }
}

impl From<Config> for ConfigBuilder {
    #[inline]
    fn from(config: Config) -> Self {
        ConfigBuilder { inner: config }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::{
        fs::{remove_file, rename, OpenOptions},
        io::Write,
        sync::atomic::{AtomicUsize, Ordering::Relaxed},
        thread::sleep,
    };
    use tempfile::Builder as TempFileBuilder;

    #[test]
    fn test_load_config() -> Result<()> {
        env_logger::try_init().ok();

        let mut config = Config {
            access_key: "test-ak-1".into(),
            secret_key: "test-sk-1".into(),
            bucket: "test-bucket-1".into(),
            io_urls: Some(vec!["http://io1.com".into(), "http://io2.com".into()]),
            ..Default::default()
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

        config.access_key = "test-ak-3".into();
        config.secret_key = "test-sk-3".into();
        config.bucket = "test-bucket-3".into();

        {
            remove_file(&tempfile_path)?;
            let mut tempfile = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&tempfile_path)?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
        }

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 6);

        {
            let new_tempfile_path = {
                let mut new_path = tempfile_path.to_owned().into_os_string();
                new_path.push(".tmp");
                new_path
            };
            let mut tempfile = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&new_tempfile_path)?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            rename(&new_tempfile_path, &tempfile_path)?;
        }

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 9);

        {
            let new_tempfile_path = {
                let mut new_path = tempfile_path.to_owned().into_os_string();
                new_path.push(".tmp");
                new_path
            };
            let mut tempfile = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&new_tempfile_path)?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            remove_file(&tempfile_path)?;
            rename(&new_tempfile_path, &tempfile_path)?;
        }

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 12);

        remove_file(&tempfile_path)?;

        Ok(())
    }

    #[test]
    fn test_set_config() {
        env_logger::try_init().ok();

        let mut config = Config {
            access_key: "test-ak-1".into(),
            secret_key: "test-sk-1".into(),
            bucket: "test-bucket-1".into(),
            io_urls: Some(vec!["http://io1.com".into(), "http://io2.com".into()]),
            ..Default::default()
        };

        static UPDATED: AtomicUsize = AtomicUsize::new(0);
        UPDATED.store(0, Relaxed);

        on_config_updated(|| {
            UPDATED.fetch_add(1, Relaxed);
        });
        on_config_updated(|| {
            UPDATED.fetch_add(1, Relaxed);
        });
        on_config_updated(|| {
            UPDATED.fetch_add(1, Relaxed);
        });

        set_qiniu_config(config.to_owned());
        assert_eq!(UPDATED.load(Relaxed), 3);

        config.access_key = "test-ak-2".into();
        config.secret_key = "test-sk-2".into();
        config.bucket = "test-bucket-2".into();

        set_qiniu_config(config);
        assert_eq!(UPDATED.load(Relaxed), 6);
    }

    #[test]
    fn test_load_config_without_hot_reloading() -> Result<()> {
        env_logger::try_init().ok();

        struct QiniuHotReloadingEnvGuard;

        impl QiniuHotReloadingEnvGuard {
            #[inline]
            fn new() -> Self {
                env::set_var(QINIU_DISABLE_CONFIG_HOT_RELOADING_ENV, "1");
                Self
            }
        }

        impl Drop for QiniuHotReloadingEnvGuard {
            #[inline]
            fn drop(&mut self) {
                env::remove_var(QINIU_DISABLE_CONFIG_HOT_RELOADING_ENV)
            }
        }

        let _guard = QiniuHotReloadingEnvGuard::new();

        let mut config = Config {
            access_key: "test-ak-1".into(),
            secret_key: "test-sk-1".into(),
            bucket: "test-bucket-1".into(),
            io_urls: Some(vec!["http://io1.com".into(), "http://io2.com".into()]),
            ..Default::default()
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
        assert_eq!(UPDATED.load(Relaxed), 0);

        Ok(())
    }
}
