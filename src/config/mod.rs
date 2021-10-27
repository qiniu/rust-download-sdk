mod configurable;
mod multi_clusters;
mod single_cluster;
mod static_vars;
mod watcher;

pub use configurable::Configurable;
pub use multi_clusters::{
    MultipleClustersConfig, MultipleClustersConfigBuilder, MultipleClustersConfigParseError,
};
use once_cell::sync::Lazy;
pub use single_cluster::{Config, ConfigBuilder, SingleClusterConfig, SingleClusterConfigBuilder};

pub(crate) use static_vars::*;

use super::{base::credential::Credential, download::RangeReaderBuilder};
use log::{error, info, warn};
use reqwest::blocking::Client as HTTPClient;
use std::{env, fs, sync::RwLock, time::Duration};
use tap::prelude::*;
use thiserror::Error;
use watcher::ensure_watches;

#[inline]
fn build_http_client() -> RwLock<HTTPClient> {
    return RwLock::new(_build_http_client()).tap(|_| {
        on_config_updated(|| {
            let mut http_client = http_client().write().unwrap();
            *http_client = _build_http_client();
            info!("HTTP_CLIENT reloaded: {:?}", *http_client);
        })
    });

    fn _build_http_client() -> HTTPClient {
        let mut base_timeout = Duration::from_millis(3000u64);
        let mut dial_timeout = Duration::from_millis(50u64);
        with_current_qiniu_config(|config| {
            if let Some(config) = config {
                if let Some(value) = config.base_timeout() {
                    if value > Duration::from_millis(0) {
                        base_timeout = value;
                    }
                }
                if let Some(value) = config.connect_timeout() {
                    if value > Duration::from_millis(0) {
                        dial_timeout = value;
                    }
                }
            }
        });
        let user_agent = format!("QiniuRustDownload/{}", env!("CARGO_PKG_VERSION"));
        HTTPClient::builder()
            .user_agent(user_agent)
            .connect_timeout(dial_timeout)
            .timeout(base_timeout)
            .pool_max_idle_per_host(5)
            .connection_verbose(true)
            .build()
            .expect("Failed to build Reqwest Client")
    }
}

/// 判断当前是否已经启用七牛环境
///
/// 如果当前没有设置 QINIU 环境变量，或加载该环境变量出现错误，则返回 false
#[inline]
pub fn is_qiniu_enabled() -> bool {
    with_current_qiniu_config(|config| config.is_some())
}

/// 在回调函数内获取只读的当前七牛环境配置
///
/// 需要注意的是，在回调函数内，当前七牛环境配置会被用读锁保护，因此回调函数应该尽快返回
#[inline]
pub fn with_current_qiniu_config<T>(f: impl FnOnce(Option<&Configurable>) -> T) -> T {
    f(qiniu_config().read().unwrap().as_ref())
}

/// 在回调函数内获取可写的当前七牛环境配置
///
/// 需要注意的是，在回调函数内，当前七牛环境配置会被用写锁保护，因此回调函数应该尽快返回
#[inline]
pub fn with_current_qiniu_config_mut<T>(f: impl FnOnce(Option<&mut Configurable>) -> T) -> T {
    f(qiniu_config().write().unwrap().as_mut())
}

#[inline]
fn with_current_qiniu_config_mut_inner<T>(f: impl FnOnce(&mut Option<Configurable>) -> T) -> T {
    f(&mut qiniu_config().write().unwrap())
}

/// 手动设置单集群七牛环境配置
#[inline]
pub fn set_qiniu_config(config: Config) {
    set_config_and_reload(config.into(), false)
}

/// 手动设置单集群七牛环境配置
#[inline]
pub fn set_qiniu_single_cluster_config(config: SingleClusterConfig) {
    set_qiniu_config(config)
}

/// 手动设置多集群七牛环境配置
#[inline]
pub fn set_qiniu_multi_clusters_config(config: MultipleClustersConfig) {
    set_config_and_reload(config.into(), false)
}

const QINIU_ENV: &str = "QINIU";
const QINIU_MULTI_ENV: &str = "QINIU_MULTI_CLUSTER";
const QINIU_DISABLE_CONFIG_HOT_RELOADING_ENV: &str = "QINIU_DISABLE_CONFIG_HOT_RELOADING";

fn load_config() -> Option<Configurable> {
    return env::var_os(QINIU_MULTI_ENV)
        .map(|path| (path, EnvFrom::FromQiniuMulti))
        .or_else(|| env::var_os(QINIU_ENV).map(|path| (path, EnvFrom::FromQiniu)))
        .tap_none(|| warn!("QINIU Env IS NOT ENABLED"))
        .and_then(|(qiniu_config_path, env_from)| {
            fs::read(&qiniu_config_path)
                .tap_err(|err| {
                    error!(
                        "Qiniu config file ({:?}) cannot be open: {}",
                        qiniu_config_path, err
                    )
                })
                .ok()
                .and_then(|qiniu_config| {
                    Configurable::parse(
                        &qiniu_config_path,
                        &qiniu_config,
                        matches!(env_from, EnvFrom::FromQiniuMulti),
                    )
                    .tap_err(|err| {
                        error!(
                            "Qiniu config file ({:?}) cannot be deserialized: {}",
                            qiniu_config_path, err
                        )
                    })
                    .ok()
                    .tap_some(|config| {
                        if env::var_os(QINIU_DISABLE_CONFIG_HOT_RELOADING_ENV).is_none() {
                            ensure_watches(&config.config_paths()).ok();
                        }
                    })
                })
        });

    enum EnvFrom {
        FromQiniu,
        FromQiniuMulti,
    }
}

#[inline]
fn reload_config(migrate_callback: bool) {
    if let Some(config) = load_config() {
        set_config_and_reload(config, migrate_callback)
    }
}

#[inline]
fn set_config_and_reload(mut config: Configurable, migrate_callback: bool) {
    with_current_qiniu_config_mut_inner(|current| {
        if migrate_callback {
            if let (Some(current), Some(new)) = (
                current.as_mut().and_then(|current| current.as_multi_mut()),
                config.as_multi_mut(),
            ) {
                new.set_config_select_callback_raw(current.take_config_select_callback());
            }
        }
        ensure_watches(&config.config_paths()).ok();
        info!("QINIU_CONFIG reloaded: {:?}", config);
        *current = Some(config);
    });
    for handle in CONFIG_UPDATE_HANDLERS.read().unwrap().iter() {
        handle();
    }
}

/// 七牛配置信息解析错误
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ClustersConfigParseError {
    /// 七牛配置信息 JSON 解析错误
    #[error("Parse config as json error: {0}")]
    JSONError(#[from] serde_json::Error),

    /// 七牛配置信息 TOML 解析错误
    #[error("Parse config as toml error: {0}")]
    TOMLError(#[from] toml::de::Error),
}

type ConfigUpdateHandlers = Vec<fn()>;
static CONFIG_UPDATE_HANDLERS: Lazy<RwLock<ConfigUpdateHandlers>> = Lazy::new(Default::default);

pub(super) fn on_config_updated(handle: fn()) {
    CONFIG_UPDATE_HANDLERS.write().unwrap().push(handle);
}

pub(super) fn build_range_reader_builder_from_config(
    key: String,
    config: &Config,
) -> RangeReaderBuilder {
    let mut builder = RangeReaderBuilder::new(
        config.bucket().to_owned(),
        key,
        Credential::new(config.access_key(), config.secret_key()),
        config
            .io_urls()
            .map(|urls| urls.to_owned())
            .unwrap_or_default(),
    );

    if let Some(uc_urls) = config.uc_urls() {
        if !uc_urls.is_empty() {
            builder = builder.uc_urls(uc_urls.to_owned());
        }
    }

    if let Some(monitor_urls) = config.monitor_urls() {
        if !monitor_urls.is_empty() {
            builder = builder.monitor_urls(monitor_urls.to_owned());
        }
    }

    if let Some(retry) = config.retry() {
        if retry > 0 {
            builder = builder.io_tries(retry).dot_tries(retry);
        }
    }

    if let Some(punish_time) = config.punish_time() {
        if punish_time > Duration::from_secs(0) {
            builder = builder.punish_duration(punish_time);
        }
    }

    if let Some(base_timeout) = config.base_timeout() {
        if base_timeout > Duration::from_millis(0) {
            builder = builder.base_timeout(base_timeout);
        }
    }

    if let Some(dot_interval) = config.dot_interval() {
        if dot_interval > Duration::from_secs(0) {
            builder = builder.dot_interval(dot_interval);
        }
    }

    if let Some(max_dot_buffer_size) = config.max_dot_buffer_size() {
        if max_dot_buffer_size > 0 {
            builder = builder.max_dot_buffer_size(max_dot_buffer_size);
        }
    }

    if let Some(true) = config.private() {
        builder = builder.private_url_lifetime(Some(Duration::from_secs(3600)));
    }

    if let Some(use_getfile_api) = config.use_getfile_api() {
        builder = builder.use_getfile_api(use_getfile_api);
    }

    if let Some(normalize_key) = config.normalize_key() {
        builder = builder.normalize_key(normalize_key);
    }

    builder
}

pub(super) fn build_range_reader_builder_from_env(
    key: String,
    only_single_cluster: bool,
) -> Option<RangeReaderBuilder> {
    with_current_qiniu_config(|config| {
        config.and_then(|config| {
            if only_single_cluster && config.as_single().is_some() {
                return None;
            }
            config.with_key(&key.to_owned(), move |config| {
                build_range_reader_builder_from_config(key, config)
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{super::download::RangeReader, watcher::unwatch_all, *};
    use anyhow::Result;
    use std::{
        collections::HashMap,
        ffi::OsStr,
        fs::{remove_file, rename, OpenOptions},
        io::Write,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering::Relaxed},
        thread::sleep,
    };
    use tempfile::{tempdir, Builder as TempFileBuilder};
    use watcher::{watch_dirs_count, watch_files_count};

    #[test]
    fn test_load_config() -> Result<()> {
        env_logger::try_init().ok();
        let _defer = ResetFinally;

        let mut config = ConfigBuilder::new(
            "test-ak-1",
            "test-sk-1",
            "test-bucket-1",
            Some(vec!["http://io1.com".into(), "http://io2.com".into()]),
        )
        .build();

        let tempfile_path = {
            let mut tempfile = TempFileBuilder::new().suffix(".toml").tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };
        let _env_guard = QiniuEnvGuard::new(tempfile_path.as_os_str());

        static UPDATED: AtomicUsize = AtomicUsize::new(0);

        let loaded = load_config().unwrap();
        assert_eq!(loaded.as_single(), Some(&config));

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

        config.set_access_key("test-ak-2");
        config.set_secret_key("test-sk-2");
        config.set_bucket("test-bucket-2");

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

        config.set_access_key("test-ak-3");
        config.set_secret_key("test-sk-3");
        config.set_bucket("test-bucket-3");

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
    fn test_set_config() -> Result<()> {
        env_logger::try_init().ok();
        let _defer = ResetFinally;

        let mut config = ConfigBuilder::new(
            "test-ak-1",
            "test-sk-1",
            "test-bucket-1",
            Some(vec!["http://io1.com".into(), "http://io2.com".into()]),
        )
        .build();

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

        config.set_access_key("test-ak-2");
        config.set_secret_key("test-sk-2");
        config.set_bucket("test-bucket-2");

        set_qiniu_config(config);
        assert_eq!(UPDATED.load(Relaxed), 6);

        Ok(())
    }

    #[test]
    fn test_load_multi_clusters_config() -> Result<()> {
        env_logger::try_init().ok();
        let _defer = ResetFinally;

        let tempfile_path_1 = {
            let config = ConfigBuilder::new(
                "test-ak-1",
                "test-sk-1",
                "test-bucket-1",
                Some(vec!["http://io-11.com".into(), "http://io-12.com".into()]),
            )
            .build();
            let mut tempfile = TempFileBuilder::new()
                .prefix("1-")
                .suffix(".toml")
                .tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };
        let tempfile_path_2 = {
            let config = ConfigBuilder::new(
                "test-ak-2",
                "test-sk-2",
                "test-bucket-2",
                Some(vec!["http://io-21.com".into(), "http://io-22.com".into()]),
            )
            .build();
            let mut tempfile = TempFileBuilder::new()
                .prefix("2-")
                .suffix(".toml")
                .tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };
        let tempdir = tempdir()?;
        let tempfile_path = {
            let mut config = HashMap::with_capacity(2);
            config.insert("config_1", tempfile_path_1.to_path_buf());
            config.insert("config_2", tempfile_path_2.to_path_buf());
            let mut tempfile = TempFileBuilder::new()
                .prefix("all-")
                .suffix(".toml")
                .tempfile_in(tempdir.path())?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            env::set_var(QINIU_MULTI_ENV, tempfile.path().as_os_str());
            tempfile.into_temp_path()
        };
        let _env_guard = QiniuEnvGuard::new(tempfile_path.as_os_str());

        static UPDATED: AtomicUsize = AtomicUsize::new(0);

        on_config_updated(|| {
            UPDATED.fetch_add(1, Relaxed);
        });

        with_current_qiniu_config_mut(|config| {
            let multi_config = config.unwrap().as_multi_mut().unwrap();
            assert!(multi_config
                .with_key("config_1", |config| {
                    assert_eq!(config.access_key(), "test-ak-1");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_2", |config| {
                    assert_eq!(config.access_key(), "test-ak-2");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_3", |config| {
                    assert_eq!(config.access_key(), "test-ak-3");
                })
                .is_none());
            multi_config.set_config_select_callback(|configs, key| match key {
                "config_1" => configs.get("config_2"),
                "config_2" => configs.get("config_1"),
                "config_3" => configs.get("config_3"),
                _ => None,
            });
            assert!(multi_config
                .with_key("config_1", |config| {
                    assert_eq!(config.access_key(), "test-ak-2");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_2", |config| {
                    assert_eq!(config.access_key(), "test-ak-1");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_3", |config| {
                    assert_eq!(config.access_key(), "test-ak-3");
                })
                .is_none());
        });

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 0);

        {
            let config = ConfigBuilder::new(
                "test-ak-22",
                "test-sk-22",
                "test-bucket-22",
                Some(vec!["http://io-21.com".into(), "http://io-22.com".into()]),
            )
            .build();
            fs::write(&tempfile_path_2, &toml::to_vec(&config)?)?;
        }

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 1);

        with_current_qiniu_config_mut(|config| {
            let multi_config = config.unwrap().as_multi_mut().unwrap();
            assert!(multi_config
                .with_key("config_1", |config| {
                    assert_eq!(config.access_key(), "test-ak-22");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_2", |config| {
                    assert_eq!(config.access_key(), "test-ak-1");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_3", |config| {
                    assert_eq!(config.access_key(), "test-ak-3");
                })
                .is_none());
        });

        let tempfile_path_3 = {
            let config = ConfigBuilder::new(
                "test-ak-3",
                "test-sk-3",
                "test-bucket-3",
                Some(vec!["http://io-31.com".into(), "http://io-32.com".into()]),
            )
            .build();
            let mut tempfile = TempFileBuilder::new()
                .prefix("3-")
                .suffix(".toml")
                .tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };

        {
            let mut config = HashMap::with_capacity(3);
            config.insert("config_1", tempfile_path_1.to_path_buf());
            config.insert("config_2", tempfile_path_2.to_path_buf());
            config.insert("config_3", tempfile_path_3.to_path_buf());
            fs::write(&tempfile_path, &toml::to_vec(&config)?)?;
        };

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 2);

        {
            let mut config = qiniu_config().write().unwrap();
            let multi_config = config.as_mut().unwrap().as_multi_mut().unwrap();
            assert!(multi_config
                .with_key("config_1", |config| {
                    assert_eq!(config.access_key(), "test-ak-22");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_2", |config| {
                    assert_eq!(config.access_key(), "test-ak-1");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_3", |config| {
                    assert_eq!(config.access_key(), "test-ak-3");
                })
                .is_some());
        }

        {
            let config = ConfigBuilder::new(
                "test-ak-32",
                "test-sk-32",
                "test-bucket-32",
                Some(vec!["http://io-31.com".into(), "http://io-32.com".into()]),
            )
            .build();
            fs::write(&tempfile_path_3, &toml::to_vec(&config)?)?;
        }

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 3);

        with_current_qiniu_config_mut(|config| {
            let multi_config = config.unwrap().as_multi_mut().unwrap();
            assert!(multi_config
                .with_key("config_1", |config| {
                    assert_eq!(config.access_key(), "test-ak-22");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_2", |config| {
                    assert_eq!(config.access_key(), "test-ak-1");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_3", |config| {
                    assert_eq!(config.access_key(), "test-ak-32");
                })
                .is_some());
        });

        assert_eq!(watch_dirs_count(), 2);
        assert_eq!(watch_files_count(), 4);

        {
            let mut config = HashMap::with_capacity(2);
            config.insert("config_2", tempfile_path_2.to_path_buf());
            config.insert("config_3", tempfile_path_3.to_path_buf());
            fs::write(&tempfile_path, &toml::to_vec(&config)?)?;
        }

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 4);

        with_current_qiniu_config_mut(|config| {
            let multi_config = config.unwrap().as_multi_mut().unwrap();
            assert!(multi_config
                .with_key("config_1", |config| {
                    assert_eq!(config.access_key(), "test-ak-22");
                })
                .is_some());
            assert!(multi_config
                .with_key("config_2", |config| {
                    assert_eq!(config.access_key(), "test-ak-1");
                })
                .is_none());
            assert!(multi_config
                .with_key("config_3", |config| {
                    assert_eq!(config.access_key(), "test-ak-32");
                })
                .is_some());
        });

        {
            let config = ConfigBuilder::new(
                "test-ak-12",
                "test-sk-12",
                "test-bucket-12",
                Some(vec!["http://io-11.com".into(), "http://io-12.com".into()]),
            )
            .build();
            fs::write(&tempfile_path_1, &toml::to_vec(&config)?)?;
        }

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 4);

        assert_eq!(watch_dirs_count(), 2);
        assert_eq!(watch_files_count(), 3);

        {
            fs::write(
                &tempfile_path,
                &toml::to_vec(&HashMap::<String, PathBuf>::new())?,
            )?;
        };

        sleep(Duration::from_secs(1));
        assert_eq!(UPDATED.load(Relaxed), 5);

        assert_eq!(watch_dirs_count(), 1);
        assert_eq!(watch_files_count(), 1);

        unwatch_all()?;

        Ok(())
    }

    #[test]
    fn test_load_config_without_hot_reloading() -> Result<()> {
        env_logger::try_init().ok();
        let _defer = ResetFinally;

        let _guard = QiniuHotReloadingEnvGuard::new();

        let mut config = ConfigBuilder::new(
            "test-ak-1",
            "test-sk-1",
            "test-bucket-1",
            Some(vec!["http://io1.com".into(), "http://io2.com".into()]),
        )
        .build();
        let tempfile_path = {
            let mut tempfile = TempFileBuilder::new().suffix(".toml").tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };
        let _env_guard = QiniuEnvGuard::new(tempfile_path.as_os_str());

        static UPDATED: AtomicUsize = AtomicUsize::new(0);
        UPDATED.store(0, Relaxed);

        let loaded = load_config().unwrap();
        assert_eq!(loaded.as_single(), Some(&config));

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

        config.set_access_key("test-ak-2");
        config.set_secret_key("test-sk-2");
        config.set_bucket("test-bucket-2");

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

    #[test]
    fn test_default_select_callback_of_multi_clusters_config() -> Result<()> {
        env_logger::try_init().ok();
        let _defer = ResetFinally;

        let tempfile_path_1 = {
            let config = ConfigBuilder::new(
                "test-ak-1",
                "test-sk-1",
                "test-bucket-1",
                Some(vec!["http://io-11.com".into(), "http://io-12.com".into()]),
            )
            .build();
            let mut tempfile = TempFileBuilder::new()
                .prefix("1-")
                .suffix(".toml")
                .tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };
        let tempfile_path_2 = {
            let config = ConfigBuilder::new(
                "test-ak-2",
                "test-sk-2",
                "test-bucket-2",
                Some(vec!["http://io-21.com".into(), "http://io-22.com".into()]),
            )
            .build();
            let mut tempfile = TempFileBuilder::new()
                .prefix("2-")
                .suffix(".toml")
                .tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };
        let tempfile_path = {
            let mut config = HashMap::with_capacity(2);
            config.insert("/node1", tempfile_path_1.to_path_buf());
            config.insert("/node12", tempfile_path_2.to_path_buf());
            let mut tempfile = TempFileBuilder::new()
                .prefix("all-")
                .suffix(".toml")
                .tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };
        let _env_guard = QiniuMultiEnvGuard::new(tempfile_path.as_os_str());

        with_current_qiniu_config_mut(|config| {
            let multi_config = config.unwrap().as_multi_mut().unwrap();
            assert!(multi_config
                .with_key("/node1", |config| {
                    assert_eq!(config.access_key(), "test-ak-1");
                })
                .is_some());
            assert!(multi_config
                .with_key("/node12", |config| {
                    assert_eq!(config.access_key(), "test-ak-2");
                })
                .is_some());
        });

        Ok(())
    }

    #[test]
    fn test_range_reader_from_multi_clusters_config() -> Result<()> {
        env_logger::try_init().ok();
        let _defer = ResetFinally;

        let tempfile_path_1 = {
            let config = ConfigBuilder::new(
                "test-ak-1",
                "test-sk-1",
                "test-bucket-1",
                Some(vec!["http://io-11.com".into(), "http://io-12.com".into()]),
            )
            .build();
            let mut tempfile = TempFileBuilder::new()
                .prefix("1-")
                .suffix(".toml")
                .tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };
        let tempfile_path_2 = {
            let config = ConfigBuilder::new(
                "test-ak-2",
                "test-sk-2",
                "test-bucket-2",
                Some(vec!["http://io-21.com".into(), "http://io-22.com".into()]),
            )
            .build();
            let mut tempfile = TempFileBuilder::new()
                .prefix("2-")
                .suffix(".toml")
                .tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };
        let tempfile_path = {
            let mut config = HashMap::with_capacity(2);
            config.insert("/node1", tempfile_path_1.to_path_buf());
            config.insert("/node2", tempfile_path_2.to_path_buf());
            let mut tempfile = TempFileBuilder::new()
                .prefix("all-")
                .suffix(".toml")
                .tempfile()?;
            tempfile.write_all(&toml::to_vec(&config)?)?;
            tempfile.flush()?;
            tempfile.into_temp_path()
        };
        let _env_guard = QiniuMultiEnvGuard::new(tempfile_path.as_os_str());

        assert_eq!(
            RangeReader::from_env("/node1/file1").unwrap().io_urls(),
            vec!["http://io-11.com".to_owned(), "http://io-12.com".to_owned()]
        );
        assert_eq!(
            RangeReader::from_env("/node2/file1").unwrap().io_urls(),
            vec!["http://io-21.com".to_owned(), "http://io-22.com".to_owned()]
        );
        assert!(RangeReader::from_env("/node3/file1").is_none());

        {
            let config = ConfigBuilder::new(
                "test-ak-1",
                "test-sk-1",
                "test-bucket-1",
                Some(vec!["http://io-112.com".into(), "http://io-122.com".into()]),
            )
            .build();
            fs::write(&tempfile_path_1, &toml::to_vec(&config)?)?;
        }

        sleep(Duration::from_secs(1));

        assert_eq!(
            RangeReader::from_env("/node1/file1").unwrap().io_urls(),
            vec![
                "http://io-112.com".to_owned(),
                "http://io-122.com".to_owned()
            ]
        );
        assert_eq!(
            RangeReader::from_env("/node2/file1").unwrap().io_urls(),
            vec!["http://io-21.com".to_owned(), "http://io-22.com".to_owned()]
        );

        {
            let mut config = HashMap::with_capacity(2);
            config.insert("/node1", tempfile_path_1.to_path_buf());
            fs::write(&tempfile_path, &toml::to_vec(&config)?)?;
        }

        sleep(Duration::from_secs(1));

        assert_eq!(
            RangeReader::from_env("/node1/file1").unwrap().io_urls(),
            vec![
                "http://io-112.com".to_owned(),
                "http://io-122.com".to_owned()
            ]
        );
        assert!(RangeReader::from_env("/node2/file1").is_none());

        Ok(())
    }

    struct ResetFinally;

    impl Drop for ResetFinally {
        #[inline]
        fn drop(&mut self) {
            reset_static_vars();
            CONFIG_UPDATE_HANDLERS.write().unwrap().clear();
            unwatch_all().unwrap();
        }
    }

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

    struct QiniuEnvGuard;

    impl QiniuEnvGuard {
        #[inline]
        fn new(val: &OsStr) -> Self {
            env::set_var(QINIU_ENV, val);
            Self
        }
    }

    impl Drop for QiniuEnvGuard {
        #[inline]
        fn drop(&mut self) {
            env::remove_var(QINIU_ENV)
        }
    }

    struct QiniuMultiEnvGuard;

    impl QiniuMultiEnvGuard {
        #[inline]
        fn new(val: &OsStr) -> Self {
            env::set_var(QINIU_MULTI_ENV, val);
            Self
        }
    }

    impl Drop for QiniuMultiEnvGuard {
        #[inline]
        fn drop(&mut self) {
            env::remove_var(QINIU_MULTI_ENV)
        }
    }
}