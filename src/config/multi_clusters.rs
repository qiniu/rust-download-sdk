use super::{single_cluster::Config, ClustersConfigParseError};
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt, fs,
    io::Error as IOError,
    mem::swap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tap::TapFallible;
use thiserror::Error;

type SelectConfigFn = Arc<
    dyn for<'a> Fn(&'a HashMap<String, Config>, &str) -> Option<&'a Config> + Send + Sync + 'static,
>;

static DEFAULT_CONFIG_SELECT_CALLBACK: Lazy<SelectConfigFn> =
    Lazy::new(|| Arc::new(default_select_config));

/// 多集群七牛配置信息
#[derive(Clone, Deserialize)]
#[serde(try_from = "HashMap<String, PathBuf>")]
pub struct MultipleClustersConfig {
    configs: HashMap<String, Config>,
    original_path: Option<PathBuf>,
    original_paths: HashMap<String, PathBuf>,
    select_config: SelectConfigFn,
}

impl MultipleClustersConfig {
    /// 创建多集群七牛配置信息构建器
    #[inline]
    pub fn builder() -> MultipleClustersConfigBuilder {
        MultipleClustersConfigBuilder(Default::default())
    }

    /// 设置配置选取回调函数，提供多集群配置信息和当前要访问的对象名称，返回要使用的配置信息
    #[inline]
    pub fn set_config_select_callback(
        &mut self,
        f: impl for<'a> Fn(&'a HashMap<String, Config>, &str) -> Option<&'a Config>
            + Send
            + Sync
            + 'static,
    ) -> &mut Self {
        self.set_config_select_callback_raw(Arc::new(f));
        self
    }

    pub(super) fn take_config_select_callback(&mut self) -> SelectConfigFn {
        let mut new_config_select_callback = DEFAULT_CONFIG_SELECT_CALLBACK.to_owned();
        swap(&mut self.select_config, &mut new_config_select_callback);
        new_config_select_callback
    }

    pub(super) fn set_config_select_callback_raw(&mut self, callback: SelectConfigFn) {
        self.select_config = callback;
    }

    #[inline]
    pub(super) fn with_key<T>(&self, key: &str, f: impl FnOnce(&Config) -> T) -> Option<T> {
        (self.select_config)(&self.configs, key).map(f)
    }

    #[inline]
    pub(super) fn parse(path: &Path, bytes: &[u8]) -> Result<Self, ClustersConfigParseError> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("toml") => toml::from_slice(bytes).map_err(|err| err.into()),
            Some("json") => serde_json::from_slice(bytes).map_err(|err| err.into()),
            _ => panic!("QINIU env can only support to be given .toml or .json file"),
        }
        .tap_ok_mut(|config: &mut Self| {
            config.original_path = Some(path.to_owned());
        })
    }

    #[inline]
    pub(super) fn base_timeout(&self) -> Option<Duration> {
        self.configs
            .iter()
            .filter_map(|(_, config)| config.base_timeout())
            .max()
    }

    #[inline]
    pub(super) fn connect_timeout(&self) -> Option<Duration> {
        self.configs
            .iter()
            .filter_map(|(_, config)| config.connect_timeout())
            .max()
    }

    #[inline]
    pub(super) fn config_paths(&self) -> Vec<PathBuf> {
        let mut paths = self
            .original_path
            .as_ref()
            .map(|path| vec![path.to_owned()])
            .unwrap_or_default();
        paths.extend(self.original_paths.iter().map(|(_, path)| path).cloned());
        paths
    }
}

impl TryFrom<HashMap<String, PathBuf>> for MultipleClustersConfig {
    type Error = MultipleClustersConfigParseError;

    #[inline]
    fn try_from(configs: HashMap<String, PathBuf>) -> Result<Self, Self::Error> {
        Ok(Self {
            original_paths: configs.to_owned(),
            configs: configs
                .into_iter()
                .map(|(name, path)| {
                    fs::read(&path)
                        .map_err(MultipleClustersConfigParseError::from)
                        .and_then(|bytes| {
                            Config::parse(&path, &bytes)
                                .map_err(MultipleClustersConfigParseError::from)
                        })
                        .map(|config| (name, config))
                })
                .collect::<Result<_, _>>()?,
            original_path: None,
            select_config: DEFAULT_CONFIG_SELECT_CALLBACK.to_owned(),
        })
    }
}

/// 多集群七牛配置信息解析错误
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MultipleClustersConfigParseError {
    /// 多集群七牛配置信息解析错误
    #[error("Parse config error: {0}")]
    ParseError(#[from] ClustersConfigParseError),

    /// 多集群七牛配置信息读取 I/O 错误
    #[error("I/O error: {0}")]
    IOError(#[from] IOError),
}

impl Default for MultipleClustersConfig {
    #[inline]
    fn default() -> Self {
        Self {
            configs: Default::default(),
            original_path: None,
            original_paths: Default::default(),
            select_config: DEFAULT_CONFIG_SELECT_CALLBACK.to_owned(),
        }
    }
}

impl fmt::Debug for MultipleClustersConfig {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultipleClustersConfig")
            .field("configs", &self.configs)
            .field("original_path", &self.original_path)
            .field("original_paths", &self.original_paths)
            .finish()
    }
}

#[inline]
fn default_select_config<'a>(
    configs: &'a HashMap<String, Config>,
    key: &str,
) -> Option<&'a Config> {
    configs
        .iter()
        .find(|(name, _)| Path::new(key).starts_with(Path::new(name.as_str())))
        .map(|(_, config)| config)
}

/// 多集群七牛配置信息构建器
#[derive(Default, Debug)]
pub struct MultipleClustersConfigBuilder(MultipleClustersConfig);

impl MultipleClustersConfigBuilder {
    /// 构建多集群七牛配置信息
    #[inline]
    pub fn build(self) -> MultipleClustersConfig {
        self.0
    }

    /// 增加集群配置
    #[inline]
    pub fn add_cluster(mut self, name: impl Into<String>, config: Config) -> Self {
        self.0.configs.insert(name.into(), config);
        self
    }

    /// 配置选取回调函数，提供多集群配置信息和当前要访问的对象名称，返回要使用的配置信息
    #[inline]
    pub fn config_select_callback(
        mut self,
        f: impl for<'a> Fn(&'a HashMap<String, Config>, &str) -> Option<&'a Config>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.0.set_config_select_callback(f);
        self
    }
}
