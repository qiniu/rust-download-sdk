use super::{
    super::{async_api::RangeReaderHandle as AsyncRangeReaderHandle, sync_api::RangeReaderInner},
    ClustersConfigParseError, Timeouts,
};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    convert::TryInto,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tap::TapFallible;

/// 单集群七牛配置信息
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
    max_retry_concurrency: Option<u32>,

    #[serde(skip)]
    extra: Extra,
}

/// 单集群七牛配置信息
pub type SingleClusterConfig = Config;

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

    pub(super) fn with_key<T>(&self, _key: &str, f: impl FnOnce(&Config) -> T) -> Option<T> {
        Some(f(self))
    }

    pub(super) fn parse(path: &Path, bytes: &[u8]) -> Result<Self, ClustersConfigParseError> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("toml") => toml::from_slice(bytes).map_err(|err| err.into()),
            Some("json") => serde_json::from_slice(bytes).map_err(|err| err.into()),
            _ => panic!("QINIU env can only support to be given .toml or .json file"),
        }
        .tap_ok_mut(|config: &mut Self| {
            config.extra.original_path = Some(path.to_owned());
        })
    }

    /// 获取七牛 Access Key
    #[inline]
    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// 设置七牛 Access Key
    #[inline]
    pub fn set_access_key(&mut self, access_key: impl Into<String>) -> &mut Self {
        self.access_key = access_key.into();
        self.uninit_range_reader_inner();
        self
    }

    /// 获取七牛 Secret Key
    #[inline]
    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }

    /// 设置七牛 Secret Key
    #[inline]
    pub fn set_secret_key(&mut self, secret_key: impl Into<String>) -> &mut Self {
        self.secret_key = secret_key.into();
        self.uninit_range_reader_inner();
        self
    }

    /// 获取七牛存储空间
    #[inline]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// 设置七牛存储空间
    #[inline]
    pub fn set_bucket(&mut self, bucket: impl Into<String>) -> &mut Self {
        self.bucket = bucket.into();
        self.uninit_range_reader_inner();
        self
    }

    /// 获取 IO 服务器 URL 列表
    #[inline]
    pub fn io_urls(&self) -> Option<&[String]> {
        self.io_urls.as_ref().map(|urls| urls.as_ref())
    }

    /// 设置 IO 服务器 URL 列表
    #[inline]
    pub fn set_io_urls(&mut self, io_urls: Option<impl Into<Vec<String>>>) -> &mut Self {
        self.io_urls = io_urls.map(|urls| urls.into());
        self.uninit_range_reader_inner();
        self
    }

    /// 获取 UC 服务器 URL 列表
    #[inline]
    pub fn uc_urls(&self) -> Option<&[String]> {
        self.uc_urls.as_ref().map(|urls| urls.as_ref())
    }

    /// 设置 UC 服务器 URL 列表
    #[inline]
    pub fn set_uc_urls(&mut self, uc_urls: Option<impl Into<Vec<String>>>) -> &mut Self {
        self.uc_urls = uc_urls.map(|urls| urls.into());
        self.uninit_range_reader_inner();
        self
    }

    /// 获取监控服务器服务器 URL 列表
    #[inline]
    pub fn monitor_urls(&self) -> Option<&[String]> {
        self.monitor_urls.as_ref().map(|urls| urls.as_ref())
    }

    /// 设置监控服务器 URL 列表
    #[inline]
    pub fn set_monitor_urls(&mut self, monitor_urls: Option<impl Into<Vec<String>>>) -> &mut Self {
        self.monitor_urls = monitor_urls.map(|urls| urls.into());
        self.uninit_range_reader_inner();
        self
    }

    /// 是否使用 Getfile API
    #[inline]
    pub fn use_getfile_api(&self) -> Option<bool> {
        self.sim.map(|b| !b)
    }

    /// 设置是否使用 Getfile API
    #[inline]
    pub fn set_use_getfile_api(&mut self, use_getfile_api: Option<bool>) -> &mut Self {
        self.sim = use_getfile_api.map(|b| !b);
        self.uninit_range_reader_inner();
        self
    }

    /// 是否对 key 进行格式化
    #[inline]
    pub fn normalize_key(&self) -> Option<bool> {
        self.normalize_key
    }

    /// 设置是否对 key 进行格式化
    #[inline]
    pub fn set_normalize_key(&mut self, normalize_key: Option<bool>) -> &mut Self {
        self.normalize_key = normalize_key;
        self.uninit_range_reader_inner();
        self
    }

    /// 是否使用私有存储空间
    #[inline]
    pub fn private(&self) -> Option<bool> {
        self.private
    }

    /// 设置是否使用私有存储空间
    #[inline]
    pub fn set_private(&mut self, private: Option<bool>) -> &mut Self {
        self.private = private;
        self.uninit_range_reader_inner();
        self
    }

    /// 获取 IO 和 UC 服务器访问重试次数
    #[inline]
    pub fn retry(&self) -> Option<usize> {
        self.retry
    }

    /// 设置 IO 和 UC 服务器访问重试次数
    #[inline]
    pub fn set_retry(&mut self, retry: Option<usize>) -> &mut Self {
        self.retry = retry;
        self.uninit_range_reader_inner();
        self
    }

    /// 获取打点记录上传频率
    #[inline]
    pub fn dot_interval(&self) -> Option<Duration> {
        self.dot_interval_s.map(Duration::from_secs)
    }

    /// 设置打点记录上传频率
    #[inline]
    pub fn set_dot_interval(&mut self, dot_interval: Option<Duration>) -> &mut Self {
        self.dot_interval_s = dot_interval.map(|d| d.as_secs());
        self.uninit_range_reader_inner();
        self
    }

    /// 获取打点记录本地缓存文件尺寸上限
    #[inline]
    pub fn max_dot_buffer_size(&self) -> Option<u64> {
        self.max_dot_buffer_size
    }

    /// 设置打点记录本地缓存文件尺寸上限
    #[inline]
    pub fn set_max_dot_buffer_size(&mut self, max_dot_buffer_size: Option<u64>) -> &mut Self {
        self.max_dot_buffer_size = max_dot_buffer_size;
        self.uninit_range_reader_inner();
        self
    }

    /// 获取域名访问失败后的惩罚时长
    #[inline]
    pub fn punish_time(&self) -> Option<Duration> {
        self.punish_time_s.map(Duration::from_secs)
    }

    /// 设置域名访问失败后的惩罚时长
    #[inline]
    pub fn set_punish_time(&mut self, punish_time: Option<Duration>) -> &mut Self {
        self.punish_time_s = punish_time.map(|d| d.as_secs());
        self.uninit_range_reader_inner();
        self
    }

    /// 获取域名访问的基础超时时长
    #[inline]
    pub fn base_timeout(&self) -> Option<Duration> {
        self.base_timeout_ms.map(Duration::from_millis)
    }

    /// 设置域名访问失败后的惩罚时长
    #[inline]
    pub fn set_base_timeout(&mut self, base_timeout: Option<Duration>) -> &mut Self {
        self.base_timeout_ms = base_timeout.map(|d| d.as_millis().try_into().unwrap_or(u64::MAX));
        self.uninit_range_reader_inner();
        self
    }

    /// 获取域名连接的超时时长
    #[inline]
    pub fn connect_timeout(&self) -> Option<Duration> {
        self.dial_timeout_ms.map(Duration::from_millis)
    }

    /// 设置域名连接的超时时长
    #[inline]
    pub fn set_connect_timeout(&mut self, connect_timeout: Option<Duration>) -> &mut Self {
        self.dial_timeout_ms =
            connect_timeout.map(|d| d.as_millis().try_into().unwrap_or(u64::MAX));
        self.uninit_range_reader_inner();
        self
    }

    /// 获取最大并行重试次数
    #[inline]
    pub fn max_retry_concurrency(&self) -> Option<u32> {
        self.max_retry_concurrency
    }

    /// 设置最大并行重试次数，如果设置为 Some(0) 则表示禁止并行重试功能
    #[inline]
    pub fn set_max_retry_concurrency(&mut self, max_retry_concurrency: Option<u32>) -> &mut Self {
        self.max_retry_concurrency = max_retry_concurrency;
        self.uninit_range_reader_inner();
        self
    }

    pub(super) fn original_path(&self) -> Option<&Path> {
        self.extra.original_path.as_ref().map(|p| p.as_ref())
    }

    #[allow(dead_code)]
    pub(super) fn original_path_mut(&mut self) -> &mut Option<PathBuf> {
        &mut self.extra.original_path
    }

    pub(super) fn config_paths(&self) -> Vec<PathBuf> {
        self.extra
            .original_path
            .as_ref()
            .map(|path| vec![path.to_owned()])
            .unwrap_or_default()
    }

    pub(super) fn timeouts_set(&self) -> HashSet<Timeouts> {
        let mut set = HashSet::with_capacity(1);
        set.insert(Timeouts::from(self));
        set
    }

    pub(crate) fn get_or_init_range_reader_inner(
        &self,
        f: impl FnOnce() -> Arc<RangeReaderInner>,
    ) -> Arc<RangeReaderInner> {
        self.extra.range_reader_inner.get_or_init(f).to_owned()
    }

    pub(crate) fn get_or_init_async_range_reader_inner(
        &self,
        f: impl FnOnce() -> AsyncRangeReaderHandle,
    ) -> AsyncRangeReaderHandle {
        self.extra
            .async_range_reader_inner
            .get_or_init(f)
            .to_owned()
    }

    fn uninit_range_reader_inner(&mut self) {
        self.extra.range_reader_inner.take();
        self.extra.async_range_reader_inner.take();
    }
}

/// 七牛单集群配置信息构建器
#[derive(Debug)]
pub struct ConfigBuilder(Config);

/// 七牛单集群配置信息构建器
pub type SingleClusterConfigBuilder = ConfigBuilder;

impl ConfigBuilder {
    /// 创建七牛配置信息构建器
    pub fn new(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        bucket: impl Into<String>,
        io_urls: Option<Vec<String>>,
    ) -> Self {
        Self(Config {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            bucket: bucket.into(),
            io_urls,
            ..Default::default()
        })
    }

    /// 构建七牛配置信息
    #[inline]
    pub fn build(self) -> Config {
        self.0
    }

    /// 配置七牛 Access Key
    #[inline]
    pub fn access_key(mut self, access_key: impl Into<String>) -> Self {
        self.0.access_key = access_key.into();
        self
    }

    /// 配置七牛 Secret Key
    #[inline]
    pub fn secret_key(mut self, secret_key: impl Into<String>) -> Self {
        self.0.secret_key = secret_key.into();
        self
    }

    /// 配置七牛存储空间
    #[inline]
    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.0.bucket = bucket.into();
        self
    }

    /// 配置 IO 服务器 URL 列表
    #[inline]
    pub fn io_urls(mut self, io_urls: Option<Vec<String>>) -> Self {
        self.0.io_urls = io_urls;
        self
    }

    /// 配置 UC 服务器域名列表
    #[inline]
    pub fn uc_urls(mut self, uc_urls: Option<Vec<String>>) -> Self {
        self.0.uc_urls = uc_urls;
        self
    }

    /// 配置监控服务器域名列表，如果不配置或配置为空，则不会启用打点功能
    #[inline]
    pub fn monitor_urls(mut self, monitor_urls: Option<Vec<String>>) -> Self {
        self.0.monitor_urls = monitor_urls;
        self
    }

    /// 是否使用 Getfile API，默认为 true
    #[inline]
    pub fn use_getfile_api(mut self, use_getfile_api: Option<bool>) -> Self {
        self.0.sim = use_getfile_api.map(|b| !b);
        self
    }

    /// 是否对 key 进行格式化，默认为 false
    #[inline]
    pub fn normalize_key(mut self, normalize_key: Option<bool>) -> Self {
        self.0.normalize_key = normalize_key;
        self
    }

    /// 是否使用私有存储空间，默认不使用
    #[inline]
    pub fn private(mut self, private: Option<bool>) -> Self {
        self.0.private = private;
        self
    }

    /// 配置 IO 和 UC 服务器访问重试次数，默认为 10
    #[inline]
    pub fn retry(mut self, retry: Option<usize>) -> Self {
        self.0.retry = retry;
        self
    }

    /// 配置域名访问失败后的惩罚时长，默认为 30 分钟
    #[inline]
    pub fn punish_duration(mut self, punish_duration: Option<Duration>) -> Self {
        self.0.punish_time_s = punish_duration.map(|d| d.as_secs());
        self
    }

    /// 配置域名访问的基础超时时长，默认为 3000 毫秒
    #[inline]
    pub fn base_timeout(mut self, base_timeout: Option<Duration>) -> Self {
        self.0.base_timeout_ms = base_timeout.map(|d| d.as_millis().try_into().unwrap_or(u64::MAX));
        self
    }

    /// 配置域名连接的超时时长，默认为 50 毫秒
    #[inline]
    pub fn connect_timeout(mut self, connect_timeout: Option<Duration>) -> Self {
        self.0.dial_timeout_ms =
            connect_timeout.map(|d| d.as_millis().try_into().unwrap_or(u64::MAX));
        self
    }

    /// 配置最大并行重试次数，默认为 5，如果设置为 Some(0) 则表示禁止并行重试功能
    #[inline]
    pub fn max_retry_concurrency(mut self, max_retry_concurrency: Option<u32>) -> Self {
        self.0.max_retry_concurrency = max_retry_concurrency;
        self
    }

    /// 设置打点记录上传频率，默认为 10 秒
    #[inline]
    pub fn dot_interval(mut self, dot_interval: Option<Duration>) -> Self {
        self.0.dot_interval_s = dot_interval.map(|d| d.as_secs());
        self
    }

    /// 设置打点记录本地缓存文件尺寸上限，默认为 1 MB
    #[inline]
    pub fn max_dot_buffer_size(mut self, max_dot_buffer_size: Option<u64>) -> Self {
        self.0.max_dot_buffer_size = max_dot_buffer_size;
        self
    }

    #[inline]
    #[cfg(test)]
    pub(super) fn original_path(mut self, original_path: Option<PathBuf>) -> Self {
        self.0.extra.original_path = original_path;
        self
    }
}

impl From<Config> for ConfigBuilder {
    #[inline]
    fn from(config: Config) -> Self {
        ConfigBuilder(config)
    }
}

#[derive(Default, Clone, Debug)]
struct Extra {
    original_path: Option<PathBuf>,
    range_reader_inner: OnceCell<Arc<RangeReaderInner>>,
    async_range_reader_inner: OnceCell<AsyncRangeReaderHandle>,
}

impl PartialEq for Extra {
    #[inline]
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for Extra {}
