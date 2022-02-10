use super::{
    async_api::{
        RangePart, RangeReader as AsyncRangeReader, RangeReaderBuilder as AsyncRangeReaderBuilder,
    },
    base::{credential::Credential, download::RangeReaderBuilder as BaseRangeReaderBuilder},
    config::{
        build_base_range_reader_builder_from_config, build_base_range_reader_builder_from_env,
        with_current_qiniu_config, Config,
    },
    sync_api::{
        RangeReader as SyncRangeReader, RangeReaderBuilder as SyncRangeReaderBuilder, WriteSeek,
    },
};
use positioned_io::ReadAt;
use std::{io::Result as IoResult, time::Duration};

#[derive(Debug)]
/// 对象范围下载构建器
pub struct RangeReaderBuilder(BaseRangeReaderBuilder);

impl RangeReaderBuilder {
    /// 创建对象范围下载构建器
    /// # Arguments
    ///
    /// * `bucket` - 存储空间
    /// * `key` - 对象名称
    /// * `credential` - 存储空间所在账户的凭证
    /// * `io_urls` - 七牛 IO 服务器 URL 列表

    pub fn new(
        bucket: impl Into<String>,
        key: impl Into<String>,
        credential: Credential,
        io_urls: Vec<String>,
    ) -> Self {
        Self(BaseRangeReaderBuilder::new(
            bucket.into(),
            key.into(),
            credential,
            io_urls,
        ))
    }

    /// 设置七牛 UC 服务器 URL 列表

    pub fn uc_urls(self, urls: Vec<String>) -> Self {
        self.with_inner(|b| b.uc_urls(urls))
    }

    /// 设置七牛监控服务器 URL 列表

    pub fn monitor_urls(self, urls: Vec<String>) -> Self {
        self.with_inner(|b| b.monitor_urls(urls))
    }

    /// 设置对象下载最大尝试次数

    pub fn io_tries(self, tries: usize) -> Self {
        self.with_inner(|b| b.io_tries(tries))
    }

    /// 设置 UC 查询的最大尝试次数

    pub fn uc_tries(self, tries: usize) -> Self {
        self.with_inner(|b| b.uc_tries(tries))
    }

    /// 设置打点记录上传的最大尝试次数

    pub fn dot_tries(self, tries: usize) -> Self {
        self.with_inner(|b| b.dot_tries(tries))
    }

    /// 设置 UC 查询的频率

    pub fn update_interval(self, interval: Duration) -> Self {
        self.with_inner(|b| b.update_interval(interval))
    }

    /// 设置域名访问失败后的惩罚时长

    pub fn punish_duration(self, duration: Duration) -> Self {
        self.with_inner(|b| b.punish_duration(duration))
    }

    /// 设置域名访问的基础超时时长

    pub fn base_timeout(self, timeout: Duration) -> Self {
        self.with_inner(|b| b.base_timeout(timeout))
    }

    /// 设置域名访问的连接时长

    pub fn connect_timeout(self, timeout: Duration) -> Self {
        self.with_inner(|b| b.connect_timeout(timeout))
    }

    /// 设置失败域名的最大重试次数
    ///
    /// 一旦一个域名的被惩罚次数超过限制，则域名选择器不会选择该域名，除非被惩罚的域名比例超过上限，或惩罚时长超过指定时长

    pub fn max_punished_times(self, max_times: usize) -> Self {
        self.with_inner(|b| b.max_punished_times(max_times))
    }

    /// 设置被惩罚的域名最大比例
    ///
    /// 域名选择器在搜索域名时，一旦被跳过的域名比例大于该值，则下一个域名将被选中，不管该域名是否也被惩罚。一旦该域名成功，则惩罚将立刻被取消

    pub fn max_punished_hosts_percent(self, percent: u8) -> Self {
        self.with_inner(|b| b.max_punished_hosts_percent(percent))
    }

    /// 设置是否使用 getfile API 下载

    pub fn use_getfile_api(self, use_getfile_api: bool) -> Self {
        self.with_inner(|b| b.use_getfile_api(use_getfile_api))
    }

    /// 设置是否对 key 进行格式化

    pub fn normalize_key(self, normalize_key: bool) -> Self {
        self.with_inner(|b| b.normalize_key(normalize_key))
    }

    /// 设置私有空间下载 URL 有效期，如果为 None，则使用公开空间下载 URL

    pub fn private_url_lifetime(self, private_url_lifetime: Option<Duration>) -> Self {
        self.with_inner(|b| b.private_url_lifetime(private_url_lifetime))
    }

    /// 设置打点记录上传频率

    pub fn dot_interval(self, dot_interval: Duration) -> Self {
        self.with_inner(|b| b.dot_interval(dot_interval))
    }

    /// 设置打点记录本地缓存文件尺寸上限

    pub fn max_dot_buffer_size(self, max_dot_buffer_size: u64) -> Self {
        self.with_inner(|b| b.max_dot_buffer_size(max_dot_buffer_size))
    }

    /// 设置最大并行重试次数，如果设置为 0 则表示禁止并行重试功能
    pub fn max_retry_concurrency(self, max_retry_concurrency: usize) -> Self {
        self.with_inner(|b| b.max_retry_concurrency(max_retry_concurrency))
    }

    /// 设置是否使用 HTTPS 协议来访问 IO 服务器

    pub fn use_https(self, use_https: bool) -> Self {
        self.with_inner(|b| b.use_https(use_https))
    }

    fn with_inner(
        mut self,
        f: impl FnOnce(BaseRangeReaderBuilder) -> BaseRangeReaderBuilder,
    ) -> Self {
        self.0 = f(self.0);
        self
    }

    /// 构建范围下载器
    pub fn build(self) -> RangeReader {
        if self.0.max_retry_concurrency == Some(0) {
            RangeReader(RangeReaderImpl::Sync(
                SyncRangeReaderBuilder::from(self.0).build(),
            ))
        } else {
            RangeReader(RangeReaderImpl::Async(
                AsyncRangeReaderBuilder::from(self.0).build(),
            ))
        }
    }

    /// 从配置创建范围下载构建器
    /// # Arguments
    ///
    /// * `key` - 对象名称
    /// * `config` - 下载配置

    pub fn from_config(key: impl Into<String>, config: &Config) -> Self {
        Self(build_base_range_reader_builder_from_config(
            key.into(),
            config,
        ))
    }

    /// 从环境变量创建范围下载构建器
    /// # Arguments
    ///
    /// * `key` - 对象名称

    pub fn from_env(key: impl Into<String>) -> Option<Self> {
        build_base_range_reader_builder_from_env(key.into(), false).map(Self)
    }
}

/// 对象范围下载器
#[derive(Debug)]
pub struct RangeReader(RangeReaderImpl);

#[derive(Debug)]
enum RangeReaderImpl {
    Sync(SyncRangeReader),
    Async(AsyncRangeReader),
}

impl RangeReader {
    /// 创建范围下载构建器

    pub fn builder(
        bucket: impl Into<String>,
        key: impl Into<String>,
        credential: Credential,
        io_urls: Vec<String>,
    ) -> RangeReaderBuilder {
        RangeReaderBuilder::new(bucket, key, credential, io_urls)
    }

    /// 从配置创建范围下载器
    /// # Arguments
    ///
    /// * `key` - 对象名称
    /// * `config` - 下载配置
    pub fn from_config(key: impl Into<String>, config: &Config) -> Self {
        if config.max_retry_concurrency() == Some(0) {
            Self(RangeReaderImpl::Sync(SyncRangeReader::from_config(
                key.into(),
                config,
            )))
        } else {
            Self(RangeReaderImpl::Async(AsyncRangeReader::from_config(
                key.into(),
                config,
            )))
        }
    }

    /// 从环境变量创建范围下载器
    /// # Arguments
    ///
    /// * `key` - 对象名称
    pub fn from_env(key: impl Into<String>) -> Option<Self> {
        let key = key.into();
        let range_reader = with_current_qiniu_config(|config| {
            config.and_then(|config| {
                config.with_key(&key.to_owned(), |config| {
                    if config.max_retry_concurrency() == Some(0) {
                        SyncRangeReader::from_env(key)
                            .map(RangeReaderImpl::Sync)
                            .map(Self)
                    } else {
                        AsyncRangeReader::from_env(key)
                            .map(RangeReaderImpl::Async)
                            .map(Self)
                    }
                })
            })
        });
        match range_reader {
            Some(Some(range_reader)) => Some(range_reader),
            _ => None,
        }
    }

    /// 主动更新域名列表
    ///
    /// 如果返回为 true 表示更新成功，否则返回 false
    pub fn update_urls(&self) -> bool {
        match &self.0 {
            RangeReaderImpl::Sync(range_reader) => range_reader.update_urls(),
            RangeReaderImpl::Async(range_reader) => range_reader.update_urls(),
        }
    }

    /// 获取当前可用的 IO 节点的域名
    pub fn io_urls(&self) -> Vec<String> {
        match &self.0 {
            RangeReaderImpl::Sync(range_reader) => range_reader.io_urls(),
            RangeReaderImpl::Async(range_reader) => range_reader.io_urls(),
        }
    }

    /// 读取文件的多个区域，返回每个区域对应的数据
    /// # Arguments
    /// * `range` - 区域列表，每个区域有开始偏移量和区域长度组成
    pub fn read_multi_ranges(&self, ranges: &[(u64, u64)]) -> IoResult<Vec<RangePart>> {
        match &self.0 {
            RangeReaderImpl::Sync(range_reader) => range_reader.read_multi_ranges(ranges),
            RangeReaderImpl::Async(range_reader) => range_reader.read_multi_ranges(ranges),
        }
    }

    /// 判定当前对象是否存在
    pub fn exist(&self) -> IoResult<bool> {
        match &self.0 {
            RangeReaderImpl::Sync(range_reader) => range_reader.exist(),
            RangeReaderImpl::Async(range_reader) => range_reader.exist(),
        }
    }

    /// 获取当前对象的文件大小
    pub fn file_size(&self) -> IoResult<u64> {
        match &self.0 {
            RangeReaderImpl::Sync(range_reader) => range_reader.file_size(),
            RangeReaderImpl::Async(range_reader) => range_reader.file_size(),
        }
    }

    /// 下载当前对象到内存缓冲区中
    pub fn download(&self) -> IoResult<Vec<u8>> {
        match &self.0 {
            RangeReaderImpl::Sync(range_reader) => range_reader.download(),
            RangeReaderImpl::Async(range_reader) => range_reader.download(),
        }
    }

    /// 下载当前对象到指定输出流中
    pub fn download_to(&self, writer: &mut dyn WriteSeek) -> IoResult<u64> {
        match &self.0 {
            RangeReaderImpl::Sync(range_reader) => range_reader.download_to(writer),
            RangeReaderImpl::Async(range_reader) => range_reader.download_to(writer),
        }
    }

    /// 下载对象的最后指定个字节到缓冲区中，返回实际下载的字节数和整个文件的大小
    pub fn read_last_bytes(&self, buf: &mut [u8]) -> IoResult<(u64, u64)> {
        match &self.0 {
            RangeReaderImpl::Sync(range_reader) => range_reader.read_last_bytes(buf),
            RangeReaderImpl::Async(range_reader) => range_reader.read_last_bytes(buf),
        }
    }
}

impl ReadAt for RangeReader {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> IoResult<usize> {
        match &self.0 {
            RangeReaderImpl::Sync(range_reader) => range_reader.read_at(pos, buf),
            RangeReaderImpl::Async(range_reader) => range_reader.read_at(pos, buf),
        }
    }
}
