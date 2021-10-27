use super::{
    base::credential::Credential,
    config::{
        build_range_reader_builder_from_config, build_range_reader_builder_from_env, http_client,
        with_current_qiniu_config, Config,
    },
    dot::{ApiName, DotType, Dotter},
    host_selector::HostSelector,
    query::HostsQuerier,
    req_id::{get_req_id, REQUEST_ID_HEADER},
};
use log::{debug, error, info, warn};
use multipart::server::Multipart;
use positioned_io::ReadAt;
use reqwest::{
    blocking::{RequestBuilder as HTTPRequestBuilder, Response as HTTPResponse},
    header::{HeaderValue, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    Error as ReqwestError, Method, StatusCode,
};
use std::{
    io::{
        copy as io_copy, Cursor, Error as IOError, ErrorKind as IOErrorKind, Read,
        Result as IOResult, Seek, SeekFrom, Write,
    },
    result::Result,
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant, SystemTime, SystemTimeError, UNIX_EPOCH},
};
use tap::prelude::*;
use text_io::{try_scan as try_scan_text, Error as TextIOError};
use url::Url;

/// 为私有空间签发对象下载 URL
/// # Arguments
///
/// * `c` - 私有空间所在账户的凭证
/// * `url` - 对象下载 URL
/// * `deadline` - 下载 URL 有效截止时间
pub fn sign_download_url_with_deadline(
    c: &Credential,
    url: Url,
    deadline: SystemTime,
) -> Result<String, SystemTimeError> {
    let mut signed_url = url.to_string();

    if signed_url.contains('?') {
        signed_url.push_str("&e=");
    } else {
        signed_url.push_str("?e=");
    }

    let deadline = deadline.duration_since(UNIX_EPOCH)?.as_secs().to_string();
    signed_url.push_str(&deadline);
    let signature = c.sign(signed_url.as_bytes());
    signed_url.push_str("&token=");
    signed_url.push_str(&signature);
    Ok(signed_url)
}

/// 为私有空间签发对象下载 URL
/// # Arguments
///
/// * `c` - 私有空间所在账户的凭证
/// * `url` - 对象下载 URL
/// * `lifetime` - 下载 URL 有效期
pub fn sign_download_url_with_lifetime(
    c: &Credential,
    url: Url,
    lifetime: Duration,
) -> Result<String, SystemTimeError> {
    let deadline = SystemTime::now() + lifetime;
    sign_download_url_with_deadline(c, url, deadline)
}

/// 对象范围下载器
#[derive(Debug)]
pub struct RangeReader {
    inner: Arc<RangeReaderInner>,
    key: String,
}

#[derive(Debug)]
pub(super) struct RangeReaderInner {
    io_selector: HostSelector,
    dotter: Dotter,
    credential: Credential,
    bucket: String,
    tries: usize,
    use_getfile_api: bool,
    normalize_key: bool,
    use_https: bool,
    private_url_lifetime: Option<Duration>,
}

#[derive(Debug)]
/// 对象范围下载构建器
pub struct RangeReaderBuilder {
    credential: Credential,
    bucket: String,
    key: String,
    io_urls: Vec<String>,
    uc_urls: Vec<String>,
    monitor_urls: Vec<String>,
    io_tries: usize,
    uc_tries: usize,
    dot_tries: usize,
    update_interval: Duration,
    punish_duration: Duration,
    base_timeout: Duration,
    max_punished_times: usize,
    max_punished_hosts_percent: u8,
    use_getfile_api: bool,
    normalize_key: bool,
    private_url_lifetime: Option<Duration>,
    use_https: bool,
    dot_interval: Option<Duration>,
    max_dot_buffer_size: Option<u64>,
}

impl RangeReaderBuilder {
    /// 创建对象范围下载构建器
    /// # Arguments
    ///
    /// * `bucket` - 存储空间
    /// * `key` - 对象名称
    /// * `credential` - 存储空间所在账户的凭证
    /// * `io_urls` - 七牛 IO 服务器 URL 列表
    #[inline]
    pub fn new(
        bucket: impl Into<String>,
        key: impl Into<String>,
        credential: Credential,
        io_urls: Vec<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
            credential,
            io_urls,
            uc_urls: vec![],
            monitor_urls: vec![],
            io_tries: 10,
            uc_tries: 10,
            dot_tries: 10,
            update_interval: Duration::from_secs(60),
            punish_duration: Duration::from_secs(30 * 60),
            base_timeout: Duration::from_secs(3),
            max_punished_times: 5,
            max_punished_hosts_percent: 50,
            use_getfile_api: true,
            normalize_key: false,
            private_url_lifetime: None,
            use_https: false,
            dot_interval: None,
            max_dot_buffer_size: None,
        }
    }

    /// 设置七牛 UC 服务器 URL 列表
    #[inline]
    pub fn uc_urls(mut self, urls: Vec<String>) -> Self {
        self.uc_urls = urls;
        self
    }

    /// 设置七牛监控服务器 URL 列表
    #[inline]
    pub fn monitor_urls(mut self, urls: Vec<String>) -> Self {
        self.monitor_urls = urls;
        self
    }

    /// 设置对象下载最大尝试次数
    #[inline]
    pub fn io_tries(mut self, tries: usize) -> Self {
        self.io_tries = tries;
        self
    }

    /// 设置 UC 查询的最大尝试次数
    #[inline]
    pub fn uc_tries(mut self, tries: usize) -> Self {
        self.uc_tries = tries;
        self
    }

    /// 设置打点记录上传的最大尝试次数
    #[inline]
    pub fn dot_tries(mut self, tries: usize) -> Self {
        self.dot_tries = tries;
        self
    }

    /// 设置 UC 查询的频率
    #[inline]
    pub fn update_interval(mut self, interval: Duration) -> Self {
        self.update_interval = interval;
        self
    }

    /// 设置域名访问失败后的惩罚时长
    #[inline]
    pub fn punish_duration(mut self, duration: Duration) -> Self {
        self.punish_duration = duration;
        self
    }

    /// 设置域名访问的基础超时时长
    #[inline]
    pub fn base_timeout(mut self, timeout: Duration) -> Self {
        self.base_timeout = timeout;
        self
    }

    /// 设置失败域名的最大重试次数
    ///
    /// 一旦一个域名的被惩罚次数超过限制，则域名选择器不会选择该域名，除非被惩罚的域名比例超过上限，或惩罚时长超过指定时长
    #[inline]
    pub fn max_punished_times(mut self, max_times: usize) -> Self {
        self.max_punished_times = max_times;
        self
    }

    /// 设置被惩罚的域名最大比例
    ///
    /// 域名选择器在搜索域名时，一旦被跳过的域名比例大于该值，则下一个域名将被选中，不管该域名是否也被惩罚。一旦该域名成功，则惩罚将立刻被取消
    #[inline]
    pub fn max_punished_hosts_percent(mut self, percent: u8) -> Self {
        self.max_punished_hosts_percent = percent;
        self
    }

    /// 设置是否使用 getfile API 下载
    #[inline]
    pub fn use_getfile_api(mut self, use_getfile_api: bool) -> Self {
        self.use_getfile_api = use_getfile_api;
        self
    }

    /// 设置是否对 key 进行格式化
    #[inline]
    pub fn normalize_key(mut self, normalize_key: bool) -> Self {
        self.normalize_key = normalize_key;
        self
    }

    /// 设置私有空间下载 URL 有效期，如果为 None，则使用公开空间下载 URL
    #[inline]
    pub fn private_url_lifetime(mut self, private_url_lifetime: Option<Duration>) -> Self {
        self.private_url_lifetime = private_url_lifetime;
        self
    }

    /// 设置打点记录上传频率
    #[inline]
    pub fn dot_interval(mut self, dot_interval: Duration) -> Self {
        self.dot_interval = Some(dot_interval);
        self
    }

    /// 设置打点记录本地缓存文件尺寸上限
    #[inline]
    pub fn max_dot_buffer_size(mut self, max_dot_buffer_size: u64) -> Self {
        self.max_dot_buffer_size = Some(max_dot_buffer_size);
        self
    }

    /// 设置是否使用 HTTPS 协议来访问 IO 服务器
    #[inline]
    pub fn use_https(mut self, use_https: bool) -> Self {
        self.use_https = use_https;
        self
    }

    /// 构建范围下载器
    #[inline]
    pub fn build(self) -> RangeReader {
        let (inner, key) = self.build_inner_and_key();
        RangeReader { inner, key }
    }

    fn build_inner_and_key(self) -> (Arc<RangeReaderInner>, String) {
        let dotter = Dotter::new(
            self.credential.to_owned(),
            self.bucket.to_owned(),
            self.monitor_urls,
            self.dot_interval,
            self.max_dot_buffer_size,
            Some(self.dot_tries),
            Some(self.punish_duration),
            Some(self.max_punished_times),
            Some(self.max_punished_hosts_percent),
            Some(self.base_timeout),
        );

        let io_querier = if self.uc_urls.is_empty() {
            None
        } else {
            Some(HostsQuerier::new(
                HostSelector::builder(self.uc_urls)
                    .update_interval(self.update_interval)
                    .punish_duration(self.punish_duration)
                    .max_punished_times(self.max_punished_times)
                    .max_punished_hosts_percent(self.max_punished_hosts_percent)
                    .base_timeout(self.base_timeout)
                    .build(),
                self.uc_tries,
                dotter.to_owned(),
            ))
        };
        let access_key = self.credential.access_key().to_owned();
        let bucket = self.bucket.to_owned();
        let use_https = self.use_https;

        let io_selector = HostSelector::builder(self.io_urls)
            .update_callback(Some(Box::new(move || -> IOResult<Vec<String>> {
                if let Some(io_querier) = &io_querier {
                    io_querier.query_for_io_urls(&access_key, &bucket, use_https)
                } else {
                    Ok(vec![])
                }
            })))
            .should_punish_callback(Some(Box::new(|error| {
                !matches!(error.kind(), IOErrorKind::InvalidData)
            })))
            .update_interval(self.update_interval)
            .punish_duration(self.punish_duration)
            .max_punished_times(self.max_punished_times)
            .max_punished_hosts_percent(self.max_punished_hosts_percent)
            .base_timeout(self.base_timeout)
            .build();

        (
            Arc::new(RangeReaderInner {
                io_selector,
                dotter,
                credential: self.credential,
                bucket: self.bucket,
                tries: self.io_tries,
                use_getfile_api: self.use_getfile_api,
                normalize_key: self.normalize_key,
                use_https: self.use_https,
                private_url_lifetime: self.private_url_lifetime,
            }),
            self.key,
        )
    }

    /// 从配置创建范围下载构建器
    /// # Arguments
    ///
    /// * `key` - 对象名称
    /// * `config` - 下载配置
    #[inline]
    pub fn from_config(key: impl Into<String>, config: &Config) -> Self {
        build_range_reader_builder_from_config(key.into(), config)
    }

    /// 从环境变量创建范围下载构建器
    /// # Arguments
    ///
    /// * `key` - 对象名称
    #[inline]
    pub fn from_env(key: impl Into<String>) -> Option<Self> {
        build_range_reader_builder_from_env(key.into(), false)
    }
}

impl RangeReader {
    /// 创建范围下载构建器
    #[inline]
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
    #[inline]
    pub fn from_config(key: impl Into<String>, config: &Config) -> Self {
        RangeReaderBuilder::from_config(key, config).build()
    }

    /// 从环境变量创建范围下载器
    /// # Arguments
    ///
    /// * `key` - 对象名称
    #[inline]
    pub fn from_env(key: impl Into<String>) -> Option<Self> {
        let key = key.into();
        with_current_qiniu_config(|config| {
            config.and_then(|config| {
                config.with_key(&key.to_owned(), |config| {
                    config.get_or_init_range_reader_inner(|| {
                        RangeReaderBuilder::from_config(String::new(), config)
                            .build_inner_and_key()
                            .0
                    })
                })
            })
        })
        .map(|inner| Self { inner, key })
    }

    /// 主动更新域名列表
    ///
    /// 如果返回为 true 表示更新成功，否则返回 false
    pub fn update_urls(&self) -> bool {
        self.inner.io_selector.update_hosts()
    }

    /// 获取当前可用的 IO 节点的域名
    pub fn io_urls(&self) -> Vec<String> {
        return self
            .inner
            .io_selector
            .hosts()
            .iter()
            .map(|host| normalize_host(host, self.inner.use_https))
            .collect();

        #[inline]
        fn normalize_host(host: &str, use_https: bool) -> String {
            if host.contains("://") {
                host.to_string()
            } else if use_https {
                "https://".to_owned() + host
            } else {
                "http://".to_owned() + host
            }
        }
    }
}

impl ReadAt for RangeReader {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> IOResult<usize> {
        let size = buf.len() as u64;
        if size == 0 {
            return Ok(0);
        }
        let mut cursor = Cursor::new(buf);
        let range = format!("bytes={}-{}", pos, pos + size - 1);
        let begin_at = Instant::now();

        self.with_retries(
            Method::GET,
            ApiName::RangeReaderReadAt,
            |tries, request_builder, req_id, download_url, chosen_host, timeout_power| {
                debug!(
                    "[{}] read_at url: {}, req_id: {:?}, range: {}",
                    tries, download_url, req_id, &range
                );
                let begin_at = Instant::now();

                let result = request_builder
                    .header(RANGE, &range)
                    .send()
                    .tap_err(|err| self.punish_if_needed(chosen_host, timeout_power, err))
                    .map_err(|err| IOError::new(IOErrorKind::ConnectionAborted, err))
                    .and_then(|resp| {
                        let code = resp.status();
                        if code != StatusCode::PARTIAL_CONTENT && code != StatusCode::OK {
                            return Err(unexpected_status_code(&resp));
                        }
                        let content_length = parse_content_length(&resp);
                        let max_size = content_length.min(size);
                        io_copy(
                            &mut self.wrap_reader(resp.take(max_size), chosen_host, timeout_power),
                            &mut cursor,
                        )
                        .map_err(|err| IOError::new(IOErrorKind::BrokenPipe, err))
                    });

                result
                    .map(|size| size as usize)
                    .tap_ok(|_| {
                        info!(
                            "[{}] read_at ok url: {}, range: {}, req_id: {:?}, elapsed: {:?}",
                            tries,
                            download_url,
                            range,
                            req_id,
                            begin_at.elapsed(),
                        );
                    })
                    .tap_err(|err| {
                        warn!(
                            "[{}] read_at error url: {}, range: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                            tries, download_url, range, err, req_id, begin_at.elapsed(),
                        );
                        cursor.set_position(0);
                    })
            },
            |err, download_url| {
                error!(
                    "final failed read_at url = {}, error: {:?}, elapsed: {:?}",
                    download_url, err, begin_at.elapsed(),
                );
            },
        )
    }
}

/// 通过 RangeReader::read_multi_ranges() 获取文件的区域以及对应的数据
#[derive(Debug, Clone)]
pub struct RangePart {
    /// 区域对应的数据
    pub data: Vec<u8>,
    /// 区域的开始偏移量和区域长度
    pub range: (u64, u64),
}

impl RangeReader {
    /// 读取文件的多个区域，返回每个区域对应的数据
    /// # Arguments
    /// * `range` - 区域列表，每个区域有开始偏移量和区域长度组成
    pub fn read_multi_ranges(&self, ranges: &[(u64, u64)]) -> IOResult<Vec<RangePart>> {
        let range_header_value = format!("bytes={}", generate_range_header(ranges));
        let begin_at = Instant::now();

        return self.with_retries(
            Method::GET,
            ApiName::RangeReaderReadMultiRanges,
            |tries, http_request_builder, req_id, download_url, chosen_host, timeout_power| {
                debug!(
                    "[{}] read_multi_ranges url: {}, req_id: {:?}",
                    tries, download_url, req_id,
                );
                let begin_at = Instant::now();
                let result = http_request_builder
                    .header(RANGE, &range_header_value)
                    .send()
                    .tap_err(|err| self.punish_if_needed(chosen_host, timeout_power, err))
                    .map_err(|err| IOError::new(IOErrorKind::ConnectionAborted, err))
                    .and_then(|resp| {
                        let mut parts = Vec::with_capacity(ranges.len());
                        match resp.status() {
                            StatusCode::OK => {
                                let mut body = Vec::new();
                                self.wrap_reader(resp, chosen_host, timeout_power)
                                    .read_to_end(&mut body)
                                    .map_err(|err| IOError::new(IOErrorKind::BrokenPipe, err))?;
                                for &(from, len) in ranges.iter() {
                                    let from = (from as usize).min(body.len());
                                    let len = (len as usize).min(body.len() - from);
                                    if len > 0 {
                                        parts.push(RangePart {
                                            data: body[from..(from + len)].to_vec(),
                                            range: (from as u64, len as u64),
                                        });
                                    }
                                }
                            }
                            StatusCode::PARTIAL_CONTENT => {
                                if ranges.len() > 1 {
                                    let boundary = {
                                        let content_type =
                                            resp.headers().get(CONTENT_TYPE).ok_or_else(|| {
                                                IOError::new(
                                                    IOErrorKind::InvalidInput,
                                                    "Content-Type must be existed",
                                                )
                                            })?;
                                        extract_boundary(content_type.to_str().map_err(|err| {
                                            IOError::new(IOErrorKind::InvalidInput, err)
                                        })?)
                                        .ok_or_else(|| {
                                            IOError::new(
                                                IOErrorKind::InvalidInput,
                                                "Boundary must be existed in Content-Type",
                                            )
                                        })?
                                        .to_owned()
                                    };

                                    let mut body =
                                        self.wrap_reader(resp, chosen_host, timeout_power);
                                    let mut multipart = Multipart::with_body(&mut body, boundary);
                                    loop {
                                        match multipart.read_entry() {
                                            Ok(Some(mut field)) => {
                                                let content_range = field
                                                    .headers
                                                    .content_range
                                                    .ok_or_else(|| {
                                                        IOError::new(
                                                            IOErrorKind::InvalidInput,
                                                            "Content-Range must be existed",
                                                        )
                                                    })?;
                                                let (from, to, _) = parse_range_header(
                                                    &content_range,
                                                )
                                                .map_err(|_| {
                                                    IOError::new(
                                                        IOErrorKind::InvalidInput,
                                                        "Invalid Content-Range",
                                                    )
                                                })?;
                                                let len = to - from + 1;
                                                let mut data = Vec::with_capacity(len as usize);
                                                field.data.read_to_end(&mut data).map_err(
                                                    |err| {
                                                        IOError::new(IOErrorKind::BrokenPipe, err)
                                                    },
                                                )?;
                                                parts.push(RangePart {
                                                    data,
                                                    range: (from, len),
                                                });
                                            }
                                            Ok(None) => break,
                                            Err(err) => {
                                                return Err(IOError::new(
                                                    IOErrorKind::BrokenPipe,
                                                    err,
                                                ))
                                            }
                                        }
                                    }
                                } else {
                                    let content_range =
                                        resp.headers().get(CONTENT_RANGE).ok_or_else(|| {
                                            IOError::new(
                                                IOErrorKind::InvalidInput,
                                                "Content-Range must be existed",
                                            )
                                        })?;
                                    let (from, to, _) = content_range
                                        .to_str()
                                        .ok()
                                        .and_then(|r| parse_range_header(r).ok())
                                        .ok_or_else(|| {
                                            IOError::new(
                                                IOErrorKind::InvalidInput,
                                                "Invalid Content-Range",
                                            )
                                        })?;
                                    let len = to - from + 1;
                                    let mut data = Vec::with_capacity(len as usize);
                                    self.wrap_reader(resp, chosen_host, timeout_power)
                                        .read_to_end(&mut data)?;
                                    parts.push(RangePart {
                                        data,
                                        range: (from, len),
                                    });
                                }
                            }
                            _ => {
                                return Err(unexpected_status_code(&resp));
                            }
                        }

                        Ok(parts)
                    });
                result
                    .tap_ok(|_| {
                        info!(
                            "[{}] read_multi_ranges ok url: {}, req_id: {:?}, elapsed: {:?}",
                            tries, download_url, req_id, begin_at.elapsed(),
                        );
                    })
                    .tap_err(|err| {
                        warn!(
                            "[{}] read_multi_ranges error url: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                            tries, download_url, err, req_id, begin_at.elapsed(),
                        );
                    })
            },
            |err, download_url| {
                error!(
                    "final failed read_multi_ranges url = {}, error: {:?}, elapsed: {:?}",
                    download_url, err, begin_at.elapsed(),
                );
            },
        );

        fn generate_range_header(range: &[(u64, u64)]) -> String {
            range
                .iter()
                .map(|range| {
                    let start = range.0;
                    let end = start + range.1 - 1;
                    format!("{}-{}", start, end)
                })
                .collect::<Vec<_>>()
                .join(",")
        }

        fn extract_boundary(content_type: &str) -> Option<&str> {
            const BOUNDARY: &str = "boundary=";
            content_type.find(BOUNDARY).map(|idx| {
                let start = idx + BOUNDARY.len();
                let end = content_type[start..]
                    .find(';')
                    .map_or(content_type.len(), |end| start + end);
                &content_type[start..end]
            })
        }
    }

    /// 判定当前对象是否存在
    pub fn exist(&self) -> IOResult<bool> {
        let begin_at = Instant::now();
        self.with_retries(
            Method::HEAD,
            ApiName::RangeReaderExist,
            |tries, request_builder, req_id, download_url, chosen_host, timeout_power| {
                debug!(
                    "[{}] exist url: {}, req_id: {:?}",
                    tries, download_url, req_id
                );
                let begin_at = Instant::now();
                let result = request_builder
                    .send()
                    .tap_err(|err| self.punish_if_needed(chosen_host, timeout_power, err))
                    .map_err(|err| IOError::new(IOErrorKind::ConnectionAborted, err))
                    .and_then(|resp| match resp.status() {
                        StatusCode::OK => Ok(true),
                        StatusCode::NOT_FOUND => Ok(false),
                        _ => Err(unexpected_status_code(&resp)),
                    });
                result
                    .tap_ok(|_| {
                        info!(
                            "[{}] exist ok url: {}, req_id: {:?}, elapsed: {:?}",
                            tries,
                            download_url,
                            req_id,
                            begin_at.elapsed(),
                        );
                    })
                    .tap_err(|err| {
                        warn!(
                            "[{}] exist error url: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                            tries,
                            download_url,
                            err,
                            req_id,
                            begin_at.elapsed(),
                        );
                    })
            },
            |err, download_url| {
                error!(
                    "final failed exist url = {}, error: {:?}, elapsed: {:?}",
                    download_url,
                    err,
                    begin_at.elapsed(),
                );
            },
        )
    }

    /// 获取当前对象的文件大小
    pub fn file_size(&self) -> IOResult<u64> {
        let begin_at = Instant::now();
        self.with_retries(
            Method::HEAD,
            ApiName::RangeReaderFileSize,
            |tries, request_builder, req_id, download_url, chosen_host, timeout_power| {
                debug!(
                    "[{}] file_size url: {}, req_id: {:?}",
                    tries, download_url, req_id
                );
                let begin_at = Instant::now();
                let result = request_builder
                    .send()
                    .tap_err(|err| self.punish_if_needed(chosen_host, timeout_power, err))
                    .map_err(|err| IOError::new(IOErrorKind::Other, err))
                    .and_then(|resp| {
                        if resp.status() == StatusCode::OK {
                            Ok(parse_content_length(&resp))
                        } else {
                            Err(unexpected_status_code(&resp))
                        }
                    });
                result
                    .tap_ok(|_| {
                        info!(
                            "[{}] file_size ok url: {}, req_id: {:?}, elapsed: {:?}",
                            tries,
                            download_url,
                            req_id,
                            begin_at.elapsed(),
                        );
                    })
                    .tap_err(|err| {
                        warn!(
                            "[{}] file_size error url: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                            tries,
                            download_url,
                            err,
                            req_id,
                            begin_at.elapsed(),
                        );
                    })
            },
            |err, download_url| {
                error!(
                    "final failed file_size url = {}, error: {:?}, elapsed: {:?}",
                    download_url,
                    err,
                    begin_at.elapsed(),
                );
            },
        )
    }

    /// 下载当前对象到内存缓冲区中
    pub fn download(&self) -> IOResult<Vec<u8>> {
        let mut bytes = Cursor::new(Vec::new());
        self.download_to(&mut bytes)?;
        Ok(bytes.into_inner())
    }

    /// 下载当前对象到指定输出流中
    pub fn download_to(&self, writer: &mut dyn WriteSeek) -> IOResult<u64> {
        let init_start_from = writer.seek(SeekFrom::End(0))?;
        let mut start_from = init_start_from;
        let begin_at = Instant::now();

        self.with_retries(
            Method::GET,
            ApiName::RangeReaderDownloadTo,
            |tries, mut request_builder, req_id, download_url, chosen_host, timeout_power| {
                debug!(
                    "[{}] download_to url: {}, req_id: {:?}, start_from: {}",
                    tries, download_url, req_id, start_from
                );
                let begin_at = Instant::now();
                if start_from > 0 {
                    request_builder =
                        request_builder.header(RANGE, format!("bytes={}-", start_from));
                }
                let result = request_builder
                    .send()
                    .tap_err(|err| self.punish_if_needed(chosen_host, timeout_power, err))
                    .map_err(|err| IOError::new(IOErrorKind::ConnectionAborted, err))
                    .and_then(|resp| {
                        if resp.status() == StatusCode::RANGE_NOT_SATISFIABLE {
                            Ok(0)
                        } else if resp.status() != StatusCode::OK
                            && resp.status() != StatusCode::PARTIAL_CONTENT
                        {
                            Err(unexpected_status_code(&resp))
                        } else {
                            io_copy(
                                &mut self.wrap_reader(resp, chosen_host, timeout_power),
                                writer,
                            )
                            .map_err(|err| IOError::new(IOErrorKind::BrokenPipe, err))
                        }
                    });
                let origin_start_from = start_from;
                start_from = writer.seek(SeekFrom::Current(0))?;
                result
                    .map(|_| start_from - init_start_from)
                    .tap_ok(|_| {
                        info!(
                            "[{}] download ok url: {}, start_from: {}, req_id: {:?}, elapsed: {:?}",
                            tries, download_url, origin_start_from, req_id, begin_at.elapsed(),
                        );
                    })
                    .tap_err(|err| {
                        warn!(
                            "[{}] download error url: {}, start_from: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                            tries, download_url, origin_start_from, err, req_id, begin_at.elapsed(),
                        );
                    })
            },
            |err, download_url| {
                error!(
                    "final failed download url = {}, start_from: {}, error: {:?}, elapsed: {:?}",
                    download_url, init_start_from, err, begin_at.elapsed(),
                );
            },
        )
    }

    /// 下载对象的最后指定个字节到缓冲区中，返回实际下载的字节数和整个文件的大小
    pub fn read_last_bytes(&self, buf: &mut [u8]) -> IOResult<(u64, u64)> {
        let size = buf.len() as u64;
        let mut cursor = Cursor::new(buf);
        let range = format!("bytes=-{}", size);
        let begin_at = Instant::now();

        self.with_retries(
            Method::GET,
            ApiName::RangeReaderReadLastBytes,
            |tries, request_builder, req_id, download_url, chosen_host, timeout_power| {
                debug!(
                    "[{}] read_last_bytes url: {}, req_id: {:?}, len: {}",
                    tries, download_url, req_id, size
                );
                let begin_at = Instant::now();
                let result = request_builder
                    .header(RANGE, &range)
                    .send()
                    .tap_err(|err| self.punish_if_needed(chosen_host, timeout_power, err))
                    .map_err(|err| IOError::new(IOErrorKind::ConnectionAborted, err))
                    .and_then(|resp| {
                        if resp.status() != StatusCode::PARTIAL_CONTENT {
                            return Err(unexpected_status_code(&resp));
                        }
                        let content_range = resp
                            .headers()
                            .get(CONTENT_RANGE)
                            .and_then(|r| r.to_str().ok())
                            .ok_or_else(|| {
                                IOError::new(
                                    IOErrorKind::InvalidData,
                                    "Content-Range must be existed",
                                )
                            })?;
                        let (_, _, total_size) =
                            parse_range_header(content_range).map_err(|_| {
                                IOError::new(IOErrorKind::InvalidData, "Invalid Content-Range")
                            })?;
                        let actual_size = io_copy(&mut resp.take(size), &mut cursor)?;
                        Ok((actual_size, total_size))
                    });
                result
                    .tap_ok(|_| {
                        info!(
                            "[{}] download ok url: {}, len: {}, req_id: {:?}, elapsed: {:?}",
                            tries, download_url, size, req_id, begin_at.elapsed(),
                        );
                    })
                    .tap_err(|err| {
                        warn!(
                            "[{}] download error url: {}, len: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                            tries, download_url, size, err, req_id, begin_at.elapsed(),
                        );
                        cursor.set_position(0);
                    })
            },
            |err, download_url| {
                error!(
                    "final failed read_last_bytes url = {}, len: {}, error: {:?}, elapsed: {:?}",
                    download_url, size, err, begin_at.elapsed(),
                );
            },
        )
    }

    #[inline]
    fn wrap_reader<'a, R: 'a + Read>(
        &'a self,
        source: R,
        chosen_host: &'a str,
        timeout_power: usize,
    ) -> impl Read + 'a {
        self.inner
            .io_selector
            .wrap_reader(source, chosen_host, timeout_power)
    }

    fn with_retries<T>(
        &self,
        method: Method,
        api_name: ApiName,
        mut for_each_url: impl FnMut(
            usize,
            HTTPRequestBuilder,
            &HeaderValue,
            &str,
            &str,
            usize,
        ) -> IOResult<T>,
        final_error: impl FnOnce(&IOError, &str),
    ) -> IOResult<T> {
        let begin_at = SystemTime::now();
        let begin_at_instant = Instant::now();
        assert!(self.inner.tries > 0);

        for tries in 0..self.inner.tries {
            sleep_before_retry(tries);
            let last_try = self.inner.tries - tries <= 1;

            let chosen_io_info = self.inner.io_selector.select_host();
            let download_url = sign_download_url_if_needed(
                &make_download_url(
                    &chosen_io_info.host,
                    self.inner.credential.access_key(),
                    &self.inner.bucket,
                    &self.key,
                    self.inner.use_getfile_api,
                    self.inner.normalize_key,
                ),
                self.inner.private_url_lifetime,
                &self.inner.credential,
            );
            let req_id = get_req_id(begin_at, tries);
            let request_begin_at_instant = Instant::now();
            let request_builder = http_client()
                .read()
                .unwrap()
                .request(method.to_owned(), download_url.to_owned())
                .header(REQUEST_ID_HEADER, req_id.to_owned())
                .timeout(chosen_io_info.timeout);
            match for_each_url(
                tries,
                request_builder,
                &req_id,
                download_url.as_str(),
                &chosen_io_info.host,
                chosen_io_info.timeout_power,
            ) {
                Ok(result) => {
                    self.inner.io_selector.reward(&chosen_io_info.host);
                    self.inner
                        .dotter
                        .dot(DotType::Sdk, api_name, true, begin_at_instant.elapsed())
                        .ok();
                    self.inner
                        .dotter
                        .dot(
                            DotType::Http,
                            ApiName::IoGetfile,
                            true,
                            request_begin_at_instant.elapsed(),
                        )
                        .ok();
                    return Ok(result);
                }
                Err(err) => {
                    let punished = self.inner.io_selector.punish(
                        &chosen_io_info.host,
                        &err,
                        &self.inner.dotter,
                    );
                    self.inner
                        .dotter
                        .dot(
                            DotType::Http,
                            ApiName::IoGetfile,
                            false,
                            request_begin_at_instant.elapsed(),
                        )
                        .ok();
                    if !punished || last_try {
                        final_error(&err, download_url.as_str());
                        self.inner
                            .dotter
                            .dot(DotType::Sdk, api_name, false, begin_at_instant.elapsed())
                            .ok();
                        return Err(err);
                    }
                }
            }
        }
        unreachable!();

        fn make_download_url(
            io_url: &str,
            access_key: &str,
            bucket: &str,
            key: &str,
            use_getfile_api: bool,
            normalize_key: bool,
        ) -> String {
            let mut url = if use_getfile_api {
                format!("{}/getfile/{}/{}", io_url, access_key, bucket)
            } else {
                io_url.to_owned()
            };
            if normalize_key {
                if url.ends_with('/') && key.starts_with('/') {
                    url.truncate(url.len() - 1);
                } else if !url.ends_with('/') && !key.starts_with('/') {
                    url.push('/');
                }
            }
            url.push_str(key);
            url
        }

        fn sign_download_url_if_needed(
            url: &str,
            private_url_lifetime: Option<Duration>,
            credential: &Credential,
        ) -> Url {
            if let Some(private_url_lifetime) = private_url_lifetime {
                Url::parse(
                    &sign_download_url_with_lifetime(
                        credential,
                        Url::parse(url).unwrap(),
                        private_url_lifetime,
                    )
                    .unwrap(),
                )
                .unwrap()
            } else {
                Url::parse(url).unwrap()
            }
        }

        #[inline]
        fn sleep_before_retry(tries: usize) {
            if tries >= 3 {
                sleep(Duration::from_secs(tries as u64));
            }
        }
    }

    #[inline]
    fn punish_if_needed(&self, host: &str, timeout_power: usize, err: &ReqwestError) {
        if err.is_timeout() {
            self.inner
                .io_selector
                .increase_timeout_power_by(host, timeout_power)
        } else if err.is_connect() {
            self.inner.io_selector.mark_connection_as_failed(host)
        }
    }
}

pub trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}

#[inline]
fn unexpected_status_code(resp: &HTTPResponse) -> IOError {
    let error_kind = if resp.status().is_client_error() {
        IOErrorKind::InvalidData
    } else {
        IOErrorKind::Other
    };
    IOError::new(
        error_kind,
        format!("Unexpected status code {}", resp.status().as_u16()),
    )
}

fn parse_content_length(resp: &HTTPResponse) -> u64 {
    resp.headers()
        .get(CONTENT_LENGTH)
        .and_then(|length| length.to_str().ok())
        .and_then(|length| length.parse().ok())
        .expect("Content-Length must be existed")
}

fn parse_range_header(range: &str) -> Result<(u64, u64, u64), TextIOError> {
    let from: u64;
    let to: u64;
    let total_size: u64;
    try_scan_text!(range.bytes() => "bytes {}-{}/{}", from, to, total_size);
    Ok((from, to, total_size))
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            cache_dir_path_of,
            dot::{DotRecordKey, DotRecords, DotRecordsDashMap, DOT_FILE_NAME},
        },
        *,
    };
    use futures::{channel::oneshot::channel, future::join};
    use multipart::client::lazy::Multipart;
    use serde_json::{json, to_vec as json_to_vec};
    use std::{
        boxed::Box,
        error::Error,
        io::Read,
        result::Result,
        sync::{
            atomic::{AtomicUsize, Ordering::Relaxed},
            Arc,
        },
    };
    use tokio::{
        task::{spawn, spawn_blocking},
        time::sleep as delay_for,
    };
    use warp::{
        header,
        http::{header::AUTHORIZATION, HeaderValue, StatusCode},
        hyper::Body,
        path,
        reply::Response,
        Filter,
    };

    macro_rules! starts_with_server {
        ($addr:ident, $routes:ident, $code:block) => {{
            let (tx, rx) = channel();
            let ($addr, server) =
                warp::serve($routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async move {
                    rx.await.ok();
                });
            let handler = spawn(server);
            $code;
            tx.send(()).ok();
            handler.await.ok();
        }};
        ($io_addr:ident, $monitor_addr:ident, $io_routes:ident, $records_map:ident, $code:block) => {{
            let (io_tx, io_rx) = channel();
            let (monitor_tx, monitor_rx) = channel();
            let ($io_addr, io_server) = warp::serve($io_routes).bind_with_graceful_shutdown(
                ([127, 0, 0, 1], 0),
                async move {
                    io_rx.await.ok();
                },
            );
            let $records_map = Arc::new(DotRecordsDashMap::default());
            let monitor_routes = {
                let records_map = $records_map.to_owned();
                path!("v1" / "stat")
                    .and(warp::header::value(AUTHORIZATION.as_str()))
                    .and(warp::body::json())
                    .map(move |authorization: HeaderValue, records: DotRecords| {
                        assert!(authorization.to_str().unwrap().starts_with("UpToken "));
                        records_map.merge_with_records(records);
                        Response::new(Body::empty())
                    })
            };
            let ($monitor_addr, monitor_server) = warp::serve(monitor_routes)
                .bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async move {
                    monitor_rx.await.ok();
                });
            let io_handler = spawn(io_server);
            let monitor_handler = spawn(monitor_server);
            $code;
            io_tx.send(()).ok();
            monitor_tx.send(()).ok();
            let _ = join(io_handler, monitor_handler).await;
        }};
        ($io_addr:ident, $uc_addr:ident, $io_routes:ident, $code:block) => {{
            let (io_tx, io_rx) = channel();
            let (uc_tx, uc_rx) = channel();
            let ($io_addr, io_server) = warp::serve($io_routes).bind_with_graceful_shutdown(
                ([127, 0, 0, 1], 0),
                async move {
                    io_rx.await.ok();
                },
            );
            let io_addr = $io_addr.to_owned();
            let uc_routes = {
                path!("v4" / "query")
                    .map(move || {
                        Response::new(json_to_vec(&json!({
                            "hosts": [{
                                "ttl": 86400,
                                "io": {
                                    "domains": [io_addr]
                                },
                                "uc": {
                                    "domains": []
                                }
                            }]
                        })).unwrap().into())
                    })
            };
            let ($uc_addr, uc_server) = warp::serve(uc_routes).bind_with_graceful_shutdown(
                ([127, 0, 0, 1], 0),
                async move {
                    uc_rx.await.ok();
                },
            );
            let io_handler = spawn(io_server);
            let uc_handler = spawn(uc_server);
            $code;
            io_tx.send(()).ok();
            uc_tx.send(()).ok();
            let _ = join(io_handler, uc_handler).await;
        }};
    }

    #[inline]
    fn get_credential() -> Credential {
        Credential::new("1234567890", "abcdefghijk")
    }

    #[tokio::test]
    async fn test_read_at() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let io_routes = {
            let action_1 =
                path!("file")
                    .and(header::value(RANGE.as_str()))
                    .map(|range: HeaderValue| {
                        assert_eq!(range.to_str().unwrap(), "bytes=5-10");
                        Response::new("1234567890".into())
                    });
            let action_2 =
                path!("file2")
                    .and(header::value(RANGE.as_str()))
                    .map(|range: HeaderValue| {
                        assert_eq!(range.to_str().unwrap(), "bytes=5-16");
                        Response::new("1234567890".into())
                    });
            action_1.or(action_2)
        };

        starts_with_server!(io_addr, monitor_addr, io_routes, records_map, {
            let io_urls = vec![format!("http://{}", io_addr)];
            {
                let downloader =
                    RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                        .use_getfile_api(false)
                        .normalize_key(true)
                        .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                        .dot_interval(Duration::from_millis(0))
                        .max_dot_buffer_size(1)
                        .build();
                spawn_blocking(move || {
                    let mut buf = [0u8; 6];
                    assert_eq!(downloader.read_at(5, &mut buf).unwrap(), 6);
                    assert_eq!(&buf, b"123456");
                })
                .await?;
            }
            {
                let downloader =
                    RangeReader::builder("bucket", "file2", get_credential(), io_urls.to_owned())
                        .use_getfile_api(false)
                        .normalize_key(true)
                        .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                        .dot_interval(Duration::from_millis(0))
                        .max_dot_buffer_size(1)
                        .build();
                spawn_blocking(move || {
                    let mut buf = [0u8; 12];
                    assert_eq!(downloader.read_at(5, &mut buf).unwrap(), 10);
                    assert_eq!(&buf[..10], b"1234567890");
                })
                .await?;
            }

            delay_for(Duration::from_secs(5)).await;
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Http, ApiName::IoGetfile))
                    .unwrap();
                assert_eq!(record.success_count(), Some(2));
                assert_eq!(record.failed_count(), Some(0));
            }
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Sdk, ApiName::RangeReaderReadAt))
                    .unwrap();
                assert_eq!(record.success_count(), Some(2));
                assert_eq!(record.failed_count(), Some(0));
            }
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_read_at_2() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let io_called = Arc::new(AtomicUsize::new(0));
        let io_routes = {
            let io_called = io_called.to_owned();
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=1-5");
                    io_called.fetch_add(1, Relaxed);
                    let mut resp = Response::new("12345".into());
                    *resp.status_mut() = StatusCode::NOT_IMPLEMENTED;
                    resp
                })
        };
        starts_with_server!(io_addr, monitor_addr, io_routes, records_map, {
            let io_urls = vec![format!("http://{}", io_addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .io_tries(3)
                    .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                    .dot_interval(Duration::from_millis(0))
                    .max_dot_buffer_size(1)
                    .build();
            spawn_blocking(move || {
                let mut buf = [0u8; 5];
                downloader.read_at(1, &mut buf).unwrap_err();
                assert_eq!(io_called.load(Relaxed), 3);
            })
            .await?;

            delay_for(Duration::from_secs(5)).await;
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Http, ApiName::IoGetfile))
                    .unwrap();
                assert_eq!(record.success_count(), Some(0));
                assert_eq!(record.failed_count(), Some(3));
            }
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Sdk, ApiName::RangeReaderReadAt))
                    .unwrap();
                assert_eq!(record.success_count(), Some(0));
                assert_eq!(record.failed_count(), Some(1));
            }
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_read_at_3() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let io_called = Arc::new(AtomicUsize::new(0));
        let io_routes = {
            let io_called = io_called.to_owned();
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=1-5");
                    io_called.fetch_add(1, Relaxed);
                    let mut resp = Response::new("12345".into());
                    *resp.status_mut() = StatusCode::BAD_REQUEST;
                    resp
                })
        };
        starts_with_server!(io_addr, monitor_addr, io_routes, records_map, {
            let io_urls = vec![format!("http://{}", io_addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                    .dot_interval(Duration::from_millis(0))
                    .max_dot_buffer_size(1)
                    .build();
            spawn_blocking(move || {
                let mut buf = [0u8; 5];
                downloader.read_at(1, &mut buf).unwrap_err();
                assert_eq!(io_called.load(Relaxed), 1);
            })
            .await?;

            delay_for(Duration::from_secs(5)).await;
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Http, ApiName::IoGetfile))
                    .unwrap();
                assert_eq!(record.success_count(), Some(0));
                assert_eq!(record.failed_count(), Some(1));
            }
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Sdk, ApiName::RangeReaderReadAt))
                    .unwrap();
                assert_eq!(record.success_count(), Some(0));
                assert_eq!(record.failed_count(), Some(1));
            }
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_read_last_bytes() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let io_routes =
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(|range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=-10");
                    let mut resp = Response::new("1234567890".into());
                    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                    resp.headers_mut().insert(
                        CONTENT_RANGE,
                        "bytes 157286390-157286399/157286400".parse().unwrap(),
                    );
                    resp
                });
        starts_with_server!(io_addr, monitor_addr, io_routes, records_map, {
            let io_urls = vec![format!("http://{}", io_addr)];
            {
                let downloader =
                    RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                        .use_getfile_api(false)
                        .normalize_key(true)
                        .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                        .dot_interval(Duration::from_millis(0))
                        .max_dot_buffer_size(1)
                        .build();

                spawn_blocking(move || {
                    let mut buf = [0u8; 10];
                    assert_eq!(
                        downloader.read_last_bytes(&mut buf).unwrap(),
                        (10, 157286400)
                    );
                    assert_eq!(&buf, b"1234567890");
                })
                .await?;

                delay_for(Duration::from_secs(5)).await;
                {
                    let record = records_map
                        .get(&DotRecordKey::new(DotType::Http, ApiName::IoGetfile))
                        .unwrap();
                    assert_eq!(record.success_count(), Some(1));
                    assert_eq!(record.failed_count(), Some(0));
                }
                {
                    let record = records_map
                        .get(&DotRecordKey::new(
                            DotType::Sdk,
                            ApiName::RangeReaderReadLastBytes,
                        ))
                        .unwrap();
                    assert_eq!(record.success_count(), Some(1));
                    assert_eq!(record.failed_count(), Some(0));
                }
            }
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_file() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let io_routes = { path!("file").map(|| Response::new("1234567890".into())) };
        starts_with_server!(io_addr, monitor_addr, io_routes, records_map, {
            let io_urls = vec![format!("http://{}", io_addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                    .dot_interval(Duration::from_millis(0))
                    .max_dot_buffer_size(1)
                    .build();
            spawn_blocking(move || {
                assert!(downloader.exist().unwrap());
                assert_eq!(downloader.file_size().unwrap(), 10);
                assert_eq!(&downloader.download().unwrap(), b"1234567890");
            })
            .await?;

            delay_for(Duration::from_secs(5)).await;
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Http, ApiName::IoGetfile))
                    .unwrap();
                assert_eq!(record.success_count(), Some(3));
                assert_eq!(record.failed_count(), Some(0));
            }
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Sdk, ApiName::RangeReaderExist))
                    .unwrap();
                assert_eq!(record.success_count(), Some(1));
                assert_eq!(record.failed_count(), Some(0));
            }
            {
                let record = records_map
                    .get(&DotRecordKey::new(
                        DotType::Sdk,
                        ApiName::RangeReaderFileSize,
                    ))
                    .unwrap();
                assert_eq!(record.success_count(), Some(1));
                assert_eq!(record.failed_count(), Some(0));
            }
            {
                let record = records_map
                    .get(&DotRecordKey::new(
                        DotType::Sdk,
                        ApiName::RangeReaderDownloadTo,
                    ))
                    .unwrap();
                assert_eq!(record.success_count(), Some(1));
                assert_eq!(record.failed_count(), Some(0));
            }
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_file_2() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let counter = Arc::new(AtomicUsize::new(0));
        let routes = {
            let counter = counter.to_owned();
            path!("file").map(move || {
                counter.fetch_add(1, Relaxed);
                let mut resp = Response::new("12345".into());
                *resp.status_mut() = StatusCode::NOT_IMPLEMENTED;
                resp
            })
        };
        starts_with_server!(addr, monitor_addr, routes, records_map, {
            let io_urls = vec![format!("http://{}", addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .io_tries(3)
                    .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .dot_interval(Duration::from_millis(0))
                    .max_dot_buffer_size(1)
                    .build();
            spawn_blocking(move || {
                downloader.exist().unwrap_err();
                downloader.file_size().unwrap_err();
                downloader.download().unwrap_err();
                assert_eq!(counter.load(Relaxed), 3 * 3);
            })
            .await?;
            delay_for(Duration::from_secs(5)).await;
            {
                let record = records_map
                    .get(&DotRecordKey::new(
                        DotType::Sdk,
                        ApiName::RangeReaderFileSize,
                    ))
                    .unwrap();
                assert_eq!(record.success_count(), Some(0));
                assert_eq!(record.failed_count(), Some(1));
            }
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Sdk, ApiName::RangeReaderExist))
                    .unwrap();
                assert_eq!(record.success_count(), Some(0));
                assert_eq!(record.failed_count(), Some(1));
            }
            {
                let record = records_map
                    .get(&DotRecordKey::new(
                        DotType::Sdk,
                        ApiName::RangeReaderDownloadTo,
                    ))
                    .unwrap();
                assert_eq!(record.success_count(), Some(0));
                assert_eq!(record.failed_count(), Some(1));
            }
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Http, ApiName::IoGetfile))
                    .unwrap();
                assert_eq!(record.success_count(), Some(0));
                assert_eq!(record.failed_count(), Some(9));
            }
            {
                let record = records_map.get(&DotRecordKey::punished()).unwrap();
                assert_eq!(record.punished_count(), Some(4));
            }
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_file_3() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let counter = Arc::new(AtomicUsize::new(0));
        let routes = {
            let counter = counter.to_owned();
            path!("file").map(move || {
                counter.fetch_add(1, Relaxed);
                let mut resp = Response::new("12345".into());
                *resp.status_mut() = StatusCode::BAD_REQUEST;
                resp
            })
        };
        starts_with_server!(addr, routes, {
            let io_urls = vec![format!("http://{}", addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .build();
            spawn_blocking(move || {
                downloader.exist().unwrap_err();
                downloader.file_size().unwrap_err();
                downloader.download().unwrap_err();
                assert_eq!(counter.load(Relaxed), 3);
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_file_4() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let routes = { path!("file").map(|| Response::new("1234567890".into())) };
        starts_with_server!(addr, routes, {
            let io_urls = vec![format!("http://{}", addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .build();
            spawn_blocking(move || {
                assert!(downloader.exist().unwrap());
                assert_eq!(downloader.file_size().unwrap(), 10);
                let mut buf = Vec::new();
                {
                    let mut cursor = Cursor::new(&mut buf);
                    assert_eq!(downloader.download_to(&mut cursor).unwrap(), 10);
                }
                assert_eq!(&buf, b"1234567890");
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_range() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let routes = {
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-9");
                    let mut response_body = Multipart::new();
                    response_body.add_stream(
                        "",
                        Cursor::new(b"12345"),
                        None,
                        None,
                        Some("bytes 0-4/10"),
                    );
                    response_body.add_stream(
                        "",
                        Cursor::new(b"67890"),
                        None,
                        None,
                        Some("bytes 5-9/19"),
                    );
                    let mut fields = response_body.prepare().unwrap();
                    let mut buffer = Vec::new();
                    fields.read_to_end(&mut buffer).unwrap();
                    let mut response = Response::new(buffer.into());
                    *response.status_mut() = StatusCode::PARTIAL_CONTENT;
                    response.headers_mut().insert(
                        CONTENT_TYPE,
                        ("multipart/form-data; boundary=".to_owned() + fields.boundary())
                            .parse()
                            .unwrap(),
                    );
                    response
                })
        };
        starts_with_server!(addr, routes, {
            let io_urls = vec![format!("http://{}", addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .build();
            let ranges = [(0, 5), (5, 5)];
            spawn_blocking(move || {
                let parts = downloader.read_multi_ranges(&ranges).unwrap();
                assert_eq!(parts.len(), 2);
                assert_eq!(&parts.get(1).unwrap().data, b"12345");
                assert_eq!(parts.get(1).unwrap().range, (0, 5));
                assert_eq!(&parts.get(0).unwrap().data, b"67890");
                assert_eq!(parts.get(0).unwrap().range, (5, 5));
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_range_2() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let routes = {
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-9");
                    "12345678901357924680"
                })
        };
        starts_with_server!(addr, routes, {
            let io_urls = vec![format!("http://{}", addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .build();
            let ranges = [(0, 5), (5, 5)];
            spawn_blocking(move || {
                let parts = downloader.read_multi_ranges(&ranges).unwrap();
                assert_eq!(parts.len(), 2);
                assert_eq!(&parts.get(0).unwrap().data, b"12345");
                assert_eq!(parts.get(0).unwrap().range, (0, 5));
                assert_eq!(&parts.get(1).unwrap().data, b"67890");
                assert_eq!(parts.get(1).unwrap().range, (5, 5));
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_range_3() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let counter = Arc::new(AtomicUsize::new(0));
        let routes = {
            let counter = counter.to_owned();
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(move |range: HeaderValue| {
                    counter.fetch_add(1, Relaxed);
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-9");
                    let mut resp = Response::new("12345".into());
                    *resp.status_mut() = StatusCode::NOT_IMPLEMENTED;
                    resp
                })
        };
        starts_with_server!(addr, routes, {
            let io_urls = vec![format!("http://{}", addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .io_tries(3)
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .build();
            let ranges = [(0, 5), (5, 5)];
            let c = counter.to_owned();
            spawn_blocking(move || {
                downloader.read_multi_ranges(&ranges).unwrap_err();
                assert_eq!(c.load(Relaxed), 3);
            })
            .await?;

            let io_urls = vec![format!("http://{}", addr)];
            let downloader =
                RangeReader::builder("bucket", "/file", get_credential(), io_urls.to_owned())
                    .io_tries(3)
                    .use_getfile_api(false)
                    .build();
            let ranges = [(0, 5), (5, 5)];
            let c = counter.to_owned();
            spawn_blocking(move || {
                downloader.read_multi_ranges(&ranges).unwrap_err();
                assert_eq!(c.load(Relaxed), 6);
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_range_4() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let routes = {
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-9");
                    let mut response_body = Multipart::new();
                    response_body.add_stream(
                        "",
                        Cursor::new(b"12345"),
                        None,
                        None,
                        Some("bytes 0-4/6"),
                    );
                    response_body.add_stream(
                        "",
                        Cursor::new(b"6"),
                        None,
                        None,
                        Some("bytes 5-5/6"),
                    );
                    let mut fields = response_body.prepare().unwrap();
                    let mut buffer = Vec::new();
                    fields.read_to_end(&mut buffer).unwrap();
                    let mut response = Response::new(buffer.into());
                    *response.status_mut() = StatusCode::PARTIAL_CONTENT;
                    response.headers_mut().insert(
                        CONTENT_TYPE,
                        ("multipart/form-data; boundary=".to_owned() + fields.boundary())
                            .parse()
                            .unwrap(),
                    );
                    response
                })
        };
        starts_with_server!(addr, routes, {
            let io_urls = vec![format!("http://{}", addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .build();
            let ranges = [(0, 5), (5, 5)];
            spawn_blocking(move || {
                let parts = downloader.read_multi_ranges(&ranges).unwrap();
                assert_eq!(parts.len(), 2);
                assert_eq!(&parts.get(1).unwrap().data, b"12345");
                assert_eq!(parts.get(1).unwrap().range, (0, 5));
                assert_eq!(&parts.get(0).unwrap().data, b"6");
                assert_eq!(parts.get(0).unwrap().range, (5, 1));
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_range_5() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let routes = {
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-5");
                    "1234"
                })
        };
        starts_with_server!(addr, routes, {
            let io_urls = vec![format!("http://{}", addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .build();
            let ranges = [(0, 5), (5, 1)];
            spawn_blocking(move || {
                let parts = downloader.read_multi_ranges(&ranges).unwrap();
                assert_eq!(parts.len(), 1);
                assert_eq!(&parts.get(0).unwrap().data, b"1234");
                assert_eq!(parts.get(0).unwrap().range, (0, 4));
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_range_6() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        let routes = {
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-3");
                    let mut response = Response::new("123".into());
                    response
                        .headers_mut()
                        .insert(CONTENT_RANGE, "bytes 0-3/3".parse().unwrap());
                    response
                })
        };
        starts_with_server!(addr, routes, {
            let io_urls = vec![format!("http://{}", addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .build();
            let ranges = [(0, 4)];
            spawn_blocking(move || {
                let parts = downloader.read_multi_ranges(&ranges).unwrap();
                assert_eq!(parts.len(), 1);
                assert_eq!(&parts.get(0).unwrap().data, b"123");
                assert_eq!(parts.get(0).unwrap().range, (0, 3));
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_update_hosts() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let routes = { path!("file").map(move || Response::new("12345".into())) };
        starts_with_server!(io_addr, uc_addr, routes, {
            let io_urls = vec!["http://fakedomain:12345".to_owned()];
            let uc_urls = vec![format!("http://{}", uc_addr)];
            let downloader =
                RangeReader::builder("bucket", "file", get_credential(), io_urls.to_owned())
                    .uc_urls(uc_urls)
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .build();
            spawn_blocking(move || {
                assert_eq!(downloader.io_urls(), io_urls);
                assert!(downloader.update_urls());
                assert_eq!(downloader.io_urls(), vec![format!("http://{}", io_addr)]);
                assert_eq!(&downloader.download().unwrap(), b"12345");
            })
            .await?;
        });
        Ok(())
    }

    #[test]
    fn test_sign_download_url_with_deadline() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let credential = Credential::new("abcdefghklmnopq", "1234567890");
        assert_eq!(
            sign_download_url_with_deadline(&credential,
                Url::parse("http://www.qiniu.com/?go=1")?,
                SystemTime::UNIX_EPOCH + Duration::from_secs(1_234_567_890 + 3600),
            )?,
            "http://www.qiniu.com/?go=1&e=1234571490&token=abcdefghklmnopq:KjQtlGAkEOhSwtFjJfYtYa2-reE=",
        );
        Ok(())
    }

    fn clear_cache() -> IOResult<()> {
        let cache_file_path = cache_dir_path_of("query-cache.json")?;
        std::fs::remove_file(&cache_file_path).or_else(|err| {
            if err.kind() == IOErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        })?;
        let dot_file_path = cache_dir_path_of(DOT_FILE_NAME)?;
        std::fs::remove_file(&dot_file_path).or_else(|err| {
            if err.kind() == IOErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        })?;
        Ok(())
    }
}
