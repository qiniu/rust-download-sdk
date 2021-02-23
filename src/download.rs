use super::{
    base::credential::Credential,
    config::{build_range_reader_builder_from_config, build_range_reader_builder_from_env, Config},
    query::query_for_io_urls,
    req_id::{get_req_id, REQUEST_ID_HEADER},
    HTTP_CLIENT,
};
use log::{debug, warn};
use multipart::server::Multipart;
use positioned_io::ReadAt;
use rand::{seq::SliceRandom, thread_rng};
use reqwest::{
    blocking::Response as HTTPResponse,
    header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    StatusCode,
};
use std::{
    io::{
        copy as io_copy, Cursor, Error as IOError, ErrorKind as IOErrorKind, Read,
        Result as IOResult, Seek, SeekFrom, Write,
    },
    result::Result,
    thread::sleep,
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};
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
    urls: Vec<Url>,
    tries: usize,
    max_continuous_failed_times: usize,
    max_continuous_failed_duration: Duration,
    max_seek_hosts_percent: usize,
    // hostname_failures: DashMap<String, FailureInfo>,
}

/// 对象范围下载构建器
#[derive(Debug, Default)]
pub struct RangeReaderBuilder {
    inner: RangeReader,
}

impl RangeReaderBuilder {
    /// 设置对象下载 URL 列表
    #[inline]
    pub fn urls(mut self, urls: Vec<Url>) -> Self {
        self.inner.urls = urls;
        self
    }

    /// 设置对象下载最大尝试次数
    #[inline]
    pub fn tries(mut self, tries: usize) -> Self {
        self.inner.tries = tries;
        self
    }

    /// 设置对象下载最大连续错误次数
    #[inline]
    pub fn max_continuous_failed_times(mut self, max_times: usize) -> Self {
        self.inner.max_continuous_failed_times = max_times;
        self
    }

    /// 设置对象下载最大连续错误时长
    #[inline]
    pub fn max_continuous_failed_duration(mut self, failed_duration: Duration) -> Self {
        self.inner.max_continuous_failed_duration = failed_duration;
        self
    }

    /// 设置对象下载最大搜索主机地址百分比
    #[inline]
    pub fn max_seek_hosts_percent(mut self, percent: usize) -> Self {
        self.inner.max_seek_hosts_percent = percent;
        self
    }

    /// 构建范围下载器
    #[inline]
    pub fn build(self) -> RangeReader {
        self.inner
    }

    /// 创建范围下载构建器
    /// # Arguments
    ///
    /// * `bucket` - 存储空间
    /// * `key` - 对象名称
    /// * `uc_urls` - UC 服务器 URL 列表
    /// * `io_urls` - IO 服务器 URL 列表
    /// * `credential` - 存储空间所在账户的凭证
    /// * `use_getfile` - 是否使用 getfile API 下载
    /// * `private_url_lifetime` - 私有空间下载 URL 有效期，如果为 None，则使用公开空间下载 URL
    /// * `tries` - 最大下载测试次数
    pub fn new(
        bucket: &str,
        key: &str,
        uc_urls: Option<&[String]>,
        io_urls: &[String],
        credential: &Credential,
        use_getfile: bool,
        private_url_lifetime: Option<Duration>,
        tries: usize,
    ) -> Self {
        assert!(tries > 0);
        let new_io_urls = uc_urls
            .map(|uc_urls| {
                query_for_io_urls(credential.access_key(), bucket, uc_urls, false)
                    .unwrap_or_else(|_| io_urls.to_owned())
            })
            .unwrap_or_else(|| io_urls.to_owned());
        let urls = new_io_urls
            .iter()
            .map(|io_url| {
                let url =
                    make_download_url(io_url, credential.access_key(), bucket, key, use_getfile);
                sign_download_url_if_needed(&url, private_url_lifetime, credential)
            })
            .collect::<Vec<_>>();
        return Self::default().urls(urls).tries(tries);

        fn make_download_url(
            io_url: &str,
            access_key: &str,
            bucket: &str,
            key: &str,
            use_getfile: bool,
        ) -> String {
            let mut url = if use_getfile {
                io_url.to_owned()
            } else {
                format!("{}/getfile/{}/{}", io_url, access_key, bucket)
            };
            if url.ends_with("/") && key.starts_with("/") {
                url.truncate(url.len() - 1);
            } else if !url.ends_with("/") && !key.starts_with("/") {
                url.push_str("/");
            }
            url.push_str(key);
            return url;
        }

        fn sign_download_url_if_needed(
            url: &str,
            private_url_lifetime: Option<Duration>,
            credential: &Credential,
        ) -> Url {
            if let Some(private_url_lifetime) = private_url_lifetime {
                Url::parse(
                    &sign_download_url_with_lifetime(
                        &credential,
                        Url::parse(&url).unwrap(),
                        private_url_lifetime,
                    )
                    .unwrap(),
                )
                .unwrap()
            } else {
                Url::parse(&url).unwrap()
            }
        }
    }

    /// 从配置创建范围下载构建器
    /// # Arguments
    ///
    /// * `key` - 对象名称
    /// * `config` - 下载配置
    #[inline]
    pub fn from_config(key: impl AsRef<str>, config: &Config) -> Self {
        build_range_reader_builder_from_config(key.as_ref(), config)
    }

    /// 从环境变量创建范围下载构建器
    /// # Arguments
    ///
    /// * `key` - 对象名称
    #[inline]
    pub fn from_env(key: impl AsRef<str>) -> Option<Self> {
        build_range_reader_builder_from_env(key.as_ref())
    }
}

impl Default for RangeReader {
    #[inline]
    fn default() -> Self {
        Self {
            urls: Default::default(),
            tries: 5,
            max_continuous_failed_times: 5,
            max_continuous_failed_duration: Duration::from_secs(60),
            max_seek_hosts_percent: 50,
            // hostname_failures: Default::default(),
        }
    }
}

impl RangeReader {
    /// 创建范围下载构建器
    #[inline]
    pub fn builder() -> RangeReaderBuilder {
        RangeReaderBuilder::default()
    }

    /// 快速创建范围下载器
    pub fn new(urls: Vec<Url>, tries: usize) -> Self {
        Self::builder().urls(urls).tries(tries).build()
    }

    /// 从配置创建范围下载器
    /// # Arguments
    ///
    /// * `key` - 对象名称
    /// * `config` - 下载配置
    #[inline]
    pub fn from_config(key: impl AsRef<str>, config: &Config) -> Self {
        RangeReaderBuilder::from_config(key, config).build()
    }

    /// 从环境变量创建范围下载器
    /// # Arguments
    ///
    /// * `key` - 对象名称
    #[inline]
    pub fn from_env(key: impl AsRef<str>) -> Option<Self> {
        RangeReaderBuilder::from_env(key).map(|b| b.build())
    }
}

impl ReadAt for RangeReader {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> IOResult<usize> {
        let size = buf.len() as u64;
        if size == 0 {
            return Ok(0);
        }
        let mut cursor = Cursor::new(buf);
        let mut io_error: Option<IOError> = None;
        let mut last_url: Option<&Url> = None;
        let mut retry = true;
        let range = format!("bytes={}-{}", pos, pos + size - 1);

        let now = SystemTime::now();
        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            last_url = Some(&url);
            debug!("read_at url: {}, range: {}", url, &range);

            let result = HTTP_CLIENT
                .get(url.as_str())
                .header(RANGE, &range)
                .header(REQUEST_ID_HEADER, get_req_id(now, tries))
                .send()
                .map_err(|err| IOError::new(IOErrorKind::ConnectionAborted, err))
                .and_then(|resp| {
                    let code = resp.status();
                    if code != StatusCode::PARTIAL_CONTENT && code != StatusCode::OK {
                        if code.is_client_error() {
                            retry = false;
                        }
                        return Err(IOError::new(
                            IOErrorKind::InvalidData,
                            "Status code is neither 200 nor 206",
                        ));
                    }
                    let content_length = parse_content_length(&resp);
                    let max_size = content_length.min(size);
                    io_copy(&mut resp.take(max_size), &mut cursor)
                });
            match result {
                Ok(size) => {
                    return Ok(size as usize);
                }
                Err(err) => {
                    warn!(
                        "read_at error url: {}, range: {}, error: {}",
                        url, range, err
                    );
                    cursor.set_position(0);
                    io_error = Some(IOError::new(IOErrorKind::ConnectionAborted, err));

                    if !retry {
                        break;
                    }
                }
            }
        }
        warn!(
            "final failed read at url = {}, error: {:?}",
            last_url.unwrap(),
            io_error
        );
        Err(io_error.unwrap())
    }
}

#[derive(Debug, Clone)]
pub struct RangePart {
    pub data: Vec<u8>,
    pub range: (u64, u64),
}

impl RangeReader {
    /// 读取文件的多个区域，返回每个区域对应的数据
    /// # Arguments
    /// * `range` - 区域列表，每个区域有开始和结束两个偏移量组成（包含开始偏移量，不包含结束偏移量），
    pub fn read_multi_range(&self, range: &[(u64, u64)]) -> IOResult<Vec<RangePart>> {
        let range_header_value = format!("bytes={}", generate_range_header(range));
        let mut io_error: Option<IOError> = None;
        let now = SystemTime::now();
        let mut retry = true;

        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            debug!("read_multi_range url: {}, range: {:?}", url, range);

            let result = HTTP_CLIENT
                .get(url.as_str())
                .header(RANGE, &range_header_value)
                .header(REQUEST_ID_HEADER, get_req_id(now, tries))
                .send()
                .map_err(|err| IOError::new(IOErrorKind::Other, err))
                .and_then(|mut resp| {
                    let mut parts = Vec::with_capacity(range.len());
                    match resp.status() {
                        StatusCode::OK => {
                            let body = resp
                                .bytes()
                                .map_err(|err| IOError::new(IOErrorKind::BrokenPipe, err))?;
                            for &(from, len) in range.iter() {
                                let from = (from as usize).min(body.len());
                                let len = (len as usize).min(body.len() - from);
                                if len > 0 {
                                    parts.push(RangePart {
                                        data: body.slice(from..(from + len)).to_vec(),
                                        range: (from as u64, len as u64),
                                    });
                                }
                            }
                        }
                        StatusCode::PARTIAL_CONTENT => {
                            if range.len() > 1 {
                                let boundary = {
                                    let content_type =
                                        resp.headers().get(CONTENT_TYPE).ok_or_else(|| {
                                            IOError::new(
                                                IOErrorKind::InvalidData,
                                                "Content-Type must be existed",
                                            )
                                        })?;
                                    extract_boundary(&content_type.to_str().map_err(|err| {
                                        IOError::new(IOErrorKind::InvalidData, err)
                                    })?)
                                    .ok_or_else(|| {
                                        IOError::new(
                                            IOErrorKind::InvalidData,
                                            "Boundary must be existed in Content-Type",
                                        )
                                    })?
                                    .to_owned()
                                };

                                let mut multipart = Multipart::with_body(&mut resp, boundary);
                                loop {
                                    match multipart.read_entry() {
                                        Ok(Some(mut field)) => {
                                            let content_range =
                                                field.headers.content_range.ok_or_else(|| {
                                                    IOError::new(
                                                        IOErrorKind::InvalidData,
                                                        "Content-Range must be existed",
                                                    )
                                                })?;
                                            let (from, to, _) = parse_range_header(&content_range)
                                                .map_err(|_| {
                                                    IOError::new(
                                                        IOErrorKind::InvalidData,
                                                        "Invalid Content-Range",
                                                    )
                                                })?;
                                            let len = to - from + 1;
                                            let mut data = Vec::with_capacity(len as usize);
                                            field.data.read_to_end(&mut data).map_err(|err| {
                                                IOError::new(IOErrorKind::BrokenPipe, err)
                                            })?;
                                            parts.push(RangePart {
                                                data,
                                                range: (from, len),
                                            });
                                        }
                                        Ok(None) => break,
                                        Err(err) => {
                                            return Err(IOError::new(IOErrorKind::BrokenPipe, err))
                                        }
                                    }
                                }
                            } else {
                                let content_range =
                                    resp.headers().get(CONTENT_RANGE).ok_or_else(|| {
                                        IOError::new(
                                            IOErrorKind::InvalidData,
                                            "Content-Range must be existed",
                                        )
                                    })?;
                                let (from, to, _) = content_range
                                    .to_str()
                                    .ok()
                                    .and_then(|r| parse_range_header(r).ok())
                                    .ok_or_else(|| {
                                        IOError::new(
                                            IOErrorKind::InvalidData,
                                            "Invalid Content-Range",
                                        )
                                    })?;
                                let len = to - from + 1;
                                let mut data = Vec::with_capacity(len as usize);
                                resp.copy_to(&mut data).unwrap();
                                parts.push(RangePart {
                                    data,
                                    range: (from, len),
                                });
                            }
                        }
                        _ => {
                            if resp.status().is_client_error() {
                                retry = false;
                            }
                            return Err(IOError::new(
                                IOErrorKind::Other,
                                "Status code is neither 200 nor 206",
                            ));
                        }
                    }

                    Ok(parts)
                });
            match result {
                Ok(parts) => {
                    return Ok(parts);
                }
                Err(err) => {
                    warn!(
                        "read_multi_range error url: {}, range: {:?}, error: {}",
                        url, range, err
                    );
                    io_error = Some(err);
                    if !retry {
                        break;
                    }
                }
            }
        }
        return Err(io_error.unwrap());

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

        fn extract_boundary<'s>(content_type: &'s str) -> Option<&'s str> {
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
        let mut io_error: Option<IOError> = None;
        let now = SystemTime::now();
        let mut retry = true;
        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            debug!("exist url: {}", url);

            let result = HTTP_CLIENT
                .head(url.as_str())
                .header(REQUEST_ID_HEADER, get_req_id(now, tries))
                .send()
                .map_err(|err| IOError::new(IOErrorKind::ConnectionAborted, err))
                .and_then(|resp| match resp.status() {
                    StatusCode::OK => Ok(true),
                    StatusCode::NOT_FOUND => Ok(false),
                    _ => {
                        if resp.status().is_client_error() {
                            retry = false;
                        }
                        Err(IOError::new(
                            IOErrorKind::BrokenPipe,
                            "Status code is neither 200 nor 404",
                        ))
                    }
                });
            match result {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    warn!("exist error url: {}, error: {}", url, err);
                    io_error = Some(err);
                    if !retry {
                        break;
                    }
                }
            }
        }
        Err(io_error.unwrap())
    }

    /// 获取当前对象的文件大小
    pub fn file_size(&self) -> IOResult<u64> {
        let mut io_error: Option<IOError> = None;
        let now = SystemTime::now();
        let mut retry = true;
        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            debug!("file_size url: {}", url);

            let result = HTTP_CLIENT
                .head(url.as_str())
                .header(REQUEST_ID_HEADER, get_req_id(now, tries))
                .send()
                .map_err(|err| IOError::new(IOErrorKind::Other, err))
                .and_then(|resp| {
                    if resp.status() == StatusCode::OK {
                        Ok(parse_content_length(&resp))
                    } else {
                        if resp.status().is_client_error() {
                            retry = false;
                        }
                        Err(IOError::new(IOErrorKind::Other, "Status code is not 200"))
                    }
                });
            match result {
                Ok(size) => {
                    return Ok(size);
                }
                Err(err) => {
                    warn!("file_size error url: {}, error: {}", url, err);
                    io_error = Some(err);
                    if !retry {
                        break;
                    }
                }
            }
        }
        Err(io_error.unwrap())
    }

    /// 下载当前对象到内存缓冲区中
    pub fn download(&self) -> IOResult<Vec<u8>> {
        let mut bytes = Cursor::new(Vec::new());
        self.download_to(&mut bytes)?;
        Ok(bytes.into_inner())
    }

    /// 下载当前对象到指定输出流中
    pub fn download_to(&self, writer: &mut dyn WriteSeek) -> IOResult<u64> {
        let mut io_error: Option<IOError> = None;
        let init_start_from = writer.seek(SeekFrom::End(0))?;
        let mut start_from = init_start_from;
        let now = SystemTime::now();
        let mut retry = true;
        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            debug!("download_to url: {}, start_from: {}", url, start_from);

            let mut req = HTTP_CLIENT.get(url.as_str());
            if start_from > 0 {
                req = req.header(RANGE, format!("bytes={}-", start_from));
            }
            let result = req
                .header(REQUEST_ID_HEADER, get_req_id(now, tries))
                .send()
                .map_err(|err| IOError::new(IOErrorKind::Other, err))
                .and_then(|mut resp| {
                    if resp.status() == StatusCode::RANGE_NOT_SATISFIABLE {
                        return Ok(0);
                    } else if resp.status() != StatusCode::OK
                        && resp.status() != StatusCode::PARTIAL_CONTENT
                    {
                        if resp.status().is_client_error() {
                            retry = false;
                        }
                        return Err(IOError::new(
                            IOErrorKind::Other,
                            "Status code is neither 200 nor 206",
                        ));
                    }
                    resp.copy_to(writer)
                        .map_err(|err| IOError::new(IOErrorKind::BrokenPipe, err))
                });
            let origin_start_from = start_from;
            start_from = writer.seek(SeekFrom::Current(0))?;
            match result {
                Ok(_) => {
                    return Ok(start_from - init_start_from);
                }
                Err(err) => {
                    warn!(
                        "download error url: {}, start_from: {}, error: {}",
                        url, origin_start_from, err
                    );
                    io_error = Some(err);
                    if !retry {
                        break;
                    }
                }
            }
        }
        Err(io_error.unwrap())
    }

    /// 下载对象的最后指定个字节到缓冲区中，返回实际下载的字节数和整个文件的大小
    pub fn read_last_bytes(&self, buf: &mut [u8]) -> IOResult<(u64, u64)> {
        let size = buf.len() as u64;
        let mut cursor = Cursor::new(buf);
        let mut io_error: Option<IOError> = None;
        let range = format!("bytes=-{}", size);
        let now = SystemTime::now();
        let mut retry = false;
        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            debug!("read_last_bytes url: {}, len: {}", url, size);

            let result = HTTP_CLIENT
                .get(url.as_str())
                .header(RANGE, &range)
                .header(REQUEST_ID_HEADER, get_req_id(now, tries))
                .send()
                .map_err(|err| IOError::new(IOErrorKind::ConnectionAborted, err))
                .and_then(|resp| {
                    if resp.status() != StatusCode::PARTIAL_CONTENT {
                        if resp.status().is_client_error() {
                            retry = false;
                        }
                        return Err(IOError::new(IOErrorKind::Other, "Status code is not 206"));
                    }
                    let content_range = resp
                        .headers()
                        .get(CONTENT_RANGE)
                        .and_then(|r| r.to_str().ok())
                        .ok_or_else(|| {
                            IOError::new(IOErrorKind::InvalidData, "Content-Range must be existed")
                        })?;
                    let (_, _, total_size) = parse_range_header(content_range).map_err(|_| {
                        IOError::new(IOErrorKind::InvalidData, "Invalid Content-Range")
                    })?;
                    let actual_size = io_copy(&mut resp.take(size), &mut cursor)?;
                    Ok((actual_size, total_size))
                });
            match result {
                Ok((actual_size, total_size)) => {
                    return Ok((actual_size, total_size));
                }
                Err(err) => {
                    warn!("download error url: {}, len: {}, error: {}", url, size, err);
                    cursor.set_position(0);
                    io_error = Some(err);
                    if !retry {
                        break;
                    }
                }
            }
        }
        Err(io_error.unwrap())
    }

    fn choose_urls(&self) -> Vec<&Url> {
        let mut urls: Vec<_> = self
            .urls
            .choose_multiple(&mut thread_rng(), self.tries)
            .collect();
        if urls.len() < self.tries {
            let still_needed = self.tries - urls.len();
            for i in 0..still_needed {
                let index = i % self.urls.len();
                urls.push(urls[index]);
            }
        }
        urls
    }
}

pub trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}

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

fn sleep_before_retry(tries: usize) {
    if tries >= 3 {
        let wait = 500 * (tries << 1) as u64;
        if wait > 0 {
            sleep(Duration::from_millis(wait));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::oneshot::channel;
    use multipart::client::lazy::Multipart;
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
    use tokio::task::{spawn, spawn_blocking};
    use warp::{
        header,
        http::{HeaderValue, StatusCode},
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
    }

    #[tokio::test]
    async fn test_read_at() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        let routes = {
            let action_1 = path!("file")
                .and(header::value("Range"))
                .map(|range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=5-10");
                    Response::new("1234567890".into())
                });
            let action_2 = path!("file2")
                .and(header::value("Range"))
                .map(|range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=5-16");
                    Response::new("1234567890".into())
                });
            action_1.or(action_2)
        };
        starts_with_server!(addr, routes, {
            {
                let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
                let downloader = RangeReader::new(vec![url], 3);
                spawn_blocking(move || {
                    let mut buf = [0u8; 6];
                    assert_eq!(downloader.read_at(5, &mut buf).unwrap(), 6);
                    assert_eq!(&buf, b"123456");
                })
                .await?;
            }
            {
                let url = Url::parse(&format!("http://{}/file2", addr)).unwrap();
                let downloader = RangeReader::new(vec![url], 3);
                spawn_blocking(move || {
                    let mut buf = [0u8; 12];
                    assert_eq!(downloader.read_at(5, &mut buf).unwrap(), 10);
                    assert_eq!(&buf[..10], b"1234567890");
                })
                .await?;
            }
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_read_at_2() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        let counter = Arc::new(AtomicUsize::new(0));
        let routes = {
            let counter = counter.to_owned();
            path!("file")
                .and(header::value("Range"))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=1-5");
                    counter.fetch_add(1, Relaxed);
                    let mut resp = Response::new("12345".into());
                    *resp.status_mut() = StatusCode::NOT_IMPLEMENTED;
                    resp
                })
        };
        starts_with_server!(addr, routes, {
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let downloader = RangeReader::new(vec![url], 3);
            spawn_blocking(move || {
                let mut buf = [0u8; 5];
                downloader.read_at(1, &mut buf).unwrap_err();
                assert_eq!(counter.load(Relaxed), 3);
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_read_at_3() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        let counter = Arc::new(AtomicUsize::new(0));
        let routes = {
            let counter = counter.to_owned();
            path!("file")
                .and(header::value("Range"))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=1-5");
                    counter.fetch_add(1, Relaxed);
                    let mut resp = Response::new("12345".into());
                    *resp.status_mut() = StatusCode::BAD_REQUEST;
                    resp
                })
        };
        starts_with_server!(addr, routes, {
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let downloader = RangeReader::new(vec![url], 3);
            spawn_blocking(move || {
                let mut buf = [0u8; 5];
                downloader.read_at(1, &mut buf).unwrap_err();
                assert_eq!(counter.load(Relaxed), 1);
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_read_last_bytes() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        let routes = path!("file")
            .and(header::value("Range"))
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
        starts_with_server!(addr, routes, {
            {
                let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
                let downloader = RangeReader::new(vec![url], 3);
                spawn_blocking(move || {
                    let mut buf = [0u8; 10];
                    assert_eq!(
                        downloader.read_last_bytes(&mut buf).unwrap(),
                        (10, 157286400)
                    );
                    assert_eq!(&buf, b"1234567890");
                })
                .await?;
            }
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_file() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        let routes = { path!("file").map(|| Response::new("1234567890".into())) };
        starts_with_server!(addr, routes, {
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let downloader = RangeReader::new(vec![url], 3);
            spawn_blocking(move || {
                assert!(downloader.exist().unwrap());
                assert_eq!(downloader.file_size().unwrap(), 10);
                assert_eq!(&downloader.download().unwrap(), b"1234567890");
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_file_2() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

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
        starts_with_server!(addr, routes, {
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let downloader = RangeReader::new(vec![url], 3);
            spawn_blocking(move || {
                downloader.exist().unwrap_err();
                downloader.file_size().unwrap_err();
                downloader.download().unwrap_err();
                assert_eq!(counter.load(Relaxed), 3 * 3);
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_file_3() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

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
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let downloader = RangeReader::new(vec![url], 3);
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

        let routes = { path!("file").map(|| Response::new("1234567890".into())) };
        starts_with_server!(addr, routes, {
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let downloader = RangeReader::new(vec![url], 3);
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

        let routes = {
            path!("file")
                .and(header::value("Range"))
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
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let range_reader = RangeReader::new(vec![url], 3);
            let ranges = [(0, 5), (5, 5)];
            spawn_blocking(move || {
                let parts = range_reader.read_multi_range(&ranges).unwrap();
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

        let routes = {
            path!("file")
                .and(header::value("Range"))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-9");
                    "12345678901357924680"
                })
        };
        starts_with_server!(addr, routes, {
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let range_reader = RangeReader::new(vec![url], 3);
            let ranges = [(0, 5), (5, 5)];
            spawn_blocking(move || {
                let parts = range_reader.read_multi_range(&ranges).unwrap();
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

        let counter = Arc::new(AtomicUsize::new(0));
        let routes = {
            let counter = counter.to_owned();
            path!("file")
                .and(header::value("Range"))
                .map(move |range: HeaderValue| {
                    counter.fetch_add(1, Relaxed);
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-9");
                    let mut resp = Response::new("12345".into());
                    *resp.status_mut() = StatusCode::NOT_IMPLEMENTED;
                    resp
                })
        };
        starts_with_server!(addr, routes, {
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let range_reader = RangeReader::new(vec![url], 3);
            let ranges = [(0, 5), (5, 5)];
            spawn_blocking(move || {
                range_reader.read_multi_range(&ranges).unwrap_err();
                assert_eq!(counter.load(Relaxed), 3);
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_range_4() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        let routes = {
            path!("file")
                .and(header::value("Range"))
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
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let range_reader = RangeReader::new(vec![url], 3);
            let ranges = [(0, 5), (5, 5)];
            spawn_blocking(move || {
                let parts = range_reader.read_multi_range(&ranges).unwrap();
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

        let routes = {
            path!("file")
                .and(header::value("Range"))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-5");
                    "1234"
                })
        };
        starts_with_server!(addr, routes, {
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let range_reader = RangeReader::new(vec![url], 3);
            let ranges = [(0, 5), (5, 1)];
            spawn_blocking(move || {
                let parts = range_reader.read_multi_range(&ranges).unwrap();
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
                .and(header::value("Range"))
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
            let url = Url::parse(&format!("http://{}/file", addr)).unwrap();
            let range_reader = RangeReader::new(vec![url], 3);
            let ranges = [(0, 4)];
            spawn_blocking(move || {
                let parts = range_reader.read_multi_range(&ranges).unwrap();
                assert_eq!(parts.len(), 1);
                assert_eq!(&parts.get(0).unwrap().data, b"123");
                assert_eq!(parts.get(0).unwrap().range, (0, 3));
            })
            .await?;
        });
        Ok(())
    }

    #[test]
    fn test_sign_download_url_with_deadline() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

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
}
