#![allow(clippy::too_many_arguments)]

use super::{
    super::{
        base::{credential::Credential, download::RangeReaderBuilder as BaseRangeReaderBuilder},
        config::{build_range_reader_builder_from_config, Config, Timeouts},
    },
    dot::{ApiName, DotType, Dotter},
    host_selector::{HostInfo, HostSelector, HostSelectorBuilder},
    query::HostsQuerier,
    req_id::{get_req_id2, REQUEST_ID_HEADER},
};
use async_once_cell::Lazy as AsyncLazy;
use futures::{AsyncReadExt, TryStreamExt};
use hyper::HeaderMap;
use log::{debug, info, warn};
use mime::{Mime, BOUNDARY};
use multer::Multipart;
use reqwest::{
    header::{HeaderValue, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    Client as HttpClient, Error as ReqwestError, Method, RequestBuilder as HttpRequestBuilder,
    Response as HttpResponse, StatusCode, Url,
};
use std::{
    collections::HashSet,
    error::Error as StdError,
    fmt::{self, Debug},
    future::Future,
    io::{Cursor, Error as IoError, ErrorKind as IoErrorKind, Result as IoResult},
    mem::take,
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, Instant, SystemTime, SystemTimeError, UNIX_EPOCH},
};
use tap::prelude::*;
use text_io::{try_scan as try_scan_text, Error as TextIoError};
use tokio::{
    io::{copy as io_copy, AsyncWrite},
    spawn,
    sync::Mutex,
};
use tokio_util::{compat::FuturesAsyncReadCompatExt, either::Either};

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

#[derive(Debug)]
pub(super) struct AsyncRangeReaderBuilder(BaseRangeReaderBuilder);

impl From<BaseRangeReaderBuilder> for AsyncRangeReaderBuilder {
    fn from(builder: BaseRangeReaderBuilder) -> Self {
        Self(builder)
    }
}

impl From<AsyncRangeReaderBuilder> for BaseRangeReaderBuilder {
    fn from(builder: AsyncRangeReaderBuilder) -> Self {
        builder.0
    }
}

impl AsyncRangeReaderBuilder {
    pub(super) fn take_key(&mut self) -> String {
        take(&mut self.0.key)
    }

    pub(super) fn build(self) -> AsyncRangeReader {
        AsyncRangeReader(Arc::new(AsyncLazy::new(Box::pin(async move {
            self.build_inner().await
        }))))
    }

    async fn build_inner(self) -> Arc<AsyncRangeReaderInner> {
        let builder = self.0;
        let http_client =
            Timeouts::new(builder.base_timeout, builder.dial_timeout).async_http_client();
        let dotter = Dotter::new(
            http_client.to_owned(),
            builder.credential.to_owned(),
            builder.bucket.to_owned(),
            builder.monitor_urls,
            builder.dot_interval,
            builder.max_dot_buffer_size,
            builder.dot_tries,
            builder.punish_duration,
            builder.max_punished_times,
            builder.max_punished_hosts_percent,
            builder.base_timeout,
        )
        .await;

        let params = HostSelectorParams {
            update_interval: builder.update_interval,
            punish_duration: builder.punish_duration,
            max_punished_times: builder.max_punished_times,
            max_punished_hosts_percent: builder.max_punished_hosts_percent,
            base_timeout: builder.base_timeout,
        };

        let io_querier = if builder.uc_urls.is_empty() {
            None
        } else {
            Some(HostsQuerier::new(
                make_uc_host_selector(builder.uc_urls, &params).await,
                builder.uc_tries,
                dotter.to_owned(),
                http_client.to_owned(),
            ))
        };
        let io_selector = make_io_selector(
            builder.io_urls,
            io_querier,
            builder.credential.access_key().to_owned(),
            builder.bucket.to_owned(),
            builder.use_https,
            &params,
        )
        .await;

        return Arc::new(AsyncRangeReaderInner {
            io_selector,
            dotter,
            http_client,
            credential: builder.credential,
            bucket: builder.bucket,
            use_getfile_api: builder.use_getfile_api,
            normalize_key: builder.normalize_key,
            use_https: builder.use_https,
            private_url_lifetime: builder.private_url_lifetime,
        });

        #[derive(Clone, Debug)]
        struct HostSelectorParams {
            update_interval: Option<Duration>,
            punish_duration: Option<Duration>,
            max_punished_times: Option<usize>,
            max_punished_hosts_percent: Option<u8>,
            base_timeout: Option<Duration>,
        }

        impl HostSelectorParams {
            fn set_builder(&self, mut builder: HostSelectorBuilder) -> HostSelectorBuilder {
                if let Some(update_interval) = self.update_interval {
                    builder = builder.update_interval(update_interval);
                }
                if let Some(punish_duration) = self.punish_duration {
                    builder = builder.punish_duration(punish_duration);
                }
                if let Some(max_punished_times) = self.max_punished_times {
                    builder = builder.max_punished_times(max_punished_times);
                }
                if let Some(max_punished_hosts_percent) = self.max_punished_hosts_percent {
                    builder = builder.max_punished_hosts_percent(max_punished_hosts_percent);
                }
                if let Some(base_timeout) = self.base_timeout {
                    builder = builder.base_timeout(base_timeout);
                }
                builder
            }
        }

        async fn make_uc_host_selector(
            uc_urls: Vec<String>,
            params: &HostSelectorParams,
        ) -> HostSelector {
            params
                .set_builder(HostSelector::builder(uc_urls))
                .build()
                .await
        }

        async fn make_io_selector(
            io_urls: Vec<String>,
            io_querier: Option<HostsQuerier>,
            access_key: String,
            bucket: String,
            use_https: bool,
            params: &HostSelectorParams,
        ) -> HostSelector {
            let builder = HostSelector::builder(io_urls)
                .update_callback(Some(Box::new(move || {
                    let io_querier = io_querier.to_owned();
                    let access_key = access_key.to_owned();
                    let bucket = bucket.to_owned();
                    Box::pin(async move {
                        if let Some(io_querier) = io_querier.as_ref() {
                            io_querier
                                .query_for_io_urls(&access_key, &bucket, use_https)
                                .await
                        } else {
                            Ok(vec![])
                        }
                    })
                })))
                .should_punish_callback(Some(Box::new(|error| {
                    let kind = error.kind();
                    Box::pin(async move { !matches!(kind, IoErrorKind::InvalidData) })
                })));
            params.set_builder(builder).build().await
        }
    }

    pub(crate) fn from_config(key: String, config: &Config) -> Self {
        build_range_reader_builder_from_config(key, config).into()
    }
}

#[derive(Clone)]
pub(super) struct AsyncRangeReader(Arc<AsyncLazy<Arc<AsyncRangeReaderInner>>>);

#[derive(Debug)]
struct AsyncRangeReaderInner {
    io_selector: HostSelector,
    dotter: Dotter,
    credential: Credential,
    http_client: Arc<HttpClient>,
    bucket: String,
    use_getfile_api: bool,
    normalize_key: bool,
    use_https: bool,
    private_url_lifetime: Option<Duration>,
}

impl AsyncRangeReader {
    // pub(super) fn from_env_with_extra_items(
    //     key: String,
    // ) -> Option<(Self, String, Option<usize>, Option<usize>)> {
    //     with_current_qiniu_config(|config| {
    //         config.and_then(|config| {
    //             config.with_key(&key.to_owned(), |config| {
    //                 let config = Arc::new(config.to_owned());
    //                 let range_reader_config = config.to_owned();
    //                 (
    //                     config.max_retry_concurrency(),
    //                     config.retry(),
    //                     Box::pin(async move {
    //                         config
    //                             .get_or_init_async_range_reader_inner(move || async move {
    //                                 AsyncRangeReaderBuilder::from_config(
    //                                     String::new(),
    //                                     &range_reader_config,
    //                                 )
    //                                 .build_inner()
    //                                 .await
    //                             })
    //                             .await
    //                     }),
    //                 )
    //             })
    //         })
    //     })
    //     .map(|(max_retry_concurrency, total_tries, fut)| {
    //         (
    //             Self(Arc::new(AsyncLazy::new(fut))),
    //             key,
    //             max_retry_concurrency,
    //             total_tries,
    //         )
    //     })
    // }

    pub(super) async fn update_urls(&self) -> bool {
        self.inner().await.io_selector.update_hosts().await
    }

    pub(super) async fn io_urls(&self) -> Vec<String> {
        let inner = self.inner().await;
        return inner
            .io_selector
            .hosts()
            .await
            .iter()
            .map(|host| normalize_host(host, inner.use_https))
            .collect();

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

    pub(super) async fn base_timeout(&self) -> Duration {
        self.inner().await.io_selector.base_timeout()
    }

    pub(super) async fn increase_timeout_power_by(&self, host: &str, timeout_power: usize) {
        self.inner()
            .await
            .io_selector
            .increase_timeout_power_by(host, timeout_power)
    }

    pub(super) async fn read_at<F: FnMut(HostInfo) -> Fut, Fut: Future<Output = ()>>(
        &self,
        pos: u64,
        size: u64,
        key: &str,
        async_task_id: usize,
        tries_info: TriesInfo<'_>,
        trying_hosts: &TryingHosts,
        on_host_selected: F,
    ) -> IoResult3<Vec<u8>> {
        if size == 0 {
            return Ok(Default::default()).into();
        }
        return self.with_retries(
            key,
            Method::GET,
            ApiName::RangeReaderReadAt,
            async_task_id,
            tries_info,
            trying_hosts,
            on_host_selected,
            |tries, request_builder, req_id, download_url, host_info| {
                async move {
                    let range = generate_range_header(pos, size);
                    debug!(
                        "{{{}}} [{}] read_at url: {}, req_id: {:?}, range: {}",
                        async_task_id, tries, download_url, req_id, &range
                    );
                    let begin_at = Instant::now();
                    let result = request_builder
                        .header(RANGE, &range)
                        .send()
                        .await;
                        if let Err(err) = &result {
                            self.punish_if_needed(host_info.host(), host_info.timeout_power(), err).await;
                        }
                    let result = result
                        .map_err(io_error_from(IoErrorKind::ConnectionAborted))
                        .and_then(|resp| {
                            if resp.status() != StatusCode::PARTIAL_CONTENT && resp.status() != StatusCode::OK {
                                return Err(unexpected_status_code(&resp));
                            }
                            Ok(resp)
                        })
                        .map(|resp| {
                            let max_size = parse_content_length(&resp).min(size);
                            (resp, max_size)
                        });
                    match result {
                        Ok((resp, max_size)) => {
                            read_response_body(resp, Some(max_size)).await
                        }
                        Err(err) => Err(err),
                    }
                        .tap_ok(|_| {
                            info!(
                                "{{{}}} [{}] read_at ok url: {}, range: {}, req_id: {:?}, elapsed: {:?}",
                                async_task_id,
                                tries,
                                download_url,
                                range,
                                req_id,
                                begin_at.elapsed(),
                            );
                        })
                        .tap_err(|err| {
                            warn!(
                                "{{{}}} [{}] read_at error url: {}, range: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                                async_task_id, tries, download_url, range, err, req_id, begin_at.elapsed(),
                            );
                        })
                }
            },
        )
        .await;

        fn generate_range_header(pos: u64, size: u64) -> String {
            format!("bytes={}-{}", pos, pos + size - 1)
        }
    }

    pub(super) async fn read_multi_ranges<F: FnMut(HostInfo) -> Fut, Fut: Future<Output = ()>>(
        &self,
        ranges: &[(u64, u64)],
        key: &str,
        async_task_id: usize,
        tries_info: TriesInfo<'_>,
        trying_hosts: &TryingHosts,
        on_host_selected: F,
    ) -> IoResult3<Vec<RangePart>> {
        return self
            .with_retries(
                key,
                Method::GET,
                ApiName::RangeReaderReadAt,
                async_task_id,
                tries_info,
                trying_hosts,
                on_host_selected,
                |tries, request_builder, req_id, download_url, host_info| async move {
                    debug!(
                        "{{{}}} [{}] read_multi_ranges url: {}, req_id: {:?}, range counts: {}",
                        async_task_id,
                        tries,
                        download_url,
                        req_id,
                        ranges.len(),
                    );
                    let range = generate_range_header(ranges);
                    let begin_at = Instant::now();
                    let result = request_builder
                        .header(RANGE, &range)
                        .send()
                        .await;
                    if let Err(err) = &result {
                        self.punish_if_needed(host_info.host(), host_info.timeout_power(), err).await;
                    }
                    let result = result.map_err(io_error_from(IoErrorKind::ConnectionAborted));
                    match result {
                        Ok(resp) => {
                            let mut parts = Vec::with_capacity(ranges.len());
                            match resp.status() {
                                StatusCode::OK => {
                                    let body = read_response_body(resp, None).await?;
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
                                StatusCode::PARTIAL_CONTENT if ranges.len() > 1 => {
                                    let content_type = resp
                                        .headers()
                                        .get(CONTENT_TYPE)
                                        .ok_or_else(new_io_error(
                                            IoErrorKind::InvalidInput,
                                            "Content-Type must be existed",
                                        ))?;
                                    let content_type: Mime = content_type
                                        .to_str()
                                        .map_err(io_error_from(IoErrorKind::InvalidInput))?
                                        .parse()
                                        .map_err(io_error_from(IoErrorKind::InvalidInput))?;
                                    let boundary = content_type.get_param(BOUNDARY).unwrap();
                                    let mut multipart =
                                        Multipart::new(resp.bytes_stream(), boundary.as_str());
                                    while let Some(field) = multipart
                                        .next_field()
                                        .await
                                        .map_err(io_error_from(IoErrorKind::BrokenPipe))?
                                    {
                                        let (from, to, _) = extract_range_header(field.headers())?;
                                        let len = to - from + 1;
                                        parts.push(RangePart {
                                            data: field
                                                .bytes()
                                                .await
                                                .map(|b| b.to_vec())
                                                .map_err(io_error_from(IoErrorKind::BrokenPipe))?,
                                            range: (from, len),
                                        });
                                    }
                                }
                                StatusCode::PARTIAL_CONTENT => {
                                    let (from, to, _) = extract_range_header(resp.headers())?;
                                    let len = to - from + 1;
                                    parts.push(RangePart {
                                        data: read_response_body(resp, None).await?,
                                        range: (from, len),
                                    });
                                }
                                _ => {
                                    return Err(unexpected_status_code(&resp));
                                }
                            }
                            Ok(parts)
                        }
                        Err(err) => Err(err),
                    }
                    .tap_ok(|_| {
                        info!(
                            "{{{}}} [{}] read_multi_ranges ok url: {}, req_id: {:?}, elapsed: {:?}",
                            async_task_id, tries, download_url, req_id, begin_at.elapsed(),
                        );
                    })
                    .tap_err(|err| {
                        warn!(
                            "{{{}}} [{}] read_multi_ranges error url: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                            async_task_id, tries, download_url, err, req_id, begin_at.elapsed(),
                        );
                    })
                },
            )
            .await;

        fn generate_range_header(ranges: &[(u64, u64)]) -> String {
            let range = ranges
                .iter()
                .map(|range| {
                    let start = range.0;
                    let end = start + range.1 - 1;
                    format!("{}-{}", start, end)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("bytes={}", range)
        }
    }

    pub(super) async fn exist<F: FnMut(HostInfo) -> Fut, Fut: Future<Output = ()>>(
        &self,
        key: &str,
        async_task_id: usize,
        tries_info: TriesInfo<'_>,
        trying_hosts: &TryingHosts,
        on_host_selected: F,
    ) -> IoResult3<bool> {
        self.with_retries(
            key,
            Method::HEAD,
            ApiName::RangeReaderExist,
            async_task_id,
            tries_info,
            trying_hosts,
            on_host_selected,
            |tries, request_builder, req_id, download_url, host_info| async move {
                debug!(
                    "{{{}}} [{}] exist url: {}, req_id: {:?}",
                    async_task_id, tries, download_url, req_id
                );
                let begin_at = Instant::now();
                let result = request_builder.send().await;
                if let Err(err) = &result {
                    self.punish_if_needed(host_info.host(), host_info.timeout_power(), err)
                        .await;
                }
                result
                    .map_err(io_error_from(IoErrorKind::ConnectionAborted))
                    .and_then(|resp| match resp.status() {
                        StatusCode::OK => Ok(true),
                        StatusCode::NOT_FOUND => Ok(false),
                        _ => Err(unexpected_status_code(&resp)),
                    })
                    .tap_ok(|_| {
                        info!(
                            "{{{}}} [{}] exist ok url: {}, req_id: {:?}, elapsed: {:?}",
                            async_task_id,
                            tries,
                            download_url,
                            req_id,
                            begin_at.elapsed(),
                        );
                    })
                    .tap_err(|err| {
                        warn!(
                            "{{{}}} [{}] exist error url: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                            async_task_id,
                            tries,
                            download_url,
                            err,
                            req_id,
                            begin_at.elapsed(),
                        );
                    })
            },
        )
        .await
    }

    pub(super) async fn file_size<F: FnMut(HostInfo) -> Fut, Fut: Future<Output = ()>>(
        &self,
        key: &str,
        async_task_id: usize,
        tries_info: TriesInfo<'_>,
        trying_hosts: &TryingHosts,
        on_host_selected: F,
    ) -> IoResult3<u64> {
        self.with_retries(
            key,
            Method::HEAD,
            ApiName::RangeReaderFileSize,
            async_task_id,
            tries_info,
            trying_hosts,
            on_host_selected,
            |tries, request_builder, req_id, download_url, host_info| async move {
                debug!(
                    "{{{}}} [{}] file_size url: {}, req_id: {:?}",
                    async_task_id, tries, download_url, req_id
                );
                let begin_at = Instant::now();
                let result = request_builder.send().await;
                if let Err(err) = &result {
                    self.punish_if_needed(host_info.host(), host_info.timeout_power(), err)
                        .await;
                }
                result
                    .map_err(io_error_from(IoErrorKind::ConnectionAborted))
                    .and_then(|resp| {
                        if resp.status() == StatusCode::OK {
                            Ok(parse_content_length(&resp))
                        } else {
                            Err(unexpected_status_code(&resp))
                        }
                    })
                    .tap_ok(|_| {
                        info!(
                            "{{{}}} [{}] file_size ok url: {}, req_id: {:?}, elapsed: {:?}",
                            async_task_id,
                            tries,
                            download_url,
                            req_id,
                            begin_at.elapsed(),
                        );
                    })
                    .tap_err(|err| {
                        warn!(
                            "{{{}}} [{}] file_size error url: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                            async_task_id,
                            tries,
                            download_url,
                            err,
                            req_id,
                            begin_at.elapsed(),
                        );
                    })
            },
        )
        .await
    }

    pub(super) async fn download<F: FnMut(HostInfo) -> Fut, Fut: Future<Output = ()>>(
        &self,
        key: &str,
        async_task_id: usize,
        tries_info: TriesInfo<'_>,
        trying_hosts: &TryingHosts,
        mut on_host_selected: F,
    ) -> IoResult3<Vec<u8>> {
        let mut result = Vec::new();
        loop {
            let (chunk, mut completed) = match self
                ._download(
                    key,
                    async_task_id,
                    result.len() as u64,
                    tries_info,
                    trying_hosts,
                    &mut on_host_selected,
                )
                .await
            {
                Result3::Ok(result) => result,
                Result3::Err(err) => return Result3::Err(err),
                Result3::NoMoreTries(err) => return Result3::NoMoreTries(err),
            };
            if result.is_empty() {
                result = chunk;
            } else if chunk.is_empty() {
                completed = true;
            } else {
                result.extend(chunk);
            }
            if completed {
                return Result3::Ok(result);
            } else {
                info!("Early EOF Response Body is detected in {}::download(), will start a new GET request for the rest body", module_path!());
            }
        }
    }

    async fn _download<F: FnMut(HostInfo) -> Fut, Fut: Future<Output = ()>>(
        &self,
        key: &str,
        async_task_id: usize,
        init_from: u64,
        tries_info: TriesInfo<'_>,
        trying_hosts: &TryingHosts,
        on_host_selected: F,
    ) -> IoResult3<(Vec<u8>, bool)> {
        let mut buf = Vec::new();
        let buf_cursor = Arc::new(Mutex::new(Cursor::new(&mut buf)));
        let result = self
            .with_retries(
                key,
                Method::GET,
                ApiName::RangeReaderDownloadTo,
                async_task_id,
                tries_info,
                trying_hosts,
                on_host_selected,
                move |tries, mut request_builder, req_id, download_url, host_info| {
                    let buf_cursor = buf_cursor.to_owned();
                    async move {
                        let mut buf_cursor = buf_cursor.lock().await;
                        let start_from = init_from + buf_cursor.position();
                        debug!(
                            "{{{}}} [{}] download_to url: {}, req_id: {:?}, start_from: {}",
                            async_task_id, tries, download_url, req_id, start_from
                        );
                        let begin_at = Instant::now();
                        if start_from > 0 {
                            request_builder =
                                request_builder.header(RANGE, format!("bytes={}-", start_from));
                        }
                        let result = request_builder
                            .send()
                            .await;
                        if let Err(err) = &result {
                            self.punish_if_needed(
                                host_info.host(),
                                host_info.timeout_power(),
                                err,
                            ).await;
                        }
                        let result = result.map_err(io_error_from(IoErrorKind::ConnectionAborted));
                        match result {
                            Ok(resp) => {
                                let content_length = parse_content_length(&resp);
                                write_to_writer(resp,  &mut *buf_cursor).await.map(|actually_downloaded| {
                                    if let Some(actually_downloaded) = actually_downloaded {
                                        (actually_downloaded, actually_downloaded < content_length)
                                    } else {
                                        (0, false)
                                    }
                                })
                            },
                            Err(err) => Err(err),
                        }
                        .tap_ok(|(downloaded, incompleted)| {
                            info!(
                                "{{{}}} [{}] download ok url: {}, start_from: {}, downloaded: {}, completed: {:?}, req_id: {:?}, elapsed: {:?}",
                                async_task_id, tries, download_url, start_from, downloaded, !incompleted, req_id, begin_at.elapsed(),
                            );
                        })
                        .tap_err(|err| {
                            warn!(
                                "{{{}}} [{}] download error url: {}, start_from: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                                async_task_id, tries, download_url, start_from, err, req_id, begin_at.elapsed(),
                            );
                        })
                    }
                },
            )
            .await;
        return match result {
            Result3::Ok((_, incompleted)) => Ok((buf, !incompleted)).into(),
            Result3::Err(err) => Result3::Err(err),
            Result3::NoMoreTries(err) => Result3::NoMoreTries(err),
        };

        async fn write_to_writer<W: AsyncWrite + Unpin>(
            resp: HttpResponse,
            mut writer: W,
        ) -> IoResult<Option<u64>> {
            if resp.status() == StatusCode::RANGE_NOT_SATISFIABLE {
                Ok(None)
            } else if resp.status() != StatusCode::OK
                && resp.status() != StatusCode::PARTIAL_CONTENT
            {
                Err(unexpected_status_code(&resp))
            } else {
                let body = resp
                    .bytes_stream()
                    .map_err(io_error_from(IoErrorKind::BrokenPipe));
                io_copy(&mut body.into_async_read().compat(), &mut writer)
                    .await
                    .map(Some)
            }
        }
    }

    pub(super) async fn read_last_bytes<F: FnMut(HostInfo) -> Fut, Fut: Future<Output = ()>>(
        &self,
        size: u64,
        key: &str,
        async_task_id: usize,
        tries_info: TriesInfo<'_>,
        trying_hosts: &TryingHosts,
        on_host_selected: F,
    ) -> IoResult3<(Vec<u8>, u64)> {
        return self.with_retries(
            key,
            Method::GET,
            ApiName::RangeReaderReadLastBytes,
            async_task_id,
            tries_info,
            trying_hosts,
            on_host_selected,
            move |tries, request_builder, req_id, download_url, host_info| async move {
                debug!(
                    "{{{}}} [{}] read_last_bytes url: {}, req_id: {:?}, len: {}",
                    async_task_id, tries, download_url, req_id, size,
                );
                let begin_at = Instant::now();
                let result = request_builder
                    .header(RANGE, format!("bytes=-{}", size))
                    .send()
                    .await;
                    if let Err(err) = &result {
                        self.punish_if_needed(host_info.host(), host_info.timeout_power(), err).await;
                    }
                    let result = result.map_err(io_error_from(IoErrorKind::ConnectionAborted))
                    .and_then(|resp| {
                        if resp.status() == StatusCode::PARTIAL_CONTENT {
                            Ok(resp)
                        } else {
                            Err(unexpected_status_code(&resp))
                        }
                    });
                match result {
                    Ok(resp) => get_response_body_and_total_size(resp, size).await,
                    Err(err) => Err(err),
                }
                .tap_ok(|_| {
                    info!(
                        "{{{}}} [{}] download ok url: {}, len: {}, req_id: {:?}, elapsed: {:?}",
                        async_task_id, tries, download_url, size, req_id, begin_at.elapsed(),
                    );
                })
                .tap_err(|err| {
                    warn!(
                        "{{{}}} [{}] download error url: {}, len: {}, error: {}, req_id: {:?}, elapsed: {:?}",
                        async_task_id, tries, download_url, size, err, req_id, begin_at.elapsed(),
                    );
                })
            }
        )
        .await;

        async fn get_response_body_and_total_size(
            resp: HttpResponse,
            limit: u64,
        ) -> IoResult<(Vec<u8>, u64)> {
            let (_, _, total_size) = extract_range_header(resp.headers())?;
            let last_bytes = read_response_body(resp, Some(limit)).await?;
            Ok((last_bytes, total_size))
        }
    }

    async fn inner(&self) -> &Arc<AsyncRangeReaderInner> {
        self.0.get().await
    }

    async fn with_retries<
        T,
        F: FnMut(usize, HttpRequestBuilder, HeaderValue, Url, HostInfo) -> Fut,
        Fut: Future<Output = IoResult<T>>,
        F2: FnMut(HostInfo) -> Fut2,
        Fut2: Future<Output = ()>,
    >(
        &self,
        key: &str,
        method: Method,
        api_name: ApiName,
        async_task_id: usize,
        tries_info: TriesInfo<'_>,
        trying_hosts: &TryingHosts,
        mut on_host_selected: F2,
        mut for_each_url: F,
    ) -> IoResult3<T> {
        let begin_at = SystemTime::now();
        let begin_at_instant = Instant::now();
        let mut last_error: Option<IoError> = None;
        let inner = self.inner().await;

        loop {
            let tries = tries_info.have_tried.fetch_add(1, Relaxed);
            if tries >= tries_info.total_tries {
                return IoResult3::NoMoreTries(last_error);
            }
            let last_try = tries_info.total_tries - tries <= 1;

            let chosen_io_info = {
                let mut guard = trying_hosts.lock().await;
                if let Some(chosen) = inner.io_selector.select_host(&guard).await {
                    guard.insert(chosen.host().to_owned());
                    drop(guard);
                    TryingHostInfo {
                        host_info: chosen,
                        trying_hosts: trying_hosts.to_owned(),
                    }
                } else {
                    return IoResult3::NoMoreTries(last_error);
                }
            };
            on_host_selected(chosen_io_info.to_owned()).await;
            let download_url = sign_download_url_if_needed(
                &make_download_url(
                    chosen_io_info.host(),
                    inner.credential.access_key(),
                    &inner.bucket,
                    key,
                    inner.use_getfile_api,
                    inner.normalize_key,
                ),
                inner.private_url_lifetime,
                &inner.credential,
            );
            let req_id = get_req_id2(
                begin_at,
                tries,
                async_task_id,
                chosen_io_info.host_info.timeout(),
            );
            let request_begin_at_instant = Instant::now();
            let request_builder = inner
                .http_client
                .request(method.to_owned(), download_url.to_owned())
                .header(REQUEST_ID_HEADER, req_id.to_owned());
            match for_each_url(
                tries,
                request_builder,
                req_id,
                download_url,
                chosen_io_info.to_owned(),
            )
            .await
            {
                Ok(result) => {
                    inner.io_selector.reward(chosen_io_info.host()).await;
                    inner
                        .dotter
                        .dot(DotType::Sdk, api_name, true, begin_at_instant.elapsed())
                        .await
                        .ok();
                    inner
                        .dotter
                        .dot(
                            DotType::Http,
                            ApiName::IoGetfile,
                            true,
                            request_begin_at_instant.elapsed(),
                        )
                        .await
                        .ok();
                    return Ok(result).into();
                }
                Err(err) => {
                    let punished = inner
                        .io_selector
                        .punish(chosen_io_info.host(), &err, &inner.dotter)
                        .await;
                    inner
                        .dotter
                        .dot(
                            DotType::Http,
                            ApiName::IoGetfile,
                            false,
                            request_begin_at_instant.elapsed(),
                        )
                        .await
                        .ok();
                    if !punished || last_try {
                        inner
                            .dotter
                            .dot(DotType::Sdk, api_name, false, begin_at_instant.elapsed())
                            .await
                            .ok();
                        return Err(err).into();
                    } else {
                        last_error = Some(err);
                    }
                }
            }
        }

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
    }

    async fn punish_if_needed(&self, host: &str, timeout_power: usize, err: &ReqwestError) {
        if err.is_timeout() {
            self.inner()
                .await
                .io_selector
                .increase_timeout_power_by(host, timeout_power)
        } else if err.is_connect() {
            self.inner()
                .await
                .io_selector
                .mark_connection_as_failed(host)
        }
    }
}

impl Debug for AsyncRangeReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncRangeReader")
            .field(&self.0.try_get())
            .finish()
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Result3<T, E> {
    Ok(T),
    Err(E),
    NoMoreTries(Option<E>),
}

pub(super) type IoResult3<T> = Result3<T, IoError>;

impl<T, E> From<Result<T, E>> for Result3<T, E> {
    fn from(r: Result<T, E>) -> Self {
        match r {
            Ok(r) => Result3::Ok(r),
            Err(e) => Result3::Err(e),
        }
    }
}

pub(super) type TryingHosts = Arc<Mutex<HashSet<String>>>;

struct TryingHostInfo {
    host_info: HostInfo,
    trying_hosts: TryingHosts,
}

impl Deref for TryingHostInfo {
    type Target = HostInfo;

    fn deref(&self) -> &Self::Target {
        &self.host_info
    }
}

impl Drop for TryingHostInfo {
    fn drop(&mut self) {
        if let Ok(mut trying_hosts) = self.trying_hosts.try_lock() {
            trying_hosts.remove(self.host_info.host());
            return;
        }
        let trying_hosts = take(&mut self.trying_hosts);
        let host_info = take(&mut self.host_info);
        spawn(async move {
            trying_hosts.lock().await.remove(host_info.host());
        });
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct TriesInfo<'a> {
    have_tried: &'a AtomicUsize,
    total_tries: usize,
}

impl<'a> TriesInfo<'a> {
    pub(super) fn new(have_tried: &'a AtomicUsize, total_tries: usize) -> Self {
        Self {
            have_tried,
            total_tries,
        }
    }
}

fn unexpected_status_code(resp: &HttpResponse) -> IoError {
    let error_kind = if resp.status().is_client_error() {
        IoErrorKind::InvalidData
    } else {
        IoErrorKind::Other
    };
    IoError::new(
        error_kind,
        format!("Unexpected status code {}", resp.status().as_u16()),
    )
}

fn parse_content_length(resp: &HttpResponse) -> u64 {
    resp.content_length()
        .and_then(|s| if s > 0 { Some(s) } else { None })
        .or_else(|| {
            resp.headers()
                .get(CONTENT_LENGTH)
                .and_then(|length| length.to_str().ok())
                .and_then(|length| length.parse().ok())
        })
        .expect("Content-Length must be existed")
}

fn extract_range_header(headers: &HeaderMap) -> IoResult<(u64, u64, u64)> {
    let content_range = headers
        .get(CONTENT_RANGE)
        .ok_or_else(new_io_error(
            IoErrorKind::InvalidInput,
            "Content-Range must be existed",
        ))?
        .to_str()
        .map_err(io_error_from(IoErrorKind::InvalidInput))?;
    let (from, to, total_size) =
        parse_range_header(content_range).map_err(io_error_from(IoErrorKind::InvalidInput))?;
    Ok((from, to, total_size))
}

fn parse_range_header(range: &str) -> Result<(u64, u64, u64), TextIoError> {
    let from: u64;
    let to: u64;
    let total_size: u64;
    try_scan_text!(range.bytes() => "bytes {}-{}/{}", from, to, total_size);
    Ok((from, to, total_size))
}

async fn read_response_body(resp: HttpResponse, limit: Option<u64>) -> IoResult<Vec<u8>> {
    let mut buf_cursor = Cursor::new(Vec::<u8>::new());
    let body = resp
        .bytes_stream()
        .map_err(io_error_from(IoErrorKind::BrokenPipe))
        .into_async_read();
    let mut copy_from = if let Some(limit) = limit {
        Either::Left(body.take(limit).compat())
    } else {
        Either::Right(body.compat())
    };
    io_copy(&mut copy_from, &mut buf_cursor).await?;
    Ok(buf_cursor.into_inner())
}

fn io_error_from<E: Into<Box<dyn StdError + Send + Sync>>>(
    kind: IoErrorKind,
) -> impl Fn(E) -> IoError {
    move |err| IoError::new(kind, err)
}

fn new_io_error<E: Into<Box<dyn StdError + Send + Sync>>>(
    kind: IoErrorKind,
    err: E,
) -> impl FnOnce() -> IoError {
    move || IoError::new(kind, err)
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            cache_dir::cache_dir_path_of,
            dot::{DotRecordKey, DotRecords, DotRecordsDashMap, DOT_FILE_NAME},
            query::CACHE_FILE_NAME,
        },
        *,
    };
    use futures::channel::oneshot::channel;
    use multipart::client::lazy::Multipart as LazyMultipart;
    use serde_json::{json, to_vec as json_to_vec};
    use std::{
        io::Read,
        sync::{
            atomic::{AtomicUsize, Ordering::Relaxed},
            Arc,
        },
    };
    use tokio::{fs::remove_file, task::spawn, time::sleep};
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
                    rx.await.unwrap();
                });
            spawn(server);
            $code;
            tx.send(()).unwrap();
        }};
        ($io_addr:ident, $monitor_addr:ident, $io_routes:ident, $records_map:ident, $code:block) => {{
            let (io_tx, io_rx) = channel();
            let (monitor_tx, monitor_rx) = channel();
            let ($io_addr, io_server) = warp::serve($io_routes).bind_with_graceful_shutdown(
                ([127, 0, 0, 1], 0),
                async move {
                    io_rx.await.unwrap();
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
                    monitor_rx.await.unwrap();
                });
            spawn(io_server);
            spawn(monitor_server);
            $code;
            io_tx.send(()).unwrap();
            monitor_tx.send(()).unwrap();
        }};
        ($io_addr:ident, $uc_addr:ident, $io_routes:ident, $code:block) => {{
            let (io_tx, io_rx) = channel();
            let (uc_tx, uc_rx) = channel();
            let ($io_addr, io_server) = warp::serve($io_routes).bind_with_graceful_shutdown(
                ([127, 0, 0, 1], 0),
                async move {
                    io_rx.await.unwrap();
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
                    uc_rx.await.unwrap();
                },
            );
            spawn(io_server);
            spawn(uc_server);
            $code;
            io_tx.send(()).unwrap();
            uc_tx.send(()).unwrap();
        }};
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_at() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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
                let have_tried = AtomicUsize::new(0);
                let io_urls = io_urls.to_owned();
                let downloader = AsyncRangeReaderBuilder::from(
                    BaseRangeReaderBuilder::new(
                        "bucket".to_owned(),
                        "file".to_owned(),
                        get_credential(),
                        io_urls,
                    )
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                    .dot_interval(Duration::from_millis(0))
                    .max_dot_buffer_size(1),
                )
                .build();

                match downloader
                    .read_at(
                        5,
                        6,
                        "file",
                        0,
                        TriesInfo::new(&have_tried, 1),
                        &Default::default(),
                        |_| async {},
                    )
                    .await
                {
                    Result3::Ok(buf) => {
                        assert_eq!(&buf, b"123456")
                    }
                    _ => unreachable!(),
                }
            }
            {
                let have_tried = AtomicUsize::new(0);
                let io_urls = io_urls.to_owned();
                let downloader = AsyncRangeReaderBuilder::from(
                    BaseRangeReaderBuilder::new(
                        "bucket".to_owned(),
                        "file2".to_owned(),
                        get_credential(),
                        io_urls,
                    )
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                    .dot_interval(Duration::from_millis(0))
                    .max_dot_buffer_size(1),
                )
                .build();

                match downloader
                    .read_at(
                        5,
                        12,
                        "file2",
                        0,
                        TriesInfo::new(&have_tried, 1),
                        &Default::default(),
                        |_| async {},
                    )
                    .await
                {
                    Result3::Ok(buf) => {
                        assert_eq!(&buf[..10], b"1234567890")
                    }
                    _ => unreachable!(),
                }
            }

            sleep(Duration::from_secs(5)).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_at_2() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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
            let have_tried = AtomicUsize::new(0);
            let io_urls = vec![format!("http://{}", io_addr)];
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true)
                .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                .dot_interval(Duration::from_millis(0))
                .max_dot_buffer_size(1),
            )
            .build();

            match downloader
                .read_at(
                    1,
                    5,
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 3),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Err(..) => {}
                _ => unreachable!(),
            }
            assert_eq!(io_called.load(Relaxed), 3);

            sleep(Duration::from_secs(5)).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_at_3() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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
            let have_tried = AtomicUsize::new(0);
            let io_urls = vec![format!("http://{}", io_addr)];
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true)
                .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                .dot_interval(Duration::from_millis(0))
                .max_dot_buffer_size(1),
            )
            .build();

            match downloader
                .read_at(
                    1,
                    5,
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Err(..) => {}
                _ => unreachable!(),
            }
            assert_eq!(io_called.load(Relaxed), 1);

            sleep(Duration::from_secs(5)).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_last_bytes() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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
            let have_tried = AtomicUsize::new(0);
            let io_urls = vec![format!("http://{}", io_addr)];
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true)
                .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                .dot_interval(Duration::from_millis(0))
                .max_dot_buffer_size(1),
            )
            .build();

            match downloader
                .read_last_bytes(
                    10,
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok((buf, total_size)) => {
                    assert_eq!(&buf, b"1234567890");
                    assert_eq!(total_size, 157286400);
                }
                _ => unreachable!(),
            }

            sleep(Duration::from_secs(5)).await;
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
        });
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_file() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

        let io_routes = { path!("file").map(|| Response::new("1234567890".into())) };
        starts_with_server!(io_addr, monitor_addr, io_routes, records_map, {
            let io_urls = vec![format!("http://{}", io_addr)];
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true)
                .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                .dot_interval(Duration::from_millis(0))
                .max_dot_buffer_size(1),
            )
            .build();

            let have_tried = AtomicUsize::new(0);
            match downloader
                .exist(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(existed) => {
                    assert!(existed);
                }
                _ => unreachable!(),
            }

            let have_tried = AtomicUsize::new(0);
            match downloader
                .file_size(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(file_size) => {
                    assert_eq!(file_size, 10);
                }
                _ => unreachable!(),
            }

            let have_tried = AtomicUsize::new(0);
            match downloader
                .download(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(buf) => {
                    assert_eq!(&buf, b"1234567890");
                }
                _ => unreachable!(),
            }

            sleep(Duration::from_secs(5)).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_file_2() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                .use_getfile_api(false)
                .normalize_key(true)
                .dot_interval(Duration::from_millis(0))
                .max_dot_buffer_size(1),
            )
            .build();

            let have_tried = AtomicUsize::new(0);
            match downloader
                .exist(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 3),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Err(_) => {}
                _ => unreachable!(),
            }

            let have_tried = AtomicUsize::new(0);
            match downloader
                .file_size(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 3),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Err(_) => {}
                _ => unreachable!(),
            }

            let have_tried = AtomicUsize::new(0);
            match downloader
                .download(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 3),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Err(_) => {}
                _ => unreachable!(),
            }

            assert_eq!(counter.load(Relaxed), 3 * 3);

            sleep(Duration::from_secs(5)).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_file_3() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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

            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true),
            )
            .build();

            let have_tried = AtomicUsize::new(0);
            match downloader
                .exist(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 3),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Err(_) => {}
                _ => unreachable!(),
            }

            let have_tried = AtomicUsize::new(0);
            match downloader
                .file_size(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 3),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Err(_) => {}
                _ => unreachable!(),
            }

            let have_tried = AtomicUsize::new(0);
            match downloader
                .download(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 3),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Err(_) => {}
                _ => unreachable!(),
            }
            assert_eq!(counter.load(Relaxed), 3);
        });
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_file_4() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

        let routes = { path!("file").map(|| Response::new("1234567890".into())) };
        starts_with_server!(addr, routes, {
            let io_urls = vec![format!("http://{}", addr)];

            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true),
            )
            .build();

            let have_tried = AtomicUsize::new(0);
            match downloader
                .exist(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(existed) => {
                    assert!(existed);
                }
                _ => unreachable!(),
            }

            let have_tried = AtomicUsize::new(0);
            match downloader
                .file_size(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(file_size) => {
                    assert_eq!(file_size, 10);
                }
                _ => unreachable!(),
            }

            let have_tried = AtomicUsize::new(0);
            match downloader
                .download(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(buf) => {
                    assert_eq!(buf, b"1234567890");
                }
                _ => unreachable!(),
            }
        });
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_range() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

        let routes = {
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-9");
                    let mut response_body = LazyMultipart::new();
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
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true),
            )
            .build();

            let ranges = [(0, 5), (5, 5)];
            let have_tried = AtomicUsize::new(0);
            match downloader
                .read_multi_ranges(
                    &ranges,
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(parts) => {
                    assert_eq!(parts.len(), 2);
                    assert_eq!(&parts.get(1).unwrap().data, b"12345");
                    assert_eq!(parts.get(1).unwrap().range, (0, 5));
                    assert_eq!(&parts.get(0).unwrap().data, b"67890");
                    assert_eq!(parts.get(0).unwrap().range, (5, 5));
                }
                _ => unreachable!(),
            }
        });
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_range_2() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true),
            )
            .build();

            let ranges = [(0, 5), (5, 5)];
            let have_tried = AtomicUsize::new(0);
            match downloader
                .read_multi_ranges(
                    &ranges,
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(parts) => {
                    assert_eq!(parts.len(), 2);
                    assert_eq!(&parts.get(0).unwrap().data, b"12345");
                    assert_eq!(parts.get(0).unwrap().range, (0, 5));
                    assert_eq!(&parts.get(1).unwrap().data, b"67890");
                    assert_eq!(parts.get(1).unwrap().range, (5, 5));
                }
                _ => unreachable!(),
            }
        });

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_range_3() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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
            let c = counter.to_owned();
            spawn(async move {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = AsyncRangeReaderBuilder::from(
                    BaseRangeReaderBuilder::new(
                        "bucket".to_owned(),
                        "file".to_owned(),
                        get_credential(),
                        io_urls,
                    )
                    .use_getfile_api(false)
                    .normalize_key(true),
                )
                .build();

                let ranges = [(0, 5), (5, 5)];
                let have_tried = AtomicUsize::new(0);
                match downloader
                    .read_multi_ranges(
                        &ranges,
                        "file",
                        0,
                        TriesInfo::new(&have_tried, 3),
                        &Default::default(),
                        |_| async {},
                    )
                    .await
                {
                    Result3::Err(..) => {}
                    _ => unreachable!(),
                }
                assert_eq!(c.load(Relaxed), 3);
            })
            .await?;

            let c = counter.to_owned();
            spawn(async move {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = AsyncRangeReaderBuilder::from(
                    BaseRangeReaderBuilder::new(
                        "bucket".to_owned(),
                        "/file".to_owned(),
                        get_credential(),
                        io_urls,
                    )
                    .use_getfile_api(false),
                )
                .build();

                let ranges = [(0, 5), (5, 5)];
                let have_tried = AtomicUsize::new(0);
                match downloader
                    .read_multi_ranges(
                        &ranges,
                        "/file",
                        0,
                        TriesInfo::new(&have_tried, 3),
                        &Default::default(),
                        |_| async {},
                    )
                    .await
                {
                    Result3::Err(..) => {}
                    _ => unreachable!(),
                }
                assert_eq!(c.load(Relaxed), 6);
            })
            .await?;
        });

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_range_4() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

        let routes = {
            path!("file")
                .and(header::value(RANGE.as_str()))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-9");
                    let mut response_body = LazyMultipart::new();
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
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true),
            )
            .build();

            let ranges = [(0, 5), (5, 5)];
            let have_tried = AtomicUsize::new(0);
            match downloader
                .read_multi_ranges(
                    &ranges,
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(parts) => {
                    assert_eq!(parts.len(), 2);
                    assert_eq!(&parts.get(1).unwrap().data, b"12345");
                    assert_eq!(parts.get(1).unwrap().range, (0, 5));
                    assert_eq!(&parts.get(0).unwrap().data, b"6");
                    assert_eq!(parts.get(0).unwrap().range, (5, 1));
                }
                _ => unreachable!(),
            }
        });

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_range_5() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true),
            )
            .build();

            let ranges = [(0, 5), (5, 1)];
            let have_tried = AtomicUsize::new(0);
            match downloader
                .read_multi_ranges(
                    &ranges,
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                IoResult3::Ok(parts) => {
                    assert_eq!(parts.len(), 1);
                    assert_eq!(&parts.get(0).unwrap().data, b"1234");
                    assert_eq!(parts.get(0).unwrap().range, (0, 4));
                }
                _ => unreachable!(),
            }
        });

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_range_6() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls,
                )
                .use_getfile_api(false)
                .normalize_key(true),
            )
            .build();

            let ranges = [(0, 4)];
            let have_tried = AtomicUsize::new(0);
            match downloader
                .read_multi_ranges(
                    &ranges,
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(parts) => {
                    assert_eq!(parts.len(), 1);
                    assert_eq!(&parts.get(0).unwrap().data, b"123");
                    assert_eq!(parts.get(0).unwrap().range, (0, 3));
                }
                _ => unreachable!(),
            }
        });

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_update_hosts() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

        let routes = { path!("file").map(move || Response::new("12345".into())) };
        starts_with_server!(io_addr, uc_addr, routes, {
            let io_urls = vec!["http://fakedomain:12345".to_owned()];
            let uc_urls = vec![format!("http://{}", uc_addr)];
            let downloader = AsyncRangeReaderBuilder::from(
                BaseRangeReaderBuilder::new(
                    "bucket".to_owned(),
                    "file".to_owned(),
                    get_credential(),
                    io_urls.to_owned(),
                )
                .uc_urls(uc_urls)
                .use_getfile_api(false)
                .normalize_key(true),
            )
            .build();

            assert_eq!(downloader.io_urls().await, io_urls);
            assert!(downloader.update_urls().await);
            assert_eq!(
                downloader.io_urls().await,
                vec![format!("http://{}", io_addr)]
            );
            let have_tried = AtomicUsize::new(0);
            match downloader
                .download(
                    "file",
                    0,
                    TriesInfo::new(&have_tried, 1),
                    &Default::default(),
                    |_| async {},
                )
                .await
            {
                Result3::Ok(buf) => {
                    assert_eq!(&buf, b"12345")
                }
                _ => unreachable!(),
            }
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_sign_download_url_with_deadline() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

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

    fn get_credential() -> Credential {
        Credential::new("1234567890", "abcdefghijk")
    }

    async fn clear_cache() -> IoResult<()> {
        let cache_file_path = cache_dir_path_of(CACHE_FILE_NAME).await?;
        remove_file(&cache_file_path).await.or_else(|err| {
            if err.kind() == IoErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        })?;
        let dot_file_path = cache_dir_path_of(DOT_FILE_NAME).await?;
        remove_file(&dot_file_path).await.or_else(|err| {
            if err.kind() == IoErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        })?;
        Ok(())
    }
}
