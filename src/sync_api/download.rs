use super::{
    super::{
        async_api::{sign_download_url_with_lifetime, RangePart},
        base::{credential::Credential, download::RangeReaderBuilder as BaseRangeReaderBuilder},
        config::{
            build_range_reader_builder_from_config, with_current_qiniu_config, Config, Timeouts,
        },
    },
    dot::{ApiName, DotType, Dotter},
    host_selector::{HostSelector, HostSelectorBuilder},
    query::HostsQuerier,
    req_id::{get_req_id, REQUEST_ID_HEADER},
};
use log::{debug, error, info, warn};
use multipart::server::Multipart;
use positioned_io::ReadAt;
use reqwest::{
    blocking::{
        Client as HTTPClient, RequestBuilder as HTTPRequestBuilder, Response as HTTPResponse,
    },
    header::{HeaderValue, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    Error as ReqwestError, Method, StatusCode, Url,
};
use std::{
    io::{
        copy as io_copy, Cursor, Error as IOError, ErrorKind as IOErrorKind, Read,
        Result as IOResult, Seek, SeekFrom, Write,
    },
    result::Result,
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant, SystemTime},
};
use tap::prelude::*;
use text_io::{try_scan as try_scan_text, Error as TextIOError};

#[derive(Debug)]
pub(crate) struct RangeReader {
    inner: Arc<RangeReaderInner>,
    key: String,
}

#[derive(Debug)]
pub(crate) struct RangeReaderInner {
    io_selector: HostSelector,
    dotter: Dotter,
    credential: Credential,
    http_client: Arc<HTTPClient>,
    bucket: String,
    tries: usize,
    use_getfile_api: bool,
    normalize_key: bool,
    use_https: bool,
    private_url_lifetime: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct RangeReaderBuilder(BaseRangeReaderBuilder);

impl From<BaseRangeReaderBuilder> for RangeReaderBuilder {
    fn from(builder: BaseRangeReaderBuilder) -> Self {
        Self(builder)
    }
}

impl From<RangeReaderBuilder> for BaseRangeReaderBuilder {
    fn from(builder: RangeReaderBuilder) -> Self {
        builder.0
    }
}

impl RangeReaderBuilder {
    pub(crate) fn build(self) -> RangeReader {
        let (inner, key) = self.build_inner_and_key();
        RangeReader { inner, key }
    }

    fn build_inner_and_key(self) -> (Arc<RangeReaderInner>, String) {
        let builder = self.0;
        let http_client = Timeouts::new(builder.base_timeout, builder.dial_timeout).http_client();
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
        );

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
                make_uc_host_selector(builder.uc_urls, &params),
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
        );

        return (
            Arc::new(RangeReaderInner {
                io_selector,
                dotter,
                http_client,
                credential: builder.credential,
                bucket: builder.bucket,
                tries: builder.io_tries,
                use_getfile_api: builder.use_getfile_api,
                normalize_key: builder.normalize_key,
                use_https: builder.use_https,
                private_url_lifetime: builder.private_url_lifetime,
            }),
            builder.key,
        );

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

        fn make_uc_host_selector(
            uc_urls: Vec<String>,
            params: &HostSelectorParams,
        ) -> HostSelector {
            params.set_builder(HostSelector::builder(uc_urls)).build()
        }

        fn make_io_selector(
            io_urls: Vec<String>,
            io_querier: Option<HostsQuerier>,
            access_key: String,
            bucket: String,
            use_https: bool,
            params: &HostSelectorParams,
        ) -> HostSelector {
            let builder = HostSelector::builder(io_urls)
                .update_callback(Some(Box::new(move || -> IOResult<Vec<String>> {
                    if let Some(io_querier) = &io_querier {
                        io_querier.query_for_io_urls(&access_key, &bucket, use_https)
                    } else {
                        Ok(vec![])
                    }
                })))
                .should_punish_callback(Some(Box::new(|error| {
                    !matches!(error.kind(), IOErrorKind::InvalidData)
                })));
            params.set_builder(builder).build()
        }
    }

    pub(crate) fn from_config(key: String, config: &Config) -> Self {
        build_range_reader_builder_from_config(key, config).into()
    }
}

impl RangeReader {
    pub(crate) fn from_config(key: String, config: &Config) -> Self {
        RangeReaderBuilder::from_config(key, config).build()
    }

    pub(crate) fn from_env(key: String) -> Option<Self> {
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

    pub(crate) fn update_urls(&self) -> bool {
        self.inner.io_selector.update_hosts()
    }

    pub(crate) fn io_urls(&self) -> Vec<String> {
        return self
            .inner
            .io_selector
            .hosts()
            .iter()
            .map(|host| normalize_host(host, self.inner.use_https))
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

impl RangeReader {
    pub(crate) fn read_multi_ranges(&self, ranges: &[(u64, u64)]) -> IOResult<Vec<RangePart>> {
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

    pub(crate) fn exist(&self) -> IOResult<bool> {
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

    pub(crate) fn file_size(&self) -> IOResult<u64> {
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

    pub(crate) fn download(&self) -> IOResult<Vec<u8>> {
        let mut bytes = Cursor::new(Vec::new());
        self.download_to(&mut bytes)?;
        Ok(bytes.into_inner())
    }

    pub(crate) fn download_to(&self, writer: &mut dyn WriteSeek) -> IOResult<u64> {
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
                start_from = writer.stream_position()?;
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
    pub(crate) fn read_last_bytes(&self, buf: &mut [u8]) -> IOResult<(u64, u64)> {
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
            let req_id = get_req_id(begin_at, tries, chosen_io_info.timeout);
            let request_begin_at_instant = Instant::now();
            let request_builder = self
                .inner
                .http_client
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

        fn sleep_before_retry(tries: usize) {
            if tries >= 3 {
                sleep(Duration::from_secs(tries as u64));
            }
        }
    }

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

#[cold]
#[inline(never)]
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
            cache_dir::cache_dir_path_of,
            dot::{DotRecordKey, DotRecords, DotRecordsDashMap, DOT_FILE_NAME},
        },
        *,
    };
    use futures::channel::oneshot::channel;
    use multipart::client::lazy::Multipart;
    use serde_json::{json, to_vec as json_to_vec};
    use std::{
        fs::remove_file,
        io::Read,
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

    fn get_credential() -> Credential {
        Credential::new("1234567890", "abcdefghijk")
    }

    #[tokio::test]
    async fn test_read_at() -> anyhow::Result<()> {
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
                let io_urls = io_urls.to_owned();
                spawn_blocking(move || {
                    let downloader = RangeReaderBuilder::from(
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
                    let mut buf = [0u8; 6];
                    assert_eq!(downloader.read_at(5, &mut buf).unwrap(), 6);
                    assert_eq!(&buf, b"123456");
                })
                .await?;
            }
            {
                let io_urls = io_urls.to_owned();
                spawn_blocking(move || {
                    let downloader = RangeReaderBuilder::from(
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
    async fn test_read_at_2() -> anyhow::Result<()> {
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
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", io_addr)];
                let downloader = RangeReaderBuilder::from(
                    BaseRangeReaderBuilder::new(
                        "bucket".to_owned(),
                        "file".to_owned(),
                        get_credential(),
                        io_urls,
                    )
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .io_tries(3)
                    .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                    .dot_interval(Duration::from_millis(0))
                    .max_dot_buffer_size(1),
                )
                .build();
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
    async fn test_read_at_3() -> anyhow::Result<()> {
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
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", io_addr)];
                let downloader = RangeReaderBuilder::from(
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
    async fn test_read_last_bytes() -> anyhow::Result<()> {
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
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", io_addr)];
                let downloader = RangeReaderBuilder::from(
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
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_file() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache()?;

        let io_routes = { path!("file").map(|| Response::new("1234567890".into())) };
        starts_with_server!(io_addr, monitor_addr, io_routes, records_map, {
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", io_addr)];
                let downloader = RangeReaderBuilder::from(
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
    async fn test_download_file_2() -> anyhow::Result<()> {
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
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = RangeReaderBuilder::from(
                    BaseRangeReaderBuilder::new(
                        "bucket".to_owned(),
                        "file".to_owned(),
                        get_credential(),
                        io_urls,
                    )
                    .io_tries(3)
                    .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .dot_interval(Duration::from_millis(0))
                    .max_dot_buffer_size(1),
                )
                .build();
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
    async fn test_download_file_3() -> anyhow::Result<()> {
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
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = RangeReaderBuilder::from(
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
    async fn test_download_file_4() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache()?;

        let routes = { path!("file").map(|| Response::new("1234567890".into())) };
        starts_with_server!(addr, routes, {
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = RangeReaderBuilder::from(
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
    async fn test_download_range() -> anyhow::Result<()> {
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
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = RangeReaderBuilder::from(
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
    async fn test_download_range_2() -> anyhow::Result<()> {
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
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = RangeReaderBuilder::from(
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
    async fn test_download_range_3() -> anyhow::Result<()> {
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
            let c = counter.to_owned();
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = RangeReaderBuilder::from(
                    BaseRangeReaderBuilder::new(
                        "bucket".to_owned(),
                        "file".to_owned(),
                        get_credential(),
                        io_urls,
                    )
                    .io_tries(3)
                    .use_getfile_api(false)
                    .normalize_key(true),
                )
                .build();
                let ranges = [(0, 5), (5, 5)];
                downloader.read_multi_ranges(&ranges).unwrap_err();
                assert_eq!(c.load(Relaxed), 3);
            })
            .await?;

            let c = counter.to_owned();
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = RangeReaderBuilder::from(
                    BaseRangeReaderBuilder::new(
                        "bucket".to_owned(),
                        "/file".to_owned(),
                        get_credential(),
                        io_urls,
                    )
                    .io_tries(3)
                    .use_getfile_api(false),
                )
                .build();
                let ranges = [(0, 5), (5, 5)];
                downloader.read_multi_ranges(&ranges).unwrap_err();
                assert_eq!(c.load(Relaxed), 6);
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_range_4() -> anyhow::Result<()> {
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
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = RangeReaderBuilder::from(
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
    async fn test_download_range_5() -> anyhow::Result<()> {
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
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = RangeReaderBuilder::from(
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
    async fn test_download_range_6() -> anyhow::Result<()> {
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
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", addr)];
                let downloader = RangeReaderBuilder::from(
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
    async fn test_update_hosts() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache()?;

        let routes = { path!("file").map(move || Response::new("12345".into())) };
        starts_with_server!(io_addr, uc_addr, routes, {
            spawn_blocking(move || {
                let io_urls = vec!["http://fakedomain:12345".to_owned()];
                let uc_urls = vec![format!("http://{}", uc_addr)];
                let downloader = RangeReaderBuilder::from(
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
                assert_eq!(downloader.io_urls(), io_urls);
                assert!(downloader.update_urls());
                assert_eq!(downloader.io_urls(), vec![format!("http://{}", io_addr)]);
                assert_eq!(&downloader.download().unwrap(), b"12345");
            })
            .await?;
        });
        Ok(())
    }

    fn clear_cache() -> IOResult<()> {
        let cache_file_path = cache_dir_path_of("query-cache.json")?;
        remove_file(&cache_file_path).or_else(|err| {
            if err.kind() == IOErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        })?;
        let dot_file_path = cache_dir_path_of(DOT_FILE_NAME)?;
        remove_file(&dot_file_path).or_else(|err| {
            if err.kind() == IOErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        })?;
        Ok(())
    }
}
