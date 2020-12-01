use super::{base::credential::Credential, query, HTTP_CLIENT};
use multipart::server::Multipart;
use positioned_io::ReadAt;
use rand::{seq::SliceRandom, thread_rng};
use rayon::prelude::*;
use reqwest::{
    blocking::Response as HTTPResponse,
    header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    StatusCode,
};
use sema::Semaphore;
use std::{
    io::{
        copy as io_copy, Cursor, Error as IOError, ErrorKind as IOErrorKind, Read,
        Result as IOResult, Seek, SeekFrom, Write,
    },
    result::Result,
    sync::Mutex,
    thread::sleep,
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};
use text_io::{try_scan as try_scan_text, Error as TextIOError};
use url::Url;

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

pub fn sign_download_url_with_lifetime(
    c: &Credential,
    url: Url,
    lifetime: Duration,
) -> Result<String, SystemTimeError> {
    let deadline = SystemTime::now() + lifetime;
    sign_download_url_with_deadline(c, url, deadline)
}

#[derive(Debug)]
pub struct RangeReader {
    urls: Vec<String>,
    tries: usize,
}

impl RangeReader {
    pub fn new(urls: &[String], tries: usize) -> Self {
        assert!(tries > 0);
        Self {
            urls: urls.to_owned(),
            tries,
        }
    }

    pub fn new_from_key(
        key: impl AsRef<str>,
        urls: &[String],
        credential: &Credential,
        lifetime: Duration,
        tries: usize,
    ) -> Result<Self, SystemTimeError> {
        let urls = urls
            .iter()
            .map(|domain| {
                let url = domain.to_owned() + key.as_ref();
                let signed_url = sign_download_url_with_lifetime(
                    credential,
                    Url::parse(&url).unwrap(),
                    lifetime,
                )?;
                Ok(signed_url)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new(&urls, tries))
    }

    pub fn new_from_bucket_and_key(
        bucket: impl AsRef<str>,
        key: impl AsRef<str>,
        uc_urls: &[String],
        credential: &Credential,
        lifetime: Duration,
        tries: usize,
        use_https: bool,
    ) -> IOResult<Self> {
        let io_urls =
            query::query_for_io_urls(credential.access_key(), bucket.as_ref(), uc_urls, use_https)?;
        Self::new_from_key(key, &io_urls, credential, lifetime, tries)
            .map_err(|err| IOError::new(IOErrorKind::Other, err))
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
        let range = format!("bytes={}-{}", pos, pos + size - 1);
        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            let result = HTTP_CLIENT
                .get(url)
                .header(RANGE, &range)
                .send()
                .map_err(|err| IOError::new(IOErrorKind::Other, err))
                .and_then(|resp| {
                    let code = resp.status();
                    if code != StatusCode::PARTIAL_CONTENT && code != StatusCode::OK {
                        return Err(IOError::new(
                            IOErrorKind::Other,
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
                    cursor.set_position(0);
                    io_error = Some(err);
                }
            }
        }
        Err(io_error.unwrap())
    }
}

#[derive(Debug, Clone)]
pub struct RangePart {
    pub data: Vec<u8>,
    pub range: (u64, u64),
}

impl RangeReader {
    pub fn read_multi_range(&self, range: &[(u64, u64)]) -> IOResult<Vec<RangePart>> {
        let range_header_value = format!("bytes={}", generate_range_header(range));
        let mut io_error: Option<IOError> = None;

        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            let result = HTTP_CLIENT
                .get(url)
                .header(RANGE, &range_header_value)
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
                                    let content_type = resp
                                        .headers()
                                        .get(CONTENT_TYPE)
                                        .expect("Content-Type must be existed");
                                    extract_boundary(
                                        &content_type
                                            .to_str()
                                            .map_err(|err| IOError::new(IOErrorKind::Other, err))?,
                                    )
                                    .expect("Boundary must be existed in Content-Type")
                                    .to_owned()
                                };

                                Multipart::with_body(&mut resp, boundary).foreach_entry(
                                    |mut field| {
                                        let content_range = field
                                            .headers
                                            .content_range
                                            .expect("Content-Range must be existed");
                                        let (from, to, _) = parse_range_header(&content_range)
                                            .expect("Invalid Content-Range");
                                        let len = to - from + 1;
                                        let mut data = Vec::with_capacity(len as usize);
                                        field.data.read_to_end(&mut data).unwrap();
                                        parts.push(RangePart {
                                            data,
                                            range: (from, len),
                                        });
                                    },
                                )?;
                            } else {
                                let content_range = resp
                                    .headers()
                                    .get(CONTENT_RANGE)
                                    .expect("Content-Range must be existed");
                                let (from, to, _) = content_range
                                    .to_str()
                                    .ok()
                                    .and_then(|r| parse_range_header(r).ok())
                                    .expect("Invalid Content-Range");
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
                    io_error = Some(err);
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

    pub fn exist(&self) -> IOResult<bool> {
        self.file_size().map(|_| true)
    }

    pub fn file_size(&self) -> IOResult<u64> {
        let mut io_error: Option<IOError> = None;
        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            let result = HTTP_CLIENT
                .head(url)
                .send()
                .map_err(|err| IOError::new(IOErrorKind::Other, err))
                .and_then(|resp| {
                    if resp.status() == StatusCode::OK {
                        Ok(parse_content_length(&resp))
                    } else {
                        Err(IOError::new(IOErrorKind::Other, "Status code is not 200"))
                    }
                });
            match result {
                Ok(size) => {
                    return Ok(size);
                }
                Err(err) => {
                    io_error = Some(err);
                }
            }
        }
        Err(io_error.unwrap())
    }

    pub fn download(&self) -> IOResult<Vec<u8>> {
        let mut bytes = Cursor::new(Vec::new());
        self.download_to(&mut bytes, None)?;
        Ok(bytes.into_inner())
    }

    pub fn download_to(&self, writer: &mut dyn WriteSeek, max_size: Option<u64>) -> IOResult<u64> {
        let mut io_error: Option<IOError> = None;
        let init_start_from = writer.seek(SeekFrom::End(0))?;
        let mut start_from = init_start_from;
        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            let mut req = HTTP_CLIENT.get(url);
            if start_from > 0 {
                req = req.header(RANGE, format!("bytes={}-", start_from));
            }
            let result = req
                .send()
                .map_err(|err| IOError::new(IOErrorKind::Other, err))
                .and_then(|mut resp| {
                    if resp.status() == StatusCode::RANGE_NOT_SATISFIABLE {
                        return Ok(0);
                    } else if resp.status() != StatusCode::OK
                        && resp.status() != StatusCode::PARTIAL_CONTENT
                    {
                        return Err(IOError::new(
                            IOErrorKind::Other,
                            "Status code is neither 200 nor 206",
                        ));
                    }
                    if let Some(max_size) = max_size {
                        io_copy(&mut resp.take(max_size), writer)
                            .map_err(|err| IOError::new(IOErrorKind::BrokenPipe, err))
                    } else {
                        resp.copy_to(writer)
                            .map_err(|err| IOError::new(IOErrorKind::BrokenPipe, err))
                    }
                });
            start_from = writer.seek(SeekFrom::Current(0))?;
            match result {
                Ok(_) => {
                    return Ok(start_from - init_start_from);
                }
                Err(err) => {
                    io_error = Some(err);
                }
            }
        }
        Err(io_error.unwrap())
    }

    pub fn flash_download_to(
        &self,
        writer: &mut (dyn WriteSeek + Send),
        max_size: Option<u64>,
        part_size: u64,
        concurrency: Option<usize>,
    ) -> IOResult<u64> {
        let total_size = {
            let file_size = self.file_size()?;
            if let Some(max_size) = max_size {
                file_size.min(max_size)
            } else {
                file_size
            }
        };
        let writer = Mutex::new(writer);
        let semaphore = {
            #[cfg(target_os = "linux")]
            {
                concurrency.map(Semaphore::new)
            }
            #[cfg(not(target_os = "linux"))]
            {
                concurrency.map(|c| Semaphore::new(c as u32))
            }
        };
        let downloaded = split_parts(part_size, total_size)
            .into_par_iter()
            .map(|(from, to)| {
                let buf = {
                    let _guard = semaphore.as_ref().take();
                    let size = to - from;
                    let mut buf = vec![0; size as usize];

                    let have_read = self.read_at(from, &mut buf)?;
                    if have_read as u64 != size {
                        return Err(IOError::new(
                            IOErrorKind::Other,
                            format!(
                                "Read from {}-{}, Expected got {} bytes, actually got {} bytes",
                                from, to, size, have_read
                            ),
                        ));
                    }
                    buf
                };
                let mut w = writer.lock().unwrap();
                w.seek(SeekFrom::Start(from))?;
                w.write_all(&buf)?;
                Ok(to - from)
            })
            .collect::<IOResult<Vec<u64>>>()?
            .iter()
            .sum();
        return Ok(downloaded);

        fn split_parts(part_size: u64, total_size: u64) -> Vec<(u64, u64)> {
            let mut parts = Vec::new();
            let mut start = 0;
            while start < total_size {
                let new_start = (start + part_size).min(total_size);
                parts.push((start, new_start));
                start = new_start;
            }
            parts
        }
    }

    pub fn read_last_bytes(&self, buf: &mut [u8]) -> IOResult<(u64, u64)> {
        let size = buf.len() as u64;
        let mut cursor = Cursor::new(buf);
        let mut io_error: Option<IOError> = None;
        let range = format!("bytes=-{}", size);
        for (tries, url) in self.choose_urls().into_iter().enumerate() {
            sleep_before_retry(tries);

            let result = HTTP_CLIENT
                .get(url)
                .header(RANGE, &range)
                .send()
                .map_err(|err| IOError::new(IOErrorKind::Other, err))
                .and_then(|resp| {
                    let code = resp.status();
                    if code != StatusCode::PARTIAL_CONTENT {
                        return Err(IOError::new(IOErrorKind::Other, "Status code is not 206"));
                    }
                    let content_range = resp
                        .headers()
                        .get(CONTENT_RANGE)
                        .and_then(|r| r.to_str().ok())
                        .expect("Content-Range must be existed");
                    let (_, _, total_size) =
                        parse_range_header(content_range).expect("Invalid Content-Range");
                    let actual_size = io_copy(&mut resp.take(size), &mut cursor)?;
                    Ok((actual_size, total_size))
                });
            match result {
                Ok((actual_size, total_size)) => {
                    return Ok((actual_size, total_size));
                }
                Err(err) => {
                    cursor.set_position(0);
                    io_error = Some(err);
                }
            }
        }
        Err(io_error.unwrap())
    }

    fn choose_urls(&self) -> Vec<&str> {
        let mut urls: Vec<&str> = self
            .urls
            .choose_multiple(&mut thread_rng(), self.tries)
            .map(|s| s.as_str())
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
    let wait = 500 * (tries << 1) as u64;
    if wait > 0 {
        sleep(Duration::from_millis(wait));
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
                let url = format!("http://{}/file", addr);
                let downloader = RangeReader::new(&[url.to_owned()], 3);
                spawn_blocking(move || {
                    let mut buf = [0u8; 6];
                    assert_eq!(downloader.read_at(5, &mut buf).unwrap(), 6);
                    assert_eq!(&buf, b"123456");
                })
                .await?;
            }
            {
                let url = format!("http://{}/file2", addr);
                let downloader = RangeReader::new(&[url.to_owned()], 3);
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
            let url = format!("http://{}/file", addr);
            let downloader = RangeReader::new(&[url.to_owned()], 3);
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
    async fn test_read_last_bytes() -> Result<(), Box<dyn Error>> {
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
                let url = format!("http://{}/file", addr);
                let downloader = RangeReader::new(&[url.to_owned()], 3);
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
        let routes = { path!("file").map(|| Response::new("1234567890".into())) };
        starts_with_server!(addr, routes, {
            let url = format!("http://{}/file", addr);
            let downloader = RangeReader::new(&[url.to_owned()], 3);
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
            let url = format!("http://{}/file", addr);
            let downloader = RangeReader::new(&[url.to_owned()], 3);
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
        let routes = { path!("file").map(|| Response::new("1234567890".into())) };
        starts_with_server!(addr, routes, {
            let url = format!("http://{}/file", addr);
            let downloader = RangeReader::new(&[url.to_owned()], 3);
            spawn_blocking(move || {
                assert!(downloader.exist().unwrap());
                assert_eq!(downloader.file_size().unwrap(), 10);
                let mut buf = Vec::new();
                {
                    let mut cursor = Cursor::new(&mut buf);
                    assert_eq!(downloader.download_to(&mut cursor, Some(5)).unwrap(), 5);
                }
                assert_eq!(&buf, b"12345");
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_download_range() -> Result<(), Box<dyn Error>> {
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
            let url = format!("http://{}/file", addr);
            let range_reader = RangeReader::new(&[url.to_owned()], 3);
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
        let routes = {
            path!("file")
                .and(header::value("Range"))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-9");
                    "12345678901357924680"
                })
        };
        starts_with_server!(addr, routes, {
            let url = format!("http://{}/file", addr);
            let range_reader = RangeReader::new(&[url.to_owned()], 3);
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
            let url = format!("http://{}/file", addr);
            let range_reader = RangeReader::new(&[url.to_owned()], 3);
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
            let url = format!("http://{}/file", addr);
            let range_reader = RangeReader::new(&[url.to_owned()], 3);
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
        let routes = {
            path!("file")
                .and(header::value("Range"))
                .map(move |range: HeaderValue| {
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-5");
                    "1234"
                })
        };
        starts_with_server!(addr, routes, {
            let url = format!("http://{}/file", addr);
            let range_reader = RangeReader::new(&[url.to_owned()], 3);
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
            let url = format!("http://{}/file", addr);
            let range_reader = RangeReader::new(&[url.to_owned()], 3);
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
