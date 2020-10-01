use super::base::credential::Credential;
use multipart::server::Multipart;
use once_cell::sync::Lazy;
use positioned_io::ReadAt;
use rand::{seq::SliceRandom, thread_rng};
use rayon::prelude::*;
use reqwest::{
    blocking::{Client as HTTPClient, Response as HTTPResponse},
    header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE},
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
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};
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

static HTTP_CLIENT: Lazy<HTTPClient> = Lazy::new(Default::default);

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
        domains: &[String],
        credential: &Credential,
        lifetime: Duration,
        tries: usize,
    ) -> Result<Self, SystemTimeError> {
        let urls = domains
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
        for url in self.choose_urls() {
            let result = HTTP_CLIENT
                .get(url)
                .header("Range", &range)
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

impl RangeReader {
    pub fn read_multi_range(&self, buf: &mut [u8], range: &[(u64, u64)]) -> IOResult<usize> {
        let range_header_value = format!("bytes={}", generate_range_header(range));
        let buf_size = buf.len();
        let mut cursor = Cursor::new(buf);
        let mut io_error: Option<IOError> = None;

        for url in self.choose_urls() {
            let result = HTTP_CLIENT
                .get(url)
                .header("Range", &range_header_value)
                .send()
                .map_err(|err| IOError::new(IOErrorKind::Other, err))
                .and_then(|mut resp| {
                    match resp.status() {
                        StatusCode::OK => {
                            let body = resp
                                .bytes()
                                .map_err(|err| IOError::new(IOErrorKind::BrokenPipe, err))?;
                            for &(from, len) in range.iter() {
                                let from = (from as usize).min(body.len());
                                let len = (len as usize)
                                    .min(body.len() - from)
                                    .min(buf_size - cursor.position() as usize);
                                if len > 0 {
                                    cursor.write_all(&body.slice(from..(from + len)))?;
                                }
                            }
                        }
                        StatusCode::PARTIAL_CONTENT => {
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

                            Multipart::with_body(&mut resp, boundary).foreach_entry(|field| {
                                let max_size = buf_size as u64 - cursor.position();
                                if max_size > 0 {
                                    io_copy(&mut field.data.take(max_size), &mut cursor).unwrap();
                                }
                            })?;
                        }
                        _ => {
                            return Err(IOError::new(
                                IOErrorKind::Other,
                                "Status code is neither 200 nor 206",
                            ));
                        }
                    }

                    Ok(cursor.position() as usize)
                });
            match result {
                Ok(size) => {
                    return Ok(size);
                }
                Err(err) => {
                    cursor.set_position(0);
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
        for url in self.choose_urls() {
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
        return Err(io_error.unwrap());
    }

    pub fn download(&self) -> IOResult<Vec<u8>> {
        let mut bytes = Cursor::new(Vec::new());
        self.download_to(&mut bytes, None)?;
        Ok(bytes.into_inner())
    }

    pub fn download_to(&self, writer: &mut dyn WriteSeek, max_size: Option<u64>) -> IOResult<u64> {
        let mut io_error: Option<IOError> = None;
        let original_position = writer.seek(SeekFrom::Current(0))?;
        for url in self.choose_urls() {
            let req = HTTP_CLIENT.get(url);
            let result = req
                .send()
                .map_err(|err| IOError::new(IOErrorKind::Other, err))
                .and_then(|mut resp| {
                    if resp.status() != StatusCode::OK
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
            match result {
                Ok(downloaded) => {
                    return Ok(downloaded);
                }
                Err(err) => {
                    writer.seek(SeekFrom::Start(original_position))?;
                    io_error = Some(err);
                }
            }
        }
        return Err(io_error.unwrap());
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
            .fold(0, |sum, have_read| sum + have_read);
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

    pub fn download_last_bytes(&self, buf: &mut [u8]) -> IOResult<u64> {
        let size = buf.len() as u64;
        let mut cursor = Cursor::new(buf);
        let mut io_error: Option<IOError> = None;
        let range = format!("bytes=-{}", size);
        for url in self.choose_urls() {
            let result = HTTP_CLIENT
                .get(url)
                .header("Range", &range)
                .send()
                .map_err(|err| IOError::new(IOErrorKind::Other, err))
                .and_then(|resp| {
                    let code = resp.status();
                    if code != StatusCode::PARTIAL_CONTENT {
                        return Err(IOError::new(IOErrorKind::Other, "Status code is not 206"));
                    }
                    let total_size = parse_total_size_from_content_range(&resp);
                    io_copy(&mut resp.take(size), &mut cursor)?;
                    Ok(total_size)
                });
            match result {
                Ok(total_size) => {
                    return Ok(total_size);
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

fn parse_total_size_from_content_range(resp: &HTTPResponse) -> u64 {
    resp.headers()
        .get(CONTENT_RANGE)
        .and_then(|line| line.to_str().ok())
        .and_then(|line| line.split("/").nth(1))
        .and_then(|length| length.parse().ok())
        .expect("Content-Range cannot be parsed")
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
    async fn test_download_last_bytes() -> Result<(), Box<dyn Error>> {
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
                    assert_eq!(downloader.download_last_bytes(&mut buf).unwrap(), 157286400);
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
                let mut buf = [0u8; 5];
                {
                    let mut cursor = Cursor::new(&mut buf[..]);
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
                    response_body.add_text("", "12345");
                    response_body.add_text("", "67890");
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
                let mut buf = [0; 10];
                assert_eq!(
                    range_reader.read_multi_range(&mut buf, &ranges).unwrap(),
                    10
                );
                assert_eq!(&buf, b"1234567890");
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
                let mut buf = [0; 10];
                assert_eq!(
                    range_reader.read_multi_range(&mut buf, &ranges).unwrap(),
                    10
                );
                assert_eq!(&buf, b"1234567890");
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
                range_reader
                    .read_multi_range(&mut Vec::<u8>::new(), &ranges)
                    .unwrap_err();
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
                    response_body.add_text("", "12345");
                    response_body.add_text("", "67890");
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
                let mut buf = [0; 6];
                assert_eq!(range_reader.read_multi_range(&mut buf, &ranges).unwrap(), 6,);
                assert_eq!(&buf, b"123456");
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
                    assert_eq!(range.to_str().unwrap(), "bytes=0-4,5-9");
                    "12345678901357924680"
                })
        };
        starts_with_server!(addr, routes, {
            let url = format!("http://{}/file", addr);
            let range_reader = RangeReader::new(&[url.to_owned()], 3);
            let ranges = [(0, 5), (5, 5)];
            spawn_blocking(move || {
                let mut buf = [0; 6];
                assert_eq!(range_reader.read_multi_range(&mut buf, &ranges).unwrap(), 6);
                assert_eq!(&buf, b"123456");
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
