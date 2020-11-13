use super::HTTP_CLIENT;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use rand::prelude::*;
use reqwest::StatusCode;
use serde::Deserialize;
use std::{
    io::{Error as IOError, ErrorKind as IOErrorKind, Result as IOResult},
    thread::spawn,
    time::{Duration, Instant},
};
use url::Url;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct CacheKey {
    ak: Box<str>,
    bucket: Box<str>,
}

impl CacheKey {
    fn new(ak: Box<str>, bucket: Box<str>) -> Self {
        Self { ak, bucket }
    }
}

#[derive(Debug, Clone)]
struct CacheValue {
    cached_response_body: ResponseBody,
    cache_lifetime: Duration,
    cached_at: Instant,
}

#[derive(Debug, Clone, Deserialize)]
struct ResponseBody {
    hosts: Vec<RegionResponseBody>,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionResponseBody {
    ttl: u64,
    io: DomainsResponseBody,
}

#[derive(Debug, Clone, Deserialize)]
struct DomainsResponseBody {
    domains: Box<[Box<str>]>,
}

static CACHE_MAP: Lazy<DashMap<CacheKey, CacheValue>> = Lazy::new(Default::default);

pub(super) fn query_for_io_urls(
    ak: &str,
    bucket: &str,
    uc_urls: &[String],
    use_https: bool,
) -> IOResult<Vec<String>> {
    let response_body = query_for_domains(ak, bucket, uc_urls)?;
    Ok(response_body
        .hosts
        .first()
        .expect("No host in uc query v4 response body")
        .io
        .domains
        .iter()
        .map(|domain| {
            if domain.contains("://") {
                domain.to_string()
            } else if use_https {
                "https://".to_owned() + domain
            } else {
                "http://".to_owned() + domain
            }
        })
        .collect())
}

fn query_for_domains(ak: &str, bucket: &str, uc_urls: &[String]) -> IOResult<ResponseBody> {
    let cache_key = CacheKey::new(ak.into(), bucket.into());
    let cache_value = CACHE_MAP
        .entry(cache_key.to_owned())
        .or_try_insert_with(|| query_for_domains_without_cache(ak, bucket, uc_urls))?;
    if cache_value.cached_at.elapsed() > cache_value.cache_lifetime {
        let ak = ak.to_owned();
        let bucket = bucket.to_owned();
        let uc_urls = uc_urls.to_owned();
        spawn(move || {
            CACHE_MAP.entry(cache_key).and_modify(|cache_value| {
                if cache_value.cached_at.elapsed() > cache_value.cache_lifetime {
                    if let Ok(new_cache_value) =
                        query_for_domains_without_cache(ak, bucket, &uc_urls)
                    {
                        *cache_value = new_cache_value;
                    }
                }
            })
        });
    }
    Ok(cache_value.cached_response_body.to_owned())
}

fn query_for_domains_without_cache(
    ak: impl AsRef<str>,
    bucket: impl AsRef<str>,
    uc_urls: &[String],
) -> IOResult<CacheValue> {
    let mut error: Option<IOError> = None;
    for _ in 0..10 {
        let url = Url::parse_with_params(
            &format!(
                "{}/v4/query",
                uc_urls
                    .choose(&mut thread_rng())
                    .expect("uc_urls must not be empty")
            ),
            &[("ak", ak.as_ref()), ("bucket", bucket.as_ref())],
        )
        .map_err(|err| IOError::new(IOErrorKind::Other, err))?;

        let response_result: IOResult<ResponseBody> = HTTP_CLIENT
            .get(&url.to_string())
            .send()
            .map_err(|err| IOError::new(IOErrorKind::Other, err))
            .and_then(|resp| {
                let code = resp.status();
                if code != StatusCode::OK {
                    return Err(IOError::new(IOErrorKind::Other, "Status code is not 200"));
                }
                resp.json::<ResponseBody>()
                    .map_err(|err| IOError::new(IOErrorKind::Other, err))
            });
        match response_result {
            Ok(response_body) => {
                let min_ttl = response_body
                    .hosts
                    .iter()
                    .map(|host| host.ttl)
                    .min()
                    .expect("No host in uc query v4 response body");
                return Ok(CacheValue {
                    cached_response_body: response_body,
                    cache_lifetime: Duration::from_secs(min_ttl),
                    cached_at: Instant::now(),
                });
            }
            Err(err) => {
                error = Some(err);
            }
        }
    }
    Err(error.expect("No tries error"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::oneshot::channel;
    use serde::Serialize;
    use serde_json::json;
    use std::{
        boxed::Box,
        error::Error,
        result::Result,
        sync::{
            atomic::{AtomicUsize, Ordering::Relaxed},
            Arc,
        },
        thread::sleep,
    };
    use tokio::task::{spawn, spawn_blocking};
    use warp::{path, reply::Response, Filter};

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

    #[derive(Deserialize, Serialize)]
    struct UcQueryParams {
        ak: String,
        bucket: String,
    }

    #[tokio::test]
    async fn test_uc_query_v4() -> Result<(), Box<dyn Error>> {
        CACHE_MAP.clear();

        const ACCESS_KEY: &str = "0123456789001234567890";
        const BUCKET_NAME: &str = "test-bucket";

        let routes = path!("v4" / "query")
            .and(warp::query::<UcQueryParams>())
            .map(|params: UcQueryParams| {
                assert_eq!(&params.ak, ACCESS_KEY);
                assert_eq!(&params.bucket, BUCKET_NAME);
                Response::new(
                    json!({
                        "hosts": [{
                            "region": "z0",
                            "ttl":10,
                            "io": {
                              "domains": [
                                "iovip.qbox.me"
                              ]
                            }
                        }]
                    })
                    .to_string()
                    .into(),
                )
            });
        starts_with_server!(addr, routes, {
            spawn_blocking(move || -> IOResult<()> {
                let io_urls = query_for_io_urls(
                    ACCESS_KEY,
                    BUCKET_NAME,
                    &["http://".to_owned() + &addr.to_string()],
                    false,
                )?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                Ok(())
            })
            .await??;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_uc_query_v4_with_cache() -> Result<(), Box<dyn Error>> {
        CACHE_MAP.clear();

        const ACCESS_KEY: &str = "0123456789001234567890";
        const BUCKET_NAME: &str = "test-bucket";
        let counter = Arc::new(AtomicUsize::new(0));

        let routes = {
            let counter = counter.to_owned();
            path!("v4" / "query")
                .and(warp::query::<UcQueryParams>())
                .map(move |params: UcQueryParams| {
                    counter.fetch_add(1, Relaxed);
                    assert_eq!(&params.ak, ACCESS_KEY);
                    assert_eq!(&params.bucket, BUCKET_NAME);
                    Response::new(
                        json!({
                            "hosts": [{
                                "region": "z0",
                                "ttl":1,
                                "io": {
                                  "domains": [
                                    "iovip.qbox.me"
                                  ]
                                }
                            }]
                        })
                        .to_string()
                        .into(),
                    )
                })
        };
        starts_with_server!(addr, routes, {
            spawn_blocking(move || -> IOResult<()> {
                let mut io_urls = query_for_io_urls(
                    ACCESS_KEY,
                    BUCKET_NAME,
                    &["http://".to_owned() + &addr.to_string()],
                    false,
                )?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(counter.load(Relaxed), 1);

                io_urls = query_for_io_urls(
                    ACCESS_KEY,
                    BUCKET_NAME,
                    &["http://".to_owned() + &addr.to_string()],
                    false,
                )?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(counter.load(Relaxed), 1);

                sleep(Duration::from_secs(1));

                io_urls = query_for_io_urls(
                    ACCESS_KEY,
                    BUCKET_NAME,
                    &["http://".to_owned() + &addr.to_string()],
                    false,
                )?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(counter.load(Relaxed), 1);

                sleep(Duration::from_secs(1));
                assert_eq!(counter.load(Relaxed), 2);

                Ok(())
            })
            .await??;
        });
        Ok(())
    }
}
