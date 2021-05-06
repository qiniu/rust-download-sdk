use super::{host_selector::HostSelector, HTTP_CLIENT};
use dashmap::DashMap;
use directories::BaseDirs;
use once_cell::sync::Lazy;
use reqwest::StatusCode;
use serde::{
    de::{Error as DeError, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_json::{from_reader as json_from_reader, to_writer as json_to_writer};
use std::{
    collections::HashMap,
    env::temp_dir,
    fmt,
    fs::{create_dir_all, OpenOptions},
    io::{Error as IOError, ErrorKind as IOErrorKind, Result as IOResult},
    path::PathBuf,
    result::Result,
    sync::Mutex,
    thread::spawn,
    time::{Duration, SystemTime},
};
use tap::prelude::*;
use url::Url;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct CacheKey {
    ak: Box<str>,
    bucket: Box<str>,
}

impl CacheKey {
    #[inline]
    fn new(ak: Box<str>, bucket: Box<str>) -> Self {
        Self { ak, bucket }
    }
}

impl Serialize for CacheKey {
    #[inline]
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&format!("{}:{}", self.ak, self.bucket))
    }
}

struct CacheKeyVisitor;

impl<'de> Visitor<'de> for CacheKeyVisitor {
    type Value = CacheKey;

    #[inline]
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Key of cache")
    }

    fn visit_str<E: DeError>(self, value: &str) -> Result<Self::Value, E> {
        let mut iter = value.splitn(2, ':');
        match (iter.next(), iter.next()) {
            (Some(ak), Some(bucket)) => Ok(CacheKey {
                ak: ak.into(),
                bucket: bucket.into(),
            }),
            _ => Err(E::custom(format!("Invalid cache_key: {}", value))),
        }
    }
}

impl<'de> Deserialize<'de> for CacheKey {
    #[inline]
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_str(CacheKeyVisitor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheValue {
    cached_response_body: ResponseBody,
    cache_deadline: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResponseBody {
    hosts: Vec<RegionResponseBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegionResponseBody {
    ttl: u64,
    io: DomainsResponseBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DomainsResponseBody {
    domains: Box<[Box<str>]>,
}

static CACHE_MAP: Lazy<DashMap<CacheKey, CacheValue>> = Lazy::new(Default::default);
static CACHE_DIR: Lazy<PathBuf> = Lazy::new(|| {
    BaseDirs::new()
        .map(|dir| dir.cache_dir().join("qiniu-download"))
        .unwrap_or_else(|| temp_dir().join("qiniu-download"))
});
static CACHE_FILE_LOCK: Lazy<Mutex<()>> = Lazy::new(Default::default);
static CACHE_INIT: Lazy<()> = Lazy::new(|| {
    load_cache().ok();
});

#[derive(Clone)]
pub(super) struct HostsQuerier {
    uc_selector: HostSelector,
    uc_tries: usize,
}

impl HostsQuerier {
    #[inline]
    pub(super) fn new(uc_selector: HostSelector, uc_tries: usize) -> Self {
        Self {
            uc_selector,
            uc_tries,
        }
    }

    pub(super) fn query_for_io_urls(
        &self,
        ak: &str,
        bucket: &str,
        use_https: bool,
    ) -> IOResult<Vec<String>> {
        Lazy::force(&CACHE_INIT);

        let response_body = self.query_for_domains(ak, bucket)?;
        return Ok(response_body
            .hosts
            .first()
            .expect("No host in uc query v4 response body")
            .io
            .domains
            .iter()
            .map(|domain| normalize_domain(domain, use_https))
            .collect());

        fn normalize_domain(domain: &str, use_https: bool) -> String {
            if domain.contains("://") {
                domain.to_string()
            } else if use_https {
                "https://".to_owned() + domain
            } else {
                "http://".to_owned() + domain
            }
        }
    }

    fn query_for_domains(&self, ak: &str, bucket: &str) -> IOResult<ResponseBody> {
        let cache_key = CacheKey::new(ak.into(), bucket.into());

        let mut modified = false;
        let cache_value = CACHE_MAP
            .entry(cache_key.to_owned())
            .or_try_insert_with(|| {
                let result =
                    query_for_domains_without_cache(ak, bucket, &self.uc_selector, self.uc_tries);
                if result.is_ok() {
                    modified = true;
                }
                result
            })?;

        if cache_value.cache_deadline < SystemTime::now() {
            let ak = ak.to_owned();
            let bucket = bucket.to_owned();
            let uc_selector = self.uc_selector.to_owned();
            let uc_tries = self.uc_tries;
            spawn(move || {
                let mut modified = false;
                CACHE_MAP.entry(cache_key).and_modify(|cache_value| {
                    if cache_value.cache_deadline < SystemTime::now() {
                        if let Ok(new_cache_value) =
                            query_for_domains_without_cache(ak, bucket, &uc_selector, uc_tries)
                        {
                            *cache_value = new_cache_value;
                            modified = true;
                        }
                    }
                });
                if modified {
                    let _ = save_cache();
                }
            });
        } else if modified {
            spawn(move || {
                let _ = save_cache();
            });
        }

        Ok(cache_value.cached_response_body.to_owned())
    }
}

fn query_for_domains_without_cache(
    ak: impl AsRef<str>,
    bucket: impl AsRef<str>,
    uc_selector: &HostSelector,
    uc_tries: usize,
) -> IOResult<CacheValue> {
    return query_with_retry(uc_selector, uc_tries, |host, timeout| {
        let url = Url::parse_with_params(
            &format!("{}/v4/query", host),
            &[("ak", ak.as_ref()), ("bucket", bucket.as_ref())],
        )
        .map_err(|err| IOError::new(IOErrorKind::InvalidInput, err))?;

        HTTP_CLIENT
            .read()
            .unwrap()
            .get(&url.to_string())
            .timeout(timeout)
            .send()
            .tap_err(|err| {
                if err.is_timeout() {
                    uc_selector.increase_timeout_power(host);
                }
            })
            .map_err(|err| IOError::new(IOErrorKind::ConnectionAborted, err))
            .and_then(|resp| {
                if resp.status() != StatusCode::OK {
                    Err(IOError::new(
                        IOErrorKind::Other,
                        format!("Unexpected status code {}", resp.status().as_u16()),
                    ))
                } else {
                    resp.json::<ResponseBody>()
                        .map_err(|err| IOError::new(IOErrorKind::BrokenPipe, err))
                }
            })
            .map(|body| {
                let min_ttl = body
                    .hosts
                    .iter()
                    .map(|host| host.ttl)
                    .min()
                    .expect("No host in uc query v4 response body");
                CacheValue {
                    cached_response_body: body,
                    cache_deadline: SystemTime::now() + Duration::from_secs(min_ttl),
                }
            })
    });

    fn query_with_retry<T>(
        uc_selector: &HostSelector,
        tries: usize,
        mut for_each_host: impl FnMut(&str, Duration) -> IOResult<T>,
    ) -> IOResult<T> {
        let mut last_error = None;
        for _ in 0..tries {
            let host_info = uc_selector.select_host();
            match for_each_host(&host_info.host, host_info.timeout) {
                Ok(response) => {
                    uc_selector.reward(&host_info.host);
                    return Ok(response);
                }
                Err(err) => {
                    let punished = uc_selector.punish(&host_info.host, &err);
                    if !punished {
                        return Err(err);
                    }
                    last_error = Some(err);
                }
            }
        }
        Err(last_error.expect("No UC tries error"))
    }
}

fn load_cache() -> IOResult<()> {
    let cache_file_path = CACHE_DIR.join("query-cache.json");
    if let Ok(cache_file) = OpenOptions::new().read(true).open(&cache_file_path) {
        let cache: HashMap<CacheKey, CacheValue> =
            json_from_reader(cache_file).map_err(|err| IOError::new(IOErrorKind::Other, err))?;
        CACHE_MAP.clear();
        for (key, value) in cache.into_iter() {
            CACHE_MAP.insert(key, value);
        }
    }
    Ok(())
}

fn save_cache() -> IOResult<()> {
    let cache_file_path = CACHE_DIR.join("query-cache.json");

    let cache_file_lock_result = CACHE_FILE_LOCK.try_lock();
    if cache_file_lock_result.is_err() {
        return Ok(());
    }

    let mut cache_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&cache_file_path)
        .or_else(|err| {
            if err.kind() == IOErrorKind::NotFound {
                create_dir_all(&*CACHE_DIR)?;
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&cache_file_path)
            } else {
                Err(err)
            }
        })?;
    json_to_writer(&mut cache_file, &*CACHE_MAP)
        .map_err(|err| IOError::new(IOErrorKind::Other, err))?;
    Ok(())
}

#[cfg(test)]
fn clear_cache() -> IOResult<()> {
    let cache_file_path = CACHE_DIR.join("query-cache.json");
    std::fs::remove_file(&cache_file_path)
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
        env_logger::try_init().ok();

        CACHE_MAP.clear();
        let _ = clear_cache();

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
                let host_selector =
                    HostSelector::builder(vec!["http://".to_owned() + &addr.to_string()]).build();
                let io_urls = HostsQuerier::new(host_selector, 1).query_for_io_urls(
                    ACCESS_KEY,
                    BUCKET_NAME,
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
        env_logger::try_init().ok();

        CACHE_MAP.clear();
        let _ = clear_cache();

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
                let host_selector =
                    HostSelector::builder(vec!["http://".to_owned() + &addr.to_string()]).build();
                let hosts_querier = HostsQuerier::new(host_selector, 1);
                let mut io_urls =
                    hosts_querier.query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(counter.load(Relaxed), 1);

                io_urls = hosts_querier.query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(counter.load(Relaxed), 1);

                sleep(Duration::from_secs(1));

                io_urls = hosts_querier.query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(counter.load(Relaxed), 1);

                sleep(Duration::from_secs(1));
                assert_eq!(counter.load(Relaxed), 2);

                CACHE_MAP.clear();
                load_cache().ok();

                io_urls = hosts_querier.query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(counter.load(Relaxed), 2);

                Ok(())
            })
            .await??;
        });
        Ok(())
    }
}
