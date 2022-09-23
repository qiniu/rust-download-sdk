use super::{
    cache_dir_path_of,
    dot::{ApiName, DotType, Dotter},
    host_selector::HostSelector,
};
use dashmap::DashMap;
use log::{info, warn};
use once_cell::sync::Lazy;
use reqwest::{blocking::Client as HTTPClient, StatusCode};
use serde::{
    de::{Error as DeError, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_json::{from_reader as json_from_reader, to_writer as json_to_writer};
use std::{
    collections::HashMap,
    fmt,
    fs::{rename as rename_file, OpenOptions},
    io::{Error as IOError, ErrorKind as IOErrorKind, Result as IOResult},
    path::Path,
    sync::{Arc, Mutex},
    thread::spawn,
    time::{Duration, Instant, SystemTime},
};
use tap::prelude::*;
use url::Url;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct CacheKey {
    ak: Box<str>,
    bucket: Box<str>,
    hosts_crc32: u32,
}

impl CacheKey {
    #[inline]
    fn new(ak: Box<str>, bucket: Box<str>, hosts_crc32: u32) -> Self {
        Self {
            ak,
            bucket,
            hosts_crc32,
        }
    }
}

impl Serialize for CacheKey {
    #[inline]
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&format!(
            "cache-key-v2:{}:{}:{}",
            self.ak, self.bucket, self.hosts_crc32
        ))
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
        if let Some(value) = value.strip_prefix("cache-key-v2:") {
            let mut iter = value.splitn(3, ':');
            match (iter.next(), iter.next(), iter.next()) {
                (Some(ak), Some(bucket), Some(crc32_str)) => Ok(CacheKey {
                    ak: ak.into(),
                    bucket: bucket.into(),
                    hosts_crc32: crc32_str.parse().map_err(|err| {
                        E::custom(format!(
                            "Cannot parse hosts_crc32 from cache_key: {}: {}",
                            value, err
                        ))
                    })?,
                }),
                _ => Err(E::custom(format!("Invalid cache_key: {}", value))),
            }
        } else {
            Err(E::custom(format!(
                "Unrecognized version of cache_key: {}",
                value
            )))
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

impl CacheValue {
    fn new(cached_response_body: ResponseBody, cache_lifetime: Duration) -> Self {
        Self {
            cached_response_body,
            cache_deadline: SystemTime::now() + cache_lifetime,
        }
    }

    fn is_expired(&self) -> bool {
        self.cache_deadline < SystemTime::now()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResponseBody {
    hosts: Vec<RegionResponseBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegionResponseBody {
    ttl: u64,
    io: DomainsResponseBody,
    uc: DomainsResponseBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DomainsResponseBody {
    domains: Box<[Box<str>]>,
}

static CACHE_MAP: Lazy<DashMap<CacheKey, CacheValue>> = Lazy::new(Default::default);
static CACHE_FILE_LOCK: Lazy<Mutex<()>> = Lazy::new(Default::default);
static CACHE_INIT: Lazy<()> = Lazy::new(|| {
    load_cache().ok();
});

#[derive(Clone)]
pub(super) struct HostsQuerier {
    uc_selector: HostSelector,
    uc_tries: usize,
    dotter: Dotter,
    http_client: Arc<HTTPClient>,
}

impl HostsQuerier {
    #[inline]
    pub(super) fn new(
        uc_selector: HostSelector,
        uc_tries: usize,
        dotter: Dotter,
        http_client: Arc<HTTPClient>,
    ) -> Self {
        Self {
            uc_selector,
            uc_tries,
            dotter,
            http_client,
        }
    }

    #[inline]
    pub(super) fn query_for_io_urls(
        &self,
        ak: &str,
        bucket: &str,
        use_https: bool,
    ) -> IOResult<Vec<String>> {
        Lazy::force(&CACHE_INIT);

        Ok(self
            .query_for_domains(ak, bucket)?
            .hosts
            .first()
            .expect("No host in uc query v4 response body")
            .io
            .domains
            .iter()
            .map(|domain| normalize_domain(domain, use_https))
            .collect())
    }

    fn query_for_domains(&self, ak: &str, bucket: &str) -> IOResult<ResponseBody> {
        let cache_key = CacheKey::new(ak.into(), bucket.into(), self.uc_selector.all_hosts_crc32());

        let mut modified = false;
        let cache_value = CACHE_MAP
            .entry(cache_key.to_owned())
            .or_try_insert_with(|| {
                let result = query_for_domains_without_cache(
                    ak,
                    bucket,
                    &self.uc_selector,
                    self.uc_tries,
                    &self.http_client,
                    &self.dotter,
                );
                if result.is_ok() {
                    modified = true;
                }
                result
            })?;

        if cache_value.is_expired() {
            let ak = ak.to_owned();
            let bucket = bucket.to_owned();
            let uc_selector = self.uc_selector.to_owned();
            let http_client = self.http_client.to_owned();
            let dotter = self.dotter.to_owned();
            let uc_tries = self.uc_tries;
            spawn(move || {
                let mut modified = false;
                CACHE_MAP.entry(cache_key).and_modify(|cache_value| {
                    if cache_value.is_expired() {
                        if let Ok(new_cache_value) = query_for_domains_without_cache(
                            ak,
                            bucket,
                            &uc_selector,
                            uc_tries,
                            &http_client,
                            &dotter,
                        ) {
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
            spawn(save_cache);
        }

        Ok(cache_value.cached_response_body.to_owned())
    }
}

fn query_for_domains_without_cache(
    ak: impl AsRef<str>,
    bucket: impl AsRef<str>,
    uc_selector: &HostSelector,
    uc_tries: usize,
    http_client: &HTTPClient,
    dotter: &Dotter,
) -> IOResult<CacheValue> {
    return query_with_retry(
        uc_selector,
        uc_tries,
        dotter,
        |host, timeout_power, timeout| {
            info!(
                "try to query hosts from {}, ak = {}, bucket = {}",
                host,
                ak.as_ref(),
                bucket.as_ref()
            );
            let use_https = parse_url_protocol(host).unwrap_or(false);

            let url = Url::parse_with_params(
                &format!("{}/v4/query", host),
                &[("ak", ak.as_ref()), ("bucket", bucket.as_ref())],
            )
            .map_err(|err| IOError::new(IOErrorKind::InvalidInput, err))
            .tap_err(|_| {
                warn!("uc host {} is invalid", host);
            })?;

            http_client
                .get(&url.to_string())
                .timeout(timeout)
                .send()
                .tap_err(|err| {
                    if err.is_timeout() {
                        uc_selector.increase_timeout_power_by(host, timeout_power);
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
                        let body = uc_selector.wrap_reader(resp, host, timeout_power);
                        serde_json::from_reader::<_, ResponseBody>(body)
                            .map_err(|err| IOError::new(IOErrorKind::BrokenPipe, err))
                    }
                })
                .tap_ok(|body| {
                    let uc_hosts: Vec<_> = body
                        .hosts
                        .first()
                        .map(|host| {
                            host.uc
                                .domains
                                .iter()
                                .map(|domain| normalize_domain(domain, use_https))
                                .collect()
                        })
                        .expect("No host in uc query v4 response body");
                    if !uc_hosts.is_empty() {
                        uc_selector.set_hosts(uc_hosts);
                    }
                })
                .map(|body| {
                    let min_ttl = body
                        .hosts
                        .iter()
                        .map(|host| host.ttl)
                        .min()
                        .expect("No host in uc query v4 response body");
                    CacheValue::new(body, Duration::from_secs(min_ttl))
                })
                .tap_ok(|_| {
                    info!(
                        "update query cache for ak = {}, bucket = {} is successful (uc_hosts_crc32 = {})",
                        ak.as_ref(),
                        bucket.as_ref(),
                        uc_selector.all_hosts_crc32(),
                    );
                })
                .tap_err(|err| {
                    warn!(
                        "failed to query hosts from {}, ak = {}, bucket = {}, err = {:?}",
                        host,
                        ak.as_ref(),
                        bucket.as_ref(),
                        err
                    );
                })
        },
    );

    fn query_with_retry<T>(
        uc_selector: &HostSelector,
        tries: usize,
        dotter: &Dotter,
        mut for_each_host: impl FnMut(&str, usize, Duration) -> IOResult<T>,
    ) -> IOResult<T> {
        let mut last_error = None;
        for _ in 0..tries {
            let host_info = uc_selector.select_host();
            let begin_at = Instant::now();
            match for_each_host(&host_info.host, host_info.timeout_power, host_info.timeout) {
                Ok(response) => {
                    uc_selector.reward(&host_info.host);
                    dotter
                        .dot(DotType::Http, ApiName::UcV4Query, true, begin_at.elapsed())
                        .ok();
                    return Ok(response);
                }
                Err(err) => {
                    let punished = uc_selector.punish(&host_info.host, &err, dotter);
                    dotter
                        .dot(DotType::Http, ApiName::UcV4Query, false, begin_at.elapsed())
                        .ok();
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

const CACHE_FILE_NAME: &str = "query-cache.json";
const CACHE_TEMPFILE_NAME: &str = "query-cache.tmp.json";

fn load_cache() -> IOResult<()> {
    let cache_file_path = cache_dir_path_of(CACHE_FILE_NAME)?;
    match OpenOptions::new().read(true).open(&cache_file_path) {
        Ok(cache_file) => {
            let cache: HashMap<CacheKey, CacheValue> = json_from_reader(cache_file)
                .tap_err(|err| {
                    warn!(
                        "Failed to parse cache from cache file {:?}: {}",
                        cache_file_path, err
                    )
                })
                .map_err(|err| IOError::new(IOErrorKind::Other, err))?;
            CACHE_MAP.clear();
            for (key, value) in cache.into_iter() {
                if !value.is_expired() {
                    CACHE_MAP.insert(key, value);
                }
            }
        }
        Err(err) => {
            info!(
                "Cache file is failed to open {:?}: {}",
                cache_file_path, err
            );
        }
    }
    Ok(())
}

fn save_cache() -> IOResult<()> {
    let cache_file_path = cache_dir_path_of(CACHE_FILE_NAME)?;
    let cache_tempfile_path = cache_dir_path_of(CACHE_TEMPFILE_NAME)?;
    let cache_file_lock_result = CACHE_FILE_LOCK.try_lock();
    if cache_file_lock_result.is_err() {
        info!(
            "Cache file is locked, cannot save to {:?} now",
            cache_file_path
        );
        return Ok(());
    }
    if let Err(err) = _save_cache(&cache_tempfile_path) {
        warn!("Failed to save cache {:?}: {}", cache_tempfile_path, err);
    } else {
        info!("Save cache to {:?} successfully", cache_tempfile_path);
        if let Err(err) = rename_file(&cache_tempfile_path, &cache_file_path) {
            warn!(
                "Failed to move cache file from {:?} to {:?}: {}",
                cache_tempfile_path, cache_file_path, err
            );
        } else {
            info!(
                "Move cache from {:?} to {:?} successfully",
                cache_tempfile_path, cache_file_path
            );
        }
    }
    return Ok(());

    #[inline]
    fn _save_cache(cache_file_path: &Path) -> anyhow::Result<()> {
        let mut cache_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(cache_file_path)?;
        let to_save: HashMap<_, _> = CACHE_MAP
            .iter()
            .filter_map(|cache_entry| {
                if cache_entry.is_expired() {
                    None
                } else {
                    Some((cache_entry.key().to_owned(), cache_entry.value().to_owned()))
                }
            })
            .collect();
        json_to_writer(&mut cache_file, &to_save)
            .map_err(|err| IOError::new(IOErrorKind::Other, err))?;
        Ok(())
    }
}

#[inline]
fn normalize_domain(domain: &str, use_https: bool) -> String {
    if domain.contains("://") {
        domain.to_string()
    } else if use_https {
        "https://".to_owned() + domain
    } else {
        "http://".to_owned() + domain
    }
}

#[inline]
fn parse_url_protocol(url: &str) -> Option<bool> {
    if url.starts_with("https://") {
        Some(true)
    } else if url.starts_with("http://") {
        Some(false)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            base::credential::Credential,
            config::Timeouts,
            dot::{DotRecordKey, DotRecords, DotRecordsDashMap, DOT_FILE_NAME},
        },
        *,
    };
    use futures::{channel::oneshot::channel, future::join};
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
    use warp::{
        http::header::{HeaderValue, AUTHORIZATION},
        hyper::Body,
        path,
        reply::Response,
        Filter,
    };

    macro_rules! starts_with_server {
        ($uc_addr:ident, $uc_routes:ident, $code:block) => {{
            let (tx, rx) = channel();
            let ($uc_addr, server) = warp::serve($uc_routes).bind_with_graceful_shutdown(
                ([127, 0, 0, 1], 0),
                async move {
                    rx.await.ok();
                },
            );
            let handler = spawn(server);
            $code;
            tx.send(()).ok();
            handler.await.ok();
        }};
        ($uc_addr:ident, $monitor_addr:ident, $uc_routes:ident, $monitor_routes:ident, $code:block) => {{
            let (uc_tx, uc_rx) = channel();
            let (monitor_tx, monitor_rx) = channel();
            let ($uc_addr, uc_server) = warp::serve($uc_routes).bind_with_graceful_shutdown(
                ([127, 0, 0, 1], 0),
                async move {
                    uc_rx.await.ok();
                },
            );
            let ($monitor_addr, monitor_server) = warp::serve($monitor_routes)
                .bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async move {
                    monitor_rx.await.ok();
                });
            let uc_handler = spawn(uc_server);
            let monitor_handler = spawn(monitor_server);
            $code;
            uc_tx.send(()).ok();
            monitor_tx.send(()).ok();
            let _ = join(uc_handler, monitor_handler).await;
        }};
    }

    #[derive(Deserialize, Serialize)]
    struct UcQueryParams {
        ak: String,
        bucket: String,
    }

    const ACCESS_KEY: &str = "0123456789001234567890";
    const SECRET_KEY: &str = "abcdefghijklmnioqrstuv";
    const BUCKET_NAME: &str = "test-bucket";

    #[inline]
    fn get_credential() -> Credential {
        Credential::new(ACCESS_KEY, SECRET_KEY)
    }

    #[tokio::test]
    async fn test_uc_query_v4() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        CACHE_MAP.clear();
        clear_cache()?;

        let uc_routes = path!("v4" / "query")
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
                            },
                            "uc": {
                              "domains": [
                                "uc.qbox.me"
                              ]
                            }
                        }]
                    })
                    .to_string()
                    .into(),
                )
            });

        let monitor_called = Arc::new(AtomicUsize::new(0));
        let monitor_routes = {
            let monitor_called = monitor_called.to_owned();
            path!("v1" / "stat")
                .and(warp::header::value(AUTHORIZATION.as_str()))
                .and(warp::body::json())
                .map(move |authorization: HeaderValue, records: DotRecords| {
                    monitor_called.fetch_add(1, Relaxed);
                    assert!(authorization.to_str().unwrap().starts_with("UpToken "));
                    assert_eq!(records.records().len(), 1);
                    let record = records.records().first().unwrap();
                    assert_eq!(record.dot_type(), Some(DotType::Http));
                    assert_eq!(record.api_name(), Some(ApiName::UcV4Query));
                    assert_eq!(record.success_count(), Some(1));
                    assert_eq!(record.failed_count(), Some(0));
                    Response::new(Body::empty())
                })
        };
        starts_with_server!(uc_addr, monitor_addr, uc_routes, monitor_routes, {
            spawn_blocking(move || -> IOResult<()> {
                let dotter = Dotter::new(
                    Timeouts::default_http_client(),
                    get_credential(),
                    BUCKET_NAME.to_owned(),
                    vec!["http://".to_owned() + &monitor_addr.to_string()],
                    Some(Duration::from_millis(0)),
                    Some(1),
                    None,
                    None,
                    None,
                    None,
                    None,
                );
                let host_selector =
                    HostSelector::builder(vec!["http://".to_owned() + &uc_addr.to_string()])
                        .build();
                let querier =
                    HostsQuerier::new(host_selector, 1, dotter, Timeouts::default_http_client());
                let io_urls = querier.query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)?;
                assert_eq!(&io_urls, &["http://iovip.qbox.me".to_owned()]);
                assert_eq!(
                    &querier.uc_selector.hosts(),
                    &["http://uc.qbox.me".to_owned()]
                );
                assert_eq!(&querier.uc_selector.select_host().host, "http://uc.qbox.me");
                sleep(Duration::from_secs(5));
                assert_eq!(monitor_called.load(Relaxed), 1);
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
        clear_cache()?;

        let uc_called = Arc::new(AtomicUsize::new(0));
        let records_map = Arc::new(DotRecordsDashMap::default());

        let uc_routes = {
            let uc_called = uc_called.to_owned();
            path!("v4" / "query")
                .and(warp::query::<UcQueryParams>())
                .map(move |params: UcQueryParams| {
                    uc_called.fetch_add(1, Relaxed);
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
                                },
                                "uc": {
                                  "domains": []
                                }
                            }]
                        })
                        .to_string()
                        .into(),
                    )
                })
        };
        let monitor_routes = {
            let records_map = records_map.to_owned();
            path!("v1" / "stat")
                .and(warp::header::value(AUTHORIZATION.as_str()))
                .and(warp::body::json())
                .map(move |authorization: HeaderValue, records: DotRecords| {
                    assert!(authorization.to_str().unwrap().starts_with("UpToken "));
                    records_map.merge_with_records(records);
                    Response::new(Body::empty())
                })
        };

        starts_with_server!(uc_addr, monitor_addr, uc_routes, monitor_routes, {
            spawn_blocking(move || -> IOResult<()> {
                let dotter = Dotter::new(
                    Timeouts::default_http_client(),
                    get_credential(),
                    BUCKET_NAME.to_owned(),
                    vec!["http://".to_owned() + &monitor_addr.to_string()],
                    Some(Duration::from_millis(0)),
                    Some(1),
                    None,
                    None,
                    None,
                    None,
                    None,
                );
                let host_selector =
                    HostSelector::builder(vec!["http://".to_owned() + &uc_addr.to_string()])
                        .build();
                let hosts_querier =
                    HostsQuerier::new(host_selector, 1, dotter, Timeouts::default_http_client());
                let mut io_urls =
                    hosts_querier.query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(uc_called.load(Relaxed), 1);

                io_urls = hosts_querier.query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(uc_called.load(Relaxed), 1);

                sleep(Duration::from_secs(1));

                io_urls = hosts_querier.query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(uc_called.load(Relaxed), 1);

                sleep(Duration::from_secs(1));
                assert_eq!(uc_called.load(Relaxed), 2);

                CACHE_MAP.clear();
                load_cache().ok();

                io_urls = hosts_querier.query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(uc_called.load(Relaxed), 2);

                sleep(Duration::from_secs(5));
                {
                    let record = records_map
                        .get(&DotRecordKey::new(DotType::Http, ApiName::UcV4Query))
                        .unwrap();
                    assert_eq!(record.success_count(), Some(2));
                    assert_eq!(record.failed_count(), Some(0));
                }

                Ok(())
            })
            .await??;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_uc_query_v4_with_dynamic_uc_addr() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        CACHE_MAP.clear();
        clear_cache()?;

        let uc_called = Arc::new(AtomicUsize::new(0));
        let uc_routes = {
            let uc_called = uc_called.to_owned();
            path!("v4" / "query")
                .and(warp::query::<UcQueryParams>())
                .map(move |params: UcQueryParams| {
                    uc_called.fetch_add(1, Relaxed);
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
                                },
                                "uc": {
                                  "domains": ["uc.qbox.me"]
                                }
                            }]
                        })
                        .to_string()
                        .into(),
                    )
                })
        };
        starts_with_server!(uc_addr, uc_routes, {
            spawn_blocking(move || -> IOResult<()> {
                let dotter = Dotter::default();
                let host_selector =
                    HostSelector::builder(vec!["http://".to_owned() + &uc_addr.to_string()])
                        .build();
                let hosts_querier =
                    HostsQuerier::new(host_selector, 1, dotter, Timeouts::default_http_client());
                let io_urls =
                    hosts_querier.query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)?;
                assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
                assert_eq!(uc_called.load(Relaxed), 1);

                assert_eq!(CACHE_MAP.len(), 1);

                sleep(Duration::from_secs(2));

                CACHE_MAP.clear();
                load_cache()?;

                assert!(CACHE_MAP.is_empty());

                Ok(())
            })
            .await??;
        });
        Ok(())
    }

    fn clear_cache() -> IOResult<()> {
        let cache_file_path = cache_dir_path_of(CACHE_FILE_NAME)?;
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
