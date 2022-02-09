use super::{
    cache_dir::cache_dir_path_of,
    dot::{ApiName, DotType, Dotter},
    host_selector::{HostInfo, HostSelector},
};
use atomic_once_cell::AtomicLazy;
use futures::TryFutureExt;
use log::{info, warn};
use reqwest::{Client as HttpClient, StatusCode};
use serde::{
    de::{Error as DeError, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_json::{from_slice as json_from_slice, to_vec as json_to_vec};
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult},
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tap::prelude::*;
use tokio::{
    fs::{rename as rename_file, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    spawn,
    sync::{Mutex, OnceCell, RwLock},
};
use url::Url;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct CacheKey {
    ak: Box<str>,
    bucket: Box<str>,
    hosts_crc32: u32,
}

impl CacheKey {
    fn new(ak: Box<str>, bucket: Box<str>, hosts_crc32: u32) -> Self {
        Self {
            ak,
            bucket,
            hosts_crc32,
        }
    }
}

impl Serialize for CacheKey {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&format!(
            "cache-key-v2:{}:{}:{}",
            self.ak, self.bucket, self.hosts_crc32
        ))
    }
}

type CacheMap = RwLock<HashMap<CacheKey, CacheValue>>;

struct CacheKeyVisitor;

impl<'de> Visitor<'de> for CacheKeyVisitor {
    type Value = CacheKey;

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
    uc: DomainsResponseBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DomainsResponseBody {
    domains: Box<[Box<str>]>,
}

#[derive(Clone)]
pub(super) struct HostsQuerier {
    uc_selector: HostSelector,
    uc_tries: usize,
    dotter: Dotter,
    http_client: Arc<HttpClient>,
}

impl HostsQuerier {
    pub(super) fn new(
        uc_selector: HostSelector,
        uc_tries: usize,
        dotter: Dotter,
        http_client: Arc<HttpClient>,
    ) -> Self {
        Self {
            uc_selector,
            uc_tries,
            dotter,
            http_client,
        }
    }

    pub(super) async fn query_for_io_urls(
        &self,
        ak: &str,
        bucket: &str,
        use_https: bool,
    ) -> IoResult<Vec<String>> {
        let response_body = self.query_for_domains(ak, bucket).await?;
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

    async fn query_for_domains(&self, ak: &str, bucket: &str) -> IoResult<ResponseBody> {
        let cache_key = CacheKey::new(
            ak.into(),
            bucket.into(),
            self.uc_selector.all_hosts_crc32().await,
        );

        let mut modified = false;
        let cache_value = {
            let mut map = cache_map(false).await?.write().await;
            match map.get(&cache_key) {
                Some(cache_value) => cache_value.to_owned(),
                None => query_for_domains_without_cache(
                    ak,
                    bucket,
                    &self.uc_selector,
                    self.uc_tries,
                    &self.http_client,
                    &self.dotter,
                )
                .await?
                .tap(|cache_value| {
                    map.insert(cache_key.to_owned(), cache_value.to_owned());
                    modified = true;
                }),
            }
        };

        if cache_value.cache_deadline < SystemTime::now() {
            let ak = ak.to_owned();
            let bucket = bucket.to_owned();
            let uc_selector = self.uc_selector.to_owned();
            let http_client = self.http_client.to_owned();
            let dotter = self.dotter.to_owned();
            let uc_tries = self.uc_tries;
            spawn(async move {
                let mut modified = false;
                if let Some(cache_value) = cache_map(false).await?.write().await.get_mut(&cache_key)
                {
                    if cache_value.cache_deadline < SystemTime::now() {
                        let new_cache_value = query_for_domains_without_cache(
                            ak,
                            bucket,
                            &uc_selector,
                            uc_tries,
                            &http_client,
                            &dotter,
                        )
                        .await?;
                        *cache_value = new_cache_value;
                        modified = true
                    }
                }
                if modified {
                    save_cache().await?;
                }
                Ok::<_, anyhow::Error>(())
            });
        } else if modified {
            spawn(async move { save_cache().await });
        }

        Ok(cache_value.cached_response_body)
    }
}

async fn query_for_domains_without_cache(
    ak: impl AsRef<str>,
    bucket: impl AsRef<str>,
    uc_selector: &HostSelector,
    uc_tries: usize,
    http_client: &HttpClient,
    dotter: &Dotter,
) -> IoResult<CacheValue> {
    let ak = ak.as_ref();
    let bucket = bucket.as_ref();
    return query_with_retry(uc_selector, uc_tries, dotter, |host_info| async move {
        info!(
            "try to query hosts from {}, ak = {}, bucket = {}",
            host_info.host(),
            &ak,
            &bucket
        );

        let url = Url::parse_with_params(
            &format!("{}/v4/query", host_info.host()),
            &[("ak", &ak), ("bucket", &bucket)],
        )
        .map_err(|err| IoError::new(IoErrorKind::InvalidInput, err))
        .tap_err(|_| {
            warn!("uc host {} is invalid", host_info.host());
        })?;

        let body_result = match http_client
            .get(&url.to_string())
            .timeout(host_info.timeout())
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status() != StatusCode::OK {
                    Err(IoError::new(
                        IoErrorKind::Other,
                        format!("Unexpected status code {}", resp.status().as_u16()),
                    ))
                } else {
                    resp.json::<ResponseBody>()
                        .await
                        .tap_err(|err| {
                            if err.is_timeout() {
                                uc_selector.increase_timeout_power_by(
                                    host_info.host(),
                                    host_info.timeout_power(),
                                );
                            }
                        })
                        .map_err(|err| IoError::new(IoErrorKind::BrokenPipe, err))
                }
            }
            Err(err) => {
                if err.is_timeout() {
                    uc_selector
                        .increase_timeout_power_by(host_info.host(), host_info.timeout_power());
                }
                Err(IoError::new(IoErrorKind::ConnectionAborted, err))
            }
        };

        if let Ok(body) = body_result.as_ref() {
            let uc_hosts: Vec<_> = body
                .hosts
                .first()
                .map(|host| {
                    host.uc
                        .domains
                        .iter()
                        .map(|domain| domain.to_string())
                        .collect()
                })
                .expect("No host in uc query v4 response body");
            if !uc_hosts.is_empty() {
                uc_selector.set_hosts(uc_hosts).await;
            }
        }

        body_result
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
            .tap_ok(|_| {
                info!(
                    "update query cache for ak = {}, bucket = {} is successful",
                    ak, bucket,
                );
            })
            .tap_err(|err| {
                warn!(
                    "failed to query hosts from {}, ak = {}, bucket = {}, err = {:?}",
                    host_info.host(),
                    ak,
                    bucket,
                    err
                );
            })
    })
    .await
    .and_then(|cache_value| {
        cache_value.map(Ok).unwrap_or_else(|| {
            Err(IoError::new(
                IoErrorKind::AddrNotAvailable,
                "HostSelector cannot select any host",
            ))
        })
    });

    async fn query_with_retry<T, F: FnMut(HostInfo) -> Fut, Fut: Future<Output = IoResult<T>>>(
        uc_selector: &HostSelector,
        tries: usize,
        dotter: &Dotter,
        mut for_each_host: F,
    ) -> IoResult<Option<T>> {
        let mut last_error = None;
        for _ in 0..tries {
            let host_info = uc_selector.select_host(&Default::default()).await;
            if let Some(host_info) = host_info {
                let begin_at = Instant::now();
                match for_each_host(host_info.to_owned()).await {
                    Ok(response) => {
                        uc_selector.reward(host_info.host()).await;
                        dotter
                            .dot(DotType::Http, ApiName::UcV4Query, true, begin_at.elapsed())
                            .await
                            .ok();
                        return Ok(Some(response));
                    }
                    Err(err) => {
                        let punished = uc_selector.punish(host_info.host(), &err, dotter).await;
                        dotter
                            .dot(DotType::Http, ApiName::UcV4Query, false, begin_at.elapsed())
                            .await
                            .ok();
                        if !punished {
                            return Err(err);
                        }
                        last_error = Some(err);
                    }
                }
            }
        }
        last_error.map(Err).unwrap_or(Ok(None))
    }
}

static CACHE_FILE_LOCK: AtomicLazy<Mutex<()>> = AtomicLazy::new(Default::default);

pub(super) const CACHE_FILE_NAME: &str = "query-cache.json";
pub(super) const CACHE_TEMPFILE_NAME: &str = "query-cache.tmp.json";

async fn cache_map(force_reload: bool) -> IoResult<&'static CacheMap> {
    static CACHE_INIT: OnceCell<CacheMap> = OnceCell::const_new();

    return CACHE_INIT
        .get_or_try_init(|| async { load_cache_map().await.map(RwLock::new) })
        .and_then(|map| async move {
            if force_reload {
                *map.write().await = load_cache_map().await?;
            }
            Ok(map)
        })
        .await;

    async fn load_cache_map() -> IoResult<HashMap<CacheKey, CacheValue>> {
        let mut cache_map = HashMap::default();
        let cache_file_path = cache_dir_path_of(CACHE_FILE_NAME).await?;
        match OpenOptions::new().read(true).open(&cache_file_path).await {
            Ok(mut cache_file) => {
                let mut cache_content = Vec::new();
                cache_file.read_to_end(&mut cache_content).await?;
                let cache = json_from_slice::<'_, HashMap<CacheKey, CacheValue>>(&cache_content)
                    .tap_err(|err| {
                        warn!(
                            "Failed to parse cache from cache file {:?}: {}",
                            cache_file_path, err
                        )
                    })
                    .map_err(|err| IoError::new(IoErrorKind::Other, err))?;
                for (key, value) in cache.into_iter() {
                    cache_map.insert(key, value);
                }
            }
            Err(err) => {
                info!(
                    "Cache file is failed to open {:?}: {}",
                    cache_file_path, err
                );
            }
        }
        Ok(cache_map)
    }
}

async fn save_cache() -> IoResult<()> {
    let cache_file_path = cache_dir_path_of(CACHE_FILE_NAME).await?;
    let cache_tempfile_path = cache_dir_path_of(CACHE_TEMPFILE_NAME).await?;
    let cache_file_lock_result = CACHE_FILE_LOCK.try_lock();
    if cache_file_lock_result.is_err() {
        info!(
            "Cache file is locked, cannot save to {:?} now",
            cache_file_path
        );
        return Ok(());
    }
    if let Err(err) = _save_cache(&cache_tempfile_path).await {
        warn!("Failed to save cache {:?}: {}", cache_tempfile_path, err);
    } else {
        info!("Save cache to {:?} successfully", cache_tempfile_path);
        if let Err(err) = rename_file(&cache_tempfile_path, &cache_file_path).await {
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

    async fn _save_cache(cache_file_path: &Path) -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(cache_file_path)
            .await?;
        file.write_all(&json_to_vec(&*cache_map(false).await?.read().await)?)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            super::{base::credential::Credential, config::Timeouts},
            dot::{DotRecordKey, DotRecords, DotRecordsDashMap, DOT_FILE_NAME},
        },
        *,
    };
    use futures::{channel::oneshot::channel, future::join};
    use serde::Serialize;
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    };
    use tokio::{fs::remove_file, task::spawn, time::sleep};
    use warp::{
        http::header::{HeaderValue, AUTHORIZATION},
        hyper::Body,
        path,
        reply::Response,
        Filter,
    };

    macro_rules! starts_with_server {
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
            let result: anyhow::Result<()> = $code;
            let _ = result?;
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

    fn get_credential() -> Credential {
        Credential::new(ACCESS_KEY, SECRET_KEY)
    }

    #[tokio::test]
    async fn test_uc_query_v4() -> anyhow::Result<()> {
        env_logger::try_init().ok();

        let _ = cache_map(true).await?;
        clear_cache().await?;

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
            let dotter = Dotter::new(
                Timeouts::default_async_http_client(),
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
            )
            .await;
            let host_selector =
                HostSelector::builder(vec!["http://".to_owned() + &uc_addr.to_string()])
                    .build()
                    .await;
            let querier = HostsQuerier::new(
                host_selector,
                1,
                dotter,
                Timeouts::default_async_http_client(),
            );
            let io_urls = querier
                .query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)
                .await?;
            assert_eq!(&io_urls, &["http://iovip.qbox.me".to_owned()]);
            assert_eq!(
                &querier.uc_selector.hosts().await,
                &["uc.qbox.me".to_owned()]
            );
            assert_eq!(
                querier
                    .uc_selector
                    .select_host(&Default::default())
                    .await
                    .unwrap()
                    .host(),
                "uc.qbox.me"
            );
            sleep(Duration::from_secs(5)).await;
            assert_eq!(monitor_called.load(Relaxed), 1);
            Ok(())
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_uc_query_v4_with_cache() -> anyhow::Result<()> {
        env_logger::try_init().ok();

        let _ = cache_map(true).await?;
        clear_cache().await?;

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
                                "ttl":1u64,
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
            let dotter = Dotter::new(
                Timeouts::default_async_http_client(),
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
            )
            .await;
            let host_selector =
                HostSelector::builder(vec!["http://".to_owned() + &uc_addr.to_string()])
                    .build()
                    .await;
            let hosts_querier = HostsQuerier::new(
                host_selector,
                1,
                dotter,
                Timeouts::default_async_http_client(),
            );
            let mut io_urls = hosts_querier
                .query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)
                .await?;
            assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
            assert_eq!(uc_called.load(Relaxed), 1);

            io_urls = hosts_querier
                .query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)
                .await?;
            assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
            assert_eq!(uc_called.load(Relaxed), 1);

            sleep(Duration::from_secs(1)).await;

            io_urls = hosts_querier
                .query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)
                .await?;
            assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
            assert_eq!(uc_called.load(Relaxed), 1);

            sleep(Duration::from_secs(1)).await;
            assert_eq!(uc_called.load(Relaxed), 2);

            let _ = cache_map(true).await?;

            io_urls = hosts_querier
                .query_for_io_urls(ACCESS_KEY, BUCKET_NAME, false)
                .await?;
            assert_eq!(io_urls, vec!["http://iovip.qbox.me".to_owned()]);
            assert_eq!(uc_called.load(Relaxed), 2);

            sleep(Duration::from_secs(5)).await;
            {
                let record = records_map
                    .get(&DotRecordKey::new(DotType::Http, ApiName::UcV4Query))
                    .unwrap();
                assert_eq!(record.success_count(), Some(2));
                assert_eq!(record.failed_count(), Some(0));
            }

            Ok(())
        });
        Ok(())
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
