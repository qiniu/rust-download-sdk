use super::SingleClusterConfig;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use reqwest::blocking::Client as HTTPClient;
use std::{collections::HashSet, sync::Arc, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Timeouts {
    base_timeout: Duration,
    dial_timeout: Duration,
}

type Key = Timeouts;
type Value = Arc<HTTPClient>;

static HTTP_CLIENTS: Lazy<DashMap<Key, Value>> = Lazy::new(Default::default);

impl Timeouts {
    #[inline]
    #[cfg(test)]
    pub(crate) fn default_http_client() -> Arc<HTTPClient> {
        Self::new(None, None).http_client()
    }

    #[inline]
    pub(crate) fn new(base_timeout: Option<Duration>, dial_timeout: Option<Duration>) -> Self {
        Self {
            base_timeout: base_timeout
                .filter(|&value| value > Duration::from_millis(0))
                .unwrap_or_else(|| Duration::from_millis(3000)),
            dial_timeout: dial_timeout
                .filter(|&value| value > Duration::from_millis(0))
                .unwrap_or_else(|| Duration::from_millis(50)),
        }
    }

    #[inline]
    pub(crate) fn http_client(&self) -> Arc<HTTPClient> {
        return HTTP_CLIENTS
            .entry(self.to_owned())
            .or_insert_with(|| build_http_client(self))
            .to_owned();

        fn build_http_client(timeouts: &Timeouts) -> Arc<HTTPClient> {
            const USER_AGENT: &str = concat!("QiniuRustDownload/", env!("CARGO_PKG_VERSION"));
            Arc::new(
                HTTPClient::builder()
                    .user_agent(USER_AGENT)
                    .connect_timeout(timeouts.dial_timeout)
                    .timeout(timeouts.base_timeout)
                    .pool_max_idle_per_host(5)
                    .connection_verbose(true)
                    .build()
                    .expect("Failed to build Reqwest Client"),
            )
        }
    }
}

impl<'a> From<&'a SingleClusterConfig> for Timeouts {
    #[inline]
    fn from(config: &'a SingleClusterConfig) -> Self {
        Self::new(config.base_timeout(), config.connect_timeout())
    }
}

#[inline]
pub(super) fn ensure_http_clients(set: &HashSet<Timeouts>) {
    HTTP_CLIENTS.retain(|key, _| set.contains(key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_client() {
        env_logger::try_init().ok();

        let c1 =
            Timeouts::new(Some(Duration::from_secs(1)), Some(Duration::from_secs(1))).http_client();
        let c2 =
            Timeouts::new(Some(Duration::from_secs(1)), Some(Duration::from_secs(1))).http_client();
        let c3 =
            Timeouts::new(Some(Duration::from_secs(1)), Some(Duration::from_secs(2))).http_client();
        let c4 =
            Timeouts::new(Some(Duration::from_secs(2)), Some(Duration::from_secs(1))).http_client();
        let c5 =
            Timeouts::new(Some(Duration::from_secs(2)), Some(Duration::from_secs(2))).http_client();

        assert_eq!(3, Arc::strong_count(&c1));
        assert_eq!(0, Arc::weak_count(&c1));
        assert_eq!(3, Arc::strong_count(&c2));
        assert_eq!(0, Arc::weak_count(&c2));
        assert_eq!(2, Arc::strong_count(&c3));
        assert_eq!(0, Arc::weak_count(&c3));
        assert_eq!(2, Arc::strong_count(&c4));
        assert_eq!(0, Arc::weak_count(&c4));
        assert_eq!(2, Arc::strong_count(&c5));
        assert_eq!(0, Arc::weak_count(&c5));
    }
}
