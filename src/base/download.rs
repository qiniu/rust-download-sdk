use super::credential::Credential;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct RangeReaderBuilder {
    pub(crate) credential: Credential,
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) io_urls: Vec<String>,
    pub(crate) uc_urls: Vec<String>,
    pub(crate) monitor_urls: Vec<String>,
    pub(crate) io_tries: usize,
    pub(crate) uc_tries: usize,
    pub(crate) update_interval: Option<Duration>,
    pub(crate) punish_duration: Option<Duration>,
    pub(crate) base_timeout: Option<Duration>,
    pub(crate) dial_timeout: Option<Duration>,
    pub(crate) max_punished_times: Option<usize>,
    pub(crate) max_punished_hosts_percent: Option<u8>,
    pub(crate) use_getfile_api: bool,
    pub(crate) normalize_key: bool,
    pub(crate) private_url_lifetime: Option<Duration>,
    pub(crate) use_https: bool,
    pub(crate) dot_tries: Option<usize>,
    pub(crate) dot_interval: Option<Duration>,
    pub(crate) max_dot_buffer_size: Option<u64>,
    pub(crate) max_retry_concurrency: Option<usize>,
}

impl RangeReaderBuilder {
    pub(crate) fn new(
        bucket: String,
        key: String,
        credential: Credential,
        io_urls: Vec<String>,
    ) -> Self {
        RangeReaderBuilder {
            bucket,
            key,
            credential,
            io_urls,
            uc_urls: vec![],
            monitor_urls: vec![],
            io_tries: 10,
            uc_tries: 10,
            update_interval: None,
            punish_duration: None,
            base_timeout: None,
            dial_timeout: None,
            max_punished_times: None,
            max_punished_hosts_percent: None,
            use_getfile_api: true,
            normalize_key: false,
            private_url_lifetime: None,
            use_https: false,
            dot_tries: None,
            dot_interval: None,
            max_dot_buffer_size: None,
            max_retry_concurrency: None,
        }
    }

    pub(crate) fn uc_urls(mut self, urls: Vec<String>) -> Self {
        self.uc_urls = urls;
        self
    }

    pub(crate) fn monitor_urls(mut self, urls: Vec<String>) -> Self {
        self.monitor_urls = urls;
        self
    }

    pub(crate) fn io_tries(mut self, tries: usize) -> Self {
        self.io_tries = tries;
        self
    }

    pub(crate) fn uc_tries(mut self, tries: usize) -> Self {
        self.uc_tries = tries;
        self
    }

    pub(crate) fn dot_tries(mut self, tries: usize) -> Self {
        self.dot_tries = Some(tries);
        self
    }

    pub(crate) fn update_interval(mut self, interval: Duration) -> Self {
        self.update_interval = Some(interval);
        self
    }

    pub(crate) fn punish_duration(mut self, duration: Duration) -> Self {
        self.punish_duration = Some(duration);
        self
    }

    pub(crate) fn base_timeout(mut self, timeout: Duration) -> Self {
        self.base_timeout = Some(timeout);
        self
    }

    pub(crate) fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.dial_timeout = Some(timeout);
        self
    }

    ///

    pub(crate) fn max_punished_times(mut self, max_times: usize) -> Self {
        self.max_punished_times = Some(max_times);
        self
    }

    ///

    pub(crate) fn max_punished_hosts_percent(mut self, percent: u8) -> Self {
        self.max_punished_hosts_percent = Some(percent);
        self
    }

    pub(crate) fn use_getfile_api(mut self, use_getfile_api: bool) -> Self {
        self.use_getfile_api = use_getfile_api;
        self
    }

    pub(crate) fn normalize_key(mut self, normalize_key: bool) -> Self {
        self.normalize_key = normalize_key;
        self
    }

    pub(crate) fn private_url_lifetime(mut self, private_url_lifetime: Option<Duration>) -> Self {
        self.private_url_lifetime = private_url_lifetime;
        self
    }

    pub(crate) fn dot_interval(mut self, dot_interval: Duration) -> Self {
        self.dot_interval = Some(dot_interval);
        self
    }

    pub(crate) fn max_dot_buffer_size(mut self, max_dot_buffer_size: u64) -> Self {
        self.max_dot_buffer_size = Some(max_dot_buffer_size);
        self
    }

    pub(crate) fn max_retry_concurrency(mut self, max_retry_concurrency: usize) -> Self {
        self.max_retry_concurrency = Some(max_retry_concurrency);
        self
    }

    pub(crate) fn use_https(mut self, use_https: bool) -> Self {
        self.use_https = use_https;
        self
    }
}
