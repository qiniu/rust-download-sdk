use dashmap::DashMap;
use rand::{seq::SliceRandom, thread_rng};
use std::{
    collections::HashSet,
    fmt::{Debug, Formatter, Result as FormatResult},
    io::{Error as IOError, Result as IOResult},
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc, RwLock,
    },
    thread::{sleep, Builder as ThreadBuilder},
    time::{Duration, SystemTime},
};

#[derive(Debug)]
struct PunishedInfo {
    last_punished_at: SystemTime,
    continuous_punished_times: usize,
    timeout_power: usize,
}

impl Default for PunishedInfo {
    fn default() -> Self {
        Self {
            last_punished_at: SystemTime::UNIX_EPOCH,
            continuous_punished_times: 0,
            timeout_power: 0,
        }
    }
}

type UpdateFn = Box<dyn Fn() -> IOResult<Vec<String>> + Sync + Send + 'static>;

struct HostsUpdater {
    hosts: RwLock<Vec<String>>,
    hosts_map: DashMap<String, PunishedInfo>,
    update_func: Option<UpdateFn>,
    index: AtomicUsize,
}

impl HostsUpdater {
    fn new(hosts: Vec<String>, update_func: Option<UpdateFn>) -> Arc<Self> {
        Arc::new(Self {
            hosts_map: hosts
                .iter()
                .map(|host| (host.to_owned(), Default::default()))
                .collect(),
            hosts: RwLock::new(hosts),
            update_func,
            index: AtomicUsize::new(0),
        })
    }

    fn start_auto_update(updater: &Arc<HostsUpdater>, update_interval: Duration) {
        let updater = Arc::downgrade(updater);
        ThreadBuilder::new()
            .name("host-selector-auto-updater".into())
            .spawn(move || loop {
                sleep(update_interval);
                if let Some(updater) = updater.upgrade() {
                    updater.update_hosts();
                } else {
                    break;
                }
            })
            .unwrap();
    }

    fn set_hosts(&self, mut hosts: Vec<String>) {
        let mut new_hosts_set = HashSet::with_capacity(hosts.len());
        for host in hosts.iter() {
            new_hosts_set.insert(host.to_owned());
            self.hosts_map.entry(host.to_owned()).or_default();
        }
        self.hosts_map
            .retain(|host, _| new_hosts_set.contains(host));
        hosts.shuffle(&mut thread_rng());
        *self.hosts.write().unwrap() = hosts;
    }

    fn update_hosts(&self) {
        if let Some(update_func) = &self.update_func {
            let new_hosts = update_func();
            if let Ok(new_hosts) = new_hosts {
                if !new_hosts.is_empty() {
                    self.set_hosts(new_hosts);
                }
            }
        }
    }

    #[inline]
    fn next_index(&self) -> usize {
        self.index.fetch_add(1, Relaxed)
    }
}

impl Debug for HostsUpdater {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        f.debug_struct("HostsUpdater")
            .field("hosts_map", &self.hosts_map)
            .finish()
    }
}

type ShouldPunishFn = Box<dyn Fn(&IOError) -> bool + Send + Sync + 'static>;
struct HostPunisher {
    should_punish_func: Option<ShouldPunishFn>,
    punish_duration: Duration,
    base_timeout: Duration,
    max_punished_times: usize,
    max_punished_hosts_percent: u8,
}

impl HostPunisher {
    #[inline]
    fn max_seek_times(&self, hosts_count: usize) -> usize {
        hosts_count * usize::from(self.max_punished_hosts_percent) / 100
    }

    #[inline]
    fn is_available(&self, punished_info: &PunishedInfo) -> bool {
        punished_info.continuous_punished_times <= self.max_punished_times
    }

    #[inline]
    fn is_punishment_expired(&self, punished_info: &PunishedInfo) -> bool {
        punished_info.last_punished_at + self.punish_duration < SystemTime::now()
    }

    #[inline]
    fn timeout(&self, punished_info: &PunishedInfo) -> Duration {
        self.base_timeout * (1 << punished_info.timeout_power)
    }

    #[inline]
    fn should_punish(&self, error: &IOError) -> bool {
        if let Some(should_punish_func) = &self.should_punish_func {
            should_punish_func(error)
        } else {
            true
        }
    }
}

impl Debug for HostPunisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        f.debug_struct("HostPunisher")
            .field("should_punish", &self.should_punish_func.is_some())
            .field("punish_duration", &self.punish_duration)
            .field("base_timeout", &self.base_timeout)
            .field("max_punished_times", &self.max_punished_times)
            .field(
                "max_punished_hosts_percent",
                &self.max_punished_hosts_percent,
            )
            .finish()
    }
}

#[derive(Debug, Clone)]
pub(super) struct HostSelector {
    hosts_updater: Arc<HostsUpdater>,
    host_punisher: Arc<HostPunisher>,
}

pub(super) struct HostSelectorBuilder {
    hosts: Vec<String>,
    update_func: Option<UpdateFn>,
    should_punish_func: Option<ShouldPunishFn>,
    update_interval: Duration,
    punish_duration: Duration,
    base_timeout: Duration,
    max_punished_times: usize,
    max_punished_hosts_percent: u8,
}

impl HostSelectorBuilder {
    pub(super) fn new(hosts: Vec<String>) -> Self {
        Self {
            hosts,
            update_func: None,
            should_punish_func: None,
            update_interval: Duration::from_secs(5 * 60),
            punish_duration: Duration::from_secs(30),
            base_timeout: Duration::from_millis(500),
            max_punished_times: 5,
            max_punished_hosts_percent: 50,
        }
    }

    #[inline]
    pub(super) fn update_callback(mut self, update_func: Option<UpdateFn>) -> Self {
        self.update_func = update_func;
        self
    }

    #[inline]
    pub(super) fn should_punish_callback(
        mut self,
        should_punish_func: Option<ShouldPunishFn>,
    ) -> Self {
        self.should_punish_func = should_punish_func;
        self
    }

    #[inline]
    pub(super) fn update_interval(mut self, interval: Duration) -> Self {
        self.update_interval = interval;
        self
    }

    #[inline]
    pub(super) fn punish_duration(mut self, duration: Duration) -> Self {
        self.punish_duration = duration;
        self
    }

    #[inline]
    pub(super) fn base_timeout(mut self, timeout: Duration) -> Self {
        self.base_timeout = timeout;
        self
    }

    #[inline]
    pub(super) fn max_punished_times(mut self, times: usize) -> Self {
        self.max_punished_times = times;
        self
    }

    #[inline]
    pub(super) fn max_punished_hosts_percent(mut self, percent: u8) -> Self {
        self.max_punished_hosts_percent = percent;
        self
    }

    #[inline]
    pub(super) fn build(self) -> HostSelector {
        let auto_update_enabled = self.update_func.is_some();
        let is_hosts_empty = self.hosts.is_empty();
        let hosts_updater = HostsUpdater::new(self.hosts, self.update_func);

        if auto_update_enabled {
            if is_hosts_empty {
                hosts_updater.update_hosts();
            }
            HostsUpdater::start_auto_update(&hosts_updater, self.update_interval);
        }

        HostSelector {
            hosts_updater,
            host_punisher: Arc::new(HostPunisher {
                should_punish_func: self.should_punish_func,
                punish_duration: self.punish_duration,
                base_timeout: self.base_timeout,
                max_punished_times: self.max_punished_times,
                max_punished_hosts_percent: self.max_punished_hosts_percent,
            }),
        }
    }
}

pub(super) struct HostInfo {
    pub(super) host: String,
    pub(super) timeout: Duration,
}

impl HostSelector {
    #[inline]
    pub(super) fn builder(hosts: Vec<String>) -> HostSelectorBuilder {
        HostSelectorBuilder::new(hosts)
    }

    pub(super) fn select_host(&self) -> HostInfo {
        #[derive(Debug)]
        struct CurrentHostInfo<'a> {
            host: &'a str,
            timeout: Duration,
        }
        let mut current_host_info = None;

        let hosts = self.hosts_updater.hosts.read().unwrap();
        for _ in 0..=self.host_punisher.max_seek_times(hosts.len()) {
            let index = self.hosts_updater.next_index();
            let host = hosts[index % hosts.len()].as_str();
            current_host_info = Some(CurrentHostInfo {
                host,
                timeout: self.host_punisher.base_timeout,
            });
            if let Some(punished_info) = self.hosts_updater.hosts_map.get(host) {
                if self.host_punisher.is_punishment_expired(&punished_info) {
                    break;
                }
                current_host_info = Some(CurrentHostInfo {
                    host,
                    timeout: self.host_punisher.timeout(&punished_info),
                });
                if self.host_punisher.is_available(&punished_info) {
                    break;
                }
            }
        }
        current_host_info
            .map(|h| HostInfo {
                host: h.host.to_owned(),
                timeout: h.timeout,
            })
            .unwrap()
    }

    pub(super) fn reward(&self, host: &str) {
        if let Some(mut punished_info) = self.hosts_updater.hosts_map.get_mut(host) {
            *punished_info = Default::default();
        }
    }

    pub(super) fn punish(&self, host: &str, error: &IOError) -> bool {
        if self.host_punisher.should_punish(error) {
            if let Some(mut punished_info) = self.hosts_updater.hosts_map.get_mut(host) {
                punished_info.continuous_punished_times += 1;
                punished_info.last_punished_at = SystemTime::now();
            }
            true
        } else {
            false
        }
    }

    pub(super) fn increase_timeout_power(&self, host: &str) {
        if let Some(mut punished_info) = self.hosts_updater.hosts_map.get_mut(host) {
            punished_info.timeout_power += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind as IOErrorKind, sync::Mutex};

    use super::*;

    #[test]
    fn test_hosts_updater() {
        let hosts_updater = HostsUpdater::new(
            vec![
                "http://host1".to_owned(),
                "http://host2".to_owned(),
                "http://host3".to_owned(),
            ],
            Some(Box::new(|| {
                Ok(vec![
                    "http://host1".to_owned(),
                    "http://host2".to_owned(),
                    "http://host4".to_owned(),
                    "http://host5".to_owned(),
                ])
            })),
        );
        assert_eq!(hosts_updater.hosts.read().unwrap().len(), 3);
        assert_eq!(hosts_updater.hosts_map.len(), 3);
        hosts_updater.update_hosts();
        assert_eq!(hosts_updater.hosts.read().unwrap().len(), 4);
        assert_eq!(hosts_updater.hosts_map.len(), 4);
        assert!(hosts_updater.hosts_map.get("http://host4").is_some());
        assert!(hosts_updater.hosts_map.get("http://host5").is_some());
        assert!(hosts_updater.hosts_map.get("http://host3").is_none());
    }

    #[test]
    fn test_hosts_update() {
        let host_selector = HostSelectorBuilder::new(vec![])
            .update_callback(Some(Box::new(|| {
                Ok(vec![
                    "http://host1".to_owned(),
                    "http://host2".to_owned(),
                    "http://host4".to_owned(),
                    "http://host5".to_owned(),
                ])
            })))
            .build();
        assert!([
            "http://host1".to_owned(),
            "http://host2".to_owned(),
            "http://host4".to_owned(),
            "http://host5".to_owned(),
        ]
        .contains(&host_selector.select_host().host))
    }

    #[test]
    fn test_hosts_updater_auto_update() {
        let hosts_updater = HostsUpdater::new(
            vec![
                "http://host1".to_owned(),
                "http://host2".to_owned(),
                "http://host3".to_owned(),
            ],
            Some(Box::new(|| {
                Ok(vec![
                    "http://host1".to_owned(),
                    "http://host2".to_owned(),
                    "http://host4".to_owned(),
                    "http://host5".to_owned(),
                ])
            })),
        );
        assert_eq!(hosts_updater.hosts.read().unwrap().len(), 3);
        assert_eq!(hosts_updater.hosts_map.len(), 3);
        HostsUpdater::start_auto_update(&hosts_updater, Duration::from_millis(500));
        sleep(Duration::from_millis(800));
        assert_eq!(hosts_updater.hosts.read().unwrap().len(), 4);
        assert_eq!(hosts_updater.hosts_map.len(), 4);
        assert!(hosts_updater.hosts_map.get("http://host4").is_some());
        assert!(hosts_updater.hosts_map.get("http://host5").is_some());
        assert!(hosts_updater.hosts_map.get("http://host3").is_none());
    }

    #[test]
    fn test_hosts_selector() {
        let punished_errs = Arc::new(Mutex::new(Vec::new()));
        {
            let host_selector = HostSelectorBuilder::new(vec![
                "http://host1".to_owned(),
                "http://host2".to_owned(),
                "http://host3".to_owned(),
            ])
            .should_punish_callback(Some({
                let punished_errs = punished_errs.to_owned();
                Box::new(move |error| {
                    punished_errs.lock().unwrap().push(error.to_string());
                    true
                })
            }))
            .punish_duration(Duration::from_millis(500))
            .base_timeout(Duration::from_millis(100))
            .max_punished_times(2)
            .build();
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host1".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            assert_eq!(host_selector.select_host().host, "http://host2".to_owned());
            assert_eq!(host_selector.select_host().host, "http://host3".to_owned());
            assert_eq!(host_selector.select_host().host, "http://host1".to_owned());
            host_selector.increase_timeout_power("http://host1");
            host_selector.punish("http://host1", &IOError::new(IOErrorKind::Other, "error 1"));
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            host_selector.punish("http://host1", &IOError::new(IOErrorKind::Other, "error 2"));
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            host_selector.punish("http://host1", &IOError::new(IOErrorKind::Other, "error 3"));
            assert_eq!(host_selector.select_host().host, "http://host2".to_owned());
            host_selector.punish("http://host2", &IOError::new(IOErrorKind::Other, "error 4"));
            assert_eq!(host_selector.select_host().host, "http://host3".to_owned());
            host_selector.increase_timeout_power("http://host2");
            host_selector.punish("http://host2", &IOError::new(IOErrorKind::Other, "error 5"));
            host_selector.increase_timeout_power("http://host2");
            host_selector.punish("http://host2", &IOError::new(IOErrorKind::Other, "error 6"));
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(400));
            }
            assert_eq!(host_selector.select_host().host, "http://host3".to_owned());
            sleep(Duration::from_millis(500));
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host1".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            host_selector.increase_timeout_power("http://host3");
            host_selector.punish("http://host3", &IOError::new(IOErrorKind::Other, "error 7"));
            host_selector.increase_timeout_power("http://host3");
            host_selector.punish("http://host3", &IOError::new(IOErrorKind::Other, "error 8"));
            host_selector.increase_timeout_power("http://host3");
            host_selector.punish("http://host3", &IOError::new(IOErrorKind::Other, "error 9"));
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host1".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host1".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            host_selector.reward("http://host3");
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            assert_eq!(host_selector.select_host().host, "http://host1".to_owned());
            assert_eq!(host_selector.select_host().host, "http://host2".to_owned());
            assert_eq!(host_selector.select_host().host, "http://host3".to_owned());
            assert_eq!(host_selector.select_host().host, "http://host1".to_owned());
            assert_eq!(host_selector.select_host().host, "http://host2".to_owned());
        }
        assert_eq!(
            Arc::try_unwrap(punished_errs)
                .unwrap()
                .into_inner()
                .unwrap()
                .len(),
            9
        );
    }
}