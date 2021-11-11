use super::dot::Dotter;
use dashmap::DashMap;
use log::{info, warn};
use rand::{seq::SliceRandom, thread_rng};
use reqwest::Error as ReqwestError;
use std::{
    cmp::{min, Ordering},
    collections::HashSet,
    fmt::{Debug, Formatter, Result as FormatResult},
    io::{Error as IOError, ErrorKind as IOErrorKind, Read, Result as IOResult},
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc, Mutex, RwLock,
    },
    thread::Builder as ThreadBuilder,
    time::{Duration, Instant},
};
use tap::prelude::*;

#[derive(Default, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
struct OptionalInstantTime(Option<Instant>);

impl Deref for OptionalInstantTime {
    type Target = Option<Instant>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl OptionalInstantTime {
    #[inline]
    fn now() -> Self {
        Self(Some(Instant::now()))
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
struct PunishedInfo {
    last_punished_at: OptionalInstantTime,
    continuous_punished_times: usize,
    timeout_power: usize,
    failed_to_connect: bool,
}

impl<'a> Ord for PunishedInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.failed_to_connect != other.failed_to_connect {
            return self.failed_to_connect.cmp(&other.failed_to_connect);
        }
        if self.timeout_power != other.timeout_power {
            return self.timeout_power.cmp(&other.timeout_power);
        }
        if self.continuous_punished_times != other.continuous_punished_times {
            return self
                .continuous_punished_times
                .cmp(&other.continuous_punished_times);
        }
        self.last_punished_at.cmp(&other.last_punished_at)
    }
}

impl<'a> PartialOrd for PunishedInfo {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct Candidate<'a> {
    host: &'a str,
    punish_duration: Duration,
    max_punished_times: usize,
    punished_info: PunishedInfo,
}

impl<'a> Eq for Candidate<'a> {}
impl<'a> PartialEq for Candidate<'a> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.punished_info == other.punished_info
            && self.punish_duration == other.punish_duration
            && self.max_punished_times == other.max_punished_times
    }
}

impl<'a> Ord for Candidate<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.is_punishment_expired(), other.is_punishment_expired()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => match (self.is_available(), other.is_available()) {
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                _ => other.punished_info.cmp(&self.punished_info),
            },
        }
    }
}

impl<'a> PartialOrd for Candidate<'a> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Candidate<'a> {
    #[inline]
    fn is_punishment_expired(&self) -> bool {
        if let Some(last_punished_at) = self.punished_info.last_punished_at.as_ref() {
            last_punished_at.elapsed() >= self.punish_duration
        } else {
            true
        }
    }

    #[inline]
    fn is_available(&self) -> bool {
        !self.punished_info.failed_to_connect
            && self.punished_info.continuous_punished_times <= self.max_punished_times
    }
}

type UpdateFn = Box<dyn Fn() -> IOResult<Vec<String>> + Sync + Send + 'static>;

struct HostsUpdater {
    hosts: RwLock<Vec<String>>,
    hosts_map: DashMap<String, PunishedInfo>,
    update_option: Option<UpdateOption>,
    index: AtomicUsize,
    current_timeout_power: AtomicUsize,
}

struct UpdateOption {
    func: UpdateFn,
    interval: Duration,
    last_updated_at: Mutex<Instant>,
}

impl UpdateOption {
    #[inline]
    fn new(func: UpdateFn, interval: Duration) -> Self {
        Self {
            func,
            interval,
            last_updated_at: Mutex::new(Instant::now()),
        }
    }
}

impl HostsUpdater {
    fn new(hosts: Vec<String>, update_option: Option<UpdateOption>) -> Arc<Self> {
        Arc::new(Self {
            hosts_map: hosts
                .iter()
                .map(|host| (host.to_owned(), Default::default()))
                .collect(),
            hosts: RwLock::new(hosts),
            update_option,
            index: AtomicUsize::new(0),
            current_timeout_power: AtomicUsize::new(0),
        })
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

    fn update_hosts(&self) -> bool {
        if let Some(update_option) = &self.update_option {
            if let Ok(new_hosts) = (update_option.func)() {
                if !new_hosts.is_empty() {
                    self.set_hosts(new_hosts);
                    return true;
                }
            }
        }
        false
    }

    #[inline]
    fn next_index(updater: &Arc<HostsUpdater>) -> usize {
        return updater.index.fetch_add(1, Relaxed).tap(|_| {
            try_to_auto_update(updater);
        });

        fn try_to_auto_update(updater: &Arc<HostsUpdater>) {
            if let Some(update_option) = &updater.update_option {
                if let Ok(last_updated_at) = update_option.last_updated_at.try_lock() {
                    if last_updated_at.elapsed() >= update_option.interval {
                        let updater = updater.to_owned();
                        drop(last_updated_at);
                        if let Err(err) = ThreadBuilder::new()
                            .name("host-selector-auto-updater".into())
                            .spawn(move || try_to_auto_update_in_thread(updater))
                        {
                            warn!("failed to start thread `host-selector-auto-updater` to update hosts: {:?}",err);
                        }
                    }
                }
            }
        }

        fn try_to_auto_update_in_thread(updater: Arc<HostsUpdater>) {
            if let Some(update_option) = &updater.update_option {
                if let Ok(mut last_updated_at) = update_option.last_updated_at.lock() {
                    if last_updated_at.elapsed() >= update_option.interval {
                        if updater.update_hosts() {
                            info!("`host-selector-auto-updater` update hosts successfully");
                        };
                        *last_updated_at = Instant::now();
                    }
                }
            }
        }
    }

    #[inline]
    pub(super) fn increase_timeout_power_by(&self, host: &str, mut timeout_power: usize) {
        if let Some(mut punished_info) = self.hosts_map.get_mut(host) {
            timeout_power = timeout_power.saturating_add(1);
            if punished_info.timeout_power < timeout_power {
                punished_info.timeout_power = timeout_power;
                info!(
                    "The timeout_power of host {} increases, now is {}",
                    host, punished_info.timeout_power
                );
            }
            punished_info.last_punished_at = OptionalInstantTime::now();
        }
    }

    #[inline]
    pub(super) fn mark_connection_as_failed(&self, host: &str) {
        if let Some(mut punished_info) = self.hosts_map.get_mut(host) {
            punished_info.failed_to_connect = true;
            punished_info.last_punished_at = OptionalInstantTime::now();
        }
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
        !punished_info.failed_to_connect
            && punished_info.continuous_punished_times <= self.max_punished_times
    }

    #[inline]
    fn is_punishment_expired(&self, punished_info: &PunishedInfo) -> bool {
        if let Some(last_punished_at) = punished_info.last_punished_at.as_ref() {
            last_punished_at.elapsed() >= self.punish_duration
        } else {
            true
        }
    }

    #[inline]
    fn timeout(&self, punished_info: &PunishedInfo) -> Duration {
        min(
            // 超时时长有上限，否则可能超过 tokio 极限
            self.base_timeout * (1 << punished_info.timeout_power),
            Duration::from_secs(600),
        )
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
            update_interval: Duration::from_secs(60),
            punish_duration: Duration::from_secs(30 * 60),
            base_timeout: Duration::from_millis(3000),
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
        let update_interval = self.update_interval;
        let hosts_updater = HostsUpdater::new(
            self.hosts,
            self.update_func
                .map(|f| UpdateOption::new(f, update_interval)),
        );

        if auto_update_enabled && is_hosts_empty {
            hosts_updater.update_hosts();
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

#[derive(Debug, Clone)]
pub(super) struct HostInfo {
    pub(super) host: String,
    pub(super) timeout_power: usize,
    pub(super) timeout: Duration,
}

impl HostSelector {
    #[inline]
    pub(super) fn builder(hosts: Vec<String>) -> HostSelectorBuilder {
        HostSelectorBuilder::new(hosts)
    }

    #[inline]
    pub(super) fn set_hosts(&self, hosts: Vec<String>) {
        self.hosts_updater.set_hosts(hosts)
    }

    #[inline]
    pub(super) fn hosts(&self) -> Vec<String> {
        self.hosts_updater
            .hosts
            .read()
            .unwrap()
            .iter()
            .filter(|&host| {
                self.hosts_updater
                    .hosts_map
                    .get(host)
                    .map(|punished_info| {
                        self.host_punisher.is_punishment_expired(&punished_info)
                            || self.host_punisher.is_available(&punished_info)
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    #[inline]
    pub(super) fn all_hosts_crc32(&self) -> u32 {
        let mut hosts = self.hosts_updater.hosts.read().unwrap().to_owned();
        hosts.sort();
        let mut hasher = crc32fast::Hasher::new();
        hosts.iter().enumerate().for_each(|(i, host)| {
            if i > 0 {
                hasher.update(b"$");
            }
            hasher.update(host.as_bytes());
        });
        hasher.finalize()
    }

    #[inline]
    pub(super) fn update_hosts(&self) -> bool {
        if self.hosts_updater.update_hosts() {
            info!("manual update hosts successfully");
            if let Some(mut last_updated_at) = self
                .hosts_updater
                .update_option
                .as_ref()
                .and_then(|option| option.last_updated_at.lock().ok())
            {
                *last_updated_at = Instant::now();
            }
            true
        } else {
            false
        }
    }

    pub(super) fn select_host(&self) -> HostInfo {
        struct CurrentHostInfo<'a> {
            host: &'a str,
            timeout: Duration,
            timeout_power: usize,
        }
        let mut chosen_host_info = None;

        let hosts = self.hosts_updater.hosts.read().unwrap();
        let max_seek_times = self.host_punisher.max_seek_times(hosts.len());
        let mut candidates = Vec::with_capacity(max_seek_times + 1);
        for _ in 0..=max_seek_times {
            let index = HostsUpdater::next_index(&self.hosts_updater);
            let host = hosts[index % hosts.len()].as_str();
            if let Some(punished_info) = self.hosts_updater.hosts_map.get(host) {
                if self.host_punisher.is_punishment_expired(&punished_info) {
                    info!("host {} is selected directly because there is no punishment or punishment is expired, timeout: {:?}", host,self.host_punisher.base_timeout);
                    chosen_host_info = Some(CurrentHostInfo {
                        host,
                        timeout: self.host_punisher.base_timeout,
                        timeout_power: 0,
                    });
                    break;
                }

                if self.is_satisfied_with(&punished_info) {
                    info!(
                        "host {} is selected, timeout: {:?}, timeout power: {:?}",
                        host,
                        self.host_punisher.timeout(&punished_info),
                        punished_info.timeout_power,
                    );
                    chosen_host_info = Some(CurrentHostInfo {
                        host,
                        timeout: self.host_punisher.timeout(&punished_info),
                        timeout_power: punished_info.timeout_power,
                    });
                    break;
                } else {
                    info!(
                        "host {} is unsatisfied, put it into candidates, timeout: {:?}, timeout power: {:?}",
                        host,
                        self.host_punisher.timeout(&punished_info),
                        punished_info.timeout_power,
                    );
                    candidates.push(Candidate {
                        host,
                        punish_duration: self.host_punisher.punish_duration,
                        max_punished_times: self.host_punisher.max_punished_times,
                        punished_info: punished_info.to_owned(),
                    });
                }
            }
        }
        let chosen_host_info = chosen_host_info.unwrap_or_else(|| {
            candidates
                .into_iter()
                .max()
                .map(|c| CurrentHostInfo {
                    host: c.host,
                    timeout: self.host_punisher.timeout(&c.punished_info),
                    timeout_power: c.punished_info.timeout_power,
                })
                .unwrap()
                .tap(|c| {
                    info!(
                        "candidate_host {} is selected, timeout: {:?}, timeout power: {:?}",
                        c.host, c.timeout, c.timeout_power,
                    );
                })
        });
        self.hosts_updater
            .current_timeout_power
            .store(chosen_host_info.timeout_power, Relaxed);
        HostInfo {
            host: chosen_host_info.host.to_owned(),
            timeout: chosen_host_info.timeout,
            timeout_power: chosen_host_info.timeout_power,
        }
    }

    #[inline]
    pub(super) fn reward(&self, host: &str) {
        if let Some(mut punished_info) = self.hosts_updater.hosts_map.get_mut(host) {
            punished_info.continuous_punished_times = 0;
            punished_info.failed_to_connect = false;
            punished_info.timeout_power = punished_info.timeout_power.saturating_sub(1);
            info!(
                "Reward host {}, now timeout_power is {}",
                host, punished_info.timeout_power
            );
        }
    }

    pub(super) fn punish(&self, host: &str, error: &IOError, dotter: &Dotter) -> bool {
        match self.punish_without_dotter(host, error) {
            PunishResult::NoPunishment => false,
            PunishResult::Punished => true,
            PunishResult::PunishedAndFreezed => {
                dotter.punish().ok();
                true
            }
        }
    }

    pub(super) fn punish_without_dotter(&self, host: &str, error: &IOError) -> PunishResult {
        if self.host_punisher.should_punish(error) {
            if let Some(mut punished_info) = self.hosts_updater.hosts_map.get_mut(host) {
                punished_info.continuous_punished_times += 1;
                punished_info.last_punished_at = OptionalInstantTime::now();
                info!(
                    "Punish host {}, now continuous_punished_times is {}, and timeout_power is {}",
                    host, punished_info.continuous_punished_times, punished_info.timeout_power
                );

                if !self.host_punisher.is_available(&punished_info) {
                    return PunishResult::PunishedAndFreezed;
                }
            }
            PunishResult::Punished
        } else {
            PunishResult::NoPunishment
        }
    }

    #[inline]
    pub(super) fn increase_timeout_power_by(&self, host: &str, timeout_power: usize) {
        self.hosts_updater
            .increase_timeout_power_by(host, timeout_power)
    }

    #[inline]
    pub(super) fn mark_connection_as_failed(&self, host: &str) {
        self.hosts_updater.mark_connection_as_failed(host)
    }

    #[inline]
    pub(super) fn wrap_reader<'a, R: Read>(
        &'a self,
        reader: R,
        host: &'a str,
        timeout_power: usize,
    ) -> ReaderWithTimeoutPower<'a, R> {
        ReaderWithTimeoutPower {
            reader,
            host,
            timeout_power,
            hosts_updater: &self.hosts_updater,
        }
    }

    #[inline]
    fn is_satisfied_with(&self, punished_info: &PunishedInfo) -> bool {
        self.host_punisher.is_available(punished_info)
            && self.hosts_updater.current_timeout_power.load(Relaxed) >= punished_info.timeout_power
    }
}

pub(super) struct ReaderWithTimeoutPower<'a, R: Read> {
    reader: R,
    hosts_updater: &'a HostsUpdater,
    host: &'a str,
    timeout_power: usize,
}

impl<'a, R: Read> Read for ReaderWithTimeoutPower<'a, R> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        match self.reader.read(buf) {
            Ok(have_read) => Ok(have_read),
            Err(err) if err.kind() == IOErrorKind::TimedOut => {
                self.hosts_updater
                    .increase_timeout_power_by(self.host, self.timeout_power);
                Err(err)
            }
            Err(err) => {
                if let Some(inner_err) = err.get_ref() {
                    if let Some(reqwest_err) = inner_err.downcast_ref::<ReqwestError>() {
                        if reqwest_err.is_timeout() {
                            self.hosts_updater
                                .increase_timeout_power_by(self.host, self.timeout_power);
                        }
                    }
                }
                Err(err)
            }
        }
    }
}

pub(super) enum PunishResult {
    NoPunishment,
    Punished,
    PunishedAndFreezed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::blocking::Client;
    use std::{
        error::Error,
        io::{copy as io_copy, sink, ErrorKind as IOErrorKind},
        sync::Mutex,
        thread::sleep,
    };
    use tokio::{spawn, sync::oneshot::channel, task::spawn_blocking, time::sleep as delay_for};
    use warp::{hyper::Body, path, reply::Response, Filter};

    #[test]
    fn test_hosts_updater() {
        env_logger::try_init().ok();

        let hosts_updater = HostsUpdater::new(
            vec![
                "http://host1".to_owned(),
                "http://host2".to_owned(),
                "http://host3".to_owned(),
            ],
            Some(UpdateOption::new(
                Box::new(|| {
                    Ok(vec![
                        "http://host1".to_owned(),
                        "http://host2".to_owned(),
                        "http://host4".to_owned(),
                        "http://host5".to_owned(),
                    ])
                }),
                Duration::from_secs(10),
            )),
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
        env_logger::try_init().ok();

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
        env_logger::try_init().ok();

        let hosts_updater = HostsUpdater::new(
            vec![
                "http://host1".to_owned(),
                "http://host2".to_owned(),
                "http://host3".to_owned(),
            ],
            Some(UpdateOption::new(
                Box::new(|| {
                    Ok(vec![
                        "http://host1".to_owned(),
                        "http://host2".to_owned(),
                        "http://host4".to_owned(),
                        "http://host5".to_owned(),
                    ])
                }),
                Duration::from_millis(500),
            )),
        );
        HostsUpdater::next_index(&hosts_updater);
        assert_eq!(hosts_updater.hosts.read().unwrap().len(), 3);
        assert_eq!(hosts_updater.hosts_map.len(), 3);
        sleep(Duration::from_millis(500));
        HostsUpdater::next_index(&hosts_updater);
        sleep(Duration::from_millis(500));
        assert_eq!(hosts_updater.hosts.read().unwrap().len(), 4);
        assert_eq!(hosts_updater.hosts_map.len(), 4);
        assert!(hosts_updater.hosts_map.get("http://host4").is_some());
        assert!(hosts_updater.hosts_map.get("http://host5").is_some());
        assert!(hosts_updater.hosts_map.get("http://host3").is_none());
    }

    #[test]
    fn test_hosts_selector() {
        env_logger::try_init().ok();

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
            host_selector.increase_timeout_power_by("http://host1", 0);
            host_selector.punish(
                "http://host1",
                &IOError::new(IOErrorKind::Other, "err1"),
                &Default::default(),
            );
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            host_selector.punish(
                "http://host1",
                &IOError::new(IOErrorKind::Other, "err2"),
                &Default::default(),
            );
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            host_selector.increase_timeout_power_by("http://host1", 1);
            host_selector.punish(
                "http://host1",
                &IOError::new(IOErrorKind::Other, "err3"),
                &Default::default(),
            );
            assert_eq!(host_selector.select_host().host, "http://host3".to_owned());
            host_selector.punish(
                "http://host2",
                &IOError::new(IOErrorKind::Other, "err4"),
                &Default::default(),
            );
            assert_eq!(host_selector.select_host().host, "http://host2".to_owned());
            host_selector.increase_timeout_power_by("http://host2", 0);
            host_selector.punish(
                "http://host2",
                &IOError::new(IOErrorKind::Other, "err5"),
                &Default::default(),
            );
            host_selector.increase_timeout_power_by("http://host3", 1);
            host_selector.punish(
                "http://host3",
                &IOError::new(IOErrorKind::Other, "err6"),
                &Default::default(),
            );
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(400));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(200));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(400));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(200));
            }
            host_selector.increase_timeout_power_by("http://host3", 2);
            host_selector.punish(
                "http://host3",
                &IOError::new(IOErrorKind::Other, "err7"),
                &Default::default(),
            );
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(800));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(200));
            }
            host_selector.reward("http://host1");
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host1".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(200));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(200));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host1".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(200));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(200));
            }
            sleep(Duration::from_millis(500));
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
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
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            host_selector.increase_timeout_power_by("http://host3", 2);
            host_selector.punish(
                "http://host3",
                &IOError::new(IOErrorKind::Other, "err8"),
                &Default::default(),
            );
            host_selector.punish(
                "http://host3",
                &IOError::new(IOErrorKind::Other, "err9"),
                &Default::default(),
            );
            host_selector.punish(
                "http://host3",
                &IOError::new(IOErrorKind::Other, "err10"),
                &Default::default(),
            );
            host_selector.increase_timeout_power_by("http://host1", 3);
            host_selector.punish(
                "http://host1",
                &IOError::new(IOErrorKind::Other, "err11"),
                &Default::default(),
            );
            host_selector.punish(
                "http://host1",
                &IOError::new(IOErrorKind::Other, "err12"),
                &Default::default(),
            );
            host_selector.punish(
                "http://host1",
                &IOError::new(IOErrorKind::Other, "err13"),
                &Default::default(),
            );
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(800));
            }
            host_selector.increase_timeout_power_by("http://host3", 3);
            host_selector.punish(
                "http://host3",
                &IOError::new(IOErrorKind::Other, "err14"),
                &Default::default(),
            );
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host1".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(1600));
            }
            host_selector.reward("http://host3");
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(800));
            }
            host_selector.mark_connection_as_failed("http://host2");
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(800));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host1".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(1600));
            }
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host3".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(800));
            }
            host_selector.reward("http://host2");
            {
                let host_info = host_selector.select_host();
                assert_eq!(host_info.host, "http://host2".to_owned());
                assert_eq!(host_info.timeout, Duration::from_millis(100));
            }
        }
        assert_eq!(
            Arc::try_unwrap(punished_errs)
                .unwrap()
                .into_inner()
                .unwrap()
                .len(),
            14
        );
    }

    #[test]
    fn test_read_wrapper() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        let host_selector = HostSelectorBuilder::new(vec!["http://host1".to_owned()])
            .base_timeout(Duration::from_millis(10))
            .build();
        struct AlwaysTimeoutReader;

        impl Read for AlwaysTimeoutReader {
            #[inline]
            fn read(&mut self, _buf: &mut [u8]) -> IOResult<usize> {
                Err(IOError::new(IOErrorKind::TimedOut, "always timed out"))
            }
        }
        io_copy(
            &mut host_selector.wrap_reader(AlwaysTimeoutReader, "http://host1", 0),
            &mut sink(),
        )
        .unwrap_err();
        assert_eq!(host_selector.select_host().timeout_power, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_wrapper_for_reqwest() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

        let host_selector = HostSelectorBuilder::new(vec!["http://host1".to_owned()])
            .base_timeout(Duration::from_millis(10))
            .build();

        let routes = path!("file").map(move || {
            let (mut sender, body) = Body::channel();
            spawn(async move {
                delay_for(Duration::from_secs(1)).await;
                sender.send_data(vec![0u8; 0].into()).await.ok();
            });
            Response::new(body)
        });
        let (tx, rx) = channel();
        let (addr, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async move {
                rx.await.ok();
            });
        let handler = spawn(server);

        spawn_blocking(move || {
            let response = Client::new()
                .get(&format!("http://{}/file", addr))
                .timeout(Duration::from_millis(100))
                .send()
                .unwrap();
            io_copy(
                &mut host_selector.wrap_reader(response, "http://host1", 0),
                &mut sink(),
            )
            .unwrap_err();
            assert_eq!(host_selector.select_host().timeout_power, 1);
        })
        .await?;
        tx.send(()).ok();
        handler.await.ok();

        Ok(())
    }
}
