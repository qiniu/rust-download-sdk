use dashmap::DashMap;
use rand::{seq::SliceRandom, thread_rng};
use std::{
    collections::HashSet,
    io::{Error as IOError, Result as IOResult},
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc, RwLock,
    },
    thread::{sleep, Builder as ThreadBuilder, JoinHandle},
    time::{Duration, SystemTime},
};

#[derive(Debug)]
struct PunishedInfo {
    last_punished_at: SystemTime,
    continuous_punished_times: usize,
}

impl Default for PunishedInfo {
    fn default() -> Self {
        Self {
            last_punished_at: SystemTime::UNIX_EPOCH,
            continuous_punished_times: 0,
        }
    }
}

struct HostsUpdater<UpdateFn: Fn() -> IOResult<Vec<String>> + Sync + Send + 'static> {
    hosts: RwLock<Vec<String>>,
    hosts_map: DashMap<String, PunishedInfo>,
    update_func: UpdateFn,
    index: AtomicUsize,
}

impl<UpdateFn: Fn() -> IOResult<Vec<String>> + Sync + Send + 'static> HostsUpdater<UpdateFn> {
    fn new(hosts: Vec<String>, update_func: UpdateFn) -> Arc<Self> {
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

    fn start_auto_update(
        updater: &Arc<HostsUpdater<UpdateFn>>,
        update_interval: Duration,
    ) -> JoinHandle<()> {
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
            .unwrap()
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
        let new_hosts = (self.update_func)();
        if let Ok(new_hosts) = new_hosts {
            if !new_hosts.is_empty() {
                self.set_hosts(new_hosts);
            }
        }
    }

    #[inline]
    fn next_index(&self) -> usize {
        self.index.fetch_add(1, Relaxed)
    }
}

/// 域名选择器
///
/// 用于提供 API 请求必要的域名，提供自动更新域名和域名请求失败后的惩罚机制
#[derive(Clone)]
pub struct HostSelector<
    UpdateFn: Fn() -> IOResult<Vec<String>> + Sync + Send + 'static,
    ShouldPunishFn: Fn(&IOError) -> bool,
> {
    hosts_updater: Arc<HostsUpdater<UpdateFn>>,
    should_punish_func: ShouldPunishFn,
    punish_duration: Duration,
    max_punished_times: usize,
    max_punished_hosts_percent: u8,
    _update_thread: Arc<JoinHandle<()>>,
}

/// 域名选择构建器
pub struct HostSelectorBuilder<
    UpdateFn: Fn() -> IOResult<Vec<String>> + Sync + Send + 'static,
    ShouldPunishFn: Fn(&IOError) -> bool,
> {
    hosts: Vec<String>,
    update_func: UpdateFn,
    should_punish_func: ShouldPunishFn,
    update_interval: Duration,
    punish_duration: Duration,
    max_punished_times: usize,
    max_punished_hosts_percent: u8,
}

impl<
        UpdateFn: Fn() -> IOResult<Vec<String>> + Sync + Send + 'static,
        ShouldPunishFn: Fn(&IOError) -> bool,
    > HostSelectorBuilder<UpdateFn, ShouldPunishFn>
{
    /// 创建新的域名选择构建器
    /// # Arguments
    /// * `hosts` - 初始域名列表
    /// * `update_func` - 域名更新回调函数
    /// * `should_punish_func` - 是否惩罚回调函数
    pub fn new(
        hosts: Vec<String>,
        update_func: UpdateFn,
        should_punish_func: ShouldPunishFn,
    ) -> Self {
        Self {
            hosts,
            update_func,
            should_punish_func,
            update_interval: Duration::from_secs(5 * 60),
            punish_duration: Duration::from_secs(30),
            max_punished_times: 5,
            max_punished_hosts_percent: 50,
        }
    }

    /// 设置域名更新频率
    #[inline]
    pub fn update_interval(mut self, interval: Duration) -> Self {
        self.update_interval = interval;
        self
    }

    /// 设置域名惩罚时长
    #[inline]
    pub fn punish_duration(mut self, duration: Duration) -> Self {
        self.punish_duration = duration;
        self
    }

    /// 设置最大被惩罚次数
    ///
    /// 一旦一个域名的被惩罚次数超过限制，则域名选择器不会选择该域名，除非被惩罚的域名比例超过上限，或惩罚时长超过指定时长
    #[inline]
    pub fn max_punished_times(mut self, times: usize) -> Self {
        self.max_punished_times = times;
        self
    }

    /// 设置被惩罚的域名最大比例
    ///
    /// 域名选择器在搜索域名时，一旦被跳过的域名比例大于该值，则下一个域名将被选中，不管该域名是否也被惩罚。一旦该域名成功，则惩罚将立刻被取消
    #[inline]
    pub fn max_punished_hosts_percent(mut self, percent: u8) -> Self {
        self.max_punished_hosts_percent = percent;
        self
    }

    /// 构建域名选择器
    #[inline]
    pub fn build(self) -> Arc<HostSelector<UpdateFn, ShouldPunishFn>> {
        let hosts_updater = HostsUpdater::new(self.hosts, self.update_func);
        let host_selector = Arc::new(HostSelector {
            hosts_updater: hosts_updater.to_owned(),
            should_punish_func: self.should_punish_func,
            punish_duration: self.punish_duration,
            max_punished_times: self.max_punished_times,
            max_punished_hosts_percent: self.max_punished_hosts_percent,
            _update_thread: Arc::new(HostsUpdater::start_auto_update(
                &hosts_updater,
                self.update_interval,
            )),
        });

        host_selector
    }
}

impl<
        UpdateFn: Fn() -> IOResult<Vec<String>> + Sync + Send + 'static,
        ShouldPunishFn: Fn(&IOError) -> bool,
    > HostSelector<UpdateFn, ShouldPunishFn>
{
    /// 选择一个域名
    pub fn select_host(&self) -> Option<String> {
        let mut current_host = None;
        let hosts = self.hosts_updater.hosts.read().unwrap();
        for _ in 0..=(hosts.len() * usize::from(self.max_punished_hosts_percent) / 100) {
            let index = self.hosts_updater.next_index();
            let host = hosts[index % hosts.len()].as_str();
            current_host = Some(host);
            if let Some(punished_info) = self.hosts_updater.hosts_map.get(host) {
                if punished_info.continuous_punished_times <= self.max_punished_times
                    || punished_info.last_punished_at + self.punish_duration < SystemTime::now()
                {
                    break;
                }
            }
        }
        current_host.map(|h| h.to_owned())
    }

    /// 奖励一个域名，将会移除该域名先前全部的惩罚
    pub fn reward(&self, host: &str) {
        if let Some(mut punished_info) = self.hosts_updater.hosts_map.get_mut(host) {
            *punished_info = Default::default();
        }
    }

    /// 惩罚一个域名，会由 `should_punish_func` 回调函数决定是否对错误进行惩罚，如果需要惩罚，则相应的惩罚将被记录
    pub fn punish(&self, host: &str, error: &IOError) {
        if (self.should_punish_func)(error) {
            if let Some(mut punished_info) = self.hosts_updater.hosts_map.get_mut(host) {
                punished_info.continuous_punished_times += 1;
                punished_info.last_punished_at = SystemTime::now();
            }
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
            || {
                Ok(vec![
                    "http://host1".to_owned(),
                    "http://host2".to_owned(),
                    "http://host4".to_owned(),
                    "http://host5".to_owned(),
                ])
            },
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
    fn test_hosts_updater_auto_update() {
        let hosts_updater = HostsUpdater::new(
            vec![
                "http://host1".to_owned(),
                "http://host2".to_owned(),
                "http://host3".to_owned(),
            ],
            || {
                Ok(vec![
                    "http://host1".to_owned(),
                    "http://host2".to_owned(),
                    "http://host4".to_owned(),
                    "http://host5".to_owned(),
                ])
            },
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
        let punished_errs = Mutex::new(Vec::new());
        let host_selector = HostSelectorBuilder::new(
            vec![
                "http://host1".to_owned(),
                "http://host2".to_owned(),
                "http://host3".to_owned(),
            ],
            || Ok(vec![]),
            |error| {
                punished_errs.lock().unwrap().push(error.to_string());
                true
            },
        )
        .punish_duration(Duration::from_millis(500))
        .max_punished_times(2)
        .build();
        assert_eq!(host_selector.select_host(), Some("http://host1".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host2".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host3".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host1".to_owned()));
        host_selector.punish("http://host1", &IOError::new(IOErrorKind::Other, "error 1"));
        assert_eq!(host_selector.select_host(), Some("http://host2".to_owned()));
        host_selector.punish("http://host1", &IOError::new(IOErrorKind::Other, "error 2"));
        assert_eq!(host_selector.select_host(), Some("http://host3".to_owned()));
        host_selector.punish("http://host1", &IOError::new(IOErrorKind::Other, "error 3"));
        assert_eq!(host_selector.select_host(), Some("http://host2".to_owned()));
        host_selector.punish("http://host2", &IOError::new(IOErrorKind::Other, "error 4"));
        assert_eq!(host_selector.select_host(), Some("http://host3".to_owned()));
        host_selector.punish("http://host2", &IOError::new(IOErrorKind::Other, "error 5"));
        host_selector.punish("http://host2", &IOError::new(IOErrorKind::Other, "error 6"));
        assert_eq!(host_selector.select_host(), Some("http://host2".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host3".to_owned()));
        sleep(Duration::from_millis(500));
        assert_eq!(host_selector.select_host(), Some("http://host1".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host2".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host3".to_owned()));
        host_selector.punish("http://host3", &IOError::new(IOErrorKind::Other, "error 7"));
        host_selector.punish("http://host3", &IOError::new(IOErrorKind::Other, "error 8"));
        host_selector.punish("http://host3", &IOError::new(IOErrorKind::Other, "error 9"));
        assert_eq!(host_selector.select_host(), Some("http://host1".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host2".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host1".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host2".to_owned()));
        host_selector.reward("http://host3");
        assert_eq!(host_selector.select_host(), Some("http://host3".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host1".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host2".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host3".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host1".to_owned()));
        assert_eq!(host_selector.select_host(), Some("http://host2".to_owned()));

        assert_eq!(punished_errs.into_inner().unwrap().len(), 9);
    }
}
