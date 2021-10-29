use super::{
    multi_clusters::MultipleClustersConfig,
    single_cluster::{Config, SingleClusterConfig},
    ClustersConfigParseError, Timeouts,
};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

/// 七牛配置信息
#[derive(Debug, Clone)]
pub struct Configurable(ConfigurableInner);

#[derive(Debug, Clone)]
enum ConfigurableInner {
    Single(SingleClusterConfig),
    Multi(MultipleClustersConfig),
}

impl Configurable {
    /// 创建七牛单集群配置信息
    #[inline]
    pub fn new_single(config: SingleClusterConfig) -> Self {
        Self(ConfigurableInner::Single(config))
    }

    /// 获取单集群配置信息（仅当当前配置是单集群配置时才返回）
    #[inline]
    pub fn as_single(&self) -> Option<&SingleClusterConfig> {
        if let ConfigurableInner::Single(single) = &self.0 {
            Some(single)
        } else {
            None
        }
    }

    /// 获取单集群配置信息（仅当当前配置是单集群配置时才返回）
    #[inline]
    pub fn as_single_mut(&mut self) -> Option<&mut SingleClusterConfig> {
        if let ConfigurableInner::Single(single) = &mut self.0 {
            Some(single)
        } else {
            None
        }
    }

    /// 创建七牛多集群配置信息
    #[inline]
    pub fn new_multi(config: MultipleClustersConfig) -> Self {
        Self(ConfigurableInner::Multi(config))
    }

    /// 获取多集群配置信息（仅当当前配置是多集群配置时才返回）
    #[inline]
    pub fn as_multi(&self) -> Option<&MultipleClustersConfig> {
        if let ConfigurableInner::Multi(multi) = &self.0 {
            Some(multi)
        } else {
            None
        }
    }

    /// 获取多集群配置信息（仅当当前配置是多集群配置时才返回）
    #[inline]
    pub fn as_multi_mut(&mut self) -> Option<&mut MultipleClustersConfig> {
        if let ConfigurableInner::Multi(multi) = &mut self.0 {
            Some(multi)
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn with_key<T>(&self, key: &str, f: impl FnOnce(&Config) -> T) -> Option<T> {
        match &self.0 {
            ConfigurableInner::Single(single) => single.with_key(key, f),
            ConfigurableInner::Multi(multi) => multi.with_key(key, f),
        }
    }

    #[inline]
    pub(super) fn parse(
        path: impl AsRef<Path>,
        bytes: &[u8],
        multi: bool,
    ) -> Result<Self, ClustersConfigParseError> {
        if multi {
            MultipleClustersConfig::parse(path.as_ref(), bytes)
                .map(ConfigurableInner::Multi)
                .map(Self)
        } else {
            SingleClusterConfig::parse(path.as_ref(), bytes)
                .map(ConfigurableInner::Single)
                .map(Self)
        }
    }

    #[inline]
    pub(super) fn config_paths(&self) -> Vec<PathBuf> {
        match &self.0 {
            ConfigurableInner::Single(single) => single.config_paths(),
            ConfigurableInner::Multi(multi) => multi.config_paths(),
        }
    }

    #[inline]
    pub(super) fn timeouts_set(&self) -> HashSet<Timeouts> {
        match &self.0 {
            ConfigurableInner::Single(single) => single.timeouts_set(),
            ConfigurableInner::Multi(multi) => multi.timeouts_set(),
        }
    }
}

impl From<SingleClusterConfig> for Configurable {
    #[inline]
    fn from(config: SingleClusterConfig) -> Self {
        Self::new_single(config)
    }
}

impl From<MultipleClustersConfig> for Configurable {
    #[inline]
    fn from(config: MultipleClustersConfig) -> Self {
        Self::new_multi(config)
    }
}
