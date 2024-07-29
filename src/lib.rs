#![deny(
    missing_docs,
    unused_must_use,
    absolute_paths_not_starting_with_crate,
    anonymous_parameters,
    explicit_outlives_requirements,
    keyword_idents,
    macro_use_extern_crate,
    meta_variable_misuse,
    non_ascii_idents,
    trivial_casts,
    trivial_numeric_casts,
    unused_crate_dependencies,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications
)]

//! # qiniu-download
//!
//! ## 七牛下载 SDK
//!
//! 负责下载完整或部分七牛对象

mod async_api;
mod base;
mod config;
mod download;
mod sync_api;

pub use async_api::{
    disable_dot_uploading, disable_dotting, enable_dot_uploading, enable_dotting,
    is_dot_uploading_disabled, is_dotting_disabled, set_download_start_time,
    sign_download_url_with_deadline, sign_download_url_with_lifetime, total_download_duration,
    RangePart,
};
pub use base::credential::Credential;
pub use config::{
    is_qiniu_enabled, set_qiniu_config, set_qiniu_multi_clusters_config,
    set_qiniu_single_cluster_config, with_current_qiniu_config, with_current_qiniu_config_mut,
    ClustersConfigParseError, Config, ConfigBuilder, Configurable, MultipleClustersConfig,
    MultipleClustersConfigBuilder, MultipleClustersConfigParseError, SingleClusterConfig,
    SingleClusterConfigBuilder,
};
pub use download::{RangeReader, RangeReaderBuilder};
