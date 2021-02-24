#![warn(missing_docs)]

//! # qiniu-download
//!
//! ## 七牛下载 SDK
//!
//! 负责下载完整或部分七牛对象

mod base;
mod config;
mod download;
mod host_selector;
mod query;
mod req_id;

pub use base::credential::Credential;
pub use config::is_qiniu_enabled;
pub use download::{sign_download_url_with_deadline, sign_download_url_with_lifetime, RangeReader};
pub use host_selector::{HostSelector, HostSelectorBuilder};
pub use req_id::{set_download_start_time, total_download_duration};

use once_cell::sync::Lazy;
use reqwest::blocking::Client as HTTPClient;
use std::time::Duration;

static HTTP_CLIENT: Lazy<HTTPClient> = Lazy::new(|| {
    let user_agent = format!("QiniuRustDownload/{}", env!("CARGO_PKG_VERSION"));
    HTTPClient::builder()
        .user_agent(user_agent)
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(5)
        .connection_verbose(true)
        .build()
        .expect("Failed to build Reqwest Client")
});
