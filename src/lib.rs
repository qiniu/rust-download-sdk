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
use config::HTTP_CLIENT;
pub use download::{sign_download_url_with_deadline, sign_download_url_with_lifetime, RangeReader};
pub use req_id::{set_download_start_time, total_download_duration};
