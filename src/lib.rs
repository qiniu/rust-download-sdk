#![warn(missing_docs)]

//! # qiniu-download
//!
//! ## 七牛下载 SDK
//!
//! 负责下载完整或部分七牛对象

use directories::BaseDirs;
use once_cell::sync::Lazy;
use std::{
    env::temp_dir,
    fs::create_dir_all,
    io::Result as IOResult,
    path::{Path, PathBuf},
};

mod base;
mod config;
mod dot;
mod download;
mod host_selector;
mod query;
mod req_id;

pub use base::credential::Credential;
use config::HTTP_CLIENT;
pub use config::{is_qiniu_enabled, Config, ConfigBuilder};
pub use dot::{
    disable_dot_uploading, disable_dotting, enable_dot_uploading, enable_dotting,
    is_dot_uploading_disabled, is_dotting_disabled,
};
pub use download::{
    sign_download_url_with_deadline, sign_download_url_with_lifetime, RangeReader,
    RangeReaderBuilder, RangePart,
};
pub use req_id::{set_download_start_time, total_download_duration};

fn cache_dir_path_of(path: impl AsRef<Path>) -> IOResult<PathBuf> {
    static CACHE_DIR: Lazy<PathBuf> = Lazy::new(|| {
        BaseDirs::new()
            .map(|dir| dir.cache_dir().join("qiniu-download"))
            .unwrap_or_else(|| temp_dir().join("qiniu-download"))
    });

    if !CACHE_DIR.exists() {
        create_dir_all(&*CACHE_DIR)?;
    }

    Ok(CACHE_DIR.join(path.as_ref()))
}
