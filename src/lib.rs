mod base;
mod download;
mod query;

pub use base::credential::Credential;
pub use download::{sign_download_url_with_deadline, sign_download_url_with_lifetime, RangeReader};

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
        .build()
        .expect("Failed to build Reqwest Client")
});
