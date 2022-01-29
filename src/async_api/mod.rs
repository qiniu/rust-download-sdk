#![allow(dead_code, unused_imports)]

mod cache_dir;
mod host_selector;
mod query;

mod req_id;
pub use req_id::{set_download_start_time, total_download_duration};

mod dot;
pub use dot::{
    disable_dot_uploading, disable_dotting, enable_dot_uploading, enable_dotting,
    is_dot_uploading_disabled, is_dotting_disabled,
};

mod download;
pub(crate) use download::{
    sign_download_url_with_deadline, sign_download_url_with_lifetime, RangePart, RangeReader,
    RangeReaderBuilder, RangeReaderInner,
};

mod retrier;
