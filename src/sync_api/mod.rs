#![allow(dead_code)]

mod cache_dir;
mod dot;
mod host_selector;
mod query;
mod req_id;

mod download;
pub(crate) use download::{RangeReader, RangeReaderBuilder, RangeReaderInner};
