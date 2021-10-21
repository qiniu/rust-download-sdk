use super::{
    base::{credential::Credential, upload_policy::UploadPolicy, upload_token::sign_upload_token},
    cache_dir_path_of,
    host_selector::{HostSelector, PunishResult},
    HTTP_CLIENT,
};
use dashmap::DashMap;
use fd_lock::FdLock;
use log::{debug, info, warn};
use reqwest::{header::AUTHORIZATION, StatusCode};
use serde::{de::Error as DeserializeError, Deserialize, Serialize};
use serde_json::Value as JSONValue;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt,
    fs::{File, OpenOptions},
    io::{
        BufRead, BufReader, Error as IOError, ErrorKind as IOErrorKind, Result as IOResult, Seek,
        SeekFrom, Write,
    },
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc, Mutex,
    },
    thread::Builder as ThreadBuilder,
    time::{Duration, Instant, SystemTime},
    u128,
};
use tap::prelude::*;

static DOTTING_DISABLED: AtomicBool = AtomicBool::new(false);

/// 禁止打点功能
#[inline]
pub fn disable_dotting() {
    DOTTING_DISABLED.store(true, Relaxed)
}

/// 启用打点功能
#[inline]
pub fn enable_dotting() {
    DOTTING_DISABLED.store(false, Relaxed)
}

/// 打点功能是否启用
#[inline]
pub fn is_dotting_disabled() -> bool {
    DOTTING_DISABLED.load(Relaxed)
}

static DOT_UPLOADING_DISABLED: AtomicBool = AtomicBool::new(false);

/// 禁止打点上传功能
#[inline]
pub fn disable_dot_uploading() {
    DOT_UPLOADING_DISABLED.store(true, Relaxed)
}

/// 启用打点上传功能
#[inline]
pub fn enable_dot_uploading() {
    DOT_UPLOADING_DISABLED.store(false, Relaxed)
}

/// 打点上传功能是否启用
#[inline]
pub fn is_dot_uploading_disabled() -> bool {
    DOT_UPLOADING_DISABLED.load(Relaxed)
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
pub(super) enum DotType {
    Sdk,
    Http,
}

impl fmt::Display for DotType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http => write!(f, "http"),
            Self::Sdk => write!(f, "sdk"),
        }
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub(super) enum ApiName {
    IoGetfile,
    MonitorV1Stat,
    UcV4Query,
    RangeReaderReadAt,
    RangeReaderReadMultiRanges,
    RangeReaderExist,
    RangeReaderFileSize,
    RangeReaderDownloadTo,
    RangeReaderReadLastBytes,
}

impl fmt::Display for ApiName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoGetfile => write!(f, "io_getfile"),
            Self::MonitorV1Stat => write!(f, "monitor_v1_stat"),
            Self::UcV4Query => write!(f, "uc_v4_query"),
            Self::RangeReaderReadAt => write!(f, "range_reader_read_at"),
            Self::RangeReaderReadMultiRanges => write!(f, "range_reader_read_multi_ranges"),
            Self::RangeReaderExist => write!(f, "range_reader_exist"),
            Self::RangeReaderFileSize => write!(f, "range_reader_file_size"),
            Self::RangeReaderDownloadTo => write!(f, "range_reader_download_to"),
            Self::RangeReaderReadLastBytes => write!(f, "range_reader_read_last_bytes"),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct Dotter {
    inner: Option<Arc<DotterInner>>,
}

#[derive(Debug)]
struct DotterInner {
    credential: Credential,
    bucket: String,
    monitor_selector: HostSelector,
    buffered_records: DotRecordsDashMap,
    buffered_file: Mutex<FdLock<File>>,
    interval: Duration,
    uploaded_at: Instant,
    max_buffer_size: u64,
    tries: usize,
}

pub(super) const DOT_FILE_NAME: &str = "dot-file";

impl Dotter {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        credential: Credential,
        bucket: String,
        monitor_urls: Vec<String>,
        interval: Option<Duration>,
        max_buffer_size: Option<u64>,
        tries: Option<usize>,
        punish_duration: Option<Duration>,
        max_punished_times: Option<usize>,
        max_punished_hosts_percent: Option<u8>,
        base_timeout: Option<Duration>,
    ) -> Dotter {
        if !monitor_urls.is_empty() {
            if let Ok(buffered_file_path) = cache_dir_path_of(DOT_FILE_NAME) {
                if let Ok(buffer_file) = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .append(true)
                    .open(&buffered_file_path)
                {
                    let monitor_selector = HostSelector::builder(monitor_urls)
                        .punish_duration(punish_duration.unwrap_or_else(|| Duration::from_secs(30)))
                        .max_punished_times(max_punished_times.unwrap_or(5))
                        .max_punished_hosts_percent(max_punished_hosts_percent.unwrap_or(50))
                        .base_timeout(base_timeout.unwrap_or_else(|| Duration::from_secs(1)))
                        .build();
                    return Self {
                        inner: Some(Arc::new(DotterInner {
                            credential,
                            bucket,
                            monitor_selector,
                            buffered_records: Default::default(),
                            buffered_file: Mutex::new(FdLock::new(buffer_file)),
                            interval: interval.unwrap_or_else(|| Duration::from_secs(10)),
                            uploaded_at: Instant::now(),
                            max_buffer_size: max_buffer_size.unwrap_or(1 << 20),
                            tries: tries.unwrap_or(10),
                        })),
                    };
                }
            }
        }
        Self { inner: None }
    }

    pub(super) fn dot(
        &self,
        dot_type: DotType,
        api_name: ApiName,
        successful: bool,
        elapsed_duration: Duration,
    ) -> IOResult<()> {
        if is_dotting_disabled() {
            debug!("dotting is disabled")
        } else if let Some(inner) = self.inner.as_ref() {
            inner.fast_dot(dot_type, api_name, successful, elapsed_duration);
            inner.lock_buffered_file(|buffered_file| {
                inner.flush_to_file(buffered_file)?;
                if inner.is_time_to_upload(buffered_file)? {
                    self.async_upload();
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    pub(super) fn punish(&self) -> IOResult<()> {
        if is_dotting_disabled() {
            debug!("dotting is disabled")
        } else if let Some(inner) = self.inner.as_ref() {
            inner.fast_punish();
            inner.lock_buffered_file(|buffered_file| {
                inner.flush_to_file(buffered_file)?;
                if inner.is_time_to_upload(buffered_file)? {
                    self.async_upload();
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    fn async_upload(&self) {
        if let Some(inner) = self.inner.as_ref() {
            let inner = inner.to_owned();
            if let Err(err) = ThreadBuilder::new()
                .name("dots-uploader".into())
                .spawn(move || {
                    inner.lock_buffered_file(|buffered_file| {
                        if inner.is_time_to_upload(buffered_file)? {
                            inner.sync_upload()?;
                        }
                        Ok(())
                    })
                })
            {
                warn!(
                    "failed to start thread `dots-uploader` to upload dots: {:?}",
                    err
                );
            }
        }
    }
}

impl DotterInner {
    fn fast_dot(
        &self,
        dot_type: DotType,
        api_name: ApiName,
        successful: bool,
        elapsed_duration: Duration,
    ) {
        let record = if successful {
            DotRecord::new(
                dot_type,
                api_name,
                1,
                Default::default(),
                elapsed_duration.as_millis(),
                Default::default(),
            )
        } else {
            DotRecord::new(
                dot_type,
                api_name,
                Default::default(),
                1,
                Default::default(),
                elapsed_duration.as_millis(),
            )
        };
        self.buffered_records.merge_with_record(record);
    }

    #[inline]
    fn fast_punish(&self) {
        self.buffered_records
            .merge_with_record(DotRecord::punished());
    }

    #[inline]
    fn flush_to_file(&self, buffered_file: &mut File) -> IOResult<()> {
        self.buffered_records
            .retain(|_, r| write_to_file(r, buffered_file).is_err());

        return Ok(());

        #[inline]
        fn write_to_file(record: &DotRecord, file: &mut File) -> anyhow::Result<()> {
            writeln!(file, "{}", serde_json::to_string(record)?)
                .tap_err(|err| warn!("the dot file is failed to write: {:?}", err))?;
            Ok(())
        }
    }

    fn is_time_to_upload(&self, buffered_file: &File) -> IOResult<bool> {
        if is_dotting_disabled() || is_dot_uploading_disabled() {
            debug!("dot uploading is disabled, will not upload the dot file now");
            return Ok(false);
        }
        let result = self.uploaded_at.elapsed() > self.interval
            || buffered_file
                .metadata()
                .tap_err(|err| warn!("stat the dot file error: {:?}", err))?
                .len()
                > self.max_buffer_size;
        if !result {
            debug!("dot uploading condition is not satisfied")
        }
        Ok(result)
    }

    fn sync_upload(&self) -> IOResult<()> {
        let mut buffered_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&cache_dir_path_of(DOT_FILE_NAME)?)?;
        self.upload_with_retry(|monitor_host, timeout, timeout_power| {
            let url = format!("{}/v1/stat", monitor_host);
            debug!("try to upload dots to {}", url);
            let uptoken = sign_upload_token(
                &self.credential,
                &UploadPolicy::new_for_bucket(
                    self.bucket.to_owned(),
                    SystemTime::now() + Duration::from_secs(30),
                ),
            );
            let begin_at = Instant::now();
            HTTP_CLIENT
                .read()
                .unwrap()
                .post(&url)
                .header(AUTHORIZATION, format!("UpToken {}", uptoken))
                .json(&self.make_request_body(&mut buffered_file)?)
                .timeout(timeout)
                .send()
                .tap_err(|err| {
                    if err.is_timeout() {
                        self.monitor_selector
                            .increase_timeout_power_by(monitor_host, timeout_power);
                    }
                })
                .map_err(|err| IOError::new(IOErrorKind::ConnectionAborted, err))
                .and_then(|resp| {
                    if resp.status() != StatusCode::OK {
                        Err(IOError::new(
                            IOErrorKind::Other,
                            format!("Unexpected status code {}", resp.status().as_u16()),
                        ))
                    } else {
                        Ok(())
                    }
                })
                .tap(|result| {
                    self.fast_dot(
                        DotType::Http,
                        ApiName::MonitorV1Stat,
                        result.is_ok(),
                        begin_at.elapsed(),
                    );
                })
                .tap_ok(|_| info!("upload dots succeed"))
                .tap_err(|err| warn!("failed to upload dots: {:?}", err))?;
            Ok(())
        })?;
        buffered_file.set_len(0)?;
        Ok(())
    }

    fn make_request_body(&self, buffered_file: &mut File) -> IOResult<DotRecords> {
        buffered_file.seek(SeekFrom::Start(0))?;
        let file_reader = BufReader::new(buffered_file);
        let mut map = DotRecordsMap::default();

        for line in file_reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            if let Ok(record) = serde_json::from_str::<DotRecord>(&line) {
                map.merge_with_record(record);
            }
        }
        Ok(map.into_records())
    }

    fn upload_with_retry(
        &self,
        mut for_each_host: impl FnMut(&str, Duration, usize) -> IOResult<()>,
    ) -> IOResult<()> {
        let mut last_error = None;
        for _ in 0..self.tries {
            let host_info = self.monitor_selector.select_host();
            match for_each_host(&host_info.host, host_info.timeout, host_info.timeout_power) {
                Ok(response) => {
                    self.monitor_selector.reward(&host_info.host);
                    return Ok(response);
                }
                Err(err) => {
                    let punished_result = self
                        .monitor_selector
                        .punish_without_dotter(&host_info.host, &err);
                    match punished_result {
                        PunishResult::NoPunishment => {
                            return Err(err);
                        }
                        PunishResult::PunishedAndFreezed => {
                            self.fast_punish();
                        }
                        PunishResult::Punished => {}
                    }
                    last_error = Some(err);
                }
            }
        }
        Err(last_error.expect("No Monitor tries error"))
    }

    #[cfg(not(test))]
    fn lock_buffered_file(&self, f: impl FnOnce(&mut File) -> IOResult<()>) -> IOResult<()> {
        if let Ok(mut buffered_file) = self.buffered_file.try_lock() {
            loop {
                match buffered_file.try_lock() {
                    Ok(mut buffered_file) => {
                        return f(&mut buffered_file);
                    }
                    Err(err) if err.kind() == IOErrorKind::WouldBlock => {
                        debug!("the dot file is locked");
                        return Ok(());
                    }
                    Err(err) if err.kind() == IOErrorKind::Interrupted => {
                        continue;
                    }
                    Err(err) => {
                        warn!("lock the dot file error: {:?}", err);
                        return Err(err);
                    }
                }
            }
        } else {
            debug!("the dot file is locked");
        }
        Ok(())
    }

    #[cfg(test)]
    fn lock_buffered_file(&self, f: impl FnOnce(&mut File) -> IOResult<()>) -> IOResult<()> {
        if let Ok(mut buffered_file) = self.buffered_file.lock() {
            loop {
                match buffered_file.lock() {
                    Ok(mut buffered_file) => {
                        return f(&mut buffered_file);
                    }
                    Err(err) if err.kind() == IOErrorKind::Interrupted => {
                        continue;
                    }
                    Err(err) => {
                        warn!("lock the dot file error: {:?}", err);
                        return Err(err);
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(untagged)]
pub(super) enum DotRecordKey {
    APICalls {
        dot_type: DotType,
        api_name: ApiName,
    },
    PunishedCount,
}

impl DotRecordKey {
    #[inline]
    pub(super) fn new(dot_type: DotType, api_name: ApiName) -> Self {
        Self::APICalls { dot_type, api_name }
    }

    #[inline]
    pub(super) fn punished() -> Self {
        Self::PunishedCount
    }
}

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
pub(super) enum DotRecord {
    APICalls(APICallsDotRecord),
    PunishedCount(PunishedCountDotRecord),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(super) struct APICallsDotRecord {
    #[serde(rename = "type")]
    dot_type: DotType,

    api_name: ApiName,
    success_count: usize,
    success_avg_elapsed_duration: u128,
    failed_count: usize,
    failed_avg_elapsed_duration: u128,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(super) struct PunishedCountDotRecord {
    punished_count: usize,
}

impl DotRecord {
    #[inline]
    fn new(
        dot_type: DotType,
        api_name: ApiName,
        success_count: usize,
        failed_count: usize,
        success_avg_elapsed_duration: u128,
        failed_avg_elapsed_duration: u128,
    ) -> Self {
        Self::APICalls(APICallsDotRecord {
            dot_type,
            api_name,
            success_count,
            success_avg_elapsed_duration,
            failed_count,
            failed_avg_elapsed_duration,
        })
    }

    #[inline]
    fn punished() -> Self {
        Self::PunishedCount(PunishedCountDotRecord { punished_count: 1 })
    }

    #[inline]
    pub(super) fn key(&self) -> DotRecordKey {
        match self {
            Self::APICalls(record) => DotRecordKey::new(record.dot_type, record.api_name),
            Self::PunishedCount(_) => DotRecordKey::punished(),
        }
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn dot_type(&self) -> Option<DotType> {
        match self {
            Self::APICalls(record) => Some(record.dot_type),
            _ => None,
        }
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn api_name(&self) -> Option<ApiName> {
        match self {
            Self::APICalls(record) => Some(record.api_name),
            _ => None,
        }
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn success_count(&self) -> Option<usize> {
        match self {
            Self::APICalls(record) => Some(record.success_count),
            _ => None,
        }
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn success_avg_elapsed_duration_ms(&self) -> Option<u128> {
        match self {
            Self::APICalls(record) => Some(record.success_avg_elapsed_duration),
            _ => None,
        }
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn failed_count(&self) -> Option<usize> {
        match self {
            Self::APICalls(record) => Some(record.failed_count),
            _ => None,
        }
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn failed_avg_elapsed_duration_ms(&self) -> Option<u128> {
        match self {
            Self::APICalls(record) => Some(record.failed_avg_elapsed_duration),
            _ => None,
        }
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn punished_count(&self) -> Option<usize> {
        match self {
            Self::PunishedCount(record) => Some(record.punished_count),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for DotRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = JSONValue::deserialize(deserializer)?;
        if let Ok(record) = APICallsDotRecord::deserialize(&value) {
            Ok(Self::APICalls(record))
        } else {
            PunishedCountDotRecord::deserialize(&value)
                .map(Self::PunishedCount)
                .map_err(DeserializeError::custom)
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub(super) struct DotRecords {
    #[serde(rename = "logs")]
    records: Vec<DotRecord>,
}

impl DotRecords {
    #[cfg(test)]
    #[inline]
    pub(super) fn records(&self) -> &[DotRecord] {
        self.records.as_ref()
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct DotRecordsMap {
    m: HashMap<DotRecordKey, DotRecord>,
}

impl DotRecordsMap {
    #[allow(dead_code)]
    pub(super) fn merge_with_record(&mut self, record: DotRecord) {
        self.m
            .entry(record.key())
            .and_modify(|mut r| match (&mut r, &record) {
                (DotRecord::APICalls(r), DotRecord::APICalls(record)) => {
                    let success_elapsed_duration_total = r.success_avg_elapsed_duration
                        * to_u128(r.success_count)
                        + record.success_avg_elapsed_duration * to_u128(record.success_count);
                    let failed_elapsed_duration_total = r.failed_avg_elapsed_duration
                        * to_u128(r.failed_count)
                        + record.failed_avg_elapsed_duration * to_u128(record.failed_count);
                    r.success_count += record.success_count;
                    r.failed_count += record.failed_count;
                    r.success_avg_elapsed_duration = if r.success_count > 0 {
                        success_elapsed_duration_total / to_u128(r.success_count)
                    } else {
                        0
                    };
                    r.failed_avg_elapsed_duration = if r.failed_count > 0 {
                        failed_elapsed_duration_total / to_u128(r.failed_count)
                    } else {
                        0
                    };
                }
                (DotRecord::PunishedCount(r), DotRecord::PunishedCount(record)) => {
                    r.punished_count += record.punished_count;
                }
                _ => panic!("Impossible merge with {:?} and {:?}", r, record),
            })
            .or_insert(record);

        #[inline]
        fn to_u128(v: usize) -> u128 {
            u128::try_from(v).unwrap_or(u128::MAX)
        }
    }

    #[allow(dead_code)]
    pub(super) fn merge_with_records(&mut self, records: DotRecords) {
        for record in records.records.into_iter() {
            self.merge_with_record(record);
        }
    }

    #[allow(dead_code)]
    pub(super) fn into_records(self) -> DotRecords {
        DotRecords {
            records: self.m.into_iter().map(|(_, v)| v).collect(),
        }
    }
}

impl Deref for DotRecordsMap {
    type Target = HashMap<DotRecordKey, DotRecord>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.m
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct DotRecordsDashMap {
    m: DashMap<DotRecordKey, DotRecord>,
}

impl DotRecordsDashMap {
    #[allow(dead_code)]
    pub(super) fn merge_with_record(&self, record: DotRecord) {
        self.m
            .entry(record.key())
            .and_modify(|mut r| match (&mut r, &record) {
                (DotRecord::APICalls(r), DotRecord::APICalls(record)) => {
                    let success_elapsed_duration_total = r.success_avg_elapsed_duration
                        * to_u128(r.success_count)
                        + record.success_avg_elapsed_duration * to_u128(record.success_count);
                    let failed_elapsed_duration_total = r.failed_avg_elapsed_duration
                        * to_u128(r.failed_count)
                        + record.failed_avg_elapsed_duration * to_u128(record.failed_count);
                    r.success_count += record.success_count;
                    r.failed_count += record.failed_count;
                    r.success_avg_elapsed_duration = if r.success_count > 0 {
                        success_elapsed_duration_total / to_u128(r.success_count)
                    } else {
                        0
                    };
                    r.failed_avg_elapsed_duration = if r.failed_count > 0 {
                        failed_elapsed_duration_total / to_u128(r.failed_count)
                    } else {
                        0
                    };
                }
                (DotRecord::PunishedCount(r), DotRecord::PunishedCount(record)) => {
                    r.punished_count += record.punished_count;
                }
                _ => panic!("Impossible merge with {:?} and {:?}", r, record),
            })
            .or_insert(record);

        #[inline]
        fn to_u128(v: usize) -> u128 {
            u128::try_from(v).unwrap_or(u128::MAX)
        }
    }

    #[allow(dead_code)]
    pub(super) fn merge_with_records(&self, records: DotRecords) {
        for record in records.records.into_iter() {
            self.merge_with_record(record);
        }
    }

    #[allow(dead_code)]
    pub(super) fn into_records(self) -> DotRecords {
        DotRecords {
            records: self.m.into_iter().map(|(_, v)| v).collect(),
        }
    }
}

impl Deref for DotRecordsDashMap {
    type Target = DashMap<DotRecordKey, DotRecord>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.m
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::oneshot::channel;
    use rayon::ThreadPoolBuilder;
    use std::{error::Error, sync::atomic::AtomicUsize, thread::sleep};
    use tokio::task::{spawn, spawn_blocking};
    use warp::{http::HeaderValue, hyper::Body, path, reply::Response, Filter};

    macro_rules! starts_with_server {
        ($addr:ident, $routes:ident, $code:block) => {{
            let (tx, rx) = channel();
            let ($addr, server) =
                warp::serve($routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async move {
                    rx.await.ok();
                });
            let handler = spawn(server);
            $code;
            tx.send(()).ok();
            handler.await.ok();
        }};
    }

    const ACCESS_KEY: &str = "1234567890";
    const SECRET_KEY: &str = "abcdefghijk";
    const BUCKET_NAME: &str = "test-bucket";

    mod guard {
        use super::{disable_dotting, enable_dotting, is_dotting_disabled};
        pub(super) struct DottingDisableGuard {
            enabled_before: bool,
        }

        impl DottingDisableGuard {
            pub(super) fn new() -> Self {
                let disabled_before = is_dotting_disabled();
                if !disabled_before {
                    disable_dotting();
                }
                DottingDisableGuard {
                    enabled_before: !disabled_before,
                }
            }
        }

        impl Drop for DottingDisableGuard {
            fn drop(&mut self) {
                if self.enabled_before {
                    enable_dotting();
                }
            }
        }
    }
    use guard::DottingDisableGuard;

    #[inline]
    fn get_credential() -> Credential {
        Credential::new(ACCESS_KEY, SECRET_KEY)
    }

    #[tokio::test]
    async fn test_dotter_dot_nothing() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;

        let called = Arc::new(AtomicUsize::new(0));
        let routes = {
            let called = called.to_owned();
            path!("v1" / "stat").map(move || {
                called.fetch_add(1, Relaxed);
                Response::new(Body::empty())
            })
        };

        starts_with_server!(addr, routes, {
            spawn_blocking(move || {
                let dotter = Dotter::new(
                    get_credential(),
                    BUCKET_NAME.to_owned(),
                    vec![],
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                );
                assert!(dotter.inner.is_none());
                dotter
                    .dot(
                        DotType::Http,
                        ApiName::IoGetfile,
                        true,
                        Duration::from_millis(0),
                    )
                    .unwrap();
                sleep(Duration::from_secs(5));
                assert_eq!(called.load(Relaxed), 0);

                let urls = vec!["http://".to_owned() + &addr.to_string()];
                let dotter = Dotter::new(
                    get_credential(),
                    BUCKET_NAME.to_owned(),
                    urls,
                    Some(Duration::from_millis(0)),
                    Some(1),
                    None,
                    None,
                    None,
                    None,
                    None,
                );
                assert!(dotter.inner.is_some());

                let _guard = DottingDisableGuard::new();
                dotter
                    .dot(
                        DotType::Http,
                        ApiName::IoGetfile,
                        true,
                        Duration::from_millis(0),
                    )
                    .unwrap();
                sleep(Duration::from_secs(5));
                assert_eq!(called.load(Relaxed), 0);
            })
            .await?;
        });

        Ok(())
    }

    #[tokio::test]
    async fn test_dotter_dot_something() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;
        let records_map = Arc::new(DotRecordsDashMap::default());

        let routes = {
            let records_map = records_map.to_owned();
            path!("v1" / "stat")
                .and(warp::header::value(AUTHORIZATION.as_str()))
                .and(warp::body::json())
                .map(move |authorization: HeaderValue, records: DotRecords| {
                    assert!(authorization.to_str().unwrap().starts_with("UpToken "));
                    records_map.merge_with_records(records);
                    Response::new(Body::empty())
                })
        };

        starts_with_server!(addr, routes, {
            let urls = vec![
                "http://".to_owned() + &addr.to_string() + "1",
                "http://".to_owned() + &addr.to_string() + "2",
                "http://".to_owned() + &addr.to_string() + "3",
                "http://".to_owned() + &addr.to_string() + "4",
                "http://".to_owned() + &addr.to_string(),
            ];
            spawn_blocking(move || {
                let dotter = Dotter::new(
                    get_credential(),
                    BUCKET_NAME.to_owned(),
                    urls,
                    Some(Duration::from_millis(0)),
                    Some(1),
                    None,
                    None,
                    None,
                    None,
                    None,
                );

                let thread_pool = ThreadPoolBuilder::new().num_threads(10).build().unwrap();
                thread_pool.scope(|s| {
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Sdk,
                                    ApiName::IoGetfile,
                                    true,
                                    Duration::from_millis(10),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Sdk,
                                    ApiName::IoGetfile,
                                    false,
                                    Duration::from_millis(12),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Sdk,
                                    ApiName::UcV4Query,
                                    true,
                                    Duration::from_millis(14),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Sdk,
                                    ApiName::UcV4Query,
                                    true,
                                    Duration::from_millis(16),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Sdk,
                                    ApiName::UcV4Query,
                                    false,
                                    Duration::from_millis(18),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Http,
                                    ApiName::IoGetfile,
                                    true,
                                    Duration::from_millis(20),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Http,
                                    ApiName::IoGetfile,
                                    true,
                                    Duration::from_millis(22),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Http,
                                    ApiName::IoGetfile,
                                    false,
                                    Duration::from_millis(24),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Http,
                                    ApiName::UcV4Query,
                                    true,
                                    Duration::from_millis(26),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Http,
                                    ApiName::UcV4Query,
                                    true,
                                    Duration::from_millis(28),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Http,
                                    ApiName::UcV4Query,
                                    false,
                                    Duration::from_millis(30),
                                )
                                .unwrap();
                        });
                    }
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Http,
                                    ApiName::UcV4Query,
                                    true,
                                    Duration::from_millis(32),
                                )
                                .unwrap();
                        });
                    }
                });
                sleep(Duration::from_secs(5));
                {
                    let record = records_map
                        .get(&DotRecordKey::new(DotType::Sdk, ApiName::UcV4Query))
                        .unwrap();
                    assert_eq!(record.success_count(), Some(2));
                    assert_eq!(record.failed_count(), Some(1));
                    assert_eq!(record.success_avg_elapsed_duration_ms(), Some(15));
                    assert_eq!(record.failed_avg_elapsed_duration_ms(), Some(18));
                }
                {
                    let record = records_map
                        .get(&DotRecordKey::new(DotType::Sdk, ApiName::IoGetfile))
                        .unwrap();
                    assert_eq!(record.success_count(), Some(1));
                    assert_eq!(record.failed_count(), Some(1));
                    assert_eq!(record.success_avg_elapsed_duration_ms(), Some(10));
                    assert_eq!(record.failed_avg_elapsed_duration_ms(), Some(12));
                }
                {
                    let record = records_map
                        .get(&DotRecordKey::new(DotType::Http, ApiName::UcV4Query))
                        .unwrap();
                    assert_eq!(record.success_count(), Some(3));
                    assert_eq!(record.failed_count(), Some(1));
                    assert_eq!(record.success_avg_elapsed_duration_ms(), Some(28));
                    assert_eq!(record.failed_avg_elapsed_duration_ms(), Some(30));
                }
                {
                    let record = records_map
                        .get(&DotRecordKey::new(DotType::Http, ApiName::IoGetfile))
                        .unwrap();
                    assert_eq!(record.success_count(), Some(2));
                    assert_eq!(record.failed_count(), Some(1));
                    assert_eq!(record.success_avg_elapsed_duration_ms(), Some(21));
                    assert_eq!(record.failed_avg_elapsed_duration_ms(), Some(24));
                }
            })
            .await?;
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_dotter_punish() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();
        clear_cache()?;
        let records_map = Arc::new(DotRecordsDashMap::default());

        let routes = {
            let records_map = records_map.to_owned();
            path!("v1" / "stat")
                .and(warp::header::value(AUTHORIZATION.as_str()))
                .and(warp::body::json())
                .map(move |authorization: HeaderValue, records: DotRecords| {
                    assert!(authorization.to_str().unwrap().starts_with("UpToken "));
                    records_map.merge_with_records(records);
                    Response::new(Body::empty())
                })
        };
        starts_with_server!(addr, routes, {
            let urls = vec!["http://".to_owned() + &addr.to_string()];
            spawn_blocking(move || {
                let dotter = Dotter::new(
                    get_credential(),
                    BUCKET_NAME.to_owned(),
                    urls,
                    Some(Duration::from_millis(0)),
                    Some(1),
                    None,
                    None,
                    None,
                    None,
                    None,
                );

                let thread_pool = ThreadPoolBuilder::new().num_threads(10).build().unwrap();
                thread_pool.scope(|s| {
                    {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter
                                .dot(
                                    DotType::Sdk,
                                    ApiName::IoGetfile,
                                    true,
                                    Duration::from_millis(10),
                                )
                                .unwrap();
                        });
                    }
                    for _ in 0..5 {
                        let dotter = dotter.to_owned();
                        s.spawn(move |_| {
                            dotter.punish().unwrap();
                        });
                    }
                });
                sleep(Duration::from_secs(5));
                {
                    let record = records_map
                        .get(&DotRecordKey::new(DotType::Sdk, ApiName::IoGetfile))
                        .unwrap();
                    assert_eq!(record.success_count(), Some(1));
                    assert_eq!(record.failed_count(), Some(0));
                    assert_eq!(record.success_avg_elapsed_duration_ms(), Some(10));
                    assert_eq!(record.failed_avg_elapsed_duration_ms(), Some(0));
                }
                {
                    let record = records_map.get(&DotRecordKey::punished()).unwrap();
                    assert_eq!(record.punished_count(), Some(5));
                }
            })
            .await?;
        });
        Ok(())
    }

    fn clear_cache() -> IOResult<()> {
        let cache_file_path = cache_dir_path_of(DOT_FILE_NAME)?;
        std::fs::remove_file(&cache_file_path).or_else(|err| {
            if err.kind() == IOErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        })
    }
}
