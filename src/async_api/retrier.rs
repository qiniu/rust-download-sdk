use super::{
    dot::{ApiName, DotType},
    download::{AsyncRangeReader, IoResult3, Result3, TriesInfo, TryingHosts},
    host_selector::HostInfo,
    RangePart,
};
use async_trait::async_trait;
use futures::future::{join_all, select, select_all, Either};
use log::{error, info};
use std::{
    future::Future,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult},
    mem::take,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{pin, sync::RwLock, time::sleep_until, time::Instant};

#[derive(Debug, Clone)]
pub(super) struct AsyncRangeReaderWithRangeReader {
    inner: AsyncRangeReader,
    max_retry_concurrency: u32,
    total_tries: usize,
}

impl AsyncRangeReaderWithRangeReader {
    pub(super) fn new(
        range_reader: AsyncRangeReader,
        max_retry_concurrency: u32,
        total_tries: usize,
    ) -> Self {
        Self {
            inner: range_reader,
            max_retry_concurrency,
            total_tries,
        }
    }

    pub(super) async fn update_urls(&self) -> bool {
        self.inner.update_urls().await
    }

    pub(super) async fn io_urls(&self) -> Vec<String> {
        self.inner.io_urls().await
    }

    pub(super) async fn read_at(&self, key: &str, pos: u64, size: u64) -> IoResult<Vec<u8>> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        self.try_with_timeout(ApiName::RangeReaderReadAt, |async_task_id| {
            RangeReaderReadAtRetrier::new(
                pos,
                size,
                key,
                async_task_id,
                &self.inner,
                TriesInfo::new(&have_tried, self.total_tries),
                &trying_hosts,
                &selected_info,
            )
        })
        .await
    }

    pub(super) async fn read_multi_ranges(
        &self,
        key: &str,
        ranges: &[(u64, u64)],
    ) -> IoResult<Vec<RangePart>> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        self.try_with_timeout(ApiName::RangeReaderReadMultiRanges, |async_task_id| {
            RangeReaderReadMultiRangesRetrier::new(
                ranges,
                key,
                async_task_id,
                &self.inner,
                TriesInfo::new(&have_tried, self.total_tries),
                &trying_hosts,
                &selected_info,
            )
        })
        .await
    }

    pub(super) async fn exist(&self, key: &str) -> IoResult<bool> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        self.try_with_timeout(ApiName::RangeReaderExist, |async_task_id| {
            RangeReaderExistRetrier::new(
                key,
                async_task_id,
                &self.inner,
                TriesInfo::new(&have_tried, self.total_tries),
                &trying_hosts,
                &selected_info,
            )
        })
        .await
    }

    pub(super) async fn file_size(&self, key: &str) -> IoResult<u64> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        self.try_with_timeout(ApiName::RangeReaderFileSize, |async_task_id| {
            RangeReaderFileSizeRetrier::new(
                key,
                async_task_id,
                &self.inner,
                TriesInfo::new(&have_tried, self.total_tries),
                &trying_hosts,
                &selected_info,
            )
        })
        .await
    }

    pub(super) async fn download(&self, key: &str) -> IoResult<Vec<u8>> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        self.try_with_timeout(ApiName::RangeReaderDownloadTo, |async_task_id| {
            RangeReaderDownloadRetrier::new(
                key,
                async_task_id,
                &self.inner,
                TriesInfo::new(&have_tried, self.total_tries),
                &trying_hosts,
                &selected_info,
            )
        })
        .await
    }

    pub(super) async fn read_last_bytes(&self, key: &str, size: u64) -> IoResult<(Vec<u8>, u64)> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        self.try_with_timeout(ApiName::RangeReaderReadLastBytes, |async_task_id| {
            RangeReaderReadLastBytesRetrier::new(
                size,
                key,
                async_task_id,
                &self.inner,
                TriesInfo::new(&have_tried, self.total_tries),
                &trying_hosts,
                &selected_info,
            )
        })
        .await
    }

    async fn try_with_timeout<
        Output,
        T: Future<Output = IoResult3<Output>> + MaybeTimeout + Unpin + Send + Sync,
        F: Fn(u32) -> T,
    >(
        &self,
        api_name: ApiName,
        f: F,
    ) -> IoResult<Output> {
        let begin_at = Instant::now();
        let result = _try_with_timeout(f, self.max_retry_concurrency).await;
        self.inner
            .dot(
                DotType::Sdk,
                api_name,
                matches!(result, TryResult::Success(_)),
                begin_at.elapsed(),
            )
            .await
            .ok();
        return result.into();

        async fn _try_with_timeout<
            Output,
            T: Future<Output = IoResult3<Output>> + MaybeTimeout + Unpin + Send + Sync,
            F: Fn(u32) -> T,
        >(
            f: F,
            max: u32,
        ) -> TryResult<Output> {
            struct FutWithIdx<
                Output,
                T: Future<Output = IoResult3<Output>> + MaybeTimeout + Unpin + Send + Sync,
            > {
                idx: usize,
                fut: T,
            }

            impl<
                    Output,
                    T: Future<Output = IoResult3<Output>> + MaybeTimeout + Unpin + Send + Sync,
                > Future for FutWithIdx<Output, T>
            {
                type Output = IoResult3<Output>;

                fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                    Pin::new(&mut self.fut).poll(cx)
                }
            }

            #[async_trait]
            impl<
                    Output,
                    T: Future<Output = IoResult3<Output>> + MaybeTimeout + Unpin + Send + Sync,
                > MaybeTimeout for FutWithIdx<Output, T>
            {
                async fn base_timeout(&self) -> Duration {
                    self.fut.base_timeout().await
                }

                async fn increase_timeout_power_if_timed_out(self) {
                    self.fut.increase_timeout_power_if_timed_out().await
                }
            }

            let last_fut = FutWithIdx { fut: f(0), idx: 0 };
            let last_base_timeout = last_fut.base_timeout().await;
            let mut all_futures = vec![last_fut];
            let mut last_error = None;

            'timeout_loop: for i in 0..max {
                let last_try = i >= max - 1;
                let fut_timeout = future_timeout(last_base_timeout, i);
                let until = Instant::now() + fut_timeout;
                info!("{{{}}} Timeout-try ({:?})", i, fut_timeout);
                loop {
                    let timeout = sleep_until(until);
                    pin!(timeout);
                    match select(timeout, select_all(take(&mut all_futures))).await {
                        Either::Left((_, futs)) => {
                            if last_try {
                                info!(
                                    "{{{}}} Try timed out ({:?}), this is the last try",
                                    i, fut_timeout
                                );
                                break 'timeout_loop;
                            } else {
                                info!(
                                    "{{{}}} Try timed out ({:?}), spawn new async task",
                                    i, fut_timeout
                                );
                                let last_fut = f(i + 1);
                                all_futures = futs.into_inner();
                                all_futures.push(FutWithIdx {
                                    fut: last_fut,
                                    idx: all_futures.len() + 1,
                                });
                                continue 'timeout_loop;
                            }
                        }
                        Either::Right(((got_result, idx, rest_futures), _)) => match got_result {
                            Result3::Ok(output) => {
                                info!("{{{}/{}}} Try succeed", idx, i);
                                punish_all_timed_out_futures(
                                    rest_futures.into_iter().filter(|f| f.idx < idx),
                                )
                                .await;
                                return TryResult::Success(output);
                            }
                            Result3::Err(err) => {
                                info!("{{{}/{}}} Try error: {:?}", idx, i, err);
                                punish_all_timed_out_futures(
                                    rest_futures.into_iter().filter(|f| f.idx < idx),
                                )
                                .await;
                                return TryResult::Error(err);
                            }
                            Result3::NoMoreTries(maybe_err) => {
                                info!(
                            "{{{}/{}}} No more tries: {:?}, will wait for the other async tasks",
                            idx, i, maybe_err
                        );
                                if last_error.is_none() {
                                    last_error = maybe_err;
                                }
                                all_futures = rest_futures;
                            }
                        },
                    }
                }
            }

            error!("All {} async tasks are timed out", max);
            punish_all_timed_out_futures(take(&mut all_futures)).await;

            return if let Some(err) = last_error {
                TryResult::Error(err)
            } else {
                TryResult::AllTimedOut
            };

            async fn punish_all_timed_out_futures<
                I: IntoIterator<Item = Item>,
                Item: MaybeTimeout,
            >(
                iter: I,
            ) {
                join_all(
                    iter.into_iter()
                        .map(|fut| async move { fut.increase_timeout_power_if_timed_out().await }),
                )
                .await;
            }
        }
    }
}

#[async_trait]
trait MaybeTimeout {
    async fn base_timeout(&self) -> Duration;
    async fn increase_timeout_power_if_timed_out(self);
}

#[derive(Debug)]
enum TryResult<T> {
    Success(T),
    Error(IoError),
    AllTimedOut,
}

impl<T> From<TryResult<T>> for IoResult<T> {
    fn from(try_result: TryResult<T>) -> Self {
        match try_result {
            TryResult::Success(value) => Ok(value),
            TryResult::Error(err) => Err(err),
            TryResult::AllTimedOut => Err(IoError::new(
                IoErrorKind::TimedOut,
                "All concurrency requests are timed out",
            )),
        }
    }
}

fn future_timeout(last_base_timeout: Duration, index: u32) -> Duration {
    last_base_timeout * 2u32.pow(index)
}

struct SelectedHostInfoInner {
    host_info: HostInfo,
    selected_at: Instant,
}

#[derive(Clone, Default)]
struct SelectedHostInfo(Arc<RwLock<Option<SelectedHostInfoInner>>>);

struct RangeReaderRetrier<'a, T> {
    range_reader: &'a AsyncRangeReader,
    selected_info: &'a SelectedHostInfo,
    future: Pin<Box<dyn Future<Output = IoResult3<T>> + Send + Sync + 'a>>,
}

impl<T> Future for RangeReaderRetrier<'_, T> {
    type Output = IoResult3<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.future).poll(cx)
    }
}

#[async_trait]
impl<T> MaybeTimeout for RangeReaderRetrier<'_, T> {
    async fn increase_timeout_power_if_timed_out(self) {
        if let Some(selected_info) = self.selected_info.0.read().await.as_ref() {
            if selected_info.selected_at.elapsed() > selected_info.host_info.timeout() {
                self.range_reader
                    .increase_timeout_power_by(
                        selected_info.host_info.host(),
                        selected_info.host_info.timeout_power(),
                    )
                    .await;
            }
        }
    }

    async fn base_timeout(&self) -> Duration {
        self.range_reader.base_timeout().await
    }
}

struct RangeReaderReadAtRetrier<'a>(RangeReaderRetrier<'a, Vec<u8>>);

impl<'a> RangeReaderReadAtRetrier<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        pos: u64,
        size: u64,
        key: &'a str,
        async_task_id: u32,
        range_reader: &'a AsyncRangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .read_at(
                        pos,
                        size,
                        key,
                        async_task_id,
                        tries_info,
                        trying_hosts,
                        |host| async move { set_selected_info(selected_info, host).await },
                    )
                    .await
            }),
        })
    }
}

impl Future for RangeReaderReadAtRetrier<'_> {
    type Output = IoResult3<Vec<u8>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

#[async_trait]
impl MaybeTimeout for RangeReaderReadAtRetrier<'_> {
    async fn increase_timeout_power_if_timed_out(self) {
        self.0.increase_timeout_power_if_timed_out().await
    }

    async fn base_timeout(&self) -> Duration {
        self.0.base_timeout().await
    }
}

struct RangeReaderReadMultiRangesRetrier<'a>(RangeReaderRetrier<'a, Vec<RangePart>>);

impl<'a> RangeReaderReadMultiRangesRetrier<'a> {
    fn new(
        ranges: &'a [(u64, u64)],
        key: &'a str,
        async_task_id: u32,
        range_reader: &'a AsyncRangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .read_multi_ranges(
                        ranges,
                        key,
                        async_task_id,
                        tries_info,
                        trying_hosts,
                        |host| async move { set_selected_info(selected_info, host).await },
                    )
                    .await
            }),
        })
    }
}

impl Future for RangeReaderReadMultiRangesRetrier<'_> {
    type Output = IoResult3<Vec<RangePart>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

#[async_trait]
impl MaybeTimeout for RangeReaderReadMultiRangesRetrier<'_> {
    async fn increase_timeout_power_if_timed_out(self) {
        self.0.increase_timeout_power_if_timed_out().await
    }

    async fn base_timeout(&self) -> Duration {
        self.0.base_timeout().await
    }
}

struct RangeReaderExistRetrier<'a>(RangeReaderRetrier<'a, bool>);

impl<'a> RangeReaderExistRetrier<'a> {
    fn new(
        key: &'a str,
        async_task_id: u32,
        range_reader: &'a AsyncRangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .exist(
                        key,
                        async_task_id,
                        tries_info,
                        trying_hosts,
                        |host| async move { set_selected_info(selected_info, host).await },
                    )
                    .await
            }),
        })
    }
}

impl Future for RangeReaderExistRetrier<'_> {
    type Output = IoResult3<bool>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

#[async_trait]
impl MaybeTimeout for RangeReaderExistRetrier<'_> {
    async fn increase_timeout_power_if_timed_out(self) {
        self.0.increase_timeout_power_if_timed_out().await
    }

    async fn base_timeout(&self) -> Duration {
        self.0.base_timeout().await
    }
}

struct RangeReaderFileSizeRetrier<'a>(RangeReaderRetrier<'a, u64>);

impl<'a> RangeReaderFileSizeRetrier<'a> {
    fn new(
        key: &'a str,
        async_task_id: u32,
        range_reader: &'a AsyncRangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .file_size(
                        key,
                        async_task_id,
                        tries_info,
                        trying_hosts,
                        |host| async move { set_selected_info(selected_info, host).await },
                    )
                    .await
            }),
        })
    }
}

impl Future for RangeReaderFileSizeRetrier<'_> {
    type Output = IoResult3<u64>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

#[async_trait]
impl MaybeTimeout for RangeReaderFileSizeRetrier<'_> {
    async fn increase_timeout_power_if_timed_out(self) {
        self.0.increase_timeout_power_if_timed_out().await
    }

    async fn base_timeout(&self) -> Duration {
        self.0.base_timeout().await
    }
}

struct RangeReaderDownloadRetrier<'a>(RangeReaderRetrier<'a, Vec<u8>>);

impl<'a> RangeReaderDownloadRetrier<'a> {
    fn new(
        key: &'a str,
        async_task_id: u32,
        range_reader: &'a AsyncRangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .download(
                        key,
                        async_task_id,
                        tries_info,
                        trying_hosts,
                        |host| async move { set_selected_info(selected_info, host).await },
                    )
                    .await
            }),
        })
    }
}

impl Future for RangeReaderDownloadRetrier<'_> {
    type Output = IoResult3<Vec<u8>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

#[async_trait]
impl MaybeTimeout for RangeReaderDownloadRetrier<'_> {
    async fn increase_timeout_power_if_timed_out(self) {
        self.0.increase_timeout_power_if_timed_out().await
    }

    async fn base_timeout(&self) -> Duration {
        self.0.base_timeout().await
    }
}

struct RangeReaderReadLastBytesRetrier<'a>(RangeReaderRetrier<'a, (Vec<u8>, u64)>);

impl<'a> RangeReaderReadLastBytesRetrier<'a> {
    fn new(
        size: u64,
        key: &'a str,
        async_task_id: u32,
        range_reader: &'a AsyncRangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .read_last_bytes(
                        size,
                        key,
                        async_task_id,
                        tries_info,
                        trying_hosts,
                        |host| async move { set_selected_info(selected_info, host).await },
                    )
                    .await
            }),
        })
    }
}

impl Future for RangeReaderReadLastBytesRetrier<'_> {
    type Output = IoResult3<(Vec<u8>, u64)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

#[async_trait]
impl MaybeTimeout for RangeReaderReadLastBytesRetrier<'_> {
    async fn increase_timeout_power_if_timed_out(self) {
        self.0.increase_timeout_power_if_timed_out().await
    }

    async fn base_timeout(&self) -> Duration {
        self.0.base_timeout().await
    }
}

async fn set_selected_info(selected_info: &SelectedHostInfo, host: HostInfo) {
    *selected_info.0.write().await = Some(SelectedHostInfoInner {
        host_info: host.to_owned(),
        selected_at: Instant::now(),
    });
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            cache_dir::cache_dir_path_of,
            dot::{AsyncDotRecordsMap, DotRecordKey, DotRecords, DOT_FILE_NAME},
            download::AsyncRangeReaderBuilder,
        },
        *,
    };
    use crate::{base::download::RangeReaderBuilder as BaseRangeReaderBuilder, Credential};
    use futures::{channel::oneshot::channel, ready};
    use hyper::Body;
    use reqwest::header::{HeaderValue, AUTHORIZATION};
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};
    use tokio::{
        fs::remove_file,
        spawn,
        time::{sleep, Sleep},
    };
    use warp::{path, reply::Response, Filter};

    struct FakedRetrier<T> {
        base_timeout: Duration,
        result: Option<IoResult3<T>>,
        sleep: Pin<Box<Sleep>>,
        punished: Arc<AtomicBool>,
    }

    impl<T> FakedRetrier<T> {
        fn new(
            base_timeout: Duration,
            result: IoResult3<T>,
            delay: Duration,
            punished: Arc<AtomicBool>,
        ) -> Self {
            Self {
                base_timeout,
                punished,
                result: Some(result),
                sleep: Box::pin(sleep(delay)),
            }
        }
    }

    impl<T: Clone + Unpin> Future for FakedRetrier<T> {
        type Output = IoResult3<T>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            ready!(self.sleep.as_mut().poll(cx));
            Poll::Ready(self.result.take().unwrap())
        }
    }

    #[async_trait]
    impl<T: Send + Sync> MaybeTimeout for FakedRetrier<T> {
        async fn base_timeout(&self) -> Duration {
            self.base_timeout
        }

        async fn increase_timeout_power_if_timed_out(self) {
            self.punished.store(true, Relaxed);
        }
    }

    macro_rules! starts_with_server {
        ($io_addr:ident, $monitor_addr:ident, $io_routes:ident, $records_map:ident, $code:block) => {{
            let (io_tx, io_rx) = channel();
            let (monitor_tx, monitor_rx) = channel();
            let ($io_addr, io_server) = warp::serve($io_routes).bind_with_graceful_shutdown(
                ([127, 0, 0, 1], 0),
                async move {
                    io_rx.await.unwrap();
                },
            );
            let $records_map = Arc::new(AsyncDotRecordsMap::default());
            let monitor_routes = {
                let records_map = $records_map.to_owned();
                path!("v1" / "stat")
                    .and(warp::header::value(AUTHORIZATION.as_str()))
                    .and(warp::body::json())
                    .then(move |authorization: HeaderValue, records: DotRecords| {
                        assert!(authorization.to_str().unwrap().starts_with("UpToken "));
                        let records_map = records_map.to_owned();
                        async move {
                            records_map.merge_with_records(records).await;
                            Response::new(Body::empty())
                        }
                    })
            };
            let ($monitor_addr, monitor_server) = warp::serve(monitor_routes)
                .bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async move {
                    monitor_rx.await.unwrap();
                });
            spawn(io_server);
            spawn(monitor_server);
            sleep(Duration::from_secs(1)).await;
            $code;
            io_tx.send(()).unwrap();
            monitor_tx.send(()).unwrap();
        }};
    }

    #[tokio::test]
    async fn test_try_with_timeout() -> anyhow::Result<()> {
        env_logger::try_init().ok();
        clear_cache().await?;

        let io_routes = path!("file").map(|| Response::new("1234567890".into()));

        starts_with_server!(io_addr, monitor_addr, io_routes, records_map, {
            let io_urls = vec![format!("http://{}", io_addr)];
            let downloader = AsyncRangeReaderWithRangeReader::new(
                AsyncRangeReaderBuilder::from(
                    BaseRangeReaderBuilder::new(
                        "bucket".to_owned(),
                        "file".to_owned(),
                        get_credential(),
                        io_urls,
                    )
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .monitor_urls(vec!["http://".to_owned() + &monitor_addr.to_string()])
                    .dot_interval(Duration::from_millis(0))
                    .max_dot_buffer_size(1),
                )
                .build(),
                2,
                0,
            );

            let counter = Arc::new(AtomicU32::new(0));
            let retrier_punished_1 = Arc::new(AtomicBool::new(false));
            let retrier_punished_2 = Arc::new(AtomicBool::new(false));
            let result = {
                let counter = counter.to_owned();
                let retrier_punished_1 = retrier_punished_1.to_owned();
                let retrier_punished_2 = retrier_punished_2.to_owned();
                downloader.try_with_timeout(ApiName::IoGetfile, move |count| {
                    counter.store(count + 1, Relaxed);
                    let retrier_punished_1 = retrier_punished_1.to_owned();
                    let retrier_punished_2 = retrier_punished_2.to_owned();
                    match count {
                        0 => FakedRetrier::new(
                            Duration::from_millis(1000),
                            Result3::Ok(1),
                            Duration::from_millis(1800),
                            retrier_punished_1,
                        ),
                        1 => FakedRetrier::new(
                            Duration::from_millis(1000),
                            Result3::Ok(2),
                            Duration::from_millis(500),
                            retrier_punished_2,
                        ),
                        _ => unreachable!(),
                    }
                })
            }
            .await
            .unwrap();

            assert_eq!(result, 2);
            assert!(retrier_punished_1.load(Relaxed));
            assert!(!retrier_punished_2.load(Relaxed));
            assert_eq!(counter.load(Relaxed), 2);

            sleep(Duration::from_secs(5)).await;
            {
                let record = records_map
                    .read_async(
                        &DotRecordKey::new(DotType::Sdk, ApiName::IoGetfile),
                        |_, record| record.to_owned(),
                    )
                    .await
                    .unwrap();
                assert_eq!(record.success_count(), Some(1));
                assert_eq!(record.failed_count(), Some(0));
            }

            counter.store(0, Relaxed);
            retrier_punished_1.store(false, Relaxed);
            retrier_punished_2.store(false, Relaxed);

            let result = {
                let counter = counter.to_owned();
                let retrier_punished_1 = retrier_punished_1.to_owned();
                let retrier_punished_2 = retrier_punished_2.to_owned();
                downloader.try_with_timeout(ApiName::IoGetfile, move |count| {
                    counter.store(count + 1, Relaxed);
                    let retrier_punished_1 = retrier_punished_1.to_owned();
                    let retrier_punished_2 = retrier_punished_2.to_owned();
                    match count {
                        0 => FakedRetrier::new(
                            Duration::from_millis(1000),
                            Result3::Ok(1),
                            Duration::from_millis(1200),
                            retrier_punished_1,
                        ),
                        1 => FakedRetrier::new(
                            Duration::from_millis(1000),
                            Result3::Ok(2),
                            Duration::from_millis(800),
                            retrier_punished_2,
                        ),
                        _ => unreachable!(),
                    }
                })
            }
            .await
            .unwrap();

            assert_eq!(result, 1);
            assert!(!retrier_punished_1.load(Relaxed));
            assert!(!retrier_punished_2.load(Relaxed));
            assert_eq!(counter.load(Relaxed), 2);

            sleep(Duration::from_secs(5)).await;
            {
                let record = records_map
                    .read_async(
                        &DotRecordKey::new(DotType::Sdk, ApiName::IoGetfile),
                        |_, record| record.to_owned(),
                    )
                    .await
                    .unwrap();
                assert_eq!(record.success_count(), Some(2));
                assert_eq!(record.failed_count(), Some(0));
            }

            let err = {
                downloader.try_with_timeout(ApiName::IoGetfile, move |count| {
                    assert!(count < 2);
                    FakedRetrier::new(
                        Duration::from_millis(1000),
                        Result3::Ok(count),
                        Duration::from_millis(4000),
                        Arc::new(AtomicBool::new(false)),
                    )
                })
            }
            .await
            .unwrap_err();

            assert_eq!(err.kind(), IoErrorKind::TimedOut);
            assert_eq!(&err.to_string(), "All concurrency requests are timed out");

            sleep(Duration::from_secs(5)).await;
            {
                let record = records_map
                    .read_async(
                        &DotRecordKey::new(DotType::Sdk, ApiName::IoGetfile),
                        |_, record| record.to_owned(),
                    )
                    .await
                    .unwrap();
                assert_eq!(record.success_count(), Some(2));
                assert_eq!(record.failed_count(), Some(1));
            }
        });

        Ok(())
    }

    fn get_credential() -> Credential {
        Credential::new("1234567890", "abcdefghijk")
    }

    async fn clear_cache() -> IoResult<()> {
        let cache_file_path = cache_dir_path_of("query-cache.json").await?;
        remove_file(&cache_file_path).await.or_else(|err| {
            if err.kind() == IoErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        })?;
        let dot_file_path = cache_dir_path_of(DOT_FILE_NAME).await?;
        remove_file(&dot_file_path).await.or_else(|err| {
            if err.kind() == IoErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        })?;
        Ok(())
    }
}
