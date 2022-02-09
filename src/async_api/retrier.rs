use super::{
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
    max_retry_concurrency: usize,
    total_tries: usize,
}

impl AsyncRangeReaderWithRangeReader {
    pub(super) fn new(
        range_reader: AsyncRangeReader,
        max_retry_concurrency: usize,
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

    pub(super) async fn read_at(&self, pos: u64, size: u64) -> IoResult<Vec<u8>> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        try_with_timeout(
            |async_task_id| {
                RangeReaderReadAtRetrier::new(
                    pos,
                    size,
                    async_task_id,
                    &self.inner,
                    TriesInfo::new(&have_tried, self.total_tries),
                    &trying_hosts,
                    &selected_info,
                )
            },
            self.max_retry_concurrency,
        )
        .await
        .into()
    }

    pub(super) async fn read_multi_ranges(
        &self,
        ranges: &[(u64, u64)],
    ) -> IoResult<Vec<RangePart>> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        try_with_timeout(
            |async_task_id| {
                RangeReaderReadMultiRangesRetrier::new(
                    ranges,
                    async_task_id,
                    &self.inner,
                    TriesInfo::new(&have_tried, self.total_tries),
                    &trying_hosts,
                    &selected_info,
                )
            },
            self.max_retry_concurrency,
        )
        .await
        .into()
    }

    pub(super) async fn exist(&self) -> IoResult<bool> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        try_with_timeout(
            |async_task_id| {
                RangeReaderExistRetrier::new(
                    async_task_id,
                    &self.inner,
                    TriesInfo::new(&have_tried, self.total_tries),
                    &trying_hosts,
                    &selected_info,
                )
            },
            self.max_retry_concurrency,
        )
        .await
        .into()
    }

    pub(super) async fn file_size(&self) -> IoResult<u64> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        try_with_timeout(
            |async_task_id| {
                RangeReaderFileSizeRetrier::new(
                    async_task_id,
                    &self.inner,
                    TriesInfo::new(&have_tried, self.total_tries),
                    &trying_hosts,
                    &selected_info,
                )
            },
            self.max_retry_concurrency,
        )
        .await
        .into()
    }

    pub(super) async fn download(&self) -> IoResult<Vec<u8>> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        try_with_timeout(
            |async_task_id| {
                RangeReaderDownloadRetrier::new(
                    async_task_id,
                    &self.inner,
                    TriesInfo::new(&have_tried, self.total_tries),
                    &trying_hosts,
                    &selected_info,
                )
            },
            self.max_retry_concurrency,
        )
        .await
        .into()
    }

    pub(super) async fn read_last_bytes(&self, size: u64) -> IoResult<(Vec<u8>, u64)> {
        let have_tried: AtomicUsize = Default::default();
        let trying_hosts: TryingHosts = Default::default();
        let selected_info: SelectedHostInfo = Default::default();
        try_with_timeout(
            |async_task_id| {
                RangeReaderReadLastBytesRetrier::new(
                    size,
                    async_task_id,
                    &self.inner,
                    TriesInfo::new(&have_tried, self.total_tries),
                    &trying_hosts,
                    &selected_info,
                )
            },
            self.max_retry_concurrency,
        )
        .await
        .into()
    }
}

#[async_trait]
trait MayBeTimeout {
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

async fn try_with_timeout<
    Output,
    T: Future<Output = IoResult3<Output>> + MayBeTimeout + Unpin + Send + Sync,
    F: Fn(usize) -> T,
>(
    f: F,
    max: usize,
) -> TryResult<Output> {
    struct FutWithIdx<
        Output,
        T: Future<Output = IoResult3<Output>> + MayBeTimeout + Unpin + Send + Sync,
    > {
        idx: usize,
        fut: T,
    }

    impl<Output, T: Future<Output = IoResult3<Output>> + MayBeTimeout + Unpin + Send + Sync> Future
        for FutWithIdx<Output, T>
    {
        type Output = IoResult3<Output>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.fut).poll(cx)
        }
    }

    #[async_trait]
    impl<Output, T: Future<Output = IoResult3<Output>> + MayBeTimeout + Unpin + Send + Sync>
        MayBeTimeout for FutWithIdx<Output, T>
    {
        async fn base_timeout(&self) -> Duration {
            self.fut.base_timeout().await
        }

        async fn increase_timeout_power_if_timed_out(self) {
            self.fut.increase_timeout_power_if_timed_out().await
        }
    }

    let last_fut = FutWithIdx { fut: f(0), idx: 0 };
    let mut last_base_timeout = last_fut.base_timeout().await;
    let mut all_futures = vec![last_fut];
    let mut last_error = None;

    'timeout_loop: for i in 0..max {
        let last_try = i >= max - 1;
        let until = Instant::now() + last_base_timeout;
        info!("{{{}}} Timeout-try ({:?})", i, last_base_timeout);
        loop {
            let timeout = sleep_until(until);
            pin!(timeout);
            match select(timeout, select_all(take(&mut all_futures))).await {
                Either::Left((_, futs)) => {
                    if last_try {
                        info!(
                            "{{{}}} Try timed out ({:?}), this is the last try",
                            i, last_base_timeout
                        );
                        break 'timeout_loop;
                    } else {
                        info!(
                            "{{{}}} Try timed out ({:?}), spawn new async task",
                            i, last_base_timeout
                        );
                        let last_fut = f(i + 1);
                        last_base_timeout = last_fut.base_timeout().await;
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

    async fn punish_all_timed_out_futures<I: IntoIterator<Item = Item>, Item: MayBeTimeout>(
        iter: I,
    ) {
        join_all(
            iter.into_iter()
                .map(|fut| async move { fut.increase_timeout_power_if_timed_out().await }),
        )
        .await;
    }
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
impl<T> MayBeTimeout for RangeReaderRetrier<'_, T> {
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
    fn new(
        pos: u64,
        size: u64,
        async_task_id: usize,
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
impl MayBeTimeout for RangeReaderReadAtRetrier<'_> {
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
        async_task_id: usize,
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
impl MayBeTimeout for RangeReaderReadMultiRangesRetrier<'_> {
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
        async_task_id: usize,
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
                    .exist(async_task_id, tries_info, trying_hosts, |host| async move {
                        set_selected_info(selected_info, host).await
                    })
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
impl MayBeTimeout for RangeReaderExistRetrier<'_> {
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
        async_task_id: usize,
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
                    .file_size(async_task_id, tries_info, trying_hosts, |host| async move {
                        set_selected_info(selected_info, host).await
                    })
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
impl MayBeTimeout for RangeReaderFileSizeRetrier<'_> {
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
        async_task_id: usize,
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
                    .download(async_task_id, tries_info, trying_hosts, |host| async move {
                        set_selected_info(selected_info, host).await
                    })
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
impl MayBeTimeout for RangeReaderDownloadRetrier<'_> {
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
        async_task_id: usize,
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
impl MayBeTimeout for RangeReaderReadLastBytesRetrier<'_> {
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
    use super::*;
    use futures::ready;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
    use tokio::time::{sleep, Sleep};

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
    impl<T: Send + Sync> MayBeTimeout for FakedRetrier<T> {
        async fn base_timeout(&self) -> Duration {
            self.base_timeout
        }

        async fn increase_timeout_power_if_timed_out(self) {
            self.punished.store(true, Relaxed);
        }
    }

    #[tokio::test]
    async fn test_try_with_timeout() -> anyhow::Result<()> {
        let counter = Arc::new(AtomicUsize::new(0));
        let retrier_punished_1 = Arc::new(AtomicBool::new(false));
        let retrier_punished_2 = Arc::new(AtomicBool::new(false));
        let result = {
            let counter = counter.to_owned();
            let retrier_punished_1 = retrier_punished_1.to_owned();
            let retrier_punished_2 = retrier_punished_2.to_owned();
            try_with_timeout(
                move |count| {
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
                },
                2,
            )
        }
        .await;

        assert!(matches!(result, TryResult::Success(2)));
        assert!(retrier_punished_1.load(Relaxed));
        assert!(!retrier_punished_2.load(Relaxed));
        assert_eq!(counter.load(Relaxed), 2);

        counter.store(0, Relaxed);
        retrier_punished_1.store(false, Relaxed);
        retrier_punished_2.store(false, Relaxed);

        let result = {
            let counter = counter.to_owned();
            let retrier_punished_1 = retrier_punished_1.to_owned();
            let retrier_punished_2 = retrier_punished_2.to_owned();
            try_with_timeout(
                move |count| {
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
                },
                2,
            )
        }
        .await;

        assert!(matches!(result, TryResult::Success(1)));
        assert!(!retrier_punished_1.load(Relaxed));
        assert!(!retrier_punished_2.load(Relaxed));
        assert_eq!(counter.load(Relaxed), 2);

        let result = {
            try_with_timeout(
                move |count| {
                    assert!(count < 5);
                    FakedRetrier::new(
                        Duration::from_millis(1000),
                        Result3::Ok(count),
                        Duration::from_millis(6000),
                        Arc::new(AtomicBool::new(false)),
                    )
                },
                2,
            )
        }
        .await;

        assert!(matches!(result, TryResult::AllTimedOut));

        Ok(())
    }
}
