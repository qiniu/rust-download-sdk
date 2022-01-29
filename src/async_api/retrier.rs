use super::{
    download::{IoResult3, RangeReader, Result3, TriesInfo, TryingHosts},
    host_selector::HostInfo,
    RangePart,
};
use async_trait::async_trait;
use futures::{
    future::{join_all, select, select_all, BoxFuture, Either},
    FutureExt,
};
use std::{
    collections::HashSet,
    future::Future,
    io::{Error as IoError, Result as IoResult},
    mem::take,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    pin,
    sync::Mutex,
    time::Instant,
    time::{sleep, sleep_until},
};

#[async_trait]
pub(super) trait MayBeTimeout {
    fn base_timeout(&self) -> Duration;
    async fn increase_timeout_power_if_timed_out(self);
}

#[derive(Debug)]
pub(super) enum TryResult<T> {
    Success(T),
    Error(IoError),
    AllTimedOut,
}

pub(super) async fn try_with_timeout<
    Output,
    T: Future<Output = IoResult3<Output>> + MayBeTimeout + Unpin + Send,
    F: Fn() -> T,
>(
    f: F,
    max: usize,
) -> TryResult<Output> {
    struct FutWithIdx<Output, T: Future<Output = IoResult3<Output>> + MayBeTimeout + Unpin + Send> {
        idx: usize,
        fut: T,
    }

    impl<Output, T: Future<Output = IoResult3<Output>> + MayBeTimeout + Unpin + Send> Future
        for FutWithIdx<Output, T>
    {
        type Output = IoResult3<Output>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.fut).poll(cx)
        }
    }

    #[async_trait]
    impl<Output, T: Future<Output = IoResult3<Output>> + MayBeTimeout + Unpin + Send> MayBeTimeout
        for FutWithIdx<Output, T>
    {
        fn base_timeout(&self) -> Duration {
            self.fut.base_timeout()
        }

        async fn increase_timeout_power_if_timed_out(self) {
            self.fut.increase_timeout_power_if_timed_out().await
        }
    }

    let last_fut = FutWithIdx { fut: f(), idx: 0 };
    let mut last_base_timeout = last_fut.base_timeout();
    let mut all_futures = vec![last_fut];
    let mut last_error = None;

    'timeout_loop: for i in 0..max {
        let last_try = i >= max - 1;
        let until = Instant::now() + last_base_timeout;
        loop {
            let timeout = sleep_until(until);
            pin!(timeout);
            match select(timeout, select_all(take(&mut all_futures))).await {
                Either::Left((_, futs)) => {
                    if last_try {
                        break 'timeout_loop;
                    } else {
                        let last_fut = f();
                        last_base_timeout = last_fut.base_timeout();
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
                        punish_all_timed_out_futures(
                            rest_futures.into_iter().filter(|f| f.idx < idx),
                        )
                        .await;
                        return TryResult::Success(output);
                    }
                    Result3::Err(err) => {
                        punish_all_timed_out_futures(
                            rest_futures.into_iter().filter(|f| f.idx < idx),
                        )
                        .await;
                        return TryResult::Error(err);
                    }
                    Result3::NoMoreTries(maybe_err) => {
                        if last_error.is_none() {
                            last_error = maybe_err;
                        }
                        all_futures = rest_futures;
                    }
                },
            }
        }
    }

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

#[derive(Clone)]
struct SelectedHostInfo(Arc<Mutex<SelectedHostInfoInner>>);

struct RangeReaderRetrier<'a, T> {
    range_reader: &'a RangeReader,
    selected_info: &'a SelectedHostInfo,
    future: BoxFuture<'a, IoResult3<T>>,
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
        let selected_info = self.selected_info.0.lock().await;

        if selected_info.selected_at.elapsed() > selected_info.host_info.timeout() {
            self.range_reader.increase_timeout_power_by(
                selected_info.host_info.host(),
                selected_info.host_info.timeout_power(),
            );
        }
    }

    fn base_timeout(&self) -> Duration {
        self.range_reader.base_timeout()
    }
}

struct RangeReaderReadAtRetrier<'a>(RangeReaderRetrier<'a, Vec<u8>>);

impl<'a> RangeReaderReadAtRetrier<'a> {
    fn new(
        pos: u64,
        size: u64,
        range_reader: &'a RangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .read_at(pos, size, tries_info, trying_hosts, |host| async move {
                        set_selected_info(selected_info, host).await
                    })
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

    fn base_timeout(&self) -> Duration {
        self.0.base_timeout()
    }
}

struct RangeReaderReadMultiRangesRetrier<'a>(RangeReaderRetrier<'a, Vec<RangePart>>);

impl<'a> RangeReaderReadMultiRangesRetrier<'a> {
    fn new(
        ranges: &'a [(u64, u64)],
        range_reader: &'a RangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .read_multi_ranges(ranges, tries_info, trying_hosts, |host| async move {
                        set_selected_info(selected_info, host).await
                    })
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

    fn base_timeout(&self) -> Duration {
        self.0.base_timeout()
    }
}

struct RangeReaderExistRetrier<'a>(RangeReaderRetrier<'a, bool>);

impl<'a> RangeReaderExistRetrier<'a> {
    fn new(
        range_reader: &'a RangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .exist(tries_info, trying_hosts, |host| async move {
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

    fn base_timeout(&self) -> Duration {
        self.0.base_timeout()
    }
}

struct RangeReaderFileSizeRetrier<'a>(RangeReaderRetrier<'a, u64>);

impl<'a> RangeReaderFileSizeRetrier<'a> {
    fn new(
        range_reader: &'a RangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .file_size(tries_info, trying_hosts, |host| async move {
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

    fn base_timeout(&self) -> Duration {
        self.0.base_timeout()
    }
}

struct RangeReaderDownloadRetrier<'a>(RangeReaderRetrier<'a, Vec<u8>>);

impl<'a> RangeReaderDownloadRetrier<'a> {
    fn new(
        range_reader: &'a RangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .download(tries_info, trying_hosts, |host| async move {
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

    fn base_timeout(&self) -> Duration {
        self.0.base_timeout()
    }
}

struct RangeReaderReadLastBytesRetrier<'a>(RangeReaderRetrier<'a, (Vec<u8>, u64)>);

impl<'a> RangeReaderReadLastBytesRetrier<'a> {
    fn new(
        size: u64,
        range_reader: &'a RangeReader,
        tries_info: TriesInfo<'a>,
        trying_hosts: &'a TryingHosts,
        selected_info: &'a SelectedHostInfo,
    ) -> Self {
        Self(RangeReaderRetrier {
            selected_info,
            range_reader,
            future: Box::pin(async move {
                range_reader
                    .read_last_bytes(size, tries_info, trying_hosts, |host| async move {
                        set_selected_info(selected_info, host).await
                    })
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

    fn base_timeout(&self) -> Duration {
        self.0.base_timeout()
    }
}

async fn set_selected_info(selected_info: &SelectedHostInfo, host: HostInfo) {
    *selected_info.0.lock().await = SelectedHostInfoInner {
        host_info: host.to_owned(),
        selected_at: Instant::now(),
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{pin_mut, ready};
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
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
    impl<T: Send> MayBeTimeout for FakedRetrier<T> {
        fn base_timeout(&self) -> Duration {
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
                move || {
                    let count = counter.fetch_add(1, Relaxed);
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
                move || {
                    let count = counter.fetch_add(1, Relaxed);
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

        counter.store(0, Relaxed);
        let result = {
            let counter = counter.to_owned();
            try_with_timeout(
                move || {
                    let count = counter.fetch_add(1, Relaxed);
                    assert!(count < 6);
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
