use super::{
    super::{Config, Credential},
    download::{AsyncRangeReader, AsyncRangeReaderBuilder},
    retrier::AsyncRangeReaderWithRangeReader,
    RangePart,
};
use futures::{
    pin_mut, ready,
    task::{waker, ArcWake},
};
use log::{debug, error, trace};
use positioned_io::ReadAt;
use std::{
    future::Future,
    io::{Error as IoError, Result as IoResult, Write},
    sync::Arc,
    task::{Context, Poll},
    thread::{current as current_thread, park as park_thread},
    thread::{Builder as ThreadBuilder, JoinHandle, Thread},
    time::Duration,
};
use tokio::{
    macros::support::poll_fn,
    runtime::Builder as TokioRuntimeBuilder,
    spawn as spawn_tokio,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot::{channel, Sender},
    },
};

#[derive(Debug)]
pub(crate) struct RangeReaderBuilder {
    inner: AsyncRangeReaderBuilder,
    max_retry_concurrency: usize,
    total_tries: usize,
}

impl RangeReaderBuilder {
    pub(crate) fn new(
        bucket: String,
        key: String,
        credential: Credential,
        io_urls: Vec<String>,
    ) -> Self {
        Self {
            inner: AsyncRangeReaderBuilder::new(bucket, key, credential, io_urls),
            max_retry_concurrency: 5,
            total_tries: 10,
        }
    }

    pub(crate) fn uc_urls(self, urls: Vec<String>) -> Self {
        self.with_inner(|builder| builder.uc_urls(urls))
    }

    pub(crate) fn monitor_urls(self, urls: Vec<String>) -> Self {
        self.with_inner(|builder| builder.monitor_urls(urls))
    }

    pub(crate) fn uc_tries(self, tries: usize) -> Self {
        self.with_inner(|builder| builder.uc_tries(tries))
    }

    pub(crate) fn dot_tries(self, tries: usize) -> Self {
        self.with_inner(|builder| builder.dot_tries(tries))
    }

    pub(crate) fn update_interval(self, interval: Duration) -> Self {
        self.with_inner(|builder| builder.update_interval(interval))
    }

    pub(crate) fn punish_duration(self, interval: Duration) -> Self {
        self.with_inner(|builder| builder.punish_duration(interval))
    }

    pub(crate) fn base_timeout(self, timeout: Duration) -> Self {
        self.with_inner(|builder| builder.base_timeout(timeout))
    }

    pub(crate) fn connect_timeout(self, timeout: Duration) -> Self {
        self.with_inner(|builder| builder.connect_timeout(timeout))
    }

    pub(crate) fn max_punished_times(self, max_times: usize) -> Self {
        self.with_inner(|builder| builder.max_punished_times(max_times))
    }

    pub(crate) fn max_punished_hosts_percent(self, percent: u8) -> Self {
        self.with_inner(|builder| builder.max_punished_hosts_percent(percent))
    }

    pub(crate) fn use_getfile_api(self, use_getfile_api: bool) -> Self {
        self.with_inner(|builder| builder.use_getfile_api(use_getfile_api))
    }

    pub(crate) fn normalize_key(self, normalize_key: bool) -> Self {
        self.with_inner(|builder| builder.normalize_key(normalize_key))
    }

    pub(crate) fn private_url_lifetime(self, private_url_lifetime: Option<Duration>) -> Self {
        self.with_inner(|builder| builder.private_url_lifetime(private_url_lifetime))
    }

    pub(crate) fn dot_interval(self, dot_interval: Duration) -> Self {
        self.with_inner(|builder| builder.dot_interval(dot_interval))
    }

    pub(crate) fn max_dot_buffer_size(self, max_dot_buffer_size: u64) -> Self {
        self.with_inner(|builder| builder.max_dot_buffer_size(max_dot_buffer_size))
    }

    pub(crate) fn use_https(self, use_https: bool) -> Self {
        self.with_inner(|builder| builder.use_https(use_https))
    }

    pub(crate) fn io_tries(mut self, tries: usize) -> Self {
        self.total_tries = tries;
        self
    }

    pub(crate) fn max_retry_concurrency(mut self, concurrency: usize) -> Self {
        self.max_retry_concurrency = concurrency;
        self
    }

    pub(crate) fn build(self) -> RangeReader {
        RangeReader(RangeReaderHandle::new(self))
    }

    fn with_inner(
        mut self,
        func: impl FnOnce(AsyncRangeReaderBuilder) -> AsyncRangeReaderBuilder,
    ) -> Self {
        self.inner = func(self.inner);
        self
    }

    pub(crate) fn from_config(key: String, config: &Config) -> Self {
        Self {
            inner: AsyncRangeReaderBuilder::from_config(key, config),
            max_retry_concurrency: config.max_retry_concurrency().unwrap_or(5),
            total_tries: config.retry().unwrap_or(10),
        }
    }

    pub(crate) fn from_env(key: String) -> Option<Self> {
        AsyncRangeReaderBuilder::from_env_with_extra_items(key).map(
            |(builder, max_retry_concurrency, total_tries)| Self {
                inner: builder,
                max_retry_concurrency: max_retry_concurrency.unwrap_or(5),
                total_tries: total_tries.unwrap_or(10),
            },
        )
    }
}

trait BuildAsyncRangeReader: Send {
    fn build_async_range_reader(self) -> AsyncRangeReaderWithRangeReader;
}

impl BuildAsyncRangeReader for RangeReaderBuilder {
    fn build_async_range_reader(self) -> AsyncRangeReaderWithRangeReader {
        AsyncRangeReaderWithRangeReader::new(
            self.inner.build(),
            self.max_retry_concurrency,
            self.total_tries,
        )
    }
}

impl BuildAsyncRangeReader for AsyncRangeReaderWithRangeReader {
    fn build_async_range_reader(self) -> AsyncRangeReaderWithRangeReader {
        self
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RangeReader(RangeReaderHandle);

#[derive(Debug, Clone)]
struct RangeReaderHandle(Arc<RangeReaderHandleInner>);

type OneshotResponse = Sender<Response>;
type ThreadSender = UnboundedSender<(Request, OneshotResponse)>;

#[derive(Debug)]
struct RangeReaderHandleInner {
    tx: Option<ThreadSender>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Debug)]
enum Request {
    UpdateUrls,
    IoUrls,
    ReadAt { pos: u64, size: u64 },
    ReadMultiRanges { ranges: Vec<(u64, u64)> },
    Exist,
    FileSize,
    Download,
    ReadLastBytes { size: u64 },
}

type Response = IoResult<ResponseData>;

#[derive(Debug)]
enum ResponseData {
    Strings(Vec<String>),
    Bytes(Vec<u8>),
    BytesWithSize((Vec<u8>, u64)),
    Parts(Vec<RangePart>),
    Bool(bool),
    U64(u64),
}

impl Drop for RangeReaderHandleInner {
    fn drop(&mut self) {
        let id = self
            .thread
            .as_ref()
            .map(|h| h.thread().id())
            .expect("thread not dropped yet");

        trace!("closing runtime thread ({:?})", id);
        self.tx.take();
        trace!("signaled close for runtime thread ({:?})", id);
        self.thread.take().map(|h| h.join());
        trace!("closed runtime thread ({:?})", id);
    }
}

impl RangeReaderHandle {
    fn new(builder: impl BuildAsyncRangeReader + 'static) -> Self {
        let (tx, rx) = unbounded_channel::<(Request, OneshotResponse)>();
        let (spawn_tx, spawn_rx) = channel::<IoResult<()>>();

        let join_handle = ThreadBuilder::new()
            .name("qiniu-download-internal-sync-runtime".into())
            .spawn(move || {
                let rt = match TokioRuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        if let Err(e) = spawn_tx.send(Err(e)) {
                            error!("Failed to communicate runtime creation failure: {:?}", e);
                        }
                        return;
                    }
                };
                let fut = async move {
                    let range_reader = builder.build_async_range_reader();
                    if let Err(e) = spawn_tx.send(Ok(())) {
                        error!("Failed to communicate successful startup: {:?}", e);
                        return;
                    }
                    let mut rx = rx;
                    while let Some((req, req_tx)) = rx.recv().await {
                        let req_fut = req.send(range_reader.to_owned());
                        spawn_tokio(forward(req_fut, req_tx));
                    }

                    debug!("({:?}) Receiver is shutdown", current_thread().id());
                };
                trace!("({:?}) start runtime::block_on", current_thread().id());
                rt.block_on(fut);
                trace!("({:?}) end runtime::block_on", current_thread().id());
                drop(rt);
                trace!("({:?}) finished", current_thread().id());
            })
            .expect("Failed to spawn thread");

        match block_on(spawn_rx) {
            Ok(Ok(())) => Self(Arc::new(RangeReaderHandleInner {
                tx: Some(tx),
                thread: Some(join_handle),
            })),
            Ok(Err(err)) => runtime_create_error(err),
            Err(_) => event_loop_panicked(),
        }
    }

    fn execute_request(&self, request: Request) -> Response {
        let (tx, rx) = channel();
        self.0
            .tx
            .as_ref()
            .expect("core thread exited early")
            .send((request, tx))
            .expect("core thread panicked");

        match block_on(async move { rx.await.map_err::<IoError, _>(|_| event_loop_panicked()) }) {
            Ok(result) => result,
            Err(err) => Err(err),
        }
    }
}

impl RangeReader {
    pub(crate) fn builder(
        bucket: String,
        key: String,
        credential: Credential,
        io_urls: Vec<String>,
    ) -> RangeReaderBuilder {
        RangeReaderBuilder::new(bucket, key, credential, io_urls)
    }

    pub(crate) fn from_config(key: String, config: &Config) -> Self {
        RangeReaderBuilder::from_config(key, config).build()
    }

    pub(crate) fn from_env(key: String) -> Option<Self> {
        AsyncRangeReader::from_env_with_extra_items(key).map(
            |(range_reader, max_retry_concurrency, total_tries)| {
                Self(RangeReaderHandle::new(
                    AsyncRangeReaderWithRangeReader::new(
                        range_reader,
                        max_retry_concurrency.unwrap_or(5),
                        total_tries.unwrap_or(10),
                    ),
                ))
            },
        )
    }

    pub(crate) fn update_urls(&self) -> bool {
        match self.execute(Request::UpdateUrls) {
            Ok(ResponseData::Bool(b)) => b,
            response => unexpected_response(response),
        }
    }

    pub(crate) fn io_urls(&self) -> Vec<String> {
        match self.execute(Request::IoUrls) {
            Ok(ResponseData::Strings(urls)) => urls,
            response => unexpected_response(response),
        }
    }

    pub(crate) fn read_multi_ranges(&self, ranges: &[(u64, u64)]) -> IoResult<Vec<RangePart>> {
        match self.execute(Request::ReadMultiRanges {
            ranges: ranges.to_vec(),
        }) {
            Ok(ResponseData::Parts(parts)) => Ok(parts),
            Err(err) => Err(err),
            response => unexpected_response(response),
        }
    }

    pub(crate) fn exist(&self) -> IoResult<bool> {
        match self.execute(Request::Exist) {
            Ok(ResponseData::Bool(existed)) => Ok(existed),
            Err(err) => Err(err),
            response => unexpected_response(response),
        }
    }

    pub(crate) fn file_size(&self) -> IoResult<u64> {
        match self.execute(Request::Exist) {
            Ok(ResponseData::U64(size)) => Ok(size),
            Err(err) => Err(err),
            response => unexpected_response(response),
        }
    }

    pub(crate) fn download(&self) -> IoResult<Vec<u8>> {
        match self.execute(Request::Download) {
            Ok(ResponseData::Bytes(bytes)) => Ok(bytes),
            Err(err) => Err(err),
            response => unexpected_response(response),
        }
    }

    pub(crate) fn download_to(&self, writer: &mut dyn Write) -> IoResult<u64> {
        let bytes = self.download()?;
        writer.write_all(&bytes)?;
        Ok(bytes.len() as u64)
    }

    pub(crate) fn read_last_bytes(&self, buf: &mut [u8]) -> IoResult<(u64, u64)> {
        match self.execute(Request::ReadLastBytes {
            size: buf.len() as u64,
        }) {
            Ok(ResponseData::BytesWithSize((bytes, total_size))) => {
                buf[..bytes.len()].copy_from_slice(&bytes);
                Ok((bytes.len() as u64, total_size))
            }
            Err(err) => Err(err),
            response => unexpected_response(response),
        }
    }

    fn execute(&self, request: Request) -> Response {
        self.0.execute_request(request)
    }
}

impl ReadAt for RangeReader {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> IoResult<usize> {
        match self.execute(Request::ReadAt {
            pos,
            size: buf.len() as u64,
        }) {
            Ok(ResponseData::Bytes(bytes)) => {
                buf.copy_from_slice(&bytes);
                Ok(bytes.len())
            }
            Err(err) => Err(err),
            response => unexpected_response(response),
        }
    }
}

impl Request {
    async fn send(self, range_reader: AsyncRangeReaderWithRangeReader) -> Response {
        match self {
            Self::UpdateUrls => Ok(ResponseData::Bool(range_reader.update_urls().await)),
            Self::IoUrls => Ok(ResponseData::Strings(range_reader.io_urls().await)),
            Self::ReadAt { pos, size } => range_reader
                .read_at(pos, size)
                .await
                .map(ResponseData::Bytes),
            Self::ReadMultiRanges { ranges } => range_reader
                .read_multi_ranges(&ranges)
                .await
                .map(ResponseData::Parts),
            Self::Exist => range_reader.exist().await.map(ResponseData::Bool),
            Self::FileSize => range_reader.file_size().await.map(ResponseData::U64),
            Self::Download => range_reader.download().await.map(ResponseData::Bytes),
            Self::ReadLastBytes { size } => range_reader
                .read_last_bytes(size)
                .await
                .map(ResponseData::BytesWithSize),
        }
    }
}

async fn forward(fut: impl Future<Output = Response>, mut tx: OneshotResponse) {
    pin_mut!(fut);

    let result = poll_fn(|cx| match fut.as_mut().poll(cx) {
        Poll::Ready(result) => Poll::Ready(Some(result)),
        Poll::Pending => {
            ready!(tx.poll_closed(cx));
            Poll::Ready(None)
        }
    })
    .await;

    if let Some(result) = result {
        let _ = tx.send(result);
    }
    // else request is canceled
}

#[track_caller]
fn block_on<F: Future>(fut: F) -> F::Output {
    enter();
    let waker = waker(Arc::new(ThreadWaker(current_thread())));
    let mut cx = Context::from_waker(&waker);

    pin_mut!(fut);

    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => {}
        };

        debug!("({:?}) park", current_thread().id());
        park_thread();
    }

    struct ThreadWaker(Thread);

    impl ArcWake for ThreadWaker {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            debug!("({:?}) unpark", current_thread().id());
            arc_self.0.unpark();
        }
    }

    fn enter() {
        // Check we aren't already in a runtime
        #[cfg(debug_assertions)]
        {
            let _enter = TokioRuntimeBuilder::new_current_thread()
                .build()
                .expect("build shell runtime")
                .enter();
        }
    }
}

#[cold]
#[inline(never)]
#[track_caller]
fn event_loop_panicked() -> ! {
    // The only possible reason there would be a Canceled error
    // is if the thread running the event loop panicked. We could return
    // an Err here, like a BrokenPipe, but the Client is not
    // recoverable. Additionally, the panic in the other thread
    // is not normal, and should likely be propagated.
    panic!("event loop thread panicked");
}

#[cold]
#[inline(never)]
#[track_caller]
fn runtime_create_error(err: IoError) -> ! {
    panic!("tokio runtime creation error: {}", err);
}

#[cold]
#[inline(never)]
#[track_caller]
fn unexpected_response(response: Response) -> ! {
    panic!("unexpected response: {:?}", response);
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::{HeaderValue, RANGE};
    use std::thread::spawn as spawn_thread;
    use text_io::scan as scan_text;
    use tokio::task::{spawn, spawn_blocking};
    use warp::{header, path, reply::Response, Filter};

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

    fn get_credential() -> Credential {
        Credential::new("1234567890", "abcdefghijk")
    }

    #[tokio::test]
    #[allow(clippy::needless_collect)]
    async fn test_synced_read_at() -> anyhow::Result<()> {
        env_logger::try_init().ok();

        let io_routes = path!("file" / usize)
            .and(header::value(RANGE.as_str()))
            .map(|size: usize, range: HeaderValue| {
                let from: u64;
                let to: u64;
                scan_text!(range.to_str().unwrap().bytes() => "bytes={}-{}", from, to);
                let body = vec![from as u8; size];
                Response::new(body.into())
            });

        starts_with_server!(io_addr, io_routes, {
            spawn_blocking(move || {
                let io_urls = vec![format!("http://{}", io_addr)];

                for (size, base_timeout_ms) in [(1024, 100), (1024 * 1024, 1000)] {
                    let downloader = RangeReaderBuilder::new(
                        "bucket".to_owned(),
                        format!("file/{}", size),
                        get_credential(),
                        io_urls.to_owned(),
                    )
                    .use_getfile_api(false)
                    .normalize_key(true)
                    .base_timeout(Duration::from_millis(base_timeout_ms))
                    .build();

                    let threads = (0..=255u64)
                        .map(|i| {
                            let downloader = downloader.to_owned();
                            spawn_thread(move || {
                                let mut buf = vec![0u8; size];
                                assert_eq!(downloader.read_at(i, &mut buf)?, size);
                                Ok::<_, anyhow::Error>(buf)
                            })
                        })
                        .collect::<Vec<_>>();

                    for (i, response) in threads
                        .into_iter()
                        .map(|thread| thread.join().unwrap())
                        .enumerate()
                    {
                        assert_eq!(response.unwrap(), vec![i as u8; size]);
                    }
                }
            })
            .await?;
        });

        Ok(())
    }
}
