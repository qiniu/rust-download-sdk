use atomic_once_cell::AtomicLazy;
use directories::BaseDirs;
use log::warn;
use std::{
    env::temp_dir,
    io::Result as IoResult,
    path::{Path, PathBuf},
};
use tap::TapFallible;
use tokio::fs;

pub(super) async fn cache_dir_path_of(path: impl AsRef<Path>) -> IoResult<PathBuf> {
    return _cache_dir_path_of(path.as_ref())
        .await
        .tap_err(|err| warn!("Failed to get cache directory: {}", err));

    #[inline]
    async fn _cache_dir_path_of(path: &Path) -> IoResult<PathBuf> {
        static CACHE_DIR: AtomicLazy<PathBuf> = AtomicLazy::new(|| {
            BaseDirs::new()
                .map(|dir| dir.cache_dir().join("qiniu-download"))
                .unwrap_or_else(|| temp_dir().join("qiniu-download"))
        });

        if fs::metadata(&*CACHE_DIR).await.is_err() {
            fs::create_dir_all(&*CACHE_DIR).await?;
        }

        Ok(CACHE_DIR.join(path))
    }
}
