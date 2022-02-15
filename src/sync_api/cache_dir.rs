use directories::BaseDirs;
use log::warn;
use once_cell::sync::Lazy;
use std::{
    env::temp_dir,
    fs,
    io::Result as IoResult,
    path::{Path, PathBuf},
};
use tap::prelude::*;

pub(super) fn cache_dir_path_of(path: impl AsRef<Path>) -> IoResult<PathBuf> {
    return _cache_dir_path_of(path.as_ref())
        .tap_err(|err| warn!("Failed to get cache directory: {}", err));

    fn _cache_dir_path_of(path: &Path) -> IoResult<PathBuf> {
        static CACHE_DIR: Lazy<PathBuf> = Lazy::new(|| {
            BaseDirs::new()
                .map(|dir| dir.cache_dir().join("qiniu-download"))
                .unwrap_or_else(|| temp_dir().join("qiniu-download"))
        });

        if fs::metadata(&*CACHE_DIR).is_err() {
            fs::create_dir_all(&*CACHE_DIR)?;
        }

        Ok(CACHE_DIR.join(path))
    }
}
