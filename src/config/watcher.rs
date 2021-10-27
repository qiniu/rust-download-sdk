use super::reload_config;
use dashmap::{DashMap, DashSet};
use log::{error, info, warn};
use notify::{
    watcher, DebouncedEvent, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher,
};
use once_cell::sync::{Lazy, OnceCell};
use std::{
    collections::HashSet,
    io::Result as IOResult,
    path::{Path, PathBuf},
    sync::{
        mpsc::{channel, Receiver},
        RwLock,
    },
    thread::{Builder as ThreadBuilder, JoinHandle},
    time::Duration,
};
use tap::TapFallible;

static WATCHED_FILES: Lazy<DashSet<PathBuf>> = Lazy::new(Default::default);
static WATCHED_DIRS: Lazy<DashMap<PathBuf, usize>> = Lazy::new(Default::default);
static WATCHER: Lazy<RwLock<RecommendedWatcher>> = Lazy::new(|| RwLock::new(_start_watcher()));

#[inline]
pub(super) fn ensure_watches(watch_paths: &[PathBuf]) -> NotifyResult<()> {
    let watch_paths = watch_paths
        .iter()
        .map(|watch_path| canonicalize(watch_path))
        .collect::<IOResult<HashSet<PathBuf>>>()?;

    for watch_path in watch_paths.iter() {
        add_to_watcher(watch_path)
            .tap_err(|err| warn!("Failed to watch {:?}: {:?}", watch_path, err))?;
    }
    let to_remove = WATCHED_FILES
        .iter()
        .filter(|watch_path| !watch_paths.contains(watch_path.as_path()))
        .map(|watch_path| watch_path.as_path().to_owned())
        .collect::<Vec<_>>();
    for watch_path in to_remove.iter() {
        remove_from_watcher(watch_path)
            .tap_err(|err| warn!("Failed to unwatch {:?}: {:?}", watch_path, err))?;
    }

    Ok(())
}

#[inline]
fn add_to_watcher(watch_path: &Path) -> NotifyResult<()> {
    if WATCHED_FILES.insert(watch_path.to_owned()) {
        let watch_dir = parent_of(watch_path);
        WATCHED_DIRS
            .entry(watch_dir.to_owned())
            .and_modify(|count| {
                *count += 1;
            })
            .or_try_insert_with(|| {
                WATCHER
                    .write()
                    .unwrap()
                    .watch(&watch_dir, RecursiveMode::NonRecursive)
                    .map(|_| 1)
            })?;
    }

    Ok(())
}

#[inline]
fn remove_from_watcher(path: &Path) -> NotifyResult<()> {
    if WATCHED_FILES.remove(path).is_some() {
        let watch_dir = parent_of(path);
        if let Some(mut count) = WATCHED_DIRS.get_mut(&watch_dir) {
            *count -= 1;
            log::warn!("**** remove_from_watcher: {}", *count);
            if *count == 0 {
                log::warn!("**** remove_from_watcher: removed: {:?}", watch_dir);
                drop(count);
                WATCHED_DIRS.remove(&watch_dir);
                WATCHER.write().unwrap().unwatch(&watch_dir)?;
            }
        }
    }
    Ok(())
}

#[inline]
#[cfg(test)]
pub(super) fn unwatch_all() -> NotifyResult<()> {
    {
        let mut watcher = WATCHER.write().unwrap();
        for watched_dir in WATCHED_DIRS.iter() {
            log::warn!("**** unwatch: {:?}", watched_dir.key());
            watcher.unwatch(watched_dir.key())?;
        }
    }

    WATCHED_DIRS.clear();
    WATCHED_FILES.clear();

    Ok(())
}

#[inline]
#[cfg(test)]
pub(super) fn watch_dirs_count() -> usize {
    WATCHED_DIRS.len()
}

#[inline]
#[cfg(test)]
pub(super) fn watch_files_count() -> usize {
    WATCHED_FILES.len()
}

#[inline]
fn canonicalize(path: &Path) -> IOResult<PathBuf> {
    path.canonicalize()
        .tap_err(|err| warn!("Failed to canonicalize config path {:?}: {:?}", path, err))
}

#[inline]
fn parent_of(path: &Path) -> PathBuf {
    path.parent().unwrap_or_else(|| Path::new("/")).to_owned()
}

#[inline]
fn _start_watcher() -> RecommendedWatcher {
    static CONFIG_WATCHER_THREAD: OnceCell<JoinHandle<()>> = OnceCell::new();

    let (tx, rx) = channel();

    match watcher(tx, Duration::from_millis(500)) {
        Ok(watcher) => {
            if let Err(err) = CONFIG_WATCHER_THREAD.get_or_try_init(|| {
                ThreadBuilder::new()
                    .name("qiniu-config-watcher".into())
                    .spawn(move || setup_config_watcher_inner(rx))
            }) {
                error!(
                    "Failed to start thread to watch Qiniu config file: {:?}",
                    err
                );
            }
            return watcher;
        }
        Err(err) => {
            panic!("Failed to create watcher: {:?}", err)
        }
    }

    #[inline]
    fn setup_config_watcher_inner(rx: Receiver<DebouncedEvent>) -> ! {
        loop {
            match rx.recv() {
                Ok(event) => match &event {
                    DebouncedEvent::Create(ref path) => {
                        if WATCHED_FILES.contains(path) {
                            event_received(&event);
                        }
                    }
                    DebouncedEvent::Write(ref path) => {
                        if WATCHED_FILES.contains(path) {
                            event_received(&event);
                        }
                    }
                    DebouncedEvent::Error(err, _) => {
                        error!(
                            "Received error event from Qiniu config file watcher: {:?}",
                            err
                        );
                    }
                    _ => {}
                },
                Err(err) => {
                    error!(
                        "Failed to receive event from Qiniu config file watcher: {:?}",
                        err
                    );
                }
            }
        }
    }

    #[inline]
    fn event_received(event: &DebouncedEvent) {
        info!("Received event {:?} from Qiniu config file watcher", event);
        reload_config(true);
    }
}
