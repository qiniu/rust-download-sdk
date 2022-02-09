use super::{configurable::Configurable, init_config};
use once_cell::sync::OnceCell;
use std::sync::RwLock;

#[cfg(not(test))]
mod safe {
    use super::*;

    pub(in super::super) static QINIU_CONFIG: OnceCell<RwLock<Option<Configurable>>> =
        OnceCell::new();

    pub(in super::super) fn qiniu_config() -> &'static RwLock<Option<Configurable>> {
        QINIU_CONFIG.get_or_init(init_config)
    }
}

#[cfg(not(test))]
pub(super) use safe::qiniu_config;

#[cfg(test)]
mod not_safe {
    use super::*;

    pub(in super::super) static mut QINIU_CONFIG: OnceCell<RwLock<Option<Configurable>>> =
        OnceCell::new();

    pub(in super::super) fn qiniu_config() -> &'static RwLock<Option<Configurable>> {
        unsafe { &mut QINIU_CONFIG }.get_or_init(init_config)
    }

    pub(in super::super) fn reset_static_vars() {
        unsafe { &mut QINIU_CONFIG }.take();
    }
}
#[cfg(test)]
pub(super) use not_safe::{qiniu_config, reset_static_vars};
