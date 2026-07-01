#![allow(unused_imports)]

pub use branch_debug as debug;
pub use branch_error as error;
pub use branch_info as info;
pub use branch_trace as trace;
pub use branch_warn as warn;

pub use tracing as tracing_rxp;

use std::fmt;
use std::io;
use std::sync::LazyLock;

struct FmtWriter<'a, 'b>(&'a mut fmt::Formatter<'b>);

impl io::Write for FmtWriter<'_, '_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let s =
            std::str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.0
            .write_str(s)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "fmt error"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct TsNow(time::UtcDateTime);

impl TsNow {
    pub fn now() -> Self {
        Self(time::UtcDateTime::now())
    }
}

impl fmt::Display for TsNow {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let f2 = time::macros::format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"
        );
        self.0
            .format_into(&mut FmtWriter(fmt), f2)
            .map_err(|_| fmt::Error)?;
        Ok(())
    }
}

#[allow(unused)]
#[inline(always)]
pub fn is_log_direct() -> bool {
    static ONCE: LazyLock<bool> =
        LazyLock::new(|| std::env::var("LOG_DIRECT").map_or(false, |x| x == "1"));
    *ONCE
}

pub mod log_tracing {
    pub use tracing::debug;
    pub use tracing::error;
    pub use tracing::info;
    pub use tracing::trace;
    pub use tracing::warn;
    // pub use tracing::{self, event, span, Level};
}

pub mod log_direct {
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_trace {
        ($fmt:expr) => {
            eprintln!("{} TRACE {}", $crate::TsNow::now(), format_args!($fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            eprintln!("{} TRACE {}", $crate::TsNow::now(), format_args!($fmt, $($arg)*));
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_debug {
        ($fmt:expr) => {
            // eprintln!(concat!("DEBUG ", $fmt));
            // eprintln!("{}", format_args!(concat!("DEBUG ", $fmt)));
            eprintln!("{} DEBUG {}", $crate::TsNow::now(), format_args!($fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            // eprintln!(concat!("DEBUG ", $fmt), $($arg),*);
            // eprintln!("{}", format_args!(concat!("DEBUG ", $fmt), $($arg),*));
            eprintln!("{} DEBUG {}", $crate::TsNow::now(), format_args!($fmt, $($arg)*));
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_info {
        ($fmt:expr) => {
            eprintln!("{} INFO  {}", $crate::TsNow::now(), format_args!($fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            eprintln!("{} INFO  {}", $crate::TsNow::now(), format_args!($fmt, $($arg)*));
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_warn {
        ($fmt:expr) => {
            eprintln!("{} WARN  {}", $crate::TsNow::now(), format_args!($fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            eprintln!("{} WARN  {}", $crate::TsNow::now(), format_args!($fmt, $($arg)*));
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! direct_error {
        ($fmt:expr) => {
            eprintln!("{} ERROR {}", $crate::TsNow::now(), format_args!($fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            eprintln!("{} ERROR {}", $crate::TsNow::now(), format_args!($fmt, $($arg)*));
        };
    }
    pub use crate::direct_debug as debug;
    pub use crate::direct_error as error;
    pub use crate::direct_info as info;
    pub use crate::direct_trace as trace;
    pub use crate::direct_warn as warn;
}

#[allow(unused)]
#[macro_export]
macro_rules! log_v2_trace {
    // ($fmt:expr) => {
    //     let h = format_args!();
    //     eprintln!(concat!("TRACE V2 ", $fmt));
    // };
    ($fmt:expr, $($arg:expr),*) => {
        // let fmt2 = concat!("", $fmt);
        // let fmt2 = concat!("TRACE V2 ", $fmt, $($arg),*);
        // let h = format_args!($fmt, $($arg),*);
        // eprintln!("h: {:?}", h);
    };
}

pub mod log_macros_branch {
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_trace {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::trace!($($arg)*);
            } else {
                $crate::log_tracing::trace!($($arg)*);
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_debug {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::debug!($($arg)*);
            } else {
                $crate::log_tracing::debug!($($arg)*);
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_info {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::info!($($arg)*);
            } else {
                $crate::log_tracing::info!($($arg)*);
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_warn {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::warn!($($arg)*);
            } else {
                $crate::log_tracing::warn!($($arg)*);
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_error {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::error!($($arg)*);
            } else {
                $crate::log_tracing::error!($($arg)*);
            }
        };
    }
    pub use branch_debug as debug;
    pub use branch_error as error;
    pub use branch_info as info;
    pub use branch_trace as trace;
    pub use branch_warn as warn;
    pub use tracing::{self, Level, event, span};
}

pub mod log_item_emit {
    #[allow(unused)]
    #[macro_export]
    macro_rules! log_item_emit_info {
        ($fmt:expr) => {
            let msg = format!("{}", format_args!($fmt));
            let item = items_0::streamitem::LogItem::origin_level_msg(
                module_path!().into(),
                $crate::tracing_rxp::Level::INFO,
                msg,
            );
            streams::logqueue::push_log_item(item);
        };
        ($fmt:expr, $($arg:tt)*) => {
            let msg = format!("{}", format_args!($fmt, $($arg)*));
            let item = items_0::streamitem::LogItem::origin_level_msg(
                module_path!().into(),
                $crate::tracing_rxp::Level::INFO,
                msg,
            );
            streams::logqueue::push_log_item(item);
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! log_item_emit_debug {
        ($($arg:tt)*) => {
            let msg = format!("{}", format_args!($($arg)*));
            let item = items_0::streamitem::LogItem::origin_level_msg(
                module_path!().into(),
                $crate::tracing_rxp::Level::DEBUG,
                msg,
            );
            streams::logqueue::push_log_item(item);
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! log_item_emit_trace {
        ($($arg:tt)*) => {
            let msg = format!("{}", format_args!($($arg)*));
            let item = items_0::streamitem::LogItem::origin_level_msg(
                module_path!().into(),
                $crate::tracing_rxp::Level::TRACE,
                msg,
            );
            streams::logqueue::push_log_item(item);
        };
    }
    pub use log_item_emit_debug as debug;
    pub use log_item_emit_info as info;
    pub use log_item_emit_trace as trace;
}
