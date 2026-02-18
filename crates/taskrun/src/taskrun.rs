pub mod formatter;

pub use tokio;
pub use tracing;
pub use tracing_subscriber;

use crate::log::*;
use daqbuf_err as err;
// use console_subscriber::ConsoleLayer;
use err::Error;
use std::fmt;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::panic;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tracing_subscriber::layer::Context;

pub mod log {
    #[allow(unused_imports)]
    pub use tracing::{debug, error, info, trace, warn};
}

static INIT_TRACING_ONCE: Mutex<usize> = Mutex::new(0);
static RUNTIME: Mutex<Option<Arc<Runtime>>> = Mutex::new(None);

pub fn get_runtime() -> Arc<Runtime> {
    get_runtime_opts(24, 128)
}

#[allow(unused)]
fn on_thread_start() {
    let old = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let payload = if let Some(k) = info.payload().downcast_ref::<Error>() {
            format!("{:?}", k)
        } else if let Some(k) = info.payload().downcast_ref::<String>() {
            k.into()
        } else if let Some(&k) = info.payload().downcast_ref::<&str>() {
            k.into()
        } else {
            format!("unknown payload type")
        };
        error!(
            "panicking\n{:?}\nLOCATION: {:?}\nPAYLOAD: {:?}\ninfo object: {:?}\nerr: {:?}",
            Error::with_msg("catched panic in taskrun::run"),
            info.location(),
            info.payload(),
            info,
            payload,
        );
        if false {
            old(info);
        }
    }));
}

pub fn get_runtime_opts(nworkers: usize, nblocking: usize) -> Arc<Runtime> {
    match RUNTIME.lock() {
        Ok(mut g) => match g.as_ref() {
            None => {
                let res = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(nworkers)
                    .max_blocking_threads(nblocking)
                    .enable_all()
                    // .on_thread_start(on_thread_start)
                    .build();
                let res = match res {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("ERROR {e}");
                        panic!("can not create runtime {e}");
                    }
                };
                let a = Arc::new(res);
                *g = Some(a.clone());
                a
            }
            Some(g) => g.clone(),
        },
        Err(e) => {
            eprintln!("can not lock tracing init {e}");
            panic!("can not lock tracing init {e}");
        }
    }
}

pub fn run<T, E, F>(fut: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    let runtime = get_runtime();
    match tracing_init(TracingMode::Development) {
        Ok(_) => {}
        Err(()) => {
            eprintln!("ERROR tracing: can not init");
        }
    }
    let res = runtime.block_on(fut);
    match res {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("ERROR catched: {e}");
            Err(e)
        }
    }
}

#[allow(unused)]
struct LogFilterLayer<S, L>
where
    L: tracing_subscriber::Layer<S>,
    S: tracing::Subscriber,
{
    name: String,
    inner: L,
    _ph1: PhantomData<S>,
}

impl<S, L> LogFilterLayer<S, L>
where
    L: tracing_subscriber::Layer<S>,
    S: tracing::Subscriber,
{
    #[allow(unused)]
    fn new(name: String, inner: L) -> Self {
        Self {
            name,
            inner,
            _ph1: PhantomData,
        }
    }
}

impl<S, L> tracing_subscriber::Layer<S> for LogFilterLayer<S, L>
where
    L: tracing_subscriber::Layer<S>,
    S: tracing::Subscriber,
{
}

fn collect_env_list(env: &str) -> Vec<String> {
    std::env::var(env)
        .unwrap_or(String::new())
        .split(",")
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn tracing_init_inner(mode: TracingMode) -> Result<(), Error> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Layer;
    let fmtstr = "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z";
    let timer = tracing_subscriber::fmt::time::UtcTime::new(
        time::format_description::parse(fmtstr).map_err(|e| format!("{e}"))?,
    );
    if let TracingMode::Console = mode {
        // Only async console
        // let console_layer = console_subscriber::spawn();
        // let console_layer = ConsoleLayer::builder().with_default_env().init();

        #[cfg(feature = "with-console")]
        {
            let console_layer = ConsoleLayer::builder().spawn();
            tracing_subscriber::registry()
                .with(console_layer)
                .with(tracing_subscriber::fmt::layer().with_ansi(false))
                // .with(other_layer)
                .init();
            console_subscriber::init();
        }
    } else {
        // Logging setup
        #[cfg(feature = "with-env-filter")]
        let filter_1 = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(tracing::metadata::LevelFilter::INFO.into())
            .from_env()
            .map_err(|e| Error::with_msg_no_trace(format!("can not build tracing env filter {e}")))?;
        #[cfg(feature = "with-env-filter")]
        let filter_2 = tracing_subscriber::EnvFilter::builder()
            .with_env_var("RUST_LOG_2")
            .with_default_directive(tracing::metadata::LevelFilter::INFO.into())
            .from_env()
            .map_err(|e| Error::with_msg_no_trace(format!("can not build tracing env filter {e}")))?;
        let tracing_debug = collect_env_list("TRACING_DEBUG");
        let tracing_trace = collect_env_list("TRACING_TRACE");
        // let tracing_trace_always = collect_env_list("TRACING_TRACE_ALWAYS");
        let filter_3 = tracing_subscriber::filter::DynFilterFn::new(move |meta, ctx| {
            let mut tmp1 = String::with_capacity(256);
            let lgspan_trace = "log_span_trace";
            let lgspan_debug = "log_span_debug";
            fn check_target(
                meta: &tracing::Metadata,
                ctx: &Context<'_, tracing_subscriber::Registry>,
                list: &Vec<String>,
                tmp1: &mut String,
                lgspan: &str,
            ) -> bool {
                let mut target_match = false;
                for e in list {
                    tmp1.clear();
                    tmp1.push_str(e);
                    tmp1.push_str("::");
                    let target = meta.target();
                    if false && target.contains("fetchmpx") {
                        eprintln!("LOOKING AT  {lgspan}  tmp1 [{tmp1}]  target [{target}]");
                    }
                    if target == &tmp1[..tmp1.len() - 2] || target.starts_with(tmp1.as_str()) {
                        target_match = true;
                        break;
                    }
                }
                if target_match {
                    let mut sr = ctx.lookup_current();
                    let mut allow = false;
                    while let Some(g) = sr {
                        if g.name() == lgspan {
                            allow = true;
                            break;
                        } else {
                            sr = g.parent();
                        }
                    }
                    allow = true;
                    allow
                } else {
                    false
                }
            }
            if *meta.level() >= tracing::Level::TRACE {
                check_target(meta, ctx, &tracing_trace, &mut tmp1, lgspan_trace)
            } else if *meta.level() >= tracing::Level::DEBUG {
                check_target(meta, ctx, &tracing_trace, &mut tmp1, lgspan_trace)
                    || check_target(meta, ctx, &tracing_debug, &mut tmp1, lgspan_debug)
            } else {
                true
            }
        });
        let fmt_layer = tracing_subscriber::fmt::Layer::new()
            .with_writer(io::stderr)
            .with_timer(timer)
            .with_target(true)
            .with_ansi(false)
            .with_thread_names(true)
            .event_format(formatter::FormatTxt)
            .with_filter(filter_3)
            // .with_filter(filter_2)
            // .with_filter(filter_1)
        ;
        // let fmt_layer = fmt_layer.with_filter(filter_3);
        // let fmt_layer: Box<dyn Layer<tracing_subscriber::Registry>> = if std::env::var("RUST_LOG_USE_2").is_ok() {
        //     let a = fmt_layer.with_filter(filter_2);
        //     Box::new(a)
        // } else {
        //     let a = fmt_layer;
        //     Box::new(a)
        // };
        // let fmt_layer = fmt_layer.with_filter(filter_1);
        // .and_then(LogFilterLayer::new("lay1".into()))
        // .and_then(LogFilterLayer::new("lay2".into()))
        // let layer_2 = LogFilterLayer::new("lay1".into(), fmt_layer);

        let reg = tracing_subscriber::registry();

        #[cfg(feature = "DISABLED_CONSOLE")]
        let reg = {
            let (console_layer, console_server) = console_subscriber::ConsoleLayer::builder().build();
            tokio::spawn(console_server.serve());
            reg.with(console_layer)
        };

        #[cfg(feature = "DISABLED_CONSOLE")]
        let reg = {
            let pid = std::process::id();
            // let cspn = format!("/tmp/daqbuffer.tokio.console.pid.{pid}");
            let console_layer = console_subscriber::ConsoleLayer::builder()
                // .retention(std::time::Duration::from_secs(10))
                .server_addr(([127, 0, 0, 1], 14571))
                // .server_addr(std::path::Path::new(&cspn))
                .spawn();
            // .build();

            // eprintln!("spawn console sever");
            // tokio::spawn(console_server.serve());
            reg.with(console_layer)
        };

        let reg = reg.with(fmt_layer);
        reg.try_init().map_err(|e| {
            eprintln!("can not initialize tracing layer: {e}");
            format!("{e}")
        })?;
    }
    #[cfg(feature = "DISABLED_LOKI")]
    // TODO tracing_loki seems not well composable, try open telemetry instead.
    if false {
        /*let fmt_layer = tracing_subscriber::fmt::Layer::new()
        .with_writer(io::stderr)
        .with_timer(timer)
        .with_target(true)
        .with_ansi(false)
        .with_thread_names(true)
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());*/
        let url = "http://[::1]:6947";
        //let url = "http://127.0.0.1:6947";
        //let url = "http://[::1]:6132";
        let (loki_layer, loki_task) = tracing_loki::layer(
            tracing_loki::url::Url::parse(url)?,
            vec![(format!("daqbuffer"), format!("dev"))].into_iter().collect(),
            [].into(),
        )
        .map_err(|e| format!("{e}"))?;
        //let loki_layer = loki_layer.with_filter(log_filter);
        eprintln!("MADE LAYER");
        tracing_subscriber::registry()
            //.with(fmt_layer)
            .with(loki_layer)
            //.try_init()
            //.map_err(|e| format!("{e}"))?;
            .init();
        eprintln!("REGISTERED");
        if true {
            tokio::spawn(loki_task);
            eprintln!("SPAWNED TASK");
        }
        eprintln!("INFO LOKI");
    }
    Ok(())
}

pub enum TracingMode {
    Production,
    Development,
    Console,
}

pub fn tracing_init_testing() -> Result<(), ()> {
    tracing_init(TracingMode::Development)
}

pub fn tracing_init(mode: TracingMode) -> Result<(), ()> {
    match INIT_TRACING_ONCE.lock() {
        Ok(mut initg) => {
            if *initg == 0 {
                match tracing_init_inner(mode) {
                    Ok(_) => {
                        *initg = 1;
                    }
                    Err(e) => {
                        *initg = 2;
                        eprintln!("tracing_init_inner gave error {e}");
                    }
                }
                Ok(())
            } else if *initg == 1 {
                Ok(())
            } else {
                eprintln!("ERROR unknown tracing state");
                Err(())
            }
        }
        Err(e) => {
            eprintln!("can not lock tracing init {e}");
            Err(())
        }
    }
}

pub fn spawn<T>(task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(task)
}

pub fn query_log_level() -> tracing::Level {
    use tracing::Level;
    let mut level = Level::INFO;
    if false {
        let mut _reg = tracing_subscriber::registry();
    }
    tracing::Span::current().id().map(|id| {
        tracing::dispatcher::get_default(|disp| {
            disp.downcast_ref::<tracing_subscriber::Registry>().map(|reg| {
                use tracing_subscriber::registry::LookupSpan;
                if let Some(mut sp) = reg.span(&id) {
                    loop {
                        if sp.name() == "log_span_debug" {
                            if level < Level::DEBUG {
                                level = Level::DEBUG;
                            }
                        }
                        if sp.name() == "log_span_trace" {
                            if level < Level::TRACE {
                                level = Level::TRACE;
                            }
                        }
                        if let Some(x) = sp.parent() {
                            sp = x;
                        } else {
                            break;
                        }
                    }
                } else {
                    info!("reg span not available");
                }
            });
        })
    });
    level
}

pub fn trigger_error_worker_scylla_events(set: Option<bool>) -> bool {
    static TR: std::sync::Mutex<bool> = std::sync::Mutex::new(false);
    let mut g = TR.lock().unwrap();
    let v = *g;
    if let Some(x) = set {
        *g = x;
    } else {
        if *g {
            *g = false;
        }
    }
    v
}
