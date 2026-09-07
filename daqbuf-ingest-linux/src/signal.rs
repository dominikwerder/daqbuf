use std::ffi::CStr;
use std::fmt;
use std::mem::MaybeUninit;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

autoerr::create_error_v1!(
    name(Error, "LinuxSignalError"),
    enum variants {
        SignalHandlerSet,
        SignalHandlerUnset,
        SignalHandlerAlreadyInstalled,
        SignalPipeCreate,
    },
);

#[allow(unused)]
type CB1 = fn(libc::c_int) -> ();
type CB3 = fn(libc::c_int, *const libc::siginfo_t, *const libc::c_void) -> ();

pub fn set_signal_handler(signum: libc::c_int, cb: CB3, old: &RwLock<libc::sigaction>) -> Result<(), Error> {
    let mut mask: libc::sigset_t = unsafe { std::mem::zeroed() };
    let ec = unsafe { libc::sigemptyset(&mut mask) };
    if ec != 0 {
        std::process::exit(82);
    }
    let sa_sigaction: libc::sighandler_t = cb as *const libc::c_void as _;
    let act = libc::sigaction {
        sa_sigaction,
        sa_mask: mask,
        sa_flags: libc::SA_SIGINFO | libc::SA_RESTART,
        sa_restorer: None,
    };
    let mut act_old = libc::sigaction {
        sa_sigaction: libc::SIG_DFL,
        sa_mask: unsafe { std::mem::zeroed() },
        sa_flags: 0,
        sa_restorer: None,
    };
    let (ec, msg) = unsafe {
        let ec = libc::sigaction(signum, &act, &mut act_old);
        let errno = *libc::__errno_location();
        (ec, std::ffi::CStr::from_ptr(libc::strerror(errno)))
    };
    if ec != 0 {
        eprintln!("unable to set signal handler: {msg:?}");
        std::process::exit(81);
    }
    *old.write().unwrap() = act_old;
    if false {
        eprintln!("act_old.sa_sigaction {:p}", act_old.sa_sigaction as *const ());
    }
    Ok(())
}

pub fn unset_signal_handler(signum: libc::c_int) -> Result<(), Error> {
    // Safe because it creates a valid value:
    let mask: libc::sigset_t = unsafe { MaybeUninit::zeroed().assume_init() };
    let act = libc::sigaction {
        sa_sigaction: libc::SIG_DFL,
        sa_mask: mask,
        sa_flags: 0,
        sa_restorer: None,
    };
    let (ec, msg) = unsafe {
        let ec = libc::sigaction(signum, &act, std::ptr::null_mut());
        let errno = *libc::__errno_location();
        (ec, CStr::from_ptr(libc::strerror(errno)))
    };
    if ec != 0 {
        // Not valid to print here, but we will panic anyways after that.
        eprintln!("error: {:?}", msg);
        return Err(Error::SignalHandlerUnset);
    }
    Ok(())
}

/// A signal which asks the process to terminate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalEvent {
    Int,
    Term,
}

impl SignalEvent {
    pub fn name(&self) -> &'static str {
        match self {
            SignalEvent::Int => "SIGINT",
            SignalEvent::Term => "SIGTERM",
        }
    }
}

/// Number of repeated signals after which we give up on the graceful path.
const SIGNAL_COUNT_HARD_EXIT: usize = 2;
const EXIT_CODE_HARD: i32 = 13;
const EXIT_CODE_NO_PIPE: i32 = 83;

static PIPE_WRITE_FD: AtomicI32 = AtomicI32::new(-1);
static SIGINT_COUNT: AtomicUsize = AtomicUsize::new(0);
static SIGTERM_COUNT: AtomicUsize = AtomicUsize::new(0);
static INSTALLED: AtomicBool = AtomicBool::new(false);

/// Async-signal-safe: counts the signal and writes a single byte to the self-pipe.
fn note_signal(byte: u8, counter: &AtomicUsize) {
    let n = counter.fetch_add(1, Ordering::AcqRel);
    if n >= SIGNAL_COUNT_HARD_EXIT {
        // The graceful path did not get us out, leave the hard way.
        std::process::exit(EXIT_CODE_HARD);
    }
    let fd = PIPE_WRITE_FD.load(Ordering::Acquire);
    if fd < 0 {
        std::process::exit(EXIT_CODE_NO_PIPE);
    }
    let buf = [byte];
    // A failing write means the reader is gone or the pipe is full. In both cases a shutdown
    // is either already in flight or can not be delivered, and the repeat-counter above is
    // the remaining escape hatch. Nothing safe left to do here.
    let _ = unsafe { libc::write(fd, buf.as_ptr() as _, 1) };
}

fn handler_sigint(_: libc::c_int, _: *const libc::siginfo_t, _: *const libc::c_void) {
    note_signal(b'i', &SIGINT_COUNT);
}

fn handler_sigterm(_: libc::c_int, _: *const libc::siginfo_t, _: *const libc::c_void) {
    note_signal(b't', &SIGTERM_COUNT);
}

/// Receiving end of the installed shutdown signals.
///
/// Dropping this restores the default handlers for SIGINT and SIGTERM.
pub struct SignalHandles {
    rx: async_channel::Receiver<SignalEvent>,
}

impl SignalHandles {
    /// Wait for the next shutdown signal. Returns `None` if the signal handling was torn down.
    ///
    /// Cancel safe.
    pub async fn recv(&self) -> Option<SignalEvent> {
        self.rx.recv().await.ok()
    }

    pub fn receiver(&self) -> &async_channel::Receiver<SignalEvent> {
        &self.rx
    }
}

impl fmt::Debug for SignalHandles {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("SignalHandles").finish()
    }
}

impl Drop for SignalHandles {
    fn drop(&mut self) {
        if let Err(e) = unset_signal_handler(libc::SIGINT) {
            log::error!("SignalHandles drop  unset SIGINT  {e}");
        }
        if let Err(e) = unset_signal_handler(libc::SIGTERM) {
            log::error!("SignalHandles drop  unset SIGTERM  {e}");
        }
        // Closing the write end makes the reader thread observe EOF and finish.
        let fd = PIPE_WRITE_FD.swap(-1, Ordering::AcqRel);
        if fd >= 0 {
            unsafe { libc::close(fd) };
        }
        INSTALLED.store(false, Ordering::Release);
    }
}

/// Install handlers for SIGINT and SIGTERM which deliver into an async channel.
///
/// Uses the self-pipe trick: the signal handler only writes one byte, a dedicated thread turns
/// that into a `SignalEvent`. Repeated signals (more than [`SIGNAL_COUNT_HARD_EXIT`]) terminate
/// the process right away in case the graceful shutdown does not make progress.
///
/// Can only be installed once per process, the returned handle owns the installation.
pub fn install_shutdown_signals() -> Result<SignalHandles, Error> {
    if INSTALLED.swap(true, Ordering::AcqRel) {
        return Err(Error::SignalHandlerAlreadyInstalled);
    }
    let mut fds = [-1 as libc::c_int; 2];
    let ec = unsafe { libc::pipe(&mut fds[0]) };
    if ec != 0 {
        INSTALLED.store(false, Ordering::Release);
        return Err(Error::SignalPipeCreate);
    }
    let (read_fd, write_fd) = (fds[0], fds[1]);
    PIPE_WRITE_FD.store(write_fd, Ordering::Release);
    let (tx, rx) = async_channel::bounded(16);
    // Install from a plain thread, not from a tokio worker.
    let install = std::thread::spawn(|| {
        let act_old_int: RwLock<libc::sigaction> = RwLock::new(unsafe { std::mem::zeroed() });
        let act_old_term: RwLock<libc::sigaction> = RwLock::new(unsafe { std::mem::zeroed() });
        set_signal_handler(libc::SIGINT, handler_sigint, &act_old_int)?;
        set_signal_handler(libc::SIGTERM, handler_sigterm, &act_old_term)?;
        Ok::<_, Error>(())
    });
    match install.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            INSTALLED.store(false, Ordering::Release);
            return Err(e);
        }
        Err(_) => {
            INSTALLED.store(false, Ordering::Release);
            return Err(Error::SignalHandlerSet);
        }
    }
    std::thread::spawn(move || {
        let mut buf = [0u8; 8];
        loop {
            let n = unsafe { libc::read(read_fd, buf.as_mut_ptr() as _, buf.len()) };
            if n <= 0 {
                break;
            }
            for i in 0..n as usize {
                let ev = match buf[i] {
                    b'i' => SignalEvent::Int,
                    b't' => SignalEvent::Term,
                    _ => continue,
                };
                if tx.send_blocking(ev).is_err() {
                    // Receiver gone, nobody left to tell.
                    break;
                }
            }
        }
        unsafe { libc::close(read_fd) };
    });
    Ok(SignalHandles { rx })
}
