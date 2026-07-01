use std::ffi::CStr;
use std::mem::MaybeUninit;
use std::sync::RwLock;

autoerr::create_error_v1!(
    name(Error, "LinuxSignalError"),
    enum variants {
        SignalHandlerSet,
        SignalHandlerUnset,
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
