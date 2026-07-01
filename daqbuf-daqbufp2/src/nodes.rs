use crate::spawn_test_hosts;
use daqbuf_err::Error;
use netpod::log::*;
use netpod::Cluster;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::task::JoinHandle;

pub struct RunningHosts {
    pub cluster: Cluster,
    _jhs: Vec<JoinHandle<Result<(), Error>>>,
}

impl Drop for RunningHosts {
    fn drop(&mut self) {
        netpod::log::info!("\n\n+++++++++++++++++++  impl Drop for RunningHost\n\n");
    }
}

pub struct RunningSlsHost {
    pub cluster: Cluster,
    _jhs: Vec<JoinHandle<Result<(), Error>>>,
}

impl Drop for RunningSlsHost {
    fn drop(&mut self) {
        netpod::log::info!("\n\n+++++++++++++++++++  impl Drop for RunningSlsHost\n\n");
    }
}

pub struct RunningArchappHost {
    pub cluster: Cluster,
    _jhs: Vec<JoinHandle<Result<(), Error>>>,
}

impl Drop for RunningArchappHost {
    fn drop(&mut self) {
        netpod::log::info!("\n\n+++++++++++++++++++  impl Drop for RunningArchappHost\n\n");
    }
}

lazy_static::lazy_static! {
    static ref HOSTS_RUNNING: Mutex<Option<Arc<RunningHosts>>> = Mutex::new(None);
    static ref SLS_HOST_RUNNING: Mutex<Option<Arc<RunningSlsHost>>> = Mutex::new(None);
    static ref ARCHAPP_HOST_RUNNING: Mutex<Option<Arc<RunningArchappHost>>> = Mutex::new(None);
}

pub fn require_test_hosts_running() -> Result<Arc<RunningHosts>, Error> {
    let mut g = HOSTS_RUNNING.lock().unwrap();
    match g.as_ref() {
        None => {
            info!("\n\n+++++++++++++++++++  MAKE NEW RunningHosts\n\n");
            let cluster = netpod::test_cluster();
            let jhs = spawn_test_hosts(cluster.clone());
            let ret = RunningHosts {
                cluster: cluster.clone(),
                _jhs: jhs,
            };
            let a = Arc::new(ret);
            *g = Some(a.clone());
            // TODO check in different way that test hosts are up, sockets connected, ready for testing
            thread::sleep(Duration::from_millis(400));
            Ok(a)
        }
        Some(gg) => {
            debug!("\n\n+++++++++++++++++++  REUSE RunningHost\n\n");
            Ok(gg.clone())
        }
    }
}

pub fn require_sls_test_host_running() -> Result<Arc<RunningSlsHost>, Error> {
    let mut g = SLS_HOST_RUNNING.lock().unwrap();
    match g.as_ref() {
        None => {
            netpod::log::info!("\n\n+++++++++++++++++++  MAKE NEW RunningSlsHost\n\n");
            let cluster = netpod::sls_test_cluster();
            let jhs = spawn_test_hosts(cluster.clone());
            let ret = RunningSlsHost {
                cluster: cluster.clone(),
                _jhs: jhs,
            };
            let a = Arc::new(ret);
            *g = Some(a.clone());
            Ok(a)
        }
        Some(gg) => {
            netpod::log::debug!("\n\n+++++++++++++++++++  REUSE RunningSlsHost\n\n");
            Ok(gg.clone())
        }
    }
}

pub fn require_archapp_test_host_running() -> Result<Arc<RunningArchappHost>, Error> {
    let mut g = ARCHAPP_HOST_RUNNING.lock().unwrap();
    match g.as_ref() {
        None => {
            netpod::log::info!("\n\n+++++++++++++++++++  MAKE NEW RunningArchappHost\n\n");
            let cluster = netpod::archapp_test_cluster();
            let jhs = spawn_test_hosts(cluster.clone());
            let ret = RunningArchappHost {
                cluster: cluster.clone(),
                _jhs: jhs,
            };
            let a = Arc::new(ret);
            *g = Some(a.clone());
            Ok(a)
        }
        Some(gg) => {
            netpod::log::debug!("\n\n+++++++++++++++++++  REUSE RunningArchappHost\n\n");
            Ok(gg.clone())
        }
    }
}
