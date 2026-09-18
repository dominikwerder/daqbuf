use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use daqbuf_ca_proto::pva::client;
use daqbuf_ca_proto::pva::proto::PvaRequest;
use daqbuf_ca_proto::pva::value::PvaStructValue;
use daqbuf_ca_proto::pva::value::PvaValue;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio::time::timeout;

const PVA_PORT: u16 = 5075;
const PV_NAME: &str = "PVA_E2E_TEST:SINECOUNTER";
const UPDATE_PERIOD: Duration = Duration::from_millis(100);

static SERVER_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn server_lock() -> &'static Mutex<()> {
    SERVER_LOCK.get_or_init(|| Mutex::new(()))
}

fn server_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../pva-test-server-java")
}

fn jar_path() -> PathBuf {
    server_dir().join("target/pva-test-server-1.0.0.jar")
}

fn have_binary(name: &str) -> bool {
    Command::new(name)
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success() || s.code().is_some())
        .unwrap_or(false)
}

fn ensure_jar_built() -> bool {
    if jar_path().is_file() {
        return true;
    }
    if !have_binary("mvn") {
        return false;
    }
    let status = Command::new("mvn")
        .args(["-q", "-DskipTests", "package"])
        .current_dir(server_dir())
        .status();
    matches!(status, Ok(s) if s.success()) && jar_path().is_file()
}

struct ServerGuard(std::process::Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn spawn_ready_server() -> Option<ServerGuard> {
    if !have_binary("java") {
        eprintln!("skipping: no `java` binary available");
        return None;
    }
    if !ensure_jar_built() {
        eprintln!("skipping: pva-test-server jar not built and could not be built (no `mvn` or build failed)");
        return None;
    }
    if TcpStream::connect(("127.0.0.1", PVA_PORT)).await.is_ok() {
        panic!("port {PVA_PORT} already in use; stop whatever is listening there before running this test");
    }
    let child = Command::new("java")
        .arg("-jar")
        .arg(jar_path())
        .arg(PV_NAME)
        .arg("10.0")
        .env("EPICS_PVA_AUTO_ADDR_LIST", "NO")
        .env("EPICS_PVA_ADDR_LIST", "127.0.0.1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn java server");
    Some(ServerGuard(child))
}

async fn try_get_once(request: PvaRequest, per_try: Duration) -> Result<PvaStructValue, String> {
    let attempt = async {
        let tcp = TcpStream::connect(("127.0.0.1", PVA_PORT))
            .await
            .map_err(|e| format!("connect: {e}"))?;
        tcp.set_nodelay(true).ok();
        let tcp = TcpAsyncWriteRead::from(tcp);
        client::get_once(tcp, "e2e-test".into(), PV_NAME, request, 1024 * 1024)
            .await
            .map_err(|e| format!("get_once: {e}"))
    };
    match timeout(per_try, attempt).await {
        Ok(x) => x,
        Err(_) => Err("timed out".into()),
    }
}

async fn wait_until_ready(deadline: Duration) -> PvaStructValue {
    let start = Instant::now();
    loop {
        match try_get_once(PvaRequest::all(), Duration::from_secs(3)).await {
            Ok(v) => return v,
            Err(e) => {
                if start.elapsed() > deadline {
                    panic!("server never became ready within {deadline:?}: last error: {e}");
                }
                sleep(Duration::from_millis(300)).await;
            }
        }
    }
}

#[tokio::test]
#[ignore]
async fn read_sine_counter_pv_twice() {
    let _lock = server_lock().lock().await;
    let Some(_guard) = spawn_ready_server().await else {
        return;
    };

    let first = wait_until_ready(Duration::from_secs(30)).await;
    assert_eq!(first.type_id(), "example:SineCounter:1.0");

    let counter0 = first.get("counter").and_then(PvaValue::as_i64).expect("counter field");
    let value0 = first.get("value").and_then(PvaValue::as_f64).expect("value field");
    assert!((-1.0..=1.0).contains(&value0));

    let secs = first
        .get("timeStamp.secondsPastEpoch")
        .and_then(PvaValue::as_i64)
        .expect("timeStamp.secondsPastEpoch field");
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(
        (now - secs).abs() < 60,
        "server timestamp {secs} far from local time {now}"
    );

    first
        .get("timeStamp.nanoseconds")
        .and_then(PvaValue::as_i64)
        .expect("timeStamp.nanoseconds field");
    first
        .get("timeStamp.userTag")
        .and_then(PvaValue::as_i64)
        .expect("timeStamp.userTag field");

    sleep(Duration::from_millis(400)).await;

    let second = try_get_once(
        PvaRequest::parse("field(counter,value)").unwrap(),
        Duration::from_secs(5),
    )
    .await
    .expect("second get_once failed");
    let counter1 = second.get("counter").and_then(PvaValue::as_i64).expect("counter field");
    assert!(counter1 > counter0, "counter did not advance: {counter0} -> {counter1}");
    let value1 = second.get("value").and_then(PvaValue::as_f64).expect("value field");
    assert!((-1.0..=1.0).contains(&value1));
}

#[tokio::test]
#[ignore]
async fn monitor_receives_events_for_two_seconds() {
    let _lock = server_lock().lock().await;
    let Some(_guard) = spawn_ready_server().await else {
        return;
    };

    wait_until_ready(Duration::from_secs(30)).await;

    let tcp = TcpStream::connect(("127.0.0.1", PVA_PORT))
        .await
        .expect("connect failed");
    tcp.set_nodelay(true).ok();
    let tcp = TcpAsyncWriteRead::from(tcp);
    let mut session = timeout(
        Duration::from_secs(10),
        client::open_monitor(tcp, "e2e-monitor".into(), PV_NAME, PvaRequest::all(), 1024 * 1024),
    )
    .await
    .expect("open_monitor timed out")
    .expect("open_monitor failed");
    assert_eq!(session.ty().id(), "example:SineCounter:1.0");

    let collect_for = Duration::from_secs(2);
    let deadline = Instant::now() + collect_for;
    let mut count = 0usize;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, session.next_delta()).await {
            Ok(Ok(Some(_delta))) => count += 1,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("monitor error: {e}"),
            Err(_) => break,
        }
    }

    let _ = session.close().await;

    let expected = (collect_for.as_secs_f64() / UPDATE_PERIOD.as_secs_f64()).round() as i64;
    let got = count as i64;
    assert!(
        (got - expected).abs() <= expected / 2 + 3,
        "expected roughly {expected} monitor events in {collect_for:?}, got {got}"
    );
}
