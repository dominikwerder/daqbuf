use scywr::insertqueues::InsertQueuesTx;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct MetricsPrometheusShort {
    counters: Vec<String>,
}

impl MetricsPrometheusShort {
    pub fn prometheus(&self) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        for e in self.counters.iter() {
            write!(&mut s, "{}\n", e).unwrap();
        }
        s
    }
}

impl MetricsPrometheusShort {
    pub fn from_flatten_prometheus(counters: Vec<String>) -> Self {
        Self { counters }
    }

    /// Append the metrics of `inp` to the metrics of `self` so that a single
    /// scrape can carry the metrics of several sub systems.
    pub fn append(&mut self, mut inp: Self) {
        self.counters.append(&mut inp.counters);
    }
}

impl From<&stats::mett::DaemonMetrics> for MetricsPrometheusShort {
    fn from(value: &stats::mett::DaemonMetrics) -> Self {
        Self {
            counters: value.to_flatten_prometheus("daemon"),
        }
    }
}

/// Metrics of the v2 ingest code path (`ca::conn2` and `ca::connset2`).
/// The name prefix is distinct from the v1 `daemon` prefix, therefore both
/// code paths can be scraped side by side without colliding.
impl From<&stats::mett::ConnSet2Metrics> for MetricsPrometheusShort {
    fn from(value: &stats::mett::ConnSet2Metrics) -> Self {
        Self {
            counters: value.to_flatten_prometheus("daemon2_connset"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::MetricsPrometheusShort;

    /// The v2 metrics tree must reach the prometheus output, including the
    /// metrics which the CaConn collects and hands up to the ConnSet.
    #[test]
    fn connset2_metrics_to_prometheus() {
        let mut conn = stats::mett::CaConn2Metrics::new();
        conn.tcp_connected().inc();
        conn.poll_fn_begin().add(7);
        conn.connected().channel_handler_new().inc();
        conn.connected().channel_handler().event_add_recv().add(3);
        conn.connected().proto().tcp_recv_bytes().add(4096);
        let mut connset = stats::mett::ConnSet2Metrics::new();
        connset.ca_conn_create().inc();
        connset.ca_conn().ingest(conn.take_and_reset());
        connset.ca_conn_count().set(1);
        let s = MetricsPrometheusShort::from(&connset).prometheus();
        assert!(s.contains("daemon2_connset_ca_conn_create 1\n"), "{s}");
        assert!(s.contains("daemon2_connset_ca_conn_tcp_connected 1\n"), "{s}");
        assert!(s.contains("daemon2_connset_ca_conn_poll_fn_begin 7\n"), "{s}");
        assert!(
            s.contains("daemon2_connset_ca_conn_connected_channel_handler_new 1\n"),
            "{s}"
        );
        assert!(
            s.contains("daemon2_connset_ca_conn_connected_channel_handler_event_add_recv 3\n"),
            "{s}"
        );
        assert!(
            s.contains("daemon2_connset_ca_conn_connected_proto_tcp_recv_bytes 4096\n"),
            "{s}"
        );
        assert!(s.contains("daemon2_connset_ca_conn_count 1\n"), "{s}");
        // The v2 names must not collide with the v1 names.
        assert!(!s.contains("\ndaemon_"), "{s}");
    }

    /// The scylla insert worker metrics reach the daemon scrape.
    #[test]
    fn scylla_worker_metrics_to_prometheus() {
        let mut w = stats::mett::ScyllaInsertWorker::new();
        w.worker_start().inc();
        w.job_ok().add(5);
        w.db_timeout().inc();
        w.db_error().add(2);
        w.jobtrans().SeriesData().add(5);
        w.input().batch_recv().inc();
        w.input().item_recv().add(9);
        w.input().fut_prepared().add(9);
        w.input().batch_len().push_val(9);
        let mut daemon = stats::mett::DaemonMetrics::new();
        daemon.scy_inswork().ingest(w.take_and_reset());
        let s = MetricsPrometheusShort::from(&daemon).prometheus();
        assert!(s.contains("daemon_scy_inswork_worker_start 1\n"), "{s}");
        assert!(s.contains("daemon_scy_inswork_job_ok 5\n"), "{s}");
        assert!(s.contains("daemon_scy_inswork_db_timeout 1\n"), "{s}");
        assert!(s.contains("daemon_scy_inswork_db_error 2\n"), "{s}");
        assert!(s.contains("daemon_scy_inswork_jobtrans_SeriesData 5\n"), "{s}");
        assert!(s.contains("daemon_scy_inswork_input_batch_recv 1\n"), "{s}");
        assert!(s.contains("daemon_scy_inswork_input_item_recv 9\n"), "{s}");
        assert!(s.contains("daemon_scy_inswork_input_fut_prepared 9\n"), "{s}");
        assert!(s.contains("daemon_scy_inswork_input_batch_len_count 1\n"), "{s}");
    }

    /// A scrape can carry the metrics of the v1 and the v2 code path.
    #[test]
    fn append_keeps_both() {
        let daemon = stats::mett::DaemonMetrics::new();
        let connset = stats::mett::ConnSet2Metrics::new();
        let mut a = MetricsPrometheusShort::from(&daemon);
        a.append(MetricsPrometheusShort::from(&connset));
        let s = a.prometheus();
        assert!(s.contains("daemon_handle_event 0\n"), "{s}");
        assert!(s.contains("daemon2_connset_ca_conn_create 0\n"), "{s}");
    }
}
