mod Metrics {
    type StructName = ChannelHandlerMetrics;
    enum counters {
        monitor_read_expected,
        read_notify_send,
        read_notify_recv,
        event_add_recv,
        chan_tx_err,
        ts_msp_reput_onevent,
        writer_ignore_rewind_time,
        writer_ignore_same_time,
        writer_ignore_same_value,
        writer_ignore_monitor_not_min_quiet,
        writer_ignore_poll_not_min_quiet,
        writer_ignore_rate_cap,
    }
    enum histolog2s {
        ca_ts_off,
    }
}

mod Metrics {
    type StructName = CaConnConnectedMetrics;
    enum counters {
        channel_handler_new,
    }
    mod Compose {
        type Input = ChannelHandlerMetrics;
        type Name = channel_handler;
    }
    mod Compose {
        type Input = ca_proto::mett::CaProtoMetrics;
        type Name = proto;
    }
}

mod Metrics {
    type StructName = ScyllaJobTransform;
    enum counters {
        SeriesData,
        SeriesMsp,
        TimeBinSimpleF32V02,
        BinWriteIndexV04,
        Accounting,
        AccountingRecv,
    }
}

// Counts the work which arrives at a scylla insert worker, before it is
// turned into database futures. Together with ScyllaJobTransform (which counts
// the jobs that completed) this shows whether the worker keeps up.
mod Metrics {
    type StructName = ScyllaWorkerInput;
    enum counters {
        batch_recv,
        item_recv,
        item_insert,
        item_msp,
        item_timebin_simple_f32_v02,
        item_bin_write_index_v04,
        item_accounting,
        item_accounting_recv,
        item_ignored,
        fut_prepared,
    }
    enum histolog2s {
        batch_len,
        futs_per_batch,
    }
}

mod Metrics {
    type StructName = ScyllaInsertWorker;
    enum counters {
        metrics_emit,
        job_ok,
        job_err,
        worker_start,
        worker_finish,
        worker_dummy_start,
        worker_dummy_finish,
        db_timeout,
        db_error,
        db_no_future,
    }
    enum histolog2s {
        job_dt1,
        job_dt2,
        job_dt_net,
        job_npoll,
    }
    mod Compose {
        type Input = ScyllaJobTransform;
        type Name = jobtrans;
    }
    mod Compose {
        type Input = ScyllaWorkerInput;
        type Name = input;
    }
}

mod Metrics {
    type StructName = CaConnMetrics;
    enum counters {
        metrics_emit,
        metrics_emit_final,
        proto_out_push,
        logic_error,
        poll_fn_begin,
        poll_loop_begin,
        poll_no_progress_no_pending,
        poll_pending,
        poll_reloop,
        poll_wake_break,
        insert_item_queue_full,
        insert_item_queue_pressure,
        out_queue_full,
        loop2_count,
        loop3_count,
        tcp_connected,
        ca_proto_no_version_as_first,
        ca_proto_version_later,
        ca_msg_recv,
        event_add_res_recv,
        time_check_channels_state_init,
        ping_no_proto,
        ping_start,
        pong_timeout,
        caget_timeout,
        caget_issued,
        fn_handle_event_add_res,
        fn_handle_read_notify_res,
        unknown_ioid,
        monitor_stale_read_begin,
        monitor_stale_read_timeout,
        ioid_read_error_exists,
        ioid_read_begin,
        recv_read_notify_ioid_not_found,
        recv_read_notify_channel_not_found,
        recv_read_notify_channel_unexpected_state,
        recv_read_notify_channel_transition,
        recv_read_notify_channel_sid_mismatch,
        recv_read_notify_poll_wait,
        recv_read_notify_poll_idle,
        recv_read_notify_monitor_passive,
        recv_read_notify_state_read_pending,
        recv_read_notify_state_read_pending_bad_ioid,
        recv_read_notify_while_enabling_monitoring,
        recv_read_notify_but_no_longer_ready,
        recv_read_notify_but_not_init_yet,
        recv_event_add_while_wait_on_read_notify,
        no_cid_for_subid,
        monitoring_read_expected,
        monitoring_read_unexpected,
        monitoring_read_diff_time,
        monitoring_read_diff_value,
        transition_to_polling,
        transition_to_polling_bad_state,
        transition_to_polling_already_in,
        polling_read_timeout,
        unknown_subid,
        get_series_id_ok,
        channel_add_exists,
        ts_msp_reput_onevent,
        ts_msp_reput_periodic,
        series_writer_on_close,
        writer_ignore_rewind_time,
        writer_ignore_same_time,
        writer_ignore_same_value,
        writer_ignore_monitor_not_min_quiet,
        writer_ignore_poll_not_min_quiet,
        writer_ignore_rate_cap,
        emit_channel_status_item,
    }
    enum values {
        channel_all_count,
        channel_alive_count,
        channel_not_alive_count,
    }
    enum histolog2s {
        clock_ioc_diff_abs,
        caget_lat,
        poll_reloops,
        poll_all_dt,
        poll_op3_dt,
        iiq_batch_len,
        pong_recv_lat,
        ca_ts_off,
    }
    mod Compose {
        type Input = ca_proto::mett::CaProtoMetrics;
        type Name = proto;
    }
}

mod Metrics {
    type StructName = CaConnSetMetrics;
    mod Compose {
        type Input = CaConnMetrics;
        type Name = ca_conn;
    }
    enum counters {
        poll_fn_begin,
        poll_loop_begin,
        ready_for_end_of_stream,
        ready_for_end_of_stream_with_progress,
        poll_reloop,
        poll_pending,
        poll_no_progress_no_pending,
        ioc_search_start,
        chan_send_err,
        cmd_res_send_err,
        logic_err,
        channel_status_series_found,
        ioc_addr_found,
        ioc_addr_not_found,
        ioc_addr_result_for_unknown_channel,
        ca_conn_eos_ok,
        ca_conn_eos_unexpected,
        handle_add_channel_with_addr,
        try_push_ca_conn_cmds_sent,
        try_push_ca_conn_cmds_closed,
        create_ca_conn,
        storage_insert_queue_send,
        ca_conn_task_join_done_ok,
        ca_conn_task_join_done_err,
        ca_conn_task_join_err,
    }
    enum values {
        channel_info_query_queue_len,
        channel_info_query_sender_len,
        channel_info_res_tx_len,
        ca_conn_res_tx_len,
        find_ioc_query_sender_len,
        channel_rogue,
        channel_unknown_address,
        channel_search_pending,
        channel_no_address,
        channel_unassigned,
        channel_assigned,
        channel_connected,
        channel_maybe_wrong_address,
        channel_assigned_without_health_update,
        channel_health_timeout_soon,
        channel_health_timeout_reached,
    }
    enum histolog2s {
        poll_all_dt,
    }
}

// mod Metrics {
//     type StructName = IocFinderMetrics;
//     enum counters {
//         dbsearcher_batch_recv,
//         dbsearcher_item_recv,
//         dbsearcher_select_res_0,
//         dbsearcher_select_error_len_mismatch,
//         dbsearcher_batch_send,
//         dbsearcher_item_send,
//         ca_udp_error,
//         ca_udp_warn,
//         ca_udp_unaccounted_data,
//         ca_udp_batch_created,
//         ca_udp_io_error,
//         ca_udp_io_empty,
//         ca_udp_io_recv,
//         ca_udp_first_msg_not_version,
//         ca_udp_recv_result,
//         ca_udp_recv_timeout,
//         ca_udp_logic_error,
//     }
// }

mod Metrics {
    type StructName = DaemonMetrics;
    mod Compose {
        type Input = CaConnSetMetrics;
        type Name = ca_conn_set;
    }
    mod Compose {
        type Input = ScyllaInsertWorker;
        type Name = scy_inswork;
    }
    enum counters {
        handle_event,
        caconnset_health_response,
        channel_send_err,
    }
    enum values {
        proc_cpu_v0,
        proc_mem_rss,
        iqtx_len_st_rf1,
        iqtx_len_st_rf3,
        iqtx_len_mt_rf3,
        iqtx_len_lt_rf3,
        iqtx_len_lt_rf3_lat5,
    }
}

// ----------------------------------------------------------------------------
// Metrics for the v2 ingest code path (netfetch::ca::conn2 and
// netfetch::ca::connset2).
// These are declared separately from the v1 (ca::conn, ca::connset) metrics on
// purpose: the two code paths have different internal structure and we do not
// want to change the metrics which the production v1 path emits.
// ----------------------------------------------------------------------------

mod Metrics {
    type StructName = CaConn2Metrics;
    enum counters {
        metrics_emit,
        poll_fn_begin,
        poll_reloop,
        poll_pending,
        poll_no_progress_no_pending,
        ticker_fired,
        status_info_emit,
        status_info_out_queue_full,
        health_check_fail,
        cmd_recv,
        cmd_channel_add,
        cmd_channel_remove,
        cmd_disconnect_on_idle,
        cmd_dyn_v03,
        cmd_channels_for_addr_v1,
        cmd_channels_for_addr_v2,
        cmd_channels_by_regex_v1,
        cmd_send_err,
        cmd_res_send_err,
        tcp_connected,
        connect_error,
        connected_error,
        connected_end_of_stream,
        item_channel_info_query,
        item_test_value,
        item_local_log,
        item_channel_event_value,
        item_channel_write_items,
        item_channel_trace,
        write_batch_flush,
        write_batch_flush_blocked,
    }
    enum values {
        out_buf_len,
        write_batch_len,
    }
    enum histolog2s {
        poll_all_dt,
        write_batch_flush_len,
    }
    mod Compose {
        type Input = CaConnConnectedMetrics;
        type Name = connected;
    }
}

mod Metrics {
    type StructName = ConnSet2Metrics;
    enum counters {
        metrics_request,
        conn_item_recv,
        conn_metrics_recv,
        conn_status_info,
        conn_channel_info_query,
        conn_test_value,
        conn_local_log,
        conn_channel_event_value,
        conn_channel_write_items,
        conn_channel_trace,
        write_sender_batch_send,
        write_sender_closed,
        conn_recv_error,
        conn_done,
        conn_done_not_in_registry,
        conn_error_not_in_registry,
        ca_conn_create,
        cmd_channel_add,
        cmd_channel_add_exists,
        cmd_channel_remove,
        cmd_shutdown,
        cmd_connection_list_v1,
        cmd_channels_for_addr_v1,
        cmd_channels_for_addr_v2,
        cmd_dyn_v1,
        cmd_scatter_gather_v1,
        channel_idle_disconnect_trigger,
        channel_idle_disconnect_err,
    }
    enum values {
        ca_conn_count,
        channel_count,
        write_staging_len,
    }
    enum histolog2s {
        write_sender_batch_len,
    }
    mod Compose {
        type Input = CaConn2Metrics;
        type Name = ca_conn;
    }
}
