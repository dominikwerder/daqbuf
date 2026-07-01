mod Metrics {
    type StructName = CaProtoMetrics;
    enum counters {
        metrics_emit,
        tcp_recv_count,
        tcp_recv_bytes,
        protocol_issue,
        payload_std_too_large,
        payload_ext_but_small,
        payload_ext_very_large,
        out_msg_placed,
        out_bytes,
        fionread_inc,
        fionread_dec,
    }
    enum histolog2s {
        payload_size,
        data_count,
        outbuf_len,
    }
}
