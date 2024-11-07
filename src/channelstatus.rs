use daqbuf_err as err;

#[derive(Debug, Clone)]
pub enum ChannelStatusClosedReason {
    ShutdownCommand,
    ChannelRemove,
    ProtocolError,
    FrequencyQuota,
    BandwidthQuota,
    InternalError,
    IocTimeout,
    NoProtocol,
    ProtocolDone,
    ConnectFail,
    IoError,
}

#[derive(Debug, Clone)]
pub enum ChannelStatus {
    AssignedToAddress,
    Opened,
    Closed(ChannelStatusClosedReason),
    Pong,
    MonitoringSilenceReadStart,
    MonitoringSilenceReadTimeout,
    MonitoringSilenceReadUnchanged,
    HaveStatusId,
    HaveAddress,
}

impl ChannelStatus {
    pub fn to_kind(&self) -> u32 {
        use ChannelStatus::*;
        use ChannelStatusClosedReason::*;
        match self {
            AssignedToAddress => 24,
            Opened => 1,
            Closed(x) => match x {
                ShutdownCommand => 2,
                ChannelRemove => 3,
                ProtocolError => 4,
                FrequencyQuota => 5,
                BandwidthQuota => 6,
                InternalError => 7,
                IocTimeout => 8,
                NoProtocol => 9,
                ProtocolDone => 10,
                ConnectFail => 11,
                IoError => 12,
            },
            Pong => 25,
            MonitoringSilenceReadStart => 26,
            MonitoringSilenceReadTimeout => 27,
            MonitoringSilenceReadUnchanged => 28,
            HaveStatusId => 29,
            HaveAddress => 30,
        }
    }

    pub fn from_kind(kind: u32) -> Result<Self, err::Error> {
        use ChannelStatus::*;
        use ChannelStatusClosedReason::*;
        let ret = match kind {
            1 => Opened,
            2 => Closed(ShutdownCommand),
            3 => Closed(ChannelRemove),
            4 => Closed(ProtocolError),
            5 => Closed(FrequencyQuota),
            6 => Closed(BandwidthQuota),
            7 => Closed(InternalError),
            8 => Closed(IocTimeout),
            9 => Closed(NoProtocol),
            10 => Closed(ProtocolDone),
            11 => Closed(ConnectFail),
            12 => Closed(IoError),
            24 => AssignedToAddress,
            25 => Pong,
            26 => MonitoringSilenceReadStart,
            27 => MonitoringSilenceReadTimeout,
            28 => MonitoringSilenceReadUnchanged,
            29 => HaveStatusId,
            30 => HaveAddress,
            _ => {
                return Err(err::Error::with_msg_no_trace(format!(
                    "unknown ChannelStatus kind {kind}"
                )));
            }
        };
        Ok(ret)
    }

    pub fn to_u64(&self) -> u64 {
        self.to_kind() as u64
    }

    pub fn to_user_variant_string(&self) -> String {
        use ChannelStatus::*;
        let ret = match self {
            AssignedToAddress => "Located",
            Opened => "Opened",
            Closed(_) => "Closed",
            Pong => "Pongg",
            MonitoringSilenceReadStart => "MSRS",
            MonitoringSilenceReadTimeout => "MSRT",
            MonitoringSilenceReadUnchanged => "MSRU",
            HaveStatusId => "HaveStatusId",
            HaveAddress => "HaveAddress",
        };
        ret.into()
    }
}
