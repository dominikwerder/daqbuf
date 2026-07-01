mod channelarchiver;

use daqbuf_err::Error;

#[test]
fn get_sls_archive_1() -> Result<(), Error> {
    if true {
        return Ok(());
    }
    // TODO re-use test data in dedicated convert application.
    let fut = async { Err::<(), _>(Error::with_msg_no_trace("TODO")) };
    #[cfg(feature = "DISABLED")]
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ABOMA-CH-6G:U-DCLINK".into(),
            series: None,
        };
        let begstr = "2021-11-10T01:00:00Z";
        let endstr = "2021-11-10T01:01:00Z";
        let (res, jsstr) =
            get_binned_json_common_res(channel, begstr, endstr, 10, AggKind::TimeWeightedScalar, cluster).await?;
        let exp = r##"{"avgs":[24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875],"counts":[0,0,0,0,0,0,0,0,0,0,0,0],"rangeFinal":true,"maxs":[24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875],"mins":[24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875],"tsAnchor":1636506000,"tsMs":[0,5000,10000,15000,20000,25000,30000,35000,40000,45000,50000,55000,60000],"tsNs":[0,0,0,0,0,0,0,0,0,0,0,0,0]}"##;
        let exp: String = serde_json::from_str(exp).unwrap();
        check_close(&res, &exp, &jsstr)?;
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn get_sls_archive_3() -> Result<(), Error> {
    if true {
        return Ok(());
    }
    // TODO re-use test data in dedicated convert application.
    let fut = async { Err::<(), _>(Error::with_msg_no_trace("TODO")) };
    #[cfg(feature = "DISABLED")]
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ARIDI-PCT:CURRENT".into(),
            series: None,
        };
        let begstr = "2021-11-09T00:00:00Z";
        let endstr = "2021-11-11T00:10:00Z";
        let (res, jsstr) =
            get_binned_json_common_res(channel, begstr, endstr, 10, AggKind::TimeWeightedScalar, cluster).await?;
        let exp = r##"{"avgs":[401.1354675292969,401.1296081542969,401.1314392089844,401.134765625,401.1371154785156,376.5816345214844,401.13775634765625,209.2684783935547,-0.06278431415557861,-0.06278431415557861,-0.06278431415557861,-0.047479934990406036,0.0],"counts":[2772,2731,2811,2689,2803,2203,2355,1232,0,0,0,2,0],"maxs":[402.1717718261533,402.18702154022117,402.1908339687381,402.198458825772,402.17939668318724,402.194646397255,402.1908339687381,402.1908339687381,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,0.0,0.0],"mins":[400.0291869996188,400.02537457110185,400.0291869996188,400.0329994281358,400.0291869996188,0.0,400.0444367136866,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,0.0],"tsAnchor":1636416000,"tsMs":[0,14400000,28800000,43200000,57600000,72000000,86400000,100800000,115200000,129600000,144000000,158400000,172800000,187200000],"tsNs":[0,0,0,0,0,0,0,0,0,0,0,0,0,0]}"##;
        let exp: BinnedResponse = serde_json::from_str(exp).unwrap();
        check_close(&res, &exp, &jsstr)?;
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn get_sls_archive_wave_2() -> Result<(), Error> {
    if true {
        return Ok(());
    }
    // TODO re-use test data in dedicated convert application.
    let fut = async { Err::<(), _>(Error::with_msg_no_trace("TODO")) };
    #[cfg(feature = "DISABLED")]
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ARIDI-MBF-X:CBM-IN".into(),
            series: None,
        };
        let begstr = "2021-11-09T10:00:00Z";
        let endstr = "2021-11-10T06:00:00Z";
        let (res, jsstr) =
            get_binned_json_common_res(channel, begstr, endstr, 10, AggKind::TimeWeightedScalar, cluster).await?;
        let exp = r##"{"avgs":[2.0403556177939208e-8,1.9732556921780997e-8,1.9948116047885378e-8,2.024017220492169e-8,2.1306243880303555e-8,1.998394871804976e-8,1.776692748478581e-8,2.002254362309941e-8,2.0643645015638867e-8,2.0238848819076338e-8],"counts":[209,214,210,219,209,192,171,307,285,232],"maxs":[0.001784245832823217,0.0016909628175199032,0.0017036109929904342,0.0016926786629483104,0.0017604742897674441,0.0018568832892924547,0.001740367733873427,0.0017931810580193996,0.0017676990246400237,0.002342566382139921],"mins":[0.000040829672798281536,0.00004028259718324989,0.000037641591916326433,0.000039788486901670694,0.00004028418697998859,0.00003767738598980941,0.0,0.00004095739495824091,0.00004668773908633739,0.00003859612115775235],"tsAnchor":1636452000,"tsMs":[0,7200000,14400000,21600000,28800000,36000000,43200000,50400000,57600000,64800000,72000000],"tsNs":[0,0,0,0,0,0,0,0,0,0,0]}"##;
        let exp: BinnedResponse = serde_json::from_str(exp).unwrap();
        check_close(&res, &exp, &jsstr)?;
        Ok(())
    };
    taskrun::run(fut)
}
