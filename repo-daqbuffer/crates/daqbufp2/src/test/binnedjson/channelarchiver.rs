use super::*;

#[test]
fn get_scalar_2_events() -> Result<(), Error> {
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
        let begstr = "2021-11-10T00:00:00Z";
        let endstr = "2021-11-10T00:10:00Z";
        let jsstr = get_events_json_common_res(channel, begstr, endstr, cluster).await?;
        let res: ScalarEventsResponse = serde_json::from_str(&jsstr)?;
        let ts_ms: Vec<u64> = vec![
            148, 9751, 19670, 24151, 24471, 24792, 25110, 25430, 25751, 26071, 26391, 26713, 27032, 27356, 27672,
            27991, 28311, 43040, 52966, 62570, 72177, 82105, 91706, 101632, 111235, 121160, 130759, 140677, 150606,
            160209, 170134, 179738, 189980, 200224, 209831, 219751, 225514, 225834, 226154, 226475, 226794, 227116,
            227433, 227755, 228074, 228395, 228714, 229035, 229354, 229674, 245674, 255597, 265510, 275110, 284707,
            294302, 304224, 314138, 324054, 333333, 343248, 352849, 362762, 372363, 382283, 391891, 401796, 411395,
            421634, 431230, 433790, 434110, 434428, 434752, 435068, 435391, 435709, 436028, 436351, 436668, 436990,
            437308, 437628, 437953, 453304, 463222, 472824, 482417, 492019, 501934, 511851, 521447, 531364, 540959,
            550558, 560474, 570071, 579668, 589582,
        ];
        let ts_ns: Vec<u64> = vec![
            943241, 130276, 226885, 258374, 9524, 153770, 179580, 985805, 757887, 751800, 877591, 159972, 764944,
            429832, 426517, 490975, 828473, 101407, 528288, 331264, 131573, 178810, 415039, 544017, 621317, 25989,
            229791, 897343, 130766, 19213, 766900, 92172, 352772, 779613, 521675, 192592, 77354, 998756, 10378, 278841,
            811319, 520706, 673746, 687239, 676867, 251158, 253234, 304222, 241316, 387683, 600611, 524062, 235502,
            793455, 38335, 688777, 318149, 62614, 893092, 188883, 897420, 545225, 949778, 609390, 339743, 35897,
            218211, 159017, 133408, 824998, 269300, 196288, 665918, 597766, 741594, 855975, 727405, 902579, 172017,
            546991, 578579, 735680, 825184, 663507, 543606, 926800, 487587, 970423, 42198, 491516, 409085, 408228,
            480644, 404173, 856513, 364301, 945081, 81850, 868410,
        ];
        assert_eq!(res.ts_anchor, 1636502401);
        assert_eq!(&res.ts_ms, &ts_ms);
        assert_eq!(&res.ts_ns, &ts_ns);
        assert_eq!(res.finalised_range, true);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn get_scalar_2_binned() -> Result<(), Error> {
    if true {
        return Ok(());
    }
    // TODO re-use test data in dedicated convert application.
    let fut = async { return Err::<(), _>(Error::with_msg_no_trace("TODO")) };
    #[cfg(feature = "DISABLED")]
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ARIDI-PCT:CURRENT".into(),
            series: None,
        };
        let begstr = "2021-11-10T00:00:00Z";
        let endstr = "2021-11-10T00:10:00Z";
        let (res, jsstr) =
            get_binned_json_common_res(channel, begstr, endstr, 10, AggKind::TimeWeightedScalar, cluster).await?;
        let exp = r##"{"avgs":[401.1745910644531,401.5135498046875,400.8823547363281,400.66156005859375,401.8301086425781,401.19305419921875,400.5584411621094,401.4371337890625,401.4137268066406,400.77880859375],"counts":[19,6,6,19,6,6,6,19,6,6],"rangeFinal":true,"maxs":[402.04977411361034,401.8439029736943,401.22628955394583,402.1298351124666,402.1298351124666,401.5084092642013,400.8869834159359,402.05358654212733,401.74477983225313,401.1271664125047],"mins":[400.08256099885625,401.22628955394583,400.60867613419754,400.0939982844072,401.5084092642013,400.8869834159359,400.2693699961876,400.05968642775446,401.1271664125047,400.50574056423943],"tsAnchor":1636502400,"tsMs":[0,60000,120000,180000,240000,300000,360000,420000,480000,540000,600000],"tsNs":[0,0,0,0,0,0,0,0,0,0,0]}"##;
        let exp: BinnedResponse = serde_json::from_str(exp).unwrap();
        check_close(&res, &exp, &jsstr)?;
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn get_wave_1_events() -> Result<(), Error> {
    if true {
        return Ok(());
    }
    // TODO re-use test data in dedicated convert application.
    let fut = async { return Err::<(), _>(Error::with_msg_no_trace("TODO")) };
    #[cfg(feature = "DISABLED")]
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ARIDI-MBF-X:CBM-IN".into(),
            series: None,
        };
        let begstr = "2021-11-09T00:00:00Z";
        let endstr = "2021-11-09T00:10:00Z";
        let jsstr = get_events_json_common_res(channel, begstr, endstr, cluster).await?;
        let res: WaveEventsResponse = serde_json::from_str(&jsstr)?;
        // TODO compare with resources/expected/f6882ac49c.json
        let ts_ms: Vec<u64> = vec![
            389, 4389, 30390, 60391, 64391, 96401, 104398, 148393, 184394, 212395, 212395, 244396, 268396, 268396,
            308397, 366399, 408400, 446401, 482402, 484402, 508402, 544403, 570404, 570404,
        ];
        let ts_ns: Vec<u64> = vec![
            815849, 897529, 550829, 342809, 409129, 326629, 71679, 491294, 503054, 182074, 182074, 141729, 581034,
            581034, 676829, 174124, 274914, 184119, 98504, 148344, 777404, 686129, 390264, 390264,
        ];
        assert_eq!(res.ts_anchor, 1636416014);
        assert_eq!(&res.ts_ms, &ts_ms);
        assert_eq!(&res.ts_ns, &ts_ns);
        assert_eq!(res.finalised_range, true);
        assert_eq!(res.values.len(), 24);
        assert_eq!(res.values[0].len(), 480);
        assert_eq!(res.values[1].len(), 480);
        assert!(f64_close(res.values[0][0], 0.00011179182183695957));
        assert!(f64_close(res.values[1][2], 0.00014343370276037604));
        assert!(f64_close(res.values[2][4], 0.00011945325240958482));
        //let exp = r##"{}"##;
        //let exp: WaveEventsResponse = serde_json::from_str(exp).unwrap();
        //check_close_events(&res, &exp, &jsstr)?;
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn get_wave_1_binned() -> Result<(), Error> {
    if true {
        return Ok(());
    }
    // TODO re-use test data in dedicated convert application.
    let fut = async { return Err::<(), _>(Error::with_msg_no_trace("TODO")) };
    #[cfg(feature = "DISABLED")]
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ARIDI-MBF-X:CBM-IN".into(),
            series: None,
        };
        let begstr = "2021-11-09T00:00:00Z";
        let endstr = "2021-11-11T00:10:00Z";
        let (res, jsstr) =
            get_binned_json_common_res(channel, begstr, endstr, 10, AggKind::TimeWeightedScalar, cluster).await?;
        assert_eq!(res.ts_anchor, 1636416000);
        info!("{}", jsstr);
        //let exp = r##"{}"##;
        //let exp: BinnedResponse = serde_json::from_str(exp).unwrap();
        //check_close(&res, &exp, &jsstr)?;
        Ok(())
    };
    taskrun::run(fut)
}
