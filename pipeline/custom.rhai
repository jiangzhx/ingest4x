fn normalize_event(event, request) {
    if !event.contains("xwhen") || event["xwhen"] == () {
        let received_at_ms = request.received_at_ms();
        event["xwhen"] = received_at_ms;
    }

    if event.contains("xcontext") {
        let xcontext = event["xcontext"];

        if xcontext.contains("os") && xcontext["os"] != () {
            xcontext["os"] = xcontext["os"].to_lower();
        }

        if xcontext.contains("currencytype") && xcontext["currencytype"] != () {
            xcontext["currencytype"] = xcontext["currencytype"].to_upper();
        }

        if !xcontext.contains("platform") || xcontext["platform"] == () {
            if xcontext.contains("os") && xcontext["os"] != () {
                let os = xcontext["os"];
                let known_platforms = ["ios", "android", "harmony", "wechat", "toutiao", "tiktok"];
                if known_platforms.contains(os) {
                    xcontext["platform"] = os;
                }
            }
        }

        if !xcontext.contains("ip") || xcontext["ip"] == () {
            let ip = request.ip();
            if ip != () {
                xcontext["ip"] = ip;
            }
        }

        xcontext["process_info"] = process_info(request, ());

        event["xcontext"] = xcontext;
    }

    return event;
}

fn attach_reject_process_info(event, request, reason) {
    if !event.contains("xcontext") || event["xcontext"] == () {
        event["xcontext"] = #{};
    }

    let xcontext = event["xcontext"];
    xcontext["process_info"] = process_info(request, reason);
    event["xcontext"] = xcontext;
    return event;
}

fn process_info(request, reason) {
    let receive_time = request.received_at_ms();

    let info = #{
        receive_host_ip: host_ip(),
        receive_time: receive_time,
        ingest4x_version: ingest4x_version()
    };

    let request_id = request.request_id();
    if request_id != () {
        info["request_id"] = request_id;
    }

    if reason != () {
        info["reason"] = reason;
    }

    return info;
}
