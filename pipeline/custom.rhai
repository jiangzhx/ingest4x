fn normalize_event(event, request) {
    if !event.contains("xwhen") || event["xwhen"] == () {
        event["xwhen"] = epoch_ms();
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

        xcontext["process_info"] = #{
            receive_host_ip: host_ip(),
            receive_time: epoch_ms(),
            ingest4x_version: ingest4x_version()
        };

        event["xcontext"] = xcontext;
    }

    return event;
}
