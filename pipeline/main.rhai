import "custom" as custom;

fn process(event, request) {
    let validation = validate(event);
    if !validation["ok"] {
        event = custom::attach_reject_process_info(event, request, validation["error"], validation["code"]);
        emit("events_error", event);
    } else {
        event = custom::normalize_event(event, request);
        emit("events", event);
    }
}
