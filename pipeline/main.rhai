import "custom" as custom;

fn main(event, request) {
    let validation = validate(event);
    if !validation["ok"] {
        event = custom::attach_reject_process_info(event, request, validation["error"]);
        return reject(event, validation["error"]);
    }

    event = custom::normalize_event(event, request);

    return accept(event);
}
