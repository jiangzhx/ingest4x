import "custom" as custom;

fn main(event, request) {
    let validation = validate(event);
    if !validation["ok"] {
        return reject(event, validation["error"]);
    }

    event = custom::normalize_event(event, request);

    return accept(event);
}
