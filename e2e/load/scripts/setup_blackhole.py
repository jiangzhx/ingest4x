#!/usr/bin/env python3
import json
import os
import re
import sys
import urllib.error
import urllib.request


ADMIN_URL = os.environ.get("ADMIN_URL", "http://127.0.0.1:18092").rstrip("/")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "ingest4x-load")
PROJECT_NAME = os.environ.get("LOADTEST_PROJECT_NAME", "loadtest_app")
INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "igx_loadtest_token")
TARGET_ID = os.environ.get("LOADTEST_TARGET_ID", "loadtest_blackhole")
SINK_ID = os.environ.get("LOADTEST_SINK_ID", "loadtest_events")
PROCESSOR_KEY = os.environ.get("LOADTEST_PROCESSOR_KEY", "loadtest_blackhole_processor")
SINK_MODE = os.environ.get("LOADTEST_SINK_MODE", "ok")
DELAY_MS = int(os.environ.get("LOADTEST_DELAY_MS", "0"))


def request(method, path, payload=None, expected=(200,)):
    body = None
    headers = {"x-admin-password": ADMIN_PASSWORD}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["content-type"] = "application/json"

    req = urllib.request.Request(
        f"{ADMIN_URL}{path}",
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            data = response.read()
            if response.status not in expected:
                raise RuntimeError(f"{method} {path} returned {response.status}: {data!r}")
            if not data:
                return None
            return json.loads(data.decode("utf-8"))
    except urllib.error.HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed: HTTP {error.code}: {details}") from error


def first_by(items, field, value):
    return next((item for item in items if item.get(field) == value), None)


def sink_constant_name(sink_id):
    constant = ["SINK_"]
    previous_was_separator = False
    for ch in sink_id:
        if ch.isalnum() and ch.isascii():
            constant.append(ch.upper())
            previous_was_separator = False
        elif not previous_was_separator:
            constant.append("_")
            previous_was_separator = True
    name = "".join(constant).rstrip("_")
    if name == "SINK":
        raise RuntimeError(f"invalid sink id for Rhai constant: {sink_id}")
    return name


def destination_config():
    config = {}
    if SINK_MODE != "ok":
        config["mode"] = SINK_MODE
    if DELAY_MS > 0:
        config["delay_ms"] = DELAY_MS
    return config


def ensure_project():
    project = first_by(request("GET", "/api/admin/projects"), "ingest_token", INGEST_TOKEN)
    payload = {
        "name": PROJECT_NAME,
        "enabled": True,
        "ingest_token": INGEST_TOKEN,
    }
    if project is None:
        return request("POST", "/api/admin/projects", payload, expected=(201,))
    return request(
        "PUT",
        f"/api/admin/projects/{project['id']}",
        payload,
        expected=(200,),
    )


def ensure_delivery_target():
    target = first_by(request("GET", "/api/admin/delivery-targets"), "target_id", TARGET_ID)
    if target is None:
        return request(
            "POST",
            "/api/admin/delivery-targets",
            {
                "target_id": TARGET_ID,
                "name": "Loadtest Blackhole",
                "target_type": "blackhole",
                "config_json": {},
                "enabled": True,
            },
            expected=(201,),
        )
    if target.get("target_type") != "blackhole":
        raise RuntimeError(
            f"delivery target {TARGET_ID!r} already exists with target_type={target.get('target_type')!r}"
        )
    return request(
        "PUT",
        f"/api/admin/delivery-targets/{target['id']}",
        {
            "name": "Loadtest Blackhole",
            "config_json": {},
            "enabled": True,
        },
        expected=(200,),
    )


def ensure_event_sink(target_id):
    sink = first_by(request("GET", "/api/admin/event-sinks"), "sink_id", SINK_ID)
    payload = {
        "name": "Loadtest Events",
        "delivery_target_id": target_id,
        "destination_json": destination_config(),
        "auto_offset_reset": "latest",
        "enabled": True,
    }
    if sink is None:
        return request(
            "POST",
            "/api/admin/event-sinks",
            {
                "sink_id": SINK_ID,
                **payload,
            },
            expected=(201,),
        )
    return request(
        "PUT",
        f"/api/admin/event-sinks/{sink['id']}",
        payload,
        expected=(200,),
    )


def processor_source():
    sink_constant = sink_constant_name(SINK_ID)
    return f"""
fn process(event, request) {{
    let validation = validate(event);
    if !validation["ok"] {{
        if !event.contains("xcontext") || event["xcontext"] == () {{
            event["xcontext"] = #{{}};
        }}
        let xcontext = event["xcontext"];
        xcontext["loadtest_validation_code"] = validation["code"];
        event["xcontext"] = xcontext;
    }}
    emit({sink_constant}, event);
}}
""".strip()


def ensure_processor():
    scripts = request("GET", "/api/admin/processor-scripts")
    script = first_by(scripts, "script_key", PROCESSOR_KEY)
    payload = {
        "name": "Loadtest blackhole processor",
        "entry_module": "main",
        "status": "active",
        "modules": [
            {
                "module_name": "main",
                "source": processor_source(),
            }
        ],
    }
    if script is None:
        return request(
            "POST",
            "/api/admin/processor-scripts",
            {
                "script_key": PROCESSOR_KEY,
                **payload,
            },
            expected=(201,),
        )
    return request(
        "PUT",
        f"/api/admin/processor-scripts/{script['id']}",
        payload,
        expected=(200,),
    )


def assign_processor(project_id, processor_id):
    request(
        "PUT",
        f"/api/admin/projects/{project_id}/processor",
        {
            "processor_script_id": processor_id,
            "enabled": True,
        },
        expected=(204,),
    )


def main():
    if not re.fullmatch(r"[a-zA-Z0-9_.:-]+", INGEST_TOKEN):
        raise RuntimeError("INGEST_TOKEN contains unsupported characters for this setup script")
    if SINK_MODE not in {"ok", "slow", "fail"}:
        raise RuntimeError("LOADTEST_SINK_MODE must be one of: ok, slow, fail")
    if DELAY_MS < 0:
        raise RuntimeError("LOADTEST_DELAY_MS must be >= 0")

    target = ensure_delivery_target()
    sink = ensure_event_sink(target["id"])
    project = ensure_project()
    processor = ensure_processor()
    assign_processor(project["id"], processor["id"])

    print(
        json.dumps(
            {
                "admin_url": ADMIN_URL,
                "project_id": project["id"],
                "ingest_token": INGEST_TOKEN,
                "delivery_target_id": target["id"],
                "event_sink_id": sink["id"],
                "event_sink": SINK_ID,
                "processor_script_id": processor["id"],
                "mode": SINK_MODE,
                "delay_ms": DELAY_MS,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"setup_blackhole.py: {error}", file=sys.stderr)
        sys.exit(1)
