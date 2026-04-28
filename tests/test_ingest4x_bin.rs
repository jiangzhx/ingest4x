#![cfg(feature = "ingest")]

use std::process::Command;

#[test]
fn ingest4x_prints_version_in_ingest_only_build() {
    let output = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .arg("run")
        .arg("--quiet")
        .arg("--no-default-features")
        .arg("--features")
        .arg("ingest")
        .arg("--bin")
        .arg("ingest4x")
        .arg("--")
        .arg("--version")
        .output()
        .expect("run ingest4x");

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        env!("CARGO_PKG_VERSION")
    );
}

#[test]
fn ingest4x_server_prints_version_in_ingest_only_build() {
    let output = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .arg("run")
        .arg("--quiet")
        .arg("--no-default-features")
        .arg("--features")
        .arg("ingest")
        .arg("--bin")
        .arg("ingest4x")
        .arg("--")
        .arg("server")
        .arg("--version")
        .output()
        .expect("run ingest4x server");

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        env!("CARGO_PKG_VERSION")
    );
}
