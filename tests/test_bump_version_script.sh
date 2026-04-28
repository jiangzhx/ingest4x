#!/bin/sh

set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
REMOTE_DIR="$(mktemp -d)"

cleanup() {
    rm -rf "$TMP_DIR" "$REMOTE_DIR"
}

trap cleanup EXIT INT TERM

TEST_REPO="$TMP_DIR/repo"
BIN_DIR="$TMP_DIR/bin"

mkdir -p "$TEST_REPO" "$BIN_DIR"

cp "$ROOT_DIR/scripts/bump_version.sh" "$TEST_REPO/bump_version.sh"
chmod +x "$TEST_REPO/bump_version.sh"

cat > "$BIN_DIR/cargo" <<'EOF'
#!/bin/sh
exit 0
EOF
chmod +x "$BIN_DIR/cargo"

cat > "$TEST_REPO/Cargo.toml" <<'EOF'
[package]
name = "ingest4x"
version = "3.0.0"
edition = "2021"
EOF

git init --bare "$REMOTE_DIR/origin.git" >/dev/null 2>&1
git -C "$TEST_REPO" init -b main >/dev/null 2>&1
git -C "$TEST_REPO" config user.name "Codex Test"
git -C "$TEST_REPO" config user.email "codex@example.com"
git -C "$TEST_REPO" remote add origin "$REMOTE_DIR/origin.git"
git -C "$TEST_REPO" add Cargo.toml bump_version.sh
git -C "$TEST_REPO" commit -m "init" >/dev/null 2>&1

(
    cd "$TEST_REPO"
    PATH="$BIN_DIR:$PATH" sh ./bump_version.sh 3.1.0
) >/tmp/test_bump_version_script.log 2>&1

grep -q 'version = "3.1.0"' "$TEST_REPO/Cargo.toml"
[ "$(git -C "$TEST_REPO" log -1 --pretty=%s)" = "chore: bump version to 3.1.0" ]
[ "$(git --git-dir "$REMOTE_DIR/origin.git" rev-parse refs/heads/main)" = "$(git -C "$TEST_REPO" rev-parse HEAD)" ]

printf 'test_bump_version_script: ok\n'
