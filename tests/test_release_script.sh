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
GH_LOG="$TMP_DIR/gh.log"

mkdir -p "$TEST_REPO" "$BIN_DIR"

cp "$ROOT_DIR/scripts/release.sh" "$TEST_REPO/release.sh"
chmod +x "$TEST_REPO/release.sh"

cat > "$BIN_DIR/gh" <<'EOF'
#!/bin/sh
set -eu

case "${1:-} ${2:-}" in
    "auth status")
        exit 0
        ;;
    "release create")
        printf '%s\n' "$@" > "$GH_RELEASE_LOG"
        exit 0
        ;;
    *)
        printf 'unexpected gh invocation: %s\n' "$*" >&2
        exit 1
        ;;
esac
EOF
chmod +x "$BIN_DIR/gh"

cat > "$TEST_REPO/Cargo.toml" <<'EOF'
[package]
name = "ingest4x"
version = "3.1.0"
edition = "2021"
EOF

git init --bare "$REMOTE_DIR/origin.git" >/dev/null 2>&1
git -C "$TEST_REPO" init -b main >/dev/null 2>&1
git -C "$TEST_REPO" config user.name "Codex Test"
git -C "$TEST_REPO" config user.email "codex@example.com"
git -C "$TEST_REPO" remote add origin "$REMOTE_DIR/origin.git"
git -C "$TEST_REPO" add Cargo.toml release.sh
git -C "$TEST_REPO" commit -m "init" >/dev/null 2>&1

(
    cd "$TEST_REPO"
    PATH="$BIN_DIR:$PATH" GH_RELEASE_LOG="$GH_LOG" sh ./release.sh
) >/tmp/test_release_script.log 2>&1

HEAD_SHA="$(git -C "$TEST_REPO" rev-parse HEAD)"
[ "$(git -C "$TEST_REPO" tag --list 'v3.1.0')" = "v3.1.0" ]
[ "$(git --git-dir "$REMOTE_DIR/origin.git" rev-parse 'refs/tags/v3.1.0^{}')" = "$HEAD_SHA" ]
[ "$(git --git-dir "$REMOTE_DIR/origin.git" rev-parse refs/heads/main)" = "$HEAD_SHA" ]

grep -Fx -- 'release' "$GH_LOG" >/dev/null
grep -Fx -- 'create' "$GH_LOG" >/dev/null
grep -Fx -- 'v3.1.0' "$GH_LOG" >/dev/null
grep -Fx -- '--generate-notes' "$GH_LOG" >/dev/null
grep -Fx -- '--title' "$GH_LOG" >/dev/null
grep -Fx -- '--target' "$GH_LOG" >/dev/null
grep -Fx -- "$HEAD_SHA" "$GH_LOG" >/dev/null

git -C "$TEST_REPO" tag -d v3.1.0 >/dev/null 2>&1
if (
    cd "$TEST_REPO"
    PATH="$BIN_DIR:$PATH" GH_RELEASE_LOG="$GH_LOG" sh ./release.sh
) >/tmp/test_release_script_existing_tag.log 2>&1; then
    printf 'expected release.sh to fail when remote tag already exists\n' >&2
    exit 1
fi

grep -F '远端已存在 tag: v3.1.0' /tmp/test_release_script_existing_tag.log >/dev/null

printf 'test_release_script: ok\n'
