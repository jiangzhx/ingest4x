#!/bin/sh

set -eu

REMOTE="origin"
COMMIT_MESSAGE=""

usage() {
    cat <<'EOF'
用法:
  ./scripts/bump_version.sh patch
  ./scripts/bump_version.sh minor
  ./scripts/bump_version.sh major
  ./scripts/bump_version.sh 3.0.1
  ./scripts/bump_version.sh patch -m "chore: release 3.0.1"
  ./scripts/bump_version.sh patch --remote github

说明:
  1. 仅允许在干净工作区运行
  2. 自动更新 Cargo.toml 中 ingest4x 的版本；若存在 Cargo.lock 则一并更新
  3. 自动执行 cargo fmt / cargo clippy / cargo test
  4. 自动 commit 并 push 当前分支
EOF
}

log() {
    printf '%s\n' "$*"
}

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

require_clean_worktree() {
    if ! git diff --quiet --ignore-submodules --; then
        fail "工作区有未提交修改，请先处理后再升级版本"
    fi

    if ! git diff --cached --quiet --ignore-submodules --; then
        fail "暂存区存在未提交内容，请先处理后再升级版本"
    fi

    if [ -n "$(git ls-files --others --exclude-standard)" ]; then
        fail "存在未跟踪文件，请先处理后再升级版本"
    fi
}

read_current_version() {
    awk '
        $0 == "[package]" { in_package = 1; next }
        /^\[/ && $0 != "[package]" { in_package = 0 }
        in_package && $1 == "version" {
            gsub(/"/, "", $3)
            print $3
            exit
        }
    ' Cargo.toml
}

validate_semver() {
    version="$1"
    case "$version" in
        *[!0-9.]* | *.*.*.* | .* | *. | *..* | "")
            return 1
            ;;
    esac

    old_ifs=$IFS
    IFS=.
    set -- $version
    IFS=$old_ifs

    [ "$#" -eq 3 ] || return 1

    for part in "$@"; do
        case "$part" in
            "" | *[!0-9]*)
                return 1
                ;;
        esac
    done

    return 0
}

compute_next_version() {
    current="$1"
    mode="$2"

    validate_semver "$current" || fail "当前版本号不是标准 semver: $current"

    old_ifs=$IFS
    IFS=.
    set -- $current
    IFS=$old_ifs

    major="$1"
    minor="$2"
    patch="$3"

    case "$mode" in
        patch)
            patch=$((patch + 1))
            ;;
        minor)
            minor=$((minor + 1))
            patch=0
            ;;
        major)
            major=$((major + 1))
            minor=0
            patch=0
            ;;
        *)
            fail "不支持的升级类型: $mode"
            ;;
    esac

    printf '%s.%s.%s\n' "$major" "$minor" "$patch"
}

replace_versions() {
    old="$1"
    new="$2"

    OLD_VERSION="$old" NEW_VERSION="$new" perl -0pi -e '
        my $old = $ENV{OLD_VERSION};
        my $new = $ENV{NEW_VERSION};
        my $count = s/(\[package\]\nname = "ingest4x"\nversion = ")\Q$old\E(")/$1$new$2/;
        die "failed to update Cargo.toml\n" unless $count == 1;
    ' Cargo.toml

    if [ -f Cargo.lock ]; then
        OLD_VERSION="$old" NEW_VERSION="$new" perl -0pi -e '
            my $old = $ENV{OLD_VERSION};
            my $new = $ENV{NEW_VERSION};
            my $count = s/(\[\[package\]\]\nname = "ingest4x"\nversion = ")\Q$old\E(")/$1$new$2/;
            die "failed to update Cargo.lock\n" unless $count == 1;
        ' Cargo.lock
    fi
}

current_branch() {
    branch="$(git branch --show-current)"
    [ -n "$branch" ] || fail "当前处于 detached HEAD，无法自动 push"
    printf '%s\n' "$branch"
}

run_checks() {
    log "Running cargo fmt..."
    cargo fmt --all -- --check

    log "Running cargo clippy..."
    cargo clippy --all-targets --all-features -- -D warnings

    log "Running cargo test..."
    cargo test
}

push_branch() {
    branch="$1"
    remote="$2"

    if git rev-parse --abbrev-ref --symbolic-full-name "@{u}" >/dev/null 2>&1; then
        git push "$remote" "$branch"
    else
        git push -u "$remote" "$branch"
    fi
}

case "${1:-}" in
    -h|--help|"")
        usage
        [ "${1:-}" = "" ] && exit 1
        exit 0
        ;;
esac

TARGET="$1"
shift

while [ "$#" -gt 0 ]; do
    case "$1" in
        -m|--message)
            [ "$#" -ge 2 ] || fail "$1 需要额外提供 commit message"
            COMMIT_MESSAGE="$2"
            shift 2
            ;;
        --remote)
            [ "$#" -ge 2 ] || fail "--remote 需要额外提供 remote 名称"
            REMOTE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "未知参数: $1"
            ;;
    esac
done

CURRENT_VERSION="$(read_current_version)"
[ -n "$CURRENT_VERSION" ] || fail "未能从 Cargo.toml 读取当前版本号"

case "$TARGET" in
    patch|minor|major)
        NEXT_VERSION="$(compute_next_version "$CURRENT_VERSION" "$TARGET")"
        ;;
    *)
        validate_semver "$TARGET" || fail "目标版本号格式不正确: $TARGET"
        NEXT_VERSION="$TARGET"
        ;;
esac

[ "$CURRENT_VERSION" != "$NEXT_VERSION" ] || fail "目标版本号与当前版本相同: $CURRENT_VERSION"

if [ -z "$COMMIT_MESSAGE" ]; then
    COMMIT_MESSAGE="chore: bump version to $NEXT_VERSION"
fi

require_clean_worktree
BRANCH="$(current_branch)"

log "Bumping version: $CURRENT_VERSION -> $NEXT_VERSION"
replace_versions "$CURRENT_VERSION" "$NEXT_VERSION"

run_checks

git add Cargo.toml
if [ -f Cargo.lock ]; then
    git add -f Cargo.lock
fi
git commit -m "$COMMIT_MESSAGE"
push_branch "$BRANCH" "$REMOTE"

log "Done: version updated to $NEXT_VERSION and pushed to $REMOTE/$BRANCH"
