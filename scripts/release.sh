#!/bin/sh

set -eu

REMOTE="origin"

usage() {
    cat <<'EOF'
用法:
  ./scripts/release.sh
  ./scripts/release.sh --remote github

说明:
  1. 自动读取 Cargo.toml 中 ingest4x 当前版本，创建 vX.Y.Z tag
  2. 要求工作区干净，且当前不处于 detached HEAD
  3. 若本地或远端已存在同名 tag，直接报错退出
  4. 自动 push 当前分支与 tag
  5. 自动通过 gh 创建 GitHub Release，产物由 GitHub Actions 构建并上传
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
        fail "工作区有未提交修改，请先处理后再发布"
    fi

    if ! git diff --cached --quiet --ignore-submodules --; then
        fail "暂存区存在未提交内容，请先处理后再发布"
    fi

    if [ -n "$(git ls-files --others --exclude-standard)" ]; then
        fail "存在未跟踪文件，请先处理后再发布"
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

current_branch() {
    branch="$(git branch --show-current)"
    [ -n "$branch" ] || fail "当前处于 detached HEAD，无法自动发布"
    printf '%s\n' "$branch"
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

tag_exists_locally() {
    tag="$1"
    git rev-parse -q --verify "refs/tags/$tag" >/dev/null 2>&1
}

tag_exists_remotely() {
    tag="$1"
    remote="$2"
    git ls-remote --exit-code --tags "$remote" "refs/tags/$tag" >/dev/null 2>&1
}

require_gh_auth() {
    command -v gh >/dev/null 2>&1 || fail "未找到 gh，请先安装 GitHub CLI"
    gh auth status >/dev/null 2>&1 || fail "gh 未登录，请先执行 gh auth login"
}

case "${1:-}" in
    -h|--help)
        usage
        exit 0
        ;;
    "")
        ;;
    --remote)
        [ "$#" -ge 2 ] || fail "--remote 需要额外提供 remote 名称"
        REMOTE="$2"
        shift 2
        ;;
    *)
        fail "未知参数: $1"
        ;;
esac

[ "$#" -eq 0 ] || fail "存在未识别的额外参数"

require_clean_worktree
BRANCH="$(current_branch)"
VERSION="$(read_current_version)"
[ -n "$VERSION" ] || fail "未能从 Cargo.toml 读取当前版本号"
validate_semver "$VERSION" || fail "当前版本号不是标准 semver: $VERSION"

TAG="v$VERSION"
SHA="$(git rev-parse HEAD)"

tag_exists_locally "$TAG" && fail "本地已存在 tag: $TAG"

log "Fetching remote tags from $REMOTE..."
git fetch --tags "$REMOTE"

tag_exists_remotely "$TAG" "$REMOTE" && fail "远端已存在 tag: $TAG"

require_gh_auth

log "Creating tag $TAG for $SHA"
git tag -a "$TAG" -m "Release $TAG"

log "Pushing branch $BRANCH to $REMOTE..."
push_branch "$BRANCH" "$REMOTE"

log "Pushing tag $TAG to $REMOTE..."
git push "$REMOTE" "$TAG"

log "Creating GitHub Release $TAG..."
gh release create "$TAG" --title "$TAG" --generate-notes --target "$SHA"

log "Done: GitHub Release $TAG created. Build artifacts will be produced by GitHub Actions."
