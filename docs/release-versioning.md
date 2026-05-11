# 发布和版本

版本号在 `Cargo.toml`。

## 升级版本

```bash
./scripts/bump_version.sh patch
./scripts/bump_version.sh minor
./scripts/bump_version.sh major
./scripts/bump_version.sh 0.1.0
```

脚本会要求工作区干净，然后更新 `Cargo.toml` 中的 `ingest4x` 版本；如果存在 `Cargo.lock`，也会同步更新。

升级前会自动执行：

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

检查通过后，脚本会创建版本提交并 push 当前分支。

可选参数以脚本自身帮助为准：

```bash
./scripts/bump_version.sh --help
```

## 创建 Release

```bash
./scripts/release.sh
```

发布脚本会：

1. 检查工作区干净
2. 读取 `Cargo.toml` 当前版本并生成 `vX.Y.Z` tag
3. 检查本地和远端是否已有同名 tag
4. push 当前分支和 tag
5. 使用 GitHub CLI 创建 GitHub Release

真正的二进制产物由 GitHub Actions 构建并上传到 release。本地脚本只负责 tag 和 release 创建。

可选参数以脚本自身帮助为准：

```bash
./scripts/release.sh --help
```
