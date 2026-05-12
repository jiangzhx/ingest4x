# 发布与版本

版本信息定义于 `Cargo.toml`。

## 版本号提升

```bash
./scripts/bump_version.sh patch
./scripts/bump_version.sh minor
./scripts/bump_version.sh major
./scripts/bump_version.sh 0.1.0
```

该脚本要求工作区干净，会更新 `Cargo.toml` 中的 `ingest4x` 版本，并在存在 `Cargo.lock` 时一并更新。

提交前建议检查：

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

检查通过后，脚本会创建一次版本提交并推送当前分支。

可见 `./scripts/bump_version.sh --help` 获取可选参数。

## 发起发布

```bash
./scripts/release.sh
```

发布脚本步骤：

1. 确认工作区干净。
2. 从 `Cargo.toml` 读取版本，生成 `vX.Y.Z` 标签。
3. 校验本地与远端不存在同名 tag。
4. 推送分支和 tag。
5. 通过 GitHub CLI 创建 GitHub Release。

二进制构建由 GitHub Actions 产生并附加到 release，本脚本负责打标签与 release 创建。

可见 `./scripts/release.sh --help`。
