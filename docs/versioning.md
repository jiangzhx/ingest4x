# 版本升级

```bash
# patch / minor / major
./scripts/bump_version.sh patch
./scripts/bump_version.sh minor
./scripts/bump_version.sh major

# 或直接指定目标版本
./scripts/bump_version.sh 3.1.1
```

脚本会自动：

1. 检查工作区是否干净
2. 更新 `Cargo.toml` 中 `ingest4x` 版本号
3. 若存在 `Cargo.lock`，则一并更新
4. 执行 `cargo fmt --all -- --check`
5. 执行 `cargo clippy --all-targets --all-features -- -D warnings`
6. 执行 `cargo test`
7. 创建版本提交并推送当前分支

# 发布 release

```bash
./scripts/release.sh
```

脚本会自动：

1. 检查工作区是否干净
2. 读取 `Cargo.toml` 当前版本并生成 `vX.Y.Z` tag
3. 若本地或远端已存在同名 tag，则直接报错退出
4. 推送当前分支和该 tag
5. 创建 GitHub Release

二进制产物由 GitHub Actions 构建并上传，不会在本地产生 release 可执行文件。
