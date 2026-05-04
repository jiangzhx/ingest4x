# ingest4x 下载与发布

这里发布的是 `ingest4x` 的多平台可执行文件。下载后可以直接运行，不需要本地再执行 Cargo 构建。

## 发布方式

发布入口是：

```bash
./scripts/release.sh
```

脚本会在本地只做这些动作：

1. 检查工作区干净
2. 读取 `Cargo.toml` 中当前 `ingest4x` 版本
3. 创建对应的 `vX.Y.Z` tag
4. push 当前分支和 tag
5. 调用 GitHub Release API 创建 release

真正的二进制产物不会在本地构建，而是由 GitHub Actions 的 [release-ingest4x workflow](/Users/sylar/workspace/startup/tracking/ingest4x/.github/workflows/release-ingest4x.yml) 在 GitHub runner 上构建并上传到该 release。

如果本地或远端已经存在同名 tag，脚本会直接报错退出，不会覆盖已有 release。

当前发布目标包含：

- Linux `x86_64-unknown-linux-gnu`
- Windows `x86_64-pc-windows-msvc`
- macOS `aarch64-apple-darwin`

## 发布产物命名

Release 和 workflow artifact 会直接上传二进制文件，命名格式如下：

```text
ingest4x-<target>
ingest4x-<target>.exe
```

例如：

- `ingest4x-x86_64-unknown-linux-gnu`
- `ingest4x-x86_64-pc-windows-msvc.exe`
- `ingest4x-aarch64-apple-darwin`

配置模板不再打包进 release 资产，直接使用仓库根目录里的默认配置：

- `ingest4x.toml`

## 运行方式

下载后可直接执行：

```bash
chmod +x ./ingest4x-aarch64-apple-darwin
./ingest4x-aarch64-apple-darwin
```

Windows 下：

```powershell
.\ingest4x-x86_64-pc-windows-msvc.exe
```

默认配置里仍然假设你本地已有：

- Redis
- Kafka

如果部署环境不是本机，需要修改 `ingest4x.toml` 里的 Redis、Kafka 和监听地址。
