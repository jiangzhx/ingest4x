# ingest4x 文档

`ingest4x` 是一个独立的事件接入服务，将接收、持久化、校验、转换、投递、运维和监控整合到一处。

## 目录

- [项目说明（英文）](../../README.md)
- [WAL、checkpoint 与重放](wal.md)
- [事件处理](event-processing/)
- [管理端与 API](admin-api.md)
- [Sink 参数](sink-parameters.md)
- [本地 blackhole 压测报告](load-test-local-blackhole.md)
- [发布与版本](release-versioning.md)
- [目录结构](project-structure.md)

## 快速流程

```text
业务事件
  -> /ingest
  -> WAL
  -> 校验
  -> 转换与投递
  -> Event Sink
```

GitHub Pages 配置：

1. 打开仓库 `Settings -> Pages`。
2. 在 `Build and deployment` 下选择 `Deploy from a branch`。
3. 选择 `main` 分支。
4. 选择文件夹 `/docs`。
5. 访问 `https://jiangzhx.github.io/ingest4x/`。
