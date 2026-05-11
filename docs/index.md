# ingest4x 文档

`ingest4x` 是一个面向业务事件接入的独立工具，用来把接收、缓冲、校验、加工、投递、管理和监控这些常见但分散的环节收在一起。

## 入口

- [项目 README](https://github.com/jiangzhx/ingest4x#readme)
- [WAL、checkpoint 和 replay](wal.md)
- [事件处理](event-processing/)
- [管理后台和 API](admin-api.md)
- [本地 blackhole 压测报告](load-test-local-blackhole.md)
- [发布和版本](release-versioning.md)
- [项目结构](project-structure.md)

## 快速理解

```text
业务事件
  -> /ingest
  -> WAL
  -> 事件校验
  -> 事件加工和投递
  -> event sinks
```

GitHub Pages 启用方式：

1. 进入仓库 `Settings -> Pages`。
2. `Build and deployment` 选择 `Deploy from a branch`。
3. Branch 选择 `main`。
4. Folder 选择 `/docs`。
5. 保存后访问 `https://jiangzhx.github.io/ingest4x/`。
