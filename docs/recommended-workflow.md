# 推荐工作流

## 新增或修改规则

1. 在 `src/db/seed.rs` 中维护内置 seed ruleset
2. 为变化补充或更新 `tests/jlt/core/*.jlt` 样例
3. 运行 seed、JLT 和相关测试确认行为

## 常用验证命令

```bash
# 仓库内置 JLT 与数据库 seed ruleset 一致性
cargo test --test test_ingest_rules_compat
```
