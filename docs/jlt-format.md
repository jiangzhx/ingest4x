# JLT 格式

每个 `.jlt` 文件可以包含多个 case。一个 case 由 4 部分组成：

1. 以 `# ` 开头的描述行
2. 一段 JSON
3. 分隔线 `----`
4. 期望结果

## 最小示例

```text
# 正常 install
{"appid":"APPID","xwhat":"install","xcontext":{"installid":"iid","os":"ios","idfa":"idfa-1"}}
----
pass
```

## 失败用例示例

```text
# 缺少 xwhat
{"appid":"APPID","xcontext":{"installid":"iid","os":"ios","idfa":"idfa-1"}}
----
fail
missing required field `xwhat`
```

## 规则

- 结果只支持 `pass` 或 `fail`
- 当结果是 `fail` 时，可以在下一行继续写“报错子串”
- JLT 运行器会检查真实错误信息里是否包含这段文本
- 一个 `.jlt` 文件里可以连续写多个 case
- 仓库内置测试会递归扫描 `tests/jlt/core` 下所有 `.jlt` 文件
- 测试会创建内存数据库、执行内置 seed，并从数据库编译 ruleset

## 常用命令

运行仓库内置 seed + JLT 测试：

```bash
cargo test --test ingest ingest_jlt_cases_match_rules
```

## 输出示例

```text
scope `core` has failures: [...]
```
