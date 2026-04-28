# Receiver Test CLI 融合设计

## 背景

当前仓库把客户可见的命令行入口拆成两个二进制：

- `receiver`：启动服务
- `receiver-jlt`：执行 rules + JLT 校验

这会带来两套独立的使用心智、帮助信息和文档入口。客户需要记住两个程序名，而不是围绕一个 `receiver` 工具完成“启动服务”和“测试规则”两类操作。

## 目标

- 保持 `receiver -c config/development.toml` 继续默认启动服务
- 把 `receiver-jlt` 融合进 `receiver`，统一成 `receiver test`
- 为服务启动增加显式别名 `receiver server`
- 保持现有 JLT 运行语义、参数和输出格式尽量不变

## 非目标

- 不修改 `.jlt` 文件格式
- 不修改 `pass` / `fail` 语义
- 不修改 rules 目录结构
- 不把 test 逻辑耦合进 server 配置加载流程
- 不在本次改动里扩展新的 rules 管理命令

## 现状

当前 `receiver-jlt` 提供以下参数：

- `--rules-dir`
- `--jlts-dir`
- `--file`
- `--case`
- `--fail-fast`

当前运行结果包括：

- stdout 输出 `scope \`custom\`: X passed, Y failed`
- stderr 逐条输出失败 case，包含 `.jlt` 文件名、起始行号、case 描述和错误信息

这些行为已经被 `tests/test_receiver_jlt_bin.rs` 覆盖，仓库文档也默认以独立 bin 方式介绍 JLT。

## 方案概述

统一为单一二进制 `receiver`，并引入子命令：

```text
receiver [--config ...]
receiver server [--config ...]
receiver test --rules-dir ... --jlts-dir ... [--file ...] [--case ...] [--fail-fast]
```

语义如下：

- 不带子命令时，默认执行 `server`
- `server` 是显式别名，行为与顶层默认启动完全一致
- `test` 承载原 `receiver-jlt` 的 CLI 功能

## CLI 设计

### 顶层行为

- `receiver -c config/development.toml`
  - 默认启动服务
- `receiver server -c config/development.toml`
  - 显式启动服务
- `receiver --version`
  - 保持当前输出版本号行为

### Test 子命令

`receiver test` 直接承接原 `receiver-jlt` 的参数设计：

- `--rules-dir`
- `--jlts-dir`
- `--file`
- `--case`
- `--fail-fast`

约束保持不变：

- `--rules-dir` 与 `--jlts-dir` 仍然是运行整目录时的核心输入
- 传 `--file` 时，仍要求 `--rules-dir` 和 `--jlts-dir` 是有效目录
- 当过滤后没有 case 命中时，仍返回错误
- 存在失败 case 时，进程退出码保持非零

### 客户可见命令

```bash
# 默认启动服务
receiver -c config/development.toml

# 显式启动服务
receiver server -c config/development.toml

# 运行仓库内置 JLT
receiver test --rules-dir config/rules/up --jlts-dir tests/core

# 只跑指定文件
receiver test --rules-dir config/rules/up --jlts-dir tests/core --file tests/core/register_test.jlt

# 按描述过滤 case
receiver test --rules-dir config/rules/up --jlts-dir tests/core --case "正常的register场景"
```

## 代码结构调整

### 保留

- `src/jlt/mod.rs`
  - 继续负责加载 `.jlt`
  - 继续负责执行 `Rules::validate(...)`
  - 继续负责统计 passed / failed 与失败详情

### 修改

- `src/main.rs`
  - 重构为顶层命令解析入口
  - 引入 `server` / `test` 子命令
  - 实现“无子命令默认执行 server”

### 移除或过渡

- `src/bin/receiver-jlt.rs`
  - 迁移期可以短暂保留，用于兼容提示
  - 最终目标是删除，避免客户继续记忆双入口

## 迁移策略

### 推荐落地顺序

1. 在 `receiver` 中实现 `test` 子命令
2. 调整 README 和相关文档，所有示例统一切换为 `receiver test`
3. 将 bin 测试从 `receiver-jlt` 改为 `receiver test`
4. 如需短期兼容，可暂时保留 `receiver-jlt` 并提示迁移
5. 完成兼容窗口后删除 `receiver-jlt`

### 客户沟通口径

客户侧只需要理解一条迁移说明：

“以前运行规则校验用 `receiver-jlt`，现在统一改为 `receiver test`。”

## 测试设计

### 保留的验证

- `receiver --version` 仍然正确输出版本号

### 新增或调整的验证

- `receiver server` 可被正确解析
- `receiver test` 在显式 rules/jlts 目录下可成功运行
- `receiver test` 缺少 `--jlts-dir` 时返回参数错误
- `receiver test` 失败时输出包含 `.jlt` 文件名和行号
- `receiver test --case ...` 过滤行为保持不变

### 不需要新增的验证

- 不需要为 `.jlt` 语义新增额外覆盖，因为相关逻辑已由现有 rules/JLT 测试覆盖

## 风险与控制

### 风险 1：默认启动与子命令解析冲突

如果顶层 CLI 解析设计不当，可能导致 `receiver -c ...` 被误判为缺少子命令。控制方式是把顶层结构设计为“可选子命令 + 顶层 server 参数”，并在没有子命令时走 server 分支。

### 风险 2：帮助信息变复杂

如果把 server 和 test 参数放在同一层，客户会看到混杂的参数集合。控制方式是使用独立子命令，把 test 参数完全收敛到 `receiver test --help`。

### 风险 3：文档与实现脱节

当前 README、JLT 文档和 bin 测试都以 `receiver-jlt` 为准。控制方式是把文档切换与测试迁移作为同一批改动交付，不允许只改实现不改说明。

## 决策结论

本次融合采用以下决策：

- 保留 `receiver -c ...` 默认启动 server
- 新增 `receiver server`
- 用 `receiver test` 取代 `receiver-jlt`
- 尽量不动 JLT 核心执行逻辑，只重组 CLI 壳层、测试与文档

## 后续实施范围

实施时应限定在以下文件范围附近：

- `src/main.rs`
- `src/bin/receiver-jlt.rs`
- `src/jlt/mod.rs`（仅在必要时提取 CLI 复用函数）
- `tests/test_receiver_bin.rs`
- `tests/test_receiver_jlt_bin.rs`
- `README.md`
- `docs/jlt-format.md`
- `docs/recommended-workflow.md`
