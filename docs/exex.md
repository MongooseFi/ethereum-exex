# 节点 ExEx 推送

链状态由 reth 进程内的 ExEx 直接推送，不按块轮询 RPC。`mongoosefi-reth` 是 stock reth 加一个 ExEx，通过本地 Unix socket（节点侧环境变量 `MONGOOSEFI_EXEX_SOCKET`，默认工作目录下 `mongoosefi-exex.sock`）发布每个链更新。

| 组成 | 位置 | 职责 |
| --- | --- | --- |
| 线协议 | `crates/exex_stream/src/wire.rs` | 长度前缀 JSON 帧，`VERSION = 4`；不兼容改动即提升版本，版本不符直接拒绝 |
| 服务端 | `crates/exex_stream/src/server.rs` | 节点内 fan-out；`publish` 从不等待 |
| 客户端 | `crates/exex_stream/src/client.rs` | 连接，按 head 哈希或精确 cursor 续传 |
| 消费端 | [Services state-feed](https://github.com/MongooseFi/services/blob/main/crates/state_feed/src/feed/exex.rs) | 把推送的 commit/revert/reorg 交给 `LogFeed`；无逐块 RPC 回退，也不另外维护 head 缓存 |
| 观测工具 | `cargo run -p exex-stream --example tail -- <socket> [--pending]` | 打印每条通知摘要 |

生产节点在 `node.json` 的 `state.exex_socket` 指定该 socket；canonical 与 pending 走同一有序连接。

## 消息与语义

- 每条通知是 `Commit`、`Revert`（最新在前）或 `Reorg`。committed 段包含每块的环境字段与全部日志（块内 log_index），以及整段聚合后的状态差分：变化账户的新余额、nonce、code hash、变化的代码、`storage_wiped` 和变化存储槽的新值。节点按段聚合，所以中间块没有自己的状态。
- 每个区块带交易哈希列表（块内顺序）。
- 服务端保留最近 256 条通知，并以 128 MiB 为上限（`server::RING`、`server::REPLAY_BYTES`）。客户端的 `Hello` 带上最后应用的 head 哈希或其精确 cursor：仍在环中就重放其后的通知，否则先收到 `Resync`。每个客户端有 64 帧的有界队列（`server::CLIENT_QUEUE`），满了就断开它；它重连后按 cursor 续传，不丢不重。
- ExEx 从节点当前 head 开始，不主动请求历史；节点补同步时仍可能发来多区块聚合段。每个 commit 入队后立即报告 `FinishedHeight`，不拖住节点的 WAL 或裁剪。
- 单帧上限为 64 MiB，编码达到上限即停止。补同步聚合段超限时，服务端清空重放环、更换 stream 身份，并向在线消费者发送 `Resync`（队列满则断开）；旧 cursor 不再有效。ExEx 记录告警并报告该段的 `FinishedHeight`，继续处理后续通知，不因此退出 Reth。消费者必须重新 bootstrap，不能把这个缺口当成已完整应用。
- Pending 交易：客户端在 `Hello` 里设 `pending: true` 才会收到。节点把进入 pending 池的交易批量推送，每笔含哈希、sender、nonce、`to` 和 EIP-2718 原始编码。pending 帧不进重放环、断线不补；客户端队列满时跳过这一批但不断开（区块通知仍会因队列满而断开并续传）。
- 消费端把 head 放进无界队列、pending 放进有界队列，取时 head 优先：head 从不丢，pending 尽力而为。Revert 本身不产生 head，紧随的 commit 会给出新 head。

## 部署

仓库迁移只改变源码和 CI 归属，不执行以下部署步骤；既有节点的二进制、数据目录和服务配置保持原样。

1. 在本仓库根目录构建：`cargo build --locked --release -p mongoosefi-reth`；当前节点 Build Features 含 `jit` 时加 `--features jit`。
2. 停 stock `reth`，用同一命令行启动 `mongoosefi-reth`，datadir 直接兼容。`MONGOOSEFI_EXEX_SOCKET` 指向只给服务账户可访问的路径。
3. 用 `tail` 确认每块都有通知、`txs` 与区块一致，需要时能看到 pending 批次。
4. 消费者连接前，重放环必须已覆盖 bootstrap anchor；缺口或身份不符会显式 resync 并退出，不从收据近似重建状态。

回滚：停 `mongoosefi-reth`，用原命令启动 stock `reth`。

systemd 部署要检查依赖服务：若 Lighthouse 配置了 `Requires=reth.service`，停止 Reth 会连带停止 Lighthouse；启动 Reth 后必须显式恢复 Lighthouse，回滚也一样。新旧 Reth 不能同时写同一 datadir。启动验收应检查原参数保留、两项服务均 active、`eth_syncing=false`，并核对新收到的 ExEx 区块哈希与 RPC 一致；正在补同步只代表启动成功，不代表实时推送验收通过。
