# group_sync_workbench 示例

这是一个业务编排示例，演示以下流程：

1. 拉取群聊历史消息
2. 解析文字和图片消息
3. 对图片执行 OCR（`optical_char_recognition:image`）
4. 写入 Bitable 记录
5. 支持手动发送与定时发送（文字/图片）
6. 提供控制台 UI（`ui` 子命令）
7. 提供完整 Web UI 操作台（`web` 子命令）

## 运行前准备

需要启用 feature：`communication,docs,ai`

```bash
cargo run --example group_sync_workbench --features "communication,docs,ai" -- --help
```

## 必填环境变量

- `OPENLARK_APP_ID`
- `OPENLARK_APP_SECRET`

## 可选环境变量

- `OPENLARK_BASE_URL`（默认 `https://open.feishu.cn`）
- `OPENLARK_DEFAULT_CHAT_ID`（默认群 ID）
- `OPENLARK_BITABLE_APP_TOKEN`（`sync` 时必填）
- `OPENLARK_BITABLE_TABLE_ID`（`sync` 时必填）
- `OPENLARK_OCR_PARENT_NODE`（OCR 中转上传目录 token，`sync` + OCR 时必填）
- `OPENLARK_SYNC_STATE_FILE`（增量同步状态文件，默认 `group_sync_state.json`）

说明：OCR 下载图片时优先使用 `image_key`，若消息体仅包含 `file_key`，会自动回退到 `message/resource/get`。
说明：`sync` 默认增量去重（按 `message_id`），如需全量重刷可加 `--full-sync`。
说明：`sync` 写入 Bitable 时优先批量创建记录（分批），批量失败会自动降级为单条写入。
说明：写入请求会附带 `client_token`（幂等标识）以降低重试导致的重复记录风险。

## 常用命令

```bash
# 查看群信息
cargo run --example group_sync_workbench --features "communication,docs,ai" -- chat-info --chat-id oc_xxx

# 拉取群消息
cargo run --example group_sync_workbench --features "communication,docs,ai" -- pull --chat-id oc_xxx --page-size 20 --max-pages 2

# 同步到 Bitable（默认启用 OCR）
cargo run --example group_sync_workbench --features "communication,docs,ai" -- sync --chat-id oc_xxx --page-size 20 --max-pages 1

# 同步到 Bitable（跳过 OCR）
cargo run --example group_sync_workbench --features "communication,docs,ai" -- sync --chat-id oc_xxx --skip-ocr

# 同步到 Bitable（全量重刷，忽略本地去重状态）
cargo run --example group_sync_workbench --features "communication,docs,ai" -- sync --chat-id oc_xxx --full-sync

# 查看同步状态（全部群）
cargo run --example group_sync_workbench --features "communication,docs,ai" -- state

# 查看某个群的同步状态
cargo run --example group_sync_workbench --features "communication,docs,ai" -- state --chat-id oc_xxx

# 清理某个群的同步状态
cargo run --example group_sync_workbench --features "communication,docs,ai" -- clear-state --chat-id oc_xxx

# 清理全部同步状态
cargo run --example group_sync_workbench --features "communication,docs,ai" -- clear-state --all

# 发送文本
cargo run --example group_sync_workbench --features "communication,docs,ai" -- send-text --chat-id oc_xxx --text "hello"

# 定时发送图片
cargo run --example group_sync_workbench --features "communication,docs,ai" -- schedule-image --chat-id oc_xxx --image-key img_v2_xxx --interval-secs 300 --max-runs 10

# 控制台 UI
cargo run --example group_sync_workbench --features "communication,docs,ai" -- ui

# Web UI（HTTP 页面）
cargo run --example group_sync_workbench --features "communication,docs,ai" -- web --host 127.0.0.1 --port 8080
```

## Web UI 说明

- 打开 `http://127.0.0.1:8080/` 进入操作台。
- 页面内可执行：群信息查询、消息拉取、OCR 同步到 Bitable、发送文本/图片、创建/停止定时发送任务、查看/清理增量同步状态。
- Web API 路由（同域调用）：
  - `GET /api/health`
  - `POST /api/chat/info`
  - `POST /api/messages/pull`
  - `POST /api/messages/sync`
  - `POST /api/state`
  - `POST /api/state/clear`
  - `POST /api/send/text`
  - `POST /api/send/image`
  - `POST /api/schedule/text`
  - `POST /api/schedule/image`
  - `GET /api/schedules`
  - `POST /api/schedules/:id/stop`

## Bitable 字段建议

示例默认写入以下字段名（请在表中提前创建同名字段）：

- `message_id`
- `msg_type`
- `create_time`
- `sender_id`
- `text`
- `image_key`
- `file_key`
- `ocr_text`
