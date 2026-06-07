# Codex Bridge

面向 AstrBot 的本地桥接插件。

目标：

- 从 QQ/AstrBot 把文本转发到当前活跃的 Codex 对话
- 接收 Codex 注入桥发出的任务完成、审核请求等状态
- 为后续 QQ 推送流程预留统一入口

当前版本使用 WebSocket 协议作为桥接方式：

- 推荐由注入器中继脚本连接 AstrBot 的 WS 地址，绕开 Codex 页面 CSP
- Codex 页面注入脚本负责 hook 页面、暴露 API，并通过 `postMessage/CustomEvent` 与注入器通信
- AstrBot 收到 QQ 消息后，可直接把命令推送到 Codex 页面
- Codex 页面把任务完成、审核请求、状态变化主动回推给 AstrBot

## 推送配置

建议使用 AstrBot 的 `unified_msg_origin` 作为推送目标。

当前支持：

- 多个推送目标 UMO
- 配置要推送的事件类型
- 配置允许下发 `/codex` 指令的来源 UMO
- 推送后在窗口期内直接回复，不需要再输入 `/codex 回复`
- 需要审核时，直接发送单个字母 `y` 即可同意审核；发送其他内容则作为普通回复转发给 Codex

## 计划中的命令

- `/codex 状态`
- `/codex 发送 <内容>`
- `/codex 草稿 <内容>`
- `/codex 会话`
- `/codex 同意`
- `/codex 回复 <内容>`
- `/codex 停止回复`

## 目录结构

- `main.py`: AstrBot 插件入口
- `metadata.yaml`: AstrBot 插件元信息

## 预期连接方式

推荐让注入器在非页面 CSP 上下文加载：

`codex-qq-bridge-injector-relay.js`

它会连接：

`ws://192.168.10.11:32124/ws/codex`

如果直接在 Codex 页面环境中连接该 WS，可能会被页面 `connect-src` CSP 拦截，此时 AstrBot 会显示 `Codex is not connected`。
