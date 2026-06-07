# AstrBot Codex Bridge Plugin

AstrBot plugin for connecting QQ messages with the Codex QQ Bridge main runtime.

This repository contains only the AstrBot plugin. The main bridge program is maintained separately:

`https://github.com/under-the-ocean/codex-qq-bridge`

## Important Dependency

The full bridge depends on Codex++ or an equivalent Codex desktop launch method that exposes the DevTools endpoint, usually `http://127.0.0.1:9229`. The AstrBot plugin does not inject into Codex by itself; it receives events and sends commands through the main bridge relay.

## Features

- Push Codex task-complete notifications to QQ.
- Push Codex approval/review requests to QQ.
- Quick reply window for continuing the active Codex conversation.
- Quick `y`/`a` approval for current review requests.
- Quick `s` switch for review/completion notifications from non-current conversations.

## Plugin Files

- `astrbot_plugin_codex_bridge/main.py`: plugin entrypoint.
- `astrbot_plugin_codex_bridge/metadata.yaml`: AstrBot plugin metadata.
- `astrbot_plugin_codex_bridge/_conf_schema.json`: plugin configuration schema.

## Default Connection

The plugin starts a WebSocket server at:

```text
ws://0.0.0.0:32124/ws/codex
```

The main bridge relay connects to it with:

```text
ws://192.168.10.11:32124/ws/codex
```

Adjust the plugin config and relay environment variables for your own network.
