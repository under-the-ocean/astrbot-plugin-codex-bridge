"""
AstrBot Codex bridge plugin entrypoint.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from aiohttp import web
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.config.astrbot_config import AstrBotConfig


LOG_PREFIX = "[CodexBridge]"
WS_HOST = "0.0.0.0"
WS_PORT = 32124
DEFAULT_PUSH_EVENTS = {"task-complete", "review-required"}
DEFAULT_REPLY_WINDOW_SECONDS = 600
QUICK_APPROVE_LETTERS = {"y", "a"}
QUICK_SWITCH_LETTERS = {"s"}
CONVERSATION_SELECT_WINDOW_SECONDS = 60


@dataclass
class ReplySession:
    target_umo: str
    created_at: float
    conversation_id: str
    conversation_label: str
    reason: str
    source: str


@dataclass
class ConversationSelectSession:
    target_umo: str
    created_at: float


class CodexBridgePlugin(Star):
    """AstrBot plugin that will bridge QQ messages into the injected Codex page runtime."""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.context = context
        self.config = config
        self._ws_app: web.Application | None = None
        self._ws_runner: web.AppRunner | None = None
        self._ws_site: web.TCPSite | None = None
        self._client_ws: web.WebSocketResponse | None = None
        self._clients: dict[web.WebSocketResponse, dict[str, Any]] = {}
        self._client_info: dict[str, Any] = {}
        self._last_client_info: dict[str, Any] = {}
        self._last_connected_at = "-"
        self._last_disconnected_at = "-"
        self._last_message_type = "-"
        self._command_seq = 0
        self._pending_commands: deque[dict[str, Any]] = deque()
        self._command_results: deque[dict[str, Any]] = deque(maxlen=50)
        self._recent_event_signatures: dict[str, float] = {}
        self._recent_reply_signatures: dict[str, float] = {}
        self._consumed_message_signatures: dict[str, float] = {}
        self._ws_host = str(self._get_config_value("ws_host", WS_HOST) or WS_HOST)
        self._ws_port = int(self._get_config_value("ws_port", WS_PORT) or WS_PORT)
        self._push_targets = self._read_list_config("push_targets")
        self._allowed_umos = set(self._read_list_config("allowed_umos") or self._read_list_config("allow_command_sources"))
        self._push_events = set(self._read_list_config("push_events")) or set(DEFAULT_PUSH_EVENTS)
        self._reply_window_seconds = int(self._get_config_value("reply_window_seconds", DEFAULT_REPLY_WINDOW_SECONDS) or DEFAULT_REPLY_WINDOW_SECONDS)
        self._reply_sessions: dict[str, ReplySession] = {}
        self._conversation_select_sessions: dict[str, ConversationSelectSession] = {}

    async def initialize(self):
        logger.info(f"{LOG_PREFIX} plugin initialized")
        await self._start_ws_server()

    async def terminate(self):
        await self._stop_ws_server()
        logger.info(f"{LOG_PREFIX} plugin terminated")

    async def _start_ws_server(self):
        self._ws_app = web.Application()
        self._ws_app.router.add_get("/ws/codex", self._handle_ws)
        self._ws_runner = web.AppRunner(self._ws_app)
        await self._ws_runner.setup()
        self._ws_site = web.TCPSite(self._ws_runner, self._ws_host, self._ws_port)
        await self._ws_site.start()
        logger.info(f"{LOG_PREFIX} websocket server listening at ws://{self._ws_host}:{self._ws_port}/ws/codex")

    async def _stop_ws_server(self):
        for client_ws in list(self._clients.keys()):
            await client_ws.close()
        self._clients.clear()
        self._client_ws = None
        if self._ws_runner is not None:
            await self._ws_runner.cleanup()
            self._ws_runner = None
            self._ws_site = None
            self._ws_app = None

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=20)
        await ws.prepare(request)

        connected_at = self._now_text()
        self._last_connected_at = connected_at
        client_info = {
            "peer": request.remote,
            "connected_at_text": connected_at,
            "connected_at": asyncio.get_running_loop().time(),
        }
        self._clients[ws] = client_info
        self._client_ws = ws
        self._client_info = client_info
        self._last_client_info = dict(self._client_info)
        logger.info(f"{LOG_PREFIX} Codex client connected from {request.remote}")

        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    await self._handle_ws_message(ws, msg.data)
                elif msg.type == web.WSMsgType.ERROR:
                    logger.warning(f"{LOG_PREFIX} websocket error: {ws.exception()}")
        finally:
            self._last_disconnected_at = self._now_text()
            self._last_client_info = dict(self._client_info)
            self._clients.pop(ws, None)
            if self._client_ws is ws:
                self._select_active_client()
            logger.info(f"{LOG_PREFIX} Codex client disconnected")
        return ws

    async def _handle_ws_message(self, ws: web.WebSocketResponse, raw: str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"{LOG_PREFIX} received non-JSON websocket payload")
            return

        message_type = str(data.get("type") or "")
        client_info = self._clients.get(ws)
        if client_info is None:
            client_info = {}
            self._clients[ws] = client_info
        self._client_ws = ws
        self._client_info = client_info
        self._last_message_type = message_type or "-"
        client_info["last_message_type"] = self._last_message_type
        client_info["last_message_at"] = self._now_text()
        self._last_client_info = dict(client_info)
        if message_type == "hello":
            client_info.update(data.get("client") or {})
            client_info["last_hello"] = data.get("client") or {}
            self._last_client_info = dict(client_info)
            await ws.send_json(
                {
                    "type": "hello-ack",
                    "server": {
                        "name": "astrbot-codex-bridge",
                        "ws_path": "/ws/codex",
                    },
                }
            )
            return

        if message_type == "state":
            client_info["state"] = data.get("state") or {}
            self._last_client_info = dict(client_info)
            return

        if message_type == "event":
            event = data.get("event") or {}
            self._command_results.append(
                {
                    "kind": "event",
                    "event": event,
                }
            )
            await self._handle_codex_event(event)
            return

        if message_type == "command-result":
            self._command_results.append(
                {
                    "kind": "command-result",
                    "result": data,
                }
            )
            return

    async def _send_command(self, command_type: str, payload: dict[str, Any] | None = None) -> bool:
        self._select_active_client()
        if self._client_ws is None:
            return False
        self._command_seq += 1
        command = {
            "type": "command",
            "command": {
                "id": self._command_seq,
                "type": command_type,
                "payload": payload or {},
            },
        }
        self._pending_commands.append(command["command"])
        await self._client_ws.send_json(command)
        return True

    def _select_active_client(self):
        active_items = [(ws, info) for ws, info in self._clients.items() if not ws.closed]
        if not active_items:
            self._client_ws = None
            self._client_info = {}
            return
        relay_items = [
            (ws, info)
            for ws, info in active_items
            if str(info.get("scriptId") or "").startswith("codex-") or bool(info.get("relay"))
        ]
        ws, info = (relay_items or active_items)[-1]
        self._client_ws = ws
        self._client_info = info

    def _get_config_value(self, key: str, default: Any = None) -> Any:
        try:
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
        except Exception:
            pass
        try:
            return getattr(self.config, key, default)
        except Exception:
            return default

    def _read_list_config(self, key: str) -> list[str]:
        value = self._get_config_value(key, [])
        if value is None:
            return []
        if isinstance(value, str):
            text = value.strip()
            return [text] if text else []
        if isinstance(value, (list, tuple, set)):
            return [str(item).strip() for item in value if str(item).strip()]
        return []

    def _now_text(self) -> str:
        return datetime.now().isoformat(timespec="seconds")

    def _is_command_source_allowed(self, umo: str) -> bool:
        if not self._allowed_umos:
            return True
        return umo in self._allowed_umos

    def _arm_reply_session(self, umo: str, conversation_id: str, conversation_label: str, reason: str, source: str = ""):
        self._reply_sessions[umo] = ReplySession(
            target_umo=umo,
            created_at=asyncio.get_running_loop().time(),
            conversation_id=conversation_id,
            conversation_label=conversation_label,
            reason=reason,
            source=source,
        )

    def _get_reply_session(self, umo: str) -> ReplySession | None:
        session = self._reply_sessions.get(umo)
        if not session:
            return None
        now = asyncio.get_running_loop().time()
        if now - session.created_at > self._reply_window_seconds:
            self._reply_sessions.pop(umo, None)
            return None
        return session

    def _clear_reply_session(self, umo: str):
        self._reply_sessions.pop(umo, None)

    def _arm_conversation_select_session(self, umo: str):
        self._conversation_select_sessions[umo] = ConversationSelectSession(
            target_umo=umo,
            created_at=asyncio.get_running_loop().time(),
        )

    def _get_conversation_select_session(self, umo: str) -> ConversationSelectSession | None:
        session = self._conversation_select_sessions.get(umo)
        if not session:
            return None
        now = asyncio.get_running_loop().time()
        if now - session.created_at > CONVERSATION_SELECT_WINDOW_SECONDS:
            self._conversation_select_sessions.pop(umo, None)
            return None
        return session

    def _clear_conversation_select_session(self, umo: str):
        self._conversation_select_sessions.pop(umo, None)

    def _cleanup_signature_cache(self, cache: dict[str, float], ttl_seconds: int = 300):
        now = asyncio.get_running_loop().time()
        expired = [key for key, timestamp in cache.items() if now - timestamp > ttl_seconds]
        for key in expired:
            cache.pop(key, None)

    def _remember_signature(self, cache: dict[str, float], signature: str, ttl_seconds: int = 300) -> bool:
        self._cleanup_signature_cache(cache, ttl_seconds)
        if signature in cache:
            return False
        cache[signature] = asyncio.get_running_loop().time()
        return True

    def _hash_text(self, value: Any) -> str:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _event_signature(self, event_payload: dict[str, Any]) -> str:
        detail = event_payload.get("detail") or {}
        return self._hash_text(
            {
                "event": event_payload.get("event"),
                "conversationName": event_payload.get("conversationName") or event_payload.get("conversationId"),
                "text": detail.get("text"),
                "status": detail.get("status"),
                "previousStatus": detail.get("previousStatus"),
            }
        )

    def _reply_signature(self, umo: str, text: str) -> str:
        return self._hash_text({"umo": umo, "text": text})

    def _message_signature(self, event: AstrMessageEvent, text: str) -> str:
        message_obj = getattr(event, "message_obj", None)
        raw_message = getattr(event, "raw_message", None)
        message_id = getattr(event, "message_id", None)
        if not message_id and message_obj is not None:
            message_id = getattr(message_obj, "message_id", None)
        if not message_id and raw_message is not None:
            message_id = getattr(raw_message, "message_id", None)
        return self._hash_text(
            {
                "umo": str(event.unified_msg_origin or ""),
                "message_id": str(message_id or ""),
                "text": text,
            }
        )

    def _mark_message_consumed(self, event: AstrMessageEvent, text: str):
        self._remember_signature(self._consumed_message_signatures, self._message_signature(event, text), 10)

    def _is_message_consumed(self, event: AstrMessageEvent, text: str) -> bool:
        signature = self._message_signature(event, text)
        self._cleanup_signature_cache(self._consumed_message_signatures, 10)
        return signature in self._consumed_message_signatures

    def _is_quick_approve_text(self, text: str) -> bool:
        return len(text.strip()) == 1 and text.strip().lower() in QUICK_APPROVE_LETTERS

    def _is_quick_switch_text(self, text: str) -> bool:
        return len(text.strip()) == 1 and text.strip().lower() in QUICK_SWITCH_LETTERS

    def _current_state(self) -> dict[str, Any]:
        return self._client_info.get("state") or self._last_client_info.get("state") or {}

    def _event_is_current_conversation(self, event_payload: dict[str, Any]) -> bool:
        detail = event_payload.get("detail") or {}
        if detail.get("active") is True:
            return True
        state = self._current_state()
        event_id = str(event_payload.get("conversationId") or "")
        state_id = str(state.get("conversationId") or "")
        if event_id and state_id and event_id == state_id:
            return True
        event_name = str(event_payload.get("conversationName") or "")
        state_name = str(state.get("conversationName") or "")
        return bool(event_name and state_name and event_name == state_name)

    async def _handle_codex_event(self, event_payload: dict[str, Any]):
        event_name = str(event_payload.get("event") or "")
        if event_name not in self._push_events:
            return
        if not self._push_targets:
            return
        if not self._remember_signature(self._recent_event_signatures, self._event_signature(event_payload)):
            return

        detail = event_payload.get("detail") or {}
        is_current = self._event_is_current_conversation(event_payload)
        reply_source = "sidebar" if str(detail.get("source") or "") == "sidebar" and not is_current else ""
        text = self._render_push_message(event_name, event_payload, detail)
        if not text:
            return

        for target in self._push_targets:
            try:
                self._arm_reply_session(
                    target,
                    self._conversation_target(event_payload),
                    self._conversation_label(event_payload),
                    event_name,
                    reply_source,
                )
                await self.context.send_message(target, event_payload.get("_message_chain") or self._plain_chain(text))
            except Exception as exc:
                logger.warning(f"{LOG_PREFIX} failed to push event to {target}: {exc}")

    def _plain_chain(self, text: str):
        from astrbot.api.event import MessageChain

        return MessageChain().message(text)

    def _render_push_message(self, event_name: str, event_payload: dict[str, Any], detail: dict[str, Any]) -> str:
        conversation_id = self._conversation_label(event_payload)
        text = str(detail.get("text") or "").strip()
        preview = self._clean_preview_text(text)[:500] if text else ""
        reply_hint = f"{self._reply_window_seconds} 秒内快速回复（仅一次有效）"
        is_sidebar_event = str(detail.get("source") or "") == "sidebar" and not self._event_is_current_conversation(event_payload)
        switch_hint = f"{self._reply_window_seconds} 秒内快速操作：发送 s 切换到该会话"

        if event_name == "task-complete":
            quick_hint = f"{switch_hint}；切换后可继续回复。" if is_sidebar_event else f"{reply_hint}：直接发送消息即可继续对话。"
            return (
                f"[Codex] 任务已完成\n"
                f"会话：{conversation_id}\n\n"
                f"{preview}\n\n"
                f"{quick_hint}"
            )
        if event_name == "review-required":
            quick_hint = (
                f"{switch_hint}；切换后发送 y 同意审核。"
                if is_sidebar_event
                else f"{reply_hint}：发送 y 同意审核；发送其他内容则作为回复转发给 Codex。"
            )
            return (
                f"[Codex] 需要审核\n"
                f"会话：{conversation_id}\n\n"
                f"{preview}\n\n"
                f"{quick_hint}"
            )
        if event_name == "review-approved":
            return f"[Codex] 审核已同意\n会话：{conversation_id}"
        if event_name == "status-change":
            previous_status = detail.get("previousStatus") or "-"
            status = detail.get("status") or "-"
            return f"[Codex] 状态变化：{previous_status} -> {status}\n会话：{conversation_id}"
        return ""

    def _clean_preview_text(self, text: str) -> str:
        lines = []
        for raw_line in str(text or "").splitlines():
            line = raw_line.strip()
            if not line:
                lines.append("")
                continue
            if line in {"撤销", "审核"}:
                continue
            line = re.sub(r"(?:^|\s)(撤销|审核)(?=\s|$)", "", line).strip()
            line = re.sub(r"\s{2,}", " ", line).strip()
            if line:
                lines.append(line)
        cleaned = "\n".join(lines)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned

    def _conversation_label(self, payload: dict[str, Any] | None) -> str:
        payload = payload or {}
        return str(payload.get("conversationName") or payload.get("conversationId") or "-")

    def _conversation_target(self, payload: dict[str, Any] | None) -> str:
        payload = payload or {}
        return str(payload.get("conversationId") or payload.get("conversationName") or "-")

    def _state_conversation_label(self, state: dict[str, Any]) -> str:
        return str(state.get("conversationName") or state.get("conversationId") or "-")

    def _format_conversations(self, items: list[dict[str, Any]], limit: int = 20) -> str:
        if not items:
            return "暂无可读取的对话。"
        lines = []
        for item in items[:limit]:
            marker = "*" if item.get("active") else " "
            name = item.get("displayName")
            if not name:
                folder_name = item.get("folderName")
                item_name = item.get("name") or item.get("id") or "-"
                name = f"{folder_name} / {item_name}" if folder_name else item_name
            status_label = item.get("statusLabel")
            status_text = f" [{status_label}]" if status_label else ""
            lines.append(f"{marker} {item.get('index', '?')}. {name}{status_text}")
        if len(items) > limit:
            lines.append(f"... 还有 {len(items) - limit} 个")
        return "\n".join(lines)

    @filter.command("codex")
    async def codex_command(self, event: AstrMessageEvent):
        """Basic Codex bridge command placeholder."""
        text = (event.message_str or "").strip()
        parts = text.split(maxsplit=2)
        source_umo = str(event.unified_msg_origin or "")
        self._mark_message_consumed(event, text)

        if not self._is_command_source_allowed(source_umo):
            yield event.plain_result("当前会话没有权限控制 Codex。")
            return

        if len(parts) == 1:
            yield event.plain_result("可用子命令：状态、发送 <内容>、草稿 <内容>、回复 <内容>、同意、会话、切换 <编号/名称>、停止回复")
            return

        action = parts[1].strip().lower()
        payload = parts[2].strip() if len(parts) > 2 else ""

        if action in {"status", "状态"}:
            self._select_active_client()
            state = self._client_info.get("state") or {}
            info = self._client_info if self._client_ws is not None else self._last_client_info
            online = "online" if self._client_ws is not None else "offline"
            summary = [
                f"bridge: {online}",
                f"clients: {len([ws for ws in self._clients if not ws.closed])}",
                f"peer: {info.get('peer', '-')}",
                f"last_connected_at: {self._last_connected_at}",
                f"last_disconnected_at: {self._last_disconnected_at}",
                f"last_message_type: {self._last_message_type}",
                f"session: {state.get('remoteBridge', {}).get('sessionId', '-')}",
                f"conversation: {self._state_conversation_label(state)}",
                f"status: {state.get('status', '-')}",
                f"injector_relay: {json.dumps(state.get('injectorRelay', {}), ensure_ascii=False)[:500] if state else '-'}",
                f"push_targets: {', '.join(self._push_targets) if self._push_targets else '-'}",
                f"allowed_umos: {', '.join(self._allowed_umos) if self._allowed_umos else '-'}",
                f"ws: {self._ws_host}:{self._ws_port}",
            ]
            yield event.plain_result("\n".join(summary))
            return

        if action in {"send", "发送"}:
            if not payload:
                yield event.plain_result("请提供要发送给 Codex 的内容。")
                return
            ok = await self._send_command("send", {"text": payload, "submit": True})
            if not ok:
                yield event.plain_result("Codex 未连接。")
                return
            yield event.plain_result(f"已发送给 Codex：{payload}")
            return

        if action in {"draft", "草稿"}:
            if not payload:
                yield event.plain_result("请提供要写入 Codex 草稿的内容。")
                return
            ok = await self._send_command("draft", {"text": payload})
            if not ok:
                yield event.plain_result("Codex 未连接。")
                return
            yield event.plain_result(f"已写入 Codex 草稿：{payload}")
            return

        if action in {"reply", "回复"}:
            if not payload:
                yield event.plain_result("请提供要回复给 Codex 的内容。")
                return
            ok = await self._send_command("send", {"text": payload, "submit": True})
            if not ok:
                yield event.plain_result("Codex 未连接。")
                return
            yield event.plain_result(f"已回复 Codex：{payload}")
            return

        if action in {"approve", "同意"}:
            ok = await self._send_command("approve", {})
            if not ok:
                yield event.plain_result("Codex 未连接。")
                return
            yield event.plain_result("已发送审核同意。")
            return

        if action in {"session", "会话"}:
            state = self._client_info.get("state") or {}
            conversations = state.get("conversations") or []
            self._arm_conversation_select_session(source_umo)
            yield event.plain_result(
                f"当前会话：{self._state_conversation_label(state)}\n\n"
                f"{self._format_conversations(conversations)}\n\n"
                f"60 秒内直接发送编号即可切换会话。"
            )
            return

        if action in {"switch", "切换", "切换会话"}:
            if not payload:
                yield event.plain_result("请提供要切换的会话编号或名称。可先发送：/codex 会话")
                return
            ok = await self._send_command("switch-conversation", {"target": payload})
            if not ok:
                yield event.plain_result("Codex 未连接。")
                return
            self._clear_conversation_select_session(source_umo)
            yield event.plain_result(f"已请求切换会话：{payload}")
            return

        if action in {"stop-reply", "停止回复"}:
            self._clear_reply_session(source_umo)
            yield event.plain_result("已关闭当前会话的快速回复模式。")
            return

        yield event.plain_result("未知子命令。可用：状态、发送 <内容>、草稿 <内容>、回复 <内容>、同意、会话、切换 <编号/名称>、停止回复")

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def codex_followup_message(self, event: AstrMessageEvent):
        source_umo = str(event.unified_msg_origin or "")
        if not self._is_command_source_allowed(source_umo):
            return

        text = (event.message_str or "").strip()
        if not text:
            return
        if self._is_message_consumed(event, text):
            return
        if text.startswith("/") or text.startswith("%"):
            return

        select_session = self._get_conversation_select_session(source_umo)
        if select_session and text.isdigit():
            self._mark_message_consumed(event, text)
            ok = await self._send_command("switch-conversation", {"target": text})
            if not ok:
                yield event.plain_result("Codex 未连接，无法切换会话。")
                return
            self._clear_conversation_select_session(source_umo)
            yield event.plain_result(f"已请求切换到第 {text} 个会话。")
            return

        session = self._get_reply_session(source_umo)
        if not session:
            return

        if self._is_quick_switch_text(text):
            self._mark_message_consumed(event, text)
            target = session.conversation_id or session.conversation_label
            ok = await self._send_command("switch-conversation", {"target": target})
            if not ok:
                yield event.plain_result("Codex 未连接，无法切换会话。")
                return
            session.created_at = asyncio.get_running_loop().time()
            label = session.conversation_label or target
            yield event.plain_result(f"已请求切换到会话：{label}")
            return

        if session.reason == "review-required" and self._is_quick_approve_text(text):
            self._mark_message_consumed(event, text)
            if session.source == "sidebar":
                yield event.plain_result("该审核不在当前 Codex 对话中，请先发送 s 切换到该会话，再发送 y 同意审核。")
                return
            ok = await self._send_command("approve", {})
            if not ok:
                yield event.plain_result("Codex 未连接，无法发送审核同意。")
                return
            self._clear_reply_session(source_umo)
            yield event.plain_result("已发送审核同意。")
            return
        if not self._remember_signature(self._recent_reply_signatures, self._reply_signature(source_umo, text), 120):
            return

        self._mark_message_consumed(event, text)
        ok = await self._send_command("send", {"text": text, "submit": True})
        if not ok:
            yield event.plain_result("Codex 未连接。")
            return
        self._clear_reply_session(source_umo)
        yield event.plain_result("已转发你的快速回复给 Codex。")
