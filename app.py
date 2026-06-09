from __future__ import annotations

import logging
import os
import re
from collections import deque

from dotenv import load_dotenv

# IMPORTANT: load .env BEFORE importing claude_reply, because claude_reply
# reads CLAUDE_TIMEOUT / CLAUDE_BIN / CLAUDE_CWD at module-import time.
load_dotenv()

from fastapi import FastAPI, Request
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_sdk import WebClient

import catchup
import event_store
from claude_reply import (
    STATUS_OK,
    STATUS_RATE_LIMIT,
    generate_reply,
)

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.log")
_file_handler = logging.FileHandler(LOG_FILE)
_file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(), _file_handler])
log = logging.getLogger("slack-mention-trigger")

bolt_app = App(
    token=os.environ["SLACK_BOT_TOKEN"],
    signing_secret=os.environ["SLACK_SIGNING_SECRET"],
)

USER_TOKEN = os.environ["SLACK_USER_TOKEN"]
KONG_MING_USER_ID = os.environ["KONG_MING_USER_ID"]
MENTION_TAG = f"<@{KONG_MING_USER_ID}>"

user_client = WebClient(token=USER_TOKEN)

# Cache of Slack user_id -> human display name. Slack user IDs are opaque
# (U0A1BFA2Y7J); feeding them raw into the model means it has no idea WHO it's
# talking to, so it can't answer "who am I" and can't address people by name.
# We resolve once via users.info and cache for the process lifetime (names
# rarely change and a stale name is far cheaper than a per-message API call).
_USER_NAME_CACHE: dict[str, str] = {}


def resolve_user_name(user_id: str | None) -> str | None:
    """Resolve a Slack user_id to a human display name, cached.

    Prefers display_name, then real_name, then the raw handle. Returns None
    for falsy input and falls back to the raw id if the lookup fails, so the
    caller always gets *something* printable but can tell resolution worked
    (a name) from when it didn't (the raw U… id).
    """
    if not user_id:
        return None
    if user_id in _USER_NAME_CACHE:
        return _USER_NAME_CACHE[user_id]
    try:
        resp = user_client.users_info(user=user_id)
        profile = resp["user"]["profile"]
        name = (
            profile.get("display_name")
            or profile.get("real_name")
            or resp["user"].get("real_name")
            or resp["user"].get("name")
            or user_id
        )
    except Exception as e:
        log.warning("could not resolve user %s to a name: %s", user_id, e)
        name = user_id
    _USER_NAME_CACHE[user_id] = name
    return name

# In-memory dedup for the bolt-handler retry pass — Slack itself may resend the
# same event up to 3 times within ~30 min if our HTTP response is slow/failed.
# event_store.claim() is the durable equivalent (across restarts); this deque
# is a fast path so we don't hit JSON I/O for trivially-duplicate events.
_recent_event_ids: deque[str] = deque(maxlen=512)


def _mention_re() -> re.Pattern[str]:
    return re.compile(r"<@[A-Z0-9]+>")


def _strip_mentions(text: str) -> str:
    return _mention_re().sub("", text).strip()


# Slack subtypes that are NOT real user messages we should reply to.
# Anything else (no subtype, or user-message subtypes like file_share /
# thread_broadcast / me_message) is treated as a real user message.
_SYNTHETIC_SUBTYPES = frozenset({
    "message_changed",
    "message_deleted",
    "message_replied",
    "bot_message",
    "channel_join",
    "channel_leave",
    "channel_topic",
    "channel_purpose",
    "channel_name",
    "channel_archive",
    "channel_unarchive",
    "group_join",
    "group_leave",
    "pinned_item",
    "unpinned_item",
    "tombstone",
    "reminder_add",
})


def _is_from_bot_or_system(message: dict) -> bool:
    if message.get("bot_id"):
        return True
    subtype = message.get("subtype")
    return bool(subtype) and subtype in _SYNTHETIC_SUBTYPES


def _event_key(event: dict) -> str:
    """Deterministic event key shared by webhook + catch-up scanner."""
    return f"msg:{event.get('channel')}:{event.get('ts')}"


def _add_eyes_reaction(channel: str, ts: str) -> None:
    try:
        user_client.reactions_add(channel=channel, name="eyes", timestamp=ts)
    except Exception as e:
        log.info("eyes reaction not added: %s", e)


def _fetch_thread_context(channel: str, thread_ts: str, limit: int = 20) -> list[dict]:
    try:
        resp = user_client.conversations_replies(channel=channel, ts=thread_ts, limit=limit)
    except Exception as e:
        log.warning("failed to fetch thread context: %s", e)
        return []

    out = []
    for m in resp.get("messages", [])[:-1]:
        if _is_from_bot_or_system(m):
            continue
        user = m.get("user", "unknown")
        if user == KONG_MING_USER_ID:
            user = "me"
        else:
            user = resolve_user_name(user) or user
        text = _strip_mentions(m.get("text", ""))
        if text:
            out.append({"user": user, "text": text})
    return out


THINKING_PLACEHOLDER = "_thinking..._"
FAILURE_PLACEHOLDER = "_couldn't fetch that right now — please try again in a moment._"
RATE_LIMIT_PLACEHOLDER = "_at capacity right now — I'll get back to you shortly._"


def _post_placeholder(channel: str, thread_ts: str | None) -> str | None:
    try:
        kwargs: dict = {"channel": channel, "text": THINKING_PLACEHOLDER}
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        resp = user_client.chat_postMessage(**kwargs)
        ts = resp.get("ts")
        log.info("posted placeholder: ok=%s ts=%s channel=%s", resp.get("ok"), ts, channel)
        return ts
    except Exception as e:
        log.exception("failed to post placeholder (channel=%s thread_ts=%s): %s", channel, thread_ts, e)
        return None


def _update_message(channel: str, ts: str, text: str) -> bool:
    try:
        resp = user_client.chat_update(channel=channel, ts=ts, text=text)
        log.info("updated msg: ok=%s ts=%s channel=%s", resp.get("ok"), ts, channel)
        return bool(resp.get("ok"))
    except Exception as e:
        log.exception("failed to update msg (channel=%s ts=%s): %s", channel, ts, e)
        return False


def _post_as_kong_ming(channel: str, text: str, thread_ts: str | None) -> None:
    try:
        if thread_ts:
            resp = user_client.chat_postMessage(channel=channel, text=text, thread_ts=thread_ts)
        else:
            resp = user_client.chat_postMessage(channel=channel, text=text)
        log.info("posted reply: ok=%s ts=%s channel=%s", resp.get("ok"), resp.get("ts"), channel)
    except Exception as e:
        log.exception("failed to post reply (channel=%s thread_ts=%s): %s", channel, thread_ts, e)


def _process_trigger(event: dict, source: str = "webhook") -> None:
    """Shared trigger path used by the bolt webhook handler AND the catch-up
    scanner. Atomically claims the event so the two paths can't double-process.

    Mark-processed semantics:
      - STATUS_OK         → mark_processed (won't be retried)
      - STATUS_RATE_LIMIT → do NOT mark; let catchup retry once quota resets
      - STATUS_TIMEOUT    → mark_processed (don't infinite-retry timeouts)
      - STATUS_ERROR      → mark_processed (don't infinite-retry real errors)
    """
    event_id = _event_key(event)

    if not event_store.claim(event_id):
        log.debug("skip: already processed or in-flight (source=%s id=%s)", source, event_id)
        return

    try:
        sender = event.get("user", "?")
        channel = event.get("channel", "?")
        text = event.get("text", "") or ""
        is_dm = event.get("channel_type") == "im"
        is_mention = MENTION_TAG in text
        msg_ts = event["ts"]
        # Thread choice:
        #  - If the asker started a thread (event has thread_ts), reply in it.
        #  - DMs without an existing thread: reply INLINE (no thread_ts). DMs
        #    are 1:1, and threaded replies hide behind a "1 reply" badge that
        #    askers often miss. Inline is the natural place.
        #  - Channels without an existing thread: open a new thread on the
        #    triggering message so the reply doesn't clutter the channel.
        existing_thread = event.get("thread_ts")
        if existing_thread:
            thread_ts = existing_thread
        elif is_dm:
            thread_ts = None
        else:
            thread_ts = msg_ts

        log.info(
            "trigger (%s): sender=%s channel=%s dm=%s mention=%s ts=%s",
            source, sender, channel, is_dm, is_mention, msg_ts,
        )

        _add_eyes_reaction(channel, msg_ts)
        clean_text = _strip_mentions(text)
        placeholder_ts = _post_placeholder(channel=channel, thread_ts=thread_ts)

        # Context — only when the message is itself in a thread.
        if event.get("thread_ts"):
            context = _fetch_thread_context(channel, event["thread_ts"])
        else:
            context = []

        sender_name = resolve_user_name(sender)
        reply, status = generate_reply(
            mention_text=clean_text,
            thread_context=context,
            sender_name=sender_name,
        )

        if status == STATUS_OK and reply:
            posted_via = "edit"
            if placeholder_ts:
                if not _update_message(channel=channel, ts=placeholder_ts, text=reply):
                    _post_as_kong_ming(channel=channel, text=reply, thread_ts=thread_ts)
                    posted_via = "post-after-edit-fail"
            else:
                _post_as_kong_ming(channel=channel, text=reply, thread_ts=thread_ts)
                posted_via = "post-no-placeholder"
            event_store.mark_processed(event_id)
            # Clean up the retry queue if this event was previously failing.
            event_store.remove_retry_item(event_id)
            log.info(
                "reply delivered (%s): id=%s channel=%s thread=%s len=%d via=%s",
                source, event_id, channel, thread_ts, len(reply), posted_via,
            )
            return

        if status == STATUS_RATE_LIMIT:
            # Quota limit hit. Update placeholder to a clearer message and
            # leave event UN-marked so the catch-up loop retries when the
            # quota window resets.
            event_store.mark_rate_limited()
            if placeholder_ts:
                _update_message(channel=channel, ts=placeholder_ts, text=RATE_LIMIT_PLACEHOLDER)
            else:
                _post_as_kong_ming(channel=channel, text=RATE_LIMIT_PLACEHOLDER, thread_ts=thread_ts)
            log.warning(
                "rate-limited (%s): id=%s NOT marked processed — catchup will retry",
                source, event_id,
            )
            return

        # STATUS_TIMEOUT or STATUS_ERROR — webhook delivery failed. Surface
        # the placeholder to the asker, then enqueue for retry by the catch-up
        # loop. We do NOT mark processed here — the queue's MAX_RETRY_ATTEMPTS
        # caps the retry blast radius, and the queue drainer marks processed
        # only after success or exhaustion.
        if placeholder_ts:
            _update_message(channel=channel, ts=placeholder_ts, text=FAILURE_PLACEHOLDER)
        else:
            _post_as_kong_ming(channel=channel, text=FAILURE_PLACEHOLDER, thread_ts=thread_ts)
        event_store.enqueue_retry(event, status=status)
        log.warning(
            "failure (%s): status=%s id=%s — posted fallback and enqueued for retry",
            source, status, event_id,
        )
    finally:
        event_store.release(event_id)


@bolt_app.event("message")
def handle_message(event, body):
    # Quick in-memory dedup for Slack's own webhook retries.
    bolt_event_id = (body or {}).get("event_id")
    if bolt_event_id:
        if bolt_event_id in _recent_event_ids:
            log.info("dedup: skipping repeated slack delivery %s", bolt_event_id)
            return
        _recent_event_ids.append(bolt_event_id)

    sender = event.get("user", "?")
    channel = event.get("channel", "?")
    channel_type = event.get("channel_type")
    text = event.get("text", "") or ""
    is_dm = channel_type == "im"
    is_mention = MENTION_TAG in text
    is_self = sender == KONG_MING_USER_ID
    is_bot = _is_from_bot_or_system(event)

    log.info(
        "msg seen: sender=%s channel=%s channel_type=%s dm=%s mention=%s self=%s bot=%s text=%r",
        sender, channel, channel_type, is_dm, is_mention, is_self, is_bot, text[:120],
    )

    # Even if we don't reply to this specific message, record the DM channel
    # so the catch-up scanner can poll it later.
    if is_dm and channel and channel.startswith("D"):
        event_store.add_seen_dm(channel)

    if is_self:
        return
    if is_bot:
        return
    if not (is_dm or is_mention):
        return

    _process_trigger(event, source="webhook")


api = FastAPI()
handler = SlackRequestHandler(bolt_app)


@api.post("/slack/events")
async def slack_events(req: Request):
    return await handler.handle(req)


@api.get("/health")
def health():
    return {"status": "ok"}


# Kick off the background catch-up scanner. Daemon thread; dies with process.
catchup.start(
    user_client=user_client,
    kong_ming_user_id=KONG_MING_USER_ID,
    process_fn=lambda ev: _process_trigger(ev, source="catchup"),
)
