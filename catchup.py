"""Background scanner: finds Slack messages we never replied to, and replays
them through the bot's normal trigger handler.

Runs in a daemon thread:
- After STARTUP_DELAY seconds (so the webhook server has time to come up)
- Every CATCHUP_INTERVAL seconds thereafter

For each DM the user has, walks the last CATCHUP_LOOKBACK_HOURS hours of
history. If a message from someone else has no later self-reply AND its
event key is not in event_store's processed set, the scanner queues it
through process_fn.

If event_store says we're inside a recent rate-limit window, the scanner
skips that cycle entirely (no point burning quota that's already exhausted).

NOTE: channel mentions are NOT scanned here in this version. Real-time
mention events still flow through the webhook; only DMs get catch-up
coverage. Adding channel coverage means listing every channel the user
is in and scanning each — doable, but a lot of API calls. Defer to v2 if
mention-missing becomes an actual problem.
"""
from __future__ import annotations

import logging
import os
import time
from threading import Event, Thread

import event_store

log = logging.getLogger(__name__)

CATCHUP_INTERVAL = int(os.environ.get("CATCHUP_INTERVAL_SECONDS", "300"))
LOOKBACK_HOURS = int(os.environ.get("CATCHUP_LOOKBACK_HOURS", "24"))
STARTUP_DELAY = int(os.environ.get("CATCHUP_STARTUP_DELAY_SECONDS", "20"))
HISTORY_LIMIT = int(os.environ.get("CATCHUP_HISTORY_LIMIT", "50"))

# Self-messages whose text starts with one of these are placeholder/fallback
# strings the bot posted itself — they do NOT count as a real reply, so
# catchup should keep looking back further to find an actual reply.
_PLACEHOLDER_PREFIXES: tuple[str, ...] = (
    "_thinking...",
    "_thinking…",
    "_couldn't fetch",
    "_at capacity",
)


def _is_placeholder_text(text: str) -> bool:
    t = (text or "").strip()
    return any(t.startswith(p) for p in _PLACEHOLDER_PREFIXES)

_stop = Event()


def start(user_client, kong_ming_user_id, process_fn):
    """Spawn the catch-up daemon thread.

    `process_fn(event_dict)` is called for each missed message. It should
    behave like the webhook handler — claim, post placeholder, run Claude,
    post reply, mark processed.
    """
    t = Thread(
        target=_loop,
        args=(user_client, kong_ming_user_id, process_fn),
        daemon=True,
        name="catchup",
    )
    t.start()
    log.info(
        "catchup thread started (interval=%ds lookback=%dh startup_delay=%ds)",
        CATCHUP_INTERVAL, LOOKBACK_HOURS, STARTUP_DELAY,
    )
    return t


def stop() -> None:
    _stop.set()


def _loop(user_client, my_user_id, process_fn) -> None:
    # Wait a bit so the rest of the server has time to come up cleanly.
    if _stop.wait(STARTUP_DELAY):
        return
    while True:
        try:
            if event_store.is_in_rate_limit_window():
                log.info("catchup: in rate-limit window, skipping this scan cycle")
            else:
                _scan_once(user_client, my_user_id, process_fn)
        except Exception:
            log.exception("catchup: scan cycle failed")
        if _stop.wait(CATCHUP_INTERVAL):
            return


def _scan_once(user_client, my_user_id, process_fn) -> None:
    # 1) Drain the retry queue FIRST — these are failed webhook deliveries
    # the bot saw in real time but couldn't reply to. This covers channel
    # mentions (which DM scan doesn't see) and any STATUS_ERROR / TIMEOUT.
    _drain_retry_queue(user_client, my_user_id, process_fn)

    # 2) Then scan known DM channels for "missing self-reply" gaps.
    cutoff_ts = time.time() - (LOOKBACK_HOURS * 3600)
    # We don't have im:read scope (needed to list DM channels server-side),
    # but we have im:history (read contents of channels we know). So we scan
    # the DM channels we've already observed via webhook events.
    dm_channel_ids = event_store.seen_dm_channels()
    log.info("catchup: scanning %d known DM channels (cutoff_ts=%d)", len(dm_channel_ids), int(cutoff_ts))

    total_unreplied = 0
    triggered = 0

    for ch_id in dm_channel_ids:
        if not ch_id:
            continue
        unreplied = _find_unreplied_dms(user_client, ch_id, my_user_id, cutoff_ts)
        for msg in unreplied:
            total_unreplied += 1
            event = {
                "type": "message",
                "channel": ch_id,
                "channel_type": "im",
                "user": msg.get("user"),
                "text": msg.get("text", ""),
                "ts": msg["ts"],
                "thread_ts": msg.get("thread_ts"),
            }
            try:
                process_fn(event)
                triggered += 1
            except Exception:
                log.exception(
                    "catchup: process_fn failed (ch=%s ts=%s)", ch_id, msg.get("ts"),
                )
            # If we got rate-limited during this trigger, bail out for the cycle.
            if event_store.is_in_rate_limit_window(seconds=60):
                log.info("catchup: rate-limited mid-cycle, aborting scan")
                return

    log.info("catchup: scan done; unreplied=%d triggered=%d", total_unreplied, triggered)


def _drain_retry_queue(user_client, my_user_id, process_fn) -> None:
    """Process every item in the persistent retry queue.

    For each item:
      - If already marked processed elsewhere → remove.
      - If a real self-reply now exists in the message's thread (someone
        replied manually, or via another path) → mark processed + remove.
      - If attempts >= MAX → mark processed (give up) + remove + warn.
      - Otherwise: bump attempt counter, run process_fn(event), let it
        succeed/fail via the normal _process_trigger path. process_fn marks
        processed on success and removes from the queue itself.
    """
    items = event_store.retry_queue_snapshot()
    if not items:
        return
    log.info("catchup: draining retry queue (%d items)", len(items))

    delivered = 0
    skipped_already_done = 0
    skipped_externally_replied = 0
    given_up = 0

    for item in items:
        item_id = item.get("id")
        ev = item.get("event") or {}
        ch = ev.get("channel")
        ts = ev.get("ts")
        attempts = int(item.get("attempts", 0))

        if not item_id or not ch or not ts:
            event_store.remove_retry_item(item_id or "")
            continue

        if event_store.is_processed(item_id):
            event_store.remove_retry_item(item_id)
            skipped_already_done += 1
            continue

        # Did someone (you, or another path) reply in the meantime?
        if _has_real_self_reply_in_thread(user_client, ch, ts, my_user_id):
            event_store.mark_processed(item_id)
            event_store.remove_retry_item(item_id)
            skipped_externally_replied += 1
            log.info("retry: %s already replied externally, removed", item_id)
            continue

        if attempts >= event_store.MAX_RETRY_ATTEMPTS:
            event_store.mark_processed(item_id)
            event_store.remove_retry_item(item_id)
            given_up += 1
            log.warning(
                "retry: %s exhausted (attempts=%d), giving up — needs manual followup",
                item_id, attempts,
            )
            continue

        # Bail out if a rate-limit was just hit — don't burn quota retrying.
        if event_store.is_in_rate_limit_window(seconds=60):
            log.info("retry: rate-limit window active mid-drain, stopping")
            break

        # Try again. process_fn (=_process_trigger) handles success/failure
        # internally — it marks_processed on STATUS_OK and removes from queue,
        # or re-enqueues (same id, no duplicate) on continued failure.
        event_store.bump_retry_attempt(item_id, status="retrying")
        try:
            process_fn(ev)
        except Exception:
            log.exception("retry: process_fn threw for %s", item_id)

        if event_store.is_processed(item_id):
            delivered += 1

    log.info(
        "catchup: drain done — delivered=%d skipped_done=%d skipped_extreply=%d given_up=%d",
        delivered, skipped_already_done, skipped_externally_replied, given_up,
    )


def _list_dm_channels(user_client) -> list[dict]:
    try:
        resp = user_client.users_conversations(
            types="im",
            limit=200,
            exclude_archived=True,
        )
        return resp.get("channels", []) or []
    except Exception as e:
        log.warning("catchup: list DMs failed: %s", e)
        return []


def _find_unreplied_dms(user_client, channel_id, my_user_id, oldest_ts) -> list[dict]:
    """Return messages in this DM that don't have a later reply from us.

    Two-pass detection:
      Pass 1 (top-level): walk channel history chronologically; a real
              self-message in the top-level stream resets the accumulator.
      Pass 2 (thread):    for each remaining candidate, fetch its thread
              replies and drop it if a real self-reply exists in-thread.
              This is the important bit because the bot currently posts
              replies as thread replies on the trigger message, and those
              don't appear in conversations.history.
    """
    try:
        resp = user_client.conversations_history(
            channel=channel_id,
            oldest=str(oldest_ts),
            limit=HISTORY_LIMIT,
        )
    except Exception as e:
        log.warning("catchup: history failed for %s: %s", channel_id, e)
        return []

    msgs = resp.get("messages", []) or []
    # Slack returns newest-first. Reverse for chronological.
    msgs = list(reversed(msgs))

    candidates: list[dict] = []
    for msg in msgs:
        if msg.get("subtype"):
            continue
        # Check self first: posts via OUR user token come back with bot_id
        # set (to our app's bot_id), so checking bot_id first would
        # incorrectly drop our own replies.
        if msg.get("user") == my_user_id:
            if _is_placeholder_text(msg.get("text", "")):
                continue
            candidates = []
            continue
        # Real third-party bots (Helios, ChatGPT, etc.) — ignore.
        if msg.get("bot_id"):
            continue
        candidates.append(msg)

    # Pass 2: filter out any candidate that has a real self-reply in its thread.
    final: list[dict] = []
    for msg in candidates:
        if _has_real_self_reply_in_thread(user_client, channel_id, msg["ts"], my_user_id):
            continue
        final.append(msg)
    return final


def _has_real_self_reply_in_thread(user_client, channel_id, parent_ts, my_user_id) -> bool:
    """True if there's a non-placeholder self-reply in the thread of parent_ts.

    If we can't fetch (API error), be conservative and say "no reply" — that
    causes at worst a retry, vs. silently dropping a real unreplied message.
    """
    try:
        resp = user_client.conversations_replies(
            channel=channel_id, ts=parent_ts, limit=20,
        )
    except Exception as e:
        log.debug("catchup: thread check failed for %s/%s: %s", channel_id, parent_ts, e)
        return False
    messages = resp.get("messages", []) or []
    # First entry is the parent itself; skip it.
    for m in messages[1:]:
        if m.get("subtype"):
            continue
        # Self-reply check first — see comment in _find_unreplied_dms.
        if m.get("user") != my_user_id:
            continue
        if _is_placeholder_text(m.get("text", "")):
            continue
        return True
    return False
