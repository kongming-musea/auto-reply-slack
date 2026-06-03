"""Persistent + in-memory tracking of Slack events processed by the bot.

`processed_events.json` on disk holds:
- "events": list[str]      — event keys we've successfully replied to
- "last_rate_limit_at": float — unix ts of the most recent rate-limit hit

`_in_flight` (memory only) prevents the webhook handler and the catch-up
scanner from racing on the same message.

An "event key" is a deterministic string derived from the Slack message
(channel + ts), so the webhook and catch-up scanner agree on identity.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from threading import Lock

log = logging.getLogger(__name__)

STORE_PATH = Path(__file__).resolve().parent / "processed_events.json"
MAX_KEEP = 2000  # bound persisted history
MAX_RETRY_QUEUE = 200  # bound retry queue size
MAX_RETRY_ATTEMPTS = 10  # give up after N attempts

_lock = Lock()
_processed: set[str] | None = None
_order: list[str] | None = None
_in_flight: set[str] = set()
_last_rate_limit_at: float = 0.0
_seen_dms: set[str] | None = None
_retry_queue: list[dict] | None = None  # list of {id, event, attempts, first_seen_at, last_attempt_at, last_status}


def _ensure_loaded() -> None:
    global _processed, _order, _last_rate_limit_at, _seen_dms, _retry_queue
    if _processed is not None:
        return
    _processed = set()
    _order = []
    _seen_dms = set()
    _retry_queue = []
    if STORE_PATH.exists():
        try:
            data = json.loads(STORE_PATH.read_text())
            for ev in data.get("events", []) or []:
                if ev not in _processed:
                    _processed.add(ev)
                    _order.append(ev)
            _last_rate_limit_at = float(data.get("last_rate_limit_at", 0) or 0)
            for ch in data.get("seen_dm_channels", []) or []:
                _seen_dms.add(ch)
            for item in data.get("retry_queue", []) or []:
                if isinstance(item, dict) and item.get("id"):
                    _retry_queue.append(item)
            log.info(
                "event_store loaded: %d processed events, %d known DMs, %d retry-queue items; last_rate_limit_at=%s",
                len(_processed), len(_seen_dms), len(_retry_queue), _last_rate_limit_at,
            )
        except Exception as e:
            log.warning("event_store load failed: %s", e)


def _save() -> None:
    try:
        STORE_PATH.write_text(json.dumps({
            "events": _order[-MAX_KEEP:] if _order else [],
            "last_rate_limit_at": _last_rate_limit_at,
            "seen_dm_channels": sorted(_seen_dms) if _seen_dms else [],
            "retry_queue": _retry_queue or [],
        }))
    except Exception as e:
        log.warning("event_store save failed: %s", e)


def is_processed(event_id: str) -> bool:
    if not event_id:
        return False
    with _lock:
        _ensure_loaded()
        return event_id in _processed


def mark_processed(event_id: str) -> None:
    if not event_id:
        return
    with _lock:
        _ensure_loaded()
        if event_id in _processed:
            return
        _processed.add(event_id)
        _order.append(event_id)
        # Trim every so often
        if len(_order) > int(MAX_KEEP * 1.2):
            kept = _order[-MAX_KEEP:]
            _order[:] = kept
            _processed.clear()
            _processed.update(kept)
        _save()


def claim(event_id: str) -> bool:
    """Atomically claim event_id for processing.

    Returns True iff the caller now owns this event (not already processed,
    not currently in flight). False means another handler is on it.
    """
    if not event_id:
        return True  # no key to dedup on
    with _lock:
        _ensure_loaded()
        if event_id in _processed:
            return False
        if event_id in _in_flight:
            return False
        _in_flight.add(event_id)
        return True


def release(event_id: str) -> None:
    if not event_id:
        return
    with _lock:
        _in_flight.discard(event_id)


def mark_rate_limited() -> None:
    global _last_rate_limit_at
    with _lock:
        _ensure_loaded()
        _last_rate_limit_at = time.time()
        _save()


def is_in_rate_limit_window(seconds: int = 1800) -> bool:
    """True if a rate-limit was recorded within the last `seconds` (default 30 min).

    Used by the catch-up scanner to back off so we don't pile retries onto
    a quota that's already exhausted.
    """
    with _lock:
        _ensure_loaded()
        return _last_rate_limit_at > 0 and (time.time() - _last_rate_limit_at) < seconds


def add_seen_dm(channel_id: str) -> None:
    """Remember a DM channel ID. Catch-up uses this list (we don't have the
    `im:read` scope to list DMs server-side, but `im:history` lets us read
    the contents of any channel we already know about)."""
    if not channel_id or not channel_id.startswith("D"):
        return
    with _lock:
        _ensure_loaded()
        if channel_id in _seen_dms:
            return
        _seen_dms.add(channel_id)
        _save()


def seen_dm_channels() -> list[str]:
    with _lock:
        _ensure_loaded()
        return sorted(_seen_dms) if _seen_dms else []


# ----------------- retry queue -----------------
#
# When a webhook delivery fails (STATUS_ERROR / STATUS_TIMEOUT / empty reply),
# the bot enqueues the event here. The catchup scanner drains the queue every
# cycle: it retries each item, removes successes, increments attempts on
# failures, and gives up after MAX_RETRY_ATTEMPTS. This is the safety net for
# CHANNEL mentions (catchup's DM scan can't see them) and for failures whose
# stdout/stderr didn't match our rate-limit patterns.


def enqueue_retry(event: dict, status: str = "error") -> bool:
    """Add a failed webhook event to the retry queue.

    `event` should be the dict that `_process_trigger` receives (channel, ts,
    channel_type, user, text, thread_ts). If the same id is already queued,
    no duplicate is added (we'll just retry the existing entry).

    Returns True if newly enqueued, False if already present or skipped.
    """
    if not event or not event.get("ts") or not event.get("channel"):
        return False
    item_id = f"msg:{event['channel']}:{event['ts']}"
    with _lock:
        _ensure_loaded()
        if any(i.get("id") == item_id for i in _retry_queue):
            return False
        # Persist a minimal copy of the event — enough to reconstruct.
        keep = {k: event.get(k) for k in ("channel", "channel_type", "user", "text", "ts", "thread_ts")}
        now = time.time()
        _retry_queue.append({
            "id": item_id,
            "event": keep,
            "attempts": 0,
            "first_seen_at": now,
            "last_attempt_at": 0,
            "last_status": status,
        })
        # Bound queue size — drop oldest
        if len(_retry_queue) > MAX_RETRY_QUEUE:
            dropped = _retry_queue[:-MAX_RETRY_QUEUE]
            _retry_queue[:] = _retry_queue[-MAX_RETRY_QUEUE:]
            log.warning("retry queue full — dropped %d oldest items", len(dropped))
        _save()
        log.info("enqueue_retry: %s (status=%s)", item_id, status)
        return True


def retry_queue_snapshot() -> list[dict]:
    """Return a copy of current queue items (safe to iterate without holding the lock)."""
    with _lock:
        _ensure_loaded()
        return [dict(i) for i in _retry_queue]


def remove_retry_item(item_id: str) -> bool:
    """Remove a queue item by id."""
    if not item_id:
        return False
    with _lock:
        _ensure_loaded()
        before = len(_retry_queue)
        _retry_queue[:] = [i for i in _retry_queue if i.get("id") != item_id]
        if len(_retry_queue) < before:
            _save()
            return True
        return False


def bump_retry_attempt(item_id: str, status: str = "error") -> int:
    """Increment attempt counter on an item. Returns new attempt count."""
    if not item_id:
        return 0
    with _lock:
        _ensure_loaded()
        for i in _retry_queue:
            if i.get("id") == item_id:
                i["attempts"] = int(i.get("attempts", 0)) + 1
                i["last_attempt_at"] = time.time()
                i["last_status"] = status
                _save()
                return i["attempts"]
        return 0
