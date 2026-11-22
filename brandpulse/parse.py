"""Turn a raw Jetstream message into a flat post record, or None if it is not a new post.

Bluesky runs two Jetstream formats. v1 (jetstream1/2.*.bsky.network/subscribe) sends
{"did", "time_us", "kind": "commit", "commit": {...}}. v2
(jetstream.*.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents) wraps the same
commit in {"$type": "message", "payload": {...}}. Both are handled here so the producer
keeps working if one endpoint goes away.
"""
from __future__ import annotations

from datetime import datetime, timezone

POST = "app.bsky.feed.post"


def _iso_to_us(ts: str | None) -> int | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def parse_event(msg: dict) -> dict | None:
    """Return {uri, did, rkey, text, langs, created_at, event_us, is_reply, cursor} or None."""
    if msg.get("$type") == "message" and isinstance(msg.get("payload"), dict):  # v2
        p = msg["payload"]
        if not str(p.get("$type", "")).endswith("#commit"):
            return None
        did, op, coll, rkey, record = p.get("did"), p.get("operation"), p.get("collection"), p.get("rkey"), p.get("record")
        event_us = _iso_to_us(p.get("time"))
        cursor = p.get("seq")
    elif msg.get("kind") == "commit" and isinstance(msg.get("commit"), dict):  # v1
        c = msg["commit"]
        did, op, coll, rkey, record = msg.get("did"), c.get("operation"), c.get("collection"), c.get("rkey"), c.get("record")
        event_us = msg.get("time_us")
        cursor = event_us
    else:
        return None

    if op != "create" or coll != POST or not isinstance(record, dict):
        return None
    text = (record.get("text") or "").strip()
    if not text or not did or not rkey:
        return None
    langs = [l for l in (record.get("langs") or []) if isinstance(l, str)][:3]
    return {
        "uri": f"at://{did}/{POST}/{rkey}",
        "did": did,
        "rkey": rkey,
        "text": text[:3000],
        "langs": langs,
        "created_at": record.get("createdAt"),
        "event_us": int(event_us) if event_us is not None else None,
        "is_reply": "reply" in record,
        "cursor": cursor,
    }


def post_url(did: str, rkey: str) -> str:
    """Public web link to a post, used as evidence next to an alert."""
    return f"https://bsky.app/profile/{did}/post/{rkey}"
