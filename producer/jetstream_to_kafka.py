"""Stream every new public Bluesky post into a Kafka topic.

Runs on a laptop. Reads Bluesky's Jetstream websocket, keeps only new posts, and
produces them to Confluent Cloud as Avro, registered in Schema Registry.

    python producer/jetstream_to_kafka.py --dry-run --minutes 1     # look at the stream, no Kafka
    python producer/jetstream_to_kafka.py                           # produce until Ctrl+C
    python producer/jetstream_to_kafka.py --rewind-minutes 60       # start an hour back, if the relay still has it

On disconnect it reconnects with the last cursor, so a dropped connection loses nothing
inside Jetstream's lookback window.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path

import websockets

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from brandpulse.parse import parse_event  # noqa: E402

ENDPOINTS = [
    "wss://jetstream2.us-east.bsky.network/subscribe",
    "wss://jetstream1.us-west.bsky.network/subscribe",
]
COLLECTION = "app.bsky.feed.post"


def load_env(path: Path) -> None:
    """Minimal .env reader, so the only secrets live in a git-ignored file."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def make_sink(dry_run: bool, out_path: str | None):
    if dry_run:
        fh = open(out_path, "a", encoding="utf-8") if out_path else None

        def send(post: dict) -> None:
            if fh:
                fh.write(json.dumps(post, ensure_ascii=False) + "\n")

        return send, (lambda: fh and fh.close())

    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    need = ["BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET", "SCHEMA_REGISTRY_URL", "SR_API_KEY", "SR_API_SECRET"]
    missing = [k for k in need if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing settings in .env: {', '.join(missing)}")
    topic = os.environ.get("TOPIC", "bluesky.posts")

    schema_str = (ROOT / "schemas" / "bluesky_post.avsc").read_text()
    sr = SchemaRegistryClient({"url": os.environ["SCHEMA_REGISTRY_URL"],
                               "basic.auth.user.info": f'{os.environ["SR_API_KEY"]}:{os.environ["SR_API_SECRET"]}'})
    serialize = AvroSerializer(sr, schema_str)
    producer = Producer({
        "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 200,              # batch small posts together: cheaper and faster
        "compression.type": "zstd",
        "client.id": "brand-pulse-producer",
    })
    ctx = SerializationContext(topic, MessageField.VALUE)
    errors = {"n": 0}

    def on_delivery(err, _msg):
        if err is not None:
            errors["n"] += 1

    def send(post: dict) -> None:
        value = {
            "uri": post["uri"], "did": post["did"], "rkey": post["rkey"], "text": post["text"],
            "langs": post["langs"], "is_reply": post["is_reply"], "created_at": post["created_at"],
            "event_time": post["event_us"] or int(time.time() * 1_000_000),
        }
        while True:
            try:
                producer.produce(topic, key=post["did"].encode(), value=serialize(value, ctx), on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)  # local queue full: wait for deliveries, then retry
        producer.poll(0)

    def close() -> None:
        left = producer.flush(30)
        print(f"flushed; undelivered={left}, delivery errors={errors['n']}")

    return send, close


async def run(args) -> None:
    send, close = make_sink(args.dry_run, args.out)
    cursor = int((time.time() - args.rewind_minutes * 60) * 1_000_000) if args.rewind_minutes else None
    deadline = time.time() + args.minutes * 60 if args.minutes else None
    stats = {"msgs": 0, "posts": 0, "t0": time.time(), "last": time.time()}
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass

    attempt = 0
    while not stop.is_set() and (deadline is None or time.time() < deadline):
        base = ENDPOINTS[attempt % len(ENDPOINTS)]
        url = f"{base}?wantedCollections={COLLECTION}" + (f"&cursor={cursor}" if cursor else "")
        try:
            async with websockets.connect(url, max_size=2**22, ping_interval=20, open_timeout=15) as ws:
                print(f"connected to {base}" + (" (resuming from cursor)" if cursor else ""))
                attempt = 0
                while not stop.is_set():
                    if deadline and time.time() >= deadline:
                        stop.set()
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        continue
                    stats["msgs"] += 1
                    post = parse_event(json.loads(raw))
                    if post is None:
                        continue
                    cursor = post["cursor"] or cursor
                    stats["posts"] += 1
                    send(post)
                    if args.dry_run and not args.out and stats["posts"] <= args.show:
                        print(f'  [{",".join(post["langs"]) or "?"}] {post["text"][:110]!r}')
                    now = time.time()
                    if now - stats["last"] >= args.every:
                        rate = stats["posts"] / (now - stats["t0"])
                        print(f"{time.strftime('%H:%M:%S')}  posts={stats['posts']:,}  rate={rate:,.1f}/s")
                        stats["last"] = now
        except (OSError, websockets.WebSocketException) as exc:
            attempt += 1
            wait = min(30, 2 ** min(attempt, 5))
            print(f"connection lost ({type(exc).__name__}: {exc}); retrying in {wait}s")
            try:
                await asyncio.wait_for(stop.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass

    close()
    secs = max(1.0, time.time() - stats["t0"])
    print(f"done: {stats['posts']:,} posts in {secs / 60:.1f} min ({stats['posts'] / secs:,.1f}/s)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="read the stream without Kafka")
    ap.add_argument("--out", help="with --dry-run, append posts as JSON lines to this file")
    ap.add_argument("--show", type=int, default=15, help="with --dry-run, print this many sample posts")
    ap.add_argument("--minutes", type=float, default=0, help="stop after N minutes (0 = until Ctrl+C)")
    ap.add_argument("--rewind-minutes", type=float, default=0, help="start N minutes in the past")
    ap.add_argument("--every", type=float, default=30, help="seconds between progress lines")
    args = ap.parse_args()
    load_env(ROOT / ".env")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
