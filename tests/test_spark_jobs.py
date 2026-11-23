"""End-to-end check of the Spark layer on a synthetic hour of posts, using local Spark."""
import os
import random
from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")
pytest.importorskip("vaderSentiment")
from pyspark.sql import SparkSession  # noqa: E402

from brandpulse import spark_jobs as sj  # noqa: E402
from brandpulse.match import load_watchlist  # noqa: E402

HERE = os.path.dirname(__file__)
WATCHLIST = open(os.path.join(HERE, "..", "watchlist.yml"), encoding="utf-8").read()


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("brand-pulse-tests")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def synthetic_bronze(spark):
    rng = random.Random(7)
    t0 = datetime(2026, 11, 22, 20, 0)
    filler = ["what a day", "coffee time", "reading a book", "new song out", "weather is nice", "bonjour tout le monde"]
    rows = []
    n = 0
    for minute in range(90):
        for _ in range(40):  # 40 posts a minute of background
            n += 1
            text = rng.choice(filler)
            if rng.random() < 0.01:
                text = "flying air canada tomorrow, excited"
            rows.append((f"at://did:plc:{n}/app.bsky.feed.post/{n}", f"did:plc:{n}", str(n), text, ["en"], False, None,
                         t0 + timedelta(minutes=minute, seconds=rng.randint(0, 59))))
        if 80 <= minute < 85:  # an outage story breaks
            for _ in range(8):
                n += 1
                rows.append((f"at://did:plc:{n}/app.bsky.feed.post/{n}", f"did:plc:{n}", str(n),
                              "Air Canada system outage, flights cancelled, terrible, stuck at the airport", ["en"],
                              False, None, t0 + timedelta(minutes=minute, seconds=rng.randint(0, 59))))
        if minute % 3 == 0:  # a deal bot that must not count as Air Canada conversation
            n += 1
            rows.append((f"at://did:plc:{n}/app.bsky.feed.post/{n}", f"did:plc:{n}", str(n),
                         "Air Canada luggage tag only $9.99 (Reg $19.99) amzn.to/x", ["en"], False, None,
                         t0 + timedelta(minutes=minute, seconds=5)))
    # a duplicate delivery, as Kafka can produce
    rows.append(rows[-1])
    return spark.createDataFrame(rows, "uri string, did string, rkey string, text string, langs array<string>, "
                                       "is_reply boolean, created_at string, event_time timestamp")


def test_pipeline_finds_the_outage_and_nothing_else(spark):
    bronze = synthetic_bronze(spark)
    silver = sj.build_silver(bronze, WATCHLIST).cache()
    assert set(r.brand for r in silver.select("brand").distinct().collect()) == {"Air Canada"}

    names = [b.name for b in load_watchlist(WATCHLIST)]
    platform = sj.platform_windows(bronze)
    gold = sj.brand_windows(silver, platform, names)
    assert gold.count() == len(names) * platform.count()  # zero-filled grid

    scored = sj.score_windows(gold).cache()
    alerts = scored.where("is_alert").collect()
    assert alerts, "the outage should alert"
    assert {a.brand for a in alerts} == {"Air Canada"}
    assert all(a.window_start.hour == 21 and a.window_start.minute >= 20 for a in alerts)
    assert any(a.label == "negative spike" for a in alerts)


def test_kafka_start_offsets_resume_one_past_the_last_offset(spark):
    import json

    bronze = spark.createDataFrame([(0, 10), (0, 12), (2, 5)], "kafka_partition int, kafka_offset long")
    got = json.loads(sj.kafka_start_offsets(bronze, "bluesky.posts", 3))
    assert got == {"bluesky.posts": {"0": 13, "1": -2, "2": 6}}
    assert sj.kafka_start_offsets(None, "t", 3) == "earliest"
