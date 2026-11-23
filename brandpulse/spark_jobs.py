"""Spark transformations for Brand Pulse. Pure functions of DataFrames, so they run the
same on Databricks and in local tests.

bronze  every post from Kafka, decoded
silver  one row per (post, brand mentioned), with sentiment
gold    per brand, per window: mentions, negative share, platform volume (zero-filled)
alerts  windows where a brand spikes against its own share-of-voice baseline
"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from brandpulse.anomaly import Rules, score_brand
from brandpulse.match import PROMO_PATTERN, load_watchlist, match_brands

NEG, POS = -0.05, 0.05  # VADER's standard cut-offs on the compound score


def match_udf(watchlist_yaml: str):
    @F.pandas_udf(T.ArrayType(T.StringType()))
    def _match(texts: pd.Series) -> pd.Series:
        brands = load_watchlist(watchlist_yaml)
        return texts.apply(lambda t: match_brands(t or "", brands))

    return _match


@F.pandas_udf(T.DoubleType())
def vader_compound(texts: pd.Series, langs: pd.Series) -> pd.Series:
    """VADER compound score for English posts; None for other languages.

    VADER is a lexicon built for English social media. Scoring French with it would
    produce confident nonsense, so French posts count toward volume but not sentiment.
    """
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    sia = SentimentIntensityAnalyzer()

    def score(text, lang_list):
        lang_list = list(lang_list) if lang_list is not None else []
        if lang_list and not any(str(l).lower().startswith("en") for l in lang_list):
            return None
        return float(sia.polarity_scores(text or "")["compound"])

    return pd.Series([score(t, l) for t, l in zip(texts, langs)])


def build_silver(bronze: DataFrame, watchlist_yaml: str) -> DataFrame:
    matched = bronze.withColumn("brands", match_udf(watchlist_yaml)(F.col("text")))
    return (
        matched.where(F.size("brands") > 0)
        .withColumn("brand", F.explode("brands"))
        .withColumn("compound", vader_compound(F.col("text"), F.col("langs")))
        .withColumn(
            "sentiment",
            F.when(F.col("compound").isNull(), F.lit(None))
            .when(F.col("compound") <= NEG, "negative")
            .when(F.col("compound") >= POS, "positive")
            .otherwise("neutral"),
        )
        .select("uri", "did", "rkey", "brand", "text", "langs", "is_reply", "event_time", "compound", "sentiment",
                *(["kafka_time"] if "kafka_time" in bronze.columns else []))
    )


def kafka_start_offsets(bronze: DataFrame | None, topic: str, partitions: int) -> str:
    """Where the next incremental Kafka read should begin, as Spark's startingOffsets JSON.

    Each partition resumes one past the highest offset already in bronze, so every record
    is read exactly once across runs. Partitions bronze has never seen start from the
    earliest retained offset (-2).
    """
    import json

    if bronze is None or "kafka_offset" not in bronze.columns:
        return "earliest"
    rows = (bronze.where(F.col("kafka_offset").isNotNull())
            .groupBy("kafka_partition").agg(F.max("kafka_offset").alias("m")).collect())
    seen = {int(r.kafka_partition): int(r.m) + 1 for r in rows}
    return json.dumps({topic: {str(p): seen.get(p, -2) for p in range(partitions)}})


def platform_windows(bronze: DataFrame, minutes: int = 5) -> DataFrame:
    return (
        bronze.dropDuplicates(["uri"])
        .groupBy(F.window("event_time", f"{minutes} minutes").alias("w"))
        .agg(F.count("*").alias("platform_posts"))
        .select(F.col("w.start").alias("window_start"), "platform_posts")
    )


def organic(silver: DataFrame) -> DataFrame:
    """Mentions that are conversation, not deal bots or affiliate ads."""
    return silver.where(~F.col("text").rlike(PROMO_PATTERN))


def brand_windows(silver: DataFrame, platform: DataFrame, brand_names: list[str], minutes: int = 5) -> DataFrame:
    spark = silver.sparkSession
    counts = (
        organic(silver).dropDuplicates(["uri", "brand"])
        .groupBy("brand", F.window("event_time", f"{minutes} minutes").alias("w"))
        .agg(
            F.count("*").alias("mentions"),
            F.count("compound").alias("scored"),
            F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("neg"),
            F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("pos"),
            F.avg("compound").alias("avg_compound"),
        )
        .select("brand", F.col("w.start").alias("window_start"), "mentions", "scored", "neg", "pos", "avg_compound")
    )
    # Zero-fill: a brand with no posts in a window is a real zero, and the baseline needs it.
    brands = spark.createDataFrame([(b,) for b in brand_names], "brand string")
    grid = brands.crossJoin(platform)
    return (
        grid.join(counts, ["brand", "window_start"], "left")
        .fillna(0, subset=["mentions", "scored", "neg", "pos"])
        .select("brand", "window_start", "platform_posts", "mentions", "scored", "neg", "pos", "avg_compound")
    )


ALERT_SCHEMA = (
    "brand string, window_start timestamp, platform_posts long, mentions long, scored long, neg long, pos long, "
    "avg_compound double, expected double, ratio double, p_value double, history int, neg_share double, "
    "base_neg_share double, is_alert boolean, label string"
)


def score_windows(gold: DataFrame, rules: Rules = Rules()) -> DataFrame:
    cols = [c.split()[0] for c in ALERT_SCHEMA.split(", ")]

    def _score(pdf: pd.DataFrame) -> pd.DataFrame:
        out = score_brand(pdf, rules)
        return out[cols]

    return gold.groupBy("brand").applyInPandas(_score, schema=ALERT_SCHEMA)
