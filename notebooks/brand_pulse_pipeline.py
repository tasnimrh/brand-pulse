# Databricks notebook source
# MAGIC %md
# MAGIC # Brand Pulse: which brands are having a bad day, and why
# MAGIC
# MAGIC Every new public Bluesky post flows from a laptop producer into Kafka (Confluent Cloud).
# MAGIC This notebook lands it in Delta, finds brand mentions, scores sentiment, and flags windows
# MAGIC where a brand spikes against its own share-of-voice baseline. Each alert comes with the
# MAGIC words that surged and links to the posts behind it.
# MAGIC
# MAGIC | Layer | Table | What it holds |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze_posts` | every post, decoded from Avro, with its Kafka partition and offset |
# MAGIC | Silver | `silver_mentions` | one row per post and brand, with VADER sentiment |
# MAGIC | Gold | `gold_brand_windows` | per brand, per 5-minute window, zero-filled, with platform volume |
# MAGIC | Alerts | `alerts` | scored windows, and `alerts_explained` with surging words and evidence |
# MAGIC
# MAGIC Runs on Databricks Free Edition serverless as incremental batches: each run reads Kafka from
# MAGIC the offsets bronze already holds up to the latest, so every record is read exactly once and the
# MAGIC run always ends. (Structured Streaming with `availableNow` was the first design; on serverless the
# MAGIC queries kept running after catching up, so a scheduled job could never finish.) Schedule the
# MAGIC notebook as a job every 10 minutes for a near-live view.

# COMMAND ----------

# MAGIC %pip install vaderSentiment==3.3.2 pyyaml --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

# This notebook lives in a Git folder: <repo>/notebooks/. Put <repo> on the path.
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import cloudpickle
import brandpulse

# Ship the brandpulse package to the workers inside each UDF, by value.
cloudpickle.register_pickle_by_value(brandpulse)

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from brandpulse import spark_jobs as sj
from brandpulse.anomaly import Rules
from brandpulse.match import load_watchlist
from brandpulse.parse import post_url
from brandpulse.terms import surging_terms

print("repo:", REPO_ROOT)



# COMMAND ----------

# ---- Settings (endpoints are notebook parameters; credentials come from the secret scope) ----
import json

CATALOG, SCHEMA = "workspace", "brand_pulse"
WINDOW_MINUTES = 5
RULES = Rules()  # thresholds live in brandpulse/anomaly.py and are unit-tested
SECRET_SCOPE = "confluent"   # keys: api_key, api_secret

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.state")
STATE = f"/Volumes/{CATALOG}/{SCHEMA}/state"
CHK = f"{STATE}/checkpoints"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

# The bootstrap server is configuration, not a secret. Enter it once in the parameter box;
# it is remembered in the volume, so later runs, pulls and scheduled jobs need no input.
dbutils.widgets.text("bootstrap_servers", "", "Kafka bootstrap server")
dbutils.widgets.text("topic", "bluesky.posts", "Kafka topic")
CONFIG = f"{STATE}/config.json"
saved = json.load(open(CONFIG)) if os.path.exists(CONFIG) else {}
BOOTSTRAP_SERVERS = dbutils.widgets.get("bootstrap_servers").strip() or saved.get("bootstrap_servers", "")
TOPIC = dbutils.widgets.get("topic").strip() or saved.get("topic", "bluesky.posts")
assert BOOTSTRAP_SERVERS.endswith(":9092"), "Enter the Kafka bootstrap server in the Parameters panel once."
json.dump({"bootstrap_servers": BOOTSTRAP_SERVERS, "topic": TOPIC}, open(CONFIG, "w"))

scopes = {s.name for s in dbutils.secrets.listScopes()}
assert SECRET_SCOPE in scopes, f"Create the secret scope '{SECRET_SCOPE}' with keys api_key and api_secret first."
keys = {k.key for k in dbutils.secrets.list(SECRET_SCOPE)}
assert {"api_key", "api_secret"} <= keys, f"Scope '{SECRET_SCOPE}' needs keys api_key and api_secret; found {sorted(keys)}."

KAFKA_API_KEY = dbutils.secrets.get(SECRET_SCOPE, "api_key")
KAFKA_API_SECRET = dbutils.secrets.get(SECRET_SCOPE, "api_secret")
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";'
    ),
}

AVRO_SCHEMA = open(os.path.join(REPO_ROOT, "schemas", "bluesky_post.avsc")).read()
WATCHLIST_YAML = open(os.path.join(REPO_ROOT, "watchlist.yml"), encoding="utf-8").read()
BRANDS = load_watchlist(WATCHLIST_YAML)
print(f"{len(BRANDS)} brands on the watchlist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze: Kafka to Delta, incremental by offset
# MAGIC Confluent's Avro wire format puts a magic byte and a 4-byte schema id in front of the
# MAGIC Avro body. The producer and this reader share `schemas/bluesky_post.avsc`, so the header is
# MAGIC checked and stripped, then the body decoded. Each record keeps its Kafka partition and offset,
# MAGIC and the next run starts one past the highest offset per partition.

# COMMAND ----------

PARTITIONS = 6
bronze_exists = spark.catalog.tableExists(T("bronze_posts"))
existing = spark.table(T("bronze_posts")) if bronze_exists else None
start = sj.kafka_start_offsets(existing, TOPIC, PARTITIONS)
first_load = start == "earliest"
print("starting offsets:", start)

raw = (
    spark.read.format("kafka")
    .options(**KAFKA_OPTIONS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", start)
    .option("endingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

new_posts = (
    raw.where(F.expr("substring(value, 1, 1)") == F.lit(bytearray(b"\x00")))
    .select(
        from_avro(F.expr("substring(value, 6, length(value) - 5)"), AVRO_SCHEMA, {"mode": "PERMISSIVE"}).alias("p"),
        F.col("timestamp").alias("kafka_time"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    .where(F.col("p").isNotNull())
    .select("p.*", "kafka_time", "kafka_partition", "kafka_offset")
)

# First load (or a bronze table from before offsets were tracked): rebuild from the topic.
writer = new_posts.write.format("delta").option("mergeSchema", "true")
(writer.mode("overwrite").option("overwriteSchema", "true") if first_load else writer.mode("append")).saveAsTable(T("bronze_posts"))
print("bronze rows:", spark.table(T("bronze_posts")).count(), "| first load" if first_load else "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver: brand mentions with sentiment
# MAGIC Matching handles ambiguous names on purpose: "Rogers" only counts as Rogers Communications in
# MAGIC unambiguous forms, "TD" only as TD Bank, "RBC" only in capitals. The cases are unit-tested in
# MAGIC `tests/test_match.py`.

# COMMAND ----------

bronze_all = spark.table(T("bronze_posts"))
rebuild = first_load or not spark.catalog.tableExists(T("silver_mentions")) \
    or "kafka_time" not in spark.table(T("silver_mentions")).columns
if rebuild:
    todo = bronze_all
else:
    mark = spark.table(T("silver_mentions")).agg(F.max("kafka_time")).first()[0]
    todo = bronze_all.where(F.col("kafka_time") > F.lit(mark)) if mark is not None else bronze_all
silver_new = sj.build_silver(todo, WATCHLIST_YAML)
w = silver_new.write.format("delta").option("mergeSchema", "true")
(w.mode("overwrite").option("overwriteSchema", "true") if rebuild else w.mode("append")).saveAsTable(T("silver_mentions"))
print("silver rows:", spark.table(T("silver_mentions")).count(), "| rebuilt" if rebuild else "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold: brand by window, zero-filled, with platform volume
# MAGIC Recomputed in full each run. It is small (brands x windows), and a full rebuild is simpler and
# MAGIC safer than streaming aggregation with late data.

# COMMAND ----------

bronze_t = spark.table(T("bronze_posts"))
silver_t = spark.table(T("silver_mentions"))
platform = sj.platform_windows(bronze_t, WINDOW_MINUTES)
gold = sj.brand_windows(silver_t, platform, [b.name for b in BRANDS], WINDOW_MINUTES)
gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_brand_windows"))

scored = sj.score_windows(spark.table(T("gold_brand_windows")), RULES)
scored.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("alerts"))

display(
    spark.table(T("alerts")).where("is_alert")
    .select("brand", "window_start", "label", "mentions", F.round("expected", 1).alias("expected"),
            F.round("ratio", 1).alias("ratio"), F.round("neg_share", 2).alias("neg_share"),
            F.round("base_neg_share", 2).alias("base_neg_share"), "p_value")
    .orderBy(F.desc("window_start"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explain each alert: what surged, and the posts behind it

# COMMAND ----------

alias_words = {
    b.name: {w for a in (b.aliases + b.case_sensitive + [b.name]) for w in a.lower().replace("-", " ").lstrip("@#").split()}
    for b in BRANDS
}
alerts = spark.table(T("alerts")).where("is_alert").orderBy(F.desc("window_start")).limit(100).collect()

rows = []
for a in alerts:
    w0 = a.window_start
    w1 = F.lit(w0) + F.expr(f"INTERVAL {WINDOW_MINUTES} MINUTES")
    b0 = F.lit(w0) - F.expr(f"INTERVAL {WINDOW_MINUTES * RULES.history_windows} MINUTES")
    brand_posts = sj.organic(silver_t).where(F.col("brand") == a.brand)
    win = brand_posts.where((F.col("event_time") >= F.lit(w0)) & (F.col("event_time") < w1))
    base = brand_posts.where((F.col("event_time") >= b0) & (F.col("event_time") < F.lit(w0)))
    win_rows = win.select("text", "did", "rkey", "compound").collect()
    base_texts = [r.text for r in base.select("text").collect()]
    terms = surging_terms([r.text for r in win_rows], base_texts, alias_words[a.brand])
    worst = sorted(win_rows, key=lambda r: (r.compound if r.compound is not None else 0))[:3]
    rows.append((a.brand, a.window_start, a.label, int(a.mentions), float(a.expected), float(a.ratio),
                 a.neg_share, ", ".join(terms), [post_url(r.did, r.rkey) for r in worst]))

explained_schema = ("brand string, window_start timestamp, label string, mentions int, expected double, ratio double, "
                    "neg_share double, surging_terms string, evidence array<string>")
spark.createDataFrame(rows, explained_schema).write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(T("alerts_explained"))
display(spark.table(T("alerts_explained")).orderBy(F.desc("window_start")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4b. How much of each brand's "conversation" is deal bots and affiliate ads?
# MAGIC These posts stay in silver for transparency but are left out of gold and alerts.

# COMMAND ----------

from brandpulse.match import PROMO_PATTERN

display(
    spark.table(T("silver_mentions")).dropDuplicates(["uri", "brand"])
    .groupBy("brand")
    .agg(F.count("*").alias("all_mentions"),
         F.sum(F.col("text").rlike(PROMO_PATTERN).cast("int")).alias("promo_or_bot"))
    .withColumn("promo_share", F.round(F.col("promo_or_bot") / F.col("all_mentions"), 2))
    .orderBy(F.desc("all_mentions"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Right now: share of voice and mood, last hour

# COMMAND ----------

display(spark.sql(f"""
  WITH last_hour AS (
    SELECT * FROM {T('gold_brand_windows')}
    WHERE window_start >= (SELECT max(window_start) FROM {T('gold_brand_windows')}) - INTERVAL 60 MINUTES
  )
  SELECT brand,
         sum(mentions) AS mentions,
         round(1e4 * sum(mentions) / sum(platform_posts), 2) AS per_10k_posts,
         round(sum(neg) / nullif(sum(scored), 0), 2) AS negative_share
  FROM last_hour GROUP BY brand HAVING sum(mentions) > 0
  ORDER BY mentions DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Audit: how precise is the brand matching?
# MAGIC Draws a fixed random sample of 100 mentions to label by hand (is this post really about the
# MAGIC brand?). The share of "yes" is the matching precision reported in the README.

# COMMAND ----------

if not spark.catalog.tableExists(T("audit_sample")):
    (spark.table(T("silver_mentions")).dropDuplicates(["uri", "brand"])
     .orderBy(F.rand(seed=2026)).limit(100)
     .select("brand", "text", F.lit(None).cast("boolean").alias("is_about_brand"))
     .write.saveAsTable(T("audit_sample")))
display(spark.table(T("audit_sample")))
