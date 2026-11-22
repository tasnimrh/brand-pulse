-- Queries for a Databricks AI/BI dashboard. Each block is one dataset.

-- 1. Live alerts with the explanation (table widget)
SELECT window_start, brand, label, mentions, round(expected, 1) AS expected, round(ratio, 1) AS x_baseline,
       round(neg_share, 2) AS negative_share, surging_terms, evidence[0] AS top_post
FROM workspace.brand_pulse.alerts_explained
ORDER BY window_start DESC;

-- 2. Share of voice over time per brand (line chart: x = window_start, y = per_10k_posts, series = brand)
SELECT window_start, brand, round(1e4 * mentions / platform_posts, 2) AS per_10k_posts
FROM workspace.brand_pulse.gold_brand_windows
WHERE window_start >= current_timestamp() - INTERVAL 24 HOURS;

-- 3. Mood leaderboard, last 24 hours (bar chart)
SELECT brand, sum(mentions) AS mentions, round(sum(neg) / nullif(sum(scored), 0), 2) AS negative_share
FROM workspace.brand_pulse.gold_brand_windows
WHERE window_start >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY brand HAVING sum(mentions) >= 20
ORDER BY negative_share DESC;

-- 4. Pipeline health: posts per minute reaching Delta
SELECT date_trunc('minute', event_time) AS minute, count(*) AS posts
FROM workspace.brand_pulse.bronze_posts
WHERE event_time >= current_timestamp() - INTERVAL 2 HOURS
GROUP BY 1 ORDER BY 1;
