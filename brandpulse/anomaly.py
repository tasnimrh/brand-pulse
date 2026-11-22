"""Decide when a brand is "having a bad day".

Each brand is scored on fixed windows (5 minutes by default) against its own trailing
baseline. Two things keep the alerts honest:

1. Share of voice, not raw counts. Bluesky's total volume swings by time of day, so the
   expected mentions in a window are the brand's trailing *rate per platform post* times
   the platform volume in that window. A global surge does not fire every brand at once.
2. A Poisson test plus floors. A window alerts only if it has enough mentions, beats the
   expected count by a clear ratio, and is unlikely under the baseline (p < alpha).

A volume spike is labelled "negative" when the share of negative posts also jumps
above the brand's own baseline.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Rules:
    history_windows: int = 12          # 12 x 5 min = trailing hour
    min_history_windows: int = 6       # need at least 30 min of baseline
    min_mentions: int = 8
    min_ratio: float = 3.0
    alpha: float = 1e-3
    neg_jump: float = 0.15             # +15 points of negative share
    min_scored: int = 6                # posts with a sentiment score, for the negative label
    prior_mentions: float = 0.5        # smoothing so a quiet brand never has expected = 0


def poisson_sf(k: int, lam: float) -> float:
    """P(X >= k) for X ~ Poisson(lam), stable for the small counts used here."""
    if k <= 0:
        return 1.0
    if lam <= 0:
        return 0.0
    # 1 - P(X <= k-1), summed in log space
    log_terms = [i * math.log(lam) - lam - math.lgamma(i + 1) for i in range(k)]
    m = max(log_terms)
    cdf = math.exp(m) * sum(math.exp(t - m) for t in log_terms)
    return max(0.0, min(1.0, 1.0 - cdf))


def score_brand(g: pd.DataFrame, rules: Rules = Rules()) -> pd.DataFrame:
    """Score one brand's windows.

    Expects columns: brand, window_start, mentions, scored, neg, platform_posts,
    with one row per window (zero-filled), sorted or not.
    Returns the same rows plus expected, ratio, p_value, neg_share, base_neg_share,
    history, is_alert, label.
    """
    g = g.sort_values("window_start").reset_index(drop=True).copy()
    hist = rules.history_windows
    m_sum = g["mentions"].shift(1).rolling(hist, min_periods=1).sum()
    p_sum = g["platform_posts"].shift(1).rolling(hist, min_periods=1).sum()
    n_hist = g["mentions"].shift(1).rolling(hist, min_periods=1).count()
    s_sum = g["scored"].shift(1).rolling(hist, min_periods=1).sum()
    neg_sum = g["neg"].shift(1).rolling(hist, min_periods=1).sum()

    rate = (m_sum.fillna(0) + rules.prior_mentions) / p_sum.replace(0, np.nan).fillna(1)
    g["expected"] = (rate * g["platform_posts"]).astype(float).clip(lower=0.25)
    g["ratio"] = g["mentions"] / g["expected"]
    g["p_value"] = [poisson_sf(int(k), float(l)) for k, l in zip(g["mentions"], g["expected"])]
    g["history"] = n_hist.fillna(0).astype(int)
    g["neg_share"] = (g["neg"] / g["scored"].replace(0, np.nan)).astype(float)
    g["base_neg_share"] = (neg_sum / s_sum.replace(0, np.nan)).astype(float)

    spike = (
        (g["history"] >= rules.min_history_windows)
        & (g["mentions"] >= rules.min_mentions)
        & (g["ratio"] >= rules.min_ratio)
        & (g["p_value"] < rules.alpha)
    )
    negative = (
        spike
        & (g["scored"] >= rules.min_scored)
        & ((g["neg_share"] - g["base_neg_share"].fillna(0)) >= rules.neg_jump)
    )
    g["is_alert"] = spike
    g["label"] = "none"
    g.loc[spike, "label"] = "volume spike"
    g.loc[negative, "label"] = "negative spike"
    return g
