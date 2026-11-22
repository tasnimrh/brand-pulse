import pandas as pd

from brandpulse.anomaly import Rules, poisson_sf, score_brand


def frame(mentions, platform, neg=None, scored=None):
    n = len(mentions)
    return pd.DataFrame(
        {
            "brand": "X",
            "window_start": pd.date_range("2026-11-22 20:00", periods=n, freq="5min"),
            "mentions": mentions,
            "platform_posts": platform,
            "neg": neg if neg is not None else [0] * n,
            "scored": scored if scored is not None else mentions,
        }
    )


def test_poisson_sf_matches_known_values():
    assert abs(poisson_sf(1, 1.0) - (1 - 0.36787944)) < 1e-6
    assert poisson_sf(0, 3.0) == 1.0
    assert poisson_sf(20, 2.0) < 1e-10


def test_quiet_brand_never_alerts():
    out = score_brand(frame([2, 1, 3, 2, 2, 1, 2, 3, 2, 2], [10000] * 10))
    assert not out["is_alert"].any()


def test_real_spike_alerts():
    out = score_brand(frame([2, 1, 3, 2, 2, 1, 2, 3, 30], [10000] * 9))
    assert out["is_alert"].iloc[-1]
    assert out["label"].iloc[-1] == "volume spike"


def test_platform_surge_does_not_alert():
    # Every brand doubles when the whole platform doubles. Share of voice stays flat.
    out = score_brand(frame([5, 5, 6, 5, 4, 5, 6, 5, 12], [10000] * 8 + [24000]))
    assert not out["is_alert"].iloc[-1]


def test_negative_spike_label():
    mentions = [4] * 8 + [40]
    neg = [1] * 8 + [30]
    out = score_brand(frame(mentions, [10000] * 9, neg=neg, scored=mentions))
    assert out["label"].iloc[-1] == "negative spike"


def test_needs_history_before_alerting():
    out = score_brand(frame([0, 40], [10000, 10000]), Rules())
    assert not out["is_alert"].any()
