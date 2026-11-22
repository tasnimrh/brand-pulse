import os

import pytest

from brandpulse.match import load_watchlist, match_brands

WATCHLIST = load_watchlist(os.path.join(os.path.dirname(__file__), "..", "watchlist.yml"))


@pytest.mark.parametrize(
    "text, expected",
    [
        ("Stuck at YUL again, thanks Air Canada", ["Air Canada"]),
        ("@aircanada where is my bag", ["Air Canada"]),
        ("#AirCanada strike update", ["Air Canada"]),
        ("air-canada lost my luggage", ["Air Canada"]),
        ("Hydro-Québec outage in Rosemont", ["Hydro-Québec"]),
        ("hydro quebec says power back by 9", ["Hydro-Québec"]),
        ("RBC app is down", ["RBC"]),
        ("TD Bank app is down", ["TD Bank"]),
        ("Uber Eats charged me twice", ["Uber Eats"]),
        ("Took an Uber, then ordered Uber Eats", ["Uber", "Uber Eats"]),
    ],
)
def test_true_mentions(text, expected):
    assert sorted(match_brands(text, WATCHLIST)) == sorted(expected)


@pytest.mark.parametrize(
    "text",
    [
        "Won't you be my neighbour? Fred Rogers was right",   # not Rogers Communications
        "Saved by the bell",                                   # not Bell Canada
        "Huge TD in the fourth quarter",                       # not TD Bank
        "rbc count was low at my checkup",                     # red blood cells, lowercase
        "Nikola Tesla invented so much",                       # the inventor, not the company
        "Deforestation in the Amazon rainforest",              # the forest
        "the bluesky app is slow",
        "Shopifyy is not a word",
    ],
)
def test_false_positives_are_rejected(text):
    assert match_brands(text, WATCHLIST) == []


def test_exclude_only_removes_the_excluded_phrase():
    # The inventor is excluded, but a second, real mention of the company still counts.
    assert match_brands("Nikola Tesla would hate what Tesla did today", WATCHLIST) == ["Tesla"]
