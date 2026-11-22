"""Explain an alert with the words that surged, not just the count.

Weighted log-odds with an informative Dirichlet prior (Monroe, Colaresi and Quinn, 2008,
"Fightin' Words"): compares word use in the alert window with the same brand's baseline
posts, and ranks by a z-score so rare one-off words do not dominate.
"""
from __future__ import annotations

import math
import re
from collections import Counter

STOP = set(
    """a an and are as at be but by for from has have i if in is it its just me my not of on or our so
    that the their them there they this to was we were what when who will with you your yours im it's
    i'm don't dont can't cant do does did been being would could should about after again all also am
    any because before more most no now only out over same some such than then these those through too
    very via how why where which while here get got go going one two new like up down off still even
    https http www com bsky app le la les des et en un une du de pour pas que qui est sur au aux ce
    cette dans par plus avec""".split()
)
TOKEN = re.compile(r"[#@]?[a-zà-ÿ][a-zà-ÿ0-9'\-]{2,}", re.IGNORECASE)


def tokens(text: str, drop: set[str]) -> list[str]:
    out = []
    for t in TOKEN.findall(text.lower()):
        t = t.strip("'-")
        if t in STOP or t in drop or t.lstrip("#@") in drop:
            continue
        out.append(t)
    return out


def surging_terms(window_texts: list[str], baseline_texts: list[str], brand_words: set[str],
                  k: int = 5, min_count: int = 3, prior: float = 1.0) -> list[str]:
    drop = {w.lower() for w in brand_words}
    # Count each word once per post, so one long rant cannot carry the explanation.
    a = Counter(w for t in window_texts for w in set(tokens(t, drop)))
    b = Counter(w for t in baseline_texts for w in set(tokens(t, drop)))
    vocab = set(a) | set(b)
    if not vocab:
        return []
    alpha0 = prior * len(vocab)
    na, nb = sum(a.values()), sum(b.values())
    scored = []
    for w in vocab:
        if a[w] < min_count:
            continue
        aw = bw = prior
        la = math.log((a[w] + aw) / (na + alpha0 - a[w] - aw))
        lb = math.log((b[w] + bw) / (nb + alpha0 - b[w] - bw))
        var = 1.0 / (a[w] + aw) + 1.0 / (b[w] + bw)
        scored.append(((la - lb) / math.sqrt(var), w))
    scored.sort(reverse=True)
    return [w for z, w in scored[:k] if z > 1.96]
