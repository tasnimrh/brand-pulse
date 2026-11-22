"""Brand matching with explicit handling of ambiguous names.

A naive substring match on "Rogers" counts Fred Rogers; on "TD" it counts touchdowns.
So aliases match only as whole words (case-insensitive), short acronyms match only in
capitals, and exclude patterns remove a post from a brand before it is counted.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import yaml

# A word boundary that also respects @handles, #tags and accented letters.
_LEFT = r"(?<![\w@#])"
_RIGHT = r"(?![\w])"


def _alias_pattern(alias: str) -> str:
    a = alias.strip()
    prefix = ""
    if a[:1] in "@#":
        prefix, a = re.escape(a[0]), a[1:]
    body = r"[\s\-]+".join(re.escape(part) for part in re.split(r"[\s\-]+", a))
    # Allow the same alias as a hashtag or handle: #AirCanada, @aircanada.
    lead = prefix if prefix else r"[@#]?"
    return lead + body


@dataclass
class Brand:
    name: str
    sector: str = ""
    ticker: str | None = None
    aliases: list[str] = field(default_factory=list)
    case_sensitive: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        parts = [_alias_pattern(a) for a in self.aliases]
        self._ci = re.compile(_LEFT + "(?:" + "|".join(parts) + ")" + _RIGHT, re.IGNORECASE) if parts else None
        cs = [_alias_pattern(a) for a in self.case_sensitive]
        self._cs = re.compile(_LEFT + "(?:" + "|".join(cs) + ")" + _RIGHT) if cs else None
        ex = [_alias_pattern(a) for a in self.exclude]
        self._ex = re.compile(_LEFT + "(?:" + "|".join(ex) + ")" + _RIGHT, re.IGNORECASE) if ex else None

    def matches(self, text: str) -> bool:
        hit = bool(self._ci and self._ci.search(text)) or bool(self._cs and self._cs.search(text))
        if not hit:
            return False
        if self._ex is None:
            return True
        # Drop the post only if every alias hit sits inside an excluded phrase.
        masked = self._ex.sub(" ", text)
        return bool(self._ci and self._ci.search(masked)) or bool(self._cs and self._cs.search(masked))


def load_watchlist(path_or_text: str) -> list[Brand]:
    if "\n" in path_or_text or path_or_text.lstrip().startswith("brands"):
        data = yaml.safe_load(path_or_text)
    else:
        with open(path_or_text, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    return [Brand(**b) for b in data["brands"]]


def match_brands(text: str, brands: list[Brand]) -> list[str]:
    return [b.name for b in brands if b.matches(text)]
