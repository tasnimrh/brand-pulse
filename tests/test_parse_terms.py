from brandpulse.parse import parse_event, post_url
from brandpulse.terms import surging_terms

V1 = {
    "did": "did:plc:abc",
    "time_us": 1732300000000000,
    "kind": "commit",
    "commit": {
        "operation": "create",
        "collection": "app.bsky.feed.post",
        "rkey": "3kxyz",
        "record": {"text": "Air Canada cancelled my flight", "langs": ["en"], "createdAt": "2026-11-22T23:00:00Z"},
    },
}

V2 = {
    "$type": "message",
    "payload": {
        "$type": "network.bsky.jetstream.subscribeEvents#commit",
        "did": "did:plc:abc",
        "seq": 42,
        "time": "2026-11-22T23:00:00.5Z",
        "operation": "create",
        "collection": "app.bsky.feed.post",
        "rkey": "3kxyz",
        "record": {"text": "Air Canada cancelled my flight", "createdAt": "2026-11-22T23:00:00Z", "reply": {}},
    },
}


def test_parse_v1_and_v2_agree():
    a, b = parse_event(V1), parse_event(V2)
    assert a["uri"] == b["uri"] == "at://did:plc:abc/app.bsky.feed.post/3kxyz"
    assert a["text"] == b["text"]
    assert a["cursor"] == 1732300000000000 and b["cursor"] == 42
    assert b["is_reply"] and not a["is_reply"]
    assert b["event_us"] is not None


def test_parse_skips_likes_deletes_and_identity():
    like = {**V1, "commit": {**V1["commit"], "collection": "app.bsky.feed.like"}}
    delete = {**V1, "commit": {"operation": "delete", "collection": "app.bsky.feed.post", "rkey": "x"}}
    identity = {"did": "did:plc:abc", "kind": "identity", "identity": {}}
    assert parse_event(like) is None
    assert parse_event(delete) is None
    assert parse_event(identity) is None


def test_post_url():
    assert post_url("did:plc:abc", "3kxyz") == "https://bsky.app/profile/did:plc:abc/post/3kxyz"


def test_surging_terms_finds_the_story():
    baseline = ["love the new menu", "coffee was fine today", "drive thru line long", "great donuts"] * 10
    window = ["outage at every store, card payments down", "payments down again", "card payments failing outage",
              "outage outage", "payments not working"] * 2
    terms = surging_terms(window, baseline, {"tim", "hortons"})
    assert "payments" in terms and "outage" in terms
