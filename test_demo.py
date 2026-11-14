"""The cluster behind the demo page, stepped by hand (the web app's timer is not involved)."""
from demo import Cluster


def settle(c, steps=80):
    for _ in range(steps):
        c.step()


def test_demo_cluster_elects_reports_and_recovers():
    c = Cluster(seed=7)
    settle(c)
    leader = c.leader()
    assert leader and c.state()["leader"] == leader.id
    assert any("won the election" in e["text"] for e in c.events)

    index = leader.propose({"op": "put", "key": "city", "value": "Delhi"})
    settle(c, 20)
    assert leader.results[index] == (True, None)
    assert all(n["store"] == {"city": "Delhi"} for n in c.state()["nodes"])
    assert any("Saved city = Delhi" in e["text"] for e in c.events)

    c.toggle(leader.id)                      # stop the leader
    settle(c)
    assert c.leader().id != leader.id
    stopped = next(n for n in c.state()["nodes"] if n["id"] == leader.id)
    assert stopped["role"] == "stopped" and stopped["store"] == {}

    c.toggle(leader.id)                      # restart: the log is reloaded, then re-applied
    settle(c)
    back = next(n for n in c.state()["nodes"] if n["id"] == leader.id)
    assert back["role"] == "follower" and back["store"] == {"city": "Delhi"}


def test_cut_off_leader_cannot_save():
    c = Cluster(seed=3)
    settle(c)
    old = c.leader()
    c.isolate(old.id)
    index = old.propose({"op": "put", "key": "k", "value": "lost"})
    settle(c)
    assert index not in old.results and c.leader().id != old.id
    c.isolate(old.id)                        # reconnect
    settle(c)
    assert old.results[index][0] is False
    assert any("dropped 1 unconfirmed entry" in e["text"] for e in c.events)


def test_messages_are_recorded_for_the_page():
    c = Cluster(seed=5)
    settle(c)
    kinds = {m["kind"] for m in c.sent}
    assert {"vote_request", "vote", "heartbeat", "entries"} <= kinds     # an election, then a no-op entry, then heartbeats
    seq = c.state()["seq"]
    assert c.state()["messages"] == []                                   # first call skips the backlog
    settle(c, 6)
    new = c.state(since=seq)["messages"]
    assert new and all(m["seq"] > seq and m["kind"] == "heartbeat" and m["arrives"] for m in new)

    leader = c.leader().id
    c.isolate(leader)
    settle(c, 6)
    assert any(not m["arrives"] and m["from"] == leader for m in c.sent)  # its heartbeats now go nowhere


def test_restore_brings_every_server_back():
    c = Cluster(seed=2)
    settle(c)
    c.toggle(1)
    c.toggle(2)
    c.isolate(3)
    c.restore()
    settle(c)
    state = c.state()
    assert all(n["up"] and not n["cut"] for n in state["nodes"]) and state["leader"]
