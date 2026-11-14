"""Raft tests on a simulated network: no sockets, no sleeping, fixed random seeds."""
import random

from kv_store import KVStore
from raft import RaftNode, Storage


class Cluster:
    def __init__(self, n=3, seed=1, tmp_path=None, drop_rate=0.0):
        self.ids = list(range(1, n + 1))
        self.tmp_path = tmp_path
        self.rng = random.Random(seed)
        self.drop_rate = drop_rate
        self.down = set()
        self.cut = set()          # nodes on the far side of a partition
        self.queue = []
        self.leaders_by_term = {}
        self.nodes = {i: self._make(i) for i in self.ids}

    def _make(self, i):
        storage = Storage(str(self.tmp_path / f"node{i}.json")) if self.tmp_path else None
        return RaftNode(i, [p for p in self.ids if p != i], KVStore(), storage, random.Random(self.rng.random()))

    def _blocked(self, a, b):
        return a in self.down or b in self.down or (a in self.cut) != (b in self.cut)

    def step(self):
        for i in self.ids:
            if i not in self.down:
                self.queue += self.nodes[i].tick()
        batch, self.queue = self.queue, []
        for m in batch:
            if self._blocked(m["from"], m["to"]) or self.rng.random() < self.drop_rate:
                continue
            self.queue += self.nodes[m["to"]].handle(m)
        for i in self.ids:  # election safety: never two leaders in one term
            node = self.nodes[i]
            if i not in self.down and node.role == "leader":
                assert self.leaders_by_term.setdefault(node.term, i) == i

    def run(self, steps=100):
        for _ in range(steps):
            self.step()

    def leader(self, among=None):
        live = [self.nodes[i] for i in (among or self.ids) if i not in self.down and self.nodes[i].role == "leader"]
        return max(live, key=lambda n: n.term) if live else None

    def wait_leader(self, among=None):
        for _ in range(300):
            self.step()
            if self.leader(among):
                return self.leader(among)
        raise AssertionError("no leader elected")

    def put(self, key, value, among=None):
        leader = self.wait_leader(among)
        index = leader.propose({"op": "put", "key": key, "value": value})
        self.run(30)
        return leader, index

    def crash(self, i):
        self.down.add(i)

    def restart(self, i):
        self.nodes[i] = self._make(i)  # fresh memory, state reloaded from disk
        self.down.discard(i)

    def stores(self):
        return {i: self.nodes[i].state_machine.store for i in self.ids if i not in self.down}


def test_elects_one_leader():
    c = Cluster(5)
    leader = c.wait_leader()
    c.run(50)
    assert [n.role for n in c.nodes.values()].count("leader") == 1
    assert all(n.leader_id == leader.id and n.term == leader.term for n in c.nodes.values())


def test_replicates_to_every_node():
    c = Cluster(3)
    leader, index = c.put("name", "Priyanka")
    assert leader.results[index] == (True, None)
    assert c.stores() == {1: {"name": "Priyanka"}, 2: {"name": "Priyanka"}, 3: {"name": "Priyanka"}}
    read = leader.propose({"op": "get", "key": "name"})
    c.run(20)
    assert leader.results[read] == (True, "Priyanka")


def test_follower_rejects_client_commands():
    c = Cluster(3)
    leader = c.wait_leader()
    follower = next(n for n in c.nodes.values() if n is not leader)
    assert follower.propose({"op": "put", "key": "a", "value": 1}) is None


def test_new_leader_after_crash_keeps_committed_data():
    c = Cluster(3)
    old, _ = c.put("a", 1)
    c.crash(old.id)
    new = c.wait_leader()
    assert new.id != old.id and new.term > old.term
    c.put("b", 2)
    c.restart(old.id)
    c.run(100)
    assert c.nodes[old.id].role == "follower"
    assert c.nodes[old.id].state_machine.store == {"a": 1, "b": 2}


def test_no_commit_without_majority():
    c = Cluster(3)
    leader = c.wait_leader()
    for i in c.ids:
        if i != leader.id:
            c.crash(i)
    index = leader.propose({"op": "put", "key": "a", "value": 1})
    c.run(100)
    assert index not in leader.results and leader.state_machine.store == {}


def test_partitioned_leader_loses_its_uncommitted_entries():
    c = Cluster(5)
    old, _ = c.put("a", 1)
    c.cut = {old.id}                                         # old leader alone on one side
    lost = old.propose({"op": "put", "key": "a", "value": "stale"})
    others = [i for i in c.ids if i != old.id]
    c.put("a", 2, among=others)                              # majority side moves on
    assert old.role == "leader" and old.commit_index < lost  # still thinks it leads, commits nothing
    c.cut = set()
    c.run(150)
    assert old.role == "follower"
    assert old.results[lost][0] is False                     # the client is told its write was lost
    logs = [n.log for n in c.nodes.values()]
    assert all(log == logs[0] for log in logs)               # conflicting entries were overwritten
    assert all(store == {"a": 2} for store in c.stores().values())


def test_state_survives_restart_of_every_node(tmp_path):
    c = Cluster(3, tmp_path=tmp_path)
    leader, _ = c.put("a", 1)
    c.put("b", 2)
    term, log = leader.term, list(leader.log)
    for i in c.ids:
        c.crash(i)
    for i in c.ids:
        c.restart(i)
    assert all(n.term == term and n.log == log for n in c.nodes.values())
    assert all(n.state_machine.store == {} for n in c.nodes.values())  # nothing applied until re-committed
    c.wait_leader()
    c.run(50)
    assert all(store == {"a": 1, "b": 2} for store in c.stores().values())


def test_stale_node_cannot_become_leader():
    c = Cluster(3)
    leader = c.wait_leader()
    behind = next(i for i in c.ids if i != leader.id)
    c.crash(behind)
    c.put("a", 1)
    c.crash(leader.id)
    c.restart(behind)          # no disk in this test, so it comes back with an empty log
    new = c.wait_leader()
    c.run(50)
    assert new.id != behind    # its vote requests are refused; the node that has the data wins
    assert c.nodes[behind].state_machine.store == {"a": 1}


def test_converges_with_message_loss():
    for seed in range(5):
        c = Cluster(5, seed=seed, drop_rate=0.2)
        for k in range(10):
            for _ in range(20):                 # a real client retries until its write is confirmed
                leader, index = c.put(f"k{k}", k)
                if leader.results.get(index, (False,))[0]:
                    break
            else:
                raise AssertionError("write never committed")
        c.drop_rate = 0
        c.run(100)
        expected = {f"k{k}": k for k in range(10)}
        assert all(store == expected for store in c.stores().values()), seed
