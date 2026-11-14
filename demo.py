"""A five-node Raft cluster in one process, with a web page to watch it and break it.

    uvicorn demo:app          then open http://localhost:8000

The nodes are the same RaftNode objects that server.py runs. Only the network is simulated here,
so the page can stop nodes and cut them off. Each node's "disk" is kept in memory for the same reason.
"""
import random
import threading
import time
from collections import deque
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from kv_store import KVStore
from raft import RaftNode

SPEEDS = {"normal": 0.12, "slow": 0.5}   # seconds per tick; elections take 10-20 ticks
NODES = 5
MAX_LOG = 150         # the demo has no log compaction, so stop accepting writes at this size


class Disk:
    """Stands in for raft.Storage: survives a node restart, but lives in memory."""

    def __init__(self):
        self.data = (0, None, [])

    def load(self):
        term, voted_for, log = self.data
        return term, voted_for, list(log)

    def save(self, term, voted_for, log):
        self.data = (term, voted_for, list(log))


class Cluster:
    def __init__(self, seed=None):
        self.lock = threading.Lock()
        self.rng = random.Random(seed)
        self.ids = list(range(1, NODES + 1))
        self.disks = {i: Disk() for i in self.ids}
        self.nodes = {i: self._boot(i) for i in self.ids}
        self.down = set()
        self.cut = set()            # nodes that cannot reach the rest
        self.queue = []
        self.events = deque(maxlen=30)
        self.sent = deque(maxlen=300)   # recent messages, for the page to draw
        self.seq = 0
        self.speed = "normal"
        self.leader_term = 0        # highest term for which we have announced a leader
        self.announced = 0          # highest log index announced as committed
        self._say("Cluster started. No leader yet: every server is waiting to hear from one.")

    def _boot(self, i):
        return RaftNode(i, [p for p in self.ids if p != i], KVStore(), self.disks[i], random.Random(self.rng.random()))

    def _say(self, text):
        self.events.appendleft({"at": time.time(), "text": text})

    def _connected(self, a, b):
        return a not in self.down and b not in self.down and (a in self.cut) == (b in self.cut)

    def step(self):
        """One tick of every live node, then deliver the messages sent on the previous tick."""
        before = {i: [e["term"] for e in n.log] for i, n in self.nodes.items()}
        for i in self.ids:
            if i not in self.down:
                self.queue += self.nodes[i].tick()
        batch, self.queue = self.queue, []
        for m in batch:
            arrives = self._connected(m["from"], m["to"])
            self._record(m, arrives)
            if arrives:
                self.queue += self.nodes[m["to"]].handle(m)
        self._report(before)

    def _record(self, m, arrives):
        """Keep the messages worth drawing: heartbeats, copied entries, vote requests, granted votes."""
        if m["type"] == "append_entries":
            kind = "entries" if m["entries"] else "heartbeat"
        elif m["type"] == "request_vote":
            kind = "vote_request"
        elif m["type"] == "vote_reply" and m["granted"]:
            kind = "vote"
        else:
            return
        self.seq += 1
        self.sent.append({"seq": self.seq, "from": m["from"], "to": m["to"], "kind": kind, "arrives": arrives})

    def _report(self, before):
        for i, node in self.nodes.items():
            if i in self.down:
                continue
            if node.role == "leader" and node.term > self.leader_term:
                self.leader_term = node.term
                self._say(f"Server {i} won the election and is leader for term {node.term}.")
            # entries it had a moment ago that are now gone or replaced by the leader's
            lost = sum(1 for k, term in enumerate(before[i]) if k >= len(node.log) or node.log[k]["term"] != term)
            if lost:
                self._say(f"Server {i} dropped {lost} unconfirmed {'entry' if lost == 1 else 'entries'} to match the leader's log.")
        leader = self.leader()
        if leader:
            for index in range(self.announced + 1, leader.commit_index + 1):
                c = leader.log[index - 1]["command"]
                if c["op"] == "put":
                    self._say(f"Saved {c['key']} = {c['value']} on a majority of servers (entry {index}).")
                elif c["op"] == "delete":
                    self._say(f"Deleted {c['key']} on a majority of servers (entry {index}).")
            self.announced = max(self.announced, leader.commit_index)

    def leader(self):
        """The leader with the newest term: the one a majority follows."""
        live = [n for i, n in self.nodes.items() if i not in self.down and n.role == "leader"]
        return max(live, key=lambda n: n.term) if live else None

    # ------------------------------------------------------------ controls
    def toggle(self, i):
        if i in self.down:
            self.nodes[i] = self._boot(i)    # memory is gone; term, vote and log come back from its disk
            self.down.discard(i)
            self._say(f"Server {i} restarted and reloaded its log from disk.")
        else:
            self.down.add(i)
            self._say(f"Server {i} stopped.")

    def isolate(self, i):
        if i in self.cut:
            self.cut.discard(i)
            self._say(f"Server {i} is reconnected to the others.")
        else:
            self.cut.add(i)
            self._say(f"Server {i} is cut off: it is running but its messages do not get through.")

    def restore(self):
        """Start every stopped server and reconnect every cut-off one."""
        for i in sorted(self.down):
            self.toggle(i)
        for i in sorted(self.cut):
            self.isolate(i)

    def state(self, since=0):
        leader = self.leader()
        nodes = []
        for i in self.ids:
            n = self.nodes[i]
            up = i not in self.down
            nodes.append({
                "id": i, "up": up, "cut": i in self.cut, "role": n.role if up else "stopped", "term": n.term,
                "commit_index": n.commit_index if up else 0,
                "patience": 1 - n.election_elapsed / n.election_timeout if up and n.role != "leader" else None,
                "log": [{"term": e["term"], **e["command"]} for e in n.log],
                "store": n.state_machine.store if up else {},
            })
        return {"nodes": nodes, "leader": leader.id if leader else None, "events": list(self.events),
                "speed": self.speed, "tick_ms": int(SPEEDS[self.speed] * 1000), "seq": self.seq,
                "messages": [m for m in self.sent if m["seq"] > since] if since else []}


class Command(BaseModel):
    op: str = Field(pattern="^(put|get|delete)$")
    key: str = Field(min_length=1, max_length=24)
    value: str | None = Field(default=None, max_length=40)
    node: int | None = None   # send to this node instead of the current leader


app = FastAPI(title="RaftKV demo")
cluster = Cluster()


def run():
    while True:
        time.sleep(SPEEDS[cluster.speed])
        with cluster.lock:
            cluster.step()


threading.Thread(target=run, daemon=True).start()


@app.get("/")
def home():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/state")
def state(since: int = 0):
    """`since` is the last message number the page has drawn; 0 means "none yet, skip the backlog"."""
    with cluster.lock:
        return cluster.state(since)


@app.post("/api/speed/{speed}")
def set_speed(speed: str):
    if speed not in SPEEDS:
        raise HTTPException(404, "speed is normal or slow")
    cluster.speed = speed
    return {"ok": True}


@app.post("/api/restore")
def restore():
    with cluster.lock:
        cluster.restore()
    return {"ok": True}


@app.post("/api/nodes/{node_id}/toggle")
def toggle(node_id: int):
    with cluster.lock:
        if node_id not in cluster.nodes:
            raise HTTPException(404, "no such node")
        cluster.toggle(node_id)
    return {"ok": True}


@app.post("/api/nodes/{node_id}/isolate")
def isolate(node_id: int):
    with cluster.lock:
        if node_id not in cluster.nodes:
            raise HTTPException(404, "no such node")
        cluster.isolate(node_id)
    return {"ok": True}


@app.post("/api/reset")
def reset():
    global cluster
    fresh = Cluster()
    with cluster.lock:
        cluster = fresh
    return {"ok": True}


@app.post("/api/command")
def command(c: Command):
    """Send a client command and wait for the cluster's answer, as server.py does."""
    with cluster.lock:
        target = cluster.nodes.get(c.node) if c.node else cluster.leader()
        if target is None or target.id in cluster.down:
            return {"ok": False, "message": "There is no leader right now. Wait a moment for the nodes to elect one."}
        if target.role != "leader":
            return {"ok": False, "message": f"Server {target.id} is not the leader, so it would redirect you to the leader."}
        if c.op == "put" and len(target.log) >= MAX_LOG:
            return {"ok": False, "message": "The demo log is full. Reset the demo to keep going."}
        body = {"op": c.op, "key": c.key} | ({"value": c.value or ""} if c.op == "put" else {})
        index = target.propose(body)
        cluster.queue += target.replicate()
        node = target
    deadline = time.monotonic() + 25 * SPEEDS[cluster.speed]
    while time.monotonic() < deadline:
        time.sleep(0.05)
        with cluster.lock:
            result = node.results.pop(index, None)
        if result:
            ok, value = result
            if not ok:
                return {"ok": False, "message": "A newer leader replaced this entry, so the command was not applied."}
            if value == "Key Not Found":
                return {"ok": False, "message": f"There is no key called {c.key}."}
            done = {"put": f"Saved {c.key}.", "get": f"{c.key} = {value}", "delete": f"Deleted {c.key}."}[c.op]
            return {"ok": True, "message": done, "value": value}
    return {"ok": False, "message": f"Server {node.id} could not reach a majority, so nothing was saved. "
                                    "A value only counts once most nodes have it."}
