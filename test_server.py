"""Three real nodes as separate processes: write, read, kill the leader, restart it."""
import subprocess
import sys
import time

import httpx
import pytest

PORTS = {1: 8101, 2: 8102, 3: 8103}
URL = {i: f"http://127.0.0.1:{p}" for i, p in PORTS.items()}


def start(i, data):
    peers = ",".join(f"{j}={URL[j]}" for j in PORTS if j != i)
    return subprocess.Popen([sys.executable, "server.py", "--id", str(i), "--port", str(PORTS[i]),
                             "--peers", peers, "--data", str(data)])


def leader_id(alive):
    """Poll until the live nodes agree on a leader that is itself alive."""
    for _ in range(100):
        try:
            seen = {httpx.get(URL[i] + "/status", timeout=1).json()["leader"] for i in alive}
        except httpx.HTTPError:
            seen = set()
        if len(seen) == 1 and seen <= set(alive):
            return seen.pop()
        time.sleep(0.1)
    raise AssertionError("no leader")


@pytest.fixture
def cluster(tmp_path):
    procs = {i: start(i, tmp_path) for i in PORTS}
    yield procs, tmp_path
    for p in procs.values():
        p.kill()


def test_cluster_survives_leader_failure(cluster):
    procs, data = cluster
    client = httpx.Client(follow_redirects=True, timeout=5)
    first = leader_id(PORTS)
    follower = next(i for i in PORTS if i != first)

    assert client.put(URL[follower] + "/kv/name", json={"value": "Priyanka"}).json() == {"ok": True}  # via redirect
    assert client.get(URL[first] + "/kv/name").json()["value"] == "Priyanka"
    assert client.get(URL[follower] + "/kv/missing").status_code == 404

    procs[first].kill()
    procs[first].wait()
    alive = [i for i in PORTS if i != first]
    second = leader_id(alive)
    assert second != first
    assert client.get(URL[second] + "/kv/name").json()["value"] == "Priyanka"   # committed data survived
    assert client.put(URL[second] + "/kv/city", json={"value": "Delhi"}).status_code == 200

    procs[first] = start(first, data)                                            # old leader returns
    leader_id(PORTS)
    for _ in range(50):
        status = httpx.get(URL[first] + "/status").json()
        if status["role"] == "follower" and status["commit_index"] >= 4:
            break
        time.sleep(0.1)
    assert status["role"] == "follower"
    assert client.get(URL[first] + "/kv/city").json()["value"] == "Delhi"        # it caught up
    assert client.delete(URL[first] + "/kv/city").json() == {"ok": True}
    assert client.get(URL[second] + "/kv/city").status_code == 404
