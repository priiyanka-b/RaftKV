"""One RaftKV node over HTTP.

    python server.py --id 1 --port 8001 --peers 2=http://127.0.0.1:8002,3=http://127.0.0.1:8003

Clients can call any node; a follower redirects them to the leader.
"""
import argparse
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from kv_store import KVStore
from raft import RaftNode, Storage

TICK_SECONDS = 0.05      # election timeout = 10-20 ticks = 0.5-1s, heartbeat = 3 ticks
COMMIT_WAIT_SECONDS = 2


class Value(BaseModel):
    value: object


def create_app(node_id, peers, data_dir="data"):
    """peers: {node id: base url} for the other nodes."""
    os.makedirs(data_dir, exist_ok=True)
    node = RaftNode(node_id, list(peers), KVStore(), Storage(os.path.join(data_dir, f"node{node_id}.json")))
    lock = threading.Lock()  # the node is plain Python; every call into it holds this
    http = httpx.Client(timeout=0.5)
    pool = ThreadPoolExecutor(max_workers=8)
    app = FastAPI(title=f"RaftKV node {node_id}")

    def post(m):
        try:
            http.post(peers[m["to"]] + "/raft", json=m)
        except httpx.HTTPError:
            pass  # peer is down or slow; the next heartbeat retries

    def send(messages):
        for m in messages:
            pool.submit(post, m)

    def ticker():
        while True:
            time.sleep(TICK_SECONDS)
            with lock:
                out = node.tick()
            send(out)

    threading.Thread(target=ticker, daemon=True).start()

    @app.post("/raft")
    def raft(m: dict):
        with lock:
            out = node.handle(m)
        send(out)
        return {}

    def submit(command, request):
        with lock:
            index = node.propose(command)
            leader = node.leader_id
            out = node.replicate() if index else []
        send(out)
        if index is None:
            if leader in peers:
                return RedirectResponse(peers[leader] + request.url.path, status_code=307)
            raise HTTPException(503, "no leader right now, retry")
        deadline = time.monotonic() + COMMIT_WAIT_SECONDS
        while time.monotonic() < deadline:
            with lock:
                result = node.results.pop(index, None)
            if result:
                ok, value = result
                if not ok:
                    raise HTTPException(503, "leadership changed before the write committed, retry")
                return value
            time.sleep(0.01)
        raise HTTPException(504, "not committed in time (is a majority of nodes up?)")

    def found(result):
        if result == "Key Not Found":
            raise HTTPException(404, "key not found")
        return result

    @app.put("/kv/{key}")
    def put(key: str, body: Value, request: Request):
        result = submit({"op": "put", "key": key, "value": body.value}, request)
        return result if isinstance(result, RedirectResponse) else {"ok": True}

    @app.get("/kv/{key}")
    def get(key: str, request: Request):
        # reads go through the log too, so a deposed leader can never serve stale data
        result = submit({"op": "get", "key": key}, request)
        return result if isinstance(result, RedirectResponse) else {"key": key, "value": found(result)}

    @app.delete("/kv/{key}")
    def delete(key: str, request: Request):
        result = submit({"op": "delete", "key": key}, request)
        return result if isinstance(result, RedirectResponse) else {"ok": found(result) is None}

    @app.get("/status")
    def status():
        with lock:
            return {"id": node.id, "role": node.role, "term": node.term, "leader": node.leader_id,
                    "log_length": len(node.log), "commit_index": node.commit_index}

    return app


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--peers", default="", help="2=http://127.0.0.1:8002,3=http://127.0.0.1:8003")
    ap.add_argument("--data", default="data")
    args = ap.parse_args()
    peers = {int(k): v for k, v in (p.split("=", 1) for p in args.peers.split(",") if p)}
    uvicorn.run(create_app(args.id, peers, args.data), host="127.0.0.1", port=args.port, log_level="warning")
