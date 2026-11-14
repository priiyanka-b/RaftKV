"""Raft consensus for one node.

The node does no I/O and has no clock of its own. The caller calls tick() at a fixed interval and
handle() for each message that arrives; both return the messages to send. That keeps the protocol
testable with a simulated network (test_raft.py) and lets server.py put it on real HTTP.

Log indexes are 1-based as in the paper; index 0 means "nothing yet".
"""
import json
import os
import random

HEARTBEAT_TICKS = 3
ELECTION_TICKS = (10, 20)  # a follower waits a random number of ticks in this range


class Storage:
    """Term, vote and log on disk. Raft requires these to be saved before replying to anyone."""

    def __init__(self, path):
        self.path = path

    def load(self):
        if not os.path.exists(self.path):
            return 0, None, []
        with open(self.path) as f:
            data = json.load(f)
        return data["term"], data["voted_for"], data["log"]

    def save(self, term, voted_for, log):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"term": term, "voted_for": voted_for, "log": log}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)  # atomic: a crash leaves the old file or the new one, never half


class RaftNode:
    def __init__(self, node_id, peers, state_machine, storage=None, rng=None):
        self.id = node_id
        self.peers = list(peers)
        self.state_machine = state_machine
        self.storage = storage
        self.rng = rng or random.Random()

        self.term, self.voted_for, self.log = storage.load() if storage else (0, None, [])
        self.commit_index = 0
        self.last_applied = 0
        self.role = "follower"
        self.leader_id = None
        self.votes = set()
        self.next_index = {}   # leader only: next entry to send to each peer
        self.match_index = {}  # leader only: highest entry known to be on each peer
        self.waiting = {}      # index -> term, for commands a client is waiting on
        self.results = {}      # index -> (ok, value) once that command is applied or lost
        self.heartbeat_elapsed = 0
        self._reset_election_timer()

    # ------------------------------------------------------------ helpers
    def _persist(self):
        if self.storage:
            self.storage.save(self.term, self.voted_for, self.log)

    def _reset_election_timer(self):
        self.election_elapsed = 0
        self.election_timeout = self.rng.randint(*ELECTION_TICKS)

    def _last(self):
        return len(self.log), (self.log[-1]["term"] if self.log else 0)

    def _majority(self, count):
        return count > (len(self.peers) + 1) // 2

    def _msg(self, to, kind, **fields):
        return {"type": kind, "from": self.id, "to": to, "term": self.term, **fields}

    # -------------------------------------------------------------- clock
    def tick(self):
        if self.role == "leader":
            self.heartbeat_elapsed += 1
            if self.heartbeat_elapsed >= HEARTBEAT_TICKS:
                return self.replicate()
            return []
        self.election_elapsed += 1
        if self.election_elapsed >= self.election_timeout:
            return self._start_election()
        return []

    # ----------------------------------------------------------- election
    def _start_election(self):
        self.term += 1
        self.role = "candidate"
        self.voted_for = self.id
        self.votes = {self.id}
        self.leader_id = None
        self._persist()
        self._reset_election_timer()
        if self._majority(len(self.votes)):  # single-node cluster
            return self._become_leader()
        index, term = self._last()
        return [self._msg(p, "request_vote", last_index=index, last_term=term) for p in self.peers]

    def _on_request_vote(self, m):
        # grant only if we have not voted for someone else this term and the candidate's
        # log is at least as up to date as ours (election restriction, section 5.4.1)
        my_index, my_term = self._last()
        up_to_date = (m["last_term"], m["last_index"]) >= (my_term, my_index)
        granted = m["term"] == self.term and self.voted_for in (None, m["from"]) and up_to_date
        if granted:
            self.voted_for = m["from"]
            self._persist()
            self._reset_election_timer()
        return [self._msg(m["from"], "vote_reply", granted=granted)]

    def _on_vote_reply(self, m):
        if self.role != "candidate" or m["term"] != self.term or not m["granted"]:
            return []
        self.votes.add(m["from"])
        return self._become_leader() if self._majority(len(self.votes)) else []

    def _become_leader(self):
        self.role = "leader"
        self.leader_id = self.id
        self.next_index = {p: len(self.log) + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        # A leader may only count replicas for entries of its own term, so start the term with
        # a no-op. Committing it also commits everything before it (section 5.4.2).
        self.log.append({"term": self.term, "command": {"op": "noop"}})
        self._persist()
        self._advance_commit()
        return self.replicate()

    # -------------------------------------------------------- replication
    def propose(self, command):
        """Append a client command. Returns its log index, or None if this node is not the leader."""
        if self.role != "leader":
            return None
        self.log.append({"term": self.term, "command": command})
        self._persist()
        index = len(self.log)
        self.waiting[index] = self.term
        self._advance_commit()
        return index

    def _entries_for(self, peer):
        prev = self.next_index[peer] - 1
        return self._msg(peer, "append_entries",
                         prev_index=prev, prev_term=self.log[prev - 1]["term"] if prev else 0,
                         entries=self.log[prev:], leader_commit=self.commit_index)

    def replicate(self):
        self.heartbeat_elapsed = 0
        return [self._entries_for(p) for p in self.peers]

    def _on_append_entries(self, m):
        if m["term"] < self.term:
            return [self._msg(m["from"], "append_reply", success=False)]
        self.role = "follower"
        self.leader_id = m["from"]
        self._reset_election_timer()

        prev = m["prev_index"]
        if prev > len(self.log) or (prev and self.log[prev - 1]["term"] != m["prev_term"]):
            return [self._msg(m["from"], "append_reply", success=False)]  # our log does not match yet

        changed = False
        for offset, entry in enumerate(m["entries"]):
            index = prev + 1 + offset
            if index <= len(self.log) and self.log[index - 1]["term"] != entry["term"]:
                del self.log[index - 1:]  # conflict: drop our entry and everything after it
                changed = True
            if index > len(self.log):
                self.log.append(entry)
                changed = True
        if changed:
            self._persist()

        match = prev + len(m["entries"])
        if m["leader_commit"] > self.commit_index:
            self.commit_index = min(m["leader_commit"], match)
            self._apply_committed()
        return [self._msg(m["from"], "append_reply", success=True, match_index=match)]

    def _on_append_reply(self, m):
        if self.role != "leader" or m["term"] != self.term:
            return []
        peer = m["from"]
        if m["success"]:
            self.match_index[peer] = max(self.match_index[peer], m["match_index"])
            self.next_index[peer] = self.match_index[peer] + 1
            self._advance_commit()
            return []
        self.next_index[peer] = max(1, self.next_index[peer] - 1)  # step back and try again
        return [self._entries_for(peer)]

    def _advance_commit(self):
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1]["term"] != self.term:
                break  # never commit an older term's entry by counting replicas
            if self._majority(1 + sum(1 for p in self.peers if self.match_index[p] >= n)):
                self.commit_index = n
                self._apply_committed()
                break

    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            value = None if entry["command"]["op"] == "noop" else self.state_machine.apply(entry["command"])
            if self.last_applied in self.waiting:
                # if another leader's entry took this slot, the client's command was lost
                ours = self.waiting.pop(self.last_applied) == entry["term"]
                self.results[self.last_applied] = (ours, value)

    # ----------------------------------------------------------- messages
    def handle(self, m):
        if m["term"] > self.term:  # someone is ahead of us: adopt their term and step down
            self.term = m["term"]
            self.voted_for = None
            self.role = "follower"
            self.leader_id = None
            self._persist()
        return {
            "request_vote": self._on_request_vote,
            "vote_reply": self._on_vote_reply,
            "append_entries": self._on_append_entries,
            "append_reply": self._on_append_reply,
        }[m["type"]](m)
