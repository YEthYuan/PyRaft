import threading

class RaftNode:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.state = 'follower'
        self.election_timeout = None
        self.vote_count = 0
        self.leader_id = None
        self.next_index = {}
        self.match_index = {}

    def start(self):
        self.election_timeout = threading.Timer(self.random_timeout(), self.start_election)
        self.election_timeout.start()

    def stop(self):
        self.election_timeout.cancel()

    def random_timeout(self):
        # TODO: Implement randomized election timeout
        pass

    def start_election(self):
        self.state = 'candidate'
        self.current_term += 1
        self.voted_for = self.node_id
        self.vote_count = 1
        self.election_timeout.cancel()

        for peer in self.peers:
            # TODO: Implement send_request_vote RPC to peers

        self.election_timeout = threading.Timer(self.random_timeout(), self.start_election)
        self.election_timeout.start()

    def on_request_vote(self, candidate_id, term, last_log_index, last_log_term):
        if term < self.current_term:
            return False

        if self.voted_for is None or self.voted_for == candidate_id:
            if self.log[-1].term <= last_log_term and len(self.log) <= last_log_index:
                self.voted_for = candidate_id
                return True

        return False

    def on_append_entries(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        if term < self.current_term:
            return False

        self.election_timeout.cancel()
        self.state = 'follower'
        self.current_term = term
        self.leader_id = leader_id

        if prev_log_index >= len(self.log) or self.log[prev_log_index].term != prev_log_term:
            return False

        for i in range(len(entries)):
            if prev_log_index + 1 + i >= len(self.log) or self.log[prev_log_index + 1 + i].term != entries[i].term:
                self.log = self.log[:prev_log_index + 1 + i]
                self.log.extend(entries[i:])
                break

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)

        return True

    def send_append_entries(self, peer):
        # TODO: Implement send_append_entries RPC to peer

    def send_request_vote(self, peer):
        # TODO: Implement send_request_vote RPC to peer