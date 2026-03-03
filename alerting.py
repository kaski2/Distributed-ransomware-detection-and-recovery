import hashlib
import hmac
import json
import socket
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4


def utc_now_iso():
    """Return the current UTC time as an ISO string."""
    return datetime.now(timezone.utc).isoformat()


def canonical_json(data):
    """Return data serialized as canonical JSON."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class AlertSigner:
    """Sign and verify messages using HMAC."""

    def __init__(self, shared_secret):
        """Initialize signer with a shared secret key."""
        self._key = shared_secret.encode("utf-8")

    def sign(self, payload):
        """Return HMAC signature for the payload."""
        return hmac.new(self._key, payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def verify(self, payload, signature):
        """Return True if signature is valid."""
        expected = self.sign(payload)
        return hmac.compare_digest(expected, signature)


class GossipNode:
    """Gossip-based node with leader election and alert forwarding."""

    def __init__(
        self,
        node_id,
        listen_host,
        listen_port,
        peers,
        signer,
        heartbeat_interval,
        leader_ttl,
        on_alert,
        on_leader_change=None,
    ):
        """Initialize gossip node."""
        self.node_id = node_id
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.peers = peers
        self.signer = signer
        self.heartbeat_interval = heartbeat_interval
        self.leader_ttl = leader_ttl
        self.on_alert = on_alert
        self.on_leader_change = on_leader_change

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((self.listen_host, self.listen_port))
        self._sock.settimeout(1.0)

        self._stop_event = threading.Event()
        self._listener_thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)

        self._lock = threading.Lock()
        self._peer_last_seen = {}
        self._peer_addr = {}
        self._peer_reported_leader = {}
        self._leader_id = None

    def start(self):
        """Start listener and heartbeat threads."""
        self._listener_thread.start()
        self._heartbeat_thread.start()

    def stop(self):
        """Stop the node."""
        self._stop_event.set()

    def is_leader(self):
        """Return True if this node is the leader."""
        return self._leader_id == self.node_id

    def send_to_coordinator(self, alert):
        """Send alert to leader or handle locally."""
        leader_id = self._leader_id
        if leader_id is None or leader_id == self.node_id:
            self.on_alert(alert, "local")
            return True

        with self._lock:
            leader_addr = self._peer_addr.get(leader_id)

        if leader_addr is None:
            return False

        message = self._build_message("coordinator_alert", alert)
        self._send_message(message, leader_addr)
        return True

    def _build_message(self, msg_type, payload):
        """Build and sign a message."""
        message = {
            "type": msg_type,
            "node_id": self.node_id,
            "ts": utc_now_iso(),
            "payload": payload,
        }
        payload_text = canonical_json({k: v for k, v in message.items() if k != "sig"})
        message["sig"] = self.signer.sign(payload_text)
        return message

    def _verify_message(self, message):
        """Verify message signature."""
        signature = message.get("sig", "")
        payload_text = canonical_json({k: v for k, v in message.items() if k != "sig"})
        return self.signer.verify(payload_text, signature)

    def _send_message(self, message, addr):
        """Send message to a peer address."""
        data = json.dumps(message, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        self._sock.sendto(data, addr)

    def _listen_loop(self):
        """Listen for incoming messages."""
        while not self._stop_event.is_set():
            try:
                data, addr = self._sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break

            try:
                message = json.loads(data.decode("utf-8"))
            except json.JSONDecodeError:
                continue

            if not self._verify_message(message):
                continue

            msg_type = message.get("type")
            node_id = message.get("node_id")
            if node_id:
                with self._lock:
                    self._peer_last_seen[node_id] = time.monotonic()
                    self._peer_addr[node_id] = addr
                    if msg_type == "heartbeat":
                        reported_leader = (message.get("payload") or {}).get("leader_id")
                        self._peer_reported_leader[node_id] = reported_leader

            if msg_type == "heartbeat":
                continue

            if msg_type == "coordinator_alert":
                payload = message.get("payload", {})
                alert_id = payload.get("alert_id")
                if not alert_id:
                    continue
                if not self.is_leader():
                    continue
                self.on_alert(payload, "coordinator")

    def _heartbeat_loop(self):
        """Send heartbeats and update leader."""
        while not self._stop_event.is_set():
            message = self._build_message("heartbeat", {"status": "ok", "leader_id": self._leader_id})
            for peer in list(self.peers):
                try:
                    self._send_message(message, peer)
                except OSError:
                    continue

            self._update_leader()
            time.sleep(self.heartbeat_interval)

    def _update_leader(self):
        """Update leader based on active peers."""
        now = time.monotonic()
        candidates = {self.node_id}
        reported_leaders = set()
        with self._lock:
            for peer_id, last_seen in list(self._peer_last_seen.items()):
                if now - last_seen > self.leader_ttl:
                    self._peer_last_seen.pop(peer_id, None)
                    self._peer_addr.pop(peer_id, None)
                    self._peer_reported_leader.pop(peer_id, None)
                    continue

                candidates.add(peer_id)
                reported_leader = self._peer_reported_leader.get(peer_id)
                if reported_leader:
                    reported_leaders.add(reported_leader)

        reported_leaders &= candidates
        if self._leader_id in candidates:
            reported_leaders.add(self._leader_id)

        next_leader_id = self._leader_id
        consensus_leader_id = min(reported_leaders) if reported_leaders else None

        if self._leader_id in candidates and self._leader_id != self.node_id:
            if consensus_leader_id is not None:
                next_leader_id = consensus_leader_id
            else:
                next_leader_id = self._leader_id
        elif consensus_leader_id is not None:
            next_leader_id = consensus_leader_id
        elif self._leader_id in candidates:
            next_leader_id = self._leader_id
        else:
            next_leader_id = min(candidates) if candidates else None

        if next_leader_id == self._leader_id:
            return

        self._leader_id = next_leader_id
        if self.on_leader_change:
            self.on_leader_change(next_leader_id)


class AlertManager:
    """Create and send alerts."""

    def __init__(
        self,
        node_id,
        gossip_node,
        send_to_coordinator,
    ):
        """Initialize alert manager."""
        self.node_id = node_id
        self.gossip_node = gossip_node
        self.send_to_coordinator = send_to_coordinator

    def send_alert(self, alert_type, details, severity):
        """Create and send an alert."""
        alert = {
            "alert_id": str(uuid4()),
            "alert_type": alert_type,
            "severity": severity,
            "node_id": self.node_id,
            "ts": utc_now_iso(),
            "details": details,
        }

        if self.send_to_coordinator:
            sent = self.gossip_node.send_to_coordinator(alert)
            if not sent:
                self.gossip_node.on_alert(alert, "local_fallback")
        else:
            self.gossip_node.on_alert(alert, "local")


class AlertDetector:
    """Detect ransomware note indicators in events."""

    def __init__(self, ransom_note_keywords):
        """Initialize detector with keywords."""
        self.ransom_note_keywords = [keyword.lower() for keyword in ransom_note_keywords]

    def detect(self, event_data):
        """Return alerts based on event data."""
        alerts = []
        event_type = event_data.get("event_type")
        if event_type not in {"created", "modified"}:
            return alerts

        file_name = (event_data.get("file_name") or "").lower()
        file_contents = (event_data.get("file_contents") or "").lower()
        file_path = event_data.get("file_path", "")

        for keyword in self.ransom_note_keywords:
            if keyword in file_name or keyword in file_contents:
                alerts.append(
                    {
                        "alert_type": "ransom_note",
                        "severity": "high",
                        "details": {
                            "file_path": file_path,
                            "keyword": keyword,
                        },
                    }
                )
                break

        return alerts
