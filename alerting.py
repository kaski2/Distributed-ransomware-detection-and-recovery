import hashlib
import hmac
import json
import socket
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def canonical_json(data):
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class AlertSigner:
    def __init__(self, shared_secret):
        self._key = shared_secret.encode("utf-8")

    def sign(self, payload):
        return hmac.new(self._key, payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def verify(self, payload, signature):
        expected = self.sign(payload)
        return hmac.compare_digest(expected, signature)


class GossipNode:
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
        self._leader_id = None

    def start(self):
        self._listener_thread.start()
        self._heartbeat_thread.start()

    def stop(self):
        self._stop_event.set()

    def is_leader(self):
        return self._leader_id == self.node_id

    def send_to_coordinator(self, alert):
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
        signature = message.get("sig", "")
        payload_text = canonical_json({k: v for k, v in message.items() if k != "sig"})
        return self.signer.verify(payload_text, signature)

    def _send_message(self, message, addr):
        data = json.dumps(message, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        self._sock.sendto(data, addr)

    def _listen_loop(self):
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
                    self._peer_last_seen[node_id] = time.time()
                    self._peer_addr[node_id] = addr

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
        while not self._stop_event.is_set():
            message = self._build_message("heartbeat", {"status": "ok"})
            for peer in list(self.peers):
                self._send_message(message, peer)

            self._update_leader()
            time.sleep(self.heartbeat_interval)

    def _update_leader(self):
        now = time.time()
        candidates = {self.node_id}
        with self._lock:
            for peer_id, last_seen in self._peer_last_seen.items():
                if now - last_seen <= self.leader_ttl:
                    candidates.add(peer_id)

        leader_id = min(candidates) if candidates else None
        if leader_id != self._leader_id:
            self._leader_id = leader_id
            if self.on_leader_change:
                self.on_leader_change(leader_id)


class AlertManager:
    def __init__(
        self,
        node_id,
        gossip_node,
        send_to_coordinator,
    ):
        self.node_id = node_id
        self.gossip_node = gossip_node
        self.send_to_coordinator = send_to_coordinator

    def send_alert(self, alert_type, details, severity):
        alert = {
            "alert_id": str(uuid4()),
            "alert_type": alert_type,
            "severity": severity,
            "node_id": self.node_id,
            "ts": utc_now_iso(),
            "details": details,
        }

        if self.send_to_coordinator:
            self.gossip_node.send_to_coordinator(alert)
