"""Deterministic half of the wedge: prove Redpanda kills the connection on the
exact FindCoordinator frame kafka-python 3.0.7 sends when it looks up a
transaction coordinator with transactional_id=None (idempotent-only producer).

This needs no race and works against ANY running Redpanda (fresh or warm):
    uv run python wedge-poison-direct.py [host:port]   # default localhost:19092
Then check the broker log for:
    Disconnected ... (short read), std::out_of_range (Asked to read a 0 byte flex string)

Frame layout (byte-for-byte what tcpdump showed on the wedged retrier):
  FindCoordinatorRequest v4, header v2, client_id "kafka-python-producer-1",
  key_type=1 (TRANSACTION), coordinator_keys=[null]  <-- unparseable null
"""

import socket
import struct
import sys

BROKER = sys.argv[1] if len(sys.argv) > 1 else "localhost:19092"
host, port = BROKER.rsplit(":", 1)

client_id = b"kafka-python-producer-1"
body = (
    struct.pack(">hhi", 10, 4, 2)          # api_key=10, api_version=4, correlation_id=2
    + struct.pack(">h", len(client_id)) + client_id  # client_id (legacy string)
    + b"\x00"                              # header tagged fields
    + b"\x01"                              # key_type = 1 (TRANSACTION)
    + b"\x02"                              # coordinator_keys: compact array, 1 element
    + b"\x00"                              # element = null compact string (the poison)
    + b"\x00"                              # body tagged fields
)
frame = struct.pack(">i", len(body)) + body
assert len(frame) == 42, len(frame)

sock = socket.create_connection((host, int(port)), timeout=5)
sock.sendall(frame)
sock.settimeout(5)
try:
    data = sock.recv(4096)
except socket.timeout:
    print("no response and no close within 5s — unexpected")
    sys.exit(2)

if data == b"":
    print("REPRODUCED (deterministic): broker closed the connection with no "
          "response to the null-key FindCoordinator.")
    print("Broker log should show: 'std::out_of_range (Asked to read a 0 byte "
          "flex string)'.")
    print("kafka-python treats this close as a retriable disconnect and resends "
          "the same frame forever -> the ~10Hz 'socket disconnected' loop.")
    sys.exit(0)

print(f"broker answered {len(data)} bytes instead of closing — not reproduced "
      "(different broker version/behavior?)")
sys.exit(1)
