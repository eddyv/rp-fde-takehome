"""Narrated, deterministic reproduction of the `socket disconnected` poison
loop (kafka-python 3.0.7 x Redpanda v25.3.15).

Run via ./wedge-repro.sh (which boots the required fresh throwaway broker),
or standalone against a FRESH broker advertising 127.0.0.1:19094:

    uv run python wedge-repro.py

The output is a stage-by-stage timeline of the causal chain, with the actual
log evidence and the responsible code locations. Full client DEBUG log goes
to wedge-repro.log. Exit 0 = reproduced, 1 = not.

The only artificial ingredient is a TCP relay that releases every producer's
first InitProducerId frame at the same instant — the same same-millisecond
collision that happens naturally when docker compose starts the worker and
retrier containers together. Everything downstream (broker responses, client
reactions, the loop) is real, unmodified behavior.
"""

import inspect
import logging
import os
import re
import socket
import struct
import sys
import threading
import time

UPSTREAM = os.environ.get("WEDGE_UPSTREAM", "localhost:19093")  # real broker
RELAY_HOST, RELAY_PORT = "127.0.0.1", 19094  # broker must ADVERTISE this
N_PRODUCERS = 6
BARRIER_TIMEOUT_S = 3.0
OBSERVE_S = 6
LOG_PATH = os.environ.get("WEDGE_LOG", "wedge-repro.log")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(threadName)s %(name)s %(levelname)s %(message)s",
    filename=LOG_PATH,
    filemode="w",
)
for noisy in ("kafka.metrics", "kafka.protocol.parser", "kafka.conn"):
    logging.getLogger(noisy).setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Evidence recorder: turns kafka-python's own DEBUG logs into a typed event
# timeline we can narrate from. Nothing here alters client behavior.
# ---------------------------------------------------------------------------

RE_INIT_SENT = re.compile(r"Request \d+: InitProducerIdRequest\(")
RE_INIT_RESP = re.compile(
    r"Response \d+ \(([\d.]+) ms\): InitProducerIdResponse\(.*error_code=(\d+)"
    r".*producer_id=(-?\d+)")
RE_POISON_ENQ = re.compile(
    r"Enqueuing transactional request FindCoordinatorRequest\(key=None")
RE_POISON_WIRE = re.compile(
    r"Request \d+: FindCoordinatorRequest\(version=\d+, key_type=1, "
    r"coordinator_keys=\[None\]\)")
RE_DISCONNECT = re.compile(r"socket disconnected")
RE_PRODUCER = re.compile(r"kafka-python-producer-(\d+)")


class ChainRecorder(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.lock_ = threading.Lock()
        self.events = []  # (t, producer_label, kind, detail)

    def _producer(self, record):
        m = RE_PRODUCER.search(record.threadName)
        return f"producer-{m.group(1)}" if m else record.threadName

    def emit(self, record):
        msg = record.getMessage()
        kind = detail = None
        if RE_INIT_SENT.search(msg):
            kind, detail = "init_sent", None
        else:
            m = RE_INIT_RESP.search(msg)
            if m:
                kind = "init_resp"
                detail = (float(m.group(1)), int(m.group(2)), int(m.group(3)))
            elif RE_POISON_ENQ.search(msg):
                kind, detail = "poison_enqueue", msg
            elif RE_POISON_WIRE.search(msg):
                kind, detail = "poison_wire", None
            elif (RE_DISCONNECT.search(msg) and record.levelno >= logging.ERROR
                  # one slam emits three ERROR lines (transport, Abort,
                  # Connection lost) — count only the transport one
                  and "Abort" not in msg and "Connection lost" not in msg):
                kind, detail = "disconnect", None
        if kind:
            with self.lock_:
                self.events.append(
                    (record.created, self._producer(record), kind, detail))

    def by_kind(self, kind):
        with self.lock_:
            return [e for e in self.events if e[2] == kind]


recorder = ChainRecorder()
for name in ("kafka.producer.transaction_manager", "kafka.producer.sender",
             "kafka.net.transport", "kafka.net.connection"):
    logging.getLogger(name).addHandler(recorder)


# ---------------------------------------------------------------------------
# TCP relay: forwards client<->broker traffic untouched, except that it parks
# every InitProducerId (api_key 22) frame at a barrier and releases them all
# at once — guaranteeing the same-millisecond arrival that triggers the bug.
# ---------------------------------------------------------------------------

INIT_PRODUCER_ID_API_KEY = 22


class Relay:
    def __init__(self, expected):
        self.expected = expected
        self.parked = 0
        self.lock = threading.Lock()
        self.release = threading.Event()
        self.released_at = None
        up_host, up_port = UPSTREAM.rsplit(":", 1)
        self.upstream = (up_host, int(up_port))

    def start(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((RELAY_HOST, RELAY_PORT))
        self.listener.listen(64)
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(client,), daemon=True).start()

    def _serve(self, client):
        try:
            up = socket.create_connection(self.upstream, timeout=5)
        except OSError:
            client.close()
            return
        for s in (client, up):
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        threading.Thread(target=self._pump_frames, args=(client, up), daemon=True).start()
        threading.Thread(target=self._pump_raw, args=(up, client), daemon=True).start()

    @staticmethod
    def _recv_exact(sock, n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError
            buf += chunk
        return buf

    def _pump_frames(self, client, up):
        try:
            while True:
                header = self._recv_exact(client, 4)
                size = struct.unpack(">i", header)[0]
                payload = self._recv_exact(client, size)
                api_key = struct.unpack(">h", payload[:2])[0]
                if api_key == INIT_PRODUCER_ID_API_KEY and not self.release.is_set():
                    with self.lock:
                        self.parked += 1
                        if self.parked == 1:
                            t = threading.Timer(BARRIER_TIMEOUT_S, self._do_release)
                            t.daemon = True
                            t.start()
                        if self.parked >= self.expected:
                            self._do_release()
                    self.release.wait()
                up.sendall(header + payload)
        except (ConnectionError, OSError):
            pass
        finally:
            for s in (client, up):
                try:
                    s.close()
                except OSError:
                    pass

    def _do_release(self):
        if not self.release.is_set():
            self.released_at = time.time()
            self.release.set()

    def _pump_raw(self, up, client):
        try:
            while True:
                data = up.recv(65536)
                if not data:
                    break
                client.sendall(data)
        except OSError:
            pass
        finally:
            for s in (client, up):
                try:
                    s.close()
                except OSError:
                    pass


# ---------------------------------------------------------------------------
# Narration helpers
# ---------------------------------------------------------------------------

T0 = None  # set to the barrier release; all timestamps shown relative to it


def clock(t):
    rel = t - T0
    wall = time.strftime("%H:%M:%S", time.localtime(t)) + f".{int(t % 1 * 1000):03d}"
    return f"{wall} (T{rel:+.3f}s)"


def stage(n, title):
    print(f"\n─── STAGE {n}: {title} " + "─" * max(1, 58 - len(title)), flush=True)


def find_bug_line():
    """Resolve the kafka-python source line that performs the null-key
    transaction-coordinator lookup — the root cause."""
    import kafka.producer.transaction_manager as tm
    src, start = inspect.getsourcelines(tm.InitProducerIdHandler.handle_response)
    for off, line in enumerate(src):
        if "_lookup_coordinator(CoordinatorType.TRANSACTION, self.transactional_id" in line:
            return tm.__file__, start + off, line.strip()
    return tm.__file__, None, None


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

import kafka
from kafka import KafkaProducer
from kafka.errors import KafkaError

WITH_FIX = "--with-fix" in sys.argv
NO_IDEMPOTENCE = "--no-idempotence" in sys.argv

if WITH_FIX:
    # The proposed kafka-python fix, applied as a runtime patch: an
    # idempotent-only producer (transactional_id=None) has no transaction
    # coordinator to find — on a retriable InitProducerId error it should
    # just re-enqueue and retry, never emit FindCoordinator(key=None).
    # Upstream this belongs in InitProducerIdHandler.handle_response
    # (guard the _lookup_coordinator call with is_transactional()).
    from kafka.producer import transaction_manager as _tm

    _orig_lookup = _tm.TransactionManager._lookup_coordinator

    def _guarded_lookup(self, coord_type, coord_key):
        if coord_key is None:
            return  # nothing to look up; handler's reenqueue() will retry
        return _orig_lookup(self, coord_type, coord_key)

    _tm.TransactionManager._lookup_coordinator = _guarded_lookup

print("=" * 74)
if WITH_FIX:
    print(" FIX DEMO: same collision, patched client (null-key lookup guarded)")
elif NO_IDEMPOTENCE:
    print(" WORKAROUND DEMO: same startup, enable_idempotence=False")
else:
    print(" REPRODUCTION: the `socket disconnected` poison loop")
print(" kafka-python idempotent producer  x  Redpanda lazy id_allocator init")
print("=" * 74)

bug_file, bug_line, bug_src = find_bug_line()
if NO_IDEMPOTENCE:
    print("""
Same fresh broker, same barrier relay — but the producers are created with
enable_idempotence=False, so they never request a producer id. Stage 1 of
the causal chain (the concurrent first-ever InitProducerId collision) has
nothing to collide, and the loop never starts. This demonstrates the
config-only workaround; the real fix is the client patch (--with-fix).
""", flush=True)
else:
    print(f"""
The chain this run demonstrates, live:

  1. TRIGGER   {N_PRODUCERS} producers send their first-ever InitProducerId to a fresh
               broker in the SAME MILLISECOND. The first one processed starts
               Redpanda's lazy id_allocator creation and politely waits; the
               broker fast-fails the concurrent rest with retriable
               COORDINATOR_NOT_AVAILABLE (error_code=16).
  2. ROOT      kafka-python 3.0.7 answers that retriable error by looking up
     CAUSE     a TRANSACTION coordinator keyed by transactional_id — which is
               None for an idempotent-only producer:
                 {bug_file}:{bug_line}
                 {bug_src}
               On the wire that becomes FindCoordinator v4 with a NULL key.
  3. SLAM      Redpanda cannot parse a null compact string. It logs
               'std::out_of_range (Asked to read a 0 byte flex string)' and
               closes the TCP connection without responding.
  4. LOOP      kafka-python sees only a disconnect, re-enqueues the SAME
               doomed FindCoordinator (priority 0 — it permanently blocks the
               InitProducerId retry), reconnects, resends... every ~100 ms,
               forever. That loop is the `socket disconnected` ERROR spam.

In production this collision happens naturally when docker compose starts the
worker and retrier together; here a relay releases the parked InitProducerId
frames simultaneously so it happens every run instead of by coin flip.
""", flush=True)

up_host, up_port = UPSTREAM.rsplit(":", 1)
deadline = time.time() + 90
while time.time() < deadline:
    try:
        socket.create_connection((up_host, int(up_port)), timeout=0.3).close()
        break
    except OSError:
        time.sleep(0.1)
else:
    print("broker never opened its port", flush=True)
    sys.exit(2)

stage(1, "setup")
relay = Relay(expected=N_PRODUCERS)
relay.start()
print(f"  kafka-python {kafka.__version__} "
      f"({os.path.dirname(kafka.__file__)})")
print(f"  broker (fresh, id_allocator never initialized): {UPSTREAM}")
print(f"  barrier relay: {RELAY_HOST}:{RELAY_PORT} -> {UPSTREAM}")
if NO_IDEMPOTENCE:
    print(f"  creating {N_PRODUCERS} producers with enable_idempotence=False...",
          flush=True)
else:
    print(f"  creating {N_PRODUCERS} idempotent producers (transactional_id=None)...",
          flush=True)

producers = [None] * N_PRODUCERS

PRODUCER_CONFIG = dict(
    bootstrap_servers=[f"{RELAY_HOST}:{RELAY_PORT}"],
    acks="all", retries=5,
)
if NO_IDEMPOTENCE:
    PRODUCER_CONFIG["enable_idempotence"] = False


def make(i):
    end = time.time() + 30
    while time.time() < end:
        try:
            producers[i] = KafkaProducer(**PRODUCER_CONFIG)
            return
        except KafkaError:
            time.sleep(0.05)


threads = [threading.Thread(target=make, args=(i,), name=f"ctor-{i}")
           for i in range(N_PRODUCERS)]
for t in threads:
    t.start()
for t in threads:
    t.join()
if NO_IDEMPOTENCE:
    # no InitProducerId is ever sent, so the barrier can't fill — give the
    # startup the same collision window, then just observe
    relay.release.wait(timeout=BARRIER_TIMEOUT_S)
    T0 = relay.released_at or time.time()
    print(f"  {relay.parked} InitProducerId frames reached the barrier "
          f"(idempotency off — expected 0)\n  observing for {OBSERVE_S}s...",
          flush=True)
else:
    relay.release.wait(timeout=15)
    T0 = relay.released_at or time.time()
    print(f"  all {relay.parked} InitProducerId frames parked at the barrier and "
          f"released together\n  observing for {OBSERVE_S}s...", flush=True)
time.sleep(OBSERVE_S)

# ---------------------------- narrate ---------------------------------------

resps = recorder.by_kind("init_resp")
winners = [(t, p, d) for t, p, k, d in resps if d[1] == 0]
victims = [(t, p, d) for t, p, k, d in resps if d[1] in (15, 16)]
enqueues = recorder.by_kind("poison_enqueue")
wire_sends = recorder.by_kind("poison_wire")
disconnects = [e for e in recorder.by_kind("disconnect") if e[0] >= T0]
sent = recorder.by_kind("init_sent")

if NO_IDEMPOTENCE:
    stage(2, "nothing to collide — no InitProducerId was ever sent")
    print(f"  InitProducerId requests sent: {len(sent)}")
    print(f"  null-key FindCoordinator enqueued: {len(enqueues)}")
    print(f"  `socket disconnected` events: {len(disconnects)}")
    print("""  WHY: the trigger needs concurrent first-ever InitProducerId requests.
  With enable_idempotence=False the producer skips producer-id allocation
  entirely, so the fast-fail -> null-key lookup -> slam loop has no entry
  point — even against a fresh broker with an uninitialized id_allocator.""")

    stage(3, "producers still deliver — the workaround holds")
    delivered = 0
    for i, prod in enumerate(producers):
        if prod is None:
            print(f"  producer-{i}: never constructed (see log)")
            continue
        try:
            md = prod.send("wedge-noidem-probe", b"ping").get(timeout=10)
            delivered += 1
            print(f"  producer-{i}: delivered to "
                  f"{md.topic}[{md.partition}]@{md.offset}")
        except KafkaError as e:
            print(f"  producer-{i}: send FAILED: {e!r}")
    print("""  TRADE-OFF: without idempotence a retried produce can be written twice
  (duplicates on broker-side retry). Acceptable as a config-only stopgap;
  the real fix is the client patch (--with-fix).""")

    healthy = (not sent and not enqueues and len(disconnects) < 20
               and delivered == N_PRODUCERS)
    print("\n" + "=" * 74)
    print(f" VERDICT: {'WORKAROUND VERIFIED' if healthy else 'UNEXPECTED — see log'} — "
          f"{len(sent)} InitProducerIds, {len(enqueues)} poison lookups, "
          f"{len(disconnects)} disconnects, "
          f"{delivered}/{N_PRODUCERS} test sends delivered")
    print(f" full client DEBUG log: {LOG_PATH}")
    print("=" * 74, flush=True)
    os._exit(0 if healthy else 1)

stage(2, "the trigger — one creation, concurrent fast-fails")
if sent:
    ts = [t for t, _, _, _ in sent[:N_PRODUCERS]]
    print(f"  {len(sent[:N_PRODUCERS])} InitProducerIds hit the broker within "
          f"{(max(ts) - min(ts)) * 1000:.1f} ms of each other:")
for t, p, d in sorted(victims, key=lambda e: e[0]):
    lat, code, _ = d
    print(f"  {clock(t)}  {p:<12} error_code={code} COORDINATOR_NOT_AVAILABLE "
          f"after {lat:.1f} ms   <- FAST-FAILED")
for rank, (t, p, d) in enumerate(sorted(winners, key=lambda e: e[0])):
    lat, _, pid = d
    note = ("<- this one triggered id_allocator creation and waited" if rank == 0
            else "<- retry succeeded once the allocator was ready")
    print(f"  {clock(t)}  {p:<12} SUCCESS after {lat:.0f} ms, producer_id={pid} "
          f"  {note}")
print(f"""  WHY: only requests processed concurrently with the creation-triggering
  request fast-fail; even 6 ms later they wait politely. This is why the bug
  needs services booting together and why it looks random in production.""")

if WITH_FIX:
    stage(3, "the fix in action — retriable error handled by plain retry")
    firsts = {}
    for t, p, d in sorted(winners, key=lambda e: e[0]):
        firsts.setdefault(p, (t, d))
    for p, (t, d) in sorted(firsts.items()):
        lat, _, pid = d
        print(f"  {clock(t)}  {p:<12} recovered: InitProducerId retried -> "
              f"producer_id={pid}")
    print(f"""  With the null-key lookup guarded, the {len(victims)} fast-failed producers
  simply re-enqueued InitProducerId and retried (~100 ms backoff) until the
  id_allocator finished initializing. No FindCoordinator(key=None) was ever
  sent: {len(enqueues)} poison lookups; {len(disconnects)} stray disconnects
  (a poison loop would show hundreds).""")
    recovered = sorted({p for _, p, _ in winners})
    fixed_ok = (not enqueues and len(disconnects) < 20
                and len(recovered) == N_PRODUCERS)
    print("\n" + "=" * 74)
    print(f" VERDICT: {'FIX VERIFIED' if fixed_ok else 'UNEXPECTED — see log'} — "
          f"{len(victims)} fast-fails, {len(recovered)}/{N_PRODUCERS} producers "
          f"recovered, no poison loop")
    print(f" full client DEBUG log: {LOG_PATH}")
    print("=" * 74, flush=True)
    os._exit(0 if fixed_ok else 1)

stage(3, "the root cause — null-key coordinator lookup (kafka-python)")
if enqueues:
    t, p, _, msg = enqueues[0]
    print(f"  {clock(t)}  {p} reacts to the retriable error INSIDE\n"
          f"  {bug_file}:{bug_line}\n"
          f"      {bug_src}\n"
          f"  with transactional_id=None (idempotent-only producer!), producing:\n"
          f"      {msg.split('request ', 1)[-1]}")
    print(f"  {len(enqueues)} such lookups were enqueued across "
          f"{len({p for _, p, _, _ in enqueues})} wedged producers.")
else:
    print("  (no poison lookup observed)")

stage(4, "the broker slams the door — unparseable null, connection killed")
print(f"""  Each FindCoordinator(coordinator_keys=[None]) frame ({len(wire_sends)} sent on the
  wire during the observe window) makes Redpanda throw while decoding the
  null compact string and close the connection with NO response.
  Broker-side proof (printed by wedge-repro.sh after this script):
      kafka - connection_context.cc - Disconnected ... (short read),
      std::out_of_range (Asked to read a 0 byte flex string)""")

stage(5, "the poison loop — the `socket disconnected` spam you saw")
wedged_set = sorted({p for _, p, _, _ in enqueues})
n_wedged = len(wedged_set)
if disconnects and n_wedged:
    dur = max(OBSERVE_S, disconnects[-1][0] - T0)
    print(f"  first disconnect: {clock(disconnects[0][0])}")
    print(f"  disconnects in {OBSERVE_S}s observe window: {len(disconnects)} "
          f"(~{len(disconnects) / dur / n_wedged:.0f}/s per wedged producer)")
print(f"""  kafka-python treats each slam as a retriable disconnect and re-enqueues
  the SAME null-key FindCoordinator. It has queue priority 0, so the
  InitProducerId retry is blocked behind it FOREVER:
      wedged producers ({n_wedged}/{N_PRODUCERS}): {', '.join(wedged_set) or '-'}
      their producer_id is still -1 -> every send() would block and the
      service's publish() dies with KafkaTimeoutError after 60s -> the
      container crashes uncommitted and the restart heals it (why the
      pipeline stayed 'functionally fine' despite the log spam).""")

reproduced = bool(enqueues) and len(disconnects) > 20
print("\n" + "=" * 74)
print(f" VERDICT: {'REPRODUCED' if reproduced else 'NOT REPRODUCED'} — "
      f"{len(victims)} fast-fails, {n_wedged} producers wedged, "
      f"{len(disconnects)} loop disconnects in {OBSERVE_S}s")
print(f" full client DEBUG log: {LOG_PATH}")
print("=" * 74, flush=True)

os._exit(0 if reproduced else 1)  # skip close(): wedged producers never flush
