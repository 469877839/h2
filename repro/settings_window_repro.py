"""
In-memory, two-peer reproduction of h2's SETTINGS / flow-control semantics.

No sockets: two H2Connection objects exchange serialized frames through plain
byte strings. Every step prints the concrete state objects, events and frames
involved, using deliberately tiny windows (50/100 bytes) so the numbers are
easy to follow.

Run:  .venv/bin/python repro/settings_window_repro.py
"""
from __future__ import annotations

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.settings import SettingCodes, Settings
from h2.exceptions import (
    FlowControlError,
    FrameTooLargeError,
    InvalidSettingsValueError,
    ProtocolError,
)
from hyperframe.frame import (
    Frame,
    SettingsFrame,
    WindowUpdateFrame,
    DataFrame,
    HeadersFrame,
)
from hpack import Encoder

IWS = SettingCodes.INITIAL_WINDOW_SIZE
MFS = SettingCodes.MAX_FRAME_SIZE
HEADERS = [
    (":method", "GET"),
    (":path", "/"),
    (":scheme", "https"),
    (":authority", "example.com"),
]
encoder = Encoder()


# --------------------------------------------------------------------------
# Small wire / state helpers
# --------------------------------------------------------------------------
def wire_frames(data: bytes) -> list[Frame]:
    """Decode a byte string into hyperframe frames (the wire view)."""
    frames: list[Frame] = []
    view = memoryview(data)
    offset = 0
    while offset < len(view):
        frame, body_len = Frame.parse_frame_header(view[offset:offset + 9])
        frame.parse_body(view[offset + 9:offset + 9 + body_len])
        frames.append(frame)
        offset += 9 + body_len
    return frames


def frame_summary(frame: Frame) -> str:
    name = type(frame).__name__
    flags = ",".join(frame.flags) if frame.flags else "-"
    extra = ""
    if isinstance(frame, SettingsFrame) and frame.settings:
        extra = " " + str(dict(frame.settings))
    if isinstance(frame, WindowUpdateFrame):
        extra = f" stream={frame.stream_id} inc={frame.window_increment}"
    if isinstance(frame, DataFrame):
        extra = f" stream={frame.stream_id} len={len(frame.data)}"
    return f"{name}[{flags}]{extra}"


def settings_frame(mapping: dict | None = None, ack: bool = False) -> SettingsFrame:
    frame = SettingsFrame(0)
    if ack:
        frame.flags.add("ACK")
    else:
        frame.settings = dict(mapping or {})
    return frame


def headers_frame(stream_id: int) -> HeadersFrame:
    frame = HeadersFrame(stream_id)
    frame.data = encoder.encode(HEADERS)
    return frame


def iws_deque(conn: H2Connection, local: bool) -> list[int]:
    """The raw stack: [current, pending1, pending2, ...]."""
    store = conn.local_settings if local else conn.remote_settings
    return list(store._settings[IWS])


def snapshot(conn: H2Connection, name: str) -> str:
    lines = [
        f"{name}: localIWS={conn.local_settings.initial_window_size} "
        f"stack={iws_deque(conn, True)} | remoteIWS={conn.remote_settings.initial_window_size} "
        f"stack={iws_deque(conn, False)}",
        f"{name}: max_outbound_frame_size={conn.max_outbound_frame_size} "
        f"max_inbound_frame_size={conn.max_inbound_frame_size} "
        f"conn_outbound_window={conn.outbound_flow_control_window}",
    ]
    for stream_id in sorted(conn.streams):
        stream = conn.streams[stream_id]
        lines.append(
            f"{name}: stream {stream_id} state={stream.state_machine.state!s} "
            f"outbound_window={stream.outbound_flow_control_window} "
            f"inbound_window={stream.inbound_flow_control_window}"
        )
    return "\n".join(lines)


def pump(sender: H2Connection, receiver: H2Connection) -> None:
    """Deliver everything queued on sender into receiver; print the wire."""
    data = sender.data_to_send()
    if not data:
        print("  (no frames queued)")
        return
    preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
    frame_data = data
    if data.startswith(preface):
        print("  WIRE -> <client connection preface>")
        frame_data = data[len(preface):]
    for frame in wire_frames(frame_data):
        print("  WIRE ->", frame_summary(frame))
    receiver.receive_data(data)


def section(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def check(label: str, actual, expected) -> None:
    status = "PASS" if actual == expected else "FAIL"
    print(f"  [{status}] {label}: actual={actual!r} expected={expected!r}")
    if actual != expected:
        raise AssertionError(label)


def make_pair() -> tuple[H2Connection, H2Connection]:
    client = H2Connection(H2Configuration(client_side=True, header_encoding="utf-8"))
    server = H2Connection(H2Configuration(client_side=False, header_encoding="utf-8"))
    client.initiate_connection()
    server.initiate_connection()
    # Handshake: client SETTINGS -> server; server SETTINGS+ACK -> client.
    pump(client, server)
    pump(server, client)
    client.data_to_send()  # discard the client's automatic ACK of server SETTINGS
    server.data_to_send()
    return client, server


# --------------------------------------------------------------------------
# Scenario 1: WE change INITIAL_WINDOW_SIZE twice (100, then 50).
# Covers: pending stack, ACK ordering, streams created in between, negative
# inbound windows on existing streams, and WINDOW_UPDATE healing.
# --------------------------------------------------------------------------
def scenario_local_double_iws() -> None:
    section("Scenario 1: local double INITIAL_WINDOW_SIZE (65535 -> 100 -> 50)")
    client, server = make_pair()

    client.send_headers(1, HEADERS)
    pump(client, server)
    print(snapshot(server, "server"))
    check("existing stream 1 starts at default inbound window",
          server.streams[1].inbound_flow_control_window, 65535)

    print("\n-- client.update_settings(IWS=100), then (IWS=50), no ACK yet --")
    client.update_settings({IWS: 100})
    client.update_settings({IWS: 50})
    print("  client local stack after two updates:", iws_deque(client, True),
          "(head is still the CURRENT value)")
    check("current local IWS unchanged while pending",
          client.local_settings.initial_window_size, 65535)
    pump(client, server)
    # The server immediately adopts each remote change (its deque never
    # stacks: it acknowledges inside the same receive_data() call). Keep the
    # two real ACK frames the server queued, to return them one at a time.
    pending_acks = wire_frames(server.data_to_send())
    check("server queued one ACK per SETTINGS frame", len(pending_acks), 2)
    print(snapshot(server, "server"))
    check("server remote IWS immediately 50",
          server.remote_settings.initial_window_size, 50)
    check("existing stream 1 outbound window adjusted by deltas",
          server.streams[1].outbound_flow_control_window, 50)
    check("existing stream 1 inbound window untouched (peer's own window)",
          server.streams[1].inbound_flow_control_window, 65535)
    check("connection outbound window untouched by stream-level setting",
          server.outbound_flow_control_window, 65535)

    print("\n-- ACK #1 returns to the client --")
    events = client.receive_data(pending_acks[0].serialize())
    print("  client events:", [type(e).__name__ for e in events],
          "changed:", {str(k): (v.original_value, v.new_value)
                       for k, v in events[0].changed_settings.items()})
    check("client current IWS after ACK1", client.local_settings.initial_window_size, 100)
    check("client stack after ACK1", iws_deque(client, True), [100, 50])

    print("\n-- new stream 3 created BETWEEN the two ACKs --")
    client.send_headers(3, HEADERS)
    pump(client, server)
    check("stream 3 inbound born at server's own local IWS (still 65535)",
          server.streams[3].inbound_flow_control_window, 65535)
    check("stream 3 outbound side uses remote IWS 50",
          server.streams[3].outbound_flow_control_window, 50)

    print("\n-- ACK #2 returns to the client --")
    events = client.receive_data(pending_acks[1].serialize())
    print("  client events:", [type(e).__name__ for e in events],
          "changed:", {str(k): (v.original_value, v.new_value)
                       for k, v in events[0].changed_settings.items()})
    check("client current IWS after ACK2", client.local_settings.initial_window_size, 50)
    check("client stack after ACK2", iws_deque(client, True), [50])

    print("\n-- stream created AFTER both ACKs --")
    client.send_headers(5, HEADERS)
    pump(client, server)
    # Inbound uses the SERVER's own local IWS (never changed -> 65535);
    # the client's advertised 50 only sizes the stream's outbound window.
    check("stream 5 inbound born from server local IWS (65535)",
          server.streams[5].inbound_flow_control_window, 65535)
    check("stream 5 outbound born from client IWS (50)",
          server.streams[5].outbound_flow_control_window, 50)

    print("\n-- negative INBOUND window: server shrinks its OWN IWS on a busy stream --")
    # Stream 1's inbound window is still 65535; consume 100 bytes first.
    server.receive_data(DataFrame(1, data=b"a" * 100).serialize())
    check("stream 1 inbound after 100 bytes",
          server.streams[1].inbound_flow_control_window, 65435)
    # Server advertises IWS 100, then 0; each round-trip goes through the
    # real wire (client auto-ACKs each SETTINGS).
    server.update_settings({IWS: 100})
    pump(server, client)
    server.receive_data(wire_frames(client.data_to_send())[0].serialize())
    check("stream 1 inbound after IWS 65535->100",
          server.streams[1].inbound_flow_control_window, 0)
    server.update_settings({IWS: 0})
    pump(server, client)
    server.receive_data(wire_frames(client.data_to_send())[0].serialize())
    check("stream 1 inbound goes negative after IWS 100->0",
          server.streams[1].inbound_flow_control_window, -100)

    print("\n-- inbound side: even an EMPTY data frame tears the connection down --")
    try:
        server.receive_data(DataFrame(1, data=b"").serialize())
        raise AssertionError("negative-window DATA should be rejected")
    except FlowControlError as exc:
        print("  empty DATA on -100 inbound ->", type(exc).__name__,
              "error_code:", exc.error_code)
    goaway = wire_frames(server.data_to_send())
    check("connection state after inbound violation",
          server.state_machine.state.name, "CLOSED")
    check("a GOAWAY is queued", type(goaway[0]).__name__, "GoAwayFrame")
    check("GOAWAY error code", int(goaway[0].error_code), 3)  # FLOW_CONTROL_ERROR
    # close() defaults last_stream_id to the highest inbound id seen so far.
    check("GOAWAY last stream id (highest seen)", goaway[0].last_stream_id, 5)
    check("connection-level inbound window never tracks IWS changes",
          server._inbound_flow_control_window_manager.current_window_size,
          65535 - 100)  # IWS adjustments skip the connection window (RFC 9113 6.9.2)


# --------------------------------------------------------------------------
# Scenario 2: the PEER changes INITIAL_WINDOW_SIZE twice (100, then 50),
# with a stream created in between and WINDOW_UPDATE frames interleaved.
# This is the sender's point of view: the value that actually governs
# send_data() is the per-stream outbound window, adjusted immediately when
# the remote SETTINGS is consumed (the auto-ACK is generated in the same
# receive_data() call).
# --------------------------------------------------------------------------
def scenario_remote_double_iws() -> None:
    section("Scenario 2: remote double INITIAL_WINDOW_SIZE (sender view)")
    client, server = make_pair()

    client.send_headers(1, HEADERS)
    pump(client, server)
    print(snapshot(client, "client"))
    check("stream 1 sendable at handshake", client.local_flow_control_window(1), 65535)

    print("\n-- remote SETTINGS IWS=100 arrives --")
    events = client.receive_data(settings_frame({IWS: 100}).serialize())
    client.data_to_send()  # drain the automatic ACK
    print("  events:", [type(e).__name__ for e in events],
          "changed:", {str(k): (v.original_value, v.new_value)
                       for k, v in events[0].changed_settings.items()})
    check("remote IWS current immediately 100",
          client.remote_settings.initial_window_size, 100)
    check("stream 1 outbound shrunk by delta",
          client.streams[1].outbound_flow_control_window, 100)

    print("\n-- remote SETTINGS IWS=50 arrives --")
    client.receive_data(settings_frame({IWS: 50}).serialize())
    client.data_to_send()
    check("remote IWS current 50", client.remote_settings.initial_window_size, 50)
    check("stream 1 outbound now 50", client.streams[1].outbound_flow_control_window, 50)
    check("remote stack never accumulates (auto-ack in receive_data)",
          iws_deque(client, False), [50])

    print("\n-- stream 3 created after both remote changes --")
    client.send_headers(3, HEADERS)
    pump(client, server)
    check("new stream born at current remote IWS 50",
          client.streams[3].outbound_flow_control_window, 50)

    print("\n-- spend the window, then a further shrink makes it negative --")
    client.send_data(3, b"a" * 50)
    check("stream 3 outbound exhausted", client.streams[3].outbound_flow_control_window, 0)
    client.receive_data(settings_frame({IWS: 0}).serialize())
    client.data_to_send()
    check("shrink 50->0 drives stream 3 outbound negative",
          client.streams[3].outbound_flow_control_window, -50)

    print("\n-- outbound side: non-empty DATA blocked, empty DATA still legal --")
    try:
        client.send_data(3, b"b")
        raise AssertionError("over-window send should be rejected")
    except FlowControlError as exc:
        print("  send_data(1 byte) ->", type(exc).__name__)
    client.send_data(3, b"")  # used for END_STREAM-only frames
    check("empty send allowed; window unchanged",
          client.streams[3].outbound_flow_control_window, -50)

    print("\n-- WINDOW_UPDATE on the stream lifts the window again --")
    wu = WindowUpdateFrame(3)
    wu.window_increment = 80
    client.receive_data(wu.serialize())
    check("stream 3 outbound after WU+80", client.streams[3].outbound_flow_control_window, 30)
    client.send_data(3, b"c" * 30)
    check("30 bytes sent; stream 3 outbound back to 0",
          client.streams[3].outbound_flow_control_window, 0)

    print("\n-- connection-level WINDOW_UPDATE is independent of IWS --")
    before = client.outbound_flow_control_window
    conn_wu = WindowUpdateFrame(0)
    conn_wu.window_increment = 10
    client.receive_data(conn_wu.serialize())
    check("connection outbound window grew by 10",
          client.outbound_flow_control_window, before + 10)


# --------------------------------------------------------------------------
# Scenario 3: MAX_FRAME_SIZE, local vs remote, existing vs new streams,
# and the inbound enforcement point.
# --------------------------------------------------------------------------
def scenario_max_frame_size() -> None:
    section("Scenario 3: MAX_FRAME_SIZE application points")
    client, server = make_pair()
    client.send_headers(1, HEADERS)
    pump(client, server)

    check("default max_outbound", client.max_outbound_frame_size, 16384)
    check("default max_inbound", client.max_inbound_frame_size, 16384)
    check("existing stream inherits connection max",
          client.streams[1].max_outbound_frame_size, 16384)

    print("\n-- remote raises MAX_FRAME_SIZE to 32768 (takes effect immediately) --")
    client.receive_data(settings_frame({MFS: 32768}).serialize())
    client.data_to_send()
    check("connection max_outbound updated", client.max_outbound_frame_size, 32768)
    check("existing stream retroactively updated",
          client.streams[1].max_outbound_frame_size, 32768)
    check("max_inbound untouched by remote setting",
          client.max_inbound_frame_size, 16384)
    client.send_data(1, b"x" * 20000)
    client.data_to_send()
    print("  20000-byte DATA frame accepted under new limit")

    print("\n-- local raises MAX_FRAME_SIZE: only inbound, and only after ACK --")
    client.update_settings({MFS: 32768})
    client.data_to_send()
    check("max_inbound unchanged before ACK", client.max_inbound_frame_size, 16384)
    client.receive_data(settings_frame(ack=True).serialize())
    check("max_inbound applied on ACK", client.max_inbound_frame_size, 32768)

    print("\n-- inbound enforcement lives in FrameBuffer, before any dispatch --")
    try:
        client.receive_data(DataFrame(3, data=b"y" * 33000).serialize())
        raise AssertionError("overlong frame should be rejected")
    except FrameTooLargeError as exc:
        print("  33000-byte frame ->", type(exc).__name__)
    check("frame_buffer threshold tracks max_inbound",
          client.incoming_buffer.max_frame_size, 32768)

    print("\n-- out-of-range value rejected at SETTINGS validation --")
    fresh, _ = make_pair()
    try:
        fresh.receive_data(settings_frame({MFS: 100}).serialize())
        raise AssertionError("invalid MFS should be rejected")
    except InvalidSettingsValueError as exc:
        print("  MFS=100 ->", type(exc).__name__, "error_code:", exc.error_code,
              "(PROTOCOL_ERROR; remote_settings is not mutated)")
    check("invalid value leaves remote MFS at default",
          fresh.remote_settings.max_frame_size, 16384)


# --------------------------------------------------------------------------
# Scenario 4: receiving a SETTINGS ACK when nothing is pending.
# RFC 9113 section 6.5.3 says this is a connection error of type
# PROTOCOL_ERROR; observe what h2 actually does.
# --------------------------------------------------------------------------
def scenario_spurious_ack() -> None:
    section("Scenario 4: SETTINGS ACK with no pending settings")
    client, server = make_pair()
    client.send_headers(1, HEADERS)
    pump(client, server)
    # The handshake ACKs were already exchanged inside make_pair(); the
    # client's local stack holds only single (current) values.
    check("no pending local settings", iws_deque(client, True), [65535])

    events = client.receive_data(settings_frame(ack=True).serialize())
    print("  events:", [(type(e).__name__, e.changed_settings) for e in events])
    check("spurious ACK silently accepted", client.state_machine.state.name, "CLIENT_OPEN")
    check("event still emitted, with empty changed_settings",
          (type(events[0]).__name__, events[0].changed_settings),
          ("SettingsAcknowledged", {}))

    events = client.receive_data(settings_frame(ack=True).serialize())
    check("second spurious ACK also accepted", client.state_machine.state.name, "CLIENT_OPEN")
    check("no GOAWAY queued", client.data_to_send(), b"")


# --------------------------------------------------------------------------
# Scenario 5: the Settings object itself -- the "current value" vs the
# "pending values" stack, drained one level per acknowledge().
# --------------------------------------------------------------------------
def scenario_settings_stack() -> None:
    section("Scenario 5: Settings stack semantics (unit level)")
    store = Settings()
    store.initial_window_size = 100
    store.initial_window_size = 50
    print("  raw deque:", list(store._settings[IWS]),
          "-> head is the CURRENT value, the rest are pending")
    check("current value while two changes pending", store.initial_window_size, 65535)

    changes = store.acknowledge()
    print("  acknowledge() #1:", {str(k): (v.original_value, v.new_value)
                                  for k, v in changes.items()})
    check("first ack applies 65535 -> 100", store.initial_window_size, 100)

    changes = store.acknowledge()
    print("  acknowledge() #2:", {str(k): (v.original_value, v.new_value)
                                  for k, v in changes.items()})
    check("second ack applies 100 -> 50", store.initial_window_size, 50)

    changes = store.acknowledge()
    check("ack on empty stack returns {}", changes, {})
    check("value stable", store.initial_window_size, 50)


if __name__ == "__main__":
    scenario_settings_stack()
    scenario_local_double_iws()
    scenario_remote_double_iws()
    scenario_max_frame_size()
    scenario_spurious_ack()
    print("\nAll scenarios completed: every check passed.")
