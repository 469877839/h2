"""
test_send_queue
~~~~~~~~~~~~~~~

Tests for the optional outbound data queue in h2.

These tests use deliberately tiny flow control windows and assert on the
exact frame sequences emitted, so fairness, byte conservation and end markers
are verified directly rather than through throughput or timing.
"""
from __future__ import annotations

import pytest
from hyperframe.frame import DataFrame, Frame, PingFrame

import h2.connection
import h2.exceptions
import h2.frame_buffer
import h2.send_queue
import h2.settings
import h2.stream


HEADERS = [
    (":authority", "example.com"),
    (":path", "/"),
    (":scheme", "https"),
    (":method", "POST"),
]


def _frames(data: bytes) -> list[Frame]:
    """
    Parse a serialized frame stream into frame objects.
    """
    buffer = h2.frame_buffer.FrameBuffer(server=False)
    buffer.max_frame_size = 2**24
    buffer.add_data(data)
    return list(buffer)


def _data_frames(data: bytes) -> list[tuple[int, bytes, bool]]:
    """
    Return ``(stream_id, payload, end_stream)`` for every DATA frame in the
    serialized output, in order.
    """
    return [
        (frame.stream_id, frame.data, "END_STREAM" in frame.flags)
        for frame in _frames(data)
        if isinstance(frame, DataFrame)
    ]


def _open_connection(frame_factory, *, max_queued_data=None, window=None, stream_ids=(1,)):
    """
    Build a client connection, optionally shrinking the remote
    SETTINGS_INITIAL_WINDOW_SIZE so that tiny windows are in play.
    """
    c = h2.connection.H2Connection(max_queued_data=max_queued_data)
    c.initiate_connection()
    for stream_id in stream_ids:
        c.send_headers(stream_id, HEADERS)
    c.clear_outbound_data_buffer()
    if window is not None:
        f = frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: window},
        )
        c.receive_data(f.serialize())
        c.clear_outbound_data_buffer()
    return c


class TestSendQueueBasic:
    """
    Basic queue semantics and the fact that plain send_data is unchanged.
    """

    def test_send_data_still_emits_immediately(self) -> None:
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.send_headers(1, HEADERS)
        c.clear_outbound_data_buffer()

        c.send_data(1, b"abc")

        assert _data_frames(c.data_to_send()) == [(1, b"abc", False)]

    def test_send_data_still_rejects_window_overflow(self) -> None:
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.send_headers(1, HEADERS)
        c.clear_outbound_data_buffer()
        c.outbound_flow_control_window = 2

        with pytest.raises(h2.exceptions.FlowControlError):
            c.send_data(1, b"abc")

    def test_queue_data_emits_immediately_when_window_open(self) -> None:
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.send_headers(1, HEADERS)
        c.clear_outbound_data_buffer()

        accepted = c.queue_data(1, b"hello", end_stream=True)

        assert accepted == 5
        assert c.pending_queued_data(1) == 0
        assert _data_frames(c.data_to_send()) == [(1, b"hello", True)]

    def test_queue_data_accepts_when_window_is_zero(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0

        accepted = c.queue_data(1, b"abcdef")

        assert accepted == 6
        assert c.pending_queued_data(1) == 6
        assert _data_frames(c.data_to_send()) == []

    def test_empty_data_is_a_noop(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)

        assert c.queue_data(1, b"") == 0
        assert c.pending_queued_data(1) == 0
        assert c.data_to_send() == b""

    def test_empty_end_stream_flushes_at_zero_window(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0

        accepted = c.queue_data(1, b"", end_stream=True)

        assert accepted == 0
        assert c.pending_queued_data(1) == 0
        assert _data_frames(c.data_to_send()) == [(1, b"", True)]

    def test_memoryview_is_accepted(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0

        accepted = c.queue_data(1, memoryview(b"mv-data"))

        assert accepted == 7
        assert c.pending_queued_data(1) == 7

    def test_queue_data_unknown_stream(self) -> None:
        c = h2.connection.H2Connection()
        c.initiate_connection()
        with pytest.raises(h2.exceptions.NoSuchStreamError):
            c.queue_data(99, b"x")

    def test_queue_data_rejects_closed_stream(self, frame_factory) -> None:
        c = _open_connection(frame_factory)
        c.reset_stream(1)

        with pytest.raises(h2.exceptions.StreamClosedError):
            c.queue_data(1, b"x")

    def test_queue_data_rejects_stream_after_end_stream(self, frame_factory) -> None:
        c = _open_connection(frame_factory)
        c.send_data(1, b"abc", end_stream=True)
        c.clear_outbound_data_buffer()

        with pytest.raises(h2.exceptions.ProtocolError):
            c.queue_data(1, b"d")

    def test_constructor_rejects_negative_limit(self) -> None:
        with pytest.raises(ValueError, match="max_queued_data"):
            h2.connection.H2Connection(max_queued_data=-1)


class TestSendQueueFraming:
    """
    Frame-level checks: window limits, frame size limits, byte conservation
    and END_STREAM placement.
    """

    def test_byte_conservation_with_one_byte_windows(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        payload = b"0123456789"
        c.queue_data(1, payload, end_stream=True)
        c.clear_outbound_data_buffer()

        sent = []
        for _ in range(len(payload)):
            c.receive_data(frame_factory.build_window_update_frame(0, 1).serialize())
            c.receive_data(frame_factory.build_window_update_frame(1, 1).serialize())
            sent.extend(_data_frames(c.data_to_send()))

        assert b"".join(data for _, data, _ in sent) == payload
        assert [end for _, _, end in sent] == [False] * 9 + [True]
        assert all(len(data) == 1 for _, data, _ in sent)
        assert c.pending_queued_data(1) == 0

    def test_connection_window_limits_total_output(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=1000, stream_ids=(1, 3))
        c.outbound_flow_control_window = 3
        c.queue_data(1, b"aaaaa")
        c.queue_data(3, b"bbbbb")

        # Only 3 bytes may leave the connection per pass. Stream 1 is at the
        # head of the rotation, so it takes the whole connection window.
        assert _data_frames(c.data_to_send()) == [(1, b"aaa", False)]

        # Stream 1 still owns the head of the rotation and finishes its tail,
        # then stream 3 takes the remaining connection byte.
        c.receive_data(frame_factory.build_window_update_frame(0, 3).serialize())
        assert _data_frames(c.data_to_send()) == [
            (1, b"aa", False),
            (3, b"b", False),
        ]

        c.receive_data(frame_factory.build_window_update_frame(0, 4).serialize())
        assert _data_frames(c.data_to_send()) == [(3, b"bbbb", False)]
        assert c.pending_queued_data() == 0

    def test_stream_window_limits_per_stream_output(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=2, stream_ids=(1, 3))
        c.queue_data(1, b"aaaa")
        c.queue_data(3, b"bbbb")

        assert _data_frames(c.data_to_send()) == [
            (1, b"aa", False),
            (3, b"bb", False),
        ]

        c.receive_data(frame_factory.build_window_update_frame(1, 2).serialize())
        assert _data_frames(c.data_to_send()) == [(1, b"aa", False)]

        c.receive_data(frame_factory.build_window_update_frame(3, 2).serialize())
        assert _data_frames(c.data_to_send()) == [(3, b"bb", False)]
        assert c.pending_queued_data() == 0

    def test_max_frame_size_splits_payload(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=100000)
        c.outbound_flow_control_window = 100000
        payload = bytes((i % 256 for i in range(40000)))
        c.queue_data(1, payload)

        frames = _data_frames(c.data_to_send())
        sizes = [len(data) for _, data, _ in frames]
        assert sizes == [16384, 16384, 7232]
        assert b"".join(data for _, data, _ in frames) == payload

    def test_settings_max_frame_size_change_is_respected(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        payload = bytes((i % 256 for i in range(40000)))
        c.queue_data(1, payload)
        c.clear_outbound_data_buffer()

        # Grow the peer's frame size while the payload is still queued.
        c.receive_data(frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.MAX_FRAME_SIZE: 32768},
        ).serialize())
        c.clear_outbound_data_buffer()

        c.receive_data(frame_factory.build_window_update_frame(0, 100000).serialize())
        c.receive_data(frame_factory.build_window_update_frame(1, 100000).serialize())

        frames = _data_frames(c.data_to_send())
        assert [len(data) for _, data, _ in frames] == [32768, 7232]
        assert b"".join(data for _, data, _ in frames) == payload


class TestSendQueueFairness:
    """
    Byte-level round-robin scheduling across multiple queued streams.
    """

    def test_streams_alternate_per_frame(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=1000, stream_ids=(1, 3))
        c.max_outbound_frame_size = 2
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"11111")
        c.queue_data(3, b"33333")

        c.receive_data(frame_factory.build_window_update_frame(0, 4).serialize())
        frames = _data_frames(c.data_to_send())

        assert frames == [
            (1, b"11", False),
            (3, b"33", False),
        ]
        assert c.pending_queued_data(1) == 3
        assert c.pending_queued_data(3) == 3

        c.receive_data(frame_factory.build_window_update_frame(0, 6).serialize())
        frames = _data_frames(c.data_to_send())

        assert frames == [
            (1, b"11", False),
            (3, b"33", False),
            (1, b"1", False),
            (3, b"3", False),
        ]
        assert c.pending_queued_data() == 0

    def test_large_body_does_not_starve_small_stream(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=1000, stream_ids=(1, 3))
        c.max_outbound_frame_size = 2
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"a" * 100)
        c.queue_data(3, b"bb")

        c.receive_data(frame_factory.build_window_update_frame(0, 6).serialize())
        frames = _data_frames(c.data_to_send())

        # The small stream is served second, before the large one gets a
        # second turn, and finishes completely within this pass.
        assert frames == [
            (1, b"aa", False),
            (3, b"bb", False),
            (1, b"aa", False),
        ]
        assert c.pending_queued_data(3) == 0
        assert c.pending_queued_data(1) == 96

    def test_blocked_stream_does_not_retry_within_a_pass(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=1000, stream_ids=(1, 3, 5))
        c.outbound_flow_control_window = 5
        c.queue_data(1, b"aaaa")   # 4 bytes fit, leaves 1
        c.queue_data(3, b"bb")     # 1 byte fits, 1 remains
        c.queue_data(5, b"ccccc")  # connection window exhausted: blocked

        frames = _data_frames(c.data_to_send())
        assert frames == [(1, b"aaaa", False), (3, b"b", False)]

        c.receive_data(frame_factory.build_window_update_frame(0, 6).serialize())
        frames = _data_frames(c.data_to_send())
        assert frames == [(3, b"b", False), (5, b"ccccc", False)]
        assert c.pending_queued_data() == 0


class TestSendQueueEndStream:
    """
    END_STREAM handling with queued payloads.
    """

    def test_end_stream_attaches_to_final_data_frame(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"abcde", end_stream=True)

        c.receive_data(frame_factory.build_window_update_frame(0, 3).serialize())
        c.receive_data(frame_factory.build_window_update_frame(1, 3).serialize())
        assert _data_frames(c.data_to_send()) == [(1, b"abc", False)]

        c.receive_data(frame_factory.build_window_update_frame(0, 2).serialize())
        c.receive_data(frame_factory.build_window_update_frame(1, 2).serialize())
        # No separate empty DATA frame: the flag rides the final payload.
        assert _data_frames(c.data_to_send()) == [(1, b"de", True)]
        assert c.pending_queued_data(1) == 0
        assert c._get_stream_by_id(1).state_machine.state == h2.stream.StreamState.HALF_CLOSED_LOCAL

    def test_end_stream_method_with_backlog(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"xy")
        c.end_stream(1)

        assert c.pending_queued_data(1) == 2
        assert c._send_queue.pending_end_stream(1)

        c.receive_data(frame_factory.build_window_update_frame(0, 5).serialize())
        c.receive_data(frame_factory.build_window_update_frame(1, 5).serialize())

        frames = _data_frames(c.data_to_send())
        assert frames == [(1, b"xy", False), (1, b"", True)]
        assert sum(1 for _, _, end in frames if end) == 1

    def test_end_stream_method_without_backlog_is_immediate(self) -> None:
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.send_headers(1, HEADERS)
        c.clear_outbound_data_buffer()

        c.end_stream(1)

        assert _data_frames(c.data_to_send()) == [(1, b"", True)]

    def test_partial_acceptance_keeps_end_stream_with_caller(self, frame_factory) -> None:
        c = _open_connection(frame_factory, max_queued_data=4, window=0)
        c.outbound_flow_control_window = 0

        accepted = c.queue_data(1, b"abcdefgh", end_stream=True)
        assert accepted == 4
        assert not c._send_queue.pending_end_stream(1)

        c.receive_data(frame_factory.build_window_update_frame(0, 4).serialize())
        c.receive_data(frame_factory.build_window_update_frame(1, 4).serialize())
        frames = _data_frames(c.data_to_send())
        assert frames == [(1, b"abcd", False)]
        assert c._get_stream_by_id(1).open

        accepted = c.queue_data(1, b"efgh", end_stream=True)
        assert accepted == 4
        c.receive_data(frame_factory.build_window_update_frame(0, 4).serialize())
        c.receive_data(frame_factory.build_window_update_frame(1, 4).serialize())
        assert _data_frames(c.data_to_send()) == [(1, b"efgh", True)]


class TestSendQueueSettings:
    """
    Behaviour when SETTINGS shrink or grow the flow control windows.
    """

    def test_shrinking_window_stalls_queue(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=5)
        c.queue_data(1, b"abcde")
        c.clear_outbound_data_buffer()

        # Shrink the stream window to zero: nothing may leave.
        c.receive_data(frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 0},
        ).serialize())
        c.clear_outbound_data_buffer()

        assert c.queue_data(1, b"zz") == 2
        assert _data_frames(c.data_to_send()) == []
        assert c.pending_queued_data(1) == 2

        # Growing the window again lets the backlog out (the SETTINGS ACK is
        # emitted first, but _data_frames only looks at DATA frames).
        c.receive_data(frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 10},
        ).serialize())
        assert _data_frames(c.data_to_send()) == [(1, b"zz", False)]

    def test_negative_window_never_sends(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=5)
        c.outbound_flow_control_window = 2
        c.queue_data(1, b"12345678")
        c.clear_outbound_data_buffer()

        # Shrink to zero: the stream window goes negative.
        c.receive_data(frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 0},
        ).serialize())
        c.clear_outbound_data_buffer()
        assert c._get_stream_by_id(1).outbound_flow_control_window == -2

        # Even with connection window space, nothing may be sent.
        c.receive_data(frame_factory.build_window_update_frame(0, 10).serialize())
        assert _data_frames(c.data_to_send()) == []

        c.receive_data(frame_factory.build_window_update_frame(1, 5).serialize())
        assert _data_frames(c.data_to_send()) == [(1, b"345", False)]
        assert c.pending_queued_data(1) == 3


class TestSendQueueLifecycle:
    """
    Reset and connection teardown discard queued payloads.
    """

    def test_local_reset_discards_queue(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"abcdef")

        c.reset_stream(1)

        assert c.pending_queued_data(1) == 0
        assert _data_frames(c.data_to_send()) == []

    def test_remote_reset_discards_queue(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"abcdef")

        c.receive_data(frame_factory.build_rst_stream_frame(1).serialize())

        assert c.pending_queued_data(1) == 0
        assert _data_frames(c.data_to_send()) == []

    def test_goaway_discards_queue(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"abcdef")

        c.receive_data(frame_factory.build_goaway_frame(last_stream_id=1).serialize())

        assert c.pending_queued_data() == 0

    def test_close_connection_discards_queue(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"abcdef")

        c.close_connection()

        assert c.pending_queued_data() == 0


class TestSendQueueCancellation:
    """
    Distinguishing framed bytes from bytes the caller can still cancel.
    """

    def test_cancel_returns_only_unframed_bytes(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=3)
        c.queue_data(1, b"abcde", end_stream=True)
        c.clear_outbound_data_buffer()
        assert c.pending_queued_data(1) == 2

        data, end_stream = c.cancel_queued_data(1)

        assert data == b"de"
        assert end_stream is True
        assert c.pending_queued_data(1) == 0
        assert c._send_queue.stream_ids == []

    def test_cancel_when_empty_is_harmless(self) -> None:
        c = h2.connection.H2Connection()
        c.initiate_connection()
        c.send_headers(1, HEADERS)

        assert c.cancel_queued_data(1) == (b"", False)
        assert c.pending_queued_data(1) == 0

    def test_cancel_does_not_close_stream(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"abc")

        c.cancel_queued_data(1)

        assert c._get_stream_by_id(1).open
        c.queue_data(1, b"xyz")
        assert c.pending_queued_data(1) == 3


class TestSendQueueLimit:
    """
    The connection-wide pending byte limit.
    """

    def test_full_queue_raises_and_keeps_caller_bytes(self, frame_factory) -> None:
        c = _open_connection(frame_factory, max_queued_data=4, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"abcd")

        with pytest.raises(h2.exceptions.QueueFullError):
            c.queue_data(1, b"ef")

        assert c.pending_queued_data(1) == 4

    def test_partial_acceptance_when_some_room(self, frame_factory) -> None:
        c = _open_connection(frame_factory, max_queued_data=4, window=0)
        c.outbound_flow_control_window = 0

        accepted = c.queue_data(1, memoryview(b"abcdef"))

        assert accepted == 4
        assert c.pending_queued_data(1) == 4

    def test_empty_end_stream_ignores_limit(self, frame_factory) -> None:
        c = _open_connection(frame_factory, max_queued_data=0)
        c.clear_outbound_data_buffer()

        accepted = c.queue_data(1, b"", end_stream=True)

        assert accepted == 0
        assert c.pending_queued_data(1) == 0
        assert _data_frames(c.data_to_send()) == [(1, b"", True)]


class TestSendQueueControlFrames:
    """
    A body backlog must never block control frames.
    """

    def test_ping_ack_not_blocked_by_backlog(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"x" * 100)
        c.clear_outbound_data_buffer()

        c.receive_data(frame_factory.build_ping_frame(b"abcdefgh").serialize())

        frames = _frames(c.data_to_send())
        assert len(frames) == 1
        assert isinstance(frames[0], PingFrame)
        assert "ACK" in frames[0].flags
        assert c.pending_queued_data(1) == 100

    def test_outgoing_control_frames_precede_data(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"x" * 100)
        c.clear_outbound_data_buffer()

        # Open both windows: the locally emitted PING must not sit behind DATA.
        c.ping(b"12345678")
        c.receive_data(frame_factory.build_window_update_frame(0, 10).serialize())
        c.receive_data(frame_factory.build_window_update_frame(1, 10).serialize())

        frame_types = [type(frame).__name__ for frame in _frames(c.data_to_send())]
        assert frame_types[0] == "PingFrame"
        assert frame_types.count("DataFrame") == 1

    def test_settings_ack_precedes_queued_data(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"x" * 10)
        c.clear_outbound_data_buffer()

        c.receive_data(frame_factory.build_settings_frame(
            {h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 10},
        ).serialize())
        c.receive_data(frame_factory.build_window_update_frame(0, 10).serialize())

        frames = _frames(c.data_to_send())
        assert type(frames[0]).__name__ == "SettingsFrame"
        assert "ACK" in frames[0].flags
        assert isinstance(frames[1], DataFrame)
        assert c.pending_queued_data(1) == 0


class TestSendQueueFlush:
    """
    The explicit flush entry point.
    """

    def test_flush_queued_data_returns_framed_bytes(self, frame_factory) -> None:
        c = _open_connection(frame_factory, window=0)
        c.outbound_flow_control_window = 0
        c.queue_data(1, b"abc")
        c.clear_outbound_data_buffer()

        assert c.flush_queued_data() == 0
        assert c.pending_queued_data(1) == 3

        c.outbound_flow_control_window = 2
        c._get_stream_by_id(1).outbound_flow_control_window = 2
        assert c.flush_queued_data() == 2
        assert _data_frames(c.data_to_send()) == [(1, b"ab", False)]
        assert c.pending_queued_data(1) == 1

    def test_flush_with_empty_queue_is_noop(self) -> None:
        c = h2.connection.H2Connection()
        c.initiate_connection()
        assert c.flush_queued_data() == 0


class TestSendQueueUnit:
    """
    Direct unit tests for the SendQueue data structure.
    """

    def test_enqueue_zero_bytes_is_noop(self) -> None:
        queue = h2.send_queue.SendQueue()
        assert queue.enqueue(1, b"") == 0
        assert queue.stream_ids == []
        assert queue.total_pending == 0

    def test_enqueue_full_queue_with_data_returns_zero(self) -> None:
        queue = h2.send_queue.SendQueue(max_size=2)
        assert queue.enqueue(1, b"ab") == 2
        assert queue.enqueue(1, b"cd") == 0
        assert queue.pending(1) == 2

    def test_duplicate_end_stream_marker_ignored(self) -> None:
        queue = h2.send_queue.SendQueue()
        assert queue.enqueue(1, b"", end_stream=True) == 0
        assert queue.enqueue(1, b"", end_stream=True) == 0
        assert queue.pending_end_stream(1)

    def test_rotation_is_byte_fair(self) -> None:
        queue = h2.send_queue.SendQueue()
        queue.enqueue(1, b"aaaa")
        queue.enqueue(3, b"bb")

        assert queue.select() == 1
        queue.consume(1, 2)
        queue.complete_turn(1)

        assert queue.select() == 3
        queue.consume(3, 2)
        queue.complete_turn(3)

        assert queue.select() == 1
        queue.consume(1, 2)
        queue.complete_turn(1)

        assert queue.select() is None
        assert queue.total_pending == 0

    def test_consume_partial_keeps_end_stream_on_remainder(self) -> None:
        queue = h2.send_queue.SendQueue()
        queue.enqueue(1, b"abc", end_stream=True)

        run = queue.consume(1, 2)
        assert run.data == b"ab"
        assert not run.end_stream

        run = queue.consume(1, 1)
        assert run.data == b"c"
        assert run.end_stream
        assert queue.total_pending == 0

    def test_cancel_unknown_stream(self) -> None:
        queue = h2.send_queue.SendQueue()
        assert queue.cancel(1) == (b"", False)

    def test_clear(self) -> None:
        queue = h2.send_queue.SendQueue()
        queue.enqueue(1, b"abc")
        queue.clear()
        assert queue.total_pending == 0
        assert queue.stream_ids == []

    def test_blocked_stream_skipped_by_select(self) -> None:
        queue = h2.send_queue.SendQueue()
        queue.enqueue(1, b"aa")
        queue.enqueue(3, b"bb")

        queue.mark_blocked(1)
        assert queue.select() == 3

        queue.mark_blocked(3)
        assert queue.select() is None

        queue.reset_blocked()
        assert queue.select() == 1
