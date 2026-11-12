"""
Tests for explicit graceful shutdown behaviour.

These tests exercise the GOAWAY handling described by RFC 9113 Section 5.2:
a GOAWAY frame carrying NO_ERROR starts a draining period during which
streams the remote peer has already accepted may complete, while streams
beyond the advertised boundary are rejected and reported as retryable. Error
GOAWAY frames keep failing fast.

Everything here runs two real :class:`H2Connection` objects against each
other in memory, so the frames are genuine serialized HTTP/2.
"""
from __future__ import annotations

import pytest

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.frame_buffer

REQUEST_HEADERS = [
    (":authority", "example.com"),
    (":path", "/"),
    (":scheme", "https"),
    (":method", "GET"),
]

RESPONSE_HEADERS = [
    (":status", "200"),
    ("server", "fake-serv/0.1.0"),
]


def make_pair():
    """
    Build a client and a server connection that exchange frames in memory.
    Returns the pair together with a ``pump`` helper that moves one
    connection's buffered output into the other's receive path.
    """
    client = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=True),
    )
    server = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=False),
    )
    client.initiate_connection()
    server.initiate_connection()

    def pump(source, target) -> list[h2.events.Event]:
        data = source.data_to_send()
        if not data:
            return []
        return target.receive_data(data)

    # Bring both connections to a fully negotiated state.
    pump(client, server)
    pump(server, client)

    return client, server, pump


def open_stream(client, stream_id: int, end_stream: bool = True) -> None:
    """
    Have the client open ``stream_id`` with a request.
    """
    client.send_headers(stream_id, REQUEST_HEADERS, end_stream=end_stream)


def parse_single(data: bytes, frame_type) -> object:
    """
    Parse exactly one frame of ``frame_type`` out of ``data``.
    """
    buffer = h2.frame_buffer.FrameBuffer()
    buffer.max_frame_size = 2**24
    buffer.add_data(data)
    frames = list(buffer)
    assert len(frames) == 1, frames
    assert isinstance(frames[0], frame_type)
    return frames[0]


class TestRemoteGracefulShutdown:
    """
    The remote peer (the server) sends the graceful GOAWAY: the client is the
    one that learns which of its requests are retryable.
    """

    def test_retained_stream_can_still_complete(self) -> None:
        """
        A stream at or below the GOAWAY boundary is still processed normally
        after the graceful GOAWAY.
        """
        client, server, pump = make_pair()

        open_stream(client, 1, end_stream=False)
        pump(client, server)

        events = client.receive_data(_goaway(1).serialize())
        terminated = events[0]
        assert isinstance(terminated, h2.events.ConnectionTerminated)
        assert terminated.graceful
        assert terminated.last_stream_id == 1
        assert client.state_machine.state == h2.connection.ConnectionState.REMOTE_CLOSING
        assert client.retryable_stream_ids == frozenset()

        # The server can still respond on stream 1 and the response reaches
        # the client.
        server.send_headers(1, RESPONSE_HEADERS)
        assert [type(e) for e in pump(server, client)] == [h2.events.ResponseReceived]

        server.send_data(1, b"hello", end_stream=True)
        events = pump(server, client)
        assert [type(e) for e in events] == [h2.events.DataReceived, h2.events.StreamEnded]

        # The client may still end its side of the retained stream.
        client.end_stream(1)
        server.receive_data(client.data_to_send())
        assert client.streams[1].closed
        assert server.streams[1].closed

    def test_streams_above_boundary_become_retryable(self) -> None:
        """
        Streams whose IDs exceed last_stream_id were never accepted: they
        become retryable and are reported precisely.
        """
        client, server, pump = make_pair()

        for stream_id in (1, 3, 5):
            open_stream(client, stream_id)
        pump(client, server)

        events = client.receive_data(_goaway(1).serialize())

        terminated = [e for e in events if isinstance(e, h2.events.ConnectionTerminated)]
        retryable = [e for e in events if isinstance(e, h2.events.StreamRetryable)]
        assert len(terminated) == 1
        assert terminated[0].last_stream_id == 1
        assert [e.stream_id for e in retryable] == [3, 5]
        assert all(e.last_stream_id == 1 for e in retryable)
        assert client.retryable_stream_ids == frozenset({3, 5})

        # Stream 1 is retained: it is not reported as retryable.
        assert client.streams[1].open
        # Streams 3 and 5 are retired: further sends on them fail.
        for stream_id in (3, 5):
            with pytest.raises(h2.exceptions.StreamClosedError):
                client.send_data(stream_id, b"late")

    def test_no_new_streams_after_graceful_goaway(self) -> None:
        """
        Once the remote peer has started a graceful shutdown no new outbound
        streams may be initiated locally.
        """
        client, server, pump = make_pair()
        open_stream(client, 1)
        pump(client, server)

        client.receive_data(_goaway(1).serialize())

        with pytest.raises(h2.exceptions.ProtocolError):
            client.send_headers(3, REQUEST_HEADERS)

    def test_multiple_goaways_lower_the_boundary(self) -> None:
        """
        A sequence of GOAWAY frames may only lower the last_stream_id, and
        each lowering expands the retryable range exactly.
        """
        client, server, pump = make_pair()
        for stream_id in (1, 3, 5, 7):
            open_stream(client, stream_id)
        pump(client, server)

        events = client.receive_data(_goaway(5).serialize())
        assert [e.stream_id for e in events if isinstance(e, h2.events.StreamRetryable)] == [7]
        assert client.retryable_stream_ids == frozenset({7})

        events = client.receive_data(_goaway(3).serialize())
        assert [e.stream_id for e in events if isinstance(e, h2.events.StreamRetryable)] == [5]
        assert client.retryable_stream_ids == frozenset({5, 7})
        assert client.remote_last_stream_id == 3

        # Repeating the same boundary emits no duplicate retryable events.
        events = client.receive_data(_goaway(3).serialize())
        assert not [e for e in events if isinstance(e, h2.events.StreamRetryable)]

    def test_closed_stream_not_retroactively_retryable(self) -> None:
        """
        A stream that already completed normally is not reported as retryable
        when a later GOAWAY lowers the boundary past it.
        """
        client, server, pump = make_pair()
        open_stream(client, 1, end_stream=False)
        open_stream(client, 3, end_stream=False)
        pump(client, server)

        server.send_headers(3, RESPONSE_HEADERS, end_stream=True)
        pump(server, client)
        client.end_stream(3)
        server.receive_data(client.data_to_send())
        assert client.streams[3].closed

        events = client.receive_data(_goaway(1).serialize())
        assert not [e for e in events if isinstance(e, h2.events.StreamRetryable)]
        assert client.retryable_stream_ids == frozenset()

    def test_increasing_last_stream_id_is_a_protocol_error(self) -> None:
        """
        RFC 9113 Section 5.2 forbids a later GOAWAY raising last_stream_id.
        """
        client, server, pump = make_pair()
        open_stream(client, 1)
        pump(client, server)

        client.receive_data(_goaway(1).serialize())

        with pytest.raises(h2.exceptions.ProtocolError):
            client.receive_data(_goaway(3).serialize())

    def test_error_goaway_still_fails_fast(self) -> None:
        """
        A GOAWAY with a non-zero error code tears the connection down at once.
        """
        client, server, pump = make_pair()
        open_stream(client, 1)
        pump(client, server)

        events = client.receive_data(
            _goaway(1, error_code=h2.errors.ErrorCodes.INTERNAL_ERROR).serialize(),
        )

        terminated = events[0]
        assert isinstance(terminated, h2.events.ConnectionTerminated)
        assert not terminated.graceful
        assert client.state_machine.state == h2.connection.ConnectionState.CLOSED
        assert not client.data_to_send()
        with pytest.raises(h2.exceptions.ProtocolError):
            client.send_headers(3, REQUEST_HEADERS)

    def test_error_goaway_after_graceful_one_fails_fast(self) -> None:
        """
        An error GOAWAY following a graceful one closes the connection even
        though it was draining.
        """
        client, server, pump = make_pair()
        open_stream(client, 1)
        pump(client, server)

        client.receive_data(_goaway(1).serialize())
        assert client.closing

        events = client.receive_data(
            _goaway(1, error_code=h2.errors.ErrorCodes.INTERNAL_ERROR).serialize(),
        )
        assert not events[0].graceful
        assert client.state_machine.state == h2.connection.ConnectionState.CLOSED
        with pytest.raises(h2.exceptions.ProtocolError):
            client.send_data(1, b"late")

    def test_both_peers_gracefully_closing(self) -> None:
        """
        If both peers send graceful GOAWAY frames the connection drains in
        both directions until every retained stream is done.
        """
        client, server, pump = make_pair()
        open_stream(client, 1, end_stream=False)
        pump(client, server)

        # Server drains first; the client answers with its own GOAWAY.
        server.close_connection(last_stream_id=1)
        events = pump(server, client)
        assert events[0].graceful
        client.close_connection(last_stream_id=0)
        pump(client, server)

        # Both sides are draining: each connection knows both its own
        # boundary and the remote peer's.
        assert client.closing and server.closing
        assert client.local_last_stream_id == 0
        assert client.remote_last_stream_id == 1
        assert server.local_last_stream_id == 1
        assert server.remote_last_stream_id == 0

        # The retained stream still completes end to end.
        server.send_headers(1, RESPONSE_HEADERS, end_stream=True)
        assert [type(e) for e in pump(server, client)] == [h2.events.ResponseReceived, h2.events.StreamEnded]
        client.end_stream(1)
        server.receive_data(client.data_to_send())
        assert client.streams[1].closed
        assert server.streams[1].closed

    def test_goaway_received_while_fully_closed(self) -> None:
        """
        Further GOAWAY frames on a connection already closed by an error are
        still accepted and report their own event.
        """
        client, server, pump = make_pair()
        client.receive_data(
            _goaway(0, error_code=h2.errors.ErrorCodes.INTERNAL_ERROR).serialize(),
        )
        events = client.receive_data(_goaway(0).serialize())
        assert isinstance(events[0], h2.events.ConnectionTerminated)
        assert client.state_machine.state == h2.connection.ConnectionState.CLOSED


class TestInFlightDataDuringShutdown:
    """
    Frames for retained streams keep being processed while the connection
    drains.
    """

    def test_data_window_update_and_end_stream(self) -> None:
        """
        DATA, WINDOW_UPDATE and END_STREAM on a retained stream continue to
        work after a graceful GOAWAY.
        """
        client, server, pump = make_pair()

        open_stream(client, 1, end_stream=False)
        pump(client, server)

        # The server starts its response, then announces a graceful shutdown.
        server.send_headers(1, RESPONSE_HEADERS)
        pump(server, client)
        server.send_data(1, b"hello")
        pump(server, client)

        client.receive_data(_goaway(1).serialize())

        # The client acknowledges the data: this flows through to the server
        # as a WINDOW_UPDATE even while closing.
        client.acknowledge_received_data(5, 1)
        server.receive_data(client.data_to_send())

        # The in-flight stream finishes normally in both directions.
        server.send_data(1, b" world", end_stream=True)
        events = pump(server, client)
        assert [type(e) for e in events] == [h2.events.DataReceived, h2.events.StreamEnded]

        client.end_stream(1)
        server.receive_data(client.data_to_send())
        assert client.streams[1].closed
        assert server.streams[1].closed

    def test_rst_stream_on_retained_stream(self) -> None:
        """
        RST_STREAM for a retained stream still resets it during shutdown.
        """
        client, server, pump = make_pair()
        open_stream(client, 1, end_stream=False)
        pump(client, server)
        assert client.streams[1].open

        client.receive_data(_goaway(1).serialize())

        events = client.receive_data(_rst(1).serialize())
        assert isinstance(events[0], h2.events.StreamReset)
        assert events[0].stream_id == 1
        assert client.streams[1].closed

    def test_frames_on_retryable_stream_are_inert(self) -> None:
        """
        WINDOW_UPDATE and RST_STREAM referring to a stream retired as
        retryable do not disturb the connection.
        """
        client, server, pump = make_pair()
        for stream_id in (1, 3):
            open_stream(client, stream_id)
        pump(client, server)

        client.receive_data(_goaway(1).serialize())
        assert client.retryable_stream_ids == frozenset({3})

        assert client.receive_data(_window_update(3, 10).serialize()) == []
        assert client.receive_data(_rst(3).serialize()) == []


class TestLocalGracefulShutdown:
    """
    This peer initiates the graceful shutdown. It keeps serving the streams
    it already accepted but refuses new ones.
    """

    def test_local_shutdown_keeps_accepted_streams(self) -> None:
        """
        After sending a graceful GOAWAY the server finishes streams at or
        below its advertised boundary and refuses later inbound streams.
        """
        client, server, pump = make_pair()

        open_stream(client, 1)
        pump(client, server)
        server.clear_outbound_data_buffer()

        server.close_connection()
        assert server.state_machine.state == h2.connection.ConnectionState.LOCAL_CLOSING
        assert server.local_last_stream_id == 1
        assert server.closing
        # Keep the GOAWAY on the server side: this test drives a client that
        # has not seen it yet, so its later request is a genuine race.
        assert server.data_to_send()
        server.clear_outbound_data_buffer()

        # The accepted request can still be answered.
        server.send_headers(1, RESPONSE_HEADERS, end_stream=True)
        pump(server, client)
        assert client.streams[1].closed

        # A new stream arriving after the GOAWAY is refused with
        # REFUSED_STREAM so the client knows it may retry elsewhere.
        open_stream(client, 3)
        data = client.data_to_send()
        assert server.receive_data(data) == []
        rst = parse_single(server.data_to_send(), h2.connection.RstStreamFrame)
        assert rst.stream_id == 3
        assert rst.error_code == h2.errors.ErrorCodes.REFUSED_STREAM
        assert 3 not in server.streams

    def test_local_shutdown_default_boundary_refuses_everything_new(self) -> None:
        """
        With no accepted streams the default boundary is 0: every new request
        is refused.
        """
        client, server, pump = make_pair()
        server.clear_outbound_data_buffer()

        server.close_connection()
        server.data_to_send()  # discard the GOAWAY; the client keeps sending

        open_stream(client, 1)
        events = server.receive_data(client.data_to_send())
        # Only a SETTINGS acknowledgement may ride along, never stream events.
        assert not [e for e in events if not isinstance(e, h2.events.SettingsAcknowledged)]
        rst = parse_single(server.data_to_send(), h2.connection.RstStreamFrame)
        assert rst.stream_id == 1
        assert rst.error_code == h2.errors.ErrorCodes.REFUSED_STREAM
        assert 1 not in server.streams

    def test_cannot_initiate_outbound_streams_after_local_shutdown(self) -> None:
        """
        Once this peer starts a graceful shutdown it cannot create new
        outbound streams, including pushed ones.
        """
        client, server, pump = make_pair()
        server.close_connection()
        with pytest.raises(h2.exceptions.ProtocolError):
            server.send_headers(2, RESPONSE_HEADERS)
        with pytest.raises(h2.exceptions.ProtocolError):
            server.push_stream(1, 4, REQUEST_HEADERS)

    def test_local_lowering_boundary(self) -> None:
        """
        A second close_connection may lower the boundary; raising it is a
        protocol error.
        """
        client, server, pump = make_pair()
        server.close_connection(last_stream_id=5)
        assert server.local_last_stream_id == 5
        server.close_connection(last_stream_id=5)
        server.close_connection(last_stream_id=1)
        assert server.local_last_stream_id == 1

        with pytest.raises(h2.exceptions.ProtocolError):
            server.close_connection(last_stream_id=3)

    def test_error_close_still_fails_fast(self) -> None:
        """
        An error close moves straight to CLOSED and emits a non-graceful
        GOAWAY.
        """
        client, server, pump = make_pair()
        server.close_connection(error_code=h2.errors.ErrorCodes.INTERNAL_ERROR)
        assert server.state_machine.state == h2.connection.ConnectionState.CLOSED

        events = pump(server, client)
        terminated = events[0]
        assert isinstance(terminated, h2.events.ConnectionTerminated)
        assert not terminated.graceful
        assert client.state_machine.state == h2.connection.ConnectionState.CLOSED


class TestServerPushDuringShutdown:
    """
    Server push interacts with the graceful shutdown boundary.
    """

    def test_client_refuses_push_past_local_boundary(self) -> None:
        """
        A PUSH_PROMISE for a promised stream beyond the boundary the client
        advertised is refused with REFUSED_STREAM.
        """
        client, server, pump = make_pair()
        open_stream(client, 1)
        pump(client, server)

        # The client starts a graceful shutdown covering only stream 1. The
        # server has not received the GOAWAY yet and legally races a push.
        client.close_connection(last_stream_id=1)
        client.data_to_send()

        server.push_stream(1, 2, REQUEST_HEADERS)
        events = client.receive_data(server.data_to_send())

        rst = parse_single(client.data_to_send(), h2.connection.RstStreamFrame)
        assert rst.stream_id == 2
        assert rst.error_code == h2.errors.ErrorCodes.REFUSED_STREAM
        assert 2 not in client.streams
        assert not [e for e in events if isinstance(e, h2.events.PushedStreamReceived)]

    def test_client_accepts_push_within_boundary(self) -> None:
        """
        Pushed streams within the boundary the client advertised are still
        accepted.
        """
        client, server, pump = make_pair()
        open_stream(client, 1)
        pump(client, server)

        client.close_connection(last_stream_id=2)
        client.data_to_send()

        server.push_stream(1, 2, REQUEST_HEADERS)
        events = client.receive_data(server.data_to_send())

        pushed = [e for e in events if isinstance(e, h2.events.PushedStreamReceived)]
        assert len(pushed) == 1
        assert pushed[0].pushed_stream_id == 2
        assert 2 in client.streams
        assert not client.data_to_send()

    def test_server_cannot_push_after_shutdown(self) -> None:
        """
        A server that has sent a graceful GOAWAY cannot create new pushed
        streams.
        """
        client, server, pump = make_pair()
        open_stream(client, 1)
        pump(client, server)

        server.close_connection()
        with pytest.raises(h2.exceptions.ProtocolError):
            server.push_stream(1, 2, REQUEST_HEADERS)


class TestInterleavedRetryRange:
    """
    The headline scenario: three interleaved streams, a boundary that is
    lowered as retained streams finish, exact retryable range.
    """

    def test_three_streams_interleaved(self) -> None:
        client, server, pump = make_pair()

        # Interleave creation: the client opens 1, the server pushes 2 in
        # response, the client then opens 3 and 5 while 2 is still in flight.
        open_stream(client, 1, end_stream=False)
        pump(client, server)
        server.push_stream(1, 2, REQUEST_HEADERS)
        pump(server, client)
        open_stream(client, 3, end_stream=False)
        open_stream(client, 5, end_stream=False)
        pump(client, server)

        # The server begins a graceful shutdown, claiming to have accepted
        # up to stream 3.
        server.close_connection(last_stream_id=3)
        events = pump(server, client)

        terminated = [e for e in events if isinstance(e, h2.events.ConnectionTerminated)]
        retryable = [e for e in events if isinstance(e, h2.events.StreamRetryable)]
        assert len(terminated) == 1
        assert terminated[0].graceful
        assert terminated[0].last_stream_id == 3
        # Only the client-initiated stream 5 is the client's to retry. The
        # pushed stream 2 belongs to the server, and the server already told
        # the client about stream 2 in the PUSH_PROMISE, so it is retained.
        assert [e.stream_id for e in retryable] == [5]
        assert client.retryable_stream_ids == frozenset({5})
        assert client.state_machine.state == h2.connection.ConnectionState.REMOTE_CLOSING
        assert server.state_machine.state == h2.connection.ConnectionState.LOCAL_CLOSING

        # Every retained stream (1, 2 and 3) finishes normally.
        for stream_id in (1, 2, 3):
            server.send_headers(stream_id, RESPONSE_HEADERS)
            pump(server, client)
            server.send_data(stream_id, b"body")
            pump(server, client)
            server.send_data(stream_id, b"", end_stream=True)
            pump(server, client)
            # The pushed stream 2 is fully closed by the response.
            if stream_id == 2:
                assert client.streams[stream_id].closed
                assert server.streams[stream_id].closed

        # The client's own request streams still owe their end-stream.
        client.end_stream(1)
        server.receive_data(client.data_to_send())
        client.end_stream(3)
        server.receive_data(client.data_to_send())
        for stream_id in (1, 3):
            assert client.streams[stream_id].closed
            assert server.streams[stream_id].closed

        # The server now lowers its boundary to 1. Streams 2 and 3 already
        # completed normally, so nothing becomes retryable retroactively.
        server.close_connection(last_stream_id=1)
        events = pump(server, client)
        assert not [e for e in events if isinstance(e, h2.events.StreamRetryable)]
        assert client.retryable_stream_ids == frozenset({5})
        assert client.remote_last_stream_id == 1

        # Neither side can create new streams any more; the client's attempt
        # is rejected locally, and any race reaching the server is refused.
        with pytest.raises(h2.exceptions.ProtocolError):
            client.send_headers(7, REQUEST_HEADERS)


def _goaway(last_stream_id, error_code=0):
    from hyperframe.frame import GoAwayFrame
    f = GoAwayFrame(0)
    f.last_stream_id = last_stream_id
    f.error_code = error_code
    return f


def _rst(stream_id, error_code=h2.errors.ErrorCodes.CANCEL):
    from hyperframe.frame import RstStreamFrame
    f = RstStreamFrame(stream_id)
    f.error_code = error_code
    return f


def _window_update(stream_id, increment):
    from hyperframe.frame import WindowUpdateFrame
    f = WindowUpdateFrame(stream_id)
    f.window_increment = increment
    return f
