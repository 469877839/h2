"""
Connection-level HPACK state across rejected and reset streams.

HPACK state lives on the connection, not on the stream: a header block updates
the shared dynamic table even when the stream carrying it is refused or
reset. Every test here builds header blocks with a real ``hpack.Encoder`` so
that the later blocks genuinely reference dynamic-table entries established
by the earlier ones; none of the tests fabricate decoded results.

For each failure mode the tests also pin down the error scope:

* stream-level failure  -> ``RST_STREAM`` and the connection keeps working
* connection-level failure -> ``GOAWAY`` with the mandated error code
"""
from __future__ import annotations

import pytest

from hpack import Encoder
from hpack.exceptions import HPACKDecodingError
from hpack.hpack import Decoder
from hyperframe.frame import (
    ContinuationFrame,
    DataFrame,
    GoAwayFrame,
    HeadersFrame,
    RstStreamFrame,
    SettingsFrame,
)

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.settings

SERVER_CONFIG = h2.config.H2Configuration(
    client_side=False,
    header_encoding=None,
    validate_inbound_headers=False,
)

# Requests share a long custom header. A stateful encoder puts that header in
# the dynamic table once and later blocks reference it by a small index. The
# :authority and :path entries get indexed as well, so every block after the
# first depends on connection-level HPACK state to decode.
REQUEST_A = [
    (b":method", b"GET"),
    (b":scheme", b"https"),
    (b":authority", b"example.com"),
    (b":path", b"/alpha"),
    (b"x-custom-header", b"alpha-value-12345"),
]
REQUEST_B = [
    (b":method", b"GET"),
    (b":scheme", b"https"),
    (b":authority", b"example.com"),
    (b":path", b"/beta"),
    (b"x-custom-header", b"alpha-value-12345"),
]
REQUEST_C = [
    (b":method", b"GET"),
    (b":scheme", b"https"),
    (b":authority", b"example.com"),
    (b":path", b"/gamma"),
    (b"x-custom-header", b"alpha-value-12345"),
]


def make_server() -> h2.connection.H2Connection:
    c = h2.connection.H2Connection(config=SERVER_CONFIG)
    c.receive_data(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
    c.clear_outbound_data_buffer()
    return c


def limit_concurrent_streams(c: h2.connection.H2Connection, value: int) -> None:
    c.update_settings(
        {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: value},
    )
    # Remote ack so the local setting is applied.
    c.receive_data(_settings_ack())
    c.clear_outbound_data_buffer()


def _settings_ack() -> bytes:
    f = SettingsFrame(0)
    f.flags.add("ACK")
    return f.serialize()


def headers_frame(
    stream_id: int,
    block: bytes,
    *,
    end_headers: bool = True,
    end_stream: bool = False,
) -> HeadersFrame:
    f = HeadersFrame(stream_id)
    f.data = block
    if end_headers:
        f.flags.add("END_HEADERS")
    if end_stream:
        f.flags.add("END_STREAM")
    return f


def continuation_frame(
    stream_id: int,
    block: bytes,
    *,
    end_headers: bool = False,
) -> ContinuationFrame:
    f = ContinuationFrame(stream_id)
    f.data = block
    if end_headers:
        f.flags.add("END_HEADERS")
    return f


def dynamic_entries(c: h2.connection.H2Connection) -> int:
    return len(c.decoder.header_table.dynamic_entries)


def request_events(events: list[h2.events.Event]) -> list[h2.events.Event]:
    return [e for e in events if isinstance(e, h2.events.RequestReceived)]


def reset_events(events: list[h2.events.Event]) -> list[h2.events.StreamReset]:
    return [e for e in events if isinstance(e, h2.events.StreamReset)]


def rst_frame(stream_id: int, error_code: h2.errors.ErrorCodes) -> bytes:
    f = RstStreamFrame(stream_id)
    f.error_code = error_code
    return f.serialize()


def goaway_frame(
    last_stream_id: int,
    error_code: h2.errors.ErrorCodes,
) -> bytes:
    f = GoAwayFrame(0)
    f.last_stream_id = last_stream_id
    f.error_code = error_code
    return f.serialize()


class TestEncoderInterdependence:
    """
    Sanity checks proving the test blocks really share HPACK state: a block
    produced by a stateful encoder cannot be decoded without the blocks that
    preceded it.
    """

    def test_stateful_block_references_earlier_dynamic_entries(self) -> None:
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)
        block_b = enc.encode(REQUEST_B)

        # A decoder that has never seen block_a cannot decode block_b.
        with pytest.raises(HPACKDecodingError):
            Decoder().decode(block_b, raw=True)

        # A decoder that saw block_a handles block_b fine and returns the
        # full header set rather than indexed references.
        decoder = Decoder()
        assert decoder.decode(block_a, raw=True) == REQUEST_A
        assert decoder.decode(block_b, raw=True) == REQUEST_B


class TestRefusedStreamKeepsHpackState:
    """
    A stream refused for exceeding MAX_CONCURRENT_STREAMS is a stream error
    (RFC 9113 § 5.1.2). Its header block must still update the connection
    HPACK state, and the connection must stay usable.
    """

    def test_refused_stream_updates_decoder_and_connection_survives(self) -> None:
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)
        block_b = enc.encode(REQUEST_B)
        block_c = enc.encode(REQUEST_C)

        c = make_server()
        limit_concurrent_streams(c, 1)

        # Stage 1: stream 1 opens and fills the concurrency slot.
        events = c.receive_data(headers_frame(1, block_a).serialize())
        assert [type(e) for e in events] == [h2.events.RequestReceived]
        assert dynamic_entries(c) == 3
        c.clear_outbound_data_buffer()

        # Stage 2: stream 3 exceeds the limit. It is refused with a stream
        # error, not a connection error, and its block still updates the
        # decoder: a new :path /beta dynamic entry is added (the custom
        # header and :authority reuse the entries from block_a).
        events = c.receive_data(headers_frame(3, block_b).serialize())
        assert request_events(events) == []
        resets = reset_events(events)
        assert len(resets) == 1
        assert resets[0].stream_id == 3
        assert resets[0].error_code == h2.errors.ErrorCodes.REFUSED_STREAM
        assert not resets[0].remote_reset
        assert c.data_to_send() == rst_frame(
            3, h2.errors.ErrorCodes.REFUSED_STREAM,
        )
        assert dynamic_entries(c) == 4
        assert c.state_machine.state is not h2.connection.ConnectionState.CLOSED

        # Stage 3: the peer closes stream 1; the refusal bookkeeping must not
        # have broken the connection.
        c.receive_data(rst_frame(1, h2.errors.ErrorCodes.NO_ERROR))
        c.clear_outbound_data_buffer()

        # Stage 4: a legal stream 5 references the dynamic entries that the
        # refused stream's block established. It decodes correctly.
        events = c.receive_data(headers_frame(5, block_c).serialize())
        requests = request_events(events)
        assert len(requests) == 1
        assert requests[0].stream_id == 5
        assert requests[0].headers == REQUEST_C

    def test_refused_stream_replayable_frame_sequence(self) -> None:
        """
        The same scenario as a single replayable byte sequence: the exact
        frames are concatenated and fed to the connection in one shot.
        """
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)
        block_b = enc.encode(REQUEST_B)
        block_c = enc.encode(REQUEST_C)

        wire = (
            headers_frame(1, block_a).serialize() +
            headers_frame(3, block_b).serialize() +
            rst_frame(1, h2.errors.ErrorCodes.NO_ERROR) +
            headers_frame(5, block_c).serialize()
        )

        c = make_server()
        limit_concurrent_streams(c, 1)
        events = c.receive_data(wire)

        # Stage assertions on the event stream: accepted, refused, reset,
        # accepted -- in that order.
        assert isinstance(events[0], h2.events.RequestReceived)
        assert events[0].stream_id == 1

        assert isinstance(events[1], h2.events.StreamReset)
        assert events[1].stream_id == 3
        assert events[1].error_code == h2.errors.ErrorCodes.REFUSED_STREAM
        assert not events[1].remote_reset

        assert isinstance(events[2], h2.events.StreamReset)
        assert events[2].stream_id == 1
        assert events[2].remote_reset

        assert isinstance(events[3], h2.events.RequestReceived)
        assert events[3].stream_id == 5
        assert events[3].headers == REQUEST_C

        # The only outbound frame is the refusal of stream 3.
        assert c.data_to_send() == rst_frame(
            3, h2.errors.ErrorCodes.REFUSED_STREAM,
        )

    def test_frames_on_refused_stream_are_stream_errors(self) -> None:
        """
        After a stream is refused, further frames on it are stream errors,
        not connection errors.
        """
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)
        block_b = enc.encode(REQUEST_B)

        c = make_server()
        limit_concurrent_streams(c, 1)
        c.receive_data(headers_frame(1, block_a).serialize())
        c.receive_data(headers_frame(3, block_b).serialize())
        c.clear_outbound_data_buffer()

        # DATA on the refused stream: stream error, connection stays up.
        f = DataFrame(3)
        f.data = b"late data"
        events = c.receive_data(f.serialize())
        assert events == []
        assert c.data_to_send() == rst_frame(
            3, h2.errors.ErrorCodes.STREAM_CLOSED,
        )
        assert c.state_machine.state is not h2.connection.ConnectionState.CLOSED

        # A duplicate HEADERS on the refused stream ID is also a stream
        # error (the stream ID is already consumed).
        c.clear_outbound_data_buffer()
        block_b2 = enc.encode(REQUEST_B)
        events = c.receive_data(headers_frame(3, block_b2).serialize())
        assert request_events(events) == []
        assert c.data_to_send() == rst_frame(
            3, h2.errors.ErrorCodes.STREAM_CLOSED,
        )
        assert c.state_machine.state is not h2.connection.ConnectionState.CLOSED


class TestResetStreamKeepsHpackState:
    """
    HEADERS that arrive on a stream that was already reset are a stream
    error, but their block must still be decoded: the dynamic table entries
    they add are connection-scoped and later streams may reference them.
    """

    @pytest.mark.parametrize("reset_by", ["local", "remote"])
    def test_headers_on_reset_stream_update_decoder(self, reset_by: str) -> None:
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)
        block_b = enc.encode(REQUEST_B)
        block_c = enc.encode(REQUEST_C)

        c = make_server()

        # Stage 1: stream 1 opens.
        events = c.receive_data(headers_frame(1, block_a).serialize())
        assert [type(e) for e in events] == [h2.events.RequestReceived]
        assert dynamic_entries(c) == 3

        # Stage 2: the stream is reset, either by us or by the peer.
        if reset_by == "local":
            c.reset_stream(1)
        else:
            c.receive_data(rst_frame(1, h2.errors.ErrorCodes.CANCEL))
        c.clear_outbound_data_buffer()

        # Stage 3: the peer had a HEADERS in flight for stream 1. It is not
        # delivered to the application, but it is decoded: the dynamic table
        # gains the :path /beta entry from block_b, and the response is a
        # stream error, not a connection error.
        events = c.receive_data(headers_frame(1, block_b).serialize())
        assert request_events(events) == []
        assert dynamic_entries(c) == 4
        assert c.data_to_send() == rst_frame(
            1, h2.errors.ErrorCodes.STREAM_CLOSED,
        )
        assert c.state_machine.state is not h2.connection.ConnectionState.CLOSED
        c.clear_outbound_data_buffer()

        # Stage 4: a legal stream 3 references the entries established by the
        # block on the reset stream. It decodes correctly.
        events = c.receive_data(headers_frame(3, block_c).serialize())
        requests = request_events(events)
        assert len(requests) == 1
        assert requests[0].stream_id == 3
        assert requests[0].headers == REQUEST_C


class TestFragmentedHeaderBlocks:
    """
    HEADERS/CONTINUATION sequences: the header block is decoded once, after
    the final CONTINUATION, and rejection of the stream still preserves the
    connection HPACK state.
    """

    def test_continuation_block_on_refused_stream(self) -> None:
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)
        block_b = enc.encode(REQUEST_B)
        block_c = enc.encode(REQUEST_C)

        c = make_server()
        limit_concurrent_streams(c, 1)
        c.receive_data(headers_frame(1, block_a).serialize())
        c.clear_outbound_data_buffer()

        # Stage 1: the refused stream's block arrives in three fragments.
        # Nothing is decoded until the final CONTINUATION.
        third = len(block_b) // 3
        fragments = [block_b[:third], block_b[third:2 * third], block_b[2 * third:]]
        wire = (
            headers_frame(3, fragments[0], end_headers=False).serialize() +
            continuation_frame(3, fragments[1]).serialize()
        )
        events = c.receive_data(wire)
        assert events == []
        assert dynamic_entries(c) == 3  # only stream 1's entries so far

        # Stage 2: the final fragment completes the block; the stream is
        # refused as a stream error and the decoder is updated.
        events = c.receive_data(
            continuation_frame(3, fragments[2], end_headers=True).serialize(),
        )
        resets = reset_events(events)
        assert len(resets) == 1
        assert resets[0].stream_id == 3
        assert resets[0].error_code == h2.errors.ErrorCodes.REFUSED_STREAM
        assert c.data_to_send() == rst_frame(
            3, h2.errors.ErrorCodes.REFUSED_STREAM,
        )
        assert dynamic_entries(c) == 4
        c.clear_outbound_data_buffer()

        # Stage 3: a legal stream decodes against the updated table.
        c.receive_data(rst_frame(1, h2.errors.ErrorCodes.NO_ERROR))
        events = c.receive_data(headers_frame(5, block_c).serialize())
        requests = request_events(events)
        assert len(requests) == 1
        assert requests[0].headers == REQUEST_C

    def test_continuation_block_on_reset_stream(self) -> None:
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)
        block_b = enc.encode(REQUEST_B)
        block_c = enc.encode(REQUEST_C)

        c = make_server()
        c.receive_data(headers_frame(1, block_a).serialize())
        c.reset_stream(1)
        c.clear_outbound_data_buffer()

        # A fragmented HEADERS lands on the reset stream: decoded but not
        # delivered.
        half = len(block_b) // 2
        wire = (
            headers_frame(1, block_b[:half], end_headers=False).serialize() +
            continuation_frame(1, block_b[half:], end_headers=True).serialize()
        )
        events = c.receive_data(wire)
        assert request_events(events) == []
        assert dynamic_entries(c) == 4
        assert c.data_to_send() == rst_frame(
            1, h2.errors.ErrorCodes.STREAM_CLOSED,
        )
        c.clear_outbound_data_buffer()

        events = c.receive_data(headers_frame(3, block_c).serialize())
        requests = request_events(events)
        assert len(requests) == 1
        assert requests[0].headers == REQUEST_C

    def test_interleaved_frame_inside_header_block_is_connection_error(self) -> None:
        """
        Any frame other than a matching CONTINUATION inside a header block is
        a connection error of type PROTOCOL_ERROR (RFC 9113 § 5.1).
        """
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)

        c = make_server()
        half = len(block_a) // 2
        wire = (
            headers_frame(1, block_a[:half], end_headers=False).serialize() +
            DataFrame(1, data=b"nope").serialize() +
            continuation_frame(1, block_a[half:], end_headers=True).serialize()
        )
        with pytest.raises(h2.exceptions.ProtocolError):
            c.receive_data(wire)
        assert c.data_to_send() == goaway_frame(
            0, h2.errors.ErrorCodes.PROTOCOL_ERROR,
        )
        assert c.state_machine.state is h2.connection.ConnectionState.CLOSED


class TestDynamicTableResize:
    """
    Dynamic table size updates are part of the connection HPACK state: they
    must be applied even when they arrive on a stream that is not delivered
    to the application.
    """

    def test_table_shrink_and_grow_across_streams(self) -> None:
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)
        enc.header_table_size = 256  # emits a dynamic table size update
        block_b = enc.encode(REQUEST_B)
        enc.header_table_size = 4096  # grow back, another size update
        block_c = enc.encode(REQUEST_C)

        c = make_server()

        # Stage 1: normal stream, default table size.
        events = c.receive_data(headers_frame(1, block_a).serialize())
        assert [type(e) for e in events] == [h2.events.RequestReceived]
        assert c.decoder.header_table.maxsize == 4096

        # Stage 2: the next block shrinks the table to 256; the decoder
        # applies the update and evicts entries to fit.
        events = c.receive_data(headers_frame(3, block_b).serialize())
        requests = request_events(events)
        assert len(requests) == 1
        assert requests[0].headers == REQUEST_B
        assert c.decoder.header_table.maxsize == 256

        # Stage 3: the table grows back; the block decodes correctly.
        events = c.receive_data(headers_frame(5, block_c).serialize())
        requests = request_events(events)
        assert len(requests) == 1
        assert requests[0].headers == REQUEST_C
        assert c.decoder.header_table.maxsize == 4096

    def test_table_shrink_on_refused_stream_is_applied(self) -> None:
        """
        A dynamic table size update inside a refused stream's block still
        resizes the connection decoder table, and subsequent blocks encoded
        against the shrunken table decode correctly.
        """
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)
        enc.header_table_size = 256
        block_b = enc.encode(REQUEST_B)
        block_c = enc.encode(REQUEST_C)

        c = make_server()
        limit_concurrent_streams(c, 1)
        c.receive_data(headers_frame(1, block_a).serialize())
        c.clear_outbound_data_buffer()

        # The refused stream's block carries the table size update.
        events = c.receive_data(headers_frame(3, block_b).serialize())
        resets = reset_events(events)
        assert len(resets) == 1
        assert resets[0].error_code == h2.errors.ErrorCodes.REFUSED_STREAM
        assert c.decoder.header_table.maxsize == 256

        # A legal stream encoded against the shrunken table decodes.
        c.receive_data(rst_frame(1, h2.errors.ErrorCodes.NO_ERROR))
        events = c.receive_data(headers_frame(5, block_c).serialize())
        requests = request_events(events)
        assert len(requests) == 1
        assert requests[0].headers == REQUEST_C

    def test_table_shrink_on_reset_stream_is_applied(self) -> None:
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)
        enc.header_table_size = 256
        block_b = enc.encode(REQUEST_B)
        block_c = enc.encode(REQUEST_C)

        c = make_server()
        c.receive_data(headers_frame(1, block_a).serialize())
        c.reset_stream(1)
        c.clear_outbound_data_buffer()

        # The in-flight HEADERS on the reset stream carries the size update.
        events = c.receive_data(headers_frame(1, block_b).serialize())
        assert request_events(events) == []
        assert c.decoder.header_table.maxsize == 256
        c.clear_outbound_data_buffer()

        events = c.receive_data(headers_frame(3, block_c).serialize())
        requests = request_events(events)
        assert len(requests) == 1
        assert requests[0].headers == REQUEST_C


class TestInvalidCompressionIndex:
    """
    A header block that cannot be decompressed is a connection error of type
    COMPRESSION_ERROR (RFC 9113 § 5.4.1): the shared HPACK context can no
    longer be trusted, so the whole connection is closed.
    """

    #: Indexed header field representation with an absurdly large index.
    INVALID_INDEX_BLOCK = b"\xff\xff\xff\xff"

    def test_invalid_index_is_compression_error(self) -> None:
        c = make_server()
        with pytest.raises(h2.exceptions.CompressionError) as exc_info:
            c.receive_data(
                headers_frame(1, self.INVALID_INDEX_BLOCK).serialize(),
            )
        assert exc_info.value.error_code == h2.errors.ErrorCodes.COMPRESSION_ERROR
        # Backwards compatibility: callers catching ProtocolError still work.
        assert isinstance(exc_info.value, h2.exceptions.ProtocolError)
        assert c.data_to_send() == goaway_frame(
            0, h2.errors.ErrorCodes.COMPRESSION_ERROR,
        )
        assert c.state_machine.state is h2.connection.ConnectionState.CLOSED

    def test_invalid_index_in_continuation_is_compression_error(self) -> None:
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)

        c = make_server()
        c.receive_data(headers_frame(1, block_a).serialize())
        c.clear_outbound_data_buffer()

        # A valid-looking first fragment, then garbage in the CONTINUATION.
        wire = (
            headers_frame(3, block_a[:4], end_headers=False).serialize() +
            continuation_frame(
                3, self.INVALID_INDEX_BLOCK, end_headers=True,
            ).serialize()
        )
        with pytest.raises(h2.exceptions.CompressionError):
            c.receive_data(wire)
        assert c.data_to_send() == goaway_frame(
            1, h2.errors.ErrorCodes.COMPRESSION_ERROR,
        )
        assert c.state_machine.state is h2.connection.ConnectionState.CLOSED

    def test_invalid_index_on_over_limit_stream_is_compression_error(self) -> None:
        """
        The header block is decoded before the concurrency limit is applied:
        an undecodable block on an over-limit stream is a connection error,
        not a stream refusal.
        """
        enc = Encoder()
        block_a = enc.encode(REQUEST_A)

        c = make_server()
        limit_concurrent_streams(c, 1)
        c.receive_data(headers_frame(1, block_a).serialize())
        c.clear_outbound_data_buffer()

        with pytest.raises(h2.exceptions.CompressionError):
            c.receive_data(
                headers_frame(3, self.INVALID_INDEX_BLOCK).serialize(),
            )
        assert c.data_to_send() == goaway_frame(
            1, h2.errors.ErrorCodes.COMPRESSION_ERROR,
        )
        assert c.state_machine.state is h2.connection.ConnectionState.CLOSED

    def test_invalid_table_size_update_is_compression_error(self) -> None:
        """
        A dynamic table size update larger than the allowed maximum is also
        a decompression failure and closes the connection.
        """
        c = make_server()
        # Dynamic table size update (001xxxxx) with an absurdly large value.
        block = b"\x3f\xff\xff\xff\x7f"
        with pytest.raises(h2.exceptions.CompressionError):
            c.receive_data(headers_frame(1, block).serialize())
        assert c.data_to_send() == goaway_frame(
            0, h2.errors.ErrorCodes.COMPRESSION_ERROR,
        )
        assert c.state_machine.state is h2.connection.ConnectionState.CLOSED
