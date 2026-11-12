"""
Tests for the receive credential interface: a per-DATA-frame token that the
application redeems to hand flow control window space back, instead of
accumulating integers for acknowledge_received_data.
"""
from __future__ import annotations

import pytest
from hyperframe.frame import Frame, RstStreamFrame, WindowUpdateFrame

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.receive_credential
import h2.settings


DEFAULT_FLOW_WINDOW = 65535


def parse_frames(data):
    """
    Parse a bytestring into a list of hyperframe frames.
    """
    frames = []
    buffer = memoryview(data)
    while len(buffer):
        frame, length = Frame.parse_frame_header(buffer[:9])
        frame.parse_body(buffer[9:9 + length])
        frames.append(frame)
        assert frame.body_len == length
        buffer = buffer[9 + length:]
    return frames


def window_updates(frames):
    return [
        (frame.stream_id, frame.window_increment)
        for frame in frames
        if isinstance(frame, WindowUpdateFrame)
    ]


class TestReceiveCredentialAccounting:
    """
    Tests for issuing and redeeming credentials on ordinary streams.
    """

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]
    server_config = h2.config.H2Configuration(
        client_side=False, receive_credentials=True,
    )

    def _setup_connection(self, frame_factory):
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.update_settings(
            {h2.settings.SettingCodes.MAX_FRAME_SIZE: DEFAULT_FLOW_WINDOW},
        )
        c.receive_data(
            frame_factory.build_settings_frame(settings={}, ack=True).serialize(),
        )
        c.clear_outbound_data_buffer()
        return c

    def _receive_headers(self, c, frame_factory, stream_id=1):
        frame = frame_factory.build_headers_frame(
            headers=self.example_request_headers,
            stream_id=stream_id,
        )
        c.receive_data(frame.serialize())
        c.clear_outbound_data_buffer()

    def test_credential_is_issued_with_padding(self, frame_factory) -> None:
        """
        A DataReceived event carries a credential pre-loaded with the
        flow-controlled length, which includes the padding and the pad-length
        byte.
        """
        c = self._setup_connection(frame_factory)
        self._receive_headers(c, frame_factory)

        data_frame = frame_factory.build_data_frame(
            b"some data", stream_id=1, padding_len=10,
        )
        events = c.receive_data(data_frame.serialize())

        event = events[0]
        assert isinstance(event, h2.events.DataReceived)
        credential = event.receive_credential
        assert isinstance(credential, h2.receive_credential.ReceiveCredential)
        assert credential.stream_id == 1
        assert event.flow_controlled_length == 9 + 1 + 10
        assert credential.remaining_bytes == event.flow_controlled_length
        assert not credential.is_complete

    def test_redeeming_partial_padding_returns_connection_window(
        self, frame_factory,
    ) -> None:
        """
        Redeeming enough bytes (padding included) hands the space back to the
        remote peer. Because automatic flow control coalesces updates, we check
        the emitted WINDOW_UPDATE frames rather than the raw window properties.
        """
        c = self._setup_connection(frame_factory)
        self._receive_headers(c, frame_factory)

        data_frame = frame_factory.build_data_frame(
            b"\x00" * 40000, stream_id=1, padding_len=10,
        )
        event = c.receive_data(data_frame.serialize())[0]
        credential = event.receive_credential
        assert credential.remaining_bytes == 40011

        credential.partially_complete(c, 40011)

        frames = parse_frames(c.data_to_send())
        assert window_updates(frames) == [(0, 40011), (1, 40011)]
        assert c.remote_flow_control_window(1) == DEFAULT_FLOW_WINDOW

    def test_partial_redemptions_are_bounded(self, frame_factory) -> None:
        """
        A credential may be redeemed in slices, and only the sum of the slices
        is returned.
        """
        c = self._setup_connection(frame_factory)
        self._receive_headers(c, frame_factory)

        data_frame = frame_factory.build_data_frame(
            b"some data", stream_id=1, padding_len=10,
        )
        credential = c.receive_data(data_frame.serialize())[0].receive_credential

        credential.partially_complete(c, 5)
        assert credential.remaining_bytes == 15
        assert not credential.is_complete

        credential.partially_complete(c, 7)
        assert credential.remaining_bytes == 8

        # Coalesced: nothing has crossed the emission threshold yet.
        assert not c.data_to_send()

        credential.complete(c)
        assert credential.remaining_bytes == 0
        assert credential.is_complete

    def test_over_redemption_is_rejected(self, frame_factory) -> None:
        """
        Redeeming more bytes than a credential represents raises.
        """
        c = self._setup_connection(frame_factory)
        self._receive_headers(c, frame_factory)

        data_frame = frame_factory.build_data_frame(
            b"some data", stream_id=1, padding_len=10,
        )
        credential = c.receive_data(data_frame.serialize())[0].receive_credential

        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential.partially_complete(c, credential.remaining_bytes + 1)

        # The rejected call must not have changed any accounting state.
        assert credential.remaining_bytes == 20
        assert not c.data_to_send()

        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential.partially_complete(c, 0)
        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential.partially_complete(c, -1)

    def test_repeat_completion_does_not_add_window(self, frame_factory) -> None:
        """
        Completing a credential twice raises and never produces an extra
        WINDOW_UPDATE.
        """
        c = self._setup_connection(frame_factory)
        self._receive_headers(c, frame_factory)

        data_frame = frame_factory.build_data_frame(
            b"\x00" * 40000, stream_id=1, padding_len=10,
        )
        credential = c.receive_data(data_frame.serialize())[0].receive_credential

        credential.complete(c)
        first = parse_frames(c.data_to_send())
        assert first
        assert window_updates(first) == [(0, 40011), (1, 40011)]

        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential.complete(c)
        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential.partially_complete(c, 1)

        assert not c.data_to_send()

    def test_cross_connection_use_is_rejected(self, frame_factory) -> None:
        """
        Credentials are bound to the connection that issued them.
        """
        c = self._setup_connection(frame_factory)
        self._receive_headers(c, frame_factory)

        data_frame = frame_factory.build_data_frame(
            b"some data", stream_id=1, padding_len=10,
        )
        credential = c.receive_data(data_frame.serialize())[0].receive_credential

        other = self._setup_connection(frame_factory)
        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential.complete(other)
        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential.partially_complete(other, 1)

        assert credential.remaining_bytes == 20
        assert not other.data_to_send()

    def test_zero_length_credential_is_complete(self, frame_factory) -> None:
        """
        A DATA frame that consumes no flow-controlled bytes yields an
        already-complete credential that cannot be redeemed again.
        """
        c = self._setup_connection(frame_factory)
        self._receive_headers(c, frame_factory)

        data_frame = frame_factory.build_data_frame(b"", stream_id=1)
        credential = c.receive_data(data_frame.serialize())[0].receive_credential

        assert credential.remaining_bytes == 0
        assert credential.is_complete
        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential.complete(c)

    def test_acknowledge_is_rejected_on_credential_stream(
        self, frame_factory,
    ) -> None:
        """
        The two accounting styles cannot be mixed on one stream: plain
        acknowledgement is refused so the same bytes are never returned twice.
        """
        c = self._setup_connection(frame_factory)
        self._receive_headers(c, frame_factory)

        data_frame = frame_factory.build_data_frame(
            b"some data", stream_id=1, padding_len=10,
        )
        credential = c.receive_data(data_frame.serialize())[0].receive_credential

        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            c.acknowledge_received_data(20, stream_id=1)

        credential.complete(c)
        c.data_to_send()

        # The marker is sticky: a late plain acknowledgement would otherwise
        # count the same 20 bytes a second time.
        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            c.acknowledge_received_data(20, stream_id=1)


class TestReceiveCredentialReset:
    """
    Tests for settling outstanding credentials when a stream is reset.
    """

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]
    server_config = h2.config.H2Configuration(
        client_side=False, receive_credentials=True,
    )

    def _setup_connection(self, frame_factory):
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.update_settings(
            {h2.settings.SettingCodes.MAX_FRAME_SIZE: DEFAULT_FLOW_WINDOW},
        )
        c.receive_data(
            frame_factory.build_settings_frame(settings={}, ack=True).serialize(),
        )
        c.increment_flow_control_window(DEFAULT_FLOW_WINDOW)
        c.clear_outbound_data_buffer()

        frame = frame_factory.build_headers_frame(
            headers=self.example_request_headers, stream_id=3,
        )
        c.receive_data(frame.serialize())
        c.clear_outbound_data_buffer()
        return c

    def test_reset_returns_connection_window_only(self, frame_factory) -> None:
        """
        A locally reset stream with outstanding credentials gives its bytes
        back to the connection window only, and never resurrects the stream
        window with a stream-level WINDOW_UPDATE.
        """
        c = self._setup_connection(frame_factory)

        data_frame = frame_factory.build_data_frame(
            b"\x00" * 65524, stream_id=3, padding_len=10,
        )
        event = c.receive_data(data_frame.serialize())[0]
        credential = event.receive_credential
        assert credential.remaining_bytes == DEFAULT_FLOW_WINDOW
        assert c.remote_flow_control_window(3) == 0

        c.reset_stream(3, error_code=h2.errors.ErrorCodes.CANCEL)

        frames = parse_frames(c.data_to_send())
        assert window_updates(frames) == [(0, DEFAULT_FLOW_WINDOW)]
        assert isinstance(frames[-1], RstStreamFrame)
        assert frames[-1].error_code == h2.errors.ErrorCodes.CANCEL

        # Connection quota came back; the dead stream window did not.
        assert c.inbound_flow_control_window == 2 * DEFAULT_FLOW_WINDOW
        assert c.remote_flow_control_window(3) == 0

        # Redeeming the stale credential would double-count the quota, so it
        # must be refused instead.
        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential.complete(c)
        assert not c.data_to_send()

    def test_reset_by_peer_also_settles(self, frame_factory) -> None:
        """
        A RST_STREAM received from the remote peer settles outstanding
        credentials in the same connection-only fashion.
        """
        c = self._setup_connection(frame_factory)

        data_frame = frame_factory.build_data_frame(
            b"\x00" * 65524, stream_id=3, padding_len=10,
        )
        event = c.receive_data(data_frame.serialize())[0]
        credential = event.receive_credential
        c.clear_outbound_data_buffer()

        rst = frame_factory.build_rst_stream_frame(
            stream_id=3, error_code=h2.errors.ErrorCodes.CANCEL,
        )
        events = c.receive_data(rst.serialize())
        assert isinstance(events[0], h2.events.StreamReset)

        frames = parse_frames(c.data_to_send())
        assert window_updates(frames) == [(0, DEFAULT_FLOW_WINDOW)]

        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential.complete(c)

    def test_acknowledgement_after_reset_is_rejected(self, frame_factory) -> None:
        """
        Even after settlement, the stream stays on the credential path: a plain
        acknowledgement cannot hand the same quota back a second time.
        """
        c = self._setup_connection(frame_factory)

        data_frame = frame_factory.build_data_frame(
            b"some data", stream_id=3, padding_len=10,
        )
        c.receive_data(data_frame.serialize())
        c.clear_outbound_data_buffer()
        c.reset_stream(3)
        c.data_to_send()

        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            c.acknowledge_received_data(20, stream_id=3)


class TestReceiveCredentialScenario:
    """
    The end-to-end ledger: two streams, padded DATA, partial redemption, and a
    mid-stream reset, checked by parsing the WINDOW_UPDATE frames directly.
    """

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]
    server_config = h2.config.H2Configuration(
        client_side=False, receive_credentials=True,
    )

    def test_ledger(self, frame_factory) -> None:
        c = h2.connection.H2Connection(config=self.server_config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.update_settings(
            {h2.settings.SettingCodes.MAX_FRAME_SIZE: DEFAULT_FLOW_WINDOW},
        )
        c.receive_data(
            frame_factory.build_settings_frame(settings={}, ack=True).serialize(),
        )
        # Give the connection twice the default window so both streams fit.
        c.increment_flow_control_window(DEFAULT_FLOW_WINDOW)
        c.clear_outbound_data_buffer()

        for stream_id in (1, 3):
            frame = frame_factory.build_headers_frame(
                headers=self.example_request_headers, stream_id=stream_id,
            )
            c.receive_data(frame.serialize())
        c.clear_outbound_data_buffer()

        # Stream 1: 40000 body bytes + 1 pad-length byte + 10 padding bytes.
        padded = frame_factory.build_data_frame(
            b"\x00" * 40000, stream_id=1, padding_len=10,
        )
        plain = frame_factory.build_data_frame(
            b"\x00" * 40000, stream_id=3,
        )
        events = c.receive_data(padded.serialize())
        events += c.receive_data(plain.serialize())
        credential_1 = events[0].receive_credential
        credential_3 = events[1].receive_credential
        assert credential_1.remaining_bytes == 40011
        assert credential_3.remaining_bytes == 40000

        # Connection window: 131070 - 40011 - 40000. Stream windows shrink
        # by their own flow-controlled lengths.
        assert c.inbound_flow_control_window == 2 * DEFAULT_FLOW_WINDOW - 80011

        # Redeem all of stream 1: only the stream window crosses the coalescing
        # threshold, so exactly one stream WINDOW_UPDATE is emitted.
        credential_1.complete(c)
        frames = parse_frames(c.data_to_send())
        assert window_updates(frames) == [(1, 40011)]

        # Redeem stream 3 in two bounded slices; the second one crosses both
        # the connection and the stream thresholds.
        credential_3.partially_complete(c, 20000)
        assert not c.data_to_send()
        credential_3.complete(c)
        frames = parse_frames(c.data_to_send())
        assert window_updates(frames) == [
            (0, 80011),   # connection: 40011 + 20000 + 20000 processed
            (3, 40000),   # stream 3: 20000 + 20000 processed
        ]

        # Both windows are fully restored and every credential is spent.
        assert c.inbound_flow_control_window == 2 * DEFAULT_FLOW_WINDOW
        assert c.remote_flow_control_window(1) == DEFAULT_FLOW_WINDOW
        assert c.remote_flow_control_window(3) == DEFAULT_FLOW_WINDOW
        assert credential_1.is_complete and credential_3.is_complete

        # A repeated completion manufactures nothing.
        for credential in (credential_1, credential_3):
            with pytest.raises(h2.exceptions.ReceiveCredentialError):
                credential.complete(c)
        assert not c.data_to_send()

        # Now stream 3 receives a full-window padded frame and is reset before
        # the application redeems it. Padding still counts: 65524 + 1 + 10.
        pending = frame_factory.build_data_frame(
            b"\x00" * 65524, stream_id=3, padding_len=10,
        )
        events = c.receive_data(pending.serialize())
        credential_pending = events[0].receive_credential
        assert credential_pending.remaining_bytes == DEFAULT_FLOW_WINDOW
        assert c.inbound_flow_control_window == DEFAULT_FLOW_WINDOW

        c.reset_stream(3, error_code=h2.errors.ErrorCodes.CANCEL)
        frames = parse_frames(c.data_to_send())
        assert window_updates(frames) == [(0, DEFAULT_FLOW_WINDOW)]
        rst = frames[-1]
        assert isinstance(rst, RstStreamFrame)
        assert rst.stream_id == 3
        assert rst.error_code == h2.errors.ErrorCodes.CANCEL

        # Connection quota is returned; stream 3's window stays dead.
        assert c.inbound_flow_control_window == 2 * DEFAULT_FLOW_WINDOW
        assert c.remote_flow_control_window(3) == 0

        # Redeeming after reset and plain acknowledgement are both refused, so
        # the settlement cannot be duplicated.
        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            credential_pending.complete(c)
        with pytest.raises(h2.exceptions.ReceiveCredentialError):
            c.acknowledge_received_data(DEFAULT_FLOW_WINDOW, stream_id=3)

        # Stream 1 remains perfectly usable and independent.
        assert c.remote_flow_control_window(1) == DEFAULT_FLOW_WINDOW


class TestReceiveCredentialOptIn:
    """
    The credential interface is opt-in: by default the existing
    acknowledge_received_data accounting is unchanged.
    """

    example_request_headers = [
        (":authority", "example.com"),
        (":path", "/"),
        (":scheme", "https"),
        (":method", "GET"),
    ]

    def _setup(self, frame_factory, *, receive_credentials):
        config = h2.config.H2Configuration(
            client_side=False, receive_credentials=receive_credentials,
        )
        c = h2.connection.H2Connection(config=config)
        c.initiate_connection()
        c.receive_data(frame_factory.preamble())
        c.update_settings(
            {h2.settings.SettingCodes.MAX_FRAME_SIZE: DEFAULT_FLOW_WINDOW},
        )
        c.receive_data(
            frame_factory.build_settings_frame(settings={}, ack=True).serialize(),
        )
        c.clear_outbound_data_buffer()
        frame = frame_factory.build_headers_frame(
            headers=self.example_request_headers, stream_id=1,
        )
        c.receive_data(frame.serialize())
        c.clear_outbound_data_buffer()
        return c

    def test_disabled_by_default(self, frame_factory) -> None:
        c = self._setup(frame_factory, receive_credentials=False)

        data_frame = frame_factory.build_data_frame(
            b"some data", stream_id=1, padding_len=10,
        )
        event = c.receive_data(data_frame.serialize())[0]
        assert event.receive_credential is None

        # The legacy accounting path keeps working.
        c.acknowledge_received_data(20, stream_id=1)
        assert not c.data_to_send()

    def test_reset_without_credentials_keeps_legacy_behaviour(
        self, frame_factory,
    ) -> None:
        c = self._setup(frame_factory, receive_credentials=False)

        data_frame = frame_factory.build_data_frame(
            b"some data", stream_id=1, padding_len=10,
        )
        c.receive_data(data_frame.serialize())
        c.clear_outbound_data_buffer()

        c.reset_stream(1)
        frames = parse_frames(c.data_to_send())
        assert window_updates(frames) == []
        assert isinstance(frames[-1], RstStreamFrame)

    def test_config_must_be_bool(self) -> None:
        with pytest.raises(ValueError):
            h2.config.H2Configuration(receive_credentials="yes")
