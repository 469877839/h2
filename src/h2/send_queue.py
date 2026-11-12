"""
h2/send_queue
~~~~~~~~~~~~~

An optional flow-control-aware queue of outbound DATA payloads.

The queue lets callers hand data to the protocol layer before there is enough
flow-control window (or room in a frame) to put it on the wire. The
:class:`~h2.connection.H2Connection` drains the queue whenever a window is
opened, data is queued, or another event makes progress possible, and emits
the corresponding ``DATA`` frames.

Scheduling is strictly byte-fair between streams: streams are served in FIFO
order, and after a stream is served once it is moved to the back of the
rotation regardless of how much data it still has pending. No stream gets a
second turn while another stream is waiting for its first.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


@dataclass
class _QueuedData:
    """
    A single contiguous run queued for one stream.

    Runs are split only on queue boundaries chosen by the caller; the
    connection further divides them into frames. ``end_stream`` marks the run
    that carries the ``END_STREAM`` flag.
    """

    data: bytes
    end_stream: bool = False


@dataclass
class _StreamQueue:
    """
    The pending state for one stream.
    """

    chunks: deque[_QueuedData] = field(default_factory=deque)
    queued: int = 0
    # Set when the stream could not make progress on its current turn. It is
    # cleared by fresh data, a new flush pass, or an explicit unblock. This
    # keeps an under-sized chunk from being retried inside one flush pass
    # while still allowing every other stream to take its turn.
    blocked: bool = False

    @property
    def has_end_stream(self) -> bool:
        return any(chunk.end_stream for chunk in self.chunks)


class SendQueue:
    """
    An optional, byte-fair queue of outbound ``DATA`` payloads.

    The queue only stores payloads and bookkeeping. Turning payloads into
    frames, enforcing flow control windows, and updating stream state is the
    job of the connection that owns and drains the queue.

    :param max_size: (optional) The maximum number of pending bytes kept for
        the whole connection. ``None`` means unbounded. Defaults to ``None``.
    :type max_size: ``int`` or ``None``
    """

    def __init__(self, max_size: int | None = None) -> None:
        self.max_size = max_size
        self._streams: dict[int, _StreamQueue] = {}
        self._rotation: deque[int] = deque()
        self._total = 0

    @property
    def total_pending(self) -> int:
        """
        The number of bytes waiting in the queue across all streams.
        """
        return self._total

    @property
    def stream_ids(self) -> list[int]:
        """
        The stream IDs with pending payloads, in scheduling order.
        """
        return list(self._rotation)

    def pending(self, stream_id: int) -> int:
        """
        The number of bytes queued for ``stream_id``.

        These bytes have *not* been handed to the protocol layer and can still
        be withdrawn with :meth:`cancel`.
        """
        state = self._streams.get(stream_id)
        return state.queued if state is not None else 0

    def pending_end_stream(self, stream_id: int) -> bool:
        """
        Whether ``stream_id`` has a queued ``END_STREAM`` marker.
        """
        state = self._streams.get(stream_id)
        return state is not None and state.has_end_stream

    def can_accept(self, amount: int) -> bool:
        """
        Whether ``amount`` bytes can be accepted without exceeding the queue
        limit. Zero-byte insertions are always accepted.
        """
        if amount == 0 or self.max_size is None:
            return True
        return self._total + amount <= self.max_size

    def enqueue(self, stream_id: int, data: bytes | memoryview, end_stream: bool = False) -> int:
        """
        Queue payload for ``stream_id``, respecting the queue limit.

        If accepting all of ``data`` would exceed the limit, as much as fits
        is queued and the rest is left with the caller. The number of accepted
        bytes is returned, so the caller always knows exactly which bytes were
        handed over and which are still theirs to keep or cancel.

        An empty payload with ``end_stream`` set is always accepted and does
        not count against the limit: it only records that the stream should be
        closed once the preceding payloads have been flushed.
        """
        raw = bytes(data) if not isinstance(data, bytes) else data
        length = len(raw)

        if length and self.max_size is not None and self._total + length > self.max_size:
            room = max(self.max_size - self._total, 0)
            length = room
            raw = raw[:room]
            if length == 0 and not end_stream:
                return 0

        if length == 0 and not end_stream:
            # A zero-byte, non-END_STREAM insertion is a no-op.
            return 0

        state = self._streams.get(stream_id)
        if state is None:
            state = _StreamQueue()
            self._streams[stream_id] = state
            self._rotation.append(stream_id)

        if length:
            state.chunks.append(_QueuedData(raw, end_stream=end_stream))
            state.queued += length
            self._total += length
        else:
            if state.has_end_stream:
                # END_STREAM was already promised; a second one would produce
                # two END_STREAM frames, so ignore it.
                return 0
            state.chunks.append(_QueuedData(b"", end_stream=True))

        # Fresh payload invalidates any "no room" judgement from an earlier
        # flush pass.
        state.blocked = False
        return length

    def cancel(self, stream_id: int) -> tuple[bytes, bool]:
        """
        Withdraw everything still queued for ``stream_id``.

        Returns ``(data, end_stream)``: every payload byte that had not yet
        been handed to the protocol layer, in order, plus whether a queued
        ``END_STREAM`` marker was also withdrawn. The stream leaves the
        rotation.
        """
        state = self._streams.pop(stream_id, None)
        if state is None:
            return b"", False

        try:
            self._rotation.remove(stream_id)
        except ValueError:  # pragma: no cover
            pass

        data = b"".join(chunk.data for chunk in state.chunks)
        end_stream = state.has_end_stream
        self._total -= state.queued
        return data, end_stream

    def reset_blocked(self) -> None:
        """
        Clear the per-stream blocked flags at the start of a flush pass so
        that every stream gets a fresh chance to make progress.
        """
        for state in self._streams.values():
            state.blocked = False

    def select(self) -> int | None:
        """
        Choose the next stream to serve without modifying the rotation.

        Blocked streams are skipped. A stream with no payload is still
        selectable when it has a bare ``END_STREAM`` run at the head. Returns
        ``None`` when no stream can make progress.
        """
        for stream_id in self._rotation:
            state = self._streams[stream_id]
            if not state.blocked and state.chunks:
                return stream_id
        return None

    def next_run(self, stream_id: int) -> _QueuedData:
        """
        Peek at the head run for a stream selected by :meth:`select`.
        """
        return self._streams[stream_id].chunks[0]

    def consume(self, stream_id: int, amount: int) -> _QueuedData:
        """
        Remove ``amount`` bytes from the head run of ``stream_id`` and return
        them as a new run, preserving the head run's ``END_STREAM`` flag only
        when the whole run was consumed.

        The connection calls this once it has decided exactly how many bytes
        the next frame may carry under the connection window, stream window
        and maximum frame size.
        """
        state = self._streams[stream_id]
        head = state.chunks[0]

        if amount >= len(head.data):
            run = self._pop_head(stream_id)
            return run

        # A partial frame never carries END_STREAM: keep the flag on the
        # remainder so it ends up on the final frame (or an empty DATA frame).
        emitted = _QueuedData(head.data[:amount], end_stream=False)
        head.data = head.data[amount:]
        state.queued -= amount
        self._total -= amount
        return emitted

    def emit_empty_end_stream(self, stream_id: int) -> _QueuedData:
        """
        Consume the bare ``END_STREAM`` run at the head of the stream's queue
        and return it. Callers must only invoke this when the head run is
        empty and carries the flag.
        """
        return self._pop_head(stream_id)

    def mark_blocked(self, stream_id: int) -> None:
        """
        Record that a selected stream cannot make any progress in this flush
        pass, allowing the rotation to move on to other streams.
        """
        self._streams[stream_id].blocked = True

    def complete_turn(self, stream_id: int) -> None:
        """
        Move a stream to the back of the rotation after it has been served,
        or drop it if nothing remains queued.
        """
        if not self._streams[stream_id].chunks:
            self._drop(stream_id)
            return
        self._rotate(stream_id)

    def clear(self) -> None:
        """
        Drop all pending payloads.
        """
        self._streams.clear()
        self._rotation.clear()
        self._total = 0

    def _pop_head(self, stream_id: int) -> _QueuedData:
        state = self._streams[stream_id]
        run = state.chunks.popleft()
        state.queued -= len(run.data)
        self._total -= len(run.data)
        return run

    def _rotate(self, stream_id: int) -> None:
        try:
            self._rotation.remove(stream_id)
        except ValueError:  # pragma: no cover
            pass
        self._rotation.append(stream_id)

    def _drop(self, stream_id: int) -> None:
        self._streams.pop(stream_id, None)
        try:
            self._rotation.remove(stream_id)
        except ValueError:  # pragma: no cover
            pass
