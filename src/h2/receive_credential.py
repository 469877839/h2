"""
h2/receive_credential
~~~~~~~~~~~~~~~~~~~~~

Defines the :class:`ReceiveCredential`, a token handed to the application
whenever a DATA frame is received. Instead of accumulating the
``flow_controlled_length`` of received frames itself, the application can
redeem credentials for the flow-controlled bytes it has actually processed.
The connection object then performs the window bookkeeping, which makes it
impossible to acknowledge the same bytes twice or to forget that padding
counts against the flow control window.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from .exceptions import ReceiveCredentialError

if TYPE_CHECKING:  # pragma: no cover
    from .connection import H2Connection


class ReceiveCredential:
    """
    A receipt for the flow-controlled bytes of a single received DATA frame.

    Credentials are created by :class:`H2Connection <h2.connection.H2Connection>`
    and attached to the corresponding
    :class:`DataReceived <h2.events.DataReceived>` event. A credential is bound
    to the connection and stream that received the frame and carries exactly
    the frame's *flow-controlled length*: that is, the request body bytes plus
    any padding, since padding also consumes flow control window space.

    A credential can be redeemed in one go with :meth:`complete`, or a bounded
    number of bytes at a time with :meth:`partially_complete`. Every redemption
    hands the corresponding space back through the connection's automatic flow
    control logic. Once fully redeemed, further redemptions raise
    :class:`ReceiveCredentialError <h2.exceptions.ReceiveCredentialError>`, so
    the same chunk of data can never enlarge the windows twice.

    .. versionadded:: 4.5.0
    """

    def __init__(
        self,
        connection: H2Connection,
        stream_id: int,
        flow_controlled_length: int,
    ) -> None:
        self._connection = connection
        self._stream_id = stream_id
        self._remaining = flow_controlled_length
        self._initial = flow_controlled_length
        self._settled = flow_controlled_length == 0

    def __repr__(self) -> str:
        return (
            f"<ReceiveCredential stream_id:{self._stream_id} "
            f"remaining:{self._remaining} of {self._initial}>"
        )

    @property
    def stream_id(self) -> int:
        """
        The ID of the stream this credential was issued for.
        """
        return self._stream_id

    @property
    def remaining_bytes(self) -> int:
        """
        The number of flow-controlled bytes this credential can still be
        redeemed for.
        """
        return self._remaining

    @property
    def is_complete(self) -> bool:
        """
        Whether every byte represented by this credential has been redeemed.
        """
        return self._settled

    def _check(self, connection: H2Connection, size: int) -> None:
        if connection is not self._connection:
            msg = (
                "ReceiveCredential was issued by a different H2Connection "
                "and cannot be redeemed here"
            )
            raise ReceiveCredentialError(msg)
        if self._settled:
            msg = "ReceiveCredential has already been fully redeemed"
            raise ReceiveCredentialError(msg)
        if size <= 0:
            msg = "Cannot redeem a non-positive number of bytes"
            raise ReceiveCredentialError(msg)
        if size > self._remaining:
            msg = (
                f"Cannot redeem {size} bytes: only {self._remaining} "
                "unredeemed bytes remain on this ReceiveCredential"
            )
            raise ReceiveCredentialError(msg)

    def partially_complete(self, connection: H2Connection, size: int) -> None:
        """
        Redeem up to ``size`` flow-controlled bytes of this credential.

        This is useful when a received DATA frame is processed incrementally:
        each call returns a bounded slice of the frame's window space. The
        redeemed bytes are handed back through the connection's automatic flow
        control logic and may cause ``WINDOW_UPDATE`` frames to be emitted on
        the connection and/or the stream.

        :param connection: The connection that issued this credential.
        :type connection: :class:`H2Connection <h2.connection.H2Connection>`
        :param size: The number of flow-controlled bytes now processed. Must be
            greater than zero and no larger than
            :attr:`remaining_bytes`.
        :type size: ``int``
        :rtype: ``None``
        """
        self._check(connection, size)
        connection._redeem_receive_credential(self, size)
        self._remaining -= size
        if self._remaining == 0:
            self._settled = True

    def complete(self, connection: H2Connection) -> None:
        """
        Redeem every flow-controlled byte this credential still represents.

        Equivalent to calling :meth:`partially_complete` with
        :attr:`remaining_bytes`. Redemptions made earlier in the credential's
        lifetime are taken into account, so calling ``complete`` twice never
        returns any additional window space.

        :param connection: The connection that issued this credential.
        :type connection: :class:`H2Connection <h2.connection.H2Connection>`
        :rtype: ``None``
        """
        self._check(connection, self._remaining)
        connection._redeem_receive_credential(self, self._remaining)
        self._remaining = 0
        self._settled = True
