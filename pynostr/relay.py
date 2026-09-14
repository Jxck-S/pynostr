import logging
from typing import Optional

from tornado import gen, httpclient
from tornado.ioloop import IOLoop
from tornado.simple_httpclient import HTTPTimeoutError
from tornado.tcpclient import TCPClient
from tornado.websocket import (
    WebSocketClientConnection,
    WebSocketError,
    websocket_connect,
)

from .base_relay import BaseRelay, RelayPolicy
from .message_pool import MessagePool

log = logging.getLogger(__name__)


class _ConnectTimeoutTCPClient(TCPClient):
    """A TCPClient that passes `timeout` through to the connector.

    Tornado's simple_httpclient calls ``TCPClient.connect()`` without a
    ``timeout`` argument, so ``_Connector`` never arms ``on_connect_timeout()``
    -- and that is the only thing that calls ``close_streams()``. The
    HTTP-level timeout fires independently, raises, and leaves every half-open
    stream behind. Supplying the timeout here arms the connector's own cleanup.
    """

    def __init__(self, connect_timeout: float, **kwargs) -> None:
        super().__init__(**kwargs)
        self._connect_timeout = connect_timeout

    def connect(self, *args, **kwargs):
        kwargs.setdefault("timeout", self._connect_timeout)
        return super().connect(*args, **kwargs)


def _websocket_connect(url: str, connect_timeout: Optional[float], **kwargs):
    """``websocket_connect`` that closes its socket when the connect times out.

    A host that accepts the SYN and then goes silent otherwise leaks one file
    descriptor per attempt, for the life of the process.

    This relies on two things Tornado does not document: that
    ``WebSocketClientConnection`` keeps its ``TCPClient`` on a ``tcp_client``
    attribute, and that it schedules its ``run()`` coroutine rather than
    starting it, which leaves a window to replace that attribute. Both are
    checked below, and any failure falls back to the stock helper -- at worst
    restoring the old leak, never breaking the connection.
    """
    if not connect_timeout:
        return websocket_connect(url, **kwargs)
    try:
        request = httpclient.HTTPRequest(
            url, connect_timeout=connect_timeout, request_timeout=connect_timeout
        )
        conn = WebSocketClientConnection(request, **kwargs)
        if not isinstance(getattr(conn, "tcp_client", None), TCPClient):
            # Not the shape we expect; don't silently do nothing.
            raise AttributeError("WebSocketClientConnection has no tcp_client")
        conn.tcp_client = _ConnectTimeoutTCPClient(connect_timeout)
        return conn.connect_future
    except Exception as err:  # pragma: no cover - depends on tornado internals
        log.debug(f"falling back to stock websocket_connect: {err}")
        return websocket_connect(url, connect_timeout=connect_timeout, **kwargs)


class Relay(BaseRelay):
    def __init__(
        self,
        url: str,
        message_pool: MessagePool,
        io_loop: IOLoop,
        policy: Optional[RelayPolicy] = None,
        timeout: float = 2.0,
        close_on_eose: bool = True,
        message_callback=None,
        message_callback_url=False,
    ) -> None:
        if policy is None:
            policy = RelayPolicy()
        super().__init__(
            url,
            policy,
            message_pool,
            timeout,
            close_on_eose,
            message_callback,
            message_callback_url,
        )
        self.ws = None
        self.io_loop = io_loop
        self.running = True

    @property
    def is_connected(self) -> bool:
        return self.ws is not None and self.ws.protocol is not None

    @gen.coroutine
    def connect(self):
        if not self.running:
            # Closed while a retry was pending on the IOLoop. Opening a socket
            # now would leak it: whoever closed us is no longer holding a
            # reference and will never close it again.
            return
        error = False
        timeout_error = False
        self.error_counter = 0
        self.timeout_error_counter = 0
        try:
            # connect_timeout rather than gen.with_timeout: the latter only
            # stops waiting, it does not cancel. The abandoned websocket_connect
            # kept running, completed, and left a socket nobody held a
            # reference to -- one leaked file descriptor per timed-out relay,
            # until the process hit its open-file limit. Tornado's own
            # connect_timeout aborts the underlying connection and closes it.
            self.ws = yield _websocket_connect(
                self.url,
                self.timeout if self.timeout > 0 else None,
                ping_interval=60,
                ping_timeout=60,
            )
            self.connected = True
            # self.io_loop.call_later(1, self.send_message, self.request)
            while True:
                if self.outgoing_messages.qsize() > 0:
                    message = self.outgoing_messages.get()
                    self.num_sent_events += 1
                    self.ws.write_message(message)
                message = yield self.ws.read_message()
                if message is None:
                    break
                self._on_message(message)
                if not self.connected:
                    break

        except (gen.TimeoutError, HTTPTimeoutError):
            log.info(f"Timeout connecting to {self.url}")
            timeout_error = True
        except WebSocketError as e:
            log.warning(f"Error connecting to WebSocket server at {self.url}: {e}")
            error = True
        except Exception as e:
            log.warning(f"Error connecting to {self.url}: {e}")
            error = True
        if error:
            self.error_counter += 1
            if self.running and self.error_counter <= self.error_threshold:
                self.io_loop.call_later(1, self.connect)
            else:
                return
        elif timeout_error:
            self.timeout_error_counter += 1
            if self.running and self.timeout_error_counter <= self.timeout_error_threshold:
                self.io_loop.call_later(1, self.connect)
            else:
                return

        log.info(f"WebSocket connection to {self.url} closed")

    @gen.coroutine
    def _eose_received(self):
        self.eose_counter += 1
        if self.close_on_eose and self.eose_counter >= self.eose_threshold:
            yield self.close()

    @gen.coroutine
    def on_error(self):
        self.error_counter += 1
        if self.error_counter > self.error_threshold:
            yield self.close()

    @gen.coroutine
    def start(self):
        self.running = True
        yield self.connect()

    @gen.coroutine
    def close(self):
        # Set first and unconditionally: this is what stops pending retries
        # from reopening the connection after the caller has finished with us.
        # Previously close() only reset the counters, which if anything made
        # the relay retry *more*.
        self.running = False
        self.connected = False
        if self.ws is not None:
            self.error_counter = 0
            self.timeout_error_counter = 0
            yield self.ws.close()
            self.ws = None
            # self.io_loop.stop()
