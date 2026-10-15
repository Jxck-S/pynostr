"""A relay closed by run_sync() must reconnect on the next run_sync().

prepare_relays() closes relays whose connection outlives the timeout, and
_eose_received() closes them after a subscription completes. Either way the
caller still holds the RelayManager, so the next run_sync() -- typically the
one that sends a queued event -- has to open the connection again.
"""

import asyncio
import json
import threading
import time
import unittest

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.netutil import bind_sockets
from tornado.web import Application
from tornado.websocket import WebSocketHandler

from pynostr.event import Event
from pynostr.filters import Filters, FiltersList
from pynostr.key import PrivateKey
from pynostr.relay_manager import RelayManager


class _RelayHandler(WebSocketHandler):
    """Answers EVENT with OK and REQ with EOSE; otherwise stays silent."""

    def on_message(self, message):
        msg = json.loads(message)
        self.application.settings["received"].append(msg)
        if msg[0] == "EVENT":
            self.write_message(json.dumps(["OK", msg[1]["id"], True, ""]))
        elif msg[0] == "REQ":
            self.write_message(json.dumps(["EOSE", msg[1]]))


class _SilentHandler(WebSocketHandler):
    """Accepts events but never answers them."""

    def on_message(self, message):
        self.application.settings["received"].append(json.loads(message))


class _LocalRelay:
    def __init__(self):
        self.received = []
        self._started = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self._started.wait(5)

    def _run(self):
        asyncio.set_event_loop(asyncio.new_event_loop())
        self.io_loop = IOLoop.current()
        sockets = bind_sockets(0, "127.0.0.1")
        self.port = sockets[0].getsockname()[1]
        self.server = HTTPServer(
            Application(
                [(r"/", _RelayHandler), (r"/silent", _SilentHandler)],
                received=self.received,
            )
        )
        self.server.add_sockets(sockets)
        self._started.set()
        self.io_loop.start()

    @property
    def url(self):
        return f"ws://127.0.0.1:{self.port}/"

    def stop(self):
        self.io_loop.add_callback(self.server.stop)
        self.io_loop.add_callback(self.io_loop.stop)
        self._thread.join(5)

    def events(self):
        return [m for m in self.received if m[0] == "EVENT"]


def _signed_event():
    pk = PrivateKey()
    event = Event("hello")
    event.sign(pk.hex())
    return event


class TestRelayReconnect(unittest.TestCase):
    def setUp(self):
        self.relay = _LocalRelay()

    def tearDown(self):
        self.relay.stop()

    def test_publish_after_timed_out_run_sync(self):
        rm = RelayManager(timeout=0.5)
        rm.add_relay(self.relay.url)
        rm.run_sync()  # nothing queued: idles until the timeout closes it

        event = _signed_event()
        rm.publish_event(event)
        rm.run_sync()
        rm.close_all_relay_connections()

        self.assertEqual([m[1]["id"] for m in self.relay.events()], [event.id])
        oks = rm.message_pool.get_all_ok()
        self.assertEqual([(ok.event_id, ok.ok) for ok in oks], [(event.id, True)])

    def test_publish_after_eose_closed_subscription(self):
        rm = RelayManager(timeout=0.5)
        rm.add_relay(self.relay.url)
        rm.add_subscription_on_all_relays("sub", FiltersList([Filters(limit=1)]))
        rm.run_sync()  # EOSE closes the relay

        event = _signed_event()
        rm.publish_event(event)
        rm.run_sync()
        rm.close_all_relay_connections()

        self.assertEqual([m[1]["id"] for m in self.relay.events()], [event.id])

    def test_all_queued_messages_are_sent(self):
        rm = RelayManager(timeout=0.5)
        rm.add_relay(self.relay.url)
        events = [_signed_event() for _ in range(3)]
        for event in events:
            rm.publish_event(event)
        rm.run_sync()
        rm.close_all_relay_connections()

        self.assertEqual(
            sorted(m[1]["id"] for m in self.relay.events()),
            sorted(e.id for e in events),
        )


class TestCloseAfterOk(unittest.TestCase):
    """run_sync() returns once every published event is acknowledged."""

    def setUp(self):
        self.relay = _LocalRelay()

    def tearDown(self):
        self.relay.stop()

    def test_publish_returns_when_ok_received(self):
        rm = RelayManager(timeout=5)
        rm.add_relay(self.relay.url)
        event = _signed_event()
        rm.publish_event(event)
        start = time.monotonic()
        rm.run_sync()
        elapsed = time.monotonic() - start
        rm.close_all_relay_connections()

        self.assertEqual([m[1]["id"] for m in self.relay.events()], [event.id])
        self.assertLess(elapsed, 2)

    def test_waits_for_every_queued_event(self):
        rm = RelayManager(timeout=5)
        rm.add_relay(self.relay.url)
        events = [_signed_event() for _ in range(3)]
        for event in events:
            rm.publish_event(event)
        rm.run_sync()
        rm.close_all_relay_connections()

        oks = {ok.event_id for ok in rm.message_pool.get_all_ok()}
        self.assertEqual(oks, {e.id for e in events})

    def test_silent_relay_still_times_out(self):
        rm = RelayManager(timeout=1)
        rm.add_relay(self.relay.url + "silent")
        event = _signed_event()
        rm.publish_event(event)
        start = time.monotonic()
        rm.run_sync()
        elapsed = time.monotonic() - start
        rm.close_all_relay_connections()

        self.assertEqual([m[1]["id"] for m in self.relay.events()], [event.id])
        self.assertGreaterEqual(elapsed, 0.9)

    def test_close_on_ok_false_keeps_old_behaviour(self):
        rm = RelayManager(timeout=1)
        rm.add_relay(self.relay.url, close_on_ok=False)
        rm.publish_event(_signed_event())
        start = time.monotonic()
        rm.run_sync()
        elapsed = time.monotonic() - start
        rm.close_all_relay_connections()

        self.assertGreaterEqual(elapsed, 0.9)


class TestStaleRetry(unittest.TestCase):
    def test_retry_scheduled_before_close_is_dropped_after_restart(self):
        rm = RelayManager()
        rm.add_relay("ws://127.0.0.1:9/")
        relay = rm.relays["ws://127.0.0.1:9/"]
        stale = relay._generation
        IOLoop.current().run_sync(relay.close)
        relay.running = True  # what start() does on the next run_sync()
        self.assertIsNone(relay._retry(stale))


if __name__ == "__main__":
    unittest.main()
