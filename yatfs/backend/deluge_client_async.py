#!/usr/bin/python3.4

import asyncio
import logging
import os
import rencode
import ssl
import zlib


RPC_RESPONSE = 1
RPC_ERROR = 2
RPC_EVENT = 3


DEFAULT_PORT = 58846
DEFAULT_TIMEOUT = 30


def log():
    return logging.getLogger(__name__)


def get_localhost_auth():
    try:
        from xdg.BaseDirectory import save_config_path
    except ImportError:
        return (None, None)
    path = os.path.join(save_config_path("deluge"), "auth")
    if not os.path.exists(path):
        return (None, None)
    with open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            line = line.strip()
            lsplit = line.split(":")

            if len(lsplit) in (2, 3):
                username, password = lsplit[:2]

            if username == "localclient":
                return (username, password)
    return (None, None)


def _decode_recursive(obj):
    if type(obj) is bytes:
        return os.fsdecode(obj)
    if type(obj) is dict:
        return {_decode_recursive(k): _decode_recursive(v)
                for k, v in obj.items()}
    if type(obj) is list:
        return [_decode_recursive(x) for x in obj]
    if type(obj) is tuple:
        return tuple(_decode_recursive(x) for x in obj)
    return obj


class Error(Exception):

    pass


class EOF(Error):

    def __init__(self):
        super(EOF, self).__init__("EOF")


class RPCError(Error):

    def __init__(self, request, exc_type, exc_message, exc_traceback):
        super(RPCError, self).__init__(exc_message)
        self.request = request
        self.type = exc_type
        self.traceback = exc_traceback


class Request(object):

    def __init__(self, request_id, method, args, kwargs):
        self.request_id = request_id
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.future = None


class MessageProtocol(asyncio.Protocol):

    def __init__(self, message_received, connection_lost, decode=False):
        self.message_received = message_received
        self.connection_lost = connection_lost
        self.decode = decode

        self.transport = None
        self._buffer = bytes()

    def data_received(self, data):
        self._buffer += data

        while self._buffer:
            decompressobj = zlib.decompressobj()
            try:
                data = decompressobj.decompress(self._buffer)
            except zlib.error:
                # Probably short read?
                return

            self._buffer = decompressobj.unused_data

            try:
                message = rencode.loads(data)
            except rencode.error:
                log().exception("bad data from server")
                continue

            if self.decode:
                message = _decode_recursive(message)

            log().debug("received: %s", message)

            try:
                self.message_received(message)
            except Exception:
                log().exception("Processing message: %s", message)

    def send_messages(self, *messages):
        assert self.transport
        log().debug("sending: %s", messages)
        data = zlib.compress(rencode.dumps(messages))
        self.transport.write(data)

    def connection_made(self, transport):
        self.transport = transport


class Client(object):

    def __init__(self, host=None, port=None, username=None, password=None,
                 loop=None, timeout=None, decode=False):
        self.host = host or "localhost"
        self.port = port or DEFAULT_PORT
        self.username = username or ""
        self.password = password or ""
        self.loop = loop or asyncio.get_event_loop()
        self.timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
        self.decode = decode

        self.protocol = None
        self.logged_in = False

        self._event_to_handlers = {}
        self._next_request_id = 0
        self._id_to_request = {}

    def _call(self, method, *args, **kwargs):
        assert self.protocol
        request = Request(self._next_request_id, method, args, kwargs)
        self._next_request_id += 1
        request.future = asyncio.Future(loop=self.loop)
        self._id_to_request[request.request_id] = request
        message = (
            request.request_id, request.method, request.args, request.kwargs)
        self.protocol.send_messages(message)
        return request.future

    def message_received(self, message):
        msg_type = message[0]

        if msg_type == RPC_EVENT:
            event_name, event_data = message[1:3]
            log().debug("RPC_EVENT: %s", event_name)
            for handler in self._event_to_handlers.get(event_name, ()):
                if asyncio.iscoroutinefunction(handler):
                    self.loop.create_task(handler(*event_data))
                else:
                    self.loop.call_soon(handler, *event_data)
        elif msg_type in (RPC_RESPONSE, RPC_ERROR):
            request_id, result = message[1:3]
            if request_id not in self._id_to_request:
                log().debug(
                    "got response to msg %s, which doesn't exist", request_id)
                return
            request = self._id_to_request[request_id]

            try:
                if not request.future.done():
                    if msg_type == RPC_RESPONSE:
                        request.future.set_result(result)
                    else:
                        exc_type, message, exc_traceback = result[0:3]
                        request.future.set_exception(RPCError(
                            request, exc_type, message, exc_traceback))
                else:
                    log().debug(
                        "msg %s already complete?", request_id)
            finally:
                del self._id_to_request[request_id]
        else:
            raise TypeError("Unknown RPC message type %s", msg_type)

    def connection_lost(self, exc):
        exc = exc or EOF()
        log().debug("connection lost: %s", exc)

        self.protocol = None
        self.logged_in = False

        for request in self._id_to_request.values():
            if not request.future.done():
                request.future.set_exception(exc)

        self._next_request_id = 0
        self._id_to_request = {}

    def protocol_factory(self):
        return MessageProtocol(
                self.message_received, self.connection_lost,
                decode=self.decode)

    def ssl_factory(self):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    @asyncio.coroutine
    def connect_and_login_async(self):
        if not self.protocol:
            _, self.protocol = (yield from self.loop.create_connection(
                self.protocol_factory, self.host, self.port,
                ssl=self.ssl_factory()))

        if not self.logged_in:
            username, password = (self.username, self.password)

            if not username and self.host in ("127.0.0.1", "localhost"):
                username, password = get_localhost_auth()

            yield from self._call("daemon.login", username, password)
            yield from self._call(
                "daemon.set_event_interest",
                list(self._event_to_handlers.keys()))

            self.logged_in = True

    @asyncio.coroutine
    def call_async(self, method, *args, timeout=None, **kwargs):
        if timeout is None:
            timeout = self.timeout
        yield from self.connect_and_login_async()
        return (yield from asyncio.wait_for(
            self._call(method, *args, **kwargs), timeout, loop=self.loop))

    @asyncio.coroutine
    def register_event_handler_async(self, event_name, handler):
        if self.decode:
            event_name = os.fsdecode(event_name)
        else:
            event_name = os.fsencode(event_name)
        if event_name not in self._event_to_handlers:
            self._event_to_handlers[event_name] = []
            if self.protocol:
                yield from self._call("daemon.set_event_interest",
                    [event_name])
        if handler not in self._event_to_handlers[event_name]:
            self._event_to_handlers[event_name].append(handler)

    @asyncio.coroutine
    def deregister_event_handler_async(self, event_name, handler):
        if self.decode:
            event_name = os.fsdecode(event_name)
        else:
            event_name = os.fsencode(event_name)
        yield
        if event_name in self._event_to_handlers:
            if handler in self._event_to_handlers[event_name]:
                self._event_to_handlers[event_name].remove(handler)
            if not self._event_to_handlers[event_name]:
                del self._event_to_handlers[event_name]
