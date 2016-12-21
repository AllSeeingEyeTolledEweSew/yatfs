import asyncio
import base64
import concurrent
import functools
import logging
import os


def log():
    return logging.getLogger(__name__)


def file_range_split(info, idx, offset, size):
    if b"files" in info:
        for i in range(0, idx):
            offset += info[b"files"][i][b"size"]
        file_size = info[b"files"][idx][b"size"]
    else:
        assert idx == 0
        file_size = info[b"length"]

    size = min(size, file_size - offset)

    if size <= 0:
        return []

    piece_length = info[b"piece_length"]
    pieces = range(
        int(offset // piece_length),
        int((offset + size - 1) / piece_length) + 1)

    split = []
    for p in pieces:
        piece_offset = p * piece_length
        lo = offset - piece_offset
        if lo < 0:
            lo = 0
        hi = offset - piece_offset + size
        if hi > piece_length:
            hi = piece_length
        split.append((p, lo, hi))
    return split


class Bitfield(object):

    def __init__(self, bitstring):
        if not bitstring:
            self.bitfield = b""
        else:
            self.bitfield = base64.b64decode(bitstring)

    def get(self, i):
        if i >> 3 >= len(self.bitfield):
            return False
        return bool(self.bitfield[i >> 3] & (0x80 >> (i & 7)))


class Info(dict):

    def __init__(self, info, cache_status):
        dict.__init__(self, info)
        self[b"piece_bitfield"] = Bitfield(self[b"piece_bitstring"])
        piece_to_write_cache = {}
        for cache_entry in cache_status[b"pieces"]:
            if cache_entry[b"kind"] == 1:
                piece_to_write_cache[cache_entry[b"piece"]] = cache_entry
        self[b"piece_to_write_cache"] = piece_to_write_cache

    def have_piece(self, piece):
        return self[b"piece_bitfield"].get(piece)

    def piece_on_disk(self, piece):
        return (
            self.have_piece(piece) and
            piece not in self[b"piece_to_write_cache"])


class Torrent(object):

    FIELDS = ("files", "piece_length", "piece_bitstring",
              "save_path", "hash", "num_pieces", "piece_priorities",
              "sequential_download", "state", "message")

    def __init__(self, backend, hash):
        self.backend = backend
        self.hash = hash

        self.client = self.backend.client
        self.config = self.backend.config
        self.routine = self.config.routine
        self.loop = self.routine.loop

        self.fis = {}

        self.info_task = None
        self.info_time = None
        self.flush_task = None
        self.flushed_event_f = None
        self.poll_task = None
        self.prioritize_task = None
        self.piece_to_f = {}
        self.we_prioritized_piece = set()

    def log_debug(self, msg, *args):
        log().debug("%s: %s" % (self.hash, msg), *args)

    def log_error(self, msg, *args):
        log().error("%s: %s" % (self.hash, msg), *args)

    def destroy(self):
        if self.info_task:
            self.info_task.cancel()
        if self.flush_task:
            self.flush_task.cancel()
        if self.poll_task:
            self.poll_task.cancel()
        if self.prioritize_task:
            self.prioritize_task.cancel()
        for future in self.piece_to_f.values():
            future.cancel()
        self.piece_to_f = {}
        for fi in self.fis.values():
            fi.destroy()
        self.fis = {}

    def is_info_expired(self):
        if not self.info_task:
            return True
        if not self.info_task.done():
            return False
        if self.info_task.cancelled() or (
                self.info_task.exception() is not None):
            return True
        if (self.loop.time() - self.info_time >
                self.config.params["info_cache_time"]):
            return True
        return False

    def invalidate_info(self):
        if self.info_task:
            self.info_task.cancel()
        self.info_task = None

    def start_info_task(self):
        self.invalidate_info()
        self.info_task = self.loop.create_task(self.do_get_info_async())

    @asyncio.coroutine
    def get_info_async(self):
        if self.is_info_expired():
            self.start_info_task()
        while True:
            try:
                return (yield from asyncio.shield(
                    self.info_task, loop=self.loop))
            except concurrent.futures.CancelledError:
                # Task might have already been restarted.
                if self.info_task is None or self.info_task.cancelled():
                    self.start_info_task()

    @asyncio.coroutine
    def fetch_info_async(self):
        return (yield from self.client.call_async(
            "core.get_torrent_status", self.hash, self.FIELDS))

    @asyncio.coroutine
    def fetch_cache_status_async(self):
        return (yield from self.client.call_async(
            "pieceio.get_cache_info", self.hash))

    @asyncio.coroutine
    def do_get_info_async(self):
        self.log_debug("update_info()")
        info = yield from self.fetch_info_async()
        while b"hash" not in info:
            # Can happen while trying to reprioritize a closing torrent that
            # never got added.
            if not self.fis:
                return None
            data = yield from self.config.get_torrent_data_async(self.hash)
            data = base64.b64encode(data)
            hash = yield from self.client.call_async(
                "core.add_torrent_file", None, data, None)
            assert hash is not None and hash.decode() == self.hash, (hash,
                    self.hash)
            info = yield from self.fetch_info_async()
            yield from self.client.call_async(
                "pieceio.prioritize_pieces", self.hash,
                [0] * info[b"num_pieces"])
            self.we_prioritized_piece = set()
            info = yield from self.fetch_info_async()
        cache_status = yield from self.fetch_cache_status_async()
        self.info_time = self.loop.time()
        info = Info(info, cache_status)
        for piece, future in list(self.piece_to_f.items()):
            if info.have_piece(piece):
                if not future.done():
                    future.set_result(None)
                del self.piece_to_f[piece]
        if not self.piece_to_f:
            if self.poll_task:
                self.poll_task.cancel()
        self.prioritize()
        return info

    def on_cache_flushed(self):
        if self.flushed_event_f and not self.flushed_event_f.done():
            self.flushed_event_f.set_result(None)
        else:
            self.log_debug("spurious cache flush")
        self.invalidate_info()

    @asyncio.coroutine
    def do_flush_cache_async(self):
        self.log_debug("flush_cache")
        self.flushed_event_f = asyncio.Future(loop=self.loop)
        yield from self.client.call_async("pieceio.flush_cache", self.hash)
        yield from asyncio.wait_for(
            self.flushed_event_f, self.config.params["cache_flush_timeout"],
            loop=self.loop)

    @asyncio.coroutine
    def flush_cache_async(self):
        if not self.flush_task or self.flush_task.done():
            self.flush_task = self.loop.create_task(
                self.do_flush_cache_async())
        yield from asyncio.shield(self.flush_task, loop=self.loop)

    @asyncio.coroutine
    def poll_async(self):
        while True:
            self.start_info_task()
            yield from asyncio.sleep(
                self.config.params["info_poll_interval"], loop=self.loop)

    @asyncio.coroutine
    def ensure_piece_async(self, piece, timeout=None):
        info = yield from self.get_info_async()
        if info.have_piece(piece):
            return
        if piece not in self.piece_to_f:
            self.piece_to_f[piece] = asyncio.Future(loop=self.loop)
        if not self.poll_task or self.poll_task.done():
            self.poll_task = self.loop.create_task(self.poll_async())
        yield from asyncio.wait_for(
            asyncio.shield(self.piece_to_f[piece], loop=self.loop), timeout,
            loop=self.loop)

    @asyncio.coroutine
    def ensure_pieces_on_disk_async(self, pieces, timeout=None):
        info = yield from self.get_info_async()
        if all(info.piece_on_disk(p) for p in pieces):
            return
        yield from asyncio.gather(
            *[self.ensure_piece_async(p, timeout=timeout) for p in pieces],
            loop=self.loop)
        while True:
            info = yield from self.get_info_async()
            if all(info.piece_on_disk(p) for p in pieces):
                return
            yield from self.flush_cache_async()

    def prioritize(self):
        if self.prioritize_task:
            self.prioritize_task.cancel()
        self.prioritize_task = self.loop.create_task(self.prioritize_async())

    @asyncio.coroutine
    def prioritize_async(self):
        info = yield from self.get_info_async()
        if info is None:
            assert not self.fis, self.fis
            return

        changes = []

        if not info[b"sequential_download"]:
            changes.append(self.client.call_async(
                "pieceio.set_sequential_download", self.hash, True))

        num_pieces = info[b"num_pieces"]
        piece_length = info[b"piece_length"]
        piece_priorities = list(info[b"piece_priorities"])

        desired_priorities = { p: 0 for p in range(num_pieces) }
        lasts = set()
        reading_now = set()
        readahead = set()

        for fi in self.fis.values():
            pieces = fi.last_read_pieces
            reading_now.update(pieces)
            if pieces:
                lasts.add(max(pieces))

        for p in lasts:
            lo = p + 1
            n = max(
                self.config.params["readahead_pieces"],
                self.config.params["readahead_bytes"] // piece_length)
            hi = min(num_pieces, lo + n)
            readahead.update(range(lo, hi))

        desired_priorities.update({ p: 4 for p in readahead })
        desired_priorities.update({ p: 7 for p in reading_now })

        modified = {}
        for p, prio in desired_priorities.items():
            if info.have_piece(p):
                continue
            if piece_priorities[p] != prio:
                if piece_priorities[p] == 0 and prio != 0:
                    self.we_prioritized_piece.add(p)
                elif piece_priorities[p] != 0 and prio == 0:
                    if p not in self.we_prioritized_piece:
                        continue
                piece_priorities[p] = prio
                modified[p] = prio

        if modified:
            self.log_debug("want %s", modified)
            changes.append(self.client.call_async(
                "pieceio.prioritize_pieces", self.hash, piece_priorities))

        if info[b"state"] == b"Paused":
            changes.append(self.client.call_async(
                "core.resume_torrent", [self.hash]))

        if changes:
            yield from asyncio.gather(*changes, loop=self.loop)
            self.invalidate_info()


class FileInfo(object):

    def __init__(self, torrent, idx):
        self.torrent = torrent
        self.idx = idx

        self.backend = self.torrent.backend
        self.config = self.backend.config
        self.routine = self.config.routine
        self.loop = self.routine.loop

        self.file_task = None
        self.last_read_pieces = ()

    def log_debug(self, msg, *args):
        log().debug("%s:%s: %s" % (self.torrent.hash, self.idx, msg), *args)

    def destroy(self):
        if self.file_task:
            self.file_task.cancel()
        self.file_task = None

    @asyncio.coroutine
    def file_async(self):
        if not self.file_task:
            self.file_task = self.loop.create_task(self.do_file_async())
        return (yield from asyncio.shield(self.file_task, loop=self.loop))

    @asyncio.coroutine
    def do_file_async(self):
        self.log_debug("file_async")
        info = yield from self.torrent.get_info_async()
        path = os.path.join(
            info[b"save_path"], info[b"files"][self.idx][b"path"])
        return (yield from self.routine.call_io_async(
            functools.partial(open, path, mode="rb")))

    @asyncio.coroutine
    def read_async(self, offset, size, piece_timeout=None):
        info = yield from self.torrent.get_info_async()
        split = file_range_split(info, self.idx, offset, size)
        pieces = [p for p, lo, hi in split]
        self.last_read_pieces = set(pieces)
        self.torrent.prioritize()
        yield from self.torrent.ensure_pieces_on_disk_async(
            pieces, timeout=piece_timeout)
        f = yield from self.file_async()
        return (yield from self.routine.call_io_async(
            os.pread, f.fileno(), size, offset))


class Backend(object):

    SESSION_SETTINGS = (
        "close_redundant_connections", "strict_end_game_mode",
        "smooth_connects", "min_reconnect_time", "max_failcount",
        "connection_speed", "connections_limit", "torrent_connect_boost")

    def __init__(self, client, config):
        self.client = client
        self.config = config
        self.routine = self.config.routine
        self.loop = self.routine.loop

        self.server_info = None
        self.server_info_time = None
        self.server_info_task = None

        self.prioritize_task = None
        self.last_release = None
        self.poll_task = None

        self.torrents = {}
        self.fis = {}
        self.next_fh = 0

    def is_server_info_expired(self):
        if not self.server_info_task:
            return True
        if not self.server_info_task.done():
            return False
        if self.server_info_task.cancelled() or (
                self.server_info_task.exception() is not None):
            return True
        if (self.loop.time() - self.server_info_time >
                self.config.params["server_info_cache_time"]):
            return True
        return False

    def invalidate_server_info(self):
        if self.server_info_task:
            self.server_info_task.cancel()
        self.server_info_task = None

    def start_server_info_task(self):
        self.invalidate_server_info()
        self.server_info_task = self.loop.create_task(
            self.do_get_server_info_async())

    @asyncio.coroutine
    def get_server_info_async(self):
        if self.is_server_info_expired():
            self.start_server_info_task()
        while True:
            try:
                return (yield from asyncio.shield(
                    self.server_info_task, loop=self.loop))
            except concurrent.futures.CancelledError:
                # Task might have already been restarted.
                if self.server_info_task.cancelled():
                    self.start_server_info_task()

    @asyncio.coroutine
    def do_get_server_info_async(self):
        log().debug("get_server_info")
        lt_version, plugins = yield from asyncio.gather(
            self.client.call_async("core.get_libtorrent_version"),
            self.client.call_async("core.get_enabled_plugins"),
            loop=self.loop)
        self.server_info_time = self.loop.time()
        if b"PieceIO" in plugins:
            session_settings = yield from self.client.call_async(
                "pieceio.session_get_settings", self.SESSION_SETTINGS)
        else:
            session_settings = None
        self.prioritize()
        return {
            b"lt_version": lt_version,
            b"plugins": plugins,
            b"session_settings": session_settings}

    @asyncio.coroutine
    def validate_server_info_async(self):
        server_info = yield from self.get_server_info_async()
        if server_info[b"lt_version"] != b"1.1.1.0":
            raise OSError(errno.EIO, "wrong libtorrent version")
        if b"PieceIO" not in server_info[b"plugins"]:
            raise OSError(errno.EIO, "the PieceIO plugin is not enabled")

    def prioritize(self):
        if self.prioritize_task:
            self.prioritize_task.cancel()
        self.prioritize_task = self.loop.create_task(self.prioritize_async())

    @asyncio.coroutine
    def prioritize_async(self):
        log().debug("prioritize")
        server_info = yield from self.get_server_info_async()
        if not server_info[b"session_settings"]:
            return

        desired_settings = {
            b"close_redundant_connections": False,
            b"strict_end_game_mode": False,
            b"smooth_connects": False,
            b"min_reconnect_time": 15,
            b"max_failcount": 5,
            b"connection_speed": 500,
            b"connections_limit": 964,
            b"torrent_connect_boost": 50,
        }

        changes = []

        if (server_info[b"session_settings"] != desired_settings):
            changes.append(self.client.call_async(
                "pieceio.session_set_settings",
                **{k.decode(): v for k, v in desired_settings.items()}))

        if changes:
            yield from asyncio.gather(*changes, loop=self.loop)
            self.invalidate_server_info()

    @asyncio.coroutine
    def poll_async(self):
        while True:
            self.start_server_info_task()
            yield from asyncio.sleep(
                self.config.params["server_info_poll_interval"],
                loop=self.loop)

    def init(self):
        return self.routine.call_in_loop(self.init_async())

    @asyncio.coroutine
    def init_async(self):
        log().debug("init")
        yield from self.client.register_event_handler_async(
            "CacheFlushedEvent", self.on_cache_flushed)
        self.poll_task = self.loop.create_task(self.poll_async())

    def destroy(self):
        return self.routine.call_in_loop(self.destroy_async())

    @asyncio.coroutine
    def destroy_async(self):
        log().debug("destroy")
        self.poll_task.cancel()
        if self.server_info_task:
            self.server_info_task.cancel()
        if self.prioritize_task:
            self.prioritize_task.cancel()
        yield from self.client.deregister_event_handler_async(
            "CacheFlushedEvent", self.on_cache_flushed)

    def open(self, hash, index, flags):
        return self.routine.call_in_loop(self.open_async(hash, index, flags))

    @asyncio.coroutine
    def open_async(self, hash, file_index, flags):
        log().debug("open(%s,%s,%s)", hash, file_index, flags)
        fh = self.next_fh
        self.next_fh += 1
        if hash not in self.torrents:
            self.torrents[hash] = Torrent(self, hash)
        torrent = self.torrents[hash]
        fi = FileInfo(torrent, file_index)
        torrent.fis[fh] = fi
        self.fis[fh] = fi
        self.prioritize()
        return fh

    def release(self, hash, index, fh):
        return self.routine.call_in_loop(self.release_async(hash, index, fh))

    @asyncio.coroutine
    def release_async(self, hash, file_index, fh):
        log().debug("release(%s,%s)", hash, file_index)
        self.last_release = self.loop.time()
        destroy_fi = None
        destroy_torrent = None
        if fh in self.fis:
            destroy_fi = self.fis.pop(fh)
        if hash in self.torrents:
            torrent = self.torrents[hash]
            if fh in torrent.fis:
                del torrent.fis[fh]
            if not torrent.fis:
                destroy_torrent = torrent
                del self.torrents[hash]
            if torrent.prioritize_task:
                torrent.prioritize_task.cancel()
            torrent.prioritize()
            try:
                yield from torrent.prioritize_task
            except concurrent.futures.CancelledError:
                pass
        if destroy_fi:
            destroy_fi.destroy()
        if destroy_torrent:
            destroy_torrent.destroy()

    def on_cache_flushed(self, hash):
        hash = hash.decode()
        log().debug("on_cache_flushed(%s)", hash)
        if hash in self.torrents:
            self.torrents[hash].on_cache_flushed()

    def read(self, hash, index, offset, size, fh):
        return self.routine.call_in_loop(
            self.read_async(hash, index, offset, size, fh))

    @asyncio.coroutine
    def read_async(self, hash, idx, offset, size, fh):
        yield from self.validate_server_info_async()
        return (yield from self.fis[fh].read_async(
            offset, size, piece_timeout=self.config.params["piece_timeout"]))
