import base64
import collections
import errno
import logging
import os
import threading
import time

import better_bencode
import concurrent.futures
import llfuse

import deluge_client_sync
from yatfs import fs


DEFAULT_KEEPALIVE = 60
DEFAULT_READ_TIMEOUT = 60
DEFAULT_READAHEAD_PIECES = 4
DEFAULT_READAHEAD_BYTES = 0x2000000


def log():
    return logging.getLogger(__name__)


def file_range_pieces(info, idx, offset, size_bytes, size_pieces):
    offset_to_file = 0
    if b"files" in info:
        for i in range(0, idx):
            offset_to_file += info[b"files"][i][b"size"]
        file_size = info[b"files"][idx][b"size"]
    else:
        assert idx == 0
        file_size = info[b"length"]

    offset += offset_to_file

    piece_length = info[b"piece_length"]
    first_piece = int(offset // piece_length)

    size_bytes = min(size_bytes, file_size - (offset - offset_to_file))

    if size_bytes <= 0 and size_pieces <= 0:
        return []

    piece_max = int((offset + size_bytes - 1) / piece_length) + 1
    piece_max = max(piece_max, first_piece + size_pieces)

    return list(range(first_piece, piece_max))


class Info(dict):

    def __init__(self, info):
        dict.__init__(self, info)
        piece_to_write_cache = {}
        for cache_entry in self.get(b"cache_status", {}).get(b"pieces", []):
            if cache_entry[b"kind"] == 1:
                piece_to_write_cache[cache_entry[b"piece"]] = cache_entry
        self[b"piece_to_write_cache"] = piece_to_write_cache

    def have_piece(self, i):
        if i >> 3 >= len(self.get(b"piece_bitfield", b"")):
            return False
        return bool(self[b"piece_bitfield"][i >> 3] & (0x80 >> (i & 7)))

    def piece_on_disk(self, piece):
        return (
            self.have_piece(piece) and
            piece not in self[b"piece_to_write_cache"])


class Backend(object):

    def __init__(self, client, keepalive=None, read_timeout=None,
                 readahead_pieces=None, readahead_bytes=None):
        self.client = client
        self.keepalive = float(keepalive or DEFAULT_KEEPALIVE)
        self.read_timeout = float(read_timeout or DEFAULT_READ_TIMEOUT)
        self.readahead_pieces = int(
            readahead_pieces or DEFAULT_READAHEAD_PIECES)
        self.readahead_bytes = int(
            readahead_bytes or DEFAULT_READAHEAD_BYTES)

        self.lock = threading.RLock()
        self.torrents = {}
        self.initialized = False
        self.shutdown = None

    def open(self, inode, flags):
        with self.lock:
            torrent = self.get_torrent(inode.info_hash())
            handle = Handle(self, inode, torrent)
            torrent.handles.add(handle)
        return handle

    def get_torrent(self, info_hash):
        with self.lock:
            torrent = self.torrents.get(info_hash)
            if not torrent:
                torrent = Torrent(self, info_hash)
                self.torrents[info_hash] = torrent
            return torrent

    def init(self):
        with self.lock:
            assert not self.initialized
            self.initialized = True
            self.shutdown = threading.Event()
            updater = threading.Thread(
                name="update-all", target=self.updater,
                args=(self.shutdown,))
            updater.daemon = True
            updater.start()

        self.client.add_event_handler(
            "TorrentAddedEvent", self.on_torrent_add)
        self.client.add_event_handler(
            "TorrentRemovedEvent", self.on_torrent_remove)
        self.client.add_event_handler(
            "CacheFlushedEvent", self.on_cache_flush)

    def destroy(self):
        with self.lock:
            assert self.initialized
            self.initialized = False
            self.shutdown.set()

        self.client.remove_event_handler(
            "TorrentAddedEvent", self.on_torrent_add)
        self.client.remove_event_handler(
            "TorrentRemovedEvent", self.on_torrent_remove)
        self.client.remove_event_handler(
            "CacheFlushedEvent", self.on_cache_flush)

    def update_all(self):
        with self.lock:
            torrents = list(self.torrents.values())
        torrent_requests = [(t, t.request_update()) for t in torrents]
        for torrent, request in torrent_requests:
            try:
                result = request.result()
            except concurrent.futures.TimeoutError:
                # Don't let a chain of timeouts hold up the updates
                raise
            except:
                log().exception("during update")
                continue
            torrent.cleanup()
            alive_before = torrent.is_alive()
            torrent.reduce(result)
            if not alive_before:
                with self.lock:
                    if not torrent.is_alive():
                        log().info("dereferencing %s", torrent.info_hash)
                        self.torrents.pop(torrent.info_hash)

    def updater(self, shutdown):
        log().debug("start")
        try:
            while not shutdown.is_set():
                try:
                    self.update_all()
                except:
                    log().exception("during update-all")
                time.sleep(1)
        except:
            log().exception("fatal error")
        finally:
            log().debug("shutting down")

    def on_torrent_add(self, event):
        with self.lock:
            torrent = self.torrents.get(event.torrent_id)

        if not torrent:
            return

        torrent.on_add()

    def on_torrent_remove(self, event):
        with self.lock:
            torrent = self.torrents.get(event.torrent_id)

        if not torrent:
            return

        torrent.on_remove()

    def on_cache_flush(self, event):
        with self.lock:
            torrent = self.torrents.get(event.torrent_id)

        if not torrent:
            return

        torrent.on_cache_flush()


class Torrent(object):

    FIELDS = (b"files", b"piece_length", b"piece_bitfield",
              b"save_path", b"hash", b"num_pieces", b"sequential_download",
              b"piece_priority_map", b"state", b"cache_status",
              b"keep_redundant_connections")

    def __init__(self, backend, info_hash):
        self.backend = backend
        self.info_hash = info_hash

        self.lock = self.backend.lock
        self.cv = threading.Condition(self.lock)
        self.client = self.backend.client
        self.info = None
        self.handles = set()
        self.last_released = None

    def piece_range(self, index, offset, size_bytes, size_pieces):
        with self.lock:
            info = self.info
        return file_range_pieces(info, index, offset, size_bytes, size_pieces)

    def iter_reads(self):
        with self.lock:
            for handle in list(self.handles):
                for read in list(handle.iter_reads()):
                    yield read

    def cleanup(self):
        with self.lock:
            pass

    def apply_delta(self, key, old, value):
        if key == b"sequential_download":
            yield self.client.request(
                b"pieceio.set_sequential_download", self.info_hash, value)
        if key == b"yatfs_piece_priority_map":
            yield self.client.request(
                b"pieceio.update_piece_priority_map", self.info_hash,
                update=value, pop=list(set(old.keys()) - set(value.keys())))
        if key == b"paused" and old and not value:
            yield self.client.request(
                b"core.resume_torrent", [self.info_hash])
        if key == b"keep_redundant_connections":
            yield self.client.request(
                b"pieceio.set_keep_redundant_connections",
                self.info_hash, value)

    def apply_deltas(self, info, target_info):
        changes = []
        for key, value in list(target_info.items()):
            old = info.get(key)
            if old != value:
                for change in self.apply_delta(key, old, value):
                    changes.append(change)
        return changes

    def get_target_info_locked(self):
        target_info = {}

        piece_length = self.info[b"piece_length"]

        priority_map = { } 

        for read in self.iter_reads():
            id = hash(read)
            priority_map[("yatfs_%s_reading" % id).encode()] = {
                p: 7 for p in read.reading_pieces() }
            priority_map[("yatfs_%s_readahead" % id).encode()] = {
                p: 4 for p in read.readahead_pieces() }

        target_info[b"yatfs_piece_priority_map"] = priority_map
        self.info[b"yatfs_piece_priority_map"] = {
            k: m for k, m in list(self.info[b"piece_priority_map"].items())
            if k.startswith(b"yatfs_")}

        self.info[b"paused"] = self.info[b"state"] == b"Paused"

        if self.is_alive():
            target_info[b"paused"] = False
            target_info[b"sequential_download"] = True
        target_info[b"keep_redundant_connections"] = self.is_alive()

        return target_info

    def request_update(self):
        return self.client.request(
                "core.get_torrent_status", self.info_hash, self.FIELDS)

    def add(self, raw_torrent):
        tinfo = better_bencode.loads(raw_torrent)[b"info"]
        if b"files" in tinfo:
            num_files = len(tinfo[b"files"])
        else:
            num_files = 1
        options = {b"file_priorities": [0] * num_files}
        self.client.call(
            "core.add_torrent_file", None, base64.b64encode(raw_torrent), 
            options)

    def reduce(self, status_dict):
        info = Info(status_dict)

        with self.lock:
            self.info = info
            self.cv.notifyAll()
            if b"hash" not in info:
                return None
            target_info = self.get_target_info_locked()
            should_flush = False
            reading_want = set()
            for read in self.iter_reads():
                if read.finished is not None:
                    continue
                reading_want.update(read.reading_pieces())
            if any(info.have_piece(p) and not info.piece_on_disk(p)
                    for p in reading_want):
                should_flush = True

        deltas = self.apply_deltas(info, target_info)
        if should_flush:
            deltas.append(self.client.request(
                "pieceio.flush_cache", self.info_hash))
        return deltas

    def update(self):
        request = self.request_update()
        result = request.result()
        self.reduce(result)

    def is_alive(self):
        with self.lock:
            if self.last_released is None:
                return True
            now = time.time()
            if self.last_released > now:
                return False
            keepalive_time = self.last_released + self.backend.keepalive
            if now > keepalive_time:
                return False
            return False

    def on_cache_flushed(self):
        self.update()

    def on_add(self):
        self.update()

    def on_remove(self):
        self.update()

    def info_wait(self, callback, inode, timeout):
        start_time = time.time()
        while True:
            need_update = False
            need_add = False

            with self.cv:
                while True:
                    if self.info is None:
                        need_update = True
                        break
                    if b"hash" not in self.info:
                        need_add = True
                        break
                    r = callback(self.info)
                    if r:
                        return r
                    now = time.time()
                    if now < start_time:
                        raise llfuse.FUSEError(errno.ETIMEDOUT)
                    wait = start_time + timeout - now
                    if wait < 0:
                        raise llfuse.FUSEError(errno.ETIMEDOUT)
                    self.cv.wait(wait)

            if need_add:
                self.add(inode.raw_torrent())
            if need_update:
                self.update()

    def wait_for_pieces_on_disk(self, inode, pieces_callback, timeout):
        def data_on_disk(info):
            for p in pieces_callback():
                if not info.piece_on_disk(p):
                    return False
            return True
        self.info_wait(data_on_disk, inode, timeout)

    def wait_for_path(self, inode, timeout):
        def get_path(info):
            if b"save_path" not in info:
                return
            if b"files" not in info:
                return
            if len(info[b"files"]) <= inode.file_index():
                return
            return os.path.join(
                info[b"save_path"],
                info[b"files"][inode.file_index()][b"path"])
        return self.info_wait(get_path, inode, timeout)


class Read(object):

    def __init__(self, handle, offset, size):
        self.handle = handle
        self.inode = handle.inode
        self.torrent = handle.torrent
        self.backend = handle.backend
        self.offset = offset
        self.size = size
        self.finished = None

    def file_index(self):
        return self.inode.file_index()

    def reading_pieces(self):
        return self.torrent.piece_range(
            self.file_index(), self.offset, self.size, 0)

    def readahead_pieces(self):
        size_bytes = max(self.size, self.backend.readahead_bytes)
        size_pieces = self.backend.readahead_pieces
        return self.torrent.piece_range(
            self.file_index(), self.offset, size_bytes, size_pieces)

    def wait_for_data_on_disk(self):
        self.torrent.wait_for_pieces_on_disk(
            self.inode, self.reading_pieces, self.backend.read_timeout)

    def is_alive(self):
        if self.finished is None:
            return True
        if self.finished < time.time():
            return False
        if self.finished + self.backend.readahead_time > time.time():
            return True
        return False


class Handle(fs.TorrentHandle):

    def __init__(self, backend, inode, torrent):
        super(Handle, self).__init__(backend, inode)
        self.torrent = torrent

        self.lock = torrent.lock
        self.reads = set()
        self.files = set()
        self.released = None

    def cleanup(self):
        with self.lock:
            self.reads = set(r for r in self.reads if r.is_alive())

    def open_file(self):
        path = self.torrent.wait_for_path(
            self.inode, self.backend.read_timeout)
        return open(path, mode="rb")

    def iter_reads(self):
        for read in list(self.reads):
            yield read

    def read_inner(self, offset, size):
        try:
            f = self.files.pop()
        except KeyError:
            f = self.open_file()

        try:
            f.seek(offset)
            return f.read(size)
        finally:
            self.files.add(f)

    def read(self, offset, size):
        read = Read(self, offset, size)
        with self.lock:
            self.reads.add(read)

        try:
            try:
                read.wait_for_data_on_disk()
                return self.read_inner(offset, size)
            except OSError as e:
                log().exception(
                    "during read(%s, %s, %s, %s)", self.inode.info_hash(),
                    self.inode.file_index(), offset, size)
                raise llfuse.FUSEError(e.errno)
            except:
                log().exception(
                    "during read(%s, %s, %s, %s)", self.inode.info_hash(),
                    self.inode.file_index(), offset, size)
                raise llfuse.FUSEError(errno.EIO)
        finally:
            with self.lock:
                read.finished = time.time()

    def release(self):
        with self.lock:
            self.released = True


def configure_backend(
        host=None, port=None, username=None, password=None, config_dir=None,
        timeout=None, **kwargs):
    client = deluge_client_sync.Client(
        host=host, port=port, username=username, password=password,
        config_dir=config_dir, timeout=timeout)
    return Backend(client, **kwargs)
