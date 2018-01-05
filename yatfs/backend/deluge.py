import base64
import collections
import errno
import logging
import os
import threading
import time
import weakref

import better_bencode
import concurrent.futures
import llfuse

import deluge_client_sync
from yatfs import fs


DEFAULT_KEY = "yatfs"
DEFAULT_KEEPALIVE = 60
DEFAULT_READ_TIMEOUT = 60
DEFAULT_READAHEAD_PIECES = 4
DEFAULT_READAHEAD_BYTES = 0x2000000
DEFAULT_READAHEAD_PRIORITY = 4
DEFAULT_READING_PRIORITY = 7


def log():
    return logging.getLogger(__name__)


def file_range_split(info, idx, offset, size):
    offset_to_file = 0
    if b"files" in info:
        for i in range(0, idx):
            offset_to_file += info[b"files"][i][b"size"]
        file_size = info[b"files"][idx][b"size"]
    else:
        assert idx == 0
        file_size = info[b"length"]

    offset += offset_to_file

    size = min(size, file_size - (offset - offset_to_file))

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


class Info(dict):

    def have_piece(self, i):
        if i >> 3 >= len(self.get(b"yatfsrpc.piece_bitfield", b"")):
            return False
        return bool(
            self[b"yatfsrpc.piece_bitfield"][i >> 3] & (0x80 >> (i & 7)))


class Backend(object):

    def __init__(self, client, key=None, keepalive=None, read_timeout=None,
                 readahead_pieces=None, readahead_bytes=None,
                 readahead_priority=None, reading_priority=None):
        self.client = client
        self.keepalive = float(keepalive or DEFAULT_KEEPALIVE)
        self.read_timeout = float(read_timeout or DEFAULT_READ_TIMEOUT)
        self.readahead_pieces = int(
            readahead_pieces or DEFAULT_READAHEAD_PIECES)
        self.readahead_bytes = int(
            readahead_bytes or DEFAULT_READAHEAD_BYTES)
        self.key = key or DEFAULT_KEY
        self.readahead_priority = int(
            readahead_priority or DEFAULT_READAHEAD_PRIORITY)
        self.reading_priority = int(
            reading_priority or DEFAULT_READING_PRIORITY)

        self.lock = threading.RLock()
        self.torrents = {}
        self.initialized = False
        self.shutdown = None

    def key_prefix(self):
        return self.key + "_"

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
            b"TorrentAddedEvent", self.on_torrent_add)
        self.client.add_event_handler(
            b"TorrentRemovedEvent", self.on_torrent_remove)
        self.client.add_event_handler(
            b"YatfsReadPieceEvent", self.on_read_piece)

    def destroy(self):
        with self.lock:
            assert self.initialized
            self.initialized = False
            self.shutdown.set()

        self.client.remove_event_handler(
            b"TorrentAddedEvent", self.on_torrent_add)
        self.client.remove_event_handler(
            b"TorrentRemovedEvent", self.on_torrent_remove)
        self.client.remove_event_handler(
            b"YatfsReadPieceEvent", self.on_read_piece)

    def update_all(self):
        with self.lock:
            for info_hash, torrent in list(self.torrents.items()):
                torrent.cleanup()
                if not torrent.is_alive():
                    del self.torrents[info_hash]
            torrents = list(self.torrents.values())
        torrent_requests = [(t, t.request_update()) for t in torrents]
        hash_to_info_request = self.client.request(
            "core.get_torrents_status", {}, (
                "yatfsrpc.piece_priority_map",
                "yatfsrpc.keep_redundant_connections_map"))

        updates = []

        for torrent, request in torrent_requests:
            try:
                result = request.result()
            except concurrent.futures.TimeoutError:
                # Don't let a chain of timeouts hold up the updates
                raise
            except:
                log().exception("during update request")
                continue
            for update in torrent.reduce(result):
                updates.append(update)

        try:
            hash_to_info = hash_to_info_request.result()
        except:
            log().exception("during global update request")
            hash_to_info = {}

        for update in self.do_global_update(hash_to_info):
            updates.append(update)

        # Get results of all updates, to surface any exceptions
        for update in updates:
            try:
                update.result()
            except concurrent.futures.TimeoutError:
                # Don't let a chain of timeouts hold up the updates
                raise
            except:
                log().exception("during an update")
                continue

    def do_global_update(self, hash_to_info):
        for info_hash, info in hash_to_info.items():
            if info_hash in self.torrents:
                continue
            delete_p_ks = [
                k for k in info[b"yatfsrpc.piece_priority_map"]
                if k.startswith(self.key_prefix().encode())]
            if delete_p_ks:
                yield self.client.request(
                    "yatfsrpc.update_piece_priority_map", info_hash,
                    delete=delete_p_ks)
            delete_k_ks = [
                k for k in info[b"yatfsrpc.keep_redundant_connections_map"]
                if k.startswith(self.key_prefix().encode())]
            if delete_k_ks:
                yield self.client.request(
                    "yatfsrpc.update_keep_redundant_connections_map",
                    info_hash, delete=delete_k_ks)

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

    def on_torrent_add(self, torrent_id):
        with self.lock:
            torrent = self.torrents.get(torrent_id)

        if not torrent:
            return

        torrent.on_add()

    def on_torrent_remove(self, torrent_id):
        with self.lock:
            torrent = self.torrents.get(torrent_id)

        if not torrent:
            return

        torrent.on_remove()

    def on_read_piece(self, torrent_id, piece, data, error):
        with self.lock:
            torrent = self.torrents.get(torrent_id)

        if not torrent:
            return

        torrent.on_read_piece(piece, data, error)


class Torrent(object):

    FIELDS = (b"files", b"piece_length", b"yatfsrpc.piece_bitfield",
              b"save_path", b"hash", b"num_pieces",
              b"yatfsrpc.sequential_download",
              b"yatfsrpc.piece_priority_map", b"state",
              b"yatfsrpc.keep_redundant_connections_map")

    def __init__(self, backend, info_hash):
        self.backend = backend
        self.info_hash = info_hash

        self.lock = self.backend.lock
        self.cv = threading.Condition(self.lock)
        self.client = self.backend.client
        self.info = None
        self.handles = weakref.WeakSet()
        self.last_released = None
        self.read_piece_to_f = collections.defaultdict(weakref.WeakSet)
        self.pre_info_reads = set()
        self.p_to_t_readahead = {}

    def key_prefix(self):
        return self.backend.key_prefix()

    def piece_split(self, index, offset, size, pieces):
        with self.lock:
            info = self.info
        size = max(size, pieces * info[b"piece_length"])
        return file_range_split(info, index, offset, size)

    def piece_range(self, index, offset, size, pieces):
        return [p for p, _, _ in self.piece_split(index, offset, size, pieces)]

    def iter_reads(self):
        with self.lock:
            for handle in list(self.handles):
                for read in list(handle.iter_reads()):
                    yield read

    def apply_delta(self, key, old, value):
        if key == b"yatfsrpc.sequential_download":
            yield self.client.request(
                b"yatfsrpc.set_sequential_download", self.info_hash, value)
        if key == b"yatfsrpc.piece_priority_map":
            yield self.client.request(
                b"yatfsrpc.update_piece_priority_map", self.info_hash,
                update=value, delete=list(set(old.keys()) - set(value.keys())))
        if key == b"paused" and old and not value:
            yield self.client.request(
                b"core.resume_torrent", [self.info_hash])
        if key == b"yatfsrpc.keep_redundant_connections":
            key = self.key_prefix() + "keepalive"
            if value:
                kwargs = {"update": {key: value}}
            else:
                kwargs = {"delete": (key,)}
            yield self.client.request(
                b"yatfsrpc.update_keep_redundant_connections_map",
                self.info_hash, **kwargs)

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

        priority_maps = {}
        for read in self.iter_reads():
            target_p_to_prio = {}
            priority = self.backend.reading_priority
            for p in read.pieces():
                if self.info.have_piece(p):
                    continue
                target_p_to_prio[p] = priority
            id = str(hash(read))
            key = (self.key_prefix() + id).encode()
            if target_p_to_prio:
                priority_maps[key] = target_p_to_prio
        target_p_to_prio = {}
        for p in self.p_to_t_readahead.keys():
            if self.info.have_piece(p):
                continue
            target_p_to_prio[p] = self.backend.readahead_priority
        key = (self.key_prefix() + "readahead").encode()
        if target_p_to_prio:
            priority_maps[key] = target_p_to_prio
        target_info[b"yatfsrpc.piece_priority_map"] = priority_maps

        priority_maps = {}
        for k, p_to_prio in self.info[b"yatfsrpc.piece_priority_map"].items():
            if not k.startswith(self.key_prefix().encode()):
                continue
            filtered_p_to_prio = {}
            for p, prio in p_to_prio.items():
                if self.info.have_piece(p):
                    continue
                filtered_p_to_prio[p] = prio
            if filtered_p_to_prio:
                priority_maps[k] = filtered_p_to_prio
        self.info[b"yatfsrpc.piece_priority_map"] = priority_maps

        self.info[b"paused"] = self.info[b"state"] == b"Paused"

        if self.is_alive():
            target_info[b"paused"] = False
            target_info[b"yatfsrpc.sequential_download"] = True
        self.info[b"yatfsrpc.keep_redundant_connections"] = bool(
            self.info[b"yatfsrpc.keep_redundant_connections_map"].get(
                (self.key_prefix() + "keepalive").encode()))
        target_info[b"yatfsrpc.keep_redundant_connections"] = self.is_alive()

        return target_info

    def cleanup(self):
        now = time.time()
        with self.lock:
            self.p_to_t_readahead= {
                p: t for p, t in self.p_to_t_readahead.items() if t >= now}

    def add_readahead_with_info(self, read):
        size_bytes = max(read.size, self.backend.readahead_bytes)
        size_pieces = self.backend.readahead_pieces
        pieces = self.piece_range(
            read.file_index(), read.offset, size_bytes, size_pieces)
        t = time.time() + self.backend.keepalive
        with self.lock:
            self.p_to_t_readahead.update({p: t for p in pieces})

    def add_readahead(self, read):
        with self.lock:
            info = self.info
            if not info:
                self.pre_info_reads.add(read)
                return
        self.add_readahead_with_info(read)

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
            for read in self.pre_info_reads:
                self.add_readahead_with_info(read)
            self.pre_info_reads = set()
            self.cv.notifyAll()
            if b"hash" not in info:
                return None
            target_info = self.get_target_info_locked()

        deltas = self.apply_deltas(info, target_info)
        return deltas

    def update(self):
        request = self.request_update()
        result = request.result()
        self.reduce(result)

    def is_alive(self):
        with self.lock:
            return bool(self.handles or self.p_to_t_readahead)

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

    def wait_for_pieces(self, inode, pieces_callback, timeout):
        def have_pieces(info):
            return all(info.have_piece(p) for p in pieces_callback())
        self.info_wait(have_pieces, inode, timeout)

    def read_piece_request(self, piece, timeout=None):
        f = concurrent.futures.Future()
        f.set_running_or_notify_cancel()
        with self.lock:
            info = self.info
            self.read_piece_to_f[piece].add(f)
        self.client.call("yatfsrpc.read_piece", info[b"hash"], piece)
        return f

    def on_read_piece(self, piece, data, error):
        with self.lock:
            fs = self.read_piece_to_f.pop(piece, [])
        if error[b"value"]:
            log().error("Reading piece %s: %s", piece, error)
        for f in fs:
            if error[b"value"]:
                f.set_exception(OSError(errno.EIO, "read piece error"))
            else:
                f.set_result(data)


class Read(object):

    def __init__(self, handle, offset, size):
        self.inode = handle.inode
        self.torrent = handle.torrent
        self.backend = handle.backend
        self.offset = offset
        self.size = size
        self.time = time.time()

    def file_index(self):
        return self.inode.file_index()

    def split(self):
        return self.torrent.piece_split(
            self.file_index(), self.offset, self.size, 0)

    def pieces(self):
        return self.torrent.piece_range(
            self.file_index(), self.offset, self.size, 0)

    def wait(self):
        self.torrent.wait_for_pieces(
            self.inode, self.pieces, self.backend.read_timeout)

    def read(self):
        log().debug("read: %s:%s[%s:%s]", self.torrent.info_hash,
                self.file_index(), self.offset, self.size)
        split = self.split()
        log().debug("split is: %s", split)
        split = [
            (self.torrent.read_piece_request(p), lo, hi)
            for p, lo, hi in split]
        try:
            return b"".join(
                f.result(timeout=self.backend.read_timeout)[lo:hi]
                for f, lo, hi in split)
        except concurrent.futures.TimeoutError:
            raise OSError(errno.ETIMEDOUT, "timeout reading piece")


class Handle(fs.TorrentHandle):

    def __init__(self, backend, inode, torrent):
        super(Handle, self).__init__(backend, inode)
        self.torrent = torrent

        self.lock = torrent.lock
        self.reads = weakref.WeakSet()

    def iter_reads(self):
        for read in list(self.reads):
            yield read

    def read(self, offset, size):
        read = Read(self, offset, size)
        with self.lock:
            self.reads.add(read)

        self.torrent.add_readahead(read)

        try:
            read.wait()
            return read.read()
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

    def release(self):
        pass


def configure_backend(
        host=None, port=None, username=None, password=None, config_dir=None,
        timeout=None, **kwargs):
    client = deluge_client_sync.Client(
        host=host, port=port, username=username, password=password,
        config_dir=config_dir, timeout=timeout)
    return Backend(client, **kwargs)
