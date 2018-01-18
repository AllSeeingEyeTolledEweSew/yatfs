import errno
import functools
import logging
import os
import stat
import sys
import threading
import weakref

import llfuse


class NoopLock(object):

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


llfuse.lock = NoopLock()


def log():
    return logging.getLogger(__name__)


class Inode(object):

    attr_timeout = 1
    entry_timeout = 1
    st_mode_type = 0
    st_mode_perm = 0

    def __init__(self):
        self.refcount = 0
        self.st_ino = None
        self.st_size = 0
        self.st_atime_ns = 0
        self.st_mtime_ns = 0
        self.st_ctime_ns = 0

    def getattr(self):
        e = llfuse.EntryAttributes()
        if self.st_ino is not None:
            e.st_ino = self.st_ino
        e.st_mode = (
            stat.S_IFMT(self.st_mode_type) | stat.S_IMODE(self.st_mode_perm))
        e.st_size = self.st_size
        e.st_atime_ns = self.st_atime_ns
        e.st_mtime_ns = self.st_mtime_ns
        e.st_ctime_ns = self.st_ctime_ns
        return e

    def access(self, mask):
        return True


class Symlink(Inode):

    st_mode_type = stat.S_IFLNK
    st_mode_perm = 0o777

    def readlink(self):
        raise llfuse.FUSEError(errno.ENOSYS)


class StaticSymlink(Symlink):

    def __init__(self, value):
        super(Symlink, self).__init__()
        self.value = value

    def readlink(self):
        return self.value


class File(Inode):

    st_mode_type = stat.S_IFREG
    st_mode_perm = 0o444

    def open(self, flags):
        return FileHandle(self, flags)

    def read(self, offset, size):
        raise llfuse.FUSEError(errno.EIO)


class TorrentFile(File):

    def __init__(self, backend):
        super(TorrentFile, self).__init__()
        self.backend = backend

    def open(self, flags):
        return self.backend.open(self, flags)

    def info_hash(self):
        raise NotImplementedError()

    def file_index(self):
        raise NotImplementedError()

    def raw_torrent(self):
        raise NotImplementedError()


class Data(File):

    def data(self):
        raise llfuse.FUSEError(errno.EIO)

    def read(self, offset, size):
        return self.data()[offset:offset + size]

    def getattr(self):
        e = super(Data, self).getattr()
        e.st_size = len(self.data())
        return e


class Dir(Inode):

    st_mode_type = stat.S_IFDIR
    st_mode_perm = 0o555

    def __init__(self):
        super(Dir, self).__init__()
        self.cache_lock = threading.RLock()
        self.cache = weakref.WeakValueDictionary()

    def lookup(self, name):
        with self.cache_lock:
            child = self.find(name)
            if not child:
                child = self.lookup_create(name)
                self.cache[name] = child
        return child

    def lookup_create(self, name):
        raise llfuse.FUSEError(errno.ENOENT)

    def find(self, name):
        with self.cache_lock:
            return self.cache.get(name)

    def opendir(self):
        return DirHandle(self)

    def readdir(self, offset):
        return []


class StaticDir(Dir):

    def __init__(self):
        super(StaticDir, self).__init__()
        self.dirents = {}

    def mkdentry(self, name, inode):
        self.dirents[name] = inode

    def lookup_create(self, name):
        inode = self.dirents.get(name)
        if not inode:
            raise llfuse.FUSEError(errno.ENOENT)
        return inode

    def readdir(self, offset):
        dirents = []
        for i, (name, inode) in enumerate(sorted(self.dirents.items())):
            dirents.append((name, inode.getattr(), i + 1))
        return dirents[offset:]


class FileHandle(object):

    def __init__(self, inode, flags):
        self.inode = inode
        self.flags = flags
        self.fh = None

    def read(self, offset, size):
        return self.inode.read(offset, size)

    def release(self):
        pass


class TorrentHandle(object):

    def __init__(self, backend, inode):
        self.backend = backend
        self.inode = inode
        self.fh = None

    def read(self, offset, size):
        raise NotImplementedError()

    def release(self):
        pass


class DirHandle(object):

    def __init__(self, inode):
        self.inode = inode
        self.fh = None

    def readdir(self, offset):
        return self.inode.readdir(offset)

    def release(self):
        pass


class Filesystem(object):

    def __init__(self, backend, root):
        self.backend = backend
        self.root = root

        self.lock = threading.RLock()

        self.inodes = {}
        self.handles = {}
        self._next_ino = llfuse.ROOT_INODE + 1
        self._next_fh = 0

    def next_ino(self):
        with self.lock:
            ino = self._next_ino
            self._next_ino += 1
            return ino

    def next_fh(self):
        with self.lock:
            fh = self._next_fh
            self._next_fh += 1
            return fh

    def inode_require(self, ino):
        with self.lock:
            inode = self.inodes.get(ino)
        if inode is None:
            raise llfuse.FUSEError(errno.ENOENT)
        return inode

    def inode_incref(self, inode):
        with self.lock:
            if inode.st_ino is None:
                inode.st_ino = self.next_ino()
            self.inodes[inode.st_ino] = inode
            inode.refcount += 1

    def inode_decref(self, inode, n):
        with self.lock:
            inode.refcount -= n
            if inode.refcount <= 0:
                self.inodes.pop(inode.st_ino)

    def handle_require(self, fh):
        with self.lock:
            handle = self.handles.get(fh)
        if handle is None:
            raise llfuse.FUSEError(errno.EBADF)
        return handle

    def handle_add(self, handle):
        with self.lock:
            if handle.fh is None:
                handle.fh = self.next_fh()
            self.handles[handle.fh] = handle

    def handle_remove(self, handle):
        with self.lock:
            self.handles.pop(handle.fh)

    def init(self):
        self.root.st_ino = llfuse.ROOT_INODE
        self.inode_incref(self.root)
        self.backend.init()

    def destroy(self):
        with self.lock:
            handles = list(self.handles.values())
            for handle in handles:
                handle.release()
            self.handles.clear()

            self.inodes.clear()

        self.backend.destroy()


class Operations(llfuse.Operations):

    def __init__(self, fs):
        self.fs = fs

    def init(self):
        self.fs.init()

    def destroy(self):
        self.fs.destroy()

    def lookup(self, parent, name, ctx):
        inode = self.fs.inode_require(parent).lookup(name)
        self.fs.inode_incref(inode)
        return inode.getattr()

    def forget(self, forgets):
        for ino, nlookup in forgets:
            try:
                inode = self.fs.inode_require(ino)
                self.fs.inode_decref(inode, nlookup)
            except:
                log().exception("during forget(%s,%s)", ino, nlookup)

    def getattr(self, ino, ctx):
        return self.fs.inode_require(ino).getattr()

    def readlink(self, ino, ctx):
        return self.fs.inode_require(ino).readlink()

    def open(self, ino, flags, ctx):
        h = self.fs.inode_require(ino).open(flags)
        self.fs.handle_add(h)
        return h.fh

    def read(self, fh, offset, size):
        return self.fs.handle_require(fh).read(offset, size)

    def release(self, fh):
        h = self.fs.handle_require(fh)
        try:
            h.release()
        finally:
            self.fs.handle_remove(h)

    def opendir(self, ino, ctx):
        h = self.fs.inode_require(ino).opendir()
        self.fs.handle_add(h)
        return h.fh

    def readdir(self, fh, offset):
        h = self.fs.handle_require(fh)
        for name, entry, next_ in h.readdir(offset):
            if type(entry) is int:
                mode = entry
                entry = llfuse.EntryAttributes()
                entry.st_mode = mode
            # Optimization: if we already have an inode for a child, return it
            # to save a lookup.
            child = h.inode.find(name)
            if (child and entry.st_mode == child.st_mode_type and
                    entry.st_ino != child.st_ino):
                entry.st_ino = child.st_ino
            # The kernel treats inode 0 as invalid and ignores it, but any
            # nonzero inode it hasn't seen yet will trigger a lookup.
            if not entry.st_ino:
                entry.st_ino = sys.maxint
            yield (name, entry, next_)

    def releasedir(self, fh):
        h = self.fs.handle_require(fh)
        try:
            h.release()
        finally:
            self.fs.handle_remove(h)

    def statfs(self, ctx):
        data = llfuse.StatvfsData()
        data.f_bsize = os.sysconf("SC_PAGE_SIZE")
        data.f_frsize = os.sysconf("SC_PAGE_SIZE")
        data.f_namemax = 255
        return data

    def access(self, ino, mode, ctx):
        return self.fs.inode_require(ino).access(mode)
