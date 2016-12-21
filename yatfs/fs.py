import errno
import logging
import os
import sys

from yatfs import fusell


ROOT_INO = 1
DEFAULT_ATTR_TIMEOUT = 1
DEFAULT_ENTRY_TIMEOUT = 1


def log():
    return logging.getLogger(__name__)


class TorrentFs(fusell.FUSELL):

    XATTRS = {
        "user.hash": "t_hash",
        "user.index": "t_index",
    }

    def __init__(self, mountpoint, config, **kwargs):
        self.config = config
        self.inodb = self.config.inodb
        self.backend = self.config.backend
        super(TorrentFs, self).__init__(
            mountpoint, fsname=self.config.inodb.path, subtype="yatfs",
            auto_unmount=True, **kwargs)

    @property
    def entry_timeout(self):
        return DEFAULT_ENTRY_TIMEOUT

    @property
    def attr_timeout(self):
        return DEFAULT_ATTR_TIMEOUT

    def init(self, userdata, conn):
        log().debug("init")
        self.backend.init()

    def destroy(self, userdata):
        log().debug("destroy")
        self.backend.destroy()

    def catch(self, req, context, *args):
        _, e, _ = sys.exc_info()
        if context:
            if e:
                log().exception(context, *args)
            else:
                log().error(context, *args)
        if e and isinstance(e, OSError):
            e = e.errno
        else:
            e = errno.EIO
        return self.reply_err(req, e)

    def getattr(self, req, ino, fh):
        log().debug("getattr(%s)", ino)
        try:
            attr = self.inodb.getattr_ino(ino)
        except:
            self.catch(req, "getattr(%s)", ino)
        else:
            self.reply_attr(req, attr, self.attr_timeout)

    def lookup(self, req, parent, name):
        log().debug("lookup(%s,%s)", parent, name)
        try:
            ino = self.inodb.lookup_ino(parent, name)
            attr = self.inodb.getattr_ino(ino)
            entry = dict(
                ino=ino, attr=attr,
                attr_timeout=self.attr_timeout,
                entry_timeout=self.entry_timeout)
        except:
            self.catch(req, "lookup(%s,%s)", parent, name)
        else:
            self.reply_entry(req, entry)

    def setattr(self, req, ino, attr, to_set, fh):
        log().debug("setattr(%s,%s,%s)", ino, attr, to_set)
        try:
            kwargs = {k: getattr(attr, k) for k in to_set}
            with self.inodb:
                self.inodb.setattr_ino(ino, **kwargs)
                attr = self.inodb.getattr_ino(ino)
        except:
            self.catch(req, "setattr(%s,%s,%s)", ino, attr, to_set)
        else:
            self.reply_attr(req, attr, self.attr_timeout)

    def mkdir(self, req, parent, name, mode):
        log().debug("mkdir(%s,%s)", parent, name)
        try:
            ctx = self.req_ctx(req)
            with self.inodb:
                ino = self.inodb.mkdir_ino(
                    parent, name, mode, ctx.uid, ctx.gid)
                attr = self.inodb.getattr_ino(ino)
                entry = dict(
                    ino=ino, attr=attr,
                    attr_timeout=self.attr_timeout,
                    entry_timeout=self.entry_timeout)
        except:
            self.catch(req, "mkdir(%s,%s)", ino, name)
        else:
            self.reply_entry(req, entry)

    def unlink(self, req, parent, name):
        log().debug("unlink(%s,%s)", parent, name)
        try:
            with self.inodb:
                self.inodb.unlink_ino(parent, name)
        except:
            self.catch(req, "unlink(%s,%s)", parent, name)
        else:
            self.reply_err(req, 0)

    def rmdir(self, req, parent, name):
        log().debug("rmdir(%s,%s)", parent, name)
        try:
            with self.inodb:
                self.inodb.rmdir_ino(parent, name)
        except:
            self.catch(req, "rmdir(%s,%s)", parent, name)
        else:
            self.reply_err(req, 0)

    def link(self, req, ino, newparent, newname):
        log().debug("link(%s,%s,%s)", ino, newparent, newname)
        try:
            with self.inodb:
                self.inodb.link_ino(ino, newparent, newname)
                attr = self.inodb.getattr_ino(ino)
                entry = dict(
                    ino=ino, attr=attr,
                    attr_timeout=self.attr_timeout,
                    entry_timeout=self.entry_timeout)
        except:
            self.catch(req, "link(%s,%s,%s)", ino, newparent, newname)
        else:
            self.reply_entry(req, entry)

    def readdir(self, req, ino, size, off, fh):
        log().debug("readdir(%s,%s,%s)", ino, off, size)
        try:
            entries = list(self.inodb.readdir_ino(ino))
        except:
            self.catch(req, "readdir(%s,%s,%s)", ino, off, size)
        else:
            self.reply_readdir(req, size, off, entries)

    def symlink(self, req, link, parent, name):
        log().debug("symlink(%s,%s,%s)", link, parent, name)
        try:
            ctx = self.req_ctx(req)
            with self.inodb:
                ino = self.inodb.symlink_ino(
                    parent, name, link, ctx.uid, ctx.gid)
            attr = self.inodb.getattr_ino(ino)
            entry = dict(
                ino=ino, attr=attr,
                attr_timeout=self.attr_timeout,
                entry_timeout=self.entry_timeout)
        except:
            self.catch(req, "symlink(%s,%s,%s)", link, parent, name)
        else:
            self.reply_entry(req, entry)

    def readlink(self, req, ino):
        try:
            link = self.inodb.readlink_ino(ino)
        except:
            self.catch(req, "readlink(%s)", ino)
        else:
            self.reply_readlink(req, link)

    def listxattr(self, req, ino, size):
        log().debug("listxattr(%s)", ino)
        try:
            attr = self.inodb.getattr_ino(ino)
        except:
            self.catch(req, "listxattr(%s)", ino)
        else:
            xattrs = []
            for xattr, key in self.XATTRS.items():
                if attr.get(key) is not None:
                    xattrs.append(xattr)
            self.reply_listxattr(req, xattrs, size)

    def getxattr(self, req, ino, xattr, size):
        log().debug("getxattr(%s,%s)", ino, xattr)
        if xattr in self.XATTRS:
            key = self.XATTRS[xattr]
            try:
                attr = self.inodb.getattr_ino(ino)
            except:
                self.catch(req, "getxattr(%s,%s)", ino, xattr)
            else:
                value = attr.get(key)
                if value is not None:
                    if isinstance(value, str):
                        value = self.encode(value)
                    elif isinstance(value, (int, float)):
                        value = self.encode(str(value))
                    self.reply_xattr(req, value, size)
                else:
                    self.reply_err(req, errno.ENODATA)
        else:
            self.reply_err(req, errno.ENODATA)

    def ino_to_fid(self, ino):
        attr = self.inodb.getattr_ino(ino)
        return (attr["t_hash"], attr["t_index"])

    def open(self, req, ino, fi):
        log().debug("open(%s)", ino)
        try:
            if fi.flags & (os.O_WRONLY | os.O_RDWR):
                raise OSError(errno.EACCES, "file writing not allowed")
            hash, idx = self.ino_to_fid(ino)
            fh = self.backend.open(hash, idx, fi.flags)
        except:
            self.catch(req, "open(%s)", ino)
        else:
            log().debug("open(%s)=%s", ino, fh)
            if self.reply_open(req, dict(fh=fh, keep_cache=1)) != 0:
                self.backend.release(hash, idx, fh)

    def release(self, req, ino, fh):
        log().debug("release(%s(%s))", ino, fh)
        try:
            hash, idx = self.ino_to_fid(ino)
            self.backend.release(hash, idx, fh)
        except:
            self.catch(req, "release(%s(%s))", ino, fh)
        else:
            self.reply_err(req, 0)

    def read(self, req, ino, size, off, fh):
        try:
            hash, idx = self.ino_to_fid(ino)
            buf = self.backend.read(hash, idx, off, size, fh)
        except:
            self.catch(req, "read(%s(%s),%s,%s)", ino, fh, off, size)
        else:
            self.reply_buf(req, buf)
