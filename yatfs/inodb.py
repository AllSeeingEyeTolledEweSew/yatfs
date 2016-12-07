import errno
import logging
import os
import stat
import sqlite3
import threading
import time


def log():
    return logging.getLogger(__name__)


ROOT_INO = 1


class InoDb(object):

    def __init__(self, path):
        self.path = path

        self._local = threading.local()

    def __enter__(self):
        if not hasattr(self._local, "context_level"):
            self._local.context_level = 0
        self._local.context_level += 1
        if self._local.context_level == 1:
            self._local.context = self.db.__enter__()
        return self._local.context

    def __exit__(self, exc_type, exc_value, traceback):
        assert self._local.context_level > 0
        self._local.context_level -= 1
        if self._local.context_level == 0:
            self.db.__exit__(exc_type, exc_value, traceback)

    @property
    def db(self):
        db = getattr(self._local, "db", None)
        if db is not None:
            return db
        if not os.path.exists(os.path.dirname(self.path)):
            os.makedirs(os.path.dirname(self.path))
        db = sqlite3.connect(self.path, isolation_level="IMMEDIATE")
        self._local.db = db
        db.row_factory = sqlite3.Row
        db.text_factory = os.fsdecode
        with db:
            self._init()
        return db

    def _init(self):
        self.db.execute(
            "create table if not exists attr ("
            "  st_ino integer primary key,"
            "  st_mode integer not null,"
            "  st_nlink integer not null,"
            "  st_uid integer not null,"
            "  st_gid integer not null,"
            "  st_size integer not null,"
            "  st_atime integer not null,"
            "  st_mtime integer not null,"
            "  st_ctime integer not null,"
            "  t_hash text,"
            "  t_index integer)")
        self.db.execute(
            "create index if not exists attr_hash "
            "on attr (t_hash)")
        self.db.execute(
            "create table if not exists dirent ("
            "  d_parent integer not null,"
            "  d_ino integer not null,"
            "  d_name text not null)")
        self.db.execute(
            "create index if not exists dirent_parent "
            "on dirent (d_parent)")
        self.db.execute(
            "create unique index if not exists dirent_parent_name "
            "on dirent (d_parent, d_name)")
        now = time.time()
        self.db.execute(
            "insert or ignore into attr "
            "  (st_ino, st_mode, st_nlink, st_uid, st_gid, "
            "   st_size, st_atime, st_mtime, st_ctime) "
            "  values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ROOT_INO, stat.S_IFDIR | 0o555, 2, 0, 0, 0, now, now, now))
        self.db.execute(
            "insert or ignore into dirent (d_parent, d_ino, d_name) "
            "values (?, ?, ?)",
            (ROOT_INO, ROOT_INO, "."))
        self.db.execute(
            "insert or ignore into dirent (d_parent, d_ino, d_name) "
            "values (?, ?, ?)",
            (ROOT_INO, ROOT_INO, ".."))

    def getattr_ino(self, ino):
        row = self.db.execute(
            "select * from attr where st_ino = ?", (ino,)).fetchone()
        if not row:
            raise OSError(errno.ENOENT, "no attr for %s" % ino)
        return dict(row)

    def getattr(self, path):
        return self.getattr_ino(self.lookup(path))

    def setattr_ino(self, ino, st_mode=None, st_uid=None, st_gid=None,
                    st_size=None, st_atime=None, st_mtime=None, st_ctime=None,
                    t_hash=None, t_index=None):
        attr = self.getattr_ino(ino)
        to_set = []
        if st_mode is not None:
            to_set.append(("st_mode", st_mode & stat.S_IMODE))
        if st_uid is not None:
            to_set.append(("st_uid", st_uid))
        if st_gid is not None:
            to_set.append(("st_gid", st_gid))
        if st_size is not None:
            to_set.append(("st_size", st_size))
        if st_atime is not None:
            to_set.append(("st_atime", st_atime))
        if st_mtime is not None:
            to_set.append(("st_mtime", st_mtime))
        if st_ctime is not None:
            to_set.append(("st_ctime", st_ctime))
        if t_hash is not None or t_index is not None:
            if attr["st_mode"] & stat.S_IFDIR:
                raise OSError(errno.EINVAL, "t_hash/t_index invalid for dir")
            if t_hash is not None:
                to_set.append(("t_hash", t_hash))
            if t_index is not None:
                to_set.append(("t_index", t_index))
        if not to_set:
            return
        fields = [("%s = ?" % field) for field, _ in to_set]
        values = [value for _, value in to_set]
        values.append(ino)
        self.db.execute(
            "update attr set %s where st_ino = ?" % fields, values)

    def setattr(self, path, **kwargs):
        return self.setattr_ino(self.lookup(path), **kwargs)

    def lookup_ino(self, parent, name):
        if not stat.S_ISDIR(self.getattr_ino(parent)["st_mode"]):
            raise OSError(errno.ENOTDIR, "%s not a directory" % parent)
        row = self.db.execute(
            "select d_ino from dirent where d_parent = ? and d_name = ?",
            (parent, name)).fetchone()
        if not row:
            raise OSError(
                errno.ENOENT, "no dirent for %s in %s" % (name, parent))
        return row["d_ino"]

    def _split(self, path):
        assert path.startswith("/")
        path = path[1:]
        if path.endswith("/"):
            path = path[:-1]
        return path.split("/") if path else ()

    def lookup(self, path):
        parent = None
        ino = ROOT_INO
        for name in self._split(path):
            parent = ino
            ino = self.lookup_ino(parent, name)
        return ino

    def readdir_ino(self, ino):
        rows = self.db.execute(
            "select d_name, d_ino from dirent where d_parent = ?", (ino,))
        for row in rows:
            yield (row["d_name"], self.getattr_ino(row["d_ino"]))

    def readdir(self, path):
        for entry in self.readdir_ino(self.lookup(path)):
            yield entry

    def _insert_dirent(self, parent, name, ino):
        row = self.db.execute(
            "select d_ino from dirent where d_parent = ? and d_name = ?",
            (parent, name)).fetchone()
        if row:
            raise OSError(
                errno.EEXIST, "dirent exists: %s in %s" % (name, parent))
        self.db.execute(
            "insert into dirent (d_parent, d_ino, d_name) "
            "values (?, ?, ?)", (parent, ino, name))
        self.db.execute(
            "update attr set st_nlink = st_nlink + 1 where st_ino = ?",
            (parent,))

    def _insert_dirent_of_file(self, parent, name, ino):
        self._insert_dirent(parent, name, ino)
        self.db.execute(
            "update attr set st_nlink = st_nlink + 1 where st_ino = ?",
            (ino,))

    def _insert_ino(self, parent, name, mode, uid, gid, size,
                   atime, mtime, ctime, hash=None, index=None):
        cur = self.db.cursor()
        cur.execute(
            "insert into attr "
            "  (st_mode, st_nlink, st_uid, st_gid, "
            "   st_size, st_atime, st_mtime, st_ctime, "
            "   t_hash, t_index) "
            "  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (mode, 0, uid, gid, size, atime, mtime, ctime, hash, index))
        ino = cur.lastrowid
        if stat.S_ISDIR(mode):
            self._insert_dirent(parent, name, ino)
        else:
            self._insert_dirent_of_file(parent, name, ino)
        return ino

    def _insert_helper(self, parent, name, mode, size, uid, gid,
            hash=None, index=None):
        now = time.time()
        ino = self._insert_ino(
            parent, name, mode, uid, gid, size, now, now, now,
            hash=hash, index=index)
        return ino

    def mkdir_ino(self, parent, name, mode, uid, gid):
        ino = self._insert_helper(
            parent, name, stat.S_IFDIR | mode, 0, uid, gid)
        self._insert_dirent(ino, ".", ino)
        self._insert_dirent(ino, "..", parent)
        return ino

    def mkdir(self, path, mode, uid, gid):
        dirname, name = os.path.split(name)
        return self.mkdir_ino(self.lookup(dirname), name, mode, uid, gid)

    def mkdir_p(self, path, mode, uid, gid):
        parent = ROOT_INO
        ino = parent
        for name in self._split(path):
            parent = ino
            try:
                ino = self.lookup_ino(parent, name)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    ino = self.mkdir_ino(parent, name, mode, uid, gid)
                else:
                    raise

    def mkfile_ino(self, parent, name, mode, hash, index, size,
                  uid, gid):
        return self._insert_helper(
            parent, name, stat.S_IFREG | mode, size, uid, gid,
            hash=hash, index=index)

    def mkfile(self, path, mode, hash, index, size, uid, gid):
        dirname, name = os.path.split(path)
        return self.mkfile_ino(self.lookup(dirname), name, mode, hash, index,
            size, uid, gid)

    def unlink_ino(self, parent, name):
        ino = self.lookup_ino(parent, name)
        if stat.S_ISDIR(self.getattr_ino(ino)["st_mode"]):
            raise OSError(errno.EISDIR, "%s in %s is dir" % (name, parent))
        self.db.execute(
            "delete from dirent where d_parent = ? and d_name = ?",
            (parent, name))
        self.db.execute(
            "update attr set st_nlink = st_nlink - 1 where st_ino in (?, ?)",
            (parent, ino))
        self.db.execute(
            "delete from attr where st_ino = ? and st_nlink = 0", (ino,))

    def unlink(self, path):
        dirname, name = os.path.split(path)
        return self.unlink_ino(self.lookup(dirname), name)

    def rmdir_ino(self, parent, name):
        ino = self.lookup_ino(parent, name)
        attr = self.getattr_ino(ino)
        if not stat.S_ISDIR(attr["st_mode"]):
            raise OSError(errno.ENOTDIR, "%s in %s not dir" % (name, parent))
        if attr["st_nlink"] < 2:
            raise OSError(errno.EIO, "%s in %s: st_nlink = %s?" %
                    (name, parent, attr["st_nlink"]))
        if attr["st_nlink"] > 2:
            raise OSError(errno.ENOTEMPTY, "%s in %s not empty" %
                    (name, parent))
        self.db.execute(
            "delete from dirent where d_parent = ? and d_name = ?",
            (parent, name))
        self.db.execute(
            "delete from dirent where d_parent = ?", (ino,))
        self.db.execute(
            "delete from attr where st_ino = ?", (ino,))

    def rmdir(self, path):
        dirname, name = os.path.split(path)
        self.rmdir_ino(self.lookup(dirname), name)

    def link_ino(self, ino, new_parent, new_name):
        attr = self.getattr_ino(ino)
        if stat.S_ISDIR(attr["st_mode"]):
            raise OSError(errno.EPERM, "%s is dir" % ino)
        self._insert_dirent_of_file(new_parent, new_name, ino)

    def link(self, old_path, new_path):
        new_dirname, new_name = os.path.split(new_path)
        self.link_ino(
            self.lookup(old_path), self.lookup(new_dirname), new_name)

    def fsck(self):
        for row in self.db.execute(
                "select d_parent, count(*) as nlink from dirent "
                "group by d_parent"):
            self.db.execute(
                "update attr set st_nlink = ? where st_ino = ?",
                (row["nlink"], row["d_parent"]))
        for row in self.db.execute(
                "select d_ino, count(*) as nlink from dirent "
                "inner join attr on st_ino = d_ino "
                "where (st_mode & ?) = 0 "
                "group by d_ino", (stat.S_IFDIR,)):
            self.db.execute(
                "update attr set st_nlink = ? where st_ino = ?",
                (row["nlink"], row["d_ino"]))
        for row in self.db.execute(
                "select st_ino from attr "
                "left outer join dirent on st_ino = d_ino "
                "where d_ino is null"):
            ino = row["st_ino"]
            log().info("orphan ino: %s", ino)
            self.db.execute("delete from attr where st_ino = ?", (ino,))
        for row in self.db.execute(
                "select d_parent, d_name from dirent "
                "left outer join attr on d_ino = st_ino "
                "where st_ino is null"):
            parent, name = row["d_parent"], row["d_name"]
            log().info("bogus dirent: %s in %s", parent, name)
            self.db.execute(
                "delete from dirent where d_parent = ? and d_name = ?",
                (parent, name))
