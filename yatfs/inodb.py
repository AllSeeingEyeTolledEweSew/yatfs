import errno
import logging
import os
import stat
import threading
import time

import apsw


def log():
    return logging.getLogger(__name__)


ROOT_INO = 1


class InoDb(object):

    def __init__(self, path):
        self.path = path

        self._local = threading.local()

    @property
    def db(self):
        db = getattr(self._local, "db", None)
        if db is not None:
            return db
        if not os.path.exists(os.path.dirname(self.path)):
            os.makedirs(os.path.dirname(self.path))
        db = apsw.Connection(self.path)
        db.setbusytimeout(5000)
        self._local.db = db
        self._init()
        return db

    def _init(self):
        with self.db:
            c = self.db.cursor()
            c.execute(
                "create table if not exists attr ("
                "st_ino integer primary key, "
                "st_mode integer not null, "
                "st_nlink integer not null, "
                "st_uid integer not null, "
                "st_gid integer not null, "
                "st_size integer not null, "
                "st_atime integer not null, "
                "st_mtime integer not null, "
                "st_ctime integer not null, "
                "t_hash text, "
                "t_index integer, "
                "link text)")
            c.execute(
                "create index if not exists attr_hash "
                "on attr (t_hash)")
            c.execute(
                "create table if not exists dirent ("
                "d_parent integer not null, "
                "d_ino integer not null, "
                "d_name text not null)")
            c.execute(
                "create index if not exists dirent_parent "
                "on dirent (d_parent)")
            c.execute(
                "create unique index if not exists dirent_parent_name "
                "on dirent (d_parent, d_name)")
            c.execute(
                "create table if not exists global "
                "(name text primary key, value blob) without rowid")
            now = time.time()
            c.execute(
                "insert or ignore into attr "
                "  (st_ino, st_mode, st_nlink, st_uid, st_gid, "
                "   st_size, st_atime, st_mtime, st_ctime) "
                "  values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ROOT_INO, stat.S_IFDIR | 0o555, 2, 0, 0, 0, now, now, now))
            c.execute(
                "insert or ignore into dirent (d_parent, d_ino, d_name) "
                "values (?, ?, ?)",
                (ROOT_INO, ROOT_INO, "."))
            c.execute(
                "insert or ignore into dirent (d_parent, d_ino, d_name) "
                "values (?, ?, ?)",
                (ROOT_INO, ROOT_INO, ".."))
        c.execute("pragma journal_mode=wal")

    def getattr_ino(self, ino):
        c = self.db.cursor()
        c.execute(
            "select * from attr where st_ino = ?", (ino,))
        desc = c.getdescription()
        row = c.fetchone()
        if not row:
            raise OSError(errno.ENOENT, "no attr for %s" % ino)
        return dict(zip((name for name, type in desc), row))

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
        fields = ", ".join([("%s = ?" % field) for field, _ in to_set])
        values = [value for _, value in to_set]
        values.append(ino)
        self.db.cursor().execute(
            "update attr set %s where st_ino = ?" % fields, values)

    def setattr(self, path, **kwargs):
        return self.setattr_ino(self.lookup(path), **kwargs)

    def lookup_ino(self, parent, name):
        if not stat.S_ISDIR(self.getattr_ino(parent)["st_mode"]):
            raise OSError(errno.ENOTDIR, "%s not a directory" % parent)
        row = self.db.cursor().execute(
            "select d_ino from dirent where d_parent = ? and d_name = ?",
            (parent, name)).fetchone()
        if not row:
            raise OSError(
                errno.ENOENT, "no dirent for %s in %s" % (name, parent))
        return row[0]

    def _split(self, path):
        assert path.startswith("/")
        path = path[1:]
        if path.endswith("/"):
            path = path[:-1]
        return path.split("/") if path else []

    def lookup_full(self, path):
        if path == "/":
            yield (None, ROOT_INO)
            return
        split = [""] + self._split(path)
        name_clauses = " ".join(
            "when %(i)d then :n%(i)d" % {"i": i} for i in range(len(split)-1))
        args = {"root": ROOT_INO, "rootmode": stat.S_IFDIR}
        args.update({"n%d" % i: n for i, n in enumerate(split[1:])})
        c = self.db.cursor().execute(
            "with recursive path(name, ino, mode, level) as ("
            "values(null, :root, :rootmode, 0) union "
            "select dirent.d_name, dirent.d_ino, attr.st_mode, path.level + 1 "
            "from path, dirent, attr "
            "where dirent.d_parent = path.ino "
            "and dirent.d_name = case level %(name_clauses)s end "
            "and attr.st_ino = path.ino) "
            "select name, ino, mode from path" %
            {"name_clauses": name_clauses}, args)
        num_rows = 0
        last_ino = None
        for name, ino, mode in c:
            last_ino = ino
            if num_rows < len(split) - 1 and not stat.S_ISDIR(mode):
                raise OSError(errno.ENOTDIR, "%s not a directory" % ino)
            yield (name, ino)
            num_rows += 1
        if num_rows != len(split):
            raise OSError(
                errno.ENOENT, "no dirent for %s in %s" %
                (split[num_rows], last_ino))

    def lookup_dirent(self, path):
        parent = None
        name = None
        ino = None
        for next_name, next_ino in self.lookup_full(path):
            parent = ino
            ino = next_ino
            name = next_name
        return (parent, name, ino)

    def lookup(self, path):
        parent, name, ino = self.lookup_dirent(path)
        return ino

    def readdir_ino(self, ino):
        c = self.db.cursor().execute(
            "select d_name, d_ino from dirent where d_parent = ?", (ino,))
        for name, ino in c:
            yield (name, self.getattr_ino(ino))

    def readdir(self, path):
        for entry in self.readdir_ino(self.lookup(path)):
            yield entry

    def _insert_dirent(self, parent, name, ino):
        row = self.db.cursor().execute(
            "select d_ino from dirent where d_parent = ? and d_name = ?",
            (parent, name)).fetchone()
        if row:
            raise OSError(
                errno.EEXIST, "dirent exists: %s in %s" % (name, parent))
        self.db.cursor().execute(
            "insert into dirent (d_parent, d_ino, d_name) "
            "values (?, ?, ?)", (parent, ino, name))
        self.db.cursor().execute(
            "update attr set st_nlink = st_nlink + 1 where st_ino = ?",
            (parent,))

    def _insert_dirent_of_file(self, parent, name, ino):
        self._insert_dirent(parent, name, ino)
        self.db.cursor().execute(
            "update attr set st_nlink = st_nlink + 1 where st_ino = ?",
            (ino,))

    def _insert_ino(self, parent, name, mode, uid, gid, size,
                    atime, mtime, ctime, hash=None, index=None, link=None):
        row = self.db.cursor().execute(
            "select d_ino from dirent where d_parent = ? and d_name = ?",
            (parent, name)).fetchone()
        if row:
            raise OSError(
                errno.EEXIST, "dirent exists: %s in %s" % (name, parent))
        cur = self.db.cursor()
        cur.execute(
            "insert into attr "
            "  (st_mode, st_nlink, st_uid, st_gid, "
            "   st_size, st_atime, st_mtime, st_ctime, "
            "   t_hash, t_index, link) "
            "  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (mode, 0, uid, gid, size, atime, mtime, ctime, hash, index, link))
        ino = self.db.last_insert_rowid()
        if stat.S_ISDIR(mode):
            self._insert_dirent(parent, name, ino)
        else:
            self._insert_dirent_of_file(parent, name, ino)
        return ino

    def _insert_helper(self, parent, name, mode, size, uid, gid,
            hash=None, index=None, link=None):
        now = time.time()
        ino = self._insert_ino(
            parent, name, mode, uid, gid, size, now, now, now,
            hash=hash, index=index, link=link)
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
        return ino

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
        c = self.db.cursor()
        c.execute(
            "delete from dirent where d_parent = ? and d_name = ?",
            (parent, name))
        c.execute(
            "update attr set st_nlink = st_nlink - 1 where st_ino in (?, ?)",
            (parent, ino))
        c.execute(
            "delete from attr where st_ino = ? and st_nlink = 0", (ino,))

    def unlink(self, path):
        parent, name, _ = self.lookup_dirent(path)
        return self.unlink_ino(parent, name)

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
        c = self.db.cursor()
        c.execute(
            "delete from dirent where d_parent = ? and d_name = ?",
            (parent, name))
        c.execute(
            "update attr set st_nlink = st_nlink - 1 where st_ino = ?",
            (parent,))
        c.execute("delete from dirent where d_parent = ?", (ino,))
        c.execute("delete from attr where st_ino = ?", (ino,))

    def rmdir(self, path):
        parent, name, _ = self.lookup_dirent(path)
        self.rmdir_ino(parent, name)

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
        for parent, nlink in self.db.cursor().execute(
                "select d_parent, count(*) from dirent group by d_parent"):
            self.db.cursor().execute(
                "update attr set st_nlink = ? where st_ino = ?",
                (nlink, parent))
        for ino, nlink in self.db.cursor().execute(
                "select d_ino, count(*) from dirent "
                "inner join attr on st_ino = d_ino "
                "where (st_mode & ?) = 0 "
                "group by d_ino", (stat.S_IFDIR,)):
            self.db.cursor().execute(
                "update attr set st_nlink = ? where st_ino = ?",
                (nlink, ino))
        for ino, in self.db.cursor().execute(
                "select st_ino from attr "
                "left outer join dirent on st_ino = d_ino "
                "where d_ino is null"):
            log().info("orphan ino: %s", ino)
            self.db.cursor().execute(
                "delete from attr where st_ino = ?", (ino,))
        for parent, name in self.db.cursor().execute(
                "select d_parent, d_name from dirent "
                "left outer join attr on d_ino = st_ino "
                "where st_ino is null"):
            log().info("bogus dirent: %s in %s", parent, name)
            self.db.cursor().execute(
                "delete from dirent where d_parent = ? and d_name = ?",
                (parent, name))

    def symlink_ino(self, parent, name, link, uid, gid):
        return self._insert_helper(
            parent, name, stat.S_IFLNK | 0o777, len(link), uid, gid,
            link=link)

    def symlink(self, path, link, uid, gid):
        dirname, name = os.path.split(path)
        return self.symlink_ino(self.lookup(dirname), name, link, uid, gid)

    def readlink_ino(self, ino):
        attr = self.getattr_ino(ino)
        if not stat.S_ISLNK(attr["st_mode"]):
            raise OSError(errno.EINVAL, "%s is not a symlink" % ino)
        return attr["link"]

    def readlink(self, path):
        return self.readlink_ino(self.lookup(path))

    def get_global(self, name):
        row = self.db.cursor().execute(
            "select value from global where name = ?", (name,)).fetchone()
        if row:
            return row[0]

    def set_global(self, name, value):
        self.db.cursor().execute(
            "insert or replace into global (name, value) values (?, ?)",
            (name, value))
