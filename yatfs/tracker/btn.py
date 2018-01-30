import errno
import logging
import stat
import threading
import time

btn_lib = __import__("btn")
import llfuse

from yatfs import fs as yatfs_fs


def log():
    return logging.getLogger(__name__)


def slash_variations(name):
    idxs = []
    for i, c in enumerate(name):
        if c == "_":
            idxs.append(i)
    for x in range(2 ** len(idxs)):
        v = name
        for i, idx in enumerate(idxs):
            if x & (1 << i):
                v = v[:idx] + "/" + v[idx + 1:]
        yield v


def apply_attr(entry, api, torrent_entry_id):
    c = api.db.cursor()
    r = c.execute(
        "select time from torrent_entry where id = ?",
        (torrent_entry_id,)).fetchone()
    if not r:
        raise llfuse.FUSEEntry(errno.ENOENT)
    t = r[0]
    t_ns = t * (10 ** 9)
    entry.st_mtime_ns = t_ns
    entry.st_ctime_ns = t_ns


class Tracker(yatfs_fs.StaticDir):

    def __init__(self, backend, api):
        super(Tracker, self).__init__()
        self.mkdentry(b"series", SeriesContainer(api))
        self.mkdentry(b"group", GroupContainer(api))
        self.mkdentry(b"torrent", TorrentContainer(backend, api))
        self.mkdentry(b"browse", BrowseContainer(api))


class BrowseContainer(yatfs_fs.Dir):

    def __init__(self, api):
        super(BrowseContainer, self).__init__()
        self.api = api

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select id, name from series where id > ? and not deleted "
            "order by id", (offset,))
        for id, name in c:
            if not name:
                continue
            name = name.replace("/", "_")
            yield (name.encode(), stat.S_IFDIR, id)

    def lookup_create(self, name):
        try:
            name = name.decode()
        except UnicodeDecodeError:
            raise llfuse.FUSEError(errno.ENOENT)
        for name in slash_variations(name):
            c = self.api.db.cursor()
            r = c.execute(
                "select id from series where name = ? and not deleted",
                (name,)).fetchone()
            if not r:
                continue
            id = r[0]
            return SeriesBrowseContainer(self.api, id)
        else:
            raise llfuse.FUSEError(errno.ENOENT)


class SeriesContainer(yatfs_fs.StaticDir):

    def __init__(self, api):
        super(SeriesContainer, self).__init__()
        self.mkdentry(b"by-id", SeriesByIdContainer(api))
        self.mkdentry(b"by-name", SeriesByNameContainer(api))
        self.mkdentry(b"by-tvdb-id", SeriesByTvdbIdContainer(api))
        self.mkdentry(b"by-imdb-id", SeriesByImdbIdContainer(api))


class SeriesByIdContainer(yatfs_fs.Dir):

    def __init__(self, api):
        super(SeriesByIdContainer, self).__init__()
        self.api = api

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select id from series where id > ? and not deleted order by id",
            (offset,))
        for id, in c:
            yield (str(id).encode(), stat.S_IFDIR, id)

    def lookup_create(self, name):
        try:
            id = int(name)
        except ValueError:
            raise llfuse.FUSEError(errno.ENOENT)
        c = self.api.db.cursor()
        r = c.execute(
            "select id from series where id = ? and not deleted",
            (id,)).fetchone()
        if not r:
            raise llfuse.FUSEError(errno.ENOENT)
        return Series(self.api, id)


class SeriesByNameContainer(yatfs_fs.Dir):

    def __init__(self, api):
        super(SeriesByNameContainer, self).__init__()
        self.api = api

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select id, name from series where id > ? and not deleted "
            "order by id", (offset,))
        for id, name in c:
            if not name:
                continue
            name = name.replace("/", "_")
            yield (name.encode(), stat.S_IFLNK, id)

    def lookup_create(self, name):
        try:
            name = name.decode()
        except UnicodeDecodeError:
            raise llfuse.FUSEError(errno.ENOENT)
        for name in slash_variations(name):
            c = self.api.db.cursor()
            r = c.execute(
                "select id from series where name = ? and not deleted",
                (name,)).fetchone()
            if not r:
                continue
            id = r[0]
            link = yatfs_fs.StaticSymlink(("../by-id/%d" % id).encode())
            link.entry_timeout = 3600
            link.attr_timeout = 86400
            return link
        else:
            raise llfuse.FUSEError(errno.ENOENT)


class SeriesByTvdbIdContainer(yatfs_fs.Dir):

    def __init__(self, api):
        super(SeriesByTvdbIdContainer, self).__init__()
        self.api = api

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select tvdb_id from series where tvdb_id > ? and not deleted "
            "order by tvdb_id", (offset,))
        for tvdb_id, in c:
            if not tvdb_id:
                continue
            yield (str(tvdb_id).encode(), stat.S_IFLNK, tvdb_id)

    def lookup_create(self, name):
        try:
            tvdb_id = int(name)
        except ValueError:
            raise fusell.FUSEError(errno.ENOENT)
        c = self.api.db.cursor()
        r = c.execute(
            "select id from series where tvdb_id = ? and not deleted",
            (tvdb_id,)).fetchone()
        if not r:
            raise llfuse.FUSEError(errno.ENOENT)
        id = r[0]
        link = yatfs_fs.StaticSymlink(("../by-id/%d" % id).encode())
        link.entry_timeout = 3600
        link.attr_timeout = 86400
        return link


class SeriesByImdbIdContainer(yatfs_fs.Dir):

    def __init__(self, api):
        super(SeriesByImdbIdContainer, self).__init__()
        self.api = api

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select id, imdb_id from series where id > ? and not deleted "
            "order by id", (offset,))
        for id, imdb_id in c:
            if not imdb_id:
                continue
            yield (imdb_id.encode(), stat.S_IFLNK, id)

    def lookup_create(self, name):
        try:
            imdb_id = name.decode()
        except UnicodeDecodeError:
            raise fusell.FUSEError(errno.ENOENT)
        c = self.api.db.cursor()
        r = c.execute(
            "select id from series where imdb_id = ? and not deleted",
            (imdb_id,)).fetchone()
        if not r:
            raise llfuse.FUSEError(errno.ENOENT)
        id = r[0]
        link = yatfs_fs.StaticSymlink(("../by-id/%d" % id).encode())
        link.entry_timeout = 3600
        link.attr_timeout = 86400
        return link


class Series(yatfs_fs.StaticDir):

    def __init__(self, api, id):
        super(Series, self).__init__()
        self.mkdentry(b"group", SeriesGroupByNameContainer(api, id))
        self.mkdentry(b"name", SeriesName(api, id))
        self.mkdentry(b"imdb_id", SeriesImdbId(api, id))
        self.mkdentry(b"tvdb_id", SeriesTvdbId(api, id))
        self.mkdentry(b"banner", SeriesBanner(api, id))
        self.mkdentry(b"poster", SeriesPoster(api, id))
        self.mkdentry(b"youtube_trailer", SeriesYoutubeTrailer(api, id))


class SeriesMetadata(yatfs_fs.Data):

    def __init__(self, api, id):
        super(SeriesMetadata, self).__init__()
        self.api = api
        self.id = id

    def getattr(self):
        e = super(SeriesMetadata, self).getattr()
        e.st_mtime_ns = time.time() * (10 ** 9)
        return e


class SeriesName(SeriesMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select name from series where id = ?", (self.id,)).fetchone()
        return r[0].encode() if r and r[0] else b""


class SeriesTvdbId(SeriesMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select tvdb_id from series where id = ?", (self.id,)).fetchone()
        return str(r[0]).encode() if r and r[0] else b""


class SeriesImdbId(SeriesMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select imdb_id from series where id = ?", (self.id,)).fetchone()
        return r[0].encode() if r and r[0] else b""


class SeriesBanner(SeriesMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select banner from series where id = ?", (self.id,)).fetchone()
        return r[0].encode() if r and r[0] else b""


class SeriesPoster(SeriesMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select poster from series where id = ?", (self.id,)).fetchone()
        return r[0].encode() if r and r[0] else b""


class SeriesYoutubeTrailer(SeriesMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select youtube_trailer from series where id = ?",
            (self.id,)).fetchone()
        return r[0].encode() if r and r[0] else b""


class SeriesGroupByNameContainer(yatfs_fs.Dir):

    def __init__(self, api, id):
        super(SeriesGroupByNameContainer, self).__init__()
        self.api = api
        self.id = id

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select id, name from torrent_entry_group "
            "where id > ? and series_id = ? and not deleted order by id",
            (offset, self.id))
        for id, name in c:
            if not name:
                continue
            name = name.replace("/", "_")
            yield (name.encode(), stat.S_IFLNK, id)

    def lookup_create(self, name):
        try:
            name = name.decode()
        except UnicodeDecodeError:
            raise llfuse.FUSEError(errno.ENOENT)
        for name in slash_variations(name):
            c = self.api.db.cursor()
            r = c.execute(
                "select id from torrent_entry_group "
                "where name = ? and series_id = ? and not deleted",
                (name, self.id)).fetchone()
            if not r:
                continue
            id = r[0]
            link = yatfs_fs.StaticSymlink(
                ("../../../../group/by-id/%d" % id).encode())
            link.entry_timeout = 3600
            link.attr_timeout = 86400
            return link
        else:
            raise llfuse.FUSEError(errno.ENOENT)


class SeriesBrowseContainer(yatfs_fs.Dir):

    def __init__(self, api, id):
        super(SeriesBrowseContainer, self).__init__()
        self.api = api
        self.id = id

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select id, name from torrent_entry_group "
            "where id > ? and series_id = ? and not deleted order by id",
            (offset, self.id))
        for id, name in c:
            if not name:
                continue
            name = name.replace("/", "_")
            yield (name.encode(), stat.S_IFLNK, id)

    def lookup_create(self, name):
        try:
            name = name.decode()
        except UnicodeDecodeError:
            raise llfuse.FUSEError(errno.ENOENT)
        for name in slash_variations(name):
            c = self.api.db.cursor()
            r = c.execute(
                "select id from torrent_entry_group "
                "where name = ? and series_id = ? and not deleted",
                (name, self.id)).fetchone()
            if not r:
                continue
            id = r[0]
            return GroupBrowseSubdir(self.api, id, b"")
        else:
            raise llfuse.FUSEError(errno.ENOENT)


class GroupContainer(yatfs_fs.StaticDir):

    def __init__(self, api):
        super(GroupContainer, self).__init__()
        self.mkdentry(b"by-id", GroupByIdContainer(api))


class GroupByIdContainer(yatfs_fs.Dir):

    def __init__(self, api):
        super(GroupByIdContainer, self).__init__()
        self.api = api

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select id from torrent_entry_group "
            "where id > ? and not deleted order by id", (offset,))
        for id, in c:
            yield (str(id).encode(), stat.S_IFDIR, id)

    def lookup_create(self, name):
        try:
            id = int(name)
        except ValueError:
            raise llfuse.FUSEError(errno.ENOENT)
        c = self.api.db.cursor()
        r = c.execute(
            "select id, series_id from torrent_entry_group "
            "where id = ? and not deleted", (id,)).fetchone()
        if not r:
            raise llfuse.FUSEError(errno.ENOENT)
        id, series_id = r
        return Group(self.api, id, series_id)


class Group(yatfs_fs.StaticDir):

    def __init__(self, api, id, series_id):
        super(Group, self).__init__()
        self.mkdentry(b"torrent", GroupTorrentByKey(api, id))
        self.mkdentry(b"series", yatfs_fs.StaticSymlink(
            ("../../../series/by-id/%s" % series_id).encode()))
        self.mkdentry(b"category", GroupCategory(api, id))
        self.mkdentry(b"name", GroupName(api, id))


class GroupMetadata(yatfs_fs.Data):

    def __init__(self, api, id):
        super(GroupMetadata, self).__init__()
        self.api = api
        self.id = id


class GroupCategory(GroupMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select category.name from torrent_entry_group inner join "
            "category on torrent_entry_group.category_id = category.id "
            "where torrent_entry_group.id = ?", (self.id,)).fetchone()
        return r[0].encode() if r and r[0] else b""


class GroupName(GroupMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select name from torrent_entry_group where id = ?",
            (self.id,)).fetchone()
        return r[0].encode() if r and r[0] else b""


class GroupTorrentByKey(yatfs_fs.Dir):

    def __init__(self, api, id):
        super(GroupTorrentByKey, self).__init__()
        self.api = api
        self.id = id

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select torrent_entry.id, container.name, codec.name, "
            "source.name, resolution.name, origin.name from torrent_entry "
            "left outer join container "
            "on container.id = torrent_entry.container_id "
            "left outer join codec "
            "on codec.id = torrent_entry.codec_id "
            "left outer join source "
            "on source.id = torrent_entry.source_id "
            "left outer join resolution "
            "on resolution.id = torrent_entry.resolution_id "
            "left outer join origin "
            "on origin.id = torrent_entry.origin_id "
            "where torrent_entry.id > ? and torrent_entry.group_id = ? "
            "and not deleted order by torrent_entry.id",
            (offset, self.id))
        for id, con, codec, src, res, org in c:
            con = con.replace(".", "")
            codec = codec.replace(".", "")
            src = src.replace(".", "")
            res = res.replace(".", "")
            org = org.replace(".", "")
            name = "%s.%s.%s.%s.%s.%s" % (
                con, codec, src, res, org, id)
            yield (name.encode(), stat.S_IFLNK, id)

    def lookup_create(self, name):
        try:
            name = name.decode()
        except UnicodeDecodeError:
            raise llfuse.FUSEError(errno.ENOENT)
        id = name.split(".")[-1]
        try:
            id = int(id)
        except ValueError:
            raise llfuse.FUSEError(errno.ENOENT)
        c = self.api.db.cursor()
        r = c.execute(
            "select group_id from torrent_entry where id = ?",
            (id,)).fetchone()
        if (not r) or r[0] != self.id:
            raise llfuse.FUSEError(errno.ENOENT)
        link = yatfs_fs.StaticSymlink(
            ("../../../../torrent/by-id/%d" % id).encode())
        link.entry_timeout = 3600
        link.attr_timeout = 86400
        apply_attr(link, self.api, id)
        return link


class TorrentContainer(yatfs_fs.StaticDir):

    def __init__(self, backend, api):
        super(TorrentContainer, self).__init__()
        self.mkdentry(b"by-id", TorrentByIdContainer(backend, api))
        self.mkdentry(b"by-infohash", TorrentByInfohashContainer(api))


class TorrentByIdContainer(yatfs_fs.Dir):

    def __init__(self, backend, api):
        super(TorrentByIdContainer, self).__init__()
        self.backend = backend
        self.api = api

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select id from torrent_entry where id > ? and not deleted "
            "order by id", (offset,))
        for id, in c:
            yield (str(id).encode(), stat.S_IFDIR, id)

    def lookup_create(self, name):
        try:
            id = int(name)
        except ValueError:
            raise llfuse.FUSEError(errno.ENOENT)
        c = self.api.db.cursor()
        r = c.execute(
            "select group_id from torrent_entry where id = ? "
            "and not deleted", (id,)).fetchone()
        if not r:
            raise llfuse.FUSEError(errno.ENOENT)
        group_id, = r
        return Torrent(self.backend, self.api, id, group_id)


class TorrentByInfohashContainer(yatfs_fs.Dir):

    def __init__(self, api):
        super(TorrentByInfohashContainer, self).__init__()
        self.api = api

    def readdir(self, offset):
        if offset == 0:
            offset = -1
        c = self.api.db.cursor()
        c.execute(
            "select id, info_hash from torrent_entry where id > ? "
            "and not deleted order by id", (offset,))
        for id, info_hash in c:
            yield (info_hash.lower().encode(), stat.S_IFLNK, id)

    def lookup_create(self, name):
        try:
            info_hash = name.decode()
        except UnicodeDecodeError:
            raise llfuse.FUSEError(errno.ENOENT)
        c = self.api.db.cursor()
        r = c.execute(
            "select id from torrent_entry where info_hash = ? "
            "and not deleted", (info_hash.upper(),)).fetchone()
        if not r:
            raise llfuse.FUSEError(errno.ENOENT)
        id, = r
        link = yatfs_fs.StaticSymlink(("../by-id/%s" % id).encode())
        apply_attr(link, self.api, id)
        return link


class Torrent(yatfs_fs.StaticDir):

    def __init__(self, backend, api, id, group_id):
        super(Torrent, self).__init__()
        self.api = api
        self.id = id
        self.mkdentry(b"data", TorrentDataContainer(backend, api, id))
        group_link = yatfs_fs.StaticSymlink(
            ("../../../group/by-id/%s" % group_id).encode())
        apply_attr(group_link, api, id)
        self.mkdentry(b"group", group_link)
        self.mkdentry(b"seeders", TorrentSeeders(api, id))
        self.mkdentry(b"leechers", TorrentLeechers(api, id))
        self.mkdentry(b"time", TorrentTime(api, id))
        self.mkdentry(b"release_name", TorrentReleaseName(api, id))

    def getattr(self):
        e = super(Torrent, self).getattr()
        apply_attr(e, self.api, self.id)
        return e


class TorrentMetadata(yatfs_fs.Data):

    def __init__(self, api, id):
        super(TorrentMetadata, self).__init__()
        self.api = api
        self.id = id

    def getattr(self):
        e = super(TorrentMetadata, self).getattr()
        e.st_mtime_ns = time.time() * (10 ** 9)
        return e


class TorrentStaticMetadata(TorrentMetadata):

    def getattr(self):
        e = super(TorrentStaticMetadata, self).getattr()
        apply_attr(e, self.api, self.id)
        return e


class TorrentSeeders(TorrentMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select seeders from tracker_stats where id = ?",
            (self.id,)).fetchone()
        return str(r[0]).encode() if r else b""


class TorrentLeechers(TorrentMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select leechers from tracker_stats where id = ?",
            (self.id,)).fetchone()
        return str(r[0]).encode() if r else b""


class TorrentTime(TorrentStaticMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select time from torrent_entry where id = ?",
            (self.id,)).fetchone()
        return str(r[0]).encode() if r else b""


class TorrentReleaseName(TorrentMetadata):

    def data(self):
        r = self.api.db.cursor().execute(
            "select release_name from torrent_entry where id = ?",
            (self.id,)).fetchone()
        return r[0].encode() if r and r[0] else b""


class TorrentDataContainer(yatfs_fs.StaticDir):

    def __init__(self, backend, api, id):
        super(TorrentDataContainer, self).__init__()
        self.mkdentry(b"by-path", TorrentSubdir(backend, api, id, b""))
        self.mkdentry(b"by-index", TorrentDataByIndex(backend, api, id))
        self.api = api
        self.id = id

    def getattr(self):
        e = super(TorrentDataContainer, self).getattr()
        apply_attr(e, self.api, self.id)
        return e


class TorrentDataByIndex(yatfs_fs.Dir):

    def __init__(self, backend, api, id):
        super(TorrentDataByIndex, self).__init__()
        self.backend = backend
        self.api = api
        self.id = id

    def getattr(self):
        e = super(TorrentDataByIndex, self).getattr()
        apply_attr(e, self.api, self.id)
        return e

    def readdir(self, offset):
        c = self.api.db.cursor()
        c.execute(
            "select file_index from file_info where id = ? and "
            "file_index >= ? order by file_index", (self.id, offset,))
        for file_index, in c:
            yield (str(file_index).encode(), stat.S_IFREG, file_index + 1)

    def lookup_create(self, name):
        try:
            file_index = int(name)
        except ValueError:
            raise llfuse.FUSEError(errno.ENOENT)
        c = self.api.db.cursor()
        r = c.execute(
            "select file_index from file_info where id = ? and file_index = ?",
            (self.id, file_index)).fetchone()
        if not r:
            raise llfuse.FUSEError(errno.ENOENT)
        return TorrentFile(self.backend, self.api, self.id, file_index)


class TorrentSubdir(yatfs_fs.Dir):

    def __init__(self, backend, api, id, prefix):
        super(TorrentSubdir, self).__init__()
        self.backend = backend
        self.api = api
        self.id = id
        self.prefix = prefix

    def getattr(self):
        e = super(TorrentSubdir, self).getattr()
        apply_attr(e, self.api, self.id)
        return e

    def readdir(self, offset):
        c = self.api.db.cursor()
        if not self.prefix:
            strip = 0
            c.execute(
                "select path from file_info where id = ? "
                "order by path limit -1 offset ?", (self.id, offset))
        else:
            strip = len(self.prefix) + 1
            lo = self.prefix + b"/"
            hi = self.prefix + b"0"
            c.execute(
                "select path from file_info where id = ? "
                "and path > ? and path < ? order by path "
                "limit -1 offset ?", (self.id, lo, hi, offset))
        prev_name, prev_type = (None, None)
        index = 0
        for index, (path,) in enumerate(c):
            tail = path[strip:].split(b"/")
            name = tail[0]
            if name != prev_name and prev_name is not None:
                yield (prev_name, prev_type, index + offset)
            if len(tail) == 1:
                type = stat.S_IFREG
            else:
                type = stat.S_IFDIR
            prev_name = name
            prev_type = type
        if prev_name is not None:
            yield (prev_name, prev_type, index + offset + 1)

    def lookup_create(self, name):
        if self.prefix:
            path = self.prefix + b"/" + name
        else:
            path = name
        c = self.api.db.cursor()
        r = c.execute(
            "select file_index from file_info where id = ? "
            "and path = ?", (self.id, path,)).fetchone()
        if r:
            file_index, = r
            return TorrentFile(self.backend, self.api, self.id, file_index)
        lo = path + b"/"
        hi = path + b"0"
        c = self.api.db.cursor()
        r = c.execute(
            "select count(*) from file_info where id = ? "
            "and path > ? and path < ?", (self.id, lo, hi)).fetchone()
        count, = r
        if count:
            return TorrentSubdir(self.backend, self.api, self.id, path)
        raise llfuse.FUSEError(errno.ENOENT)


class TorrentFile(yatfs_fs.TorrentFile):

    def __init__(self, backend, api, id, file_index):
        super(TorrentFile, self).__init__(backend)
        self.api = api
        self.id = id
        self._file_index = file_index

        self.__lock = threading.RLock()
        self._info_hash = None

    def file_index(self):
        return self._file_index

    def info_hash(self):
        with self.__lock:
            if self._info_hash is not None:
                return self._info_hash
            c = self.api.db.cursor()
            r = c.execute(
                "select info_hash from torrent_entry where id = ?",
                (self.id,)).fetchone()
            if not r:
                raise llfuse.FUSEError(errno.ENOENT)
            self._info_hash = r[0].lower().encode()
            return self._info_hash

    def raw_torrent(self):
        return self.api.getTorrentByIdCached(self.id).raw_torrent

    def getattr(self):
        e = super(TorrentFile, self).getattr()
        c = self.api.db.cursor()
        r = c.execute(
            "select length from file_info where id = ? and file_index = ?",
            (self.id, self.file_index())).fetchone()
        if not r:
            raise llfuse.FUSEError(errno.ENOENT)
        e.st_size = r[0]
        apply_attr(e, self.api, self.id)
        return e


class GroupBrowseSubdir(yatfs_fs.Dir):

    def __init__(self, api, id, prefix):
        super(GroupBrowseSubdir, self).__init__()
        self.api = api
        self.id = id
        self.prefix = prefix

    def readdir(self, offset):
        c = self.api.db.cursor()
        if not self.prefix:
            strip = 0
            c.execute(
                "select file_info.path from file_info "
                "inner join torrent_entry "
                "where file_info.id = torrent_entry.id and "
                "not torrent_entry.deleted and "
                "torrent_entry.group_id = ? "
                "order by file_info.path limit -1 offset ?", (self.id, offset))
        else:
            strip = len(self.prefix) + 1
            lo = self.prefix + b"/"
            hi = self.prefix + b"0"
            c.execute(
                "select file_info.path from file_info "
                "inner join torrent_entry "
                "where file_info.id = torrent_entry.id and "
                "not torrent_entry.deleted and "
                "torrent_entry.group_id = ? "
                "and file_info.path > ? and file_info.path < ? "
                "order by file_info.path "
                "limit -1 offset ?", (self.id, lo, hi, offset))
        prev_name, prev_type = (None, None)
        index = 0
        for index, (path,) in enumerate(c):
            tail = path[strip:].split(b"/")
            name = tail[0]
            if name != prev_name and prev_name is not None:
                yield (prev_name, prev_type, index + offset)
            if len(tail) == 1:
                type = stat.S_IFREG
            else:
                type = stat.S_IFDIR
            prev_name = name
            prev_type = type
        if prev_name is not None:
            yield (prev_name, prev_type, index + offset + 1)

    def lookup_create(self, name):
        if self.prefix:
            path = self.prefix + b"/" + name
        else:
            path = name
        c = self.api.db.cursor()
        r = c.execute(
            "select file_info.id, file_info.file_index from file_info "
            "inner join torrent_entry "
            "where file_info.id = torrent_entry.id and "
            "not torrent_entry.deleted and "
            "torrent_entry.group_id = ? "
            "and file_info.path = ?", (self.id, path,)).fetchone()
        if r:
            torrent_entry_id, file_index, = r
            count = path.count(b"/")
            path = (
                "".join(["../"] * count) +
                "../../../torrent/by-id/%s/data/by-index/%s" % (
                    torrent_entry_id, file_index))
            link = yatfs_fs.StaticSymlink(path.encode())
            apply_attr(link, self.api, torrent_entry_id)
            return link
        lo = path + b"/"
        hi = path + b"0"
        c = self.api.db.cursor()
        r = c.execute(
            "select count(*) from file_info inner join torrent_entry "
            "where file_info.id = torrent_entry.id and "
            "not torrent_entry.deleted and "
            "torrent_entry.group_id = ? "
            "and file_info.path > ? and file_info.path < ?",
            (self.id, lo, hi)).fetchone()
        count, = r
        if count:
            return GroupBrowseSubdir(self.api, self.id, path)
        raise llfuse.FUSEError(errno.ENOENT)


def add_arguments(parser):
    btn_lib.add_arguments(parser)


def configure(parser, args, backend):
    api = btn_lib.API.from_args(parser, args)
    return Tracker(backend, api)
