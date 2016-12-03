import argparse
import grp
import logging
import os
import pwd
import sys

from yatfs import config
from yatfs import fs
from yatfs import util


class Command(object):

    def __init__(self, args):
        self.args = args
        self.config_dir = self.args.config_dir
        self.config = config.Config(self.config_dir)
        self.inodb = self.config.inodb

    def get_uid_gid(self):
        if self.args.user is None:
            uid = os.geteuid()
        else:
            uid = pwd.getpwnam(self.args.user).pw_uid
        if self.args.group is None:
            gid = os.getegid()
        else:
            gid = grp.getgrnam(self.args.group).gr_gid
        return (uid, gid)

    def get_umask(self):
        if self.args.umask is None:
            # hurr
            umask = os.umask(0)
            os.umask(umask)
            return umask
        return self.args.umask

    def run(self):
        raise NotImplementedError()


class Mount(Command):

    def run(self):
        self.config.routine.run_loop_in_background()
        try:
            fs.TorrentFs(self.args.mountpoint, self.config)
        finally:
            self.config.routine.stop_loop()


class AddTorrentFile(Command):

    def run(self):
        tdata = self.args.torrent_file.read()
        info = util.tdata_tobj(tdata)[b"info"]
        hash = util.info_hash(info)
        base = self.args.dir
        uid, gid = self.get_uid_gid()
        umask = self.get_umask()
        if not base:
            base = os.path.join("/", hash)
        t = self.args.time
        with self.inodb:
            for idx, f in enumerate(util.info_files(info)):
                path = os.fsdecode(os.path.join(*f[b"path"]))
                path = os.path.join(base, path)
                size = f[b"length"]
                self.inodb.mkdir_p(
                    os.path.dirname(path), 0o555 & ~umask, uid, gid)
                ino = self.inodb.mkfile(
                    path, 0o444 & ~umask, hash, idx, size, uid, gid)
                if t is not None:
                    self.inodb.setattr_ino(ino, st_ctime=t, st_mtime=t)
            self.config.add_torrent_file(tdata)


class MkFile(Command):

    def run(self):
        uid, gid = self.get_uid_gid()
        umask = self.get_umask()
        with self.inodb:
            ino = self.inodb.mkfile(
                self.args.path, 0o444 & ~umask, self.args.hash,
                self.args.index, self.args.size, uid, gid)
            t = self.args.time
            if t is not None:
                self.inodb.setattr_ino(ino, st_ctime=t, st_mtime=t)


class Fsck(Command):

    def run(self):
        with self.inodb:
            self.inodb.fsck()


def main():
    logging.basicConfig(
        stream=sys.stdout, level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(threadName)s "
        "%(filename)s:%(lineno)d %(message)s")

    parser = argparse.ArgumentParser(
        description="Yet Another Torrent Filesystem")
    parser.add_argument("--config_dir", required=True)
    subparsers = parser.add_subparsers(title="Commands")

    mount = subparsers.add_parser("mount")
    mount.set_defaults(command=Mount)
    mount.add_argument("mountpoint")

    add_torrent_file = subparsers.add_parser("add_torrent_file")
    add_torrent_file.set_defaults(command=AddTorrentFile)
    add_torrent_file.add_argument("torrent_file", type=argparse.FileType("rb"))
    add_torrent_file.add_argument("--dir")
    add_torrent_file.add_argument("--user")
    add_torrent_file.add_argument("--group")
    add_torrent_file.add_argument("--umask", type=int)
    add_torrent_file.add_argument("--time", type=int)

    mkfile = subparsers.add_parser("mkfile")
    mkfile.set_defaults(command=MkFile)
    mkfile.add_argument("--path", required=True)
    mkfile.add_argument("--hash", required=True)
    mkfile.add_argument("--index", type=int, required=True)
    mkfile.add_argument("--size", type=int, required=True)
    mkfile.add_argument("--user")
    mkfile.add_argument("--group")
    mkfile.add_argument("--umask", type=int)
    mkfile.add_argument("--time", type=int)

    fsck = subparsers.add_parser("fsck")
    fsck.set_defaults(command=Fsck)

    args = parser.parse_args()
    command = args.command(args)
    command.run()
