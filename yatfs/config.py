import asyncio
import os
import threading
import sqlite3
import yaml

from yatfs.backend import deluge as deluge_backend
from yatfs.backend import deluge_client_async
from yatfs import inodb
from yatfs import routine
from yatfs import util


class Config(object):

    def __init__(self, path):
        self.path = path

        self._lock = threading.RLock()
        self._db = None
        self._inodb = None
        self._config = None
        self._routine = None
        self._backend = None

    @property
    def db_path(self):
        return os.path.join(self.path, "ino.db")

    @property
    def tfiles_path(self):
        return os.path.join(self.path, "files")

    @property
    def inodb(self):
        with self._lock:
            if not self._inodb:
                self._inodb = inodb.InoDb(self.db_path)
            return self._inodb

    @property
    def config_path(self):
        return os.path.join(self.path, "config.yaml")

    def __getitem__(self, name):
        with self._lock:
            if not self._config:
                if os.path.exists(self.config_path):
                    with open(self.config_path) as f:
                        self._config = yaml.load(f)
                else:
                    self._config = {}
            return self._config.get(name)

    @property
    def routine(self):
        with self._lock:
            if not self._routine:
                self._routine = routine.Routines()
            return self._routine

    @property
    def backend(self):
        with self._lock:
            if not self._backend:
                client = deluge_client_async.Client(loop=self.routine.loop)
                self._backend = deluge_backend.Backend(client, self)
            return self._backend

    def add_torrent_file(self, tdata):
        hash = util.tdata_hash(tdata)
        name = "%s.torrent" % hash
        if not os.path.exists(self.tfiles_path):
            os.makedirs(self.tfiles_path)
        with open(os.path.join(self.tfiles_path, name), mode="wb") as f:
            f.write(tdata)

    def get_torrent_data(self, hash):
        name = "%s.torrent" % hash
        with open(os.path.join(self.tfiles_path, name), mode="rb") as f:
            return f.read()

    @asyncio.coroutine
    def get_torrent_data_async(self, hash):
        return (yield from self.routine.call_io_async(
            self.get_torrent_data, hash))
