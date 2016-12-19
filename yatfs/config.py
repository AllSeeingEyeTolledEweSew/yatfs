import asyncio
import os
import threading
import sqlite3
import yaml

import deluge_client_async
from yatfs.backend import deluge as deluge_backend
from yatfs import routine
from yatfs import util


class Config(object):

    def __init__(self, inodb, get_torrent_data, params):
        self.inodb = inodb
        self.get_torrent_data = get_torrent_data
        self.params = params

        self._lock = threading.RLock()
        self._db = None
        self._routine = None
        self._backend = None

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

    @asyncio.coroutine
    def get_torrent_data_async(self, hash):
        return (yield from self.routine.call_io_async(
            self.get_torrent_data, hash))
