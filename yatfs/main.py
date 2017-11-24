import argparse
import logging
import sys

import apsw
apsw.fork_checker()
import llfuse

import deluge_client_sync
from yatfs.backend import deluge as backend_deluge
from yatfs.backend import noop as backend_noop
from yatfs.tracker import btn as tracker_btn
from yatfs import fs as yatfs_fs


BACKEND_DELUGE = "deluge"
BACKEND_NOOP = "noop"

TRACKER_BTN = "btn"


def comma_separated_list(string):
    if string:
        return string.split(",")
    else:
        return []


def comma_separated_map(string):
    d = {}
    for item in comma_separated_list(string):
        item = item.split("=", 1)
        key = item[0]
        if len(item) == 1:
            if key.startswith("no"):
                value = False
            else:
                value = True
        else:
            value = item[1]
        d[key] = value
    return d


def parse_args():
    parser = argparse.ArgumentParser(
        description="Yet Another Torrent Filesystem")

    parser.add_argument("mountpoint")
    parser.add_argument(
        "--debug", action="store_true", help="Enable debugging output")
    parser.add_argument(
        "--fuse_options", type=comma_separated_list, help="FUSE options")

    parser.add_argument(
        "--backend", choices=(BACKEND_DELUGE, BACKEND_NOOP))
    parser.add_argument(
        "--backend_options", type=comma_separated_map, help="Backend options")

    parser.add_argument(
        "--tracker", choices=(TRACKER_BTN,))
    parser.add_argument(
        "--tracker_options", type=comma_separated_map, help="Tracker options")

    parser.add_argument("--num_workers", type=int, default=64)

    args = parser.parse_args()


    return args


def configure_backend(args):
    if args.backend == BACKEND_DELUGE:
        return backend_deluge.configure_backend(**args.backend_options)
    if args.backend == BACKEND_NOOP:
        return backend_noop.Backend()


def configure_root(args, backend):
    if args.tracker == TRACKER_BTN:
        return tracker_btn.configure_root(backend, **args.tracker_options)


def configure_fuse_options(args):
    filtered = set(args.fuse_options)
    filtered.discard("default_permissions")
    return filtered


def main():
    args = parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(
        stream=sys.stdout, level=level,
        format="%(asctime)s %(levelname)s %(threadName)s "
        "%(filename)s:%(lineno)d %(message)s")

    backend = configure_backend(args)
    root = configure_root(args, backend)
    fs = yatfs_fs.Filesystem(backend, root)
    ops = yatfs_fs.Operations(fs)
    fuse_options = configure_fuse_options(args)

    llfuse.init(ops, args.mountpoint, fuse_options)

    try:
        llfuse.main(workers=args.num_workers)
    finally:
        llfuse.close()
