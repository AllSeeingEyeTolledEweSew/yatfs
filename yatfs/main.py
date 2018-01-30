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


def create_parser():
    parser = argparse.ArgumentParser(
        description="Yet Another Torrent Filesystem")

    parser.add_argument("mountpoint")
    parser.add_argument("--verbose", "-v", action="count")

    parser.add_argument(
        "--backend", choices=(BACKEND_DELUGE, BACKEND_NOOP))

    fuse_group = parser.add_argument_group("FUSE options")
    fuse_group.add_argument(
        "--fuse_options", type=comma_separated_list)
    fuse_group.add_argument("--num_workers", type=int, default=64)

    io_group = parser.add_argument_group("I/O options")
    io_group.add_argument(
        "--read_timeout", type=int, default=60)
    io_group.add_argument(
        "--keepalive", type=int, default=60)
    io_group.add_argument(
        "--readahead_pieces", type=int, default=4)
    io_group.add_argument(
        "--readahead_bytes", type=int, default=0x2000000)

    backend_deluge.add_arguments(parser)

    tracker_btn.add_arguments(parser)

    return parser


def configure_backend(parser, args):
    if args.backend == BACKEND_DELUGE:
        return backend_deluge.configure(parser, args)
    if args.backend == BACKEND_NOOP:
        return backend_noop.Backend()


def configure_root(parser, args, backend):
    root = yatfs_fs.StaticDir()
    btn_root = tracker_btn.configure(parser, args, backend)
    if btn_root is not None:
        root.mkdentry(b"btn", btn_root)
    return root


def configure_fuse_options(args):
    filtered = set(args.fuse_options)
    filtered.discard("default_permissions")
    return filtered


def main():
    parser = create_parser()
    args = parser.parse_args()

    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(
        stream=sys.stdout, level=level,
        format="%(asctime)s %(levelname)s %(threadName)s "
        "%(filename)s:%(lineno)d %(message)s")

    backend = configure_backend(parser, args)
    root = configure_root(parser, args, backend)
    fs = yatfs_fs.Filesystem(backend, root)
    ops = yatfs_fs.Operations(fs)
    fuse_options = configure_fuse_options(args)

    llfuse.init(ops, args.mountpoint, fuse_options)

    try:
        llfuse.main(workers=args.num_workers)
    finally:
        llfuse.close()
