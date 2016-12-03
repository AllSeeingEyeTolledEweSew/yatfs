import better_bencode
import hashlib


def info_files(info):
    if b"files" in info:
        return info[b"files"]
    else:
        return [{b"path": [info[b"name"]], b"length": info[b"length"]}]


def tdata_tobj(tdata):
    return better_bencode.loads(tdata)


def tobj_tdata(tobj):
    return better_bencode.dumps(tobj)


def tdata_hash(tdata):
    return info_hash(tdata_tobj(tdata)[b"info"])


def info_hash(info):
    return hashlib.sha1(tobj_tdata(info)).hexdigest()


def file_range_split(info, idx, offset, size):
    if b"files" in info:
        for i in range(0, idx):
            offset += info[b"files"][i][b"size"]
    else:
        assert idx == 0

    piece_length = info[b"piece_length"]
    pieces = range(
        int(offset / piece_length),
        int((offset + size - 1) / piece_length) + 1)

    split = []
    for p in pieces:
        piece_offset = p * piece_length
        lo = offset - piece_offset
        if lo < 0:
            lo = 0
        hi = offset - piece_offset + size
        if hi > piece_length:
            hi = piece_length
        split.append((p, lo, hi))
    return split
