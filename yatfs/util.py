import better_bencode
import hashlib


def info_files(info):
    if b"files" in info:
        return [
            {b"path": [info[b"name"]] + f[b"path"], b"length": f[b"length"]}
            for f in info[b"files"]]
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
