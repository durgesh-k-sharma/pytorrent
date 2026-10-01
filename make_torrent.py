#!/usr/bin/env python3
"""Create a single-file .torrent.   python make_torrent.py FILE -a TRACKER_URL [-l 262144]"""
import argparse
import hashlib
import os

from torrent_client import bencode


def create_torrent(path: str, piece_length: int = 256 * 1024, announce: str = "") -> bytes:
    pieces = bytearray()
    with open(path, "rb") as f:
        while chunk := f.read(piece_length):
            pieces += hashlib.sha1(chunk).digest()
    meta = {
        "announce": announce,
        "info": {
            "name": os.path.basename(path),
            "length": os.path.getsize(path),
            "piece length": piece_length,
            "pieces": bytes(pieces),
        },
    }
    return bencode(meta)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("-a", "--announce", default="udp://tracker.example.org:6969/announce")
    ap.add_argument("-l", "--piece-length", type=int, default=256 * 1024)
    ap.add_argument("-o", "--output")
    a = ap.parse_args()
    out = a.output or a.file + ".torrent"
    with open(out, "wb") as fh:
        fh.write(create_torrent(a.file, a.piece_length, a.announce))
    print("wrote", out)
