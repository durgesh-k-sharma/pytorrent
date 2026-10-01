#!/usr/bin/env python3
"""Offline tests: everything runs over localhost, no internet needed."""
import asyncio
import hashlib
import os
import random
import tempfile

from make_torrent import create_torrent
from torrent_client import Client, Torrent, bdecode, bencode

DEAD_TRACKER = "http://127.0.0.1:9/announce"   # refuses connections instantly


def sha(path):
    with open(path, "rb") as f:
        return hashlib.sha1(f.read()).hexdigest()


def test_bencode():
    assert bencode({"cow": "moo", "spam": "eggs"}) == b"d3:cow3:moo4:spam4:eggse"
    assert bdecode(b"d4:spaml1:a1:bee") == {b"spam": [b"a", b"b"]}
    assert bdecode(b"i-3e") == -3
    obj = {b"a": [1, 2, {b"b": b"xyz"}], b"c": b"\x00\xff"}
    assert bdecode(bencode(obj)) == obj
    print("ok  bencode")


def test_info_hash_uses_raw_bytes(raw):
    """info-hash must equal sha1 of the exact 'info' substring of the file."""
    t = Torrent(raw)
    start = raw.index(b"4:infod") + len(b"4:info")
    assert t.info_hash == hashlib.sha1(raw[start:-1]).digest()
    print("ok  info-hash over raw bytes")


async def main():
    test_bencode()
    with tempfile.TemporaryDirectory() as tmp:
        seed_dir, a_dir, b_dir = (os.path.join(tmp, d) for d in ("seed", "a", "b"))
        os.makedirs(seed_dir)
        src = os.path.join(seed_dir, "payload.bin")
        with open(src, "wb") as f:
            f.write(random.randbytes(3 * 1024 * 1024 + 12345))   # last piece is short

        raw = create_torrent(src, piece_length=64 * 1024, announce=DEAD_TRACKER)
        test_info_hash_uses_raw_bytes(raw)
        t = Torrent(raw)
        print(f"    {t.num_pieces} pieces, info_hash {t.info_hash.hex()}")

        seeder = Client(t, seed_dir, listen_port=46881, seed=True, show_progress=False)
        seed_task = asyncio.create_task(seeder.run())
        await asyncio.sleep(0.5)

        # 1) plain download from a seed
        a = Client(t, a_dir, listen_port=46901, peers=[("127.0.0.1", 46881)], show_progress=False)
        await asyncio.wait_for(a.run(), 60)
        assert sha(os.path.join(a_dir, "payload.bin")) == sha(src)
        print("ok  download from seeder")

        # 2) resume: corrupt two pieces on disk, only those should be refetched
        path = os.path.join(a_dir, "payload.bin")
        with open(path, "r+b") as f:
            f.seek(5 * 65536); f.write(b"\x00" * 100)
            f.seek(20 * 65536 + 7); f.write(b"garbage")
        a2 = Client(t, a_dir, listen_port=46911, peers=[("127.0.0.1", 46881)], show_progress=False)
        task = asyncio.create_task(a2.run())
        await asyncio.wait_for(task, 60)
        assert sha(path) == sha(src)
        assert a2.rx_bytes == 2 * 65536, a2.rx_bytes
        print("ok  resume re-downloaded only the 2 corrupted pieces")

        # 3) swarm: a second leecher gets pieces from both the seeder and leecher
        #    `a3` (which uploads pieces it has already verified).
        a3 = Client(t, os.path.join(tmp, "a3"), listen_port=46921,
                    peers=[("127.0.0.1", 46881)], show_progress=False)
        b = Client(t, b_dir, listen_port=46931,
                   peers=[("127.0.0.1", 46881), ("127.0.0.1", 46921)], show_progress=False)
        await asyncio.wait_for(asyncio.gather(a3.run(), b.run()), 90)
        assert sha(os.path.join(b_dir, "payload.bin")) == sha(src)
        assert sha(os.path.join(tmp, "a3", "payload.bin")) == sha(src)
        print("ok  two-leecher swarm "
              f"(a3 uploaded {a3.uploaded} B, b uploaded {b.uploaded} B, seeder {seeder.uploaded} B)")

        seed_task.cancel()
        try:
            await seed_task
        except asyncio.CancelledError:
            pass
    print("all tests passed")


if __name__ == "__main__":
    asyncio.run(main())
