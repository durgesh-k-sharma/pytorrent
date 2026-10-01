#!/usr/bin/env python3
"""
A small BitTorrent client for single-file torrents. Pure Python 3.9+, stdlib only.

Implements:
  * BEP 3   bencoding, metainfo, HTTP trackers, the peer wire protocol
  * BEP 23  compact peer lists
  * BEP 15  UDP trackers
It downloads (rarest-first, pipelined, hash-verified, resumable) and also uploads
completed pieces to peers, so two copies of this client can talk to each other.

Usage:
    python torrent_client.py file.torrent -o downloads/
    python torrent_client.py file.torrent --seed            # keep seeding afterwards
    python torrent_client.py file.torrent --peer 10.0.0.5:6881 -v

Not implemented (good next steps): multi-file torrents, magnet links / DHT /
metadata exchange (BEP 9/5/10), choking algorithm, IPv6, uTP, encryption.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import random
import socket
import struct
import sys
import time
import urllib.parse
import urllib.request

BLOCK_SIZE = 16 * 1024      # request size; BEP 3 says 2^14 is the de-facto maximum
PIPELINE = 16               # outstanding block requests per peer
MAX_MESSAGE = 2 * 1024 * 1024
PSTR = b"\x13BitTorrent protocol"

# --------------------------------------------------------------------------- #
# Bencoding
# --------------------------------------------------------------------------- #


class BencodeError(ValueError):
    pass


def bencode(obj) -> bytes:
    if isinstance(obj, bool):
        raise TypeError("bool is not bencodable")
    if isinstance(obj, int):
        return b"i%de" % obj
    if isinstance(obj, str):
        obj = obj.encode("utf-8")
    if isinstance(obj, bytes):
        return b"%d:%s" % (len(obj), obj)
    if isinstance(obj, list):
        return b"l" + b"".join(bencode(x) for x in obj) + b"e"
    if isinstance(obj, dict):
        items = sorted((k.encode() if isinstance(k, str) else k, v) for k, v in obj.items())
        return b"d" + b"".join(bencode(k) + bencode(v) for k, v in items) + b"e"
    raise TypeError(f"cannot bencode {type(obj)!r}")


class _Decoder:
    """Recursive-descent bdecoder. Remembers where the top-level 'info' value sits
    in the input, because the info-hash must be computed over those exact bytes."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0
        self.depth = 0
        self.info_span = None

    def parse(self):
        c = self.data[self.pos:self.pos + 1]
        if c == b"i":
            end = self.data.index(b"e", self.pos)
            n = int(self.data[self.pos + 1:end])
            self.pos = end + 1
            return n
        if c == b"l":
            self.pos += 1
            self.depth += 1
            out = []
            while self.data[self.pos:self.pos + 1] != b"e":
                out.append(self.parse())
            self.pos += 1
            self.depth -= 1
            return out
        if c == b"d":
            self.pos += 1
            self.depth += 1
            out = {}
            while self.data[self.pos:self.pos + 1] != b"e":
                key = self._string()
                start = self.pos
                out[key] = self.parse()
                if self.depth == 1 and key == b"info":
                    self.info_span = (start, self.pos)
            self.pos += 1
            self.depth -= 1
            return out
        if c.isdigit():
            return self._string()
        raise BencodeError(f"unexpected byte {c!r} at offset {self.pos}")

    def _string(self) -> bytes:
        colon = self.data.index(b":", self.pos)
        n = int(self.data[self.pos:colon])
        start, end = colon + 1, colon + 1 + n
        if n < 0 or end > len(self.data):
            raise BencodeError("bad string length")
        self.pos = end
        return self.data[start:end]


def bdecode(data: bytes):
    d = _Decoder(data)
    value = d.parse()
    if d.pos != len(data):
        raise BencodeError("trailing data")
    return value


# --------------------------------------------------------------------------- #
# Metainfo
# --------------------------------------------------------------------------- #


class TorrentError(Exception):
    pass


class Torrent:
    def __init__(self, raw: bytes):
        try:
            dec = _Decoder(raw)
            meta = dec.parse()
            info = meta[b"info"]
            start, end = dec.info_span
            self.info_hash = hashlib.sha1(raw[start:end]).digest()

            if b"files" in info:
                raise TorrentError("multi-file torrents are not supported yet")
            self.length = info[b"length"]
            self.piece_length = info[b"piece length"]
            # Never trust the name as a path: keep only the final component.
            self.name = os.path.basename(info[b"name"].decode("utf-8", "replace")) or "download"

            pieces = info[b"pieces"]
            if len(pieces) % 20:
                raise TorrentError("'pieces' length is not a multiple of 20")
            self.piece_hashes = [pieces[i:i + 20] for i in range(0, len(pieces), 20)]
            expected = -(-self.length // self.piece_length)
            if len(self.piece_hashes) != expected:
                raise TorrentError("piece count does not match file length")

            urls = [t.decode() for tier in meta.get(b"announce-list", []) for t in tier]
            if b"announce" in meta:
                urls.insert(0, meta[b"announce"].decode())
            self.trackers = list(dict.fromkeys(urls))
        except (KeyError, ValueError, TypeError, IndexError, AttributeError) as exc:
            raise TorrentError(f"invalid .torrent file: {exc!r}") from exc

    @property
    def num_pieces(self) -> int:
        return len(self.piece_hashes)


# --------------------------------------------------------------------------- #
# Trackers (blocking functions; the client runs them in a thread pool)
# --------------------------------------------------------------------------- #


def parse_compact_peers(blob: bytes):
    """BEP 23: 6 bytes per peer, 4-byte IPv4 + 2-byte big-endian port."""
    return [(socket.inet_ntoa(blob[i:i + 4]), struct.unpack(">H", blob[i + 4:i + 6])[0])
            for i in range(0, len(blob) - 5, 6)]


def announce_http(url, torrent, peer_id, port, uploaded, downloaded, left, event):
    params = {
        "info_hash": torrent.info_hash, "peer_id": peer_id, "port": port,
        "uploaded": uploaded, "downloaded": downloaded, "left": left, "compact": 1,
    }
    if event:
        params["event"] = event
    full = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    with urllib.request.urlopen(full, timeout=10) as resp:
        body = resp.read()
    reply = bdecode(body)
    if b"failure reason" in reply:
        raise RuntimeError(reply[b"failure reason"].decode("utf-8", "replace"))
    peers = reply.get(b"peers", b"")
    if isinstance(peers, bytes):
        found = parse_compact_peers(peers)
    else:  # original non-compact list-of-dicts form
        found = [(p[b"ip"].decode(), p[b"port"]) for p in peers]
    return reply.get(b"interval", 1800), found


def announce_udp(url, torrent, peer_id, port, uploaded, downloaded, left, event):
    """BEP 15: connect handshake, then announce."""
    u = urllib.parse.urlparse(url)
    addr = (u.hostname, u.port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(8)
    try:
        tid = random.getrandbits(32)
        sock.sendto(struct.pack(">QII", 0x41727101980, 0, tid), addr)
        data, _ = sock.recvfrom(2048)
        action, rtid, conn_id = struct.unpack(">IIQ", data[:16])
        if action != 0 or rtid != tid:
            raise RuntimeError("bad connect response")

        tid = random.getrandbits(32)
        code = {None: 0, "completed": 1, "started": 2, "stopped": 3}[event]
        sock.sendto(struct.pack(">QII20s20sQQQIIIiH", conn_id, 1, tid, torrent.info_hash,
                                peer_id, downloaded, left, uploaded, code, 0,
                                random.getrandbits(32), -1, port), addr)
        data, _ = sock.recvfrom(4096)
        action, rtid, interval = struct.unpack(">III", data[:12])
        if rtid != tid:
            raise RuntimeError("transaction id mismatch")
        if action == 3:
            raise RuntimeError(data[8:].decode("utf-8", "replace"))
        if action != 1:
            raise RuntimeError("bad announce response")
        return interval, parse_compact_peers(data[20:])
    finally:
        sock.close()


# --------------------------------------------------------------------------- #
# Storage and piece bookkeeping
# --------------------------------------------------------------------------- #


class Storage:
    def __init__(self, path: str, length: int):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.fresh = not os.path.exists(path)
        self.f = open(path, "w+b" if self.fresh else "r+b")
        if os.fstat(self.f.fileno()).st_size != length:
            self.f.truncate(length)  # sparse pre-allocation

    def write(self, offset: int, data: bytes):
        self.f.seek(offset)
        self.f.write(data)

    def read(self, offset: int, n: int) -> bytes:
        self.f.seek(offset)
        return self.f.read(n)

    def close(self):
        self.f.flush()
        self.f.close()


class PieceManager:
    def __init__(self, torrent: Torrent, storage: Storage):
        self.t = torrent
        self.storage = storage
        self.n = torrent.num_pieces
        self.piece_length = torrent.piece_length
        self.have = [False] * self.n
        self.num_have = 0
        self.have_bytes = 0
        self.availability = [0] * self.n   # how many connected peers have each piece
        self.in_progress = {}              # piece index -> set of Peer objects working on it
        self.done = asyncio.Event()

    def piece_size(self, i: int) -> int:
        if i == self.n - 1:
            return self.t.length - i * self.piece_length
        return self.piece_length

    def complete(self) -> bool:
        return self.num_have == self.n

    def _mark(self, i: int):
        self.have[i] = True
        self.num_have += 1
        self.have_bytes += self.piece_size(i)
        if self.complete():
            self.done.set()

    def verify_existing(self):
        """Resume support: hash whatever is already on disk."""
        if self.storage.fresh:
            return
        for i in range(self.n):
            size = self.piece_size(i)
            data = self.storage.read(i * self.piece_length, size)
            if len(data) == size and hashlib.sha1(data).digest() == self.t.piece_hashes[i]:
                self._mark(i)

    def bitfield(self) -> bytes:
        out = bytearray((self.n + 7) // 8)
        for i, h in enumerate(self.have):
            if h:
                out[i >> 3] |= 0x80 >> (i & 7)
        return bytes(out)

    def pick(self, peer_pieces, owner):
        """Choose a piece for `owner`: rarest-first among unassigned pieces. Once every
        missing piece is assigned to someone (endgame), duplicate in-flight pieces."""
        fresh = [i for i in peer_pieces if not self.have[i] and i not in self.in_progress]
        if fresh:
            low = min(self.availability[i] for i in fresh)
            idx = random.choice([i for i in fresh if self.availability[i] == low])
        else:
            if self.n - self.num_have - len(self.in_progress) > 0:
                return None  # unassigned pieces exist, just not ones this peer has
            dup = [i for i in peer_pieces
                   if not self.have[i] and owner not in self.in_progress.get(i, ())]
            if not dup:
                return None
            idx = random.choice(dup)
        self.in_progress.setdefault(idx, set()).add(owner)
        return idx

    def release(self, idx, owner):
        owners = self.in_progress.get(idx)
        if owners is not None:
            owners.discard(owner)
            if not owners:
                del self.in_progress[idx]

    def submit(self, idx, data: bytes, owner) -> str:
        """Verify and store a fully downloaded piece. Returns 'ok', 'dup' or 'bad'."""
        self.release(idx, owner)
        if self.have[idx]:
            return "dup"
        if hashlib.sha1(data).digest() != self.t.piece_hashes[idx]:
            return "bad"
        self.storage.write(idx * self.piece_length, data)
        self.in_progress.pop(idx, None)
        self._mark(idx)
        return "ok"


# --------------------------------------------------------------------------- #
# Peer connection
# --------------------------------------------------------------------------- #


class ProtocolError(Exception):
    pass


class Peer:
    """One TCP connection. Symmetric: the same class handles outgoing and incoming."""

    def __init__(self, client, reader, writer, addr, initiator: bool):
        self.client = client
        self.reader = reader
        self.writer = writer
        self.addr = addr
        self.initiator = initiator
        self.pieces = set()          # indices the remote peer has
        self.choked = True           # are *we* choked by them?
        self.am_interested = False
        # state of the piece we are currently downloading from this peer
        self.current = None
        self.buf = None
        self.missing = []            # [(begin, length)] not yet requested
        self.outstanding = {}        # begin -> length, requested but not received
        self.last_progress = time.time()
        self._keepalive = None

    # -- lifecycle ---------------------------------------------------------- #

    async def run(self):
        self.client.peers.add(self)
        try:
            await self._handshake()
            await self._session()
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError,
                OSError, ProtocolError, struct.error) as exc:
            self.client.log(f"peer {self.addr}: {type(exc).__name__} {exc}")
        finally:
            self.close()

    def close(self):
        pm = self.client.pm
        if self.current is not None:
            pm.release(self.current, self)
            self.current = None
        for i in self.pieces:
            pm.availability[i] -= 1
        self.pieces.clear()
        if self._keepalive:
            self._keepalive.cancel()
        self.client.peers.discard(self)
        try:
            self.writer.close()
        except Exception:
            pass

    async def _handshake(self):
        c = self.client
        hello = PSTR + bytes(8) + c.t.info_hash + c.peer_id
        if self.initiator:
            self.writer.write(hello)
        data = await asyncio.wait_for(self.reader.readexactly(68), 10)
        if data[:20] != PSTR:
            raise ProtocolError("not a BitTorrent handshake")
        if data[28:48] != c.t.info_hash:
            raise ProtocolError("info_hash mismatch")
        if data[48:] == c.peer_id:
            raise ProtocolError("connected to ourselves")
        if not self.initiator:
            self.writer.write(hello)
        await self.writer.drain()

    # -- wire helpers ------------------------------------------------------- #

    def send(self, msg_id: int, payload: bytes = b""):
        self.writer.write(struct.pack(">IB", len(payload) + 1, msg_id) + payload)

    async def _read_message(self):
        (length,) = struct.unpack(">I", await self.reader.readexactly(4))
        if length == 0:
            return None, b""             # keep-alive
        if length > MAX_MESSAGE:
            raise ProtocolError("message too large")
        data = await self.reader.readexactly(length)
        return data[0], data[1:]

    async def _keepalive_loop(self):
        ticks = 0
        while True:
            await asyncio.sleep(15)
            ticks += 1
            if ticks % 4 == 0:
                self.writer.write(b"\x00\x00\x00\x00")
            # Drop peers that accepted our requests but stopped sending data.
            if self.outstanding and time.time() - self.last_progress > 45:
                self.writer.close()

    # -- main loop ---------------------------------------------------------- #

    async def _session(self):
        pm = self.client.pm
        if pm.num_have:
            self.send(5, pm.bitfield())
        await self.writer.drain()
        self._keepalive = asyncio.create_task(self._keepalive_loop())
        while True:
            msg_id, p = await asyncio.wait_for(self._read_message(), 180)
            if msg_id is None:
                continue
            if msg_id == 0:                      # choke
                self.choked = True
                self._requeue()
            elif msg_id == 1:                    # unchoke
                self.choked = False
                await self._fill()
            elif msg_id == 2:                    # interested -> simply unchoke them
                self.send(1)
                await self.writer.drain()
            elif msg_id == 3:                    # not interested
                pass
            elif msg_id == 4:                    # have
                (i,) = struct.unpack(">I", p[:4])
                if i < pm.n and i not in self.pieces:
                    self.pieces.add(i)
                    pm.availability[i] += 1
                await self._fill()
            elif msg_id == 5:                    # bitfield
                if len(p) < (pm.n + 7) // 8:
                    raise ProtocolError("short bitfield")
                for i in range(pm.n):
                    if p[i >> 3] & (0x80 >> (i & 7)) and i not in self.pieces:
                        self.pieces.add(i)
                        pm.availability[i] += 1
                await self._fill()
            elif msg_id == 6:                    # request
                await self._serve(p)
            elif msg_id == 7:                    # piece
                await self._on_block(p)
            # 8 (cancel) and unknown/extension ids are ignored

    # -- downloading -------------------------------------------------------- #

    def _requeue(self):
        """After a choke, peers discard our pending requests: queue them again."""
        if self.outstanding:
            self.missing = sorted(self.outstanding.items()) + self.missing
            self.outstanding = {}

    def _abandon(self):
        self.client.pm.release(self.current, self)
        self.current, self.buf, self.missing, self.outstanding = None, None, [], {}

    async def _fill(self):
        pm = self.client.pm

        # Keep interest state accurate (BEP 3).
        wanted = not pm.complete() and any(not pm.have[i] for i in self.pieces)
        if wanted != self.am_interested:
            self.am_interested = wanted
            self.send(2 if wanted else 3)
            await self.writer.drain()

        if self.choked or pm.complete():
            return
        if self.current is not None and pm.have[self.current]:
            self._abandon()                      # another peer finished it first
        if self.current is None:
            idx = pm.pick(self.pieces, self)
            if idx is None:
                return
            size = pm.piece_size(idx)
            self.current = idx
            self.buf = bytearray(size)
            self.missing = [(b, min(BLOCK_SIZE, size - b)) for b in range(0, size, BLOCK_SIZE)]
            self.outstanding = {}

        if not self.outstanding:
            self.last_progress = time.time()
        while self.missing and len(self.outstanding) < PIPELINE:
            begin, length = self.missing.pop(0)
            self.outstanding[begin] = length
            self.send(6, struct.pack(">III", self.current, begin, length))
        await self.writer.drain()

    async def _on_block(self, p: bytes):
        idx, begin = struct.unpack(">II", p[:8])
        block = p[8:]
        if idx != self.current or self.outstanding.get(begin) != len(block):
            return                               # stale or unsolicited
        del self.outstanding[begin]
        self.buf[begin:begin + len(block)] = block
        self.client.rx_bytes += len(block)
        self.last_progress = time.time()

        if not self.missing and not self.outstanding:
            idx, data = self.current, bytes(self.buf)
            self.current = self.buf = None
            result = self.client.pm.submit(idx, data, self)
            if result == "bad":
                raise ProtocolError(f"piece {idx} failed hash check")
            if result == "ok":
                self.client.broadcast_have(idx)
        await self._fill()

    # -- uploading ---------------------------------------------------------- #

    async def _serve(self, p: bytes):
        pm = self.client.pm
        idx, begin, length = struct.unpack(">III", p[:12])
        if (idx >= pm.n or not pm.have[idx] or length > BLOCK_SIZE
                or begin + length > pm.piece_size(idx)):
            return
        data = pm.storage.read(idx * pm.piece_length + begin, length)
        self.send(7, struct.pack(">II", idx, begin) + data)
        self.client.uploaded += length
        await self.writer.drain()


# --------------------------------------------------------------------------- #
# Client: ties trackers, peer discovery, listening and progress together
# --------------------------------------------------------------------------- #


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.1f} {unit}"
        n /= 1024


class Client:
    def __init__(self, torrent: Torrent, out_dir=".", listen_port=6881, max_peers=40,
                 seed=False, peers=(), verbose=False, show_progress=True):
        self.t = torrent
        self.out_dir = out_dir
        self.listen_port = listen_port       # None disables listening, 0 = any free port
        self.port = listen_port or 6881      # port announced to trackers
        self.max_peers = max_peers
        self.seed = seed
        self.verbose = verbose
        self.show_progress = show_progress
        self.peer_id = b"-PY0001-" + os.urandom(12)
        self.known = set(peers)              # (host, port) candidates
        self.peers = set()                   # live Peer objects
        self.active = {}                     # addr -> outbound connection task
        self.failed = {}                     # addr -> retry-after timestamp
        self.rx_bytes = 0
        self.uploaded = 0
        self.server = None
        self.pm = None
        self.storage = None

    def log(self, msg):
        if self.verbose:
            print(msg, file=sys.stderr)

    # -- connections -------------------------------------------------------- #

    async def _listen(self):
        if self.listen_port is None:
            return
        ports = [0] if self.listen_port == 0 else range(self.listen_port, self.listen_port + 10)
        for port in ports:
            try:
                self.server = await asyncio.start_server(self._on_incoming, "0.0.0.0", port)
                self.port = self.server.sockets[0].getsockname()[1]
                return
            except OSError:
                continue
        self.log("could not bind a listening port; incoming connections disabled")

    async def _on_incoming(self, reader, writer):
        if len(self.peers) >= self.max_peers * 2:
            writer.close()
            return
        await Peer(self, reader, writer, writer.get_extra_info("peername"), False).run()

    async def _outbound(self, addr):
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(*addr), 8)
        except (OSError, asyncio.TimeoutError):
            return
        await Peer(self, reader, writer, addr, True).run()

    def _connection_done(self, addr):
        self.active.pop(addr, None)
        self.failed[addr] = time.time() + 60     # back off before redialling

    async def _connector(self):
        while not self.pm.complete():
            now = time.time()
            for addr in list(self.known):
                if len(self.active) >= self.max_peers:
                    break
                if addr in self.active or self.failed.get(addr, 0) > now:
                    continue
                task = asyncio.create_task(self._outbound(addr))
                self.active[addr] = task
                task.add_done_callback(lambda _t, a=addr: self._connection_done(a))
            await asyncio.sleep(1)

    def broadcast_have(self, idx):
        msg = struct.pack(">I", idx)
        for p in list(self.peers):
            try:
                p.send(4, msg)
            except Exception:
                pass

    # -- trackers ----------------------------------------------------------- #

    async def _announce(self, event):
        loop = asyncio.get_running_loop()
        left = self.t.length - self.pm.have_bytes
        args = (self.t, self.peer_id, self.port, self.uploaded, self.rx_bytes, left, event)
        handlers = {"http": announce_http, "https": announce_http, "udp": announce_udp}
        jobs = []
        for url in self.t.trackers:
            fn = handlers.get(urllib.parse.urlparse(url).scheme)
            if fn:
                jobs.append((url, loop.run_in_executor(None, fn, url, *args)))
        results = await asyncio.gather(*(j for _, j in jobs), return_exceptions=True)
        interval = None
        for (url, _), res in zip(jobs, results):
            if isinstance(res, Exception):
                self.log(f"tracker {url}: {res!r}")
                continue
            iv, found = res
            self.log(f"tracker {url}: {len(found)} peers")
            self.known.update(found)
            interval = iv if interval is None else min(interval, iv)
        return interval

    async def _tracker_loop(self):
        event = "started"
        while True:
            interval = await self._announce(event)
            event = None
            wait = 30 if (interval is None or not self.known) else max(30, min(interval, 600))
            await asyncio.sleep(wait)

    # -- progress ----------------------------------------------------------- #

    async def _progress_loop(self):
        last_t, last_rx, speed = time.time(), self.rx_bytes, 0.0
        while True:
            await asyncio.sleep(1)
            now = time.time()
            inst = (self.rx_bytes - last_rx) / (now - last_t)
            speed = inst if speed == 0 else 0.7 * speed + 0.3 * inst
            last_t, last_rx = now, self.rx_bytes
            frac = self.pm.num_have / self.pm.n
            bar = "#" * int(frac * 30)
            sys.stdout.write(
                f"\r[{bar:<30}] {frac * 100:5.1f}%  {self.pm.num_have}/{self.pm.n} pieces  "
                f"{fmt_bytes(speed)}/s  peers {len(self.peers)} (known {len(self.known)})   ")
            sys.stdout.flush()

    # -- top level ---------------------------------------------------------- #

    async def run(self):
        loop = asyncio.get_running_loop()
        self.storage = Storage(os.path.join(self.out_dir, self.t.name), self.t.length)
        self.pm = PieceManager(self.t, self.storage)
        if self.show_progress:
            print("Checking existing data...")
        await loop.run_in_executor(None, self.pm.verify_existing)

        if self.pm.complete() and not self.seed:
            if self.show_progress:
                print("File is already complete.")
            self.storage.close()
            return

        await self._listen()
        tasks = [asyncio.create_task(self._tracker_loop()),
                 asyncio.create_task(self._connector())]
        if self.show_progress:
            tasks.append(asyncio.create_task(self._progress_loop()))
        try:
            if not self.pm.complete():
                await self.pm.done.wait()
                if self.show_progress:
                    print(f"\nDownload complete: {os.path.join(self.out_dir, self.t.name)}")
                try:
                    await asyncio.wait_for(self._announce("completed"), 10)
                except Exception:
                    pass
            if self.seed:
                if self.show_progress:
                    print("Seeding. Press Ctrl-C to stop.")
                await asyncio.Event().wait()
        finally:
            for t in tasks:
                t.cancel()
            for p in list(self.peers):
                p.close()
            if self.server:
                self.server.close()
            try:
                await asyncio.wait_for(self._announce("stopped"), 3)
            except BaseException:
                pass
            self.storage.close()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def parse_addr(s: str):
    host, _, port = s.rpartition(":")
    return host, int(port)


def main():
    ap = argparse.ArgumentParser(description="Minimal BitTorrent client (single-file torrents)")
    ap.add_argument("torrent", help="path to a .torrent file")
    ap.add_argument("-o", "--output", default=".", help="download directory (default: .)")
    ap.add_argument("-p", "--port", type=int, default=6881, help="first port to listen on")
    ap.add_argument("--no-listen", action="store_true", help="do not accept incoming connections")
    ap.add_argument("--max-peers", type=int, default=40)
    ap.add_argument("--peer", action="append", default=[], metavar="HOST:PORT",
                    help="add a peer manually (repeatable)")
    ap.add_argument("--seed", action="store_true", help="keep seeding after the download finishes")
    ap.add_argument("-v", "--verbose", action="store_true", help="log tracker/peer events to stderr")
    args = ap.parse_args()

    with open(args.torrent, "rb") as f:
        raw = f.read()
    try:
        torrent = Torrent(raw)
    except TorrentError as exc:
        sys.exit(f"error: {exc}")

    print(f"Name:      {torrent.name}")
    print(f"Size:      {fmt_bytes(torrent.length)} in {torrent.num_pieces} pieces "
          f"of {fmt_bytes(torrent.piece_length)}")
    print(f"Info hash: {torrent.info_hash.hex()}")
    print(f"Trackers:  {len(torrent.trackers)}")

    client = Client(torrent, args.output,
                    listen_port=None if args.no_listen else args.port,
                    max_peers=args.max_peers, seed=args.seed,
                    peers=[parse_addr(p) for p in args.peer], verbose=args.verbose)
    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)


if __name__ == "__main__":
    main()
