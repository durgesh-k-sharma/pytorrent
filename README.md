# pytorrent

A small, readable BitTorrent client written in pure Python. No dependencies, one file, built directly from the [BEP 3 specification](https://www.bittorrent.org/beps/bep_0003.html) as a way to learn how peer-to-peer networking works.

It downloads and verifies single-file torrents, resumes interrupted downloads, and uploads pieces back to the swarm.

## Features

- **Bencoding**: encoder and decoder; the info-hash is computed over the raw `info` bytes, as the spec requires
- **Trackers**: HTTP with compact peer lists (BEP 23) and UDP (BEP 15), queried in parallel
- **Peer wire protocol**: handshake, choke/unchoke, interested, have, bitfield, request, piece
- **Smart downloading**: rarest-first piece selection, pipelined block requests, endgame mode
- **Integrity**: every piece is SHA-1 verified before it is written to disk
- **Resume**: existing data is re-hashed on start, and only missing pieces are fetched
- **Upload and seeding**: serves verified pieces to other peers; `--seed` keeps serving after completion

## Requirements

Python 3.9 or newer. Nothing to install.

## Quick start

```bash
python torrent_client.py file.torrent -o downloads/
```

```
Name:      example.iso
Size:      1.4 GiB in 5632 pieces of 256.0 KiB
Info hash: 7bb3add4dea3b4ab67a3e4057975dc531b3acfa7
Checking existing data...
[############                  ]  41.3%  2326/5632 pieces  6.2 MiB/s  peers 18 (known 54)
```

### Options

| Flag | Description |
| --- | --- |
| `-o, --output DIR` | Download directory (default: current directory) |
| `-p, --port PORT` | First port to listen on; tries 10 consecutive ports (default: 6881) |
| `--no-listen` | Do not accept incoming connections |
| `--max-peers N` | Maximum outgoing connections (default: 40) |
| `--peer HOST:PORT` | Add a peer manually; repeatable |
| `--seed` | Keep seeding after the download finishes |
| `-v, --verbose` | Log tracker and peer events to stderr |

### Creating a torrent

```bash
python make_torrent.py myfile.bin -a http://tracker.example/announce -l 262144
```

## How it works

```
 .torrent ──► Torrent (bdecode, info-hash, piece hashes)
                  │
                  ▼
   Client ──► tracker loop ──► HTTP / UDP announce ──► peer addresses
     │
     ├─► connector ──► outbound TCP connections ─┐
     └─► listener  ──► inbound TCP connections  ─┤
                                                 ▼
                                       Peer (one per connection)
                                         handshake → messages
                                                 │
                                                 ▼
                                PieceManager (rarest-first, endgame)
                                         │ verify SHA-1
                                         ▼
                                      Storage (file on disk)
```

Everything runs on a single `asyncio` event loop. Each connection is a `Peer` object that handles both directions of the protocol, so the same code serves outgoing and incoming connections.

## Project layout

| File | Purpose |
| --- | --- |
| `torrent_client.py` | The client: bencode, metainfo, trackers, piece manager, peer protocol, CLI |
| `make_torrent.py` | Builds `.torrent` files |
| `test_local.py` | End-to-end tests |

## Testing

```bash
python test_local.py
```

The tests run entirely over localhost and need no internet. They cover:

- bencode round trips and info-hash calculation
- a full download from a seeder, including a short final piece
- resume after on-disk corruption (only the damaged pieces are refetched)
- a small swarm where leechers exchange pieces with each other

## Limitations and roadmap

- [ ] Multi-file torrents
- [ ] Magnet links (DHT, BEP 5; metadata exchange, BEP 9/10)
- [ ] Tit-for-tat choking (currently every interested peer is unchoked)
- [ ] `cancel` messages in endgame mode
- [ ] IPv6, uTP, protocol encryption
- [ ] Non-blocking disk I/O

## References

- [BEP 3: The BitTorrent Protocol Specification](https://www.bittorrent.org/beps/bep_0003.html)
- [BEP 15: UDP Tracker Protocol](https://www.bittorrent.org/beps/bep_0015.html)
- [BEP 23: Tracker Returns Compact Peer Lists](https://www.bittorrent.org/beps/bep_0023.html)
