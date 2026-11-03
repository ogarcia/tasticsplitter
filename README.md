# Tastic Splitter

[Meshtastic](https://meshtastic.org) **client multiplexer with cache**.
Speaks the client API on both sides: it connects to a physical radio
(serial or TCP) and serves a TCP interface to multiple concurrent clients
(the official apps over TCP, the Python CLI, Meshtastic Web through a
bridge, home automation, bots...). The radio's API only takes one client
at a time; the proxy keeps that single session and gives every client the
same view of the radio — it acts as a **faithful mirror**.

Sibling of [Core Splitter](https://github.com/ogarcia/coresplitter), which
does the same for MeshCore.

## How it works

```
┌────────┐             ┌────────────────┐                    ┌──────────┐
│ Client │◄───────────►│ Tastic Splitter│◄──────────────────►│ Physical │
│ A      │     TCP     │  (proxy)       │     TCP/serial     │ Radio    │
├────────┤  0x94 0xC3  │                │  ToRadio/FromRadio │ Node     │
│ Client │             │  ┌──────────┐  │                    │          │
│ B      │             │  │  SQLite  │  │                    │          │
└────────┘             │  │ messages │  │                    │          │
                       │  └──────────┘  │                    │          │
                       └────────────────┘                    └──────────┘
```

1. The proxy connects to the radio and sends its own `want_config`. The
   whole answer (my info, node database, channels, config, module
   config, metadata...) is cached in memory, and only then does the TCP
   frontend start listening. If the radio disconnects, every client
   connection is closed and the frontend stops listening until the radio
   is back: the proxy behaves like an absent node rather than serving a
   radio it doesn't have.
2. A client's `want_config` is answered from that cache, in the radio's
   order, ending with the client's own `config_complete_id`. The special
   nonces 69420 (config only) and 69421 (node database only) are
   honoured, splitting the stream exactly as the firmware does. A client gets no other traffic until it completes its
   `want_config`, as with the radio itself.
3. Packets from clients are forwarded to the radio byte for byte (a
   packet without id gets one, since replies are matched by id). The
   radio's `QueueStatus` for it, its routing ACK and any response
   (`request_id`: admin, traceroute, position requests...) go back only to
   the client that sent it.
4. Once the radio queues a text message from a client, the other clients
   get it as if it had arrived over the air (`from` set to the local
   node). Refused packets are not echoed.
5. Everything else the radio sends (incoming packets, log records,
   notifications...) goes to every client.
6. The cached node database follows the traffic like the firmware's
   does: last heard, SNR, hops, and the user, position and device
   metrics carried by NODEINFO, POSITION and TELEMETRY packets. New
   nodes are added. The whole configuration is read again after an
   admin command to the local node, when the radio reboots, and every
   `TASTICSPLITTER_RESYNC_MINUTES`.
7. Missed messages: text messages (incoming, or echoed from another
   client) are stored in SQLite. A client is identified by the frontend
   port it connects to — the API carries no app name. When a known
   client completes its `want_config`, the messages it missed are sent
   right after it, except its own. A client never seen before starts
   from that moment. Give each app its own port
   (`--tcp-frontend-port 4403,4404`) so each keeps its own history.
8. Heartbeats and `disconnect` from clients stay in the proxy, which
   keeps the radio session alive with its own heartbeats.

The proxy never re-encodes what it relays, so fields added by firmware
newer than the protobufs it was built with reach the clients intact.

## Requirements

- **Rust edition 2024** (requires Rust ≥ 1.85)
- **Linux** (primary target)

## Build

```bash
cargo build --release
```

The binary is at `target/release/tasticsplitter`.

## Usage

### TCP backend

A node with WiFi/Ethernet, or `meshtasticd`:

```bash
tasticsplitter \
  --tcp-backend-host 192.168.1.100 \
  --tcp-frontend-port 4403 \
  --data-dir /var/lib/tasticsplitter
```

### Serial backend

```bash
tasticsplitter --serial /dev/ttyUSB0 --tcp-frontend-port 4403
```

Clients then connect to the proxy as they would to a network node, e.g.
`meshtastic --host proxy-host`.

### Environment variables

| Variable | Description |
|---|---|
| `TASTICSPLITTER_SERIAL_PORT` | Serial port path |
| `TASTICSPLITTER_BAUD_RATE` | Baud rate (default: 115200) |
| `TASTICSPLITTER_TCP_BACKEND_HOST` | Radio TCP host |
| `TASTICSPLITTER_TCP_BACKEND_PORT` | Radio TCP port (default: 4403) |
| `TASTICSPLITTER_TCP_FRONTEND_HOST` | Frontend TCP bind (default: 0.0.0.0) |
| `TASTICSPLITTER_TCP_FRONTEND_PORT` | Frontend TCP port(s), comma separated (default: 4403) |
| `TASTICSPLITTER_DATA_DIR` | Data directory (default: ./data) |
| `TASTICSPLITTER_LOG_LEVEL` | Log level: off, error, warn, info, debug, verbose |
| `TASTICSPLITTER_LOG_JSON` | Enable JSON event logging |
| `TASTICSPLITTER_RESYNC_MINUTES` | Minutes between full re-reads of the radio's config, 0 = only when it changes (default: 60) |
| `TASTICSPLITTER_MESSAGES_RETENTION_DAYS` | Days to keep stored messages, 0 = forever (default: 0) |
| `TASTICSPLITTER_CLIENTS_RETENTION_DAYS` | Days a client may stay away before it is forgotten, 0 = never (default: 180) |

### Database maintenance

At startup and every 24 hours the proxy purges the SQLite database
(`<data-dir>/state.db`) according to the retention settings, then returns
the freed space with an incremental vacuum. Messages expire by local
arrival time. A client forgotten after the clients retention is treated
as new when it comes back.

### Running as a systemd service

Install the binary as `/usr/local/bin/tasticsplitter` and create
`/etc/systemd/system/tasticsplitter.service`:

```ini
[Unit]
Description=Meshtastic client multiplexer with cache
Wants=network-online.target
After=network-online.target

[Service]
Type=exec
ExecStart=/usr/local/bin/tasticsplitter
EnvironmentFile=-/etc/tasticsplitter.conf
Environment=TASTICSPLITTER_DATA_DIR=/var/lib/tasticsplitter
DynamicUser=yes
StateDirectory=tasticsplitter
# Serial backend: grant access to the port (dialout on Debian/Ubuntu,
# uucp on Arch)
#SupplementaryGroups=dialout
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

The configuration goes in `/etc/tasticsplitter.conf` using the
[environment variables](#environment-variables) above, for example:

```bash
TASTICSPLITTER_SERIAL_PORT=/dev/ttyUSB0
TASTICSPLITTER_TCP_FRONTEND_PORT=4403,4404
TASTICSPLITTER_MESSAGES_RETENTION_DAYS=365
```

Then enable it and follow the log:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tasticsplitter
journalctl -u tasticsplitter -f
```

The proxy keeps retrying the radio connection on its own and stops
cleanly on `systemctl stop`.

## Test suite

Unit tests: `cargo test`.

Integration tests run the proxy between a fake radio
(`tools/fake_radio.py`) and test clients. They need `uv` and use
`target/debug/tasticsplitter`, so build it first:

```bash
cargo build
uv run tools/test_proxy.py
```

Each test gets a fresh fake radio and proxy on ephemeral ports.

| Test | Checks |
|---|---|
| `config_served_from_cache` | Two clients get the full config with their own nonce; the radio sees a single `want_config` |
| `special_nonces` | 69420 leaves out other nodes; 69421 sends only node infos |
| `incoming_text_fans_out` | A message from the mesh reaches every client |
| `no_traffic_before_handshake` | A client gets nothing until it completes `want_config` |
| `send_routes_replies_and_echoes` | The sender gets its queue status and ACK; the other client gets the echo, not the replies |
| `refused_packet_not_echoed` | A packet the radio refuses is not echoed |
| `packet_without_id_gets_one` | A packet with id 0 is given one before reaching the radio |
| `missed_messages_replayed` | A client that was away gets what it missed, once |
| `own_messages_not_replayed` | A client never gets its own messages back |
| `new_node_reaches_cache` | A node first heard after startup is in the next client's node database |
| `local_admin_triggers_resync` | After `set_owner` the cached config is read again |
| `admin_response_to_requester_only` | An admin response only reaches the client that asked |
| `radio_loss_closes_clients` | Losing the radio closes clients and the frontend; it comes back with the radio |

The fake radio also runs standalone:

```bash
uv run tools/fake_radio.py --port 4403 --inject 10
```

## Container

```bash
docker build -t tasticsplitter -f Containerfile .
docker run --rm tasticsplitter --help
```

## Project status

Early. Tested against a Heltec V3 with firmware 2.8.2 over TCP, with the
`meshtastic` CLI (`--info`, `--nodes`, `--traceroute`) alongside other
clients. Pending: serial backend on real hardware and the official apps.

Not supported yet:

- **BLE backend**: Meshtastic's BLE API (toRadio / fromRadio / fromNum
  characteristics) differs from the stream API and needs its own
  transport.
- Replay of anything other than text messages (waypoints, positions...).
  On connecting, the radio flushes the packets it queued while nobody
  was attached; those that arrive before any client is ready are only
  kept if they are text messages.
- XModem file transfers with more than one client doing them at once.
