#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["meshtastic"]
# ///
"""Fake Meshtastic radio speaking the client API over TCP.

Mimics what tasticsplitter relies on from the firmware: the want_config
stream, a QueueStatus for every packet, routing ACKs, admin get/set owner
and only one API client at a time (a new connection replaces the old).
Usable standalone or imported by test_proxy.py to inject traffic.
"""
import argparse
import asyncio
import logging
import random
import struct
import time

from meshtastic.protobuf import (
    admin_pb2,
    channel_pb2,
    config_pb2,
    mesh_pb2,
    module_config_pb2,
    portnums_pb2,
    telemetry_pb2,
)

log = logging.getLogger("fake_radio")

START1, START2 = 0x94, 0xC3
BROADCAST = 0xFFFFFFFF
MY_NODE = 0x11111111
OTHER_NODES = {0x22222222: "Alice", 0x33333333: "Bob"}
SPECIAL_NONCES = (69420, 69421)
HISTORY_PORTS = (
    portnums_pb2.POSITION_APP,
    portnums_pb2.NODEINFO_APP,
    portnums_pb2.TELEMETRY_APP,
    portnums_pb2.NODE_STATUS_APP,
)


def frame(payload: bytes) -> bytes:
    return bytes([START1, START2]) + struct.pack(">H", len(payload)) + payload


class FrameReader:
    def __init__(self, reader: asyncio.StreamReader):
        self.r = reader

    async def read(self) -> bytes:
        while True:
            b = await self.r.readexactly(1)
            if b[0] != START1:
                continue
            if (await self.r.readexactly(1))[0] != START2:
                continue
            (length,) = struct.unpack(">H", await self.r.readexactly(2))
            return await self.r.readexactly(length)


def user(num: int, long_name: str) -> mesh_pb2.User:
    return mesh_pb2.User(
        id=f"!{num:08x}", long_name=long_name, short_name=long_name[:4]
    )


class FakeRadio:
    def __init__(self, port: int):
        self.port = port
        self.owner = "Fake Radio"
        self.nodes = dict(OTHER_NODES)
        self.writer: asyncio.StreamWriter | None = None
        self.server: asyncio.base_events.Server | None = None
        self.want_configs: list[int] = []
        self.received: list[mesh_pb2.ToRadio] = []
        self.connections = 0
        self.next_from_id = 1
        # Last packet per (node, port, telemetry kind), replayed after every
        # full want_config like firmware 2.8 does. Seeded with old traffic.
        self.history: dict[tuple, mesh_pb2.MeshPacket] = {}
        now = int(time.time())
        position = mesh_pb2.Position(latitude_i=433311401, longitude_i=-84637327)
        self.record(
            self.make_packet(
                0x22222222, BROADCAST, portnums_pb2.POSITION_APP,
                position.SerializeToString(), rx_time=now - 86400,
            )
        )
        for metrics, age in (
            (telemetry_pb2.Telemetry(device_metrics=telemetry_pb2.DeviceMetrics(battery_level=80)), 3600),
            (telemetry_pb2.Telemetry(environment_metrics=telemetry_pb2.EnvironmentMetrics(temperature=21.5)), 7200),
        ):
            self.record(
                self.make_packet(
                    0x33333333, BROADCAST, portnums_pb2.TELEMETRY_APP,
                    metrics.SerializeToString(), rx_time=now - age,
                )
            )

    async def start(self):
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", self.port)
        log.info("listening on %d", self.port)

    async def stop(self):
        """Go away like a powered-off node: close the client and stop listening."""
        if self.writer:
            self.writer.close()
            self.writer = None
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    async def handle(self, reader, writer):
        if self.writer:
            self.writer.close()
        self.writer = writer
        self.connections += 1
        frames = FrameReader(reader)
        try:
            while True:
                msg = mesh_pb2.ToRadio()
                msg.ParseFromString(await frames.read())
                self.received.append(msg)
                await self.on_to_radio(msg)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            if self.writer is writer:
                self.writer = None

    def send(self, msg: mesh_pb2.FromRadio):
        if self.writer:
            self.next_from_id += 1
            msg.id = self.next_from_id
            self.writer.write(frame(msg.SerializeToString()))

    async def on_to_radio(self, msg: mesh_pb2.ToRadio):
        kind = msg.WhichOneof("payload_variant")
        if kind == "want_config_id":
            self.want_configs.append(msg.want_config_id)
            self.send_config(msg.want_config_id)
        elif kind == "packet":
            self.on_packet(msg.packet)

    def send_config(self, nonce: int):
        self.send(mesh_pb2.FromRadio(my_info=mesh_pb2.MyNodeInfo(my_node_num=MY_NODE)))
        self.send(
            mesh_pb2.FromRadio(
                node_info=mesh_pb2.NodeInfo(num=MY_NODE, user=user(MY_NODE, self.owner))
            )
        )
        self.send(
            mesh_pb2.FromRadio(
                metadata=mesh_pb2.DeviceMetadata(firmware_version="2.7.0.fake")
            )
        )
        self.send(
            mesh_pb2.FromRadio(
                channel=channel_pb2.Channel(
                    index=0,
                    role=channel_pb2.Channel.Role.PRIMARY,
                    settings=channel_pb2.ChannelSettings(name="LongFast"),
                )
            )
        )
        self.send(
            mesh_pb2.FromRadio(
                config=config_pb2.Config(lora=config_pb2.Config.LoRaConfig(hop_limit=3))
            )
        )
        self.send(
            mesh_pb2.FromRadio(
                moduleConfig=module_config_pb2.ModuleConfig(
                    mqtt=module_config_pb2.ModuleConfig.MQTTConfig(enabled=False)
                )
            )
        )
        for num, name in self.nodes.items():
            self.send(
                mesh_pb2.FromRadio(
                    node_info=mesh_pb2.NodeInfo(
                        num=num, user=user(num, name), last_heard=int(time.time()) - 600
                    )
                )
            )
        self.send(mesh_pb2.FromRadio(config_complete_id=nonce))
        if nonce not in SPECIAL_NONCES:
            for p in sorted(self.history.values(), key=lambda p: p.rx_time):
                self.send(mesh_pb2.FromRadio(packet=p))

    def on_packet(self, p: mesh_pb2.MeshPacket):
        # Channel indexes past 7 don't exist: the radio refuses the packet.
        res = 0 if p.channel < 8 else 1
        self.send(
            mesh_pb2.FromRadio(
                queueStatus=mesh_pb2.QueueStatus(
                    res=res, free=16, maxlen=16, mesh_packet_id=p.id
                )
            )
        )
        if res != 0 or not p.HasField("decoded"):
            return

        port = p.decoded.portnum
        if port == portnums_pb2.TEXT_MESSAGE_APP and p.want_ack:
            routing = mesh_pb2.Routing(error_reason=mesh_pb2.Routing.Error.NONE)
            self.send_packet(
                p.to if p.to != BROADCAST else MY_NODE,
                MY_NODE,
                portnums_pb2.ROUTING_APP,
                routing.SerializeToString(),
                request_id=p.id,
            )
        elif port == portnums_pb2.ADMIN_APP and p.to in (0, MY_NODE):
            admin = admin_pb2.AdminMessage()
            admin.ParseFromString(p.decoded.payload)
            which = admin.WhichOneof("payload_variant")
            if which == "set_owner":
                self.owner = admin.set_owner.long_name
            elif which == "get_owner_request" and p.decoded.want_response:
                reply = admin_pb2.AdminMessage(get_owner_response=user(MY_NODE, self.owner))
                self.send_packet(
                    MY_NODE,
                    MY_NODE,
                    portnums_pb2.ADMIN_APP,
                    reply.SerializeToString(),
                    request_id=p.id,
                )

    @staticmethod
    def history_key(p: mesh_pb2.MeshPacket):
        if p.decoded.portnum not in HISTORY_PORTS:
            return None
        kind = None
        if p.decoded.portnum == portnums_pb2.TELEMETRY_APP:
            t = telemetry_pb2.Telemetry()
            t.ParseFromString(p.decoded.payload)
            kind = t.WhichOneof("variant")
        return (getattr(p, "from"), p.decoded.portnum, kind)

    def record(self, p: mesh_pb2.MeshPacket):
        key = self.history_key(p)
        if key:
            self.history[key] = p

    @staticmethod
    def make_packet(src, dst, portnum, payload: bytes, channel=0, request_id=0, rx_time=None):
        return mesh_pb2.MeshPacket(
            to=dst,
            channel=channel,
            id=random.randint(1, 0xFFFFFFFF),
            rx_time=rx_time or int(time.time()),
            hop_start=3,
            hop_limit=2,
            decoded=mesh_pb2.Data(portnum=portnum, payload=payload, request_id=request_id),
            **{"from": src},
        )

    def send_packet(self, src, dst, portnum, payload: bytes, channel=0, request_id=0):
        p = self.make_packet(src, dst, portnum, payload, channel, request_id)
        self.record(p)
        self.send(mesh_pb2.FromRadio(packet=p))

    # --- Injection helpers for tests ---

    def inject_text(self, text: str, src=0x22222222, dst=BROADCAST, channel=0):
        self.send_packet(src, dst, portnums_pb2.TEXT_MESSAGE_APP, text.encode(), channel)

    def inject_telemetry(self, num: int, battery_level: int):
        metrics = telemetry_pb2.Telemetry(
            device_metrics=telemetry_pb2.DeviceMetrics(battery_level=battery_level)
        )
        self.send_packet(num, BROADCAST, portnums_pb2.TELEMETRY_APP, metrics.SerializeToString())

    def inject_nodeinfo(self, num: int, long_name: str):
        self.send_packet(
            num, BROADCAST, portnums_pb2.NODEINFO_APP, user(num, long_name).SerializeToString()
        )


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=4403)
    parser.add_argument(
        "--inject", type=int, default=0, help="inject a text message every N seconds"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    radio = FakeRadio(args.port)
    await radio.start()
    n = 0
    while True:
        await asyncio.sleep(args.inject or 3600)
        if args.inject:
            n += 1
            radio.inject_text(f"fake message {n}")
            log.info("injected fake message %d", n)


if __name__ == "__main__":
    asyncio.run(main())
