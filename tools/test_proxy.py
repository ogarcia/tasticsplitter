#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["meshtastic"]
# ///
"""Integration tests: tasticsplitter between a fake radio and test clients.

Builds nothing: run `cargo build` first, it uses target/debug/tasticsplitter.
"""
import asyncio
import logging
import os
import random
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(__file__))

from fake_radio import BROADCAST, MY_NODE, FakeRadio, FrameReader, frame  # noqa: E402
from meshtastic.protobuf import admin_pb2, mesh_pb2, portnums_pb2, telemetry_pb2  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("test")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BINARY = os.path.join(ROOT, "target", "debug", "tasticsplitter")
TIMEOUT = 5.0


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Client:
    def __init__(self, reader, writer):
        self.frames = FrameReader(reader)
        self.w = writer

    @classmethod
    async def connect(cls, port: int) -> "Client":
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        return cls(reader, writer)

    def send(self, msg: mesh_pb2.ToRadio):
        self.w.write(frame(msg.SerializeToString()))

    async def recv(self, timeout=TIMEOUT) -> mesh_pb2.FromRadio:
        msg = mesh_pb2.FromRadio()
        msg.ParseFromString(await asyncio.wait_for(self.frames.read(), timeout))
        return msg

    async def handshake(self, nonce=None) -> list[mesh_pb2.FromRadio]:
        """want_config and everything up to its config_complete_id."""
        nonce = nonce or random.randint(1, 60000)
        self.send(mesh_pb2.ToRadio(want_config_id=nonce))
        got = []
        while True:
            msg = await self.recv()
            got.append(msg)
            if msg.WhichOneof("payload_variant") == "config_complete_id":
                assert msg.config_complete_id == nonce, (msg.config_complete_id, nonce)
                return got

    async def expect(self, predicate, timeout=TIMEOUT) -> mesh_pb2.FromRadio:
        """First message matching predicate; the rest are skipped."""
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            left = deadline - asyncio.get_running_loop().time()
            if left <= 0:
                raise TimeoutError("expected message never arrived")
            msg = await self.recv(left)
            if predicate(msg):
                return msg

    async def nothing(self, predicate=lambda m: True, wait=1.0):
        """Assert no matching message arrives within `wait` seconds."""
        try:
            msg = await self.expect(predicate, wait)
        except (TimeoutError, asyncio.TimeoutError):
            return
        raise AssertionError(f"unexpected message: {msg}")

    def send_text(self, text: str, packet_id: int, channel=0, to=BROADCAST):
        self.send(
            mesh_pb2.ToRadio(
                packet=mesh_pb2.MeshPacket(
                    to=to,
                    id=packet_id,
                    channel=channel,
                    want_ack=True,
                    decoded=mesh_pb2.Data(
                        portnum=portnums_pb2.TEXT_MESSAGE_APP, payload=text.encode()
                    ),
                )
            )
        )

    def close(self):
        self.w.close()


def is_text(text: str):
    def check(m):
        return (
            m.WhichOneof("payload_variant") == "packet"
            and m.packet.decoded.portnum == portnums_pb2.TEXT_MESSAGE_APP
            and m.packet.decoded.payload == text.encode()
        )

    return check


def is_queue_status(packet_id: int):
    return lambda m: (
        m.WhichOneof("payload_variant") == "queueStatus"
        and m.queueStatus.mesh_packet_id == packet_id
    )


def is_response_to(packet_id: int):
    return lambda m: (
        m.WhichOneof("payload_variant") == "packet"
        and m.packet.decoded.request_id == packet_id
    )


def is_history(m) -> bool:
    return m.WhichOneof("payload_variant") == "packet" and m.packet.decoded.portnum in (
        portnums_pb2.POSITION_APP,
        portnums_pb2.TELEMETRY_APP,
    )


def describe(m) -> str:
    """'position:<node>' or 'telemetry:<kind>:<node>[:<battery>]' for a packet."""
    p = m.packet
    sender = f"{getattr(p, 'from'):08x}"
    if p.decoded.portnum == portnums_pb2.POSITION_APP:
        return f"position:{sender}"
    t = telemetry_pb2.Telemetry()
    t.ParseFromString(p.decoded.payload)
    kind = t.WhichOneof("variant")
    extra = f":{t.device_metrics.battery_level}" if kind == "device_metrics" else ""
    return f"telemetry:{kind}:{sender}{extra}"


async def history_after_handshake(c: Client, nonce=None) -> list[str]:
    """Handshake, then the packets that follow it right away."""
    await c.handshake(nonce)
    got = []
    try:
        while True:
            m = await c.recv(0.5)
            if is_history(m):
                got.append(describe(m))
    except (TimeoutError, asyncio.TimeoutError):
        return got


def node_names(stream) -> dict[int, str]:
    return {
        m.node_info.num: m.node_info.user.long_name
        for m in stream
        if m.WhichOneof("payload_variant") == "node_info"
    }


class Env:
    """A fake radio plus a proxy with two frontend ports."""

    async def __aenter__(self):
        self.radio = FakeRadio(free_port())
        await self.radio.start()
        self.ports = [free_port(), free_port()]
        self.data_dir = tempfile.mkdtemp(prefix="tasticsplitter-test-")
        self.proc = subprocess.Popen(
            [
                BINARY,
                "--tcp-backend-host", "127.0.0.1",
                "--tcp-backend-port", str(self.radio.port),
                "--tcp-frontend-host", "127.0.0.1",
                "--tcp-frontend-port", ",".join(map(str, self.ports)),
                "--data-dir", self.data_dir,
                "--log-level", "debug",
            ],
            stdout=subprocess.DEVNULL,
            stderr=open(os.path.join(self.data_dir, "proxy.log"), "w"),
        )
        await self.wait_listening(self.ports[0])
        return self

    async def __aexit__(self, *exc):
        self.proc.terminate()
        self.proc.wait(timeout=5)
        await self.radio.stop()
        if exc[0]:
            log.error("proxy log: %s", os.path.join(self.data_dir, "proxy.log"))
        else:
            shutil.rmtree(self.data_dir, ignore_errors=True)

    async def wait_listening(self, port, timeout=10.0):
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            try:
                _, w = await asyncio.open_connection("127.0.0.1", port)
                w.close()
                return
            except OSError:
                await asyncio.sleep(0.1)
        raise TimeoutError(f"proxy not listening on {port}")

    async def client(self, which=0, handshake=True) -> Client:
        c = await Client.connect(self.ports[which])
        if handshake:
            await c.handshake()
        return c


# --- Tests ---

async def test_config_served_from_cache(env: Env):
    a = await env.client(handshake=False)
    b = await env.client(1, handshake=False)
    stream = await a.handshake(1234)
    await b.handshake(4321)
    kinds = [m.WhichOneof("payload_variant") for m in stream]
    assert kinds[0] == "my_info", kinds
    assert stream[0].my_info.my_node_num == MY_NODE
    for kind in ("metadata", "channel", "config", "moduleConfig"):
        assert kind in kinds, (kind, kinds)
    assert set(node_names(stream)) == {MY_NODE, 0x22222222, 0x33333333}
    assert len(env.radio.want_configs) == 1, env.radio.want_configs
    a.close()
    b.close()


async def test_special_nonces(env: Env):
    c = await env.client(handshake=False)
    only_config = await c.handshake(69420)
    assert set(node_names(only_config)) == {MY_NODE}
    assert "config" in [m.WhichOneof("payload_variant") for m in only_config]
    only_nodes = await c.handshake(69421)
    kinds = {m.WhichOneof("payload_variant") for m in only_nodes}
    assert kinds == {"node_info", "config_complete_id"}, kinds
    assert len(node_names(only_nodes)) == 3
    c.close()


async def test_incoming_text_fans_out(env: Env):
    a = await env.client()
    b = await env.client(1)
    env.radio.inject_text("hello mesh")
    await a.expect(is_text("hello mesh"))
    await b.expect(is_text("hello mesh"))
    a.close()
    b.close()


async def test_no_traffic_before_handshake(env: Env):
    a = await env.client(handshake=False)
    env.radio.inject_text("too early")
    await a.nothing()
    a.close()


async def test_send_routes_replies_and_echoes(env: Env):
    a = await env.client()
    b = await env.client(1)
    a.send_text("from a", 0x1001)
    await a.expect(is_queue_status(0x1001))
    await a.expect(is_response_to(0x1001))
    echo = await b.expect(is_text("from a"))
    assert getattr(echo.packet, "from") == MY_NODE
    assert echo.packet.id == 0x1001
    await b.nothing(lambda m: is_queue_status(0x1001)(m) or is_response_to(0x1001)(m))
    await a.nothing(is_text("from a"))
    a.close()
    b.close()


async def test_refused_packet_not_echoed(env: Env):
    a = await env.client()
    b = await env.client(1)
    a.send_text("nowhere", 0x2002, channel=9)
    status = await a.expect(is_queue_status(0x2002))
    assert status.queueStatus.res != 0
    await b.nothing(is_text("nowhere"))
    a.close()
    b.close()


async def test_packet_without_id_gets_one(env: Env):
    a = await env.client()
    a.send_text("no id", 0)
    status = await a.expect(lambda m: m.WhichOneof("payload_variant") == "queueStatus")
    assert status.queueStatus.mesh_packet_id != 0
    sent = env.radio.received[-1].packet
    assert sent.id == status.queueStatus.mesh_packet_id
    a.close()


async def test_missed_messages_replayed(env: Env):
    a = await env.client()
    b = await env.client(1)
    b.close()
    await asyncio.sleep(0.3)
    env.radio.inject_text("while b was away")
    await a.expect(is_text("while b was away"))

    b = await env.client(1, handshake=False)
    await b.handshake()
    await b.expect(is_text("while b was away"))
    b.close()

    # Already delivered: not again.
    b = await env.client(1)
    await b.nothing(is_text("while b was away"))
    b.close()
    a.close()


async def test_own_messages_not_replayed(env: Env):
    a = await env.client()
    b = await env.client(1)
    b.send_text("b says", 0x3003)
    await a.expect(is_text("b says"))
    b.close()
    await asyncio.sleep(0.3)
    b = await env.client(1)
    await b.nothing(is_text("b says"))
    a.close()
    b.close()


async def test_new_node_reaches_cache(env: Env):
    a = await env.client()
    env.radio.inject_nodeinfo(0x44444444, "Carol")
    await a.expect(lambda m: getattr(m.packet, "from") == 0x44444444)
    b = await env.client(1, handshake=False)
    names = node_names(await b.handshake())
    assert names.get(0x44444444) == "Carol", names
    a.close()
    b.close()


async def test_local_admin_triggers_resync(env: Env):
    a = await env.client()
    admin = admin_pb2.AdminMessage(set_owner=mesh_pb2.User(long_name="Renamed Radio"))
    a.send(
        mesh_pb2.ToRadio(
            packet=mesh_pb2.MeshPacket(
                to=MY_NODE,
                id=0x4004,
                decoded=mesh_pb2.Data(
                    portnum=portnums_pb2.ADMIN_APP, payload=admin.SerializeToString()
                ),
            )
        )
    )
    await a.expect(is_queue_status(0x4004))
    for _ in range(50):
        if len(env.radio.want_configs) >= 2:
            break
        await asyncio.sleep(0.2)
    assert len(env.radio.want_configs) >= 2, env.radio.want_configs
    await asyncio.sleep(0.5)
    b = await env.client(1, handshake=False)
    names = node_names(await b.handshake())
    assert names[MY_NODE] == "Renamed Radio", names
    a.close()
    b.close()


async def test_admin_response_to_requester_only(env: Env):
    a = await env.client()
    b = await env.client(1)
    req = admin_pb2.AdminMessage(get_owner_request=True)
    a.send(
        mesh_pb2.ToRadio(
            packet=mesh_pb2.MeshPacket(
                to=MY_NODE,
                id=0x5005,
                decoded=mesh_pb2.Data(
                    portnum=portnums_pb2.ADMIN_APP,
                    payload=req.SerializeToString(),
                    want_response=True,
                ),
            )
        )
    )
    await a.expect(is_response_to(0x5005))
    await b.nothing(is_response_to(0x5005))
    # A query changes nothing: no need to read the config back.
    await asyncio.sleep(4)
    assert len(env.radio.want_configs) == 1, env.radio.want_configs
    a.close()
    b.close()


async def test_radio_loss_closes_clients(env: Env):
    a = await env.client()
    port = env.radio.port
    await env.radio.stop()
    try:
        await a.expect(lambda m: False, timeout=3)
        raise AssertionError("client still open")
    except asyncio.IncompleteReadError:
        pass
    await asyncio.sleep(0.5)
    try:
        await Client.connect(env.ports[0])
        raise AssertionError("frontend still listening without radio")
    except OSError:
        pass

    env.radio.port = port
    await env.radio.start()
    await env.wait_listening(env.ports[0], timeout=15)
    b = await env.client()
    env.radio.inject_text("back again")
    await b.expect(is_text("back again"))
    b.close()


async def test_history_after_handshake(env: Env):
    c = await env.client(handshake=False)
    # The radio's own replay, oldest first.
    assert await history_after_handshake(c) == [
        "position:22222222",
        "telemetry:environment_metrics:33333333",
        "telemetry:device_metrics:33333333:80",
    ]
    # Split handshakes get no history, like from the radio.
    assert await history_after_handshake(c, 69420) == []
    assert await history_after_handshake(c, 69421) == []
    c.close()


async def test_history_does_not_rewind_nodes(env: Env):
    c = await env.client(handshake=False)
    stream = await c.handshake()
    alice = next(m.node_info for m in stream if m.node_info.num == 0x22222222)
    # The radio says 10 minutes ago; its day-old position must not win.
    assert time.time() - alice.last_heard < 3600, alice.last_heard
    c.close()


async def test_history_follows_live_traffic(env: Env):
    a = await env.client()
    env.radio.inject_telemetry(0x33333333, 42)
    await a.expect(lambda m: is_history(m) and describe(m).endswith(":42"))
    b = await env.client(1, handshake=False)
    history = await history_after_handshake(b)
    assert history[-1] == "telemetry:device_metrics:33333333:42", history
    assert "telemetry:device_metrics:33333333:80" not in history, history
    assert "telemetry:environment_metrics:33333333" in history, history
    a.close()
    b.close()


async def test_resync_replay_not_rebroadcast(env: Env):
    a = await env.client(handshake=False)
    assert len(await history_after_handshake(a)) == 3
    admin = admin_pb2.AdminMessage(set_owner=mesh_pb2.User(long_name="Again"))
    a.send(
        mesh_pb2.ToRadio(
            packet=mesh_pb2.MeshPacket(
                to=MY_NODE,
                id=0x6006,
                decoded=mesh_pb2.Data(
                    portnum=portnums_pb2.ADMIN_APP, payload=admin.SerializeToString()
                ),
            )
        )
    )
    for _ in range(50):
        if len(env.radio.want_configs) >= 2:
            break
        await asyncio.sleep(0.2)
    assert len(env.radio.want_configs) >= 2, env.radio.want_configs
    await a.nothing(is_history, wait=1.5)
    env.radio.inject_text("still live")
    await a.expect(is_text("still live"))
    a.close()


async def test_reconnect_backs_off(env: Env):
    """Somebody using the node directly is kicked out less and less often."""
    times = env.radio.connect_times

    async def take_over():
        # The newest connection wins, so this kicks the proxy out.
        _, w = await asyncio.open_connection("127.0.0.1", env.radio.port)
        return w

    async def proxy_back(count, timeout):
        for _ in range(int(timeout / 0.1)):
            if len(times) >= count:
                return
            await asyncio.sleep(0.1)
        raise TimeoutError("proxy did not reconnect")

    delays = []
    for _ in range(2):
        await take_over()
        kicked = len(times)
        await proxy_back(kicked + 1, 20)
        delays.append(times[kicked] - times[kicked - 1])
    assert 4 <= delays[0] < 8, delays
    assert 9 <= delays[1] < 13, delays


TESTS = [
    test_config_served_from_cache,
    test_special_nonces,
    test_incoming_text_fans_out,
    test_no_traffic_before_handshake,
    test_send_routes_replies_and_echoes,
    test_refused_packet_not_echoed,
    test_packet_without_id_gets_one,
    test_missed_messages_replayed,
    test_own_messages_not_replayed,
    test_new_node_reaches_cache,
    test_local_admin_triggers_resync,
    test_admin_response_to_requester_only,
    test_radio_loss_closes_clients,
    test_history_after_handshake,
    test_history_does_not_rewind_nodes,
    test_history_follows_live_traffic,
    test_resync_replay_not_rebroadcast,
    test_reconnect_backs_off,
]


async def main() -> int:
    if not os.path.exists(BINARY):
        log.error("%s not found, run `cargo build` first", BINARY)
        return 1
    failed = []
    for test in TESTS:
        try:
            async with Env() as env:
                await test(env)
            log.info("PASS %s", test.__name__)
        except Exception as e:
            log.error("FAIL %s: %r", test.__name__, e)
            failed.append(test.__name__)
    log.info("%d/%d passed", len(TESTS) - len(failed), len(TESTS))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
