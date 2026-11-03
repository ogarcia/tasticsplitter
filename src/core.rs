use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use meshtastic::Message;
use meshtastic::protobufs::{self as pb, from_radio, mesh_packet, to_radio};
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, Instant};

use crate::backend::{self, RadioIo};
use crate::frontend::tcp::{ClientEvent, ClientId, TcpFrontend};
use crate::store::{NewMessage, Retention, Store};

/// want_config nonces with a meaning of their own: only the config (no
/// other nodes), or only the node database.
const NONCE_ONLY_CONFIG: u32 = 69420;
const NONCE_ONLY_NODES: u32 = 69421;

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
/// A want_config stream that takes longer than this is considered lost.
const SYNC_TIMEOUT: Duration = Duration::from_secs(60);
/// The official clients send one every five minutes; on serial the
/// firmware drops the API session when it hears nothing for a while.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(300);
/// How long the originator of a packet is remembered, for its queue
/// status, routing ACK and any response (traceroute, admin, ...).
const PENDING_TTL: Duration = Duration::from_secs(600);
/// Admin commands to the local node are applied after the radio queues
/// them; give it a moment before reading the config back.
const ADMIN_RESYNC_DELAY: Duration = Duration::from_secs(3);
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(24 * 3600);
const TICK: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub enum Backend {
    Serial { path: String, baud: u32 },
    Tcp { host: String, port: u16 },
}

#[derive(Debug, Clone)]
pub struct CoreConfig {
    pub backend: Backend,
    pub frontend_host: String,
    pub frontend_ports: Vec<u16>,
    pub data_dir: PathBuf,
    pub retention: Retention,
    /// Re-read the radio's config and node database this often (0 = only
    /// when something changes it).
    pub resync_minutes: u32,
}

/// A client connection. It only gets traffic once it has finished its
/// own want_config, like a client of the radio itself.
struct Client {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    port: u16,
    ready: bool,
}

/// A packet a client sent, keyed by its packet id.
struct Pending {
    client_id: ClientId,
    port: u16,
    sent_at: Instant,
    /// Text packet to echo to the other clients once the radio queues it.
    echo: Option<pb::MeshPacket>,
    /// Admin command to the local node: its config must be read again.
    local_admin: bool,
}

/// One entry of the want_config stream, in the order the radio sent it.
#[derive(Clone)]
enum Item {
    /// A node; its current FromRadio lives in `ConfigCache::nodes`.
    Node(u32),
    Other(Vec<u8>),
}

/// The radio's answer to want_config, served to every client.
#[derive(Clone, Default)]
struct ConfigCache {
    my_node_num: u32,
    items: Vec<Item>,
    nodes: HashMap<u32, Vec<u8>>,
}

impl ConfigCache {
    fn push(&mut self, frame: Vec<u8>, msg: &pb::FromRadio) {
        match &msg.payload_variant {
            Some(from_radio::PayloadVariant::MyInfo(info)) => {
                self.my_node_num = info.my_node_num;
                self.items.push(Item::Other(frame));
            }
            Some(from_radio::PayloadVariant::NodeInfo(node)) => {
                if self.nodes.insert(node.num, frame).is_none() {
                    self.items.push(Item::Node(node.num));
                }
            }
            _ => self.items.push(Item::Other(frame)),
        }
    }

    /// The stream for a client's want_config, ending with its nonce. Like
    /// the firmware, ONLY_CONFIG leaves out every node but our own and
    /// ONLY_NODES sends nothing but nodes (not even my_info).
    fn stream_for(&self, nonce: u32) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(self.items.len() + 1);
        for item in &self.items {
            match item {
                Item::Node(num) => {
                    let own = *num == self.my_node_num;
                    if nonce == NONCE_ONLY_CONFIG && !own {
                        continue;
                    }
                    if let Some(frame) = self.nodes.get(num) {
                        out.push(frame.clone());
                    }
                }
                Item::Other(frame) => {
                    if nonce != NONCE_ONLY_NODES {
                        out.push(frame.clone());
                    }
                }
            }
        }
        out.push(encode_from_radio(
            from_radio::PayloadVariant::ConfigCompleteId(nonce),
        ));
        out
    }

    /// Fold what a packet tells about its sender into the node database,
    /// as the firmware does, so clients connecting later don't get stale
    /// last-heard times, positions or names.
    fn update_from_packet(&mut self, p: &pb::MeshPacket) {
        if p.from == 0 {
            return;
        }
        let mut node = self
            .nodes
            .get(&p.from)
            .and_then(|frame| pb::FromRadio::decode(frame.as_slice()).ok())
            .and_then(|msg| match msg.payload_variant {
                Some(from_radio::PayloadVariant::NodeInfo(node)) => Some(node),
                _ => None,
            })
            .unwrap_or_else(|| pb::NodeInfo {
                num: p.from,
                ..Default::default()
            });

        if p.from != self.my_node_num {
            if p.rx_time != 0 {
                node.last_heard = p.rx_time;
            }
            node.snr = p.rx_snr;
            node.via_mqtt = p.via_mqtt;
            if p.hop_start != 0 && p.hop_start >= p.hop_limit {
                node.hops_away = Some(p.hop_start - p.hop_limit);
            }
        }

        if let Some(mesh_packet::PayloadVariant::Decoded(data)) = &p.payload_variant {
            let payload = data.payload.as_slice();
            match pb::PortNum::try_from(data.portnum) {
                Ok(pb::PortNum::NodeinfoApp) => {
                    if let Ok(user) = pb::User::decode(payload) {
                        node.user = Some(user);
                    }
                }
                Ok(pb::PortNum::PositionApp) => {
                    if let Ok(position) = pb::Position::decode(payload)
                        && (position.latitude_i.is_some() || position.longitude_i.is_some())
                    {
                        node.position = Some(position);
                    }
                }
                Ok(pb::PortNum::TelemetryApp) => {
                    if let Ok(pb::Telemetry {
                        variant: Some(pb::telemetry::Variant::DeviceMetrics(metrics)),
                        ..
                    }) = pb::Telemetry::decode(payload)
                    {
                        node.device_metrics = Some(metrics);
                    }
                }
                _ => {}
            }
        }

        let num = node.num;
        let frame = encode_from_radio(from_radio::PayloadVariant::NodeInfo(node));
        if self.nodes.insert(num, frame).is_none() {
            // New nodes go after the last known one, like in the radio.
            let at = self
                .items
                .iter()
                .rposition(|item| matches!(item, Item::Node(_)))
                .map_or(self.items.len(), |i| i + 1);
            self.items.insert(at, Item::Node(num));
        }
    }
}

/// A want_config of our own in progress.
struct Sync {
    nonce: u32,
    started_at: Instant,
    cache: ConfigCache,
}

enum SessionEnd {
    RadioLost,
    Shutdown,
}

pub struct Core {
    config: CoreConfig,
    store: Arc<Store>,
    shutdown_rx: watch::Receiver<bool>,
    radio_up_tx: watch::Sender<bool>,
    event_rx: mpsc::UnboundedReceiver<ClientEvent>,
    clients: HashMap<ClientId, Client>,
    pending: HashMap<u32, Pending>,
    cache: Option<ConfigCache>,
    sync: Option<Sync>,
    resync_at: Option<Instant>,
}

impl Core {
    pub async fn new(config: CoreConfig, shutdown_rx: watch::Receiver<bool>) -> Result<Self> {
        let store = Arc::new(Store::open(config.data_dir.join("state.db")).await?);

        let (radio_up_tx, radio_up_rx) = watch::channel(false);
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let bind_addrs = config
            .frontend_ports
            .iter()
            .map(|port| format!("{}:{port}", config.frontend_host))
            .collect();
        let mut frontend = TcpFrontend::new(bind_addrs, event_tx, radio_up_rx, shutdown_rx.clone());
        tokio::spawn(async move {
            if let Err(e) = frontend.run().await {
                tracing::error!(error = %e, "TCP frontend failed");
            }
        });

        tokio::spawn(maintenance_loop(
            store.clone(),
            config.retention,
            shutdown_rx.clone(),
        ));

        Ok(Self {
            config,
            store,
            shutdown_rx,
            radio_up_tx,
            event_rx,
            clients: HashMap::new(),
            pending: HashMap::new(),
            cache: None,
            sync: None,
            resync_at: None,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            let io = match self.connect_radio().await {
                Ok(io) => io,
                Err(e) => {
                    tracing::warn!(error = %e, "radio connection failed, retrying");
                    if self.wait(RECONNECT_DELAY).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            tracing::info!("radio connected");

            let end = self.session(io).await;

            // The frontend closes every client when the radio goes down;
            // forget what was in flight with this radio.
            self.radio_up_tx.send_replace(false);
            self.pending.clear();
            self.sync = None;
            self.resync_at = None;

            match end {
                SessionEnd::Shutdown => return Ok(()),
                SessionEnd::RadioLost => {
                    tracing::warn!("radio lost, reconnecting");
                    if self.wait(RECONNECT_DELAY).await {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn connect_radio(&self) -> Result<RadioIo> {
        match &self.config.backend {
            Backend::Serial { path, baud } => backend::serial::connect(path, *baud).await,
            Backend::Tcp { host, port } => backend::tcp::connect(host, *port).await,
        }
    }

    /// Sleep while still tracking client events. Returns true on shutdown.
    async fn wait(&mut self, delay: Duration) -> bool {
        let deadline = tokio::time::sleep(delay);
        tokio::pin!(deadline);
        let mut shutdown_rx = self.shutdown_rx.clone();
        loop {
            tokio::select! {
                _ = &mut deadline => return false,
                _ = shutdown_rx.wait_for(|stop| *stop) => return true,
                Some(event) = self.event_rx.recv() => {
                    // No radio: the frontend is closed, only departures
                    // can arrive here.
                    if let ClientEvent::Disconnected { client_id } = event {
                        self.clients.remove(&client_id);
                    }
                }
            }
        }
    }

    async fn session(&mut self, mut io: RadioIo) -> SessionEnd {
        self.start_sync(&io);

        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut tick = tokio::time::interval(TICK);
        let mut next_heartbeat = Instant::now() + HEARTBEAT_INTERVAL;
        let resync_every = Duration::from_secs(u64::from(self.config.resync_minutes) * 60);
        let mut next_periodic = (!resync_every.is_zero()).then(|| Instant::now() + resync_every);

        loop {
            tokio::select! {
                frame = io.recv_rx.recv() => {
                    let Some(frame) = frame else {
                        return SessionEnd::RadioLost;
                    };
                    self.handle_radio(&io, frame).await;
                }
                Some(event) = self.event_rx.recv() => {
                    self.handle_client_event(&io, event).await;
                }
                _ = tick.tick() => {
                    let now = Instant::now();
                    self.pending.retain(|_, p| now.duration_since(p.sent_at) < PENDING_TTL);

                    if let Some(sync) = &self.sync
                        && now.duration_since(sync.started_at) > SYNC_TIMEOUT
                    {
                        if self.cache.is_none() || !*self.radio_up_tx.borrow() {
                            tracing::error!("radio did not send its configuration");
                            return SessionEnd::RadioLost;
                        }
                        tracing::warn!("config resync timed out, keeping the previous one");
                        self.sync = None;
                    }

                    if now >= next_heartbeat {
                        send_to_radio(&io, to_radio::PayloadVariant::Heartbeat(pb::Heartbeat::default()));
                        next_heartbeat = now + HEARTBEAT_INTERVAL;
                    }

                    if let Some(at) = next_periodic
                        && now >= at
                    {
                        self.resync_at.get_or_insert(now);
                        next_periodic = Some(now + resync_every);
                    }

                    if let Some(at) = self.resync_at
                        && now >= at
                        && self.sync.is_none()
                    {
                        self.resync_at = None;
                        self.start_sync(&io);
                    }
                }
                _ = shutdown_rx.wait_for(|stop| *stop) => {
                    tracing::info!("shutdown signal received, stopping");
                    return SessionEnd::Shutdown;
                }
            }
        }
    }

    /// Ask the radio for its configuration and node database. Clients keep
    /// being served the previous copy until the new one is complete.
    fn start_sync(&mut self, io: &RadioIo) {
        let nonce = loop {
            let n: u32 = rand::random();
            if n != 0 && n != NONCE_ONLY_CONFIG && n != NONCE_ONLY_NODES {
                break n;
            }
        };
        tracing::info!(nonce, "requesting configuration from radio");
        self.sync = Some(Sync {
            nonce,
            started_at: Instant::now(),
            cache: ConfigCache::default(),
        });
        send_to_radio(io, to_radio::PayloadVariant::WantConfigId(nonce));
    }

    // --- Radio → clients ---

    async fn handle_radio(&mut self, io: &RadioIo, frame: Vec<u8>) {
        let msg = match pb::FromRadio::decode(frame.as_slice()) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(error = %e, len = frame.len(), "undecodable FromRadio, relaying as is");
                self.broadcast(None, &frame);
                return;
            }
        };

        use from_radio::PayloadVariant as V;
        match &msg.payload_variant {
            Some(V::ConfigCompleteId(nonce)) => self.finish_sync(*nonce),
            // Parts of a want_config answer: only ours, clients get the
            // cached copy. A newer firmware's unknown variant (None) during
            // our sync is most likely config too.
            Some(
                V::MyInfo(_)
                | V::NodeInfo(_)
                | V::Config(_)
                | V::ModuleConfig(_)
                | V::Channel(_)
                | V::Metadata(_)
                | V::FileInfo(_)
                | V::DeviceuiConfig(_),
            ) => match &mut self.sync {
                Some(sync) => sync.cache.push(frame, &msg),
                None => tracing::debug!("config item outside a sync, dropped"),
            },
            None if self.sync.is_some() => {
                if let Some(sync) = &mut self.sync {
                    sync.cache.push(frame, &msg);
                }
            }
            Some(V::Packet(p)) => self.handle_radio_packet(frame.clone(), p).await,
            Some(V::QueueStatus(q)) => self.handle_queue_status(&frame, q).await,
            Some(V::Rebooted(_)) => {
                tracing::warn!("radio rebooted, reading its configuration again");
                self.broadcast(None, &frame);
                self.start_sync(io);
            }
            _ => self.broadcast(None, &frame),
        }
    }

    fn finish_sync(&mut self, nonce: u32) {
        let Some(sync) = self.sync.take_if(|s| s.nonce == nonce) else {
            tracing::debug!(nonce, "stale config_complete_id, ignored");
            return;
        };
        tracing::info!(
            my_node = format_args!("!{:08x}", sync.cache.my_node_num),
            nodes = sync.cache.nodes.len(),
            items = sync.cache.items.len(),
            "radio configuration cached"
        );
        self.cache = Some(sync.cache);
        self.radio_up_tx.send_replace(true);
    }

    async fn handle_radio_packet(&mut self, frame: Vec<u8>, p: &pb::MeshPacket) {
        if let Some(cache) = &mut self.cache {
            cache.update_from_packet(p);
        }

        let data = match &p.payload_variant {
            Some(mesh_packet::PayloadVariant::Decoded(data)) => Some(data),
            _ => None,
        };

        // ACKs and responses belong to whoever sent the request.
        if let Some(data) = data
            && data.request_id != 0
            && let Some(pending) = self.pending.get(&data.request_id)
        {
            let (client_id, local_admin) = (pending.client_id, pending.local_admin);
            if local_admin {
                self.schedule_resync(ADMIN_RESYNC_DELAY);
            }
            tracing::debug!(
                client = %client_id,
                request_id = format_args!("{:#010x}", data.request_id),
                portnum = data.portnum,
                "reply routed to its requester"
            );
            self.send_to(&client_id, frame);
            return;
        }

        if let Some(data) = data
            && is_text(data.portnum)
        {
            let text = String::from_utf8_lossy(&data.payload);
            self.store_and_broadcast(None, p, &text, frame).await;
        } else {
            self.broadcast(None, &frame);
        }
    }

    async fn handle_queue_status(&mut self, frame: &[u8], q: &pb::QueueStatus) {
        let Some(pending) = self.pending.get_mut(&q.mesh_packet_id) else {
            self.broadcast(None, frame);
            return;
        };
        let client_id = pending.client_id;
        let port = pending.port;
        let local_admin = pending.local_admin;
        let echo = pending.echo.take();
        self.send_to(&client_id, frame.to_vec());

        // res is a meshtastic_Routing_Error: 0 means queued.
        if q.res != 0 {
            tracing::debug!(id = q.mesh_packet_id, res = q.res, "radio refused packet");
            return;
        }
        if local_admin {
            self.schedule_resync(ADMIN_RESYNC_DELAY);
        }
        if let Some(mut p) = echo {
            // What the other clients would see had it come over the air.
            let my_node_num = self.cache.as_ref().map_or(0, |c| c.my_node_num);
            if p.from == 0 {
                p.from = my_node_num;
            }
            p.rx_time = chrono::Utc::now().timestamp() as u32;
            let text = match &p.payload_variant {
                Some(mesh_packet::PayloadVariant::Decoded(data)) => {
                    String::from_utf8_lossy(&data.payload).into_owned()
                }
                _ => String::new(),
            };
            let echo_frame = encode_from_radio(from_radio::PayloadVariant::Packet(p.clone()));
            tracing::debug!(id = p.id, "echoing text to the other clients");
            self.store_and_broadcast(Some((client_id, port)), &p, &text, echo_frame)
                .await;
        }
    }

    /// Store a text message and send it to every ready client but its
    /// sender; every client that saw it moves its cursor past it.
    async fn store_and_broadcast(
        &mut self,
        origin: Option<(ClientId, u16)>,
        p: &pb::MeshPacket,
        text: &str,
        frame: Vec<u8>,
    ) {
        let stored = self
            .store
            .insert_message(&NewMessage {
                from_node: p.from,
                to_node: p.to,
                channel: p.channel,
                packet_id: p.id,
                text,
                frame: &frame,
                origin_port: origin.map(|(_, port)| port),
            })
            .await;

        self.broadcast(origin.map(|(id, _)| id), &frame);

        match stored {
            Ok(id) => {
                let mut ports: Vec<u16> = self
                    .clients
                    .values()
                    .filter(|c| c.ready)
                    .map(|c| c.port)
                    .collect();
                ports.sort_unstable();
                ports.dedup();
                for port in ports {
                    if let Err(e) = self.store.advance_client_cursor(port, id).await {
                        tracing::warn!(error = %e, port, "failed to advance client cursor");
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to store message"),
        }
    }

    // --- Clients → radio ---

    async fn handle_client_event(&mut self, io: &RadioIo, event: ClientEvent) {
        match event {
            ClientEvent::Connected {
                client_id,
                local_port,
                tx,
            } => {
                self.clients.insert(
                    client_id,
                    Client {
                        tx,
                        port: local_port,
                        ready: false,
                    },
                );
            }
            ClientEvent::Disconnected { client_id } => {
                self.clients.remove(&client_id);
            }
            ClientEvent::Frame { client_id, payload } => {
                self.handle_client_frame(io, client_id, payload).await;
            }
        }
    }

    async fn handle_client_frame(&mut self, io: &RadioIo, client_id: ClientId, payload: Vec<u8>) {
        let msg = match pb::ToRadio::decode(payload.as_slice()) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(client = %client_id, error = %e, "undecodable ToRadio, relaying as is");
                let _ = io.send_tx.send(payload);
                return;
            }
        };

        use to_radio::PayloadVariant as V;
        match msg.payload_variant {
            Some(V::WantConfigId(nonce)) => self.serve_config(client_id, nonce).await,
            Some(V::Packet(p)) => self.handle_client_packet(io, client_id, payload, p),
            // The proxy keeps the radio session alive on its own.
            Some(V::Heartbeat(_)) => {}
            Some(V::Disconnect(_)) => {
                tracing::info!(client = %client_id, "client said goodbye");
                // Dropping its sender closes the connection.
                self.clients.remove(&client_id);
            }
            _ => {
                let _ = io.send_tx.send(payload);
            }
        }
    }

    /// Answer a client's want_config from the cache, then hand it the
    /// messages it missed while away.
    async fn serve_config(&mut self, client_id: ClientId, nonce: u32) {
        let Some(cache) = &self.cache else {
            return;
        };
        let stream = cache.stream_for(nonce);
        let Some(client) = self.clients.get_mut(&client_id) else {
            return;
        };
        tracing::info!(client = %client_id, nonce, frames = stream.len(), "serving cached configuration");
        for frame in stream {
            let _ = client.tx.send(frame);
        }
        if nonce == NONCE_ONLY_CONFIG {
            // The node database comes next, in a second want_config.
            return;
        }
        client.ready = true;
        let port = client.port;
        self.replay_missed(client_id, port).await;
    }

    async fn replay_missed(&mut self, client_id: ClientId, port: u16) {
        let result: Result<()> = async {
            let head = self.store.message_head().await?;
            let Some(cursor) = self.store.client_cursor(port).await? else {
                // Never seen: starts from now.
                tracing::info!(client = %client_id, port, "new client identity");
                return self.store.advance_client_cursor(port, head).await;
            };
            let missed = self.store.missed_frames(cursor, port).await?;
            if !missed.is_empty() {
                tracing::info!(client = %client_id, port, count = missed.len(), "replaying missed messages");
            }
            for (_, frame) in missed {
                self.send_to(&client_id, frame);
            }
            self.store.advance_client_cursor(port, head).await
        }
        .await;
        if let Err(e) = result {
            tracing::warn!(client = %client_id, error = %e, "failed to replay missed messages");
        }
    }

    fn handle_client_packet(
        &mut self,
        io: &RadioIo,
        client_id: ClientId,
        mut payload: Vec<u8>,
        mut p: pb::MeshPacket,
    ) {
        let Some(port) = self.clients.get(&client_id).map(|c| c.port) else {
            return;
        };

        // Responses are routed by packet id, so every packet needs one.
        if p.id == 0 {
            p.id = loop {
                let id: u32 = rand::random();
                if id != 0 && !self.pending.contains_key(&id) {
                    break id;
                }
            };
            payload = pb::ToRadio {
                payload_variant: Some(to_radio::PayloadVariant::Packet(p.clone())),
            }
            .encode_to_vec();
        }

        let my_node_num = self.cache.as_ref().map_or(0, |c| c.my_node_num);
        let (echo, local_admin) = match &p.payload_variant {
            Some(mesh_packet::PayloadVariant::Decoded(data)) => (
                is_text(data.portnum).then(|| p.clone()),
                data.portnum == pb::PortNum::AdminApp as i32 && (p.to == 0 || p.to == my_node_num),
            ),
            _ => (None, false),
        };

        if let Some(previous) = self.pending.insert(
            p.id,
            Pending {
                client_id,
                port,
                sent_at: Instant::now(),
                echo,
                local_admin,
            },
        ) && previous.client_id != client_id
        {
            tracing::warn!(id = p.id, "packet id reused by another client");
        }

        tracing::debug!(
            client = %client_id,
            id = format_args!("{:#010x}", p.id),
            to = format_args!("!{:08x}", p.to),
            portnum = portnum_of(&p),
            "packet to radio"
        );
        let _ = io.send_tx.send(payload);
    }

    // --- Helpers ---

    fn schedule_resync(&mut self, delay: Duration) {
        let at = Instant::now() + delay;
        if self.resync_at.is_none_or(|current| at < current) {
            self.resync_at = Some(at);
        }
    }

    fn send_to(&self, client_id: &ClientId, frame: Vec<u8>) {
        if let Some(client) = self.clients.get(client_id) {
            let _ = client.tx.send(frame);
        }
    }

    /// Send to every client that finished its want_config, except one.
    fn broadcast(&self, except: Option<ClientId>, frame: &[u8]) {
        for (id, client) in &self.clients {
            if client.ready && Some(*id) != except {
                let _ = client.tx.send(frame.to_vec());
            }
        }
    }
}

fn portnum_of(p: &pb::MeshPacket) -> i32 {
    match &p.payload_variant {
        Some(mesh_packet::PayloadVariant::Decoded(data)) => data.portnum,
        _ => -1,
    }
}

fn is_text(portnum: i32) -> bool {
    portnum == pb::PortNum::TextMessageApp as i32
}

fn encode_from_radio(variant: from_radio::PayloadVariant) -> Vec<u8> {
    pb::FromRadio {
        id: 0,
        payload_variant: Some(variant),
    }
    .encode_to_vec()
}

fn send_to_radio(io: &RadioIo, variant: to_radio::PayloadVariant) {
    let msg = pb::ToRadio {
        payload_variant: Some(variant),
    };
    let _ = io.send_tx.send(msg.encode_to_vec());
}

/// Purge the database at startup and once a day.
async fn maintenance_loop(
    store: Arc<Store>,
    retention: Retention,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        match store.maintain(&retention).await {
            Ok(stats) => tracing::info!(
                messages = stats.messages,
                clients = stats.clients,
                "database maintenance done"
            ),
            Err(e) => tracing::warn!(error = %e, "database maintenance failed"),
        }
        tokio::select! {
            _ = tokio::time::sleep(MAINTENANCE_INTERVAL) => {}
            _ = shutdown_rx.wait_for(|stop| *stop) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_frame(num: u32, name: &str) -> (Vec<u8>, pb::FromRadio) {
        let msg = pb::FromRadio {
            id: 0,
            payload_variant: Some(from_radio::PayloadVariant::NodeInfo(pb::NodeInfo {
                num,
                user: Some(pb::User {
                    long_name: name.into(),
                    ..Default::default()
                }),
                ..Default::default()
            })),
        };
        (msg.encode_to_vec(), msg)
    }

    fn cache() -> ConfigCache {
        let mut cache = ConfigCache::default();
        let my_info = pb::FromRadio {
            id: 0,
            payload_variant: Some(from_radio::PayloadVariant::MyInfo(pb::MyNodeInfo {
                my_node_num: 1,
                ..Default::default()
            })),
        };
        cache.push(my_info.encode_to_vec(), &my_info);
        let (frame, msg) = node_frame(1, "me");
        cache.push(frame, &msg);
        let channel = pb::FromRadio {
            id: 0,
            payload_variant: Some(from_radio::PayloadVariant::Channel(pb::Channel::default())),
        };
        cache.push(channel.encode_to_vec(), &channel);
        let (frame, msg) = node_frame(2, "other");
        cache.push(frame, &msg);
        cache
    }

    fn variants(stream: &[Vec<u8>]) -> Vec<String> {
        stream
            .iter()
            .map(
                |f| match pb::FromRadio::decode(f.as_slice()).unwrap().payload_variant {
                    Some(from_radio::PayloadVariant::MyInfo(_)) => "my_info".into(),
                    Some(from_radio::PayloadVariant::NodeInfo(n)) => format!("node{}", n.num),
                    Some(from_radio::PayloadVariant::Channel(_)) => "channel".into(),
                    Some(from_radio::PayloadVariant::ConfigCompleteId(n)) => format!("complete{n}"),
                    _ => "other".into(),
                },
            )
            .collect()
    }

    #[test]
    fn stream_keeps_radio_order_and_client_nonce() {
        assert_eq!(
            variants(&cache().stream_for(7)),
            ["my_info", "node1", "channel", "node2", "complete7"]
        );
    }

    #[test]
    fn special_nonces_split_config_and_nodes() {
        assert_eq!(
            variants(&cache().stream_for(NONCE_ONLY_CONFIG)),
            ["my_info", "node1", "channel", "complete69420"]
        );
        assert_eq!(
            variants(&cache().stream_for(NONCE_ONLY_NODES)),
            ["node1", "node2", "complete69421"]
        );
    }

    #[test]
    fn packets_update_and_add_nodes() {
        let mut cache = cache();
        let user = pb::User {
            long_name: "renamed".into(),
            ..Default::default()
        };
        cache.update_from_packet(&pb::MeshPacket {
            from: 2,
            rx_time: 1000,
            hop_start: 3,
            hop_limit: 1,
            payload_variant: Some(mesh_packet::PayloadVariant::Decoded(pb::Data {
                portnum: pb::PortNum::NodeinfoApp as i32,
                payload: user.encode_to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        });
        cache.update_from_packet(&pb::MeshPacket {
            from: 3,
            rx_time: 2000,
            ..Default::default()
        });

        let nodes: Vec<pb::NodeInfo> = cache
            .stream_for(7)
            .iter()
            .filter_map(
                |f| match pb::FromRadio::decode(f.as_slice()).unwrap().payload_variant {
                    Some(from_radio::PayloadVariant::NodeInfo(n)) => Some(n),
                    _ => None,
                },
            )
            .collect();
        assert_eq!(nodes.iter().map(|n| n.num).collect::<Vec<_>>(), [1, 2, 3]);
        assert_eq!(nodes[1].user.as_ref().unwrap().long_name, "renamed");
        assert_eq!(nodes[1].last_heard, 1000);
        assert_eq!(nodes[1].hops_away, Some(2));
        assert_eq!(nodes[2].last_heard, 2000);
    }
}
