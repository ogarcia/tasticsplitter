use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::time::Duration;

use crate::protocol::frame::{self, FrameParser};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

const BIND_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct ClientId(u64);

impl ClientId {
    fn new() -> Self {
        Self(NEXT_CLIENT_ID.fetch_add(1, Ordering::SeqCst))
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What the frontend tells the core about its clients.
#[derive(Debug)]
pub enum ClientEvent {
    /// A client connected to `local_port`. The port is its identity
    /// across reconnections: the API has no app name to tell apps apart.
    /// Frames for it go through `tx`; dropping `tx` closes the connection.
    Connected {
        client_id: ClientId,
        local_port: u16,
        tx: mpsc::UnboundedSender<Vec<u8>>,
    },
    /// A ToRadio payload from the client.
    Frame {
        client_id: ClientId,
        payload: Vec<u8>,
    },
    Disconnected {
        client_id: ClientId,
    },
}

pub struct TcpFrontend {
    bind_addrs: Vec<String>,
    event_tx: mpsc::UnboundedSender<ClientEvent>,
    // Whether the radio is attached and its configuration cached. While it
    // is false the frontend behaves like an absent node: it does not listen
    // and every client connection is closed.
    radio_up_rx: watch::Receiver<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl TcpFrontend {
    pub fn new(
        bind_addrs: Vec<String>,
        event_tx: mpsc::UnboundedSender<ClientEvent>,
        radio_up_rx: watch::Receiver<bool>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            bind_addrs,
            event_tx,
            radio_up_rx,
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            // Stay closed until the radio is up.
            tokio::select! {
                result = self.radio_up_rx.wait_for(|up| *up) => {
                    if result.is_err() {
                        return Ok(());
                    }
                }
                _ = self.shutdown_rx.wait_for(|stop| *stop) => {
                    tracing::info!("TCP frontend shutting down");
                    return Ok(());
                }
            }

            let Some(listeners) = self.bind_all().await else {
                tokio::time::sleep(BIND_RETRY_DELAY).await;
                continue;
            };

            // Separate receiver: the select arm below borrows it mutably
            // while the accept arm clones self.radio_up_rx for each client.
            let mut down_rx = self.radio_up_rx.clone();
            loop {
                // Accept on whichever listener gets a connection first.
                let accept_any =
                    futures::future::select_all(listeners.iter().map(|l| Box::pin(l.accept())));
                tokio::select! {
                    (result, _, _) = accept_any => {
                        let (stream, addr) = match result {
                            Ok(conn) => conn,
                            Err(e) => {
                                tracing::warn!(error = %e, "TCP frontend accept failed");
                                continue;
                            }
                        };
                        tokio::spawn(handle_client(
                            stream,
                            addr,
                            self.event_tx.clone(),
                            self.radio_up_rx.clone(),
                        ));
                    }
                    _ = down_rx.wait_for(|up| !*up) => {
                        tracing::warn!("radio unavailable, TCP frontend closed");
                        break;
                    }
                    _ = self.shutdown_rx.wait_for(|stop| *stop) => {
                        tracing::info!("TCP frontend shutting down");
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Bind every configured address, or none: a partial frontend would
    /// silently leave the clients of the failed port without service.
    async fn bind_all(&self) -> Option<Vec<TcpListener>> {
        let mut listeners = Vec::with_capacity(self.bind_addrs.len());
        for addr in &self.bind_addrs {
            match TcpListener::bind(addr).await {
                Ok(l) => {
                    tracing::info!(addr = %addr, "TCP frontend listening");
                    listeners.push(l);
                }
                Err(e) => {
                    tracing::error!(addr = %addr, error = %e, "TCP frontend bind failed, retrying");
                    return None;
                }
            }
        }
        Some(listeners)
    }
}

async fn handle_client(
    stream: TcpStream,
    addr: SocketAddr,
    event_tx: mpsc::UnboundedSender<ClientEvent>,
    mut radio_up_rx: watch::Receiver<bool>,
) {
    let client_id = ClientId::new();
    let _ = stream.set_nodelay(true);
    let local_port = stream.local_addr().map(|a| a.port()).unwrap_or(0);
    tracing::info!(client = %client_id, addr = %addr, port = local_port, "client connected");

    let (mut reader, mut writer) = stream.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    if event_tx
        .send(ClientEvent::Connected {
            client_id,
            local_port,
            tx,
        })
        .is_err()
    {
        return;
    }

    let read_events = event_tx.clone();
    let mut read_handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        let mut parser = FrameParser::new();
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    for payload in parser.feed(&buf[..n]) {
                        let event = ClientEvent::Frame { client_id, payload };
                        if read_events.send(event).is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(client = %client_id, error = %e, "client read error");
                    break;
                }
            }
        }
    });

    // Ends when the core drops the client's sender, or on a write error.
    let mut write_handle = tokio::spawn(async move {
        while let Some(payload) = rx.recv().await {
            if writer.write_all(&frame::encode(&payload)).await.is_err() {
                break;
            }
        }
    });

    tokio::select! {
        _ = &mut read_handle => {},
        _ = &mut write_handle => {},
        _ = radio_up_rx.wait_for(|up| !*up) => {
            tracing::info!(client = %client_id, "radio unavailable, closing client");
        }
    }

    // Dropping a JoinHandle does not cancel its task. Abort both halves so
    // the socket is actually closed and the peer sees the disconnect.
    read_handle.abort();
    write_handle.abort();

    let _ = event_tx.send(ClientEvent::Disconnected { client_id });
    tracing::info!(client = %client_id, addr = %addr, "client disconnected");
}
