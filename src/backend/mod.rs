pub mod serial;
pub mod tcp;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A connected radio: frame payloads (FromRadio) come in through
/// `recv_rx` and go out (ToRadio) through `send_tx`. `recv_rx` closes
/// when the link drops.
pub struct RadioIo {
    pub send_tx: mpsc::UnboundedSender<Vec<u8>>,
    pub recv_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pub _handle: JoinHandle<()>,
}

/// Run the read and write halves of a byte stream until either fails.
fn spawn_io<S>(stream: S, name: &'static str) -> RadioIo
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::protocol::frame;

    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (recv_tx, recv_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let handle = tokio::spawn(async move {
        let (mut reader, mut writer) = tokio::io::split(stream);

        let read = async move {
            let mut buf = vec![0u8; 4096];
            let mut parser = frame::FrameParser::new();
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        tracing::warn!("{name}: connection closed");
                        break;
                    }
                    Ok(n) => {
                        for payload in parser.feed(&buf[..n]) {
                            tracing::trace!(len = payload.len(), "{name}: received frame");
                            if recv_tx.send(payload).is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "{name}: read error");
                        break;
                    }
                }
            }
        };

        let write = async move {
            while let Some(data) = send_rx.recv().await {
                let framed = frame::encode(&data);
                if let Err(e) = writer.write_all(&framed).await {
                    tracing::error!(error = %e, "{name}: write error");
                    break;
                }
                if let Err(e) = writer.flush().await {
                    tracing::error!(error = %e, "{name}: flush error");
                    break;
                }
                tracing::trace!(len = data.len(), "{name}: sent frame");
            }
        };

        // Whichever half ends first takes the link down; dropping the
        // other closes recv_tx so the core sees the disconnect.
        tokio::select! {
            _ = read => {},
            _ = write => {},
        }
    });

    RadioIo {
        send_tx,
        recv_rx,
        _handle: handle,
    }
}
