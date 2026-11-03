use anyhow::{Context as _, Result};
use tokio::net::TcpStream;

use super::RadioIo;

pub async fn connect(host: &str, port: u16) -> Result<RadioIo> {
    let addr = format!("{host}:{port}");
    let stream = TcpStream::connect(&addr)
        .await
        .context(format!("failed to connect to TCP backend at {addr}"))?;
    let _ = stream.set_nodelay(true);
    Ok(super::spawn_io(stream, "tcp backend"))
}
