use anyhow::{Context, Result};
use tokio::io::AsyncWriteExt;
use tokio_serial::SerialPortBuilderExt;

use super::RadioIo;
use crate::protocol::frame;

pub async fn connect(path: &str, baud: u32) -> Result<RadioIo> {
    let mut serial = tokio_serial::new(path, baud)
        .open_native_async()
        .context("failed to open serial port")?;

    serial
        .set_exclusive(false)
        .context("failed to set serial exclusive mode")?;

    // Like the official clients: a run of START2 wakes a sleeping device
    // and gets the firmware out of the middle of a half-read frame.
    serial
        .write_all(&[frame::START2; 32])
        .await
        .context("failed to wake the device")?;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    Ok(super::spawn_io(serial, "serial"))
}
