use std::path::PathBuf;

use clap::Parser;

use crate::core::{Backend, CoreConfig};
use crate::store::Retention;

#[derive(Parser, Debug)]
#[command(
    name = "tasticsplitter",
    version,
    about = "Meshtastic client multiplexer with cache"
)]
pub struct Cli {
    /// Serial port path (e.g., /dev/ttyUSB0)
    #[arg(long, short = 's', env = "TASTICSPLITTER_SERIAL_PORT")]
    pub serial: Option<String>,

    /// Serial baud rate
    #[arg(long, default_value_t = 115200, env = "TASTICSPLITTER_BAUD_RATE")]
    pub baud: u32,

    /// TCP backend host (a WiFi/Ethernet node or meshtasticd)
    #[arg(
        long,
        short = 't',
        env = "TASTICSPLITTER_TCP_BACKEND_HOST",
        conflicts_with = "serial"
    )]
    pub tcp_backend_host: Option<String>,

    /// TCP backend port
    #[arg(
        long,
        short = 'p',
        default_value_t = 4403,
        env = "TASTICSPLITTER_TCP_BACKEND_PORT"
    )]
    pub tcp_backend_port: u16,

    /// TCP frontend bind address
    #[arg(
        long,
        default_value = "0.0.0.0",
        env = "TASTICSPLITTER_TCP_FRONTEND_HOST"
    )]
    pub tcp_frontend_host: String,

    /// TCP frontend port(s), comma separated. Clients are told apart by
    /// port, so give each app its own to keep separate message histories.
    #[arg(
        long,
        default_value = "4403",
        value_delimiter = ',',
        num_args = 1..,
        env = "TASTICSPLITTER_TCP_FRONTEND_PORT"
    )]
    pub tcp_frontend_port: Vec<u16>,

    /// Data directory for the state database
    #[arg(long, default_value = "./data", env = "TASTICSPLITTER_DATA_DIR")]
    pub data_dir: PathBuf,

    /// Log level: off, error, warn, info, debug, verbose
    #[arg(long, default_value = "info", env = "TASTICSPLITTER_LOG_LEVEL")]
    pub log_level: String,

    /// JSON event logging
    #[arg(long, env = "TASTICSPLITTER_LOG_JSON")]
    pub json: bool,

    /// Minutes between full re-reads of the radio's config and node
    /// database (0 = only when something changes it)
    #[arg(long, default_value_t = 60, env = "TASTICSPLITTER_RESYNC_MINUTES")]
    pub resync_minutes: u32,

    /// Days to keep stored messages (0 = forever)
    #[arg(
        long,
        default_value_t = 0,
        env = "TASTICSPLITTER_MESSAGES_RETENTION_DAYS"
    )]
    pub messages_retention_days: u32,

    /// Days a client may stay away before it is forgotten; if it comes
    /// back it is treated as new (0 = never)
    #[arg(
        long,
        default_value_t = 180,
        env = "TASTICSPLITTER_CLIENTS_RETENTION_DAYS"
    )]
    pub clients_retention_days: u32,
}

impl Cli {
    pub fn into_config(self) -> anyhow::Result<CoreConfig> {
        let backend = match (self.serial, self.tcp_backend_host) {
            (Some(path), None) => Backend::Serial {
                path,
                baud: self.baud,
            },
            (None, Some(host)) => Backend::Tcp {
                host,
                port: self.tcp_backend_port,
            },
            _ => anyhow::bail!("set a radio: --serial or --tcp-backend-host"),
        };

        Ok(CoreConfig {
            backend,
            frontend_host: self.tcp_frontend_host,
            frontend_ports: self.tcp_frontend_port,
            data_dir: self.data_dir,
            retention: Retention {
                messages_days: self.messages_retention_days,
                clients_days: self.clients_retention_days,
            },
            resync_minutes: self.resync_minutes,
        })
    }
}
