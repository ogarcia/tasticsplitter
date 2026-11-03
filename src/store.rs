use std::path::Path;

use anyhow::{Context, Result};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqlitePoolOptions};

/// SQLite state: the messages clients saw, so a client that was away
/// gets the ones it missed, and the cursor of each client identity.
pub struct Store {
    pool: SqlitePool,
}

/// A message to store.
pub struct NewMessage<'a> {
    pub from_node: u32,
    pub to_node: u32,
    pub channel: u32,
    pub packet_id: u32,
    pub text: &'a str,
    /// The FromRadio exactly as clients saw it, replayed verbatim.
    pub frame: &'a [u8],
    /// Frontend port of the client that sent it through the proxy, so it
    /// is never replayed back to it. `None` for traffic from the radio.
    pub origin_port: Option<u16>,
}

/// Retention periods in days; 0 disables purging for that table.
#[derive(Debug, Clone, Copy)]
pub struct Retention {
    pub messages_days: u32,
    pub clients_days: u32,
}

#[derive(Debug, Default)]
pub struct PurgeStats {
    pub messages: u64,
    pub clients: u64,
}

impl Store {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Incremental auto-vacuum lets maintain() hand freed pages back to
        // the filesystem after purging, without a full VACUUM each time.
        let options = SqliteConnectOptions::new()
            .filename(path.as_ref())
            .create_if_missing(true)
            .auto_vacuum(SqliteAutoVacuum::Incremental);

        // Single connection on purpose: the core serializes all access.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .context("failed to open SQLite database")?;

        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<()> {
        // received_at is the local arrival time; the sender's clock can't
        // be trusted for retention.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at INTEGER NOT NULL,
                from_node INTEGER NOT NULL,
                to_node INTEGER NOT NULL,
                channel INTEGER NOT NULL,
                packet_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                frame BLOB NOT NULL,
                origin_port INTEGER
            )",
        )
        .execute(&self.pool)
        .await?;

        // Clients are identified by the frontend port they connect to.
        // last_seen_id is the highest messages.id the client has seen,
        // live or replayed; whatever is above it is delivered on reconnect.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS clients (
                port INTEGER PRIMARY KEY,
                last_seen_id INTEGER NOT NULL,
                last_connected_at INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // --- Messages ---

    pub async fn insert_message(&self, msg: &NewMessage<'_>) -> Result<i64> {
        let result = sqlx::query(
            "INSERT INTO messages
                 (received_at, from_node, to_node, channel, packet_id, text, frame, origin_port)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(chrono::Utc::now().timestamp())
        .bind(i64::from(msg.from_node))
        .bind(i64::from(msg.to_node))
        .bind(i64::from(msg.channel))
        .bind(i64::from(msg.packet_id))
        .bind(msg.text)
        .bind(msg.frame)
        .bind(msg.origin_port.map(i64::from))
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    /// Highest message id stored so far (0 when there are none).
    pub async fn message_head(&self) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM messages")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// Frames of the messages above `after` for the client on `port`,
    /// oldest first, leaving out those it sent itself.
    pub async fn missed_frames(&self, after: i64, port: u16) -> Result<Vec<(i64, Vec<u8>)>> {
        Ok(sqlx::query_as(
            "SELECT id, frame FROM messages
             WHERE id > ?1 AND (origin_port IS NULL OR origin_port != ?2)
             ORDER BY id",
        )
        .bind(after)
        .bind(i64::from(port))
        .fetch_all(&self.pool)
        .await?)
    }

    // --- Clients ---

    /// Last message id seen by the client on `port`, `None` if new.
    pub async fn client_cursor(&self, port: u16) -> Result<Option<i64>> {
        Ok(
            sqlx::query_scalar("SELECT last_seen_id FROM clients WHERE port = ?1")
                .bind(i64::from(port))
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    /// Move a client's cursor forward to `seen_id`; never backwards, since
    /// two connections on the same port may report out of order.
    pub async fn advance_client_cursor(&self, port: u16, seen_id: i64) -> Result<()> {
        sqlx::query(
            "INSERT INTO clients (port, last_seen_id, last_connected_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT (port) DO UPDATE SET
                 last_seen_id = MAX(last_seen_id, excluded.last_seen_id),
                 last_connected_at = excluded.last_connected_at",
        )
        .bind(i64::from(port))
        .bind(seen_id)
        .bind(chrono::Utc::now().timestamp())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // --- Maintenance ---

    /// Purge rows older than the given retention (in days; 0 keeps them
    /// forever) and give the freed space back.
    pub async fn maintain(&self, retention: &Retention) -> Result<PurgeStats> {
        let now = chrono::Utc::now().timestamp();
        let cutoff = |days: u32| now - i64::from(days) * 86_400;
        let mut stats = PurgeStats::default();

        if retention.messages_days > 0 {
            stats.messages = sqlx::query("DELETE FROM messages WHERE received_at < ?1")
                .bind(cutoff(retention.messages_days))
                .execute(&self.pool)
                .await?
                .rows_affected();
        }

        // A purged client that comes back is treated as new.
        if retention.clients_days > 0 {
            stats.clients = sqlx::query("DELETE FROM clients WHERE last_connected_at < ?1")
                .bind(cutoff(retention.clients_days))
                .execute(&self.pool)
                .await?
                .rows_affected();
        }

        sqlx::query("PRAGMA incremental_vacuum")
            .execute(&self.pool)
            .await?;
        sqlx::query("PRAGMA optimize").execute(&self.pool).await?;

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn open_memory() -> Store {
        Store::open(":memory:").await.unwrap()
    }

    fn message(text: &str, origin_port: Option<u16>) -> NewMessage<'_> {
        NewMessage {
            from_node: 1,
            to_node: 0xFFFF_FFFF,
            channel: 0,
            packet_id: 42,
            text,
            frame: text.as_bytes(),
            origin_port,
        }
    }

    #[tokio::test]
    async fn missed_frames_skip_own_messages() {
        let store = open_memory().await;
        let a = store.insert_message(&message("a", None)).await.unwrap();
        store
            .insert_message(&message("b", Some(4403)))
            .await
            .unwrap();
        let c = store
            .insert_message(&message("c", Some(4404)))
            .await
            .unwrap();

        let ids: Vec<i64> = store
            .missed_frames(0, 4403)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, vec![a, c]);
        assert_eq!(store.missed_frames(c, 4403).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn cursor_never_moves_back() {
        let store = open_memory().await;
        assert_eq!(store.client_cursor(4403).await.unwrap(), None);
        store.advance_client_cursor(4403, 5).await.unwrap();
        store.advance_client_cursor(4403, 3).await.unwrap();
        assert_eq!(store.client_cursor(4403).await.unwrap(), Some(5));
    }
}
