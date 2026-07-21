use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use diesel::connection::SimpleConnection;
use diesel::deserialize::QueryableByName;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Binary, Integer, Nullable, Text};
use prost::Message as ProstMessage;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use whatsapp_rust::waproto::whatsapp as wa;

use crate::model::{Chat, Contact, Message, MessageKind};

pub const SCHEMA_VERSION: i32 = 1;
const WRITER_CAPACITY: usize = 2;

#[derive(Default)]
pub struct CachedState {
    pub contacts: Vec<Contact>,
    pub chats: Vec<(Chat, Option<u64>)>,
    pub messages: Vec<Message>,
    pub next_activity_order: u64,
}

pub struct CacheBootstrap {
    pub state: CachedState,
    pub writer: Option<CacheWriter>,
    pub warnings: Vec<String>,
}

pub struct CacheWriter {
    tx: mpsc::Sender<WriterCommand>,
    task: JoinHandle<()>,
}

enum WriterCommand {
    Save(CachedState),
    Flush(
        CachedState,
        tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ),
    Shutdown,
}

impl CacheWriter {
    pub fn try_save(&self, state: CachedState) -> std::result::Result<(), CachedState> {
        self.tx
            .try_send(WriterCommand::Save(state))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(WriterCommand::Save(state))
                | mpsc::error::TrySendError::Closed(WriterCommand::Save(state)) => state,
                _ => unreachable!(),
            })
    }

    pub async fn shutdown(self, final_state: CachedState) -> Result<()> {
        let (reply, response) = tokio::sync::oneshot::channel();
        self.tx
            .send(WriterCommand::Flush(final_state, reply))
            .await
            .map_err(|_| anyhow!("cache writer stopped before final flush"))?;
        let flush_result = response
            .await
            .map_err(|_| anyhow!("cache writer stopped during final flush"))?;
        let _ = self.tx.send(WriterCommand::Shutdown).await;
        if let Err(error) = self.task.await {
            return Err(anyhow!("cache writer task failed: {error}"));
        }
        flush_result.map_err(anyhow::Error::msg)
    }
}

pub async fn open(path: PathBuf, account: String) -> CacheBootstrap {
    match tokio::task::spawn_blocking(move || open_blocking(&path, &account)).await {
        Ok(Ok(bootstrap)) => bootstrap,
        Ok(Err(error)) => CacheBootstrap {
            warnings: vec![format!("conversation cache disabled: {error}")],
            ..empty_bootstrap()
        },
        Err(error) => CacheBootstrap {
            warnings: vec![format!("conversation cache worker failed: {error}")],
            ..empty_bootstrap()
        },
    }
}

fn empty_bootstrap() -> CacheBootstrap {
    CacheBootstrap {
        state: CachedState::default(),
        writer: None,
        warnings: Vec::new(),
    }
}

fn open_blocking(path: &Path, account: &str) -> Result<CacheBootstrap> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    match open_existing(path, account) {
        Ok(result) => Ok(result),
        Err(error) => {
            let recovery = preserve_corrupt_cache(path)?;
            let mut result = create_fresh(path, account)?;
            result.warnings.push(format!(
                "conversation cache was corrupt and was preserved as {}; rebuilding from sync ({error})",
                recovery.display()
            ));
            Ok(result)
        }
    }
}

fn open_existing(path: &Path, account: &str) -> Result<CacheBootstrap> {
    let existed = path.exists();
    let mut connection = establish(path)?;
    let version = schema_version(&mut connection)?;
    if version > SCHEMA_VERSION {
        return Ok(CacheBootstrap {
            warnings: vec![format!(
                "conversation cache schema {version} is newer than supported schema {SCHEMA_VERSION}; cache is disabled for this run"
            )],
            ..empty_bootstrap()
        });
    }
    if existed {
        verify_integrity(&mut connection)?;
    }
    migrate(&mut connection, version)?;
    configure(&mut connection)?;
    prepare_account(&mut connection, account)?;
    let state = load_state(&mut connection, account)?;
    Ok(CacheBootstrap {
        state,
        writer: Some(start_writer(connection, account.to_owned())),
        warnings: Vec::new(),
    })
}

fn create_fresh(path: &Path, account: &str) -> Result<CacheBootstrap> {
    let mut connection = establish(path)?;
    migrate(&mut connection, 0)?;
    configure(&mut connection)?;
    prepare_account(&mut connection, account)?;
    Ok(CacheBootstrap {
        state: CachedState::default(),
        writer: Some(start_writer(connection, account.to_owned())),
        warnings: Vec::new(),
    })
}

fn establish(path: &Path) -> Result<SqliteConnection> {
    SqliteConnection::establish(path.to_string_lossy().as_ref())
        .with_context(|| format!("failed to open {}", path.display()))
}

#[derive(QueryableByName)]
struct IntegerRow {
    #[diesel(sql_type = Integer)]
    value: i32,
}

fn schema_version(connection: &mut SqliteConnection) -> Result<i32> {
    Ok(
        diesel::sql_query("SELECT user_version AS value FROM pragma_user_version")
            .get_result::<IntegerRow>(connection)?
            .value,
    )
}

fn verify_integrity(connection: &mut SqliteConnection) -> Result<()> {
    #[derive(QueryableByName)]
    struct CheckRow {
        #[diesel(sql_type = Text)]
        result: String,
    }
    let result = diesel::sql_query("SELECT quick_check AS result FROM pragma_quick_check")
        .get_result::<CheckRow>(connection)?
        .result;
    if result != "ok" {
        bail!("SQLite quick_check failed: {result}");
    }
    Ok(())
}

fn migrate(connection: &mut SqliteConnection, version: i32) -> Result<()> {
    if version < 1 {
        connection.batch_execute(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS cache_metadata (
               key TEXT PRIMARY KEY NOT NULL,
               value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS contacts (
               account_id TEXT NOT NULL,
               id TEXT NOT NULL,
               name TEXT NOT NULL,
               short TEXT NOT NULL,
               PRIMARY KEY (account_id, id)
             );
             CREATE TABLE IF NOT EXISTS chats (
               account_id TEXT NOT NULL,
               id TEXT NOT NULL,
               is_group INTEGER NOT NULL,
               name TEXT NOT NULL,
               unread INTEGER NOT NULL,
               last_message INTEGER NOT NULL,
               preview TEXT NOT NULL,
               last_message_kind INTEGER NOT NULL,
               last_sender TEXT NOT NULL,
               last_from_me INTEGER NOT NULL,
               activity_order INTEGER,
               PRIMARY KEY (account_id, id)
             );
             CREATE TABLE IF NOT EXISTS messages (
               account_id TEXT NOT NULL,
               id TEXT NOT NULL,
               chat_id TEXT NOT NULL,
               sender_id TEXT NOT NULL,
               contact_id TEXT NOT NULL,
               contact_name TEXT NOT NULL,
               contact_short TEXT NOT NULL,
               timestamp INTEGER NOT NULL,
               from_me INTEGER NOT NULL,
               forwarded INTEGER NOT NULL,
               text TEXT NOT NULL,
               kind INTEGER NOT NULL,
               mime_type TEXT NOT NULL,
               file_name TEXT NOT NULL,
               unread INTEGER NOT NULL,
               raw_message BLOB,
               PRIMARY KEY (account_id, id)
             );
             CREATE INDEX IF NOT EXISTS messages_by_chat
               ON messages(account_id, chat_id, timestamp, id);
             PRAGMA user_version = 1;
             COMMIT;",
        )?;
    }
    Ok(())
}

fn configure(connection: &mut SqliteConnection) -> Result<()> {
    connection.batch_execute(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 3000;",
    )?;
    Ok(())
}

#[derive(QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    value: String,
}

fn prepare_account(connection: &mut SqliteConnection, account: &str) -> Result<()> {
    let stored =
        diesel::sql_query("SELECT value FROM cache_metadata WHERE key = 'account_id' LIMIT 1")
            .get_result::<TextRow>(connection)
            .optional()?
            .map(|row| row.value);
    if stored.as_deref() != Some(account) {
        connection.transaction::<_, anyhow::Error, _>(|connection| {
            diesel::sql_query("DELETE FROM messages").execute(connection)?;
            diesel::sql_query("DELETE FROM chats").execute(connection)?;
            diesel::sql_query("DELETE FROM contacts").execute(connection)?;
            diesel::sql_query(
                "INSERT INTO cache_metadata(key, value) VALUES ('account_id', ?)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind::<Text, _>(account)
            .execute(connection)?;
            Ok(())
        })?;
    }
    Ok(())
}

#[derive(QueryableByName)]
struct ContactRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    short: String,
}

#[derive(QueryableByName)]
struct ChatRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Integer)]
    is_group: i32,
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = BigInt)]
    unread: i64,
    #[diesel(sql_type = BigInt)]
    last_message: i64,
    #[diesel(sql_type = Text)]
    preview: String,
    #[diesel(sql_type = Integer)]
    last_message_kind: i32,
    #[diesel(sql_type = Text)]
    last_sender: String,
    #[diesel(sql_type = Integer)]
    last_from_me: i32,
    #[diesel(sql_type = Nullable<BigInt>)]
    activity_order: Option<i64>,
}

#[derive(QueryableByName)]
struct MessageRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    chat_id: String,
    #[diesel(sql_type = Text)]
    sender_id: String,
    #[diesel(sql_type = Text)]
    contact_id: String,
    #[diesel(sql_type = Text)]
    contact_name: String,
    #[diesel(sql_type = Text)]
    contact_short: String,
    #[diesel(sql_type = BigInt)]
    timestamp: i64,
    #[diesel(sql_type = Integer)]
    from_me: i32,
    #[diesel(sql_type = Integer)]
    forwarded: i32,
    #[diesel(sql_type = Text)]
    text: String,
    #[diesel(sql_type = Integer)]
    kind: i32,
    #[diesel(sql_type = Text)]
    mime_type: String,
    #[diesel(sql_type = Text)]
    file_name: String,
    #[diesel(sql_type = Integer)]
    unread: i32,
    #[diesel(sql_type = Nullable<Binary>)]
    raw_message: Option<Vec<u8>>,
}

fn load_state(connection: &mut SqliteConnection, account: &str) -> Result<CachedState> {
    let contacts =
        diesel::sql_query("SELECT id, name, short FROM contacts WHERE account_id = ? ORDER BY id")
            .bind::<Text, _>(account)
            .load::<ContactRow>(connection)?
            .into_iter()
            .map(|row| Contact {
                id: row.id,
                name: row.name,
                short: row.short,
            })
            .collect();
    let chat_rows = diesel::sql_query(
        "SELECT id, is_group, name, unread, last_message, preview, last_message_kind,
                last_sender, last_from_me, activity_order
         FROM chats WHERE account_id = ? ORDER BY id",
    )
    .bind::<Text, _>(account)
    .load::<ChatRow>(connection)?;
    let next_activity_order = chat_rows
        .iter()
        .filter_map(|row| row.activity_order)
        .max()
        .unwrap_or_default()
        .max(0) as u64;
    let chats = chat_rows
        .into_iter()
        .map(|row| {
            (
                Chat {
                    id: row.id,
                    is_group: row.is_group != 0,
                    name: row.name,
                    unread: row.unread.max(0) as usize,
                    last_message: row.last_message,
                    preview: row.preview,
                    last_message_kind: kind_from_i32(row.last_message_kind),
                    last_sender: row.last_sender,
                    last_from_me: row.last_from_me != 0,
                },
                row.activity_order.map(|value| value.max(0) as u64),
            )
        })
        .collect();
    let messages = diesel::sql_query(
        "SELECT id, chat_id, sender_id, contact_id, contact_name, contact_short, timestamp,
                from_me, forwarded, text, kind, mime_type, file_name, unread, raw_message
         FROM messages WHERE account_id = ? ORDER BY chat_id, timestamp, id",
    )
    .bind::<Text, _>(account)
    .load::<MessageRow>(connection)?
    .into_iter()
    .map(message_from_row)
    .collect::<Result<Vec<_>>>()?;
    Ok(CachedState {
        contacts,
        chats,
        messages,
        next_activity_order,
    })
}

fn message_from_row(row: MessageRow) -> Result<Message> {
    let raw_message = row
        .raw_message
        .map(|bytes| wa::Message::decode(bytes.as_slice()).map(Arc::new))
        .transpose()
        .context("failed to decode cached message payload")?;
    Ok(Message {
        id: row.id,
        chat_id: row.chat_id,
        sender_id: row.sender_id,
        contact_id: row.contact_id,
        contact_name: row.contact_name,
        contact_short: row.contact_short,
        timestamp: row.timestamp.max(0) as u64,
        from_me: row.from_me != 0,
        forwarded: row.forwarded != 0,
        text: row.text,
        kind: kind_from_i32(row.kind),
        mime_type: row.mime_type,
        file_name: row.file_name,
        unread: row.unread != 0,
        raw_message,
    })
}

fn start_writer(mut connection: SqliteConnection, account: String) -> CacheWriter {
    let (tx, mut rx) = mpsc::channel(WRITER_CAPACITY);
    let task = tokio::task::spawn_blocking(move || {
        while let Some(command) = rx.blocking_recv() {
            match command {
                WriterCommand::Save(state) => {
                    if let Err(error) = save_state(&mut connection, &account, &state) {
                        log::error!("conversation cache write failed: {error}");
                    }
                }
                WriterCommand::Flush(state, reply) => {
                    let result = save_state(&mut connection, &account, &state)
                        .map_err(|error| error.to_string());
                    let _ = reply.send(result);
                }
                WriterCommand::Shutdown => break,
            }
        }
    });
    CacheWriter { tx, task }
}

fn save_state(connection: &mut SqliteConnection, account: &str, state: &CachedState) -> Result<()> {
    connection.transaction::<_, anyhow::Error, _>(|connection| {
        for table in ["messages", "chats", "contacts"] {
            diesel::sql_query(format!("DELETE FROM {table} WHERE account_id = ?"))
                .bind::<Text, _>(account)
                .execute(connection)?;
        }
        for contact in &state.contacts {
            diesel::sql_query(
                "INSERT INTO contacts(account_id, id, name, short) VALUES (?, ?, ?, ?)",
            )
            .bind::<Text, _>(account)
            .bind::<Text, _>(&contact.id)
            .bind::<Text, _>(&contact.name)
            .bind::<Text, _>(&contact.short)
            .execute(connection)?;
        }
        for (chat, activity_order) in &state.chats {
            diesel::sql_query(
                "INSERT INTO chats(account_id, id, is_group, name, unread, last_message, preview,
                    last_message_kind, last_sender, last_from_me, activity_order)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind::<Text, _>(account)
            .bind::<Text, _>(&chat.id)
            .bind::<Integer, _>(i32::from(chat.is_group))
            .bind::<Text, _>(&chat.name)
            .bind::<BigInt, _>(chat.unread.min(i64::MAX as usize) as i64)
            .bind::<BigInt, _>(chat.last_message)
            .bind::<Text, _>(&chat.preview)
            .bind::<Integer, _>(kind_to_i32(chat.last_message_kind))
            .bind::<Text, _>(&chat.last_sender)
            .bind::<Integer, _>(i32::from(chat.last_from_me))
            .bind::<Nullable<BigInt>, _>(
                activity_order.map(|value| value.min(i64::MAX as u64) as i64),
            )
            .execute(connection)?;
        }
        for message in &state.messages {
            let raw = message
                .raw_message
                .as_ref()
                .map(|message| message.encode_to_vec());
            diesel::sql_query(
                "INSERT INTO messages(account_id, id, chat_id, sender_id, contact_id, contact_name,
                    contact_short, timestamp, from_me, forwarded, text, kind, mime_type, file_name,
                    unread, raw_message)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind::<Text, _>(account)
            .bind::<Text, _>(&message.id)
            .bind::<Text, _>(&message.chat_id)
            .bind::<Text, _>(&message.sender_id)
            .bind::<Text, _>(&message.contact_id)
            .bind::<Text, _>(&message.contact_name)
            .bind::<Text, _>(&message.contact_short)
            .bind::<BigInt, _>(message.timestamp.min(i64::MAX as u64) as i64)
            .bind::<Integer, _>(i32::from(message.from_me))
            .bind::<Integer, _>(i32::from(message.forwarded))
            .bind::<Text, _>(&message.text)
            .bind::<Integer, _>(kind_to_i32(message.kind))
            .bind::<Text, _>(&message.mime_type)
            .bind::<Text, _>(&message.file_name)
            .bind::<Integer, _>(i32::from(message.unread))
            .bind::<Nullable<Binary>, _>(raw)
            .execute(connection)?;
        }
        Ok(())
    })?;
    Ok(())
}

pub async fn remove_files(path: &Path) -> Result<()> {
    for file in database_files(path) {
        match tokio::fs::remove_file(&file).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("failed to remove {}", file.display()));
            }
        }
    }
    Ok(())
}

pub fn database_files(path: &Path) -> Vec<PathBuf> {
    vec![
        path.to_owned(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ]
}

fn preserve_corrupt_cache(path: &Path) -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let recovery = PathBuf::from(format!("{}.corrupt-{timestamp}", path.display()));
    let files = database_files(path);
    for (index, file) in files.into_iter().enumerate() {
        if !file.exists() {
            continue;
        }
        let target = if index == 0 {
            recovery.clone()
        } else if index == 1 {
            PathBuf::from(format!("{}-wal", recovery.display()))
        } else {
            PathBuf::from(format!("{}-shm", recovery.display()))
        };
        std::fs::rename(&file, &target)
            .with_context(|| format!("failed to preserve corrupt cache {}", file.display()))?;
    }
    if !recovery.exists() {
        return Err(anyhow!(
            "cache failed before a recoverable database file was created"
        ));
    }
    Ok(recovery)
}

pub fn kind_to_i32(kind: MessageKind) -> i32 {
    match kind {
        MessageKind::Text => 1,
        MessageKind::Image => 2,
        MessageKind::Video => 3,
        MessageKind::Audio => 4,
        MessageKind::Document => 5,
        MessageKind::Sticker => 6,
        MessageKind::Unknown => 0,
    }
}

pub fn kind_from_i32(kind: i32) -> MessageKind {
    match kind {
        1 => MessageKind::Text,
        2 => MessageKind::Image,
        3 => MessageKind::Video,
        4 => MessageKind::Audio,
        5 => MessageKind::Document,
        6 => MessageKind::Sticker,
        _ => MessageKind::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_cache(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "whatscli-cache-{label}-{}-{nonce}",
                std::process::id()
            ))
            .join("cache.db")
    }

    #[test]
    fn sticker_uses_stable_persisted_value_six() {
        assert_eq!(kind_to_i32(MessageKind::Sticker), 6);
        assert_eq!(kind_from_i32(6), MessageKind::Sticker);
    }

    #[tokio::test]
    async fn new_database_is_migrated_to_the_current_schema() {
        let path = temp_cache("migration");
        let bootstrap = open(path.clone(), "owner@wa".into()).await;
        assert!(bootstrap.warnings.is_empty());
        bootstrap
            .writer
            .unwrap()
            .shutdown(CachedState::default())
            .await
            .unwrap();
        let mut connection = establish(&path).unwrap();
        assert_eq!(schema_version(&mut connection).unwrap(), SCHEMA_VERSION);
        drop(connection);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn future_schema_is_left_untouched_and_disabled() {
        let path = temp_cache("future");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut connection = establish(&path).unwrap();
        connection
            .batch_execute(&format!("PRAGMA user_version = {}", SCHEMA_VERSION + 1))
            .unwrap();
        drop(connection);

        let bootstrap = open(path.clone(), "owner@wa".into()).await;
        assert!(bootstrap.writer.is_none());
        assert!(bootstrap.warnings[0].contains("newer than supported"));
        let mut connection = establish(&path).unwrap();
        assert_eq!(schema_version(&mut connection).unwrap(), SCHEMA_VERSION + 1);
        drop(connection);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[tokio::test]
    async fn corrupt_database_is_preserved_and_rebuilt() {
        let path = temp_cache("corrupt");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"this is not sqlite").unwrap();

        let bootstrap = open(path.clone(), "owner@wa".into()).await;
        assert!(bootstrap.writer.is_some());
        assert!(bootstrap.warnings[0].contains("was corrupt"));
        bootstrap
            .writer
            .unwrap()
            .shutdown(CachedState::default())
            .await
            .unwrap();
        let recovered = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("cache.db.corrupt-")
            });
        assert!(recovered);
        let mut connection = establish(&path).unwrap();
        assert_eq!(schema_version(&mut connection).unwrap(), SCHEMA_VERSION);
        drop(connection);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
