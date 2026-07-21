use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{TimeZone, Utc};
use regex::Regex;
use tokio::sync::{RwLock, Semaphore, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use whatsapp_rust::bot::{Bot, BotHandle};
use whatsapp_rust::download::MediaType;
use whatsapp_rust::proto_helpers::MessageExt;
use whatsapp_rust::store::SqliteStore;
use whatsapp_rust::transport::{TokioWebSocketTransportFactory, UreqHttpClient};
use whatsapp_rust::types::events::{Event, EventHandler, LazyHistorySync};
use whatsapp_rust::types::message::{
    EditAttribute, MessageCategory, MessageInfo, MessageSource, MsgMetaInfo,
};
use whatsapp_rust::upload::{UploadOptions, UploadResponse};
use whatsapp_rust::waproto::whatsapp as wa;
use whatsapp_rust::{
    Client, GroupCreateOptions, GroupParticipantOptions, GroupSubject, Jid, RevokeType,
    TokioRuntime,
};

use crate::config::Config;
use crate::model::{
    Chat, Command, ConnectionState, GROUP_SUFFIX, Message, MessageKind, SessionStatus,
    TaskCategory, TaskInfo, UiEvent,
};
use crate::qr;
use crate::storage::{QUEUE_CAPACITY, StorageHandle, start_storage_actor};

const TRANSFER_LIMIT: usize = 2;

fn http_client_without_idle_pool() -> UreqHttpClient {
    UreqHttpClient::with_agent(http_agent_without_idle_pool())
}

fn http_agent_without_idle_pool() -> ureq::Agent {
    ureq::config::Config::builder()
        .input_buffer_size(16 * 1024)
        .output_buffer_size(16 * 1024)
        .max_idle_connections(0)
        .max_idle_connections_per_host(0)
        .build()
        .into()
}

pub struct BackgroundRuntime {
    pub commands: mpsc::Sender<Command>,
    pub events: mpsc::Receiver<UiEvent>,
    handle: BackgroundHandle,
}

pub struct BackgroundHandle {
    shutdown_tx: watch::Sender<bool>,
    supervisor: JoinHandle<()>,
}

impl BackgroundRuntime {
    pub fn into_parts(
        self,
    ) -> (
        mpsc::Sender<Command>,
        mpsc::Receiver<UiEvent>,
        BackgroundHandle,
    ) {
        (self.commands, self.events, self.handle)
    }
}

impl BackgroundHandle {
    pub async fn shutdown(self) {
        self.shutdown_with_timeout(Duration::from_secs(3)).await;
    }

    async fn shutdown_with_timeout(self, timeout: Duration) -> bool {
        let _ = self.shutdown_tx.send(true);
        let mut supervisor = self.supervisor;
        if tokio::time::timeout(timeout, &mut supervisor)
            .await
            .is_err()
        {
            supervisor.abort();
            let _ = supervisor.await;
            true
        } else {
            false
        }
    }
}

#[derive(Clone)]
pub struct SessionManager {
    storage: StorageHandle,
    current_receiver: Arc<RwLock<String>>,
    client: Arc<Client>,
    ui_tx: mpsc::Sender<UiEvent>,
    config: Arc<Config>,
}

enum IntegrationWork {
    Command(Command),
    Notification {
        task: TaskInfo,
        title: String,
        message: String,
    },
}

enum HistoryWork {
    Command(Command),
    Protocol {
        event: Arc<Event>,
        queued_at: Instant,
    },
}

struct ProtocolEvent {
    event: Arc<Event>,
    queued_at: Instant,
}

struct OrderedEventHandler {
    tx: async_channel::Sender<ProtocolEvent>,
}

impl EventHandler for OrderedEventHandler {
    fn handle_event(&self, event: Arc<Event>) {
        let kind = event_kind(&event);
        let envelope = ProtocolEvent {
            event,
            queued_at: Instant::now(),
        };
        if self.tx.try_send(envelope).is_ok() {
            log::debug!("protocol event queued type={kind} depth={}", self.tx.len());
        } else {
            log::debug!("protocol event ignored after queue close type={kind}");
        }
    }
}

impl SessionManager {
    pub fn start(config: Arc<Config>) -> BackgroundRuntime {
        let (command_tx, command_rx) = mpsc::channel(QUEUE_CAPACITY);
        let (ui_tx, ui_rx) = mpsc::channel(QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let supervisor = tokio::spawn(supervise(config, command_rx, ui_tx, shutdown_rx));
        BackgroundRuntime {
            commands: command_tx,
            events: ui_rx,
            handle: BackgroundHandle {
                shutdown_tx,
                supervisor,
            },
        }
    }

    async fn initialize(
        config: Arc<Config>,
        protocol_tx: async_channel::Sender<ProtocolEvent>,
    ) -> Result<(Arc<Client>, BotHandle)> {
        finish_pending_session_reset(&config.session_file).await?;
        let session_path = config.session_file.to_string_lossy();
        let store = Arc::new(SqliteStore::new(&session_path).await.with_context(|| {
            format!(
                "failed to open session database {}",
                config.session_file.display()
            )
        })?);
        let mut bot = Bot::builder()
            .with_backend(store)
            .with_transport_factory(TokioWebSocketTransportFactory::new())
            .with_http_client(http_client_without_idle_pool())
            .with_runtime(TokioRuntime)
            .build()
            .await?;
        let client = bot.client();
        client.register_handler(Arc::new(OrderedEventHandler { tx: protocol_tx }));
        let handle = bot.run().await?;
        Ok((client, handle))
    }

    async fn execute(&self, command: Command) -> Result<()> {
        match command.name.as_str() {
            "select" => {
                let id = require_param(&command, 0)?;
                *self.current_receiver.write().await = id.to_owned();
                self.storage.select(id.to_owned()).await?;
            }
            "connect" | "login" => {
                if !self.client.is_connected() {
                    let _ = self
                        .ui_tx
                        .send(UiEvent::Status(SessionStatus {
                            state: ConnectionState::Connecting,
                            last_seen: String::new(),
                        }))
                        .await;
                    self.client.connect().await?;
                }
                self.text("Successfully connected to WhatsApp").await;
            }
            "disconnect" => self.client.disconnect().await,
            "logout" => {
                self.client.logout().await?;
                self.storage.clear_cache().await?;
                self.text("Successfully logged out").await;
            }
            "reset" => self.reset_session().await?,
            "send" => {
                let chat = require_param(&command, 0)?;
                let text = command.params.get(1..).unwrap_or_default().join(" ");
                if text.is_empty() {
                    bail!("Usage: send [chat-id] [message text]");
                }
                self.send_text(chat, &text).await?;
            }
            "read" => self.mark_current_chat_read().await?,
            "backlog" | "more" => self.load_backlog().await?,
            "info" => {
                let id = require_param(&command, 0)?;
                self.text(self.storage.message_info(id.to_owned()).await?)
                    .await;
            }
            "download" => self.download_command(&command, false, false).await?,
            "open" => self.download_command(&command, true, false).await?,
            "show" => self.download_command(&command, true, true).await?,
            "view" | "play" => self.media_command(&command).await?,
            "url" => self.open_url(&command).await?,
            "upload" => {
                self.send_media_command(&command, MessageKind::Document)
                    .await?
            }
            "sendimage" => {
                self.send_media_command(&command, MessageKind::Image)
                    .await?
            }
            "sendvideo" => {
                self.send_media_command(&command, MessageKind::Video)
                    .await?
            }
            "sendaudio" => {
                self.send_media_command(&command, MessageKind::Audio)
                    .await?
            }
            "revoke" => self.revoke_message(&command).await?,
            "leave" => {
                let group = self.current_group().await?;
                self.client.groups().leave(&group).await?;
                self.text(format!("left group {group}")).await;
            }
            "create" => self.create_group(&command).await?,
            "add" | "remove" | "admin" | "removeadmin" => {
                self.update_participants(&command).await?
            }
            "subject" => self.update_subject(&command).await?,
            "colorlist" => {
                let _ = self.ui_tx.send(UiEvent::ColorList).await;
            }
            "clipboard-copy" => self.copy_to_clipboard(&command).await?,
            "clipboard-paste" => self.paste_from_clipboard().await?,
            other => bail!("Unknown command: {other}"),
        }
        Ok(())
    }

    async fn text(&self, text: impl Into<String>) {
        let _ = self.ui_tx.send(UiEvent::Text(text.into())).await;
    }

    async fn reset_session(&self) -> Result<()> {
        self.client.logout().await?;
        self.storage.clear_cache().await?;
        let marker = reset_marker(&self.config.session_file);
        tokio::fs::write(&marker, b"pending")
            .await
            .with_context(|| format!("failed to create reset marker {}", marker.display()))?;
        let mut deferred = false;
        for path in database_files(&self.config.session_file) {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => deferred = true,
            }
        }
        if !deferred {
            let _ = tokio::fs::remove_file(&marker).await;
        }
        self.text(if deferred {
            "Session reset scheduled. Restart whatscli to pair with a new QR code."
        } else {
            "Session reset. Restart whatscli to pair with a new QR code."
        })
        .await;
        Ok(())
    }

    async fn send_text(&self, chat_id: &str, text: &str) -> Result<()> {
        ensure_connected(&self.client)?;
        let jid: Jid = chat_id.parse().context("invalid JID")?;
        let response = self
            .client
            .send_message(
                jid,
                wa::Message {
                    conversation: Some(text.into()),
                    ..Default::default()
                },
            )
            .await?;
        let own = self
            .client
            .get_pn()
            .await
            .map(|jid| jid.to_string())
            .unwrap_or_default();
        let contact_id = if chat_id.ends_with(GROUP_SUFFIX) {
            own.clone()
        } else {
            chat_id.to_owned()
        };
        let (contact_name, contact_short) = self
            .storage
            .resolve_contact(contact_id.clone(), contact_id.clone(), String::new())
            .await?;
        let msg = Message {
            id: response.message_id,
            chat_id: chat_id.into(),
            sender_id: own,
            contact_id: contact_id.clone(),
            contact_name,
            contact_short,
            timestamp: Utc::now().timestamp().max(0) as u64,
            from_me: true,
            text: text.into(),
            kind: MessageKind::Text,
            raw_message: None,
            ..Default::default()
        };
        self.storage.add_message(msg, false).await?;
        Ok(())
    }

    async fn load_backlog(&self) -> Result<()> {
        ensure_connected(&self.client)?;
        let chat_id = self.current_receiver.read().await.clone();
        if chat_id.is_empty() {
            bail!("Usage: backlog -> only works in a chat");
        }
        let oldest = self.storage.oldest_message(chat_id.clone()).await?
            .ok_or_else(|| anyhow!("No local message anchor found yet. Wait for history sync, then try /backlog again."))?;
        let jid: Jid = chat_id.parse().context("invalid JID")?;
        self.text("Retrieving message history...").await;
        self.client
            .fetch_message_history(
                &jid,
                &oldest.id,
                oldest.from_me,
                oldest.timestamp as i64 * 1000,
                self.config.general.backlog_msg_quantity,
            )
            .await?;
        self.text("Requested older messages from WhatsApp. Waiting for sync response.")
            .await;
        Ok(())
    }

    async fn mark_current_chat_read(&self) -> Result<()> {
        ensure_connected(&self.client)?;
        let chat_id = self.current_receiver.read().await.clone();
        if chat_id.is_empty() {
            bail!("Usage: read -> only works in a chat");
        }
        let chat: Jid = chat_id.parse().context("invalid JID")?;
        let cleared = self.storage.mark_chat_read(chat_id.clone()).await?;
        if cleared.is_empty() {
            self.text("No unread messages in current chat").await;
            return Ok(());
        }
        let mut batches: HashMap<String, Vec<String>> = HashMap::new();
        for msg in cleared {
            let sender = if chat_id.ends_with(GROUP_SUFFIX) {
                msg.sender_id
            } else {
                String::new()
            };
            batches.entry(sender).or_default().push(msg.id);
        }
        for (sender, ids) in batches {
            let sender_jid = if sender.is_empty() {
                None
            } else {
                Some(sender.parse::<Jid>()?)
            };
            self.client
                .mark_as_read(&chat, sender_jid.as_ref(), ids)
                .await?;
        }
        Ok(())
    }

    async fn download_command(&self, command: &Command, preview: bool, show: bool) -> Result<()> {
        let id = require_param(command, 0)?;
        let msg = self
            .storage
            .message(id.to_owned())
            .await?
            .ok_or_else(|| anyhow!("message not found"))?;
        if show && msg.kind != MessageKind::Image {
            bail!("show only works for image messages");
        }
        let path = self.download_message(&msg, preview).await?;
        if show {
            self.show_image(path).await?;
        } else if preview {
            self.open_target(path.to_string_lossy().into_owned())
                .await?;
            self.text("Opened with the system application").await;
        } else {
            self.text(format!("-> {}", path.display())).await;
        }
        Ok(())
    }

    async fn download_message(&self, msg: &Message, preview: bool) -> Result<PathBuf> {
        let file_name = download_file_name(msg);
        if preview {
            let path = crate::media_cache::cache_path(
                &self.config.general.preview_path,
                &msg.id,
                &file_name,
            )
            .await?;
            if tokio::fs::try_exists(&path).await? {
                crate::media_cache::touch(&path).await?;
                return Ok(path);
            }
        } else {
            let cached = crate::media_cache::cache_path(
                &self.config.general.media_cache_path,
                &msg.id,
                &file_name,
            )
            .await?;
            if tokio::fs::try_exists(&cached).await? {
                crate::media_cache::touch(&cached).await?;
                let data = tokio::fs::read(cached).await?;
                return crate::media_cache::atomic_write_download(
                    &self.config.general.download_path,
                    &file_name,
                    &data,
                )
                .await;
            }
        }
        ensure_connected(&self.client)?;
        let raw = msg
            .raw_message
            .as_ref()
            .ok_or_else(|| anyhow!("This is not a downloadable message"))?;
        let base = raw.get_base_message();
        let data = match msg.kind {
            MessageKind::Image => {
                self.client
                    .download(
                        base.image_message
                            .as_deref()
                            .ok_or_else(|| anyhow!("missing image payload"))?,
                    )
                    .await?
            }
            MessageKind::Video => {
                self.client
                    .download(
                        base.video_message
                            .as_deref()
                            .ok_or_else(|| anyhow!("missing video payload"))?,
                    )
                    .await?
            }
            MessageKind::Audio => {
                self.client
                    .download(
                        base.audio_message
                            .as_deref()
                            .ok_or_else(|| anyhow!("missing audio payload"))?,
                    )
                    .await?
            }
            MessageKind::Document => {
                self.client
                    .download(
                        base.document_message
                            .as_deref()
                            .ok_or_else(|| anyhow!("missing document payload"))?,
                    )
                    .await?
            }
            MessageKind::Sticker => {
                let sticker = base
                    .sticker_message
                    .as_deref()
                    .ok_or_else(|| anyhow!("missing sticker payload"))?;
                ensure_supported_sticker(sticker)?;
                self.client.download(sticker).await?
            }
            _ => bail!("This is not a downloadable message"),
        };
        if preview {
            let path = crate::media_cache::cache_path(
                &self.config.general.preview_path,
                &msg.id,
                &file_name,
            )
            .await?;
            crate::media_cache::atomic_write_cached(&path, &data).await
        } else {
            crate::media_cache::atomic_write_download(
                &self.config.general.download_path,
                &file_name,
                &data,
            )
            .await
        }
    }

    async fn media_command(&self, command: &Command) -> Result<()> {
        let id = require_param(command, 0)?;
        let msg = self
            .storage
            .message(id.to_owned())
            .await?
            .ok_or_else(|| anyhow!("message not found"))?;
        let valid = match command.name.as_str() {
            "view" => matches!(msg.kind, MessageKind::Image | MessageKind::Sticker),
            "play" => matches!(msg.kind, MessageKind::Audio | MessageKind::Video),
            _ => false,
        };
        if !valid {
            bail!("{} is not available for this message", command.name);
        }
        let file_name = download_file_name(&msg);
        let path = crate::media_cache::cache_path(
            &self.config.general.media_cache_path,
            &msg.id,
            &file_name,
        )
        .await?;
        let path = if tokio::fs::try_exists(&path).await? {
            crate::media_cache::touch(&path).await?;
            path
        } else {
            ensure_connected(&self.client)?;
            let raw = msg
                .raw_message
                .as_ref()
                .ok_or_else(|| anyhow!("This media has no downloadable payload"))?;
            let base = raw.get_base_message();
            let data = match msg.kind {
                MessageKind::Image => {
                    self.client
                        .download(
                            base.image_message
                                .as_deref()
                                .ok_or_else(|| anyhow!("missing image payload"))?,
                        )
                        .await?
                }
                MessageKind::Video => {
                    self.client
                        .download(
                            base.video_message
                                .as_deref()
                                .ok_or_else(|| anyhow!("missing video payload"))?,
                        )
                        .await?
                }
                MessageKind::Audio => {
                    self.client
                        .download(
                            base.audio_message
                                .as_deref()
                                .ok_or_else(|| anyhow!("missing audio payload"))?,
                        )
                        .await?
                }
                MessageKind::Sticker => {
                    let sticker = base
                        .sticker_message
                        .as_deref()
                        .ok_or_else(|| anyhow!("missing sticker payload"))?;
                    ensure_supported_sticker(sticker)?;
                    self.client.download(sticker).await?
                }
                _ => bail!("This is not playable media"),
            };
            crate::media_cache::atomic_write_cached(&path, &data).await?
        };
        let _ = self
            .ui_tx
            .send(UiEvent::MediaReady {
                path,
                kind: msg.kind,
                title: if msg.file_name.is_empty() {
                    msg.kind.to_string()
                } else {
                    msg.file_name
                },
            })
            .await;
        Ok(())
    }

    async fn open_url(&self, command: &Command) -> Result<()> {
        let id = require_param(command, 0)?;
        let text = self
            .storage
            .message(id.to_owned())
            .await?
            .map(|m| m.text.clone())
            .ok_or_else(|| anyhow!("message not found"))?;
        let url = Regex::new(r"https?://[^\s]+")?
            .find(&text)
            .ok_or_else(|| anyhow!("No URL found in message"))?
            .as_str()
            .to_owned();
        self.open_target(url).await?;
        self.text("Opened URL with the system application").await;
        Ok(())
    }

    async fn send_media_command(&self, command: &Command, kind: MessageKind) -> Result<()> {
        let chat = self.current_receiver.read().await.clone();
        if chat.is_empty() {
            bail!("{} only works in a chat", command_name(kind));
        }
        if command.params.is_empty() {
            bail!("Usage: {} /path/to/file", command_name(kind));
        }
        self.send_media(&chat, Path::new(&command.params.join(" ")), kind)
            .await
    }

    async fn send_media(&self, chat_id: &str, path: &Path, kind: MessageKind) -> Result<()> {
        ensure_connected(&self.client)?;
        let data = tokio::fs::read(path).await?;
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_owned();
        let mime = mime_guess::from_path(path)
            .first_or_octet_stream()
            .essence_str()
            .to_owned();
        let media_type = media_type(kind);
        let upload = self
            .client
            .upload(data, media_type, UploadOptions::default())
            .await?;
        let raw = build_media_message(upload, kind, &mime, &file_name)?;
        let jid: Jid = chat_id.parse()?;
        let response = self.client.send_message(jid, raw.clone()).await?;
        let own = self
            .client
            .get_pn()
            .await
            .map(|j| j.to_string())
            .unwrap_or_default();
        let contact_id = if chat_id.ends_with(GROUP_SUFFIX) {
            own.clone()
        } else {
            chat_id.to_owned()
        };
        let (contact_name, contact_short) = self
            .storage
            .resolve_contact(contact_id.clone(), contact_id.clone(), String::new())
            .await?;
        let msg = Message {
            id: response.message_id,
            chat_id: chat_id.into(),
            sender_id: own,
            contact_id: contact_id.clone(),
            contact_name,
            contact_short,
            timestamp: Utc::now().timestamp().max(0) as u64,
            from_me: true,
            text: media_display_text(kind, &file_name, ""),
            kind,
            mime_type: mime,
            file_name,
            raw_message: Some(Arc::new(raw)),
            ..Default::default()
        };
        self.storage.add_message(msg, false).await?;
        Ok(())
    }

    async fn revoke_message(&self, command: &Command) -> Result<()> {
        ensure_connected(&self.client)?;
        let id = require_param(command, 0)?;
        let msg = self
            .storage
            .message(id.to_owned())
            .await?
            .ok_or_else(|| anyhow!("message not found"))?;
        let chat: Jid = msg.chat_id.parse()?;
        let revoke_type = if msg.from_me {
            RevokeType::Sender
        } else {
            RevokeType::Admin {
                original_sender: msg.sender_id.parse()?,
            }
        };
        self.client
            .revoke_message(chat, msg.id.clone(), revoke_type)
            .await?;
        self.storage.mark_message_revoked(msg.id.clone()).await?;
        self.text(format!("revoked: {}", msg.id)).await;
        Ok(())
    }

    async fn current_group(&self) -> Result<Jid> {
        let id = self.current_receiver.read().await.clone();
        if !id.ends_with(GROUP_SUFFIX) {
            bail!("not a group");
        }
        id.parse().context("invalid group JID")
    }

    async fn create_group(&self, command: &Command) -> Result<()> {
        if command.params.is_empty() {
            bail!("Usage: create [user-id] [user-id] New Group Subject");
        }
        let (participant_params, subject) = split_group_params(&command.params);
        let participants = participant_params
            .iter()
            .map(|raw| raw.parse().map(GroupParticipantOptions::new))
            .collect::<Result<Vec<_>, _>>()?;
        let options = GroupCreateOptions::builder()
            .subject(subject)
            .participants(participants)
            .build();
        let result = self.client.groups().create_group(options).await?;
        let chat = Chat {
            id: result.metadata.id.to_string(),
            is_group: true,
            name: result.metadata.subject,
            last_message: Utc::now().timestamp(),
            ..Default::default()
        };
        self.storage.add_chat(chat.clone()).await?;
        self.text(format!("created new group {}", chat.id)).await;
        Ok(())
    }

    async fn update_participants(&self, command: &Command) -> Result<()> {
        let group = self.current_group().await?;
        if command.params.is_empty() {
            bail!("Usage: {} [user-id]", command.name);
        }
        let participants = command
            .params
            .iter()
            .map(|raw| raw.parse())
            .collect::<Result<Vec<Jid>, _>>()?;
        match command.name.as_str() {
            "add" => {
                self.client
                    .groups()
                    .add_participants(&group, &participants)
                    .await?;
            }
            "remove" => {
                self.client
                    .groups()
                    .remove_participants(&group, &participants)
                    .await?;
            }
            "admin" => {
                self.client
                    .groups()
                    .promote_participants(&group, &participants)
                    .await?
            }
            "removeadmin" => {
                self.client
                    .groups()
                    .demote_participants(&group, &participants)
                    .await?
            }
            _ => unreachable!(),
        }
        self.text(format!("updated members for {group}")).await;
        Ok(())
    }

    async fn update_subject(&self, command: &Command) -> Result<()> {
        let group = self.current_group().await?;
        let subject = command.params.join(" ");
        if subject.is_empty() {
            bail!("Usage: subject new-subject -> in group chat");
        }
        self.client
            .groups()
            .set_subject(&group, GroupSubject::new(subject.clone())?)
            .await?;
        self.storage
            .add_chat(Chat {
                id: group.to_string(),
                is_group: true,
                name: subject,
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    async fn open_target(&self, target: String) -> Result<()> {
        tokio::task::spawn_blocking(move || open::that(target))
            .await
            .context("system opener task failed")??;
        Ok(())
    }

    async fn copy_to_clipboard(&self, command: &Command) -> Result<()> {
        let value = require_param(command, 0)?.to_owned();
        tokio::task::spawn_blocking(move || arboard::Clipboard::new()?.set_text(value))
            .await
            .context("clipboard task failed")??;
        self.text("User ID copied").await;
        Ok(())
    }

    async fn paste_from_clipboard(&self) -> Result<()> {
        let text = tokio::task::spawn_blocking(move || arboard::Clipboard::new()?.get_text())
            .await
            .context("clipboard task failed")??;
        let _ = self.ui_tx.send(UiEvent::ClipboardText(text)).await;
        Ok(())
    }

    async fn show_image(&self, path: PathBuf) -> Result<()> {
        let show_command = self.config.general.show_command.clone();
        let output = tokio::task::spawn_blocking(move || -> Result<String> {
            let mut parts = shell_words::split(&show_command).context("Invalid show_command")?;
            if parts.is_empty() {
                bail!("show_command is empty");
            }
            let program = parts.remove(0);
            let output = std::process::Command::new(program)
                .args(parts)
                .arg(path)
                .output()?;
            if !output.status.success() {
                bail!("{}", String::from_utf8_lossy(&output.stderr));
            }
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        })
        .await
        .context("preview task failed")??;
        let _ = self.ui_tx.send(UiEvent::Preview(output)).await;
        Ok(())
    }
}

async fn supervise(
    config: Arc<Config>,
    command_rx: mpsc::Receiver<Command>,
    ui_tx: mpsc::Sender<UiEvent>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let _ = ui_tx
        .send(UiEvent::Status(SessionStatus {
            state: ConnectionState::Connecting,
            last_seen: String::new(),
        }))
        .await;
    let (protocol_tx, protocol_rx) = async_channel::unbounded();
    let initialize_task = TaskInfo {
        id: Command::new("initialize", Vec::new()).id,
        category: TaskCategory::Session,
        label: "initializing WhatsApp".into(),
    };
    let _ = ui_tx
        .send(UiEvent::TaskStarted(initialize_task.clone()))
        .await;
    let initialization = SessionManager::initialize(Arc::clone(&config), protocol_tx);
    let (client, connection_task) = tokio::select! {
        _ = shutdown_rx.changed() => {
            return;
        }
        result = initialization => match result {
            Ok(value) => {
                let _ = ui_tx
                    .send(UiEvent::TaskCompleted(initialize_task))
                    .await;
                value
            }
            Err(error) => {
                log::error!("WhatsApp initialization failed");
                let _ = ui_tx
                    .send(UiEvent::TaskFailed {
                        task: initialize_task,
                        error: error.to_string(),
                    })
                    .await;
                let _ = ui_tx.send(UiEvent::Status(SessionStatus::default())).await;
                return;
            }
        }
    };
    let account = client.get_pn().await.map(|jid| jid.to_string());
    let (storage, storage_task) = start_storage_actor(
        ui_tx.clone(),
        config.cache_file.clone(),
        account,
        config.general.history_sync_limit,
    )
    .await;
    let storage_shutdown = storage.clone();
    let context = SessionManager {
        storage,
        current_receiver: Arc::new(RwLock::new(String::new())),
        client,
        ui_tx,
        config,
    };
    run_workers(
        context,
        command_rx,
        protocol_rx,
        connection_task,
        shutdown_rx,
    )
    .await;
    if let Err(error) = storage_shutdown.close().await {
        log::error!("failed to close conversation cache cleanly: {error}");
    }
    let _ = storage_task.await;
}

async fn run_workers(
    context: SessionManager,
    command_rx: mpsc::Receiver<Command>,
    protocol_rx: async_channel::Receiver<ProtocolEvent>,
    connection_task: BotHandle,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let (session_tx, session_rx) = mpsc::channel(QUEUE_CAPACITY);
    let (history_tx, history_rx) = mpsc::channel(QUEUE_CAPACITY);
    let (transfer_tx, transfer_rx) = mpsc::channel(QUEUE_CAPACITY);
    let (integration_tx, integration_rx) = mpsc::channel(QUEUE_CAPACITY);
    let mut workers = JoinSet::new();
    workers.spawn(command_worker(context.clone(), session_rx));
    workers.spawn(history_worker(context.clone(), history_rx));
    workers.spawn(transfer_worker(context.clone(), transfer_rx));
    workers.spawn(integration_worker(context.clone(), integration_rx));
    workers.spawn(protocol_worker(
        context.clone(),
        protocol_rx,
        history_tx.clone(),
        integration_tx.clone(),
        shutdown_rx.clone(),
    ));
    workers.spawn(connection_monitor(context.ui_tx.clone(), connection_task));
    workers.spawn(command_router(
        context.clone(),
        command_rx,
        session_tx,
        history_tx,
        transfer_tx,
        integration_tx,
        shutdown_rx.clone(),
    ));

    let _ = shutdown_rx.changed().await;
    context.client.disconnect().await;
    while workers.join_next().await.is_some() {}
}

async fn connection_monitor(ui_tx: mpsc::Sender<UiEvent>, task: BotHandle) {
    match task.await {
        Ok(()) => {}
        Err(error) => {
            log::error!("WhatsApp connection task stopped");
            let _ = ui_tx
                .send(UiEvent::Error(format!(
                    "WhatsApp connection task stopped: {error}"
                )))
                .await;
        }
    }
    let _ = ui_tx.send(UiEvent::Status(SessionStatus::default())).await;
}

async fn command_router(
    context: SessionManager,
    mut rx: mpsc::Receiver<Command>,
    session_tx: mpsc::Sender<Command>,
    history_tx: mpsc::Sender<HistoryWork>,
    transfer_tx: mpsc::Sender<Command>,
    integration_tx: mpsc::Sender<IntegrationWork>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut conversations: HashMap<String, mpsc::Sender<Command>> = HashMap::new();
    let mut conversation_workers = JoinSet::new();
    let mut closing = false;
    loop {
        let command = if closing {
            rx.recv().await
        } else {
            tokio::select! {
                command = rx.recv() => command,
                _ = shutdown_rx.changed() => {
                    rx.close();
                    closing = true;
                    continue;
                }
            }
        };
        let Some(command) = command else { break };
        let category = command.category;
        let saturated = match category {
            TaskCategory::Session => session_tx.try_send(command).is_err(),
            TaskCategory::History => history_tx.try_send(HistoryWork::Command(command)).is_err(),
            TaskCategory::Transfer => transfer_tx.try_send(command).is_err(),
            TaskCategory::Integration => integration_tx
                .try_send(IntegrationWork::Command(command))
                .is_err(),
            TaskCategory::Conversation => {
                let key = conversation_key(&command, &context).await;
                let tx = conversations.entry(key).or_insert_with(|| {
                    let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
                    conversation_workers.spawn(command_worker(context.clone(), rx));
                    tx
                });
                tx.try_send(command).is_err()
            }
        };
        if saturated {
            log::warn!("command queue saturated category={}", category.label());
            let _ = context.ui_tx.send(UiEvent::QueueSaturated(category)).await;
        }
    }
    drop(conversations);
    while conversation_workers.join_next().await.is_some() {}
}

async fn conversation_key(command: &Command, context: &SessionManager) -> String {
    if command.name == "send" {
        return command.params.first().cloned().unwrap_or_default();
    }
    context.current_receiver.read().await.clone()
}

async fn command_worker(context: SessionManager, mut rx: mpsc::Receiver<Command>) {
    while let Some(command) = rx.recv().await {
        execute_task(&context, command).await;
    }
}

async fn transfer_worker(context: SessionManager, mut rx: mpsc::Receiver<Command>) {
    let permits = Arc::new(Semaphore::new(TRANSFER_LIMIT));
    let mut running = JoinSet::new();
    while let Some(command) = rx.recv().await {
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            break;
        };
        let context = context.clone();
        running.spawn(async move {
            let _permit = permit;
            execute_task(&context, command).await;
        });
        while running.try_join_next().is_some() {}
    }
    while running.join_next().await.is_some() {}
}

async fn integration_worker(context: SessionManager, mut rx: mpsc::Receiver<IntegrationWork>) {
    let mut running = JoinSet::new();
    while let Some(work) = rx.recv().await {
        let context = context.clone();
        running.spawn(async move {
            match work {
                IntegrationWork::Command(command) => execute_task(&context, command).await,
                IntegrationWork::Notification {
                    task,
                    title,
                    message,
                } => {
                    let _ = context.ui_tx.send(UiEvent::TaskStarted(task.clone())).await;
                    let config = Arc::clone(&context.config);
                    let result =
                        tokio::task::spawn_blocking(move || notify(&config, &title, &message))
                            .await
                            .map_err(anyhow::Error::from)
                            .and_then(|result| result);
                    finish_task(&context.ui_tx, task, result).await;
                }
            }
        });
        while running.try_join_next().is_some() {}
    }
    while running.join_next().await.is_some() {}
}

async fn execute_task(context: &SessionManager, command: Command) {
    let task = TaskInfo::from(&command);
    let was_connect = matches!(command.name.as_str(), "connect" | "login");
    let _ = context.ui_tx.send(UiEvent::TaskStarted(task.clone())).await;
    let result = context.execute(command).await;
    if was_connect && result.is_err() {
        let _ = context
            .ui_tx
            .send(UiEvent::Status(SessionStatus::default()))
            .await;
    }
    finish_task(&context.ui_tx, task, result).await;
}

async fn finish_task(ui_tx: &mpsc::Sender<UiEvent>, task: TaskInfo, result: Result<()>) {
    let event = match result {
        Ok(()) => {
            log::info!("task completed category={}", task.category.label());
            UiEvent::TaskCompleted(task)
        }
        Err(error) => {
            log::error!("task failed category={}", task.category.label());
            UiEvent::TaskFailed {
                task,
                error: error.to_string(),
            }
        }
    };
    let _ = ui_tx.send(event).await;
}

fn optional_u32(value: Option<u32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".into())
}

fn event_kind(event: &Event) -> &'static str {
    match event {
        Event::Connected(_) => "connected",
        Event::Disconnected(_) => "disconnected",
        Event::PairSuccess(_) => "pair_success",
        Event::PairError(_) => "pair_error",
        Event::LoggedOut(_) => "logged_out",
        Event::PairingQrCode { .. } => "pairing_qr_code",
        Event::PairingCode { .. } => "pairing_code",
        Event::QrScannedWithoutMultidevice(_) => "qr_scanned_without_multidevice",
        Event::ClientOutdated(_) => "client_outdated",
        Event::Message(_, _) => "message",
        Event::Receipt(_) => "receipt",
        Event::UndecryptableMessage(_) => "undecryptable_message",
        Event::Notification(_) => "notification",
        Event::ChatPresence(_) => "chat_presence",
        Event::Presence(_) => "presence",
        Event::PictureUpdate(_) => "picture_update",
        Event::UserAboutUpdate(_) => "user_about_update",
        Event::ContactUpdated(_) => "contact_updated",
        Event::ContactNumberChanged(_) => "contact_number_changed",
        Event::ContactSyncRequested(_) => "contact_sync_requested",
        Event::GroupUpdate(_) => "group_update",
        Event::ContactUpdate(_) => "contact_update",
        Event::IncomingCall(_) => "incoming_call",
        Event::PushNameUpdate(_) => "push_name_update",
        Event::SelfPushNameUpdated(_) => "self_push_name_updated",
        Event::PinUpdate(_) => "pin_update",
        Event::MuteUpdate(_) => "mute_update",
        Event::ArchiveUpdate(_) => "archive_update",
        Event::StarUpdate(_) => "star_update",
        Event::MarkChatAsReadUpdate(_) => "mark_chat_as_read_update",
        Event::DeleteChatUpdate(_) => "delete_chat_update",
        Event::DeleteMessageForMeUpdate(_) => "delete_message_for_me_update",
        Event::HistorySync(_) => "history_sync",
        Event::OfflineSyncPreview(_) => "offline_sync_preview",
        Event::OfflineSyncCompleted(_) => "offline_sync_completed",
        Event::DeviceListUpdate(_) => "device_list_update",
        Event::IdentityChange(_) => "identity_change",
        Event::BusinessStatusUpdate(_) => "business_status_update",
        Event::StreamReplaced(_) => "stream_replaced",
        Event::TemporaryBan(_) => "temporary_ban",
        Event::ConnectFailure(_) => "connect_failure",
        Event::StreamError(_) => "stream_error",
        Event::DisappearingModeChanged(_) => "disappearing_mode_changed",
        Event::NewsletterLiveUpdate(_) => "newsletter_live_update",
        Event::RawNode(_) => "raw_node",
        Event::MexNotification(_) => "mex_notification",
        _ => "unknown",
    }
}

async fn protocol_worker(
    context: SessionManager,
    rx: async_channel::Receiver<ProtocolEvent>,
    history_tx: mpsc::Sender<HistoryWork>,
    integration_tx: mpsc::Sender<IntegrationWork>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut connection_state = ConnectionEventState::new();
    loop {
        let envelope = tokio::select! {
            event = rx.recv() => event,
            _ = shutdown_rx.changed() => break,
        };
        let Ok(envelope) = envelope else { break };
        let ProtocolEvent { event, queued_at } = envelope;
        log::debug!(
            "protocol event processing type={} queue_wait_ms={} depth={}",
            event_kind(&event),
            queued_at.elapsed().as_millis(),
            rx.len()
        );
        if matches!(&*event, Event::HistorySync(_)) {
            let work = HistoryWork::Protocol {
                event,
                queued_at: Instant::now(),
            };
            let send = history_tx.send(work);
            tokio::select! {
                result = send => {
                    if result.is_err() {
                        break;
                    }
                    log::debug!(
                        "history event queued depth={}",
                        history_tx.max_capacity() - history_tx.capacity()
                    );
                }
                _ = shutdown_rx.changed() => break,
            }
            continue;
        }
        if let Err(error) =
            handle_event(event, &context, &integration_tx, &mut connection_state).await
        {
            log::error!("protocol event failed");
            let _ = context.ui_tx.send(UiEvent::Error(error.to_string())).await;
        }
    }
}

struct ConnectionEventState {
    pairing_allowed: bool,
}

impl ConnectionEventState {
    fn new() -> Self {
        Self {
            pairing_allowed: true,
        }
    }

    fn paired_or_connected(&mut self) {
        self.pairing_allowed = false;
    }

    fn disconnected(&mut self) {
        self.pairing_allowed = true;
    }

    fn accepts_pairing(&self) -> bool {
        self.pairing_allowed
    }
}

async fn history_worker(context: SessionManager, mut rx: mpsc::Receiver<HistoryWork>) {
    while let Some(work) = rx.recv().await {
        match work {
            HistoryWork::Command(command) => execute_task(&context, command).await,
            HistoryWork::Protocol { event, queued_at } => {
                let Event::HistorySync(lazy) = &*event else {
                    continue;
                };
                let started_at = Instant::now();
                let command = Command::new("history-sync", Vec::new());
                let task = TaskInfo {
                    id: command.id,
                    category: TaskCategory::History,
                    label: "syncing history".into(),
                };
                let _ = context.ui_tx.send(UiEvent::TaskStarted(task.clone())).await;
                log::info!(
                    "history sync started type={} order={} progress={} queue_wait_ms={}",
                    lazy.sync_type(),
                    optional_u32(lazy.chunk_order()),
                    optional_u32(lazy.progress()),
                    queued_at.elapsed().as_millis()
                );
                let result = match decode_history_sync(lazy) {
                    Ok(history) => {
                        let limit = history_limit(
                            lazy.sync_type(),
                            context.config.general.history_sync_limit,
                        );
                        handle_history(history, &context.client, &context.storage, limit).await
                    }
                    Err(error) => Err(error),
                };
                match &result {
                    Ok(()) => log::info!(
                        "history sync completed type={} duration_ms={}",
                        lazy.sync_type(),
                        started_at.elapsed().as_millis()
                    ),
                    Err(error) => log::error!(
                        "history sync failed type={} duration_ms={} error={}",
                        lazy.sync_type(),
                        started_at.elapsed().as_millis(),
                        crate::logging::redact(&error.to_string())
                    ),
                }
                finish_task(&context.ui_tx, task, result).await;
            }
        }
    }
}

fn decode_history_sync(lazy: &LazyHistorySync) -> Result<&wa::HistorySync> {
    lazy.get().ok_or_else(|| {
        anyhow!(
            "history sync decode failed (type={}, order={}, progress={})",
            lazy.sync_type(),
            optional_u32(lazy.chunk_order()),
            optional_u32(lazy.progress())
        )
    })
}

async fn handle_event(
    event: Arc<Event>,
    context: &SessionManager,
    integration_tx: &mpsc::Sender<IntegrationWork>,
    connection_state: &mut ConnectionEventState,
) -> Result<()> {
    let tx = &context.ui_tx;
    match &*event {
        Event::PairingQrCode { code, timeout } => {
            if !connection_state.accepts_pairing() {
                log::warn!("stale pairing QR ignored after connected or paired state");
                return Ok(());
            }
            log::info!("connection state=pairing");
            let rendered = qr::render(code)?;
            let _ = tx
                .send(UiEvent::Status(SessionStatus {
                    state: ConnectionState::Pairing,
                    last_seen: String::new(),
                }))
                .await;
            let _ = tx
                .send(UiEvent::Qr {
                    code: rendered,
                    expires_in: timeout.as_secs(),
                })
                .await;
        }
        Event::PairSuccess(_) => {
            connection_state.paired_or_connected();
            log::info!("connection state=connecting reason=pair_success");
            let _ = tx.send(UiEvent::ClearQr).await;
            let _ = tx
                .send(UiEvent::Status(SessionStatus {
                    state: ConnectionState::Connecting,
                    last_seen: String::new(),
                }))
                .await;
        }
        Event::Connected(_) => {
            connection_state.paired_or_connected();
            log::info!("connection state=connected");
            let _ = tx
                .send(UiEvent::Status(SessionStatus {
                    state: ConnectionState::Connected,
                    last_seen: String::new(),
                }))
                .await;
            if let Some(account) = context.client.get_pn().await {
                context.storage.set_account(account.to_string()).await?;
            }
            load_groups(&context.client, &context.storage).await?;
        }
        Event::Disconnected(_) => {
            connection_state.disconnected();
            log::info!("connection state=disconnected");
            let _ = tx.send(UiEvent::Status(SessionStatus::default())).await;
        }
        Event::LoggedOut(info) => {
            connection_state.disconnected();
            log::info!("connection state=disconnected reason=logged_out");
            let _ = tx.send(UiEvent::Status(SessionStatus::default())).await;
            let _ = tx
                .send(UiEvent::Text(format!("Logged out: {:?}", info.reason)))
                .await;
        }
        Event::PushNameUpdate(update) => {
            context
                .storage
                .update_push_name(
                    update.jid.to_string(),
                    update.old_push_name.clone(),
                    update.new_push_name.clone(),
                )
                .await?;
        }
        Event::MarkChatAsReadUpdate(update) => {
            let unread = usize::from(!update.action.read.unwrap_or(true));
            context
                .storage
                .update_chat_unread(update.jid.to_string(), unread)
                .await?;
        }
        Event::Message(raw, info) => {
            if let Some(revoke_id) = revoked_message_id(raw) {
                context.storage.mark_message_revoked(revoke_id).await?;
            } else if let Some(msg) =
                message_from_info(info, Arc::clone(raw), &context.storage).await
            {
                let selected = context.current_receiver.read().await.clone();
                let mark_unread = !msg.from_me && msg.chat_id != selected;
                context
                    .storage
                    .add_message(msg.clone(), mark_unread)
                    .await?;
                if mark_unread && msg.timestamp + 30 > Utc::now().timestamp().max(0) as u64 {
                    let command = Command::new("notification", Vec::new());
                    let work = IntegrationWork::Notification {
                        task: TaskInfo {
                            id: command.id,
                            category: TaskCategory::Integration,
                            label: "showing notification".into(),
                        },
                        title: msg.contact_short,
                        message: msg.text,
                    };
                    if integration_tx.try_send(work).is_err() {
                        let _ = tx
                            .send(UiEvent::QueueSaturated(TaskCategory::Integration))
                            .await;
                    }
                }
            }
        }
        Event::GroupUpdate(_) | Event::ContactUpdate(_) | Event::ContactUpdated(_) => {
            load_groups(&context.client, &context.storage).await?;
        }
        _ => {}
    }
    Ok(())
}

async fn load_groups(client: &Arc<Client>, storage: &StorageHandle) -> Result<()> {
    let Ok(groups) = client.groups().get_participating().await else {
        return Ok(());
    };
    for (jid, group) in groups {
        storage
            .add_chat(Chat {
                id: jid.to_string(),
                is_group: true,
                name: group.subject,
                ..Default::default()
            })
            .await?;
    }
    Ok(())
}

async fn handle_history(
    history: &wa::HistorySync,
    client: &Arc<Client>,
    storage: &StorageHandle,
    message_limit: usize,
) -> Result<()> {
    let mut processed_messages = 0_usize;
    let mut stored_messages = 0_usize;
    let mut limited_messages = 0_usize;
    let mut names = HashMap::new();
    for push in &history.pushnames {
        if let (Some(id), Some(name)) = (&push.id, &push.pushname)
            && !name.is_empty()
            && name != "-"
        {
            names.insert(id.clone(), name.clone());
        }
    }
    storage.update_push_names(names.clone()).await?;
    for id in names.keys().filter(|id| id.ends_with("@s.whatsapp.net")) {
        let (name, _) = storage
            .resolve_contact(id.clone(), names[id].clone(), names[id].clone())
            .await?;
        storage
            .add_chat(Chat {
                id: id.clone(),
                name,
                ..Default::default()
            })
            .await?;
    }
    storage.refresh_contact_names().await?;
    let candidate_ids = history
        .conversations
        .iter()
        .flat_map(|conversation| history_messages(conversation, message_limit))
        .filter_map(|historical| historical.message.as_ref())
        .filter_map(|web| web.key.id.clone())
        .collect::<Vec<_>>();
    let messages_needing_hydration = storage.messages_needing_hydration(candidate_ids).await?;
    for conversation in &history.conversations {
        let chat_id = if conversation.id.is_empty() {
            conversation.new_jid.clone().unwrap_or_default()
        } else {
            conversation.id.clone()
        };
        let Ok(chat_jid) = chat_id.parse::<Jid>() else {
            continue;
        };
        let name = conversation
            .name
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| conversation.display_name.clone().filter(|s| !s.is_empty()))
            .unwrap_or_else(|| chat_id.split('@').next().unwrap_or(&chat_id).to_owned());
        storage
            .add_chat(Chat {
                id: chat_id.clone(),
                is_group: chat_id.ends_with(GROUP_SUFFIX),
                name,
                unread: conversation.unread_count.unwrap_or(0) as usize,
                last_message: conversation
                    .last_msg_timestamp
                    .or(conversation.conversation_timestamp)
                    .unwrap_or(0) as i64,
                ..Default::default()
            })
            .await?;
        let selected_messages = history_messages(conversation, message_limit);
        limited_messages += conversation
            .messages
            .len()
            .saturating_sub(selected_messages.len());
        for historical in selected_messages {
            let Some(web) = &historical.message else {
                continue;
            };
            let Some(message_id) = web.key.id.as_ref() else {
                continue;
            };
            if !messages_needing_hydration.contains(message_id) {
                continue;
            }
            let Some(raw) = &web.message else { continue };
            let Some(info) = history_message_info(web, &chat_jid, client).await else {
                continue;
            };
            processed_messages += 1;
            if let Some(message) = message_from_info(&info, Arc::new(raw.clone()), storage).await {
                stored_messages +=
                    usize::from(storage.add_historical_message(message, false).await?);
            }
        }
        storage
            .update_chat_unread(chat_id, conversation.unread_count.unwrap_or(0) as usize)
            .await?;
    }
    log::info!(
        "history sync applied conversations={} processed={} stored={} limited={}",
        history.conversations.len(),
        processed_messages,
        stored_messages,
        limited_messages
    );
    Ok(())
}

fn history_limit(sync_type: i32, configured_limit: usize) -> usize {
    if sync_type == wa::history_sync::HistorySyncType::OnDemand as i32 {
        0
    } else {
        configured_limit
    }
}

fn history_messages(conversation: &wa::Conversation, limit: usize) -> Vec<&wa::HistorySyncMsg> {
    let mut messages = conversation.messages.iter().collect::<Vec<_>>();
    if limit == 0 || messages.len() <= limit {
        return messages;
    }
    messages.sort_by_key(|historical| {
        (
            historical
                .message
                .as_ref()
                .and_then(|message| message.message_timestamp)
                .unwrap_or_default(),
            historical.msg_order_id.unwrap_or_default(),
        )
    });
    messages.drain(..messages.len() - limit);
    messages
}

async fn history_message_info(
    web: &wa::WebMessageInfo,
    chat: &Jid,
    client: &Arc<Client>,
) -> Option<MessageInfo> {
    let from_me = web.key.from_me.unwrap_or(false);
    let sender = if let Some(participant) = &web.key.participant {
        participant.parse().ok()?
    } else if from_me {
        client.get_pn().await.unwrap_or_else(|| chat.clone())
    } else {
        chat.clone()
    };
    Some(MessageInfo {
        source: MessageSource {
            chat: chat.clone(),
            sender,
            is_from_me: from_me,
            is_group: chat.to_string().ends_with(GROUP_SUFFIX),
            ..Default::default()
        },
        id: web.key.id.clone().unwrap_or_default(),
        server_id: 0,
        r#type: String::new(),
        push_name: web.push_name.clone().unwrap_or_default(),
        timestamp: Utc
            .timestamp_opt(web.message_timestamp.unwrap_or(0) as i64, 0)
            .single()
            .unwrap_or_else(Utc::now),
        category: MessageCategory::default(),
        multicast: false,
        media_type: String::new(),
        edit: EditAttribute::default(),
        bot_info: None,
        meta_info: MsgMetaInfo::default(),
        verified_name: None,
        device_sent_meta: None,
        ephemeral_expiration: None,
        is_offline: true,
        unavailable_request_id: None,
    })
}

async fn message_from_info(
    info: &MessageInfo,
    raw: Arc<wa::Message>,
    storage: &StorageHandle,
) -> Option<Message> {
    let base = raw.get_base_message();
    let chat_id = info.source.chat.to_string();
    if chat_id.is_empty() {
        return None;
    }
    let contact_id = if info.source.is_group {
        info.source.sender.to_string()
    } else {
        chat_id.clone()
    };
    let fallback = first_non_empty(&[
        &info.push_name,
        info.source.sender.user.as_str(),
        &contact_id,
    ])
    .to_owned();
    let (contact_name, contact_short) = storage
        .resolve_contact(contact_id.clone(), fallback, info.push_name.clone())
        .await
        .ok()?;
    let mut msg = Message {
        id: info.id.clone(),
        chat_id,
        sender_id: info.source.sender.to_string(),
        contact_id: contact_id.clone(),
        contact_name,
        contact_short,
        timestamp: info.timestamp.timestamp().max(0) as u64,
        from_me: info.source.is_from_me,
        forwarded: is_forwarded(base),
        raw_message: None,
        ..Default::default()
    };
    if let Some(text) = raw.text_content() {
        msg.kind = MessageKind::Text;
        msg.text = text.into();
    } else if let Some(media) = &base.image_message {
        msg.kind = MessageKind::Image;
        msg.raw_message = Some(Arc::clone(&raw));
        msg.mime_type = media.mimetype.clone().unwrap_or_default();
        msg.text = media_display_text(msg.kind, "", media.caption.as_deref().unwrap_or(""));
    } else if let Some(media) = &base.video_message {
        msg.kind = MessageKind::Video;
        msg.raw_message = Some(Arc::clone(&raw));
        msg.mime_type = media.mimetype.clone().unwrap_or_default();
        msg.text = media_display_text(msg.kind, "", media.caption.as_deref().unwrap_or(""));
    } else if let Some(media) = &base.audio_message {
        msg.kind = MessageKind::Audio;
        msg.raw_message = Some(Arc::clone(&raw));
        msg.mime_type = media.mimetype.clone().unwrap_or_default();
        msg.text = media_display_text(msg.kind, "", "");
    } else if let Some(media) = &base.document_message {
        msg.kind = MessageKind::Document;
        msg.raw_message = Some(Arc::clone(&raw));
        msg.mime_type = media.mimetype.clone().unwrap_or_default();
        msg.file_name = media.file_name.clone().unwrap_or_default();
        msg.text = media_display_text(
            msg.kind,
            &msg.file_name,
            media.caption.as_deref().unwrap_or(""),
        );
    } else if let Some(media) = &base.sticker_message {
        msg.kind = MessageKind::Sticker;
        msg.raw_message = Some(Arc::clone(&raw));
        msg.mime_type = media
            .mimetype
            .clone()
            .unwrap_or_else(|| "image/webp".into());
        msg.file_name = format!("{}.webp", msg.id);
        msg.text = media_display_text(msg.kind, "", "");
    } else {
        return None;
    }
    Some(msg)
}

fn revoked_message_id(raw: &wa::Message) -> Option<String> {
    let protocol = raw.protocol_message.as_ref()?;
    if protocol.r#type != Some(wa::message::protocol_message::Type::Revoke as i32) {
        return None;
    }
    protocol.key.as_ref()?.id.clone()
}

fn is_forwarded(message: &wa::Message) -> bool {
    let base = message.get_base_message();
    base.extended_text_message
        .as_deref()
        .and_then(|m| m.context_info.as_deref())
        .or_else(|| {
            base.image_message
                .as_deref()
                .and_then(|m| m.context_info.as_deref())
        })
        .or_else(|| {
            base.video_message
                .as_deref()
                .and_then(|m| m.context_info.as_deref())
        })
        .or_else(|| {
            base.audio_message
                .as_deref()
                .and_then(|m| m.context_info.as_deref())
        })
        .or_else(|| {
            base.document_message
                .as_deref()
                .and_then(|m| m.context_info.as_deref())
        })
        .or_else(|| {
            base.sticker_message
                .as_deref()
                .and_then(|m| m.context_info.as_deref())
        })
        .and_then(|context| context.is_forwarded)
        .unwrap_or(false)
}

fn build_media_message(
    upload: UploadResponse,
    kind: MessageKind,
    mime: &str,
    file_name: &str,
) -> Result<wa::Message> {
    let common = (
        Some(upload.url),
        Some(upload.direct_path),
        Some(upload.media_key.to_vec()),
        Some(upload.file_enc_sha256.to_vec()),
        Some(upload.file_sha256.to_vec()),
        Some(upload.file_length),
        Some(upload.media_key_timestamp),
    );
    Ok(match kind {
        MessageKind::Image => wa::Message {
            image_message: Some(Box::new(wa::message::ImageMessage {
                url: common.0,
                direct_path: common.1,
                media_key: common.2,
                file_enc_sha256: common.3,
                file_sha256: common.4,
                file_length: common.5,
                media_key_timestamp: common.6,
                mimetype: Some(mime.into()),
                ..Default::default()
            })),
            ..Default::default()
        },
        MessageKind::Video => wa::Message {
            video_message: Some(Box::new(wa::message::VideoMessage {
                url: common.0,
                direct_path: common.1,
                media_key: common.2,
                file_enc_sha256: common.3,
                file_sha256: common.4,
                file_length: common.5,
                media_key_timestamp: common.6,
                mimetype: Some(mime.into()),
                ..Default::default()
            })),
            ..Default::default()
        },
        MessageKind::Audio => wa::Message {
            audio_message: Some(Box::new(wa::message::AudioMessage {
                url: common.0,
                direct_path: common.1,
                media_key: common.2,
                file_enc_sha256: common.3,
                file_sha256: common.4,
                file_length: common.5,
                media_key_timestamp: common.6,
                mimetype: Some(mime.into()),
                ptt: Some(false),
                ..Default::default()
            })),
            ..Default::default()
        },
        MessageKind::Document => wa::Message {
            document_message: Some(Box::new(wa::message::DocumentMessage {
                url: common.0,
                direct_path: common.1,
                media_key: common.2,
                file_enc_sha256: common.3,
                file_sha256: common.4,
                file_length: common.5,
                media_key_timestamp: common.6,
                mimetype: Some(mime.into()),
                title: Some(file_name.into()),
                file_name: Some(file_name.into()),
                ..Default::default()
            })),
            ..Default::default()
        },
        _ => bail!("unsupported media type"),
    })
}

fn media_type(kind: MessageKind) -> MediaType {
    match kind {
        MessageKind::Image => MediaType::Image,
        MessageKind::Video => MediaType::Video,
        MessageKind::Audio => MediaType::Audio,
        _ => MediaType::Document,
    }
}
fn command_name(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Image => "sendimage",
        MessageKind::Video => "sendvideo",
        MessageKind::Audio => "sendaudio",
        _ => "upload",
    }
}
fn media_display_text(kind: MessageKind, file_name: &str, caption: &str) -> String {
    let label = match kind {
        MessageKind::Image => "[IMAGE]",
        MessageKind::Video => "[VIDEO]",
        MessageKind::Audio => "[AUDIO]",
        MessageKind::Document => "[DOCUMENT]",
        MessageKind::Sticker => "[STICKER]",
        _ => "[FILE]",
    };
    [
        label,
        if kind == MessageKind::Document {
            file_name
        } else {
            ""
        },
        caption,
    ]
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect::<Vec<_>>()
    .join(" ")
}

pub fn download_file_name(msg: &Message) -> String {
    if !msg.file_name.is_empty() {
        let normalized = msg.file_name.replace('\\', "/");
        if let Some(name) = normalized.rsplit('/').next()
            && !name.is_empty()
            && name != "."
            && name != ".."
        {
            return crate::media_cache::safe_file_name(name, &msg.id);
        }
    }
    let extension = mime_guess::get_mime_extensions_str(&msg.mime_type)
        .and_then(|all| all.first())
        .map(|ext| format!(".{ext}"))
        .unwrap_or_default();
    crate::media_cache::safe_file_name(&format!("{}{extension}", msg.id), "media")
}

fn ensure_supported_sticker(sticker: &wa::message::StickerMessage) -> Result<()> {
    if sticker.is_lottie.unwrap_or(false) {
        bail!("Lottie stickers are not supported yet; WebP stickers can be viewed")
    }
    Ok(())
}

fn require_param(command: &Command, index: usize) -> Result<&str> {
    command
        .params
        .get(index)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("Usage: {} requires more parameters", command.name))
}
fn ensure_connected(client: &Client) -> Result<()> {
    if client.is_connected() {
        Ok(())
    } else {
        bail!("not connected to WhatsApp")
    }
}
fn first_non_empty<'a>(values: &'a [&'a str]) -> &'a str {
    values.iter().copied().find(|v| !v.is_empty()).unwrap_or("")
}
fn database_files(path: &Path) -> Vec<PathBuf> {
    vec![
        path.to_owned(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ]
}

fn reset_marker(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.reset", path.display()))
}

async fn finish_pending_session_reset(path: &Path) -> Result<()> {
    let marker = reset_marker(path);
    if !tokio::fs::try_exists(&marker).await? {
        return Ok(());
    }
    for database_file in database_files(path) {
        match tokio::fs::remove_file(&database_file).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to finish reset of {}", database_file.display())
                });
            }
        }
    }
    tokio::fs::remove_file(&marker)
        .await
        .with_context(|| format!("failed to remove reset marker {}", marker.display()))
}

fn split_group_params(params: &[String]) -> (&[String], String) {
    let split = params
        .iter()
        .position(|param| !param.ends_with("@s.whatsapp.net"))
        .unwrap_or(params.len());
    if split == params.len() {
        (&params[..0], params.join(" "))
    } else {
        (&params[..split], params[split..].join(" "))
    }
}

fn notify(config: &Config, title: &str, message: &str) -> Result<()> {
    if !config.general.enable_notifications {
        return Ok(());
    }
    if config.general.use_terminal_bell {
        print!("\x07");
        return Ok(());
    }
    notify_rust::Notification::new()
        .summary(title)
        .body(message)
        .timeout(notify_rust::Timeout::Milliseconds(
            (config.general.notification_timeout.max(0) * 1000) as u32,
        ))
        .show()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn download_name_sanitizes_unix_traversal() {
        assert_eq!(
            download_file_name(&Message {
                id: "msg-1".into(),
                file_name: "../../.ssh/authorized_keys".into(),
                ..Default::default()
            }),
            "authorized_keys"
        );
    }
    #[test]
    fn download_name_sanitizes_windows_traversal() {
        assert_eq!(
            download_file_name(&Message {
                id: "msg-2".into(),
                file_name: r"..\..\AppData\startup.bat".into(),
                ..Default::default()
            }),
            "startup.bat"
        );
    }
    #[test]
    fn download_name_falls_back_for_invalid_name() {
        assert_eq!(
            download_file_name(&Message {
                id: "msg-3".into(),
                file_name: "..".into(),
                mime_type: "image/png".into(),
                ..Default::default()
            }),
            "msg-3.png"
        );
    }

    #[test]
    fn group_params_split_participants_from_subject() {
        let params = vec![
            "5511999999999@s.whatsapp.net".into(),
            "5511888888888@s.whatsapp.net".into(),
            "Rust".into(),
            "Group".into(),
        ];
        let (participants, subject) = split_group_params(&params);
        assert_eq!(participants, &params[..2]);
        assert_eq!(subject, "Rust Group");
    }

    #[test]
    fn group_params_preserve_legacy_all_jids_subject_fallback() {
        let params = vec![
            "5511999999999@s.whatsapp.net".into(),
            "5511888888888@s.whatsapp.net".into(),
        ];
        let (participants, subject) = split_group_params(&params);
        assert!(participants.is_empty());
        assert_eq!(subject, params.join(" "));
    }

    #[test]
    fn reset_marker_sits_beside_database() {
        assert_eq!(
            reset_marker(Path::new("/config/session-rust.db")),
            PathBuf::from("/config/session-rust.db.reset")
        );
    }

    #[tokio::test]
    async fn background_shutdown_waits_for_a_normal_stop() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let supervisor = tokio::spawn(async move {
            let _ = shutdown_rx.changed().await;
        });
        let timed_out = BackgroundHandle {
            shutdown_tx,
            supervisor,
        }
        .shutdown_with_timeout(Duration::from_millis(100))
        .await;
        assert!(!timed_out);
    }

    #[tokio::test]
    async fn background_shutdown_cancels_a_stuck_supervisor_after_timeout() {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let supervisor = tokio::spawn(std::future::pending());
        let timed_out = BackgroundHandle {
            shutdown_tx,
            supervisor,
        }
        .shutdown_with_timeout(Duration::from_millis(10))
        .await;
        assert!(timed_out);
    }

    #[tokio::test]
    async fn transfer_semaphore_never_allows_more_than_two_jobs() {
        let permits = Arc::new(Semaphore::new(TRANSFER_LIMIT));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let mut jobs = JoinSet::new();
        for _ in 0..8 {
            let permits = Arc::clone(&permits);
            let active = Arc::clone(&active);
            let maximum = Arc::clone(&maximum);
            jobs.spawn(async move {
                let _permit = permits.acquire_owned().await.unwrap();
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(count, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(2)).await;
                active.fetch_sub(1, Ordering::SeqCst);
            });
        }
        while jobs.join_next().await.is_some() {}
        assert_eq!(maximum.load(Ordering::SeqCst), TRANSFER_LIMIT);
    }

    #[tokio::test]
    async fn protocol_handler_keeps_fifo_order_beyond_the_old_capacity() {
        let (tx, rx) = async_channel::unbounded();
        let handler = OrderedEventHandler { tx };
        for index in 0..256 {
            handler.handle_event(Arc::new(Event::PairingQrCode {
                code: index.to_string(),
                timeout: Duration::from_secs(30),
            }));
        }
        handler.handle_event(Arc::new(Event::Connected(
            whatsapp_rust::types::events::Connected,
        )));

        for expected in 0..256 {
            let envelope = rx.recv().await.unwrap();
            let Event::PairingQrCode { code, .. } = &*envelope.event else {
                panic!("event order changed");
            };
            assert_eq!(code, &expected.to_string());
        }
        assert!(matches!(
            &*rx.recv().await.unwrap().event,
            Event::Connected(_)
        ));
    }

    #[test]
    fn event_log_metadata_omits_qr_message_and_payload_contents() {
        let secrets = [
            "private-qr-code",
            "private message text",
            "5511999999999@s.whatsapp.net",
            "https://mmg.whatsapp.net/private-media",
            "raw-payload",
        ];
        let qr = Event::PairingQrCode {
            code: secrets[0].into(),
            timeout: Duration::from_secs(30),
        };
        let message = Event::Message(
            Arc::new(wa::Message {
                conversation: Some(secrets[1..].join(" ")),
                ..Default::default()
            }),
            Arc::new(MessageInfo::default()),
        );
        let line = format!(
            "protocol event type={} next_type={}",
            event_kind(&qr),
            event_kind(&message)
        );
        for secret in secrets {
            assert!(!line.contains(secret));
        }
    }

    #[test]
    fn pairing_requires_a_disconnect_after_connected_or_pair_success() {
        let mut state = ConnectionEventState::new();
        assert!(state.accepts_pairing());
        state.paired_or_connected();
        assert!(!state.accepts_pairing());
        state.disconnected();
        assert!(state.accepts_pairing());
    }

    #[test]
    fn lottie_stickers_have_an_actionable_error() {
        let sticker = wa::message::StickerMessage {
            is_lottie: Some(true),
            ..Default::default()
        };
        let error = ensure_supported_sticker(&sticker).unwrap_err().to_string();
        assert!(error.contains("Lottie"));
        assert!(error.contains("WebP"));
    }

    #[test]
    fn automatic_history_keeps_only_the_most_recent_messages() {
        let mut conversation = wa::Conversation {
            id: "chat".into(),
            ..Default::default()
        };
        for timestamp in (0..500_u64).rev() {
            conversation.messages.push(wa::HistorySyncMsg {
                message: Some(wa::WebMessageInfo {
                    message_timestamp: Some(timestamp),
                    ..Default::default()
                }),
                msg_order_id: Some(timestamp),
            });
        }
        let selected = history_messages(&conversation, 200);
        assert_eq!(selected.len(), 200);
        assert_eq!(
            selected
                .first()
                .unwrap()
                .message
                .as_ref()
                .unwrap()
                .message_timestamp,
            Some(300)
        );
        assert_eq!(
            selected
                .last()
                .unwrap()
                .message
                .as_ref()
                .unwrap()
                .message_timestamp,
            Some(499)
        );
    }

    #[test]
    fn on_demand_history_is_not_limited() {
        assert_eq!(
            history_limit(wa::history_sync::HistorySyncType::Recent as i32, 200),
            200
        );
        assert_eq!(
            history_limit(wa::history_sync::HistorySyncType::OnDemand as i32, 200),
            0
        );
    }

    #[test]
    fn history_decode_failure_contains_only_batch_metadata() {
        let lazy = LazyHistorySync::new(vec![0xff].into(), 3, Some(7), Some(42));
        let error = decode_history_sync(&lazy).unwrap_err().to_string();
        assert_eq!(
            error,
            "history sync decode failed (type=3, order=7, progress=42)"
        );
    }

    #[tokio::test]
    async fn history_decode_failure_emits_task_failed() {
        let lazy = LazyHistorySync::new(vec![0xff].into(), 3, Some(7), Some(42));
        let error = decode_history_sync(&lazy).unwrap_err();
        let (ui_tx, mut ui_rx) = mpsc::channel(1);
        let task = TaskInfo {
            id: 1,
            category: TaskCategory::History,
            label: "syncing history".into(),
        };
        finish_task(&ui_tx, task, Err(error)).await;
        let UiEvent::TaskFailed { error, .. } = ui_rx.recv().await.unwrap() else {
            panic!("decode failure was reported as success");
        };
        assert_eq!(
            error,
            "history sync decode failed (type=3, order=7, progress=42)"
        );
    }

    #[test]
    fn http_agent_disables_idle_connection_pool_without_changing_buffers() {
        let agent = http_agent_without_idle_pool();
        assert_eq!(agent.config().input_buffer_size(), 16 * 1024);
        assert_eq!(agent.config().output_buffer_size(), 16 * 1024);
        assert_eq!(agent.config().max_idle_connections(), 0);
        assert_eq!(agent.config().max_idle_connections_per_host(), 0);
    }

    #[tokio::test]
    async fn text_drops_raw_proto_while_media_retains_it() {
        let (ui_tx, _ui_rx) = mpsc::channel(QUEUE_CAPACITY);
        let (storage, storage_task) = start_storage_actor(ui_tx, PathBuf::new(), None, 200).await;
        let chat: Jid = "123@s.whatsapp.net".parse().unwrap();
        let info = MessageInfo {
            source: MessageSource {
                chat: chat.clone(),
                sender: chat,
                ..Default::default()
            },
            id: "text".into(),
            timestamp: Utc::now(),
            ..Default::default()
        };
        let text = message_from_info(
            &info,
            Arc::new(wa::Message {
                conversation: Some("private content".into()),
                ..Default::default()
            }),
            &storage,
        )
        .await
        .unwrap();
        assert!(text.raw_message.is_none());

        let mut media_info = info;
        media_info.id = "image".into();
        let image = message_from_info(
            &media_info,
            Arc::new(wa::Message {
                image_message: Some(Box::new(wa::message::ImageMessage::default())),
                ..Default::default()
            }),
            &storage,
        )
        .await
        .unwrap();
        assert!(image.raw_message.is_some());

        let mut sticker_info = media_info;
        sticker_info.id = "sticker".into();
        let sticker = message_from_info(
            &sticker_info,
            Arc::new(wa::Message {
                sticker_message: Some(Box::new(wa::message::StickerMessage {
                    mimetype: Some("image/webp".into()),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            &storage,
        )
        .await
        .unwrap();
        assert_eq!(sticker.kind, MessageKind::Sticker);
        assert_eq!(sticker.mime_type, "image/webp");
        assert_eq!(sticker.file_name, "sticker.webp");
        assert!(sticker.raw_message.is_some());
        storage.close().await.unwrap();
        storage_task.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_session_lane_processes_commands_in_fifo_order() {
        let (tx, mut rx) = mpsc::channel(QUEUE_CAPACITY);
        let worker = tokio::spawn(async move {
            let mut order = Vec::new();
            while let Some(value) = rx.recv().await {
                order.push(value);
            }
            order
        });
        for value in ["connect", "logout", "reset"] {
            tx.send(value).await.unwrap();
        }
        drop(tx);
        assert_eq!(worker.await.unwrap(), ["connect", "logout", "reset"]);
    }

    #[tokio::test]
    async fn conversation_lanes_keep_local_order_while_other_chats_progress() {
        let (chat_a_tx, mut chat_a_rx) = mpsc::channel(QUEUE_CAPACITY);
        let (chat_b_tx, mut chat_b_rx) = mpsc::channel(QUEUE_CAPACITY);
        let (events_tx, mut events_rx) = mpsc::channel(QUEUE_CAPACITY);
        let (a_started_tx, a_started_rx) = tokio::sync::oneshot::channel();
        let release_a = Arc::new(tokio::sync::Notify::new());

        let a_events = events_tx.clone();
        let a_release = Arc::clone(&release_a);
        let chat_a = tokio::spawn(async move {
            let first = chat_a_rx.recv().await.unwrap();
            let _ = a_started_tx.send(());
            a_release.notified().await;
            a_events.send(first).await.unwrap();
            a_events
                .send(chat_a_rx.recv().await.unwrap())
                .await
                .unwrap();
        });
        let b_events = events_tx;
        let chat_b = tokio::spawn(async move {
            b_events
                .send(chat_b_rx.recv().await.unwrap())
                .await
                .unwrap();
        });

        chat_a_tx.send("a-1").await.unwrap();
        chat_a_tx.send("a-2").await.unwrap();
        a_started_rx.await.unwrap();
        chat_b_tx.send("b-1").await.unwrap();
        assert_eq!(events_rx.recv().await, Some("b-1"));
        release_a.notify_one();
        assert_eq!(events_rx.recv().await, Some("a-1"));
        assert_eq!(events_rx.recv().await, Some("a-2"));
        chat_a.await.unwrap();
        chat_b.await.unwrap();
    }
}
