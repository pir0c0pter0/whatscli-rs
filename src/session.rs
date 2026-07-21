use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{TimeZone, Utc};
use regex::Regex;
use tokio::sync::{Mutex, RwLock, mpsc};
use whatsapp_rust::bot::Bot;
use whatsapp_rust::download::MediaType;
use whatsapp_rust::proto_helpers::MessageExt;
use whatsapp_rust::store::SqliteStore;
use whatsapp_rust::transport::{TokioWebSocketTransportFactory, UreqHttpClient};
use whatsapp_rust::types::events::Event;
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
    Chat, Command, ConnectionState, Contact, GROUP_SUFFIX, Message, MessageKind, SessionStatus,
    UiEvent,
};
use crate::qr;
use crate::storage::MessageDatabase;

pub struct SessionManager {
    pub db: Arc<Mutex<MessageDatabase>>,
    current_receiver: Arc<RwLock<String>>,
    client: Arc<Client>,
    command_rx: mpsc::UnboundedReceiver<Command>,
    ui_tx: mpsc::UnboundedSender<UiEvent>,
    config: Arc<Config>,
}

impl SessionManager {
    pub async fn start(
        config: Arc<Config>,
    ) -> Result<(
        mpsc::UnboundedSender<Command>,
        mpsc::UnboundedReceiver<UiEvent>,
        Arc<Mutex<MessageDatabase>>,
    )> {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (ui_tx, ui_rx) = mpsc::unbounded_channel();
        let _ = ui_tx.send(UiEvent::Status(SessionStatus {
            state: ConnectionState::Connecting,
            last_seen: String::new(),
        }));
        let db = Arc::new(Mutex::new(MessageDatabase::default()));
        let current_receiver = Arc::new(RwLock::new(String::new()));

        finish_pending_session_reset(&config.session_file).await?;
        let session_path = config.session_file.to_string_lossy();
        let store = Arc::new(SqliteStore::new(&session_path).await.with_context(|| {
            format!(
                "failed to open session database {}",
                config.session_file.display()
            )
        })?);
        let event_db = Arc::clone(&db);
        let event_current = Arc::clone(&current_receiver);
        let event_tx = ui_tx.clone();
        let event_config = Arc::clone(&config);
        let mut bot = Bot::builder()
            .with_backend(store)
            .with_transport_factory(TokioWebSocketTransportFactory::new())
            .with_http_client(UreqHttpClient::new())
            .with_runtime(TokioRuntime)
            .on_event(move |event, client| {
                let db = Arc::clone(&event_db);
                let current = Arc::clone(&event_current);
                let tx = event_tx.clone();
                let config = Arc::clone(&event_config);
                async move {
                    if let Err(error) =
                        handle_event(event, client, db, current, tx.clone(), config).await
                    {
                        let _ = tx.send(UiEvent::Error(error.to_string()));
                    }
                }
            })
            .build()
            .await?;
        let client = bot.client();
        let handle = bot.run().await?;
        let ended_tx = ui_tx.clone();
        tokio::spawn(async move {
            let result = handle.await;
            if let Err(error) = result {
                let _ = ended_tx.send(UiEvent::Error(format!(
                    "WhatsApp connection task stopped: {error}"
                )));
            }
            let _ = ended_tx.send(UiEvent::Status(SessionStatus::default()));
        });

        let mut manager = Self {
            db: Arc::clone(&db),
            current_receiver,
            client,
            command_rx,
            ui_tx,
            config,
        };
        tokio::spawn(async move {
            manager.run().await;
        });
        Ok((command_tx, ui_rx, db))
    }

    async fn run(&mut self) {
        while let Some(command) = self.command_rx.recv().await {
            let was_connect = matches!(command.name.as_str(), "connect" | "login");
            if let Err(error) = self.execute(command).await {
                if was_connect {
                    let _ = self.ui_tx.send(UiEvent::Status(SessionStatus::default()));
                }
                let _ = self.ui_tx.send(UiEvent::Error(error.to_string()));
            }
        }
        self.client.disconnect().await;
    }

    async fn execute(&self, command: Command) -> Result<()> {
        match command.name.as_str() {
            "select" => {
                let id = require_param(&command, 0)?;
                *self.current_receiver.write().await = id.to_owned();
                let _ = self.ui_tx.send(UiEvent::Refresh);
            }
            "connect" | "login" => {
                if !self.client.is_connected() {
                    let _ = self.ui_tx.send(UiEvent::Status(SessionStatus {
                        state: ConnectionState::Connecting,
                        last_seen: String::new(),
                    }));
                    self.client.connect().await?;
                }
                self.text("Successfully connected to WhatsApp");
            }
            "disconnect" => self.client.disconnect().await,
            "logout" => {
                self.client.logout().await?;
                self.text("Successfully logged out");
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
                self.text(self.db.lock().await.message_info(id));
            }
            "download" => self.download_command(&command, false, false).await?,
            "open" => self.download_command(&command, true, false).await?,
            "show" => self.download_command(&command, true, true).await?,
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
                self.text(format!("left group {group}"));
            }
            "create" => self.create_group(&command).await?,
            "add" | "remove" | "admin" | "removeadmin" => {
                self.update_participants(&command).await?
            }
            "subject" => self.update_subject(&command).await?,
            "colorlist" => {
                let _ = self.ui_tx.send(UiEvent::ColorList);
            }
            other => bail!("Unknown command: {other}"),
        }
        Ok(())
    }

    fn text(&self, text: impl Into<String>) {
        let _ = self.ui_tx.send(UiEvent::Text(text.into()));
    }

    async fn reset_session(&self) -> Result<()> {
        self.client.logout().await?;
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
        });
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
        let mut db = self.db.lock().await;
        let msg = Message {
            id: response.message_id,
            chat_id: chat_id.into(),
            sender_id: own,
            contact_id: contact_id.clone(),
            contact_name: db.id_name(&contact_id),
            contact_short: db.id_short(&contact_id),
            timestamp: Utc::now().timestamp().max(0) as u64,
            from_me: true,
            text: text.into(),
            kind: MessageKind::Text,
            raw_message: Some(Arc::new(wa::Message {
                conversation: Some(text.into()),
                ..Default::default()
            })),
            ..Default::default()
        };
        db.add_message(msg.clone(), false);
        drop(db);
        let _ = self.ui_tx.send(UiEvent::Message(Box::new(msg)));
        Ok(())
    }

    async fn load_backlog(&self) -> Result<()> {
        ensure_connected(&self.client)?;
        let chat_id = self.current_receiver.read().await.clone();
        if chat_id.is_empty() {
            bail!("Usage: backlog -> only works in a chat");
        }
        let oldest = self.db.lock().await.oldest_message(&chat_id).cloned()
            .ok_or_else(|| anyhow!("No local message anchor found yet. Wait for history sync, then try /backlog again."))?;
        let jid: Jid = chat_id.parse().context("invalid JID")?;
        self.text("Retrieving message history...");
        self.client
            .fetch_message_history(
                &jid,
                &oldest.id,
                oldest.from_me,
                oldest.timestamp as i64 * 1000,
                self.config.general.backlog_msg_quantity,
            )
            .await?;
        self.text("Requested older messages from WhatsApp. Waiting for sync response.");
        Ok(())
    }

    async fn mark_current_chat_read(&self) -> Result<()> {
        ensure_connected(&self.client)?;
        let chat_id = self.current_receiver.read().await.clone();
        if chat_id.is_empty() {
            bail!("Usage: read -> only works in a chat");
        }
        let chat: Jid = chat_id.parse().context("invalid JID")?;
        let cleared = self.db.lock().await.mark_chat_read(&chat_id);
        if cleared.is_empty() {
            self.text("No unread messages in current chat");
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
        let _ = self.ui_tx.send(UiEvent::Refresh);
        Ok(())
    }

    async fn download_command(&self, command: &Command, preview: bool, show: bool) -> Result<()> {
        let id = require_param(command, 0)?;
        let msg = self
            .db
            .lock()
            .await
            .message(id)
            .cloned()
            .ok_or_else(|| anyhow!("message not found"))?;
        if show && msg.kind != MessageKind::Image {
            bail!("show only works for image messages");
        }
        let path = self.download_message(&msg, preview).await?;
        if show {
            let _ = self
                .ui_tx
                .send(UiEvent::ShowImage(path.to_string_lossy().into()));
        } else if preview {
            let _ = self
                .ui_tx
                .send(UiEvent::Open(path.to_string_lossy().into()));
        } else {
            self.text(format!("-> {}", path.display()));
        }
        Ok(())
    }

    async fn download_message(&self, msg: &Message, preview: bool) -> Result<PathBuf> {
        ensure_connected(&self.client)?;
        let raw = msg
            .raw_message
            .as_ref()
            .ok_or_else(|| anyhow!("This is not a downloadable message"))?;
        let base = raw.get_base_message();
        let base_dir = if preview {
            &self.config.general.preview_path
        } else {
            &self.config.general.download_path
        };
        tokio::fs::create_dir_all(base_dir).await?;
        let path = base_dir.join(download_file_name(msg));
        if path.exists() {
            return Ok(path);
        }
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
            _ => bail!("This is not a downloadable message"),
        };
        tokio::fs::write(&path, data).await?;
        Ok(path)
    }

    async fn open_url(&self, command: &Command) -> Result<()> {
        let id = require_param(command, 0)?;
        let text = self
            .db
            .lock()
            .await
            .message(id)
            .map(|m| m.text.clone())
            .ok_or_else(|| anyhow!("message not found"))?;
        let url = Regex::new(r"https?://[^\s]+")?
            .find(&text)
            .ok_or_else(|| anyhow!("No URL found in message"))?;
        let _ = self.ui_tx.send(UiEvent::Open(url.as_str().into()));
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
        let mut db = self.db.lock().await;
        let msg = Message {
            id: response.message_id,
            chat_id: chat_id.into(),
            sender_id: own,
            contact_id: contact_id.clone(),
            contact_name: db.id_name(&contact_id),
            contact_short: db.id_short(&contact_id),
            timestamp: Utc::now().timestamp().max(0) as u64,
            from_me: true,
            text: media_display_text(kind, &file_name, ""),
            kind,
            mime_type: mime,
            file_name,
            raw_message: Some(Arc::new(raw)),
            ..Default::default()
        };
        db.add_message(msg.clone(), false);
        drop(db);
        let _ = self.ui_tx.send(UiEvent::Message(Box::new(msg)));
        Ok(())
    }

    async fn revoke_message(&self, command: &Command) -> Result<()> {
        ensure_connected(&self.client)?;
        let id = require_param(command, 0)?;
        let msg = self
            .db
            .lock()
            .await
            .message(id)
            .cloned()
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
        self.db.lock().await.mark_message_revoked(&msg.id);
        let _ = self.ui_tx.send(UiEvent::Refresh);
        self.text(format!("revoked: {}", msg.id));
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
        self.db.lock().await.add_chat(chat.clone());
        let _ = self.ui_tx.send(UiEvent::Refresh);
        self.text(format!("created new group {}", chat.id));
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
        self.text(format!("updated members for {group}"));
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
        self.db.lock().await.add_chat(Chat {
            id: group.to_string(),
            is_group: true,
            name: subject,
            ..Default::default()
        });
        let _ = self.ui_tx.send(UiEvent::Refresh);
        Ok(())
    }
}

async fn handle_event(
    event: Arc<Event>,
    client: Arc<Client>,
    db: Arc<Mutex<MessageDatabase>>,
    current: Arc<RwLock<String>>,
    tx: mpsc::UnboundedSender<UiEvent>,
    config: Arc<Config>,
) -> Result<()> {
    match &*event {
        Event::PairingQrCode { code, timeout } => {
            let rendered = qr::render(code)?;
            let _ = tx.send(UiEvent::Status(SessionStatus {
                state: ConnectionState::Pairing,
                last_seen: String::new(),
            }));
            let _ = tx.send(UiEvent::Qr {
                code: rendered,
                expires_in: timeout.as_secs(),
            });
        }
        Event::Connected(_) => {
            let _ = tx.send(UiEvent::Status(SessionStatus {
                state: ConnectionState::Connected,
                last_seen: String::new(),
            }));
            load_groups(&client, &db).await;
            let _ = tx.send(UiEvent::Refresh);
        }
        Event::Disconnected(_) => {
            let _ = tx.send(UiEvent::Status(SessionStatus::default()));
        }
        Event::LoggedOut(info) => {
            let _ = tx.send(UiEvent::Status(SessionStatus::default()));
            let _ = tx.send(UiEvent::Text(format!("Logged out: {:?}", info.reason)));
        }
        Event::PushNameUpdate(update) => {
            let id = update.jid.to_string();
            let mut database = db.lock().await;
            database.update_push_name(&id, &update.old_push_name, &update.new_push_name);
            if id.ends_with("@s.whatsapp.net") {
                let name = database
                    .get_contact(&id)
                    .map(|contact| contact.name.clone())
                    .unwrap_or_else(|| update.new_push_name.clone());
                database.add_chat(Chat {
                    id,
                    name,
                    ..Default::default()
                });
            }
            drop(database);
            let _ = tx.send(UiEvent::Refresh);
        }
        Event::Message(raw, info) => {
            if let Some(revoke_id) = revoked_message_id(raw) {
                db.lock().await.mark_message_revoked(&revoke_id);
                let _ = tx.send(UiEvent::Refresh);
            } else if let Some(msg) = message_from_info(info, Arc::clone(raw), &db).await {
                let selected = current.read().await.clone();
                let mark_unread = !msg.from_me && msg.chat_id != selected;
                let is_new = db.lock().await.add_message(msg.clone(), mark_unread);
                if msg.chat_id == selected && is_new {
                    let _ = tx.send(UiEvent::Message(Box::new(msg.clone())));
                } else {
                    let _ = tx.send(UiEvent::Refresh);
                }
                if mark_unread && msg.timestamp + 30 > Utc::now().timestamp().max(0) as u64 {
                    notify(&config, &msg.contact_short, &msg.text)?;
                }
            }
        }
        Event::HistorySync(lazy) => {
            if let Some(history) = lazy.get() {
                handle_history(history, &client, &db).await;
            }
            let _ = tx.send(UiEvent::Refresh);
        }
        Event::GroupUpdate(_) | Event::ContactUpdate(_) | Event::ContactUpdated(_) => {
            load_groups(&client, &db).await;
            let _ = tx.send(UiEvent::Refresh);
        }
        _ => {}
    }
    Ok(())
}

async fn load_groups(client: &Arc<Client>, db: &Arc<Mutex<MessageDatabase>>) {
    let Ok(groups) = client.groups().get_participating().await else {
        return;
    };
    let mut db = db.lock().await;
    for (jid, group) in groups {
        db.add_chat(Chat {
            id: jid.to_string(),
            is_group: true,
            name: group.subject,
            ..Default::default()
        });
    }
}

async fn handle_history(
    history: &wa::HistorySync,
    client: &Arc<Client>,
    db: &Arc<Mutex<MessageDatabase>>,
) {
    let mut names = HashMap::new();
    for push in &history.pushnames {
        if let (Some(id), Some(name)) = (&push.id, &push.pushname)
            && !name.is_empty()
            && name != "-"
        {
            names.insert(id.clone(), name.clone());
        }
    }
    {
        let mut database = db.lock().await;
        database.update_push_names(names.clone());
        for id in names.keys().filter(|id| id.ends_with("@s.whatsapp.net")) {
            let name = database
                .get_contact(id)
                .map(|contact| contact.name.clone())
                .unwrap_or_else(|| names[id].clone());
            database.add_chat(Chat {
                id: id.clone(),
                name,
                ..Default::default()
            });
        }
        database.refresh_contact_names();
    }
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
        db.lock().await.add_chat(Chat {
            id: chat_id.clone(),
            is_group: chat_id.ends_with(GROUP_SUFFIX),
            name,
            unread: conversation.unread_count.unwrap_or(0) as usize,
            last_message: conversation
                .last_msg_timestamp
                .or(conversation.conversation_timestamp)
                .unwrap_or(0) as i64,
            ..Default::default()
        });
        for historical in &conversation.messages {
            let Some(web) = &historical.message else {
                continue;
            };
            let Some(raw) = &web.message else { continue };
            let Some(info) = history_message_info(web, &chat_jid, client).await else {
                continue;
            };
            if let Some(message) = message_from_info(&info, Arc::new(raw.clone()), db).await {
                db.lock().await.add_message(message, false);
            }
        }
        db.lock()
            .await
            .update_chat_unread(&chat_id, conversation.unread_count.unwrap_or(0) as usize);
    }
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
    db: &Arc<Mutex<MessageDatabase>>,
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
    {
        let mut db = db.lock().await;
        if !info.push_name.is_empty() {
            db.update_push_name(&contact_id, "", &info.push_name);
        }
        if db.get_contact(&contact_id).is_none() {
            db.add_contact(Contact {
                id: contact_id.clone(),
                name: fallback.clone(),
                short: fallback,
            });
        }
    }
    let db_guard = db.lock().await;
    let mut msg = Message {
        id: info.id.clone(),
        chat_id,
        sender_id: info.source.sender.to_string(),
        contact_id: contact_id.clone(),
        contact_name: db_guard.id_name(&contact_id),
        contact_short: db_guard.id_short(&contact_id),
        timestamp: info.timestamp.timestamp().max(0) as u64,
        from_me: info.source.is_from_me,
        forwarded: is_forwarded(base),
        raw_message: Some(raw.clone()),
        ..Default::default()
    };
    drop(db_guard);
    if let Some(text) = raw.text_content() {
        msg.kind = MessageKind::Text;
        msg.text = text.into();
    } else if let Some(media) = &base.image_message {
        msg.kind = MessageKind::Image;
        msg.mime_type = media.mimetype.clone().unwrap_or_default();
        msg.text = media_display_text(msg.kind, "", media.caption.as_deref().unwrap_or(""));
    } else if let Some(media) = &base.video_message {
        msg.kind = MessageKind::Video;
        msg.mime_type = media.mimetype.clone().unwrap_or_default();
        msg.text = media_display_text(msg.kind, "", media.caption.as_deref().unwrap_or(""));
    } else if let Some(media) = &base.audio_message {
        msg.kind = MessageKind::Audio;
        msg.mime_type = media.mimetype.clone().unwrap_or_default();
        msg.text = media_display_text(msg.kind, "", "");
    } else if let Some(media) = &base.document_message {
        msg.kind = MessageKind::Document;
        msg.mime_type = media.mimetype.clone().unwrap_or_default();
        msg.file_name = media.file_name.clone().unwrap_or_default();
        msg.text = media_display_text(
            msg.kind,
            &msg.file_name,
            media.caption.as_deref().unwrap_or(""),
        );
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
            return name.into();
        }
    }
    let extension = mime_guess::get_mime_extensions_str(&msg.mime_type)
        .and_then(|all| all.first())
        .map(|ext| format!(".{ext}"))
        .unwrap_or_default();
    format!("{}{extension}", msg.id)
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
}
