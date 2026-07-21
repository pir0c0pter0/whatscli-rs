use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use chrono::{DateTime, Local};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::model::{
    CONTACT_SUFFIX, Chat, Contact, DatabaseSnapshot, GROUP_SUFFIX, Message, MessageKind, UiEvent,
};

pub const QUEUE_CAPACITY: usize = 128;

#[derive(Clone)]
pub struct StorageHandle {
    tx: mpsc::Sender<StorageCommand>,
}

enum StorageCommand {
    Select(String),
    AddMessage {
        message: Message,
        mark_unread: bool,
        promote_chat: bool,
        reply: oneshot::Sender<bool>,
    },
    AddChat(Chat),
    ResolveContact {
        id: String,
        fallback: String,
        push_name: String,
        reply: oneshot::Sender<(String, String)>,
    },
    UpdatePushName(String, String, String),
    UpdatePushNames(HashMap<String, String>),
    RefreshContactNames,
    UpdateChatUnread(String, usize),
    MarkChatRead(String, oneshot::Sender<Vec<Message>>),
    MarkMessageRevoked(String, oneshot::Sender<bool>),
    Message(String, oneshot::Sender<Option<Message>>),
    OldestMessage(String, oneshot::Sender<Option<Message>>),
    MessageInfo(String, oneshot::Sender<String>),
}

pub fn start_storage_actor(ui_tx: mpsc::Sender<UiEvent>) -> (StorageHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
    let handle = StorageHandle { tx };
    let task = tokio::spawn(storage_actor(rx, ui_tx));
    (handle, task)
}

impl StorageHandle {
    async fn send(&self, command: StorageCommand) -> Result<()> {
        let started_at = Instant::now();
        let depth = self.tx.max_capacity() - self.tx.capacity();
        let result = self
            .tx
            .send(command)
            .await
            .map_err(|_| anyhow!("storage worker stopped"));
        log::trace!(
            "storage queue send depth={} wait_us={}",
            depth,
            started_at.elapsed().as_micros()
        );
        result
    }

    pub async fn select(&self, chat_id: String) -> Result<()> {
        self.send(StorageCommand::Select(chat_id)).await
    }

    pub async fn add_message(&self, message: Message, mark_unread: bool) -> Result<bool> {
        self.store_message(message, mark_unread, true).await
    }

    pub async fn add_historical_message(
        &self,
        message: Message,
        mark_unread: bool,
    ) -> Result<bool> {
        self.store_message(message, mark_unread, false).await
    }

    async fn store_message(
        &self,
        message: Message,
        mark_unread: bool,
        promote_chat: bool,
    ) -> Result<bool> {
        let (reply, response) = oneshot::channel();
        self.send(StorageCommand::AddMessage {
            message,
            mark_unread,
            promote_chat,
            reply,
        })
        .await?;
        response
            .await
            .map_err(|_| anyhow!("storage worker stopped"))
    }

    pub async fn add_chat(&self, chat: Chat) -> Result<()> {
        self.send(StorageCommand::AddChat(chat)).await
    }

    pub async fn resolve_contact(
        &self,
        id: String,
        fallback: String,
        push_name: String,
    ) -> Result<(String, String)> {
        let (reply, response) = oneshot::channel();
        self.send(StorageCommand::ResolveContact {
            id,
            fallback,
            push_name,
            reply,
        })
        .await?;
        response
            .await
            .map_err(|_| anyhow!("storage worker stopped"))
    }

    pub async fn update_push_name(
        &self,
        id: String,
        old_name: String,
        new_name: String,
    ) -> Result<()> {
        self.send(StorageCommand::UpdatePushName(id, old_name, new_name))
            .await
    }

    pub async fn update_push_names(&self, names: HashMap<String, String>) -> Result<()> {
        self.send(StorageCommand::UpdatePushNames(names)).await
    }

    pub async fn refresh_contact_names(&self) -> Result<()> {
        self.send(StorageCommand::RefreshContactNames).await
    }

    pub async fn update_chat_unread(&self, chat_id: String, unread: usize) -> Result<()> {
        self.send(StorageCommand::UpdateChatUnread(chat_id, unread))
            .await
    }

    pub async fn mark_chat_read(&self, chat_id: String) -> Result<Vec<Message>> {
        let (reply, response) = oneshot::channel();
        self.send(StorageCommand::MarkChatRead(chat_id, reply))
            .await?;
        response
            .await
            .map_err(|_| anyhow!("storage worker stopped"))
    }

    pub async fn mark_message_revoked(&self, id: String) -> Result<bool> {
        let (reply, response) = oneshot::channel();
        self.send(StorageCommand::MarkMessageRevoked(id, reply))
            .await?;
        response
            .await
            .map_err(|_| anyhow!("storage worker stopped"))
    }

    pub async fn message(&self, id: String) -> Result<Option<Message>> {
        let (reply, response) = oneshot::channel();
        self.send(StorageCommand::Message(id, reply)).await?;
        response
            .await
            .map_err(|_| anyhow!("storage worker stopped"))
    }

    pub async fn oldest_message(&self, chat_id: String) -> Result<Option<Message>> {
        let (reply, response) = oneshot::channel();
        self.send(StorageCommand::OldestMessage(chat_id, reply))
            .await?;
        response
            .await
            .map_err(|_| anyhow!("storage worker stopped"))
    }

    pub async fn message_info(&self, id: String) -> Result<String> {
        let (reply, response) = oneshot::channel();
        self.send(StorageCommand::MessageInfo(id, reply)).await?;
        response
            .await
            .map_err(|_| anyhow!("storage worker stopped"))
    }
}

async fn storage_actor(mut rx: mpsc::Receiver<StorageCommand>, ui_tx: mpsc::Sender<UiEvent>) {
    let mut database = MessageDatabase::default();
    let mut selected_chat = String::new();
    let mut revision = 0_u64;
    let mut dirty = true;
    let start = tokio::time::Instant::now() + Duration::from_millis(16);
    let mut snapshots = tokio::time::interval_at(start, Duration::from_millis(16));
    snapshots.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else { break };
                let changed = apply_storage_command(command, &mut database, &mut selected_chat);
                if changed {
                    revision = revision.wrapping_add(1);
                    dirty = true;
                }
            }
            _ = snapshots.tick(), if dirty => {
                let snapshot = DatabaseSnapshot {
                    revision,
                    selected_chat: selected_chat.clone(),
                    chats: database.chats(),
                    messages: database.messages(&selected_chat),
                };
                match ui_tx.try_send(UiEvent::Snapshot(snapshot)) {
                    Ok(()) => dirty = false,
                    Err(mpsc::error::TrySendError::Full(_)) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
        }
    }
}

fn apply_storage_command(
    command: StorageCommand,
    database: &mut MessageDatabase,
    selected_chat: &mut String,
) -> bool {
    match command {
        StorageCommand::Select(chat_id) => {
            *selected_chat = chat_id;
            true
        }
        StorageCommand::AddMessage {
            message,
            mark_unread,
            promote_chat,
            reply,
        } => {
            let _ = reply.send(database.store_message(message, mark_unread, promote_chat));
            true
        }
        StorageCommand::AddChat(chat) => {
            database.add_chat(chat);
            true
        }
        StorageCommand::ResolveContact {
            id,
            fallback,
            push_name,
            reply,
        } => {
            if !push_name.is_empty() {
                database.update_push_name(&id, "", &push_name);
            }
            if database.get_contact(&id).is_none() {
                database.add_contact(Contact {
                    id: id.clone(),
                    name: fallback.clone(),
                    short: fallback,
                });
            }
            let _ = reply.send((database.id_name(&id), database.id_short(&id)));
            true
        }
        StorageCommand::UpdatePushName(id, old_name, new_name) => {
            database.update_push_name(&id, &old_name, &new_name);
            if id.ends_with(CONTACT_SUFFIX) {
                database.add_chat(Chat {
                    name: database.id_name(&id),
                    id,
                    ..Default::default()
                });
            }
            true
        }
        StorageCommand::UpdatePushNames(names) => {
            database.update_push_names(names);
            true
        }
        StorageCommand::RefreshContactNames => {
            database.refresh_contact_names();
            true
        }
        StorageCommand::UpdateChatUnread(chat_id, unread) => {
            database.update_chat_unread(&chat_id, unread);
            true
        }
        StorageCommand::MarkChatRead(chat_id, reply) => {
            let _ = reply.send(database.mark_chat_read(&chat_id));
            true
        }
        StorageCommand::MarkMessageRevoked(id, reply) => {
            let changed = database.mark_message_revoked(&id);
            let _ = reply.send(changed);
            changed
        }
        StorageCommand::Message(id, reply) => {
            let _ = reply.send(database.message(&id).cloned());
            false
        }
        StorageCommand::OldestMessage(chat_id, reply) => {
            let _ = reply.send(database.oldest_message(&chat_id).cloned());
            false
        }
        StorageCommand::MessageInfo(id, reply) => {
            let _ = reply.send(database.message_info(&id));
            false
        }
    }
}

#[derive(Default)]
pub struct MessageDatabase {
    messages: HashMap<String, Vec<Message>>,
    message_chat_by_id: HashMap<String, String>,
    chats: HashMap<String, Chat>,
    contacts: HashMap<String, Contact>,
    chat_activity_order: HashMap<String, u64>,
    next_activity_order: u64,
}

impl MessageDatabase {
    pub fn add_message(&mut self, msg: Message, mark_unread: bool) -> bool {
        self.store_message(msg, mark_unread, true)
    }

    fn store_message(&mut self, mut msg: Message, mark_unread: bool, promote_chat: bool) -> bool {
        if let Some(chat_id) = self.message_chat_by_id.get(&msg.id).cloned()
            && let Some(existing) = self
                .messages
                .get_mut(&chat_id)
                .and_then(|messages| messages.iter_mut().find(|stored| stored.id == msg.id))
        {
            let newly_unread = mark_unread && !existing.unread;
            if existing.raw_message.is_none() && msg.raw_message.is_some() {
                existing.raw_message = msg.raw_message.take();
            }
            if existing.kind == MessageKind::Unknown && msg.kind != MessageKind::Unknown {
                existing.kind = msg.kind;
            }
            if existing.text.is_empty() && !msg.text.is_empty() {
                existing.text = msg.text;
            }
            if existing.file_name.is_empty() && !msg.file_name.is_empty() {
                existing.file_name = msg.file_name;
            }
            if existing.mime_type.is_empty() && !msg.mime_type.is_empty() {
                existing.mime_type = msg.mime_type;
            }
            existing.unread |= mark_unread;
            let merged = existing.clone();
            self.update_chat_from_message(&merged, newly_unread);
            return false;
        }

        msg.unread = mark_unread;
        self.update_chat_from_message(&msg, mark_unread);
        if promote_chat {
            self.next_activity_order = self.next_activity_order.saturating_add(1);
            self.chat_activity_order
                .insert(msg.chat_id.clone(), self.next_activity_order);
        }
        self.message_chat_by_id
            .insert(msg.id.clone(), msg.chat_id.clone());
        let messages = self.messages.entry(msg.chat_id.clone()).or_default();
        messages.push(msg);
        messages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)));
        true
    }

    fn update_chat_from_message(&mut self, msg: &Message, mark_unread: bool) {
        let chat = self
            .chats
            .entry(msg.chat_id.clone())
            .or_insert_with(|| Chat {
                id: msg.chat_id.clone(),
                is_group: msg.chat_id.ends_with(GROUP_SUFFIX),
                name: msg.contact_name.clone(),
                ..Default::default()
            });
        if chat.name.is_empty() {
            chat.name.clone_from(&msg.contact_name);
        }
        if msg.timestamp as i64 >= chat.last_message {
            chat.last_message = msg.timestamp as i64;
            chat.preview = message_preview(msg);
            chat.last_message_kind = msg.kind;
            chat.last_sender.clone_from(&msg.contact_short);
            chat.last_from_me = msg.from_me;
        }
        if mark_unread {
            chat.unread += 1;
        }
        if !msg.contact_id.is_empty() {
            self.contacts
                .entry(msg.contact_id.clone())
                .or_insert_with(|| Contact {
                    id: msg.contact_id.clone(),
                    name: msg.contact_name.clone(),
                    short: msg.contact_short.clone(),
                });
        }
    }

    pub fn add_chat(&mut self, mut chat: Chat) {
        if let Some(existing) = self.chats.get(&chat.id) {
            if chat.name.is_empty() {
                chat.name.clone_from(&existing.name);
            }
            chat.last_message = chat.last_message.max(existing.last_message);
            chat.unread = chat.unread.max(existing.unread);
            if chat.preview.is_empty() {
                chat.preview.clone_from(&existing.preview);
                chat.last_message_kind = existing.last_message_kind;
                chat.last_sender.clone_from(&existing.last_sender);
                chat.last_from_me = existing.last_from_me;
            }
        }
        self.chats.insert(chat.id.clone(), chat);
    }

    pub fn update_chat_unread(&mut self, chat_id: &str, unread: usize) {
        let ids: HashSet<_> = self
            .messages
            .get(chat_id)
            .into_iter()
            .flatten()
            .rev()
            .filter(|msg| !msg.from_me)
            .take(unread)
            .map(|msg| msg.id.clone())
            .collect();
        if let Some(messages) = self.messages.get_mut(chat_id) {
            for msg in messages {
                msg.unread = ids.contains(&msg.id);
            }
        }
        if let Some(chat) = self.chats.get_mut(chat_id) {
            chat.unread = unread;
        }
    }

    pub fn mark_chat_read(&mut self, chat_id: &str) -> Vec<Message> {
        let mut cleared = Vec::new();
        if let Some(messages) = self.messages.get_mut(chat_id) {
            for msg in messages {
                if msg.unread {
                    cleared.push(msg.clone());
                    msg.unread = false;
                }
            }
        }
        if let Some(chat) = self.chats.get_mut(chat_id) {
            chat.unread = 0;
        }
        cleared
    }

    pub fn mark_message_revoked(&mut self, id: &str) -> bool {
        let Some(chat_id) = self.message_chat_by_id.get(id).cloned() else {
            return false;
        };
        let Some(msg) = self
            .messages
            .get_mut(&chat_id)
            .and_then(|messages| messages.iter_mut().find(|msg| msg.id == id))
        else {
            return false;
        };
        msg.text = "[message revoked]".into();
        msg.raw_message = None;
        msg.kind = MessageKind::Unknown;
        let updated = msg.clone();
        self.update_chat_from_message(&updated, false);
        true
    }

    pub fn add_contact(&mut self, mut contact: Contact) {
        if let Some(existing) = self.contacts.get(&contact.id) {
            if contact.name.is_empty() {
                contact.name.clone_from(&existing.name);
            }
            if contact.short.is_empty() {
                contact.short.clone_from(&existing.short);
            }
        }
        self.contacts.insert(contact.id.clone(), contact);
    }

    pub fn get_contact(&self, id: &str) -> Option<&Contact> {
        self.contacts.get(id)
    }

    pub fn update_push_name(&mut self, id: &str, old_name: &str, new_name: &str) {
        if id.is_empty() || new_name.is_empty() {
            return;
        }
        let mut updates = HashMap::new();
        updates.insert(id.to_owned(), (old_name.to_owned(), new_name.to_owned()));
        self.update_push_names_inner(&updates);
    }

    pub fn update_push_names(&mut self, names: HashMap<String, String>) {
        let updates = names
            .into_iter()
            .filter(|(id, name)| !id.is_empty() && !name.is_empty())
            .map(|(id, name)| (id, (String::new(), name)))
            .collect();
        self.update_push_names_inner(&updates);
    }

    fn update_push_names_inner(&mut self, updates: &HashMap<String, (String, String)>) {
        let mut changed = HashMap::new();
        for (id, (old_name, new_name)) in updates {
            let contact = self.contacts.entry(id.clone()).or_insert_with(|| Contact {
                id: id.clone(),
                ..Default::default()
            });
            let name_needs_update =
                is_fallback_name(&contact.name, id) || contact.name == *old_name;
            if contact.short == *new_name && (!name_needs_update || contact.name == *new_name) {
                continue;
            }
            if name_needs_update {
                contact.name.clone_from(new_name);
            }
            contact.short.clone_from(new_name);
            changed.insert(id.clone(), contact.clone());
        }
        for messages in self.messages.values_mut() {
            for msg in messages {
                let Some(contact) = changed.get(&msg.contact_id) else {
                    continue;
                };
                let (old_name, new_name) = &updates[&msg.contact_id];
                if is_fallback_name(&msg.contact_name, &msg.contact_id)
                    || msg.contact_name == *old_name
                {
                    msg.contact_name.clone_from(&contact.name);
                }
                msg.contact_short.clone_from(new_name);
            }
        }
        for (id, contact) in changed {
            if let Some(chat) = self.chats.get_mut(&id)
                && !chat.is_group
                && (is_fallback_name(&chat.name, &id) || chat.name == updates[&id].0)
            {
                chat.name = contact.name;
            }
        }
    }

    pub fn refresh_contact_names(&mut self) {
        for messages in self.messages.values_mut() {
            for msg in messages {
                let Some(contact) = self.contacts.get(&msg.contact_id) else {
                    continue;
                };
                msg.contact_name.clone_from(&contact.name);
                msg.contact_short.clone_from(&contact.short);
            }
        }
        for (id, chat) in &mut self.chats {
            if let Some(contact) = self.contacts.get(id)
                && !chat.is_group
                && !contact.name.is_empty()
            {
                chat.name.clone_from(&contact.name);
            }
        }
    }

    pub fn chats(&self) -> Vec<Chat> {
        let mut chats: Vec<_> = self.chats.values().cloned().collect();
        chats.sort_by(|a, b| {
            let a_activity = self.chat_activity_order.get(&a.id).copied();
            let b_activity = self.chat_activity_order.get(&b.id).copied();
            b_activity
                .is_some()
                .cmp(&a_activity.is_some())
                .then_with(|| {
                    b_activity
                        .unwrap_or_default()
                        .cmp(&a_activity.unwrap_or_default())
                })
                .then_with(|| b.last_message.cmp(&a.last_message))
                .then_with(|| a.name.cmp(&b.name))
        });
        chats
    }

    pub fn messages(&self, chat_id: &str) -> Vec<Message> {
        self.messages.get(chat_id).cloned().unwrap_or_default()
    }

    pub fn message(&self, id: &str) -> Option<&Message> {
        let chat_id = self.message_chat_by_id.get(id)?;
        self.messages
            .get(chat_id)?
            .iter()
            .find(|message| message.id == id)
    }

    pub fn oldest_message(&self, chat_id: &str) -> Option<&Message> {
        self.messages.get(chat_id)?.first()
    }

    pub fn message_info(&self, id: &str) -> String {
        let Some(msg) = self.message(id) else {
            return "Message not found".into();
        };
        let direction = if msg.from_me { "→" } else { "←" };
        let date = DateTime::from_timestamp(msg.timestamp as i64, 0)
            .map(|d| d.with_timezone(&Local).to_rfc2822())
            .unwrap_or_else(|| "invalid timestamp".into());
        let mut info = format!(
            "ID: {}\nType: {}\nFrom: {} ({}) {}\nTime: {}\nChat: {}",
            msg.id,
            msg.kind,
            self.id_name(&msg.contact_id),
            self.id_short(&msg.contact_id),
            direction,
            date,
            msg.chat_id
        );
        if !msg.file_name.is_empty() {
            info.push_str("\nFile: ");
            info.push_str(&msg.file_name);
        }
        if !msg.mime_type.is_empty() {
            info.push_str("\nMIME: ");
            info.push_str(&msg.mime_type);
        }
        if !msg.sender_id.is_empty() {
            info.push_str("\nSender: ");
            info.push_str(&msg.sender_id);
        }
        info
    }

    pub fn id_name(&self, id: &str) -> String {
        if id.is_empty() {
            return "Unknown".into();
        }
        if let Some(contact) = self.contacts.get(id) {
            if !contact.name.is_empty() {
                return contact.name.clone();
            }
            if !contact.short.is_empty() {
                return contact.short.clone();
            }
        }
        if let Some(chat) = self.chats.get(id)
            && !chat.name.is_empty()
        {
            return chat.name.clone();
        }
        trim_jid(id).to_owned()
    }

    pub fn id_short(&self, id: &str) -> String {
        if id.is_empty() {
            return "Unknown".into();
        }
        if let Some(contact) = self.contacts.get(id) {
            if !contact.short.is_empty() {
                return contact.short.clone();
            }
            if !contact.name.is_empty() {
                return contact.name.clone();
            }
        }
        if let Some(chat) = self.chats.get(id)
            && !chat.name.is_empty()
        {
            return chat.name.clone();
        }
        trim_jid(id).to_owned()
    }
}

fn message_preview(message: &Message) -> String {
    let text = message
        .text
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if !text.is_empty() {
        text
    } else if !message.file_name.is_empty() {
        format!("{} · {}", message.kind, message.file_name)
    } else {
        message.kind.to_string()
    }
}

pub fn is_fallback_name(name: &str, id: &str) -> bool {
    name.is_empty() || name == id || name == id.split('@').next().unwrap_or(id)
}

fn trim_jid(id: &str) -> &str {
    id.strip_suffix(CONTACT_SUFFIX)
        .or_else(|| id.strip_suffix(GROUP_SUFFIX))
        .unwrap_or(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(id: &str, chat: &str, timestamp: u64) -> Message {
        Message {
            id: id.into(),
            chat_id: chat.into(),
            contact_id: chat.into(),
            contact_name: "Alice".into(),
            contact_short: "Alice".into(),
            timestamp,
            kind: MessageKind::Text,
            ..Default::default()
        }
    }

    #[test]
    fn add_message_and_mark_chat_read() {
        let mut db = MessageDatabase::default();
        assert!(db.add_message(message("msg-1", "123@s.whatsapp.net", 100), false));
        assert!(db.add_message(message("msg-2", "123@s.whatsapp.net", 101), true));
        assert_eq!(db.messages("123@s.whatsapp.net").len(), 2);
        assert_eq!(db.chats()[0].unread, 1);
        assert_eq!(db.mark_chat_read("123@s.whatsapp.net")[0].id, "msg-2");
        assert_eq!(db.chats()[0].unread, 0);
    }

    #[test]
    fn update_unread_marks_latest_incoming_messages() {
        let mut db = MessageDatabase::default();
        for i in 0..4 {
            let mut msg = message(&i.to_string(), "group@g.us", i);
            msg.from_me = i == 0;
            db.add_message(msg, false);
        }
        db.update_chat_unread("group@g.us", 2);
        assert_eq!(
            db.messages("group@g.us")
                .iter()
                .filter(|m| m.unread)
                .count(),
            2
        );
    }

    #[test]
    fn server_unread_count_is_preserved_when_local_history_is_limited() {
        let mut db = MessageDatabase::default();
        let chat = "group@g.us";
        for i in 0..200 {
            db.add_message(message(&i.to_string(), chat, i), false);
        }
        db.update_chat_unread(chat, 450);
        assert_eq!(db.chats()[0].unread, 450);
        assert_eq!(
            db.messages(chat)
                .iter()
                .filter(|message| message.unread)
                .count(),
            200
        );
    }

    #[test]
    fn id_index_points_to_the_single_stored_message_copy() {
        let mut db = MessageDatabase::default();
        let chat = "123@s.whatsapp.net";
        let raw = std::sync::Arc::new(whatsapp_rust::waproto::whatsapp::Message::default());
        let mut media = message("media", chat, 1);
        media.kind = MessageKind::Image;
        media.raw_message = Some(std::sync::Arc::clone(&raw));
        db.add_message(media, false);

        assert_eq!(
            db.message_chat_by_id.get("media").map(String::as_str),
            Some(chat)
        );
        assert_eq!(std::sync::Arc::strong_count(&raw), 2);
        assert!(db.message("media").is_some());
        assert!(db.message_info("media").contains("Type: image"));
        db.add_message(message("media", chat, 1), true);
        db.add_message(message("media", chat, 1), true);
        assert_eq!(db.messages(chat).len(), 1);
        assert_eq!(db.chats()[0].unread, 1);
        assert_eq!(db.mark_chat_read(chat).len(), 1);
        assert!(db.mark_message_revoked("media"));
        assert_eq!(db.message("media").unwrap().text, "[message revoked]");
        assert!(db.message("media").unwrap().raw_message.is_none());
    }

    #[test]
    fn push_name_replaces_phone_fallback_everywhere() {
        let mut db = MessageDatabase::default();
        let id = "5511999999999@s.whatsapp.net";
        let mut msg = message("msg-1", id, 1);
        msg.contact_name = "5511999999999".into();
        msg.contact_short = "5511999999999".into();
        db.add_message(msg, false);
        db.update_push_name(id, "", "Maria");
        assert_eq!(db.get_contact(id).unwrap().name, "Maria");
        assert_eq!(db.message("msg-1").unwrap().contact_short, "Maria");
        assert_eq!(db.chats()[0].name, "Maria");
    }

    #[test]
    fn push_name_preserves_saved_contact_name() {
        let mut db = MessageDatabase::default();
        let id = "5511888888888@s.whatsapp.net";
        db.add_contact(Contact {
            id: id.into(),
            name: "Maria da Silva".into(),
            short: "Mari".into(),
        });
        db.update_push_name(id, "Mari", "Maria");
        assert_eq!(db.get_contact(id).unwrap().name, "Maria da Silva");
        assert_eq!(db.get_contact(id).unwrap().short, "Maria");
    }

    #[test]
    fn contact_refresh_preserves_chat_arrival_order() {
        let mut db = MessageDatabase::default();
        let older = "5511@s.whatsapp.net";
        let newer = "5522@s.whatsapp.net";
        db.add_message(message("old", older, 100), false);
        db.add_message(message("new", newer, 200), false);
        db.add_contact(Contact {
            id: older.into(),
            name: "Zoe".into(),
            short: "Zoe".into(),
        });
        db.add_contact(Contact {
            id: newer.into(),
            name: "Ana".into(),
            short: "Ana".into(),
        });
        db.refresh_contact_names();
        db.update_push_name(older, "Zoe", "Zoe Updated");
        assert_eq!(
            db.chats().iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            vec![newer, older]
        );
    }

    #[test]
    fn equal_timestamps_are_ordered_by_message_arrival() {
        let mut db = MessageDatabase::default();
        let first_chat = "5511@s.whatsapp.net";
        let second_chat = "5522@s.whatsapp.net";
        let mut first = message("first", first_chat, 100);
        first.contact_name = "Ana".into();
        let mut second = message("second", second_chat, 100);
        second.contact_name = "Zoe".into();

        db.add_message(first, false);
        db.add_message(second, false);

        assert_eq!(
            db.chats()
                .iter()
                .map(|chat| chat.id.as_str())
                .collect::<Vec<_>>(),
            vec![second_chat, first_chat]
        );
    }

    #[test]
    fn delayed_new_message_promotes_its_chat() {
        let mut db = MessageDatabase::default();
        let newer_chat = "5511@s.whatsapp.net";
        let delayed_chat = "5522@s.whatsapp.net";
        db.store_message(message("newer", newer_chat, 200), false, false);
        db.store_message(message("older", delayed_chat, 100), false, false);
        assert_eq!(db.chats()[0].id, newer_chat);

        db.add_message(message("delayed", delayed_chat, 50), false);

        assert_eq!(db.chats()[0].id, delayed_chat);
        assert_eq!(db.chats()[0].last_message, 100);
    }

    #[test]
    fn historical_messages_do_not_override_live_arrival_order() {
        let mut db = MessageDatabase::default();
        let live_chat = "5511@s.whatsapp.net";
        let history_chat = "5522@s.whatsapp.net";
        db.add_message(message("live", live_chat, 100), false);

        db.store_message(message("history", history_chat, 200), false, false);

        assert_eq!(db.chats()[0].id, live_chat);
    }

    #[test]
    fn latest_message_metadata_is_cached_in_chat_preview() {
        let mut db = MessageDatabase::default();
        let mut first = message("one", "123@s.whatsapp.net", 100);
        first.text = "  café\n  amanhã  ".into();
        db.add_message(first, false);
        let mut older = message("older", "123@s.whatsapp.net", 50);
        older.text = "must not replace preview".into();
        db.add_message(older, true);
        let chat = &db.chats()[0];
        assert_eq!(chat.preview, "café amanhã");
        assert_eq!(chat.last_message_kind, MessageKind::Text);
        assert_eq!(chat.unread, 1);
    }

    #[tokio::test]
    async fn storage_actor_coalesces_mutations_into_a_consistent_snapshot() {
        let (ui_tx, mut ui_rx) = mpsc::channel(QUEUE_CAPACITY);
        let (storage, task) = start_storage_actor(ui_tx);
        let chat = "123@s.whatsapp.net";
        storage.select(chat.into()).await.unwrap();
        for index in 0..3 {
            storage
                .add_message(message(&format!("msg-{index}"), chat, index), false)
                .await
                .unwrap();
        }

        let snapshot = tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if let Some(UiEvent::Snapshot(snapshot)) = ui_rx.recv().await
                    && snapshot.revision >= 4
                {
                    break snapshot;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(snapshot.selected_chat, chat);
        assert_eq!(snapshot.messages.len(), 3);
        assert_eq!(snapshot.chats.len(), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(8), ui_rx.recv())
                .await
                .is_err()
        );
        task.abort();
    }
}
