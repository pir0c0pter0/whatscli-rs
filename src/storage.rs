use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Local};

use crate::model::{CONTACT_SUFFIX, Chat, Contact, GROUP_SUFFIX, Message, MessageKind};

#[derive(Default)]
pub struct MessageDatabase {
    messages: HashMap<String, Vec<Message>>,
    messages_by_id: HashMap<String, Message>,
    chats: HashMap<String, Chat>,
    contacts: HashMap<String, Contact>,
}

impl MessageDatabase {
    pub fn add_message(&mut self, mut msg: Message, mark_unread: bool) -> bool {
        if let Some(existing) = self.messages_by_id.get_mut(&msg.id) {
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
            self.replace_message(&merged);
            self.update_chat_from_message(&merged, mark_unread);
            return false;
        }

        msg.unread = mark_unread;
        self.messages_by_id.insert(msg.id.clone(), msg.clone());
        let messages = self.messages.entry(msg.chat_id.clone()).or_default();
        messages.push(msg.clone());
        messages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)));
        self.update_chat_from_message(&msg, mark_unread);
        true
    }

    fn replace_message(&mut self, msg: &Message) {
        if let Some(messages) = self.messages.get_mut(&msg.chat_id)
            && let Some(slot) = messages.iter_mut().find(|stored| stored.id == msg.id)
        {
            *slot = msg.clone();
        }
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
                if let Some(stored) = self.messages_by_id.get_mut(&msg.id) {
                    stored.unread = msg.unread;
                }
            }
        }
        if let Some(chat) = self.chats.get_mut(chat_id) {
            chat.unread = ids.len();
        }
    }

    pub fn mark_chat_read(&mut self, chat_id: &str) -> Vec<Message> {
        let mut cleared = Vec::new();
        if let Some(messages) = self.messages.get_mut(chat_id) {
            for msg in messages {
                if msg.unread {
                    cleared.push(msg.clone());
                    msg.unread = false;
                    if let Some(stored) = self.messages_by_id.get_mut(&msg.id) {
                        stored.unread = false;
                    }
                }
            }
        }
        if let Some(chat) = self.chats.get_mut(chat_id) {
            chat.unread = 0;
        }
        cleared
    }

    pub fn mark_message_revoked(&mut self, id: &str) -> bool {
        let Some(msg) = self.messages_by_id.get_mut(id) else {
            return false;
        };
        msg.text = "[message revoked]".into();
        msg.raw_message = None;
        msg.kind = MessageKind::Unknown;
        let updated = msg.clone();
        self.replace_message(&updated);
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
                self.messages_by_id.insert(msg.id.clone(), msg.clone());
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
                self.messages_by_id.insert(msg.id.clone(), msg.clone());
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
            b.last_message
                .cmp(&a.last_message)
                .then_with(|| a.name.cmp(&b.name))
        });
        chats
    }

    pub fn messages(&self, chat_id: &str) -> Vec<Message> {
        self.messages.get(chat_id).cloned().unwrap_or_default()
    }

    pub fn message(&self, id: &str) -> Option<&Message> {
        self.messages_by_id.get(id)
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
}
