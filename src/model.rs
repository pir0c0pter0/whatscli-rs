use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use whatsapp_rust::waproto::whatsapp as wa;

pub const GROUP_SUFFIX: &str = "@g.us";
pub const CONTACT_SUFFIX: &str = "@s.whatsapp.net";
pub const STATUS_SUFFIX: &str = "status@broadcast";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MessageKind {
    Text,
    Image,
    Video,
    Audio,
    Document,
    #[default]
    Unknown,
}

impl std::fmt::Display for MessageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Text => "text",
            Self::Image => "image",
            Self::Video => "video",
            Self::Audio => "audio",
            Self::Document => "document",
            Self::Unknown => "unknown",
        };
        f.write_str(name)
    }
}

#[derive(Clone, Default)]
pub struct Message {
    pub id: String,
    pub chat_id: String,
    pub sender_id: String,
    pub contact_id: String,
    pub contact_name: String,
    pub contact_short: String,
    pub timestamp: u64,
    pub from_me: bool,
    pub forwarded: bool,
    pub text: String,
    pub kind: MessageKind,
    pub mime_type: String,
    pub file_name: String,
    pub unread: bool,
    pub raw_message: Option<Arc<wa::Message>>,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("id", &self.id)
            .field("chat_id", &self.chat_id)
            .field("sender_id", &self.sender_id)
            .field("contact_id", &self.contact_id)
            .field("contact_name", &self.contact_name)
            .field("timestamp", &self.timestamp)
            .field("from_me", &self.from_me)
            .field("text", &self.text)
            .field("kind", &self.kind)
            .field("unread", &self.unread)
            .finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Chat {
    pub id: String,
    pub is_group: bool,
    pub name: String,
    pub unread: usize,
    pub last_message: i64,
    pub preview: String,
    pub last_message_kind: MessageKind,
    pub last_sender: String,
    pub last_from_me: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Contact {
    pub id: String,
    pub name: String,
    pub short: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConnectionState {
    #[default]
    Disconnected,
    Connecting,
    Pairing,
    Connected,
}

impl ConnectionState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Disconnected => "offline",
            Self::Connecting => "connecting",
            Self::Pairing => "pairing",
            Self::Connected => "online",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SessionStatus {
    pub state: ConnectionState,
    pub last_seen: String,
}

impl SessionStatus {
    pub fn connected(&self) -> bool {
        self.state == ConnectionState::Connected
    }
}

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskCategory {
    Session,
    Conversation,
    History,
    Transfer,
    Integration,
}

impl TaskCategory {
    pub fn label(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Conversation => "conversation",
            Self::History => "history",
            Self::Transfer => "transfer",
            Self::Integration => "system",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub id: u64,
    pub category: TaskCategory,
    pub name: String,
    pub params: Vec<String>,
}

impl Command {
    pub fn new(name: impl Into<String>, params: Vec<String>) -> Self {
        let name = name.into();
        Self {
            id: NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed),
            category: classify_command(&name),
            name,
            params,
        }
    }

    pub fn label(&self) -> String {
        match self.name.as_str() {
            "send" => "sending message".into(),
            "backlog" | "more" => "syncing history".into(),
            "download" => "downloading media".into(),
            "open" => "opening media".into(),
            "show" => "rendering preview".into(),
            "clipboard-copy" => "copying to clipboard".into(),
            "clipboard-paste" => "reading clipboard".into(),
            name => name.replace('-', " "),
        }
    }
}

pub fn classify_command(name: &str) -> TaskCategory {
    match name {
        "connect" | "login" | "disconnect" | "logout" | "reset" | "select" | "colorlist" => {
            TaskCategory::Session
        }
        "backlog" | "more" => TaskCategory::History,
        "download" | "open" | "show" => TaskCategory::Transfer,
        "url" | "clipboard-copy" | "clipboard-paste" => TaskCategory::Integration,
        _ => TaskCategory::Conversation,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskInfo {
    pub id: u64,
    pub category: TaskCategory,
    pub label: String,
}

impl From<&Command> for TaskInfo {
    fn from(command: &Command) -> Self {
        Self {
            id: command.id,
            category: command.category,
            label: command.label(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DatabaseSnapshot {
    pub revision: u64,
    pub selected_chat: String,
    pub chats: Vec<Chat>,
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone)]
pub enum UiEvent {
    Status(SessionStatus),
    Snapshot(DatabaseSnapshot),
    TaskStarted(TaskInfo),
    TaskCompleted(TaskInfo),
    TaskFailed { task: TaskInfo, error: String },
    QueueSaturated(TaskCategory),
    ClipboardText(String),
    Preview(String),
    Text(String),
    ColorList,
    Error(String),
    Qr { code: String, expires_in: u64 },
    ClearQr,
}

#[cfg(test)]
mod command_tests {
    use super::*;

    #[test]
    fn commands_are_classified_for_independent_workers() {
        assert_eq!(classify_command("connect"), TaskCategory::Session);
        assert_eq!(classify_command("send"), TaskCategory::Conversation);
        assert_eq!(classify_command("backlog"), TaskCategory::History);
        assert_eq!(classify_command("download"), TaskCategory::Transfer);
        assert_eq!(
            classify_command("clipboard-paste"),
            TaskCategory::Integration
        );
    }

    #[test]
    fn command_ids_are_unique() {
        let first = Command::new("connect", Vec::new());
        let second = Command::new("connect", Vec::new());
        assert_ne!(first.id, second.id);
    }
}
