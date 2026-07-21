use std::sync::Arc;

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

#[derive(Debug, Clone)]
pub struct Command {
    pub name: String,
    pub params: Vec<String>,
}

impl Command {
    pub fn new(name: impl Into<String>, params: Vec<String>) -> Self {
        Self {
            name: name.into(),
            params,
        }
    }
}

#[derive(Debug, Clone)]
pub enum UiEvent {
    Status(SessionStatus),
    Message(Box<Message>),
    Refresh,
    Text(String),
    ColorList,
    Error(String),
    Qr { code: String, expires_in: u64 },
    Open(String),
    ShowImage(String),
}
