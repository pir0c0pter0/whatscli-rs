use std::io::{self, Stdout};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{Event as TerminalEvent, EventStream, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::{Mutex, mpsc};

use crate::config::Config;
use crate::model::{Chat, Command, ConnectionState, Message, SessionStatus, UiEvent};
use crate::session::SessionManager;
use crate::storage::MessageDatabase;
use crate::terminal_safe_text;
use crate::ui::editor::Editor;
use crate::ui::theme::Theme;
use crate::ui::{Focus, Overlay, Toast, ToastKind, ViewModel, chat_results, palette_results};

pub struct App {
    config: Arc<Config>,
    commands: mpsc::UnboundedSender<Command>,
    events: mpsc::UnboundedReceiver<UiEvent>,
    db: Arc<Mutex<MessageDatabase>>,
    chats: Vec<Chat>,
    messages: Vec<Message>,
    editor: Editor,
    status: SessionStatus,
    focus: Focus,
    chat_index: usize,
    message_index: usize,
    selected_chat: String,
    overlay: Option<Overlay>,
    toast: Option<Toast>,
    loading_history: bool,
    should_quit: bool,
}

impl App {
    pub async fn new(config: Config) -> Result<Self> {
        let config = Arc::new(config);
        let (commands, events, db) = SessionManager::start(Arc::clone(&config)).await?;
        Ok(Self::from_parts(config, commands, events, db))
    }

    fn from_parts(
        config: Arc<Config>,
        commands: mpsc::UnboundedSender<Command>,
        events: mpsc::UnboundedReceiver<UiEvent>,
        db: Arc<Mutex<MessageDatabase>>,
    ) -> Self {
        Self {
            config,
            commands,
            events,
            db,
            chats: Vec::new(),
            messages: Vec::new(),
            editor: Editor::default(),
            status: SessionStatus::default(),
            focus: Focus::Input,
            chat_index: 0,
            message_index: 0,
            selected_chat: String::new(),
            overlay: None,
            toast: None,
            loading_history: false,
            should_quit: false,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut terminal = TerminalGuard::new()?;
        let mut reader = EventStream::new();
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        while !self.should_quit {
            self.expire_feedback();
            terminal.terminal.draw(|frame| self.draw(frame))?;
            tokio::select! {
                maybe_event = reader.next() => {
                    match maybe_event {
                        Some(Ok(TerminalEvent::Key(key))) if key.kind == crossterm::event::KeyEventKind::Press => self.handle_key(key).await?,
                        Some(Err(error)) => return Err(error.into()),
                        None => break,
                        _ => {}
                    }
                }
                maybe_event = self.events.recv() => {
                    if let Some(event) = maybe_event { self.handle_ui_event(event).await?; }
                }
                _ = tick.tick() => self.expire_feedback(),
                _ = tokio::signal::ctrl_c() => self.should_quit = true,
            }
        }
        let _ = self.commands.send(Command::new("disconnect", Vec::new()));
        Ok(())
    }

    fn draw(&self, frame: &mut ratatui::Frame<'_>) {
        crate::ui::draw(
            frame,
            &ViewModel {
                config: &self.config,
                theme: Theme::from_config(&self.config),
                chats: &self.chats,
                messages: &self.messages,
                editor: &self.editor,
                status: &self.status,
                focus: self.focus,
                chat_index: self.chat_index,
                message_index: self.message_index,
                selected_chat: &self.selected_chat,
                overlay: self.overlay.as_ref(),
                toast: self.toast.as_ref(),
                loading_history: self.loading_history,
            },
        );
    }

    async fn handle_ui_event(&mut self, event: UiEvent) -> Result<()> {
        match event {
            UiEvent::Status(status) => {
                let old = self.status.state;
                let new = status.state;
                self.status = status;
                if new == ConnectionState::Connected {
                    if matches!(self.overlay, Some(Overlay::Pairing { .. })) {
                        self.overlay = None;
                    }
                    if old != new {
                        self.show_toast(ToastKind::Success, "Connected to WhatsApp");
                    }
                } else if old != new && new == ConnectionState::Disconnected {
                    self.show_toast(ToastKind::Warning, "WhatsApp is offline");
                }
            }
            UiEvent::Message(message) => {
                let message = *message;
                let id = message.id.clone();
                if message.chat_id == self.selected_chat {
                    if let Some(existing) = self.messages.iter_mut().find(|item| item.id == id) {
                        *existing = message;
                    } else {
                        self.messages.push(message);
                    }
                    self.messages
                        .sort_by_key(|item| (item.timestamp, item.id.clone()));
                    self.message_index = self
                        .messages
                        .iter()
                        .position(|item| item.id == id)
                        .unwrap_or_else(|| self.messages.len().saturating_sub(1));
                }
                self.refresh().await;
            }
            UiEvent::Refresh => {
                self.loading_history = false;
                self.refresh().await;
            }
            UiEvent::Text(text) => {
                let lower = text.to_ascii_lowercase();
                let kind = if lower.contains("success") || lower.contains("created") {
                    ToastKind::Success
                } else {
                    ToastKind::Info
                };
                self.show_toast(kind, text);
            }
            UiEvent::ColorList => {
                self.overlay = Some(Overlay::Text {
                    title: "Terminal colors".into(),
                    body: "black · red · green · yellow · blue · purple · cyan · gray · white\nHex colors use #RRGGBB when true color is enabled.".into(),
                });
            }
            UiEvent::Error(error) => self.show_toast(ToastKind::Error, error),
            UiEvent::Qr { code, expires_in } => {
                self.overlay = Some(Overlay::Pairing {
                    code,
                    expires_at: Instant::now() + Duration::from_secs(expires_in),
                });
            }
            UiEvent::Open(target) => {
                if let Err(error) = open::that(&target) {
                    self.show_toast(ToastKind::Error, error.to_string());
                } else {
                    self.show_toast(ToastKind::Success, "Opened with the system application");
                }
            }
            UiEvent::ShowImage(path) => self.show_image(&path),
        }
        Ok(())
    }

    async fn refresh(&mut self) {
        let selected_message = self
            .messages
            .get(self.message_index)
            .map(|message| message.id.clone());
        let db = self.db.lock().await;
        self.chats = db.chats();
        if !self.selected_chat.is_empty() {
            self.messages = db.messages(&self.selected_chat);
        }
        if let Some(index) = self
            .chats
            .iter()
            .position(|chat| chat.id == self.selected_chat)
        {
            self.chat_index = index;
        }
        if let Some(index) = selected_message
            .as_ref()
            .and_then(|id| self.messages.iter().position(|message| &message.id == id))
        {
            self.message_index = index;
        }
        self.chat_index = self.chat_index.min(self.chats.len().saturating_sub(1));
        self.message_index = self
            .message_index
            .min(self.messages.len().saturating_sub(1));
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Result<()> {
        if self.overlay.is_some() {
            return self.handle_overlay_key(key).await;
        }
        let keys = &self.config.keymap;
        if matches_binding(&keys.open_palette, key) {
            self.overlay = Some(Overlay::Palette {
                query: Editor::default(),
                selected: 0,
            });
            return Ok(());
        }
        if matches_binding(&keys.search_chats, key) {
            self.overlay = Some(Overlay::ChatSearch {
                query: Editor::default(),
                selected: 0,
            });
            return Ok(());
        }
        if matches_binding(&keys.command_quit, key) {
            self.should_quit = true;
            return Ok(());
        }
        if matches_binding(&keys.switch_panels, key) {
            self.focus = if self.focus == Focus::Chats {
                Focus::Input
            } else {
                Focus::Chats
            };
            return Ok(());
        }
        if matches_binding(&keys.focus_messages, key) {
            self.focus = Focus::Messages;
            return Ok(());
        }
        if matches_binding(&keys.focus_input, key) {
            self.focus = Focus::Input;
            return Ok(());
        }
        if matches_binding(&keys.focus_chats, key) {
            self.focus = Focus::Chats;
            return Ok(());
        }
        if matches_binding(&keys.command_connect, key) {
            return self.send_command("connect", Vec::new());
        }
        if matches_binding(&keys.command_backlog, key) {
            self.loading_history = true;
            return self.send_command("backlog", Vec::new());
        }
        if matches_binding(&keys.command_read, key) {
            return self.send_command("read", Vec::new());
        }
        if matches_binding(&keys.command_help, key) {
            self.overlay = Some(Overlay::Help);
            return Ok(());
        }
        if matches_binding(&keys.copyuser, key) {
            self.copy_selected();
            return Ok(());
        }
        if matches_binding(&keys.pasteuser, key) {
            self.paste_clipboard();
            return Ok(());
        }
        match self.focus {
            Focus::Input => self.handle_input_key(key).await,
            Focus::Chats => self.handle_chat_key(key),
            Focus::Messages => self.handle_message_key(key),
        }
    }

    async fn handle_overlay_key(&mut self, key: KeyEvent) -> Result<()> {
        if key.code == KeyCode::Esc {
            self.overlay = None;
            self.focus = Focus::Input;
            return Ok(());
        }
        match self.overlay.as_mut() {
            Some(Overlay::Palette { query, selected }) => {
                let count = palette_results(query.text()).len();
                match key.code {
                    KeyCode::Up => *selected = selected.saturating_sub(1),
                    KeyCode::Down => *selected = (*selected + 1).min(count.saturating_sub(1)),
                    KeyCode::Enter => {
                        let command = palette_results(query.text()).get(*selected).copied();
                        if let Some(command) = command {
                            self.overlay = None;
                            return self.activate_palette(command).await;
                        }
                    }
                    _ => edit_query(query, key),
                }
            }
            Some(Overlay::ChatSearch { query, selected }) => {
                let results = chat_results(query.text(), &self.chats);
                match key.code {
                    KeyCode::Up => *selected = selected.saturating_sub(1),
                    KeyCode::Down => {
                        *selected = (*selected + 1).min(results.len().saturating_sub(1))
                    }
                    KeyCode::Enter => {
                        if let Some(index) = results.get(*selected).copied() {
                            self.chat_index = index;
                            self.overlay = None;
                            self.focus = Focus::Input;
                            return self.select_current_chat();
                        }
                    }
                    _ => {
                        edit_query(query, key);
                        *selected = 0;
                    }
                }
            }
            Some(Overlay::Confirm {
                command, params, ..
            }) if key.code == KeyCode::Enter => {
                let command = command.clone();
                let params = params.clone();
                self.overlay = None;
                if command == "quit" {
                    self.should_quit = true;
                    return Ok(());
                }
                return self.send_command(&command, params);
            }
            _ => {}
        }
        Ok(())
    }

    async fn activate_palette(&mut self, command: crate::ui::CommandSpec) -> Result<()> {
        if command.name == "help" {
            self.overlay = Some(Overlay::Help);
        } else if command.name == "quit" {
            self.should_quit = true;
        } else if command.needs_args {
            self.editor.set(format!(
                "{}{name} ",
                self.config.general.cmd_prefix,
                name = command.name
            ));
            self.focus = Focus::Input;
        } else if command.destructive {
            self.overlay = Some(Overlay::Confirm {
                title: format!("Run /{}?", command.name),
                command: command.name.into(),
                params: Vec::new(),
            });
        } else {
            if command.name == "backlog" {
                self.loading_history = true;
            }
            self.send_command(command.name, Vec::new())?;
        }
        Ok(())
    }

    async fn handle_input_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Enter => self.submit_input().await,
            KeyCode::Esc => {
                self.focus = Focus::Input;
                Ok(())
            }
            KeyCode::Left => {
                self.editor.move_left();
                Ok(())
            }
            KeyCode::Right => {
                self.editor.move_right();
                Ok(())
            }
            KeyCode::Home => {
                self.editor.home();
                Ok(())
            }
            KeyCode::End => {
                self.editor.end();
                Ok(())
            }
            KeyCode::Backspace => {
                self.editor.backspace();
                Ok(())
            }
            KeyCode::Delete => {
                self.editor.delete();
                Ok(())
            }
            KeyCode::Char(ch)
                if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
            {
                self.editor.insert(ch);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn handle_chat_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Esc => self.focus = Focus::Input,
            KeyCode::Up | KeyCode::Char('k') => self.chat_index = self.chat_index.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => {
                self.chat_index = (self.chat_index + 1).min(self.chats.len().saturating_sub(1))
            }
            KeyCode::PageUp => self.chat_index = self.chat_index.saturating_sub(10),
            KeyCode::PageDown => {
                self.chat_index = (self.chat_index + 10).min(self.chats.len().saturating_sub(1))
            }
            KeyCode::Enter => return self.select_current_chat(),
            _ => {}
        }
        Ok(())
    }

    fn handle_message_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Esc => self.focus = Focus::Input,
            KeyCode::Up | KeyCode::Char('k') => {
                self.message_index = self.message_index.saturating_sub(1)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.message_index =
                    (self.message_index + 1).min(self.messages.len().saturating_sub(1))
            }
            KeyCode::PageUp => self.message_index = self.message_index.saturating_sub(10),
            KeyCode::PageDown => {
                self.message_index =
                    (self.message_index + 10).min(self.messages.len().saturating_sub(1))
            }
            KeyCode::Char('g') => self.message_index = 0,
            KeyCode::Char('G') => self.message_index = self.messages.len().saturating_sub(1),
            _ if matches_binding(&self.config.keymap.message_info, key) => {
                if let Some(message) = self.messages.get(self.message_index) {
                    self.overlay = Some(Overlay::MessageInfo(message_info(message)));
                }
            }
            _ if matches_binding(&self.config.keymap.message_revoke, key) => {
                if let Some(message) = self.messages.get(self.message_index) {
                    self.overlay = Some(Overlay::Confirm {
                        title: "Revoke the selected message?".into(),
                        command: "revoke".into(),
                        params: vec![message.id.clone()],
                    });
                }
            }
            _ => {
                let command = if matches_binding(&self.config.keymap.message_download, key) {
                    Some("download")
                } else if matches_binding(&self.config.keymap.message_open, key) {
                    Some("open")
                } else if matches_binding(&self.config.keymap.message_show, key) {
                    Some("show")
                } else if matches_binding(&self.config.keymap.message_url, key) {
                    Some("url")
                } else {
                    None
                };
                if let Some(command) = command
                    && let Some(message) = self.messages.get(self.message_index)
                {
                    self.send_command(command, vec![message.id.clone()])?;
                }
            }
        }
        Ok(())
    }

    async fn submit_input(&mut self) -> Result<()> {
        let text = self.editor.take();
        if text.trim().is_empty() {
            return Ok(());
        }
        let prefix = &self.config.general.cmd_prefix;
        if let Some(body) = text.strip_prefix(prefix) {
            let parts = shell_words::split(body)
                .unwrap_or_else(|_| body.split_whitespace().map(str::to_owned).collect());
            let Some((name, params)) = parts.split_first() else {
                return Ok(());
            };
            match name.as_str() {
                "help" => self.overlay = Some(Overlay::Help),
                "commands" => {
                    self.overlay = Some(Overlay::Palette {
                        query: Editor::default(),
                        selected: 0,
                    })
                }
                "quit" => self.should_quit = true,
                "logout" | "reset" | "leave" | "remove" | "removeadmin" => {
                    self.overlay = Some(Overlay::Confirm {
                        title: format!("Run /{name}?"),
                        command: name.clone(),
                        params: params.to_vec(),
                    });
                }
                "backlog" | "more" => {
                    self.loading_history = true;
                    self.send_command(name, params.to_vec())?;
                }
                _ => self.send_command(name, params.to_vec())?,
            }
        } else if self.selected_chat.is_empty() {
            self.show_toast(ToastKind::Warning, "Select a conversation before sending");
            self.editor.set(text);
        } else {
            self.send_command("send", vec![self.selected_chat.clone(), text])?;
        }
        Ok(())
    }

    fn select_current_chat(&mut self) -> Result<()> {
        let Some(chat) = self.chats.get(self.chat_index) else {
            return Ok(());
        };
        self.selected_chat.clone_from(&chat.id);
        self.message_index = 0;
        self.send_command("select", vec![chat.id.clone()])
    }

    fn send_command(&self, name: &str, params: Vec<String>) -> Result<()> {
        self.commands
            .send(Command::new(name, params))
            .map_err(|_| anyhow::anyhow!("session manager stopped"))
    }

    fn copy_selected(&mut self) {
        let value = if self.focus == Focus::Messages {
            self.messages
                .get(self.message_index)
                .map(|message| message.contact_id.clone())
        } else {
            self.chats.get(self.chat_index).map(|chat| chat.id.clone())
        };
        if let Some(value) = value {
            match arboard::Clipboard::new().and_then(|mut clipboard| clipboard.set_text(value)) {
                Ok(()) => self.show_toast(ToastKind::Success, "User ID copied"),
                Err(error) => {
                    self.show_toast(ToastKind::Error, format!("Clipboard unavailable: {error}"))
                }
            }
        }
    }

    fn paste_clipboard(&mut self) {
        match arboard::Clipboard::new().and_then(|mut clipboard| clipboard.get_text()) {
            Ok(text) => self.editor.insert_str(&text),
            Err(error) => {
                self.show_toast(ToastKind::Error, format!("Clipboard unavailable: {error}"))
            }
        }
    }

    fn show_image(&mut self, path: &str) {
        let Ok(mut parts) = shell_words::split(&self.config.general.show_command) else {
            self.show_toast(ToastKind::Error, "Invalid show_command");
            return;
        };
        if parts.is_empty() {
            self.show_toast(ToastKind::Error, "show_command is empty");
            return;
        }
        let program = parts.remove(0);
        match std::process::Command::new(program)
            .args(parts)
            .arg(path)
            .output()
        {
            Ok(output) if output.status.success() => {
                self.overlay = Some(Overlay::Text {
                    title: "Image preview".into(),
                    body: String::from_utf8_lossy(&output.stdout).into_owned(),
                });
            }
            Ok(output) => {
                self.show_toast(ToastKind::Error, String::from_utf8_lossy(&output.stderr))
            }
            Err(error) => self.show_toast(ToastKind::Error, error.to_string()),
        }
    }

    fn show_toast(&mut self, kind: ToastKind, message: impl AsRef<str>) {
        let message = message
            .as_ref()
            .lines()
            .find(|line| !line.trim().is_empty())
            .map(terminal_safe_text)
            .unwrap_or_default();
        self.toast = Some(Toast {
            kind,
            message,
            expires_at: Instant::now()
                + Duration::from_secs(if kind == ToastKind::Error { 6 } else { 4 }),
        });
    }

    fn expire_feedback(&mut self) {
        if self
            .toast
            .as_ref()
            .is_some_and(|toast| toast.expires_at <= Instant::now())
        {
            self.toast = None;
        }
        if matches!(self.overlay, Some(Overlay::Pairing { expires_at, .. }) if expires_at <= Instant::now())
        {
            self.overlay = None;
            self.show_toast(
                ToastKind::Warning,
                "Pairing code expired. Reconnect for a new code",
            );
        }
    }
}

fn edit_query(editor: &mut Editor, key: KeyEvent) {
    match key.code {
        KeyCode::Left => editor.move_left(),
        KeyCode::Right => editor.move_right(),
        KeyCode::Home => editor.home(),
        KeyCode::End => editor.end(),
        KeyCode::Backspace => editor.backspace(),
        KeyCode::Delete => editor.delete(),
        KeyCode::Char(ch) if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT => {
            editor.insert(ch)
        }
        _ => {}
    }
}

fn message_info(message: &Message) -> String {
    let direction = if message.from_me {
        "outgoing"
    } else {
        "incoming"
    };
    let mut lines = vec![
        format!("ID: {}", message.id),
        format!("Direction: {direction}"),
        format!("Author: {}", message.contact_name),
        format!("Type: {}", message.kind),
        format!("Chat: {}", message.chat_id),
    ];
    if message.forwarded {
        lines.push("Forwarded: yes".into());
    }
    if !message.file_name.is_empty() {
        lines.push(format!("File: {}", message.file_name));
    }
    if !message.mime_type.is_empty() {
        lines.push(format!("MIME: {}", message.mime_type));
    }
    lines.join("\n")
}

fn matches_binding(spec: &str, event: KeyEvent) -> bool {
    let normalized = spec.trim().to_ascii_lowercase();
    let (modifiers, key) = if let Some(key) = normalized.strip_prefix("ctrl+") {
        (KeyModifiers::CONTROL, key)
    } else if let Some(key) = normalized.strip_prefix("alt+") {
        (KeyModifiers::ALT, key)
    } else {
        (KeyModifiers::NONE, normalized.as_str())
    };
    if event.modifiers != modifiers
        && !(modifiers == KeyModifiers::NONE && event.modifiers == KeyModifiers::SHIFT)
    {
        return false;
    }
    match key {
        "tab" => event.code == KeyCode::Tab,
        "space" => event.code == KeyCode::Char(' '),
        "enter" => event.code == KeyCode::Enter,
        "esc" | "escape" => event.code == KeyCode::Esc,
        "?" => event.code == KeyCode::Char('?'),
        value if value.chars().count() == 1 => {
            event.code == KeyCode::Char(value.chars().next().unwrap())
        }
        _ => false,
    }
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    fn new() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        terminal.clear()?;
        Ok(Self { terminal })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MessageKind;
    use ratatui::backend::TestBackend;

    fn test_app() -> App {
        let (command_tx, _command_rx) = mpsc::unbounded_channel();
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let mut app = App::from_parts(
            Arc::new(Config::default()),
            command_tx,
            event_rx,
            Arc::new(Mutex::new(MessageDatabase::default())),
        );
        app.status.state = ConnectionState::Connected;
        app.chats = vec![
            Chat {
                id: "maria@s.whatsapp.net".into(),
                name: "Maria Oliveira".into(),
                preview: "Você viu as fotos da viagem?".into(),
                unread: 2,
                last_message: 1_750_000_000,
                ..Default::default()
            },
            Chat {
                id: "produto@g.us".into(),
                name: "Equipe Produto".into(),
                preview: "Ravi: revisão amanhã às 9h".into(),
                is_group: true,
                last_message: 1_749_990_000,
                ..Default::default()
            },
        ];
        app.selected_chat = app.chats[0].id.clone();
        app.messages = vec![
            Message {
                id: "1".into(),
                chat_id: app.selected_chat.clone(),
                contact_name: "Maria Oliveira".into(),
                contact_short: "Maria".into(),
                timestamp: 1_750_000_000,
                text: "Oi! Você viu as fotos da viagem? 📷".into(),
                kind: MessageKind::Text,
                unread: true,
                ..Default::default()
            },
            Message {
                id: "2".into(),
                chat_id: app.selected_chat.clone(),
                contact_name: "Mario".into(),
                contact_short: "Mario".into(),
                timestamp: 1_750_000_060,
                text: "Vi sim — ficaram lindas. Depois te mando as minhas também 😊".into(),
                kind: MessageKind::Text,
                from_me: true,
                ..Default::default()
            },
        ];
        app.message_index = 1;
        app
    }

    fn render(app: &App, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
        let buffer = terminal.backend().buffer();
        (0..height)
            .map(|y| {
                let line = (0..width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>();
                line.trim_end().to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn responsive_layout_snapshots() {
        let app = test_app();
        insta::assert_snapshot!("layout_wide", render(&app, 120, 32));
        insta::assert_snapshot!("layout_medium", render(&app, 86, 26));
        insta::assert_snapshot!("layout_narrow", render(&app, 60, 24));
        insta::assert_snapshot!("layout_short", render(&app, 100, 12));
    }

    #[test]
    fn overlay_snapshots() {
        let mut app = test_app();
        app.overlay = Some(Overlay::Palette {
            query: Editor::new("send"),
            selected: 0,
        });
        insta::assert_snapshot!("command_palette", render(&app, 100, 28));
        app.overlay = Some(Overlay::ChatSearch {
            query: Editor::new("maria"),
            selected: 0,
        });
        insta::assert_snapshot!("chat_search", render(&app, 100, 28));
        app.overlay = Some(Overlay::Help);
        insta::assert_snapshot!("help", render(&app, 100, 28));
        app.overlay = Some(Overlay::Pairing {
            code: "████  ██\n██  ████\n████████".into(),
            expires_at: Instant::now() + Duration::from_secs(45),
        });
        insta::assert_snapshot!("pairing", render(&app, 100, 28));
    }

    #[test]
    fn empty_toast_and_theme_snapshots() {
        let mut app = test_app();
        app.selected_chat.clear();
        app.messages.clear();
        insta::assert_snapshot!("conversation_empty", render(&app, 100, 24));

        app.toast = Some(Toast {
            kind: ToastKind::Error,
            message: "Connection failed. Try again.".into(),
            expires_at: Instant::now() + Duration::from_secs(5),
        });
        insta::assert_snapshot!("toast_error", render(&app, 100, 24));

        let mut truecolor = (*app.config).clone();
        truecolor.ui.color_mode = "truecolor".into();
        app.config = Arc::new(truecolor);
        insta::assert_snapshot!(
            "theme_truecolor",
            format!(
                "{:?}\n{}",
                Theme::from_config(&app.config),
                render(&app, 100, 24)
            )
        );

        let mut ansi = (*app.config).clone();
        ansi.ui.color_mode = "ansi16".into();
        app.config = Arc::new(ansi);
        insta::assert_snapshot!(
            "theme_ansi16",
            format!(
                "{:?}\n{}",
                Theme::from_config(&app.config),
                render(&app, 100, 24)
            )
        );
    }

    #[tokio::test]
    async fn escape_closes_overlay_before_returning_to_input() {
        let mut app = test_app();
        app.focus = Focus::Messages;
        app.overlay = Some(Overlay::Help);
        app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await
            .unwrap();
        assert!(app.overlay.is_none());
        assert_eq!(app.focus, Focus::Input);
    }

    #[tokio::test]
    async fn refresh_preserves_selected_message_by_id() {
        let mut app = test_app();
        app.message_index = 1;
        let mut db = MessageDatabase::default();
        db.add_message(app.messages[1].clone(), false);
        let mut older = app.messages[0].clone();
        older.timestamp -= 100;
        db.add_message(older, false);
        app.db = Arc::new(Mutex::new(db));
        app.refresh().await;
        assert_eq!(app.messages[app.message_index].id, "2");
    }

    #[tokio::test]
    async fn connection_state_transition_closes_pairing_overlay() {
        let mut app = test_app();
        app.status.state = ConnectionState::Pairing;
        app.overlay = Some(Overlay::Pairing {
            code: "qr".into(),
            expires_at: Instant::now() + Duration::from_secs(30),
        });
        app.handle_ui_event(UiEvent::Status(SessionStatus {
            state: ConnectionState::Connected,
            last_seen: String::new(),
        }))
        .await
        .unwrap();
        assert!(app.overlay.is_none());
        assert_eq!(app.toast.as_ref().unwrap().kind, ToastKind::Success);
    }
}
