pub mod editor;
pub mod theme;

use std::time::Instant;

use chrono::{Local, TimeZone};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Style, Stylize};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Clear, List, ListItem, ListState, Padding, Paragraph, Wrap,
};
use unicode_width::UnicodeWidthStr;

use crate::VERSION;
use crate::config::Config;
use crate::media::MediaView;
use crate::model::{
    Chat, ConnectionState, Message, MessageKind, SessionStatus, TaskCategory, TaskInfo,
};
use crate::terminal_safe_text;
use editor::{Editor, truncate_width, wrap_width};
use theme::Theme;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Input,
    Chats,
    Messages,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Interaction {
    Chat(usize),
    Message(usize),
    MessageAction(usize),
    Composer { column: u16, width: u16 },
    PaletteRow(usize),
    SearchRow(usize),
    Confirm,
    Cancel,
    MediaToggle,
    MediaSeek(f64),
    MediaVolumeDown,
    MediaVolumeUp,
    MediaMute,
    MediaClose,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HitRegion {
    pub rect: Rect,
    pub interaction: Interaction,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct InteractionMap {
    pub regions: Vec<HitRegion>,
    pub chats_area: Option<Rect>,
    pub messages_area: Option<Rect>,
    pub overlay_area: Option<Rect>,
    pub media_image_area: Option<Rect>,
}

impl InteractionMap {
    pub fn hit_region(&self, x: u16, y: u16) -> Option<HitRegion> {
        self.regions
            .iter()
            .rev()
            .find(|hit| contains(hit.rect, x, y))
            .copied()
    }

    pub fn hit(&self, x: u16, y: u16) -> Option<Interaction> {
        self.hit_region(x, y).map(|hit| hit.interaction)
    }

    pub fn in_chats(&self, x: u16, y: u16) -> bool {
        self.chats_area.is_some_and(|r| contains(r, x, y))
    }
    pub fn in_messages(&self, x: u16, y: u16) -> bool {
        self.messages_area.is_some_and(|r| contains(r, x, y))
    }
    pub fn in_overlay(&self, x: u16, y: u16) -> bool {
        self.overlay_area.is_some_and(|r| contains(r, x, y))
    }
}

fn contains(rect: Rect, x: u16, y: u16) -> bool {
    x >= rect.x
        && x < rect.x.saturating_add(rect.width)
        && y >= rect.y
        && y < rect.y.saturating_add(rect.height)
}

#[derive(Debug, Clone)]
pub enum Overlay {
    Help,
    Palette {
        query: Editor,
        selected: usize,
    },
    ChatSearch {
        query: Editor,
        selected: usize,
    },
    MessageInfo(String),
    Confirm {
        title: String,
        command: String,
        params: Vec<String>,
    },
    Pairing {
        code: String,
        expires_at: Instant,
    },
    Text {
        title: String,
        body: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastKind {
    Success,
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone)]
pub struct Toast {
    pub kind: ToastKind,
    pub message: String,
    pub expires_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub struct CommandSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub category: &'static str,
    pub shortcut: &'static str,
    pub needs_args: bool,
    pub destructive: bool,
}

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "help",
        description: "Show keyboard help",
        category: "App",
        shortcut: "Ctrl+?",
        needs_args: false,
        destructive: false,
    },
    CommandSpec {
        name: "connect",
        description: "Connect to WhatsApp",
        category: "Session",
        shortcut: "Ctrl+r",
        needs_args: false,
        destructive: false,
    },
    CommandSpec {
        name: "disconnect",
        description: "Close the current connection",
        category: "Session",
        shortcut: "",
        needs_args: false,
        destructive: false,
    },
    CommandSpec {
        name: "backlog",
        description: "Load older messages",
        category: "Messages",
        shortcut: "Ctrl+b",
        needs_args: false,
        destructive: false,
    },
    CommandSpec {
        name: "read",
        description: "Mark this chat as read",
        category: "Messages",
        shortcut: "Ctrl+n",
        needs_args: false,
        destructive: false,
    },
    CommandSpec {
        name: "upload",
        description: "Send a document",
        category: "Media",
        shortcut: "",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "sendimage",
        description: "Send an image",
        category: "Media",
        shortcut: "",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "sendvideo",
        description: "Send a video",
        category: "Media",
        shortcut: "",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "sendaudio",
        description: "Send an audio file",
        category: "Media",
        shortcut: "",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "view",
        description: "View an image or WebP sticker inside the TUI",
        category: "Media",
        shortcut: "Enter",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "play",
        description: "Play audio or video inside the TUI",
        category: "Media",
        shortcut: "Enter",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "download",
        description: "Save a media message to the downloads folder",
        category: "Media",
        shortcut: "d",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "create",
        description: "Create a group",
        category: "Groups",
        shortcut: "",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "subject",
        description: "Change group subject",
        category: "Groups",
        shortcut: "",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "add",
        description: "Add group members",
        category: "Groups",
        shortcut: "",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "remove",
        description: "Remove group members",
        category: "Groups",
        shortcut: "",
        needs_args: true,
        destructive: true,
    },
    CommandSpec {
        name: "admin",
        description: "Promote group members",
        category: "Groups",
        shortcut: "",
        needs_args: true,
        destructive: false,
    },
    CommandSpec {
        name: "removeadmin",
        description: "Remove group admins",
        category: "Groups",
        shortcut: "",
        needs_args: true,
        destructive: true,
    },
    CommandSpec {
        name: "leave",
        description: "Leave the current group",
        category: "Groups",
        shortcut: "",
        needs_args: false,
        destructive: true,
    },
    CommandSpec {
        name: "logout",
        description: "Unlink this device",
        category: "Session",
        shortcut: "",
        needs_args: false,
        destructive: true,
    },
    CommandSpec {
        name: "reset",
        description: "Remove the local session",
        category: "Session",
        shortcut: "",
        needs_args: false,
        destructive: true,
    },
    CommandSpec {
        name: "quit",
        description: "Quit WhatsCLI",
        category: "App",
        shortcut: "Ctrl+q",
        needs_args: false,
        destructive: false,
    },
];

pub fn fuzzy_score(query: &str, candidate: &str) -> Option<i32> {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return Some(0);
    }
    let candidate = candidate.to_lowercase();
    let mut score = 0;
    let mut position = 0;
    let mut previous = None;
    for needle in query.chars() {
        let found = candidate[position..].find(needle)? + position;
        score += if previous == Some(found.saturating_sub(1)) {
            8
        } else {
            2
        };
        if found == 0 || candidate[..found].ends_with([' ', '-', '_']) {
            score += 5;
        }
        score -= found as i32 / 4;
        position = found + needle.len_utf8();
        previous = Some(found);
    }
    Some(score)
}

pub fn palette_results(query: &str) -> Vec<CommandSpec> {
    let mut results: Vec<_> = COMMANDS
        .iter()
        .filter_map(|command| {
            let haystack = format!(
                "{} {} {}",
                command.name, command.description, command.category
            );
            fuzzy_score(query, &haystack).map(|score| (score, *command))
        })
        .collect();
    results.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.name.cmp(b.1.name)));
    results.into_iter().map(|(_, command)| command).collect()
}

pub fn chat_results(query: &str, chats: &[Chat]) -> Vec<usize> {
    let mut results: Vec<_> = chats
        .iter()
        .enumerate()
        .filter_map(|(index, chat)| {
            fuzzy_score(
                query,
                &format!("{} {} {}", chat.name, chat.id, chat.preview),
            )
            .map(|score| (score, index))
        })
        .collect();
    results.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    results.into_iter().map(|(_, index)| index).collect()
}

pub struct ViewModel<'a> {
    pub config: &'a Config,
    pub theme: Theme,
    pub chats: &'a [Chat],
    pub messages: &'a [Message],
    pub editor: &'a Editor,
    pub status: &'a SessionStatus,
    pub focus: Focus,
    pub chat_index: usize,
    pub message_index: usize,
    pub selected_chat: &'a str,
    pub overlay: Option<&'a Overlay>,
    pub toast: Option<&'a Toast>,
    pub active_tasks: &'a [TaskInfo],
    pub closing: bool,
    pub media: Option<&'a MediaView>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayoutMode {
    Wide,
    Compact,
    Narrow,
}

pub fn draw(frame: &mut Frame<'_>, model: &ViewModel<'_>) -> InteractionMap {
    let mut interactions = InteractionMap::default();
    let area = frame.area();
    frame.render_widget(
        Block::new().style(Style::new().bg(model.theme.background)),
        area,
    );
    if area.width < 24 || area.height < 6 {
        frame.render_widget(
            Paragraph::new("WhatsCLI needs at least 24×6")
                .style(
                    Style::new()
                        .fg(model.theme.warning)
                        .bg(model.theme.background),
                )
                .alignment(Alignment::Center),
            area,
        );
        return interactions;
    }
    let short = area.height < model.config.ui.short_height;
    let mode = if area.width >= model.config.ui.wide_breakpoint {
        LayoutMode::Wide
    } else if area.width >= model.config.ui.compact_breakpoint {
        LayoutMode::Compact
    } else {
        LayoutMode::Narrow
    };
    let header_height = if short { 1 } else { 2 };
    let composer_height = if short { 1 } else { 3 };
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(header_height),
            Constraint::Min(3),
            Constraint::Length(composer_height),
            Constraint::Length(1),
        ])
        .split(area);
    render_header(frame, vertical[0], model, short);
    match mode {
        LayoutMode::Wide | LayoutMode::Compact => {
            let sidebar = if mode == LayoutMode::Wide {
                model.config.ui.chat_sidebar_width.clamp(30, area.width / 2)
            } else {
                model.config.ui.chat_sidebar_width.clamp(22, 28)
            };
            let body = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Length(sidebar), Constraint::Min(20)])
                .split(vertical[1]);
            render_chats(frame, body[0], model, mode, &mut interactions);
            render_thread(frame, body[1], model, &mut interactions);
        }
        LayoutMode::Narrow if model.focus == Focus::Chats => {
            render_chats(frame, vertical[1], model, mode, &mut interactions)
        }
        LayoutMode::Narrow => render_thread(frame, vertical[1], model, &mut interactions),
    }
    render_composer(frame, vertical[2], model, short, &mut interactions);
    render_footer(frame, vertical[3], model, mode);
    if let Some(overlay) = model.overlay {
        render_overlay(frame, area, overlay, model, &mut interactions);
    }
    if let Some(media) = model.media {
        render_media(frame, area, media, model.theme, &mut interactions);
    }
    if let Some(toast) = model.toast {
        render_toast(frame, area, toast, model.theme);
    }
    interactions
}

fn render_header(frame: &mut Frame<'_>, area: Rect, model: &ViewModel<'_>, short: bool) {
    let state_color = match model.status.state {
        ConnectionState::Connected => model.theme.primary,
        ConnectionState::Connecting | ConnectionState::Pairing => model.theme.warning,
        ConnectionState::Disconnected => model.theme.error,
    };
    let current = model
        .chats
        .iter()
        .find(|chat| chat.id == model.selected_chat)
        .map(|chat| terminal_safe_text(&chat.name))
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "Choose a conversation".into());
    let brand = Span::styled(
        format!(" WhatsCLI {VERSION} "),
        Style::new()
            .fg(model.theme.background)
            .bg(model.theme.primary)
            .bold(),
    );
    let status = Span::styled(
        format!(" ● {} ", model.status.state.label()),
        Style::new().fg(state_color).bg(model.theme.surface).bold(),
    );
    let available = area
        .width
        .saturating_sub(brand.width() as u16 + status.width() as u16 + 2);
    let line = Line::from(vec![
        brand,
        Span::styled(
            format!(" {}", truncate_width(&current, available as usize)),
            Style::new()
                .fg(model.theme.foreground)
                .bg(model.theme.surface)
                .bold(),
        ),
        status,
    ])
    .style(Style::new().bg(model.theme.surface));
    frame.render_widget(Paragraph::new(line), area);
    if !short && area.height > 1 {
        frame.render_widget(
            Paragraph::new("  conversations / thread / composer")
                .style(Style::new().fg(model.theme.muted).bg(model.theme.surface)),
            Rect::new(area.x, area.y + 1, area.width, 1),
        );
    }
}

fn render_chats(
    frame: &mut Frame<'_>,
    area: Rect,
    model: &ViewModel<'_>,
    mode: LayoutMode,
    interactions: &mut InteractionMap,
) {
    let full = mode == LayoutMode::Wide && area.height >= 12;
    let items = model
        .chats
        .iter()
        .map(|chat| {
            let name = if chat.name.is_empty() {
                chat.id.split('@').next().unwrap_or(&chat.id)
            } else {
                &chat.name
            };
            let badge = if chat.unread > 0 {
                format!(" [{}]", chat.unread)
            } else {
                String::new()
            };
            let row_width = area.width.saturating_sub(4) as usize;
            let badge_width = UnicodeWidthStr::width(badge.as_str());
            let name = truncate_width(
                &terminal_safe_text(name),
                row_width.saturating_sub(badge_width),
            );
            let mut lines = vec![Line::from(vec![
                Span::styled(
                    if chat.is_group { "# " } else { "• " },
                    Style::new().fg(model.theme.primary),
                ),
                Span::styled(name, Style::new().fg(model.theme.foreground).bold()),
                Span::styled(badge, Style::new().fg(model.theme.warning).bold()),
            ])];
            if full {
                let direction = if chat.last_from_me { "You: " } else { "" };
                lines.push(Line::styled(
                    truncate_width(
                        &format!("  {direction}{}", terminal_safe_text(&chat.preview)),
                        row_width,
                    ),
                    Style::new().fg(model.theme.muted),
                ));
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("  {}  ", kind_icon(chat.last_message_kind)),
                        Style::new().fg(model.theme.info),
                    ),
                    Span::styled(
                        chat_time(chat.last_message),
                        Style::new().fg(model.theme.muted),
                    ),
                ]));
            } else if mode != LayoutMode::Narrow && !chat.preview.is_empty() {
                lines.push(Line::styled(
                    truncate_width(
                        &format!("  {}", terminal_safe_text(&chat.preview)),
                        row_width,
                    ),
                    Style::new().fg(model.theme.muted),
                ));
            }
            ListItem::new(Text::from(lines))
        })
        .collect::<Vec<_>>();
    let border = if model.focus == Focus::Chats {
        model.theme.primary
    } else {
        model.theme.elevated
    };
    let block = Block::new()
        .title(Line::from(" Conversations ").style(Style::new().fg(model.theme.muted)))
        .borders(Borders::RIGHT)
        .border_style(Style::new().fg(border))
        .padding(Padding::horizontal(1))
        .style(Style::new().bg(model.theme.surface));
    let inner = block.inner(area);
    let list = List::new(items)
        .block(block)
        .highlight_symbol("› ")
        .highlight_style(
            Style::new()
                .fg(model.theme.foreground)
                .bg(model.theme.elevated)
                .bold(),
        );
    let mut state =
        ListState::default().with_selected((!model.chats.is_empty()).then_some(model.chat_index));
    frame.render_stateful_widget(list, area, &mut state);
    interactions.chats_area = Some(inner);
    let row_height = if full {
        3
    } else if mode != LayoutMode::Narrow {
        2
    } else {
        1
    };
    let mut y = inner.y;
    for index in state.offset()..model.chats.len() {
        if y >= inner.bottom() {
            break;
        }
        let height = row_height.min(inner.bottom().saturating_sub(y));
        interactions.regions.push(HitRegion {
            rect: Rect::new(inner.x, y, inner.width, height),
            interaction: Interaction::Chat(index),
        });
        y = y.saturating_add(row_height);
    }
}

struct RenderedItemLayout {
    message: Option<usize>,
    height: u16,
    action: Option<(u16, u16, u16)>,
}

fn render_thread(
    frame: &mut Frame<'_>,
    area: Rect,
    model: &ViewModel<'_>,
    interactions: &mut InteractionMap,
) {
    let loading_history = model
        .active_tasks
        .iter()
        .any(|task| task.category == TaskCategory::History);
    let block = Block::new()
        .padding(Padding::horizontal(1))
        .style(Style::new().bg(model.theme.background));
    let inner = block.inner(area);
    interactions.messages_area = Some(inner);
    frame.render_widget(block, area);
    if model.selected_chat.is_empty() {
        render_empty(frame, inner, model);
        return;
    }
    if model.messages.is_empty() && !loading_history {
        frame.render_widget(
            Paragraph::new(Text::from(vec![
                Line::styled(
                    "No messages here yet",
                    Style::new().fg(model.theme.foreground).bold(),
                ),
                Line::styled(
                    "Write below to start the conversation.",
                    Style::new().fg(model.theme.muted),
                ),
            ]))
            .alignment(Alignment::Center),
            centered_rect(inner, 70, 4.min(inner.height)),
        );
        return;
    }
    let max_bubble = ((inner.width as usize * 72) / 100).clamp(12, 68);
    let first_unread = model.messages.iter().position(|message| message.unread);
    let mut selected_item = 0;
    let mut items = Vec::new();
    let mut layouts = Vec::new();
    if loading_history {
        items.push(ListItem::new(Text::from(vec![
            Line::styled(
                "░░░ loading older messages",
                Style::new().fg(model.theme.muted),
            ),
            Line::styled("  ░░░░░░░░░░░░░░░░░", Style::new().fg(model.theme.elevated)),
        ])));
        layouts.push(RenderedItemLayout {
            message: None,
            height: 2,
            action: None,
        });
    }
    for (index, message) in model.messages.iter().enumerate() {
        if first_unread == Some(index) {
            items.push(ListItem::new(Line::styled(
                "──────── unread messages ────────",
                Style::new().fg(model.theme.primary),
            )));
            layouts.push(RenderedItemLayout {
                message: None,
                height: 1,
                action: None,
            });
        }
        if index == model.message_index {
            selected_item = items.len();
        }
        let (item, height, action) = message_item(
            message,
            index == model.message_index && model.focus == Focus::Messages,
            inner.width as usize,
            max_bubble,
            model.theme,
        );
        items.push(item);
        layouts.push(RenderedItemLayout {
            message: Some(index),
            height,
            action,
        });
    }
    let list = List::new(items);
    let mut state = ListState::default().with_selected(Some(selected_item));
    frame.render_stateful_widget(list, inner, &mut state);
    let mut y = inner.y;
    for layout in layouts.into_iter().skip(state.offset()) {
        let RenderedItemLayout {
            message,
            height,
            action,
        } = layout;
        if y >= inner.bottom() {
            break;
        }
        let visible_height = height.min(inner.bottom().saturating_sub(y));
        if let Some(index) = message {
            interactions.regions.push(HitRegion {
                rect: Rect::new(inner.x, y, inner.width, visible_height),
                interaction: Interaction::Message(index),
            });
            if let Some((action_row, action_x, action_width)) = action
                && action_row < visible_height
            {
                interactions.regions.push(HitRegion {
                    rect: Rect::new(inner.x + action_x, y + action_row, action_width, 1),
                    interaction: Interaction::MessageAction(index),
                });
            }
        }
        y = y.saturating_add(height);
    }
}

fn message_item(
    message: &Message,
    selected: bool,
    area_width: usize,
    max_bubble: usize,
    theme: Theme,
) -> (ListItem<'static>, u16, Option<(u16, u16, u16)>) {
    let body = if message.text.is_empty() {
        kind_icon(message.kind).to_owned()
    } else {
        terminal_safe_text(&message.text)
    };
    let body_lines = wrap_width(&body, max_bubble.saturating_sub(2));
    let author = if message.from_me {
        "You"
    } else if message.contact_short.is_empty() {
        "Unknown"
    } else {
        &message.contact_short
    };
    let time = Local
        .timestamp_opt(message.timestamp as i64, 0)
        .single()
        .map(|date| date.format("%H:%M").to_string())
        .unwrap_or_default();
    let mut metadata = Vec::new();
    if message.forwarded {
        metadata.push("forwarded".to_owned());
    }
    if message.kind != MessageKind::Text {
        metadata.push(message.kind.to_string());
    }
    if !message.file_name.is_empty() {
        metadata.push(message.file_name.clone());
    }
    let meta = format!(
        "{}{}",
        if metadata.is_empty() {
            String::new()
        } else {
            format!("{} · ", metadata.join(" · "))
        },
        time
    );
    let action = message_action(message.kind);
    let content_width = body_lines
        .iter()
        .map(|line| UnicodeWidthStr::width(line.as_str()))
        .chain([
            UnicodeWidthStr::width(author),
            UnicodeWidthStr::width(meta.as_str()),
            action.map(UnicodeWidthStr::width).unwrap_or_default(),
        ])
        .max()
        .unwrap_or(1)
        .min(max_bubble.saturating_sub(2));
    let bubble_width = content_width + 2;
    let author = truncate_width(author, content_width);
    let meta = truncate_width(&meta, content_width);
    let offset = if message.from_me {
        area_width.saturating_sub(bubble_width)
    } else {
        0
    };
    let background = if message.from_me {
        theme.outgoing
    } else {
        theme.incoming
    };
    let marker = if message.from_me { "▶" } else { "◀" };
    let selected_marker = if selected { "›" } else { " " };
    let mut lines = vec![bubble_line(
        offset,
        &format!("{selected_marker}{author}"),
        bubble_width,
        Style::new()
            .fg(if selected {
                theme.warning
            } else {
                theme.primary
            })
            .bg(background)
            .bold(),
    )];
    for line in body_lines {
        lines.push(bubble_line(
            offset,
            &format!(" {line}"),
            bubble_width,
            Style::new().fg(theme.foreground).bg(background),
        ));
    }
    let action_row = if let Some(action) = action {
        let row = lines.len() as u16;
        lines.push(bubble_line(
            offset,
            &format!(" {action}"),
            bubble_width,
            Style::new().fg(theme.info).bg(background).bold(),
        ));
        Some((
            row,
            offset.saturating_add(1) as u16,
            UnicodeWidthStr::width(action) as u16,
        ))
    } else {
        None
    };
    lines.push(bubble_line(
        offset,
        &format!(" {meta} {marker}"),
        bubble_width,
        Style::new().fg(theme.muted).bg(background),
    ));
    lines.push(Line::from(""));
    let height = lines.len() as u16;
    (ListItem::new(Text::from(lines)), height, action_row)
}

fn message_action(kind: MessageKind) -> Option<&'static str> {
    match kind {
        MessageKind::Image | MessageKind::Sticker => Some("[ Visualizar ]"),
        MessageKind::Audio | MessageKind::Video => Some("[ Reproduzir ]"),
        MessageKind::Document => Some("[ Baixar ]"),
        _ => None,
    }
}

fn bubble_line(offset: usize, text: &str, width: usize, style: Style) -> Line<'static> {
    let used = UnicodeWidthStr::width(text);
    Line::from(vec![
        Span::raw(" ".repeat(offset)),
        Span::styled(
            format!("{text}{}", " ".repeat(width.saturating_sub(used))),
            style,
        ),
    ])
}

fn render_empty(frame: &mut Frame<'_>, area: Rect, model: &ViewModel<'_>) {
    frame.render_widget(
        Paragraph::new(Text::from(vec![
            Line::styled("◉", Style::new().fg(model.theme.primary).bold()),
            Line::styled(
                "Your conversations, without leaving the terminal",
                Style::new().fg(model.theme.foreground).bold(),
            ),
            Line::styled(
                "Select a chat on the left or press Ctrl+f to search.",
                Style::new().fg(model.theme.muted),
            ),
            Line::styled(
                "Ctrl+p opens every action.",
                Style::new().fg(model.theme.muted),
            ),
        ]))
        .alignment(Alignment::Center),
        centered_rect(area, 80, 6.min(area.height)),
    );
}

fn render_composer(
    frame: &mut Frame<'_>,
    area: Rect,
    model: &ViewModel<'_>,
    short: bool,
    interactions: &mut InteractionMap,
) {
    let active = model.focus == Focus::Input && model.overlay.is_none();
    let border = if active {
        model.theme.primary
    } else {
        model.theme.elevated
    };
    let mut block = Block::new()
        .borders(if short { Borders::TOP } else { Borders::ALL })
        .border_style(Style::new().fg(border))
        .padding(Padding::horizontal(1))
        .style(Style::new().bg(model.theme.surface));
    if !short {
        block = block.title(Line::from(" Message ").style(Style::new().fg(model.theme.muted)));
    }
    let inner = block.inner(area);
    interactions.regions.push(HitRegion {
        rect: inner,
        interaction: Interaction::Composer {
            column: 0,
            width: inner.width,
        },
    });
    frame.render_widget(block, area);
    let placeholder = if model.selected_chat.is_empty() {
        "Select a conversation to start"
    } else if model.status.connected() {
        "Type a message or /command"
    } else {
        "Offline — commands are still available"
    };
    if model.editor.is_empty() {
        frame.render_widget(
            Paragraph::new(placeholder)
                .style(Style::new().fg(model.theme.muted).bg(model.theme.surface)),
            inner,
        );
    } else {
        let (visible, _) = model.editor.viewport(inner.width as usize);
        frame.render_widget(
            Paragraph::new(visible).style(
                Style::new()
                    .fg(model.theme.foreground)
                    .bg(model.theme.surface),
            ),
            inner,
        );
    }
    if active && inner.width > 0 && inner.height > 0 {
        let (_, cursor) = model.editor.viewport(inner.width as usize);
        frame.set_cursor_position((inner.x + cursor.min(inner.width.saturating_sub(1)), inner.y));
    }
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, model: &ViewModel<'_>, mode: LayoutMode) {
    let hints = match model.focus {
        Focus::Input => " Enter send  ·  Ctrl+p commands  ·  Ctrl+f search  ·  Tab chats ",
        Focus::Chats => " ↑↓ navigate  ·  Enter open  ·  Tab composer  ·  Ctrl+f search ",
        Focus::Messages => {
            " ↑↓ select  ·  Enter action  ·  i info  ·  d download  ·  Esc composer "
        }
    };
    let narrow = mode == LayoutMode::Narrow;
    let hints = if narrow {
        hints.replace("  ·  ", " · ")
    } else {
        hints.to_owned()
    };
    let task = if model.closing {
        Some("◌ closing".to_owned())
    } else {
        model.active_tasks.last().map(|latest| {
            let additional = model.active_tasks.len().saturating_sub(1);
            format!(
                "◌ {}{}",
                latest.label,
                if additional == 0 {
                    String::new()
                } else {
                    format!(" +{additional}")
                }
            )
        })
    };
    let value = task.map_or(hints.clone(), |task| format!(" {task}  · {hints}"));
    frame.render_widget(
        Paragraph::new(truncate_width(&value, area.width as usize)).style(
            Style::new()
                .fg(model.theme.muted)
                .bg(model.theme.background),
        ),
        area,
    );
}

fn render_overlay(
    frame: &mut Frame<'_>,
    area: Rect,
    overlay: &Overlay,
    model: &ViewModel<'_>,
    interactions: &mut InteractionMap,
) {
    match overlay {
        Overlay::Pairing { code, expires_at } => {
            render_pairing(frame, area, code, *expires_at, model.theme)
        }
        Overlay::Palette { query, selected } => {
            let modal = modal(frame, area, 76, 20, " Command palette ", model.theme);
            interactions.overlay_area = Some(modal);
            let parts = Layout::vertical([
                Constraint::Length(3),
                Constraint::Min(2),
                Constraint::Length(1),
            ])
            .split(modal);
            render_query(frame, parts[0], query, "Search actions…", model.theme, true);
            let results = palette_results(query.text());
            let items = results.iter().map(|command| {
                ListItem::new(Line::from(vec![
                    Span::styled(
                        format!("/{:<12}", command.name),
                        Style::new().fg(model.theme.primary).bold(),
                    ),
                    Span::styled(
                        format!("{:<10}", command.category),
                        Style::new().fg(model.theme.muted),
                    ),
                    Span::styled(command.description, Style::new().fg(model.theme.foreground)),
                    Span::styled(
                        format!("  {}", command.shortcut),
                        Style::new().fg(model.theme.warning),
                    ),
                ]))
            });
            let mut state = ListState::default().with_selected(
                (!results.is_empty()).then_some((*selected).min(results.len().saturating_sub(1))),
            );
            frame.render_stateful_widget(
                List::new(items)
                    .highlight_symbol("› ")
                    .highlight_style(Style::new().bg(model.theme.elevated).bold()),
                parts[1],
                &mut state,
            );
            for row in state.offset()..results.len().min(state.offset() + parts[1].height as usize)
            {
                interactions.regions.push(HitRegion {
                    rect: Rect::new(
                        parts[1].x,
                        parts[1].y + (row - state.offset()) as u16,
                        parts[1].width,
                        1,
                    ),
                    interaction: Interaction::PaletteRow(row),
                });
            }
            frame.render_widget(
                Paragraph::new("↑↓ choose  Enter run  Esc close")
                    .style(Style::new().fg(model.theme.muted)),
                parts[2],
            );
        }
        Overlay::ChatSearch { query, selected } => {
            let modal = modal(frame, area, 72, 18, " Find a conversation ", model.theme);
            interactions.overlay_area = Some(modal);
            let parts = Layout::vertical([
                Constraint::Length(3),
                Constraint::Min(2),
                Constraint::Length(1),
            ])
            .split(modal);
            render_query(
                frame,
                parts[0],
                query,
                "Name, number or message…",
                model.theme,
                true,
            );
            let results = chat_results(query.text(), model.chats);
            let items = results.iter().map(|index| {
                let chat = &model.chats[*index];
                ListItem::new(Line::from(vec![
                    Span::styled(
                        format!("{:<24}", truncate_width(&chat.name, 23)),
                        Style::new().fg(model.theme.foreground).bold(),
                    ),
                    Span::styled(
                        truncate_width(&chat.preview, 36),
                        Style::new().fg(model.theme.muted),
                    ),
                    Span::styled(
                        if chat.unread > 0 {
                            format!("  [{}]", chat.unread)
                        } else {
                            String::new()
                        },
                        Style::new().fg(model.theme.warning),
                    ),
                ]))
            });
            let mut state = ListState::default().with_selected(
                (!results.is_empty()).then_some((*selected).min(results.len().saturating_sub(1))),
            );
            frame.render_stateful_widget(
                List::new(items)
                    .highlight_symbol("› ")
                    .highlight_style(Style::new().bg(model.theme.elevated).bold()),
                parts[1],
                &mut state,
            );
            for row in state.offset()..results.len().min(state.offset() + parts[1].height as usize)
            {
                interactions.regions.push(HitRegion {
                    rect: Rect::new(
                        parts[1].x,
                        parts[1].y + (row - state.offset()) as u16,
                        parts[1].width,
                        1,
                    ),
                    interaction: Interaction::SearchRow(row),
                });
            }
            frame.render_widget(
                Paragraph::new(format!(
                    "{} matches  ·  Enter open  ·  Esc close",
                    results.len()
                ))
                .style(Style::new().fg(model.theme.muted)),
                parts[2],
            );
        }
        Overlay::Help => {
            let modal = modal(frame, area, 72, 24, " Keyboard & mouse help ", model.theme);
            interactions.overlay_area = Some(modal);
            let k = &model.config.keymap;
            let help = vec![
                Line::styled("Navigation", Style::new().fg(model.theme.primary).bold()),
                Line::from(format!("  {:<12} switch chats/composer", k.switch_panels)),
                Line::from(format!("  {:<12} focus messages", k.focus_messages)),
                Line::from(format!("  {:<12} search conversations", k.search_chats)),
                Line::from(format!("  {:<12} command palette", k.open_palette)),
                Line::from(""),
                Line::styled("Composer", Style::new().fg(model.theme.primary).bold()),
                Line::from("  ←/→ Home End   move cursor by grapheme"),
                Line::from("  Backspace/Delete edit safely · Enter sends"),
                Line::from(""),
                Line::styled("Messages", Style::new().fg(model.theme.primary).bold()),
                Line::from(format!(
                    "  {}/{}/{}/{}/{}/{}/{}  activate/download/open/show/url/info/revoke",
                    k.message_activate,
                    k.message_download,
                    k.message_open,
                    k.message_show,
                    k.message_url,
                    k.message_info,
                    k.message_revoke
                )),
                Line::from("  Media: Space pause · ←/→ seek · -/+ volume · m mute"),
                Line::from(""),
                Line::styled("Mouse", Style::new().fg(model.theme.primary).bold()),
                Line::from("  Click selects · media button activates · wheel navigates"),
                Line::from(""),
                Line::styled(
                    format!("Config: {}", model.config.config_file.display()),
                    Style::new().fg(model.theme.muted),
                ),
                Line::styled(
                    "Esc closes this overlay",
                    Style::new().fg(model.theme.warning),
                ),
            ];
            frame.render_widget(Paragraph::new(help).wrap(Wrap { trim: false }), modal);
        }
        Overlay::MessageInfo(body) => {
            interactions.overlay_area = Some(render_text_modal(
                frame,
                area,
                " Message details ",
                body,
                model.theme,
            ));
        }
        Overlay::Text { title, body } => {
            interactions.overlay_area = Some(render_text_modal(
                frame,
                area,
                &format!(" {title} "),
                body,
                model.theme,
            ));
        }
        Overlay::Confirm { title, .. } => {
            let modal = modal(frame, area, 62, 10, " Confirm action ", model.theme);
            interactions.overlay_area = Some(modal);
            frame.render_widget(
                Paragraph::new(Text::from(vec![
                    Line::styled(
                        title.clone(),
                        Style::new().fg(model.theme.foreground).bold(),
                    ),
                    Line::from(""),
                    Line::styled(
                        "This can affect your WhatsApp account or conversation.",
                        Style::new().fg(model.theme.warning),
                    ),
                    Line::from(""),
                    Line::styled(
                        "[ Confirm ]  ·  [ Cancel ]",
                        Style::new().fg(model.theme.muted),
                    ),
                ]))
                .wrap(Wrap { trim: false }),
                modal,
            );
            interactions.regions.push(HitRegion {
                rect: Rect::new(modal.x, modal.y.saturating_add(4), modal.width.min(11), 1),
                interaction: Interaction::Confirm,
            });
            interactions.regions.push(HitRegion {
                rect: Rect::new(
                    modal.x.saturating_add(16),
                    modal.y.saturating_add(4),
                    modal.width.saturating_sub(16).min(10),
                    1,
                ),
                interaction: Interaction::Cancel,
            });
        }
    }
}

fn render_media(
    frame: &mut Frame<'_>,
    area: Rect,
    media: &MediaView,
    theme: Theme,
    interactions: &mut InteractionMap,
) {
    let playable = matches!(media.kind, MessageKind::Audio | MessageKind::Video);
    let height = area.height.saturating_sub(2).max(6);
    let inner = modal(
        frame,
        area,
        88,
        height,
        &format!(" {} · {} · {:?} ", media.title, media.kind, media.protocol),
        theme,
    );
    interactions.overlay_area = Some(inner);
    let error_height = if media.error.is_some() { 2 } else { 0 };
    let parts = Layout::vertical([
        Constraint::Min(2),
        Constraint::Length(u16::from(playable)),
        Constraint::Length(1),
        Constraint::Length(error_height),
    ])
    .split(inner);
    if matches!(
        media.kind,
        MessageKind::Image | MessageKind::Sticker | MessageKind::Video
    ) {
        interactions.media_image_area = Some(parts[0]);
    } else {
        frame.render_widget(
            Paragraph::new("♪\nAudio playback")
                .alignment(Alignment::Center)
                .style(Style::new().fg(theme.primary).bold()),
            centered_rect(parts[0], 50, 2),
        );
    }
    if playable {
        let progress_width = parts[1].width.saturating_sub(16);
        let fraction = if media.duration.is_zero() {
            0.0
        } else {
            (media.position.as_secs_f64() / media.duration.as_secs_f64()).clamp(0.0, 1.0)
        };
        let filled = (f64::from(progress_width) * fraction).round() as usize;
        let bar = format!(
            "{}{}",
            "━".repeat(filled),
            "─".repeat(progress_width as usize - filled)
        );
        let progress = format!(
            "{} {} / {}",
            bar,
            media_time(media.position),
            media_time(media.duration)
        );
        frame.render_widget(
            Paragraph::new(truncate_width(&progress, parts[1].width as usize))
                .style(Style::new().fg(theme.primary)),
            parts[1],
        );
        interactions.regions.push(HitRegion {
            rect: Rect::new(parts[1].x, parts[1].y, progress_width, 1),
            interaction: Interaction::MediaSeek(0.0),
        });
    }
    let play = if media.playing {
        "[ Pause ]"
    } else {
        "[ Play ]"
    };
    let mute = if media.muted {
        "[ Unmute ]"
    } else {
        "[ Mute ]"
    };
    let controls = if playable {
        format!("{play}  [ -10s ]  [ +10s ]  [ Vol- ]  [ Vol+ ]  {mute}  [ Close ]")
    } else {
        "[ Close ]".to_owned()
    };
    frame.render_widget(
        Paragraph::new(truncate_width(&controls, parts[2].width as usize))
            .style(Style::new().fg(theme.foreground)),
        parts[2],
    );
    let mut x = parts[2].x;
    let playable_controls = [
        (play, Interaction::MediaToggle),
        ("[ -10s ]", Interaction::MediaSeek(-10.0)),
        ("[ +10s ]", Interaction::MediaSeek(10.0)),
        ("[ Vol- ]", Interaction::MediaVolumeDown),
        ("[ Vol+ ]", Interaction::MediaVolumeUp),
        (mute, Interaction::MediaMute),
        ("[ Close ]", Interaction::MediaClose),
    ];
    let image_controls = [("[ Close ]", Interaction::MediaClose)];
    let controls = if playable {
        playable_controls.as_slice()
    } else {
        image_controls.as_slice()
    };
    for &(label, interaction) in controls {
        if x >= parts[2].right() {
            break;
        }
        let width = (UnicodeWidthStr::width(label) as u16).min(parts[2].right().saturating_sub(x));
        interactions.regions.push(HitRegion {
            rect: Rect::new(x, parts[2].y, width, 1),
            interaction,
        });
        x = x.saturating_add(width + 2);
    }
    if let Some(error) = &media.error {
        frame.render_widget(
            Paragraph::new(error.as_str())
                .style(Style::new().fg(theme.error))
                .wrap(Wrap { trim: true }),
            parts[3],
        );
    }
}

fn media_time(duration: std::time::Duration) -> String {
    let total = duration.as_secs();
    format!("{}:{:02}", total / 60, total % 60)
}

fn render_pairing(
    frame: &mut Frame<'_>,
    area: Rect,
    code: &str,
    expires_at: Instant,
    theme: Theme,
) {
    let (code_width, code_height) = pairing_code_size(code);
    let modal_area = pairing_modal_area(area, code_width, code_height);
    frame.render_widget(Clear, modal_area);
    let block = Block::new()
        .title(" Link WhatsCLI ")
        .borders(Borders::ALL)
        .border_style(Style::new().fg(theme.primary))
        .padding(Padding::uniform(1))
        .style(Style::new().bg(theme.surface));
    let inner = block.inner(modal_area);
    frame.render_widget(block, modal_area);
    if inner.width < code_width
        || inner.height < code_height.saturating_add(PAIRING_FIXED_CONTENT_HEIGHT)
    {
        let message_area = centered_rect(inner, 100, 4.min(inner.height));
        frame.render_widget(
            Paragraph::new(Text::from(vec![
                Line::styled(
                    "Terminal too small for this QR code",
                    Style::new().fg(theme.warning).bold(),
                ),
                Line::styled(
                    format!(
                        "Resize to at least {} columns × {} rows",
                        code_width.saturating_add(PAIRING_CHROME_WIDTH),
                        code_height
                            .saturating_add(PAIRING_FIXED_CONTENT_HEIGHT)
                            .saturating_add(PAIRING_CHROME_HEIGHT)
                    ),
                    Style::new().fg(theme.muted),
                ),
                Line::from(""),
                Line::styled("Esc hides this code", Style::new().fg(theme.error)),
            ]))
            .alignment(Alignment::Center),
            message_area,
        );
        return;
    }
    let remaining = expires_at
        .saturating_duration_since(Instant::now())
        .as_secs();
    let parts = Layout::vertical([
        Constraint::Length(PAIRING_HEADER_HEIGHT),
        Constraint::Min(3),
        Constraint::Length(PAIRING_FOOTER_HEIGHT),
    ])
    .split(inner);
    frame.render_widget(
        Paragraph::new(Text::from(vec![
            Line::styled(
                "Scan with WhatsApp on your phone",
                Style::new().fg(theme.foreground).bold(),
            ),
            Line::styled(
                "Settings → Linked devices → Link a device",
                Style::new().fg(theme.muted),
            ),
        ]))
        .alignment(Alignment::Center),
        parts[0],
    );
    frame.render_widget(
        Paragraph::new(code.to_owned())
            .style(Style::new().fg(theme.foreground).bg(theme.surface))
            .alignment(Alignment::Center),
        parts[1],
    );
    frame.render_widget(
        Paragraph::new(format!("Expires in {remaining}s  ·  Esc hides this code"))
            .style(Style::new().fg(if remaining < 15 {
                theme.error
            } else {
                theme.warning
            }))
            .alignment(Alignment::Center),
        parts[2],
    );
}

const PAIRING_HEADER_HEIGHT: u16 = 3;
const PAIRING_FOOTER_HEIGHT: u16 = 2;
const PAIRING_FIXED_CONTENT_HEIGHT: u16 = PAIRING_HEADER_HEIGHT + PAIRING_FOOTER_HEIGHT;
// One cell each for the border and padding on every side.
const PAIRING_CHROME_WIDTH: u16 = 4;
const PAIRING_CHROME_HEIGHT: u16 = 4;

fn pairing_code_size(code: &str) -> (u16, u16) {
    let width = code
        .lines()
        .map(UnicodeWidthStr::width)
        .max()
        .unwrap_or_default()
        .min(u16::MAX as usize) as u16;
    let height = code.lines().count().min(u16::MAX as usize) as u16;
    (width, height)
}

fn pairing_modal_area(area: Rect, code_width: u16, code_height: u16) -> Rect {
    let default_width = area.width.saturating_mul(90) / 100;
    let width = default_width
        .max(code_width.saturating_add(PAIRING_CHROME_WIDTH))
        .clamp(1, area.width);
    let desired_height = code_height
        .saturating_add(PAIRING_FIXED_CONTENT_HEIGHT)
        .saturating_add(PAIRING_CHROME_HEIGHT);
    let height_with_margin = area.height.saturating_sub(2);
    let height = if desired_height <= height_with_margin {
        // Preserve the roomy layout for small placeholder/test QR codes.
        desired_height.max(38).min(height_with_margin)
    } else {
        // A real WhatsApp QR often needs the last two rows that the old fixed
        // 38-row modal clipped. Use the full terminal before giving up.
        desired_height.min(area.height)
    };
    centered_size(area, width, height.max(1))
}

fn render_text_modal(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &str,
    body: &str,
    theme: Theme,
) -> Rect {
    let modal = modal(frame, area, 72, 18, title, theme);
    frame.render_widget(
        Paragraph::new(terminal_safe_text(body))
            .style(Style::new().fg(theme.foreground))
            .wrap(Wrap { trim: false }),
        modal,
    );
    modal
}

fn modal(
    frame: &mut Frame<'_>,
    area: Rect,
    percent: u16,
    height: u16,
    title: &str,
    theme: Theme,
) -> Rect {
    let outer = centered_rect(area, percent, height.min(area.height.saturating_sub(2)));
    frame.render_widget(Clear, outer);
    let block = Block::new()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::new().fg(theme.primary))
        .padding(Padding::uniform(1))
        .style(Style::new().fg(theme.foreground).bg(theme.surface));
    let inner = block.inner(outer);
    frame.render_widget(block, outer);
    inner
}

fn render_query(
    frame: &mut Frame<'_>,
    area: Rect,
    editor: &Editor,
    placeholder: &str,
    theme: Theme,
    cursor: bool,
) {
    let block = Block::new()
        .borders(Borders::BOTTOM)
        .border_style(Style::new().fg(theme.elevated))
        .padding(Padding::horizontal(1));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let (visible, cursor_x) = editor.viewport(inner.width as usize);
    let value = if editor.is_empty() {
        placeholder.to_owned()
    } else {
        visible
    };
    frame.render_widget(
        Paragraph::new(format!("⌕ {value}")).style(
            Style::new()
                .fg(if editor.is_empty() {
                    theme.muted
                } else {
                    theme.foreground
                })
                .bg(theme.surface),
        ),
        inner,
    );
    if cursor && inner.width > 2 {
        frame.set_cursor_position((
            inner.x + 2 + cursor_x.min(inner.width.saturating_sub(3)),
            inner.y,
        ));
    }
}

fn render_toast(frame: &mut Frame<'_>, area: Rect, toast: &Toast, theme: Theme) {
    let color = match toast.kind {
        ToastKind::Success => theme.primary,
        ToastKind::Error => theme.error,
        ToastKind::Warning => theme.warning,
        ToastKind::Info => theme.info,
    };
    let width = area.width.saturating_sub(4).min(54);
    let toast_area = Rect::new(
        area.right().saturating_sub(width + 2),
        area.y + 1,
        width,
        3.min(area.height),
    );
    frame.render_widget(Clear, toast_area);
    frame.render_widget(
        Paragraph::new(truncate_width(
            &toast.message,
            width.saturating_sub(4) as usize,
        ))
        .block(
            Block::new()
                .borders(Borders::ALL)
                .border_style(Style::new().fg(color))
                .padding(Padding::horizontal(1)),
        )
        .style(Style::new().fg(theme.foreground).bg(theme.elevated)),
        toast_area,
    );
}

fn centered_rect(area: Rect, width_percent: u16, height: u16) -> Rect {
    let width = (area.width.saturating_mul(width_percent) / 100).clamp(1, area.width);
    let height = height.clamp(1, area.height);
    centered_size(area, width, height)
}

fn centered_size(area: Rect, width: u16, height: u16) -> Rect {
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

fn chat_time(timestamp: i64) -> String {
    Local
        .timestamp_opt(timestamp, 0)
        .single()
        .map(|date| date.format("%H:%M").to_string())
        .unwrap_or_default()
}

fn kind_icon(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Text => "text",
        MessageKind::Image => "▧ image",
        MessageKind::Video => "▶ video",
        MessageKind::Audio => "♪ audio",
        MessageKind::Document => "▤ document",
        MessageKind::Sticker => "▧ sticker",
        MessageKind::Unknown => "◇ message",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzzy_match_rewards_consecutive_characters() {
        assert!(
            fuzzy_score("send", "send image").unwrap()
                > fuzzy_score("send", "session ended").unwrap()
        );
        assert!(fuzzy_score("xyz", "send image").is_none());
    }

    #[test]
    fn pairing_modal_expands_to_show_every_qr_row() {
        let area = Rect::new(0, 0, 100, 46);
        let modal = pairing_modal_area(area, 73, 37);

        assert_eq!(modal.height, 46);
        assert!(modal.width >= 73 + PAIRING_CHROME_WIDTH);

        let inner_height = modal.height - PAIRING_CHROME_HEIGHT;
        assert!(inner_height >= 37 + PAIRING_FIXED_CONTENT_HEIGHT);
    }

    #[test]
    fn pairing_modal_reports_when_the_terminal_cannot_fit_the_qr() {
        let area = Rect::new(0, 0, 70, 35);
        let modal = pairing_modal_area(area, 73, 37);
        let inner_width = modal.width.saturating_sub(PAIRING_CHROME_WIDTH);
        let inner_height = modal.height.saturating_sub(PAIRING_CHROME_HEIGHT);

        assert!(
            inner_width < 73 || inner_height < 37 + PAIRING_FIXED_CONTENT_HEIGHT,
            "the test terminal should be too small for the QR code"
        );
    }
}
