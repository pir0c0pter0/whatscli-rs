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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayoutMode {
    Wide,
    Compact,
    Narrow,
}

pub fn draw(frame: &mut Frame<'_>, model: &ViewModel<'_>) {
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
        return;
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
            render_chats(frame, body[0], model, mode);
            render_thread(frame, body[1], model);
        }
        LayoutMode::Narrow if model.focus == Focus::Chats => {
            render_chats(frame, vertical[1], model, mode)
        }
        LayoutMode::Narrow => render_thread(frame, vertical[1], model),
    }
    render_composer(frame, vertical[2], model, short);
    render_footer(frame, vertical[3], model, mode);
    if let Some(overlay) = model.overlay {
        render_overlay(frame, area, overlay, model);
    }
    if let Some(toast) = model.toast {
        render_toast(frame, area, toast, model.theme);
    }
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

fn render_chats(frame: &mut Frame<'_>, area: Rect, model: &ViewModel<'_>, mode: LayoutMode) {
    let full = mode == LayoutMode::Wide && area.height >= 12;
    let items = model.chats.iter().map(|chat| {
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
    });
    let border = if model.focus == Focus::Chats {
        model.theme.primary
    } else {
        model.theme.elevated
    };
    let list = List::new(items)
        .block(
            Block::new()
                .title(Line::from(" Conversations ").style(Style::new().fg(model.theme.muted)))
                .borders(Borders::RIGHT)
                .border_style(Style::new().fg(border))
                .padding(Padding::horizontal(1))
                .style(Style::new().bg(model.theme.surface)),
        )
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
}

fn render_thread(frame: &mut Frame<'_>, area: Rect, model: &ViewModel<'_>) {
    let loading_history = model
        .active_tasks
        .iter()
        .any(|task| task.category == TaskCategory::History);
    let block = Block::new()
        .padding(Padding::horizontal(1))
        .style(Style::new().bg(model.theme.background));
    let inner = block.inner(area);
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
    if loading_history {
        items.push(ListItem::new(Text::from(vec![
            Line::styled(
                "░░░ loading older messages",
                Style::new().fg(model.theme.muted),
            ),
            Line::styled("  ░░░░░░░░░░░░░░░░░", Style::new().fg(model.theme.elevated)),
        ])));
    }
    for (index, message) in model.messages.iter().enumerate() {
        if first_unread == Some(index) {
            items.push(ListItem::new(Line::styled(
                "──────── unread messages ────────",
                Style::new().fg(model.theme.primary),
            )));
        }
        if index == model.message_index {
            selected_item = items.len();
        }
        items.push(message_item(
            message,
            index == model.message_index && model.focus == Focus::Messages,
            inner.width as usize,
            max_bubble,
            model.theme,
        ));
    }
    let list = List::new(items);
    let mut state = ListState::default().with_selected(Some(selected_item));
    frame.render_stateful_widget(list, inner, &mut state);
}

fn message_item(
    message: &Message,
    selected: bool,
    area_width: usize,
    max_bubble: usize,
    theme: Theme,
) -> ListItem<'static> {
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
    let content_width = body_lines
        .iter()
        .map(|line| UnicodeWidthStr::width(line.as_str()))
        .chain([
            UnicodeWidthStr::width(author),
            UnicodeWidthStr::width(meta.as_str()),
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
    lines.push(bubble_line(
        offset,
        &format!(" {meta} {marker}"),
        bubble_width,
        Style::new().fg(theme.muted).bg(background),
    ));
    lines.push(Line::from(""));
    ListItem::new(Text::from(lines))
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

fn render_composer(frame: &mut Frame<'_>, area: Rect, model: &ViewModel<'_>, short: bool) {
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
        Focus::Messages => " ↑↓ select  ·  i info  ·  d download  ·  Esc composer ",
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

fn render_overlay(frame: &mut Frame<'_>, area: Rect, overlay: &Overlay, model: &ViewModel<'_>) {
    match overlay {
        Overlay::Pairing { code, expires_at } => {
            render_pairing(frame, area, code, *expires_at, model.theme)
        }
        Overlay::Palette { query, selected } => {
            let modal = modal(frame, area, 76, 20, " Command palette ", model.theme);
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
            frame.render_widget(
                Paragraph::new("↑↓ choose  Enter run  Esc close")
                    .style(Style::new().fg(model.theme.muted)),
                parts[2],
            );
        }
        Overlay::ChatSearch { query, selected } => {
            let modal = modal(frame, area, 72, 18, " Find a conversation ", model.theme);
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
            let modal = modal(frame, area, 72, 20, " Keyboard help ", model.theme);
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
                    "  {}/{}/{}/{}/{}/{}  download/open/show/url/info/revoke",
                    k.message_download,
                    k.message_open,
                    k.message_show,
                    k.message_url,
                    k.message_info,
                    k.message_revoke
                )),
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
            render_text_modal(frame, area, " Message details ", body, model.theme)
        }
        Overlay::Text { title, body } => {
            render_text_modal(frame, area, &format!(" {title} "), body, model.theme)
        }
        Overlay::Confirm { title, .. } => {
            let modal = modal(frame, area, 62, 8, " Confirm action ", model.theme);
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
                        "Enter confirm  ·  Esc cancel",
                        Style::new().fg(model.theme.muted),
                    ),
                ]))
                .wrap(Wrap { trim: false }),
                modal,
            );
        }
    }
}

fn render_pairing(
    frame: &mut Frame<'_>,
    area: Rect,
    code: &str,
    expires_at: Instant,
    theme: Theme,
) {
    let modal_area = centered_rect(area, 90, area.height.saturating_sub(2).min(38));
    frame.render_widget(Clear, modal_area);
    let block = Block::new()
        .title(" Link WhatsCLI ")
        .borders(Borders::ALL)
        .border_style(Style::new().fg(theme.primary))
        .padding(Padding::uniform(1))
        .style(Style::new().bg(theme.surface));
    let inner = block.inner(modal_area);
    frame.render_widget(block, modal_area);
    let remaining = expires_at
        .saturating_duration_since(Instant::now())
        .as_secs();
    let parts = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(3),
        Constraint::Length(2),
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

fn render_text_modal(frame: &mut Frame<'_>, area: Rect, title: &str, body: &str, theme: Theme) {
    let modal = modal(frame, area, 72, 18, title, theme);
    frame.render_widget(
        Paragraph::new(terminal_safe_text(body))
            .style(Style::new().fg(theme.foreground))
            .wrap(Wrap { trim: false }),
        modal,
    );
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
}
