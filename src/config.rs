use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use ini::Ini;

#[derive(Debug, Clone)]
pub struct General {
    pub download_path: PathBuf,
    pub preview_path: PathBuf,
    pub cmd_prefix: String,
    pub show_command: String,
    pub enable_notifications: bool,
    pub use_terminal_bell: bool,
    pub notification_timeout: i64,
    pub backlog_msg_quantity: i32,
    pub history_sync_limit: usize,
    pub log_level: String,
    pub log_retention_days: usize,
}

#[derive(Debug, Clone)]
pub struct Keymap {
    pub open_palette: String,
    pub search_chats: String,
    pub switch_panels: String,
    pub focus_messages: String,
    pub focus_input: String,
    pub focus_chats: String,
    pub copyuser: String,
    pub pasteuser: String,
    pub command_backlog: String,
    pub command_read: String,
    pub command_connect: String,
    pub command_quit: String,
    pub command_help: String,
    pub message_download: String,
    pub message_open: String,
    pub message_show: String,
    pub message_url: String,
    pub message_info: String,
    pub message_revoke: String,
}

#[derive(Debug, Clone)]
pub struct Ui {
    pub chat_sidebar_width: u16,
    pub theme: String,
    pub color_mode: String,
    pub wide_breakpoint: u16,
    pub compact_breakpoint: u16,
    pub short_height: u16,
}

#[derive(Debug, Clone)]
pub struct Colors {
    pub background: String,
    pub text: String,
    pub forwarded_text: String,
    pub list_header: String,
    pub list_contact: String,
    pub list_group: String,
    pub chat_contact: String,
    pub chat_me: String,
    pub borders: String,
    pub input_background: String,
    pub input_text: String,
    pub unread_count: String,
    pub positive: String,
    pub negative: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub general: General,
    pub keymap: Keymap,
    pub ui: Ui,
    pub colors: Colors,
    pub config_file: PathBuf,
    pub session_file: PathBuf,
    pub cache_file: PathBuf,
    pub startup_warnings: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let config_dir = dirs::config_dir()
            .unwrap_or_else(|| home.join(".config"))
            .join("whatscli");
        Self {
            general: General {
                download_path: home.join("Downloads"),
                preview_path: home.join("Downloads"),
                cmd_prefix: "/".into(),
                show_command: "jp2a --color".into(),
                enable_notifications: false,
                use_terminal_bell: false,
                notification_timeout: 60,
                backlog_msg_quantity: 10,
                history_sync_limit: 200,
                log_level: "info".into(),
                log_retention_days: 7,
            },
            keymap: Keymap {
                open_palette: "Ctrl+p".into(),
                search_chats: "Ctrl+f".into(),
                switch_panels: "Tab".into(),
                focus_messages: "Ctrl+w".into(),
                focus_input: "Ctrl+Space".into(),
                focus_chats: "Ctrl+e".into(),
                copyuser: "Ctrl+c".into(),
                pasteuser: "Ctrl+v".into(),
                command_backlog: "Ctrl+b".into(),
                command_read: "Ctrl+n".into(),
                command_connect: "Ctrl+r".into(),
                command_quit: "Ctrl+q".into(),
                command_help: "Ctrl+?".into(),
                message_download: "d".into(),
                message_open: "o".into(),
                message_show: "s".into(),
                message_url: "u".into(),
                message_info: "i".into(),
                message_revoke: "r".into(),
            },
            ui: Ui {
                chat_sidebar_width: 30,
                theme: "whatsapp-dark".into(),
                color_mode: "auto".into(),
                wide_breakpoint: 100,
                compact_breakpoint: 72,
                short_height: 18,
            },
            colors: Colors {
                background: "black".into(),
                text: "white".into(),
                forwarded_text: "purple".into(),
                list_header: "yellow".into(),
                list_contact: "green".into(),
                list_group: "blue".into(),
                chat_contact: "green".into(),
                chat_me: "blue".into(),
                borders: "white".into(),
                input_background: "blue".into(),
                input_text: "white".into(),
                unread_count: "yellow".into(),
                positive: "green".into(),
                negative: "red".into(),
            },
            config_file: config_dir.join("whatscli.config"),
            session_file: config_dir.join("session-rust.db"),
            cache_file: config_dir.join("cache.db"),
            startup_warnings: Vec::new(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        Self::load_config(Self::default())
    }

    fn load_config(mut config: Self) -> Result<Self> {
        if let Some(parent) = config.config_file.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        if !config.config_file.exists() {
            config.save_defaults()?;
            return Ok(config);
        }

        let ini = Ini::load_from_file(&config.config_file)
            .with_context(|| format!("failed to load {}", config.config_file.display()))?;
        if let Some(s) = ini.section(Some("general")) {
            config.general.download_path =
                path_value(s.get("download_path"), &config.general.download_path);
            config.general.preview_path =
                path_value(s.get("preview_path"), &config.general.preview_path);
            string_value(&mut config.general.cmd_prefix, s.get("cmd_prefix"));
            string_value(&mut config.general.show_command, s.get("show_command"));
            config.general.enable_notifications = bool_value(
                s.get("enable_notifications"),
                config.general.enable_notifications,
            );
            config.general.use_terminal_bell =
                bool_value(s.get("use_terminal_bell"), config.general.use_terminal_bell);
            config.general.notification_timeout = number_value(
                s.get("notification_timeout"),
                config.general.notification_timeout,
            );
            config.general.backlog_msg_quantity = number_value(
                s.get("backlog_msg_quantity"),
                config.general.backlog_msg_quantity,
            );
            config.general.history_sync_limit = number_value(
                s.get("history_sync_limit"),
                config.general.history_sync_limit,
            );
            if let Some(level) = s.get("log_level") {
                let normalized = level.trim().to_ascii_lowercase();
                if matches!(
                    normalized.as_str(),
                    "error" | "warn" | "info" | "debug" | "trace"
                ) {
                    config.general.log_level = normalized;
                } else {
                    config.general.log_level = "info".into();
                    config
                        .startup_warnings
                        .push("invalid general.log_level; using info".into());
                }
            }
            config.general.log_retention_days = number_value(
                s.get("log_retention_days"),
                config.general.log_retention_days,
            )
            .max(1);
        }
        if let Some(s) = ini.section(Some("keymap")) {
            macro_rules! key {
                ($field:ident) => {
                    string_value(&mut config.keymap.$field, s.get(stringify!($field)));
                };
            }
            key!(open_palette);
            key!(search_chats);
            key!(switch_panels);
            key!(focus_messages);
            key!(focus_input);
            key!(focus_chats);
            key!(copyuser);
            key!(pasteuser);
            key!(command_backlog);
            key!(command_read);
            key!(command_connect);
            key!(command_quit);
            key!(command_help);
            key!(message_download);
            key!(message_open);
            key!(message_show);
            key!(message_url);
            key!(message_info);
            key!(message_revoke);
        }
        if let Some(s) = ini.section(Some("ui")) {
            config.ui.chat_sidebar_width =
                number_value(s.get("chat_sidebar_width"), config.ui.chat_sidebar_width);
            string_value(&mut config.ui.theme, s.get("theme"));
            string_value(&mut config.ui.color_mode, s.get("color_mode"));
            config.ui.wide_breakpoint =
                number_value(s.get("wide_breakpoint"), config.ui.wide_breakpoint);
            config.ui.compact_breakpoint =
                number_value(s.get("compact_breakpoint"), config.ui.compact_breakpoint);
            config.ui.short_height = number_value(s.get("short_height"), config.ui.short_height);
        }
        let mut has_custom_colors = false;
        if let Some(s) = ini.section(Some("colors")) {
            macro_rules! color {
                ($field:ident) => {
                    string_value(&mut config.colors.$field, s.get(stringify!($field)));
                };
            }
            color!(background);
            color!(text);
            color!(forwarded_text);
            color!(list_header);
            color!(list_contact);
            color!(list_group);
            color!(chat_contact);
            color!(chat_me);
            color!(borders);
            color!(input_background);
            color!(input_text);
            color!(unread_count);
            color!(positive);
            color!(negative);
            has_custom_colors = config.colors != Colors::legacy_defaults();
        }
        if ini
            .section(Some("ui"))
            .and_then(|section| section.get("theme"))
            .is_none()
            && has_custom_colors
        {
            config.ui.theme = "custom".into();
        }
        Ok(config)
    }

    fn save_defaults(&self) -> Result<()> {
        let mut ini = Ini::new();
        ini.with_section(Some("general"))
            .set(
                "download_path",
                self.general.download_path.to_string_lossy().as_ref(),
            )
            .set(
                "preview_path",
                self.general.preview_path.to_string_lossy().as_ref(),
            )
            .set("cmd_prefix", &self.general.cmd_prefix)
            .set("show_command", &self.general.show_command)
            .set(
                "enable_notifications",
                self.general.enable_notifications.to_string(),
            )
            .set(
                "use_terminal_bell",
                self.general.use_terminal_bell.to_string(),
            )
            .set(
                "notification_timeout",
                self.general.notification_timeout.to_string(),
            )
            .set(
                "backlog_msg_quantity",
                self.general.backlog_msg_quantity.to_string(),
            )
            .set(
                "history_sync_limit",
                self.general.history_sync_limit.to_string(),
            )
            .set("log_level", &self.general.log_level)
            .set(
                "log_retention_days",
                self.general.log_retention_days.to_string(),
            );
        let keymap = &self.keymap;
        ini.with_section(Some("keymap"))
            .set("open_palette", &keymap.open_palette)
            .set("search_chats", &keymap.search_chats)
            .set("switch_panels", &keymap.switch_panels)
            .set("focus_messages", &keymap.focus_messages)
            .set("focus_input", &keymap.focus_input)
            .set("focus_chats", &keymap.focus_chats)
            .set("copyuser", &keymap.copyuser)
            .set("pasteuser", &keymap.pasteuser)
            .set("command_backlog", &keymap.command_backlog)
            .set("command_read", &keymap.command_read)
            .set("command_connect", &keymap.command_connect)
            .set("command_quit", &keymap.command_quit)
            .set("command_help", &keymap.command_help)
            .set("message_download", &keymap.message_download)
            .set("message_open", &keymap.message_open)
            .set("message_show", &keymap.message_show)
            .set("message_url", &keymap.message_url)
            .set("message_info", &keymap.message_info)
            .set("message_revoke", &keymap.message_revoke);
        ini.with_section(Some("ui"))
            .set("chat_sidebar_width", self.ui.chat_sidebar_width.to_string())
            .set("theme", &self.ui.theme)
            .set("color_mode", &self.ui.color_mode)
            .set("wide_breakpoint", self.ui.wide_breakpoint.to_string())
            .set("compact_breakpoint", self.ui.compact_breakpoint.to_string())
            .set("short_height", self.ui.short_height.to_string());
        let c = &self.colors;
        ini.with_section(Some("colors"))
            .set("background", &c.background)
            .set("text", &c.text)
            .set("forwarded_text", &c.forwarded_text)
            .set("list_header", &c.list_header)
            .set("list_contact", &c.list_contact)
            .set("list_group", &c.list_group)
            .set("chat_contact", &c.chat_contact)
            .set("chat_me", &c.chat_me)
            .set("borders", &c.borders)
            .set("input_background", &c.input_background)
            .set("input_text", &c.input_text)
            .set("unread_count", &c.unread_count)
            .set("positive", &c.positive)
            .set("negative", &c.negative);
        ini.write_to_file(&self.config_file)
            .with_context(|| format!("failed to write {}", self.config_file.display()))
    }
}

impl PartialEq for Colors {
    fn eq(&self, other: &Self) -> bool {
        self.background == other.background
            && self.text == other.text
            && self.forwarded_text == other.forwarded_text
            && self.list_header == other.list_header
            && self.list_contact == other.list_contact
            && self.list_group == other.list_group
            && self.chat_contact == other.chat_contact
            && self.chat_me == other.chat_me
            && self.borders == other.borders
            && self.input_background == other.input_background
            && self.input_text == other.input_text
            && self.unread_count == other.unread_count
            && self.positive == other.positive
            && self.negative == other.negative
    }
}

impl Colors {
    fn legacy_defaults() -> Self {
        Self {
            background: "black".into(),
            text: "white".into(),
            forwarded_text: "purple".into(),
            list_header: "yellow".into(),
            list_contact: "green".into(),
            list_group: "blue".into(),
            chat_contact: "green".into(),
            chat_me: "blue".into(),
            borders: "white".into(),
            input_background: "blue".into(),
            input_text: "white".into(),
            unread_count: "yellow".into(),
            positive: "green".into(),
            negative: "red".into(),
        }
    }
}

fn expand_env(value: &str) -> String {
    let mut out = value.to_owned();
    for (key, val) in std::env::vars() {
        out = out.replace(&format!("${{{key}}}"), &val);
        out = out.replace(&format!("${key}"), &val);
    }
    if let Some(rest) = out.strip_prefix("~/")
        && let Some(home) = dirs::home_dir()
    {
        return home.join(rest).to_string_lossy().into_owned();
    }
    out
}

fn path_value(value: Option<&str>, default: &Path) -> PathBuf {
    value
        .map(expand_env)
        .map(PathBuf::from)
        .unwrap_or_else(|| default.to_owned())
}

fn string_value(target: &mut String, value: Option<&str>) {
    if let Some(value) = value {
        *target = expand_env(value);
    }
}

fn bool_value(value: Option<&str>, default: bool) -> bool {
    value.and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn number_value<T: std::str::FromStr>(value: Option<&str>, default: T) -> T {
    value.and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::Config;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_config(contents: &str) -> (Config, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("whatscli-config-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("whatscli.config");
        fs::write(&path, contents).unwrap();
        let defaults = Config {
            config_file: path,
            session_file: directory.join("session-rust.db"),
            cache_file: directory.join("cache.db"),
            ..Default::default()
        };
        (Config::load_config(defaults).unwrap(), directory)
    }

    #[test]
    fn defaults_preserve_legacy_command_prefix() {
        let config = Config::default();
        assert_eq!(config.general.cmd_prefix, "/");
        assert_eq!(
            config.cache_file.file_name().and_then(|name| name.to_str()),
            Some("cache.db")
        );
    }

    #[test]
    fn modern_ui_defaults_do_not_remove_legacy_settings() {
        let config = Config::default();
        assert_eq!(config.ui.theme, "whatsapp-dark");
        assert_eq!(config.ui.color_mode, "auto");
        assert_eq!(config.ui.wide_breakpoint, 100);
        assert_eq!(config.ui.compact_breakpoint, 72);
        assert_eq!(config.keymap.open_palette, "Ctrl+p");
        assert_eq!(config.keymap.search_chats, "Ctrl+f");
        assert_eq!(config.colors, super::Colors::legacy_defaults());
    }

    #[test]
    fn legacy_file_gets_sync_and_log_defaults_without_being_rewritten() {
        let contents = "[general]\ncmd_prefix=!\n";
        let (config, directory) = test_config(contents);
        assert_eq!(config.general.history_sync_limit, 200);
        assert_eq!(config.general.log_level, "info");
        assert_eq!(config.general.log_retention_days, 7);
        assert_eq!(fs::read_to_string(&config.config_file).unwrap(), contents);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn new_general_values_accept_unlimited_sync_and_clamp_retention() {
        let contents = "[general]\nhistory_sync_limit=0\nlog_level=TRACE\nlog_retention_days=0\n";
        let (config, directory) = test_config(contents);
        assert_eq!(config.general.history_sync_limit, 0);
        assert_eq!(config.general.log_level, "trace");
        assert_eq!(config.general.log_retention_days, 1);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn invalid_log_level_falls_back_with_a_warning() {
        let (config, directory) = test_config("[general]\nlog_level=verbose\n");
        assert_eq!(config.general.log_level, "info");
        assert_eq!(
            config.startup_warnings,
            ["invalid general.log_level; using info"]
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
