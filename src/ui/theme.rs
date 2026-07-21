use ratatui::style::Color;

use crate::config::Config;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorMode {
    TrueColor,
    Ansi16,
}

#[derive(Debug, Clone, Copy)]
pub struct Theme {
    pub background: Color,
    pub surface: Color,
    pub elevated: Color,
    pub foreground: Color,
    pub muted: Color,
    pub primary: Color,
    pub info: Color,
    pub warning: Color,
    pub error: Color,
    pub incoming: Color,
    pub outgoing: Color,
    pub mode: ColorMode,
}

impl Theme {
    pub fn from_config(config: &Config) -> Self {
        let mode = match config.ui.color_mode.trim().to_ascii_lowercase().as_str() {
            "ansi" | "ansi16" | "16" => ColorMode::Ansi16,
            "truecolor" | "rgb" | "24bit" => ColorMode::TrueColor,
            _ if std::env::var_os("NO_COLOR").is_some() => ColorMode::Ansi16,
            _ if std::env::var("COLORTERM")
                .map(|value| {
                    let value = value.to_ascii_lowercase();
                    value.contains("truecolor") || value.contains("24bit")
                })
                .unwrap_or(false) =>
            {
                ColorMode::TrueColor
            }
            _ => ColorMode::Ansi16,
        };
        let name = config.ui.theme.trim().to_ascii_lowercase();
        if name == "custom" {
            return Self {
                background: parse_color(&config.colors.background),
                surface: parse_color(&config.colors.input_background),
                elevated: parse_color(&config.colors.borders),
                foreground: parse_color(&config.colors.text),
                muted: Color::Gray,
                primary: parse_color(&config.colors.positive),
                info: parse_color(&config.colors.chat_me),
                warning: parse_color(&config.colors.unread_count),
                error: parse_color(&config.colors.negative),
                incoming: parse_color(&config.colors.chat_contact),
                outgoing: parse_color(&config.colors.chat_me),
                mode,
            };
        }
        if mode == ColorMode::Ansi16 {
            return Self {
                background: Color::Black,
                surface: Color::Black,
                elevated: Color::DarkGray,
                foreground: Color::White,
                muted: Color::Gray,
                primary: Color::Green,
                info: Color::Cyan,
                warning: Color::Yellow,
                error: Color::Red,
                incoming: Color::DarkGray,
                outgoing: Color::Green,
                mode,
            };
        }
        if name == "high-contrast" {
            return Self {
                background: Color::Rgb(0, 0, 0),
                surface: Color::Rgb(18, 18, 18),
                elevated: Color::Rgb(48, 48, 48),
                foreground: Color::Rgb(255, 255, 255),
                muted: Color::Rgb(202, 210, 214),
                primary: Color::Rgb(0, 255, 190),
                info: Color::Rgb(99, 211, 255),
                warning: Color::Rgb(255, 224, 122),
                error: Color::Rgb(255, 111, 127),
                incoming: Color::Rgb(43, 55, 62),
                outgoing: Color::Rgb(0, 105, 86),
                mode,
            };
        }
        Self {
            background: Color::Rgb(11, 20, 26),
            surface: Color::Rgb(17, 27, 33),
            elevated: Color::Rgb(32, 44, 51),
            foreground: Color::Rgb(233, 237, 239),
            muted: Color::Rgb(134, 150, 160),
            primary: Color::Rgb(0, 168, 132),
            info: Color::Rgb(83, 189, 235),
            warning: Color::Rgb(255, 210, 121),
            error: Color::Rgb(241, 92, 109),
            incoming: Color::Rgb(32, 44, 51),
            outgoing: Color::Rgb(0, 92, 75),
            mode,
        }
    }
}

pub fn parse_color(name: &str) -> Color {
    match name.trim().to_ascii_lowercase().as_str() {
        "black" => Color::Black,
        "red" => Color::Red,
        "green" => Color::Green,
        "yellow" => Color::Yellow,
        "blue" => Color::Blue,
        "purple" | "magenta" => Color::Magenta,
        "cyan" => Color::Cyan,
        "gray" | "grey" => Color::Gray,
        "darkgray" | "darkgrey" => Color::DarkGray,
        "white" => Color::White,
        value if value.starts_with('#') && value.len() == 7 => Color::Rgb(
            u8::from_str_radix(&value[1..3], 16).unwrap_or(255),
            u8::from_str_radix(&value[3..5], 16).unwrap_or(255),
            u8::from_str_radix(&value[5..7], 16).unwrap_or(255),
        ),
        _ => Color::Reset,
    }
}
