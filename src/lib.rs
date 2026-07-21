pub mod app;
pub mod cache;
pub mod config;
pub mod logging;
pub mod media;
pub mod media_cache;
pub mod model;
pub mod qr;
pub mod session;
pub mod storage;
pub mod ui;

pub const VERSION: &str = concat!("v", env!("CARGO_PKG_VERSION"));

pub fn terminal_safe_text(value: &str) -> String {
    value.replace('\u{f8ff}', "Apple")
}

#[cfg(test)]
mod tests {
    use super::terminal_safe_text;

    #[test]
    fn terminal_safe_text_replaces_unsupported_apple_glyph() {
        assert_eq!(terminal_safe_text("\u{f8ff} Mario"), "Apple Mario");
    }
}
