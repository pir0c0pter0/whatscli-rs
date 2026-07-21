use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Editor {
    text: String,
    cursor: usize,
}

impl Editor {
    pub fn new(text: impl Into<String>) -> Self {
        let text = text.into();
        let cursor = text.len();
        Self { text, cursor }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    pub fn set(&mut self, text: impl Into<String>) {
        self.text = text.into();
        self.cursor = self.text.len();
    }

    pub fn take(&mut self) -> String {
        self.cursor = 0;
        std::mem::take(&mut self.text)
    }

    pub fn clear(&mut self) {
        self.text.clear();
        self.cursor = 0;
    }

    pub fn insert(&mut self, ch: char) {
        self.text.insert(self.cursor, ch);
        self.cursor += ch.len_utf8();
    }

    pub fn insert_str(&mut self, value: &str) {
        let clean = value.split_whitespace().collect::<Vec<_>>().join(" ");
        self.text.insert_str(self.cursor, &clean);
        self.cursor += clean.len();
    }

    pub fn move_left(&mut self) {
        self.cursor = self.text[..self.cursor]
            .grapheme_indices(true)
            .next_back()
            .map(|(index, _)| index)
            .unwrap_or(0);
    }

    pub fn move_right(&mut self) {
        self.cursor = self.text[self.cursor..]
            .grapheme_indices(true)
            .nth(1)
            .map(|(index, _)| self.cursor + index)
            .unwrap_or(self.text.len());
    }

    pub fn home(&mut self) {
        self.cursor = 0;
    }

    pub fn end(&mut self) {
        self.cursor = self.text.len();
    }

    pub fn set_cursor_column(&mut self, column: usize, width: usize) {
        let (_, visible_cursor) = self.viewport(width);
        let cursor_prefix_width = UnicodeWidthStr::width(&self.text[..self.cursor]);
        let viewport_start = cursor_prefix_width.saturating_sub(visible_cursor as usize);
        let target = viewport_start.saturating_add(column);
        let mut used = 0;
        self.cursor = self.text.len();
        for (index, grapheme) in self.text.grapheme_indices(true) {
            let next = used + UnicodeWidthStr::width(grapheme);
            if target < next {
                self.cursor = index;
                break;
            }
            used = next;
        }
    }

    pub fn backspace(&mut self) {
        let end = self.cursor;
        self.move_left();
        if self.cursor < end {
            self.text.drain(self.cursor..end);
        }
    }

    pub fn delete(&mut self) {
        let start = self.cursor;
        self.move_right();
        if self.cursor > start {
            self.text.drain(start..self.cursor);
            self.cursor = start;
        }
    }

    pub fn viewport(&self, width: usize) -> (String, u16) {
        if width == 0 {
            return (String::new(), 0);
        }
        let before = &self.text[..self.cursor];
        let mut before_parts: Vec<&str> = before.graphemes(true).collect();
        let mut before_width = UnicodeWidthStr::width(before);
        while before_width >= width && !before_parts.is_empty() {
            before_width -= UnicodeWidthStr::width(before_parts.remove(0));
        }
        let mut visible = before_parts.concat();
        let mut used = before_width;
        for grapheme in self.text[self.cursor..].graphemes(true) {
            let grapheme_width = UnicodeWidthStr::width(grapheme);
            if used + grapheme_width > width {
                break;
            }
            visible.push_str(grapheme);
            used += grapheme_width;
        }
        (visible, before_width.min(width.saturating_sub(1)) as u16)
    }
}

pub fn truncate_width(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let current = UnicodeWidthStr::width(text);
    if current <= width {
        return text.to_owned();
    }
    let ellipsis = if width > 1 { "…" } else { "" };
    let target = width.saturating_sub(UnicodeWidthStr::width(ellipsis));
    let mut out = String::new();
    let mut used = 0;
    for grapheme in text.graphemes(true) {
        let grapheme_width = UnicodeWidthStr::width(grapheme);
        if used + grapheme_width > target {
            break;
        }
        out.push_str(grapheme);
        used += grapheme_width;
    }
    out.push_str(ellipsis);
    out
}

pub fn wrap_width(text: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return vec![String::new()];
    }
    let mut lines = Vec::new();
    for source_line in text.lines().chain(text.is_empty().then_some("")) {
        let mut line = String::new();
        let mut used = 0;
        for word in source_line.split_inclusive(char::is_whitespace) {
            let word_width = UnicodeWidthStr::width(word);
            if used > 0 && used + word_width > width {
                lines.push(line.trim_end().to_owned());
                line.clear();
                used = 0;
            }
            for grapheme in word.graphemes(true) {
                let grapheme_width = UnicodeWidthStr::width(grapheme);
                if used > 0 && used + grapheme_width > width {
                    lines.push(line.trim_end().to_owned());
                    line.clear();
                    used = 0;
                }
                line.push_str(grapheme);
                used += grapheme_width;
            }
        }
        lines.push(line.trim_end().to_owned());
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edits_whole_graphemes() {
        let mut editor = Editor::new("ação 👩🏽‍💻");
        editor.backspace();
        assert_eq!(editor.text(), "ação ");
        editor.move_left();
        editor.delete();
        assert_eq!(editor.text(), "ação");
    }

    #[test]
    fn viewport_keeps_cursor_visible_for_long_unicode_text() {
        let editor = Editor::new("mensagem longa com café ☕");
        let (visible, cursor) = editor.viewport(10);
        assert!(UnicodeWidthStr::width(visible.as_str()) <= 10);
        assert!(cursor < 10);
    }

    #[test]
    fn mouse_column_positions_cursor_on_grapheme_boundary() {
        let mut editor = Editor::new("aé👩🏽‍💻z");
        editor.set_cursor_column(1, 20);
        editor.insert('X');
        assert_eq!(editor.text(), "aXé👩🏽‍💻z");
    }
}
