use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result, anyhow};
use chrono::{Local, NaiveDate};
use log::{LevelFilter, Log, Metadata, Record};
use regex::Regex;

use crate::config::Config;

const LOG_PREFIX: &str = "whatscli.";
const LOG_SUFFIX: &str = ".log";

pub fn init(config: &Config) -> Result<()> {
    let config_dir = config
        .config_file
        .parent()
        .ok_or_else(|| anyhow!("configuration path has no parent directory"))?;
    let writer = RotatingFileWriter::new(
        config_dir.join("logs"),
        config.general.log_retention_days.max(1),
    )?;
    let level = level_filter(&config.general.log_level);
    let logger = Box::leak(Box::new(FileLogger {
        level,
        writer: Mutex::new(writer),
    }));
    log::set_logger(logger).map_err(|_| anyhow!("logger was already initialized"))?;
    log::set_max_level(level);
    Ok(())
}

pub fn redact(value: &str) -> String {
    static URL: OnceLock<Regex> = OnceLock::new();
    static JID: OnceLock<Regex> = OnceLock::new();
    let without_urls = URL
        .get_or_init(|| Regex::new(r"(?i)https?://[^\s]+").expect("valid URL regex"))
        .replace_all(value, "[redacted-url]");
    JID.get_or_init(|| {
        Regex::new(r"(?i)\b[^\s@]+@(s\.whatsapp\.net|g\.us|lid|broadcast)\b")
            .expect("valid JID regex")
    })
    .replace_all(&without_urls, "[redacted-jid]")
    .into_owned()
}

fn level_filter(value: &str) -> LevelFilter {
    match value {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    }
}

struct FileLogger {
    level: LevelFilter,
    writer: Mutex<RotatingFileWriter>,
}

impl Log for FileLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= self.level && metadata.target().starts_with("whatscli")
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let message = redact(&record.args().to_string());
        if let Ok(mut writer) = self.writer.lock() {
            let now = Local::now();
            let line = format!(
                "{} {:<5} {}",
                now.format("%Y-%m-%dT%H:%M:%S%:z"),
                record.level(),
                message
            );
            let _ = writer.write_line_at(now.date_naive(), &line);
        }
    }

    fn flush(&self) {
        if let Ok(mut writer) = self.writer.lock() {
            let _ = writer.file.flush();
        }
    }
}

struct RotatingFileWriter {
    directory: PathBuf,
    retention: usize,
    date: NaiveDate,
    file: BufWriter<File>,
}

impl RotatingFileWriter {
    fn new(directory: PathBuf, retention: usize) -> Result<Self> {
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create log directory {}", directory.display()))?;
        let date = Local::now().date_naive();
        let file = open_log_file(&directory, date)?;
        let writer = Self {
            directory,
            retention: retention.max(1),
            date,
            file,
        };
        writer.purge_old()?;
        Ok(writer)
    }

    fn write_line_at(&mut self, date: NaiveDate, line: &str) -> Result<()> {
        if date != self.date {
            self.file.flush().context("failed to flush log file")?;
            self.file = open_log_file(&self.directory, date)?;
            self.date = date;
            self.purge_old()?;
        }
        writeln!(self.file, "{line}").context("failed to write log entry")?;
        self.file.flush().context("failed to flush log entry")
    }

    fn purge_old(&self) -> Result<()> {
        let mut files = fs::read_dir(&self.directory)
            .with_context(|| format!("failed to read log directory {}", self.directory.display()))?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                is_daily_log_name(&name).then_some((name, entry.path()))
            })
            .collect::<Vec<_>>();
        files.sort_by(|a, b| a.0.cmp(&b.0));
        let remove_count = files.len().saturating_sub(self.retention);
        for (_, path) in files.into_iter().take(remove_count) {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove old log file {}", path.display()))?;
        }
        Ok(())
    }
}

fn open_log_file(directory: &Path, date: NaiveDate) -> Result<BufWriter<File>> {
    let path = directory.join(format!(
        "{LOG_PREFIX}{}{LOG_SUFFIX}",
        date.format("%Y-%m-%d")
    ));
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open log file {}", path.display()))?;
    Ok(BufWriter::new(file))
}

fn is_daily_log_name(name: &str) -> bool {
    name.strip_prefix(LOG_PREFIX)
        .and_then(|value| value.strip_suffix(LOG_SUFFIX))
        .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
        .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("whatscli-{name}-{}-{nonce}", std::process::id()))
    }

    #[test]
    fn redacts_urls_and_jids() {
        let line =
            redact("download https://mmg.whatsapp.net/file for 5511999999999@s.whatsapp.net");
        assert!(!line.contains("mmg.whatsapp.net"));
        assert!(!line.contains("5511999999999"));
        assert_eq!(line, "download [redacted-url] for [redacted-jid]");
    }

    #[test]
    fn rotates_daily_and_keeps_configured_file_count() {
        let directory = test_dir("logs");
        let mut writer = RotatingFileWriter::new(directory.clone(), 2).unwrap();
        for day in 1..=3 {
            writer
                .write_line_at(
                    NaiveDate::from_ymd_opt(2027, 7, day).unwrap(),
                    "safe metadata",
                )
                .unwrap();
        }
        drop(writer);
        let mut names = fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            vec!["whatscli.2027-07-02.log", "whatscli.2027-07-03.log"]
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
