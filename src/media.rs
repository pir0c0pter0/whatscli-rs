use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use image::{AnimationDecoder, DynamicImage, ImageBuffer, ImageReader, Rgb};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui_image::StatefulImage;
use ratatui_image::picker::{Picker, ProtocolType};
use ratatui_image::protocol::StatefulProtocol;
use rodio::{OutputStream, OutputStreamBuilder, Sink, buffer::SamplesBuffer};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::model::MessageKind;

const VIDEO_FPS: u64 = 15;
const AUDIO_CHANNELS: u16 = 2;
const AUDIO_RATE: u32 = 48_000;

#[derive(Debug, Clone)]
pub struct MediaView {
    pub title: String,
    pub kind: MessageKind,
    pub playing: bool,
    pub position: Duration,
    pub duration: Duration,
    pub volume: f32,
    pub muted: bool,
    pub error: Option<String>,
    pub protocol: ProtocolType,
}

#[derive(Debug)]
enum VideoEvent {
    Frame {
        width: u32,
        height: u32,
        rgb: Vec<u8>,
    },
    Error(String),
    Finished,
}

#[derive(Debug)]
enum AudioEvent {
    Samples(Vec<f32>),
    Error(String),
    Finished,
}

#[derive(Debug, Clone)]
struct PlayerClock {
    base: Duration,
    started: Option<Instant>,
}

impl PlayerClock {
    fn new(playing: bool) -> Self {
        Self {
            base: Duration::ZERO,
            started: playing.then(Instant::now),
        }
    }

    fn position(&self) -> Duration {
        self.base
            + self
                .started
                .map(|start| start.elapsed())
                .unwrap_or_default()
    }

    fn pause(&mut self) {
        self.base = self.position();
        self.started = None;
    }

    fn play(&mut self) {
        if self.started.is_none() {
            self.started = Some(Instant::now());
        }
    }

    fn seek(&mut self, position: Duration) {
        self.base = position;
        if self.started.is_some() {
            self.started = Some(Instant::now());
        }
    }
}

pub struct MediaController {
    picker: Picker,
    protocol: Option<StatefulProtocol>,
    view: Option<MediaView>,
    path: Option<PathBuf>,
    dimensions: Option<(u32, u32)>,
    clock: PlayerClock,
    video_tx: mpsc::Sender<VideoEvent>,
    video_rx: mpsc::Receiver<VideoEvent>,
    audio_tx: mpsc::Sender<AudioEvent>,
    audio_rx: mpsc::Receiver<AudioEvent>,
    workers: Vec<JoinHandle<()>>,
    stream: Option<OutputStream>,
    sink: Option<Sink>,
    animation: Vec<(DynamicImage, Duration)>,
    animation_index: usize,
    next_animation_frame: Option<Instant>,
}

impl MediaController {
    pub fn new(picker: Picker) -> Self {
        let (video_tx, video_rx) = mpsc::channel(3);
        let (audio_tx, audio_rx) = mpsc::channel(64);
        Self {
            picker,
            protocol: None,
            view: None,
            path: None,
            dimensions: None,
            clock: PlayerClock::new(false),
            video_tx,
            video_rx,
            audio_tx,
            audio_rx,
            workers: Vec::new(),
            stream: None,
            sink: None,
            animation: Vec::new(),
            animation_index: 0,
            next_animation_frame: None,
        }
    }

    pub fn fallback() -> Self {
        let mut picker = Picker::from_fontsize((8, 16));
        picker.set_protocol_type(ProtocolType::Halfblocks);
        Self::new(picker)
    }

    pub fn view(&self) -> Option<&MediaView> {
        self.view.as_ref()
    }

    pub fn is_open(&self) -> bool {
        self.view.is_some()
    }

    pub async fn open(&mut self, path: PathBuf, kind: MessageKind, title: String) {
        self.close();
        let playing = matches!(kind, MessageKind::Audio | MessageKind::Video);
        self.clock = PlayerClock::new(playing);
        self.path = Some(path.clone());
        self.view = Some(MediaView {
            title,
            kind,
            playing,
            position: Duration::ZERO,
            duration: Duration::ZERO,
            volume: 1.0,
            muted: false,
            error: None,
            protocol: self.picker.protocol_type(),
        });
        let result = match kind {
            MessageKind::Image | MessageKind::Sticker => self.load_image(&path, kind).await,
            MessageKind::Audio | MessageKind::Video => self.load_av(&path, kind).await,
            _ => Err(anyhow!("this file type has no internal viewer")),
        };
        if let Err(error) = result {
            self.set_error(error.to_string());
            self.set_playing(false);
        }
    }

    async fn load_image(&mut self, path: &Path, kind: MessageKind) -> Result<()> {
        let image = ImageReader::open(path)
            .with_context(|| format!("cannot open {}", path.display()))?
            .with_guessed_format()?
            .decode()
            .context("the image or WebP sticker is corrupt or unsupported")?;
        self.dimensions = Some((image.width(), image.height()));
        self.protocol = Some(self.picker.new_resize_protocol(image));
        if kind == MessageKind::Sticker {
            self.load_webp_animation(path);
        }
        Ok(())
    }

    fn load_webp_animation(&mut self, path: &Path) {
        let Ok(file) = std::fs::File::open(path) else {
            return;
        };
        let Ok(decoder) = image::codecs::webp::WebPDecoder::new(BufReader::new(file)) else {
            return;
        };
        if !decoder.has_animation() {
            return;
        }
        let Ok(frames) = decoder.into_frames().collect_frames() else {
            return;
        };
        self.animation = frames
            .into_iter()
            .map(|frame| {
                let (num, den) = frame.delay().numer_denom_ms();
                let delay = Duration::from_millis((u64::from(num) / u64::from(den.max(1))).max(20));
                (DynamicImage::ImageRgba8(frame.into_buffer()), delay)
            })
            .collect();
        if let Some((_, delay)) = self.animation.first() {
            self.next_animation_frame = Some(Instant::now() + *delay);
        }
    }

    async fn load_av(&mut self, path: &Path, kind: MessageKind) -> Result<()> {
        let metadata = probe(path).await?;
        if let Some(view) = self.view.as_mut() {
            view.duration = metadata.duration;
        }
        self.dimensions = metadata.dimensions;
        if kind == MessageKind::Video && self.dimensions.is_none() {
            bail!("ffprobe did not report video dimensions")
        }
        self.restart_workers();
        Ok(())
    }

    pub fn tick(&mut self) {
        let Some(view) = self.view.as_mut() else {
            return;
        };
        view.position = self
            .clock
            .position()
            .min(view.duration.max(self.clock.position()));
        if view.duration > Duration::ZERO && view.position >= view.duration {
            self.clock.pause();
            view.position = view.duration;
            view.playing = false;
            self.abort_workers();
        }
        let mut latest = None;
        while let Ok(event) = self.video_rx.try_recv() {
            match event {
                VideoEvent::Frame { width, height, rgb } => latest = Some((width, height, rgb)),
                VideoEvent::Error(error) => self.set_error(error),
                VideoEvent::Finished => {
                    if let Some(view) = self.view.as_mut() {
                        view.playing = false;
                    }
                    self.clock.pause();
                }
            }
        }
        if let Some((width, height, rgb)) = latest
            && let Some(buffer) = ImageBuffer::<Rgb<u8>, _>::from_raw(width, height, rgb)
        {
            self.protocol = Some(
                self.picker
                    .new_resize_protocol(DynamicImage::ImageRgb8(buffer)),
            );
        }
        while let Ok(event) = self.audio_rx.try_recv() {
            match event {
                AudioEvent::Samples(samples) => {
                    if let Some(sink) = &self.sink {
                        sink.append(SamplesBuffer::new(AUDIO_CHANNELS, AUDIO_RATE, samples));
                    }
                }
                AudioEvent::Error(error) => self.set_error(error),
                AudioEvent::Finished => {
                    if self
                        .view
                        .as_ref()
                        .is_some_and(|view| view.kind == MessageKind::Audio)
                    {
                        self.clock.pause();
                        if let Some(view) = self.view.as_mut() {
                            view.playing = false;
                        }
                    }
                }
            }
        }
        if self
            .next_animation_frame
            .is_some_and(|at| Instant::now() >= at)
            && !self.animation.is_empty()
        {
            self.animation_index = (self.animation_index + 1) % self.animation.len();
            let (image, delay) = &self.animation[self.animation_index];
            self.protocol = Some(self.picker.new_resize_protocol(image.clone()));
            self.next_animation_frame = Some(Instant::now() + *delay);
        }
    }

    pub fn render(&mut self, frame: &mut Frame<'_>, area: Rect) {
        if let Some(protocol) = self.protocol.as_mut() {
            frame.render_stateful_widget(StatefulImage::new(None), area, protocol);
        }
    }

    pub fn toggle(&mut self) {
        let playing = self.view.as_ref().is_some_and(|view| view.playing);
        self.set_playing(!playing);
    }

    pub fn set_playing(&mut self, playing: bool) {
        let Some(view) = self.view.as_mut() else {
            return;
        };
        if !matches!(view.kind, MessageKind::Audio | MessageKind::Video) {
            return;
        }
        if playing == view.playing {
            return;
        }
        view.playing = playing;
        if playing {
            self.clock.play();
            self.restart_workers();
        } else {
            self.clock.pause();
            self.abort_workers();
        }
    }

    pub fn seek_relative(&mut self, seconds: i64) {
        let current = self.clock.position().as_secs_f64();
        self.seek(Duration::from_secs_f64((current + seconds as f64).max(0.0)));
    }

    pub fn seek_fraction(&mut self, fraction: f64) {
        let duration = self
            .view
            .as_ref()
            .map(|view| view.duration)
            .unwrap_or_default();
        self.seek(duration.mul_f64(fraction.clamp(0.0, 1.0)));
    }

    fn seek(&mut self, position: Duration) {
        let duration = self
            .view
            .as_ref()
            .map(|view| view.duration)
            .unwrap_or_default();
        self.clock.seek(position.min(duration));
        if let Some(view) = self.view.as_mut() {
            view.position = self.clock.position();
            if view.playing {
                self.restart_workers();
            }
        }
    }

    pub fn adjust_volume(&mut self, delta: f32) {
        if let Some(view) = self.view.as_mut() {
            view.volume = (view.volume + delta).clamp(0.0, 1.0);
            self.apply_volume();
        }
    }

    pub fn toggle_mute(&mut self) {
        if let Some(view) = self.view.as_mut() {
            view.muted = !view.muted;
            self.apply_volume();
        }
    }

    fn apply_volume(&self) {
        if let (Some(view), Some(sink)) = (&self.view, &self.sink) {
            sink.set_volume(if view.muted { 0.0 } else { view.volume });
        }
    }

    fn restart_workers(&mut self) {
        self.abort_workers();
        let Some(view) = self.view.as_ref() else {
            return;
        };
        let Some(path) = self.path.clone() else {
            return;
        };
        let position = self.clock.position();
        if view.kind == MessageKind::Video
            && let Some((width, height)) = self.dimensions
        {
            self.workers.push(spawn_video(
                path.clone(),
                position,
                width,
                height,
                self.video_tx.clone(),
            ));
        }
        if matches!(view.kind, MessageKind::Audio | MessageKind::Video) {
            match OutputStreamBuilder::open_default_stream() {
                Ok(stream) => {
                    let sink = Sink::connect_new(stream.mixer());
                    self.stream = Some(stream);
                    self.sink = Some(sink);
                    self.apply_volume();
                    self.workers
                        .push(spawn_audio(path, position, self.audio_tx.clone()));
                }
                Err(error) => self.set_error(format!(
                    "audio device unavailable: {error}; video remains usable"
                )),
            }
        }
    }

    fn abort_workers(&mut self) {
        for worker in self.workers.drain(..) {
            worker.abort();
        }
        if let Some(sink) = self.sink.take() {
            sink.stop();
        }
        self.stream = None;
        while self.video_rx.try_recv().is_ok() {}
        while self.audio_rx.try_recv().is_ok() {}
    }

    fn set_error(&mut self, error: String) {
        if let Some(view) = self.view.as_mut() {
            match view.error.as_mut() {
                Some(existing) if !existing.contains(&error) => {
                    existing.push_str(" · ");
                    existing.push_str(&error);
                }
                None => view.error = Some(error),
                _ => {}
            }
        }
    }

    pub fn close(&mut self) {
        self.abort_workers();
        self.view = None;
        self.path = None;
        self.protocol = None;
        self.animation.clear();
        self.next_animation_frame = None;
    }
}

impl Drop for MediaController {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Debug)]
struct ProbeMetadata {
    duration: Duration,
    dimensions: Option<(u32, u32)>,
}

async fn probe(path: &Path) -> Result<ProbeMetadata> {
    probe_with("ffprobe", path).await
}

async fn probe_with(binary: &str, path: &Path) -> Result<ProbeMetadata> {
    let output = Command::new(binary)
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration:stream=width,height",
            "-of",
            "default=noprint_wrappers=1",
            "--",
        ])
        .arg(path)
        .output()
        .await
        .context(
            "ffprobe is required for audio and video; install FFmpeg and ensure ffprobe is in PATH",
        )?;
    if !output.status.success() {
        bail!("ffprobe could not read this media (the file may be corrupt)")
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let value = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(&format!("{name}=")))
    };
    let duration = value("duration")
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0)
        .map(Duration::from_secs_f64)
        .unwrap_or_default();
    let width = value("width").and_then(|v| v.parse::<u32>().ok());
    let height = value("height").and_then(|v| v.parse::<u32>().ok());
    Ok(ProbeMetadata {
        duration,
        dimensions: width.zip(height),
    })
}

fn spawn_video(
    path: PathBuf,
    position: Duration,
    width: u32,
    height: u32,
    tx: mpsc::Sender<VideoEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut child = match Command::new("ffmpeg")
            .args([
                "-v",
                "error",
                "-ss",
                &format!("{:.3}", position.as_secs_f64()),
                "-i",
            ])
            .arg(path)
            .args([
                "-an",
                "-vf",
                &format!("fps={VIDEO_FPS}"),
                "-f",
                "rawvideo",
                "-pix_fmt",
                "rgb24",
                "pipe:1",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                let _ = tx
                    .send(VideoEvent::Error(format!(
                        "ffmpeg is required for video: {error}"
                    )))
                    .await;
                return;
            }
        };
        let Some(mut stdout) = child.stdout.take() else {
            return;
        };
        let Some(frame_len) = (width as usize)
            .checked_mul(height as usize)
            .and_then(|n| n.checked_mul(3))
        else {
            return;
        };
        let mut interval = tokio::time::interval(Duration::from_millis(1000 / VIDEO_FPS));
        loop {
            let mut rgb = vec![0; frame_len];
            if stdout.read_exact(&mut rgb).await.is_err() {
                break;
            }
            interval.tick().await;
            let _ = tx.try_send(VideoEvent::Frame { width, height, rgb });
        }
        let _ = tx.send(VideoEvent::Finished).await;
    })
}

fn spawn_audio(path: PathBuf, position: Duration, tx: mpsc::Sender<AudioEvent>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut child = match Command::new("ffmpeg")
            .args([
                "-v",
                "error",
                "-ss",
                &format!("{:.3}", position.as_secs_f64()),
                "-i",
            ])
            .arg(path)
            .args(["-vn", "-f", "f32le", "-ac", "2", "-ar", "48000", "pipe:1"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                let _ = tx
                    .send(AudioEvent::Error(format!(
                        "ffmpeg is required for audio: {error}"
                    )))
                    .await;
                return;
            }
        };
        let Some(mut stdout) = child.stdout.take() else {
            return;
        };
        let mut bytes = vec![0_u8; 8192];
        let mut pending = Vec::with_capacity(8195);
        loop {
            let read = match stdout.read(&mut bytes).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            pending.extend_from_slice(&bytes[..read]);
            let aligned = pending.len() - pending.len() % 4;
            let samples = pending[..aligned]
                .chunks_exact(4)
                .map(|v| f32::from_le_bytes([v[0], v[1], v[2], v[3]]))
                .collect::<Vec<_>>();
            pending.drain(..aligned);
            let chunk_duration = Duration::from_secs_f64(
                samples.len() as f64 / f64::from(AUDIO_CHANNELS) / f64::from(AUDIO_RATE),
            );
            if tx.send(AudioEvent::Samples(samples)).await.is_err() {
                break;
            }
            tokio::time::sleep(chunk_duration).await;
        }
        let _ = tx.send(AudioEvent::Finished).await;
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_pause_and_seek_are_stable() {
        let mut clock = PlayerClock::new(true);
        std::thread::sleep(Duration::from_millis(2));
        clock.pause();
        let paused = clock.position();
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(clock.position(), paused);
        clock.seek(Duration::from_secs(10));
        assert_eq!(clock.position(), Duration::from_secs(10));
    }

    #[test]
    fn volume_and_mute_are_clamped_without_audio_device() {
        let mut media = MediaController::fallback();
        media.view = Some(MediaView {
            title: "x".into(),
            kind: MessageKind::Audio,
            playing: false,
            position: Duration::ZERO,
            duration: Duration::from_secs(1),
            volume: 1.0,
            muted: false,
            error: None,
            protocol: ProtocolType::Halfblocks,
        });
        media.adjust_volume(1.0);
        assert_eq!(media.view().unwrap().volume, 1.0);
        media.adjust_volume(-2.0);
        assert_eq!(media.view().unwrap().volume, 0.0);
        media.toggle_mute();
        assert!(media.view().unwrap().muted);
    }

    #[tokio::test]
    async fn corrupt_image_reports_error_and_close_clears_modal() {
        let path =
            std::env::temp_dir().join(format!("whatscli-corrupt-image-{}", std::process::id()));
        tokio::fs::write(&path, b"not an image").await.unwrap();
        let mut media = MediaController::fallback();
        media
            .open(path.clone(), MessageKind::Image, "broken".into())
            .await;
        assert!(media.is_open());
        assert!(
            media
                .view()
                .unwrap()
                .error
                .as_ref()
                .unwrap()
                .contains("image")
        );
        media.close();
        assert!(!media.is_open());
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn missing_ffprobe_has_an_actionable_error() {
        let error = probe_with(
            "whatscli-definitely-missing-ffprobe",
            Path::new("media.mp4"),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("ffprobe is required"));
        assert!(error.contains("PATH"));
    }
}
