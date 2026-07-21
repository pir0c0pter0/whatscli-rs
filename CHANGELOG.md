# Changelog

All notable changes to WhatsCLI RS are documented in this file.

## Unreleased

### Added

- Mouse navigation for conversations, variable-height messages, the composer, palette/search,
  confirmations and media controls, with terminal mouse mode restored on exit.
- An in-terminal photo and animated/static WebP sticker viewer using Kitty, Sixel, iTerm2 or a
  universal half-block fallback, plus an FFmpeg/Rodio audio and video player with play/pause, seek,
  progress, volume and mute controls.
- Internal `view` and `play` commands and primary `Enter` actions on media bubbles; documents now
  display an explicit download action while the legacy `open` and `show` commands remain available.
- A separate XDG media cache with configurable 30-day retention and 1 GiB default limit, and atomic
  collision-safe explicit downloads that never overwrite an existing file.
- Persisted `Sticker` message kind value `6`, including live/history hydration and WebP download.
- `mouse_enabled`, `media_cache_path`, `media_cache_retention_days`, `media_cache_max_mb` and
  `message_activate` configuration values with backwards-compatible defaults.

- A versioned SQLite conversation cache at `~/.config/whatscli/cache.db` that restores contacts,
  conversation ordering, previews, unread counters, recent messages and media payloads before the
  network history sync completes.
- Account isolation, corrupt-cache preservation/rebuild, future-schema protection and automatic
  migration of supported older cache schemas.
- Daily rotating logs under `~/.config/whatscli/logs`, with configurable level and retention and
  redaction tests that keep QR data, identifiers, message content, media URLs and payloads out.
- `history_sync_limit`, `log_level` and `log_retention_days` settings under `[general]`, with
  backwards-compatible in-memory defaults for existing configuration files.
- Background task identifiers, categories and lifecycle events.
- A footer indicator for the latest active task, including a `+N` count for concurrent work.
- Bounded queues with visible saturation feedback instead of blocking the terminal interface.
- Tests for queue saturation, task feedback, FIFO ordering, per-conversation parallelism, transfer
  limits, snapshot coalescing and coordinated shutdown.

### Changed

- History sync now batch-checks hydrated message IDs and converts only missing or incomplete
  records while still applying server conversation metadata and unread counts.
- `/backlog` messages remain available for the current run, while the on-disk cache retains only
  the configured `history_sync_limit` window per conversation (`0` remains unlimited).
- Cache writes are coalesced onto a dedicated transactional writer and shutdown now flushes and
  confirms its final transaction; `/logout` and `/reset` remove the cache plus WAL/SHM sidecars.
- Registered a synchronous ordered WhatsApp event handler backed by a lossless internal queue;
  history batches now wait for sequential processing instead of being dropped under pressure.
- Limited automatic history sync to the 200 most recent messages per conversation by default;
  `/backlog` `ON_DEMAND` batches remain unlimited and can explicitly extend local history.
- Replaced the duplicate message-by-ID store with an ID-to-conversation index and retained raw
  protobuf only for downloadable image, video, audio, document and sticker messages.
- Disabled the HTTP client's idle connection pool while preserving the existing 16 KiB buffers and
  response-size limit.
- Moved WhatsApp initialization, session operations, history synchronization, media transfers and
  operating-system integrations under a Tokio background supervisor.
- Preserved FIFO ordering for session commands and for mutations within each conversation while
  allowing independent categories and conversations to progress concurrently.
- Limited media downloads to two simultaneous transfers.
- Replaced the UI-shared message database mutex with an exclusive storage actor that emits
  consistent conversation and message snapshots in approximately 16 ms batches.
- Moved clipboard access, notifications, file/URL opening and external image previews to Tokio's
  blocking pool.
- Changed shutdown to stop new work, disconnect WhatsApp, drain workers for up to three seconds and
  cancel any tasks still pending before restoring the terminal.

### Fixed

- A delayed QR event can no longer regress an already connected session to `pairing`; successful
  pairing now clears the QR and reports `connecting` until `Connected` arrives.
- History decode failures now fail the visible task with safe batch metadata instead of reporting a
  false success, and server unread counts survive a limited local history window.
- Connection, live-message and history events are no longer lost when a protocol burst exceeds the
  former 128-event queue capacity.
- Slow network, storage and desktop integration operations no longer block keyboard input, ticks,
  redraws or quit handling in the Ratatui loop.
- Clipboard paste now updates the editor only after the background worker returns successfully.
- Snapshot refreshes preserve the currently selected message by ID.
