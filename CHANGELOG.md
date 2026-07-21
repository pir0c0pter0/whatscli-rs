# Changelog

All notable changes to WhatsCLI RS are documented in this file.

## Unreleased

### Added

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

- Registered a synchronous ordered WhatsApp event handler backed by a lossless internal queue;
  history batches now wait for sequential processing instead of being dropped under pressure.
- Limited automatic history sync to the 200 most recent messages per conversation by default;
  `/backlog` `ON_DEMAND` batches remain unlimited and can explicitly extend local history.
- Replaced the duplicate message-by-ID store with an ID-to-conversation index and retained raw
  protobuf only for downloadable image, video, audio and document messages.
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
