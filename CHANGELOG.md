# Changelog

All notable changes to WhatsCLI RS are documented in this file.

## Unreleased

### Added

- Background task identifiers, categories and lifecycle events.
- A footer indicator for the latest active task, including a `+N` count for concurrent work.
- Bounded queues with visible saturation feedback instead of blocking the terminal interface.
- Tests for queue saturation, task feedback, FIFO ordering, per-conversation parallelism, transfer
  limits, snapshot coalescing and coordinated shutdown.

### Changed

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

- Slow network, storage and desktop integration operations no longer block keyboard input, ticks,
  redraws or quit handling in the Ratatui loop.
- Clipboard paste now updates the editor only after the background worker returns successfully.
- Snapshot refreshes preserve the currently selected message by ID.
