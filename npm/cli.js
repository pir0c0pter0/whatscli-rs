#!/usr/bin/env node

const { spawnSync } = require("node:child_process");
const { join } = require("node:path");

const binary = join(__dirname, process.platform === "win32" ? "whatscli-bin.exe" : "whatscli-bin");
const result = spawnSync(binary, process.argv.slice(2), { stdio: "inherit" });

if (result.error) {
  throw result.error;
}
process.exit(result.status ?? 1);
