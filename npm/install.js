const { chmodSync, writeFileSync } = require("node:fs");
const { join } = require("node:path");
const { version } = require("../package.json");

const assets = {
  "linux-x64": `whatscli-v${version}-linux-x64`,
  "linux-arm": `whatscli-v${version}-linux-armv7`,
  "win32-x64": `whatscli-v${version}-windows-x64.exe`,
};

function assetFor(platform = process.platform, arch = process.arch) {
  const asset = assets[`${platform}-${arch}`];
  if (!asset) {
    throw new Error(`WhatsCLI does not provide a binary for ${platform}-${arch}`);
  }
  return asset;
}

async function install() {
  const asset = assetFor();
  const url = `https://github.com/Pir0c0pter0/whatscli-rs/releases/download/v${version}/${asset}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to download ${url}: ${response.status} ${response.statusText}`);
  }

  const target = join(__dirname, process.platform === "win32" ? "whatscli-bin.exe" : "whatscli-bin");
  writeFileSync(target, Buffer.from(await response.arrayBuffer()), { mode: 0o755 });
  if (process.platform !== "win32") chmodSync(target, 0o755);
}

if (require.main === module) {
  install().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}

module.exports = { assetFor };
