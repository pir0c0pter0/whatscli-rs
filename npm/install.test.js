const assert = require("node:assert/strict");
const test = require("node:test");
const { version } = require("../package.json");
const { assetFor } = require("./install");

test("maps supported npm platforms to release assets", () => {
  assert.equal(assetFor("linux", "x64"), `whatscli-v${version}-linux-x64`);
  assert.equal(assetFor("linux", "arm"), `whatscli-v${version}-linux-armv7`);
  assert.equal(assetFor("win32", "x64"), `whatscli-v${version}-windows-x64.exe`);
  assert.throws(() => assetFor("darwin", "arm64"), /does not provide a binary/);
});
