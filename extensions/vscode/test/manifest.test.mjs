import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";

test("VS Code boundary is local UI only", async () => {
  const packageJson = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
  assert.deepEqual(packageJson.extensionKind, ["ui"]);
  assert.equal(packageJson.capabilities.untrustedWorkspaces.supported, true);
  assert.equal(packageJson.capabilities.virtualWorkspaces.supported, false);
  assert.equal(packageJson.activationEvents[0], "onStartupFinished");
});

test("VS Code sources retain the local event-only boundary", async () => {
  const sources = await Promise.all([
    "extension.ts",
    "adapter-session.ts",
    "core-client.ts",
    "uds.ts",
  ].map((name) => readFile(new URL(`../src/${name}`, import.meta.url), "utf8")));
  const source = sources.join("\n");
  assert.equal(/child_process|vscode\.window\.createTerminal|vscode\.tasks|vscode\.debug|fetch\(/.test(source), false);
  assert.match(source, /dev\.fubun\.vscode\.workspace\.opened\.v1/);
  assert.match(source, /resource_id/);
});
