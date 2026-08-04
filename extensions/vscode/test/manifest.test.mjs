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
