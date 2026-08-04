import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";

test("manifest keeps permissions narrow", async () => {
  const manifest = JSON.parse(await readFile(new URL("../manifest.json", import.meta.url), "utf8"));
  assert.equal(manifest.manifest_version, 3);
  assert.equal(manifest.incognito, "not_allowed");
  assert.deepEqual(manifest.permissions.sort(), ["activeTab", "nativeMessaging", "storage"]);
  assert.equal("content_scripts" in manifest, false);
  assert.equal("host_permissions" in manifest, false);
});

test("browser sources keep raw locations and forbidden APIs out of storage", async () => {
  const source = await readFile(new URL("../src/service-worker.ts", import.meta.url), "utf8");
  assert.equal(/chrome\.scripting|chrome\.cookies|chrome\.history|chrome\.webRequest|content_scripts/.test(source), false);
  assert.equal(/storage\.sync|tab\.title|favicon|clipboard/.test(source), false);
  assert.match(source, /canonical_url_hash/);
  assert.match(source, /resource_id/);
});
