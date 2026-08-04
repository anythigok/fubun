import assert from "node:assert/strict";
import test from "node:test";
import { canonicalizeWebUrl, canonicalUrlHash, encodeUdsFrame, decodeUdsFrame, rejectUnknownKeys } from "../dist/index.js";

test("canonical URL and Rust fixture hash", async () => {
  const canonical = canonicalizeWebUrl("https://Example.COM:443/research?id=123#section");
  assert.equal(canonical, "https://example.com/research");
  assert.equal(await canonicalUrlHash(canonical), "468164e75ba0e4cf47eee057fe3459fa5bc5fd4ba259de08f117c780e261da59");
});

test("UDS frame uses little endian and bounded JSON", () => {
  const frame = encodeUdsFrame({ ok: true });
  assert.deepEqual(decodeUdsFrame(frame), { ok: true });
  assert.throws(() => decodeUdsFrame(new Uint8Array([1, 0, 0])));
});

test("canonicalization rejects unsafe browser schemes and unknown fields are explicit", () => {
  assert.throws(() => canonicalizeWebUrl("file:///tmp/private"));
  assert.throws(() => canonicalizeWebUrl("https://user@example.com/"));
  assert.throws(() => rejectUnknownKeys({ known: true, extra: true }, ["known"]));
});
