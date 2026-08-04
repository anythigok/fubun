import assert from "node:assert/strict";
import test from "node:test";
import { BrowserIntegration } from "../dist/integration.js";
import { BrowserNativeBridge } from "../dist/native-bridge.js";

const BROWSER_MAPPING_SCHEMA_VERSION = "dev.fubun.browser-mapping/1";
const PROTOCOL_VERSION = { major: 1, minor: 0 };

const resourceId = "11111111-1111-4111-8111-111111111111";
const scopeId = "22222222-2222-4222-8222-222222222222";
const actionId = "33333333-3333-4333-8333-333333333333";

async function canonicalUrlHash(value) {
  const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function waitFor(predicate) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("condition was not reached");
}

function enabledPayload() {
  return {
    resource: {
      id: resourceId,
      kind: "web.page",
      label: "Example",
      locator: "https://example.com/page",
      canonical_locator: "https://example.com/page",
      sensitivity: "normal",
      scope: "exact",
      created_at: "2026-08-04T00:00:00Z",
      updated_at: "2026-08-04T00:00:00Z",
    },
    scope: {
      id: scopeId,
      source: "browser.chromium",
      resource_id: resourceId,
      status: "active",
      created_at: "2026-08-04T00:00:00Z",
      updated_at: "2026-08-04T00:00:00Z",
    },
    canonical_url_hash: "",
    origin_pattern: "https://example.com/*",
  };
}

async function mapping() {
  return {
    resource_id: resourceId,
    scope_id: scopeId,
    canonical_url_hash: await canonicalUrlHash("https://example.com/page"),
    origin_pattern: "https://example.com/*",
    label: "Example",
    schema_version: BROWSER_MAPPING_SCHEMA_VERSION,
    state: "active",
  };
}

class FakeNative {
  constructor() {
    this.calls = [];
    this.notifications = [];
    this.responses = new Map();
    this.ready = true;
    this.reconnects = 0;
    this.onReconnect = undefined;
  }
  async start() {}
  async reconnect() { this.reconnects += 1; await this.onReconnect?.(); }
  isReady() { return this.ready; }
  async request(type, payload, expected) {
    this.calls.push({ type, payload, expected });
    const handler = this.responses.get(type);
    if (handler instanceof Error) throw handler;
    if (typeof handler === "function") return handler(type, payload, expected);
    return handler;
  }
  async requestPrepared(type, expected, payloadFactory) {
    if (!this.ready) {
      this.ready = true;
      await this.reconnect();
    }
    return this.request(type, payloadFactory(), expected);
  }
  async notify(type, requestId, payload) { this.notifications.push({ type, requestId, payload }); }
}

function fixture({ permissions = new Set(["https://example.com/*"]), initial = [], native = new FakeNative(), suppression } = {}) {
  const store = { items: initial, writes: [], async read() { return this.items; }, async write(items) { this.writes.push(items); this.items = items; } };
  const browser = {
    active: { id: 1, url: "https://example.com/page", incognito: false },
    tabs: [],
    created: [],
    async activeTab() { return this.active; },
    async allTabs() { return this.tabs; },
    async createPreparedTab() { this.created.push("about:blank"); return 42; },
    async navigateTab(_tabId, url) { this.created[this.created.length - 1] = url; },
    async permissionGranted(origin) { return permissions.has(origin); },
  };
  const integration = new BrowserIntegration({
    browser,
    store,
    native,
    extensionId: () => "a".repeat(32),
    extensionVersion: () => "0.1.0",
    uuid: (() => { let value = 10; return () => `00000000-0000-4000-8000-${String(value++).padStart(12, "0")}`; })(),
    now: () => 1_000,
    suppression,
  });
  return { browser, integration, native, permissions, store };
}

test("permission denial never sends observation enable", async () => {
  const { integration, native, store } = fixture({ permissions: new Set() });
  await assert.rejects(integration.observeCurrentPage(), /permission denied/);
  assert.equal(native.calls.length, 0);
  assert.equal(store.writes.length, 0);
});

test("enable waits for Core acknowledgement before saving a mapping", async () => {
  const { integration, native, store } = fixture();
  const response = enabledPayload();
  response.canonical_url_hash = await canonicalUrlHash("https://example.com/page");
  native.responses.set("browser.observation.enable", response);
  await integration.observeCurrentPage();
  assert.equal(native.calls[0].type, "browser.observation.enable");
  assert.equal(store.items.length, 1);
  assert.equal(store.items[0].schema_version, BROWSER_MAPPING_SCHEMA_VERSION);
  assert.equal(native.reconnects, 1);
});

test("stop disables local event eligibility before awaiting Core pause", async () => {
  const active = await mapping();
  const { integration, native, store } = fixture({ initial: [active] });
  let release;
  native.responses.set("browser.observation.pause", () => new Promise((resolve) => { release = resolve; }));
  const stopping = integration.stopCurrentPage();
  await waitFor(() => native.calls.length === 1);
  assert.equal(store.items[0].state, "pause_pending");
  release({ id: scopeId, source: "browser.chromium", resource_id: resourceId, status: "paused", created_at: "x", updated_at: "x" });
  await stopping;
  assert.equal(store.items.length, 0);
});

test("pause failure leaves the mapping locally inactive", async () => {
  const active = await mapping();
  const { integration, native, store } = fixture({ initial: [active] });
  native.responses.set("browser.observation.pause", new Error("offline"));
  await assert.rejects(integration.stopCurrentPage(), /locally paused/);
  assert.equal(store.items[0].state, "pause_pending");
});

test("pause response for another scope does not delete the locally inactive mapping", async () => {
  const active = await mapping();
  const { integration, native, store } = fixture({ initial: [active] });
  native.responses.set("browser.observation.pause", { id: "99999999-9999-4999-8999-999999999999", source: "browser.chromium", resource_id: resourceId, status: "paused", created_at: "x", updated_at: "x" });
  await assert.rejects(integration.stopCurrentPage(), /locally paused/);
  assert.equal(store.items[0].state, "pause_pending");
});

test("permission removal persists inactive mappings before Core pause and reconnect", async () => {
  const active = await mapping();
  const { integration, native, store } = fixture({ initial: [active] });
  native.responses.set("browser.observation.pause", { id: scopeId, source: "browser.chromium", resource_id: resourceId, status: "paused", created_at: "x", updated_at: "x" });
  await integration.onPermissionsRemoved(["https://example.com/*"]);
  assert.equal(native.calls[0].type, "browser.observation.pause");
  assert.equal(store.writes[0][0].state, "pause_pending");
  assert.equal(store.items.length, 0);
  assert.equal(native.reconnects, 1);
});

test("startup reconciliation excludes revoked mappings from the next adapter snapshot", async () => {
  const active = await mapping();
  const { integration, native, store } = fixture({ initial: [active], permissions: new Set() });
  native.responses.set("browser.observation.pause", { id: scopeId, source: "browser.chromium", resource_id: resourceId, status: "paused", created_at: "x", updated_at: "x" });
  await integration.start();
  const hello = await integration.extensionHello();
  assert.equal(native.calls[0].type, "browser.observation.pause");
  assert.equal(store.items.length, 0);
  assert.deepEqual(hello.payload.permitted_resource_ids, []);
});

test("startup reconciliation retries a locally pending Core pause without reactivating events", async () => {
  const pending = { ...await mapping(), state: "pause_pending" };
  const { integration, native, store } = fixture({ initial: [pending] });
  native.responses.set("browser.observation.pause", { id: scopeId, source: "browser.chromium", resource_id: resourceId, status: "paused", created_at: "x", updated_at: "x" });
  await integration.start();
  assert.equal(native.calls[0].type, "browser.observation.pause");
  assert.equal(store.items.length, 0);
});

test("navigation events use strictly increasing sequence numbers", async () => {
  const active = await mapping();
  const { browser, integration, native } = fixture({ initial: [active] });
  native.responses.set("browser.event.emit", { event_id: actionId, stored: true, duplicate: false });
  await integration.onNavigation({ id: 1, url: "https://example.com/page", incognito: false });
  await integration.onNavigation({ id: 2, url: "https://example.com/page", incognito: false });
  const events = native.calls.filter((call) => call.type === "browser.event.emit");
  assert.deepEqual(events.map((call) => call.payload.sequence_no), [1, 2]);
  browser.active = { id: 3, url: "https://example.com/page", incognito: false };
});

test("navigation allocates sequence after reconnect and never reuses a failed sequence", async () => {
  const active = await mapping();
  const { integration, native } = fixture({ initial: [active] });
  native.responses.set("browser.event.emit", { event_id: actionId, stored: true, duplicate: false });
  native.onReconnect = () => integration.extensionHello();
  native.responses.set("browser.event.emit", new Error("ack timeout"));
  await assert.rejects(integration.onNavigation({ id: 1, url: "https://example.com/page", incognito: false }));
  native.responses.set("browser.event.emit", { event_id: actionId, stored: true, duplicate: false });
  await integration.onNavigation({ id: 2, url: "https://example.com/page", incognito: false });
  native.ready = false;
  await integration.onNavigation({ id: 3, url: "https://example.com/page", incognito: false });
  await integration.onNavigation({ id: 4, url: "https://example.com/page", incognito: false });
  const events = native.calls.filter((call) => call.type === "browser.event.emit");
  assert.deepEqual(events.map((call) => call.payload.sequence_no), [1, 2, 1, 2]);
  assert.equal(native.reconnects, 1);
});

test("one adapter instance receives one hundred unique increasing sequences", async () => {
  const active = await mapping();
  const { integration, native } = fixture({ initial: [active] });
  native.responses.set("browser.event.emit", { event_id: actionId, stored: true, duplicate: false });
  for (let tabId = 1; tabId <= 100; tabId += 1) {
    await integration.onNavigation({ id: tabId, url: "https://example.com/page", incognito: false });
  }
  const sequences = native.calls
    .filter((call) => call.type === "browser.event.emit")
    .map((call) => call.payload.sequence_no);
  assert.deepEqual(sequences, Array.from({ length: 100 }, (_value, index) => index + 1));
});

test("a navigation with revoked permission pauses the Core scope and emits no event", async () => {
  const active = await mapping();
  const { integration, native, store } = fixture({ initial: [active], permissions: new Set() });
  native.responses.set("browser.observation.pause", { id: scopeId, source: "browser.chromium", resource_id: resourceId, status: "paused", created_at: "x", updated_at: "x" });
  await integration.onNavigation({ id: 1, url: "https://example.com/page", incognito: false });
  assert.equal(native.calls.length, 1);
  assert.equal(native.calls[0].type, "browser.observation.pause");
  assert.equal(store.items.length, 0);
});

test("action resource identity mismatch never opens a tab", async () => {
  const active = await mapping();
  const { browser, integration, native } = fixture({ initial: [active] });
  await integration.handleActionExecute({
    protocol_version: PROTOCOL_VERSION,
    request_id: actionId,
    type: "browser.action.execute",
    payload: {
      request_id: actionId,
      action_execution_id: "44444444-4444-4444-8444-444444444444",
      action: { type: "browser.tab.ensure_open.v1", resource_id: resourceId },
      resolved_resource: { resource_id: "55555555-5555-4555-8555-555555555555", kind: "web.page", canonical_locator: "https://example.com/page" },
    },
  });
  assert.equal(browser.created.length, 0);
  assert.equal(native.notifications[0].payload.result.result_code, "resource_identity_mismatch");
});

test("browser action envelopes with unknown fields are rejected before tab access", async () => {
  const active = await mapping();
  const { browser, integration, native } = fixture({ initial: [active] });
  await integration.handleActionExecute({
    protocol_version: PROTOCOL_VERSION,
    request_id: actionId,
    type: "browser.action.execute",
    payload: {
      request_id: actionId,
      action_execution_id: "44444444-4444-4444-8444-444444444444",
      action: { type: "browser.tab.ensure_open.v1", resource_id: resourceId, unexpected: true },
      resolved_resource: { resource_id: resourceId, kind: "web.page", canonical_locator: "https://example.com/page" },
    },
  });
  assert.equal(browser.created.length, 0);
  assert.equal(native.notifications[0].payload.result.result_code, "action_failed");
});

test("browser action skips an existing tab and opens a missing registered tab", async () => {
  const active = await mapping();
  const { browser, integration, native } = fixture({ initial: [active] });
  browser.tabs = [{ id: 8, url: "https://example.com/page", incognito: false }];
  const message = {
    protocol_version: PROTOCOL_VERSION,
    request_id: actionId,
    type: "browser.action.execute",
    payload: {
      request_id: actionId,
      action_execution_id: "44444444-4444-4444-8444-444444444444",
      action: { type: "browser.tab.ensure_open.v1", resource_id: resourceId },
      resolved_resource: { resource_id: resourceId, kind: "web.page", canonical_locator: "https://example.com/page" },
    },
  };
  await integration.handleActionExecute(message);
  assert.equal(native.notifications[0].payload.result.result_code, "already_open");
  browser.tabs = [];
  await integration.handleActionExecute({ ...message, request_id: "66666666-6666-4666-8666-666666666666", payload: { ...message.payload, request_id: "66666666-6666-4666-8666-666666666666" } });
  assert.deepEqual(browser.created, ["https://example.com/page"]);
  assert.equal(native.notifications[1].payload.result.result_code, "opened");
});

test("self-generated browser tab navigation is suppressed once and expires safely", async () => {
  const active = await mapping();
  const entries = new Map();
  const suppression = {
    async mark(tabId, resourceId, expiresAt) { entries.set(tabId, { resourceId, expiresAt }); },
    async consume(tabId, resourceId, now, phase = "complete") {
      const entry = entries.get(tabId);
      if (entry === undefined) return false;
      if (entry.resourceId !== resourceId) return false;
      if (entry.expiresAt < now) { entries.delete(tabId); return false; }
      if (phase === "complete") entries.delete(tabId);
      return true;
    },
  };
  const { browser, integration, native } = fixture({ initial: [active], suppression });
  await integration.handleActionExecute({
    protocol_version: PROTOCOL_VERSION,
    request_id: actionId,
    type: "browser.action.execute",
    payload: {
      request_id: actionId,
      action_execution_id: "44444444-4444-4444-8444-444444444444",
      action: { type: "browser.tab.ensure_open.v1", resource_id: resourceId },
      resolved_resource: { resource_id: resourceId, kind: "web.page", canonical_locator: "https://example.com/page" },
    },
  });
  assert.equal(browser.created.length, 1);
  native.responses.set("browser.event.emit", { event_id: actionId, stored: true, duplicate: false });
  await integration.onNavigation({ id: 42, url: "https://example.com/page", incognito: false });
  assert.equal(native.calls.filter((call) => call.type === "browser.event.emit").length, 0);
  await integration.onNavigation({ id: 43, url: "https://example.com/page", incognito: false });
  assert.equal(native.calls.filter((call) => call.type === "browser.event.emit").length, 1);
});

test("self-generated suppression is stored before a registered URL is navigated", async () => {
  const active = await mapping();
  const order = [];
  const suppression = {
    async mark() { order.push("mark"); },
    async consume() { return true; },
  };
  const { browser, integration, native } = fixture({ initial: [active], suppression });
  const prepared = browser.createPreparedTab.bind(browser);
  browser.createPreparedTab = async () => { order.push("create"); return prepared(); };
  browser.navigateTab = async (_tabId, url) => { order.push(`navigate:${url}`); };
  await integration.handleActionExecute({
    protocol_version: PROTOCOL_VERSION,
    request_id: actionId,
    type: "browser.action.execute",
    payload: {
      request_id: actionId,
      action_execution_id: "44444444-4444-4444-8444-444444444444",
      action: { type: "browser.tab.ensure_open.v1", resource_id: resourceId },
      resolved_resource: { resource_id: resourceId, kind: "web.page", canonical_locator: "https://example.com/page" },
    },
  });
  assert.deepEqual(order, ["create", "mark", "navigate:https://example.com/page"]);
});

test("URL and complete callbacks both consume one self-generated navigation", async () => {
  const active = await mapping();
  const entries = new Map([[42, { resourceId, expiresAt: 2_000 }]]);
  const suppression = {
    async mark(tabId, id, expiresAt) { entries.set(tabId, { resourceId: id, expiresAt }); },
    async consume(tabId, id, now, phase = "complete") {
      const entry = entries.get(tabId);
      if (entry === undefined || entry.resourceId !== id || entry.expiresAt < now) return false;
      if (phase === "complete") entries.delete(tabId);
      return true;
    },
  };
  const { integration, native } = fixture({ initial: [active], suppression });
  native.responses.set("browser.event.emit", { event_id: actionId, stored: true, duplicate: false });
  await integration.onNavigation({ id: 42, url: "https://example.com/page", incognito: false, navigationPhase: "url" });
  await integration.onNavigation({ id: 42, url: "https://example.com/page", incognito: false, navigationPhase: "complete" });
  assert.equal(native.calls.filter((call) => call.type === "browser.event.emit").length, 0);
  assert.equal(entries.has(42), false);
});

test("suppression entries with a different resource are not consumed", async () => {
  const active = await mapping();
  const entries = new Map([[42, { resourceId: "66666666-6666-4666-8666-666666666666", expiresAt: 2_000 }]]);
  const suppression = {
    async mark() {},
    async consume(tabId, id) {
      const entry = entries.get(tabId);
      if (entry === undefined || entry.resourceId !== id) return false;
      entries.delete(tabId);
      return true;
    },
  };
  const { integration, native } = fixture({ initial: [active], suppression });
  native.responses.set("browser.event.emit", { event_id: actionId, stored: true, duplicate: false });
  await integration.onNavigation({ id: 42, url: "https://example.com/page", incognito: false });
  assert.equal(entries.has(42), true);
  assert.equal(native.calls.filter((call) => call.type === "browser.event.emit").length, 1);
});

class FakePort {
  constructor() { this.sent = []; this.messageListeners = []; this.disconnectListeners = []; }
  postMessage(message) { this.sent.push(message); }
  disconnect() { for (const listener of this.disconnectListeners) listener(); }
  onMessage = { addListener: (listener) => this.messageListeners.push(listener) };
  onDisconnect = { addListener: (listener) => this.disconnectListeners.push(listener) };
  emit(message) { for (const listener of this.messageListeners) listener(message); }
}

test("NativeBridge correlates responses and delivers action before event acknowledgement", async () => {
  const port = new FakePort();
  const actions = [];
  const bridge = new BrowserNativeBridge({
    createPort: () => port,
    createHello: async () => ({ protocol_version: PROTOCOL_VERSION, request_id: resourceId, type: "extension.hello", payload: {} }),
    onActionExecute: async (message) => { actions.push(message.request_id); },
    requestTimeoutMs: 1_000,
  });
  const started = bridge.start();
  await new Promise((resolve) => setImmediate(resolve));
  port.emit({ protocol_version: PROTOCOL_VERSION, request_id: resourceId, type: "extension.hello.ack", payload: {} });
  await started;
  const event = bridge.request("browser.event.emit", { sequence_no: 1 }, "browser.event.ack");
  await new Promise((resolve) => setImmediate(resolve));
  const requestId = port.sent.at(-1).request_id;
  port.emit({ protocol_version: PROTOCOL_VERSION, request_id: actionId, type: "browser.action.execute", payload: {} });
  port.emit({ protocol_version: PROTOCOL_VERSION, request_id: requestId, type: "browser.event.ack", payload: {} });
  await event;
  assert.deepEqual(actions, [actionId]);
  assert.equal(bridge.pendingCount(), 0);
});

test("NativeBridge prepares connection-bound payload only after Hello completes", async () => {
  const port = new FakePort();
  let factoryCalls = 0;
  const bridge = new BrowserNativeBridge({
    createPort: () => port,
    createHello: async () => ({ protocol_version: PROTOCOL_VERSION, request_id: resourceId, type: "extension.hello", payload: {} }),
    onActionExecute: async () => {},
    requestTimeoutMs: 1_000,
  });
  const started = bridge.start();
  const event = bridge.requestPrepared("browser.event.emit", "browser.event.ack", () => {
    factoryCalls += 1;
    return { sequence_no: 1 };
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(factoryCalls, 0);
  port.emit({ protocol_version: PROTOCOL_VERSION, request_id: resourceId, type: "extension.hello.ack", payload: {} });
  await started;
  await waitFor(() => factoryCalls === 1);
  const requestId = port.sent.at(-1).request_id;
  port.emit({ protocol_version: PROTOCOL_VERSION, request_id: requestId, type: "browser.event.ack", payload: {} });
  await event;
  bridge.stop();
});

test("NativeBridge rejects pending requests on disconnect", async () => {
  const port = new FakePort();
  const bridge = new BrowserNativeBridge({
    createPort: () => port,
    createHello: async () => ({ protocol_version: PROTOCOL_VERSION, request_id: resourceId, type: "extension.hello", payload: {} }),
    onActionExecute: async () => {},
    requestTimeoutMs: 1_000,
  });
  const started = bridge.start();
  await new Promise((resolve) => setImmediate(resolve));
  port.emit({ protocol_version: PROTOCOL_VERSION, request_id: resourceId, type: "extension.hello.ack", payload: {} });
  await started;
  const event = bridge.request("browser.event.emit", {}, "browser.event.ack");
  await waitFor(() => bridge.pendingCount() === 1);
  port.disconnect();
  await assert.rejects(event, /disconnected/);
  assert.equal(bridge.pendingCount(), 0);
  bridge.stop();
});

test("NativeBridge releases an event acknowledgement on timeout", async () => {
  const port = new FakePort();
  const timers = new Map();
  let nextTimer = 1;
  const bridge = new BrowserNativeBridge({
    createPort: () => port,
    createHello: async () => ({ protocol_version: PROTOCOL_VERSION, request_id: resourceId, type: "extension.hello", payload: {} }),
    onActionExecute: async () => {},
    requestTimeoutMs: 1_000,
    setTimer: (callback) => {
      const timer = nextTimer;
      nextTimer += 1;
      timers.set(timer, callback);
      return timer;
    },
    clearTimer: (timer) => { timers.delete(timer); },
  });
  const started = bridge.start();
  await new Promise((resolve) => setImmediate(resolve));
  port.emit({ protocol_version: PROTOCOL_VERSION, request_id: resourceId, type: "extension.hello.ack", payload: {} });
  await started;
  const event = bridge.request("browser.event.emit", {}, "browser.event.ack");
  await waitFor(() => bridge.pendingCount() === 1);
  const [timer] = timers.entries();
  timer[1]();
  await assert.rejects(event, /timed out/);
  assert.equal(bridge.pendingCount(), 0);
  bridge.stop();
});
