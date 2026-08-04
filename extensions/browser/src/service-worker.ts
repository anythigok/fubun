import {
  BrowserResourceMapping,
  PROTOCOL_VERSION,
  canonicalUrlHash,
  canonicalizeWebUrl,
  isKnownNativeType,
  originPattern,
  rejectUnknownKeys,
} from "@fubun/protocol-ts";

const HOST = "dev.fubun.browser";
const mappingsKey = "browser_mappings";
const dedup = new Map<string, number>();
let port: chrome.runtime.Port | undefined;
let reconnectDelay = 250;
let reconnectTimer: number | undefined;
let hostReady = false;

async function mappings(): Promise<BrowserResourceMapping[]> {
  const value = await chrome.storage.local.get(mappingsKey);
  const candidate: unknown = value[mappingsKey];
  return Array.isArray(candidate) ? candidate.filter(isMapping) : [];
}

function isMapping(value: unknown): value is BrowserResourceMapping {
  if (typeof value !== "object" || value === null) return false;
  const record = value as Record<string, unknown>;
  return typeof record.resource_id === "string"
    && typeof record.scope_id === "string"
    && typeof record.canonical_url_hash === "string"
    && typeof record.origin_pattern === "string"
    && typeof record.label === "string"
    && record.schema_version === "dev.fubun.ritual/1";
}

function connect(): chrome.runtime.Port {
  if (port !== undefined) return port;
  if (reconnectTimer !== undefined) {
    clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
  }
  const next = chrome.runtime.connectNative(HOST);
  port = next;
  next.onDisconnect.addListener(() => {
    if (port !== next) return;
    port = undefined;
    hostReady = false;
    reconnectDelay = Math.min(reconnectDelay * 2, 30_000);
    if (reconnectTimer === undefined) {
      reconnectTimer = setTimeout(() => {
        reconnectTimer = undefined;
        connect();
      }, reconnectDelay);
    }
  });
  next.onMessage.addListener((message: unknown) => {
    reconnectDelay = 250;
    void handleHostMessage(message).catch(() => undefined);
  });
  void mappings().then((current) => {
    next.postMessage({
      protocol_version: PROTOCOL_VERSION,
      request_id: crypto.randomUUID(),
      type: "extension.hello",
      payload: {
        extension_id: chrome.runtime.id,
        extension_version: chrome.runtime.getManifest().version,
        extension_instance_id: crypto.randomUUID(),
        permitted_resource_ids: current.map((item) => item.resource_id),
      },
    });
  });
  return next;
}

function send<T>(type: string, requestId: string, payload: T): void {
  connect().postMessage({ protocol_version: PROTOCOL_VERSION, request_id: requestId, type, payload });
}

async function observeCurrentPage(requestedCanonical?: string): Promise<void> {
  let canonical = requestedCanonical;
  if (canonical === undefined) {
    const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
    const tab = tabs[0];
    if (tab === undefined || tab.incognito === true || typeof tab.url !== "string") throw new Error("unsupported page");
    canonical = canonicalizeWebUrl(tab.url);
  }
  if (!await chrome.permissions.contains({ origins: [originPattern(canonical)] })) throw new Error("permission denied");
  const requestId = crypto.randomUUID();
  send("browser.observation.enable", requestId, { label: new URL(canonical).hostname, url: canonical });
}

async function stopCurrentPage(): Promise<void> {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  const tab = tabs[0];
  if (tab === undefined || tab.incognito === true || typeof tab.url !== "string") return;
  const hash = await canonicalUrlHash(canonicalizeWebUrl(tab.url));
  const mapping = (await mappings()).find((candidate) => candidate.canonical_url_hash === hash);
  if (mapping !== undefined) send("browser.observation.pause", crypto.randomUUID(), { scope_id: mapping.scope_id });
}

async function handleHostMessage(message: unknown): Promise<void> {
  if (typeof message !== "object" || message === null) return;
  const record = message as Record<string, unknown>;
  rejectUnknownKeys(record, ["protocol_version", "request_id", "type", "payload"]);
  if (typeof record.request_id !== "string" || typeof record.type !== "string" || !isKnownNativeType(record.type)) throw new Error("unknown native message");
  const version = record.protocol_version;
  if (typeof version !== "object" || version === null || (version as Record<string, unknown>).major !== PROTOCOL_VERSION.major) throw new Error("native protocol mismatch");
  if (record.type === "extension.hello.ack") {
    hostReady = true;
    return;
  }
  if (record.type === "browser.observation.enabled") {
    const envelope = record.payload;
    const payload = typeof envelope === "object" && envelope !== null && "data" in envelope
      ? (envelope as { data: unknown }).data
      : envelope;
    if (typeof payload !== "object" || payload === null) return;
    const value = payload as Record<string, unknown>;
    const resource = value.resource;
    const scope = value.scope;
    if (typeof resource !== "object" || resource === null || typeof scope !== "object" || scope === null) return;
    const mapping: BrowserResourceMapping = {
      resource_id: String((resource as Record<string, unknown>).id),
      scope_id: String((scope as Record<string, unknown>).id),
      canonical_url_hash: String(value.canonical_url_hash),
      origin_pattern: String(value.origin_pattern),
      label: String((resource as Record<string, unknown>).label),
      schema_version: "dev.fubun.ritual/1",
    };
    const current = await mappings();
    await chrome.storage.local.set({ [mappingsKey]: [...current.filter((item) => item.resource_id !== mapping.resource_id), mapping] });
    // Core binds permission state to one Adapter Instance. Reconnect after a
    // successful registration so the new permitted_resource_ids snapshot is
    // registered atomically; events and actions never use a stale instance.
    port?.disconnect();
    port = undefined;
    connect();
    return;
  }
  if (record.type !== "browser.action.execute" || typeof record.payload !== "object" || record.payload === null) return;
  const payload = record.payload as Record<string, unknown>;
  const action = payload.action;
  const resource = payload.resolved_resource;
  if (typeof action !== "object" || action === null || typeof resource !== "object" || resource === null) return;
  const actionRecord = action as Record<string, unknown>;
  const resourceRecord = resource as Record<string, unknown>;
  const resourceId = resourceRecord.resource_id;
  const mapping = (await mappings()).find((item) => item.resource_id === resourceId);
  let resultCode = "action_failed";
  let redactedMessage = "browser action failed";
  let resultStatus: "succeeded" | "skipped" | "failed" = "failed";
  if (actionRecord.type === "browser.tab.ensure_open.v1" && mapping !== undefined && typeof resourceRecord.canonical_locator === "string") {
      const permitted = await chrome.permissions.contains({ origins: [mapping.origin_pattern] });
      const canonical = canonicalizeWebUrl(resourceRecord.canonical_locator);
      const hash = await canonicalUrlHash(canonical);
      if (permitted && hash === mapping.canonical_url_hash) {
      const tabs = await chrome.tabs.query({});
      // URL inspection is asynchronous; compare only URLs that can be parsed.
      let alreadyOpen = false;
      for (const tab of tabs) {
        if (typeof tab.url !== "string") continue;
        try {
          if (await canonicalUrlHash(canonicalizeWebUrl(tab.url)) === hash) { alreadyOpen = true; break; }
        } catch { /* unsupported tab schemes are ignored */ }
      }
      if (alreadyOpen) {
        resultStatus = "skipped";
        resultCode = "already_open";
        redactedMessage = "registered page is already open";
      } else {
        await chrome.tabs.create({ url: canonical, active: true });
        resultStatus = "succeeded";
        resultCode = "opened";
        redactedMessage = "registered page opened";
      }
    }
  }
  send("browser.action.result", String(record.request_id), {
    request_id: String(record.request_id),
    action_execution_id: String(payload.action_execution_id),
    result: { status: resultStatus, result_code: resultCode, redacted_message: redactedMessage },
  });
}

chrome.runtime.onMessage.addListener((message: unknown, _sender, sendResponse) => {
  if (typeof message !== "object" || message === null) return false;
  const record = message as Record<string, unknown>;
  if (record.type === "status") {
    sendResponse({ native_host: hostReady, core: hostReady });
    return false;
  }
  if (record.type === "observe") {
    const requestedCanonical = typeof record.canonical_url === "string" ? record.canonical_url : undefined;
    void observeCurrentPage(requestedCanonical).then(() => sendResponse({ ok: true })).catch((error: unknown) => sendResponse({ ok: false, error: error instanceof Error ? error.message : "failed" }));
    return true;
  }
  if (record.type === "stop") {
    void stopCurrentPage().then(() => sendResponse({ ok: true })).catch(() => sendResponse({ ok: false }));
    return true;
  }
  return false;
});

chrome.runtime.onConnect.addListener(() => { void mappings(); });

chrome.tabs.onUpdated.addListener((transientTabId, changeInfo, tab) => {
  if (tab.incognito === true || (changeInfo.status !== "complete" && changeInfo.url === undefined) || typeof tab.url !== "string") return;
  void (async () => {
    const canonical = canonicalizeWebUrl(tab.url as string);
    const hash = await canonicalUrlHash(canonical);
    const mapping = (await mappings()).find((candidate) => candidate.canonical_url_hash === hash);
    if (mapping === undefined) return;
    const key = `${transientTabId}:${mapping.resource_id}`;
    const now = Date.now();
    if ((dedup.get(key) ?? 0) + 5000 > now) return;
    dedup.set(key, now);
    send("browser.event.emit", crypto.randomUUID(), { sequence_no: now, occurred_at: new Date().toISOString(), resource_id: mapping.resource_id });
  })().catch(() => undefined);
});

chrome.permissions.onRemoved.addListener((permissions) => {
  if (permissions.origins === undefined) return;
  void mappings().then((current) => {
    const remaining = current.filter((mapping) => !permissions.origins?.includes(mapping.origin_pattern));
    if (remaining.length !== current.length) {
      void chrome.storage.local.set({ [mappingsKey]: remaining });
      port?.disconnect();
      port = undefined;
      connect();
    }
  });
});

void chrome.storage.local.setAccessLevel?.({ accessLevel: "TRUSTED_CONTEXTS" });
