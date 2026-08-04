import {
  BROWSER_MAPPING_SCHEMA_VERSION,
  BrowserResourceMapping,
  isUuid,
  rejectUnknownKeys,
} from "@fubun/protocol-ts";
import { BrowserIntegration, BrowserTab, MappingStore } from "./integration.js";
import { BrowserNativeBridge, NativePort } from "./native-bridge.js";

const HOST = "dev.fubun.browser";
const mappingsKey = "browser_mappings";

const store: MappingStore = {
  async read(): Promise<BrowserResourceMapping[]> {
    const value = await chrome.storage.local.get(mappingsKey);
    const candidate: unknown = value[mappingsKey];
    return Array.isArray(candidate) ? candidate.filter(isStoredMapping) : [];
  },
  async write(mappings: BrowserResourceMapping[]): Promise<void> {
    await chrome.storage.local.set({ [mappingsKey]: mappings });
  },
};

let integration: BrowserIntegration;
const bridge = new BrowserNativeBridge({
  createPort: createNativePort,
  createHello: async () => integration.extensionHello(),
  onActionExecute: async (message) => integration.handleActionExecute(message),
});

integration = new BrowserIntegration({
  browser: {
    async activeTab(): Promise<BrowserTab | undefined> {
      const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
      return tabFromChrome(tabs[0]);
    },
    async allTabs(): Promise<BrowserTab[]> {
      return (await chrome.tabs.query({}))
        .map(tabFromChrome)
        .filter((tab): tab is BrowserTab => tab !== undefined);
    },
    async createTab(url: string): Promise<void> {
      await chrome.tabs.create({ url, active: true });
    },
    async permissionGranted(origin: string): Promise<boolean> {
      return chrome.permissions.contains({ origins: [origin] });
    },
  },
  store,
  native: bridge,
  extensionId: () => chrome.runtime.id,
  extensionVersion: () => chrome.runtime.getManifest().version,
});

function createNativePort(): NativePort {
  const port = chrome.runtime.connectNative(HOST);
  return {
    postMessage(message: unknown): void {
      port.postMessage(message);
    },
    disconnect(): void {
      port.disconnect();
    },
    onMessage: {
      addListener(listener): void {
        port.onMessage.addListener((message: unknown) => listener(message));
      },
    },
    onDisconnect: {
      addListener(listener): void {
        port.onDisconnect.addListener(listener);
      },
    },
  };
}

function tabFromChrome(tab: chrome.tabs.Tab | undefined): BrowserTab | undefined {
  if (tab === undefined) return undefined;
  const result: BrowserTab = {};
  if (tab.id !== undefined) result.id = tab.id;
  if (tab.url !== undefined) result.url = tab.url;
  if (tab.incognito !== undefined) result.incognito = tab.incognito;
  return result;
}

function isStoredMapping(value: unknown): value is BrowserResourceMapping {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  try {
    rejectUnknownKeys(record, ["resource_id", "scope_id", "canonical_url_hash", "origin_pattern", "label", "schema_version", "state"]);
  } catch {
    return false;
  }
  return record.schema_version === BROWSER_MAPPING_SCHEMA_VERSION
    && isUuid(record.resource_id)
    && isUuid(record.scope_id)
    && typeof record.canonical_url_hash === "string"
    && /^[0-9a-f]{64}$/u.test(record.canonical_url_hash)
    && typeof record.origin_pattern === "string"
    && typeof record.label === "string"
    && (record.state === undefined || record.state === "active" || record.state === "inactive" || record.state === "pause_pending");
}

chrome.runtime.onMessage.addListener((message: unknown, _sender, sendResponse) => {
  if (typeof message !== "object" || message === null || Array.isArray(message)) return false;
  const record = message as Record<string, unknown>;
  if (record.type === "status") {
    void integration.status().then(sendResponse).catch(() => sendResponse({ native_host: false, core: false }));
    return true;
  }
  if (record.type === "observe") {
    const requestedCanonical = typeof record.canonical_url === "string" ? record.canonical_url : undefined;
    void integration.observeCurrentPage(requestedCanonical)
      .then(() => sendResponse({ ok: true }))
      .catch((error: unknown) => sendResponse({ ok: false, error: error instanceof Error ? error.message : "failed" }));
    return true;
  }
  if (record.type === "stop") {
    void integration.stopCurrentPage()
      .then(() => sendResponse({ ok: true }))
      .catch((error: unknown) => sendResponse({ ok: false, error: error instanceof Error ? error.message : "failed" }));
    return true;
  }
  return false;
});

chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.status !== "complete" && changeInfo.url === undefined) return;
  const mapped = tabFromChrome(tab);
  if (mapped === undefined) return;
  void integration.onNavigation({ ...mapped, id: tabId }).catch(() => undefined);
});

chrome.permissions.onRemoved.addListener((permissions) => {
  if (permissions.origins === undefined) return;
  void integration.onPermissionsRemoved(permissions.origins).catch(() => undefined);
});

void chrome.storage.local.setAccessLevel?.({ accessLevel: "TRUSTED_CONTEXTS" });
void integration.start().catch(() => undefined);
