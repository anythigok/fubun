import { canonicalUrlHash, canonicalizeWebUrl, originPattern } from "@fubun/protocol-ts";

export {};

const status = document.querySelector<HTMLParagraphElement>("#status");
const connection = document.querySelector<HTMLParagraphElement>("#connection");
const page = document.querySelector<HTMLParagraphElement>("#page");
const registration = document.querySelector<HTMLParagraphElement>("#registration");
const permission = document.querySelector<HTMLParagraphElement>("#permission");
const observe = document.querySelector<HTMLButtonElement>("#observe");
const stop = document.querySelector<HTMLButtonElement>("#stop");

function show(value: string): void {
  if (status !== null) status.textContent = value;
}

async function refresh(): Promise<void> {
  const integration = await chrome.runtime.sendMessage({ type: "status" }) as { native_host?: boolean; core?: boolean };
  if (connection !== null) connection.textContent = `Native Host: ${integration.native_host === true ? "connected" : "disconnected"}; Core: ${integration.core === true ? "connected" : "disconnected"}`;
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  const tab = tabs[0];
  if (tab === undefined || tab.incognito === true || typeof tab.url !== "string") {
    if (page !== null) page.textContent = "Current page: unsupported";
    if (registration !== null) registration.textContent = "Observation: unavailable";
    if (permission !== null) permission.textContent = "Origin permission: unavailable";
    return;
  }
  try {
    const canonical = canonicalizeWebUrl(tab.url);
    const hash = await canonicalUrlHash(canonical);
    const stored = await chrome.storage.local.get("browser_mappings");
    const mappings: Array<{ canonical_url_hash?: unknown }> = Array.isArray(stored.browser_mappings)
      ? stored.browser_mappings as Array<{ canonical_url_hash?: unknown }>
      : [];
    const mapping = mappings.find((candidate) => candidate.canonical_url_hash === hash);
    const permitted = await chrome.permissions.contains({ origins: [originPattern(canonical)] });
    if (page !== null) page.textContent = "Current page: supported http(s)";
    if (registration !== null) registration.textContent = `Observation: ${mapping === undefined ? "not registered" : "registered"}`;
    if (permission !== null) permission.textContent = `Origin permission: ${permitted ? "granted" : "not granted"}`;
  } catch {
    if (page !== null) page.textContent = "Current page: unsupported";
    if (registration !== null) registration.textContent = "Observation: unavailable";
    if (permission !== null) permission.textContent = "Origin permission: unavailable";
  }
}

observe?.addEventListener("click", () => {
  void (async () => {
    const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
    const tab = tabs[0];
    if (tab === undefined || tab.incognito === true || typeof tab.url !== "string") throw new Error("unsupported page");
    const canonical = canonicalizeWebUrl(tab.url);
    if (!await chrome.permissions.request({ origins: [originPattern(canonical)] })) throw new Error("permission denied");
    return chrome.runtime.sendMessage({ type: "observe", canonical_url: canonical });
  })().then((result: unknown) => {
    show(typeof result === "object" && result !== null && (result as { ok?: boolean }).ok === true ? "Observation enabled" : "Permission or URL rejected");
    void refresh();
  }).catch(() => show("Permission or URL rejected"));
});

stop?.addEventListener("click", () => {
  void chrome.runtime.sendMessage({ type: "stop" }).then(() => show("Observation paused")).catch(() => show("Unable to pause observation"));
  void refresh();
});

show("Native host status is checked on demand");
void refresh().catch(() => show("Integration status unavailable"));
