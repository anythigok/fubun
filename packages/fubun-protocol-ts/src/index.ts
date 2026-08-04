export const MAX_MESSAGE_SIZE = 256 * 1024;
export const PROTOCOL_VERSION = { major: 1, minor: 0 } as const;
export const BROWSER_MAPPING_SCHEMA_VERSION = "dev.fubun.browser-mapping/1";

export type ProtocolVersion = Readonly<typeof PROTOCOL_VERSION>;
export type EventType =
  | "dev.fubun.browser.resource.opened.v1"
  | "dev.fubun.vscode.workspace.opened.v1";

export interface NativeEnvelope<T> {
  protocol_version: ProtocolVersion;
  request_id: string;
  type: string;
  payload: T;
}

export interface BrowserResourceMapping {
  resource_id: string;
  scope_id: string;
  canonical_url_hash: string;
  origin_pattern: string;
  label: string;
  schema_version: string;
  state?: "active" | "inactive" | "pause_pending";
}

const forbiddenControl = /[\u0000-\u001f\u007f]/u;

export function canonicalizeWebUrl(input: string): string {
  if (input.length === 0 || input.length > 2048) throw new Error("invalid URL length");
  const url = new URL(input);
  if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("unsupported scheme");
  if (url.username !== "" || url.password !== "") throw new Error("userinfo is forbidden");
  url.search = "";
  url.hash = "";
  if (url.pathname === "") url.pathname = "/";
  if ((url.protocol === "http:" && url.port === "80") || (url.protocol === "https:" && url.port === "443")) url.port = "";
  const canonical = url.toString();
  if (canonical.length > 2048 || forbiddenControl.test(canonical)) throw new Error("invalid URL");
  return canonical;
}

export async function canonicalUrlHash(canonicalUrl: string): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(canonicalUrl));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function originPattern(canonicalUrl: string): string {
  const url = new URL(canonicalUrl);
  return `${url.protocol}//${url.host}/*`;
}

export function encodeUdsFrame(value: unknown): Uint8Array {
  const payload = new TextEncoder().encode(JSON.stringify(value));
  if (payload.byteLength > MAX_MESSAGE_SIZE) throw new Error("message too large");
  const frame = new Uint8Array(payload.byteLength + 4);
  new DataView(frame.buffer).setUint32(0, payload.byteLength, true);
  frame.set(payload, 4);
  return frame;
}

export function decodeUdsFrame(frame: Uint8Array): unknown {
  if (frame.byteLength < 4) throw new Error("partial frame");
  const length = new DataView(frame.buffer, frame.byteOffset, 4).getUint32(0, true);
  if (length === 0 || length > MAX_MESSAGE_SIZE || frame.byteLength !== length + 4) throw new Error("invalid frame");
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(frame.slice(4)));
}

export function isBrowserEventType(value: string): value is EventType {
  return value === "dev.fubun.browser.resource.opened.v1";
}

export function isKnownNativeType(value: string): boolean {
  return [
    "extension.hello",
    "browser.observation.enable",
    "browser.observation.pause",
    "browser.event.emit",
    "browser.action.result",
    "integration.ping",
    "extension.hello.ack",
    "browser.observation.enabled",
    "browser.observation.paused",
    "browser.action.execute",
    "browser.event.ack",
    "integration.pong",
    "integration.error",
  ].includes(value);
}

export function isUuid(value: unknown): value is string {
  return typeof value === "string"
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(value);
}

export function parseNativeEnvelope(value: unknown): NativeEnvelope<unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("native envelope must be an object");
  const record = value as Record<string, unknown>;
  rejectUnknownKeys(record, ["protocol_version", "request_id", "type", "payload"]);
  const version = record.protocol_version;
  if (typeof version !== "object" || version === null || Array.isArray(version)) throw new Error("invalid protocol version");
  const versionRecord = version as Record<string, unknown>;
  rejectUnknownKeys(versionRecord, ["major", "minor"]);
  if (versionRecord.major !== PROTOCOL_VERSION.major || versionRecord.minor !== PROTOCOL_VERSION.minor) throw new Error("native protocol mismatch");
  if (!isUuid(record.request_id) || typeof record.type !== "string" || !isKnownNativeType(record.type)) throw new Error("invalid native envelope");
  return {
    protocol_version: PROTOCOL_VERSION,
    request_id: record.request_id,
    type: record.type,
    payload: record.payload,
  };
}

export function rejectUnknownKeys(value: Record<string, unknown>, keys: readonly string[]): void {
  const allowed = new Set(keys);
  for (const key of Object.keys(value)) if (!allowed.has(key)) throw new Error(`unknown field: ${key}`);
}
