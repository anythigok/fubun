import {
  BROWSER_MAPPING_SCHEMA_VERSION,
  BrowserResourceMapping,
  NativeEnvelope,
  PROTOCOL_VERSION,
  canonicalUrlHash,
  canonicalizeWebUrl,
  isUuid,
  originPattern,
  rejectUnknownKeys,
} from "@fubun/protocol-ts";

export interface BrowserTab {
  id?: number;
  url?: string;
  incognito?: boolean;
  navigationPhase?: NavigationPhase;
}

export type NavigationPhase = "url" | "complete";

export interface BrowserApi {
  activeTab(): Promise<BrowserTab | undefined>;
  allTabs(): Promise<BrowserTab[]>;
  createPreparedTab(): Promise<number | undefined>;
  navigateTab(tabId: number, url: string): Promise<void>;
  permissionGranted(origin: string): Promise<boolean>;
}

export interface SelfGeneratedSuppressionStore {
  mark(tabId: number, resourceId: string, expiresAt: number): Promise<void>;
  consume(tabId: number, resourceId: string, now: number, phase?: NavigationPhase): Promise<boolean>;
}

export interface SessionSuppressionStorage {
  get(key: string): Promise<Record<string, unknown>>;
  set(items: Record<string, unknown>): Promise<void>;
  remove(key: string): Promise<void>;
}

type StoredSuppressionEntry = {
  resource_id: string;
  expires_at: number;
  phase: "awaiting_complete" | "completed";
};

const SUPPRESSION_TOMBSTONE_MS = 5_000;

/**
 * Stores each ephemeral tab suppression under an independent session key.
 * Per-key queues make same-tab phase transitions cancellation-safe while
 * allowing unrelated tabs to update concurrently without lost updates.
 */
export function createSessionSuppressionStore(storage: SessionSuppressionStorage): SelfGeneratedSuppressionStore {
  const queues = new Map<string, Promise<void>>();
  const keyFor = (tabId: number): string => `fubun.self-generated-tab.${tabId}`;

  function enqueue<T>(key: string, work: () => Promise<T>): Promise<T> {
    const previous = queues.get(key) ?? Promise.resolve();
    const run = previous.catch(() => undefined).then(work);
    const marker = run.then(() => undefined, () => undefined);
    queues.set(key, marker);
    void marker.then(() => {
      if (queues.get(key) === marker) queues.delete(key);
    });
    return run;
  }

  function parse(value: unknown): StoredSuppressionEntry | undefined {
    if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
    const record = value as Record<string, unknown>;
    try {
      rejectUnknownKeys(record, ["resource_id", "expires_at", "phase"]);
    } catch {
      return undefined;
    }
    if (!isUuid(record.resource_id) || typeof record.expires_at !== "number" || !Number.isFinite(record.expires_at)
      || (record.phase !== "awaiting_complete" && record.phase !== "completed")) {
      return undefined;
    }
    return {
      resource_id: record.resource_id,
      expires_at: record.expires_at,
      phase: record.phase,
    };
  }

  return {
    mark(tabId, resourceId, expiresAt) {
      const key = keyFor(tabId);
      return enqueue(key, async () => {
        await storage.set({ [key]: { resource_id: resourceId, expires_at: expiresAt, phase: "awaiting_complete" } });
      });
    },
    consume(tabId, resourceId, now, phase = "complete") {
      const key = keyFor(tabId);
      return enqueue(key, async () => {
        const value = await storage.get(key);
        const candidate = parse(value[key]);
        if (candidate === undefined || candidate.resource_id !== resourceId) return false;
        if (candidate.expires_at <= now) {
          await storage.remove(key);
          return false;
        }
        if (phase === "complete" && candidate.phase === "awaiting_complete") {
          await storage.set({
            [key]: {
              ...candidate,
              phase: "completed",
              expires_at: Math.min(candidate.expires_at, now + SUPPRESSION_TOMBSTONE_MS),
            },
          });
        }
        return true;
      });
    },
  };
}

export interface MappingStore {
  read(): Promise<BrowserResourceMapping[]>;
  write(mappings: BrowserResourceMapping[]): Promise<void>;
}

export interface NativeRequester {
  start(): Promise<void>;
  reconnect(): Promise<void>;
  isReady(): boolean;
  request(type: string, payload: unknown, expectedType: string): Promise<unknown>;
  requestPrepared(type: string, expectedType: string, payloadFactory: () => unknown): Promise<unknown>;
  notify(type: string, requestId: string, payload: unknown): Promise<void>;
}

export interface BrowserIntegrationOptions {
  browser: BrowserApi;
  store: MappingStore;
  native: NativeRequester;
  extensionId: () => string;
  extensionVersion: () => string;
  uuid?: () => string;
  now?: () => number;
  suppression?: SelfGeneratedSuppressionStore;
}

type MappingState = "active" | "inactive" | "pause_pending";

interface EnabledPayload {
  resourceId: string;
  scopeId: string;
  canonicalUrlHash: string;
  originPattern: string;
  label: string;
}

interface ActionExecutePayload {
  requestId: string;
  actionExecutionId: string;
  resourceId: string;
  canonicalLocator: string;
}

export class BrowserIntegration {
  private readonly uuid: () => string;
  private readonly now: () => number;
  private adapterInstanceId: string;
  private nextSequence = 1;
  private eventChain: Promise<void> = Promise.resolve();
  private readonly dedup = new Map<string, number>();

  public constructor(private readonly options: BrowserIntegrationOptions) {
    this.uuid = options.uuid ?? (() => crypto.randomUUID());
    this.now = options.now ?? (() => Date.now());
    this.adapterInstanceId = this.uuid();
  }

  public async start(): Promise<void> {
    await this.reconcilePermissions();
    await this.options.native.start();
  }

  public async status(): Promise<{ native_host: boolean; core: boolean }> {
    try {
      await this.options.native.start();
    } catch {
      return { native_host: false, core: false };
    }
    const ready = this.options.native.isReady();
    return { native_host: ready, core: ready };
  }

  public async extensionHello(): Promise<NativeEnvelope<unknown>> {
    this.adapterInstanceId = this.uuid();
    this.nextSequence = 1;
    const permittedResourceIds: string[] = [];
    for (const mapping of await this.mappings()) {
      if (this.isActive(mapping) && await this.options.browser.permissionGranted(mapping.origin_pattern)) {
        permittedResourceIds.push(mapping.resource_id);
      }
    }
    return {
      protocol_version: PROTOCOL_VERSION,
      request_id: this.uuid(),
      type: "extension.hello",
      payload: {
        extension_id: this.options.extensionId(),
        extension_version: this.options.extensionVersion(),
        extension_instance_id: this.adapterInstanceId,
        permitted_resource_ids: permittedResourceIds,
      },
    };
  }

  public async observeCurrentPage(requestedCanonical?: string): Promise<void> {
    const canonical = await this.currentCanonical(requestedCanonical);
    const pattern = originPattern(canonical);
    if (!await this.options.browser.permissionGranted(pattern)) throw new Error("permission denied");
    const response = await this.options.native.request(
      "browser.observation.enable",
      { label: new URL(canonical).hostname, url: canonical },
      "browser.observation.enabled",
    );
    const enabled = parseEnabledPayload(response);
    const expectedHash = await canonicalUrlHash(canonical);
    if (enabled.canonicalUrlHash !== expectedHash || enabled.originPattern !== pattern) {
      throw new Error("core returned an inconsistent browser resource");
    }
    if (!await this.options.browser.permissionGranted(pattern)) throw new Error("permission was revoked");
    const mapping: BrowserResourceMapping = {
      resource_id: enabled.resourceId,
      scope_id: enabled.scopeId,
      canonical_url_hash: expectedHash,
      origin_pattern: pattern,
      label: enabled.label,
      schema_version: BROWSER_MAPPING_SCHEMA_VERSION,
      state: "active",
    };
    const current = await this.mappings();
    await this.options.store.write([
      ...current.filter((candidate) => candidate.resource_id !== mapping.resource_id),
      mapping,
    ]);
    await this.options.native.reconnect();
  }

  public async stopCurrentPage(): Promise<void> {
    const canonical = await this.currentCanonical();
    const hash = await canonicalUrlHash(canonical);
    const current = await this.mappings();
    const mapping = current.find((candidate) => candidate.canonical_url_hash === hash && this.isActive(candidate));
    if (mapping === undefined) return;
    const inactive = this.inactive(mapping, "pause_pending");
    await this.options.store.write(current.map((candidate) => candidate.resource_id === mapping.resource_id ? inactive : candidate));
    try {
      await this.pauseCore(mapping.scope_id);
    } catch {
      throw new Error("observation is locally paused but Core pause is pending");
    }
    await this.options.store.write((await this.mappings()).filter((candidate) => candidate.resource_id !== mapping.resource_id));
    await this.options.native.reconnect();
  }

  public async onPermissionsRemoved(origins: readonly string[]): Promise<void> {
    const current = await this.mappings();
    const affected = current.filter((mapping) => this.isActive(mapping) && origins.includes(mapping.origin_pattern));
    if (affected.length === 0) return;
    const affectedIds = new Set(affected.map((mapping) => mapping.resource_id));
    await this.options.store.write(current.map((mapping) => affectedIds.has(mapping.resource_id)
      ? this.inactive(mapping, "pause_pending")
      : mapping));
    await this.syncPausedScopes(affected);
    await this.options.native.reconnect();
  }

  public async reconcilePermissions(): Promise<void> {
    const current = await this.mappings();
    const affected: BrowserResourceMapping[] = [];
    for (const mapping of current) {
      if (mapping.state === "pause_pending") {
        // A prior pause failure must never reactivate local observation. Retry
        // the Core synchronization once when this worker starts again.
        affected.push(mapping);
      } else if (this.isActive(mapping) && !await this.options.browser.permissionGranted(mapping.origin_pattern)) {
        affected.push(mapping);
      }
    }
    if (affected.length === 0) return;
    const affectedIds = new Set(affected.map((mapping) => mapping.resource_id));
    await this.options.store.write(current.map((mapping) => affectedIds.has(mapping.resource_id)
      ? this.inactive(mapping, "pause_pending")
      : mapping));
    await this.syncPausedScopes(affected);
    try {
      await this.options.native.reconnect();
    } catch {
      // The local inactive state is durable; the next explicit start can retry once.
    }
  }

  public async onNavigation(tab: BrowserTab): Promise<void> {
    if (tab.incognito === true || typeof tab.id !== "number" || typeof tab.url !== "string") return;
    let canonical: string;
    try {
      canonical = canonicalizeWebUrl(tab.url);
    } catch {
      return;
    }
    const hash = await canonicalUrlHash(canonical);
    const mapping = (await this.mappings()).find((candidate) => candidate.canonical_url_hash === hash && this.isActive(candidate));
    if (mapping === undefined) return;
    if (this.options.suppression !== undefined && typeof tab.id === "number"
      && await this.options.suppression.consume(tab.id, mapping.resource_id, this.now(), tab.navigationPhase)) return;
    if (!await this.options.browser.permissionGranted(mapping.origin_pattern)) {
      await this.onPermissionsRemoved([mapping.origin_pattern]);
      return;
    }
    const key = `${tab.id}:${mapping.resource_id}`;
    const now = this.now();
    const previous = this.dedup.get(key);
    if (previous !== undefined && previous + 5000 > now) return;
    this.dedup.set(key, now);
    const event = async (): Promise<void> => {
      const response = await this.options.native.requestPrepared(
        "browser.event.emit",
        "browser.event.ack",
        () => {
          const sequenceNo = this.nextSequence;
          this.nextSequence += 1;
          return {
            sequence_no: sequenceNo,
            occurred_at: new Date(now).toISOString(),
            resource_id: mapping.resource_id,
          };
        },
      );
      parseEventAck(response);
    };
    const queued = this.eventChain.then(event);
    this.eventChain = queued.catch(() => undefined);
    await queued;
  }

  public async handleActionExecute(message: NativeEnvelope<unknown>): Promise<void> {
    let result: { status: "succeeded" | "skipped" | "failed"; result_code: string; redacted_message: string };
    let actionExecutionId = this.uuid();
    try {
      const payload = parseActionExecutePayload(message);
      actionExecutionId = payload.actionExecutionId;
      const mapping = (await this.mappings()).find((candidate) => candidate.resource_id === payload.resourceId && this.isActive(candidate));
      if (mapping === undefined) {
        result = failure("resource_not_permitted", "registered browser resource is unavailable");
      } else if (!await this.options.browser.permissionGranted(mapping.origin_pattern)) {
        result = failure("permission_denied", "browser origin permission is unavailable");
      } else {
        const canonical = canonicalizeWebUrl(payload.canonicalLocator);
        const hash = await canonicalUrlHash(canonical);
        if (hash !== mapping.canonical_url_hash) {
          result = failure("resource_identity_mismatch", "resolved browser resource did not match the registered resource");
        } else {
          result = await this.ensureTabOpen(canonical, hash, mapping.resource_id);
        }
      }
    } catch (error) {
      const code = error instanceof Error && error.message === "resource identity mismatch"
        ? "resource_identity_mismatch"
        : "action_failed";
      result = failure(code, "browser action was rejected");
    }
    await this.options.native.notify("browser.action.result", message.request_id, {
      request_id: message.request_id,
      action_execution_id: actionExecutionId,
      result,
    });
  }

  private async ensureTabOpen(canonical: string, expectedHash: string, resourceId: string): Promise<{ status: "succeeded" | "skipped" | "failed"; result_code: string; redacted_message: string }> {
    for (const tab of await this.options.browser.allTabs()) {
      if (typeof tab.url !== "string") continue;
      try {
        if (await canonicalUrlHash(canonicalizeWebUrl(tab.url)) === expectedHash) {
          return { status: "skipped", result_code: "already_open", redacted_message: "registered page is already open" };
        }
      } catch {
        // Unsupported tab URLs are never emitted or persisted.
      }
    }
    const tabId = await this.options.browser.createPreparedTab();
    if (tabId === undefined || !Number.isInteger(tabId) || tabId < 0) {
      return failure("tab_id_unavailable", "browser did not return a usable tab identifier");
    }
    if (this.options.suppression !== undefined) {
      // Persist the ephemeral suppression before navigating the newly
      // created tab.  This ordering closes the navigation-vs-storage race.
      await this.options.suppression.mark(tabId, resourceId, this.now() + 60_000);
    }
    await this.options.browser.navigateTab(tabId, canonical);
    return { status: "succeeded", result_code: "opened", redacted_message: "registered page opened" };
  }

  private async syncPausedScopes(affected: BrowserResourceMapping[]): Promise<void> {
    const completed = new Set<string>();
    for (const mapping of affected) {
      try {
        await this.pauseCore(mapping.scope_id);
        completed.add(mapping.resource_id);
      } catch {
        // Keep the local mapping inactive until a later explicit reconciliation.
      }
    }
    if (completed.size > 0) {
      await this.options.store.write((await this.mappings()).filter((mapping) => !completed.has(mapping.resource_id)));
    }
  }

  private async pauseCore(scopeId: string): Promise<void> {
    const response = await this.options.native.request(
      "browser.observation.pause",
      { scope_id: scopeId },
      "browser.observation.paused",
    );
    parsePausedPayload(response, scopeId);
  }

  private async currentCanonical(requestedCanonical?: string): Promise<string> {
    if (requestedCanonical !== undefined) return canonicalizeWebUrl(requestedCanonical);
    const tab = await this.options.browser.activeTab();
    if (tab === undefined || tab.incognito === true || typeof tab.url !== "string") throw new Error("unsupported page");
    return canonicalizeWebUrl(tab.url);
  }

  private async mappings(): Promise<BrowserResourceMapping[]> {
    return (await this.options.store.read()).filter(isMapping);
  }

  private isActive(mapping: BrowserResourceMapping): boolean {
    return mapping.state === undefined || mapping.state === "active";
  }

  private inactive(mapping: BrowserResourceMapping, state: Extract<MappingState, "inactive" | "pause_pending">): BrowserResourceMapping {
    return { ...mapping, state };
  }
}

function isMapping(value: BrowserResourceMapping): boolean {
  return value.schema_version === BROWSER_MAPPING_SCHEMA_VERSION
    && isUuid(value.resource_id)
    && isUuid(value.scope_id)
    && /^[0-9a-f]{64}$/u.test(value.canonical_url_hash)
    && typeof value.origin_pattern === "string"
    && typeof value.label === "string"
    && (value.state === undefined || value.state === "active" || value.state === "inactive" || value.state === "pause_pending");
}

function parseEnabledPayload(value: unknown): EnabledPayload {
  const record = objectRecord(value, ["resource", "scope", "canonical_url_hash", "origin_pattern"]);
  const resource = objectRecord(record.resource, ["id", "kind", "label", "locator", "canonical_locator", "sensitivity", "scope", "created_at", "updated_at"]);
  const scope = objectRecord(record.scope, ["id", "source", "resource_id", "status", "created_at", "updated_at"]);
  if (!isUuid(resource.id) || resource.kind !== "web.page" || typeof resource.label !== "string"
    || !isUuid(scope.id) || scope.source !== "browser.chromium" || scope.status !== "active"
    || scope.resource_id !== resource.id || typeof record.canonical_url_hash !== "string"
    || typeof record.origin_pattern !== "string") {
    throw new Error("invalid browser observation response");
  }
  return {
    resourceId: resource.id,
    scopeId: scope.id,
    canonicalUrlHash: record.canonical_url_hash,
    originPattern: record.origin_pattern,
    label: resource.label,
  };
}

function parsePausedPayload(value: unknown, expectedScopeId: string): void {
  const scope = objectRecord(value, ["id", "source", "resource_id", "status", "created_at", "updated_at"]);
  if (!isUuid(scope.id) || scope.id !== expectedScopeId || scope.source !== "browser.chromium"
    || scope.status !== "paused" || !isUuid(scope.resource_id)) {
    throw new Error("invalid browser pause response");
  }
}

function parseEventAck(value: unknown): void {
  const record = objectRecord(value, ["event_id", "stored", "duplicate"]);
  if (!isUuid(record.event_id) || typeof record.stored !== "boolean" || typeof record.duplicate !== "boolean") {
    throw new Error("invalid browser event acknowledgement");
  }
}

function parseActionExecutePayload(message: NativeEnvelope<unknown>): ActionExecutePayload {
  const payload = objectRecord(message.payload, ["request_id", "action_execution_id", "action", "resolved_resource"]);
  const action = objectRecord(payload.action, ["type", "resource_id"]);
  const resource = objectRecord(payload.resolved_resource, ["resource_id", "kind", "canonical_locator"]);
  if (!isUuid(payload.request_id) || payload.request_id !== message.request_id || !isUuid(payload.action_execution_id)
    || action.type !== "browser.tab.ensure_open.v1" || !isUuid(action.resource_id)
    || !isUuid(resource.resource_id) || action.resource_id !== resource.resource_id
    || resource.kind !== "web.page" || typeof resource.canonical_locator !== "string") {
    throw new Error("resource identity mismatch");
  }
  return {
    requestId: payload.request_id,
    actionExecutionId: payload.action_execution_id,
    resourceId: action.resource_id,
    canonicalLocator: resource.canonical_locator,
  };
}

function objectRecord(value: unknown, allowed: readonly string[]): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("invalid browser payload");
  const record = value as Record<string, unknown>;
  rejectUnknownKeys(record, allowed);
  return record;
}

function failure(resultCode: string, message: string): { status: "failed"; result_code: string; redacted_message: string } {
  return { status: "failed", result_code: resultCode, redacted_message: message };
}
