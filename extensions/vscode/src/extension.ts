import * as vscode from "vscode";
import { canonicalUrlHash, encodeUdsFrame } from "@fubun/protocol-ts";
import { realpath } from "node:fs/promises";
import { connect } from "node:net";

const resourceKey = "fubun.resource_id";
const scopeKey = "fubun.scope_id";
const hashKey = "fubun.workspace_hash";
let lastEventAt = 0;

interface WorkspaceMapping {
  resourceId: string;
  scopeId: string;
  pathHash: string;
}

function currentWorkspace(): vscode.WorkspaceFolder | undefined {
  const folders = vscode.workspace.workspaceFolders;
  if (folders === undefined || folders.length !== 1) return undefined;
  const folder = folders[0];
  if (folder === undefined || folder.uri.scheme !== "file") return undefined;
  return folder;
}

async function callCore<T>(message: unknown): Promise<T> {
  const socketPath = process.env.XDG_RUNTIME_DIR === undefined ? undefined : `${process.env.XDG_RUNTIME_DIR}/fubun/core.sock`;
  if (socketPath === undefined) throw new Error("XDG_RUNTIME_DIR is required");
  return new Promise<T>((resolve, reject) => {
    const socket = connect(socketPath);
    let settled = false;
    const timer = setTimeout(() => finishReject(new Error("Core request timed out")), 5000);
    const finishResolve = (value: T): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(value);
    };
    const finishReject = (error: unknown): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(error);
    };
    let buffered = Buffer.alloc(0);
    let frameCount = 0;
    socket.on("error", finishReject);
    socket.on("close", () => finishReject(new Error("Core connection closed")));
    socket.on("data", (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 4) {
        const length = buffered.readUInt32LE(0);
        if (length === 0 || length > 256 * 1024 || buffered.length < length + 4) return;
        const frame = buffered.subarray(4, length + 4);
        buffered = buffered.subarray(length + 4);
        frameCount += 1;
        if (frameCount === 1) continue;
        try { finishResolve(JSON.parse(frame.toString("utf8")) as T); } catch (error: unknown) { finishReject(error); }
        socket.end();
        return;
      }
    });
    socket.on("connect", () => {
      const hello = { protocol_version: { major: 1, minor: 0 }, request_id: crypto.randomUUID(), body: { method: "client.hello", params: { client_name: "fubun-vscode", client_version: "0.1.0", protocol_version: { major: 1, minor: 0 } } } };
      socket.write(Buffer.from(encodeUdsFrame(hello)));
      socket.write(Buffer.from(encodeUdsFrame(message)));
    });
  });
}

async function observe(context: vscode.ExtensionContext): Promise<void> {
  const folder = currentWorkspace();
  if (folder === undefined) { void vscode.window.showWarningMessage("Fubun supports one local file workspace only."); return; }
  const canonicalPath = await realpath(folder.uri.fsPath);
  const label = await vscode.window.showInputBox({ prompt: "Fubun workspace label", value: folder.name });
  if (label === undefined || label.length === 0) return;
  const response = await callCore<Record<string, unknown>>({ protocol_version: { major: 1, minor: 0 }, request_id: crypto.randomUUID(), body: { method: "vscode.observation.enable", params: { label, absolute_path: canonicalPath } } });
  const body = response.body as Record<string, unknown>;
  const payload = body.data as Record<string, unknown>;
  const resource = payload.resource as Record<string, unknown>;
  const scope = payload.scope as Record<string, unknown>;
  const pathHash = await canonicalUrlHash(`file://${canonicalPath}`);
  await vscode.commands.executeCommand("setContext", "fubun.observing", true);
  const mapping: WorkspaceMapping = { resourceId: String(resource.id), scopeId: String(scope.id), pathHash };
  await context.globalState.update(resourceKey, mapping.resourceId);
  await context.globalState.update(scopeKey, mapping.scopeId);
  await context.globalState.update(hashKey, mapping.pathHash);
  void vscode.window.showInformationMessage("Fubun workspace observation enabled.");
  await sendWorkspaceEvent(mapping);
}

async function sendWorkspaceEvent(mapping: WorkspaceMapping): Promise<void> {
  const now = Date.now();
  if (now - lastEventAt < 5000) return;
  lastEventAt = now;
  const socketPath = process.env.XDG_RUNTIME_DIR === undefined ? undefined : `${process.env.XDG_RUNTIME_DIR}/fubun/core.sock`;
  if (socketPath === undefined) return;
  await new Promise<void>((resolve, reject) => {
    const socket = connect(socketPath);
    let settled = false;
    const timer = setTimeout(() => finishReject(new Error("Event request timed out")), 5000);
    const finishResolve = (): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve();
    };
    const finishReject = (error: unknown): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(error);
    };
    let buffered = Buffer.alloc(0);
    let frames = 0;
    socket.on("error", finishReject);
    socket.on("close", () => finishReject(new Error("Core connection closed")));
    socket.on("data", (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 4) {
        const length = buffered.readUInt32LE(0);
        if (length === 0 || length > 256 * 1024 || buffered.length < length + 4) return;
        buffered = buffered.subarray(length + 4);
        frames += 1;
        if (frames >= 2) { finishResolve(); socket.end(); return; }
      }
    });
    socket.on("connect", () => {
      const instanceId = crypto.randomUUID();
      socket.write(Buffer.from(encodeUdsFrame({ protocol_version: { major: 1, minor: 0 }, request_id: crypto.randomUUID(), body: { method: "adapter.hello", params: { adapter_id: "dev.fubun.vscode", adapter_version: "0.1.0", instance_id: instanceId, action_capabilities: [], event_capabilities: ["dev.fubun.vscode.workspace.opened.v1"], status: { tools: [], desktop_entry_ids: [], permitted_resource_ids: [] } } } })));
      socket.write(Buffer.from(encodeUdsFrame({ protocol_version: { major: 1, minor: 0 }, request_id: crypto.randomUUID(), action_execution_id: crypto.randomUUID(), body: { kind: "event.emit", data: { sequence_no: now, occurred_at: new Date(now).toISOString(), type: "dev.fubun.vscode.workspace.opened.v1", resource_id: mapping.resourceId } } })));
    });
  });
}

async function emitIfRegistered(context: vscode.ExtensionContext): Promise<void> {
  const folder = currentWorkspace();
  if (folder === undefined) return;
  const resourceId = context.globalState.get<string>(resourceKey);
  const scopeId = context.globalState.get<string>(scopeKey);
  const pathHash = context.globalState.get<string>(hashKey);
  if (resourceId === undefined || scopeId === undefined || pathHash === undefined) return;
  try {
    const currentHash = await realpath(folder.uri.fsPath)
      .then((path) => canonicalUrlHash(`file://${path}`));
    if (currentHash === pathHash) await sendWorkspaceEvent({ resourceId, scopeId, pathHash });
  } catch {
    // A workspace that is no longer canonicalizable is simply not observed.
  }
}

async function stop(context: vscode.ExtensionContext): Promise<void> {
  const scopeId = context.globalState.get<string>(scopeKey);
  if (scopeId !== undefined) {
    await callCore<Record<string, unknown>>({ protocol_version: { major: 1, minor: 0 }, request_id: crypto.randomUUID(), body: { method: "observation.pause", params: { scope_id: scopeId } } });
  }
  await context.globalState.update(resourceKey, undefined);
  await context.globalState.update(scopeKey, undefined);
  await context.globalState.update(hashKey, undefined);
  void vscode.window.showInformationMessage("Fubun workspace observation paused.");
}

export function activate(context: vscode.ExtensionContext): void {
  context.subscriptions.push(
    vscode.commands.registerCommand("fubun.observeCurrentWorkspace", () => observe(context)),
    vscode.commands.registerCommand("fubun.stopObservingCurrentWorkspace", () => stop(context)),
    vscode.commands.registerCommand("fubun.showIntegrationStatus", () => vscode.window.showInformationMessage("Fubun integration status is available from `fubun integrations status`.")),
    vscode.workspace.onDidChangeWorkspaceFolders(() => { void emitIfRegistered(context); }),
  );
  void emitIfRegistered(context);
}

export function deactivate(): void { /* no child process, shell, or network lifecycle */ }
