import * as vscode from "vscode";
import { canonicalUrlHash, isUuid } from "@fubun/protocol-ts";
import { realpath } from "node:fs/promises";
import { VscodeAdapterSession } from "./adapter-session.js";
import { CoreClient } from "./core-client.js";
import { coreSocketPath, nodeSocketFactory, record } from "./uds.js";

const resourceKey = "fubun.resource_id";
const scopeKey = "fubun.scope_id";
const hashKey = "fubun.workspace_hash";

interface WorkspaceMapping {
  resourceId: string;
  scopeId: string;
  pathHash: string;
}

let adapterSession: VscodeAdapterSession | undefined;
let lastEventAt = 0;

function currentWorkspace(): vscode.WorkspaceFolder | undefined {
  if (vscode.env.remoteName !== undefined) return undefined;
  const folders = vscode.workspace.workspaceFolders;
  if (folders === undefined || folders.length !== 1) return undefined;
  const folder = folders[0];
  if (folder === undefined || folder.uri.scheme !== "file") return undefined;
  return folder;
}

function client(): CoreClient {
  return new CoreClient({ socketPath: coreSocketPath(), createTransport: nodeSocketFactory });
}

function session(): VscodeAdapterSession {
  if (adapterSession === undefined) {
    adapterSession = new VscodeAdapterSession({
      socketPath: coreSocketPath(),
      createTransport: nodeSocketFactory,
    });
  }
  return adapterSession;
}

async function observe(context: vscode.ExtensionContext): Promise<void> {
  const folder = currentWorkspace();
  if (folder === undefined) {
    await vscode.window.showWarningMessage("Fubun supports one local file workspace only.");
    return;
  }
  const canonicalPath = await realpath(folder.uri.fsPath);
  const label = await vscode.window.showInputBox({ prompt: "Fubun workspace label", value: folder.name });
  if (label === undefined || label.length === 0) return;
  const response = await client().request(
    "vscode.observation.enable",
    { label, absolute_path: canonicalPath },
    "vscode.observation.enabled",
  );
  const mapping = await workspaceMapping(response, canonicalPath);
  await context.globalState.update(resourceKey, mapping.resourceId);
  await context.globalState.update(scopeKey, mapping.scopeId);
  await context.globalState.update(hashKey, mapping.pathHash);
  await vscode.commands.executeCommand("setContext", "fubun.observing", true);
  await session().start();
  await sendWorkspaceEvent(mapping);
  await vscode.window.showInformationMessage("Fubun workspace observation enabled.");
}

async function workspaceMapping(response: unknown, canonicalPath: string): Promise<WorkspaceMapping> {
  const payload = record(response, ["resource", "scope"]);
  const resource = record(payload.resource, ["id", "kind", "label", "locator", "canonical_locator", "sensitivity", "scope", "created_at", "updated_at"]);
  const scope = record(payload.scope, ["id", "source", "resource_id", "status", "created_at", "updated_at"]);
  if (!isUuid(resource.id) || resource.kind !== "filesystem.directory" || !isUuid(scope.id)
    || scope.source !== "vscode.workspace" || scope.status !== "active" || scope.resource_id !== resource.id) {
    throw new Error("invalid VS Code observation response");
  }
  return {
    resourceId: resource.id,
    scopeId: scope.id,
    pathHash: await canonicalUrlHash(`file://${canonicalPath}`),
  };
}

async function sendWorkspaceEvent(mapping: WorkspaceMapping): Promise<void> {
  const now = Date.now();
  if (now - lastEventAt < 5000) return;
  lastEventAt = now;
  await session().emitWorkspaceOpened(mapping.resourceId);
}

async function emitIfRegistered(context: vscode.ExtensionContext): Promise<void> {
  const folder = currentWorkspace();
  if (folder === undefined) return;
  const resourceId = context.globalState.get<string>(resourceKey);
  const scopeId = context.globalState.get<string>(scopeKey);
  const pathHash = context.globalState.get<string>(hashKey);
  if (resourceId === undefined || scopeId === undefined || pathHash === undefined) return;
  try {
    const currentHash = await realpath(folder.uri.fsPath).then((path) => canonicalUrlHash(`file://${path}`));
    if (currentHash !== pathHash) return;
    await session().start();
    await sendWorkspaceEvent({ resourceId, scopeId, pathHash });
  } catch {
    // A non-canonicalizable or disconnected workspace is never observed.
  }
}

async function stop(context: vscode.ExtensionContext): Promise<void> {
  const scopeId = context.globalState.get<string>(scopeKey);
  adapterSession?.stop();
  await context.globalState.update(resourceKey, undefined);
  await context.globalState.update(scopeKey, undefined);
  await context.globalState.update(hashKey, undefined);
  await vscode.commands.executeCommand("setContext", "fubun.observing", false);
  if (scopeId !== undefined) {
    await client().request("observation.pause", { scope_id: scopeId }, "observation.paused");
  }
  await vscode.window.showInformationMessage("Fubun workspace observation paused.");
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

export function deactivate(): void {
  adapterSession?.stop();
  adapterSession = undefined;
}
