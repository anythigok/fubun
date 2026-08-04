import { PROTOCOL_VERSION } from "@fubun/protocol-ts";
import {
  SocketFactory,
  SocketTransport,
  UdsFrameDecoder,
  framed,
  parseAdapterEventAck,
  parseCoreResponse,
} from "./uds.js";

interface PendingEvent {
  resolve: () => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
}

interface PendingHello {
  requestId: string;
  resolve: () => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
}

export interface VscodeAdapterSessionOptions {
  socketPath: string;
  createTransport: SocketFactory;
  uuid?: () => string;
  now?: () => number;
  timeoutMs?: number;
  setTimer?: (callback: () => void, delay: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
}

export class VscodeAdapterSession {
  private readonly uuid: () => string;
  private readonly now: () => number;
  private readonly timeoutMs: number;
  private readonly setTimer: (callback: () => void, delay: number) => ReturnType<typeof setTimeout>;
  private readonly clearTimer: (timer: ReturnType<typeof setTimeout>) => void;
  private readonly instanceId: string;
  private readonly pending = new Map<string, PendingEvent>();
  private transport: SocketTransport | undefined;
  private decoder = new UdsFrameDecoder();
  private connecting: Promise<void> | undefined;
  private hello: PendingHello | undefined;
  private desired = false;
  private connected = false;
  private nextSequence = 1;
  private eventChain: Promise<void> = Promise.resolve();
  private reconnectDelay = 250;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;

  public constructor(private readonly options: VscodeAdapterSessionOptions) {
    this.uuid = options.uuid ?? (() => crypto.randomUUID());
    this.now = options.now ?? (() => Date.now());
    this.timeoutMs = options.timeoutMs ?? 5_000;
    this.setTimer = options.setTimer ?? ((callback, delay) => setTimeout(callback, delay));
    this.clearTimer = options.clearTimer ?? ((timer) => clearTimeout(timer));
    this.instanceId = this.uuid();
  }

  public instanceIdValue(): string {
    return this.instanceId;
  }

  public isConnected(): boolean {
    return this.connected;
  }

  public pendingCount(): number {
    return this.pending.size;
  }

  public async start(): Promise<void> {
    this.desired = true;
    await this.ensureConnected();
  }

  public stop(): void {
    this.desired = false;
    if (this.reconnectTimer !== undefined) {
      this.clearTimer(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.closeCurrent(new Error("VS Code adapter stopped"));
  }

  public async emitWorkspaceOpened(resourceId: string): Promise<void> {
    const sequenceNo = this.nextSequence;
    this.nextSequence += 1;
    const occurredAt = new Date(this.now()).toISOString();
    const queued = this.eventChain.then(() => this.emitOne(resourceId, sequenceNo, occurredAt));
    this.eventChain = queued.catch(() => undefined);
    await queued;
  }

  private async emitOne(resourceId: string, sequenceNo: number, occurredAt: string): Promise<void> {
    const transport = await this.ensureConnected();
    const requestId = this.uuid();
    await new Promise<void>((resolve, reject) => {
      const timeout = this.setTimer(() => {
        const pending = this.pending.get(requestId);
        if (pending === undefined) return;
        this.pending.delete(requestId);
        pending.reject(new Error("VS Code event acknowledgement timed out"));
      }, this.timeoutMs);
      this.pending.set(requestId, { resolve, reject, timeout });
      try {
        transport.write(framed({
          protocol_version: PROTOCOL_VERSION,
          request_id: requestId,
          action_execution_id: "00000000-0000-0000-0000-000000000000",
          body: {
            kind: "event.emit",
            data: {
              sequence_no: sequenceNo,
              occurred_at: occurredAt,
              type: "dev.fubun.vscode.workspace.opened.v1",
              resource_id: resourceId,
            },
          },
        }));
      } catch {
        const pending = this.pending.get(requestId);
        if (pending !== undefined) {
          this.pending.delete(requestId);
          this.clearTimer(pending.timeout);
          pending.reject(new Error("VS Code event write failed"));
        }
      }
    });
  }

  private async ensureConnected(): Promise<SocketTransport> {
    if (!this.desired) throw new Error("VS Code observation is inactive");
    if (this.transport !== undefined && this.connected) return this.transport;
    if (this.connecting !== undefined) {
      await this.connecting;
      if (this.transport === undefined || !this.connected) throw new Error("VS Code adapter unavailable");
      return this.transport;
    }
    const connecting = this.establish();
    this.connecting = connecting;
    try {
      await connecting;
    } finally {
      if (this.connecting === connecting) this.connecting = undefined;
    }
    if (this.transport === undefined || !this.connected) throw new Error("VS Code adapter unavailable");
    return this.transport;
  }

  private async establish(): Promise<void> {
    let transport: SocketTransport;
    try {
      transport = this.options.createTransport(this.options.socketPath);
    } catch {
      this.scheduleReconnect();
      throw new Error("VS Code adapter connection failed");
    }
    this.transport = transport;
    this.decoder = new UdsFrameDecoder();
    transport.onData((chunk) => this.handleData(transport, chunk));
    transport.onError(() => this.handleClose(transport));
    transport.onClose(() => this.handleClose(transport));
    return new Promise<void>((resolve, reject) => {
      const requestId = this.uuid();
      const timeout = this.setTimer(() => {
        const hello = this.hello;
        if (hello === undefined || hello.requestId !== requestId) return;
        this.hello = undefined;
        hello.reject(new Error("VS Code adapter hello timed out"));
      }, this.timeoutMs);
      this.hello = { requestId, resolve, reject, timeout };
      transport.onConnect(() => {
        if (this.transport !== transport || this.hello?.requestId !== requestId) return;
        try {
          transport.write(framed({
            protocol_version: PROTOCOL_VERSION,
            request_id: requestId,
            body: {
              method: "adapter.hello",
              params: {
                adapter_id: "dev.fubun.vscode",
                adapter_version: "0.1.0",
                instance_id: this.instanceId,
                action_capabilities: [],
                event_capabilities: ["dev.fubun.vscode.workspace.opened.v1"],
                status: { tools: [], desktop_entry_ids: [], permitted_resource_ids: [] },
              },
            },
          }));
        } catch {
          this.handleClose(transport);
        }
      });
    }).then(() => {
      this.connected = true;
      this.reconnectDelay = 250;
    }).catch((error: unknown) => {
      this.closeCurrent(error instanceof Error ? error : new Error("VS Code adapter hello failed"));
      this.scheduleReconnect();
      throw error;
    });
  }

  private handleData(transport: SocketTransport, chunk: Uint8Array): void {
    if (this.transport !== transport) return;
    let frames: unknown[];
    try {
      frames = this.decoder.push(chunk);
    } catch {
      this.handleClose(transport);
      return;
    }
    for (const frame of frames) {
      if (this.hello !== undefined) {
        try {
          const response = parseCoreResponse(frame);
          if (response.requestId !== this.hello.requestId || response.payload.kind !== "adapter.hello_ack") {
            throw new Error("invalid VS Code adapter hello acknowledgement");
          }
          const hello = this.hello;
          this.hello = undefined;
          this.clearTimer(hello.timeout);
          hello.resolve();
        } catch (error) {
          const hello = this.hello;
          this.hello = undefined;
          if (hello !== undefined) this.clearTimer(hello.timeout);
          hello?.reject(error instanceof Error ? error : new Error("invalid adapter hello response"));
        }
        continue;
      }
      try {
        const ack = parseAdapterEventAck(frame);
        const pending = this.pending.get(ack.requestId);
        if (pending === undefined) continue;
        this.pending.delete(ack.requestId);
        this.clearTimer(pending.timeout);
        pending.resolve();
      } catch {
        this.handleClose(transport);
        return;
      }
    }
  }

  private handleClose(transport: SocketTransport): void {
    if (this.transport !== transport) return;
    this.closeCurrent(new Error("VS Code adapter disconnected"));
    this.scheduleReconnect();
  }

  private closeCurrent(error: Error): void {
    const transport = this.transport;
    this.transport = undefined;
    this.connected = false;
    const hello = this.hello;
    this.hello = undefined;
    if (hello !== undefined) {
      this.clearTimer(hello.timeout);
      hello.reject(error);
    }
    for (const [requestId, pending] of this.pending) {
      this.pending.delete(requestId);
      this.clearTimer(pending.timeout);
      pending.reject(error);
    }
    if (transport !== undefined) {
      try {
        transport.end();
      } catch {
        // The connection is already closed.
      }
    }
  }

  private scheduleReconnect(): void {
    if (!this.desired || this.reconnectTimer !== undefined) return;
    const delay = this.reconnectDelay;
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30_000);
    this.reconnectTimer = this.setTimer(() => {
      this.reconnectTimer = undefined;
      if (!this.desired || this.transport !== undefined || this.connecting !== undefined) return;
      void this.ensureConnected().catch(() => undefined);
    }, delay);
  }
}
