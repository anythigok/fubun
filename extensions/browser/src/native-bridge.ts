import {
  MAX_MESSAGE_SIZE,
  NativeEnvelope,
  PROTOCOL_VERSION,
  parseNativeEnvelope,
} from "@fubun/protocol-ts";

export interface NativePort {
  postMessage(message: unknown): void;
  disconnect(): void;
  onMessage: { addListener(listener: (message: unknown) => void): void };
  onDisconnect: { addListener(listener: () => void): void };
}

export interface NativeBridgeOptions {
  createPort: () => NativePort;
  createHello: () => Promise<NativeEnvelope<unknown>>;
  onActionExecute: (message: NativeEnvelope<unknown>) => Promise<void>;
  requestTimeoutMs?: number;
  maxPending?: number;
  setTimer?: (callback: () => void, delay: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
}

interface PendingNativeRequest {
  expectedType: string;
  resolve: (payload: unknown) => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
}

export class BrowserNativeBridge {
  private readonly pending = new Map<string, PendingNativeRequest>();
  private readonly requestTimeoutMs: number;
  private readonly maxPending: number;
  private readonly setTimer: (callback: () => void, delay: number) => ReturnType<typeof setTimeout>;
  private readonly clearTimer: (timer: ReturnType<typeof setTimeout>) => void;
  private port: NativePort | undefined;
  private connecting: Promise<void> | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnectDelay = 250;
  private desired = false;
  private ready = false;

  public constructor(private readonly options: NativeBridgeOptions) {
    this.requestTimeoutMs = options.requestTimeoutMs ?? 10_000;
    this.maxPending = options.maxPending ?? 64;
    this.setTimer = options.setTimer ?? ((callback, delay) => setTimeout(callback, delay));
    this.clearTimer = options.clearTimer ?? ((timer) => clearTimeout(timer));
  }

  public isReady(): boolean {
    return this.ready;
  }

  public pendingCount(): number {
    return this.pending.size;
  }

  public async start(): Promise<void> {
    this.desired = true;
    await this.ensureConnected();
  }

  public async reconnect(): Promise<void> {
    this.desired = true;
    this.cancelReconnect();
    this.closeCurrentPort(new Error("native host connection replaced"));
    await this.ensureConnected();
  }

  public stop(): void {
    this.desired = false;
    this.cancelReconnect();
    this.closeCurrentPort(new Error("native host connection stopped"));
  }

  public async request(type: string, payload: unknown, expectedType: string): Promise<unknown> {
    return this.requestPrepared(type, expectedType, () => payload);
  }

  public async requestPrepared(
    type: string,
    expectedType: string,
    payloadFactory: () => unknown,
  ): Promise<unknown> {
    const port = await this.ensureConnected();
    const envelope: NativeEnvelope<unknown> = {
      protocol_version: PROTOCOL_VERSION,
      request_id: crypto.randomUUID(),
      type,
      payload: payloadFactory(),
    };
    return this.requestOnPort(port, envelope, expectedType);
  }

  public async notify(type: string, requestId: string, payload: unknown): Promise<void> {
    const port = await this.ensureConnected();
    const envelope: NativeEnvelope<unknown> = {
      protocol_version: PROTOCOL_VERSION,
      request_id: requestId,
      type,
      payload,
    };
    this.assertMessageSize(envelope);
    port.postMessage(envelope);
  }

  private async ensureConnected(): Promise<NativePort> {
    if (this.port !== undefined && this.ready) return this.port;
    if (this.connecting !== undefined) {
      await this.connecting;
      if (this.port === undefined || !this.ready) throw new Error("native host is unavailable");
      return this.port;
    }
    const connecting = this.establish();
    this.connecting = connecting;
    try {
      await connecting;
    } finally {
      if (this.connecting === connecting) this.connecting = undefined;
    }
    if (this.port === undefined || !this.ready) throw new Error("native host is unavailable");
    return this.port;
  }

  private async establish(): Promise<void> {
    let port: NativePort;
    try {
      port = this.options.createPort();
    } catch {
      this.scheduleReconnect();
      throw new Error("native host connection failed");
    }
    this.port = port;
    port.onMessage.addListener((message) => this.handleMessage(port, message));
    port.onDisconnect.addListener(() => this.handleDisconnect(port));
    try {
      const hello = await this.options.createHello();
      await this.requestOnPort(port, hello, "extension.hello.ack");
      if (this.port !== port) throw new Error("native host connection changed");
      this.ready = true;
      this.reconnectDelay = 250;
    } catch (error) {
      if (this.port === port) {
        this.closeCurrentPort(error instanceof Error ? error : new Error("native host hello failed"));
        this.scheduleReconnect();
      }
      throw error;
    }
  }

  private requestOnPort(port: NativePort, envelope: NativeEnvelope<unknown>, expectedType: string): Promise<unknown> {
    this.assertMessageSize(envelope);
    if (this.pending.size >= this.maxPending) return Promise.reject(new Error("too many native requests"));
    return new Promise<unknown>((resolve, reject) => {
      const timeout = this.setTimer(() => {
        const request = this.pending.get(envelope.request_id);
        if (request === undefined) return;
        this.pending.delete(envelope.request_id);
        request.reject(new Error("native request timed out"));
      }, this.requestTimeoutMs);
      this.pending.set(envelope.request_id, { expectedType, resolve, reject, timeout });
      try {
        port.postMessage(envelope);
      } catch {
        const request = this.pending.get(envelope.request_id);
        if (request !== undefined) {
          this.pending.delete(envelope.request_id);
          this.clearTimer(request.timeout);
          request.reject(new Error("native host write failed"));
        }
      }
    });
  }

  private handleMessage(port: NativePort, rawMessage: unknown): void {
    if (this.port !== port) return;
    let message: NativeEnvelope<unknown>;
    try {
      message = parseNativeEnvelope(rawMessage);
    } catch {
      return;
    }
    if (message.type === "browser.action.execute") {
      void this.options.onActionExecute(message).catch(() => undefined);
      return;
    }
    const pending = this.pending.get(message.request_id);
    if (pending === undefined) return;
    this.pending.delete(message.request_id);
    this.clearTimer(pending.timeout);
    if (message.type === "integration.error") {
      pending.reject(new Error("native host rejected request"));
      return;
    }
    if (message.type !== pending.expectedType) {
      pending.reject(new Error("native response type mismatch"));
      return;
    }
    pending.resolve(message.payload);
  }

  private handleDisconnect(port: NativePort): void {
    if (this.port !== port) return;
    this.port = undefined;
    this.ready = false;
    this.rejectAll(new Error("native host disconnected"));
    this.scheduleReconnect();
  }

  private closeCurrentPort(error: Error): void {
    const port = this.port;
    this.port = undefined;
    this.ready = false;
    this.rejectAll(error);
    if (port !== undefined) {
      try {
        port.disconnect();
      } catch {
        // A disconnected native port has already completed the same cleanup.
      }
    }
  }

  private rejectAll(error: Error): void {
    for (const [requestId, pending] of this.pending) {
      this.pending.delete(requestId);
      this.clearTimer(pending.timeout);
      pending.reject(error);
    }
  }

  private scheduleReconnect(): void {
    if (!this.desired || this.reconnectTimer !== undefined) return;
    const delay = this.reconnectDelay;
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30_000);
    this.reconnectTimer = this.setTimer(() => {
      this.reconnectTimer = undefined;
      if (!this.desired || this.port !== undefined || this.connecting !== undefined) return;
      void this.ensureConnected().catch(() => undefined);
    }, delay);
  }

  private cancelReconnect(): void {
    if (this.reconnectTimer !== undefined) {
      this.clearTimer(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
  }

  private assertMessageSize(message: NativeEnvelope<unknown>): void {
    const size = new TextEncoder().encode(JSON.stringify(message)).byteLength;
    if (size === 0 || size > MAX_MESSAGE_SIZE) throw new Error("native message too large");
  }
}
