import { MAX_MESSAGE_SIZE, PROTOCOL_VERSION, encodeUdsFrame, isUuid, rejectUnknownKeys } from "@fubun/protocol-ts";
import { Socket, connect } from "node:net";

export interface SocketTransport {
  write(data: Uint8Array): void;
  end(): void;
  onConnect(listener: () => void): void;
  onData(listener: (chunk: Uint8Array) => void): void;
  onClose(listener: () => void): void;
  onError(listener: (error: Error) => void): void;
}

export type SocketFactory = (path: string) => SocketTransport;

export function nodeSocketFactory(path: string): SocketTransport {
  const socket = connect(path);
  return transportFromSocket(socket);
}

export function transportFromSocket(socket: Socket): SocketTransport {
  return {
    write(data: Uint8Array): void {
      socket.write(Buffer.from(data));
    },
    end(): void {
      socket.end();
    },
    onConnect(listener: () => void): void {
      socket.on("connect", listener);
    },
    onData(listener: (chunk: Uint8Array) => void): void {
      socket.on("data", (chunk: Buffer) => listener(new Uint8Array(chunk)));
    },
    onClose(listener: () => void): void {
      socket.on("close", listener);
    },
    onError(listener: (error: Error) => void): void {
      socket.on("error", listener);
    },
  };
}

export class UdsFrameDecoder {
  private buffer = Buffer.alloc(0);

  public push(chunk: Uint8Array): unknown[] {
    this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)]);
    const values: unknown[] = [];
    while (this.buffer.length >= 4) {
      const length = this.buffer.readUInt32LE(0);
      if (length === 0 || length > MAX_MESSAGE_SIZE) throw new Error("invalid UDS frame length");
      if (this.buffer.length < length + 4) break;
      const payload = this.buffer.subarray(4, length + 4);
      this.buffer = this.buffer.subarray(length + 4);
      const text = new TextDecoder("utf-8", { fatal: true }).decode(payload);
      values.push(JSON.parse(text) as unknown);
    }
    return values;
  }
}

export function framed(value: unknown): Uint8Array {
  return encodeUdsFrame(value);
}

export interface CorePayload {
  kind: string;
  data: unknown;
}

export function parseCoreResponse(value: unknown): { requestId: string; payload: CorePayload } {
  const envelope = record(value, ["protocol_version", "request_id", "body"]);
  assertProtocol(envelope.protocol_version);
  if (!isUuid(envelope.request_id)) throw new Error("invalid Core request ID");
  const body = record(envelope.body, ["status", "payload"]);
  if (body.status === "error") {
    const error = record(body.payload, ["code", "message"]);
    if (typeof error.code !== "string" || typeof error.message !== "string") throw new Error("invalid Core error");
    throw new Error("Core rejected request");
  }
  if (body.status !== "ok") throw new Error("invalid Core response status");
  const payload = record(body.payload, ["kind", "data"]);
  if (typeof payload.kind !== "string") throw new Error("invalid Core payload kind");
  return { requestId: envelope.request_id, payload: { kind: payload.kind, data: payload.data } };
}

export function parseAdapterEventAck(value: unknown): { requestId: string; stored: boolean; duplicate: boolean } {
  const envelope = record(value, ["protocol_version", "request_id", "action_execution_id", "body"]);
  assertProtocol(envelope.protocol_version);
  if (!isUuid(envelope.request_id) || !isUuid(envelope.action_execution_id)) throw new Error("invalid adapter request ID");
  const body = record(envelope.body, ["method", "params"]);
  if (body.method !== "event.ack") throw new Error("unexpected adapter request");
  const ack = record(body.params, ["event_id", "stored", "duplicate"]);
  if (!isUuid(ack.event_id) || typeof ack.stored !== "boolean" || typeof ack.duplicate !== "boolean") {
    throw new Error("invalid adapter event acknowledgement");
  }
  return { requestId: envelope.request_id, stored: ack.stored, duplicate: ack.duplicate };
}

export function coreSocketPath(environment: NodeJS.ProcessEnv = process.env): string {
  const runtime = environment.XDG_RUNTIME_DIR;
  if (runtime === undefined || runtime.length === 0) throw new Error("XDG_RUNTIME_DIR is required");
  return `${runtime}/fubun/core.sock`;
}

export function record(value: unknown, allowed: readonly string[]): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("invalid protocol object");
  const result = value as Record<string, unknown>;
  rejectUnknownKeys(result, allowed);
  return result;
}

function assertProtocol(value: unknown): void {
  const version = record(value, ["major", "minor"]);
  if (version.major !== PROTOCOL_VERSION.major || version.minor !== PROTOCOL_VERSION.minor) {
    throw new Error("protocol mismatch");
  }
}
