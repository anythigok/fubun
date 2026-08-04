import { PROTOCOL_VERSION } from "@fubun/protocol-ts";
import { SocketFactory, SocketTransport, UdsFrameDecoder, framed, parseCoreResponse } from "./uds.js";

interface PendingClientRequest {
  requestId: string;
  expectedKind: string;
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
}

export interface CoreClientOptions {
  socketPath: string;
  createTransport: SocketFactory;
  clientName?: string;
  clientVersion?: string;
  timeoutMs?: number;
}

export class CoreClient {
  private readonly timeoutMs: number;
  private readonly clientName: string;
  private readonly clientVersion: string;

  public constructor(private readonly options: CoreClientOptions) {
    this.timeoutMs = options.timeoutMs ?? 5_000;
    this.clientName = options.clientName ?? "fubun-vscode";
    this.clientVersion = options.clientVersion ?? "0.1.0";
  }

  public async request(method: string, params: unknown, expectedKind: string): Promise<unknown> {
    const transport = this.options.createTransport(this.options.socketPath);
    const decoder = new UdsFrameDecoder();
    const helloId = crypto.randomUUID();
    const requestId = crypto.randomUUID();
    return new Promise<unknown>((resolve, reject) => {
      let settled = false;
      let helloComplete = false;
      const finish = (error: Error | undefined, value?: unknown): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        transport.end();
        if (error !== undefined) reject(error);
        else resolve(value);
      };
      const timeout = setTimeout(() => finish(new Error("Core request timed out")), this.timeoutMs);
      const request: PendingClientRequest = {
        requestId,
        expectedKind,
        resolve: (value) => finish(undefined, value),
        reject: (error) => finish(error),
        timeout,
      };
      transport.onError((error) => finish(error));
      transport.onClose(() => finish(new Error("Core connection closed")));
      transport.onData((chunk) => {
        let frames: unknown[];
        try {
          frames = decoder.push(chunk);
        } catch (error) {
          finish(error instanceof Error ? error : new Error("invalid Core frame"));
          return;
        }
        for (const frame of frames) {
          try {
            const response = parseCoreResponse(frame);
            if (!helloComplete) {
              if (response.requestId !== helloId || response.payload.kind !== "client.hello_ack") {
                throw new Error("invalid Core hello acknowledgement");
              }
              helloComplete = true;
              transport.write(framed({
                protocol_version: PROTOCOL_VERSION,
                request_id: request.requestId,
                body: { method, params },
              }));
              continue;
            }
            if (response.requestId !== request.requestId || response.payload.kind !== request.expectedKind) {
              throw new Error("unexpected Core response");
            }
            request.resolve(response.payload.data);
          } catch (error) {
            request.reject(error instanceof Error ? error : new Error("invalid Core response"));
          }
        }
      });
      transport.onConnect(() => {
        transport.write(framed({
          protocol_version: PROTOCOL_VERSION,
          request_id: helloId,
          body: {
            method: "client.hello",
            params: {
              client_name: this.clientName,
              client_version: this.clientVersion,
              protocol_version: PROTOCOL_VERSION,
            },
          },
        }));
      });
    });
  }
}
