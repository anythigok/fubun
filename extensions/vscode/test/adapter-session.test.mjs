import assert from "node:assert/strict";
import test from "node:test";
import { VscodeAdapterSession } from "../dist/adapter-session.js";
import { CoreClient } from "../dist/core-client.js";

const PROTOCOL_VERSION = { major: 1, minor: 0 };
const resourceId = "11111111-1111-4111-8111-111111111111";

function frame(value) {
  const payload = Buffer.from(JSON.stringify(value));
  const header = Buffer.alloc(4);
  header.writeUInt32LE(payload.length, 0);
  return Buffer.concat([header, payload]);
}

function decodeFrame(bytes) {
  const buffer = Buffer.from(bytes);
  return JSON.parse(buffer.subarray(4, buffer.readUInt32LE(0) + 4).toString("utf8"));
}

function coreOk(requestId, kind, data = {}) {
  return {
    protocol_version: PROTOCOL_VERSION,
    request_id: requestId,
    body: { status: "ok", payload: { kind, data } },
  };
}

class FakeTransport {
  constructor() { this.writes = []; this.connectListeners = []; this.dataListeners = []; this.closeListeners = []; this.errorListeners = []; this.ended = false; }
  write(data) { this.writes.push(data); }
  end() { this.ended = true; }
  onConnect(listener) { this.connectListeners.push(listener); }
  onData(listener) { this.dataListeners.push(listener); }
  onClose(listener) { this.closeListeners.push(listener); }
  onError(listener) { this.errorListeners.push(listener); }
  connect() { for (const listener of this.connectListeners) listener(); }
  emit(value, split = false) {
    const bytes = frame(value);
    if (split) {
      for (const listener of this.dataListeners) listener(bytes.subarray(0, 3));
      for (const listener of this.dataListeners) listener(bytes.subarray(3));
      return;
    }
    for (const listener of this.dataListeners) listener(bytes);
  }
  close() { for (const listener of this.closeListeners) listener(); }
}

function ids() {
  let value = 1;
  return () => `00000000-0000-4000-8000-${String(value++).padStart(12, "0")}`;
}

test("event-only adapter reuses a stable instance and monotonic sequence", async () => {
  const transports = [];
  const session = new VscodeAdapterSession({
    socketPath: "/tmp/fubun/core.sock",
    createTransport: () => { const transport = new FakeTransport(); transports.push(transport); return transport; },
    uuid: ids(),
  });
  const start = session.start();
  const transport = transports[0];
  transport.connect();
  const hello = decodeFrame(transport.writes[0]);
  transport.emit(coreOk(hello.request_id, "adapter.hello_ack"), true);
  await start;
  const first = session.emitWorkspaceOpened(resourceId);
  await new Promise((resolve) => setImmediate(resolve));
  const firstEvent = decodeFrame(transport.writes[1]);
  assert.equal(firstEvent.body.data.sequence_no, 1);
  assert.equal(hello.body.params.instance_id, session.instanceIdValue());
  transport.emit({
    protocol_version: PROTOCOL_VERSION,
    request_id: firstEvent.request_id,
    action_execution_id: "00000000-0000-0000-0000-000000000000",
    body: { method: "event.ack", params: { event_id: resourceId, stored: true, duplicate: false } },
  });
  await first;
  const second = session.emitWorkspaceOpened(resourceId);
  await new Promise((resolve) => setImmediate(resolve));
  const secondEvent = decodeFrame(transport.writes[2]);
  assert.equal(secondEvent.body.data.sequence_no, 2);
  assert.notEqual(firstEvent.request_id, secondEvent.request_id);
  transport.emit({
    protocol_version: PROTOCOL_VERSION,
    request_id: secondEvent.request_id,
    action_execution_id: "00000000-0000-0000-0000-000000000000",
    body: { method: "event.ack", params: { event_id: resourceId, stored: true, duplicate: false } },
  });
  await second;
  assert.equal(transports.length, 1);
  assert.equal(session.pendingCount(), 0);
  session.stop();
});

test("adapter rejects mismatched hello acknowledgement and releases pending work", async () => {
  const transport = new FakeTransport();
  const session = new VscodeAdapterSession({ socketPath: "/tmp/fubun/core.sock", createTransport: () => transport, uuid: ids() });
  const start = session.start();
  transport.connect();
  const hello = decodeFrame(transport.writes[0]);
  transport.emit(coreOk("99999999-9999-4999-8999-999999999999", "adapter.hello_ack"));
  await assert.rejects(start, /invalid VS Code adapter hello acknowledgement/);
  assert.equal(session.isConnected(), false);
  assert.equal(session.pendingCount(), 0);
  session.stop();
  await assert.rejects(session.emitWorkspaceOpened(resourceId), /inactive/);
  assert.equal(hello.body.params.adapter_id, "dev.fubun.vscode");
});

test("adapter rejects a protocol-mismatched hello acknowledgement", async () => {
  const transport = new FakeTransport();
  const session = new VscodeAdapterSession({ socketPath: "/tmp/fubun/core.sock", createTransport: () => transport, uuid: ids() });
  const start = session.start();
  transport.connect();
  const hello = decodeFrame(transport.writes[0]);
  transport.emit({ ...coreOk(hello.request_id, "adapter.hello_ack"), protocol_version: { major: 2, minor: 0 } });
  await assert.rejects(start, /protocol mismatch/);
  assert.equal(session.isConnected(), false);
  session.stop();
});

test("adapter disconnect before hello acknowledgement rejects the persistent session without hanging", async () => {
  const transport = new FakeTransport();
  const session = new VscodeAdapterSession({ socketPath: "/tmp/fubun/core.sock", createTransport: () => transport, uuid: ids() });
  const start = session.start();
  transport.close();
  await assert.rejects(start, /disconnected/);
  assert.equal(session.isConnected(), false);
  assert.equal(session.pendingCount(), 0);
  session.stop();
});

test("Core client validates hello and response request IDs instead of skipping frames", async () => {
  const transport = new FakeTransport();
  const client = new CoreClient({ socketPath: "/tmp/fubun/core.sock", createTransport: () => transport });
  const request = client.request("observation.pause", { scope_id: resourceId }, "observation.paused");
  transport.connect();
  const hello = decodeFrame(transport.writes[0]);
  transport.emit(coreOk(hello.request_id, "client.hello_ack"));
  const body = decodeFrame(transport.writes[1]);
  transport.emit(coreOk("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "observation.paused"));
  await assert.rejects(request, /unexpected Core response/);
  assert.equal(body.body.method, "observation.pause");
});

test("adapter event payload contains only the registered resource identifier", async () => {
  const transports = [];
  const session = new VscodeAdapterSession({
    socketPath: "/tmp/fubun/core.sock",
    createTransport: () => { const transport = new FakeTransport(); transports.push(transport); return transport; },
    uuid: ids(),
  });
  const start = session.start();
  const transport = transports[0];
  transport.connect();
  const hello = decodeFrame(transport.writes[0]);
  transport.emit(coreOk(hello.request_id, "adapter.hello_ack"));
  await start;
  const emitted = session.emitWorkspaceOpened(resourceId);
  await new Promise((resolve) => setImmediate(resolve));
  const event = decodeFrame(transport.writes[1]);
  assert.deepEqual(Object.keys(event.body.data).sort(), ["occurred_at", "resource_id", "sequence_no", "type"]);
  transport.emit({
    protocol_version: PROTOCOL_VERSION,
    request_id: event.request_id,
    action_execution_id: "00000000-0000-0000-0000-000000000000",
    body: { method: "event.ack", params: { event_id: resourceId, stored: true, duplicate: false } },
  });
  await emitted;
  session.stop();
});

test("adapter disconnect releases an in-flight event acknowledgement", async () => {
  const transport = new FakeTransport();
  const session = new VscodeAdapterSession({ socketPath: "/tmp/fubun/core.sock", createTransport: () => transport, uuid: ids() });
  const start = session.start();
  transport.connect();
  const hello = decodeFrame(transport.writes[0]);
  transport.emit(coreOk(hello.request_id, "adapter.hello_ack"));
  await start;
  const emitted = session.emitWorkspaceOpened(resourceId);
  await new Promise((resolve) => setImmediate(resolve));
  transport.close();
  await assert.rejects(emitted, /disconnected/);
  assert.equal(session.pendingCount(), 0);
  session.stop();
});
