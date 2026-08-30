import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createResilientSocket, type WebSocketLike } from "./resilient-ws";

// --- Fake WebSocket -----------------------------------------------------
// A local, in-memory stand-in for the standard WebSocket. Never touches a
// real socket or a real exchange. `close()` fires the `close` listener
// synchronously so tests don't need to juggle microtask flushing on top of
// vitest's fake timers.

const CONNECTING = 0;
const OPEN = 1;
const CLOSED = 3;

class FakeWebSocket implements WebSocketLike {
  static instances: FakeWebSocket[] = [];

  readyState = CONNECTING;
  sent: string[] = [];
  private listeners: Record<string, Array<(payload?: unknown) => void>> = {
    open: [],
    message: [],
    close: [],
    error: [],
  };

  constructor(public url: string) {
    FakeWebSocket.instances.push(this);
  }

  send(data: string): void {
    if (this.readyState !== OPEN) throw new Error("send on non-open socket");
    this.sent.push(data);
  }

  close(code?: number, reason?: string): void {
    if (this.readyState === CLOSED) return; // real sockets tolerate double-close; so must the fake
    this.readyState = CLOSED;
    this.fire("close", { code: code ?? 1000, reason: reason ?? "" });
  }

  // Overloads mirrored from WebSocketLike so the fake is a genuine
  // structural match, not just "close enough" — a strict-mode caller must
  // be able to pass this class wherever a real WebSocket is expected.
  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (ev: { data: unknown }) => void): void;
  addEventListener(type: "close", listener: (ev: { code: number; reason: string }) => void): void;
  addEventListener(type: "error", listener: (ev: unknown) => void): void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- bridges the
  // overloads above to one internal storage array; never exposed to callers.
  addEventListener(type: string, cb: (payload?: any) => void): void {
    this.listeners[type].push(cb);
  }

  fire(type: string, payload?: unknown): void {
    for (const l of [...this.listeners[type]]) l(payload);
  }

  // --- test-only helpers, not part of WebSocketLike ---
  simulateOpen(): void {
    this.readyState = OPEN;
    this.fire("open");
  }

  simulateMessage(data: unknown): void {
    this.fire("message", { data });
  }
}

const lastSocket = (): FakeWebSocket => {
  const s = FakeWebSocket.instances[FakeWebSocket.instances.length - 1];
  if (!s) throw new Error("no socket created yet");
  return s;
};

/** Flushes the microtask the module awaits before creating the socket
 *  (`await opts.url` inside `connect()`, even for a plain string). Fake
 *  timers don't advance microtasks on their own. */
const flush = async (): Promise<void> => {
  await vi.advanceTimersByTimeAsync(0);
};

beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("resilient-ws", () => {
  it("(a) silence while open triggers a reconnect", async () => {
    const client = createResilientSocket({
      url: "wss://fake.test/spot",
      onMessage: () => true,
      createWebSocket: (url) => new FakeWebSocket(url),
      staleAfterMs: 5_000,
      watchdogTickMs: 1_000,
      backoffBaseMs: 100,
      backoffMaxMs: 1_000,
    });

    const staleEvents: unknown[] = [];
    const reconnectEvents: unknown[] = [];
    client.on("stale", (p) => staleEvents.push(p));
    client.on("reconnect", (p) => reconnectEvents.push(p));

    client.start();
    await flush();
    expect(FakeWebSocket.instances).toHaveLength(1);
    lastSocket().simulateOpen();

    // Total silence: no message, no pong, past staleAfterMs.
    await vi.advanceTimersByTimeAsync(6_000);

    expect(staleEvents).toHaveLength(1);
    expect(lastSocket().readyState).toBe(CLOSED); // watchdog forced the teardown
    expect(reconnectEvents).toHaveLength(1);

    // Backoff elapses -> a fresh socket is created, proving reconnect actually happens.
    await vi.advanceTimersByTimeAsync(200);
    expect(FakeWebSocket.instances).toHaveLength(2);

    client.stop();
  });

  it("(b) inbound application data resets the staleness timer", async () => {
    const client = createResilientSocket({
      url: "wss://fake.test/spot",
      onMessage: () => true,
      createWebSocket: (url) => new FakeWebSocket(url),
      staleAfterMs: 5_000,
      watchdogTickMs: 1_000,
    });

    const staleEvents: unknown[] = [];
    client.on("stale", (p) => staleEvents.push(p));

    client.start();
    await flush();
    lastSocket().simulateOpen();

    // Feed a message every 3s (< staleAfterMs) for 4 rounds = 12s total,
    // well past staleAfterMs if the clock were never reset.
    for (let i = 0; i < 4; i++) {
      await vi.advanceTimersByTimeAsync(3_000);
      lastSocket().simulateMessage(JSON.stringify({ channel: "trade", data: [] }));
    }
    expect(staleEvents).toHaveLength(0);

    // Now go silent for real -> the watchdog must still catch it.
    await vi.advanceTimersByTimeAsync(6_000);
    expect(staleEvents).toHaveLength(1);

    client.stop();
  });

  it("(c) a reconnect leaves no orphan timer running", async () => {
    const client = createResilientSocket({
      url: "wss://fake.test/spot",
      onMessage: () => true,
      createWebSocket: (url) => new FakeWebSocket(url),
      staleAfterMs: 5_000,
      watchdogTickMs: 1_000,
      backoffBaseMs: 100,
      backoffMaxMs: 1_000,
    });

    client.start();
    await flush();
    lastSocket().simulateOpen();
    // Exactly one running timer: the watchdog interval.
    expect(vi.getTimerCount()).toBe(1);

    // Drive it stale -> watchdog clears itself, close handler schedules a
    // reconnect timeout. Still exactly one timer, never two.
    await vi.advanceTimersByTimeAsync(6_000);
    expect(vi.getTimerCount()).toBe(1);

    // Reconnect fires -> new socket -> new watchdog once it opens. Still one.
    await vi.advanceTimersByTimeAsync(200);
    expect(FakeWebSocket.instances).toHaveLength(2);
    lastSocket().simulateOpen();
    expect(vi.getTimerCount()).toBe(1);

    client.stop();
    // stop() tears down the watchdog and cancels any scheduled reconnect.
    expect(vi.getTimerCount()).toBe(0);
  });

  it("manual stop() cancels a scheduled reconnect (no timer survives shutdown)", async () => {
    const client = createResilientSocket({
      url: "wss://fake.test/spot",
      onMessage: () => true,
      createWebSocket: (url) => new FakeWebSocket(url),
      staleAfterMs: 5_000,
      watchdogTickMs: 1_000,
      backoffBaseMs: 1_000,
    });

    client.start();
    await flush();
    lastSocket().simulateOpen();
    lastSocket().close(1006, "network drop"); // peer-initiated drop, not the watchdog

    // A reconnect is now scheduled (backoff timer pending).
    expect(vi.getTimerCount()).toBe(1);

    client.stop();
    expect(vi.getTimerCount()).toBe(0);

    // Even if time passes, no new socket must appear.
    await vi.advanceTimersByTimeAsync(10_000);
    expect(FakeWebSocket.instances).toHaveLength(1);
  });
});
