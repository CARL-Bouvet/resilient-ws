// resilient-ws.ts
//
// A small, dependency-free wrapper around the standard WebSocket API that
// implements one invariant: detect a silently-dead connection and reconnect
// with backoff, without ever leaking a timer.
//
// Public API (five lines):
//   const client = createResilientSocket({ url, subscribe, onMessage })
//   client.start()
//   client.stop()
//   client.on('reconnect' | 'stale' | 'open' | 'close' | 'error', handler)
//
// See the README for the incident history this module encodes.

/** Minimal shape of the standard WebSocket constructor this module needs.
 *  Accepting it as a parameter (rather than importing `WebSocket` directly)
 *  is what makes the module testable with a fake, and portable between
 *  Bun/Node's global `WebSocket` and any spec-compliant polyfill. */
export interface WebSocketLike {
  readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (ev: { data: unknown }) => void): void;
  addEventListener(type: "close", listener: (ev: { code: number; reason: string }) => void): void;
  addEventListener(type: "error", listener: (ev: unknown) => void): void;
}

export type WebSocketFactory = (url: string) => WebSocketLike;

// Standard WebSocket readyState values (avoids importing the `ws` package's
// enum just to read four constants).
const OPEN = 1;

export type ResilientSocketEvent =
  | "open" // transport open, subscribe payload just sent
  | "close" // transport closed, reconnect (or shutdown) decision about to be made
  | "error" // transport-level error event
  | "stale" // watchdog decided the peer is dead and is forcing a teardown
  | "reconnect"; // a reconnect attempt is about to start, with the delay used

export interface ResilientSocketOptions {
  /** Endpoint to connect to. Re-read on every reconnect (a plain string is
   *  fine for a fixed endpoint; pass a function if the URL can rotate, e.g.
   *  a token appended to it — the module never caches it). */
  url: string | (() => string | Promise<string>);

  /** Called once per successful `open`, to build the subscribe payload sent
   *  right away. Return `undefined`/`null` to send nothing (e.g. a protocol
   *  that requires waiting for a server "welcome" first — see `onMessage`,
   *  which can call `client.send(...)` itself once that arrives). */
  subscribe?: () => string | undefined | null;

  /** Called for every inbound message. Returning `true` (or nothing) counts
   *  the message as proof of life — the invariant found in both source
   *  implementations ("any accepted application payload is liveness", not
   *  only explicit pongs). Return `false` for messages that should NOT reset
   *  the staleness clock (rare — e.g. a message the caller considers
   *  malformed/rejected). */
  onMessage: (data: unknown) => boolean | void;

  /** Optional application-level ping. If provided, sent every `pingEveryMs`
   *  while the socket is OPEN. If omitted, the watchdog still fires on pure
   *  silence but never sends anything — supports exchanges that need no
   *  explicit client ping. */
  ping?: () => string;

  /** How often to send `ping()` (ms). Ignored if `ping` is not provided.
   *  Default 25_000, matching the source connectors' Kraken value. */
  pingEveryMs?: number;

  /** How long without ANY proof of life before the connection is presumed
   *  dead (ms). Must be > pingEveryMs so one slow round-trip can't cause a
   *  false kill. Default: pingEveryMs + 10_000 (the source's pong-timeout
   *  margin), or 30_000 if pingEveryMs is not set. */
  staleAfterMs?: number;

  /** How often the watchdog checks staleness (ms). Default 2_000 — matches
   *  `heartbeat.ts`'s poll cadence; independent of pingEveryMs/staleAfterMs,
   *  it only needs to be small enough that the real delay before detection
   *  is close to staleAfterMs. */
  watchdogTickMs?: number;

  /** Base delay for exponential backoff (ms). Default 1_000. */
  backoffBaseMs?: number;

  /** Backoff ceiling (ms). Default 30_000. */
  backoffMaxMs?: number;

  /** Consecutive failures allowed before falling back to the long cooldown.
   *  Default 5, matching both source connectors. */
  maxAttempts?: number;

  /** Cooldown applied once maxAttempts is exceeded, after which the attempt
   *  counter resets and normal backoff resumes. Default 300_000 (5 min). */
  cooldownMs?: number;

  /** Injectable WebSocket constructor — defaults to the ambient global.
   *  Overriding this is how tests substitute a fake server without touching
   *  a real socket. */
  createWebSocket?: WebSocketFactory;

  /** Injectable timer functions — defaults to the ambient globals. Tests
   *  override these with a fake clock to assert on virtual time instead of
   *  waiting on wall-clock backoff/staleness delays. */
  setTimeoutFn?: (fn: () => void, ms: number) => unknown;
  clearTimeoutFn?: (handle: unknown) => void;
  setIntervalFn?: (fn: () => void, ms: number) => unknown;
  clearIntervalFn?: (handle: unknown) => void;
  nowFn?: () => number;
}

type Listener = (payload?: unknown) => void;

export interface ResilientSocket {
  /** Opens the first connection. No-op if already started. */
  start(): void;
  /** Manual, permanent shutdown. Wins over any scheduled reconnect/watchdog —
   *  rule 8 of the invariant. Idempotent. */
  stop(): void;
  /** Sends raw data if the socket is currently OPEN; silently drops
   *  otherwise (the caller decides whether that matters). */
  send(data: string): boolean;
  on(event: ResilientSocketEvent, listener: Listener): void;
  off(event: ResilientSocketEvent, listener: Listener): void;
  /** Current attempt counter — exposed for tests/observability only. */
  readonly attempts: number;
}

export function createResilientSocket(opts: ResilientSocketOptions): ResilientSocket {
  const pingEveryMs = opts.pingEveryMs ?? 25_000;
  const staleAfterMs = opts.staleAfterMs ?? (opts.pingEveryMs ? pingEveryMs + 10_000 : 30_000);
  const watchdogTickMs = opts.watchdogTickMs ?? 2_000;
  const backoffBaseMs = opts.backoffBaseMs ?? 1_000;
  const backoffMaxMs = opts.backoffMaxMs ?? 30_000;
  const maxAttempts = opts.maxAttempts ?? 5;
  const cooldownMs = opts.cooldownMs ?? 300_000;

  const createWebSocket: WebSocketFactory =
    opts.createWebSocket ??
    ((url: string) => new (globalThis as unknown as { WebSocket: new (u: string) => WebSocketLike }).WebSocket(url));
  const setTimeoutFn = opts.setTimeoutFn ?? ((fn, ms) => setTimeout(fn, ms));
  const clearTimeoutFn = opts.clearTimeoutFn ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
  const setIntervalFn = opts.setIntervalFn ?? ((fn, ms) => setInterval(fn, ms));
  const clearIntervalFn = opts.clearIntervalFn ?? ((h) => clearInterval(h as ReturnType<typeof setInterval>));
  const nowFn = opts.nowFn ?? (() => Date.now());

  const listeners = new Map<ResilientSocketEvent, Set<Listener>>();
  const emit = (event: ResilientSocketEvent, payload?: unknown): void => {
    listeners.get(event)?.forEach((l) => l(payload));
  };

  let socket: WebSocketLike | null = null;
  let manuallyStopped = false;
  let attempts = 0;
  let lastSeenAt = 0;

  // Bounded, single-owner timer handles. Both are ALWAYS non-null XOR the
  // thing they represent is not running — invariant checked at every mutation
  // site below so a reconnect can never leave one of these dangling
  // ("orphaned timer" is exactly two of these handles disagreeing with
  // reality).
  let watchdogHandle: unknown = null;
  let reconnectHandle: unknown = null;

  const clearWatchdog = (): void => {
    if (watchdogHandle !== null) {
      clearIntervalFn(watchdogHandle);
      watchdogHandle = null;
    }
  };

  const clearReconnectTimer = (): void => {
    if (reconnectHandle !== null) {
      clearTimeoutFn(reconnectHandle);
      reconnectHandle = null;
    }
  };

  const markAlive = (): void => {
    lastSeenAt = nowFn();
  };

  const startWatchdog = (): void => {
    // Defensive: never stack two watchdogs on the same connection.
    clearWatchdog();
    let lastPingAt = nowFn();
    watchdogHandle = setIntervalFn(() => {
      const since = nowFn() - lastSeenAt;
      if (since > staleAfterMs) {
        // Rule 3: staleness kills the connection from HERE, and does nothing
        // else — no reconnect scheduling. That is the close handler's job,
        // unconditionally (rule 4), so a stale-kill and a network drop are
        // handled by exactly one code path instead of two.
        emit("stale", { since });
        // Clear BEFORE terminating: if terminate() synchronously re-enters
        // (some polyfills fire close synchronously), the close handler must
        // find no watchdog left to double-clear races against.
        clearWatchdog();
        forceClose();
        return;
      }
      if (opts.ping && nowFn() - lastPingAt >= pingEveryMs && socket && socket.readyState === OPEN) {
        try {
          socket.send(opts.ping());
          lastPingAt = nowFn();
        } catch {
          // A failed send here is not fatal on its own — the staleness
          // check above is what eventually kills a truly broken socket.
        }
      }
    }, watchdogTickMs);
  };

  // Force-teardown a suspected-dead socket. Uses close() with a distinct,
  // non-1000 code: the standard WebSocket has no `terminate()` (that is a
  // Node `ws`-library-only escape hatch used by the source implementations
  // to force a TCP-level abort past a half-open state). close() is the
  // closest portable equivalent; the source's own comment about close()
  // being a no-op off-OPEN applies mainly to the `ws` library's stricter
  // state machine — the standard API tolerates calling close() from any
  // state and still eventually fires `close`. Wrapped in try/catch because
  // a socket already mid-teardown can throw synchronously.
  const forceClose = (): void => {
    if (socket) {
      try {
        socket.close(4000, "stale connection");
      } catch {
        // ignored — the close handler is what matters, not this call
      }
    }
  };

  const scheduleReconnect = (): void => {
    attempts++;
    if (attempts > maxAttempts) {
      emit("reconnect", { delayMs: cooldownMs, cooldown: true, attempts });
      reconnectHandle = setTimeoutFn(() => {
        reconnectHandle = null;
        attempts = 0;
        connect();
      }, cooldownMs);
      return;
    }
    const delayMs = Math.min(backoffBaseMs * 2 ** (attempts - 1), backoffMaxMs);
    emit("reconnect", { delayMs, cooldown: false, attempts });
    reconnectHandle = setTimeoutFn(() => {
      reconnectHandle = null;
      connect();
    }, delayMs);
  };

  const connect = (): void => {
    // Rule 8: manual stop always wins, even over an already-scheduled retry
    // that fires after stop() was called.
    if (manuallyStopped) return;

    void (async () => {
      const url = typeof opts.url === "function" ? await opts.url() : opts.url;
      if (manuallyStopped) return; // stop() may have been called during the await

      let next: WebSocketLike;
      try {
        next = createWebSocket(url);
      } catch (err) {
        emit("error", err);
        scheduleReconnect();
        return;
      }
      socket = next;

      next.addEventListener("open", () => {
        // Rule 7: a connection that reaches OPEN resets the failure streak,
        // not merely one that has exchanged its first message — a socket
        // that opens then immediately goes silent is still a "successful
        // connect, unsuccessful session" and should not inherit backoff
        // penalties from a previous, unrelated failure to connect at all.
        attempts = 0;
        markAlive(); // seed: prevents the very first watchdog tick from
        // seeing lastSeenAt=0 and firing a false-positive stale kill before
        // any data had a chance to arrive.
        const payload = opts.subscribe?.();
        if (payload != null) {
          try {
            next.send(payload);
          } catch (err) {
            emit("error", err);
          }
        }
        startWatchdog();
        emit("open");
      });

      next.addEventListener("message", (ev) => {
        const countsAsAlive = opts.onMessage(ev.data);
        // Rule 1: any accepted application payload is proof of life, not
        // only an explicit protocol pong. `onMessage` returning `false` is
        // the caller's escape hatch for a message it considers rejected.
        if (countsAsAlive !== false) markAlive();
      });

      next.addEventListener("error", (err) => {
        emit("error", err);
      });

      next.addEventListener("close", () => {
        // Rule 5: every exit clears the watchdog, unconditionally and
        // idempotently — whether the watchdog itself already cleared it
        // (stale path) or this is the first clear (peer-initiated drop,
        // network error, or manual stop).
        clearWatchdog();
        socket = null;
        emit("close");

        if (manuallyStopped) return; // rule 8, again: no reconnect after stop()
        scheduleReconnect();
      });
    })();
  };

  return {
    start(): void {
      if (socket !== null || reconnectHandle !== null) return; // already running/scheduled
      manuallyStopped = false;
      attempts = 0;
      connect();
    },

    stop(): void {
      manuallyStopped = true;
      // Cancel a pending reconnect so a stop() during backoff can't be
      // silently overridden by a timer that fires seconds later.
      clearReconnectTimer();
      clearWatchdog();
      if (socket) {
        try {
          socket.close(1000, "manual stop");
        } catch {
          // ignored — we're shutting down anyway
        }
        socket = null;
      }
    },

    send(data: string): boolean {
      if (socket && socket.readyState === OPEN) {
        socket.send(data);
        return true;
      }
      return false;
    },

    on(event, listener): void {
      if (!listeners.has(event)) listeners.set(event, new Set());
      listeners.get(event)!.add(listener);
    },

    off(event, listener): void {
      listeners.get(event)?.delete(listener);
    },

    get attempts(): number {
      return attempts;
    },
  };
}
