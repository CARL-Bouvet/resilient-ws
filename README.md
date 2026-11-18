# resilient-ws

A dependency-free WebSocket wrapper that treats **incoming data** as the proof of life, so a socket
that stays open but goes silent is detected, torn down, and reconnected — without ever leaving a
timer behind.

One file, 366 lines, no dependencies, TypeScript, runs anywhere the standard `WebSocket` global
exists — Bun, Node 22+, Deno, the browser.

*If you are here, you have probably already lost an afternoon to a service that was up and doing
nothing. This is the part of the job nobody demos, and it is not optional once you operate what you
build.*

---

## The problem

A WebSocket does not always fail by closing. It fails by going quiet.

The socket's `readyState` is still `OPEN`. The TCP connection is still established. No `close`
event fires, no `error` event fires, and every reconnection strategy you wrote is waiting for one
of those two events that will never come. Meanwhile your application believes it is ingesting a
live feed, and it is ingesting nothing.

I ran into this on a market-data service that holds a dozen exchange WebSocket connections open
continuously. The failure is unremarkable when you see it in a log and expensive when you don't:
prices stop updating, no alarm fires, and the gap is only visible later, in the data.

The causes are boring and well documented — a NAT table entry evicted, a proxy's idle timeout, a
blocked event loop — and none of them produce a `close` event. That is the whole difficulty:
**the connection is not broken, it is abandoned.** The transport has no opinion about whether the
peer is still sending you anything, so you have to form one yourself.

## Why the usual heartbeat is not enough

The standard answer is ping/pong: send a ping, expect a pong, kill the connection if the pong does
not come. This is real and it works — the [`ws` heartbeat
pattern](https://github.com/websockets/ws/blob/master/README.md) is the canonical implementation,
and `socket.io` does the equivalent internally with `pingTimeout`.

But it answers a narrower question than the one you have. Ping/pong proves that the peer's
**protocol layer** is responsive. It does not prove that the peer is still **delivering the data
you subscribed to**. A server can answer pings perfectly while its market-data publisher has
stalled, and you will happily keep the connection alive for hours.

The exchange documentation shows how thin this ice is. [Binance's
docs](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md) describe
a server ping every 20 seconds and a 24-hour connection lifetime, and treat ping/pong as the only
liveness signal — data staleness is not mentioned. [Kraken
provides](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ping) an
*application-level* ping, explicitly distinct from the protocol ping, framed as "check that the
connection is alive" — with no warning that a live pong says nothing about event delivery.

### The dominant library has this gap, and it is documented

CCXT is the reference library for crypto exchange connectivity, and CCXT Pro is its WebSocket half.
Its client tracks exactly one liveness timestamp, `lastPong`, and tears the connection down when
`now - lastPong > keepAlive × maxPingPongMisses` (defaults: 30 000 ms and 2.0) — see
[`ts/src/base/ws/Client.ts`](https://github.com/ccxt/ccxt/blob/master/ts/src/base/ws/Client.ts).
**Inbound application messages do not update it.** A feed whose ping/pong loop stays healthy while
its data stops is, by construction, invisible to that watchdog. In browser builds, where true ping
frames cannot be sent, the implementation refreshes `lastPong` on its own interval tick — so the
zombie check there protects against nothing at all.

This is not theoretical. It is reported, repeatedly, and still open:

- [#22662](https://github.com/ccxt/ccxt/issues/22662) — `watch_order_book` hangs indefinitely:
  socket open, pings and pongs succeeding, no data, no exception raised.
- [#12918](https://github.com/ccxt/ccxt/issues/12918) — WebSocket fails silently; `onOpen`,
  `onPing` and `onPong` all fire, nothing is delivered, and `verbose: true` still reports the
  socket as alive.
- [#20667](https://github.com/ccxt/ccxt/issues/20667) — a random subset of symbols stops
  delivering while the connection stays up.

The maintainers' answer, consistently, is to wrap the call in `try/catch` and re-enter the loop
from user code. A transport-level read timeout was merged on the Python side; no application-data
staleness guard exists in the library. That is a defensible scope decision for them, and it is
exactly the gap this file fills.

### Prior art, honestly

| | Detects OPEN-but-silent? | Application data counts as liveness? | Reconnect + backoff? |
|---|---|---|---|
| [`reconnecting-websocket`](https://www.npmjs.com/package/reconnecting-websocket) (~200k dl/wk, last release 2019) | No — reacts to `close`/`error` only | No | Yes |
| `partysocket`, `robust-websocket` | No — same model | No | Yes |
| [`ws` heartbeat pattern](https://github.com/websockets/ws/blob/master/README.md) | Yes | **No** — protocol pong only | Not client-side |
| [CCXT Pro](https://github.com/ccxt/ccxt/blob/master/ts/src/base/ws/Client.ts) | Partial — only if the pings stop too | **No** — only pongs update `lastPong` | Yes, per exchange |
| [`websocket-heartbeat-js`](https://www.npmjs.com/package/websocket-heartbeat-js) (last release 2023) | Yes | **Yes** — resets on any message | Yes; timer cleanup undocumented |
| `socket.io` / `engine.io` `pingTimeout` | Yes | No — protocol ping | Yes, internal |

`websocket-heartbeat-js` is the closest prior art and gets the central idea right. The gap I kept
hitting is the combination: *data as proof of life* **and** a staleness watchdog **and** a
reconnect path that provably leaves no orphaned timer behind. The third one is what actually bit
me — a reconnect loop that quietly accumulates one extra interval per reconnection is a slow leak
that only shows up after a week of uptime.

So: if `websocket-heartbeat-js` covers your case, use it. This file exists because I needed the
three together, in one place I could reason about.

## What it does

Eight rules, extracted from a set of connectors that had drifted into several designs while
agreeing on the essentials:

1. **Proof of life is any accepted inbound message**, not only a pong. Liveness must not depend on
   the peer choosing to answer pings specifically. (One important exception, below.)
2. **One clock per connection** — a single `lastSeenAt` timestamp, set to now whenever rule 1
   fires. No per-message-type counters.
3. **One staleness watchdog per connection.** On each tick: if `now - lastSeenAt > staleAfterMs`,
   presume the peer dead, stop the watchdog's own timer *first*, then force the socket closed. It
   does nothing else — in particular it does not reconnect. `staleAfterMs` must be strictly greater
   than `pingEveryMs`, so one slow round-trip cannot cause a false kill.
4. **Reconnect is owned by the `close` event, unconditionally** — whether the close came from the
   peer, from a network error, or from the watchdog itself. The close handler never asks why it
   fired, which is what keeps stale-kill and network-drop on one code path instead of two.
5. **Every close cancels every connection-scoped timer**, idempotently. Cancelling twice must be
   safe, because the watchdog and the close handler will both try.
6. **Exponential backoff** from a base delay, capped, for N consecutive failures; past N, a longer
   fixed cooldown, then the counter resets. Never stop retrying forever, never retry in a tight
   loop.
7. **A successful open resets the failure counter.**
8. **Manual `stop()` is a hard override**, checked before any reconnect or connect attempt. It wins
   over anything already scheduled.

### When data-as-liveness is the wrong rule

Rule 1 has an exception, and in my own system it is the heaviest feed.

Binance sends a server ping roughly every 20 seconds whether or not the market is moving. That
makes the ping a *stronger* signal than the data: a quiet market legitimately produces no trades,
but a healthy Binance connection always produces pings. So that connector deliberately does the
opposite of rule 1 — it keeps two timestamps, watches the **ping** clock to decide whether to kill
the connection, and keeps the data clock for diagnostics only. Killing on data silence there would
mean reconnecting every time the order flow goes quiet.

The general rule is therefore not "data is liveness" but: **measure liveness on whatever the peer
has promised to send unconditionally.** For most feeds that is the data, because their pings are
reactive — they only answer yours. For a feed that pings on its own fixed schedule, the ping is the
better clock, and data silence becomes something you *alert* on rather than something you
*reconnect* on. This module lets you pick: return `false` from `onMessage` for anything that should
not count, or leave `ping` undefined and drive the clock purely from inbound traffic.

Two layers live outside this module and belong to the same problem. I would not ship a feed without
either:

- **TCP keepalive on the underlying socket** (`setKeepAlive(true, 30_000)` under Node's `ws`). It
  catches a dead peer at kernel level, independently of anything happening in application code. It
  is not reachable through the standard `WebSocket` API, so it is not in this file — set it on the
  raw socket if your runtime exposes one.
- **A business-level "no data at all" watchdog**, one per data stream rather than one per
  connection. Mine fires when nothing has arrived for a given market in N minutes, however healthy
  every socket looks. It is the layer that catches the case where all transport checks pass and the
  pipeline still produces nothing — a subscription silently dropped after a reconnect, for
  instance.

## Usage

```ts
import { createResilientSocket } from "./resilient-ws";

const client = createResilientSocket({
  url: "wss://ws.kraken.com/v2",

  // Sent on every successful open — including after a reconnect.
  subscribe: () =>
    JSON.stringify({ method: "subscribe", params: { channel: "trade", symbol: ["BTC/USD"] } }),

  // Every inbound message resets the staleness clock, unless you return false.
  onMessage: (data) => {
    const msg = JSON.parse(String(data));
    if (msg.channel !== "trade") return false; // does not count as liveness
    handleTrades(msg.data);
  },

  // Optional. Without it, the watchdog still fires on silence — it just never
  // sends anything, which is what some feeds want.
  ping: () => JSON.stringify({ method: "ping" }),
});

client.on("stale", ({ since }) => log.warn(`no data for ${since}ms — reconnecting`));
client.on("reconnect", ({ attempts, delayMs }) => log.info(`attempt ${attempts} in ${delayMs}ms`));

client.start();
// client.stop() for a permanent, manual shutdown.
```

Defaults: `pingEveryMs` 25 000 · `staleAfterMs` = `pingEveryMs + 10 000` (or 30 000 with no ping) ·
`watchdogTickMs` 2 000 · backoff 1 000 → 30 000 · `maxAttempts` 5 → 5 min cooldown.

Events: `open`, `close`, `error`, `stale`, `reconnect`.

The `WebSocket` constructor, the timer functions and the clock are all injectable — which is how
the tests drive reconnection and staleness on a fake clock instead of waiting on wall time.

## Detection is not repair

This module is a **sensor**. It notices that the feed went quiet and gets you reconnected — and a
reconnect, on its own, loses everything that happened during the gap. In an ingestion pipeline that
is where the work starts, not where it ends.

What I run around it, deliberately kept out of this file because none of it is a transport concern:

- **Short gaps, at reconnect.** On re-open, refetch exactly `[last stored timestamp + 1, now]` from
  the exchange's REST endpoint, bounded by a threshold — mine is five minutes — past which the
  reconnect path gives up and hands over to the next layer. A timestamp boundary is simpler than a
  sequence-number one and, for a trade feed, sufficient. One known limit I have not fixed: a single
  unpaginated REST call caps how many events one gap may contain.
- **Longer gaps, asynchronously.** A separate detector scans stored history for holes over a
  rolling window and refills them from whichever exchange API can serve that range, then recomputes
  whatever was derived from the missing rows. Writes are upserts keyed on `{market, timestamp}`, so
  the same gap can be filled twice without consequence — the layers overlap on purpose, and
  idempotency is what makes that safe.
- **Historical loads.** An offline importer for backfills measured in months. It has nothing to do
  with the live path; it exists because the analytics on top only become meaningful past roughly
  ninety days of history, which is why a gap is never merely cosmetic.

The handoff between these layers is explicit in the code rather than implied: the reconnect path
logs that it is deferring, and to whom. That was the fix for a real class of bug — two layers each
assuming the other had covered the gap.

## What it still does not do

None of the following is implemented here, or anywhere in my stack. It is what a mature market-data
setup adds:

- **Redundant connections with failover.** Two or more independent streams on the same channel, one
  active, switching when the active one goes silent or falls behind.
  [Coinbase publishes two endpoints](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview)
  (`ws-direct` and `ws-feed`) precisely for this, and
  [Coin Metrics](https://gitbook-docs.coinmetrics.io/market-data/market-data-overview/market-order-books)
  describes the same pattern.
- **Sequence-number gap detection.** If you receive N+2 after N you lost N+1, and no timestamp
  boundary will tell you so. [Coinbase's
  procedure](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) — buffer, take a REST
  snapshot, replay while discarding anything at or below it — is the reference, and it is what an
  order book requires. Timestamps are enough for a trade feed; they are not enough for state.
- **Proactive reconnection before a forced disconnect.** Binance closes at 24 hours; the robust move
  is to build the replacement connection while the old one is still alive, then switch.
- **Jitter on the backoff**, so a fleet of clients does not reconnect in lockstep after an outage.
- **Snapshot-driven state reset.** Never assume state accumulated across a reconnect is coherent.

So: this file is the piece that notices something is wrong. The system around it should survive the
sensor being wrong, in both directions.

## Why a socket goes silent

Worth knowing before you blame the network. Ordered by likelihood for a long-lived client on a
modest VPS, and by cost of diagnosis — cheapest first:

| Cause | Mechanism | How to check |
|---|---|---|
| Middlebox idle timeout (proxy, load balancer, NAT) | each hop runs its own timer and drops the entry with no FIN or RST; `readyState` stays `OPEN` | read the timeout config — [typical values](https://websocket.org/guides/troubleshooting/timeout/): ALB 60 s, Cloudflare 100 s, consumer routers 60–300 s. Disconnections at suspiciously regular intervals are this. |
| NAT `conntrack` eviction | the 5-tuple disappears; server→client packets are dropped silently | leave the connection idle past the shortest timeout in the chain |
| **Blocked event loop** | the kernel receives and ACKs, but no `'message'` is ever emitted — externally indistinguishable from a dead socket | `perf_hooks.monitorEventLoopDelay({ resolution: 10 })`, alert above p99 50 ms |
| Backpressure / zero TCP window | the receive buffer fills, the kernel advertises Window=0, the sender stops | `ss -tipm` → `rcv_space` collapsing; Wireshark shows `TCP ZeroWindow` |
| TCP keepalive miscalibrated | Linux defaults `tcp_keepalive_time` to **7200 s**, and a peer that ACKs may still have stopped delivering | `sysctl net.ipv4.tcp_keepalive_time` |
| VPS steal time | the hypervisor gives cycles to a neighbour, slowing your loop — this feeds back into the blocked-event-loop case; **it does not cut TCP** | `vmstat 1 30`, column `st`; above 5 % the host is oversubscribed |

Two things worth stating plainly. First, "my host is throttling my WebSockets" is a forum anecdote:
steal time is involuntary oversubscription, and it does not sever a TCP connection. Second, the
packet capture belongs at the *end* of that list, not the start — three of the six causes are local
to your own process, and the cheap checks separate them in minutes.

## Where this comes from

I extracted this from a private market-data service: seven ingestion services, WebSocket
connectors to about ten exchanges, spot and futures, and — once I went and read all of them, instead of the two I remembered —
**three distinct resilience designs**, not one.

One design keeps liveness inside the connection's own closure and treats any accepted payload as
proof of life. One watches server pings instead, with a second timestamp for data: the Binance case
above. A third, on the futures side, has no continuous liveness clock at all — a ping arms a
timeout, a pong disarms it, and between two pings nothing is watching. All three reconnect from the
close handler with capped exponential backoff, and all three are careful to clear their timers.
They agree on the plumbing and disagree on the question.

That disagreement is why this file exists. Thirteen near-copies of an invariant are not thirteen
implementations; they are one design decision that was never written down, drifting. So I wrote it
down: the rules first, then this module from the rules — not a copy-paste of any connector.

## Install, test

No npm package. It is one file with no dependencies: copy `resilient-ws.ts` into your project.

```
npm install   # vitest + typescript, dev only
npm test
```

Four tests, on injected fake timers and a fake socket: silence while open triggers a reconnect,
inbound data resets the staleness clock, a reconnect leaves no orphan timer running, and `stop()`
cancels a reconnect already scheduled.

## License

MIT — see [LICENSE](LICENSE).

## Author

Carl Bouvet — I run the market-data service this was extracted from. The full diagnosis, including
why ping/pong does not catch this failure, is written up
[here](https://dev.to/carlbouvet/my-websocket-was-open-the-whole-time-it-just-stopped-sending-anything-140e).

Available for freelance work: ingestion pipelines, MongoDB, self-hosted infrastructure.
[carlbouvet.contact@gmail.com](mailto:carlbouvet.contact@gmail.com) · [github.com/CARL-Bouvet](https://github.com/CARL-Bouvet)
