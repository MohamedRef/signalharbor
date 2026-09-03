# Signalharbor

Signalharbor is a Rust library for durable webhook ingestion, tenant-scoped
deduplication, topic routing, retry scheduling, and dead-letter recovery. The
first vertical slice is intentionally transport-agnostic: an HTTP adapter can
feed validated events into the engine and workers can claim due deliveries
without putting network behavior inside the domain model.

State is rebuilt from a file-backed append-only journal. Route changes, events,
delivery creation, attempts, and redrives are synced before the in-memory view
changes. Event plus delivery creation is written as one journal transaction so
a restart cannot accept a dedupe key while losing its initial routing work.
Binary webhook bodies and arbitrary error text use a delimiter-safe hex
encoding. A malformed journal fails closed and identifies its line number.

## Behavior

- Exact topics and trailing wildcards such as `orders.*` are supported.
- Dedupe keys are isolated by tenant; an identical replay is idempotent, while
  reuse with a different topic or body is rejected.
- Provider batches use `ingest_batch` to validate and persist all new events
  and their routed deliveries in one transaction. Input order and same-batch
  duplicate receipts are preserved; a conflict, freeze, or invalid member
  rejects the whole batch. Bounds are 1,000 inputs, 16 MiB of body data and
  10,000 new deliveries; empty and duplicate-only batches do not append.
- HTTP 408, 425, 429, 5xx, and transport errors receive capped exponential
  backoff. Other non-2xx HTTP results go directly to dead letter.
- A route controls its maximum attempts and base delay. Route deactivation
  stops new deliveries without discarding already journaled work.
- Dead letters can be redriven only while their route is active.
- Operators can redrive a bounded route-level dead-letter batch in stable
  delivery order. The batch is one journal transaction, preserves attempt
  history, and reports truncation plus remaining dead letters.
- Due queues and summaries are deterministic, making worker behavior easy to
  test and monitor.
- Workers can atomically claim due deliveries with durable, expiring leases.
  Only the lease owner can report the attempt while it is live; after expiry,
  another worker can recover the delivery without manual journal repair.
- Workers can relinquish a live claim without manufacturing an attempt. A
  release may defer work for at most one day, clears ownership durably, and
  returns the delivery to the deterministic due queue.
- A long-running worker can extend a live claim with `renew_claim`, supplying
  its observed expiry as a compare-and-set guard. Renewal cannot revive expired
  ownership, shorten the deadline, or replace a newer claim; it survives replay
  without creating a delivery attempt.
- Every delivery attempt is journaled atomically with its resulting delivery
  snapshot, including worker, transport or HTTP outcome, retry schedule, and
  terminal state. Histories survive redrive and restart, and summaries expose
  retry, success, dead-letter, transport-failure, and time-bound counts.
- Each route has a configurable consecutive-failure threshold and cooldown.
  Retryable HTTP and transport failures open only that route's circuit; its due
  work remains durable but unavailable to workers until the cooldown expires.
  A successful half-open probe closes the circuit, a failed probe restarts the
  cooldown, and both failure counts and open deadlines survive process restart.
- Historical backfill connects events that predate a route using an inclusive
  receive-time window and hard batch limit. Event-route pairs are idempotent,
  ordered deterministically, and committed as one durable delivery batch.
- Endpoint rotation offers a future-only cutover or an atomic migration of
  pending work that has never been attempted or leased. Attempted, leased, and
  terminal deliveries retain their captured endpoint so retries remain stable.

```rust
use signalharbor::{Route, SignalHarbor, WebhookInput};

let mut harbor = SignalHarbor::open("data/signalharbor.journal")?;
harbor.register_route(Route::new(
    "orders-primary",
    "acme",
    "orders.*",
    "https://hooks.example/orders",
    5,
    10,
)?)?;

let result = harbor.ingest(WebhookInput::new(
    "acme",
    "orders.created",
    "provider-event-804",
    br#"{"order":"A-42"}"#,
    1_787_000_000,
)?)?;
assert!(!result.duplicate);
# Ok::<(), signalharbor::HarborError>(())
```

Operators can cancel pending deliveries with a durable reason. Active leases prevent cancellation,
expired claims are cleared atomically, cancelled work leaves the due queue, and the accountable
cancellation record is verified during journal replay.

Operators can also place a pending delivery on a bounded hold while investigating receiver,
security, or contractual concerns. Holds reject live leases, clear expired leases atomically,
exclude work from both due and claim queues, and expire automatically within seven days. Explicit
release records the responsible operator, while the full hold history remains available after
journal replay.

Each route can enforce a durable in-flight lease limit. Claims consume capacity independently per
route, expired leases restore capacity automatically, and health queries expose live and available
slots. This prevents one slow receiver from accumulating more concurrent work than it can handle.

Planned route maintenance provides bounded, non-overlapping outage windows with scheduler context.
Due queues, claims, and direct attempts pause only while a window is active, then resume without
rewriting delivery schedules. Operators can cancel a future or active window with a durable reason;
ordered maintenance history and all cancellation metadata survive journal replay.

## Verification

The project has no third-party runtime dependencies. Rust 1.92.0, Rustfmt, and
Clippy are pinned in `rust-toolchain.toml`.

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
cargo doc --no-deps
docker build --tag signalharbor-dependencies .
```

## Ownership

This private project is developed and owned by MohamedRef. Project direction,
review, repository ownership, and final decisions remain with MohamedRef. The
repository does not claim external eligibility or certification.

Route endpoints can now be governed by a durable health-probe journal. Probes are accepted only
when due and only for the configured HTTPS endpoint, classify timeouts and HTTP failures, degrade
before a configurable failure threshold, and recover on success. Delivery admission fails closed
for unknown, unavailable, or stale endpoint health while due-route queues remain deterministic.
