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
- HTTP 408, 425, 429, 5xx, and transport errors receive capped exponential
  backoff. Other non-2xx HTTP results go directly to dead letter.
- A route controls its maximum attempts and base delay. Route deactivation
  stops new deliveries without discarding already journaled work.
- Dead letters can be redriven only while their route is active.
- Due queues and summaries are deterministic, making worker behavior easy to
  test and monitor.
- Workers can atomically claim due deliveries with durable, expiring leases.
  Only the lease owner can report the attempt while it is live; after expiry,
  another worker can recover the delivery without manual journal repair.
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

## Verification

The project has no third-party runtime dependencies. Rust 1.89.0, Rustfmt, and
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
