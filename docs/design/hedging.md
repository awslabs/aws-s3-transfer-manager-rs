# Hedging

A request can be slow without failing. It sits behind a straggler connection, a congested path, or a
server-side tail-latency event — nothing has errored, the response is just late. A retry cannot help
here, because a retry waits for a failure that never comes. Hedging cuts that tail by racing a fresh
copy of a request that has run longer than expected and cancelling the original once the re-issue is
in flight. It is speculative: the request was never observed to fail, so the re-issue is a bet that a
second attempt will beat waiting for the first. The technique and its ~5% load ceiling are from Dean
& Barroso, [*The Tail at Scale*](https://research.google/pubs/the-tail-at-scale/) (CACM 2013).

Speculation costs load. If the deadline that triggers a hedge is too tight, or if a slowdown is
broad rather than per-connection, hedging amplifies a load event into a re-issue storm — the opposite
of what it is for. So the mechanism is two halves: an **adaptive per-request deadline** that decides
*when* a request has run long enough to be a hedge candidate, and a **self-limiting budget** that
decides *whether* the client can afford to hedge right now. This document covers both, and the
contract the hedge emits to the retry loop.

The deadline controller and the budget are transfer-agnostic. Only the download body path composes
them today; the same mechanism extends to upload part-sends (see [Future Work](#future-work)).

---

## Requirements

### Cut tail latency without a fixed timeout

A single slow request should not hold up a transfer when a fresh attempt would likely be faster. But
a fixed timeout cannot serve this: too low and it false-cancels healthy-but-slow requests on a
high-latency link; too high and it never fires on a fast one. The trigger must adapt to the latency
the client is actually observing, per operation, so the same code paces correctly on a 1 ms
same-region link and a 300 ms cross-region one.

### Speculation must be self-limiting

Hedging adds load exactly when the system may already be under load. The mechanism must bound the
speculative load it can add — a sustained fraction of total requests, not an unbounded multiplier —
and it must *give up on its own* when hedging stops paying off. A broad slowdown (every connection
slow, not one straggler) is the case where hedging is worthless and dangerous: re-issues are as slow
as the originals and double the offered load. The design must detect this and stop, without a
human-set rate limit that would be wrong at some fleet size.

### Do not churn re-issues on a link that is simply slow

Some links are slow across the board — a satellite path, a throttled tenant, a distant region. On
such a link every request exceeds any deadline seeded from fast expectations, so a naive hedger fires
on every request and never helps, doubling load on an already-strained path. The mechanism must
recognize a uniformly slow link before it arms anything and decline to hedge it at all.

### Do not let the adaptive deadline run away

A deadline that adapts to observed latency invites a runaway. The natural adaptation — fold every
sample into a running estimate — is a trap if timeouts count: a timed-out request has no true latency
(it was cancelled), and treating the deadline itself as its latency pulls the deadline down toward
the timeouts, tightening it, causing more timeouts. The adaptation must be immune to this feedback.

### Bounded, terminating speculation per operation

A single logical operation must not spin on speculation. At most one hedge per operation, and the
hedge must not consume the operation's genuine failure-retry budget (that budget belongs to
[retries](./retry.md), and a speculative cancel is not a failure).

---

## Architecture

The pieces: a per-operation `LatencyTracker` holds a mutex-guarded `DeadlineController`; the
controller maintains the adaptive deadline and the hedge budget; `LatencyTracker::guarded` wraps one
request attempt, arming a timeout when a hedge is both permitted and affordable and emitting
`GuardError::DeadlineExceeded` when it fires. The retry loop composes `guarded` inside the future it
builds per attempt and consumes that error as a retryable arm; the loop side of the contract lives in
[retry.md](./retry.md).

### What the deadline guards

The deadline guards the **response**, not the whole transfer — the interval from sending a request to
receiving its response head, before any body transfer begins. On the download body path this is the
time to first byte of a ranged GET: from send to the point the response head returns and the body
stream is in hand. The body *read* that follows is untimed. A body that has started streaming is
making progress; a stall mid-stream is a failure the retry loop handles, not a straggler to hedge.

```text
  request attempt
  ├──────────────── guarded (deadline armed here) ────────────────┤
  │                                                                │
  send ────────────────────────► response head ► body ────────────► drain body
       └── time to first byte ────┘  in hand      (untimed)          to sink
              (the hedged span)

  deadline exceeded ─► drop the in-flight future
                       (releases its connection back to the pool)
                       ─► GuardError::DeadlineExceeded ─► retry loop re-issues
```

On the download path the first attempt of a chunk reuses the body stream already opened by discovery
— no network send — so it is not timed at all: recording a ~0 µs "send" would drag the latency mean
toward zero and corrupt every subsequent deadline. Only a genuine re-issue goes through `guarded` and
contributes a sample.

**Cancellation frees the connection.** A hedge drops the in-flight future rather than signalling it.
Dropping the future releases its connection back to the pool, so the re-issue runs on a fresh
connection instead of queueing behind the straggler on the same one — which is the point, since the
straggler *is* often the connection.

### The adaptive deadline

The deadline is a control variable, not a configured constant. It has three phases: a cold warmup
during which it observes but does not arm, a one-time seed at the end of warmup, and steady-state
relaxation thereafter.

```text
  successes:  0 ──────────────► WARM_THRESHOLD ──────────────────────► ∞
              │  warmup (cold)   │  seed once     │  steady state
              │  buffer samples  │                │  relax toward target
              │  arm nothing     │                │
              │                  ▼                │
              │        warmup_mean ≥ ESCAPE_US ?  │
              │         yes ─► stop_timeout latch: never arm, ever
              │         no  ─► deadline = max(p90(samples), SEED_FLOOR)
              │                                   │
              │                        each success:
              │                        mean  ← EWMA(mean, sample)   [ages]
              │                        deadline ← EWMA(deadline, mean + OFFSET)
              │                        each timeout: count only, move nothing
```

**Warmup buffers, does not arm.** The first `WARM_THRESHOLD` (10) successful samples are buffered and
timed against nothing. A deadline seeded from one or two samples would be noise; ten gives a
distribution to take a percentile of.

**Seed at the tail, not the mean.** At the threshold the deadline is seeded once at
`max(p90(samples), SEED_FLOOR_US)`. The p90 — not the mean — because a high-variance link has fast
requests that dominate the mean and a slow tail that a mean-based deadline would sit *under* and
false-cancel. Seeding at the p90 puts the initial deadline above the slow tenth, so a genuinely
slow-but-healthy request is not cancelled the moment steady state begins. The `SEED_FLOOR_US` (1 s)
keeps a uniformly-fast link from seeding a sub-second deadline that would hedge on trivial jitter.

**Alternative: seed at the mean plus a fixed offset.** On a link whose fast requests pull the mean
well below its tail, a mean-based seed lands inside the tail and cancels healthy requests from the
first steady-state sample — the exact clip the p90 seed avoids. Eight 100 ms samples and two 3 s
samples seed at 3 s (the p90), where a mean-plus-offset would give ~1.38 s and cancel the slow tenth.

**The uniformly-slow latch (`stop_timeout`).** At the seed point, if the *arithmetic* mean of the
buffered warmup samples reaches `ESCAPE_US` (5 s), the link is too slow for re-issues to help: a
re-issue would be as slow as the original. The controller latches `stop_timeout` and never arms a
deadline for the life of the client, even if the link later speeds up. The gate uses the arithmetic
warmup mean, deliberately *not* the aging EWMA (`mean_ttfb`) that drives steady state: the EWMA
weights the first sample ~0.87 over ten samples, so a single cold-start request (an 8 s TLS/DNS
setup) would latch a healthy link off: one 8 s sample plus nine 50 ms ones gives an arithmetic mean
of 845 ms (under the escape, so timed) where the EWMA would report ~7 s and falsely latch.

**Steady-state relaxation.** After seeding, each success does two things: it ages the observed-latency
mean by `MEAN_EWMA_ALPHA` (1/64, ~64-request memory), and it relaxes the deadline toward
`mean + EXPECTED_OFFSET_US` by `VALUE_EWMA_ALPHA` (0.01). The two rates differ on purpose. The mean
tracks conditions reasonably quickly; the deadline converges slowly, so a transient latency spike
folded into the mean does not whip the deadline and trigger a hedge wave. `EXPECTED_OFFSET_US`
(700 ms) is a safety margin over the mean.

**Alternative: a lifetime cumulative mean that never ages.** A never-aging mean dilutes a lasting shift
in network conditions with all-time history, so the deadline stops tracking reality on a long-lived
client. Aging the mean lets a sustained change move the deadline; a bounded memory (1/64) keeps it
from overreacting to one sample.

**Timeouts move nothing.** A timed-out request increments a lifetime counter and touches neither the
mean nor the deadline. This is the censored-P99 trap avoided directly: a cancelled request has no
real latency to contribute, and letting the deadline value stand in for it would ratchet the deadline
tighter with every timeout.

### The hedge budget

The deadline decides a request is a *candidate* for hedging. The budget decides whether the client
*acts* on the candidacy. It is a token bucket whose replenishment rule is the crux: **tokens are
returned by successes, not by the clock.**

```text
  hedge_tokens ∈ [0, HEDGE_CAPACITY=10], starts full

    success     ─► +HEDGE_REWARD (0.05), capped at capacity
    hedge fires ─► −HEDGE_COST (1.0), floored at 0
    arm a hedge only while hedge_tokens ≥ HEDGE_COST

  sustainable hedge fraction = reward / (reward + cost) = 0.05 / 1.05 ≈ 4.8%
```

This ~4.8% setpoint is the *The Tail at Scale* ~5% speculative-load bound made into a control law.

**Normal tail-hedging stays under the setpoint and the budget stays full.** Candidacy is set at
`mean + offset` ≈ p99+, so in steady state only ~1% of requests are candidates. One hedge costs 1
token; ~20 successes at +0.05 each refill it. The budget sits near capacity and never binds — hedging
works freely on the rare true straggler.

```text
  hedge_tokens over time  (capacity 10; hedge −1.0, success +0.05)

  rare stragglers (~1%)                   broad slowdown (candidates > ~4.8%)
  10 ●─╮   ╭─●─╮   ╭─●──                   10 ●
     │ ╰─●─╯   ╰─●─╯                          ╰─●
   5 │                                      5    ╰─●─╮
     │   one dip per hedge, refilled           │    ╰─●─╮
   0 ┼──────────────────────► t            0 ┼──────────╰─●──► t
       budget rides near full                    drains to 0 → hedging halts
```

**A broad slowdown drains the budget and halts hedging — the give-up.** When many requests turn
candidate at once, hedges fire faster than the trickle of successes can refill. Each round nets
negative (a candidate rate above ~4.8% means more than `cost` spent per `reward × successes`
returned), so the budget walks to zero and `can_hedge()` returns false. Further candidates ride to
completion untimed. Tying replenishment to *success* rather than a wall-clock rate is what makes this
automatic: when the thing hedging is supposed to help with (fast completion) stops happening, the
fuel for hedging stops arriving. A 16.7% candidate rate net-drains the budget even with successes
interleaved and counting.

**The capacity is checked, not reserved.** Every warm request arms its deadline if the budget can
afford *one* hedge; a token is charged only when a hedge actually *fires* (a timeout), because an
armed request that succeeds issued no re-issue and cost the service nothing. So the budget does not cap
the instantaneous count of simultaneously-armed deadlines — under a correlated wave, up to the
concurrent-request count can hedge in one burst — after which the budget is empty and speculation
halts. What the budget bounds is the *sustained* fraction, which is the quantity that matters for
offered load.

**A fixed absolute cap self-scales.** A larger concurrent fleet drains a fixed budget faster, so it
gives up sooner — the safe direction, and no per-fleet tuning. Empty-to-full takes
`capacity / reward` (~200) successes.

**Alternative: a single value-based escape (relax the deadline under sustained timeouts, then stop
timing once an estimator crosses a threshold).** This is CRT's give-up mechanism. An estimator that
relaxes between timeouts can fail to converge to "stop" under a candidate rate that is high but not
catastrophic — it recovers just enough between hedges to keep arming them. The budget's
success-replenishment makes the drain unconditional above the setpoint: no run of successes can hold
it open once the candidate rate clears ~4.8%. That unconditional-drain property is why the budget is
the sole give-up rather than one of two overlapping mechanisms.

### The contract emitted to the retry loop

`LatencyTracker::guarded(allow_hedge, fut)` wraps one attempt and is the entire seam between hedging
and retry:

- `allow_hedge` **false**, or cold, or budget drained → run `fut` untimed. It can only fail with its
  inner error (`GuardError::Inner`).
- otherwise → run `fut` under `tokio::time::timeout(deadline)`. On elapse: drop the future, charge the
  budget, record the timeout, return `GuardError::DeadlineExceeded(deadline)`.
- on success (either path) → record the latency, which warms the controller and replenishes the
  budget.

`GuardError::DeadlineExceeded` is the hedge signal. It carries no inner error — nothing failed — only
the deadline that was exceeded. The [retry loop](./retry.md) treats it as a retryable arm and
re-issues. `allow_hedge` is the loop's per-operation gate: it is set on the first attempt and cleared
after the first exceedance (**hedge-once**), so a single operation is speculatively cancelled at most
once. The [free-hedge rule](./retry.md#a-hedge-never-consumes-a-transport-retry) — that this re-issue
does not consume a genuine failure-retry attempt — is a property of the loop's attempt accounting and
is specified there.

---

## Correctness Invariants

The deadline controller is shared across an operation's concurrent chunk requests, so its invariants
are stated against concurrent mutation.

### Speculation is self-limiting

**Invariant.** A sustained hedge-candidate rate above `reward / (reward + cost)` (~4.8%) drives the
budget to zero, after which no hedge is armed until successes refill it.

**What it rules out.** A broad slowdown being amplified into an unbounded re-issue storm: every slow
request hedging, every hedge as slow as its original, offered load doubling and staying doubled.

**Mechanism.** A hedge costs `HEDGE_COST` on fire; a success returns `HEDGE_REWARD`, capped at
capacity. Above the setpoint, per-round net token change is negative regardless of how successes
interleave, so `hedge_tokens` reaches zero and `can_hedge()` gates arming off. Replenishment is tied
to success, not the clock, so the refill stops exactly when completions stop.

### A hedge is armed at most once per operation

**Invariant.** Within one `retry` call, `guarded` arms a deadline on at most one attempt.

**What it rules out.** An operation speculatively cancelling itself repeatedly — cancelling a path
already shown to be slow, spinning re-issues without a genuine failure.

**Mechanism.** The retry loop passes `allow_hedge = true` only until the first
`GuardError::DeadlineExceeded`, then `false` for every later attempt (hedge-once). With `allow_hedge`
false, `guarded` runs the future untimed and cannot produce a further exceedance.

### A timeout never ratchets the deadline tighter

**Invariant.** Only a completed request moves the observed-latency mean or the deadline value.

**What it rules out.** The runaway where the deadline stands in for a cancelled request's latency,
pulling the deadline down, causing more timeouts, pulling it down further.

**Mechanism.** `record_timeout` increments a lifetime counter and returns. The mean EWMA and the
deadline relaxation are updated only in `record_success`.

### A uniformly slow link is never hedged

**Invariant.** If the arithmetic mean of the warmup samples reaches `ESCAPE_US` at the seed point, no
deadline is ever armed for the life of the tracker.

**What it rules out.** Hedging every request on a link where re-issues cannot help (the re-issue is as
slow as the original), and the inverse false-latch where one cold-start sample disables hedging on a
healthy link.

**Mechanism.** `stop_timeout` is a permanent latch set once at the seed point, gated on the
arithmetic warmup mean (not the aging EWMA, which one cold sample would skew). Once latched,
`record_success` returns before touching the deadline.

### Concurrency safety

**Invariant.** The controller stays consistent under concurrent access from an operation's chunk
requests, and the lock is never held across an `await`.

**What it rules out.** A torn budget or mean under contention; a deadlock from holding the controller
lock across the guarded future.

**Mechanism.** `LatencyTracker` guards the `DeadlineController` behind a `Mutex`. Every controller
operation is O(1) and drops the lock before the `guarded` future is awaited — the timeout wraps the
future, and the budget/timeout bookkeeping on elapse takes the lock in a separate short critical
section.

---

## Relationship to CRT

The S3 CRT client hedges from the same *The Tail at Scale* lineage, and this design inherits its
constants directly: the 1 s initial deadline floor (`SEED_FLOOR_US`), the 700 ms expected-timeout
offset (`EXPECTED_OFFSET_US`, `g_expect_timeout_offset_ms` in CRT), and the ~5% speculative-load
ceiling. The control law around those constants diverges in three places, each argued in the
Alternatives above rather than repeated here:

| Concern | CRT | This design |
|---|---|---|
| Give-up under broad slowdown | Value-based escape (relax deadline, stop on threshold) | Success-replenished token budget |
| Latency estimate | Lifetime cumulative mean (never ages) | EWMA, ~64-request memory |
| Initial deadline | Warmup mean plus offset | p90 of warmup latencies, floored |

The constants are CRT's; the mechanisms are not.

---

## Open Questions

**The offset constant is not download-tuned.** `EXPECTED_OFFSET_US` (700 ms) is CRT's
`g_expect_timeout_offset_ms`, tuned for upload response-to-first-byte and reused unchanged as the
download time-to-first-byte margin. A download-specific value may pace hedging better; it has not been
separately measured.

**Auto-tuning the budget constants.** `HEDGE_CAPACITY`, `HEDGE_REWARD`, and `HEDGE_COST` set the
sustainable fraction and the drain/refill speed. They are fixed at values that reproduce the ~5%
bound; whether they should adapt to observed conditions (a tighter budget under sustained pressure) is
open.

---

## Future Work

**Extend hedging to uploads.** The controller and budget are transfer-agnostic, but only the download
body path composes `guarded` today. Uploads carry no latency deadline yet; a slow upload part-send is
a hedge candidate by the same logic. Requires deciding what the hedged span is for an upload (the part
send, not a response TTFB) and handling the non-rewindable-body case.

**Hedged-request metrics.** There is a lifetime timeout counter but no histogram of hedge outcomes
(fired-and-won vs fired-and-lost vs never-armed). Recording whether a hedge actually beat its original
would let the budget constants be tuned against real win rates rather than a load-ceiling heuristic.

**Trace the deadline events under a pinned budget.** See the open question — before any change to how
the deadline treats backpressure, the cause of the elevated time-to-first-byte on already-admitted
requests needs to be traced. A design (suppress the hedge, adjust the timed span) follows only once
the cause is known.
