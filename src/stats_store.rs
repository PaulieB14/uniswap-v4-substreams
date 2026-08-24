//! Lifetime totals for pools and hooks — the running-total half of the stats.
//!
//! `map_enriched` already emits [`pb::PoolStats`] / [`pb::HookStats`], and those
//! are **per-block deltas**. They stay that way; they are correct and useful as
//! deltas. This module adds the missing carry: two add-policy stores that fold
//! every block's delta into a lifetime figure, and a map that reads the current
//! figure back out for the entities a block touched, so the SQL side receives
//! finished numbers and never has to accumulate.
//!
//! ```text
//!   map_events ───────────────► store_pool_totals   (add, bigint)
//!        │                              │
//!   map_enriched ──┬──────────► store_hook_totals   (add, bigint)
//!                  │                    │
//!                  └──► map_totals ◄────┘  (both stores, mode: get)
//!                            │
//!                    pb::Events.pool_totals / .hook_totals
//! ```
//!
//! # Why an add-policy STORE is backfill-safe and `UPDATE … SET n = n + d` is not
//!
//! Both spell `+=`. They are not the same operation, and the difference is the
//! entire reason this module exists instead of a trigger on the Postgres side.
//!
//! A Substreams store is a **derived** structure that the engine owns. For a
//! given module hash and block range there is exactly one correct store state,
//! and the engine reserves the right to recompute it: parallel backfill splits
//! the chain into segments and executes them out of order, a cache miss or an
//! evicted segment recomputes from the last snapshot, a worker dies and its
//! segment is redone, a reorg unwinds and re-applies. On every one of those
//! paths the engine does **not** re-apply the delta on top of whatever value is
//! sitting there. It rebuilds the segment's deltas from the input and folds them
//! into a *known* base snapshot. Executing block N a hundred times therefore
//! produces exactly the state of executing it once. The idempotence is a
//! property of the engine's replay model — the `+` is only ever applied to a
//! base the engine can reconstruct — not a property of addition.
//!
//! `UPDATE pool_totals SET swap_count = swap_count + 3 WHERE pool_id = …` has
//! none of that machinery. The row is not derived from anything the database can
//! see; it is a number with no record of which blocks are already folded into
//! it. So:
//!
//! * Replay block N (any of the five causes above) and the row is permanently 3
//!   too high. Nothing detects it, because the correct value is not knowable
//!   from the row.
//! * A reorg needs the delta *subtracted*, and the sink does not have the
//!   undone block's delta to subtract.
//! * Parallel backfill makes it a certainty rather than a risk: 40 workers, each
//!   at-least-once, all writing `n = n + d` to the same row.
//!
//! The split of labour that falls out of that:
//!
//! * **store** accumulates — idempotent by construction, replayed by the engine.
//! * **map** reads the finished total and puts it on the row.
//! * **SQL** does `INSERT … ON CONFLICT DO UPDATE SET total = EXCLUDED.total` —
//!   an overwrite of an already-final number, which is idempotent under any
//!   number of replays. Add `WHERE EXCLUDED.last_block >= pool_totals.last_block`
//!   and it is also safe under out-of-order arrival. **Never `SET total =
//!   total + n` in the sink.**
//!
//! This is also why `map_enriched` is right not to try: a `map` is stateless by
//! contract and cannot hold a carry, so its output has to be a delta. Producing
//! the total needs the one module kind the engine promises to re-derive.
//!
//! # What is NOT here
//!
//! `HookStats.distinct_fee_values` has no counterpart in [`pb::HookTotals`]. It
//! is a set cardinality: a hook charging one fee value across a thousand blocks
//! contributes 1 to each block's delta, and adding those gives 1000. An add
//! store is structurally the wrong tool. A lifetime distinct count needs the
//! *values* retained — a set-policy store keyed `hook:<addr>:fee:<value>` and
//! counted by prefix — which is a different module and is deliberately not built
//! here rather than shipped as a wrong number.
//!
//! `HookTotals.pool_count` *is* here, and it is the exception that proves the
//! rule. It is fed exclusively from `Initialize` logs, and V4's `initialize`
//! reverts on an already-initialised pool, so one poolId emits exactly one
//! Initialize for the life of the chain. Counting those logs is counting
//! distinct pools. That is an invariant of v4-core, not a property of addition;
//! if [`pb::Events::pools`] ever carries anything that can repeat per pool, this
//! field silently becomes a double count.

use std::collections::BTreeMap;

use substreams::errors::Error;
use substreams::scalar::BigInt;
use substreams::store::{StoreAdd, StoreAddBigInt, StoreGet, StoreGetBigInt, StoreNew, StoreGetString};

use crate::pb::uniswap::v4::v1 as pb;

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------
//
// Two stores, so `pool:` and `hook:` live in separate keyspaces and could in
// principle both be bare ids. They are prefixed anyway, for the same reason
// `store_pools` prefixes: a store is a single flat namespace and the two stores
// may be merged later, at which point an unprefixed key is an unrecoverable
// collision. Note the `pool:` prefix here is NOT the same keyspace as
// `store_pools::POOL_KEY_PREFIX` — different module, different store — and the
// two must not be assumed interchangeable.

/// Metric-suffixed key namespace for per-pool lifetime counters.
pub const POOL_TOTAL_PREFIX: &str = "pool:";
/// Metric-suffixed key namespace for per-hook lifetime counters.
pub const HOOK_TOTAL_PREFIX: &str = "hook:";

/// Cumulative count of `Swap` logs.
pub const METRIC_SWAPS: &str = "swaps";
/// Cumulative count of `ModifyLiquidity` logs.
pub const METRIC_MODIFIES: &str = "modifies";
/// Cumulative `|amount0|`, raw token units.
pub const METRIC_VOL0: &str = "vol0";
/// Cumulative `|amount1|`, raw token units.
pub const METRIC_VOL1: &str = "vol1";
/// Cumulative count of `Initialize` logs. Hook keyspace only — a pool is
/// initialised once, so the metric is meaningless per pool.
pub const METRIC_POOLS: &str = "pools";

/// `pool:<poolId>:<metric>`.
///
/// A builder rather than an inline `format!`, for the reason that bit
/// `store_pools`: writer and reader address the store by raw string, and a
/// disagreement does not error — `get_last` returns `None` and every total
/// silently reads 0. Binding both ends to this function makes drift a compile
/// error. `pool_id` arrives 0x-prefixed lowercase from `hooks::pool_id_hex`;
/// do not re-case it.
pub fn pool_total_key(pool_id: &str, metric: &str) -> String {
    format!("{}{}:{}", POOL_TOTAL_PREFIX, pool_id, metric)
}

/// `hook:<address>:<metric>`. Address is 0x-prefixed lowercase from
/// `hooks::addr_hex`.
pub fn hook_total_key(hook_address: &str, metric: &str) -> String {
    format!("{}{}:{}", HOOK_TOTAL_PREFIX, hook_address, metric)
}

/// Ordinal every write in this module uses.
///
/// The value written per key is a **whole-block aggregate** (see
/// [`pool_deltas`]): no single log owns it, so no log's ordinal describes it
/// honestly and a mid-block `get_at` cannot be meaningful whatever we pick.
/// Zero is chosen because it is the one ordinal with a clean reading — "this
/// block's contribution, as a unit" — and because `add` is commutative and
/// associative, so unlike `store_pools` the relative order of writes carries no
/// information to preserve. `get_first` (state before the block) still excludes
/// it; `get_last` (state after the block) still includes it, which is exactly
/// the inclusive semantics [`map_totals`] wants.
const BLOCK_ORD: u64 = 0;

// ---------------------------------------------------------------------------
// Block-local aggregation
// ---------------------------------------------------------------------------

/// One block's contribution for one entity.
///
/// Counts are `u64` here and only widen to `BigInt` at write time: a single
/// block cannot overflow a u64 swap count, and BigInt arithmetic per swap would
/// be an allocation per event for nothing. The volumes are BigInt from the
/// start because a single uint256 swap amount already exceeds 64 bits.
///
/// No `Debug`/`Default` derive: `substreams::scalar::BigInt` implements neither
/// in 0.7.
pub struct Totals {
    pub swaps: u64,
    pub modifies: u64,
    /// `Initialize` logs. Only ever non-zero in the hook keyspace.
    pub pools: u64,
    pub vol0: BigInt,
    pub vol1: BigInt,
}

impl Totals {
    fn new() -> Self {
        Self {
            swaps: 0,
            modifies: 0,
            pools: 0,
            vol0: BigInt::zero(),
            vol1: BigInt::zero(),
        }
    }
}

/// Fold a block into one delta per pool.
///
/// **Aggregate first, write once.** A busy Base block runs ~900 swaps across a
/// few dozen pools; calling `store.add` per swap would be ~3,600 host calls
/// across the WASM boundary per block, versus ~120 for the pre-aggregated form.
/// This is only legal because `add` is associative — `(a+b)+c == a+(b+c)` — so
/// summing in WASM and writing the sum is by definition the same store state as
/// writing each term. It also guarantees each key is written at most once per
/// block, which is what makes the single shared [`BLOCK_ORD`] safe.
///
/// `BTreeMap`, not `HashMap`: module output and store writes must be
/// byte-identical across re-executions, `HashMap` iteration order is not
/// stable, and `substreams_ethereum::init!()` installs a `getrandom` that always
/// errors, so `RandomState` seeding is a live failure on wasm32 rather than a
/// theoretical one.
///
/// Takes raw `map_events` output: a pool delta needs only `pool_id` and the
/// amounts, both present before enrichment, so this store does not have to
/// depend on `store_pools` (and through it on `store_tokens`' eth_calls). The
/// numbers are identical either way — enrichment fills fields, it never adds or
/// drops a row.
pub fn pool_deltas(events: &pb::Events) -> BTreeMap<String, Totals> {
    let mut acc: BTreeMap<String, Totals> = BTreeMap::new();

    for s in &events.swaps {
        // An empty pool_id would mint the key "pool::swaps" — a single garbage
        // bucket silently absorbing every malformed row. Skip instead; the raw
        // swap row is still emitted by map_events either way.
        if s.pool_id.is_empty() {
            continue;
        }
        let e = acc.entry(s.pool_id.clone()).or_insert_with(Totals::new);
        e.swaps += 1;
        e.vol0 = e.vol0.clone() + abs_amount(&s.amount0);
        e.vol1 = e.vol1.clone() + abs_amount(&s.amount1);
    }

    for m in &events.modify_liquidity {
        if m.pool_id.is_empty() {
            continue;
        }
        let e = acc.entry(m.pool_id.clone()).or_insert_with(Totals::new);
        // `liquidity_delta` is deliberately NOT folded into vol0/vol1: it is in
        // units of L = sqrt(x*y), not token units, and mixing it into a token
        // volume is a unit error no consumer could detect afterwards. Same call
        // as `enrich`.
        e.modifies += 1;
    }

    acc
}

/// Fold a block into one delta per hook, from **enriched** events.
///
/// Must read `map_enriched`, not `map_events`: a raw `Swap` log carries only the
/// poolId, so the hook is simply not on the row until `store_pools` has been
/// joined onto it. Rows whose pool is absent from `store_pools` contribute
/// nothing here — the same exclusion `enrich` applies to `HookStats`, so the
/// delta and the total are consistent about what they cover.
///
/// Hookless pools are excluded rather than bucketed under the zero address,
/// again matching `enrich`: a `0x000…0` row would be a chain-wide aggregate
/// wearing a hook's clothes.
pub fn hook_deltas(events: &pb::Events) -> BTreeMap<String, Totals> {
    let mut acc: BTreeMap<String, Totals> = BTreeMap::new();

    // `Initialize` — the one and only feed for pool_count. See the module doc:
    // its addability rests on `initialize` reverting for an already-initialised
    // pool, so this must never be widened to another event.
    for p in &events.pools {
        if let Some(addr) = hook_address(p.hook.as_ref()) {
            acc.entry(addr).or_insert_with(Totals::new).pools += 1;
        }
    }

    for s in &events.swaps {
        if let Some(addr) = hook_address(s.hook.as_ref()) {
            let e = acc.entry(addr).or_insert_with(Totals::new);
            e.swaps += 1;
            e.vol0 = e.vol0.clone() + abs_amount(&s.amount0);
            e.vol1 = e.vol1.clone() + abs_amount(&s.amount1);
        }
    }

    for m in &events.modify_liquidity {
        if let Some(addr) = hook_address(m.hook.as_ref()) {
            acc.entry(addr).or_insert_with(Totals::new).modifies += 1;
        }
    }

    acc
}

/// The hook's address, or `None` for "no hook" / "pool unresolved".
///
/// `has_hook` is the decoded `address != 0` flag from `hooks::decode_hook`;
/// trusting it rather than string-comparing the address keeps the zero-address
/// definition in exactly one place.
fn hook_address(h: Option<&pb::HookPermissions>) -> Option<String> {
    match h {
        Some(h) if h.has_hook && !h.address.is_empty() => Some(h.address.clone()),
        _ => None,
    }
}

/// Push one entity's block delta into an add store.
///
/// Zero components are skipped. Skipping is not just a host-call saving: it
/// keeps the store free of keys that exist only to hold 0, so a future
/// prefix-scan over `pool:` sees pools that actually did something. A missing
/// key reads back as `None`, which [`map_totals`] renders as `"0"` — identical
/// to having written the zero.
fn add_delta<F>(store: &StoreAddBigInt, id: &str, t: &Totals, key: F)
where
    F: Fn(&str, &str) -> String,
{
    if t.swaps > 0 {
        store.add(BLOCK_ORD, key(id, METRIC_SWAPS), &BigInt::from(t.swaps));
    }
    if t.modifies > 0 {
        store.add(BLOCK_ORD, key(id, METRIC_MODIFIES), &BigInt::from(t.modifies));
    }
    if t.pools > 0 {
        store.add(BLOCK_ORD, key(id, METRIC_POOLS), &BigInt::from(t.pools));
    }
    if t.vol0 != BigInt::zero() {
        store.add(BLOCK_ORD, key(id, METRIC_VOL0), &t.vol0);
    }
    if t.vol1 != BigInt::zero() {
        store.add(BLOCK_ORD, key(id, METRIC_VOL1), &t.vol1);
    }
}

/// Lifetime per-pool counters. Input: `map_events` (see [`pool_deltas`]).
#[substreams::handlers::store]
pub fn store_pool_totals(events: pb::Events, store: StoreAddBigInt) {
    for (pool_id, t) in pool_deltas(&events) {
        add_delta(&store, &pool_id, &t, pool_total_key);
    }
}

/// Lifetime per-hook counters. Input: `map_enriched` (see [`hook_deltas`]).
#[substreams::handlers::store]
pub fn store_hook_totals(events: pb::Events, store: StoreAddBigInt) {
    for (hook_address, t) in hook_deltas(&events) {
        add_delta(&store, &hook_address, &t, hook_total_key);
    }
}

// ---------------------------------------------------------------------------
// Read-back
// ---------------------------------------------------------------------------

/// Identity of a pool touched this block, as carried on the enriched rows.
struct Identity {
    token0: String,
    token1: String,
    hook: Option<pb::HookPermissions>,
}

/// Emit the CURRENT lifetime total for every entity this block touched.
///
/// Split out from the `#[handlers::map]` entry point so it is testable off-WASM:
/// `StoreGetBigInt` wraps host imports and cannot be constructed natively, but a
/// closure over a `BTreeMap` can.
///
/// ## `get_last`, and why "current" includes this block
///
/// The closures are expected to be backed by [`StoreGet::get_last`] — store
/// state *after* every write in the block being processed. So a swap in block N
/// is already inside the total reported for block N. That is the intended
/// reading of "lifetime total as of block N", and it is what makes the emitted
/// row directly usable as a snapshot: the sink overwrites and is done.
/// `get_first` would report the total as of the END OF BLOCK N-1 on a row
/// stamped `last_block = N`, which is an off-by-one-block lie for any pool
/// active in N.
///
/// Unlike `enrich`, the time-travel hazard that normally attaches to `get_last`
/// does not apply: there is nothing here to read too early. The value is a
/// commutative sum over the whole block, so "as of ordinal k" is not a quantity
/// this module has any use for.
///
/// ## Pass-through
///
/// Everything else on `events` is returned untouched, so `map_totals` is a
/// drop-in replacement for `map_enriched` as `db_out`'s input — one Events shape
/// end to end, one manifest line to adopt the totals. The cost is that a
/// consumer wanting only totals also carries the raw rows; that is the cheaper
/// mistake than forcing `db_out` to merge two upstreams.
pub fn totals<P, H>(events: pb::Events, pool_get: P, hook_get: H) -> pb::Events
where
    P: Fn(&str) -> Option<BigInt>,
    H: Fn(&str) -> Option<BigInt>,
{
    let mut out = events;
    let block = block_number(&out);

    // ---- which entities did this block touch --------------------------------
    // Initialize rows are included alongside swaps/modifies. A pool created with
    // no other activity is genuinely touched — it is the moment its identity and
    // its hook's pool_count become known — and emitting the all-zero row is how
    // the totals table learns the pool exists at all.
    let mut pools: BTreeMap<String, Identity> = BTreeMap::new();
    for p in &out.pools {
        note(&mut pools, &p.id, &p.token0, &p.token1, p.hook.as_ref());
    }
    for s in &out.swaps {
        note(&mut pools, &s.pool_id, &s.token0, &s.token1, s.hook.as_ref());
    }
    for m in &out.modify_liquidity {
        note(&mut pools, &m.pool_id, &m.token0, &m.token1, m.hook.as_ref());
    }

    // Hooks are derived from the pools touched, not scanned separately: a hook
    // is touched exactly when one of its pools is, and going through the pool
    // map dedupes a multi-pool hook for free.
    let mut hooks: BTreeMap<String, pb::HookPermissions> = BTreeMap::new();
    for id in pools.values() {
        if let Some(h) = id.hook.as_ref() {
            if h.has_hook && !h.address.is_empty() {
                hooks.entry(h.address.clone()).or_insert_with(|| h.clone());
            }
        }
    }

    // ---- read the finished totals back --------------------------------------
    // A key that was never written reads `None` and renders "0": that is the
    // true lifetime total for a metric an entity has never contributed to (a
    // brand-new pool has made zero swaps), not a missing value.
    let read = |get: &dyn Fn(&str) -> Option<BigInt>, key: String| -> String {
        get(&key).unwrap_or_else(BigInt::zero).to_string()
    };

    out.pool_totals = pools
        .iter()
        .map(|(pool_id, id)| pb::PoolTotals {
            pool_id: pool_id.clone(),
            token0: id.token0.clone(),
            token1: id.token1.clone(),
            swap_count: read(&pool_get, pool_total_key(pool_id, METRIC_SWAPS)),
            modify_liquidity_count: read(&pool_get, pool_total_key(pool_id, METRIC_MODIFIES)),
            volume_token0_abs: read(&pool_get, pool_total_key(pool_id, METRIC_VOL0)),
            volume_token1_abs: read(&pool_get, pool_total_key(pool_id, METRIC_VOL1)),
            hook: id.hook.clone(),
            last_block: block,
            // Decimal-adjusted volumes, set only when BOTH tokens have a
            // measured decimals(); an unmeasured decimals defaults to 18 and a
            // silently-wrong human-readable volume is worse than none.
            volume_token0_adjusted: String::new(),
            volume_token1_adjusted: String::new(),
            volumes_adjusted: false,
        })
        .collect();

    out.hook_totals = hooks
        .into_iter()
        .map(|(addr, perms)| pb::HookTotals {
            swap_count: read(&hook_get, hook_total_key(&addr, METRIC_SWAPS)),
            modify_liquidity_count: read(&hook_get, hook_total_key(&addr, METRIC_MODIFIES)),
            pool_count: read(&hook_get, hook_total_key(&addr, METRIC_POOLS)),
            volume_token0_abs: read(&hook_get, hook_total_key(&addr, METRIC_VOL0)),
            volume_token1_abs: read(&hook_get, hook_total_key(&addr, METRIC_VOL1)),
            hook_address: addr,
            permissions: Some(perms),
            last_block: block,
        })
        .collect();

    out
}

/// Record a touched pool, upgrading a previously-unknown identity if a later row
/// in the same block carries one.
///
/// The upgrade matters because the rows disagree: an `Initialize` in this block
/// always names its tokens, while a swap on a pool missing from `store_pools`
/// carries empty ones. First-writer-wins would let a leading unresolved swap
/// pin the pool as anonymous even though the block also contains its Initialize.
/// Non-empty always beats empty; two non-empty values cannot disagree, because
/// they both come from the same immutable PoolKey.
fn note(
    map: &mut BTreeMap<String, Identity>,
    pool_id: &str,
    token0: &str,
    token1: &str,
    hook: Option<&pb::HookPermissions>,
) {
    if pool_id.is_empty() {
        return;
    }
    let e = map.entry(pool_id.to_string()).or_insert_with(|| Identity {
        token0: String::new(),
        token1: String::new(),
        hook: None,
    });
    if e.token0.is_empty() && !token0.is_empty() {
        e.token0 = token0.to_string();
    }
    if e.token1.is_empty() && !token1.is_empty() {
        e.token1 = token1.to_string();
    }
    if e.hook.is_none() {
        if let Some(h) = hook {
            e.hook = Some(h.clone());
        }
    }
}

/// Read both stats stores and stamp lifetime totals onto the block.
///
/// Manifest input order must match this signature exactly: `map: map_enriched`,
/// then `store: store_pool_totals` (`mode: get`), then `store:
/// store_hook_totals` (`mode: get`). Substreams binds store inputs positionally
/// and by TYPE only — both stores are `StoreGetBigInt`, so swapping the two
/// manifest lines compiles, runs, and silently reports pool totals against hook
/// keys (every lookup misses, every total reads 0). The keyspace prefixes are
/// the only thing that makes that failure detectable at all, which is a second
/// reason they are there.
#[substreams::handlers::map]
pub fn map_totals(
    events: pb::Events,
    pool_totals: StoreGetBigInt,
    hook_totals: StoreGetBigInt,
    prices: StoreGetString,
) -> Result<pb::Events, Error> {
    let mut out = totals(
        events,
        |key| pool_totals.get_last(key),
        |key| hook_totals.get_last(key),
    );
    attach_usd(&mut out, &prices);
    Ok(out)
}

/// Fill the USD columns on every swap the price store can anchor.
///
/// Done here rather than in `map_enriched` because `store_prices` is fed BY
/// map_enriched — reading it there would be a cycle. This is the first stage
/// downstream of the store, and it already passes the whole block through.
///
/// A swap is left unpriced unless a leg is a stablecoin, native, or a
/// whitelisted token with a `derived_native:` entry. `priced` records which
/// happened so a consumer can filter instead of guessing why a zero is a zero.
fn attach_usd(events: &mut pb::Events, prices: &StoreGetString) {
    use substreams::store::StoreGet;
    let native_usd = prices
        .get_last(crate::pricing::NATIVE_USD_KEY)
        .and_then(|v| crate::pricing::decode_price_value(&v))
        .map(|r| r.price);

    for s in events.swaps.iter_mut() {
        let (d0, d1) = match (
            crate::pricing::effective_decimals(&s.token0, s.token0_decimals, s.decimals_measured),
            crate::pricing::effective_decimals(&s.token1, s.token1_decimals, s.decimals_measured),
        ) {
            (Some(a), Some(b)) => (a, b),
            // No measured decimals means no honest human amount, so no USD.
            _ => continue,
        };
        let (h0, h1) = match (
            crate::pricing::to_human(&s.amount0, d0),
            crate::pricing::to_human(&s.amount1, d1),
        ) {
            (Some(a), Some(b)) => (a, b),
            _ => continue,
        };
        let dn = |t: &str| {
            prices
                .get_last(crate::pricing::derived_native_key(t))
                .and_then(|v| crate::pricing::decode_price_value(&v))
                .map(|r| r.price)
        };
        let u0 = crate::pricing::usd_for_leg(
            &s.token0, &crate::pricing::abs_bd(&h0), native_usd.as_ref(), dn(&s.token0).as_ref());
        let u1 = crate::pricing::usd_for_leg(
            &s.token1, &crate::pricing::abs_bd(&h1), native_usd.as_ref(), dn(&s.token1).as_ref());

        if let Some(a) = &u0 { s.amount0_usd = a.to_string(); }
        if let Some(b) = &u1 { s.amount1_usd = b.to_string(); }
        if let Some(t) = crate::pricing::tracked_amount_usd(u0.as_ref(), u1.as_ref()) {
            s.amount_usd = t.to_string();
            s.priced = true;
        }
        if let Some(n) = &native_usd { s.native_price_usd = n.to_string(); }
        // Human-readable amounts, signed, only when both decimals were measured.
        s.amount0_adjusted = h0.to_string();
        s.amount1_adjusted = h1.to_string();
        s.amounts_adjusted = true;
    }
}

/// Magnitude of a signed decimal-integer string.
///
/// Intentionally duplicated from `enrich::abs_amount` (private there) rather
/// than re-exported, but the CONVENTION must not diverge: `PoolStats.volume_*`
/// and `PoolTotals.volume_*` are the delta and the running sum of the same
/// quantity, and if one took the signed value the totals would silently stop
/// being the sum of the deltas. `abs_matches_enrich_convention` below pins it.
///
/// Absolute, never signed: V4 amounts are swapper-centric and signed (one leg
/// negative, one positive), so a plain sum measures net flow and converges to
/// zero on a two-sided market. Netting stays derivable from the raw swap rows;
/// volume cannot be recovered from a net.
fn abs_amount(s: &str) -> BigInt {
    match s.parse::<BigInt>() {
        Ok(v) => v.absolute(),
        // Unreachable against our own producer — these strings come from
        // `BigInt::to_string()` in pool_manager.rs — and reachable only for a
        // proto3-default empty string. Contribute zero: panicking would abort
        // the block over a cosmetic problem, and any non-zero substitute would
        // fabricate volume that then becomes PERMANENT in the store.
        Err(_) => BigInt::zero(),
    }
}

/// Block height to stamp on the totals rows.
///
/// Only the three event kinds that can produce a totals row are consulted, in
/// the order they are most likely to be present. A block with none of them
/// yields 0 and also yields no rows, so the value is unobservable.
fn block_number(events: &pb::Events) -> u64 {
    events
        .swaps
        .first()
        .and_then(|s| s.meta.as_ref())
        .or_else(|| {
            events
                .modify_liquidity
                .first()
                .and_then(|m| m.meta.as_ref())
        })
        .or_else(|| events.pools.first().and_then(|p| p.meta.as_ref()))
        .map(|m| m.block_number)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const POOL_A: &str = "0xaaaa000000000000000000000000000000000000000000000000000000000001";
    const POOL_B: &str = "0xbbbb000000000000000000000000000000000000000000000000000000000002";
    const HOOK_1: &str = "0x0000fe59823933ac763611a69c88f91d45f81888";

    fn perms(address: &str, has_hook: bool) -> pb::HookPermissions {
        pb::HookPermissions {
            address: address.to_string(),
            has_hook,
            ..Default::default()
        }
    }

    fn meta(block: u64, log_index: u32) -> pb::Meta {
        pb::Meta {
            block_number: block,
            log_index,
            ..Default::default()
        }
    }

    fn swap(pool: &str, a0: &str, a1: &str, hook: Option<pb::HookPermissions>) -> pb::Swap {
        pb::Swap {
            pool_id: pool.to_string(),
            amount0: a0.to_string(),
            amount1: a1.to_string(),
            token0: "0xtoken0".to_string(),
            token1: "0xtoken1".to_string(),
            hook,
            meta: Some(meta(100, 1)),
            ..Default::default()
        }
    }

    fn modify(pool: &str, hook: Option<pb::HookPermissions>) -> pb::ModifyLiquidity {
        pb::ModifyLiquidity {
            pool_id: pool.to_string(),
            hook,
            meta: Some(meta(100, 2)),
            ..Default::default()
        }
    }

    // ---- key contract ------------------------------------------------------

    #[test]
    fn keys_are_prefixed_and_verbatim() {
        assert_eq!(
            pool_total_key(POOL_A, METRIC_SWAPS),
            format!("pool:{}:swaps", POOL_A)
        );
        assert_eq!(
            hook_total_key(HOOK_1, METRIC_VOL0),
            format!("hook:{}:vol0", HOOK_1)
        );
        // The id must survive untouched: a reader looks up the same string it
        // read off the row.
        assert!(pool_total_key(POOL_A, METRIC_VOL1).contains(POOL_A));
    }

    #[test]
    fn key_format_is_stable() {
        // Changing any of these does not error at runtime — it silently resets
        // every lifetime total to 0 and invalidates every already-built store.
        assert_eq!(POOL_TOTAL_PREFIX, "pool:");
        assert_eq!(HOOK_TOTAL_PREFIX, "hook:");
        assert_eq!(METRIC_SWAPS, "swaps");
        assert_eq!(METRIC_MODIFIES, "modifies");
        assert_eq!(METRIC_VOL0, "vol0");
        assert_eq!(METRIC_VOL1, "vol1");
        assert_eq!(METRIC_POOLS, "pools");
    }

    #[test]
    fn pool_and_hook_keyspaces_cannot_collide() {
        // Guards the "both stores are StoreGetBigInt" hazard called out on
        // map_totals: crossed inputs must miss, not silently resolve.
        assert_ne!(
            pool_total_key(HOOK_1, METRIC_SWAPS),
            hook_total_key(HOOK_1, METRIC_SWAPS)
        );
    }

    // ---- store side --------------------------------------------------------

    #[test]
    fn pool_deltas_sum_absolute_volume_per_pool() {
        let events = pb::Events {
            swaps: vec![
                // Swapper-centric signs: one leg negative. Volume must be 100+50,
                // not 100-50.
                swap(POOL_A, "-100", "50", None),
                swap(POOL_A, "100", "-50", None),
                swap(POOL_B, "-7", "7", None),
            ],
            modify_liquidity: vec![modify(POOL_A, None), modify(POOL_A, None)],
            ..Default::default()
        };

        let d = pool_deltas(&events);
        assert_eq!(d.len(), 2);
        let a = d.get(POOL_A).unwrap();
        assert_eq!(a.swaps, 2);
        assert_eq!(a.modifies, 2);
        assert_eq!(a.vol0.to_string(), "200");
        assert_eq!(a.vol1.to_string(), "100");
        let b = d.get(POOL_B).unwrap();
        assert_eq!(b.swaps, 1);
        assert_eq!(b.modifies, 0);
        assert_eq!(b.vol0.to_string(), "7");
    }

    #[test]
    fn pool_deltas_skip_empty_pool_id() {
        // Otherwise every malformed row lands in one "pool::swaps" bucket that
        // no consumer can attribute or remove.
        let events = pb::Events {
            swaps: vec![swap("", "1", "1", None)],
            ..Default::default()
        };
        assert!(pool_deltas(&events).is_empty());
    }

    #[test]
    fn pool_deltas_are_order_independent() {
        // The property the add store relies on. Aggregating in WASM and writing
        // the sum is only equivalent to writing each term if the fold is
        // order-independent — which is also what makes parallel backfill safe.
        let a = swap(POOL_A, "-100", "50", None);
        let b = swap(POOL_A, "3", "-4", None);
        let fwd = pb::Events {
            swaps: vec![a.clone(), b.clone()],
            ..Default::default()
        };
        let rev = pb::Events {
            swaps: vec![b, a],
            ..Default::default()
        };
        assert_eq!(
            pool_deltas(&fwd).get(POOL_A).unwrap().vol0.to_string(),
            pool_deltas(&rev).get(POOL_A).unwrap().vol0.to_string()
        );
    }

    #[test]
    fn hook_deltas_roll_pools_up_and_count_initialize_once() {
        let h = perms(HOOK_1, true);
        let events = pb::Events {
            // Two pools initialised under the one hook, in one block.
            pools: vec![
                pb::Pool {
                    id: POOL_A.to_string(),
                    hook: Some(h.clone()),
                    meta: Some(meta(100, 0)),
                    ..Default::default()
                },
                pb::Pool {
                    id: POOL_B.to_string(),
                    hook: Some(h.clone()),
                    meta: Some(meta(100, 0)),
                    ..Default::default()
                },
            ],
            swaps: vec![
                swap(POOL_A, "-10", "10", Some(h.clone())),
                swap(POOL_B, "-1", "1", Some(h.clone())),
            ],
            modify_liquidity: vec![modify(POOL_A, Some(h.clone()))],
            ..Default::default()
        };

        let d = hook_deltas(&events);
        assert_eq!(d.len(), 1);
        let t = d.get(HOOK_1).unwrap();
        assert_eq!(t.pools, 2);
        assert_eq!(t.swaps, 2);
        assert_eq!(t.modifies, 1);
        assert_eq!(t.vol0.to_string(), "11");
    }

    #[test]
    fn hook_deltas_exclude_hookless_and_unresolved() {
        // hook: None  => pool absent from store_pools (unenrichable row).
        // has_hook:false => a real hookless pool.
        // Both must stay out: a zero-address bucket would be a chain-wide
        // aggregate wearing a hook's clothes, matching enrich's HookStats rule.
        let events = pb::Events {
            swaps: vec![
                swap(POOL_A, "-10", "10", None),
                swap(POOL_B, "-10", "10", Some(perms("0x0000000000000000000000000000000000000000", false))),
            ],
            ..Default::default()
        };
        assert!(hook_deltas(&events).is_empty());
    }

    // ---- read-back side ----------------------------------------------------

    fn store(pairs: &[(&str, &str)]) -> BTreeMap<String, BigInt> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.parse::<BigInt>().unwrap()))
            .collect()
    }

    #[test]
    fn totals_reports_lifetime_values_for_touched_entities() {
        let h = perms(HOOK_1, true);
        let events = pb::Events {
            swaps: vec![swap(POOL_A, "-10", "10", Some(h.clone()))],
            ..Default::default()
        };

        let p = store(&[
            (&pool_total_key(POOL_A, METRIC_SWAPS), "4321"),
            // Deliberately larger than u64::MAX: a uint256 volume routinely is,
            // which is why every totals field is a decimal string.
            (
                &pool_total_key(POOL_A, METRIC_VOL0),
                "340282366920938463463374607431768211456",
            ),
        ]);
        let hs = store(&[(&hook_total_key(HOOK_1, METRIC_POOLS), "9")]);

        let out = totals(
            events,
            |k| p.get(k).cloned(),
            |k| hs.get(k).cloned(),
        );

        assert_eq!(out.pool_totals.len(), 1);
        let pt = &out.pool_totals[0];
        assert_eq!(pt.pool_id, POOL_A);
        assert_eq!(pt.swap_count, "4321");
        assert_eq!(pt.volume_token0_abs, "340282366920938463463374607431768211456");
        // Never written => absent => "0", the true lifetime total, not a null.
        assert_eq!(pt.modify_liquidity_count, "0");
        assert_eq!(pt.volume_token1_abs, "0");
        assert_eq!(pt.token0, "0xtoken0");
        assert_eq!(pt.last_block, 100);

        assert_eq!(out.hook_totals.len(), 1);
        let ht = &out.hook_totals[0];
        assert_eq!(ht.hook_address, HOOK_1);
        assert_eq!(ht.pool_count, "9");
        assert_eq!(ht.swap_count, "0");
        assert_eq!(ht.last_block, 100);
    }

    #[test]
    fn totals_emits_a_row_for_an_initialize_only_pool() {
        // A pool created with no other activity is still touched — this is how
        // the totals table learns it exists.
        let events = pb::Events {
            pools: vec![pb::Pool {
                id: POOL_A.to_string(),
                token0: "0xt0".to_string(),
                token1: "0xt1".to_string(),
                hook: Some(perms(HOOK_1, true)),
                meta: Some(meta(777, 0)),
                ..Default::default()
            }],
            ..Default::default()
        };
        let empty: BTreeMap<String, BigInt> = BTreeMap::new();
        let out = totals(events, |k| empty.get(k).cloned(), |k| empty.get(k).cloned());

        assert_eq!(out.pool_totals.len(), 1);
        assert_eq!(out.pool_totals[0].swap_count, "0");
        assert_eq!(out.pool_totals[0].token0, "0xt0");
        assert_eq!(out.pool_totals[0].last_block, 777);
        assert_eq!(out.hook_totals.len(), 1);
    }

    #[test]
    fn totals_upgrades_identity_from_a_later_row_in_the_block() {
        // Unresolved swap first, Initialize second. First-writer-wins would pin
        // the pool as anonymous for the whole block.
        let mut s = swap(POOL_A, "-1", "1", None);
        s.token0 = String::new();
        s.token1 = String::new();
        let events = pb::Events {
            pools: vec![pb::Pool {
                id: POOL_A.to_string(),
                token0: "0xreal0".to_string(),
                token1: "0xreal1".to_string(),
                hook: Some(perms(HOOK_1, true)),
                meta: Some(meta(100, 0)),
                ..Default::default()
            }],
            swaps: vec![s],
            ..Default::default()
        };
        let empty: BTreeMap<String, BigInt> = BTreeMap::new();
        let out = totals(events, |k| empty.get(k).cloned(), |k| empty.get(k).cloned());
        assert_eq!(out.pool_totals[0].token0, "0xreal0");
        assert!(out.pool_totals[0].hook.is_some());
    }

    #[test]
    fn totals_passes_the_rest_of_the_block_through() {
        // map_totals must be a drop-in replacement for map_enriched as db_out's
        // input; dropping any repeated field here would silently empty a table.
        let events = pb::Events {
            swaps: vec![swap(POOL_A, "-1", "1", None)],
            modify_liquidity: vec![modify(POOL_A, None)],
            position_events: vec![pb::PositionEvent {
                id: "pe".to_string(),
                ..Default::default()
            }],
            donates: vec![pb::Donate {
                id: "d".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let empty: BTreeMap<String, BigInt> = BTreeMap::new();
        let out = totals(events, |k| empty.get(k).cloned(), |k| empty.get(k).cloned());
        assert_eq!(out.swaps.len(), 1);
        assert_eq!(out.modify_liquidity.len(), 1);
        assert_eq!(out.position_events.len(), 1);
        assert_eq!(out.donates.len(), 1);
    }

    #[test]
    fn abs_matches_enrich_convention() {
        // PoolTotals.volume_* must be the running sum of PoolStats.volume_*.
        // If these two ever use different conventions, the total silently stops
        // being the sum of the deltas.
        assert_eq!(abs_amount("-100").to_string(), "100");
        assert_eq!(abs_amount("100").to_string(), "100");
        // Empty / unparseable contributes zero rather than aborting the block —
        // and a store write is permanent, so fabricating anything else here
        // would be unrecoverable.
        assert_eq!(abs_amount("").to_string(), "0");
    }
}
