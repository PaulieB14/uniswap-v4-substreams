//! Pool-identity enrichment and per-block analytics.
//!
//! # The defect this module exists to fix
//!
//! A V4 `Swap` log carries `(id, sender, amount0, amount1, sqrtPriceX96,
//! liquidity, tick, fee)`. `id` is the **PoolId** — a keccak of the PoolKey —
//! and that is *all* the identity on the wire. The tokens, the configured fee,
//! the tick spacing and the hook live in the PoolKey, which is hashed at
//! `initialize` time and never re-emitted. `ModifyLiquidity` is the same.
//!
//! So a consumer streaming `map_events` from a recent block cannot answer "what
//! did this swap trade" without backfilling to that pool's `Initialize`, which
//! may be millions of blocks earlier. The subgraph papers over this with
//! `Pool.load(poolId)` — an implicit, always-present global entity store.
//! Substreams has no implicit state, so the join has to be an explicit module:
//! `store_pools` persists each `Initialize`, and this module reads it back and
//! denormalises the pool's identity onto every row that references it.
//!
//! # Divergence from the subgraph
//!
//! * The subgraph *drops* events for pools it has never seen (`handleSwap`
//!   logs "Pool not found" and returns). Here an unresolvable pool leaves the
//!   denormalised fields at their proto3 defaults and the row is still emitted:
//!   the swap genuinely happened, and `amount0` is still true even when we
//!   cannot name the token. Empty `token0` means "not in the store", never
//!   "the pool has no token0". The count is logged per block — see
//!   [`enrich`] — so a half-enriched stream is loud, not silent.
//! * `PoolStats` / `HookStats` have no subgraph counterpart at all. The
//!   subgraph has no hook entity (it stores `hooks` as an opaque string), so
//!   "which hooks reprice per swap" is not expressible there.

use std::collections::{BTreeMap, BTreeSet};

use substreams::errors::Error;
use substreams::log;
use substreams::scalar::BigInt;
use substreams::store::{StoreGetString, StoreGet, StoreGetProto};

use crate::pb::uniswap::v4::v1 as pb;
// The key builder is imported from the WRITER rather than re-implemented here.
// A store is addressed by a raw string and a disagreement between reader and
// writer does not error — `get_last` just returns `None` and every row silently
// emits unenriched. Binding to `store_pools::pool_key` turns that class of drift
// into a compile error. `key_format_is_stable` below additionally pins the
// on-the-wire literal, since changing it invalidates every already-built store.
use crate::store_pools::pool_key;

/// Per-pool scratch for one block: the resolved identity plus this block's
/// counters. One entry per *distinct* pool, so the store is read once per pool
/// per block rather than once per event — a busy Base block runs ~900 swaps
/// across a few dozen pools, and every `get_last` is a host call across the
/// WASM boundary plus a prost decode.
struct PoolAcc {
    /// `None` = the pool is not in the store. Kept as an explicit `Option`
    /// rather than an empty `Pool`, so "unknown" and "known" are impossible to
    /// confuse at every use site below.
    pool: Option<pb::Pool>,
    swap_count: u64,
    modify_count: u64,
    vol0: BigInt,
    vol1: BigInt,
    /// Distinct effective `Swap.fee` values seen on this pool this block.
    /// Accumulated per pool and only then unioned into the hook, so the hook's
    /// distinct-fee set is a true union and not a sum of per-pool counts.
    fees: BTreeSet<u32>,
}

impl PoolAcc {
    fn new(pool: Option<pb::Pool>) -> Self {
        Self {
            pool,
            swap_count: 0,
            modify_count: 0,
            vol0: BigInt::zero(),
            vol1: BigInt::zero(),
            fees: BTreeSet::new(),
        }
    }
}

/// Per-hook scratch for one block.
struct HookAcc {
    permissions: pb::HookPermissions,
    pools: BTreeSet<String>,
    swap_count: u64,
    vol0: BigInt,
    vol1: BigInt,
    fees: BTreeSet<u32>,
}

impl HookAcc {
    fn new(permissions: pb::HookPermissions) -> Self {
        Self {
            permissions,
            pools: BTreeSet::new(),
            swap_count: 0,
            vol0: BigInt::zero(),
            vol1: BigInt::zero(),
            fees: BTreeSet::new(),
        }
    }
}

/// Denormalise pool identity onto every swap / modify_liquidity row, then emit
/// per-block `PoolStats` and `HookStats`.
///
/// ## Store read semantics — how a same-block pool still resolves
///
/// `resolve` is expected to be backed by [`StoreGet::get_last`], **not**
/// `get_first` and not `get_at`. The distinction is the whole ballgame for a
/// pool created in the block we are currently enriching:
///
/// * `get_first` — store state at the START of this block. A pool initialised
///   in this block would be invisible, and its very first swaps (routinely in
///   the same transaction as the `Initialize`) would emit unenriched.
/// * `get_at(ord)` — state as of an ordinal mid-block. Correct but needless
///   here, and it would couple this module to whatever ordinal `store_pools`
///   happened to write with.
/// * `get_last` — "the state of the store as of the end of the block being
///   processed, after all changes were applied within the current block"
///   (substreams 0.7 `store.rs`). Same-block initialisations resolve. This is
///   what we use.
///
/// Reading end-of-block state is normally a time-travel hazard: you can see a
/// write that happens *after* the event you are enriching. It is safe here
/// because every field copied — `token0`, `token1`, `fee_tier`, `tick_spacing`,
/// `hook` — comes from the PoolKey and is **frozen at initialize**. The
/// end-of-block value and the value at the swap's own ordinal are the same
/// value by construction. Nor can `get_last` over-resolve: a pool must be
/// initialised before it can be swapped, so "resolved from later in the block"
/// cannot describe a pool that did not exist at swap time.
///
/// **If this module is ever extended to copy mutable pool state** — `sqrt_price`,
/// `tick`, `liquidity`, all of which `pb::Pool` also carries — `get_last` becomes
/// wrong and the lookup must move to `get_at(meta.log_index)`, with
/// `store_pools` writing at that same ordinal.
///
/// Split out from the `#[handlers::map]` entry point so the logic is testable:
/// `StoreGetProto` is a thin wrapper over host imports and cannot be
/// constructed off-WASM, but a closure over a `BTreeMap` can.
pub fn enrich<F, G>(events: pb::Events, resolve: F, resolve_token: G) -> pb::Events
where
    F: Fn(&str) -> Option<pb::Pool>,
    G: Fn(&str) -> Option<String>,
{
    let mut out = events;
    let block = block_number(&out);

    // BTreeMap, not HashMap, throughout. Two reasons, both load-bearing:
    // (1) Substreams caches module output and re-executes on reorg/backfill —
    //     output must be byte-identical across runs, and std's HashMap iteration
    //     order is not stable. (2) `substreams_ethereum::init!()` installs a
    //     getrandom that always errors, so RandomState seeding is a live hazard
    //     on wasm32 rather than a theoretical one.
    let mut pools: BTreeMap<String, PoolAcc> = BTreeMap::new();

    // ---- pass 1: resolve each distinct pool once, fold in this block's counters
    for s in &out.swaps {
        let acc = pools
            .entry(s.pool_id.clone())
            .or_insert_with(|| PoolAcc::new(resolve(&s.pool_id)));
        acc.swap_count += 1;
        acc.vol0 += abs_amount(&s.amount0);
        acc.vol1 += abs_amount(&s.amount1);
        acc.fees.insert(s.fee);
    }
    for m in &out.modify_liquidity {
        let acc = pools
            .entry(m.pool_id.clone())
            .or_insert_with(|| PoolAcc::new(resolve(&m.pool_id)));
        // Liquidity deltas are NOT folded into vol0/vol1. `liquidity_delta` is
        // in units of L (sqrt(x*y)), not token units; adding it to a token
        // volume would be a unit error that no downstream consumer could detect.
        acc.modify_count += 1;
    }

    // ---- pass 2: write the resolved identity back onto every row
    //
    // A `HookPermissions` clone per row is the cost of denormalisation and is
    // the point of the module: the row has to stand alone.
    for s in out.swaps.iter_mut() {
        if let Some(p) = pools.get(&s.pool_id).and_then(|a| a.pool.as_ref()) {
            s.token0 = p.token0.clone();
            s.token1 = p.token1.clone();
            s.fee_tier = p.fee_tier;
            s.tick_spacing = p.tick_spacing;
            s.hook = p.hook.clone();
            let (s0, s1, d0, d1, meas) = attach_tokens(&resolve_token, &s.token0, &s.token1);
            s.token0_symbol = s0;
            s.token1_symbol = s1;
            s.token0_decimals = d0;
            s.token1_decimals = d1;
            s.decimals_measured = meas;
        }
        // else: left at proto3 defaults. Deliberately NOT invented — a
        // plausible-looking zero address for token0 would be indistinguishable
        // from a real native-ETH pool, whose currency0 IS the zero address.
    }
    for m in out.modify_liquidity.iter_mut() {
        if let Some(p) = pools.get(&m.pool_id).and_then(|a| a.pool.as_ref()) {
            m.token0 = p.token0.clone();
            m.token1 = p.token1.clone();
            m.fee_tier = p.fee_tier;
            m.tick_spacing = p.tick_spacing;
            m.hook = p.hook.clone();
            let (s0, s1, d0, d1, meas) = attach_tokens(&resolve_token, &m.token0, &m.token1);
            m.token0_symbol = s0;
            m.token1_symbol = s1;
            m.token0_decimals = d0;
            m.token1_decimals = d1;
            m.decimals_measured = meas;
        }
    }

    // ---- pass 3: roll pools up into hooks
    let mut hooks: BTreeMap<String, HookAcc> = BTreeMap::new();
    let mut unresolved_pools = 0u64;
    let mut unresolved_swaps = 0u64;
    let mut unresolved_modifies = 0u64;

    for (pool_id, acc) in pools.iter() {
        let pool = match acc.pool.as_ref() {
            Some(p) => p,
            None => {
                unresolved_pools += 1;
                unresolved_swaps += acc.swap_count;
                unresolved_modifies += acc.modify_count;
                continue;
            }
        };

        let perms = match pool.hook.as_ref() {
            Some(h) if h.has_hook => h,
            // Hookless pools are excluded rather than bucketed under the zero
            // address. A "0x000…0" HookStats row would be a chain-wide
            // aggregate wearing a hook's clothes, and it would poison the
            // headline metric specifically: pooling every hookless pool
            // together makes `distinct_fee_values` large purely because those
            // pools sit on different static tiers, which reads identically to
            // a hook repricing per swap. See the confound note on
            // `distinct_fee_values` below — it applies to real multi-pool
            // hooks too, just less severely.
            _ => continue,
        };

        let h = hooks
            .entry(perms.address.clone())
            .or_insert_with(|| HookAcc::new(perms.clone()));
        // Every pool the hook touched this block, by swap OR by modify — this
        // is "pools active under this hook", not "pools it was swapped on".
        h.pools.insert(pool_id.clone());
        h.swap_count += acc.swap_count;
        h.vol0 += acc.vol0.clone();
        h.vol1 += acc.vol1.clone();
        h.fees.extend(acc.fees.iter().copied());
    }

    // Loud, not silent. There is no field on `pb::Events` for this (see the
    // module report), so the block log is the channel. One line per block, only
    // when something was actually dropped — never per row.
    if unresolved_pools > 0 {
        log::info!(
            "enrich: block {} — {} pool(s) absent from store_pools; {} swap(s) and {} modify_liquidity row(s) emitted with token0/token1/fee_tier/tick_spacing/hook UNSET, and excluded from hook_stats. Expected when the stream starts after those pools were initialised (no backfill to their Initialize event); unexpected otherwise.",
            block,
            unresolved_pools,
            unresolved_swaps,
            unresolved_modifies
        );
    }

    // ---- pass 4: emit
    //
    // These rows are PER-BLOCK DELTAS, not running totals. `map` modules are
    // stateless by definition and are re-executed out of order during parallel
    // backfill, so a cumulative counter cannot be computed here. A downstream
    // add-policy store folds these into lifetime totals.
    //
    // NOT summable that way: `HookStats.distinct_fee_values` and
    // `HookStats.pool_count` are set cardinalities. Summing a cardinality over
    // blocks double-counts every value/pool that recurs. A consumer wanting
    // lifetime distinct-fee needs the set unioned in a store, not added.
    out.pool_stats = pools
        .into_iter()
        .map(|(pool_id, acc)| {
            // Unresolved pools still get a stats row: swap_count, modify count
            // and both volumes are fully correct for them — only the naming is
            // missing. Dropping the row would silently understate block volume,
            // which is a worse failure than an obviously-empty token0.
            let (token0, token1, hook) = match acc.pool {
                Some(p) => (p.token0, p.token1, p.hook),
                None => (String::new(), String::new(), None),
            };
            pb::PoolStats {
                pool_id,
                token0,
                token1,
                swap_count: acc.swap_count,
                volume_token0_abs: acc.vol0.to_string(),
                volume_token1_abs: acc.vol1.to_string(),
                modify_liquidity_count: acc.modify_count,
                hook,
                last_block: block,
                // Decimal-adjusted volumes, set only when BOTH tokens have a
                // measured decimals(); an unmeasured decimals defaults to 18 and a
                // silently-wrong human-readable volume is worse than none.
                volume_token0_adjusted: String::new(),
                volume_token1_adjusted: String::new(),
                volumes_adjusted: false,
            }
        })
        .collect();

    out.hook_stats = hooks
        .into_iter()
        .map(|(hook_address, acc)| pb::HookStats {
            hook_address,
            permissions: Some(acc.permissions),
            pool_count: acc.pools.len() as u64,
            swap_count: acc.swap_count,
            volume_token0_abs: acc.vol0.to_string(),
            volume_token1_abs: acc.vol1.to_string(),
            // The headline metric. A pool on a static fee emits exactly one
            // value; a hook resolving the fee per swap emits many.
            //
            // CONFOUND, and it is real: one hook serving three pools on tiers
            // 500 / 3000 / 10000 reports 3 here without overriding anything.
            // Within a single block that is usually distinguishable (a static
            // multi-pool hook's count is capped by its pool count, an
            // overriding hook's is not), but the two are not separable from
            // this field alone. The clean fix is a per-pool distinct-fee count
            // to compare against, or a count of swaps where `fee != fee_tier`;
            // neither has a home in the current proto.
            distinct_fee_values: acc.fees.len() as u64,
            last_block: block,
        })
        .collect();

    out
}

/// Enrich `map_events` output against `store_pools`.
///
/// Manifest input order must match this signature exactly — `map: map_events`
/// then `store: store_pools` with `mode: get`.

/// Join token metadata onto a row that already knows its token addresses.
///
/// A miss is left empty, never guessed. `store_tokens` only records a token the
/// first time a pool mentions it, so a stream that starts after a token's first
/// pool will legitimately have no entry — and an invented symbol is worse than a
/// blank one, because it looks authoritative.
fn attach_tokens<G: Fn(&str) -> Option<String>>(
    lookup: &G,
    t0: &str,
    t1: &str,
) -> (String, String, u32, u32, bool) {
    let get = |a: &str| {
        if a.is_empty() {
            return None;
        }
        lookup(&a.to_ascii_lowercase()).and_then(|v| crate::tokens::decode_token_value(a, &v))
    };
    let (m0, m1) = (get(t0), get(t1));
    // decimals_measured is AND, not OR: the flag guards arithmetic that uses
    // both sides, so one unmeasured token makes the pair untrustworthy.
    let measured = matches!((&m0, &m1), (Some((_, r0)), Some((_, r1)))
        if r0.decimals_measured && r1.decimals_measured);
    (
        m0.as_ref().map(|(m, _)| m.symbol.clone()).unwrap_or_default(),
        m1.as_ref().map(|(m, _)| m.symbol.clone()).unwrap_or_default(),
        m0.as_ref().map(|(m, _)| m.decimals as u32).unwrap_or(0),
        m1.as_ref().map(|(m, _)| m.decimals as u32).unwrap_or(0),
        measured,
    )
}

#[substreams::handlers::map]
pub fn map_enriched(
    events: pb::Events,
    store: StoreGetProto<pb::Pool>,
    tokens: StoreGetString,
) -> Result<pb::Events, Error> {
    // `get_last`, not `get_first`/`get_at` — see the doc comment on `enrich`
    // for why that is what makes a same-block `Initialize` resolve, and why it
    // is safe only for PoolKey-immutable fields.
    Ok(enrich(
        events,
        |pool_id| store.get_last(pool_key(pool_id)),
        |addr| tokens.get_last(format!("token:{}", addr)),
    ))
}

/// Magnitude of a signed decimal-integer string.
///
/// Volume is `|amount|`, never the signed sum. V4 swap amounts are
/// **swapper-centric and signed**: for a given token, a buy is negative and a
/// sell is positive. Summing them measures net flow, which on any two-sided
/// market converges toward zero no matter how much traded — a pool that turned
/// over a billion dollars evenly in both directions would report ~0 volume.
/// The direction is not lost by doing this: the signed per-swap amounts are
/// still on the `Swap` rows, so netting stays derivable downstream, whereas
/// volume cannot be recovered from a net.
fn abs_amount(s: &str) -> BigInt {
    match s.parse::<BigInt>() {
        Ok(v) => v.absolute(),
        // Unreachable against our own producer: these strings come from
        // `BigInt::to_string()` in pool_manager.rs. Reachable only for a
        // proto3-default empty string. Contribute zero — panicking would abort
        // the block for a cosmetic problem, and substituting anything non-zero
        // would fabricate volume.
        Err(_) => BigInt::zero(),
    }
}

/// Block height for the stats rows.
///
/// Derived from the first event's `meta` rather than taken as a module input:
/// the handler's inputs are `Events` + the store, and `Events` has no envelope
/// block field. Every branch is checked because a block can carry, say,
/// position events and no swaps. A block with no events at all yields 0, and
/// also yields no stats rows, so the value is never observable.
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
        .or_else(|| events.position_events.first().and_then(|p| p.meta.as_ref()))
        .or_else(|| {
            events
                .hook_deployments
                .first()
                .and_then(|h| h.meta.as_ref())
        })
        .map(|m| m.block_number)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const POOL_A: &str = "0xaaaa000000000000000000000000000000000000000000000000000000000001";
    const POOL_B: &str = "0xbbbb000000000000000000000000000000000000000000000000000000000002";
    const POOL_UNKNOWN: &str = "0xcccc000000000000000000000000000000000000000000000000000000000003";

    const HOOK_1: &str = "0x0000fe59823933ac763611a69c88f91d45f81888";
    const HOOK_2: &str = "0x00000000000000000000000000000000000010c0";

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

    fn pool(id: &str, t0: &str, t1: &str, fee: u64, hook: &str) -> pb::Pool {
        pb::Pool {
            id: id.to_string(),
            token0: t0.to_string(),
            token1: t1.to_string(),
            fee_tier: fee,
            tick_spacing: 60,
            hook: Some(perms(
                hook,
                hook != "0x0000000000000000000000000000000000000000",
            )),
            ..Default::default()
        }
    }

    fn swap(pool_id: &str, a0: &str, a1: &str, fee: u32) -> pb::Swap {
        pb::Swap {
            pool_id: pool_id.to_string(),
            amount0: a0.to_string(),
            amount1: a1.to_string(),
            fee,
            meta: Some(meta(35_000_000, 0)),
            ..Default::default()
        }
    }

    fn modify(pool_id: &str, delta: &str) -> pb::ModifyLiquidity {
        pb::ModifyLiquidity {
            pool_id: pool_id.to_string(),
            liquidity_delta: delta.to_string(),
            meta: Some(meta(35_000_000, 1)),
            ..Default::default()
        }
    }

    /// Resolver over a fixed map — stands in for `store.get_last`.
    fn resolver(pools: Vec<pb::Pool>) -> impl Fn(&str) -> Option<pb::Pool> {
        let map: BTreeMap<String, pb::Pool> =
            pools.into_iter().map(|p| (p.id.clone(), p)).collect();
        move |id: &str| map.get(id).cloned()
    }

    #[test]
    fn key_format_is_stable() {
        // Pins the contract with store_pools. If this literal changes, the
        // store lookup silently returns None for every pool.
        assert_eq!(pool_key("0xdead"), "pool:0xdead");
    }

    #[test]
    fn fills_identity_on_swaps_and_modifies() {
        let events = pb::Events {
            swaps: vec![swap(POOL_A, "-100", "250", 3000)],
            modify_liquidity: vec![modify(POOL_A, "-5000")],
            ..Default::default()
        };
        let out = enrich(
            events,
            resolver(vec![pool(POOL_A, "0xt0", "0xt1", 3000, HOOK_1)]),
            |_| None,
        );

        let s = &out.swaps[0];
        assert_eq!(s.token0, "0xt0");
        assert_eq!(s.token1, "0xt1");
        assert_eq!(s.fee_tier, 3000);
        assert_eq!(s.tick_spacing, 60);
        assert_eq!(s.hook.as_ref().unwrap().address, HOOK_1);

        let m = &out.modify_liquidity[0];
        assert_eq!(m.token0, "0xt0");
        assert_eq!(m.fee_tier, 3000);
        assert_eq!(m.hook.as_ref().unwrap().address, HOOK_1);
    }

    #[test]
    fn unknown_pool_leaves_fields_empty_but_keeps_the_row() {
        let events = pb::Events {
            swaps: vec![swap(POOL_UNKNOWN, "-100", "250", 3000)],
            ..Default::default()
        };
        let out = enrich(events, resolver(vec![]), |_| None);

        // The row survives — the subgraph would have dropped it.
        assert_eq!(out.swaps.len(), 1);
        let s = &out.swaps[0];
        assert_eq!(s.token0, "");
        assert_eq!(s.token1, "");
        assert_eq!(s.fee_tier, 0);
        assert_eq!(s.tick_spacing, 0);
        assert!(s.hook.is_none());
        // Raw payload untouched.
        assert_eq!(s.amount0, "-100");

        // Counters are still correct, so block volume is not understated...
        assert_eq!(out.pool_stats.len(), 1);
        assert_eq!(out.pool_stats[0].swap_count, 1);
        assert_eq!(out.pool_stats[0].volume_token0_abs, "100");
        assert_eq!(out.pool_stats[0].token0, "");
        // ...but the pool cannot be attributed to a hook.
        assert!(out.hook_stats.is_empty());
    }

    #[test]
    fn volume_is_absolute_not_netted() {
        // Two equal-and-opposite swaps: a signed sum is 0, volume is 200/500.
        let events = pb::Events {
            swaps: vec![
                swap(POOL_A, "-100", "250", 3000),
                swap(POOL_A, "100", "-250", 3000),
            ],
            ..Default::default()
        };
        let out = enrich(
            events,
            resolver(vec![pool(POOL_A, "0xt0", "0xt1", 3000, HOOK_1)]),
            |_| None,
        );

        assert_eq!(out.pool_stats[0].volume_token0_abs, "200");
        assert_eq!(out.pool_stats[0].volume_token1_abs, "500");
        assert_eq!(out.pool_stats[0].swap_count, 2);
    }

    #[test]
    fn volume_survives_uint256_scale() {
        // A u64 accumulator would have wrapped long before here.
        let big = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        let events = pb::Events {
            swaps: vec![swap(POOL_A, big, "0", 3000)],
            ..Default::default()
        };
        let out = enrich(
            events,
            resolver(vec![pool(POOL_A, "0xt0", "0xt1", 3000, HOOK_1)]),
            |_| None,
        );
        assert_eq!(out.pool_stats[0].volume_token0_abs, big);
    }

    #[test]
    fn distinct_fee_values_separates_static_from_repricing_hooks() {
        // HOOK_1 reprices every swap; HOOK_2 charges its static tier.
        let events = pb::Events {
            swaps: vec![
                swap(POOL_A, "-1", "1", 19900),
                swap(POOL_A, "-1", "1", 20100),
                swap(POOL_A, "-1", "1", 17350),
                swap(POOL_B, "-1", "1", 3000),
                swap(POOL_B, "-1", "1", 3000),
            ],
            ..Default::default()
        };
        let out = enrich(
            events,
            resolver(vec![
                pool(POOL_A, "0xt0", "0xt1", 8_388_608, HOOK_1), // dynamic-fee sentinel
                pool(POOL_B, "0xt0", "0xt1", 3000, HOOK_2),
            ]),
            |_| None,
        );

        let h1 = out
            .hook_stats
            .iter()
            .find(|h| h.hook_address == HOOK_1)
            .unwrap();
        let h2 = out
            .hook_stats
            .iter()
            .find(|h| h.hook_address == HOOK_2)
            .unwrap();

        assert_eq!(h1.distinct_fee_values, 3);
        assert_eq!(h1.swap_count, 3);
        assert_eq!(h2.distinct_fee_values, 1);
        assert_eq!(h2.swap_count, 2);
    }

    #[test]
    fn hook_stats_aggregate_across_pools() {
        let events = pb::Events {
            swaps: vec![
                swap(POOL_A, "-10", "20", 500),
                swap(POOL_B, "-30", "40", 500),
            ],
            modify_liquidity: vec![modify(POOL_B, "1")],
            ..Default::default()
        };
        let out = enrich(
            events,
            resolver(vec![
                pool(POOL_A, "0xt0", "0xt1", 500, HOOK_1),
                pool(POOL_B, "0xt2", "0xt3", 500, HOOK_1),
            ]),
            |_| None,
        );

        assert_eq!(out.hook_stats.len(), 1);
        let h = &out.hook_stats[0];
        assert_eq!(h.pool_count, 2);
        assert_eq!(h.swap_count, 2);
        assert_eq!(h.volume_token0_abs, "40");
        assert_eq!(h.volume_token1_abs, "60");
        // One fee value across both pools — a union, not 1+1.
        assert_eq!(h.distinct_fee_values, 1);

        // modify_liquidity contributes the pool but no swap volume.
        let b = out.pool_stats.iter().find(|p| p.pool_id == POOL_B).unwrap();
        assert_eq!(b.modify_liquidity_count, 1);
        assert_eq!(b.swap_count, 1);
    }

    #[test]
    fn hookless_pools_are_excluded_from_hook_stats() {
        let zero = "0x0000000000000000000000000000000000000000";
        let events = pb::Events {
            swaps: vec![swap(POOL_A, "-1", "1", 500), swap(POOL_B, "-1", "1", 3000)],
            ..Default::default()
        };
        let out = enrich(
            events,
            resolver(vec![
                pool(POOL_A, "0xt0", "0xt1", 500, zero),
                pool(POOL_B, "0xt0", "0xt1", 3000, zero),
            ]),
            |_| None,
        );

        // Both pools are still fully enriched and counted...
        assert_eq!(out.pool_stats.len(), 2);
        assert_eq!(out.swaps[0].fee_tier, 500);
        // ...but there is no 0x0 pseudo-hook row claiming 2 distinct fee values.
        assert!(out.hook_stats.is_empty());
    }

    #[test]
    fn modify_only_pool_still_gets_stats() {
        let events = pb::Events {
            modify_liquidity: vec![modify(POOL_A, "-5000")],
            ..Default::default()
        };
        let out = enrich(
            events,
            resolver(vec![pool(POOL_A, "0xt0", "0xt1", 3000, HOOK_1)]),
            |_| None,
        );

        assert_eq!(out.pool_stats.len(), 1);
        assert_eq!(out.pool_stats[0].swap_count, 0);
        assert_eq!(out.pool_stats[0].modify_liquidity_count, 1);
        assert_eq!(out.pool_stats[0].volume_token0_abs, "0");
        // Present under the hook even with zero swaps: the pool was active.
        assert_eq!(out.hook_stats.len(), 1);
        assert_eq!(out.hook_stats[0].pool_count, 1);
        assert_eq!(out.hook_stats[0].swap_count, 0);
        assert_eq!(out.hook_stats[0].distinct_fee_values, 0);
    }

    #[test]
    fn empty_block_emits_nothing() {
        let out = enrich(pb::Events::default(), resolver(vec![]), |_| None);
        assert!(out.pool_stats.is_empty());
        assert!(out.hook_stats.is_empty());
    }

    #[test]
    fn passes_through_untouched_streams_and_stamps_the_block() {
        let events = pb::Events {
            pools: vec![pool(POOL_A, "0xt0", "0xt1", 3000, HOOK_1)],
            swaps: vec![swap(POOL_A, "-1", "1", 3000)],
            position_events: vec![pb::PositionEvent {
                id: "pe".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = enrich(
            events,
            resolver(vec![pool(POOL_A, "0xt0", "0xt1", 3000, HOOK_1)]),
            |_| None,
        );

        assert_eq!(out.pools.len(), 1);
        assert_eq!(out.position_events.len(), 1);
        assert_eq!(out.pool_stats[0].last_block, 35_000_000);
        assert_eq!(out.hook_stats[0].last_block, 35_000_000);
    }

    #[test]
    fn output_ordering_is_deterministic() {
        // Insertion order B-then-A must still emit sorted, or cached output
        // differs between a live run and a replay.
        let events = pb::Events {
            swaps: vec![swap(POOL_B, "-1", "1", 500), swap(POOL_A, "-1", "1", 500)],
            ..Default::default()
        };
        let out = enrich(
            events,
            resolver(vec![
                pool(POOL_A, "0xt0", "0xt1", 500, HOOK_2),
                pool(POOL_B, "0xt0", "0xt1", 500, HOOK_1),
            ]),
            |_| None,
        );

        let ids: Vec<&str> = out.pool_stats.iter().map(|p| p.pool_id.as_str()).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(ids, sorted);

        let hooks: Vec<&str> = out
            .hook_stats
            .iter()
            .map(|h| h.hook_address.as_str())
            .collect();
        let mut hsorted = hooks.clone();
        hsorted.sort_unstable();
        assert_eq!(hooks, hsorted);
    }

    #[test]
    fn malformed_amount_contributes_zero_rather_than_aborting() {
        let mut s = swap(POOL_A, "", "not-a-number", 500);
        s.id = "x".to_string();
        let events = pb::Events {
            swaps: vec![s],
            ..Default::default()
        };
        let out = enrich(
            events,
            resolver(vec![pool(POOL_A, "0xt0", "0xt1", 500, HOOK_1)]),
            |_| None,
        );
        assert_eq!(out.pool_stats[0].volume_token0_abs, "0");
        assert_eq!(out.pool_stats[0].volume_token1_abs, "0");
        assert_eq!(out.pool_stats[0].swap_count, 1);
    }
}
