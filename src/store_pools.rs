//! Pool metadata store — the join a V4 consumer cannot do without.
//!
//! ## The defect this exists to fix
//!
//! A V4 `Swap` log carries the `poolId` and nothing else about the pool. The
//! tokens, the configured fee, the tick spacing and the hook are all fields of
//! the `PoolKey`, which is keccak-hashed into the id at `initialize` time and
//! never emitted again. So a consumer streaming from a recent block sees
//!
//! ```text
//! swap  pool 0x86ca82ba…  amount0 -1000000  amount1 +42
//! ```
//!
//! and cannot say what was traded. The only place the answer exists on-chain is
//! that pool's `Initialize` event, which may be millions of blocks back.
//!
//! The source subgraph hides this: `handleSwap` calls `Pool.load(poolId)` and
//! graph-node silently serves it from the entity store. Substreams has no
//! implicit store — the join has to be an explicit module, and this is it.
//!
//! ## Shape
//!
//! `map_events` (decode) → `store_pools` (this: remember) → enriching map
//! (read back, denormalise onto each swap/modify_liquidity row). Standard
//! Substreams three-module cache shape, minus the RPC: everything stored here
//! came out of a log, so there is no `eth_call` and no per-block RPC budget.
//!
//! Divergence from the subgraph: the subgraph MUTATES its `Pool` entity on every
//! swap (`pool.feeTier`, `pool.liquidity`, `pool.sqrtPrice`, tx counts), so its
//! stored pool is a moving current-state row. This store holds the pool AS
//! INITIALISED — the immutable half of the PoolKey plus the seed state. That is
//! deliberate: the fields a swap row needs to describe itself (token0, token1,
//! fee_tier, tick_spacing, hook) are exactly the fields that can never change,
//! and keeping mutable state out means a reader never has to reason about
//! whether it got the value as of this block or as of the last write.

use substreams::store::{StoreNew, StoreSet, StoreSetProto};

use crate::pb::uniswap::v4::v1 as pb;

/// Namespace for pool records. The store is a single flat keyspace shared by
/// whatever else lands in it later, so every key is prefixed by what it holds
/// rather than being a bare id — and `delete_prefix("pool:")` stays available.
pub const POOL_KEY_PREFIX: &str = "pool:";

/// Build the store key for a pool id.
///
/// A function rather than an inline `format!` at each call site because the
/// writer here and every future reader must agree on the string byte-for-byte;
/// a store lookup that disagrees does not error, it just returns `None`, and the
/// resulting swap rows are silently un-enriched. `pool_id` is already
/// 0x-prefixed lowercase hex from `hooks::pool_id_hex` — do not re-case it.
pub fn pool_key(pool_id: &str) -> String {
    format!("{}{}", POOL_KEY_PREFIX, pool_id)
}

/// Remember every pool the block initialised.
///
/// ### Why `StoreSetProto<pb::Pool>` and not `StoreSetString`
///
/// The whole pool record is needed downstream, not one field of it: the
/// enriching module denormalises `token0`, `token1`, `fee_tier`, `tick_spacing`
/// and the entire 17-field `HookPermissions` submessage onto each swap. Packing
/// that into a delimited string would invent a SECOND serialisation format for
/// data that already has one — a format with no schema, whose writer and reader
/// live in different modules, and which silently corrupts the moment a field is
/// added to `Pool` (as fields were added to `Swap` in this very change) or a
/// token address someday contains the delimiter. `StoreSetProto` reuses the
/// prost encoding the proto contract already defines, so adding a field to
/// `Pool` needs no edit here and cannot desynchronise the two ends.
///
/// The cost is real but small: the value is a few hundred bytes of protobuf
/// instead of a ~120-byte string, written once per pool for the life of the
/// chain, never per swap.
///
/// ### Why `set` and not `set_if_not_exists`
///
/// V4's `initialize` reverts on an already-initialised pool, so a given poolId
/// can only ever produce one `Initialize` log and the two policies are
/// behaviourally identical today. `set` is chosen because it keeps the door open
/// for the store to carry a pool's mutable state (liquidity, sqrt_price) later
/// without a policy change, which would be a module-hash break for every
/// consumer. Note the flip side: if this module is ever fed a source that
/// re-emits pools, `set` will overwrite. That is why it reads only
/// `events.pools` — the `Initialize` rows — and not any other repeated field.
#[substreams::handlers::store]
pub fn store_pools(events: pb::Events, store: StoreSetProto<pb::Pool>) {
    // Ordinals must be non-decreasing within a block: they tell the engine where
    // a write sits relative to the block's other operations, so a downstream
    // `get_at`/`get_last` at a given ordinal sees exactly the writes that
    // preceded it in chain order. `pool_manager::extract` appends in `blk.logs()`
    // order, so `meta.log_index` (block-scoped, per `hooks::meta`) is already
    // monotonic here. The running max only defends the one case that would break
    // it — a `Pool` arriving with `meta: None`, which would otherwise read as
    // ordinal 0 after a larger one.
    let mut last_ord: u64 = 0;

    for pool in &events.pools {
        let ord = pool
            .meta
            .as_ref()
            .map(|m| m.log_index as u64)
            .unwrap_or(last_ord)
            .max(last_ord);
        last_ord = ord;

        // Pass `pool` by reference: `StoreSet::set` takes `&V` and prost encodes
        // straight from the borrow, so the record is never cloned.
        store.set(ord, pool_key(&pool.id), pool);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Only the pure key builder is unit-testable here. The handler itself writes
    // through the WASM host's state externs, which are no-ops off wasm32, so
    // calling it natively would assert nothing. Its behaviour is covered by
    // running the packed module (see README).

    #[test]
    fn key_is_prefixed_and_verbatim() {
        let id = "0x86ca82ba1b1b0e0a2f2e0e1e0d0c0b0a09080706050403020100fedcba987654";
        assert_eq!(pool_key(id), format!("pool:{}", id));
        // The id must survive untouched — a reader looks up the same string it
        // read off the swap row.
        assert!(pool_key(id).ends_with(id));
    }

    #[test]
    fn prefix_is_stable() {
        // Guards the writer/reader contract: changing this constant silently
        // un-enriches every downstream row rather than failing.
        assert_eq!(POOL_KEY_PREFIX, "pool:");
    }
}
