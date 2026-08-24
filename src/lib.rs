//! Uniswap V4 on Base — Substreams port of the `uniswap-v4-base-3` subgraph
//! (Qmbsc6XQWbiv4DfLVfaNciScqYLyDWUYjWzrFBbzzmRsMB).
//!
//! # Module graph
//!
//! ```text
//!   sf.ethereum.type.v2.Block
//!            |
//!        map_events        decode PoolManager / PositionManager / Arrakis logs
//!         |      \
//!         |    store_pools      remember every Initialize, keyed pool:<poolId>
//!         |      /
//!       map_enriched       join the two: denormalise the PoolKey onto every
//!            |             swap / modify_liquidity row, emit PoolStats+HookStats
//!         db_out           render as Postgres DatabaseChanges
//! ```
//!
//! ## Why the store exists at all
//!
//! A V4 `Swap` log carries the **poolId and nothing else about the pool**. The
//! tokens, the configured fee, the tick spacing and the hook are fields of the
//! `PoolKey`, which is keccak-hashed into the id at `initialize` and never
//! re-emitted. A consumer streaming from a recent block therefore cannot say
//! what a swap traded. The subgraph papers over this with `Pool.load(poolId)`
//! against graph-node's implicit entity store; Substreams has no implicit
//! state, so the join has to be an explicit store module. That is the
//! correctness fix this package now carries, and it is why `db_out` consumes
//! `map_enriched` rather than `map_events`.
//!
//! ## What is exposed to the engine
//!
//! `map_events`, `store_pools`, `map_enriched`, `store_tokens` and `db_out` are
//! declared in `substreams.yaml`. `map_events` is kept as a public module and
//! NOT folded into `map_enriched`: it is the cacheable, store-free stage, so a
//! consumer that only wants raw decoded logs pays nothing for the join, and
//! re-running the enrichment does not re-decode the chain.

mod abi;
mod arrakis;
mod db_out;
mod enrich;
mod hooks;
mod pb;
mod pool_manager;
mod position_manager;
mod store_pools;
mod tokens;

use substreams::errors::Error;
use substreams_ethereum::pb::eth::v2::Block;

use crate::pb::uniswap::v4::v1 as proto;

// Registers a custom getrandom that always errors. Without it, anything in the
// dependency tree that reaches for entropy fails to LINK on
// wasm32-unknown-unknown rather than failing at runtime.
substreams_ethereum::init!();

/// Single pass over the block, fanned out to the three contract extractors.
///
/// They share one `Events` accumulator instead of returning their own and
/// being merged: each already walks `blk.logs()` and appends to a distinct
/// repeated field, so there is nothing to reconcile and no intermediate
/// allocation.
///
/// Extractor order fixes the order of the repeated fields, but not the order
/// changes are applied downstream — `db_out` ordinals every row by its
/// block-scoped log index, so the sink replays a block in true chain order
/// regardless of how the events are grouped here.
///
/// Output of this module leaves `Swap`/`ModifyLiquidity` pool identity at
/// proto3 defaults; see `enrich` for why and where they are filled.
#[substreams::handlers::map]
pub fn map_events(blk: Block) -> Result<proto::Events, Error> {
    let mut events = proto::Events::default();

    pool_manager::extract(&blk, &mut events);
    position_manager::extract(&blk, &mut events);
    arrakis::extract(&blk, &mut events);

    Ok(events)
}
