//! Uniswap V4 on Base — Substreams port of the `uniswap-v4-base-3` subgraph
//! (Qmbsc6XQWbiv4DfLVfaNciScqYLyDWUYjWzrFBbzzmRsMB).
//!
//! Two modules are exposed to the engine: `map_events`, which turns a block
//! into the `Events` envelope, and `db_out`, which renders that envelope as
//! Postgres `DatabaseChanges`. Everything else is an internal extractor.

mod abi;
mod arrakis;
mod db_out;
mod hooks;
mod pb;
mod pool_manager;
mod position_manager;

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
#[substreams::handlers::map]
pub fn map_events(blk: Block) -> Result<proto::Events, Error> {
    let mut events = proto::Events::default();

    pool_manager::extract(&blk, &mut events);
    position_manager::extract(&blk, &mut events);
    arrakis::extract(&blk, &mut events);

    Ok(events)
}
