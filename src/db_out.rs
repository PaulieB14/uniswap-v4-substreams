//! PostgreSQL sink writer for the Uniswap V4 (Base) pipeline.
//!
//! Emits `sf.substreams.sink.database.v1.DatabaseChanges`, which is the only
//! mode that gives mutable rows (see the rationale block at the top of
//! `db/schema.sql`). Every column name written here must exist in that file —
//! the sink resolves fields against the live catalogue and fails the stream on
//! an unknown column.
//!
//! WHY THE RAW `DatabaseChanges` BUILDER AND NOT `tables::Tables`
//! -------------------------------------------------------------
//! `Tables` keys its rows on (table, primary key) in a HashMap and *panics* if
//! the same key is touched twice with different operations
//! ("cannot create a row that was marked for update"). That is exactly what
//! this pipeline does: a pool can be initialised and swapped in the same
//! block, and a position NFT can be minted and transferred in the same block.
//! `DatabaseChanges::push_change` appends changes in emission order and lets
//! CREATE-then-UPDATE on one key work, which is what the chain actually does.

use substreams::errors::Error;
use substreams_database_change::pb::database::{
    table_change::Operation, DatabaseChanges, TableChange,
};

use crate::pb::uniswap::v4::v1 as pb;

#[substreams::handlers::map]
pub fn db_out(events: pb::Events) -> Result<DatabaseChanges, Error> {
    let mut changes = DatabaseChanges::default();

    // Order matters. Pool rows are pushed before anything that UPDATEs them so
    // that an Initialize and a Swap landing in the same block are applied
    // INSERT-then-UPDATE regardless of whether the sink preserves array order
    // or re-sorts on the ordinal (ordinals are log indexes, and a pool's
    // Initialize log always precedes any swap against it).
    for (idx, pool) in events.pools.iter().enumerate() {
        push_pool_create(&mut changes, pool, idx);
    }

    for (idx, hook) in events.hook_deployments.iter().enumerate() {
        push_hook_deployment(&mut changes, hook, idx);
    }

    for (idx, swap) in events.swaps.iter().enumerate() {
        push_swap(&mut changes, swap, idx);
    }

    for (idx, ml) in events.modify_liquidity.iter().enumerate() {
        push_modify_liquidity(&mut changes, ml, idx);
    }

    for (idx, pe) in events.position_events.iter().enumerate() {
        push_position_event(&mut changes, pe, idx);
    }

    Ok(changes)
}

// ---------------------------------------------------------------------------
// pool
// ---------------------------------------------------------------------------

fn push_pool_create(changes: &mut DatabaseChanges, pool: &pb::Pool, idx: usize) {
    if pool.id.is_empty() {
        // A pool row with no PoolId is unaddressable; dropping it beats
        // aborting the whole stream on a primary-key violation.
        return;
    }
    let ordinal = ordinal_of(pool.meta.as_ref(), idx);
    let empty = pb::Meta::default();
    let meta = pool.meta.as_ref().unwrap_or(&empty);

    let tc = changes.push_change("pool", &pool.id, ordinal, Operation::Create);
    tc.change("token0", ("", pool.token0.as_str()));
    tc.change("token1", ("", pool.token1.as_str()));
    tc.change("fee_tier", (0u64, pool.fee_tier));
    tc.change("tick_spacing", (0i32, pool.tick_spacing));
    tc.change("is_dynamic_fee", (false, pool.is_dynamic_fee));

    // Seed state from Initialize. `liquidity` is 0 at creation by definition —
    // the pool cannot have liquidity before it exists — and is thereafter
    // refreshed from the Swap event rather than accumulated from
    // ModifyLiquidity deltas (see schema.sql).
    tc.change("sqrt_price", ("0", numeric(&pool.sqrt_price)));
    tc.change("tick", (0i32, pool.tick));
    tc.change("liquidity", ("0", numeric(&pool.liquidity)));

    tc.change("created_at_block", (0u64, meta.block_number));
    tc.change("created_at_timestamp", (0u64, meta.block_timestamp));
    tc.change("created_at_tx", ("", meta.tx_hash.as_str()));

    let hook = pool.hook.as_ref();
    tc.change(
        "hook_address",
        ("", hook.map(|h| h.address.as_str()).unwrap_or("")),
    );
    tc.change("has_hook", (false, hook.map(|h| h.has_hook).unwrap_or(false)));
    set_hook_permissions(tc, hook);
}

/// Post-swap pool state. The PoolManager emits the authoritative sqrtPrice,
/// tick and active liquidity on every Swap, so current state is a straight
/// copy — no tick math, no store module, no read-modify-write.
///
/// This is an UPDATE, not an UPSERT: the manifest starts at the PoolManager's
/// own deploy block, so every pool that can be swapped was Initialized inside
/// the indexed range and its row already exists. If the sink is ever started
/// mid-history the UPDATE simply affects zero rows — no error, but the pool
/// table will be incomplete. Backfill from the deploy block instead.
fn push_pool_state_update(changes: &mut DatabaseChanges, swap: &pb::Swap, ordinal: u64) {
    if swap.pool_id.is_empty() {
        return;
    }
    let empty = pb::Meta::default();
    let meta = swap.meta.as_ref().unwrap_or(&empty);

    let tc = changes.push_change("pool", &swap.pool_id, ordinal, Operation::Update);
    tc.change("sqrt_price", ("0", numeric(&swap.sqrt_price_x96)));
    tc.change("tick", (0i32, swap.tick));
    tc.change("liquidity", ("0", numeric(&swap.liquidity)));
    tc.change("last_swap_block", (0u64, meta.block_number));
    tc.change("last_swap_timestamp", (0u64, meta.block_timestamp));
}

// ---------------------------------------------------------------------------
// event tables (immutable — always Operation::Create)
// ---------------------------------------------------------------------------

fn push_swap(changes: &mut DatabaseChanges, swap: &pb::Swap, idx: usize) {
    if swap.id.is_empty() {
        return;
    }
    let ordinal = ordinal_of(swap.meta.as_ref(), idx);
    {
        let tc = changes.push_change("swap", &swap.id, ordinal, Operation::Create);
        tc.change("pool_id", ("", swap.pool_id.as_str()));
        tc.change("sender", ("", swap.sender.as_str()));
        tc.change("amount0", ("0", numeric(&swap.amount0)));
        tc.change("amount1", ("0", numeric(&swap.amount1)));
        tc.change("sqrt_price_x96", ("0", numeric(&swap.sqrt_price_x96)));
        tc.change("liquidity", ("0", numeric(&swap.liquidity)));
        tc.change("tick", (0i32, swap.tick));
        // The fee actually charged, which a beforeSwap hook or a dynamic-fee
        // pool can move away from pool.fee_tier. The subgraph discards it.
        tc.change("fee", (0u32, swap.fee));
        set_meta(tc, swap.meta.as_ref());
    }
    push_pool_state_update(changes, swap, ordinal);
}

fn push_modify_liquidity(changes: &mut DatabaseChanges, ml: &pb::ModifyLiquidity, idx: usize) {
    if ml.id.is_empty() {
        return;
    }
    let ordinal = ordinal_of(ml.meta.as_ref(), idx);
    let tc = changes.push_change("modify_liquidity", &ml.id, ordinal, Operation::Create);
    tc.change("pool_id", ("", ml.pool_id.as_str()));
    tc.change("sender", ("", ml.sender.as_str()));
    tc.change("tick_lower", (0i32, ml.tick_lower));
    tc.change("tick_upper", (0i32, ml.tick_upper));
    tc.change("liquidity_delta", ("0", numeric(&ml.liquidity_delta)));
    tc.change("salt", ("", ml.salt.as_str()));
    set_meta(tc, ml.meta.as_ref());
}

fn push_position_event(changes: &mut DatabaseChanges, pe: &pb::PositionEvent, idx: usize) {
    if pe.id.is_empty() {
        return;
    }
    let ordinal = ordinal_of(pe.meta.as_ref(), idx);
    {
        let tc = changes.push_change("position_event", &pe.id, ordinal, Operation::Create);
        // token_id lands in a NUMERIC(78,0) column, so the mapper must emit the
        // uint256 as a decimal string (the subgraph's positionId() does exactly
        // that). "0" rather than "" on the empty path so the cast cannot fail.
        tc.change("token_id", ("0", numeric(&pe.token_id)));
        tc.change("kind", ("", pe.kind.as_str()));
        tc.change("address", ("", pe.address.as_str()));
        // `from`/`to` are SQL reserved words, hence the _address suffix.
        tc.change("from_address", ("", pe.from.as_str()));
        tc.change("to_address", ("", pe.to.as_str()));
        set_meta(tc, pe.meta.as_ref());
    }
    push_position_state(changes, pe, ordinal);
}

/// Current owner of a PositionManager NFT, reconstructed from the transfer
/// stream exactly as the subgraph's handleTransfer does: the mint (from ==
/// 0x0) creates the row and carries the immutable origin/createdAt, every
/// later transfer just moves `owner`.
///
/// A burn (to == 0x0) is left as an owner update rather than a DELETE so the
/// row stays joinable against position_event history.
fn push_position_state(changes: &mut DatabaseChanges, pe: &pb::PositionEvent, ordinal: u64) {
    // Subscribe/unsubscribe do not change ownership, and — matching the
    // subgraph, which never creates a Position from a Subscription — they must
    // not conjure a row either.
    if pe.kind != "transfer" || pe.token_id.is_empty() {
        return;
    }
    let empty = pb::Meta::default();
    let meta = pe.meta.as_ref().unwrap_or(&empty);

    let is_mint = is_zero_address(&pe.from);
    let op = if is_mint {
        Operation::Create
    } else {
        Operation::Update
    };

    let tc = changes.push_change("position", numeric(&pe.token_id), ordinal, op);
    tc.change("owner", ("", pe.to.as_str()));
    tc.change("last_transfer_block", (0u64, meta.block_number));
    if is_mint {
        tc.change("origin", ("", meta.origin.as_str()));
        tc.change("created_at_block", (0u64, meta.block_number));
        tc.change("created_at_timestamp", (0u64, meta.block_timestamp));
    }
}

fn push_hook_deployment(changes: &mut DatabaseChanges, hd: &pb::HookDeployment, idx: usize) {
    if hd.id.is_empty() {
        return;
    }
    let ordinal = ordinal_of(hd.meta.as_ref(), idx);
    let tc = changes.push_change("hook_deployment", &hd.id, ordinal, Operation::Create);
    tc.change("hook_address", ("", hd.hook.as_str()));
    tc.change("module", ("", hd.module.as_str()));
    tc.change("salt", ("", hd.salt.as_str()));
    tc.change("factory", ("", hd.factory.as_str()));
    set_hook_permissions(tc, hd.permissions.as_ref());
    set_meta(tc, hd.meta.as_ref());
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Block/tx provenance, flattened onto every event row. Written even when the
/// mapper left `meta` unset, because the columns are NOT NULL: an omitted
/// column makes the sink's INSERT fail the whole batch, whereas a zeroed one
/// is visibly wrong but recoverable.
fn set_meta(tc: &mut TableChange, meta: Option<&pb::Meta>) {
    let empty = pb::Meta::default();
    let m = meta.unwrap_or(&empty);
    tc.change("block_number", (0u64, m.block_number));
    tc.change("block_timestamp", (0u64, m.block_timestamp));
    tc.change("tx_hash", ("", m.tx_hash.as_str()));
    tc.change("log_index", (0u32, m.log_index));
    tc.change("origin", ("", m.origin.as_str()));
    tc.change("gas_used", (0u64, m.gas_used));
    tc.change("gas_price", ("0", numeric(&m.gas_price)));
}

/// The 14 hook capability bits plus the raw mask. Free to compute — V4 mines
/// the permission set into the low 14 bits of the hook address — so both
/// `pool` and `hook_deployment` carry them denormalised.
fn set_hook_permissions(tc: &mut TableChange, perms: Option<&pb::HookPermissions>) {
    let empty = pb::HookPermissions::default();
    let p = perms.unwrap_or(&empty);
    tc.change("hook_flags", (0u32, p.flags));
    tc.change("hook_before_initialize", (false, p.before_initialize));
    tc.change("hook_after_initialize", (false, p.after_initialize));
    tc.change("hook_before_add_liquidity", (false, p.before_add_liquidity));
    tc.change("hook_after_add_liquidity", (false, p.after_add_liquidity));
    tc.change(
        "hook_before_remove_liquidity",
        (false, p.before_remove_liquidity),
    );
    tc.change(
        "hook_after_remove_liquidity",
        (false, p.after_remove_liquidity),
    );
    tc.change("hook_before_swap", (false, p.before_swap));
    tc.change("hook_after_swap", (false, p.after_swap));
    tc.change("hook_before_donate", (false, p.before_donate));
    tc.change("hook_after_donate", (false, p.after_donate));
    tc.change(
        "hook_before_swap_returns_delta",
        (false, p.before_swap_returns_delta),
    );
    tc.change(
        "hook_after_swap_returns_delta",
        (false, p.after_swap_returns_delta),
    );
    tc.change(
        "hook_after_add_liquidity_returns_delta",
        (false, p.after_add_liquidity_returns_delta),
    );
    tc.change(
        "hook_after_remove_liquidity_returns_delta",
        (false, p.after_remove_liquidity_returns_delta),
    );
}

/// Guards the NUMERIC columns. The sink forwards field values as raw SQL
/// literals, so an empty string reaches Postgres as `''::numeric` and raises
/// `invalid input syntax for type numeric` — which kills the batch, not just
/// the row. Anything the mapper leaves blank becomes 0 instead.
fn numeric(value: &str) -> &str {
    if value.is_empty() {
        "0"
    } else {
        value
    }
}

/// True for 0x0…0 in either the `0x`-prefixed or bare-hex spelling, so the
/// mint detection does not depend on how the mapper formats addresses.
fn is_zero_address(value: &str) -> bool {
    let hex = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    !hex.is_empty() && hex.bytes().all(|b| b == b'0')
}

/// Ordinals order changes within a block. The log index is the natural choice:
/// it is already the chain's intra-block ordering, and it keeps a pool's
/// Initialize (low log index) ahead of any Swap that updates the same row.
/// Falls back to the vector position only if the mapper omitted `meta`.
fn ordinal_of(meta: Option<&pb::Meta>, fallback: usize) -> u64 {
    meta.map(|m| m.log_index as u64).unwrap_or(fallback as u64)
}
