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
//!
//! WHAT THIS MODULE CONSUMES
//! -------------------------
//! `map_enriched`, NOT `map_events` (see substreams.yaml). The two have the
//! same `Events` type, so pointing it at the wrong one is a silent downgrade
//! rather than a build error: every `swap.token0` / `fee_tier` / `hook_address`
//! column would be written empty and the `pool_stats` / `hook_stats` tables
//! would never receive a row. If those columns come out blank in Postgres,
//! check the manifest input first.

use substreams::errors::Error;
use substreams_database_change::pb::sf::substreams::sink::database::v1::{
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

    // Previously-unhomed PoolManager events. These loops are live but will not
    // fire yet: pool_manager.rs still lets Donate / ERC-6909 / ProtocolFee logs
    // fall through undecoded, so the repeated fields arrive empty. Wiring them
    // now means the decoder is a mapping change, not a schema migration on a
    // running sink.
    for (idx, d) in events.donates.iter().enumerate() {
        push_donate(&mut changes, d, idx);
    }

    for (idx, ct) in events.claim_token_events.iter().enumerate() {
        push_claim_token_event(&mut changes, ct, idx);
    }

    for (idx, pf) in events.protocol_fee_events.iter().enumerate() {
        push_protocol_fee_event(&mut changes, pf, idx);
    }

    // Aggregates last. They summarise the block's event rows, so applying them
    // after those rows keeps a mid-batch reader from seeing a summary that
    // counts events it cannot yet see. See `stats_ordinal`.
    for (idx, ps) in events.pool_stats.iter().enumerate() {
        push_pool_stats(&mut changes, ps, idx);
    }

    for (idx, hs) in events.hook_stats.iter().enumerate() {
        push_hook_stats(&mut changes, hs, idx);
    }

    for (i, pt) in events.pool_totals.iter().enumerate() {
        push_pool_totals(&mut changes, pt, i);
    }
    for (i, ht) in events.hook_totals.iter().enumerate() {
        push_hook_totals(&mut changes, ht, i);
    }

    // One row per distinct token the block resolved. Written from the enriched
    // swaps rather than a dedicated message: store_tokens already resolved the
    // metadata and attached it here, so a separate proto type would just be a
    // second copy to keep in sync.
    let mut seen: std::collections::BTreeMap<&str, (&str, u32, bool, u64)> =
        std::collections::BTreeMap::new();
    // Pools first: a pool exists from Initialize and may never trade, so
    // collecting only from swaps would leave a freshly created pair's tokens
    // absent from the token table indefinitely.
    for p in &events.pools {
        let blk = p.meta.as_ref().map(|m| m.block_number).unwrap_or(0);
        for (addr, sym, dec) in [
            (p.token0.as_str(), p.token0_symbol.as_str(), p.token0_decimals),
            (p.token1.as_str(), p.token1_symbol.as_str(), p.token1_decimals),
        ] {
            if addr.is_empty() {
                continue;
            }
            seen.entry(addr).or_insert((sym, dec, p.decimals_measured, blk));
        }
    }
    for sw in &events.swaps {
        let blk = sw.meta.as_ref().map(|m| m.block_number).unwrap_or(0);
        for (addr, sym, dec) in [
            (sw.token0.as_str(), sw.token0_symbol.as_str(), sw.token0_decimals),
            (sw.token1.as_str(), sw.token1_symbol.as_str(), sw.token1_decimals),
        ] {
            if addr.is_empty() {
                continue;
            }
            seen.entry(addr).or_insert((sym, dec, sw.decimals_measured, blk));
        }
    }
    for (i, (addr, (sym, dec, measured, blk))) in seen.iter().enumerate() {
        // Upsert, not Create: this is the only table whose primary key recurs
        // across blocks (WETH appears in most of them), and the sink refuses a
        // duplicate PK — both in-batch and at the database constraint. Must be
        // Upsert on EVERY write, never mixed with Create for this table.
        let tc = changes.push_change("token", addr, stats_ordinal(i), Operation::Upsert);
        tc.change("symbol", ("", *sym));
        tc.change("decimals", (0u32, *dec));
        tc.change("decimals_measured", (false, *measured));
        // last_seen, not first_seen: an upsert overwrites, and TableChange has
        // no min()/set_if_null() (those live on the Row builder this module does
        // not use). Naming it for what it actually holds beats a column that
        // silently means the opposite of its name.
        tc.change("last_seen_block", (0u64, *blk));
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
    tc.change("token0_symbol", ("", pool.token0_symbol.as_str()));
    tc.change("token1_symbol", ("", pool.token1_symbol.as_str()));
    tc.change("token0_decimals", (0u32, pool.token0_decimals));
    tc.change("token1_decimals", (0u32, pool.token1_decimals));
    tc.change("decimals_measured", (false, pool.decimals_measured));
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
        // Denormalised pool identity, filled upstream by map_enriched from
        // store_pools. All empty/zero if this module was wired to map_events by
        // mistake, or if the pool was genuinely not in the store.
        tc.change("token0", ("", swap.token0.as_str()));
        tc.change("token1", ("", swap.token1.as_str()));
        tc.change("fee_tier", (0u64, swap.fee_tier));
        tc.change("tick_spacing", (0i32, swap.tick_spacing));
        tc.change("token0_symbol", ("", swap.token0_symbol.as_str()));
        tc.change("token1_symbol", ("", swap.token1_symbol.as_str()));
        tc.change("token0_decimals", (0u32, swap.token0_decimals));
        tc.change("token1_decimals", (0u32, swap.token1_decimals));
        tc.change("decimals_measured", (false, swap.decimals_measured));
        // USD and human-readable amounts. Populated only when the price store
        // could anchor the swap; `priced` separates "not anchored" from
        // "genuinely zero", which a bare 0 in amount_usd cannot.
        tc.change("amount0_adjusted", ("0", numeric(&swap.amount0_adjusted)));
        tc.change("amount1_adjusted", ("0", numeric(&swap.amount1_adjusted)));
        tc.change("amounts_adjusted", (false, swap.amounts_adjusted));
        tc.change("amount0_usd", ("0", numeric(&swap.amount0_usd)));
        tc.change("amount1_usd", ("0", numeric(&swap.amount1_usd)));
        tc.change("amount_usd", ("0", numeric(&swap.amount_usd)));
        tc.change("native_price_usd", ("0", numeric(&swap.native_price_usd)));
        tc.change("priced", (false, swap.priced));
        tc.change("amount0_priced", (false, swap.amount0_priced));
        tc.change("amount1_priced", (false, swap.amount1_priced));
        set_hook_identity(tc, swap.hook.as_ref());
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
    tc.change("token0", ("", ml.token0.as_str()));
    tc.change("token1", ("", ml.token1.as_str()));
    tc.change("fee_tier", (0u64, ml.fee_tier));
    tc.change("tick_spacing", (0i32, ml.tick_spacing));
    set_hook_identity(tc, ml.hook.as_ref());
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
// per-block aggregates
// ---------------------------------------------------------------------------

/// One row per (pool, block) touched.
///
/// These are DELTAS, not running totals — `map_enriched` is a stateless `map`
/// re-executed out of order by parallel backfill workers, so it can only report
/// what happened in the block it was handed. `swap_count`,
/// `modify_liquidity_count` and both volumes SUM correctly over a block range;
/// nothing else here is additive. Spelled out in db/schema.sql too, because a
/// consumer reading `swap_count` as lifetime volume gets a plausible wrong
/// number rather than an error.
fn push_pool_stats(changes: &mut DatabaseChanges, ps: &pb::PoolStats, idx: usize) {
    if ps.pool_id.is_empty() {
        return;
    }
    // "<pool_id>-<block>". A synthetic single-column key rather than a
    // composite PK: `push_change_composite` exists, but sink support for
    // composite keys has varied across versions and every other table here is
    // on the single-key path. Both components are also written as real columns,
    // so nothing has to parse the id back apart.
    let id = format!("{}-{}", ps.pool_id, ps.last_block);

    let tc = changes.push_change("pool_stats", &id, stats_ordinal(idx), Operation::Create);
    tc.change("pool_id", ("", ps.pool_id.as_str()));
    tc.change("block_number", (0u64, ps.last_block));
    tc.change("token0", ("", ps.token0.as_str()));
    tc.change("token1", ("", ps.token1.as_str()));
    tc.change("swap_count", (0u64, ps.swap_count));
    tc.change("modify_liquidity_count", (0u64, ps.modify_liquidity_count));
    tc.change("volume_token0_abs", ("0", numeric(&ps.volume_token0_abs)));
    tc.change("volume_token1_abs", ("0", numeric(&ps.volume_token1_abs)));
    set_hook_identity(tc, ps.hook.as_ref());
}

/// One row per (hook, block) touched — the roll-up the source subgraph cannot
/// produce at all, since it stores `hooks` as an opaque string.
///
/// `pool_count` and `distinct_fee_values` are set cardinalities scoped to this
/// one block and do NOT sum across blocks; see db/schema.sql for the queries
/// that answer those correctly from the base tables.
fn push_hook_stats(changes: &mut DatabaseChanges, hs: &pb::HookStats, idx: usize) {
    if hs.hook_address.is_empty() {
        return;
    }
    let id = format!("{}-{}", hs.hook_address, hs.last_block);

    let tc = changes.push_change("hook_stats", &id, stats_ordinal(idx), Operation::Create);
    tc.change("hook_address", ("", hs.hook_address.as_str()));
    tc.change("block_number", (0u64, hs.last_block));
    tc.change("pool_count", (0u64, hs.pool_count));
    tc.change("swap_count", (0u64, hs.swap_count));
    tc.change("volume_token0_abs", ("0", numeric(&hs.volume_token0_abs)));
    tc.change("volume_token1_abs", ("0", numeric(&hs.volume_token1_abs)));
    tc.change("distinct_fee_values", (0u64, hs.distinct_fee_values));
    // The full 14 booleans here, unlike on `swap`: hook_stats is a small table
    // (one row per active hook per block) and it is the natural place to filter
    // hooks BY capability, so the join to `pool` is worth eliminating.
    set_hook_permissions(tc, hs.permissions.as_ref());
}

// ---------------------------------------------------------------------------
// previously-unhomed PoolManager events
//
// All three writers are complete but currently unreachable: pool_manager.rs
// does not decode Donate / ERC-6909 / ProtocolFee logs yet, so the repeated
// fields arrive empty. Do not read an empty `donate` table as "V4 has no
// donations on Base".
// ---------------------------------------------------------------------------

fn push_donate(changes: &mut DatabaseChanges, d: &pb::Donate, idx: usize) {
    if d.id.is_empty() {
        return;
    }
    let ordinal = ordinal_of(d.meta.as_ref(), idx);
    let tc = changes.push_change("donate", &d.id, ordinal, Operation::Create);
    tc.change("pool_id", ("", d.pool_id.as_str()));
    tc.change("sender", ("", d.sender.as_str()));
    tc.change("amount0", ("0", numeric(&d.amount0)));
    tc.change("amount1", ("0", numeric(&d.amount1)));
    set_meta(tc, d.meta.as_ref());
    // Deliberately no pool-state UPDATE. A donation carries no authoritative
    // post-event sqrtPrice/tick/liquidity — unlike a Swap, which is why only
    // Swap refreshes the pool row.
}

fn push_claim_token_event(changes: &mut DatabaseChanges, ct: &pb::ClaimTokenEvent, idx: usize) {
    if ct.id.is_empty() {
        return;
    }
    let ordinal = ordinal_of(ct.meta.as_ref(), idx);
    let tc = changes.push_change("claim_token_event", &ct.id, ordinal, Operation::Create);
    tc.change("kind", ("", ct.kind.as_str()));
    tc.change("caller", ("", ct.caller.as_str()));
    // `owner` is a reserved-ish word and `position.owner` already exists with a
    // different meaning; the column is owner_address.
    tc.change("owner_address", ("", ct.owner.as_str()));
    tc.change("from_address", ("", ct.from.as_str()));
    tc.change("to_address", ("", ct.to.as_str()));
    tc.change("spender", ("", ct.spender.as_str()));
    tc.change("operator", ("", ct.operator.as_str()));
    // uint256 decimal strings into NUMERIC(78,0). Fields the event's `kind`
    // does not carry arrive as proto3 empty and `numeric()` turns them into 0,
    // which is what the column defaults to anyway.
    tc.change("currency_id", ("0", numeric(&ct.currency_id)));
    tc.change("amount", ("0", numeric(&ct.amount)));
    tc.change("approved", (false, ct.approved));
    set_meta(tc, ct.meta.as_ref());
}

fn push_protocol_fee_event(changes: &mut DatabaseChanges, pf: &pb::ProtocolFeeEvent, idx: usize) {
    if pf.id.is_empty() {
        return;
    }
    let ordinal = ordinal_of(pf.meta.as_ref(), idx);
    let tc = changes.push_change("protocol_fee_event", &pf.id, ordinal, Operation::Create);
    tc.change("kind", ("", pf.kind.as_str()));
    // Empty for kind='controller_updated' — a controller change is global, not
    // per-pool. Written rather than omitted because the column is NOT NULL.
    tc.change("pool_id", ("", pf.pool_id.as_str()));
    // uint24 kept PACKED (low 12 bits = fee 0->1, high 12 = fee 1->0). The
    // split is a v4-core encoding detail; the raw value is always recoverable.
    tc.change("protocol_fee", (0u32, pf.protocol_fee));
    tc.change("controller", ("", pf.controller.as_str()));
    set_meta(tc, pf.meta.as_ref());
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Hook identity in its compact form: address plus the raw permission MASK.
///
/// Used on the high-volume tables (`swap`, `modify_liquidity`, `pool_stats`)
/// instead of the 14 unpacked booleans. No information is lost — `hook_flags`
/// IS the mask those booleans are decoded from, so `hook_flags & 128 <> 0` is
/// exactly `hook_before_swap` — and it saves ~14 bytes on every row of a table
/// assumed to reach billions.
///
/// Note `has_hook` is intentionally NOT stored: it is not derivable from the
/// flags (a hook may legally mine an address with no permission bits set), so
/// deriving it from the ADDRESS is the only correct route, and the address is
/// already here. See db/schema.sql for the predicate.
fn set_hook_identity(tc: &mut TableChange, perms: Option<&pb::HookPermissions>) {
    let empty = pb::HookPermissions::default();
    let p = perms.unwrap_or(&empty);
    tc.change("hook_address", ("", p.address.as_str()));
    tc.change("hook_flags", (0u32, p.flags));
}

/// Ordinal for a block-level aggregate row.
///
/// Ordinals order changes within a block, and every other row here uses its log
/// index. A stats row has no log index — it summarises the whole block — so it
/// is placed above any plausible log index so the summary applies after the
/// rows it summarises. Base blocks carry low thousands of logs at most; 1e6 is
/// four orders of magnitude of headroom and still nowhere near u64 overflow.
fn stats_ordinal(idx: usize) -> u64 {
    1_000_000u64 + idx as u64
}

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

/// Lifetime totals, maintained by the add-policy stores.
///
/// Distinct from `push_pool_stats`, which writes per-block deltas. Both are
/// emitted because they answer different questions and neither derives from the
/// other cheaply: deltas SUM over an arbitrary range, totals give "as of now"
/// without scanning history.
fn push_pool_totals(changes: &mut DatabaseChanges, pt: &pb::PoolTotals, idx: usize) {
    if pt.pool_id.is_empty() {
        return;
    }
    let id = format!("{}-{}", pt.pool_id, pt.last_block);
    let tc = changes.push_change("pool_totals", &id, stats_ordinal(idx), Operation::Create);
    tc.change("pool_id", ("", pt.pool_id.as_str()));
    tc.change("block_number", (0u64, pt.last_block));
    tc.change("token0", ("", pt.token0.as_str()));
    tc.change("token1", ("", pt.token1.as_str()));
    tc.change(
        "hook_address",
        ("", pt.hook.as_ref().map(|h| h.address.as_str()).unwrap_or("")),
    );
    tc.change("swap_count", ("0", numeric(&pt.swap_count)));
    tc.change("modify_liquidity_count", ("0", numeric(&pt.modify_liquidity_count)));
    tc.change("volume_token0_abs", ("0", numeric(&pt.volume_token0_abs)));
    tc.change("volume_token1_abs", ("0", numeric(&pt.volume_token1_abs)));
}

fn push_hook_totals(changes: &mut DatabaseChanges, ht: &pb::HookTotals, idx: usize) {
    if ht.hook_address.is_empty() {
        return;
    }
    let id = format!("{}-{}", ht.hook_address, ht.last_block);
    let tc = changes.push_change("hook_totals", &id, stats_ordinal(idx), Operation::Create);
    tc.change("hook_address", ("", ht.hook_address.as_str()));
    tc.change("block_number", (0u64, ht.last_block));
    tc.change("pool_count", ("0", numeric(&ht.pool_count)));
    tc.change("swap_count", ("0", numeric(&ht.swap_count)));
    tc.change("modify_liquidity_count", ("0", numeric(&ht.modify_liquidity_count)));
    tc.change("volume_token0_abs", ("0", numeric(&ht.volume_token0_abs)));
    tc.change("volume_token1_abs", ("0", numeric(&ht.volume_token1_abs)));
}
