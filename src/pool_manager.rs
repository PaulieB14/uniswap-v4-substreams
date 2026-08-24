//! PoolManager (0x4985…2b2b) event extraction — the core of the port.
//!
//! Scope note: this module is a pure decoder. Every row it emits is derivable
//! from the log itself, so unlike the subgraph it never needs a prior `Pool`
//! entity, token decimals, or an ETH/USD bundle to already exist. That matters
//! for completeness, not just tidiness: `handleInitialize` bails out entirely
//! when `fetchTokenDecimals` returns null, after which `handleSwap` and
//! `handleModifyLiquidity` log "Pool not found" and silently drop *every*
//! subsequent event for that pool, forever. Here nothing is dropped —
//! decimal/USD enrichment is a downstream concern, not a precondition for
//! indexing.
//!
//! Two more subgraph blind spots this module does not inherit:
//!   * `poolsToSkip` — a hardcoded per-chain denylist that erases pools wholesale.
//!   * aggregator-hook pools — `handleSwapHelper` deliberately does not persist a
//!     `Swap` entity when the pool's hook is in `usdStableStableHookAddresses`,
//!     deferring to the hook's own `HookSwap` event. We emit the PoolManager
//!     swap unconditionally, so `swaps` is a complete record of what the
//!     singleton actually executed.

use substreams::hex;
use substreams_ethereum::pb::eth::v2::Block;
use substreams_ethereum::Event;

use crate::abi::pool_manager::events;
use crate::hooks;
use crate::pb::uniswap::v4::v1 as pb;

/// Base mainnet PoolManager. Kept as a byte array so the hot-loop filter is a
/// 20-byte memcmp rather than a hex-string comparison — this loop sees every
/// log on Base.
const POOL_MANAGER: [u8; 20] = hex!("498581ff718922c3f8e6a244956af099b2652b2b");

/// Decode every PoolManager log in the block into `events`.
///
/// Rows are appended in log order, so `meta.log_index` is monotonically
/// increasing within each repeated field and a sink can rely on that ordering
/// without a secondary sort.
pub fn extract(blk: &Block, events: &mut pb::Events) {
    for log in blk.logs() {
        if log.address() != POOL_MANAGER.as_slice() {
            continue;
        }

        // Ordered by expected frequency: Swap dwarfs everything else on Base,
        // and each arm short-circuits on a topic0 compare, so the common case
        // costs one comparison.
        if let Some(ev) = events::Swap::match_and_decode(log) {
            let meta = hooks::meta(blk, log.receipt.transaction, log.log);
            events.swaps.push(pb::Swap {
                id: event_id(&meta),
                pool_id: hooks::pool_id_hex(&ev.id),
                // The contract that called into the singleton (a router,
                // usually) — NOT the EOA. `meta.origin` carries the EOA.
                sender: hooks::addr_hex(&ev.sender),
                // RAW V4 amounts, swapper-centric: negative = the swapper paid
                // that token in, positive = the swapper received it. The
                // subgraph negates both ("Unlike V3, a negative amount
                // represents that amount is being sent to the pool") to restore
                // V3's pool-centric convention *and* divides by token decimals.
                // We can do neither losslessly here — decimals need an RPC
                // round-trip we refuse to pay per swap — so we keep the
                // untouched on-chain integers and leave sign convention and
                // scaling to the sink. Flipping the sign downstream is exact;
                // recovering the raw integer from a rounded decimal is not.
                amount0: ev.amount0.to_string(),
                amount1: ev.amount1.to_string(),
                sqrt_price_x96: ev.sqrt_price_x96.to_string(),
                liquidity: ev.liquidity.to_string(),
                tick: ev.tick.to_i32(),
                // The fee ACTUALLY charged on this swap. A `beforeSwap` hook can
                // return an override, so this diverges from the pool's static
                // `fee_tier` on every dynamic-fee pool. The subgraph collapses
                // the two by writing this value back onto `pool.feeTier`,
                // destroying the pool's configured fee; keeping both is what
                // makes "effective fee over time per pool" answerable at all.
                fee: ev.fee.to_u64() as u32,
                meta: Some(meta),
                // Denormalised pool identity (token0/token1/fee_tier/
                // tick_spacing/hook) is left at proto3 default HERE ON PURPOSE.
                // This module sees exactly one block and a Swap log carries only
                // the poolId; the PoolKey was hashed away at Initialize, possibly
                // millions of blocks back. Filling these is `map_enriched`'s job,
                // reading `store_pools`. Writing a plausible zero address for
                // token0 here would be worse than empty: 0x0 is a REAL currency
                // on V4 (native ETH), so a guess would be indistinguishable from
                // the truth.
                ..Default::default()
            });
        } else if let Some(ev) = events::ModifyLiquidity::match_and_decode(log) {
            let meta = hooks::meta(blk, log.receipt.transaction, log.log);
            events.modify_liquidity.push(pb::ModifyLiquidity {
                id: event_id(&meta),
                pool_id: hooks::pool_id_hex(&ev.id),
                sender: hooks::addr_hex(&ev.sender),
                tick_lower: ev.tick_lower.to_i32(),
                tick_upper: ev.tick_upper.to_i32(),
                // int256, SIGNED: negative is a burn/withdraw, and roughly half
                // of all rows are negative. The ABI binding builds this with
                // `BigInt::from_signed_bytes_be`, so `to_string()` keeps the
                // minus sign — switching to the unsigned constructor would turn
                // every removal into a ~2^256 add and is the single easiest way
                // to corrupt this pipeline.
                liquidity_delta: ev.liquidity_delta.to_string(),
                // Position discriminator within (pool, owner, tick range).
                // Non-zero for PositionManager-managed positions. The subgraph
                // drops it, which makes its ModifyLiquidity rows ambiguous
                // whenever one sender holds two salted positions on one range.
                salt: bytes32_hex(&ev.salt),
                meta: Some(meta),
                // Same as Swap above: pool identity is filled by `map_enriched`
                // from `store_pools`, not here.
                ..Default::default()
            });
        } else if let Some(ev) = events::Initialize::match_and_decode(log) {
            let meta = hooks::meta(blk, log.receipt.transaction, log.log);
            let fee = ev.fee.to_u64();
            events.pools.push(pb::Pool {
                id: hooks::pool_id_hex(&ev.id),
                // V4 *currencies*, not ERC-20 addresses: currency0 is the zero
                // address for native-ETH pools. Emitted as-is rather than
                // rewritten to WETH, because the singleton really does hold ETH.
                token0: hooks::addr_hex(&ev.currency0),
                token1: hooks::addr_hex(&ev.currency1),
                // The pool key's configured fee, frozen at initialize time.
                // Never overwritten by later swaps (see the Swap arm above).
                fee_tier: fee,
                tick_spacing: ev.tick_spacing.to_i32(),
                // The headline divergence: the hook's permission set, decoded
                // from the low 14 bits of its own address. No RPC, no ABI, and
                // it works for a hook that has never been seen before.
                hook: Some(hooks::decode_hook(&ev.hooks)),
                sqrt_price: ev.sqrt_price_x96.to_string(),
                tick: ev.tick.to_i32(),
                // A pool is always initialised empty — the first ModifyLiquidity
                // is a separate event. Matches the subgraph's ZERO_BI seed.
                liquidity: "0".to_string(),
                is_dynamic_fee: hooks::is_dynamic_fee(fee),
                meta: Some(meta),
                // Filled later by map_enriched from store_tokens. Empty here on
                // purpose: at Initialize the token contracts have not been read
                // yet, and a placeholder symbol would outlive the gap.
                token0_symbol: String::new(),
                token1_symbol: String::new(),
                token0_decimals: 0,
                token1_decimals: 0,
                decimals_measured: false,
            });
        }
        // Donate / ERC-6909 Transfer, Approval, OperatorSet / ProtocolFeeUpdated
        // / ProtocolFeeControllerUpdated fall through deliberately — see the
        // UNHOMED EVENTS note at the bottom of this file. They are NOT squeezed
        // into pb::Swap or pb::PositionEvent, because every available shape
        // either loses a field outright or makes the row indistinguishable from
        // a real swap/position to a downstream aggregator.
    }
}

/// `<txHash>-<logIndex>`, byte-identical to the subgraph's event entity ids
/// (`transaction.id + '-' + event.logIndex`) so a sink can write over the
/// existing tables without an id migration. Built from `meta` rather than
/// re-hexing the hash, since `meta` already paid for that string.
fn event_id(meta: &pb::Meta) -> String {
    format!("{}-{}", meta.tx_hash, meta.log_index)
}

/// bytes32 → 0x-hex. Same formatting as `hooks::pool_id_hex`, aliased under an
/// honest name: a `salt` is not a pool id, and a reader should not have to
/// second-guess the call site.
fn bytes32_hex(b: &[u8; 32]) -> String {
    hooks::pool_id_hex(b)
}

// ---------------------------------------------------------------------------
// UNHOMED EVENTS — proposed proto additions (proto is frozen, so these are
// decoded by nobody today; the ABI bindings already exist, so each is a ~10
// line arm in the loop above the moment `Events` grows a field).
// ---------------------------------------------------------------------------
//
//   Donate(id, sender, amount0, amount1)
//     -> message Donate { id, pool_id, sender, amount0, amount1, meta }
//        Direct fee donation to in-range LPs. Cannot ride inside pb::Swap:
//        pb::Swap has no kind discriminator, so every donation would be counted
//        as swap volume by any consumer of `Events.swaps`. Zeroing
//        sqrt_price_x96 as a sentinel is not viable either — the subgraph
//        already writes legitimate swap rows with a zero sqrtPriceX96 for
//        aggregator-hook pools, so the sentinel is taken.
//
//   Transfer(caller, from, to, id, amount)      // ERC-6909 claim tokens
//   Approval(owner, spender, id, amount)
//   OperatorSet(owner, operator, approved)
//     -> message ClaimTokenEvent { id, kind, caller, owner, from, to, spender,
//                                  currency_id, amount, approved, meta }
//        This is the flash-accounting rail: how routers and searchers park
//        value *inside* the singleton between swaps instead of settling to
//        ERC-20. Nothing in the V4 subgraph surfaces it, and it is the cleanest
//        available signal for "who is running multi-hop/atomic strategies on
//        V4". pb::PositionEvent is the wrong home twice over: its `token_id`
//        means a PositionManager ERC-721 tokenId, whereas the ERC-6909 `id` is
//        a *currency* id (uint256 of the currency address) — overloading it
//        would poison `position_events` for the position_manager module — and
//        it has nowhere at all to put `amount`.
//
//   ProtocolFeeUpdated(id, protocol_fee)
//   ProtocolFeeControllerUpdated(protocol_fee_controller)
//     -> message ProtocolFeeEvent { id, kind, pool_id, protocol_fee,
//                                   controller, meta }
//        Governance-level fee config, and the only on-chain record of protocol
//        revenue being switched on for a pool. A partial pb::Pool row is not an
//        option: a sink doing an upsert on `Pool.id` cannot distinguish it from
//        a pool creation and would zero out token0/token1/hook.
