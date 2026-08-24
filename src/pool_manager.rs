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
use substreams::log;
use substreams::scalar::BigInt;
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

        // Ordered by MEASURED frequency, not by how the ABI lists them. Over
        // Base blocks 35000000-35001999 this loop saw 87,732 ModifyLiquidity,
        // 15,000 Swap, 1,169 Initialize, 1,113 ERC-6909 Transfer and 0 of the
        // remaining five; Donate ran at 4 per 10,000 blocks nearby. Swap stays
        // first anyway because it is the arm every downstream consumer cares
        // about and the gap to ModifyLiquidity is not worth the churn.
        //
        // Each arm short-circuits on a topic0 compare, so ordering only decides
        // how many 32-byte compares a log costs, and the rare governance events
        // at the bottom cost the full nine.
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
        } else if let Some(ev) = events::Transfer::match_and_decode(log) {
            // ERC-6909 claim-token move — V4's flash-accounting rail. A router
            // that finishes a lock holding a credit can `mint` claim tokens
            // instead of settling out to ERC-20, then move them for free later.
            // The singleton IS the token contract, so these logs share an
            // address with Swap; there is no separate ERC-20 to watch, which is
            // why nothing outside this loop can pick them up.
            let meta = hooks::meta(blk, log.receipt.transaction, log.log);
            events.claim_token_events.push(pb::ClaimTokenEvent {
                id: event_id(&meta),
                kind: KIND_TRANSFER.to_string(),
                // ERC-6909 carries BOTH `caller` (msg.sender) and `from`, and
                // they diverge exactly when an operator or an approved spender
                // moves someone else's balance — the case worth indexing. So
                // neither is folded into the other.
                caller: hooks::addr_hex(&ev.caller),
                from: hooks::addr_hex(&ev.from),
                to: hooks::addr_hex(&ev.to),
                currency_id: currency_id_decimal(&ev.id),
                amount: ev.amount.to_string(),
                meta: Some(meta),
                // owner / spender / operator / approved are not fields of
                // Transfer; left at proto3 default rather than aliased onto
                // `from`, so `kind` alone tells a consumer which columns mean
                // anything on this row.
                ..Default::default()
            });
        } else if let Some(ev) = events::Donate::match_and_decode(log) {
            // A direct fee donation to the pool's in-range LPs. Kept out of
            // `swaps` on purpose: pb::Swap has no kind discriminator, so a
            // donation folded in there would be booked as swap volume by every
            // consumer of Events.swaps, and the obvious sentinel (a zero
            // sqrtPriceX96) is already taken by legitimate aggregator-hook swap
            // rows.
            let meta = hooks::meta(blk, log.receipt.transaction, log.log);
            events.donates.push(pb::Donate {
                id: event_id(&meta),
                // Donate's `id` IS a PoolId (bytes32 keccak of the PoolKey) —
                // the same identifier Swap and ModifyLiquidity carry, and NOT
                // the ERC-6909 currency id decoded two arms up. Same Solidity
                // parameter name, completely different type.
                pool_id: hooks::pool_id_hex(&ev.id),
                sender: hooks::addr_hex(&ev.sender),
                // uint256 and UNSIGNED, unlike Swap.amount0/1: a donation only
                // ever adds. No sign convention to reason about, so these go
                // out as the raw integers with no negation.
                amount0: ev.amount0.to_string(),
                amount1: ev.amount1.to_string(),
                // Filled by map_enriched once the pool store resolves the token
                // addresses and store_tokens supplies decimals. Empty here, not
                // zero: "unknown" and "donated nothing" must not look alike.
                token0: String::new(),
                token1: String::new(),
                amount0_adjusted: String::new(),
                amount1_adjusted: String::new(),
                amounts_adjusted: false,
                meta: Some(meta),
            });
        } else if let Some(ev) = events::Approval::match_and_decode(log) {
            // Per-currency allowance on claim tokens. Distinct from OperatorSet
            // below: an approval is scoped to ONE currency id and a finite
            // amount, an operator is unlimited across every currency.
            let meta = hooks::meta(blk, log.receipt.transaction, log.log);
            events.claim_token_events.push(pb::ClaimTokenEvent {
                id: event_id(&meta),
                kind: KIND_APPROVAL.to_string(),
                owner: hooks::addr_hex(&ev.owner),
                spender: hooks::addr_hex(&ev.spender),
                currency_id: currency_id_decimal(&ev.id),
                // Routinely 2^256-1 (infinite approval). Emitted verbatim
                // rather than clamped — the column is NUMERIC(78,0), which
                // holds it, and "infinite" is a UI decision, not a decode one.
                amount: ev.amount.to_string(),
                meta: Some(meta),
                ..Default::default()
            });
        } else if let Some(ev) = events::OperatorSet::match_and_decode(log) {
            let meta = hooks::meta(blk, log.receipt.transaction, log.log);
            events.claim_token_events.push(pb::ClaimTokenEvent {
                id: event_id(&meta),
                kind: KIND_OPERATOR_SET.to_string(),
                owner: hooks::addr_hex(&ev.owner),
                operator: hooks::addr_hex(&ev.operator),
                // `approved` is a SET, not a grant: false is a revocation and
                // is just as meaningful as true, so the row is emitted either
                // way and the boolean is carried rather than filtered on.
                approved: ev.approved,
                meta: Some(meta),
                // currency_id stays at proto3 default (-> 0 in Postgres) and
                // that is not a currency: an operator is authorised across
                // EVERY id at once, so there is no single currency to name.
                // Read `currency_id` only when kind <> 'operator_set'.
                ..Default::default()
            });
        } else if let Some(ev) = events::ProtocolFeeUpdated::match_and_decode(log) {
            // Governance switching protocol revenue on/off for one pool — the
            // only on-chain record of it. Not folded into the `pool` row: a
            // sink upserting on Pool.id could not tell a fee update from a pool
            // creation and would blank token0/token1/hook.
            let meta = hooks::meta(blk, log.receipt.transaction, log.log);
            events.protocol_fee_events.push(pb::ProtocolFeeEvent {
                id: event_id(&meta),
                kind: KIND_FEE_UPDATED.to_string(),
                // bytes32 PoolId again, as with Donate.
                pool_id: hooks::pool_id_hex(&ev.id),
                // uint24 kept PACKED: low 12 bits = fee charged on 0->1, high
                // 12 bits = fee on 1->0. Splitting here would bake a v4-core
                // encoding detail into the schema; the packed value is always
                // recoverable, a pre-split pair is not. `to_u64()` cannot
                // overflow — the ABI type is uint24.
                protocol_fee: ev.protocol_fee.to_u64() as u32,
                controller: String::new(),
                meta: Some(meta),
            });
        } else if let Some(ev) = events::ProtocolFeeControllerUpdated::match_and_decode(log) {
            let meta = hooks::meta(blk, log.receipt.transaction, log.log);
            events.protocol_fee_events.push(pb::ProtocolFeeEvent {
                id: event_id(&meta),
                kind: KIND_CONTROLLER_UPDATED.to_string(),
                // GLOBAL, not per-pool: this swaps the contract that is allowed
                // to set protocol fees on every pool at once. pool_id stays
                // empty, which is why the schema's pool_id index is partial
                // (WHERE pool_id <> '').
                pool_id: String::new(),
                protocol_fee: 0,
                controller: hooks::addr_hex(&ev.protocol_fee_controller),
                meta: Some(meta),
            });
        }
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
// ERC-6909 currency ids, and the trap under them
// ---------------------------------------------------------------------------

/// `ClaimTokenEvent.kind` discriminators.
///
/// Constants rather than inline literals because db/schema.sql pins these exact
/// spellings into a `VARCHAR(16)` column. A typo would not fail anything — it
/// would silently make every consumer's `WHERE kind = 'transfer'` match fewer
/// rows than exist, which is the worst failure mode available.
const KIND_TRANSFER: &str = "transfer";
const KIND_APPROVAL: &str = "approval";
const KIND_OPERATOR_SET: &str = "operator_set";

/// `ProtocolFeeEvent.kind` discriminators — same reasoning.
const KIND_FEE_UPDATED: &str = "fee_updated";
const KIND_CONTROLLER_UPDATED: &str = "controller_updated";

/// The ERC-6909 `id` as the uint256 the chain actually emitted.
///
/// # THE TRAP
///
/// This `id` is a **CURRENCY id**, not a token id. v4-core mints claim tokens
/// with
///
/// ```text
/// id = uint256(uint160(Currency.unwrap(currency)))
/// ```
///
/// so the number IS the currency's 20-byte address widened to 32 bytes — the
/// same address space as `Pool.token0` / `Pool.token1`, and `id == 0` is native
/// ETH, a real currency rather than a null. Use [`currency_address`] to get it.
///
/// One caveat that only shows up in real data: that guarantee holds for ids the
/// singleton MINTS (every `Transfer`), because it mints them from a currency.
/// `Approval` is caller-supplied, so a caller may name an id that is not a
/// currency at all — Base block 25781641 log 190 carries `id = 12017`, which
/// narrows to the (empty) address 0x0000000000000000000000000000000000002ef1.
/// It is emitted verbatim rather than filtered: an approval for a nonexistent
/// currency is a real, observable act, and dropping it would silently rewrite
/// history. Treat [`currency_address`] as "the currency this row refers to",
/// not as proof the currency exists.
///
/// It is emphatically **not** a PositionManager ERC-721 `tokenId`. Both are
/// called `id` in Solidity, both are uint256, and both show up on this chain in
/// the same transactions — but a tokenId is a mint counter (1, 2, 3, …) that
/// indexes an LP NFT, while a currency id is an address. Routing these events
/// into `pb::PositionEvent.token_id` because the field names line up would put
/// twenty-byte addresses into a column that `position_manager.rs` fills with
/// mint counters, and every `JOIN position ON token_id` downstream would start
/// matching garbage. That is why `ClaimTokenEvent` exists as its own message.
///
/// # Why the wire value stays decimal
///
/// `currency_id` is declared `string` in the proto and lands in a
/// `NUMERIC(78,0)` column (see `db/schema.sql`), so this returns the raw
/// integer, not the hex address: writing `0x…` there is not a formatting
/// preference, it is invalid input for the column and would fail the insert.
/// The conversion is one call away and exact (see [`currency_address`]), while
/// the reverse — recovering a uint256 the protocol did not intend as an address
/// — is not.
fn currency_id_decimal(id: &BigInt) -> String {
    if currency_address(id).is_none() {
        // Structurally impossible today: the high 96 bits of a v4 claim-token
        // id are always zero because the id is a widened address. If that ever
        // stops holding, the id is no longer an address and every downstream
        // "which currency is this" join silently answers the wrong thing — so
        // it is worth one log line. The raw integer is still emitted: it is
        // what the chain said, and this decoder does not drop rows.
        log::info!("erc6909 id is not address-shaped, currency join unsafe: {}", id);
    }
    id.to_string()
}

/// Narrow an ERC-6909 currency id back to the 20-byte currency address.
///
/// Exact, not lossy, whenever the id fits in 160 bits — which is guaranteed for
/// every id v4-core mints (see [`currency_id_decimal`]). `None` is returned for
/// anything that does not fit, because a truncated address would be worse than
/// no address: it would look like a real, wrong token.
///
/// This is the single implementation of the mapping. It is deliberately kept
/// here rather than duplicated in the sink, so that if `ClaimTokenEvent` ever
/// grows a `currency_address` column (the natural follow-up — it would make
/// `claim_token_event` joinable to `pool.token0`/`token1` directly) there is
/// exactly one place that knows the rule.
pub(crate) fn currency_address(id: &BigInt) -> Option<String> {
    // Guard the sign before looking at magnitude: the ABI decoder builds this
    // with the unsigned constructor so it cannot be negative today, but
    // `to_bytes_be` reports magnitude only and would happily turn -1 into
    // 0x…01.
    if *id < BigInt::zero() {
        return None;
    }
    // MINIMAL big-endian magnitude — 0x42000000…06 comes back as 20 bytes but
    // USDC-with-a-leading-zero-byte comes back as 19, and native ETH (0) comes
    // back as a single 0x00. So left-pad into a fixed 20-byte buffer; indexing
    // from the front would shift every short id into a different address.
    let (_, magnitude) = id.to_bytes_be();
    if magnitude.len() > 20 {
        return None;
    }
    let mut addr = [0u8; 20];
    addr[20 - magnitude.len()..].copy_from_slice(&magnitude);
    Some(hooks::addr_hex(&addr))
}

// ---------------------------------------------------------------------------
// WHY THESE EVENTS GOT THEIR OWN MESSAGES
//
// All six are decoded above now. The note is kept because the shapes are not
// obvious and the alternatives all look cheaper than they are:
//
//   Donate(id, sender, amount0, amount1)              -> pb::Donate
//     Cannot ride inside pb::Swap: pb::Swap has no kind discriminator, so every
//     donation would be counted as swap volume by any consumer of
//     `Events.swaps`. Zeroing sqrt_price_x96 as a sentinel is not viable
//     either — the subgraph already writes legitimate swap rows with a zero
//     sqrtPriceX96 for aggregator-hook pools, so the sentinel is taken.
//
//   Transfer(caller, from, to, id, amount)            -> pb::ClaimTokenEvent
//   Approval(owner, spender, id, amount)                 kind=transfer /
//   OperatorSet(owner, operator, approved)               approval / operator_set
//     The flash-accounting rail: how routers and searchers park value *inside*
//     the singleton between swaps instead of settling to ERC-20. Nothing in the
//     V4 subgraph surfaces it. pb::PositionEvent is the wrong home twice over —
//     its `token_id` means a PositionManager ERC-721 tokenId whereas this `id`
//     is a currency id (see currency_id_decimal), and it has nowhere at all to
//     put `amount`.
//
//   ProtocolFeeUpdated(id, protocol_fee)              -> pb::ProtocolFeeEvent
//   ProtocolFeeControllerUpdated(controller)             kind=fee_updated /
//                                                        controller_updated
//     Governance-level fee config, and the only on-chain record of protocol
//     revenue being switched on for a pool. A partial pb::Pool row is not an
//     option: a sink doing an upsert on `Pool.id` cannot distinguish it from a
//     pool creation and would zero out token0/token1/hook.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn id(decimal: &str) -> BigInt {
        decimal.parse::<BigInt>().expect("test id parses")
    }

    /// USDC on Base, 0x833589fCd6EDb6E08f4c7C32D4f71b54bdA02913, is currency
    /// 749071750893463290574776461331093852760741783827 as an ERC-6909 id.
    /// Decimal in, checksum-free lowercase hex out, matching `hooks::addr_hex`
    /// so it joins byte-for-byte against `pool.token0` / `pool.token1`.
    #[test]
    fn currency_id_narrows_to_base_usdc() {
        assert_eq!(
            currency_address(&id("749071750893463290574776461331093852760741783827")).unwrap(),
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        );
    }

    /// WETH on Base — a 20-byte magnitude with no leading zero byte, i.e. the
    /// no-padding path.
    #[test]
    fn currency_id_narrows_to_base_weth() {
        assert_eq!(
            currency_address(&id("376793390874373408599387495934666716005045108742")).unwrap(),
            "0x4200000000000000000000000000000000000006"
        );
    }

    /// Currency id 0 is NATIVE ETH — a real V4 currency, not a null. The
    /// padding path that matters most: `to_bytes_be` hands back a single zero
    /// byte and a naive copy would produce a 1-byte "address".
    #[test]
    fn currency_id_zero_is_native_eth_not_missing() {
        assert_eq!(
            currency_address(&id("0")).unwrap(),
            "0x0000000000000000000000000000000000000000"
        );
    }

    /// A 19-byte magnitude (leading zero byte in the address) must be
    /// LEFT-padded. Getting this backwards silently maps one token onto
    /// another, which no downstream check would catch.
    #[test]
    fn currency_id_left_pads_short_magnitude() {
        // 0x0000fe59823933ac763611a69c88f91d45f81888 — the live Base hook
        // address from hooks.rs, reused here purely because it has two leading
        // zero bytes.
        let short = id("22156978853971505865374558639720525723080840");
        assert_eq!(
            currency_address(&short).unwrap(),
            "0x0000fe59823933ac763611a69c88f91d45f81888"
        );
    }

    /// 2^160 does not fit in an address. Refused rather than truncated: a
    /// truncated address looks like a real, wrong token.
    #[test]
    fn currency_id_wider_than_an_address_is_refused() {
        assert!(currency_address(&id("1461501637330902918203684832716283019655932542976")).is_none());
    }

    /// The wire value is the untouched integer — never the hex address, which
    /// would be invalid input for the NUMERIC(78,0) column it lands in.
    #[test]
    fn wire_currency_id_stays_decimal() {
        let d = "749071750893463290574776461331093852760741783827";
        assert_eq!(currency_id_decimal(&id(d)), d);
    }

    /// Live case, Base block 25781641 log 190: an `Approval` for id 12017.
    /// Caller-supplied ids need not be currencies, and the decoder must pass
    /// them through unchanged rather than treat a small integer as a tokenId
    /// or drop the row.
    #[test]
    fn caller_supplied_approval_id_is_passed_through_and_still_narrows() {
        assert_eq!(currency_id_decimal(&id("12017")), "12017");
        assert_eq!(
            currency_address(&id("12017")).unwrap(),
            "0x0000000000000000000000000000000000002ef1"
        );
    }

    /// The `kind` strings are a contract with db/schema.sql, not free text.
    #[test]
    fn kind_discriminators_match_the_schema() {
        assert_eq!(KIND_TRANSFER, "transfer");
        assert_eq!(KIND_APPROVAL, "approval");
        assert_eq!(KIND_OPERATOR_SET, "operator_set");
        assert_eq!(KIND_FEE_UPDATED, "fee_updated");
        assert_eq!(KIND_CONTROLLER_UPDATED, "controller_updated");
    }
}
