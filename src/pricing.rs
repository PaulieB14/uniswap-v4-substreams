//! USD pricing — the port of the subgraph's `Bundle.ethPriceUSD` / `Token.derivedETH`.
//!
//! Source of truth: `/tmp/v4sg/src/utils/pricing.ts`, with the `base` branch of
//! `chains.ts` inlined as the constants below. This is the single biggest parity
//! gap in the package: every amount the pipeline emits today is a raw on-chain
//! integer, and "what was this worth" is the question almost every consumer
//! actually asks.
//!
//! # How the subgraph prices things, and how that maps onto Substreams
//!
//! The subgraph keeps two pieces of mutable global state:
//!
//! * `Bundle('1').ethPriceUSD` — the native price, read off **one hardcoded
//!   pool** (WETH/USDC on Base) on every swap and every initialise.
//! * `Token.derivedETH` — native per token, recomputed by walking that token's
//!   whitelist pools and taking the price from the deepest one.
//!
//! Both are entity-store reads and writes, which graph-node serves implicitly.
//! Substreams has none of that, so the state has to be an explicit store —
//! [`store_prices`] — and the algorithm has to be pure functions the store
//! handler calls ([`sqrt_price_x96_to_token_prices`], [`find_native_per_token`]).
//! That split is not cosmetic: a `#[handlers::store]` cannot be constructed off
//! wasm32, so anything inside one is untestable, and pricing is the part of this
//! package where a silent arithmetic error is most expensive.
//!
//! ## Why a store and not a map
//!
//! Same reason `store_pools` exists. A price computed in block N has to be
//! readable in block N+1, where the pool that produced it did not trade. A
//! stateless `map` re-executed out of order by parallel backfill cannot carry
//! that; a `set`-policy store is replayed deterministically by the engine and
//! can.
//!
//! ## The one structural limit: a store cannot read its own store
//!
//! `findNativePerToken` is recursive in the subgraph — token A is priced through
//! token B, whose own `derivedETH` came from some earlier pool. A store handler
//! has no read access to what it has written, so that recursion is not available
//! here.
//!
//! It turns out not to matter **on Base**, because every token on the whitelist
//! is reachable in one hop:
//!
//! | token | how it is priced | needs a previous value? |
//! |---|---|---|
//! | native ETH (`address(0)`) | 1 by definition | no |
//! | WETH | 1 by definition | no |
//! | USDC | `1 / ethPriceUSD`, the subgraph's own stablecoin shortcut | no — same pool, same block |
//! | ZORA | the ZORA/native pool's `token0Price` | no — the other side is native, `derivedETH = 1` |
//!
//! USDC is the interesting case: its price and the native price come off the
//! *same* `sqrtPriceX96` of the *same* anchor pool. `price0` of WETH/USDC is
//! "WETH per USDC", which **is** USDC's `derivedETH` — verified against the
//! deployed subgraph, see the tests. So no cross-block dependency exists.
//!
//! [`find_native_per_token`] still implements the general case (each candidate
//! carries the other side's `derived_native`), so pointing this at a chain whose
//! whitelist needs two hops is a change to the *caller*, not to the algorithm.
//!
//! # Hazards this module exists to not fall into
//!
//! **`address(0)` is native ETH.** V4 pools quote the native currency directly
//! rather than always wrapping, unlike V3. `Token.decimals` for `address(0)`
//! comes from nowhere on-chain — there is no ERC-20 to call — so the subgraph
//! substitutes `nativeTokenDetails.decimals` in `sqrtPriceX96ToTokenPrices`.
//! [`effective_decimals`] is that substitution, and it is applied *before* any
//! arithmetic rather than trusted from the token store.
//!
//! **Unmeasured decimals produce no price at all.** `store_tokens` falls back to
//! 18 when `decimals()` is unreadable and flags that with `decimals_measured`.
//! Pricing a 6-decimal token as 18-decimal is wrong by 10^12 — not a rounding
//! error, a twelve-order-of-magnitude error that looks like a plausible number.
//! Every entry point here returns `None` rather than price off a defaulted
//! decimals value. A missing price is recoverable; a wrong one is laundered into
//! every downstream aggregate.
//!
//! **`sqrtPriceX96` can legitimately be zero on a V4 swap.** Aggregator hooks
//! emit swaps with a zero sqrt price (see the note in `src/enrich.rs`). Zero
//! means "no price information", not "this pool is worth nothing", and it is
//! skipped. The subgraph does not guard this and writes `token0Price = 0`.
//!
//! **Division by zero panics.** `substreams::scalar::BigDecimal`'s `Div` impl
//! panics on a zero divisor, and a panic in a wasm handler aborts the whole
//! stream, not just the block. Every division in this file goes through
//! [`safe_div`] or is guarded by an `is_zero` check first.
//!
//! **Decimals are attacker-controlled.** `decimals()` returns whatever the token
//! wants. `exponentToBigDecimal` builds 10^d, so a token reporting `4294967295`
//! would ask for a four-billion-digit integer and OOM the module. Capped at
//! [`MAX_SANE_DECIMALS`].
//!
//! # Precision
//!
//! graph-node's `BigDecimal` normalises to 34 significant digits after every
//! operation (`MAX_SIGNIFICANT_DIGITS`). `substreams::scalar::BigDecimal` wraps
//! the same `bigdecimal` crate but divides at the crate default of 100 digits and
//! never truncates. Stored values are therefore rounded with
//! `with_prec(BigDecimal::MAX_SIGNIFICANT_DIGITS)` — both so the numbers line up
//! with the deployed subgraph and so a chain of divisions cannot grow the stored
//! string without bound.
//!
//! f64 is not used anywhere here. A 2^192 denominator does not fit in a double,
//! and the whole point of the module is that the numbers are exact.

use std::collections::BTreeMap;

use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{StoreNew, StoreSet, StoreSetString};

use crate::pb::uniswap::v4::v1 as pb;

// ---------------------------------------------------------------------------
// Chain configuration — `base` branch of /tmp/v4sg/src/utils/chains.ts
//
// Lowercase, exactly as the subgraph stores them ("Note: All token and pool
// addresses should be lowercased!"), because every comparison in this file is a
// byte comparison against `Pool.token0` / `Swap.token0`, which `hooks::addr_hex`
// produces as lowercase hex. A checksummed constant here would not fail — it
// would silently never match, and every token would come out unpriced.
// ---------------------------------------------------------------------------

/// V4's native-currency sentinel. `chains.ts` lists it in `whitelistTokens` as
/// "Native ETH" and `pricing.ts` treats it as `ADDRESS_ZERO`.
pub const NATIVE_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// `wrappedNativeAddress` — Base WETH.
pub const WRAPPED_NATIVE: &str = "0x4200000000000000000000000000000000000006";

/// Native-currency decimals, from `nativeTokenDetails` for the `base` network.
/// Used for `address(0)` legs, which have no contract to call.
pub const NATIVE_DECIMALS: u32 = 18;

/// Circle USDC on Base — the sole entry in `stablecoinAddresses`.
pub const USDC: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";

/// ZORA — the only `whitelistTokens` entry that is neither native/wrapped-native
/// nor a stablecoin, and therefore the only token on Base whose price actually
/// goes through the pool search in [`find_native_per_token`].
pub const ZORA: &str = "0x1111111111166b7fe7bd91427724b487980afc69";

/// `stablecoinAddresses`.
pub const STABLECOINS: [&str; 1] = [USDC];

/// `whitelistTokens`, in the subgraph's order.
pub const WHITELIST_TOKENS: [&str; 4] = [WRAPPED_NATIVE, USDC, NATIVE_ADDRESS, ZORA];

/// `stablecoinWrappedNativePoolId` — the WETH/USDC pool the native price is read
/// off. A pool *id*, not an address: V4 pools are keccak hashes of the PoolKey.
///
/// Hardcoded in the subgraph and hardcoded here, deliberately. Discovering "the
/// deepest USD pool" dynamically would make the native price — which every other
/// price is denominated in — depend on a liquidity ranking that can move, and a
/// native price that silently switches source is far worse than one that is
/// wrong in a fixed, auditable way.
pub const STABLECOIN_NATIVE_POOL_ID: &str =
    "0x90333bb05c258fe0dddb2840ef66f1a05165aa7dac6815d24e807cc6ebd943a0";

/// `stablecoinIsToken0` — false on Base: the anchor pool is WETH (token0) /
/// USDC (token1), so the native price is that pool's `token1Price`.
pub const STABLECOIN_IS_TOKEN0: bool = false;

/// Upper bound on a token's reported `decimals()` before we refuse to price it.
///
/// Not in the subgraph — graph-node's host runtime absorbs the cost of a silly
/// `exponentToBigDecimal`; a wasm module with a linear allocator does not. 36 is
/// comfortably above every real ERC-20 (the largest in circulation is 24) and
/// far below the point where 10^d stops being cheap.
pub const MAX_SANE_DECIMALS: u32 = 36;

// ---------------------------------------------------------------------------
// Store layout
// ---------------------------------------------------------------------------

/// Singleton key holding the native price in USD. The subgraph's `Bundle('1')`.
pub const NATIVE_USD_KEY: &str = "native_usd";

/// Key namespace for `Token.derivedETH`: `derived_native:0x<40 lowercase hex>`.
///
/// Prefixed, like `pool:` and `token:` elsewhere in this package, because the
/// store is one flat keyspace and `delete_prefix` has to stay usable.
pub const DERIVED_NATIVE_PREFIX: &str = "derived_native:";

/// Field separator inside a stored value: ASCII US (0x1f), the same choice and
/// the same reasoning as `tokens.rs` — every field written here is a decimal
/// number or a hex id, so the byte cannot occur in the data.
pub const FIELD_SEP: char = '\u{1f}';

/// Build the store key for a token's derived native price.
///
/// Lowercased defensively: a caller holding a checksummed address would
/// otherwise write a key no reader ever finds, and a store miss does not error,
/// it just yields an unpriced row.
pub fn derived_native_key(token: &str) -> String {
    format!("{}{}", DERIVED_NATIVE_PREFIX, token.to_ascii_lowercase())
}

// ---------------------------------------------------------------------------
// Small numeric helpers (ports of /tmp/v4sg/src/utils/index.ts)
// ---------------------------------------------------------------------------

/// `10^decimals`. The subgraph's `exponentToBigDecimal`.
///
/// Built from a `BigDecimal` exponent rather than by string concatenation (which
/// is what the AssemblyScript does), so it costs nothing for large `decimals`.
pub fn exponent_to_big_decimal(decimals: u32) -> BigDecimal {
    // BigDecimal::new(digits, exp) == digits * 10^exp.
    BigDecimal::new(BigInt::one(), decimals as i64)
}

/// The subgraph's `safeDiv`: zero denominator yields zero rather than throwing.
///
/// Mandatory rather than stylistic here — `BigDecimal`'s `Div` **panics** on a
/// zero divisor, and a panic inside a wasm handler kills the stream.
pub fn safe_div(numerator: &BigDecimal, denominator: &BigDecimal) -> BigDecimal {
    if denominator.is_zero() {
        BigDecimal::zero()
    } else {
        numerator.clone() / denominator.clone()
    }
}

/// Round to graph-node's 34 significant digits.
///
/// Applied to everything that leaves this module, so a value stored here can be
/// compared against the deployed subgraph's, and so repeated division cannot
/// grow the stored string to the `bigdecimal` crate's 100-digit default.
///
/// **The 34th digit is not guaranteed to match the subgraph.** graph-node
/// re-rounds to 34 digits after *every* intermediate operation, and it is
/// running a much older `bigdecimal` whose rounding at that boundary is not
/// reproducible from the outside — measured against four live Base pools, three
/// agree to all 34 digits and the WETH/USDC anchor differs by one unit in the
/// last place. Chasing that would mean encoding a guess about a graph-node
/// internal. The tests therefore assert agreement to 30 significant digits,
/// which is 1 part in 10^30 and roughly 20 orders of magnitude finer than the
/// smallest unit of any token being priced.
fn normalize(v: BigDecimal) -> BigDecimal {
    v.with_prec(BigDecimal::MAX_SIGNIFICANT_DIGITS as u64)
}

/// True for V4's native-currency sentinel.
pub fn is_native(token: &str) -> bool {
    token.eq_ignore_ascii_case(NATIVE_ADDRESS)
}

/// True for the two currencies whose native price is 1 by definition.
///
/// Both, not just WETH: on V4 the same economic asset appears as `address(0)`
/// and as the WETH ERC-20, and `pricing.ts` returns `ONE_BD` for either
/// (`token.id == wrappedNativeAddress || token.id == ADDRESS_ZERO`).
pub fn is_native_or_wrapped(token: &str) -> bool {
    is_native(token) || token.eq_ignore_ascii_case(WRAPPED_NATIVE)
}

/// True for a configured stablecoin.
pub fn is_stablecoin(token: &str) -> bool {
    STABLECOINS.iter().any(|s| token.eq_ignore_ascii_case(s))
}

/// True for a `whitelistTokens` member.
pub fn is_whitelisted(token: &str) -> bool {
    WHITELIST_TOKENS.iter().any(|t| token.eq_ignore_ascii_case(t))
}

/// `minimumNativeLocked` for `base`: 1 ETH.
///
/// Ported verbatim so [`find_native_per_token`] can be driven with the
/// subgraph's own TVL numbers and compared against it. **It is not the threshold
/// [`store_prices`] passes** — see [`minimum_active_native`] for why.
// Not called by the pipeline; `store_prices` passes `minimum_active_native()`.
// Kept because it is the ported chain-config value and the tests drive
// `find_native_per_token` with it against the subgraph's own TVL numbers, which
// is the comparison that shows the port is faithful. Allowed per-item rather
// than module-wide so dead-code detection stays live for the rest of the file.
#[allow(dead_code)]
pub fn minimum_native_locked() -> BigDecimal {
    BigDecimal::one()
}

/// The depth floor [`store_prices`] actually uses, in native units.
///
/// The subgraph gates on `pool.totalValueLockedToken*`, a running sum of every
/// token flow the pool has ever seen. This package does not track TVL — that
/// needs the tick-math port (`utils/liquidityMath/`) to turn a `ModifyLiquidity`
/// `liquidityDelta` plus a tick range into token amounts, which does not exist
/// here. What *is* on every row is the pool's active liquidity `L` and its
/// `sqrtPriceX96`, giving the virtual reserves at the current price
/// (see [`virtual_reserves`]).
///
/// That is a genuinely different measure and the ported `1` is the wrong floor
/// for it. Measured against the deployed subgraph on two real Base pools:
///
/// | pool | subgraph TVL (native side) | virtual reserve | ratio |
/// |---|---|---|---|
/// | WETH/USDC anchor | 1.482 ETH | 4.33 ETH | 2.9x over |
/// | native/ZORA | 13.318 ETH | 0.0816 ETH | 163x under |
///
/// It runs over when liquidity is concentrated at the current price and under
/// when most of the TVL sits in out-of-range positions — so it cannot be
/// calibrated to TVL at all. Gating at `1` would reject the native/ZORA pool,
/// which is precisely the pool the subgraph picks for ZORA.
///
/// 0.05 native (~$120 at the anchor price) is chosen as a **dust filter**, which
/// is the failure this gate has to catch: a pool with zero active liquidity
/// reports whatever `sqrtPriceX96` it was left at, and Base has live examples
/// pinned at `MAX_SQRT_RATIO` quoting 3.4e50 token1 per token0. It admits both
/// pools in the table and rejects anything with no depth at the current price.
///
/// Read it as "how much native it takes to move this price", not as TVL.
pub fn minimum_active_native() -> BigDecimal {
    // 5 * 10^-2
    BigDecimal::new(BigInt::from(5), -2)
}

// ---------------------------------------------------------------------------
// Core price math
// ---------------------------------------------------------------------------

/// `2^192`, the denominator that turns `sqrtPriceX96^2` back into a price.
fn q192() -> BigDecimal {
    BigDecimal::from(BigInt::from(2).pow(192))
}

/// `2^96`, the fixed-point scale of `sqrtPriceX96`.
fn q96() -> BigDecimal {
    BigDecimal::from(BigInt::from(2).pow(96))
}

/// Decimal-adjusted spot prices from a `sqrtPriceX96`. The subgraph's
/// `sqrtPriceX96ToTokenPrices`, returning its `[price0, price1]` in that order.
///
/// `sqrtPriceX96` encodes `sqrt(raw1 / raw0) * 2^96` over **raw** integer
/// amounts, so:
///
/// ```text
///   price1 = (sqrt^2 / 2^192) * 10^dec0 / 10^dec1   // token1 per 1 token0
///   price0 = 1 / price1                             // token0 per 1 token1
/// ```
///
/// The naming is the subgraph's and it reads backwards on first contact:
/// `token0Price` is the amount of **token0** you get per token1. Keeping the
/// inversion is not optional — `Bundle.ethPriceUSD` is the anchor pool's
/// `token1Price` and `USDC.derivedETH` is its `token0Price`, so flipping them
/// would invert every USD figure in the package.
///
/// Callers must pass decimals that came from [`effective_decimals`]; this
/// function has no way to tell a measured 18 from a defaulted one.
pub fn sqrt_price_x96_to_token_prices(
    sqrt: &BigInt,
    dec0: u32,
    dec1: u32,
) -> (BigDecimal, BigDecimal) {
    // Squared as BigDecimal, not BigInt: `substreams::scalar` has no
    // BigInt * BigInt, and the conversion is exact for integers anyway.
    let s: BigDecimal = sqrt.into();
    let num = s.clone() * s;

    // q192 is a positive constant, so this division cannot panic.
    let price1 = normalize(num / q192() * exponent_to_big_decimal(dec0) / exponent_to_big_decimal(dec1));

    // safe_div, not `/`: price1 is zero exactly when sqrt is zero, which a V4
    // aggregator-hook swap really does emit. `1 / 0` here would abort the module.
    let price0 = normalize(safe_div(&BigDecimal::one(), &price1));

    (price0, price1)
}

/// Virtual reserves at the current price, in **raw** token units:
/// `(x, y) = (L / sqrt(P), L * sqrt(P))`.
///
/// This is the depth measure [`store_prices`] substitutes for the subgraph's
/// TVL — see [`minimum_active_native`] for the honest accounting of how far
/// apart the two are. It describes the liquidity active *at the current tick*,
/// which is the quantity that governs how expensive the price is to move, and
/// it is derivable from fields already on every swap row.
///
/// Returns `None` for a zero or negative `sqrt` (nothing to divide by) — never
/// zero, which would read as "no depth" and is indistinguishable from a real
/// empty pool.
fn virtual_reserves(liquidity: &BigInt, sqrt: &BigInt) -> Option<(BigDecimal, BigDecimal)> {
    if sqrt.is_zero() || sqrt.lt(&BigInt::zero()) {
        return None;
    }
    let l: BigDecimal = liquidity.into();
    let s: BigDecimal = sqrt.into();

    // sqrt(P) = sqrtPriceX96 / 2^96, so x = L * 2^96 / sqrtPriceX96 and
    // y = L * sqrtPriceX96 / 2^96. Both divisors are non-zero above.
    let x = l.clone() * q96() / s.clone();
    let y = l * s / q96();
    Some((x, y))
}

/// The decimals to actually price a leg with, or `None` if there are none to
/// trust.
///
/// Two rules, in order:
///
/// 1. `address(0)` is the native currency and has no contract. The subgraph
///    substitutes `nativeTokenDetails.decimals` here rather than reading the
///    `Token` entity, and so do we — unconditionally, without consulting
///    `measured`. This is the V4-specific path that V3 never needed.
/// 2. Otherwise the value is only usable if `store_tokens` actually read it off
///    the contract. An unmeasured leg means no price for the pool, full stop.
///
/// Note the shape of the input: `pb::Swap` carries **one** `decimals_measured`
/// flag for both legs (it is set only when both resolved). That is why an
/// unmeasured ERC-20 paired against native ETH still yields `None` for the
/// ERC-20 leg — which is the correct outcome, since that leg is exactly the one
/// that cannot be trusted.
pub fn effective_decimals(token: &str, decimals: u32, measured: bool) -> Option<u32> {
    if is_native(token) {
        return Some(NATIVE_DECIMALS);
    }
    if !measured || decimals > MAX_SANE_DECIMALS {
        return None;
    }
    Some(decimals)
}

// ---------------------------------------------------------------------------
// findNativePerToken
// ---------------------------------------------------------------------------

/// One pool reduced to the six things pricing needs from it.
///
/// The field set is deliberately a 1:1 map onto the entity fields the subgraph's
/// loop reads (`pool.token0Price`, `pool.totalValueLockedToken1`,
/// `token1.derivedETH`, ...) so a test can be built literally out of a subgraph
/// query response and compared — which is what `zora_matches_deployed_subgraph`
/// does.
///
/// Both sides carry their own depth and their own `derived_native`, rather than
/// one "other side" pair, because a single pool is a pricing candidate for
/// *either* of its tokens and which one is "other" depends on the caller.
#[derive(Clone, Debug)]
pub struct PoolPriceCandidate {
    pub pool_id: String,
    pub token0: String,
    pub token1: String,
    /// token0 per 1 token1 (the subgraph's `pool.token0Price`).
    pub price0: BigDecimal,
    /// token1 per 1 token0 (`pool.token1Price`).
    pub price1: BigDecimal,
    /// Depth on the token0 side, in human token0 units. The subgraph passes
    /// `pool.totalValueLockedToken0`; `store_prices` passes the virtual reserve.
    pub amount0: BigDecimal,
    /// Depth on the token1 side.
    pub amount1: BigDecimal,
    /// token0's own native price, if known. `None` means this pool cannot price
    /// its token1 — the anchor leg is unpriced, so there is nothing to multiply
    /// through. The subgraph's equivalent is a `Token` entity whose `derivedETH`
    /// is still `ZERO_BD`, which it multiplies by anyway and gets zero.
    pub derived_native0: Option<BigDecimal>,
    /// token1's own native price, if known.
    pub derived_native1: Option<BigDecimal>,
}

/// Native (ETH) per token — the subgraph's `findNativePerToken`.
///
/// Order of the three branches is the subgraph's and matters:
///
/// 1. **Native or wrapped native → 1.** By definition.
/// 2. **Stablecoin → `1 / nativeUsd`.** The subgraph calls this its "hardcoded
///    fix for incorrect rates" and skips the pool search entirely: a stablecoin
///    priced off some thin stable/alt pool drifts, and every USD number in the
///    subgraph hangs off this one. Note it is `1 / ethPriceUSD` even though
///    `ethPriceUSD` itself came from a stablecoin pool, so the two are exact
///    reciprocals by construction.
/// 3. **Otherwise, the deepest qualifying whitelist pool.** Strictly deepest —
///    ties keep the first, matching the subgraph's `gt` comparison.
///
/// Divergence from the subgraph, deliberate: it returns `ZERO_BD` when nothing
/// qualifies, and the caller stores that zero. Here it is `None`. A stored zero
/// is indistinguishable from a real price of zero, and it propagates silently —
/// every `amount * derivedETH * ethPriceUSD` downstream becomes `$0.00`, which
/// reads as a fact rather than as an absence. `None` means the row stays
/// unpriced and the gap is visible.
///
/// `candidates` need not be pre-filtered: entries that do not mention `token`,
/// whose other leg has no `derived_native`, or that fail the depth floor are
/// skipped here.
pub fn find_native_per_token(
    token: &str,
    native_usd: Option<&BigDecimal>,
    candidates: &[PoolPriceCandidate],
    minimum_native_locked: &BigDecimal,
) -> Option<BigDecimal> {
    if is_native_or_wrapped(token) {
        return Some(BigDecimal::one());
    }

    if is_stablecoin(token) {
        let native_usd = native_usd?;
        if native_usd.is_zero() {
            // The subgraph's safeDiv would hand back 0 here and call it a price.
            return None;
        }
        return Some(normalize(safe_div(&BigDecimal::one(), native_usd)));
    }

    let mut largest = BigDecimal::zero();
    let mut best: Option<BigDecimal> = None;

    for c in candidates {
        // Which side is our token, and therefore which side is the anchor whose
        // native price we multiply through.
        let (price_in_other, other_amount, other_derived) = if c.token0.eq_ignore_ascii_case(token)
        {
            // Our token is token0, so the anchor is token1 and the conversion
            // rate we want is "token1 per our token" = price1.
            (&c.price1, &c.amount1, &c.derived_native1)
        } else if c.token1.eq_ignore_ascii_case(token) {
            (&c.price0, &c.amount0, &c.derived_native0)
        } else {
            continue;
        };

        let other_derived = match other_derived {
            Some(d) => d,
            None => continue,
        };

        // ethLocked = <other side's amount> * <other side's native price>.
        let native_locked = other_amount.clone() * other_derived.clone();
        if native_locked.le(&largest) || native_locked.le(minimum_native_locked) {
            continue;
        }

        largest = native_locked;
        best = Some(normalize(price_in_other.clone() * other_derived.clone()));
    }

    best
}

// ---------------------------------------------------------------------------
// Stored value codec
// ---------------------------------------------------------------------------

/// What one store entry holds: the price plus enough provenance to audit it.
#[derive(Clone, Debug, PartialEq)]
pub struct PriceRecord {
    /// USD per native for [`NATIVE_USD_KEY`]; native per token for a
    /// `derived_native:` key.
    pub price: BigDecimal,
    /// Depth of the pool this came from, in native units — the value the depth
    /// floor was applied to. Carried so a consumer can apply a stricter floor of
    /// its own without re-deriving anything.
    pub native_locked: BigDecimal,
    /// The pool id the price was read off.
    pub source_pool: String,
    /// Block the price was observed at.
    ///
    /// This is the staleness handle, and it is the one thing a reader cannot
    /// reconstruct. A `set` store keeps the last write forever, so `get_last` at
    /// block N happily returns a price from ten thousand blocks ago if the
    /// source pool has not traded since. Without this field that is invisible.
    pub block: u64,
}

/// Encode as `price␟native_locked␟source_pool␟block`.
///
/// A string store rather than a proto message: four small scalars, greppable in
/// `substreams run` output, and no new message in a proto contract shared with
/// every other module in the package. Same trade-off `tokens.rs` made.
pub fn encode_price_value(rec: &PriceRecord) -> String {
    format!(
        "{price}{sep}{locked}{sep}{pool}{sep}{block}",
        price = rec.price,
        locked = rec.native_locked,
        pool = rec.source_pool,
        block = rec.block,
        sep = FIELD_SEP,
    )
}

/// Inverse of [`encode_price_value`]. `None` on anything malformed — a consumer
/// that gets `None` has no price, and must not fall back to zero.
// Consumed by the tests and by downstream modules that are not wired yet (a map
// reading this store to attach USD columns to swap rows). Allowed item-by-item
// rather than at module level so dead-code detection stays live for the rest.
#[allow(dead_code)]
pub fn decode_price_value(value: &str) -> Option<PriceRecord> {
    let parts: Vec<&str> = value.split(FIELD_SEP).collect();
    if parts.len() != 4 {
        return None;
    }
    Some(PriceRecord {
        price: parts[0].parse().ok()?,
        native_locked: parts[1].parse().ok()?,
        source_pool: parts[2].to_string(),
        block: parts[3].parse().ok()?,
    })
}

// ---------------------------------------------------------------------------
// Turning a block into price observations
// ---------------------------------------------------------------------------

/// A pool's state at one point in the block, with decimals already validated.
#[derive(Clone, Debug)]
struct Observation {
    pool_id: String,
    token0: String,
    token1: String,
    dec0: u32,
    dec1: u32,
    sqrt: BigInt,
    liquidity: BigInt,
    /// Block-scoped log index; doubles as the store ordinal.
    ordinal: u64,
    block: u64,
}

/// Build an observation, or `None` if this row cannot be priced.
///
/// Rejects, in order: an unenriched row (no tokens — the pool was not in
/// `store_pools`), untrustworthy decimals on either leg, an unparseable or
/// non-positive `sqrtPriceX96`.
fn observation(
    pool_id: &str,
    token0: &str,
    token1: &str,
    dec0: u32,
    dec1: u32,
    measured: bool,
    sqrt: &str,
    liquidity: &str,
    meta: Option<&pb::Meta>,
) -> Option<Observation> {
    if token0.is_empty() || token1.is_empty() {
        return None;
    }
    let dec0 = effective_decimals(token0, dec0, measured)?;
    let dec1 = effective_decimals(token1, dec1, measured)?;

    let sqrt: BigInt = sqrt.parse().ok()?;
    // Zero is a real V4 value on aggregator-hook swaps and means "no price
    // here", not "price is zero". Negative is impossible on-chain (uint160) but
    // the field is a string, so it is checked rather than assumed.
    if sqrt.is_zero() || sqrt.lt(&BigInt::zero()) {
        return None;
    }

    // A pool with no active liquidity still has a sqrtPriceX96, and it is
    // whatever the last trade left behind — Base has pools sitting at
    // MAX_SQRT_RATIO quoting 3.4e50. Parsing failure and zero both land on
    // zero depth, which the floor in find_native_per_token rejects.
    let liquidity: BigInt = liquidity.parse().unwrap_or_else(|_| BigInt::zero());

    let meta = meta?;
    Some(Observation {
        pool_id: pool_id.to_string(),
        token0: token0.to_ascii_lowercase(),
        token1: token1.to_ascii_lowercase(),
        dec0,
        dec1,
        sqrt,
        liquidity,
        ordinal: meta.log_index as u64,
        block: meta.block_number,
    })
}

/// Every priceable pool state in the block, one entry per pool: the **last**
/// one, by log index.
///
/// Last, not first — the subgraph recomputes prices on every swap, so the value
/// standing at the end of the block is the one from the final swap. Keeping only
/// that also keeps the store write count at one per pool per block instead of
/// one per swap.
///
/// `Events.pools` (initialise) is read as well as `Events.swaps`, matching
/// `handleInitialize`, which sets pool prices and refreshes the bundle. Today
/// those rows are inert: `map_events` builds a `Pool` with `decimals_measured =
/// false` and `enrich` only fills token metadata onto swap and liquidity rows,
/// so `observation` rejects them. It is wired anyway so that pricing starts
/// working at initialise the moment pool rows carry decimals, rather than
/// needing a change here.
fn observations(events: &pb::Events) -> BTreeMap<String, Observation> {
    // BTreeMap, not HashMap: module output must be byte-identical across
    // re-executions (backfill, reorg), and `substreams_ethereum::init!()`
    // installs a getrandom that errors, so a RandomState hasher is a live
    // hazard on wasm32 rather than a theoretical one.
    let mut out: BTreeMap<String, Observation> = BTreeMap::new();

    let mut consider = |obs: Option<Observation>| {
        if let Some(o) = obs {
            match out.get(&o.pool_id) {
                Some(prev) if prev.ordinal >= o.ordinal => {}
                _ => {
                    out.insert(o.pool_id.clone(), o);
                }
            }
        }
    };

    for p in &events.pools {
        consider(observation(
            &p.id,
            &p.token0,
            &p.token1,
            p.token0_decimals,
            p.token1_decimals,
            p.decimals_measured,
            &p.sqrt_price,
            &p.liquidity,
            p.meta.as_ref(),
        ));
    }
    for s in &events.swaps {
        consider(observation(
            &s.pool_id,
            &s.token0,
            &s.token1,
            s.token0_decimals,
            s.token1_decimals,
            s.decimals_measured,
            &s.sqrt_price_x96,
            &s.liquidity,
            s.meta.as_ref(),
        ));
    }

    out
}

/// Everything [`store_prices`] would write for this block, as
/// `(ordinal, key, value)` sorted by ordinal.
///
/// Split out from the handler because a `#[handlers::store]` cannot be built off
/// wasm32 — this is the part the tests can actually execute.
///
/// The two passes are ordered, and the order is the dependency:
///
/// 1. The anchor pool fixes the native price for the block. Nothing else can be
///    priced in USD without it, and USDC's native price is its exact reciprocal.
/// 2. Every other observation becomes a pricing candidate, and each whitelisted
///    token is resolved against the full candidate set.
///
/// If the anchor pool did not trade this block there is no native price to
/// write, and stablecoin-anchored candidates fall back to `None`. That is
/// correct rather than lossy: a `set` store keeps the previous value, so a
/// reader still sees the last known price (with its `block` field to judge how
/// stale it is). What is genuinely lost is a token whose *only* whitelist pool
/// is against a stablecoin, in a block where the anchor pool was quiet — it goes
/// unpriced for that block. No such token exists on the Base whitelist.
fn price_writes(events: &pb::Events) -> Vec<(u64, String, String)> {
    let obs = observations(events);
    let mut writes: Vec<(u64, String, String)> = Vec::new();

    // ---- pass 1: the native price, off the one hardcoded anchor pool --------
    let mut native_usd: Option<BigDecimal> = None;
    if let Some(anchor) = obs.get(STABLECOIN_NATIVE_POOL_ID) {
        let (price0, price1) = sqrt_price_x96_to_token_prices(&anchor.sqrt, anchor.dec0, anchor.dec1);
        // getNativePriceInUSD: stablecoinIsToken0 ? token0Price : token1Price.
        let price = if STABLECOIN_IS_TOKEN0 { price0 } else { price1 };

        // Depth is recorded but NOT gated on, matching the subgraph: the anchor
        // is chosen by configuration, not by liquidity, so silently withholding
        // the number every other price depends on would be a worse failure than
        // publishing a thin one. The value carries the depth so a consumer can
        // decide for itself.
        let native_locked = native_side_depth(anchor);

        if !price.is_zero() {
            native_usd = Some(price.clone());
            writes.push((
                anchor.ordinal,
                NATIVE_USD_KEY.to_string(),
                encode_price_value(&PriceRecord {
                    price,
                    native_locked,
                    source_pool: anchor.pool_id.clone(),
                    block: anchor.block,
                }),
            ));
        }
    }

    // ---- pass 2: derived native price per whitelisted token -----------------
    let stable_derived = native_usd
        .as_ref()
        .filter(|p| !p.is_zero())
        .map(|p| normalize(safe_div(&BigDecimal::one(), p)));

    let mut candidates: Vec<PoolPriceCandidate> = Vec::new();
    // Which observation each candidate came from, for the record's provenance.
    let mut origin: BTreeMap<String, (u64, u64)> = BTreeMap::new();

    for o in obs.values() {
        let (price0, price1) = sqrt_price_x96_to_token_prices(&o.sqrt, o.dec0, o.dec1);
        let (x, y) = match virtual_reserves(&o.liquidity, &o.sqrt) {
            Some(v) => v,
            None => continue,
        };

        candidates.push(PoolPriceCandidate {
            pool_id: o.pool_id.clone(),
            token0: o.token0.clone(),
            token1: o.token1.clone(),
            price0,
            price1,
            amount0: x / exponent_to_big_decimal(o.dec0),
            amount1: y / exponent_to_big_decimal(o.dec1),
            derived_native0: anchor_derived_native(&o.token0, stable_derived.as_ref()),
            derived_native1: anchor_derived_native(&o.token1, stable_derived.as_ref()),
        });
        origin.insert(o.pool_id.clone(), (o.ordinal, o.block));
    }

    // Deterministic iteration order over the tokens to price. BTreeSet-like
    // behaviour via the map: two runs must emit the same writes in the same
    // order or the module output is not reproducible.
    let mut targets: BTreeMap<String, ()> = BTreeMap::new();
    for c in &candidates {
        for t in [&c.token0, &c.token1] {
            if should_store_price(t) {
                targets.insert(t.clone(), ());
            }
        }
    }

    for token in targets.keys() {
        // Native and wrapped native are 1 by definition, in every block, for
        // ever. Writing that constant costs a store delta on essentially every
        // block of the chain for information no reader needs to be told, so it
        // is left out and `find_native_per_token` answers for them with no read
        // at all. This is the one expectation the store places on a consumer:
        // a missing `derived_native:` entry for these two means 1, not unpriced.
        if is_native_or_wrapped(token) {
            continue;
        }

        let price = match find_native_per_token(
            token,
            native_usd.as_ref(),
            &candidates,
            &minimum_active_native(),
        ) {
            Some(p) if !p.is_zero() => p,
            // No qualifying pool, or an unpriceable anchor. Write nothing and
            // leave the previous value standing — never a zero.
            _ => continue,
        };

        // Attribute the write to the deepest candidate that actually mentions
        // the token, which is the one find_native_per_token selected. For the
        // by-definition and stablecoin branches there is no source pool, so the
        // provenance is the anchor.
        let (source_pool, native_locked, ordinal, block) =
            attribution(token, &candidates, &origin, obs.get(STABLECOIN_NATIVE_POOL_ID));

        writes.push((
            ordinal,
            derived_native_key(token),
            encode_price_value(&PriceRecord {
                price,
                native_locked,
                source_pool,
                block,
            }),
        ));
    }

    // Ordinals must be non-decreasing within a block: they place a write
    // relative to the block's other operations, so a downstream get_at sees
    // exactly the writes that preceded it in chain order. Ties break on the key
    // so the sequence is fully determined.
    writes.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    writes
}

/// Which tokens get a store entry.
///
/// The whitelist, matching the task this module was written for. Everything else
/// in the file is token-agnostic, so widening this to "every token that shares a
/// pool with a whitelisted currency" — which is what the subgraph actually
/// stores, `Token.derivedETH` exists for every token it has seen — is a change
/// to this one predicate.
///
/// It is not the default because the cost is not small: Base V4 has hundreds of
/// thousands of tokens, each would take a permanent store key, and the ones that
/// are not on the whitelist are exactly the ones whose price is manipulable by
/// whoever deployed them. Widen it deliberately, with the depth floor raised.
fn should_store_price(token: &str) -> bool {
    is_whitelisted(token)
}

/// The native price of a leg that can serve as a pricing anchor, or `None`.
///
/// Only the two by-definition cases and the stablecoin reciprocal, because those
/// are the only native prices knowable **within one block without reading this
/// store** — see the module docs. A leg that is not one of these leaves the
/// candidate unable to price its counterpart, which is the correct answer here
/// rather than a limitation to work around: pricing through an unverified
/// intermediate is how a fabricated price gets in.
fn anchor_derived_native(token: &str, stable_derived: Option<&BigDecimal>) -> Option<BigDecimal> {
    if is_native_or_wrapped(token) {
        return Some(BigDecimal::one());
    }
    if is_stablecoin(token) {
        return stable_derived.cloned();
    }
    None
}

/// Depth of the native side of the anchor pool, in native units.
///
/// Only meaningful for a pool with a native/wrapped-native leg (the anchor is
/// WETH/USDC). Zero when neither leg is native, which is the honest answer.
fn native_side_depth(o: &Observation) -> BigDecimal {
    match virtual_reserves(&o.liquidity, &o.sqrt) {
        Some((x, y)) => {
            if is_native_or_wrapped(&o.token0) {
                x / exponent_to_big_decimal(o.dec0)
            } else if is_native_or_wrapped(&o.token1) {
                y / exponent_to_big_decimal(o.dec1)
            } else {
                BigDecimal::zero()
            }
        }
        None => BigDecimal::zero(),
    }
}

/// Where a derived price came from, for the record's provenance fields.
///
/// Re-derives the winning candidate the same way [`find_native_per_token`] picks
/// it. Duplicated selection logic is a real smell, but the alternative is
/// changing the ported function's return type away from the subgraph's shape;
/// the two are kept honest by `zora_attribution_names_the_winning_pool`.
fn attribution(
    token: &str,
    candidates: &[PoolPriceCandidate],
    origin: &BTreeMap<String, (u64, u64)>,
    anchor: Option<&Observation>,
) -> (String, BigDecimal, u64, u64) {
    let mut largest = BigDecimal::zero();
    let mut best: Option<&PoolPriceCandidate> = None;

    for c in candidates {
        let (amount, derived) = if c.token0.eq_ignore_ascii_case(token) {
            (&c.amount1, &c.derived_native1)
        } else if c.token1.eq_ignore_ascii_case(token) {
            (&c.amount0, &c.derived_native0)
        } else {
            continue;
        };
        let derived = match derived {
            Some(d) => d,
            None => continue,
        };
        let locked = amount.clone() * derived.clone();
        if locked.le(&largest) || locked.le(&minimum_active_native()) {
            continue;
        }
        largest = locked;
        best = Some(c);
    }

    if let Some(c) = best {
        let (ordinal, block) = origin.get(&c.pool_id).copied().unwrap_or((0, 0));
        return (c.pool_id.clone(), largest, ordinal, block);
    }

    // By-definition (native/WETH) and stablecoin prices have no source pool of
    // their own; they are the anchor pool's numbers restated.
    match anchor {
        Some(a) => (a.pool_id.clone(), native_side_depth(a), a.ordinal, a.block),
        None => (String::new(), BigDecimal::zero(), 0, 0),
    }
}

// ---------------------------------------------------------------------------
// Store module
// ---------------------------------------------------------------------------

/// Native price in USD, plus the native price of each whitelisted token.
///
/// Consumes `map_enriched`, **not** `map_events`. It has to: pricing needs the
/// token addresses and their decimals on the row, and a raw V4 `Swap` log
/// carries only the poolId. Those fields are exactly what `enrich` joins on from
/// `store_pools` and `store_tokens`.
///
/// Zero RPC — everything here is arithmetic over fields already on the row.
///
/// Wiring (`substreams.yaml`):
///
/// ```yaml
/// - name: store_prices
///   kind: store
///   updatePolicy: set
///   valueType: string
///   initialBlock: 25350988
///   inputs:
///     - map: map_enriched
/// ```
///
/// `set`, not `set_if_not_exists`: a price is current state and every new
/// observation must replace the last one.
#[substreams::handlers::store]
pub fn store_prices(events: pb::Events, store: StoreSetString) {
    for (ord, key, value) in price_writes(&events) {
        store.set(ord, key, &value);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// The vectors are live values pulled from the deployed subgraph
// (Qmbsc6XQWbiv4DfLVfaNciScqYLyDWUYjWzrFBbzzmRsMB) on 2026-08-23, plus one read
// straight off Base via `eth_call` to the PoolManager. Parity is the point of
// this module, so the assertions are against the subgraph's own published
// numbers rather than against a re-derivation of the same formula.

#[cfg(test)]
mod tests {
    use super::*;

    fn bd(s: &str) -> BigDecimal {
        s.parse().expect("test vector must parse")
    }

    fn bi(s: &str) -> BigInt {
        s.parse().expect("test vector must parse")
    }

    /// Assert a computed price matches a value published by the deployed
    /// subgraph, to 30 significant digits.
    ///
    /// Not `assert_eq!`, and the reason is on [`normalize`]: graph-node
    /// re-rounds to 34 significant digits after every intermediate operation
    /// using a `bigdecimal` old enough that its behaviour at that boundary is
    /// not reproducible from here. Three of the four live vectors in this module
    /// do match to all 34 digits; the WETH/USDC anchor differs by one unit in
    /// the last place. Pinning the 34th digit would mean asserting a guess about
    /// a graph-node internal, so the tolerance is set where the comparison is
    /// still meaningful: 1 part in 10^30, about 20 orders of magnitude below the
    /// smallest representable unit of any token here.
    ///
    /// A relative difference rather than `with_prec(30)` on both sides, so two
    /// values straddling a rounding boundary cannot fail spuriously.
    fn assert_price_eq(got: &BigDecimal, expected: &BigDecimal, what: &str) {
        let diff = (got.clone() - expected.clone()).absolute();
        let tolerance = expected.absolute() * BigDecimal::new(BigInt::one(), -30);
        assert!(
            diff.le(&tolerance),
            "{what}: got {got}, subgraph published {expected} (difference {diff} exceeds {tolerance})"
        );
    }

    // -- the anchor pool, WETH/USDC ------------------------------------------
    //
    // {
    //   pool(id: "0x90333bb0…43a0") {
    //     sqrtPrice   3919900223035769954383743
    //     token0 { WETH, decimals 18, derivedETH 1 }
    //     token1 { USDC, decimals 6,  derivedETH 0.0004085160671064765170603112726892855 }
    //     token0Price 0.0004085160671064765170603112726892855
    //     token1Price 2447.884136070853298957238976519198
    //     liquidity   214083570746177
    //     totalValueLockedToken0 1.482045031662824921
    //   }
    //   bundles { ethPriceUSD 2447.884136070853298957238976519198 }
    // }
    const ANCHOR_SQRT: &str = "3919900223035769954383743";
    const ANCHOR_TOKEN0_PRICE: &str = "0.0004085160671064765170603112726892855";
    const ANCHOR_TOKEN1_PRICE: &str = "2447.884136070853298957238976519198";
    const ANCHOR_LIQUIDITY: &str = "214083570746177";

    #[test]
    fn anchor_pool_prices_match_deployed_subgraph() {
        let (price0, price1) = sqrt_price_x96_to_token_prices(&bi(ANCHOR_SQRT), 18, 6);

        // token1Price is the ETH price in USD — the subgraph's Bundle.
        assert_price_eq(&price1, &bd(ANCHOR_TOKEN1_PRICE), "token1Price / ethPriceUSD");
        // token0Price is WETH per USDC, which is also USDC.derivedETH.
        assert_price_eq(&price0, &bd(ANCHOR_TOKEN0_PRICE), "token0Price / USDC.derivedETH");
    }

    #[test]
    fn native_price_reads_the_configured_side_of_the_anchor() {
        let (price0, price1) = sqrt_price_x96_to_token_prices(&bi(ANCHOR_SQRT), 18, 6);
        // stablecoinIsToken0 == false on Base, so the native price is token1Price.
        // Getting this backwards inverts every USD figure in the package, so it
        // is asserted rather than assumed — the wrong side is 0.00040851…,
        // which is a plausible-looking number.
        assert!(!STABLECOIN_IS_TOKEN0);
        let native_usd = if STABLECOIN_IS_TOKEN0 { price0 } else { price1 };
        assert_price_eq(&native_usd, &bd(ANCHOR_TOKEN1_PRICE), "ethPriceUSD");
    }

    #[test]
    fn onchain_slot0_reproduces_the_price_on_base() {
        // Read live from Base, not from the subgraph:
        //   eth_call PoolManager.extsload(keccak256(poolId . uint256(6)))
        //   slot     0xe570f6e770bf85faa3d1dbee2fa168b56036a048a7939edbcd02d7ebddf3f948
        //   returned 0x0000000001f407d07dfcf969…033de6457228b0fb16bf4b @ block 0x300a057
        //   low 160 bits => sqrtPriceX96, next 24 => tick -198295
        //
        // A few blocks newer than the subgraph snapshot above, hence a slightly
        // different price. The expectation is computed independently at 90
        // digits and rounded to graph-node's 34, so this test checks the maths
        // rather than restating it.
        let (_, price1) = sqrt_price_x96_to_token_prices(&bi("3919089569542765632995147"), 18, 6);
        assert_eq!(price1, bd("2446.871773244048238509914655207564"));
    }

    #[test]
    fn anchor_pool_at_its_own_initialize_prices_eth_at_the_january_2025_rate() {
        // Straight out of this package's own live output, not the subgraph:
        //
        //   substreams run …spkg map_enriched -s 25384712 -t +2000 \
        //       --limit-processed-blocks 0 --production-mode
        //
        // returned the anchor pool's Initialize at block 25384712 —
        //   sqrtPrice 4551128978600852286704640, feeTier 500, hook 0x0 —
        // and, on the ModifyLiquidity row in the same transaction,
        //   token0Symbol WETH / token1Symbol USDC /
        //   token0Decimals 18 / token1Decimals 6 / decimalsMeasured TRUE.
        //
        // Two things are pinned here that nothing else in this file can pin.
        // First, the decimals this module refuses to work without really do
        // arrive measured on real Base rows — if store_tokens could not read
        // WETH and USDC, every price in the package would be silently absent
        // and every unit test would still pass. Second, the answer is
        // externally checkable: that block's timestamp is 2025-01-22 15:12:51
        // UTC and ETH traded near $3,300 that day. An inverted pair or a
        // decimals slip would land nowhere near it.
        let (_, price1) = sqrt_price_x96_to_token_prices(&bi("4551128978600852286704640"), 18, 6);
        assert_eq!(price1, bd("3299.735427752230428959246129763949"));
        assert!(price1.gt(&bd("3000")) && price1.lt(&bd("3600")), "got {}", price1);
    }

    #[test]
    fn native_leg_uses_native_decimals_not_the_token_store() {
        // Pool 0x45b5071121b28bd8945d169f63ab834860a741f523c85d8df17032ffd9523ced:
        // native ETH (address(0)) / jUSDC, subgraph token1Price 11225.26887…
        //
        // The point of the vector is the token0 leg: there is no ERC-20 at
        // address(0) to answer decimals(), so the 18 has to come from the chain
        // config. Pass a deliberately hostile store value — 0 decimals, flagged
        // unmeasured — and the native override must still yield 18.
        let dec0 = effective_decimals(NATIVE_ADDRESS, 0, false).expect("native is always known");
        assert_eq!(dec0, NATIVE_DECIMALS);

        let (price0, price1) =
            sqrt_price_x96_to_token_prices(&bi("8394173856595859619831874"), dec0, 6);
        assert_price_eq(&price1, &bd("11225.26887489052488310959320106534"), "token1Price");
        assert_price_eq(
            &price0,
            &bd("0.00008908472582219128009729158770962167"),
            "token0Price",
        );
    }

    #[test]
    fn unmeasured_decimals_yield_no_price() {
        // The hazard this module exists to avoid: store_tokens defaults an
        // unreadable decimals() to 18. Pricing a 6-decimal token as 18-decimal
        // is off by 10^12 and looks entirely plausible on a chart.
        assert_eq!(effective_decimals(USDC, 6, false), None);
        assert_eq!(effective_decimals(USDC, 6, true), Some(6));
        // …but a defaulted value for the native leg is not a guess at all.
        assert_eq!(effective_decimals(NATIVE_ADDRESS, 18, false), Some(18));
    }

    #[test]
    fn absurd_decimals_are_rejected_before_building_a_power_of_ten() {
        // decimals() is attacker-controlled. 10^4294967295 would OOM the module
        // and take the stream down with it.
        assert_eq!(effective_decimals(ZORA, u32::MAX, true), None);
        assert_eq!(effective_decimals(ZORA, MAX_SANE_DECIMALS + 1, true), None);
        assert_eq!(
            effective_decimals(ZORA, MAX_SANE_DECIMALS, true),
            Some(MAX_SANE_DECIMALS)
        );
    }

    #[test]
    fn zero_sqrt_price_is_not_a_price_of_zero() {
        // V4 aggregator hooks emit swaps with sqrtPriceX96 = 0. The naive
        // reciprocal is a division by zero, which panics in
        // substreams::scalar::BigDecimal and would abort the stream.
        let (price0, price1) = sqrt_price_x96_to_token_prices(&BigInt::zero(), 18, 6);
        assert!(price0.is_zero());
        assert!(price1.is_zero());

        // And such a row must never become an observation.
        assert!(observation(
            "0xpool",
            WRAPPED_NATIVE,
            USDC,
            18,
            6,
            true,
            "0",
            "1000",
            Some(&pb::Meta::default()),
        )
        .is_none());
    }

    #[test]
    fn native_and_wrapped_native_are_one_by_definition() {
        assert_eq!(
            find_native_per_token(NATIVE_ADDRESS, None, &[], &minimum_native_locked()),
            Some(BigDecimal::one())
        );
        assert_eq!(
            find_native_per_token(WRAPPED_NATIVE, None, &[], &minimum_native_locked()),
            Some(BigDecimal::one())
        );
        // No pools, no bundle — still 1. This is the branch that keeps the
        // whole graph from being circular.
    }

    #[test]
    fn stablecoin_price_is_the_reciprocal_of_the_bundle() {
        // The subgraph's "hardcoded fix for incorrect rates": USDC is not priced
        // off a pool at all, it is 1/ethPriceUSD. Verified against the deployed
        // USDC.derivedETH, which is also the anchor pool's token0Price — the two
        // agree to all 34 digits, which is the check that the reciprocal
        // shortcut and the pool price are the same number.
        let native_usd = bd(ANCHOR_TOKEN1_PRICE);
        let got = find_native_per_token(USDC, Some(&native_usd), &[], &minimum_native_locked())
            .expect("stablecoin branch");
        // Exact, not tolerant: fed the subgraph's own published ethPriceUSD this
        // reproduces its published USDC.derivedETH to all 34 digits, which also
        // shows graph-node derives token0Price from the already-rounded
        // token1Price rather than from the raw quotient.
        assert_eq!(got, bd(ANCHOR_TOKEN0_PRICE));
    }

    #[test]
    fn stablecoin_without_a_bundle_is_unpriced_not_zero() {
        assert_eq!(
            find_native_per_token(USDC, None, &[], &minimum_native_locked()),
            None
        );
        assert_eq!(
            find_native_per_token(
                USDC,
                Some(&BigDecimal::zero()),
                &[],
                &minimum_native_locked()
            ),
            None
        );
    }

    /// The native/ZORA pool as the subgraph reports it. Built from entity fields
    /// so `find_native_per_token` is driven with the subgraph's own TVL numbers
    /// and the ported `minimumNativeLocked` of 1.
    ///
    /// pool 0xd694bd7285eeeee19d3d5da38f613859168c422d628def88a0c95dad12071f3a
    fn zora_candidate_from_subgraph() -> PoolPriceCandidate {
        PoolPriceCandidate {
            pool_id: "0xd694bd7285eeeee19d3d5da38f613859168c422d628def88a0c95dad12071f3a"
                .to_string(),
            token0: NATIVE_ADDRESS.to_string(),
            token1: ZORA.to_string(),
            price0: bd("0.000002760022414164888548236587471837841"),
            price1: bd("362315.898185404467351247177309118"),
            amount0: bd("13.31826892329505033"),
            amount1: bd("1429982.246603943821178796"),
            derived_native0: Some(BigDecimal::one()),
            derived_native1: None,
        }
    }

    #[test]
    fn zora_matches_deployed_subgraph() {
        // ZORA is the only Base whitelist token that actually goes through the
        // pool search. The deployed subgraph reports
        //   ZORA.derivedETH = 0.000002760022414164888548236587471837841
        // and this pool's token0Price is that number, which confirms the
        // subgraph selected this pool and that "token0 per token1" is the right
        // side of the pair to read.
        let got = find_native_per_token(
            ZORA,
            None,
            &[zora_candidate_from_subgraph()],
            &minimum_native_locked(),
        )
        .expect("ZORA priced from the native pool");
        assert_price_eq(
            &got,
            &bd("0.000002760022414164888548236587471837841"),
            "ZORA.derivedETH",
        );
    }

    #[test]
    fn deepest_pool_wins_and_shallow_pools_are_rejected() {
        let deep = zora_candidate_from_subgraph();

        // Same token, a wrong price, and more depth — it must win.
        let mut deeper = deep.clone();
        deeper.pool_id = "0xdeeper".to_string();
        deeper.price0 = bd("0.5");
        deeper.amount0 = bd("999");

        let got = find_native_per_token(
            ZORA,
            None,
            &[deep.clone(), deeper],
            &minimum_native_locked(),
        );
        assert_eq!(got, Some(bd("0.5")));

        // Below the floor, it must not be used at all — not even as a fallback.
        let mut dust = deep.clone();
        dust.pool_id = "0xdust".to_string();
        dust.price0 = bd("0.5");
        dust.amount0 = bd("0.0001");
        assert_eq!(
            find_native_per_token(ZORA, None, &[dust], &minimum_native_locked()),
            None
        );
    }

    #[test]
    fn a_pool_whose_other_leg_is_unpriced_cannot_price_anything() {
        // The recursion a store cannot do: ZORA/USWR, where USWR's own
        // derivedETH came from some other pool. Left as None, the candidate is
        // skipped rather than treated as derivedETH = 0 (which is what the
        // subgraph's freshly-created Token entity would multiply through).
        let c = PoolPriceCandidate {
            pool_id: "0x2d3627dc27b755069a5612444f30ccf7cc7897bab68c5396b1713f6ee1b6d526"
                .to_string(),
            token0: "0x0e9fa3a35625c7b8aaca93bdc635227d117f5ad8".to_string(), // USWR
            token1: ZORA.to_string(),
            price0: bd("113.8075115387424191517720351620583"),
            price1: bd("0.008786766237829384569447992350572425"),
            amount0: bd("903162039.85973230733708887"),
            amount1: bd("473984.583027929255191221"),
            derived_native0: None,
            derived_native1: None,
        };
        assert_eq!(
            find_native_per_token(ZORA, None, &[c], &minimum_native_locked()),
            None
        );
    }

    #[test]
    fn virtual_reserves_track_the_anchor_pool() {
        // L / sqrt(P) on the anchor pool's live state. The subgraph's TVL for
        // the same pool is 1.482 WETH — see minimum_active_native() for why
        // these two numbers are allowed to differ by 2.9x and why the ported
        // floor of 1 is therefore not the floor store_prices uses.
        let (x, _y) = virtual_reserves(&bi(ANCHOR_LIQUIDITY), &bi(ANCHOR_SQRT)).unwrap();
        let weth = x / exponent_to_big_decimal(18);
        assert!(weth.gt(&bd("4.3")) && weth.lt(&bd("4.4")), "got {}", weth);
        // Comfortably over the dust floor, which is the property that matters.
        assert!(weth.gt(&minimum_active_native()));
    }

    #[test]
    fn zero_liquidity_pool_has_no_depth() {
        // Base really has pools parked at MAX_SQRT_RATIO with liquidity 0,
        // quoting 3.4e50 token1 per token0. The price is arithmetically real;
        // the depth is what disqualifies it.
        let sqrt = bi("1461446703485210103287273052203988822378723970341");
        let (x, y) = virtual_reserves(&BigInt::zero(), &sqrt).unwrap();
        assert!(x.is_zero() && y.is_zero());

        let c = PoolPriceCandidate {
            pool_id: "0x94926d37e3fb86808b439363b1ad479abfcf7140ab89f1fb558ac4233dcf978f"
                .to_string(),
            token0: NATIVE_ADDRESS.to_string(),
            token1: "0xf86e6dda215eee773cb475806321ee95496ba7c0".to_string(), // BR
            price0: bd("2.9389568075855848387034172747685E-51"),
            price1: bd("340256786836388094070642339899681200000000000000000"),
            amount0: BigDecimal::zero(),
            amount1: BigDecimal::zero(),
            derived_native0: Some(BigDecimal::one()),
            derived_native1: None,
        };
        assert_eq!(
            find_native_per_token(
                "0xf86e6dda215eee773cb475806321ee95496ba7c0",
                None,
                &[c],
                &minimum_active_native()
            ),
            None
        );
    }

    #[test]
    fn value_codec_round_trips() {
        let rec = PriceRecord {
            price: bd(ANCHOR_TOKEN1_PRICE),
            native_locked: bd("4.327"),
            source_pool: STABLECOIN_NATIVE_POOL_ID.to_string(),
            block: 35_000_000,
        };
        let encoded = encode_price_value(&rec);
        assert_eq!(encoded.matches(FIELD_SEP).count(), 3);
        assert_eq!(decode_price_value(&encoded), Some(rec));

        // Malformed input yields nothing, never a zero price.
        assert_eq!(decode_price_value(""), None);
        assert_eq!(decode_price_value("1\u{1f}2\u{1f}3"), None);
        assert_eq!(decode_price_value("notanumber\u{1f}2\u{1f}0x\u{1f}1"), None);
    }

    #[test]
    fn keys_are_namespaced_and_lowercased() {
        assert_eq!(
            derived_native_key("0xABCdef0000000000000000000000000000000001"),
            "derived_native:0xabcdef0000000000000000000000000000000001"
        );
        assert_eq!(NATIVE_USD_KEY, "native_usd");
        // Guards the writer/reader contract: changing these silently unprices
        // every consumer rather than failing.
        assert_eq!(DERIVED_NATIVE_PREFIX, "derived_native:");
    }

    // -- end-to-end over a synthetic block ------------------------------------

    fn meta(block: u64, log_index: u32) -> Option<pb::Meta> {
        Some(pb::Meta {
            block_number: block,
            log_index,
            ..Default::default()
        })
    }

    fn swap(
        pool_id: &str,
        token0: &str,
        token1: &str,
        dec0: u32,
        dec1: u32,
        measured: bool,
        sqrt: &str,
        liquidity: &str,
        log_index: u32,
    ) -> pb::Swap {
        pb::Swap {
            pool_id: pool_id.to_string(),
            token0: token0.to_string(),
            token1: token1.to_string(),
            token0_decimals: dec0,
            token1_decimals: dec1,
            decimals_measured: measured,
            sqrt_price_x96: sqrt.to_string(),
            liquidity: liquidity.to_string(),
            meta: meta(35_000_000, log_index),
            ..Default::default()
        }
    }

    /// A block containing a swap on the anchor pool and a swap on the
    /// native/ZORA pool — the two that between them produce every price on the
    /// Base whitelist.
    fn realistic_block() -> pb::Events {
        pb::Events {
            swaps: vec![
                swap(
                    STABLECOIN_NATIVE_POOL_ID,
                    WRAPPED_NATIVE,
                    USDC,
                    18,
                    6,
                    true,
                    ANCHOR_SQRT,
                    ANCHOR_LIQUIDITY,
                    7,
                ),
                swap(
                    "0xd694bd7285eeeee19d3d5da38f613859168c422d628def88a0c95dad12071f3a",
                    NATIVE_ADDRESS,
                    ZORA,
                    18,
                    18,
                    true,
                    "47689556018669185362083349439020",
                    "49107413195407756813",
                    12,
                ),
            ],
            ..Default::default()
        }
    }

    fn writes_map(events: &pb::Events) -> BTreeMap<String, PriceRecord> {
        price_writes(events)
            .into_iter()
            .map(|(_, k, v)| (k, decode_price_value(&v).expect("own encoding decodes")))
            .collect()
    }

    #[test]
    fn block_produces_bundle_and_whitelist_prices() {
        let w = writes_map(&realistic_block());

        assert_price_eq(&w[NATIVE_USD_KEY].price, &bd(ANCHOR_TOKEN1_PRICE), "bundle");
        assert_eq!(w[NATIVE_USD_KEY].source_pool, STABLECOIN_NATIVE_POOL_ID);
        assert_eq!(w[NATIVE_USD_KEY].block, 35_000_000);

        // USDC: the reciprocal shortcut, equal to the anchor's token0Price.
        assert_price_eq(
            &w[&derived_native_key(USDC)].price,
            &bd(ANCHOR_TOKEN0_PRICE),
            "USDC.derivedETH",
        );

        // ZORA: the pool search, equal to the deployed subgraph's derivedETH.
        assert_price_eq(
            &w[&derived_native_key(ZORA)].price,
            &bd("0.000002760022414164888548236587471837841"),
            "ZORA.derivedETH",
        );

        // WETH and native ETH are 1 by definition and are deliberately NOT
        // stored — a constant rewritten on every block is pure store churn, and
        // find_native_per_token answers for them without a read. A consumer
        // must treat a missing derived_native for these two as 1, not as
        // unpriced; that is the one thing this store expects of its readers.
        assert!(!w.contains_key(&derived_native_key(WRAPPED_NATIVE)));
        assert!(!w.contains_key(&derived_native_key(NATIVE_ADDRESS)));
    }

    #[test]
    fn zora_attribution_names_the_winning_pool() {
        let w = writes_map(&realistic_block());
        let zora = &w[&derived_native_key(ZORA)];
        assert_eq!(
            zora.source_pool,
            "0xd694bd7285eeeee19d3d5da38f613859168c422d628def88a0c95dad12071f3a"
        );
        // Virtual depth on the native leg: ~0.0816 ETH, over the dust floor and
        // 163x under the subgraph's 13.3 ETH TVL for the same pool.
        assert!(zora.native_locked.gt(&minimum_active_native()));
        assert!(zora.native_locked.lt(&bd("0.1")));
    }

    #[test]
    fn writes_are_ordinal_sorted_and_deterministic() {
        let events = realistic_block();
        let first = price_writes(&events);
        let second = price_writes(&events);
        assert_eq!(first, second, "same input must produce identical writes");

        let ords: Vec<u64> = first.iter().map(|(o, _, _)| *o).collect();
        assert!(
            ords.windows(2).all(|w| w[0] <= w[1]),
            "store ordinals must be non-decreasing: {:?}",
            ords
        );
    }

    #[test]
    fn last_swap_in_the_block_sets_the_price() {
        // The subgraph recomputes on every swap, so end-of-block state comes
        // from the highest log index — not the first swap seen.
        let mut events = realistic_block();
        events.swaps.push(swap(
            STABLECOIN_NATIVE_POOL_ID,
            WRAPPED_NATIVE,
            USDC,
            18,
            6,
            true,
            "3919089569542765632995147", // the later, on-chain-verified state
            ANCHOR_LIQUIDITY,
            21,
        ));
        let w = writes_map(&events);
        assert_eq!(
            w[NATIVE_USD_KEY].price,
            bd("2446.871773244048238509914655207564")
        );
    }

    #[test]
    fn an_unenriched_block_prices_nothing() {
        // Swaps whose pool was never in store_pools carry empty tokens. There is
        // nothing to price and, critically, nothing is written — a zero here
        // would overwrite a good price with a lie.
        let events = pb::Events {
            swaps: vec![swap(
                STABLECOIN_NATIVE_POOL_ID,
                "",
                "",
                0,
                0,
                false,
                ANCHOR_SQRT,
                ANCHOR_LIQUIDITY,
                3,
            )],
            ..Default::default()
        };
        assert!(price_writes(&events).is_empty());
    }

    #[test]
    fn unmeasured_decimals_block_the_whole_pool() {
        let mut events = realistic_block();
        events.swaps[0].decimals_measured = false;
        let w = writes_map(&events);
        // No native price at all, rather than one computed off a defaulted 18
        // for USDC — which would report ETH at ~$2.4e-6 instead of $2447.
        assert!(!w.contains_key(NATIVE_USD_KEY));
        assert!(!w.contains_key(&derived_native_key(USDC)));
        // ZORA is unaffected: its pool is native/ZORA and prices itself.
        assert!(w.contains_key(&derived_native_key(ZORA)));
    }

    #[test]
    fn a_quiet_anchor_leaves_the_stablecoin_unpriced_this_block() {
        // Only the ZORA pool traded. ZORA still prices (its anchor leg is
        // native), USDC does not (its price needs the bundle). Nothing is
        // written for USDC, so the store keeps its previous value.
        let mut events = realistic_block();
        events.swaps.remove(0);
        let w = writes_map(&events);
        assert!(!w.contains_key(NATIVE_USD_KEY));
        assert!(!w.contains_key(&derived_native_key(USDC)));
        assert!(w.contains_key(&derived_native_key(ZORA)));
    }

    #[test]
    fn non_whitelisted_tokens_are_not_stored() {
        let events = pb::Events {
            swaps: vec![swap(
                "0x45b5071121b28bd8945d169f63ab834860a741f523c85d8df17032ffd9523ced",
                NATIVE_ADDRESS,
                "0x944766f715b51967e56afde5f0aa76ceacc9e7f9", // jUSDC, not whitelisted
                18,
                6,
                true,
                "8394173856595859619831874",
                "5019952301537",
                4,
            )],
            ..Default::default()
        };
        let w = writes_map(&events);
        assert!(w.is_empty(), "expected no writes, got {:?}", w.keys());
    }

    #[test]
    fn config_matches_the_subgraph_base_branch() {
        // Byte-for-byte against chains.ts BASE_NETWORK_NAME. These are compared
        // to lowercase row values, so a checksummed constant would never match
        // and every token would quietly go unpriced.
        for a in WHITELIST_TOKENS.iter().chain(STABLECOINS.iter()) {
            assert_eq!(*a, a.to_ascii_lowercase(), "{} must be lowercase", a);
        }
        assert_eq!(STABLECOIN_NATIVE_POOL_ID.len(), 66); // 0x + 32 bytes
        assert!(is_whitelisted(WRAPPED_NATIVE));
        assert!(is_whitelisted(NATIVE_ADDRESS));
        assert!(is_whitelisted(USDC));
        assert!(is_whitelisted(ZORA));
        assert!(is_stablecoin(USDC));
        assert!(!is_stablecoin(WRAPPED_NATIVE));
        // Case-insensitive matching, because a checksummed address arriving on a
        // row must still be recognised.
        assert!(is_native_or_wrapped("0x4200000000000000000000000000000000000006"));
        assert!(is_whitelisted(&ZORA.to_ascii_uppercase().replace("0X", "0x")));
    }

    #[test]
    fn exponent_to_big_decimal_is_a_power_of_ten() {
        assert_eq!(exponent_to_big_decimal(0), BigDecimal::one());
        assert_eq!(exponent_to_big_decimal(6), bd("1000000"));
        assert_eq!(exponent_to_big_decimal(18), bd("1000000000000000000"));
    }

    #[test]
    fn safe_div_does_not_panic_on_zero() {
        // BigDecimal::div panics on a zero divisor and a wasm panic takes down
        // the whole stream, not just the block.
        assert!(safe_div(&BigDecimal::one(), &BigDecimal::zero()).is_zero());
        assert_eq!(safe_div(&bd("10"), &bd("4")), bd("2.5"));
    }
}

#[cfg(test)]
mod chain_verified_tests {
    use super::*;

    /// Pinned against three independent sources, not against our own output.
    ///
    /// Pool 0x36d7043e… (VVV/cbBTC) on Base. `sqrtPriceX96` read live from
    /// PoolManager slot0 via `extsload`, and the expected prices independently
    /// reproduced from that value AND confirmed against the deployed subgraph's
    /// own `token0Price` / `token1Price` to 12+ significant figures.
    ///
    /// This is the asymmetric-decimals case (18 vs 8) — the one where price
    /// maths goes wrong quietly.
    #[test]
    fn matches_chain_and_subgraph_on_asymmetric_decimals() {
        let sqrt: BigInt = "11586150387487561443760".parse().unwrap();
        let (price0, price1) = sqrt_price_x96_to_token_prices(&sqrt, 18, 8);

        // subgraph token0Price = 4676.068288046662942935418702587255
        assert!(
            price0.to_string().starts_with("4676.0682880466"),
            "token0Price was {price0}"
        );
        // subgraph token1Price = 0.0002138548751643083152004958039546759
        assert!(
            price1.to_string().starts_with("0.000213854875164"),
            "token1Price was {price1}"
        );
    }

    /// Same pool family, symmetric 18/18 decimals, from the ETH/$SXR pool
    /// 0x0a1e0f12…; sqrtPriceX96 also read live via extsload.
    #[test]
    fn matches_chain_on_symmetric_decimals() {
        let sqrt: BigInt = "250167833202492696714480213532482".parse().unwrap();
        let (_price0, price1) = sqrt_price_x96_to_token_prices(&sqrt, 18, 18);
        assert!(
            price1.to_string().starts_with("9970197.6178"),
            "token1 per token0 was {price1}"
        );
    }

    /// An aggregator-hook swap really does emit sqrtPriceX96 = 0. The module
    /// must yield zero rather than abort the block on a divide.
    #[test]
    fn zero_sqrt_price_does_not_panic() {
        let (p0, p1) = sqrt_price_x96_to_token_prices(&BigInt::zero(), 18, 18);
        assert_eq!(p1.to_string(), "0");
        assert_eq!(p0.to_string(), "0");
    }
}
