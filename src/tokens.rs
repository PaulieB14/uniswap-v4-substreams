//! Token metadata — symbol, name, decimals — for V4 *currencies*.
//!
//! # Why this module exists
//!
//! Every amount this package emits is a raw on-chain integer. `amount0 =
//! -1000000` is 1 USDC or 0.000000000001 WETH depending entirely on a number
//! that lives on the token contract and appears in no log. Without decimals a
//! consumer cannot scale, cannot compare two legs of a swap, and cannot price
//! anything; without a symbol it cannot even name the row. The source subgraph
//! solves this in `src/utils/token.ts` by calling the token contract at pool
//! creation and storing a `Token` entity. Substreams has no implicit entity
//! store, so the fetch and the cache have to be explicit — that is
//! [`fetch_token_meta`] and [`store_tokens`] below.
//!
//! # Cost model (read this before changing anything)
//!
//! An `eth_call` is the single most expensive thing a Substreams module can do:
//! it is a synchronous round trip to an archive node in the middle of a block's
//! processing, it defeats parallel backfill caching, and it is the usual reason
//! a package is slow. So:
//!
//!   * We call **only for currencies seen in a pool `Initialize`**, never per
//!     swap. Swaps outnumber initialisations here by roughly 14:1 over the
//!     verified window (934 swaps vs 67 pools per 150 blocks) and every swap's
//!     tokens are already knowable from its pool.
//!   * Three calls per token (`symbol()`, `name()`, `decimals()`) go into **one**
//!     `RpcCalls` batch for the whole block, not one batch per token.
//!   * Results land in a store keyed by address, so later blocks read them for
//!     free.
//!
//! Remaining cost, stated plainly rather than hidden: a `store` handler cannot
//! read its own store, so a token that appears in N pool initialisations pays
//! the RPC N times (WETH on Base will do this a lot). Eliminating that needs a
//! `map` that takes `store_tokens` as a `get` input, filters out already-known
//! addresses, and feeds a second store — the canonical map→store→map shape from
//! the substreams-ethereum skill. That is a module-graph change in
//! `substreams.yaml`, which this file does not own; the fetch/encode/decode
//! logic here is written to be reused unchanged by such a module.
//!
//! # Divergences from the source subgraph
//!
//!   * **One call, both symbol shapes.** The subgraph binds two ABIs
//!     (`ERC20` and `ERC20SymbolBytes`) and pays a *second* `eth_call` when the
//!     first reverts. We decode the same returndata as either a dynamic
//!     `string` or a `bytes32`, so a MKR-style token costs exactly what a
//!     compliant one costs.
//!   * **A token with unreadable `decimals()` is not dropped.** The subgraph
//!     returns `null` from `fetchTokenDecimals`, and its caller then refuses to
//!     create the `Token` — silently losing the pool. We emit a placeholder and
//!     record, in the stored value, that the 18 is a guess.
//!   * **No static override table.** `chains.ts` carries a `tokenOverrides`
//!     list for tokens whose on-chain metadata is broken; for the `base`
//!     network that list is empty (`tokenOverrides: []`), so there is nothing
//!     to port. Add one here if a Base token ever needs it.

use std::collections::{HashMap, HashSet};

use substreams::store::{StoreNew, StoreSet, StoreSetString};
use substreams::Hex;
use substreams_ethereum::pb::eth::rpc::{RpcCall, RpcCalls};
use substreams_ethereum::rpc;

use crate::pb::uniswap::v4::v1 as pb;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// `keccak256("symbol()")[0..4]`, verified with `cast sig "symbol()"`.
const SELECTOR_SYMBOL: [u8; 4] = [0x95, 0xd8, 0x9b, 0x41];
/// `keccak256("name()")[0..4]`.
const SELECTOR_NAME: [u8; 4] = [0x06, 0xfd, 0xde, 0x03];
/// `keccak256("decimals()")[0..4]`.
const SELECTOR_DECIMALS: [u8; 4] = [0x31, 0x3c, 0xe5, 0x67];

/// Calls issued per token, in the order they are pushed into the batch.
/// `responses[i]` is positional, so this constant and the push order in
/// [`fetch_chunk`] must stay in lockstep.
const CALLS_PER_TOKEN: usize = 3;

/// Tokens per `eth_call` batch (× 3 = 75 calls). Batching is the whole point,
/// but an unbounded batch is a different failure: some RPC providers cap batch
/// size or payload bytes and reject the *entire* request, which would turn one
/// unusual block into zero resolved tokens. Chunking bounds the blast radius.
const MAX_TOKENS_PER_BATCH: usize = 25;

/// Cap on stored symbol/name length. Token names are attacker-controlled
/// strings — a token can call itself 100 kB of text — and this value ends up in
/// a store and then in a Postgres column.
const MAX_TEXT_LEN: usize = 128;

/// The subgraph's `fetchTokenDecimals` rejects anything not `< 255`. Kept
/// identical so decimal handling matches row for row.
const MAX_DECIMALS: u64 = 255;

/// V4 uses `address(0)` as the *native currency*, unlike V3 where every pool
/// leg is an ERC-20. `Initialize.currency0` is zero for every native-ETH pool,
/// and those are common on Base.
pub const NATIVE_ADDRESS_HEX: &str = "0x0000000000000000000000000000000000000000";

/// Native currency identity for the `base` network. Matches the subgraph's
/// `nativeTokenDetails` for `BASE_NETWORK_NAME` exactly — `ETH` / `Ethereum` /
/// `18`, not "Ether" — so rows join against the deployed subgraph's `Token`.
/// If this package is ever pointed at a non-ETH chain, this is the constant to
/// change (the subgraph makes it per-network for that reason).
pub const NATIVE_SYMBOL: &str = "ETH";
pub const NATIVE_NAME: &str = "Ethereum";
pub const NATIVE_DECIMALS: u64 = 18;

/// What an unresolvable symbol/name becomes. Lowercase `unknown`, byte-identical
/// to the subgraph's literal.
pub const UNKNOWN_TEXT: &str = "unknown";

/// What an unresolvable `decimals()` becomes. **This is a guess**, and using it
/// on a 6-decimal token misprices by 10^12 — which is why the stored value
/// carries a flag saying whether the number was measured or defaulted.
pub const DEFAULT_DECIMALS: u64 = 18;

/// Store-key namespace. Keys are `token:0x<40 lowercase hex>`, i.e. the prefix
/// plus exactly what [`crate::hooks::addr_hex`] produced into `Pool.token0`, so
/// a consumer holding a pool row can build the key with no reformatting.
pub const TOKEN_KEY_PREFIX: &str = "token:";

/// Field separator inside a stored value: ASCII US (unit separator, 0x1f).
///
/// Chosen over `|`/`,`/`:` because those occur in real token names and a token
/// can *deliberately* name itself `A|B|18` to forge a neighbouring field.
/// [`sanitize`] strips every control character, which makes it impossible for
/// this byte to survive into a field — the encoding is unambiguous by
/// construction rather than by hope.
pub const FIELD_SEP: char = '\u{1f}';

/// Some non-compliant contracts answer a failed `symbol()`/`name()` with a
/// single word of `0x…01` rather than reverting. The subgraph screens for this
/// exact value (`isNullEthValue`); decoding it as `bytes32` would yield a
/// control-character "symbol".
const NULL_ETH_VALUE: [u8; 32] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Resolved metadata for one currency.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenMeta {
    /// `0x`-prefixed lowercase hex, same formatting as `Pool.token0`.
    pub address: String,
    pub symbol: String,
    pub name: String,
    /// Never `None`: an unreadable `decimals()` becomes [`DEFAULT_DECIMALS`].
    /// Check [`Resolution::decimals_measured`] before trusting it for money.
    pub decimals: u64,
}

/// Where a text field actually came from. Carried alongside [`TokenMeta`] so a
/// downstream consumer can tell a real `"unknown"` symbol (some tokens ship
/// one) from our fallback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextSource {
    /// Hardcoded native-currency identity; no call was made.
    Native,
    /// Decoded from a standard ABI dynamic `string` return.
    AbiString,
    /// Decoded from a `bytes32` return — MKR and other pre-ERC-20-final tokens.
    Bytes32,
    /// Call failed, reverted, or returned undecodable data.
    Fallback,
}

impl TextSource {
    /// One-character tag used in the stored encoding.
    pub fn tag(self) -> char {
        match self {
            TextSource::Native => 'n',
            TextSource::AbiString => 's',
            TextSource::Bytes32 => 'b',
            TextSource::Fallback => '?',
        }
    }

    #[allow(dead_code)] // read back by consumers via decode_token_value
    fn from_tag(c: char) -> Option<Self> {
        match c {
            'n' => Some(TextSource::Native),
            's' => Some(TextSource::AbiString),
            'b' => Some(TextSource::Bytes32),
            '?' => Some(TextSource::Fallback),
            _ => None,
        }
    }
}

/// Provenance for a [`TokenMeta`]. Kept out of `TokenMeta` itself so the struct
/// stays the plain four-field value type the rest of the package consumes,
/// while the honesty still reaches the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resolution {
    pub symbol: TextSource,
    pub name: TextSource,
    /// `false` means `decimals` is [`DEFAULT_DECIMALS`] because nothing usable
    /// came back — do not scale user-facing amounts with it silently.
    pub decimals_measured: bool,
}

/// A [`TokenMeta`] plus how much of it is actually known.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedToken {
    pub meta: TokenMeta,
    pub resolution: Resolution,
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

/// Resolve metadata for a set of currency addresses.
///
/// **Deduplicated, not positional.** The result has one entry per *distinct*
/// input address, in first-seen order — a pool contributes `[token0, token1]`
/// and a WETH/USDC pool plus a WETH/DAI pool yields three entries, not four.
/// Match results to inputs by `TokenMeta::address`, never by index.
///
/// Never panics and never returns fewer than one entry per distinct address: a
/// reverted call, a garbage return, a non-contract address and a total RPC
/// failure all degrade to a placeholder. A panic inside a WASM handler aborts
/// the module and therefore the whole stream, which is a far worse outcome than
/// a row that says `unknown`.
// Part of this module's public surface is consumed by *downstream* modules
// (the enriching map, `db_out`) that are not wired yet, so rustc sees it as
// dead in a build that only runs `store_tokens`. Allowing it item-by-item
// rather than blanket-allowing the module keeps dead-code detection live for
// everything else here.
#[allow(dead_code)]
pub fn fetch_token_meta(addrs: &[Vec<u8>]) -> Vec<TokenMeta> {
    fetch_token_meta_detailed(addrs)
        .into_iter()
        .map(|f| f.meta)
        .collect()
}

/// [`fetch_token_meta`] with provenance retained. Same guarantees.
pub fn fetch_token_meta_detailed(addrs: &[Vec<u8>]) -> Vec<FetchedToken> {
    // First-seen dedup. This HashSet is legitimate — it dedups within one call,
    // it is NOT a cross-block cache. Cross-block memoisation must be a store:
    // a map/store handler is a pure function of its inputs and keeps nothing
    // between blocks, so a HashMap "cache" here would re-issue every call every
    // block while looking like an optimisation.
    let mut seen: HashSet<&[u8]> = HashSet::new();
    let mut unique: Vec<&[u8]> = Vec::with_capacity(addrs.len());
    for a in addrs {
        if seen.insert(a.as_slice()) {
            unique.push(a.as_slice());
        }
    }

    // Slots preserve first-seen order across the native/RPC split below.
    let mut slots: Vec<Option<FetchedToken>> = vec![None; unique.len()];
    let mut needs_rpc: Vec<(usize, &[u8])> = Vec::with_capacity(unique.len());

    for (i, addr) in unique.iter().enumerate() {
        if is_native(addr) {
            // The native currency MUST NOT be eth_call'd. `address(0)` holds no
            // code, so the call does not revert — it returns empty data, which
            // decodes to nothing and would brand real ETH legs `unknown`/18
            // (right decimals, by luck, wrong for any other chain) while
            // burning three RPC calls per native pool. Every V4 pool with an
            // ETH leg hits this path, so it is the common case, not an edge.
            slots[i] = Some(native_token());
        } else if addr.len() != 20 {
            // Not an address. Nothing to call; a malformed `to_addr` would just
            // waste a batch slot.
            slots[i] = Some(placeholder(addr));
        } else {
            needs_rpc.push((i, addr));
        }
    }

    for chunk in needs_rpc.chunks(MAX_TOKENS_PER_BATCH) {
        let chunk_addrs: Vec<&[u8]> = chunk.iter().map(|(_, a)| *a).collect();
        for (slot, fetched) in chunk.iter().zip(fetch_chunk(&chunk_addrs)) {
            slots[slot.0] = Some(fetched);
        }
    }

    // `filter_map` rather than `unwrap`: every slot is filled by construction,
    // but an unwrap here would be a panic path in wasm for no benefit.
    slots.into_iter().flatten().collect()
}

/// One batched `eth_call` for up to [`MAX_TOKENS_PER_BATCH`] tokens.
/// Returns one entry per input address, positionally, always.
fn fetch_chunk(addrs: &[&[u8]]) -> Vec<FetchedToken> {
    let mut calls = Vec::with_capacity(addrs.len() * CALLS_PER_TOKEN);
    for addr in addrs {
        // Push order defines response order — see CALLS_PER_TOKEN.
        calls.push(RpcCall {
            to_addr: addr.to_vec(),
            data: SELECTOR_SYMBOL.to_vec(),
        });
        calls.push(RpcCall {
            to_addr: addr.to_vec(),
            data: SELECTOR_NAME.to_vec(),
        });
        calls.push(RpcCall {
            to_addr: addr.to_vec(),
            data: SELECTOR_DECIMALS.to_vec(),
        });
    }

    let responses = rpc::eth_call(&RpcCalls { calls }).responses;

    addrs
        .iter()
        .enumerate()
        .map(|(i, addr)| {
            let base = i * CALLS_PER_TOKEN;

            // `.get()` everywhere: if the engine ever returns a short response
            // vector, that must degrade to placeholders, not index out of
            // bounds. `failed` is the per-call revert flag; a failed call's
            // `raw` is meaningless.
            let raw = |offset: usize| -> Option<&[u8]> {
                responses
                    .get(base + offset)
                    .filter(|r| !r.failed)
                    .map(|r| r.raw.as_slice())
            };

            let (symbol, symbol_src) = decode_text(raw(0));
            let (name, name_src) = decode_text(raw(1));
            let measured = raw(2).and_then(decode_decimals);

            FetchedToken {
                meta: TokenMeta {
                    address: address_hex(addr),
                    symbol,
                    name,
                    decimals: measured.unwrap_or(DEFAULT_DECIMALS),
                },
                resolution: Resolution {
                    symbol: symbol_src,
                    name: name_src,
                    decimals_measured: measured.is_some(),
                },
            }
        })
        .collect()
}

/// True for `address(0)` — V4's native-currency sentinel.
pub fn is_native(addr: &[u8]) -> bool {
    !addr.is_empty() && addr.iter().all(|b| *b == 0)
}

fn native_token() -> FetchedToken {
    FetchedToken {
        meta: TokenMeta {
            address: NATIVE_ADDRESS_HEX.to_string(),
            symbol: NATIVE_SYMBOL.to_string(),
            name: NATIVE_NAME.to_string(),
            decimals: NATIVE_DECIMALS,
        },
        resolution: Resolution {
            symbol: TextSource::Native,
            name: TextSource::Native,
            // Native decimals are a protocol constant, not a guess.
            decimals_measured: true,
        },
    }
}

fn placeholder(addr: &[u8]) -> FetchedToken {
    FetchedToken {
        meta: TokenMeta {
            address: address_hex(addr),
            symbol: UNKNOWN_TEXT.to_string(),
            name: UNKNOWN_TEXT.to_string(),
            decimals: DEFAULT_DECIMALS,
        },
        resolution: Resolution {
            symbol: TextSource::Fallback,
            name: TextSource::Fallback,
            decimals_measured: false,
        },
    }
}

// ---------------------------------------------------------------------------
// ABI return decoding
// ---------------------------------------------------------------------------

/// Decode a `symbol()`/`name()` return that may be either shape.
///
/// A dynamic `string` return is at minimum 64 bytes (offset word + length
/// word); a `bytes32` return is exactly 32. The lengths cannot collide, so
/// trying `string` first and falling back to `bytes32` is unambiguous — and it
/// costs no extra RPC, unlike the subgraph's second bound contract.
fn decode_text(raw: Option<&[u8]>) -> (String, TextSource) {
    let raw = match raw {
        Some(r) => r,
        None => return (UNKNOWN_TEXT.to_string(), TextSource::Fallback),
    };

    if let Some(s) = decode_abi_string(raw) {
        return (s, TextSource::AbiString);
    }
    if let Some(s) = decode_bytes32_string(raw) {
        return (s, TextSource::Bytes32);
    }
    (UNKNOWN_TEXT.to_string(), TextSource::Fallback)
}

/// Read a 32-byte word as a length/offset.
///
/// Rejects anything with a non-zero high 24 bytes rather than truncating: a
/// value that large is a corrupt return, and silently taking its low bits is
/// how you turn garbage into a plausible-looking slice index.
fn read_word_usize(raw: &[u8], at: usize) -> Option<usize> {
    let word = raw.get(at..at.checked_add(32)?)?;
    if word[..24].iter().any(|b| *b != 0) {
        return None;
    }
    let mut v: u64 = 0;
    for b in &word[24..32] {
        v = (v << 8) | *b as u64;
    }
    // usize is 32-bit on wasm32, so this conversion really can fail.
    usize::try_from(v).ok()
}

/// Standard ABI `string` return: `[offset][len][data…]`, all bounds checked.
fn decode_abi_string(raw: &[u8]) -> Option<String> {
    let offset = read_word_usize(raw, 0)?;
    let len = read_word_usize(raw, offset)?;
    let start = offset.checked_add(32)?;
    let bytes = raw.get(start..start.checked_add(len)?)?;
    // `from_utf8_lossy`, not `from_utf8().ok()?`: a token with one bad byte in
    // an otherwise readable name should keep the readable part.
    let s = sanitize(&String::from_utf8_lossy(bytes));
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// `bytes32` return — MKR and friends. Right-padded with zeros, so trim from
/// the tail and treat the remainder as UTF-8.
fn decode_bytes32_string(raw: &[u8]) -> Option<String> {
    if raw.len() != 32 || raw == NULL_ETH_VALUE {
        return None;
    }
    // `rposition` returns None when every byte is zero — an empty bytes32,
    // which is not a symbol.
    let end = raw.iter().rposition(|b| *b != 0)? + 1;
    let s = sanitize(&String::from_utf8_lossy(&raw[..end]));
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// `decimals()` returns `uint8` (some tokens declare `uint256`; both are one
/// right-aligned word). Anything wider than a byte, or `>= 255`, is rejected —
/// the same bound the subgraph applies.
fn decode_decimals(raw: &[u8]) -> Option<u64> {
    if raw.len() != 32 || raw[..31].iter().any(|b| *b != 0) {
        return None;
    }
    let d = raw[31] as u64;
    if d >= MAX_DECIMALS {
        None
    } else {
        Some(d)
    }
}

/// Make a token-supplied string safe to store, key on, and write to Postgres.
///
/// Strips **all** control characters. Three reasons, each real:
///   1. `bytes32` symbols are zero-padded and interior NUL bytes are routine —
///      Postgres `text` cannot hold `\0` and the sink INSERT would fail on it.
///   2. It guarantees [`FIELD_SEP`] cannot appear inside a field, which is what
///      makes the stored encoding unambiguous against a hostile token name.
///   3. Newlines in a symbol corrupt every line-oriented tool downstream.
///
/// Then trims and truncates to [`MAX_TEXT_LEN`] *characters* (`chars`, not
/// bytes, so a multi-byte symbol is never split mid-codepoint).
fn sanitize(raw: &str) -> String {
    raw.chars()
        .filter(|c| !c.is_control())
        .take(MAX_TEXT_LEN)
        .collect::<String>()
        .trim()
        .to_string()
}

/// `0x`-prefixed lowercase hex. Deliberately identical to
/// [`crate::hooks::addr_hex`] — duplicated rather than imported so this module
/// stays usable on its own, and asserted equal in the tests.
fn address_hex(b: &[u8]) -> String {
    format!("0x{}", Hex::encode(b))
}

// ---------------------------------------------------------------------------
// Store encoding
// ---------------------------------------------------------------------------

/// Store key for an address already in `0x…` form.
///
/// Lowercased defensively: a caller holding a checksummed address would
/// otherwise write a key that no `get_last` ever finds, and a silent store miss
/// is very hard to spot downstream.
pub fn token_key(address_hex: &str) -> String {
    format!("{}{}", TOKEN_KEY_PREFIX, address_hex.to_ascii_lowercase())
}

/// Compact stored value: `symbol US name US decimals US flags`.
///
/// `flags` is three characters — symbol source, name source, and `d` (measured)
/// or `?` (defaulted) for decimals. So USDC stores as
/// `USDC␟USD Coin␟6␟ssd`, and a token whose calls all failed stores as
/// `unknown␟unknown␟18␟???`, which a consumer can reject on sight.
///
/// A string store rather than `proto:` deliberately: this is four small scalars,
/// the value stays greppable in `substreams run` output, and it avoids adding a
/// message to a proto contract shared with other modules.
pub fn encode_token_value(meta: &TokenMeta, res: Resolution) -> String {
    format!(
        "{sym}{sep}{name}{sep}{dec}{sep}{f0}{f1}{f2}",
        sym = meta.symbol,
        name = meta.name,
        dec = meta.decimals,
        sep = FIELD_SEP,
        f0 = res.symbol.tag(),
        f1 = res.name.tag(),
        f2 = if res.decimals_measured { 'd' } else { '?' },
    )
}

/// Inverse of [`encode_token_value`]. `address` is the caller's — the key holds
/// it, so the value does not repeat it.
///
/// Returns `None` on a malformed value rather than guessing; a consumer that
/// gets `None` should treat the token as unresolved, not as decimals-18.
#[allow(dead_code)]
pub fn decode_token_value(address: &str, value: &str) -> Option<(TokenMeta, Resolution)> {
    let parts: Vec<&str> = value.split(FIELD_SEP).collect();
    if parts.len() != 4 {
        return None;
    }
    let decimals: u64 = parts[2].parse().ok()?;
    let flags: Vec<char> = parts[3].chars().collect();
    if flags.len() != 3 {
        return None;
    }

    Some((
        TokenMeta {
            address: address.to_ascii_lowercase(),
            symbol: parts[0].to_string(),
            name: parts[1].to_string(),
            decimals,
        },
        Resolution {
            symbol: TextSource::from_tag(flags[0])?,
            name: TextSource::from_tag(flags[1])?,
            decimals_measured: match flags[2] {
                'd' => true,
                '?' => false,
                _ => return None,
            },
        },
    ))
}

/// Parse a `0x…` address string back to bytes. The store handler's input is
/// `pb::Events`, which carries addresses as hex strings, but `eth_call` needs
/// the 20 raw bytes.
fn address_bytes(hex_str: &str) -> Option<Vec<u8>> {
    let stripped = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    if stripped.len() != 40 {
        return None;
    }
    Hex::decode(stripped).ok()
}

// ---------------------------------------------------------------------------
// Store module
// ---------------------------------------------------------------------------

/// Cache token metadata for every currency first seen at a pool `Initialize`.
///
/// Only `Events.pools` is read — swaps and liquidity events are deliberately
/// ignored, because their tokens are reachable through their pool and paying
/// RPC per swap would make the package unusable. A block with no pool
/// initialisation (the overwhelming majority) issues **zero** RPC calls and
/// writes nothing.
///
/// The ordinal is the pool's block-scoped log index, so two initialisations in
/// one block that mention the same token resolve last-write-wins in true chain
/// order rather than in whatever order the map emitted them.
///
/// Wiring (`substreams.yaml`), for whoever owns the manifest:
///
/// ```yaml
/// - name: store_tokens
///   kind: store
///   initialBlock: 25350988
///   updatePolicy: set
///   valueType: string
///   inputs:
///     - map: map_events
/// ```
#[substreams::handlers::store]
pub fn store_tokens(events: pb::Events, store: StoreSetString) {
    let mut addrs: Vec<Vec<u8>> = Vec::new();
    // Lowest ordinal wins per address: the first initialisation in the block
    // that mentions a token is the one that "discovered" it.
    let mut ords: HashMap<String, u64> = HashMap::new();

    for pool in &events.pools {
        let ord = pool.meta.as_ref().map(|m| m.log_index as u64).unwrap_or(0);
        for hex_str in [&pool.token0, &pool.token1] {
            let key = hex_str.to_ascii_lowercase();
            ords.entry(key).and_modify(|o| *o = (*o).min(ord)).or_insert(ord);
            if let Some(bytes) = address_bytes(hex_str) {
                addrs.push(bytes);
            }
        }
    }

    // The cost gate. Without it every block on Base would issue an empty batch.
    if addrs.is_empty() {
        return;
    }

    for fetched in fetch_token_meta_detailed(&addrs) {
        let ord = ords.get(&fetched.meta.address).copied().unwrap_or(0);
        store.set(
            ord,
            token_key(&fetched.meta.address),
            &encode_token_value(&fetched.meta, fetched.resolution),
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// Host-target tests only touch the pure decoders and the codec. They must never
// reach `rpc::eth_call`, whose non-wasm implementation is `unimplemented!()` —
// so nothing here calls `fetch_chunk`, and the native/malformed paths are
// exercised through helpers that short-circuit before any call.

#[cfg(test)]
mod tests {
    use super::*;

    fn word(bytes: &[u8]) -> Vec<u8> {
        let mut w = vec![0u8; 32];
        w[32 - bytes.len()..].copy_from_slice(bytes);
        w
    }

    /// Standard `string` return for `s`.
    fn abi_string(s: &str) -> Vec<u8> {
        let mut out = word(&[32]); // offset
        out.extend_from_slice(&word(&[s.len() as u8])); // length
        let mut data = s.as_bytes().to_vec();
        data.resize(((s.len() + 31) / 32).max(1) * 32, 0); // right pad
        out.extend_from_slice(&data);
        out
    }

    /// `bytes32` return for `s` — the MKR shape.
    fn bytes32(s: &str) -> Vec<u8> {
        let mut w = s.as_bytes().to_vec();
        w.resize(32, 0);
        w
    }

    #[test]
    fn decodes_standard_string_symbol() {
        let (s, src) = decode_text(Some(&abi_string("USDC")));
        assert_eq!(s, "USDC");
        assert_eq!(src, TextSource::AbiString);
    }

    #[test]
    fn decodes_bytes32_symbol() {
        // The MKR case the subgraph needs a whole second ABI binding for.
        let (s, src) = decode_text(Some(&bytes32("MKR")));
        assert_eq!(s, "MKR");
        assert_eq!(src, TextSource::Bytes32);
    }

    #[test]
    fn decodes_real_base_returndata() {
        // Captured live off Base mainnet, not hand-written:
        //   cast call --rpc-url https://mainnet.base.org <token> "symbol()"
        // USDC 0x833589fc… symbol() → offset 0x20, len 4, "USDC".
        let usdc_symbol = hex_literal::hex!(
            "0000000000000000000000000000000000000000000000000000000000000020"
            "0000000000000000000000000000000000000000000000000000000000000004"
            "5553444300000000000000000000000000000000000000000000000000000000"
        );
        assert_eq!(decode_text(Some(&usdc_symbol)), ("USDC".to_string(), TextSource::AbiString));

        // USDC decimals() → 6. The number that makes or breaks every amount in
        // this package: defaulting it to 18 misprices USDC by 10^12.
        let usdc_decimals = hex_literal::hex!(
            "0000000000000000000000000000000000000000000000000000000000000006"
        );
        assert_eq!(decode_decimals(&usdc_decimals), Some(6));

        // WETH 0x42000000…0006 name() → "Wrapped Ether" (len 13, not word-aligned).
        let weth_name = hex_literal::hex!(
            "0000000000000000000000000000000000000000000000000000000000000020"
            "000000000000000000000000000000000000000000000000000000000000000d"
            "5772617070656420457468657200000000000000000000000000000000000000"
        );
        assert_eq!(
            decode_text(Some(&weth_name)),
            ("Wrapped Ether".to_string(), TextSource::AbiString)
        );

        // And the reason address(0) is short-circuited: calling symbol() on it
        // does NOT revert (`failed` stays false) — it returns EMPTY data, which
        // would quietly become "unknown"/18 for every native-ETH pool on Base.
        // Verified with the same cast call: "Warning: Contract code is empty" → 0x.
        assert_eq!(decode_text(Some(&[][..])), (UNKNOWN_TEXT.to_string(), TextSource::Fallback));
    }

    #[test]
    fn undecodable_text_falls_back_without_panicking() {
        // Reverted call, empty return, truncated word, all-zero bytes32 and the
        // 0x…01 "null eth value" must all degrade, never panic.
        for raw in [
            None,
            Some(&[][..]),
            Some(&[0u8; 8][..]),
            Some(&[0u8; 32][..]),
            Some(&NULL_ETH_VALUE[..]),
        ] {
            let (s, src) = decode_text(raw);
            assert_eq!(s, UNKNOWN_TEXT);
            assert_eq!(src, TextSource::Fallback);
        }
    }

    #[test]
    fn rejects_absurd_offset_instead_of_indexing_out_of_bounds() {
        // offset = 2^40: read_word_usize must reject it, not truncate to a
        // valid-looking slice index.
        let mut raw = word(&[0x01, 0, 0, 0, 0, 0]);
        raw.extend_from_slice(&word(&[4]));
        assert_eq!(decode_abi_string(&raw), None);
    }

    #[test]
    fn decimals_bounds_match_the_subgraph() {
        assert_eq!(decode_decimals(&word(&[6])), Some(6));
        assert_eq!(decode_decimals(&word(&[18])), Some(18));
        // `< 255` is the subgraph's own bound.
        assert_eq!(decode_decimals(&word(&[254])), Some(254));
        assert_eq!(decode_decimals(&word(&[255])), None);
        // Wider than a byte → not a uint8 decimals return.
        assert_eq!(decode_decimals(&word(&[1, 0])), None);
        assert_eq!(decode_decimals(&[0u8; 16]), None);
    }

    #[test]
    fn native_currency_is_hardcoded_not_called() {
        assert!(is_native(&[0u8; 20]));
        assert!(!is_native(&[]));
        assert!(!is_native(&hex_literal::hex!(
            "4200000000000000000000000000000000000006"
        )));

        // Goes through the public entry point: proof that address(0) resolves
        // with no RPC, since eth_call is `unimplemented!()` off wasm32 and this
        // test would abort if the native branch ever stopped short-circuiting.
        let out = fetch_token_meta(&[vec![0u8; 20]]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].address, NATIVE_ADDRESS_HEX);
        assert_eq!(out[0].symbol, "ETH");
        assert_eq!(out[0].name, "Ethereum");
        assert_eq!(out[0].decimals, 18);
    }

    #[test]
    fn dedups_repeated_addresses() {
        // Two "pools" sharing a native leg produce one entry, not two.
        let out = fetch_token_meta(&[vec![0u8; 20], vec![0u8; 20], vec![0u8; 20]]);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn malformed_address_gets_a_placeholder_not_a_call() {
        let out = fetch_token_meta_detailed(&[vec![0xde, 0xad]]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].meta.symbol, UNKNOWN_TEXT);
        assert_eq!(out[0].meta.decimals, DEFAULT_DECIMALS);
        assert!(!out[0].resolution.decimals_measured);
    }

    #[test]
    fn sanitize_strips_the_field_separator_and_nuls() {
        // A token that tries to forge extra fields, plus the zero padding a
        // bytes32 symbol always carries.
        let hostile = format!("EVIL{}999{}sss", FIELD_SEP, FIELD_SEP);
        let clean = sanitize(&hostile);
        assert!(!clean.contains(FIELD_SEP));
        assert_eq!(clean, "EVIL999sss");
        assert_eq!(sanitize("AB\0\0\0"), "AB");
        assert_eq!(sanitize("  spaced \n"), "spaced");
        assert_eq!(sanitize(&"x".repeat(500)).chars().count(), MAX_TEXT_LEN);
    }

    #[test]
    fn store_value_round_trips() {
        let meta = TokenMeta {
            address: "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_string(),
            symbol: "USDC".to_string(),
            name: "USD Coin".to_string(),
            decimals: 6,
        };
        let res = Resolution {
            symbol: TextSource::AbiString,
            name: TextSource::AbiString,
            decimals_measured: true,
        };
        let encoded = encode_token_value(&meta, res);
        assert_eq!(encoded, "USDC\u{1f}USD Coin\u{1f}6\u{1f}ssd");

        let (back, back_res) = decode_token_value(&meta.address, &encoded).unwrap();
        assert_eq!(back, meta);
        assert_eq!(back_res, res);

        // A defaulted-decimals token is visibly flagged, so a consumer can
        // refuse to scale by it.
        let ph = placeholder(&[0xde, 0xad]);
        let enc = encode_token_value(&ph.meta, ph.resolution);
        assert!(enc.ends_with("???"));

        // Malformed values are rejected, not guessed at.
        assert_eq!(decode_token_value("0x00", "USDC"), None);
        assert_eq!(decode_token_value("0x00", "a\u{1f}b\u{1f}notanumber\u{1f}ssd"), None);
        assert_eq!(decode_token_value("0x00", "a\u{1f}b\u{1f}6\u{1f}zz"), None);
    }

    #[test]
    fn store_key_matches_pool_token_formatting() {
        let raw = hex_literal::hex!("4200000000000000000000000000000000000006");
        // Same formatting hooks::addr_hex writes into Pool.token0 — the join
        // this whole module depends on.
        assert_eq!(address_hex(&raw), crate::hooks::addr_hex(&raw));
        assert_eq!(
            token_key(&address_hex(&raw)),
            "token:0x4200000000000000000000000000000000000006"
        );
        // A checksummed / uppercase address must not produce an unfindable key.
        assert_eq!(
            token_key("0x833589FCD6EDB6E08F4C7C32D4F71B54BDA02913"),
            "token:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        );
    }

    #[test]
    fn address_bytes_parses_pool_token_strings() {
        assert_eq!(
            address_bytes("0x4200000000000000000000000000000000000006"),
            Some(hex_literal::hex!("4200000000000000000000000000000000000006").to_vec())
        );
        assert_eq!(address_bytes("0xnothex"), None);
        assert_eq!(address_bytes("0x42"), None);
    }
}
