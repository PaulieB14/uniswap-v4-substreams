//! Hook-address decoding and shared row provenance.
//!
//! Uniswap V4 has no per-pool hook registry: a pool's `hooks` field is just an
//! address, and the protocol enforces that the address itself *is* the
//! permission set. Deployers mine (CREATE2-salt-grind) an address whose low 14
//! bits equal the set of callbacks the hook implements, and `Hooks.validateHookPermissions`
//! reverts at initialize time if they disagree. That makes the permission set
//! derivable offline, with no RPC call and no ABI, for a hook nobody has ever
//! indexed before.
//!
//! The source subgraph stores `hooks` as an opaque address string, so it cannot
//! answer questions like "which pools can override the swap fee". Decoding here
//! is the headline divergence of this package.

use substreams::Hex;
use substreams_ethereum::pb::eth::v2::{Block, Log, TransactionTrace};

use crate::pb::uniswap::v4::v1 as pb;

/// Only the low 14 bits of a hook address are permission flags; everything
/// above is address entropy.
pub const HOOK_FLAG_MASK: u32 = 0x3fff;

pub const BEFORE_INITIALIZE: u32 = 1 << 13;
pub const AFTER_INITIALIZE: u32 = 1 << 12;
pub const BEFORE_ADD_LIQUIDITY: u32 = 1 << 11;
pub const AFTER_ADD_LIQUIDITY: u32 = 1 << 10;
pub const BEFORE_REMOVE_LIQUIDITY: u32 = 1 << 9;
pub const AFTER_REMOVE_LIQUIDITY: u32 = 1 << 8;
pub const BEFORE_SWAP: u32 = 1 << 7;
pub const AFTER_SWAP: u32 = 1 << 6;
pub const BEFORE_DONATE: u32 = 1 << 5;
pub const AFTER_DONATE: u32 = 1 << 4;
pub const BEFORE_SWAP_RETURNS_DELTA: u32 = 1 << 3;
pub const AFTER_SWAP_RETURNS_DELTA: u32 = 1 << 2;
pub const AFTER_ADD_LIQUIDITY_RETURNS_DELTA: u32 = 1 << 1;
pub const AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA: u32 = 1;

/// v4-core `LPFeeLibrary.DYNAMIC_FEE_FLAG`. A pool key whose fee is exactly this
/// sentinel delegates fee setting to its hook.
pub const DYNAMIC_FEE_FLAG: u64 = 0x80_0000;

/// uint24 — the solidity width of both the pool key fee and the swap fee.
const UINT24_MASK: u64 = 0xff_ffff;

/// Extract the raw 14-bit permission mask from a hook address.
///
/// Read from the tail of the slice rather than a fixed offset: callers hand us
/// whatever the ABI decoder produced, and a left-padded or short slice must
/// still yield the *low* bits rather than garbage.
pub fn hook_flags(addr: &[u8]) -> u32 {
    let n = addr.len();
    let lo = if n >= 1 { addr[n - 1] as u32 } else { 0 };
    let hi = if n >= 2 { addr[n - 2] as u32 } else { 0 };
    ((hi << 8) | lo) & HOOK_FLAG_MASK
}

/// Decode a hook address into its declared capabilities.
///
/// The zero address (a hookless pool) decodes to all-false with `has_hook =
/// false`, which is exactly right: no hook, no callbacks. Note that flags alone
/// do not prove the hook is *benign* — they say what the PoolManager is
/// permitted to call, not what the hook does when called.
pub fn decode_hook(addr: &[u8]) -> pb::HookPermissions {
    let flags = hook_flags(addr);
    let has = |bit: u32| flags & bit != 0;

    pb::HookPermissions {
        address: addr_hex(addr),
        before_initialize: has(BEFORE_INITIALIZE),
        after_initialize: has(AFTER_INITIALIZE),
        before_add_liquidity: has(BEFORE_ADD_LIQUIDITY),
        after_add_liquidity: has(AFTER_ADD_LIQUIDITY),
        before_remove_liquidity: has(BEFORE_REMOVE_LIQUIDITY),
        after_remove_liquidity: has(AFTER_REMOVE_LIQUIDITY),
        before_swap: has(BEFORE_SWAP),
        after_swap: has(AFTER_SWAP),
        before_donate: has(BEFORE_DONATE),
        after_donate: has(AFTER_DONATE),
        before_swap_returns_delta: has(BEFORE_SWAP_RETURNS_DELTA),
        after_swap_returns_delta: has(AFTER_SWAP_RETURNS_DELTA),
        after_add_liquidity_returns_delta: has(AFTER_ADD_LIQUIDITY_RETURNS_DELTA),
        after_remove_liquidity_returns_delta: has(AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA),
        flags,
        // A hook is present iff the address is non-zero. Deliberately NOT
        // `flags != 0`: an address can be non-zero with no permission bits (it
        // simply never gets called back), and treating that as "no hook" would
        // drop the pool's association with a real contract.
        has_hook: addr.iter().any(|b| *b != 0),
    }
}

/// Dynamic-fee detection.
///
/// v4-core's `isDynamicFee` is an equality test against the sentinel, not a bit
/// test: `0x800001` is not "dynamic with extra bits", it is an invalid fee that
/// `initialize` rejects. We mask to uint24 first because the ABI decoder widens
/// the on-wire uint24 into a larger integer.
///
/// Only meaningful for the pool key fee from `Initialize`. The `fee` on a `Swap`
/// event is the fee ACTUALLY charged (a dynamic-fee hook has already resolved it
/// to a concrete value), so it will practically never equal the sentinel.
pub fn is_dynamic_fee(fee: u64) -> bool {
    (fee & UINT24_MASK) == DYNAMIC_FEE_FLAG
}

/// 0x-prefixed lowercase hex, matching graph-node's `Bytes.toHexString()` so
/// ids and addresses join against the subgraph's output byte-for-byte.
pub fn addr_hex(b: &[u8]) -> String {
    format!("0x{}", Hex::encode(b))
}

/// Pool ids are bytes32 keccak hashes of the pool key, not addresses — V4 pools
/// have no contract of their own. Separate fn so the [u8; 32] arity from the ABI
/// bindings is enforced at the type level.
pub fn pool_id_hex(b: &[u8; 32]) -> String {
    format!("0x{}", Hex::encode(b))
}

/// Flattened block/tx/log provenance for a single event row.
pub fn meta(blk: &Block, trx: &TransactionTrace, log: &Log) -> pb::Meta {
    pb::Meta {
        block_number: blk.number,
        // `Block::timestamp_seconds()` unwraps the header; a panic here would
        // abort the whole stream, so degrade to 0 instead.
        block_timestamp: blk
            .header
            .as_ref()
            .and_then(|h| h.timestamp.as_ref())
            .map(|t| t.seconds as u64)
            .unwrap_or(0),
        tx_hash: addr_hex(&trx.hash),
        // graph-node's `event.logIndex` is BLOCK-scoped, while the firehose
        // `Log.index` is TRANSACTION-scoped. Use `block_index` so event ids of
        // the form `<txHash>-<logIndex>` match the subgraph's exactly.
        log_index: log.block_index,
        origin: addr_hex(&trx.from),
        // The subgraph hardcodes Transaction.gasUsed = 0 ("needs to be moved to
        // transaction receipt"). The firehose trace carries the real value, so
        // we emit it — a deliberate improvement over subgraph parity.
        gas_used: trx.gas_used,
        gas_price: trx
            .gas_price
            .as_ref()
            .map(|v| substreams::scalar::BigInt::from_unsigned_bytes_be(&v.bytes).to_string())
            .unwrap_or_else(|| "0".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verified live on Base: 0x0000fe59823933ac763611a69c88F91d45F81888.
    const LIVE_HOOK: [u8; 20] = [
        0x00, 0x00, 0xfe, 0x59, 0x82, 0x39, 0x33, 0xac, 0x76, 0x36, 0x11, 0xa6, 0x9c, 0x88, 0xf9,
        0x1d, 0x45, 0xf8, 0x18, 0x88,
    ];

    #[test]
    fn decodes_verified_base_hook() {
        let h = decode_hook(&LIVE_HOOK);
        assert_eq!(h.flags, 0x1888);
        assert!(h.has_hook);
        assert_eq!(h.address, "0x0000fe59823933ac763611a69c88f91d45f81888");

        // 0x1888 = AFTER_INITIALIZE | BEFORE_ADD_LIQUIDITY | BEFORE_SWAP
        //          | BEFORE_SWAP_RETURNS_DELTA
        assert!(h.after_initialize);
        assert!(h.before_add_liquidity);
        assert!(h.before_swap);
        assert!(h.before_swap_returns_delta);

        // and nothing else
        assert!(!h.before_initialize);
        assert!(!h.after_add_liquidity);
        assert!(!h.before_remove_liquidity);
        assert!(!h.after_remove_liquidity);
        assert!(!h.after_swap);
        assert!(!h.before_donate);
        assert!(!h.after_donate);
        assert!(!h.after_swap_returns_delta);
        assert!(!h.after_add_liquidity_returns_delta);
        assert!(!h.after_remove_liquidity_returns_delta);
    }

    #[test]
    fn zero_address_is_hookless() {
        let h = decode_hook(&[0u8; 20]);
        assert_eq!(h.flags, 0);
        assert!(!h.has_hook);
        assert_eq!(h.address, "0x0000000000000000000000000000000000000000");
        assert!(!h.before_initialize);
        assert!(!h.after_initialize);
        assert!(!h.before_add_liquidity);
        assert!(!h.after_add_liquidity);
        assert!(!h.before_remove_liquidity);
        assert!(!h.after_remove_liquidity);
        assert!(!h.before_swap);
        assert!(!h.after_swap);
        assert!(!h.before_donate);
        assert!(!h.after_donate);
        assert!(!h.before_swap_returns_delta);
        assert!(!h.after_swap_returns_delta);
        assert!(!h.after_add_liquidity_returns_delta);
        assert!(!h.after_remove_liquidity_returns_delta);
    }

    #[test]
    fn all_bits_set() {
        let mut addr = [0u8; 20];
        // Set every bit of the trailing two bytes; the mask must clip bits 14/15
        // (they are address entropy, not permissions).
        addr[18] = 0xff;
        addr[19] = 0xff;

        let h = decode_hook(&addr);
        assert_eq!(h.flags, HOOK_FLAG_MASK);
        assert!(h.has_hook);
        assert!(h.before_initialize);
        assert!(h.after_initialize);
        assert!(h.before_add_liquidity);
        assert!(h.after_add_liquidity);
        assert!(h.before_remove_liquidity);
        assert!(h.after_remove_liquidity);
        assert!(h.before_swap);
        assert!(h.after_swap);
        assert!(h.before_donate);
        assert!(h.after_donate);
        assert!(h.before_swap_returns_delta);
        assert!(h.after_swap_returns_delta);
        assert!(h.after_add_liquidity_returns_delta);
        assert!(h.after_remove_liquidity_returns_delta);
    }

    #[test]
    fn non_zero_address_without_flags_still_counts_as_a_hook() {
        let mut addr = [0u8; 20];
        addr[0] = 0xab;
        let h = decode_hook(&addr);
        assert_eq!(h.flags, 0);
        assert!(h.has_hook);
    }

    #[test]
    fn short_slices_do_not_panic() {
        assert_eq!(hook_flags(&[]), 0);
        assert_eq!(hook_flags(&[0x88]), 0x88);
        assert_eq!(hook_flags(&[0x18, 0x88]), 0x1888);
        // bits 14+ are clipped
        assert_eq!(hook_flags(&[0xff, 0xff]), 0x3fff);
    }

    #[test]
    fn dynamic_fee_sentinel() {
        assert!(is_dynamic_fee(0x80_0000));
        assert!(!is_dynamic_fee(0));
        assert!(!is_dynamic_fee(3000));
        assert!(!is_dynamic_fee(1_000_000));
        // Not a bit test: the sentinel plus anything else is an invalid fee,
        // never a dynamic-fee pool.
        assert!(!is_dynamic_fee(0x80_0001));
    }

    #[test]
    fn hex_helpers_are_lowercase_and_prefixed() {
        assert_eq!(addr_hex(&[0xde, 0xad, 0xBE, 0xEF]), "0xdeadbeef");
        let id = [0x0au8; 32];
        let s = pool_id_hex(&id);
        assert_eq!(s.len(), 66);
        assert!(s.starts_with("0x0a0a"));
    }
}
