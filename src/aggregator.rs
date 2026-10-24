//! Uniswap Aggregator Hooks (live Oct 22, 2026).
//!
//! Source of truth: github.com/Uniswap/v4-hooks-public, `src/aggregator-hooks`.
//! Each hook wraps an external venue as a v4 pool. It claims the whole swap in
//! `beforeSwap` through a BeforeSwapDelta, so the PoolManager `Swap` log for
//! such a pool reports amount0 = amount1 = 0 (verified on Base receipts). The
//! real amounts are in the hook's own `HookSwap` event.
//!
//! No hook address is hard-coded. Logs are matched by topic0 from ANY emitter
//! and `map_enriched` marks a row `verified` only when the pool's PoolKey.hooks
//! (from store_pools) equals the emitter. New hooks are therefore picked up the
//! block they register a pool, and an unrelated contract that happens to emit
//! the same signature is kept but flagged unverified.
//!
//! Which venue a hook wraps comes from the README's ID system: the first byte
//! of the mined hook address. Uniswap notes a 1/256 false-positive rate for
//! arbitrary addresses, which is why it is only applied to emitters of the
//! aggregator events, never to arbitrary hooks.

use hex_literal::hex;
use substreams_ethereum::pb::eth::v2::Block;

use crate::hooks;
use crate::pb::uniswap::v4::v1 as pb;

/// keccak256("HookSwap(bytes32,address,int256,int256,uint24)")
pub const HOOK_SWAP: [u8; 32] =
    hex!("6150bb53cd76c5702d6074239fc7b6c2a5495d339eb122fdc06252f3efdbbc1b");
/// keccak256("AggregatorPoolRegistered(bytes32)")
pub const POOL_REGISTERED: [u8; 32] =
    hex!("e4e250d624b5ed2f65699d87988884c47eac735ffb1dcccead084c5254b14373");
/// keccak256("ProtocolFeesCollected(address,address,uint256)")
pub const FEES_COLLECTED: [u8; 32] =
    hex!("db249cf7a102711dda1beaba415bb011ee0e1402c7f67617236fed5739235d6c");

/// First-byte ID table from src/aggregator-hooks/README.md.
pub fn source_of(hook: &[u8]) -> &'static str {
    match hook.first() {
        Some(0xC1) => "curve_stableswap",
        Some(0xC2) => "curve_stableswap_ng",
        Some(0xF1) => "fluid_dex_t1",
        Some(0xF2) => "fluid_dex_v2",
        Some(0xF3) => "fluid_dex_lite",
        Some(0x71) => "tempo_exchange",
        Some(0x03) => "uniswap_v3",
        Some(0xA1) => "slipstream",
        Some(0x93) => "pancakeswap_v3",
        Some(0x95) => "maker_lite_psm",
        Some(0x02) => "uniswap_v2",
        Some(0x58) => "uniswapx_dutch",
        _ => "unknown",
    }
}

fn word(data: &[u8], i: usize) -> &[u8] {
    data.get(i * 32..i * 32 + 32).unwrap_or(&[])
}

fn int256(w: &[u8]) -> String {
    if w.len() != 32 {
        return "0".into();
    }
    substreams::scalar::BigInt::from_signed_bytes_be(w).to_string()
}

fn uint256(w: &[u8]) -> String {
    if w.len() != 32 {
        return "0".into();
    }
    substreams::scalar::BigInt::from_unsigned_bytes_be(w).to_string()
}

fn topic_addr(t: &[u8]) -> String {
    hooks::addr_hex(t.get(12..32).unwrap_or(&[]))
}

pub fn extract(blk: &Block, events: &mut pb::Events) {
    // Fee logs precede their HookSwap inside the same beforeSwap call, so the
    // latest unconsumed fee from the same hook in the same tx is the match.
    let mut pending: Option<(String, String, String, String)> = None; // (tx, hook, currency, amount)
    for log in blk.logs() {
        let topics = &log.log.topics;
        let Some(t0) = topics.first() else { continue };
        let t0 = t0.as_slice();
        if t0 != HOOK_SWAP && t0 != POOL_REGISTERED && t0 != FEES_COLLECTED {
            continue;
        }
        let emitter = log.address();
        let hook = hooks::addr_hex(emitter);
        let meta = hooks::meta(blk, log.receipt.transaction, log.log);
        let data = log.log.data.as_slice();
        if t0 == HOOK_SWAP && topics.len() == 3 && data.len() >= 96 {
            let (fee_ccy, fee_amt) = match pending.take() {
                Some((tx, h, c, a)) if tx == meta.tx_hash && h == hook => (c, a),
                _ => (String::new(), "0".to_string()),
            };
            let pips = substreams::scalar::BigInt::from_unsigned_bytes_be(word(data, 2))
                .to_u64() as u32;
            events.aggregator_swaps.push(pb::AggregatorSwap {
                id: format!("{}-{}", meta.tx_hash, meta.log_index),
                pool_id: hooks::addr_hex(&topics[1]),
                source: source_of(emitter).to_string(),
                sender: topic_addr(&topics[2]),
                amount0: int256(word(data, 0)),
                amount1: int256(word(data, 1)),
                protocol_fee_pips: pips,
                protocol_fee_amount: fee_amt,
                protocol_fee_currency: fee_ccy,
                hook,
                meta: Some(meta).into(),
                ..Default::default()
            });
        } else if t0 == POOL_REGISTERED && topics.len() == 2 {
            events.aggregator_pools.push(pb::AggregatorPool {
                pool_id: hooks::addr_hex(&topics[1]),
                source: source_of(emitter).to_string(),
                hook,
                meta: Some(meta).into(),
                ..Default::default()
            });
        } else if t0 == FEES_COLLECTED && topics.len() == 3 && data.len() >= 32 {
            let currency = topic_addr(&topics[2]);
            let amount = uint256(word(data, 0));
            pending = Some((meta.tx_hash.clone(), hook.clone(), currency.clone(), amount.clone()));
            events.aggregator_fees.push(pb::AggregatorFee {
                id: format!("{}-{}", meta.tx_hash, meta.log_index),
                recipient: topic_addr(&topics[1]),
                currency,
                amount,
                hook,
                meta: Some(meta).into(),
            });
        }
    }
}

/// Join PoolKey identity onto aggregator rows. `lookup` is store_pools.get_last.
pub fn enrich(events: &mut pb::Events, lookup: impl Fn(&str) -> Option<pb::Pool>) {
    for s in events.aggregator_swaps.iter_mut() {
        if let Some(p) = lookup(&s.pool_id) {
            s.verified = p.hook.as_option().map(|h| h.address == s.hook).unwrap_or(false);
            s.token0 = p.token0;
            s.token1 = p.token1;
        }
    }
    for a in events.aggregator_pools.iter_mut() {
        if let Some(p) = lookup(&a.pool_id) {
            a.verified = p.hook.as_option().map(|h| h.address == a.hook).unwrap_or(false);
            a.token0 = p.token0;
            a.token1 = p.token1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiny_keccak_check::k;

    mod tiny_keccak_check {
        pub fn k(s: &str) -> [u8; 32] {
            use tiny_keccak::{Hasher, Keccak};
            let mut h = Keccak::v256();
            let mut o = [0u8; 32];
            h.update(s.as_bytes());
            h.finalize(&mut o);
            o
        }
    }

    #[test]
    fn topics_match_signatures() {
        assert_eq!(HOOK_SWAP, k("HookSwap(bytes32,address,int256,int256,uint24)"));
        assert_eq!(POOL_REGISTERED, k("AggregatorPoolRegistered(bytes32)"));
        assert_eq!(FEES_COLLECTED, k("ProtocolFeesCollected(address,address,uint256)"));
    }

    #[test]
    fn id_byte_table() {
        assert_eq!(source_of(&hex!("a1d866042c0989570cc35ce102c03d14184ee888")), "slipstream");
        assert_eq!(source_of(&hex!("9388fccc70a90e268dbe8a4fbc5e7024ebf86888")), "pancakeswap_v3");
        assert_eq!(source_of(&hex!("0300000000000000000000000000000000000000")), "uniswap_v3");
        assert_eq!(source_of(&hex!("ff00000000000000000000000000000000000000")), "unknown");
    }

    #[test]
    fn signed_amounts() {
        let mut w = [0xffu8; 32];
        assert_eq!(int256(&w), "-1");
        w = [0u8; 32];
        w[31] = 5;
        assert_eq!(int256(&w), "5");
        assert_eq!(int256(&[]), "0");
    }
}
