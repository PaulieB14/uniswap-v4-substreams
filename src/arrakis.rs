//! ArrakisHookFactory → `pb::HookDeployment`.
//!
//! Ports `/tmp/v4sg/src/mappings/arrakis.ts` (`handleArrakisHookDeployed`).
//!
//! DIVERGENCE FROM THE SUBGRAPH — the subgraph stores an `ArrakisHook` row that
//! is nothing but `{hook, module, salt, createdAt*}`: an opaque address you
//! cannot ask questions of. We resolve the hook's permission set at deploy time
//! via `crate::hooks::decode_hook`, which reads the low 14 bits of the mined
//! hook address. That is pure arithmetic — no RPC, no state — so every hook
//! this factory has ever minted arrives already labelled with whether it can
//! override swap fees, return deltas, gate liquidity, etc. This is the whole
//! point of carrying HookDeployment separately from Pool: a hook is discovered
//! here the moment it is created, typically before any pool has installed it.

use substreams::Hex;
use substreams_ethereum::pb::eth::v2::Block;
use substreams_ethereum::Event;

use crate::abi;
use crate::pb::uniswap::v4::v1 as pb;

/// ArrakisHookFactory on Base (startBlock 28450225).
const ARRAKIS_HOOK_FACTORY: [u8; 20] =
    hex_literal::hex!("eF129a430032C8183abA158C1a70799e3b840dF9");

/// Which factory minted the hook. The proto keeps this a string rather than an
/// enum so a second factory can be added without a breaking proto change.
const FACTORY: &str = "arrakis";

pub fn extract(blk: &Block, events: &mut pb::Events) {
    for log in blk.logs() {
        if log.address() != ARRAKIS_HOOK_FACTORY {
            continue;
        }

        let ev = match abi::arrakis::events::LogCreatePrivateHook::match_and_decode(log) {
            Some(ev) => ev,
            None => continue,
        };

        let tx = log.receipt.transaction;
        // Block-scoped log index, to match graph-node's `event.logIndex`.
        let log_index = log.log.block_index;

        events.hook_deployments.push(pb::HookDeployment {
            // The subgraph keys ArrakisHook by the hook address. We key by
            // txHash-logIndex like every other event row so the table has a
            // real primary key even if a factory ever re-emits for an address
            // (CREATE2 + selfdestruct-redeploy); the hook address is still
            // right there in `hook` for joins against Pool.hook.address.
            id: format!("0x{}-{}", Hex::encode(&tx.hash), log_index),
            hook: addr(&ev.hook),
            module: addr(&ev.module),
            // salt is bytes32, not an address — full 32-byte hex.
            salt: format!("0x{}", Hex::encode(ev.salt)),
            factory: FACTORY.to_string(),
            // The reason this module exists as more than a copy of arrakis.ts.
            permissions: Some(crate::hooks::decode_hook(&ev.hook)),
            meta: Some(meta(blk, tx, log_index)),
        });
    }
}

/// `0x`-prefixed lowercase hex — matches graph-ts `Bytes.toHexString()`.
fn addr(bytes: &[u8]) -> String {
    format!("0x{}", Hex::encode(bytes))
}

fn meta(
    blk: &Block,
    tx: &substreams_ethereum::pb::eth::v2::TransactionTrace,
    log_index: u32,
) -> pb::Meta {
    pb::Meta {
        block_number: blk.number,
        block_timestamp: blk.timestamp_seconds(),
        tx_hash: format!("0x{}", Hex::encode(&tx.hash)),
        log_index,
        origin: addr(&tx.from),
        gas_used: tx.gas_used,
        gas_price: tx
            .gas_price
            .as_ref()
            .map(|v| Into::<substreams::scalar::BigInt>::into(v).to_string())
            .unwrap_or_else(|| "0".to_string()),
    }
}
