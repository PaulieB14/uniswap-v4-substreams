//! PositionManager (ERC-721 position NFTs) → `pb::PositionEvent`.
//!
//! Ports `/tmp/v4sg/src/mappings/{subscribe,unsubscribe,transfer}.ts`.
//!
//! DIVERGENCE FROM THE SUBGRAPH — the `Position` entity is deliberately not
//! reconstructed here. In the subgraph `Position` is *mutable current state*:
//! `handleTransfer` upserts it, overwriting `owner` on every hop and pinning
//! `origin`/`createdAtTimestamp` to the first transfer it ever saw (the mint,
//! since ERC-721 mints are `Transfer(0x0 -> owner)`). A Substreams map module
//! is stateless and parallel — block N cannot read the Position row written in
//! block N-1 — so folding here would either be wrong or force a store module
//! and serialise the whole pipeline. Instead we emit the immutable event
//! stream (`Subscribe`/`Unsubscribe`/`Transfer` are already `immutable: true`
//! in the deployed schema) and let the SQL layer materialise Position with
//! `last_value(to) ORDER BY (block, log_index)` for the owner and
//! `first_value(...)` for origin/created_at. That is strictly more information
//! than the subgraph keeps: ownership history survives instead of being
//! overwritten.
//!
//! We also do NOT drop mints/burns: `from == 0x0` is a mint and `to == 0x0` a
//! burn, and both stay in the stream as ordinary transfers, exactly as the
//! subgraph records them.

use substreams::Hex;
use substreams_ethereum::pb::eth::v2::Block;
use substreams_ethereum::Event;

use crate::abi;
use crate::pb::uniswap::v4::v1 as pb;

/// PositionManager on Base (startBlock 25350993).
const POSITION_MANAGER: [u8; 20] = hex_literal::hex!("7C5f5A4bBd8fD63184577525326123B519429bDc");

pub fn extract(blk: &Block, events: &mut pb::Events) {
    for log in blk.logs() {
        if log.address() != POSITION_MANAGER {
            continue;
        }

        let tx = log.receipt.transaction;
        // graph-node's `event.logIndex` is block-scoped, not tx-scoped, so the
        // `txHash-logIndex` ids stay byte-identical to the subgraph's only if
        // we use `block_index` here rather than `index`.
        let log_index = log.log.block_index;

        if let Some(ev) = abi::position_manager::events::Subscription::match_and_decode(log) {
            events.position_events.push(pb::PositionEvent {
                id: event_id(&tx.hash, log_index),
                token_id: ev.token_id.to_string(),
                kind: "subscribe".to_string(),
                address: addr(&ev.subscriber),
                // A subscription has no counterparties; leaving these empty
                // keeps one flat table instead of three near-empty ones.
                from: String::new(),
                to: String::new(),
                meta: Some(meta(blk, tx, log_index)),
            });
        } else if let Some(ev) =
            abi::position_manager::events::Unsubscription::match_and_decode(log)
        {
            events.position_events.push(pb::PositionEvent {
                id: event_id(&tx.hash, log_index),
                token_id: ev.token_id.to_string(),
                kind: "unsubscribe".to_string(),
                address: addr(&ev.subscriber),
                from: String::new(),
                to: String::new(),
                meta: Some(meta(blk, tx, log_index)),
            });
        } else if let Some(ev) = abi::position_manager::events::Transfer::match_and_decode(log) {
            events.position_events.push(pb::PositionEvent {
                id: event_id(&tx.hash, log_index),
                // ERC-721 names this param `id`; the subgraph calls it tokenId.
                token_id: ev.id.to_string(),
                kind: "transfer".to_string(),
                // `address` is the "who does this row now concern" column, so
                // for a transfer it mirrors the new owner (see proto comment).
                address: addr(&ev.to),
                from: addr(&ev.from),
                to: addr(&ev.to),
                meta: Some(meta(blk, tx, log_index)),
            });
        }
    }
}

/// `0x`-prefixed lowercase hex — matches graph-ts `Bytes.toHexString()`, so ids
/// and address columns compare equal against the deployed subgraph's output.
fn addr(bytes: &[u8]) -> String {
    format!("0x{}", Hex::encode(bytes))
}

/// The subgraph's `eventId(tx.hash, logIndex)`.
fn event_id(tx_hash: &[u8], log_index: u32) -> String {
    format!("0x{}-{}", Hex::encode(tx_hash), log_index)
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
        // The subgraph's `origin` is `event.transaction.from` — the EOA, not
        // the router/contract that actually emitted the log.
        origin: addr(&tx.from),
        gas_used: tx.gas_used,
        gas_price: tx
            .gas_price
            .as_ref()
            .map(|v| Into::<substreams::scalar::BigInt>::into(v).to_string())
            // Pre-London / synthetic txs can omit gasPrice; "0" beats a NULL
            // that every downstream aggregate would have to guard.
            .unwrap_or_else(|| "0".to_string()),
    }
}
