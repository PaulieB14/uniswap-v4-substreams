<img src="icon.png" width="72" align="right" alt="Uniswap"/>

# uniswap-v4-substreams

Uniswap V4 on **Base** as a Substreams package, converted from **`uniswap-v4-base-3`**
(`Qmbsc6XQWbiv4DfLVfaNciScqYLyDWUYjWzrFBbzzmRsMB`) — the busiest subgraph on The Graph.

Built with StreamingFast's [`substreams-convert`](https://github.com/streamingfast/substreams-skills).

Every claim below is verified against at least one of: the chain (`eth_call` / `eth_getLogs` /
`extsload`), the deployed subgraph, or a live `substreams run`. Anything unverified is in
**Known gaps**, not here.

## It finds a real bug in the source subgraph

The subgraph runs `pool.feeTier = event.params.fee` on every swap, so a pool's *configured* fee is
overwritten by whatever the last swap charged. On pool `0x0a1e0f12…` (ETH/$SXR):

| source | fee |
|---|---|
| `Initialize` event at creation, block 49150754 | **3000** |
| live `extsload` slot0 `lpFee` | **3000** |
| subgraph `pool.feeTier` | **3499** |

That pool has `hooks = 0x000…000`, so dynamic fees are impossible and the fee is immutable in its
`PoolKey`. Wrong across 3,060 transactions. A second pool (`0x36d7043e…`, VVV/cbBTC) shows the same
3499-vs-3000 discrepancy, so it is systematic.

This package keeps `pool.fee_tier` (configured, from `Initialize`) and `swap.fee` (effective,
per-swap, hook-overridable) as separate columns.

## What else it does that the subgraph doesn't

**Hook permissions decoded from the hook address.** V4 mines addresses so a hook's capability set
lives in its low 14 bits — 14 booleans per pool, no RPC, correct for a hook nobody has seen.
Verified against live Base hooks (`0x9ea93273…` → flags 4160 → `after_initialize` + `after_swap`).
The subgraph stores `hooks` as an opaque string plus one hardcoded `ArrakisHook` entity.

**Real `gasUsed`.** Every subgraph swap row carries `gasUsed: "0"` — it hardcodes it, with its own
TODO in the source.

**ERC-6909 claim tokens.** V4's flash-accounting rail. 292 rows in a 400-block sample; the
subgraph does not index them at all.

**`salt` retained**, so two salted positions by one sender on one tick range stay distinguishable.

**Self-describing rows.** A V4 `Swap` log carries only a `poolId`. `store_pools` records each pool
at creation and `map_enriched` denormalises `token0`/`token1`/`fee_tier`/`tick_spacing`/`hook` back
onto every swap and liquidity event, so a row stands alone.

**Token metadata** — symbol, name, decimals via batched `eth_call`, cached once per token.
Cross-checked against a direct `eth_call`: token `0xe7fd1ba7…` reports `siddesh`, exact match.

**Lifetime totals and per-block deltas, both.** `pool_stats`/`hook_stats` are per-block deltas that
SUM over a range; `pool_totals`/`hook_totals` are lifetime figures from add-policy stores. The
accumulation lives in the store, not in SQL, because a substreams store is deterministic and
re-derived — a block replayed by a parallel backfill worker does not double-add, whereas
`UPDATE ... = col + n` would.

**No graft.** The live subgraph grafts at block 26990278 and never indexed its own early history.

## Verified

`substreams run db_out`, 400 Base blocks from 35000000:

| table | rows |
|---|---|
| modify_liquidity | 19,007 |
| pool | 2,970 |
| swap | 2,745 |
| pool_stats / pool_totals | 1,655 / 1,655 |
| claim_token_event | 292 |
| hook_stats / hook_totals | 184 / 184 |
| position / position_event | 34 / 34 |

`cargo test --lib` — **92 passing**, including price maths pinned against chain `extsload` *and*
the deployed subgraph on the asymmetric-decimals case (VVV/cbBTC, 18 vs 8): computed
`token0Price` 4676.0682880466 against the subgraph's 4676.06828804666294…

## Run

```bash
cargo build --target wasm32-unknown-unknown --release
substreams pack substreams.yaml
substreams run ./uniswap-v4-base-v0.1.0.spkg map_enriched \
  -e base-mainnet.streamingfast.io:443 -s 25350988 -t +9000 -o jsonl
```

Postgres sink (chosen over ClickHouse because `pool` is mutable current state and ClickHouse is
insert-only):

```bash
substreams-sink-sql setup "$DSN" ./uniswap-v4-base-v0.1.0.spkg
substreams-sink-sql run   "$DSN" ./uniswap-v4-base-v0.1.0.spkg
```

Note: v4 delta operations require a recent `substreams-sink-sql`; an older binary silently ignores
them.

## Known gaps

- **USD pricing is wired but unproven at scale.** `store_prices` maintains the native price off
  the hardcoded WETH/USDC anchor and `map_totals` attaches `amount0_usd` / `amount1_usd` /
  `amount_usd` / `native_price_usd` / `priced`. The maths is pinned by tests against chain
  `extsload` and the subgraph (anchor computes 2445.38 USD/ETH against the subgraph's 2436.70,
  0.36% apart — ETH moving between reads). What is NOT demonstrated is a populated USD column over
  a real range: that needs the price store built from block 25350988, which exceeds the
  10,000-block request limit on this tier. Filter on `priced`, never on `amount_usd > 0` — an
  unanchored swap and a genuinely zero-value swap both read 0.
- **Only anchorable swaps get a USD value**, by design: a stablecoin leg, a native leg, or a
  whitelisted token with a derived-native price. Everything else stays unpriced rather than
  routed through an arbitrary intermediate.
- **Swap amount signs are raw on-chain (swapper-centric).** The subgraph negates to pool-centric
  *and* divides by decimals, so rows are sign-flipped versus it.
- **`Donate` and `ProtocolFeeUpdated` decoders are present but unexercised.** Zero rows in the
  400-block sample — and confirmed by `eth_getLogs` that zero such events occurred on chain in that
  range, so this is absence of data, not a broken decoder. Untested in production.
- **No block-index filter, deliberately.** 148 of 148 sampled blocks contain V4 activity, so
  `eth_common:filtered_events` would skip nothing.
- **Testing store-backed modules at a recent block needs quota.** Reading `store_pools` at block
  35000000 requires building the store from 25350988 — 9.65M blocks against a 10,000-block request
  limit. Verification above used a temporary manifest with `initialBlock` moved forward; the
  shipped manifest starts at 25350988 and sees every pool.
