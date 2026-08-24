<img src="icon.png" width="72" align="right" alt="Uniswap"/>

# uniswap-v4-substreams

Uniswap V4 on Base as a Substreams package, converted from **`uniswap-v4-base-3`**
(`Qmbsc6XQWbiv4DfLVfaNciScqYLyDWUYjWzrFBbzzmRsMB`) — the busiest subgraph on The Graph.

Built with StreamingFast's [`substreams-convert`](https://github.com/streamingfast/substreams-skills) skill.

## What it does that the subgraph does not

**Hook permissions, decoded from the address.** Uniswap V4 mines hook addresses so a hook's
capabilities live in the low 14 bits of its own address. Every pool arrives with all 14 flags
resolved — no RPC, no allowlist, and correct for a hook nobody has seen before. The source
subgraph stores `hooks` as an opaque string and hardcodes a single `ArrakisHook` entity.

```
pool 0x86ca82ba…  hook 0x9ea93273…  flags 4160  →  after_initialize + after_swap
```

**The configured fee survives.** The subgraph runs `pool.feeTier = event.params.fee` on every
swap, so the pool's configured fee is overwritten by whatever the last swap charged. Here
`pool.fee_tier` (static) and `swap.fee` (effective, hook-overridable) are separate columns.
Measured over 150 Base blocks: **25 distinct per-swap fee values** across 934 swaps, the most
common being `19900` — not a standard tier.

**Real gas.** The subgraph hardcodes `transaction.gasUsed = BigInt.zero()` with its own TODO.

**`salt` is kept**, so two salted positions by one sender on one tick range stay distinguishable.

**Every swap knows what it traded.** This is the correctness fix, not a nicety. A V4 `Swap`
log carries the **poolId and nothing else about the pool** — the tokens, the configured fee,
the tick spacing and the hook are fields of the `PoolKey`, keccak-hashed into the id at
`initialize` and never re-emitted. A consumer streaming from a recent block literally cannot
say what a swap traded. The subgraph papers over this with `Pool.load(poolId)` against
graph-node's implicit entity store; Substreams has no implicit state, so the join is an
explicit module:

```
map_events ──┬─────────────────────────► store_pools   (set, proto:Pool, key "pool:<poolId>")
             │                                 │
             └──────────► map_enriched ◄───────┘  (mode: get → get_last)
                                │
                             db_out
```

`map_enriched` also emits per-block `PoolStats` and `HookStats` — the hook roll-up the source
subgraph cannot produce at all, since it stores `hooks` as an opaque string with no hook entity.

`store_tokens` (ERC-20 symbol/name/decimals over `eth_call`) is wired and packed but sits
**off** the `map_enriched`/`db_out` path: it is not needed to answer "what did this swap
trade", and an RPC dependency should not be able to stall the main pipeline. Nothing consumes
it yet.

**No graft.** The live subgraph grafts at block 26990278 and never indexed its own early
history. This backfills from 25350988 in parallel.

## Verified

150 Base blocks from 35000000:

| | |
|---|---|
| pools initialised | 67 (all hooked, 2 dynamic-fee, 4 distinct hooks) |
| swaps | 934 |
| modify_liquidity | 5,235 |
| position events | 7 |

`cargo test --lib` — 7/7, including the live-verified `0x1888` hook case.

## Run

```bash
cargo build --target wasm32-unknown-unknown --release
substreams pack substreams.yaml

# Raw decoded logs. Store-free, so it streams from any block for free.
substreams run ./uniswap-v4-base-v0.1.0.spkg map_events \
  -e base-mainnet.streamingfast.io:443 -s 35000000 -t +150 -o jsonl

# Enriched. NOTE --limit-processed-blocks 0: store_pools has to be built from
# the PoolManager deploy block before block N can be served, so a request at
# 35000000 processes ~9.65M blocks of store preparation. The CLI's 10000-block
# default safeguard rejects that outright. The work is cached server-side per
# module hash, so it is paid once — but ANY change to a Rust source file
# changes the binary hash and therefore every module hash, and the next run
# pays it again.
substreams run ./uniswap-v4-base-v0.1.0.spkg map_enriched \
  -e base-mainnet.streamingfast.io:443 -s 35000000 -t +50 -o jsonl \
  --limit-processed-blocks 0 --production-mode
```

Sink to Postgres (chosen over ClickHouse because `pool` is mutable current state and
ClickHouse is insert-only):

```bash
substreams-sink-sql setup "$DSN" ./uniswap-v4-base-v0.1.0.spkg
substreams-sink-sql run   "$DSN" ./uniswap-v4-base-v0.1.0.spkg
```

## Known gaps

- **Swap amount signs are raw on-chain (swapper-centric)**; the subgraph negates to
  pool-centric and divides by token decimals. Rows are sign-flipped versus the deployed
  subgraph. Decimals need a token-metadata lookup that is not wired yet.
- **`Donate`, ERC-6909 `Transfer`/`Approval`/`OperatorSet`, `ProtocolFeeUpdated` are still
  undecoded.** The proto messages, the Postgres tables and the `db_out` writers are all in
  place now — but `src/pool_manager.rs` still lets those logs fall through, so the three
  tables stay EMPTY. Do not read an empty `donate` table as "V4 has no donations on Base".
  Adding the decoder is a mapping change, not a schema migration.
- **`pool_stats` / `hook_stats` are per-block DELTAS, not running totals.** `map_enriched` is
  a stateless `map` re-executed out of order by parallel backfill workers. `swap_count`,
  `modify_liquidity_count` and both volumes SUM correctly over a range; `pool_count` and
  `distinct_fee_values` are set cardinalities and do **not** — answer those from the base
  tables. Folding them into a running total needs an add-policy stats store that does not
  exist yet.
- **`store_tokens` has no consumer**, and a store handler cannot read its own store, so WETH
  re-pays its `eth_call` on every new WETH pool. The fix is a `map` taking `store_tokens` as a
  `get` input to filter known addresses.
- **No USD pricing.** The subgraph's `Bundle`/`derivedETH` whitelist-pool pricing is not ported.
- **`eth_common:filtered_events` block-skip is not enabled** — it takes one address and this
  package watches three contracts.
