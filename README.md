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
substreams run ./uniswap-v4-base-v0.1.0.spkg map_events \
  -e base-mainnet.streamingfast.io:443 -s 35000000 -t +150 -o jsonl
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
- **Not yet in the proto**: `Donate`, ERC-6909 `Transfer`/`Approval`/`OperatorSet` (V4 claim
  tokens / flash accounting), `ProtocolFeeUpdated`. The ABI bindings exist; these need their
  own messages rather than being forced into `Swap` or `PositionEvent`.
- **No USD pricing.** The subgraph's `Bundle`/`derivedETH` whitelist-pool pricing is not ported.
- **`eth_common:filtered_events` block-skip is not enabled** — it takes one address and this
  package watches three contracts.
