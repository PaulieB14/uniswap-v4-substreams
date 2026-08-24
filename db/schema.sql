-- ============================================================================
-- Uniswap V4 on Base — PostgreSQL sink schema for substreams-sink-sql
-- Source of truth: subgraph Qmbsc6XQWbiv4DfLVfaNciScqYLyDWUYjWzrFBbzzmRsMB
-- ============================================================================
--
-- WHY POSTGRESQL AND NOT CLICKHOUSE
-- ---------------------------------
-- The V4 domain is not an append-only event log. `pool` is a *current-state*
-- row: sqrtPrice, tick and liquidity are overwritten on every swap, and
-- `position.owner` is overwritten on every ERC-721 transfer. Expressing that
-- needs real UPDATEs.
--
-- Of the four (engine x mode) combinations the SQL sink offers, exactly one
-- gives mutable rows:
--
--   PostgreSQL + Database Changes  -> INSERT / UPDATE / DELETE, DB-side reorg
--                                     handling via substreams_history.   <-- us
--   PostgreSQL + from-proto        -> insert-only, DDL generated from proto.
--   ClickHouse + from-proto        -> insert-only (ReplacingMergeTree).
--   ClickHouse + Database Changes  -> runs, but OnlyInserts()==true, no reorg
--                                     management, duplicate PKs tolerated.
--
-- ClickHouse would force us to model `pool` as an event stream and reconstruct
-- current state with argMax() at read time on every query — on the busiest
-- subgraph on The Graph that is the wrong tradeoff for the one table that is
-- small (tens of thousands of pools) and read on every single request.
-- The genuinely append-only tables here (swap, modify_liquidity) would be
-- happier on ClickHouse; if you ever want both, run a second sink rather than
-- compromising this one. See db_out.rs for the write side.
--
-- CONVENTIONS
-- -----------
-- * VARCHAR(66)/VARCHAR(42) sized so both `0x`-prefixed and bare hex fit.
-- * NUMERIC(78,0) for every EVM integer: uint256 needs 78 decimal digits, and
--   int128/int256 deltas are signed, so BIGINT is not an option.
-- * INTEGER for int24 ticks and uint24 fees.
-- * block_timestamp stays a unix BIGINT (what the proto carries). Wrap it in
--   to_timestamp() at query time; a stored generated TIMESTAMPTZ would cost
--   8 bytes on a table that will hold billions of swaps.
-- * NO FOREIGN KEYS. Three reasons: (1) an FK check is an index probe per
--   inserted row on a firehose-speed stream; (2) the sink groups changes into
--   one transaction per batch with no guaranteed inter-table ordering, so a
--   swap can be applied before its pool inside the same transaction; (3) if
--   the sink is ever started above the PoolManager deploy block, referenced
--   pools legitimately do not exist. Referential integrity is guaranteed by
--   the chain, not by the database.

-- ---------------------------------------------------------------------------
-- cursors — required by substreams-sink-sql.
-- `setup` normally creates this itself; it is declared here (IF NOT EXISTS) so
-- the schema is self-contained and can be applied by hand or by a migration
-- tool without the sink present.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cursors
(
    id         TEXT NOT NULL CONSTRAINT cursor_pk PRIMARY KEY,
    cursor     TEXT,
    block_num  BIGINT,
    block_id   TEXT
);

-- ---------------------------------------------------------------------------
-- pool — MUTABLE current state. One row per PoolId, created by Initialize and
-- updated by every Swap.
--
-- HOOK PERMISSIONS: 14 boolean columns live here rather than in a separate
-- `hook_permissions` table. The argument:
--
--   1. Permissions are a pure function of the hook *address* (V4 mines the low
--      14 bits of the address to encode them), so they are known and final at
--      pool creation. There is no write-after-create, which is what a
--      normalised table would exist to protect against.
--   2. A separate table keyed on hook address would need UPSERT semantics,
--      because the same hook is re-observed on every pool that reuses it. The
--      Database Changes UPSERT op is supported unevenly across sink versions,
--      and everything here is otherwise a pure INSERT. Denormalising keeps the
--      write path free of that dependency entirely.
--   3. The queries this schema exists to answer — "which pools can override
--      the swap fee", "which pools have a beforeSwap hook" — become
--      single-table index scans with no join. That matters far more than the
--      14 bytes per pool row that the duplication costs.
--   4. The normalised view is still one cheap GROUP BY away over a small
--      table, e.g.
--        SELECT hook_address, count(*) AS pools, bool_or(hook_before_swap)
--        FROM pool WHERE has_hook GROUP BY hook_address;
--      It is deliberately not declared as a VIEW: some sink versions enumerate
--      information_schema.tables at startup and reject relations without a
--      primary key.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pool
(
    -- V4 pools are bytes32 PoolIds, not addresses. There is no pool contract.
    id                        VARCHAR(66)   NOT NULL,
    token0                    VARCHAR(42)   NOT NULL,
    token1                    VARCHAR(42)   NOT NULL,
    fee_tier                  BIGINT        NOT NULL,
    tick_spacing              INTEGER       NOT NULL,
    -- 0x800000 (8388608) is the dynamic-fee sentinel; precomputed so consumers
    -- never have to know the magic number.
    is_dynamic_fee            BOOLEAN       NOT NULL DEFAULT FALSE,

    -- hook, decoded from the low 14 bits of hook_address. hook_flags is the
    -- raw mask, kept so callers can do bitmask predicates
    -- (hook_flags & 128 <> 0  ==  beforeSwap) without naming 14 columns.
    hook_address              VARCHAR(42)   NOT NULL,
    has_hook                  BOOLEAN       NOT NULL DEFAULT FALSE,
    hook_flags                INTEGER       NOT NULL DEFAULT 0,
    hook_before_initialize                     BOOLEAN NOT NULL DEFAULT FALSE, -- bit 13
    hook_after_initialize                      BOOLEAN NOT NULL DEFAULT FALSE, -- bit 12
    hook_before_add_liquidity                  BOOLEAN NOT NULL DEFAULT FALSE, -- bit 11
    hook_after_add_liquidity                   BOOLEAN NOT NULL DEFAULT FALSE, -- bit 10
    hook_before_remove_liquidity               BOOLEAN NOT NULL DEFAULT FALSE, -- bit 9
    hook_after_remove_liquidity                BOOLEAN NOT NULL DEFAULT FALSE, -- bit 8
    hook_before_swap                           BOOLEAN NOT NULL DEFAULT FALSE, -- bit 7
    hook_after_swap                            BOOLEAN NOT NULL DEFAULT FALSE, -- bit 6
    hook_before_donate                         BOOLEAN NOT NULL DEFAULT FALSE, -- bit 5
    hook_after_donate                          BOOLEAN NOT NULL DEFAULT FALSE, -- bit 4
    hook_before_swap_returns_delta             BOOLEAN NOT NULL DEFAULT FALSE, -- bit 3
    hook_after_swap_returns_delta              BOOLEAN NOT NULL DEFAULT FALSE, -- bit 2
    hook_after_add_liquidity_returns_delta     BOOLEAN NOT NULL DEFAULT FALSE, -- bit 1
    hook_after_remove_liquidity_returns_delta  BOOLEAN NOT NULL DEFAULT FALSE, -- bit 0

    -- Mutable state. Seeded from Initialize, then overwritten by every Swap.
    -- NOTE: liquidity is taken verbatim from the Swap event's `liquidity`
    -- field (the PoolManager's own post-swap active liquidity) instead of
    -- being accumulated from ModifyLiquidity deltas the way the subgraph does
    -- it. The subgraph has to run in-range tick math to decide whether a
    -- liquidity delta touches active liquidity; reading the number the
    -- contract already emitted is both cheaper and strictly more correct.
    sqrt_price                NUMERIC(78,0) NOT NULL DEFAULT 0,
    tick                      INTEGER,
    liquidity                 NUMERIC(78,0) NOT NULL DEFAULT 0,

    created_at_block          BIGINT        NOT NULL,
    created_at_timestamp      BIGINT        NOT NULL,
    created_at_tx             VARCHAR(66)   NOT NULL,
    -- When the mutable columns above were last refreshed. Swap-driven, hence
    -- the name: ModifyLiquidity and Donate deliberately do not touch the pool
    -- row, because they carry no authoritative post-event pool state and an
    -- extra UPDATE per liquidity event is pure write amplification.
    last_swap_block           BIGINT,
    last_swap_timestamp       BIGINT,

    CONSTRAINT pool_pk PRIMARY KEY (id)
);

-- The requested partial index: only pools that actually have a hook. On Base
-- the overwhelming majority of pools are hookless, so this index is a small
-- fraction of the table and answers "everything about hooked pools" directly.
CREATE INDEX IF NOT EXISTS pool_hook_address_idx ON pool (hook_address) WHERE has_hook;
CREATE INDEX IF NOT EXISTS pool_hook_flags_idx   ON pool (hook_flags)   WHERE has_hook;
-- Dynamic-fee pools are the other rare-and-interesting subset.
CREATE INDEX IF NOT EXISTS pool_dynamic_fee_idx  ON pool (id)           WHERE is_dynamic_fee;
CREATE INDEX IF NOT EXISTS pool_token0_idx       ON pool (token0);
CREATE INDEX IF NOT EXISTS pool_token1_idx       ON pool (token1);
CREATE INDEX IF NOT EXISTS pool_created_idx      ON pool (created_at_block);

-- ---------------------------------------------------------------------------
-- swap — IMMUTABLE. The hot table; assume billions of rows.
-- id = "<txHash>-<logIndex>", matching the subgraph's eventId().
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS swap
(
    id              VARCHAR(80)   NOT NULL,
    pool_id         VARCHAR(66)   NOT NULL,
    sender          VARCHAR(42)   NOT NULL,
    -- Signed int128 deltas from the pool's point of view: negative = out.
    -- Raw token units, NOT decimal-adjusted. The subgraph divides by
    -- token.decimals and prices everything in USD; that needs ERC-20
    -- eth_calls and a pricing graph, neither of which belongs in a
    -- deterministic firehose map. Adjust downstream.
    amount0         NUMERIC(78,0) NOT NULL,
    amount1         NUMERIC(78,0) NOT NULL,
    sqrt_price_x96  NUMERIC(78,0) NOT NULL,
    liquidity       NUMERIC(78,0) NOT NULL,
    tick            INTEGER       NOT NULL,
    -- The fee ACTUALLY charged on this swap. On a dynamic-fee pool or a pool
    -- with a beforeSwap hook this differs from pool.fee_tier — which is why
    -- V4 puts it on the event and why it is stored per-swap here. The subgraph
    -- drops this field entirely.
    fee             INTEGER       NOT NULL,

    -- DENORMALISED POOL IDENTITY -- the defect this schema version fixes.
    --
    -- A V4 Swap log carries ONLY the poolId. token0/token1/fee_tier/
    -- tick_spacing/hook are fields of the PoolKey, keccak-hashed into the id at
    -- initialize and never re-emitted, so "what did this swap trade" is
    -- unanswerable from the raw row. These columns are filled by map_enriched
    -- from the store_pools join.
    --
    -- Yes, this duplicates `pool`. That is the point: the alternative is a join
    -- to `pool` on every single query against the largest table in the schema,
    -- and the duplicated values are PoolKey-immutable, so they can never drift
    -- from their source the way a denormalised mutable column would.
    --
    -- EMPTY STRING / 0 means "pool not in the store", never "the pool has no
    -- token0". It should not occur: the package indexes from the PoolManager's
    -- own deploy block, so every swappable pool was initialised inside range.
    -- A non-empty count on `WHERE token0 = ''` means a partial-range run or a
    -- genuine bug -- worth an alert, not a filter.
    token0          VARCHAR(42)   NOT NULL DEFAULT '',
    token1          VARCHAR(42)   NOT NULL DEFAULT '',
    -- The pool's CONFIGURED fee, deliberately a separate column from `fee`
    -- above (the fee this swap actually paid). On a dynamic-fee pool the two
    -- differ on every row; collapsing them is precisely the source subgraph's
    -- bug (`pool.feeTier = event.params.fee`). `fee <> fee_tier` is now a
    -- single-table predicate: hook repricing, directly queryable.
    fee_tier        BIGINT        NOT NULL DEFAULT 0,
    tick_spacing    INTEGER       NOT NULL DEFAULT 0,
    -- The hook is carried as address + raw flag MASK, not as the 14 unpacked
    -- booleans that `pool` and `hook_deployment` get. On a table assumed to
    -- reach billions of rows, 14 bools cost ~14 bytes/row to store a value that
    -- is a pure function of a number already in the row: hook_flags IS the mask
    -- the booleans were decoded from, so `hook_flags & 128 <> 0` is exactly
    -- `hook_before_swap` with no information lost.
    -- has_hook is NOT derivable from hook_flags (a hook can legally mine an
    -- address with no permission bits set), so derive it from the address:
    -- hook_address NOT IN ('', '0x0000000000000000000000000000000000000000').
    hook_address    VARCHAR(42)   NOT NULL DEFAULT '',
    hook_flags      INTEGER       NOT NULL DEFAULT 0,

    block_number    BIGINT        NOT NULL,
    block_timestamp BIGINT        NOT NULL,
    tx_hash         VARCHAR(66)   NOT NULL,
    log_index       INTEGER       NOT NULL,
    origin          VARCHAR(42)   NOT NULL,   -- tx.from, the EOA
    gas_used        BIGINT,
    gas_price       NUMERIC(78,0),

    CONSTRAINT swap_pk PRIMARY KEY (id),
    amount0_adjusted        NUMERIC,
    amount1_adjusted        NUMERIC,
    amounts_adjusted        BOOLEAN,
    -- USD is populated only when a leg is a stablecoin, native, or a whitelisted
    -- token with a derived-native price. Filter on `priced`, not on
    -- amount_usd > 0: an unanchored swap and a zero-value swap both read 0.
    amount0_usd             NUMERIC,
    amount1_usd             NUMERIC,
    amount_usd              NUMERIC,
    native_price_usd        NUMERIC,
    priced                  BOOLEAN
);

-- "Recent swaps in this pool" — the single most common V4 query. DESC so the
-- planner walks the index backwards for free on ORDER BY ... DESC LIMIT n.
CREATE INDEX IF NOT EXISTS swap_pool_block_idx ON swap (pool_id, block_number DESC, log_index DESC);
-- BRIN, not btree, for the block range scan: rows arrive in block order so the
-- table is physically clustered on block_number, and a BRIN over a
-- billion-row table is kilobytes where a btree is tens of gigabytes.
CREATE INDEX IF NOT EXISTS swap_block_brin_idx ON swap USING BRIN (block_number) WITH (pages_per_range = 128);
-- "What did this EOA do" — wallet-level attribution, the thing V4's `sender`
-- (usually a router) cannot answer.
CREATE INDEX IF NOT EXISTS swap_origin_idx     ON swap (origin, block_number DESC);
-- Operational lookup by transaction. Droppable if write throughput is tight:
-- the PK is "<txHash>-<logIndex>", so a prefix match on the PK is a partial
-- substitute (needs text_pattern_ops under a non-C collation).
CREATE INDEX IF NOT EXISTS swap_tx_hash_idx    ON swap (tx_hash);
-- The queries the denormalisation exists to make possible without a join.
-- "All swaps in this token, newest first" -- previously a join to `pool`.
CREATE INDEX IF NOT EXISTS swap_token0_idx     ON swap (token0, block_number DESC);
CREATE INDEX IF NOT EXISTS swap_token1_idx     ON swap (token1, block_number DESC);
-- "Everything routed through this hook". Partial: most Base pools are
-- hookless, so this indexes the small, interesting subset.
CREATE INDEX IF NOT EXISTS swap_hook_idx       ON swap (hook_address, block_number DESC)
    WHERE hook_address <> '' AND hook_address <> '0x0000000000000000000000000000000000000000';
-- Swaps where the hook actually moved the fee off the pool's configured tier.
-- The single clearest "this hook is doing real dynamic-fee work" filter, and
-- it is only expressible because fee and fee_tier are separate columns.
CREATE INDEX IF NOT EXISTS swap_fee_override_idx ON swap (pool_id, block_number DESC)
    WHERE fee <> fee_tier;

-- ---------------------------------------------------------------------------
-- modify_liquidity — IMMUTABLE. Mints, burns and fee collections all arrive as
-- one event in V4; the sign of liquidity_delta is what separates them.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS modify_liquidity
(
    id              VARCHAR(80)   NOT NULL,
    pool_id         VARCHAR(66)   NOT NULL,
    sender          VARCHAR(42)   NOT NULL,
    tick_lower      INTEGER       NOT NULL,
    tick_upper      INTEGER       NOT NULL,
    -- Signed int256. > 0 add, < 0 remove, == 0 is a fee collect (V4 reuses the
    -- event for that), which is why there is no separate `collect` table.
    liquidity_delta NUMERIC(78,0) NOT NULL,
    -- V4's per-owner position discriminator. bytes32, not an address.
    salt            VARCHAR(66)   NOT NULL,

    -- Denormalised pool identity, same rationale and same "empty means not in
    -- the store" contract as `swap` above. Without it, "which pair did this LP
    -- provide to" needs a join for every row.
    token0          VARCHAR(42)   NOT NULL DEFAULT '',
    token1          VARCHAR(42)   NOT NULL DEFAULT '',
    fee_tier        BIGINT        NOT NULL DEFAULT 0,
    tick_spacing    INTEGER       NOT NULL DEFAULT 0,
    hook_address    VARCHAR(42)   NOT NULL DEFAULT '',
    hook_flags      INTEGER       NOT NULL DEFAULT 0,

    block_number    BIGINT        NOT NULL,
    block_timestamp BIGINT        NOT NULL,
    tx_hash         VARCHAR(66)   NOT NULL,
    log_index       INTEGER       NOT NULL,
    origin          VARCHAR(42)   NOT NULL,
    gas_used        BIGINT,
    gas_price       NUMERIC(78,0),

    CONSTRAINT modify_liquidity_pk PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS ml_pool_block_idx  ON modify_liquidity (pool_id, block_number DESC, log_index DESC);
CREATE INDEX IF NOT EXISTS ml_block_brin_idx  ON modify_liquidity USING BRIN (block_number) WITH (pages_per_range = 128);
-- The reason this table exists: liquidity-by-price-range analytics.
CREATE INDEX IF NOT EXISTS ml_pool_range_idx  ON modify_liquidity (pool_id, tick_lower, tick_upper);
CREATE INDEX IF NOT EXISTS ml_origin_idx      ON modify_liquidity (origin, block_number DESC);
CREATE INDEX IF NOT EXISTS ml_token0_idx      ON modify_liquidity (token0, block_number DESC);
CREATE INDEX IF NOT EXISTS ml_token1_idx      ON modify_liquidity (token1, block_number DESC);
CREATE INDEX IF NOT EXISTS ml_hook_idx        ON modify_liquidity (hook_address, block_number DESC)
    WHERE hook_address <> '' AND hook_address <> '0x0000000000000000000000000000000000000000';

-- ---------------------------------------------------------------------------
-- position_event — IMMUTABLE log of PositionManager NFT activity.
-- The subgraph splits this into three entities (Subscribe / Unsubscribe /
-- Transfer) that share an identical shape; one table with a `kind`
-- discriminator answers "everything that happened to token N" without a
-- three-way UNION.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS position_event
(
    id              VARCHAR(80)   NOT NULL,
    -- Decimal string of the uint256 tokenId, matching the subgraph's
    -- positionId(). NUMERIC (not TEXT) so ordering and range scans work.
    token_id        NUMERIC(78,0) NOT NULL,
    kind            VARCHAR(16)   NOT NULL,   -- subscribe | unsubscribe | transfer
    -- The counterparty: the subscriber for subscribe/unsubscribe, the
    -- recipient for a transfer. Denormalised so "everything involving this
    -- address" is one index, regardless of kind.
    address         VARCHAR(42)   NOT NULL,
    -- Populated for kind='transfer' only; empty string otherwise.
    from_address    VARCHAR(42)   NOT NULL DEFAULT '',
    to_address      VARCHAR(42)   NOT NULL DEFAULT '',

    block_number    BIGINT        NOT NULL,
    block_timestamp BIGINT        NOT NULL,
    tx_hash         VARCHAR(66)   NOT NULL,
    log_index       INTEGER       NOT NULL,
    origin          VARCHAR(42)   NOT NULL,
    gas_used        BIGINT,
    gas_price       NUMERIC(78,0),

    CONSTRAINT position_event_pk PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS pe_token_block_idx ON position_event (token_id, block_number DESC, log_index DESC);
CREATE INDEX IF NOT EXISTS pe_address_idx     ON position_event (address, block_number DESC);
CREATE INDEX IF NOT EXISTS pe_block_brin_idx  ON position_event USING BRIN (block_number) WITH (pages_per_range = 128);
-- Subscriptions are the rare, interesting subset (a subscriber contract is
-- watching the position). Partial index keeps it tiny.
CREATE INDEX IF NOT EXISTS pe_subscriber_idx  ON position_event (address) WHERE kind <> 'transfer';

-- ---------------------------------------------------------------------------
-- position — MUTABLE current state, one row per PositionManager NFT.
-- Not in the proto contract; derived here from the transfer stream, exactly as
-- the subgraph derives Position.owner from handleTransfer. It is the second
-- reason this schema is PostgreSQL: an insert-only engine cannot answer
-- "who owns position N right now" without an argMax over every transfer.
-- Row is created by the mint (from == 0x0) and UPDATEd by every later
-- transfer; a burn sets owner to the zero address rather than deleting, so the
-- history in position_event stays joinable.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS position
(
    token_id             NUMERIC(78,0) NOT NULL,
    owner                VARCHAR(42)   NOT NULL,
    -- The EOA that minted it. Never changes; that is the point of keeping it
    -- separate from owner.
    origin               VARCHAR(42)   NOT NULL,
    created_at_block     BIGINT        NOT NULL,
    created_at_timestamp BIGINT        NOT NULL,
    last_transfer_block  BIGINT,

    CONSTRAINT position_pk PRIMARY KEY (token_id)
);

CREATE INDEX IF NOT EXISTS position_owner_idx  ON position (owner);
CREATE INDEX IF NOT EXISTS position_origin_idx ON position (origin);

-- ---------------------------------------------------------------------------
-- hook_deployment — IMMUTABLE. Hooks minted by a known factory (Arrakis today;
-- `factory` is a column, not a table name, so a second factory is a mapping
-- change and not a migration).
-- Carries the same decoded permission columns as `pool` so the table is
-- self-contained: a freshly deployed hook has no pools yet, so joining to
-- `pool` to learn what it can do would return nothing.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hook_deployment
(
    id              VARCHAR(80)   NOT NULL,
    hook_address    VARCHAR(42)   NOT NULL,
    module          VARCHAR(42)   NOT NULL,
    salt            VARCHAR(66)   NOT NULL,
    factory         VARCHAR(42)   NOT NULL,

    hook_flags                                 INTEGER NOT NULL DEFAULT 0,
    hook_before_initialize                     BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_initialize                      BOOLEAN NOT NULL DEFAULT FALSE,
    hook_before_add_liquidity                  BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_add_liquidity                   BOOLEAN NOT NULL DEFAULT FALSE,
    hook_before_remove_liquidity               BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_remove_liquidity                BOOLEAN NOT NULL DEFAULT FALSE,
    hook_before_swap                           BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_swap                            BOOLEAN NOT NULL DEFAULT FALSE,
    hook_before_donate                         BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_donate                          BOOLEAN NOT NULL DEFAULT FALSE,
    hook_before_swap_returns_delta             BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_swap_returns_delta              BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_add_liquidity_returns_delta     BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_remove_liquidity_returns_delta  BOOLEAN NOT NULL DEFAULT FALSE,

    block_number    BIGINT        NOT NULL,
    block_timestamp BIGINT        NOT NULL,
    tx_hash         VARCHAR(66)   NOT NULL,
    log_index       INTEGER       NOT NULL,
    origin          VARCHAR(42)   NOT NULL,
    gas_used        BIGINT,
    gas_price       NUMERIC(78,0),

    CONSTRAINT hook_deployment_pk PRIMARY KEY (id)
);

-- Deliberately NOT UNIQUE on hook_address: CREATE2 makes a re-deploy to the
-- same address impossible, but a UNIQUE constraint would turn any duplicate
-- emission by the mapping into a hard sink failure mid-stream.
CREATE INDEX IF NOT EXISTS hd_hook_address_idx ON hook_deployment (hook_address);
CREATE INDEX IF NOT EXISTS hd_factory_idx      ON hook_deployment (factory, block_number DESC);
CREATE INDEX IF NOT EXISTS hd_block_idx        ON hook_deployment (block_number);

-- ===========================================================================
-- PER-BLOCK AGGREGATES
-- ===========================================================================
--
-- READ THIS BEFORE QUERYING pool_stats / hook_stats.
--
-- These are per-BLOCK DELTAS, not running totals. `map_enriched` is a `map`
-- module: it is stateless by definition and is re-executed out of order by
-- parallel backfill workers, so it can only ever report what happened in the
-- block it was handed. The proto comments describe these messages as "running
-- aggregates carried forward by the stats store" -- that store does not exist
-- yet, and until it does the numbers here are deltas.
--
-- The consequence is not symmetric across the columns:
--
--   * swap_count, modify_liquidity_count, volume_token0_abs, volume_token1_abs
--     are ADDITIVE. SUM() over a block range is exactly correct.
--   * pool_count and distinct_fee_values are SET CARDINALITIES. They do NOT
--     sum. Adding a hook's per-block pool_count across 1000 blocks counts the
--     same pool up to 1000 times. Answer those from the base tables instead:
--       SELECT count(DISTINCT pool_id) FROM swap WHERE hook_address = $1;
--       SELECT count(DISTINCT fee)     FROM swap WHERE hook_address = $1;
--     They are still worth storing per block: within ONE block they are true,
--     and the delta shape is what a materialised rollup would consume.
--
-- One row per (subject, block) touched. Pools and hooks with no activity in a
-- block emit nothing -- a full snapshot every block would be O(pools) rows for
-- no new information.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- pool_stats -- what each pool did in one block.
--
-- Synthetic single-column PK "<pool_id>-<block_number>" rather than a
-- composite (pool_id, block_number) PK: the Database Changes protocol supports
-- composite keys but the sink's support has varied across versions, and a
-- single-column key keeps the write path on the same push_change call as every
-- other table here. The two components are ALSO stored as real columns, so
-- nothing needs to parse the id back apart.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pool_stats
(
    id                     VARCHAR(90)   NOT NULL,   -- "<pool_id>-<block>"
    pool_id                VARCHAR(66)   NOT NULL,
    block_number           BIGINT        NOT NULL,
    -- Denormalised so a "top pools by volume" query names its rows without a
    -- second lookup. Empty when the pool was not resolvable from store_pools.
    token0                 VARCHAR(42)   NOT NULL DEFAULT '',
    token1                 VARCHAR(42)   NOT NULL DEFAULT '',

    swap_count             BIGINT        NOT NULL DEFAULT 0,
    modify_liquidity_count BIGINT        NOT NULL DEFAULT 0,
    -- ABSOLUTE-value sums in RAW token units (no decimal scaling -- see the
    -- README's known gaps). Absolute because V4 amounts are signed and
    -- swapper-centric: a plain sum measures net flow and converges to zero on
    -- a two-sided market no matter how much traded. Netting stays derivable
    -- from the signed `swap` rows; volume cannot be recovered from a net.
    volume_token0_abs      NUMERIC(78,0) NOT NULL DEFAULT 0,
    volume_token1_abs      NUMERIC(78,0) NOT NULL DEFAULT 0,

    hook_address           VARCHAR(42)   NOT NULL DEFAULT '',
    hook_flags             INTEGER       NOT NULL DEFAULT 0,

    CONSTRAINT pool_stats_pk PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS pool_stats_pool_block_idx ON pool_stats (pool_id, block_number DESC);
CREATE INDEX IF NOT EXISTS pool_stats_hook_idx       ON pool_stats (hook_address, block_number DESC)
    WHERE hook_address <> '' AND hook_address <> '0x0000000000000000000000000000000000000000';
CREATE INDEX IF NOT EXISTS pool_stats_block_brin_idx ON pool_stats USING BRIN (block_number) WITH (pages_per_range = 128);

-- ---------------------------------------------------------------------------
-- hook_stats -- what each HOOK did in one block, across all the pools it
-- serves. The roll-up the source subgraph cannot produce at all: it stores
-- `hooks` as an opaque string and has no hook entity.
--
-- Hookless pools are deliberately ABSENT rather than bucketed under the zero
-- address. A 0x0 row would be a chain-wide aggregate wearing a hook's clothes,
-- and it poisons distinct_fee_values specifically -- pooling every hookless
-- pool makes that count large purely because they sit on different static
-- tiers, which reads identically to one hook repricing per swap.
-- Pools that could not be resolved from store_pools are also absent, since the
-- hook is precisely the unknown.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hook_stats
(
    id                  VARCHAR(66)   NOT NULL,   -- "<hook_address>-<block>"
    hook_address        VARCHAR(42)   NOT NULL,
    block_number        BIGINT        NOT NULL,

    -- SET CARDINALITY, this block only. Does not sum -- see the block comment
    -- above.
    pool_count          BIGINT        NOT NULL DEFAULT 0,
    swap_count          BIGINT        NOT NULL DEFAULT 0,
    volume_token0_abs   NUMERIC(78,0) NOT NULL DEFAULT 0,
    volume_token1_abs   NUMERIC(78,0) NOT NULL DEFAULT 0,
    -- How many DISTINCT effective Swap.fee values this hook charged in the
    -- block. CONFOUND, documented rather than hidden: a hook serving three
    -- pools on static tiers 500/3000/10000 reports 3 without ever overriding
    -- anything. Compare against pool_count -- a static multi-pool hook's count
    -- is capped by its pool count, a repricing hook's is not -- or, for a real
    -- answer, count swaps where fee <> fee_tier (swap_fee_override_idx exists
    -- for exactly that).
    distinct_fee_values BIGINT        NOT NULL DEFAULT 0,

    hook_flags                                 INTEGER NOT NULL DEFAULT 0,
    hook_before_initialize                     BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_initialize                      BOOLEAN NOT NULL DEFAULT FALSE,
    hook_before_add_liquidity                  BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_add_liquidity                   BOOLEAN NOT NULL DEFAULT FALSE,
    hook_before_remove_liquidity               BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_remove_liquidity                BOOLEAN NOT NULL DEFAULT FALSE,
    hook_before_swap                           BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_swap                            BOOLEAN NOT NULL DEFAULT FALSE,
    hook_before_donate                         BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_donate                          BOOLEAN NOT NULL DEFAULT FALSE,
    hook_before_swap_returns_delta             BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_swap_returns_delta              BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_add_liquidity_returns_delta     BOOLEAN NOT NULL DEFAULT FALSE,
    hook_after_remove_liquidity_returns_delta  BOOLEAN NOT NULL DEFAULT FALSE,

    CONSTRAINT hook_stats_pk PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS hook_stats_hook_block_idx ON hook_stats (hook_address, block_number DESC);
CREATE INDEX IF NOT EXISTS hook_stats_block_brin_idx ON hook_stats USING BRIN (block_number) WITH (pages_per_range = 128);

-- ===========================================================================
-- PREVIOUSLY-UNHOMED PoolManager EVENTS
-- ===========================================================================
--
-- STATUS: the tables, the proto messages and the db_out writers below are all
-- wired, but NOTHING POPULATES THEM YET -- src/pool_manager.rs still lets
-- Donate / ERC-6909 / ProtocolFee logs fall through undecoded (see the UNHOMED
-- EVENTS block at the foot of that file). Expect these three tables to be
-- empty until a decoder lands. They are created anyway so that adding the
-- decoder is a mapping change and not a schema migration on a live sink.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- donate -- a direct fee donation to in-range LPs. Its own table, not a `swap`
-- row: pb::Swap has no kind discriminator, so any consumer summing
-- Events.swaps would book a donation as swap volume.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS donate
(
    id              VARCHAR(80)   NOT NULL,
    pool_id         VARCHAR(66)   NOT NULL,
    sender          VARCHAR(42)   NOT NULL,
    amount0         NUMERIC(78,0) NOT NULL DEFAULT 0,
    amount1         NUMERIC(78,0) NOT NULL DEFAULT 0,

    block_number    BIGINT        NOT NULL,
    block_timestamp BIGINT        NOT NULL,
    tx_hash         VARCHAR(66)   NOT NULL,
    log_index       INTEGER       NOT NULL,
    origin          VARCHAR(42)   NOT NULL,
    gas_used        BIGINT,
    gas_price       NUMERIC(78,0),

    CONSTRAINT donate_pk PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS donate_pool_block_idx ON donate (pool_id, block_number DESC, log_index DESC);
CREATE INDEX IF NOT EXISTS donate_sender_idx     ON donate (sender, block_number DESC);
CREATE INDEX IF NOT EXISTS donate_block_brin_idx ON donate USING BRIN (block_number) WITH (pages_per_range = 128);

-- ---------------------------------------------------------------------------
-- claim_token_event -- ERC-6909 activity on the singleton: Transfer, Approval,
-- OperatorSet. This is V4's flash-accounting rail: how routers and searchers
-- park value INSIDE the PoolManager between swaps instead of settling to
-- ERC-20. Nothing in the V4 subgraph surfaces it.
--
-- One table with a `kind` discriminator, not three: the three events share a
-- subject (a currency balance held by the singleton) and every consumer wants
-- them on one timeline. Columns not carried by a given kind stay at their
-- defaults.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS claim_token_event
(
    id              VARCHAR(80)   NOT NULL,
    kind            VARCHAR(16)   NOT NULL,   -- transfer | approval | operator_set
    caller          VARCHAR(42)   NOT NULL DEFAULT '',   -- transfer: msg.sender
    owner_address   VARCHAR(42)   NOT NULL DEFAULT '',   -- approval / operator_set
    -- from/to are SQL reserved words, matching position_event's convention.
    from_address    VARCHAR(42)   NOT NULL DEFAULT '',
    to_address      VARCHAR(42)   NOT NULL DEFAULT '',
    spender         VARCHAR(42)   NOT NULL DEFAULT '',   -- approval
    operator        VARCHAR(42)   NOT NULL DEFAULT '',   -- operator_set
    -- uint256 currency id = the currency address widened to uint256. Kept as
    -- the raw number, NOT re-narrowed to an address: that mapping holds only by
    -- convention and a lossy rewrite would be unrecoverable.
    currency_id     NUMERIC(78,0) NOT NULL DEFAULT 0,
    amount          NUMERIC(78,0) NOT NULL DEFAULT 0,
    approved        BOOLEAN       NOT NULL DEFAULT FALSE, -- operator_set

    block_number    BIGINT        NOT NULL,
    block_timestamp BIGINT        NOT NULL,
    tx_hash         VARCHAR(66)   NOT NULL,
    log_index       INTEGER       NOT NULL,
    origin          VARCHAR(42)   NOT NULL,
    gas_used        BIGINT,
    gas_price       NUMERIC(78,0),

    CONSTRAINT claim_token_event_pk PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS cte_currency_block_idx ON claim_token_event (currency_id, block_number DESC);
CREATE INDEX IF NOT EXISTS cte_from_idx           ON claim_token_event (from_address, block_number DESC);
CREATE INDEX IF NOT EXISTS cte_to_idx             ON claim_token_event (to_address, block_number DESC);
CREATE INDEX IF NOT EXISTS cte_kind_block_idx     ON claim_token_event (kind, block_number DESC);
CREATE INDEX IF NOT EXISTS cte_block_brin_idx     ON claim_token_event USING BRIN (block_number) WITH (pages_per_range = 128);

-- ---------------------------------------------------------------------------
-- protocol_fee_event -- ProtocolFeeUpdated / ProtocolFeeControllerUpdated. The
-- only on-chain record of protocol revenue being switched on for a pool.
--
-- Not folded into an UPDATE on `pool`: a sink upserting on pool.id could not
-- distinguish it from a pool creation and would zero out token0/token1/hook.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS protocol_fee_event
(
    id              VARCHAR(80)   NOT NULL,
    kind            VARCHAR(24)   NOT NULL,   -- fee_updated | controller_updated
    pool_id         VARCHAR(66)   NOT NULL DEFAULT '',  -- fee_updated only
    -- uint24, LEFT PACKED: low 12 bits = fee on 0->1, high 12 bits = fee on
    -- 1->0. The split is a v4-core encoding detail that may change; the raw
    -- value is always recoverable, a pre-split pair is not.
    protocol_fee    INTEGER       NOT NULL DEFAULT 0,
    controller      VARCHAR(42)   NOT NULL DEFAULT '',  -- controller_updated only

    block_number    BIGINT        NOT NULL,
    block_timestamp BIGINT        NOT NULL,
    tx_hash         VARCHAR(66)   NOT NULL,
    log_index       INTEGER       NOT NULL,
    origin          VARCHAR(42)   NOT NULL,
    gas_used        BIGINT,
    gas_price       NUMERIC(78,0),

    CONSTRAINT protocol_fee_event_pk PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS pfe_pool_block_idx  ON protocol_fee_event (pool_id, block_number DESC) WHERE pool_id <> '';
CREATE INDEX IF NOT EXISTS pfe_kind_block_idx  ON protocol_fee_event (kind, block_number DESC);

-- ---------------------------------------------------------------------------
-- Running totals.
--
-- Unlike pool_stats/hook_stats — which are per-block DELTAS and SUM over a
-- range — these are LIFETIME figures maintained by substreams `add`-policy
-- stores. That distinction matters: a substreams store is deterministic and
-- re-derived by the engine, so a block replayed by a parallel backfill worker
-- does not double-add. A Postgres `UPDATE ... = col + n` is not idempotent
-- under the same replay, which is why the accumulation lives in the store and
-- Postgres only ever sees the already-correct total.
--
-- One row per (entity, block) so history is queryable; take the row with the
-- greatest block_number for "current".
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pool_totals (
    id                      TEXT PRIMARY KEY,
    pool_id                 TEXT NOT NULL,
    block_number            BIGINT NOT NULL,
    token0                  TEXT,
    token1                  TEXT,
    hook_address            TEXT,
    swap_count              NUMERIC,
    modify_liquidity_count  NUMERIC,
    volume_token0_abs       NUMERIC,
    volume_token1_abs       NUMERIC
);
CREATE INDEX IF NOT EXISTS pool_totals_pool_block_idx ON pool_totals (pool_id, block_number DESC);

CREATE TABLE IF NOT EXISTS hook_totals (
    id                      TEXT PRIMARY KEY,
    hook_address            TEXT NOT NULL,
    block_number            BIGINT NOT NULL,
    pool_count              NUMERIC,
    swap_count              NUMERIC,
    modify_liquidity_count  NUMERIC,
    volume_token0_abs       NUMERIC,
    volume_token1_abs       NUMERIC
);
CREATE INDEX IF NOT EXISTS hook_totals_hook_block_idx ON hook_totals (hook_address, block_number DESC);
