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

    block_number    BIGINT        NOT NULL,
    block_timestamp BIGINT        NOT NULL,
    tx_hash         VARCHAR(66)   NOT NULL,
    log_index       INTEGER       NOT NULL,
    origin          VARCHAR(42)   NOT NULL,   -- tx.from, the EOA
    gas_used        BIGINT,
    gas_price       NUMERIC(78,0),

    CONSTRAINT swap_pk PRIMARY KEY (id)
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
