import os
from typing import Any, Dict, List, Optional

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not set.")
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool


# ---------------------------------------------------------------------------
# Ranked data helpers (read-only — never modifies existing ranked tables)
# ---------------------------------------------------------------------------

RANKED_PLAYERS_TABLE = os.getenv("RANKED_PLAYERS_TABLE", "players")
RANKED_USER_ID_COLUMN = os.getenv("RANKED_USER_ID_COLUMN", "user_id")
RANKED_DATA_COLUMN = os.getenv("RANKED_DATA_COLUMN", "data")
RANKED_GUILD_ID_COLUMN = os.getenv("RANKED_GUILD_ID_COLUMN", "guild_id")


async def get_top10_players(guild_id: int) -> List[Dict[str, Any]]:
    """Get top 10 players by CR from the ranked players table."""
    pool = await get_pool()
    query = f"""
        SELECT
            {RANKED_USER_ID_COLUMN} AS user_id,
            {RANKED_DATA_COLUMN}->>'name' AS name,
            {RANKED_DATA_COLUMN}->>'username' AS username,
            {RANKED_DATA_COLUMN}->>'avatar_url' AS avatar_url,
            ({RANKED_DATA_COLUMN}->>'cr')::INTEGER AS cr,
            ({RANKED_DATA_COLUMN}->>'wins')::INTEGER AS wins,
            ({RANKED_DATA_COLUMN}->>'losses')::INTEGER AS losses
        FROM {RANKED_PLAYERS_TABLE}
        WHERE {RANKED_GUILD_ID_COLUMN} = $1
        ORDER BY COALESCE(({RANKED_DATA_COLUMN}->>'cr')::INTEGER, 0) DESC
        LIMIT 10
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, guild_id)
    return [dict(r) for r in rows]


async def get_player(guild_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    """Get a single player's ranked data by user_id."""
    pool = await get_pool()
    query = f"""
        SELECT
            {RANKED_USER_ID_COLUMN} AS user_id,
            {RANKED_DATA_COLUMN}->>'name' AS name,
            {RANKED_DATA_COLUMN}->>'username' AS username,
            {RANKED_DATA_COLUMN}->>'avatar_url' AS avatar_url,
            ({RANKED_DATA_COLUMN}->>'cr')::INTEGER AS cr
        FROM {RANKED_PLAYERS_TABLE}
        WHERE {RANKED_GUILD_ID_COLUMN} = $1
          AND {RANKED_USER_ID_COLUMN} = $2
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, guild_id, user_id)
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# StarPoints helpers
# ---------------------------------------------------------------------------

STARTING_SP = int(os.getenv("STARTING_SP", "5000"))


async def get_star_points(guild_id: int, user_id: int) -> Dict[str, Any]:
    """Get or create a user's StarPoints balance."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO market_users (guild_id, user_id, balance)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET guild_id = EXCLUDED.guild_id
            RETURNING *
            """,
            guild_id,
            user_id,
            STARTING_SP,
        )
    return dict(row)


async def update_star_points(guild_id: int, user_id: int, amount: int) -> Dict[str, Any]:
    """Add (or subtract) StarPoints from a user's balance."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE market_users
            SET balance = balance + $1
            WHERE guild_id = $2 AND user_id = $3
            RETURNING *
            """,
            amount,
            guild_id,
            user_id,
        )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Transaction helpers
# ---------------------------------------------------------------------------

async def record_transaction(
    guild_id: int,
    user_id: int,
    transaction_type: str,
    details: Dict[str, Any],
) -> Dict[str, Any]:
    """Record a market transaction."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO market_transactions
                (guild_id, investor_id, player_id, type, shares, price, total)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
            """,
            guild_id,
            user_id,
            details.get("player_id"),
            transaction_type,
            details.get("shares"),
            details.get("price"),
            details.get("total", 0),
        )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Holdings helpers
# ---------------------------------------------------------------------------

async def get_holdings(guild_id: int, user_id: int) -> List[Dict[str, Any]]:
    """Get all stock holdings for a user."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM market_holdings
            WHERE guild_id = $1 AND investor_id = $2
            ORDER BY player_id
            """,
            guild_id,
            user_id,
        )
    return [dict(r) for r in rows]


async def update_holding(
    guild_id: int,
    investor_id: int,
    player_id: int,
    shares: int,
    average_price: int,
) -> Dict[str, Any]:
    """Upsert a stock holding."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO market_holdings (guild_id, investor_id, player_id, shares, average_price)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (guild_id, investor_id, player_id) DO UPDATE SET
                shares = $4,
                average_price = $5
            RETURNING *
            """,
            guild_id,
            investor_id,
            player_id,
            shares,
            average_price,
        )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Stock price helpers
# ---------------------------------------------------------------------------

async def get_stock_price(guild_id: int, player_id: int) -> Optional[Dict[str, Any]]:
    """Get the current stock price for a player."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT * FROM market_stocks
            WHERE guild_id = $1 AND player_id = $2 AND active = true
            """,
            guild_id,
            player_id,
        )
    return dict(row) if row else None


async def update_stock_price(
    guild_id: int,
    player_id: int,
    new_price: int,
    reason: str,
) -> Dict[str, Any]:
    """Update a stock price and record the change in history."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        old_price = await conn.fetchval(
            "SELECT price FROM market_stocks WHERE guild_id = $1 AND player_id = $2",
            guild_id,
            player_id,
        )
        new_price = max(1000, int(new_price))
        await conn.execute(
            "UPDATE market_stocks SET price = $1, updated_at = NOW() WHERE guild_id = $2 AND player_id = $3",
            new_price,
            guild_id,
            player_id,
        )
        await conn.execute(
            """
            INSERT INTO market_stock_history (guild_id, player_id, old_price, new_price, reason)
            VALUES ($1, $2, $3, $4, $5)
            """,
            guild_id,
            player_id,
            int(old_price) if old_price else new_price,
            new_price,
            reason,
        )
    return {"guild_id": guild_id, "player_id": player_id, "old_price": old_price, "new_price": new_price}
