import os
import math
import json
import asyncio
import datetime
from typing import Optional, List, Dict, Any, Tuple

import asyncpg
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
_raw_db_url = os.getenv("DATABASE_URL", "")
# Strip any Railway-style reference placeholders that were not resolved.
# A properly resolved URL starts with postgres:// or postgresql://.
if _raw_db_url.startswith("${{") or not _raw_db_url:
    DATABASE_URL: Optional[str] = None
else:
    # asyncpg requires the postgresql:// scheme; convert postgres:// if needed.
    DATABASE_URL = _raw_db_url.replace("postgres://", "postgresql://", 1) if _raw_db_url.startswith("postgres://") else _raw_db_url

DEVELOPER_USER_ID = int(os.getenv("DEVELOPER_USER_ID", "733871667788644445"))
TESTING_SERVER_ID = int(os.getenv("TESTING_SERVER_ID", "1511958538333851688"))
MAIN_SERVER_ID = int(os.getenv("MAIN_SERVER_ID", "1467697766837915804"))

STAFF_ROLE_IDS = {
    1473033115818786909,
    1473033135003533352,
    1509317165650673694,
    1486144217079091352,
    1473033493771452579,
}

# Phase 1 economy constants — $250,000 starting balance, $50,000 daily reward.
STARTING_SP = int(os.getenv("STARTING_SP", "250000"))
DAILY_SP = int(os.getenv("DAILY_SP", "50000"))
SELL_TAX = float(os.getenv("SELL_TAX", "0.03"))
MAX_OWNERSHIP_PERCENT = float(os.getenv("MAX_OWNERSHIP_PERCENT", "0.25"))
MAX_MARKET_PLAYERS = 10
TOP10_SYNC_MINUTES = int(os.getenv("TOP10_SYNC_MINUTES", "10"))

# Wealth role thresholds (balance required to earn each role).
WEALTH_ROLES: List[Tuple[str, int]] = [
    ("EAS Tycoon",        1_000_000_000),
    ("Stock Legend",        500_000_000),
    ("Market Mogul",        250_000_000),
    ("Investor Elite",      100_000_000),
    ("Multi-Millionaire",    50_000_000),
    ("Millionaire",          10_000_000),
]

# Default shop catalogue — seeded into market_shop_items on first startup.
DEFAULT_SHOP_ITEMS: List[Dict[str, Any]] = [
    {"item_name": "Bronze Investor Badge",   "description": "A bronze badge for dedicated investors.",          "price":    250_000, "category": "badge"},
    {"item_name": "Silver Investor Badge",   "description": "A silver badge for serious investors.",            "price":    750_000, "category": "badge"},
    {"item_name": "Gold Investor Badge",     "description": "A gold badge for elite investors.",                "price":  2_500_000, "category": "badge"},
    {"item_name": "Diamond Investor Badge",  "description": "A diamond badge for the top tier.",               "price": 10_000_000, "category": "badge"},
    {"item_name": "Market King Title",       "description": "Claim the Market King title.",                    "price": 25_000_000, "category": "title"},
    {"item_name": "Wall Street Title",       "description": "Claim the Wall Street title.",                    "price": 50_000_000, "category": "title"},
    {"item_name": "Bull Market Title",       "description": "Claim the Bull Market title.",                    "price": 75_000_000, "category": "title"},
    {"item_name": "Bear Market Title",       "description": "Claim the Bear Market title.",                    "price": 75_000_000, "category": "title"},
    {"item_name": "Custom Profile Color",    "description": "Unlock a custom profile color.",                  "price": 15_000_000, "category": "cosmetic"},
    {"item_name": "Animated Profile Effect", "description": "Unlock an animated profile effect.",              "price": 50_000_000, "category": "cosmetic"},
    {"item_name": "Custom Portfolio Theme",  "description": "Unlock a custom portfolio theme.",                "price": 25_000_000, "category": "cosmetic"},
    {"item_name": "Investor Streak Icon",    "description": "Show off your investor streak.",                  "price": 35_000_000, "category": "cosmetic"},
    {"item_name": "Hall of Investors Trophy","description": "The ultimate trophy for legendary investors.",    "price":100_000_000, "category": "trophy"},
]

# Ranked bot database source — auto-detected on startup; env vars used as fallback.
# Expected table format: players(guild_id, user_id, data JSON/JSONB)
# data contains fields like cr, wins, losses, kills, mvps, streak.
RANKED_PLAYERS_TABLE = os.getenv("RANKED_PLAYERS_TABLE", "players")
RANKED_USER_ID_COLUMN = os.getenv("RANKED_USER_ID_COLUMN", "user_id")
RANKED_GUILD_ID_COLUMN = os.getenv("RANKED_GUILD_ID_COLUMN", "guild_id")
RANKED_DATA_COLUMN = os.getenv("RANKED_DATA_COLUMN", "data")

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)
db_pool: Optional[asyncpg.Pool] = None
bot_start_time: Optional[datetime.datetime] = None


def money(n: int) -> str:
    return f"{int(n):,} SP"


def is_developer(user: discord.abc.User) -> bool:
    return user.id == DEVELOPER_USER_ID


def is_staff(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    return any(role.id in STAFF_ROLE_IDS for role in getattr(member, "roles", []))


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def get_stat(data: Dict[str, Any], *names: str, default: int = 0) -> int:
    for name in names:
        if name in data:
            return safe_int(data.get(name), default)
    return default


def base_price_from_ranked_data(data: Dict[str, Any], place: int) -> int:
    cr = get_stat(data, "cr", "CR", default=0)
    wins = get_stat(data, "wins", default=0)
    losses = get_stat(data, "losses", default=0)
    kills = get_stat(data, "kills", default=0)
    mvps = get_stat(data, "mvps", "mvp", "MVPs", default=0)
    streak = get_stat(data, "streak", "win_streak", default=0)

    # Strong but controlled Version 1 pricing.
    # Top 10 placement matters a lot, but CR and performance still matter.
    placement_bonus = (11 - place) * 2500
    winrate_bonus = 0
    games = wins + losses
    if games > 0:
        winrate_bonus = int((wins / games) * 8000)

    price = (
        5000
        + int(cr * 7)
        + int(wins * 120)
        + int(kills * 20)
        + int(mvps * 900)
        + int(streak * 700)
        + placement_bonus
        + winrate_bonus
        - int(losses * 80)
    )
    return max(1000, price)


def result_percent(win: bool, mvp: bool, high_kills: bool, upset: bool) -> float:
    pct = 0.0
    pct += 0.05 if win else -0.045
    if mvp:
        pct += 0.07
    if high_kills:
        pct += 0.025
    if upset and win:
        pct += 0.05
    elif upset and not win:
        pct -= 0.025
    return pct


async def detect_ranked_schema(pool: asyncpg.Pool) -> Dict[str, str]:
    """
    Auto-detect the ranked players table structure by querying information_schema.
    Returns a dict with keys: table, user_id_col, guild_id_col, data_col.
    Falls back to the env-var configured values if detection fails.
    """
    global RANKED_PLAYERS_TABLE, RANKED_USER_ID_COLUMN, RANKED_GUILD_ID_COLUMN, RANKED_DATA_COLUMN

    detected: Dict[str, str] = {
        "table": RANKED_PLAYERS_TABLE,
        "user_id_col": RANKED_USER_ID_COLUMN,
        "guild_id_col": RANKED_GUILD_ID_COLUMN,
        "data_col": RANKED_DATA_COLUMN,
    }

    try:
        async with pool.acquire() as db:
            # Step 1: find tables whose name contains 'player'.
            tables = await db.fetch("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name ILIKE '%player%'
            ORDER BY table_name;
            """)

            if not tables:
                # Broaden search — any table that might hold ranked data.
                tables = await db.fetch("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
                """)

            candidate_table: Optional[str] = None
            for t in tables:
                tname = t["table_name"]
                # Prefer tables named exactly 'players' or containing 'player'.
                if tname == "players" or "player" in tname.lower():
                    candidate_table = tname
                    break

            if not candidate_table:
                print("[Schema] No player table found; using env-var defaults.")
                return detected

            # Step 2: inspect columns of the candidate table.
            cols = await db.fetch("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position;
            """, candidate_table)

            col_names = [c["column_name"] for c in cols]
            col_types = {c["column_name"]: c["data_type"] for c in cols}

            def find_col(candidates: List[str]) -> Optional[str]:
                for candidate in candidates:
                    for col in col_names:
                        if col.lower() == candidate.lower():
                            return col
                # Partial match fallback.
                for candidate in candidates:
                    for col in col_names:
                        if candidate.lower() in col.lower():
                            return col
                return None

            user_col = find_col(["user_id", "userid", "discord_id", "member_id"])
            guild_col = find_col(["guild_id", "guildid", "server_id"])
            data_col = find_col(["data", "stats", "player_data", "ranked_data"])
            cr_col = find_col(["cr", "rank", "rating", "elo"])

            # If no JSON data column found, look for JSONB/JSON typed columns.
            if not data_col:
                for col, dtype in col_types.items():
                    if dtype in ("json", "jsonb"):
                        data_col = col
                        break

            # Verify the table is actually queryable.
            test_query = f"SELECT 1 FROM {candidate_table} LIMIT 1;"
            await db.fetchval(test_query)

            if user_col:
                detected["user_id_col"] = user_col
            if guild_col:
                detected["guild_id_col"] = guild_col
            if data_col:
                detected["data_col"] = data_col
            detected["table"] = candidate_table

            print(
                f"[Schema] Auto-detected ranked table: '{candidate_table}' | "
                f"user_id='{detected['user_id_col']}' guild_id='{detected['guild_id_col']}' "
                f"data='{detected['data_col']}'"
            )

            # Update module-level globals so the rest of the code picks them up.
            RANKED_PLAYERS_TABLE = detected["table"]
            RANKED_USER_ID_COLUMN = detected["user_id_col"]
            RANKED_GUILD_ID_COLUMN = detected["guild_id_col"]
            RANKED_DATA_COLUMN = detected["data_col"]

    except Exception as exc:
        print(f"[Schema] Auto-detection failed ({exc}); using env-var defaults.")

    return detected


async def init_db() -> None:
    """Initialise the database connection pool and create all market_ tables.

    All tables use the market_ prefix and have NO foreign key constraints
    pointing at the ranked players table, preventing FK violation crashes.
    """
    global db_pool
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing or was not resolved. "
            "Ensure the Railway variable reference is correctly linked."
        )
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        # Validate the connection immediately.
        async with db_pool.acquire() as _conn:
            await _conn.fetchval("SELECT 1")
        print("[DB] ✅ Connected to PostgreSQL successfully.")
    except Exception as exc:
        raise RuntimeError(f"Failed to connect to PostgreSQL: {exc}") from exc

    async with db_pool.acquire() as db:
        # ------------------------------------------------------------------ #
        # market_users — balances, daily claims, wealth roles.
        # No FK to players table; guild_id + user_id are standalone.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_users (
            guild_id   BIGINT NOT NULL,
            user_id    BIGINT NOT NULL,
            balance    BIGINT NOT NULL DEFAULT 250000,
            last_daily TIMESTAMP,
            wealth_role TEXT,
            PRIMARY KEY (guild_id, user_id)
        );
        """)

        # Migrate existing rows: add wealth_role column if it doesn't exist yet,
        # and drop the old frozen column if present (non-destructive ALTER).
        await db.execute("""
        DO $body$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='market_users' AND column_name='wealth_role'
            ) THEN
                ALTER TABLE market_users ADD COLUMN wealth_role TEXT;
            END IF;
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='market_users' AND column_name='frozen'
            ) THEN
                ALTER TABLE market_users DROP COLUMN frozen;
            END IF;
        END$body$;
        """)

        # ------------------------------------------------------------------ #
        # market_stocks — Top 10 player stock listings.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_stocks (
            guild_id              BIGINT NOT NULL,
            player_id             BIGINT NOT NULL,
            price                 BIGINT NOT NULL DEFAULT 1000,
            rank_position         INTEGER NOT NULL,
            active                BOOLEAN NOT NULL DEFAULT TRUE,
            cr                    INTEGER NOT NULL DEFAULT 0,
            wins                  INTEGER NOT NULL DEFAULT 0,
            losses                INTEGER NOT NULL DEFAULT 0,
            kills                 INTEGER NOT NULL DEFAULT 0,
            mvps                  INTEGER NOT NULL DEFAULT 0,
            streak                INTEGER NOT NULL DEFAULT 0,
            previous_rank_position INTEGER,
            updated_at            TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (guild_id, player_id)
        );
        """)

        # ------------------------------------------------------------------ #
        # market_holdings — user share holdings.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_holdings (
            guild_id      BIGINT NOT NULL,
            investor_id   BIGINT NOT NULL,
            player_id     BIGINT NOT NULL,
            shares        INTEGER NOT NULL DEFAULT 0,
            average_price BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, investor_id, player_id)
        );
        """)

        # ------------------------------------------------------------------ #
        # market_transactions — buy/sell/daily history.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_transactions (
            id          SERIAL PRIMARY KEY,
            guild_id    BIGINT NOT NULL,
            investor_id BIGINT NOT NULL,
            player_id   BIGINT,
            type        TEXT NOT NULL,
            shares      INTEGER,
            price       BIGINT,
            total       BIGINT,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        """)

        # ------------------------------------------------------------------ #
        # market_stock_history — price change audit log.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_stock_history (
            id         SERIAL PRIMARY KEY,
            guild_id   BIGINT NOT NULL,
            player_id  BIGINT NOT NULL,
            old_price  BIGINT NOT NULL,
            new_price  BIGINT NOT NULL,
            reason     TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # ------------------------------------------------------------------ #
        # market_settings — per-guild market open/close flag.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_settings (
            guild_id    BIGINT PRIMARY KEY,
            market_open BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at  TIMESTAMP DEFAULT NOW()
        );
        """)

        # ------------------------------------------------------------------ #
        # market_shop_items — purchasable item definitions (global catalogue).
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_shop_items (
            id          SERIAL PRIMARY KEY,
            item_name   TEXT NOT NULL UNIQUE,
            description TEXT,
            price       BIGINT NOT NULL,
            category    TEXT NOT NULL,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        """)

        # ------------------------------------------------------------------ #
        # market_user_items — items purchased by users per guild.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_user_items (
            id           SERIAL PRIMARY KEY,
            guild_id     BIGINT NOT NULL,
            user_id      BIGINT NOT NULL,
            item_id      INTEGER NOT NULL,
            purchased_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # ------------------------------------------------------------------ #
        # market_wealth_roles — wealth role assignment history.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_wealth_roles (
            id          SERIAL PRIMARY KEY,
            guild_id    BIGINT NOT NULL,
            user_id     BIGINT NOT NULL,
            role_name   TEXT NOT NULL,
            threshold   BIGINT NOT NULL,
            assigned_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # ------------------------------------------------------------------ #
        # Seed default shop items (INSERT … ON CONFLICT DO NOTHING).
        # ------------------------------------------------------------------ #
        seeded = 0
        for item in DEFAULT_SHOP_ITEMS:
            result = await db.execute("""
            INSERT INTO market_shop_items (item_name, description, price, category)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (item_name) DO NOTHING;
            """, item["item_name"], item["description"], item["price"], item["category"])
            if result == "INSERT 0 1":
                seeded += 1
        if seeded:
            print(f"[Shop] ✅ Seeded {seeded} new shop items into market_shop_items.")
        else:
            print("[Shop] ✅ Shop items already present — no seeding needed.")

    print("[DB] ✅ All market_ tables verified/created.")

    # Run schema auto-detection after tables are created.
    schema = await detect_ranked_schema(db_pool)
    print(
        f"[Schema] ✅ Ranked table: '{schema['table']}' | "
        f"user_id='{schema['user_id_col']}' guild_id='{schema['guild_id_col']}' "
        f"data='{schema['data_col']}'"
    )


async def ensure_user(guild_id: int, user_id: int) -> None:
    """Create a market_users row for this guild/user if one does not exist yet.

    New users start with STARTING_SP ($250,000) balance.
    """
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute("""
        INSERT INTO market_users (guild_id, user_id, balance)
        VALUES ($1, $2, $3)
        ON CONFLICT (guild_id, user_id) DO NOTHING;
        """, guild_id, user_id, STARTING_SP)


def compute_wealth_role(balance: int) -> Optional[str]:
    """Return the highest applicable wealth role name for the given balance.

    Roles are checked from highest threshold to lowest; the first match wins.
    Returns None if the balance is below the lowest threshold.
    """
    for role_name, threshold in WEALTH_ROLES:
        if balance >= threshold:
            return role_name
    return None


async def update_wealth_role(guild_id: int, user_id: int, balance: int) -> Optional[str]:
    """Compute and persist the wealth role for a user based on their balance.

    Updates market_users.wealth_role and inserts a record into
    market_wealth_roles when the role changes.  Returns the new role name
    (or None if below all thresholds).
    """
    assert db_pool is not None
    role_name = compute_wealth_role(balance)
    async with db_pool.acquire() as db:
        current = await db.fetchval(
            "SELECT wealth_role FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, user_id,
        )
        if current != role_name:
            await db.execute(
                "UPDATE market_users SET wealth_role=$1 WHERE guild_id=$2 AND user_id=$3;",
                role_name, guild_id, user_id,
            )
            if role_name:
                threshold = next(t for n, t in WEALTH_ROLES if n == role_name)
                await db.execute("""
                INSERT INTO market_wealth_roles (guild_id, user_id, role_name, threshold)
                VALUES ($1, $2, $3, $4);
                """, guild_id, user_id, role_name, threshold)
    return role_name


async def market_is_open(guild_id: int) -> bool:
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute("""
        INSERT INTO market_settings (guild_id, market_open)
        VALUES ($1, TRUE)
        ON CONFLICT (guild_id) DO NOTHING;
        """, guild_id)
        return bool(await db.fetchval("SELECT market_open FROM market_settings WHERE guild_id=$1;", guild_id))


async def get_stock(guild_id: int, player_id: int):
    assert db_pool is not None
    async with db_pool.acquire() as db:
        return await db.fetchrow("""
        SELECT * FROM market_stocks
        WHERE guild_id=$1 AND player_id=$2 AND active=true AND rank_position BETWEEN 1 AND 10;
        """, guild_id, player_id)


async def total_shares(guild_id: int, player_id: int) -> int:
    assert db_pool is not None
    async with db_pool.acquire() as db:
        val = await db.fetchval("""
        SELECT COALESCE(SUM(shares), 0) FROM market_holdings
        WHERE guild_id=$1 AND player_id=$2;
        """, guild_id, player_id)
        return safe_int(val)


async def update_price(guild_id: int, player_id: int, new_price: int, reason: str) -> tuple[int, int]:
    assert db_pool is not None
    async with db_pool.acquire() as db:
        old_price = await db.fetchval("""
        SELECT price FROM market_stocks WHERE guild_id=$1 AND player_id=$2;
        """, guild_id, player_id)
        if old_price is None:
            raise ValueError("Stock not found")
        new_price = max(1000, int(new_price))
        await db.execute("""
        UPDATE market_stocks SET price=$1, updated_at=NOW()
        WHERE guild_id=$2 AND player_id=$3;
        """, new_price, guild_id, player_id)
        await db.execute("""
        INSERT INTO market_stock_history (guild_id, player_id, old_price, new_price, reason)
        VALUES ($1, $2, $3, $4, $5);
        """, guild_id, player_id, int(old_price), new_price, reason)
        return int(old_price), new_price


async def sync_top10_for_guild(guild: discord.Guild) -> int:
    """
    Pull top 10 by CR from the ranked players table and update market listings.
    Only processes players belonging to this specific guild (guild_id filter).
    Players entering the Top 10 get a new stock; players leaving are marked inactive
    (all holdings and history are preserved). Price movements are smoothed with a
    70/30 weighted average and boosted/penalised based on position delta.
    """
    assert db_pool is not None
    guild_id = guild.id

    # Use module-level globals (potentially updated by detect_ranked_schema).
    # Column/table names come from config only — never from user input.
    query = f"""
    SELECT {RANKED_USER_ID_COLUMN} AS user_id, {RANKED_DATA_COLUMN} AS data
    FROM {RANKED_PLAYERS_TABLE}
    WHERE {RANKED_GUILD_ID_COLUMN}=$1
    ORDER BY COALESCE(({RANKED_DATA_COLUMN}->>'cr')::INTEGER, 0) DESC
    LIMIT 10;
    """

    async with db_pool.acquire() as db:
        try:
            ranked_rows = await db.fetch(query, guild_id)
        except Exception as exc:
            print(f"[Sync] Top 10 sync failed for guild {guild_id}: {exc}")
            return 0

        # Fetch all currently active stocks for this guild only.
        current_rows = await db.fetch("""
        SELECT player_id, price, rank_position FROM market_stocks
        WHERE guild_id=$1 AND active=true;
        """, guild_id)
        current = {int(r["player_id"]): r for r in current_rows}

        new_ids: List[int] = []
        updated_count = 0

        for index, row in enumerate(ranked_rows, start=1):
            player_id = int(row["user_id"])
            new_ids.append(player_id)
            raw_data = row["data"] or {}
            data = dict(raw_data) if not isinstance(raw_data, str) else json.loads(raw_data)

            base_price = base_price_from_ranked_data(data, index)
            old = current.get(player_id)

            wins = get_stat(data, "wins")
            losses = get_stat(data, "losses")
            kills = get_stat(data, "kills")
            mvps = get_stat(data, "mvps", "mvp", "MVPs")
            streak = get_stat(data, "streak", "win_streak")
            cr = get_stat(data, "cr", "CR")

            if old:
                # Player was already listed — apply smooth price transition.
                old_position = int(old["rank_position"])
                old_price = int(old["price"])

                # Weighted average: 70% old price + 30% new calculated price.
                blended_price = max(1000, int((old_price * 0.70) + (base_price * 0.30)))

                position_delta = old_position - index  # positive = moved up

                if position_delta > 0:
                    # Moved up: 2–3% boost per position gained.
                    boost = 0.025 * position_delta  # 2.5% per position (midpoint of 2–3%)
                    final_price = max(1000, int(blended_price * (1 + boost)))
                    reason = f"Top 10 sync: moved up from #{old_position} to #{index} (+{position_delta})"
                elif position_delta < 0:
                    # Moved down: 1–2% penalty per position lost.
                    penalty = 0.015 * abs(position_delta)  # 1.5% per position (midpoint of 1–2%)
                    final_price = max(1000, int(blended_price * (1 - penalty)))
                    reason = f"Top 10 sync: moved down from #{old_position} to #{index} ({position_delta})"
                else:
                    final_price = blended_price
                    reason = "Top 10 sync: stats updated"

                await db.execute("""
                UPDATE market_stocks
                SET price=$1, previous_rank_position=$2, rank_position=$3, active=true,
                    wins=$4, losses=$5, kills=$6, mvps=$7, streak=$8, cr=$9, updated_at=NOW()
                WHERE guild_id=$10 AND player_id=$11;
                """, final_price, old_position, index, wins, losses, kills, mvps, streak, cr, guild_id, player_id)

                if final_price != old_price:
                    await db.execute("""
                    INSERT INTO market_stock_history (guild_id, player_id, old_price, new_price, reason)
                    VALUES ($1, $2, $3, $4, $5);
                    """, guild_id, player_id, old_price, final_price, reason)
            else:
                # New entrant — create stock with placement bonus baked into base_price.
                # base_price_from_ranked_data already includes (11 - place) * 2500 bonus.
                await db.execute("""
                INSERT INTO market_stocks
                (guild_id, player_id, price, rank_position, previous_rank_position, active,
                 wins, losses, kills, mvps, streak, cr)
                VALUES ($1, $2, $3, $4, NULL, true, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (guild_id, player_id)
                DO UPDATE SET price=$3, rank_position=$4, active=true,
                    wins=$5, losses=$6, kills=$7, mvps=$8, streak=$9, cr=$10, updated_at=NOW();
                """, guild_id, player_id, base_price, index, wins, losses, kills, mvps, streak, cr)

            updated_count += 1

        # Mark players who dropped out of the Top 10 as inactive.
        # Holdings and history are preserved — only active flag changes.
        if new_ids:
            await db.execute("""
            UPDATE market_stocks SET active=false
            WHERE guild_id=$1 AND NOT (player_id = ANY($2::BIGINT[]));
            """, guild_id, new_ids)
        else:
            await db.execute("UPDATE market_stocks SET active=false WHERE guild_id=$1;", guild_id)

    return updated_count


@tasks.loop(minutes=TOP10_SYNC_MINUTES)
async def top10_sync_loop():
    # Only sync guilds the bot is actually in; guild_id filter inside the function
    # ensures testing and production data never mix.
    for guild in bot.guilds:
        await sync_top10_for_guild(guild)


@bot.event
async def on_ready():
    """Discord on_ready handler — runs once after the bot logs in.

    Initialises the database, starts the Top 10 sync loop, and syncs slash
    commands to the testing server.  Comprehensive startup logs are printed
    so Railway deployment logs are easy to read.
    """
    global bot_start_time
    bot_start_time = datetime.datetime.utcnow()

    print("=" * 60)
    print("[Startup] EAS Stock Market Bot — Phase 1")
    print(f"[Startup] Logged in as: {bot.user} (ID: {bot.user.id})")
    print(f"[Startup] Testing Server ID : {TESTING_SERVER_ID}")
    print(f"[Startup] Main Server ID    : {MAIN_SERVER_ID}")
    print(f"[Startup] Developer User ID : {DEVELOPER_USER_ID}")
    print(f"[Startup] Starting Balance  : ${STARTING_SP:,}")
    print(f"[Startup] Daily Reward      : ${DAILY_SP:,}")
    print(f"[Startup] Top 10 Sync Every : {TOP10_SYNC_MINUTES} minutes")
    print("=" * 60)

    # Initialise database — creates all market_ tables and seeds shop items.
    try:
        await init_db()
    except Exception as exc:
        print(f"[Startup] ❌ Database initialisation failed: {exc}")
        return

    # Start the background Top 10 sync loop.
    if not top10_sync_loop.is_running():
        top10_sync_loop.start()
        print(f"[Startup] ✅ Top 10 sync loop started (every {TOP10_SYNC_MINUTES} min).")
    else:
        print("[Startup] ℹ️  Top 10 sync loop already running.")

    # Sync slash commands only to the testing server during development.
    # To go live: replace with await bot.tree.sync() (global sync).
    testing_guild = discord.Object(id=TESTING_SERVER_ID)
    try:
        synced = await bot.tree.sync(guild=testing_guild)
        print(f"[Startup] ✅ Synced {len(synced)} slash command(s) to testing server {TESTING_SERVER_ID}.")
    except Exception as exc:
        print(f"[Startup] ⚠️  Command sync failed: {exc}")

    print("=" * 60)
    print(f"[Startup] ✅ EAS Stock Market Bot is READY — {bot.user}")
    print("=" * 60)


async def send_embed(interaction: discord.Interaction, title: str, description: str, color=discord.Color.blurple(), ephemeral=False):
    embed = discord.Embed(title=title, description=description, color=color)
    await interaction.response.send_message(embed=embed, ephemeral=ephemeral)


@bot.tree.command(name="ping", description="Check if the EAS Stock Market Bot is online.")
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)

    # Database connection check.
    db_status = "❌ Disconnected"
    if db_pool is not None:
        try:
            async with db_pool.acquire() as _conn:
                await _conn.fetchval("SELECT 1")
            db_status = "✅ Connected"
        except Exception:
            db_status = "⚠️ Error"

    # Uptime calculation.
    if bot_start_time is not None:
        delta = datetime.datetime.utcnow() - bot_start_time
        total_seconds = int(delta.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{hours}h {minutes}m {seconds}s"
    else:
        uptime_str = "Unknown"

    desc = (
        f"**Latency:** {latency}ms\n"
        f"**Database:** {db_status}\n"
        f"**Uptime:** {uptime_str}"
    )
    await send_embed(interaction, "🏓 Pong", desc, discord.Color.green())


@bot.tree.command(name="marketcommands", description="View EAS stock market commands by page.")
@app_commands.describe(page="Page number: 1 Economy, 2 Market, 3 Trading, 4 Staff, 5 Developer")
async def marketcommands(interaction: discord.Interaction, page: int = 1):
    """Paginated command reference for the EAS Stock Market Bot."""
    pages = {
        1: (
            "📘 Economy Commands",
            "`/ping` — Bot status, latency & uptime\n"
            "`/balance` — View your SP balance & wealth role\n"
            "`/daily` — Claim your daily $50,000 SP reward\n"
            "`/marketcommands` — This command list\n\n"
            "**Starting Balance:** $250,000 SP\n"
            "**Daily Reward:** $50,000 SP",
        ),
        2: (
            "📈 Market Commands",
            "`/market` — View the Top 10 stock market\n"
            "`/stock <player>` — View a player's stock details\n"
            "`/marketleaderboard` — Top investors by net worth\n"
            "`/shop` — Browse the investor shop\n"
            "`/portfolio` — View your holdings & P/L\n"
            "`/transactions` — Your recent trade history",
        ),
        3: (
            "💹 Trading Commands",
            "`/buy <player> <shares>` — Buy shares of a top 10 player\n"
            "`/sell <player> <shares>` — Sell shares (3% tax)\n"
            "`/topstocks` — Highest priced stocks\n"
            "`/gainers` — Recent biggest gainers\n"
            "`/losers` — Recent biggest losers",
        ),
        4: (
            "🛡️ Staff Commands",
            "`/syncmarket` — Sync top 10 from ranked database\n"
            "`/marketopen` — Open trading\n"
            "`/marketclose` — Close trading\n"
            "`/logresult` — Manually log a match result\n"
            "`/freezeportfolio` — Log a portfolio freeze\n"
            "`/unfreezeportfolio` — Log a portfolio unfreeze",
        ),
        5: (
            "👑 Developer Commands",
            "`/givepoints` — Give SP to a user\n"
            "`/takepoints` — Take SP from a user\n"
            "`/resetbalance` — Reset a user's balance\n\n"
            f"Only developer ID `{DEVELOPER_USER_ID}` can use these.",
        ),
    }
    page = max(1, min(5, page))
    title, desc = pages[page]
    embed = discord.Embed(title=title, description=desc, color=discord.Color.gold())
    embed.set_footer(text=f"Page {page}/5 • Use /marketcommands page:2 for next page")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="balance", description="View your StarPoints balance and wealth role.")
async def balance(interaction: discord.Interaction):
    """Show the caller's current balance, wealth role, and next wealth milestone."""
    await ensure_user(interaction.guild.id, interaction.user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT balance, wealth_role FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            interaction.guild.id, interaction.user.id,
        )
    bal = int(row["balance"])
    role = row["wealth_role"] or "None"

    # Determine next wealth milestone.
    next_role: Optional[str] = None
    next_threshold: Optional[int] = None
    for role_name, threshold in reversed(WEALTH_ROLES):
        if bal < threshold:
            next_role = role_name
            next_threshold = threshold

    desc = (
        f"**Balance:** {money(bal)}\n"
        f"**Wealth Role:** {role}\n"
    )
    if next_role and next_threshold:
        needed = next_threshold - bal
        desc += f"**Next Role:** {next_role} (need {money(needed)} more)"
    else:
        desc += "**Status:** 🏆 Maximum wealth role achieved!"

    embed = discord.Embed(title="⭐ StarPoints Balance", description=desc, color=discord.Color.gold())
    embed.set_footer(text=f"Guild: {interaction.guild.name}")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="daily", description="Claim your daily StarPoints reward.")
async def daily(interaction: discord.Interaction):
    """Claim the daily $50,000 SP reward (once per 24 hours)."""
    await ensure_user(interaction.guild.id, interaction.user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        can_claim = await db.fetchval("""
        SELECT last_daily IS NULL OR last_daily < NOW() - INTERVAL '24 hours'
        FROM market_users WHERE guild_id=$1 AND user_id=$2;
        """, interaction.guild.id, interaction.user.id)
        if not can_claim:
            # Calculate time remaining until next claim.
            next_claim = await db.fetchval(
                "SELECT last_daily + INTERVAL '24 hours' FROM market_users WHERE guild_id=$1 AND user_id=$2;",
                interaction.guild.id, interaction.user.id,
            )
            now = datetime.datetime.utcnow()
            if next_claim:
                remaining = next_claim.replace(tzinfo=None) - now
                total_secs = max(0, int(remaining.total_seconds()))
                hrs, rem = divmod(total_secs, 3600)
                mins, secs = divmod(rem, 60)
                time_str = f"{hrs}h {mins}m {secs}s"
            else:
                time_str = "less than 24 hours"
            await send_embed(
                interaction, "⏳ Daily Already Claimed",
                f"You already claimed today's reward.\nCome back in **{time_str}**.",
                discord.Color.orange(), True,
            )
            return
        await db.execute(
            "UPDATE market_users SET balance=balance+$1, last_daily=NOW() WHERE guild_id=$2 AND user_id=$3;",
            DAILY_SP, interaction.guild.id, interaction.user.id,
        )
        new_balance = await db.fetchval(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            interaction.guild.id, interaction.user.id,
        )
        await db.execute(
            "INSERT INTO market_transactions (guild_id, investor_id, type, total) VALUES ($1,$2,'daily',$3);",
            interaction.guild.id, interaction.user.id, DAILY_SP,
        )
    # Update wealth role after balance change.
    new_role = await update_wealth_role(interaction.guild.id, interaction.user.id, int(new_balance))
    desc = f"You received **{money(DAILY_SP)}**!\n**New Balance:** {money(int(new_balance))}"
    if new_role:
        desc += f"\n**Wealth Role:** {new_role}"
    await send_embed(interaction, "⭐ Daily Reward Claimed", desc, discord.Color.green())


@bot.tree.command(name="market", description="View the EAS Top 10 stock market.")
async def market(interaction: discord.Interaction):
    await sync_top10_for_guild(interaction.guild)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        rows = await db.fetch("SELECT * FROM market_stocks WHERE guild_id=$1 AND active=true ORDER BY rank_position ASC LIMIT 10;", interaction.guild.id)
    if not rows:
        await send_embed(interaction, "📈 EAS Exchange", "No top 10 stocks found yet. Staff can run `/syncmarket` after the ranked database is connected.", discord.Color.red())
        return
    desc = ""
    for r in rows:
        member = interaction.guild.get_member(int(r["player_id"]))
        name = member.display_name if member else f"User {r['player_id']}"
        desc += f"**#{r['rank_position']} {name}** — `{money(r['price'])}` | CR: `{r['cr']:,}`\n"
    await send_embed(interaction, "📈 EAS Exchange — Top 10 Market", desc, discord.Color.green())


@bot.tree.command(name="stock", description="View a top 10 player's stock.")
async def stock(interaction: discord.Interaction, user: discord.Member):
    row = await get_stock(interaction.guild.id, user.id)
    if not row:
        await send_embed(interaction, "❌ Not Listed", "That player is not currently in the top 10 market.", discord.Color.red(), True)
        return
    assert db_pool is not None
    async with db_pool.acquire() as db:
        hist = await db.fetch("SELECT old_price,new_price,reason FROM market_stock_history WHERE guild_id=$1 AND player_id=$2 ORDER BY created_at DESC LIMIT 5;", interaction.guild.id, user.id)
    movement = "No recent movement."
    if hist:
        lines = []
        for h in hist:
            diff = h["new_price"] - h["old_price"]
            sign = "+" if diff >= 0 else ""
            lines.append(f"`{sign}{diff:,} SP` — {h['reason']}")
        movement = "\n".join(lines)
    desc = f"**Price:** {money(row['price'])}\n**Top 10 Place:** #{row['rank_position']}\n**CR:** {row['cr']:,}\n**Wins:** {row['wins']}\n**Losses:** {row['losses']}\n**Kills:** {row['kills']}\n**MVPs:** {row['mvps']}\n**Streak:** {row['streak']}\n\n**Recent Movement**\n{movement}"
    await send_embed(interaction, f"📊 {user.display_name} Stock", desc, discord.Color.blue())


@bot.tree.command(name="buy", description="Buy shares of a top 10 player.")
@app_commands.describe(user="The top 10 player whose stock you want to buy", shares="Number of shares to purchase")
async def buy(interaction: discord.Interaction, user: discord.Member, shares: int):
    """Purchase shares of a top 10 player's stock."""
    if not await market_is_open(interaction.guild.id):
        await send_embed(interaction, "🔒 Market Closed", "Trading is currently closed.", discord.Color.red(), True); return
    if shares <= 0:
        await send_embed(interaction, "❌ Invalid Shares", "Shares must be more than 0.", discord.Color.red(), True); return
    if user.id == interaction.user.id:
        await send_embed(interaction, "❌ Not Allowed", "You cannot buy your own stock.", discord.Color.red(), True); return
    stock_row = await get_stock(interaction.guild.id, user.id)
    if not stock_row:
        await send_embed(interaction, "❌ Not Listed", "That player is not in the top 10 market.", discord.Color.red(), True); return
    await ensure_user(interaction.guild.id, interaction.user.id)
    assert db_pool is not None
    total_cost = int(stock_row["price"]) * shares
    async with db_pool.acquire() as db:
        u = await db.fetchrow(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            interaction.guild.id, interaction.user.id,
        )
        if u["balance"] < total_cost:
            await send_embed(interaction, "❌ Not Enough SP", f"You need **{money(total_cost)}** but have **{money(u['balance'])}**.", discord.Color.red(), True); return
        current_total = await total_shares(interaction.guild.id, user.id)
        current_owned = safe_int(await db.fetchval(
            "SELECT shares FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;",
            interaction.guild.id, interaction.user.id, user.id,
        ))
        if (current_owned + shares) / max(1, current_total + shares) > MAX_OWNERSHIP_PERCENT:
            await send_embed(interaction, "❌ Ownership Limit", "You cannot own more than 25% of one player's market shares.", discord.Color.red(), True); return
        old = await db.fetchrow(
            "SELECT shares,average_price FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;",
            interaction.guild.id, interaction.user.id, user.id,
        )
        if old:
            new_shares = old["shares"] + shares
            new_avg = math.floor(((old["shares"] * old["average_price"]) + total_cost) / new_shares)
            await db.execute(
                "UPDATE market_holdings SET shares=$1, average_price=$2 WHERE guild_id=$3 AND investor_id=$4 AND player_id=$5;",
                new_shares, new_avg, interaction.guild.id, interaction.user.id, user.id,
            )
        else:
            await db.execute(
                "INSERT INTO market_holdings (guild_id,investor_id,player_id,shares,average_price) VALUES ($1,$2,$3,$4,$5);",
                interaction.guild.id, interaction.user.id, user.id, shares, stock_row["price"],
            )
        await db.execute(
            "UPDATE market_users SET balance=balance-$1 WHERE guild_id=$2 AND user_id=$3;",
            total_cost, interaction.guild.id, interaction.user.id,
        )
        new_balance = await db.fetchval(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            interaction.guild.id, interaction.user.id,
        )
        await db.execute(
            "INSERT INTO market_transactions (guild_id,investor_id,player_id,type,shares,price,total) VALUES ($1,$2,$3,'buy',$4,$5,$6);",
            interaction.guild.id, interaction.user.id, user.id, shares, stock_row["price"], total_cost,
        )
    await update_wealth_role(interaction.guild.id, interaction.user.id, int(new_balance))
    await send_embed(
        interaction, "✅ Shares Purchased",
        f"Bought **{shares} shares** of **{user.display_name}** for **{money(total_cost)}**.\n**Remaining Balance:** {money(int(new_balance))}",
        discord.Color.green(),
    )


@bot.tree.command(name="sell", description="Sell shares of a player stock.")
@app_commands.describe(user="The player whose shares you want to sell", shares="Number of shares to sell")
async def sell(interaction: discord.Interaction, user: discord.Member, shares: int):
    """Sell shares of a top 10 player's stock (3% sell tax applies)."""
    if not await market_is_open(interaction.guild.id):
        await send_embed(interaction, "🔒 Market Closed", "Trading is currently closed.", discord.Color.red(), True); return
    if shares <= 0:
        await send_embed(interaction, "❌ Invalid Shares", "Shares must be more than 0.", discord.Color.red(), True); return
    stock_row = await get_stock(interaction.guild.id, user.id)
    if not stock_row:
        await send_embed(interaction, "❌ Not Listed", "That player is not in the top 10 market.", discord.Color.red(), True); return
    await ensure_user(interaction.guild.id, interaction.user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        h = await db.fetchrow(
            "SELECT shares FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;",
            interaction.guild.id, interaction.user.id, user.id,
        )
        if not h or h["shares"] < shares:
            await send_embed(interaction, "❌ Not Enough Shares", f"You only own **{h['shares'] if h else 0} shares** of {user.display_name}.", discord.Color.red(), True); return
        gross = stock_row["price"] * shares
        tax = math.floor(gross * SELL_TAX)
        net = gross - tax
        remaining = h["shares"] - shares
        if remaining <= 0:
            await db.execute(
                "DELETE FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;",
                interaction.guild.id, interaction.user.id, user.id,
            )
        else:
            await db.execute(
                "UPDATE market_holdings SET shares=$1 WHERE guild_id=$2 AND investor_id=$3 AND player_id=$4;",
                remaining, interaction.guild.id, interaction.user.id, user.id,
            )
        await db.execute(
            "UPDATE market_users SET balance=balance+$1 WHERE guild_id=$2 AND user_id=$3;",
            net, interaction.guild.id, interaction.user.id,
        )
        new_balance = await db.fetchval(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            interaction.guild.id, interaction.user.id,
        )
        await db.execute(
            "INSERT INTO market_transactions (guild_id,investor_id,player_id,type,shares,price,total) VALUES ($1,$2,$3,'sell',$4,$5,$6);",
            interaction.guild.id, interaction.user.id, user.id, shares, stock_row["price"], net,
        )
    await update_wealth_role(interaction.guild.id, interaction.user.id, int(new_balance))
    await send_embed(
        interaction, "✅ Shares Sold",
        f"Sold **{shares} shares** of **{user.display_name}** for **{money(net)}** after {int(SELL_TAX*100)}% tax.\n**New Balance:** {money(int(new_balance))}",
        discord.Color.green(),
    )


@bot.tree.command(name="portfolio", description="View your stock portfolio.")
async def portfolio(interaction: discord.Interaction):
    await ensure_user(interaction.guild.id, interaction.user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        balance = await db.fetchval("SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;", interaction.guild.id, interaction.user.id)
        rows = await db.fetch("""
        SELECT h.player_id, h.shares, h.average_price, s.price
        FROM market_holdings h
        JOIN market_stocks s ON h.guild_id=s.guild_id AND h.player_id=s.player_id
        WHERE h.guild_id=$1 AND h.investor_id=$2 AND s.active=true
        ORDER BY h.shares * s.price DESC;
        """, interaction.guild.id, interaction.user.id)
    if not rows:
        await send_embed(interaction, "📁 Portfolio", f"Wallet: **{money(balance)}**\nYou do not own any shares yet.", discord.Color.purple())
        return
    desc = ""
    value_total = 0
    for r in rows:
        member = interaction.guild.get_member(int(r["player_id"]))
        name = member.display_name if member else f"User {r['player_id']}"
        value = int(r["shares"] * r["price"])
        cost = int(r["shares"] * r["average_price"])
        pl = value - cost
        sign = "+" if pl >= 0 else ""
        value_total += value
        desc += f"**{name}** — {r['shares']} shares\nValue: `{money(value)}` | P/L: `{sign}{pl:,} SP`\n\n"
    embed = discord.Embed(title=f"📁 {interaction.user.display_name}'s Portfolio", description=desc[:3900], color=discord.Color.purple())
    embed.add_field(name="Wallet", value=money(balance))
    embed.add_field(name="Portfolio", value=money(value_total))
    embed.add_field(name="Net Worth", value=money(balance + value_total))
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="marketleaderboard", description="View the top investors by total net worth.")
async def marketleaderboard(interaction: discord.Interaction):
    """Show the top 10 investors ranked by balance + portfolio value."""
    assert db_pool is not None
    async with db_pool.acquire() as db:
        rows = await db.fetch("""
        SELECT u.user_id, u.wealth_role,
               u.balance + COALESCE(SUM(h.shares * s.price), 0) AS net_worth
        FROM market_users u
        LEFT JOIN market_holdings h ON u.guild_id=h.guild_id AND u.user_id=h.investor_id
        LEFT JOIN market_stocks s ON h.guild_id=s.guild_id AND h.player_id=s.player_id AND s.active=true
        WHERE u.guild_id=$1
        GROUP BY u.user_id, u.balance, u.wealth_role
        ORDER BY net_worth DESC
        LIMIT 10;
        """, interaction.guild.id)
    if not rows:
        await send_embed(interaction, "🏦 Market Leaderboard", "No investors yet.", discord.Color.gold())
        return
    desc = ""
    medals = ["🥇", "🥈", "🥉"]
    for i, r in enumerate(rows, start=1):
        member = interaction.guild.get_member(int(r["user_id"]))
        name = member.display_name if member else f"User {r['user_id']}"
        medal = medals[i - 1] if i <= 3 else f"**#{i}**"
        role_tag = f" _{r['wealth_role']}_" if r["wealth_role"] else ""
        desc += f"{medal} **{name}**{role_tag} — `{money(r['net_worth'])}`\n"
    embed = discord.Embed(title="🏦 Market Leaderboard — Top Investors", description=desc, color=discord.Color.gold())
    embed.set_footer(text="Ranked by balance + portfolio value")
    await interaction.response.send_message(embed=embed)



@bot.tree.command(name="shop", description="Browse the EAS investor shop.")
@app_commands.describe(page="Page number (each page shows 5 items)")
async def shop(interaction: discord.Interaction, page: int = 1):
    """Display the shop catalogue with purchasable items and badges.

    Items are loaded from market_shop_items.  Users can buy items with /buy
    (future Phase 2 command); this command is read-only browsing.
    """
    assert db_pool is not None
    async with db_pool.acquire() as db:
        total_items = await db.fetchval("SELECT COUNT(*) FROM market_shop_items;")
        items_per_page = 5
        total_pages = max(1, math.ceil(total_items / items_per_page))
        page = max(1, min(page, total_pages))
        offset = (page - 1) * items_per_page
        rows = await db.fetch(
            "SELECT * FROM market_shop_items ORDER BY price ASC LIMIT $1 OFFSET $2;",
            items_per_page, offset,
        )

    if not rows:
        await send_embed(interaction, "🛒 EAS Investor Shop", "The shop is currently empty.", discord.Color.blue())
        return

    # Group items by category for display.
    category_icons = {"badge": "🏅", "title": "📛", "cosmetic": "🎨", "trophy": "🏆"}
    desc = ""
    for r in rows:
        icon = category_icons.get(r["category"], "🛍️")
        desc += f"{icon} **{r['item_name']}** — `{money(r['price'])}`\n"
        if r["description"]:
            desc += f"  _{r['description']}_\n"
        desc += "\n"

    embed = discord.Embed(
        title="🛒 EAS Investor Shop",
        description=desc.strip(),
        color=discord.Color.blue(),
    )
    embed.set_footer(text=f"Page {page}/{total_pages} • Use /shop page:{page+1} for more")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="topstocks", description="View highest priced top 10 stocks.")
async def topstocks(interaction: discord.Interaction):
    assert db_pool is not None
    async with db_pool.acquire() as db:
        rows = await db.fetch("SELECT * FROM market_stocks WHERE guild_id=$1 AND active=true ORDER BY price DESC LIMIT 10;", interaction.guild.id)
    desc = ""
    for i, r in enumerate(rows, start=1):
        member = interaction.guild.get_member(int(r["player_id"]))
        name = member.display_name if member else f"User {r['player_id']}"
        desc += f"**#{i} {name}** — `{money(r['price'])}` | Top 10 Place: `{r['rank_position']}`\n"
    await send_embed(interaction, "💹 Top Stocks", desc or "No stocks listed.", discord.Color.green())


async def movement_board(guild_id: int, positive: bool):
    assert db_pool is not None
    op = ">" if positive else "<"
    order = "DESC" if positive else "ASC"
    async with db_pool.acquire() as db:
        return await db.fetch(f"""
        SELECT player_id, old_price, new_price, reason
        FROM market_stock_history
        WHERE guild_id=$1 AND new_price - old_price {op} 0
        ORDER BY (new_price - old_price) {order}, created_at DESC
        LIMIT 10;
        """, guild_id)


@bot.tree.command(name="gainers", description="View recent biggest gaining stocks.")
async def gainers(interaction: discord.Interaction):
    rows = await movement_board(interaction.guild.id, True)
    desc = ""
    for r in rows:
        member = interaction.guild.get_member(int(r["player_id"]))
        name = member.display_name if member else f"User {r['player_id']}"
        desc += f"**{name}** `{r['new_price'] - r['old_price']:+,} SP` — {r['reason']}\n"
    await send_embed(interaction, "📈 Biggest Gainers", desc or "No gainers yet.", discord.Color.green())


@bot.tree.command(name="losers", description="View recent biggest losing stocks.")
async def losers(interaction: discord.Interaction):
    rows = await movement_board(interaction.guild.id, False)
    desc = ""
    for r in rows:
        member = interaction.guild.get_member(int(r["player_id"]))
        name = member.display_name if member else f"User {r['player_id']}"
        desc += f"**{name}** `{r['new_price'] - r['old_price']:+,} SP` — {r['reason']}\n"
    await send_embed(interaction, "📉 Biggest Losers", desc or "No losers yet.", discord.Color.red())


@bot.tree.command(name="transactions", description="View your recent market transactions.")
async def transactions(interaction: discord.Interaction):
    assert db_pool is not None
    async with db_pool.acquire() as db:
        rows = await db.fetch("""
        SELECT * FROM market_transactions
        WHERE guild_id=$1 AND investor_id=$2
        ORDER BY created_at DESC LIMIT 10;
        """, interaction.guild.id, interaction.user.id)
    desc = ""
    for r in rows:
        target = ""
        if r["player_id"]:
            member = interaction.guild.get_member(int(r["player_id"]))
            target = f" {member.display_name if member else r['player_id']}"
        shares = f" x{r['shares']}" if r["shares"] else ""
        total = money(r["total"] or 0)
        desc += f"`{r['type']}`{target}{shares} — **{total}**\n"
    await send_embed(interaction, "🧾 Recent Transactions", desc or "No transactions yet.", discord.Color.blurple())


@bot.tree.command(name="syncmarket", description="Staff: Sync top 10 market players from ranked database.")
async def syncmarket(interaction: discord.Interaction):
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    count = await sync_top10_for_guild(interaction.guild)
    await send_embed(interaction, "✅ Market Synced", f"Updated **{count}** top 10 market stocks from the ranked database.", discord.Color.green())


@bot.tree.command(name="marketopen", description="Staff: Open market trading.")
async def marketopen(interaction: discord.Interaction):
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute("INSERT INTO market_settings (guild_id, market_open) VALUES ($1,true) ON CONFLICT (guild_id) DO UPDATE SET market_open=true, updated_at=NOW();", interaction.guild.id)
    await send_embed(interaction, "🔓 Market Open", "Trading is now open.", discord.Color.green())


@bot.tree.command(name="marketclose", description="Staff: Close market trading.")
async def marketclose(interaction: discord.Interaction):
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute("INSERT INTO market_settings (guild_id, market_open) VALUES ($1,false) ON CONFLICT (guild_id) DO UPDATE SET market_open=false, updated_at=NOW();", interaction.guild.id)
    await send_embed(interaction, "🔒 Market Closed", "Trading is now closed.", discord.Color.red())


@bot.tree.command(name="logresult", description="Staff: Manually update stock from ranked performance.")
@app_commands.describe(result="win or loss")
async def logresult(interaction: discord.Interaction, user: discord.Member, result: str, mvp: bool = False, high_kills: bool = False, upset: bool = False):
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    result = result.lower().strip()
    if result not in {"win", "loss"}:
        await send_embed(interaction, "❌ Invalid Result", "Use `win` or `loss`.", discord.Color.red(), True); return
    stock_row = await get_stock(interaction.guild.id, user.id)
    if not stock_row:
        await send_embed(interaction, "❌ Not Listed", "That player is not in the top 10 market.", discord.Color.red(), True); return
    pct = result_percent(result == "win", mvp, high_kills, upset)
    old = int(stock_row["price"])
    new = max(1000, math.floor(old * (1 + pct)))
    reason = result.upper()
    if mvp: reason += ", MVP"
    if high_kills: reason += ", High Kills"
    if upset: reason += ", Upset"
    old_price, new_price = await update_price(interaction.guild.id, user.id, new, reason)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        if result == "win":
            await db.execute("UPDATE market_stocks SET wins=wins+1, streak=streak+1, mvps=mvps+$1 WHERE guild_id=$2 AND player_id=$3;", 1 if mvp else 0, interaction.guild.id, user.id)
        else:
            await db.execute("UPDATE market_stocks SET losses=losses+1, streak=0, mvps=mvps+$1 WHERE guild_id=$2 AND player_id=$3;", 1 if mvp else 0, interaction.guild.id, user.id)
    diff = new_price - old_price
    await send_embed(interaction, "📊 Stock Updated", f"**{user.display_name}**\nOld: `{money(old_price)}`\nNew: `{money(new_price)}`\nChange: `{diff:+,} SP`\nReason: **{reason}**", discord.Color.green() if diff >= 0 else discord.Color.red())


@bot.tree.command(name="freezeportfolio", description="Staff: Record a portfolio freeze action for a user.")
async def freezeportfolio(interaction: discord.Interaction, user: discord.Member):
    """Log a portfolio freeze action in the transaction history."""
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    await ensure_user(interaction.guild.id, user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute(
            "INSERT INTO market_transactions (guild_id, investor_id, type, total) VALUES ($1,$2,'staff_freeze',0);",
            interaction.guild.id, user.id,
        )
    await send_embed(interaction, "🧊 Portfolio Freeze Logged", f"{user.mention}'s portfolio freeze has been recorded.", discord.Color.blue())


@bot.tree.command(name="unfreezeportfolio", description="Staff: Record a portfolio unfreeze action for a user.")
async def unfreezeportfolio(interaction: discord.Interaction, user: discord.Member):
    """Log a portfolio unfreeze action in the transaction history."""
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    await ensure_user(interaction.guild.id, user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute(
            "INSERT INTO market_transactions (guild_id, investor_id, type, total) VALUES ($1,$2,'staff_unfreeze',0);",
            interaction.guild.id, user.id,
        )
    await send_embed(interaction, "✅ Portfolio Unfreeze Logged", f"{user.mention}'s portfolio unfreeze has been recorded.", discord.Color.green())


@bot.tree.command(name="givepoints", description="Developer: Give StarPoints to a user.")
async def givepoints(interaction: discord.Interaction, user: discord.Member, amount: int):
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True); return
    if amount <= 0:
        await send_embed(interaction, "❌ Invalid Amount", "Amount must be more than 0.", discord.Color.red(), True); return
    await ensure_user(interaction.guild.id, user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute("UPDATE market_users SET balance=balance+$1 WHERE guild_id=$2 AND user_id=$3;", amount, interaction.guild.id, user.id)
        await db.execute("INSERT INTO market_transactions (guild_id,investor_id,type,total) VALUES ($1,$2,'developer_give',$3);", interaction.guild.id, user.id, amount)
    await send_embed(interaction, "✅ StarPoints Given", f"Gave **{money(amount)}** to {user.mention}.", discord.Color.green())


@bot.tree.command(name="takepoints", description="Developer: Take StarPoints from a user.")
async def takepoints(interaction: discord.Interaction, user: discord.Member, amount: int):
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True); return
    if amount <= 0:
        await send_embed(interaction, "❌ Invalid Amount", "Amount must be more than 0.", discord.Color.red(), True); return
    await ensure_user(interaction.guild.id, user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute("UPDATE market_users SET balance=GREATEST(balance-$1,0) WHERE guild_id=$2 AND user_id=$3;", amount, interaction.guild.id, user.id)
        await db.execute("INSERT INTO market_transactions (guild_id,investor_id,type,total) VALUES ($1,$2,'developer_take',$3);", interaction.guild.id, user.id, amount)
    await send_embed(interaction, "✅ StarPoints Taken", f"Took **{money(amount)}** from {user.mention}.", discord.Color.orange())


@bot.tree.command(name="resetbalance", description="Developer: Reset a user's StarPoints balance.")
async def resetbalance(interaction: discord.Interaction, user: discord.Member, amount: int = STARTING_SP):
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True); return
    if amount < 0:
        await send_embed(interaction, "❌ Invalid Amount", "Amount cannot be negative.", discord.Color.red(), True); return
    await ensure_user(interaction.guild.id, user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute("UPDATE market_users SET balance=$1 WHERE guild_id=$2 AND user_id=$3;", amount, interaction.guild.id, user.id)
        await db.execute("INSERT INTO market_transactions (guild_id,investor_id,type,total) VALUES ($1,$2,'developer_reset',$3);", interaction.guild.id, user.id, amount)
    await send_embed(interaction, "✅ Balance Reset", f"Set {user.mention}'s balance to **{money(amount)}**.", discord.Color.green())


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN is missing.")
    bot.run(DISCORD_TOKEN)
