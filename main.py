import os
import math
import json
import random
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

def _resolve_db_url(raw: str) -> Optional[str]:
    """Resolve and normalise a database URL, returning None if unset/unresolved."""
    if not raw or raw.startswith("${{"):
        return None
    return raw.replace("postgres://", "postgresql://", 1) if raw.startswith("postgres://") else raw

DATABASE_URL: Optional[str] = _resolve_db_url(os.getenv("DATABASE_URL", ""))
TEST_DATABASE_URL: Optional[str] = _resolve_db_url(os.getenv("TEST_DATABASE_URL", ""))

DEVELOPER_USER_ID = int(os.getenv("DEVELOPER_ID", os.getenv("DEVELOPER_USER_ID", "733871667788644445")))
TEST_GUILD_ID = int(os.getenv("TEST_GUILD_ID", os.getenv("TESTING_SERVER_ID", "1511958538333851688")))
MAIN_GUILD_ID = int(os.getenv("MAIN_GUILD_ID", os.getenv("MAIN_SERVER_ID", "1467697766837915804")))

# Keep legacy names as aliases for any code that still references them.
TESTING_SERVER_ID = TEST_GUILD_ID
MAIN_SERVER_ID = MAIN_GUILD_ID

# ---------------------------------------------------------------------------
# Fake players for test-server simulation
# ---------------------------------------------------------------------------
FAKE_PLAYERS: List[Dict[str, Any]] = [
    {"user_id": 9000000001, "name": "Test Player 1",  "cr": 12500, "wins": 450, "losses": 150, "kills": 8900, "mvps": 280, "streak": 15},
    {"user_id": 9000000002, "name": "Test Player 2",  "cr": 11800, "wins": 420, "losses": 180, "kills": 8200, "mvps": 260, "streak": 12},
    {"user_id": 9000000003, "name": "Test Player 3",  "cr": 11200, "wins": 400, "losses": 200, "kills": 7800, "mvps": 240, "streak": 10},
    {"user_id": 9000000004, "name": "Test Player 4",  "cr": 10600, "wins": 380, "losses": 220, "kills": 7400, "mvps": 220, "streak":  8},
    {"user_id": 9000000005, "name": "Test Player 5",  "cr": 10000, "wins": 360, "losses": 240, "kills": 7000, "mvps": 200, "streak":  6},
    {"user_id": 9000000006, "name": "Test Player 6",  "cr":  9400, "wins": 340, "losses": 260, "kills": 6600, "mvps": 180, "streak":  5},
    {"user_id": 9000000007, "name": "Test Player 7",  "cr":  8800, "wins": 320, "losses": 280, "kills": 6200, "mvps": 160, "streak":  4},
    {"user_id": 9000000008, "name": "Test Player 8",  "cr":  8200, "wins": 300, "losses": 300, "kills": 5800, "mvps": 140, "streak":  3},
    {"user_id": 9000000009, "name": "Test Player 9",  "cr":  7600, "wins": 280, "losses": 320, "kills": 5400, "mvps": 120, "streak":  2},
    {"user_id": 9000000010, "name": "Test Player 10", "cr":  7000, "wins": 260, "losses": 340, "kills": 5000, "mvps": 100, "streak":  1},
]

# ---------------------------------------------------------------------------
# DatabaseManager — dual-pool routing
# ---------------------------------------------------------------------------

class DatabaseManager:
    """Manages two asyncpg connection pools: one for main, one for test."""

    def __init__(self) -> None:
        self.db_pool_main: Optional[asyncpg.Pool] = None
        self.db_pool_test: Optional[asyncpg.Pool] = None

    async def get_pool(self, guild_id: int) -> asyncpg.Pool:
        """Return the appropriate connection pool for this guild."""
        if guild_id == MAIN_GUILD_ID:
            if self.db_pool_main is None:
                raise RuntimeError("Main database pool is not initialised.")
            return self.db_pool_main
        elif guild_id == TEST_GUILD_ID:
            if self.db_pool_test is None:
                raise RuntimeError("Test database pool is not initialised.")
            return self.db_pool_test
        else:
            raise ValueError(f"Unknown guild_id: {guild_id}. Only main ({MAIN_GUILD_ID}) and test ({TEST_GUILD_ID}) servers are supported.")

    async def init_pools(self) -> None:
        """Initialise both connection pools."""
        if DATABASE_URL:
            try:
                self.db_pool_main = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
                async with self.db_pool_main.acquire() as _conn:
                    await _conn.fetchval("SELECT 1")
                print("[DB] ✅ Main Database Connected (DATABASE_URL)")
            except Exception as exc:
                print(f"[DB] ❌ Main Database connection failed: {exc}")
                self.db_pool_main = None
        else:
            print("[DB] ⚠️  DATABASE_URL not set — main database unavailable.")

        if TEST_DATABASE_URL:
            try:
                self.db_pool_test = await asyncpg.create_pool(TEST_DATABASE_URL, min_size=1, max_size=5)
                async with self.db_pool_test.acquire() as _conn:
                    await _conn.fetchval("SELECT 1")
                print("[DB] ✅ Test Database Connected (TEST_DATABASE_URL)")
            except Exception as exc:
                print(f"[DB] ❌ Test Database connection failed: {exc}")
                self.db_pool_test = None
        else:
            print("[DB] ⚠️  TEST_DATABASE_URL not set — test database unavailable.")


db_manager = DatabaseManager()

# ---------------------------------------------------------------------------
# Guild validation helpers
# ---------------------------------------------------------------------------

def validate_guild(guild_id: int) -> bool:
    """Return True if guild_id is the testing or main server."""
    return guild_id in (TEST_GUILD_ID, MAIN_GUILD_ID)


def get_mode(guild_id: int) -> str:
    """Return 'test' or 'main' based on guild_id."""
    if guild_id == TEST_GUILD_ID:
        return "test"
    elif guild_id == MAIN_GUILD_ID:
        return "main"
    return "unknown"


def mode_label(guild_id: int) -> str:
    """Return a human-readable mode label for embeds."""
    return "🧪 TEST MODE" if guild_id == TEST_GUILD_ID else "🔴 LIVE MODE"

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

TERMS_AND_CONDITIONS = """
**EAS STOCK MARKET — TERMS & CONDITIONS**

By registering for the EAS Stock Market, you agree to the following terms:

**1. PARTICIPATION**
You acknowledge that you are voluntarily participating in the EAS Stock Market economy. This is a simulated market for entertainment purposes only.

**2. NO REAL VALUE**
StarPoints (SP) have no real-world monetary value. They cannot be converted to real currency, traded outside this bot, or used for any real-world transactions.

**3. MARKET VOLATILITY**
Stock prices fluctuate based on player performance. You understand that:
- Prices can increase or decrease at any time
- Your investments may lose value
- Past performance does not guarantee future results
- Market conditions can change rapidly

**4. ACCOUNT RESPONSIBILITY**
You are responsible for:
- Keeping your Discord account secure
- All transactions made on your account
- Compliance with Discord Terms of Service
- Following all server rules and guidelines

**5. STAFF AUTHORITY**
Server staff and administrators have the authority to:
- Suspend or ban users from the market
- Reverse transactions if fraud is detected
- Modify market rules with notice
- Enforce compliance with these terms

**6. NO GUARANTEES**
The market is provided "as-is" without warranties. We do not guarantee:
- Continuous availability
- Data preservation
- Specific market conditions
- Compensation for losses

**7. DISPUTE RESOLUTION**
Disputes will be resolved by server staff. Their decision is final.

**8. RULE CHANGES**
These terms may be updated at any time. Continued participation constitutes acceptance of new terms.

**9. COMPLIANCE**
You agree to comply with all Discord Terms of Service and server rules while using the market.

By clicking **Accept**, you acknowledge that you have read, understood, and agree to these terms.
"""

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
# Legacy single-pool reference — kept for backward compatibility with helper
# functions that haven't been migrated to pool-parameter style yet.
db_pool: Optional[asyncpg.Pool] = None
bot_start_time: Optional[datetime.datetime] = None

# Startup-once flags — prevent re-running seeding/registration on reconnects.
_shop_seeded: bool = False
_commands_registered: bool = False
_badges_registered: bool = False


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


async def _create_market_tables(pool: asyncpg.Pool, mode: str) -> None:
    """Create all market_ tables in the given pool.

    mode is 'main' or 'test' — used only for logging.
    """
    async with pool.acquire() as db:
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
        # Extended with rarity, stock, resale, badge/role linkage, and audit.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_shop_items (
            id              SERIAL PRIMARY KEY,
            item_name       TEXT NOT NULL UNIQUE,
            description     TEXT,
            price           BIGINT NOT NULL,
            category        TEXT NOT NULL,
            rarity          TEXT NOT NULL DEFAULT 'common',
            active          BOOLEAN NOT NULL DEFAULT TRUE,
            limited         BOOLEAN NOT NULL DEFAULT FALSE,
            max_stock       INTEGER,
            current_stock   INTEGER,
            min_value       BIGINT,
            max_value       BIGINT,
            resale_percent  INTEGER NOT NULL DEFAULT 70,
            badge_id        TEXT,
            role_id         BIGINT,
            created_at      TIMESTAMP DEFAULT NOW(),
            updated_at      TIMESTAMP DEFAULT NOW()
        );
        """)

        # Non-destructive migrations for existing market_shop_items rows.
        await db.execute("""
        DO $body$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='rarity') THEN
                ALTER TABLE market_shop_items ADD COLUMN rarity TEXT NOT NULL DEFAULT 'common';
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='active') THEN
                ALTER TABLE market_shop_items ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='limited') THEN
                ALTER TABLE market_shop_items ADD COLUMN limited BOOLEAN NOT NULL DEFAULT FALSE;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='max_stock') THEN
                ALTER TABLE market_shop_items ADD COLUMN max_stock INTEGER;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='current_stock') THEN
                ALTER TABLE market_shop_items ADD COLUMN current_stock INTEGER;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='min_value') THEN
                ALTER TABLE market_shop_items ADD COLUMN min_value BIGINT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='max_value') THEN
                ALTER TABLE market_shop_items ADD COLUMN max_value BIGINT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='resale_percent') THEN
                ALTER TABLE market_shop_items ADD COLUMN resale_percent INTEGER NOT NULL DEFAULT 70;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='badge_id') THEN
                ALTER TABLE market_shop_items ADD COLUMN badge_id TEXT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='role_id') THEN
                ALTER TABLE market_shop_items ADD COLUMN role_id BIGINT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='updated_at') THEN
                ALTER TABLE market_shop_items ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='base_value') THEN
                ALTER TABLE market_shop_items ADD COLUMN base_value BIGINT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='current_value') THEN
                ALTER TABLE market_shop_items ADD COLUMN current_value BIGINT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='demand_score') THEN
                ALTER TABLE market_shop_items ADD COLUMN demand_score INTEGER NOT NULL DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='total_bought') THEN
                ALTER TABLE market_shop_items ADD COLUMN total_bought INTEGER NOT NULL DEFAULT 0;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_shop_items' AND column_name='total_resold') THEN
                ALTER TABLE market_shop_items ADD COLUMN total_resold INTEGER NOT NULL DEFAULT 0;
            END IF;
        END$body$;
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
        # badge_definitions — canonical badge registry (source of truth).
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS badge_definitions (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            description TEXT,
            category    TEXT NOT NULL DEFAULT 'market',
            rarity      TEXT NOT NULL DEFAULT 'common',
            icon_url    TEXT,
            color       TEXT,
            visible     BOOLEAN NOT NULL DEFAULT TRUE,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        """)

        # Non-destructive migration: add color column if missing.
        await db.execute("""
        DO $body$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='badge_definitions' AND column_name='color'
            ) THEN
                ALTER TABLE badge_definitions ADD COLUMN color TEXT;
            END IF;
        END$body$;
        """)

        # ------------------------------------------------------------------ #
        # player_badges — badges awarded to users.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS player_badges (
            id          SERIAL PRIMARY KEY,
            guild_id    BIGINT NOT NULL,
            user_id     BIGINT NOT NULL,
            badge_id    TEXT NOT NULL REFERENCES badge_definitions(id) ON DELETE CASCADE,
            awarded_at  TIMESTAMP DEFAULT NOW(),
            awarded_by  BIGINT,
            source      TEXT NOT NULL DEFAULT 'system',
            UNIQUE(guild_id, user_id, badge_id)
        );
        """)

        # ------------------------------------------------------------------ #
        # market_shop_audit — audit log for all shop admin actions.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_shop_audit (
            id          SERIAL PRIMARY KEY,
            item_id     INTEGER,
            item_name   TEXT,
            action      TEXT NOT NULL,
            changed_by  BIGINT NOT NULL,
            old_value   TEXT,
            new_value   TEXT,
            field       TEXT,
            note        TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        """)

        # ------------------------------------------------------------------ #
        # market_item_value_history — price change history per shop item.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_item_value_history (
            id          SERIAL PRIMARY KEY,
            item_id     INTEGER NOT NULL,
            old_price   BIGINT NOT NULL,
            new_price   BIGINT NOT NULL,
            changed_by  BIGINT NOT NULL,
            reason      TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        """)

        # ------------------------------------------------------------------ #
        # Seed badge_definitions from badge_manifest.json.
        # ------------------------------------------------------------------ #
        try:
            import pathlib
            manifest_path = pathlib.Path(__file__).parent / "badge_manifest.json"
            if manifest_path.exists():
                with open(manifest_path) as f:
                    badge_manifest = json.load(f)
                badges_seeded = 0
                for badge in badge_manifest:
                    result = await db.execute("""
                    INSERT INTO badge_definitions (id, name, description, category, rarity, icon_url, color, visible)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (id) DO UPDATE SET
                        name=$2, description=$3, category=$4, rarity=$5, icon_url=$6, color=$7, visible=$8;
                    """, badge["id"], badge["name"], badge.get("description", ""),
                        badge.get("category", "market"), badge.get("rarity", "common"),
                        badge.get("icon_url", ""), badge.get("color"),
                        badge.get("visible", True))
                    if result and "INSERT" in result:
                        badges_seeded += 1
                print(f"[Badges/{mode}] ✅ Badge manifest synced ({len(badge_manifest)} badges).")
        except Exception as exc:
            print(f"[Badges/{mode}] ⚠️  Badge manifest sync failed: {exc}")

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
            print(f"[Shop/{mode}] ✅ Seeded {seeded} new shop items.")
        else:
            print(f"[Shop/{mode}] ✅ Shop items already present — no seeding needed.")

        # ------------------------------------------------------------------ #
        # market_registrations — user registration and T&C acceptance.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_registrations (
            id            SERIAL PRIMARY KEY,
            guild_id      BIGINT NOT NULL,
            user_id       BIGINT NOT NULL,
            username      TEXT NOT NULL,
            accepted_terms BOOLEAN NOT NULL DEFAULT FALSE,
            accepted_at   TIMESTAMP,
            registered_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(guild_id, user_id)
        );
        """)

    print(f"[DB/{mode}] ✅ All market_ tables verified/created.")
    print(f"[DB/{mode}] ✅ Registration table verified/created.")


async def seed_fake_players(test_pool: asyncpg.Pool) -> None:
    """Create fake players and initial stocks in the test database.

    Fake player IDs (9000000001–9000000010) never conflict with real Discord
    user IDs, so they are completely safe to store alongside real user data.
    """
    guild_id = TEST_GUILD_ID
    async with test_pool.acquire() as db:
        for index, player in enumerate(FAKE_PLAYERS, start=1):
            data: Dict[str, Any] = {
                "cr":     player["cr"],
                "wins":   player["wins"],
                "losses": player["losses"],
                "kills":  player["kills"],
                "mvps":   player["mvps"],
                "streak": player["streak"],
            }
            price = base_price_from_ranked_data(data, index)

            existing = await db.fetchval(
                "SELECT player_id FROM market_stocks WHERE guild_id=$1 AND player_id=$2;",
                guild_id, player["user_id"],
            )
            if existing is None:
                await db.execute("""
                INSERT INTO market_stocks
                    (guild_id, player_id, price, rank_position, active,
                     cr, wins, losses, kills, mvps, streak)
                VALUES ($1, $2, $3, $4, true, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (guild_id, player_id) DO NOTHING;
                """,
                    guild_id, player["user_id"], price, index,
                    player["cr"], player["wins"], player["losses"],
                    player["kills"], player["mvps"], player["streak"],
                )
                await db.execute("""
                INSERT INTO market_stock_history
                    (guild_id, player_id, old_price, new_price, reason)
                VALUES ($1, $2, $3, $4, 'Initial seed');
                """, guild_id, player["user_id"], price, price)

    print(f"[Sim] ✅ Fake players seeded in test database ({len(FAKE_PLAYERS)} players).")


async def simulate_market_movement(test_pool: asyncpg.Pool) -> List[Dict[str, Any]]:
    """Simulate realistic daily market movement for the test server.

    For each fake player:
    1. Adjust CR randomly by ±100 to ±500.
    2. Recalculate Top 10 positions by new CR.
    3. Apply position movement bonuses/penalties.
    4. Update prices with 70/30 weighted average.
    5. Record changes in market_stock_history.
    6. Ensure realistic swings (2–5% daily).

    Returns a list of dicts describing each player's price change.
    """
    guild_id = TEST_GUILD_ID
    results: List[Dict[str, Any]] = []

    async with test_pool.acquire() as db:
        # Fetch current fake player stocks.
        rows = await db.fetch("""
        SELECT player_id, price, rank_position, cr, wins, losses, kills, mvps, streak
        FROM market_stocks
        WHERE guild_id=$1 AND player_id >= 9000000001 AND player_id <= 9000000010
        ORDER BY rank_position ASC;
        """, guild_id)

        if not rows:
            return results

        # Step 1: Adjust each player's CR randomly.
        updated: List[Dict[str, Any]] = []
        for r in rows:
            delta = random.randint(100, 500) * random.choice([-1, 1])
            new_cr = max(1000, int(r["cr"]) + delta)
            updated.append({
                "player_id":    int(r["player_id"]),
                "old_price":    int(r["price"]),
                "old_position": int(r["rank_position"]),
                "cr":           new_cr,
                "wins":         int(r["wins"]),
                "losses":       int(r["losses"]),
                "kills":        int(r["kills"]),
                "mvps":         int(r["mvps"]),
                "streak":       int(r["streak"]),
            })

        # Step 2: Re-sort by new CR to determine new positions.
        updated.sort(key=lambda x: x["cr"], reverse=True)

        for new_index, player in enumerate(updated, start=1):
            player["new_position"] = new_index
            data: Dict[str, Any] = {
                "cr":     player["cr"],
                "wins":   player["wins"],
                "losses": player["losses"],
                "kills":  player["kills"],
                "mvps":   player["mvps"],
                "streak": player["streak"],
            }
            base_price = base_price_from_ranked_data(data, new_index)
            old_price = player["old_price"]
            old_position = player["old_position"]

            # Step 3 & 4: Weighted average + position movement bonus/penalty.
            blended = max(1000, int((old_price * 0.70) + (base_price * 0.30)))
            position_delta = old_position - new_index  # positive = moved up

            if position_delta > 0:
                boost = 0.025 * position_delta
                final_price = max(1000, int(blended * (1 + boost)))
                reason = f"Simulation: moved up #{old_position}→#{new_index} (+{position_delta})"
            elif position_delta < 0:
                penalty = 0.015 * abs(position_delta)
                final_price = max(1000, int(blended * (1 - penalty)))
                reason = f"Simulation: moved down #{old_position}→#{new_index} ({position_delta})"
            else:
                # Apply a small random daily swing (2–5%).
                swing_pct = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                final_price = max(1000, int(blended * (1 + swing_pct)))
                direction = "▲" if swing_pct > 0 else "▼"
                reason = f"Simulation: daily swing {direction}{abs(swing_pct)*100:.1f}%"

            player["new_price"] = final_price
            player["reason"] = reason

            # Step 5: Persist updates.
            await db.execute("""
            UPDATE market_stocks
            SET price=$1, rank_position=$2, previous_rank_position=$3,
                cr=$4, updated_at=NOW()
            WHERE guild_id=$5 AND player_id=$6;
            """, final_price, new_index, old_position,
                player["cr"], guild_id, player["player_id"])

            if final_price != old_price:
                await db.execute("""
                INSERT INTO market_stock_history
                    (guild_id, player_id, old_price, new_price, reason)
                VALUES ($1, $2, $3, $4, $5);
                """, guild_id, player["player_id"], old_price, final_price, reason)

            results.append({
                "player_id":    player["player_id"],
                "old_price":    old_price,
                "new_price":    final_price,
                "old_position": old_position,
                "new_position": new_index,
                "reason":       reason,
            })

    return results


async def reset_test_database(test_pool: asyncpg.Pool) -> None:
    """Clear all test-server market data and re-seed fake players."""
    guild_id = TEST_GUILD_ID
    async with test_pool.acquire() as db:
        await db.execute("DELETE FROM market_stock_history WHERE guild_id=$1;", guild_id)
        await db.execute("DELETE FROM market_transactions WHERE guild_id=$1;", guild_id)
        await db.execute("DELETE FROM market_holdings WHERE guild_id=$1;", guild_id)
        await db.execute("DELETE FROM market_users WHERE guild_id=$1;", guild_id)
        await db.execute("DELETE FROM market_stocks WHERE guild_id=$1;", guild_id)
        await db.execute("DELETE FROM market_settings WHERE guild_id=$1;", guild_id)
    print("[Sim] ✅ Test database cleared.")
    await seed_fake_players(test_pool)


async def init_db(pool: asyncpg.Pool, mode: str) -> None:
    """Initialise market_ tables in the given pool.

    mode is 'main' or 'test' — controls seeding behaviour.
    For the test database, fake players are seeded after table creation.
    For the main database, schema auto-detection is run.
    """
    await _create_market_tables(pool, mode)

    if mode == "main":
        # Run schema auto-detection so sync_top10_for_guild uses correct columns.
        schema = await detect_ranked_schema(pool)
        print(
            f"[Schema] ✅ Ranked table: '{schema['table']}' | "
            f"user_id='{schema['user_id_col']}' guild_id='{schema['guild_id_col']}' "
            f"data='{schema['data_col']}'"
        )
    elif mode == "test":
        await seed_fake_players(pool)


async def ensure_user(guild_id: int, user_id: int, pool: Optional[asyncpg.Pool] = None) -> None:
    """Create a market_users row for this guild/user if one does not exist yet.

    New users start with STARTING_SP ($250,000) balance.
    Accepts an explicit pool; falls back to the legacy db_pool global.
    """
    _pool = pool or db_pool
    assert _pool is not None
    async with _pool.acquire() as db:
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


async def update_wealth_role(guild_id: int, user_id: int, balance: int, pool: Optional[asyncpg.Pool] = None) -> Optional[str]:
    """Compute and persist the wealth role for a user based on their balance.

    Updates market_users.wealth_role and inserts a record into
    market_wealth_roles when the role changes.  Returns the new role name
    (or None if below all thresholds).
    """
    _pool = pool or db_pool
    assert _pool is not None
    role_name = compute_wealth_role(balance)
    async with _pool.acquire() as db:
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


async def market_is_open(guild_id: int, pool: Optional[asyncpg.Pool] = None) -> bool:
    _pool = pool or db_pool
    assert _pool is not None
    async with _pool.acquire() as db:
        await db.execute("""
        INSERT INTO market_settings (guild_id, market_open)
        VALUES ($1, TRUE)
        ON CONFLICT (guild_id) DO NOTHING;
        """, guild_id)
        return bool(await db.fetchval("SELECT market_open FROM market_settings WHERE guild_id=$1;", guild_id))


async def get_stock(guild_id: int, player_id: int, pool: Optional[asyncpg.Pool] = None):
    _pool = pool or db_pool
    assert _pool is not None
    async with _pool.acquire() as db:
        return await db.fetchrow("""
        SELECT * FROM market_stocks
        WHERE guild_id=$1 AND player_id=$2 AND active=true AND rank_position BETWEEN 1 AND 10;
        """, guild_id, player_id)


async def total_shares(guild_id: int, player_id: int, pool: Optional[asyncpg.Pool] = None) -> int:
    _pool = pool or db_pool
    assert _pool is not None
    async with _pool.acquire() as db:
        val = await db.fetchval("""
        SELECT COALESCE(SUM(shares), 0) FROM market_holdings
        WHERE guild_id=$1 AND player_id=$2;
        """, guild_id, player_id)
        return safe_int(val)


async def update_price(guild_id: int, player_id: int, new_price: int, reason: str, pool: Optional[asyncpg.Pool] = None) -> tuple[int, int]:
    _pool = pool or db_pool
    assert _pool is not None
    async with _pool.acquire() as db:
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

    For the main server: reads real players from the ranked table.
    For the test server: reads fake players from market_stocks (simulation).

    Players entering the Top 10 get a new stock; players leaving are marked inactive
    (all holdings and history are preserved). Price movements are smoothed with a
    70/30 weighted average and boosted/penalised based on position delta.
    """
    guild_id = guild.id

    # Route to the correct pool based on guild_id.
    try:
        pool = await db_manager.get_pool(guild_id)
    except (ValueError, RuntimeError) as exc:
        print(f"[Sync] Skipping guild {guild_id}: {exc}")
        return 0

    # Test server uses fake players — simulation handles price updates separately.
    # We still run a lightweight sync to ensure active flags are correct.
    if guild_id == TEST_GUILD_ID:
        async with pool.acquire() as db:
            count = await db.fetchval(
                "SELECT COUNT(*) FROM market_stocks WHERE guild_id=$1 AND active=true;",
                guild_id,
            )
        return int(count or 0)

    # Main server: read real players from the ranked table.
    # Use module-level globals (potentially updated by detect_ranked_schema).
    # Column/table names come from config only — never from user input.
    query = f"""
    SELECT {RANKED_USER_ID_COLUMN} AS user_id, {RANKED_DATA_COLUMN} AS data
    FROM {RANKED_PLAYERS_TABLE}
    WHERE {RANKED_GUILD_ID_COLUMN}=$1
    ORDER BY COALESCE(({RANKED_DATA_COLUMN}->>'cr')::INTEGER, 0) DESC
    LIMIT 10;
    """

    async with pool.acquire() as db:
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
    """Background loop: sync main-server Top 10 and run test-server simulation."""
    for guild in bot.guilds:
        if guild.id == MAIN_GUILD_ID:
            await sync_top10_for_guild(guild)
        elif guild.id == TEST_GUILD_ID:
            # Run market simulation for the test server every cycle.
            if db_manager.db_pool_test is not None:
                try:
                    await simulate_market_movement(db_manager.db_pool_test)
                except Exception as exc:
                    print(f"[Sim] ⚠️  Simulation tick failed: {exc}")


# ---------------------------------------------------------------------------
# register_badge_definitions — register all 10 badge definitions to the DB
# ---------------------------------------------------------------------------

BADGE_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "badge_id": "early_investor",
        "name": "Early Investor Badge",
        "description": "Awarded to the earliest supporters of the EAS market.",
        "icon": "https://raw.githubusercontent.com/honchobusiness12-web/eas-stock-market-bot/main/early_investor.svg",
        "color": "#6b7280",
        "rarity": "common",
    },
    {
        "badge_id": "rising_investor",
        "name": "Rising Investor Badge",
        "description": "For investors on the rise.",
        "icon": "https://raw.githubusercontent.com/honchobusiness12-web/eas-stock-market-bot/main/rising_investor.svg",
        "color": "#6b7280",
        "rarity": "common",
    },
    {
        "badge_id": "diamond_hands",
        "name": "Diamond Hands Badge",
        "description": "For investors who never sell.",
        "icon": "https://raw.githubusercontent.com/honchobusiness12-web/eas-stock-market-bot/main/diamond_hands.svg",
        "color": "#00d4ff",
        "rarity": "rare",
    },
    {
        "badge_id": "market_mogul_badge",
        "name": "Market Mogul Badge",
        "description": "For the most powerful market players.",
        "icon": "https://raw.githubusercontent.com/honchobusiness12-web/eas-stock-market-bot/main/market_mogul.svg",
        "color": "#00d4ff",
        "rarity": "rare",
    },
    {
        "badge_id": "stock_king",
        "name": "Stock King Badge",
        "description": "Reign supreme over the market.",
        "icon": "https://raw.githubusercontent.com/honchobusiness12-web/eas-stock-market-bot/main/stock_king.svg",
        "color": "#a855f7",
        "rarity": "epic",
    },
    {
        "badge_id": "bull_master",
        "name": "Bull Master Badge",
        "description": "Master of the bull market.",
        "icon": "https://raw.githubusercontent.com/honchobusiness12-web/eas-stock-market-bot/main/bull_master.svg",
        "color": "#a855f7",
        "rarity": "epic",
    },
    {
        "badge_id": "bear_master",
        "name": "Bear Master Badge",
        "description": "Master of the bear market.",
        "icon": "https://raw.githubusercontent.com/honchobusiness12-web/eas-stock-market-bot/main/bear_master.svg",
        "color": "#a855f7",
        "rarity": "epic",
    },
    {
        "badge_id": "hall_of_investors",
        "name": "Hall of Investors Badge",
        "description": "Inducted into the hall of legends.",
        "icon": "https://raw.githubusercontent.com/honchobusiness12-web/eas-stock-market-bot/main/hall_of_investors.svg",
        "color": "#ff6b6b",
        "rarity": "legendary",
    },
    {
        "badge_id": "starpoint_elite",
        "name": "Starpoint Elite Badge",
        "description": "The pinnacle of StarPoint achievement.",
        "icon": "https://raw.githubusercontent.com/honchobusiness12-web/eas-stock-market-bot/main/starpoint_elite.svg",
        "color": "#ff6b6b",
        "rarity": "legendary",
    },
    {
        "badge_id": "eas_tycoon_badge",
        "name": "EAS Tycoon Badge",
        "description": "The ultimate badge for the wealthiest investors.",
        "icon": "https://raw.githubusercontent.com/honchobusiness12-web/eas-stock-market-bot/main/eas_tycoon.svg",
        "color": "#ffd700",
        "rarity": "mythic",
    },
]


async def register_badge_definitions() -> int:
    """Register all 10 badge definitions to the main database.

    Uses badge_id as the unique key — checks if exists before inserting.
    Returns the number of newly registered badges.
    """
    if db_manager.db_pool_main is None:
        print("[Badges] ⚠️  Main database unavailable — skipping badge registration.")
        return 0

    registered_count = 0
    async with db_manager.db_pool_main.acquire() as db:
        for badge in BADGE_DEFINITIONS:
            badge_id = badge["badge_id"]
            # Check if badge already exists.
            existing = await db.fetchval(
                "SELECT id FROM badge_definitions WHERE id = $1;",
                badge_id,
            )
            if existing is not None:
                print(f"[Badges] ⏭️  Already registered: {badge_id}")
                continue
            await db.execute(
                """
                INSERT INTO badge_definitions (id, name, description, category, rarity, icon_url, color, visible)
                VALUES ($1, $2, $3, 'market', $4, $5, $6, TRUE)
                ON CONFLICT (id) DO NOTHING;
                """,
                badge_id,
                badge["name"],
                badge["description"],
                badge["rarity"],
                badge["icon"],
                badge["color"],
            )
            print(f"[Badges] ✅ Registered: {badge_id} ({badge['name']})")
            registered_count += 1

    print(f"[Badges] 📋 Badge registration complete — {len(BADGE_DEFINITIONS)} total definitions, {registered_count} newly registered.")
    return registered_count


# ---------------------------------------------------------------------------
# seed_market_shop_items — populate market_shop_items with the 26 catalogue items
# ---------------------------------------------------------------------------

async def seed_market_shop_items() -> int:
    """Seed the 26 canonical market shop items into the main database.

    Uses item_name as the unique key — existing items are skipped.
    Returns the number of newly inserted items.
    """
    if db_manager.db_pool_main is None:
        print("[Shop] ⚠️  Main database unavailable — skipping shop seeding.")
        return 0

    # All 26 items: (item_name, description, category, price, rarity, is_limited, max_stock,
    #                badge_id, role_id_str)
    # is_limited=True  → limited stock; max_stock=None means unlimited.
    # resale_percent: 85 for limited/ultra-rare, 70 for everything else.
    # badge_id: links to badge_definitions.id for badge category items.
    # role_id_str: Discord role name key for role category items (resolved at purchase time).
    SHOP_CATALOGUE: List[Dict[str, Any]] = [
        # ── Website Badges (10) ──────────────────────────────────────────────
        {
            "item_name": "early_investor",
            "description": "Early Investor Badge — awarded to the earliest supporters of the EAS market.",
            "category": "badge",
            "price": 250_000,
            "rarity": "common",
            "is_limited": False,
            "max_stock": None,
            "badge_id": "early_investor",
            "role_id_str": None,
        },
        {
            "item_name": "rising_investor",
            "description": "Rising Investor Badge — for investors on the rise.",
            "category": "badge",
            "price": 750_000,
            "rarity": "common",
            "is_limited": False,
            "max_stock": None,
            "badge_id": "rising_investor",
            "role_id_str": None,
        },
        {
            "item_name": "diamond_hands",
            "description": "Diamond Hands Badge — for investors who never sell.",
            "category": "badge",
            "price": 10_000_000,
            "rarity": "rare",
            "is_limited": True,
            "max_stock": 100,
            "badge_id": "diamond_hands",
            "role_id_str": None,
        },
        {
            "item_name": "market_mogul_badge",
            "description": "Market Mogul Badge — for the most powerful market players.",
            "category": "badge",
            "price": 25_000_000,
            "rarity": "rare",
            "is_limited": True,
            "max_stock": 50,
            "badge_id": "market_mogul_badge",
            "role_id_str": None,
        },
        {
            "item_name": "stock_king",
            "description": "Stock King Badge — reign supreme over the market.",
            "category": "badge",
            "price": 50_000_000,
            "rarity": "epic",
            "is_limited": True,
            "max_stock": 25,
            "badge_id": "stock_king",
            "role_id_str": None,
        },
        {
            "item_name": "bull_master",
            "description": "Bull Master Badge — master of the bull market.",
            "category": "badge",
            "price": 75_000_000,
            "rarity": "epic",
            "is_limited": True,
            "max_stock": 15,
            "badge_id": "bull_master",
            "role_id_str": None,
        },
        {
            "item_name": "bear_master",
            "description": "Bear Master Badge — master of the bear market.",
            "category": "badge",
            "price": 75_000_000,
            "rarity": "epic",
            "is_limited": True,
            "max_stock": 15,
            "badge_id": "bear_master",
            "role_id_str": None,
        },
        {
            "item_name": "hall_of_investors",
            "description": "Hall of Investors Badge — inducted into the hall of legends.",
            "category": "badge",
            "price": 100_000_000,
            "rarity": "legendary",
            "is_limited": True,
            "max_stock": 10,
            "badge_id": "hall_of_investors",
            "role_id_str": None,
        },
        {
            "item_name": "starpoint_elite",
            "description": "Starpoint Elite Badge — the pinnacle of StarPoint achievement.",
            "category": "badge",
            "price": 250_000_000,
            "rarity": "legendary",
            "is_limited": True,
            "max_stock": 10,
            "badge_id": "starpoint_elite",
            "role_id_str": None,
        },
        {
            "item_name": "eas_tycoon_badge",
            "description": "EAS Tycoon Badge — the ultimate badge for the wealthiest investors.",
            "category": "badge",
            "price": 1_000_000_000,
            "rarity": "mythic",
            "is_limited": True,
            "max_stock": 5,
            "badge_id": "eas_tycoon_badge",
            "role_id_str": None,
        },
        # ── Discord Roles (11) ───────────────────────────────────────────────
        {
            "item_name": "millionaire_role",
            "description": "Millionaire Role — unlock the Millionaire Discord role.",
            "category": "role",
            "price": 10_000_000,
            "rarity": "common",
            "is_limited": False,
            "max_stock": None,
            "badge_id": None,
            "role_id_str": "millionaire_role",
        },
        {
            "item_name": "multi_millionaire_role",
            "description": "Multi-Millionaire Role — unlock the Multi-Millionaire Discord role.",
            "category": "role",
            "price": 50_000_000,
            "rarity": "common",
            "is_limited": False,
            "max_stock": None,
            "badge_id": None,
            "role_id_str": "multi_millionaire_role",
        },
        {
            "item_name": "investor_elite_role",
            "description": "Investor Elite Role — unlock the Investor Elite Discord role.",
            "category": "role",
            "price": 100_000_000,
            "rarity": "rare",
            "is_limited": True,
            "max_stock": 100,
            "badge_id": None,
            "role_id_str": "investor_elite_role",
        },
        {
            "item_name": "market_mogul_role",
            "description": "Market Mogul Role — unlock the Market Mogul Discord role.",
            "category": "role",
            "price": 250_000_000,
            "rarity": "rare",
            "is_limited": True,
            "max_stock": 50,
            "badge_id": None,
            "role_id_str": "market_mogul_role",
        },
        {
            "item_name": "stock_legend_role",
            "description": "Stock Legend Role — unlock the Stock Legend Discord role.",
            "category": "role",
            "price": 500_000_000,
            "rarity": "epic",
            "is_limited": True,
            "max_stock": 25,
            "badge_id": None,
            "role_id_str": "stock_legend_role",
        },
        {
            "item_name": "eas_tycoon_role",
            "description": "EAS Tycoon Role — unlock the exclusive EAS Tycoon Discord role.",
            "category": "role",
            "price": 1_000_000_000,
            "rarity": "epic",
            "is_limited": True,
            "max_stock": 10,
            "badge_id": None,
            "role_id_str": "eas_tycoon_role",
        },
        # ── Ultra-Rare Roles (5) ─────────────────────────────────────────────
        {
            "item_name": "market_shark_role",
            "description": "Market Shark Role — an ultra-rare role for apex market predators.",
            "category": "role",
            "price": 5_000_000_000,
            "rarity": "mythic",
            "is_limited": True,
            "max_stock": 5,
            "badge_id": None,
            "role_id_str": "market_shark_role",
        },
        {
            "item_name": "investment_bank_role",
            "description": "Investment Bank Role — an ultra-rare role for institutional-level investors.",
            "category": "role",
            "price": 10_000_000_000,
            "rarity": "mythic",
            "is_limited": True,
            "max_stock": 3,
            "badge_id": None,
            "role_id_str": "investment_bank_role",
        },
        {
            "item_name": "global_investor_role",
            "description": "Global Investor Role — an ultra-rare role for world-class investors.",
            "category": "role",
            "price": 25_000_000_000,
            "rarity": "mythic",
            "is_limited": True,
            "max_stock": 2,
            "badge_id": None,
            "role_id_str": "global_investor_role",
        },
        {
            "item_name": "market_overlord_role",
            "description": "Market Overlord Role — an ultra-rare role for the undisputed market ruler.",
            "category": "role",
            "price": 50_000_000_000,
            "rarity": "mythic",
            "is_limited": True,
            "max_stock": 1,
            "badge_id": None,
            "role_id_str": "market_overlord_role",
        },
        {
            "item_name": "stock_emperor_role",
            "description": "Stock Emperor Role — the rarest role in existence, for the supreme stock emperor.",
            "category": "role",
            "price": 100_000_000_000,
            "rarity": "mythic",
            "is_limited": True,
            "max_stock": 1,
            "badge_id": None,
            "role_id_str": "stock_emperor_role",
        },
    ]

    seeded_count = 0
    async with db_manager.db_pool_main.acquire() as db:
        for item in SHOP_CATALOGUE:
            item_name = item["item_name"]
            price = item["price"]
            is_limited = item["is_limited"]
            max_stock = item["max_stock"]
            rarity = item["rarity"]
            badge_id = item.get("badge_id")

            # Check if item already exists.
            existing = await db.fetchval(
                "SELECT id FROM market_shop_items WHERE item_name = $1;",
                item_name,
            )
            if existing is not None:
                # Update badge_id if it's missing (backfill for existing rows).
                if badge_id is not None:
                    await db.execute(
                        "UPDATE market_shop_items SET badge_id=$1 WHERE item_name=$2 AND badge_id IS NULL;",
                        badge_id, item_name,
                    )
                print(f"[Shop] ⏭️  Already exists: {item_name}")
                continue

            # Derive computed fields.
            current_stock = max_stock if is_limited and max_stock is not None else None
            min_value = price // 2
            max_value = price * 5
            # Ultra-rare (mythic limited) and limited items get 85% resale; others 70%.
            resale_percent = 85 if (is_limited and rarity in ("mythic", "legendary", "epic")) else 70

            await db.execute(
                """
                INSERT INTO market_shop_items
                    (item_name, description, price, category, rarity, active,
                     limited, max_stock, current_stock,
                     base_value, current_value, min_value, max_value,
                     resale_percent, badge_id, demand_score, total_bought, total_resold)
                VALUES ($1,$2,$3,$4,$5,TRUE,$6,$7,$8,$9,$10,$11,$12,$13,$14,0,0,0);
                """,
                item_name,
                item["description"],
                price,
                item["category"],
                rarity,
                is_limited,
                max_stock,
                current_stock,
                price,   # base_value
                price,   # current_value
                min_value,
                max_value,
                resale_percent,
                badge_id,
            )
            print(f"[Shop] ✅ Seeded: {item_name} (${price:,})")
            seeded_count += 1

    print(f"[Shop] 📦 Seeded {seeded_count} new items to market_shop_items")
    return seeded_count


# ---------------------------------------------------------------------------
# register_guild_commands — copy all tree commands to the testing guild
# ---------------------------------------------------------------------------

async def register_guild_commands() -> int:
    """Copy all app-commands to the testing guild for instant visibility.

    Each command is logged individually.  Returns the total number of
    commands successfully synced to the testing guild.
    """
    guild_obj = discord.Object(id=TEST_GUILD_ID)
    all_cmds = bot.tree.get_commands()

    # Copy every global command into the testing guild's command tree.
    for cmd in all_cmds:
        try:
            bot.tree.copy_global_to(guild=guild_obj)
            # copy_global_to copies all at once; log each name individually.
            print(f"[Commands] ✅ Registered: /{cmd.name}")
        except Exception as exc:
            print(f"[Commands] ❌ Failed to register /{cmd.name}: {exc}")

    # Sync the testing guild's command tree to Discord.
    try:
        synced = await bot.tree.sync(guild=guild_obj)
        count = len(synced)
        print(f"[Commands] 📋 Registered {count} commands to testing guild {TEST_GUILD_ID}")
        for cmd in sorted(synced, key=lambda c: c.name):
            print(f"[Commands]    /{cmd.name}")
        return count
    except discord.errors.Forbidden:
        print(
            f"[Commands] ❌ Missing applications.commands scope for testing guild {TEST_GUILD_ID}. "
            "Re-invite the bot with scopes: bot, applications.commands"
        )
        return 0
    except Exception as exc:
        print(f"[Commands] ❌ Sync to testing guild {TEST_GUILD_ID} failed: {exc}")
        return 0


@bot.event
async def on_ready():
    """Discord on_ready handler — runs once after the bot logs in.

    Initialises both database pools, starts the Top 10 sync loop, and syncs
    slash commands to both servers.  Comprehensive startup logs are printed
    so Railway deployment logs are easy to read.
    """
    global bot_start_time, db_pool, _shop_seeded, _commands_registered, _badges_registered
    bot_start_time = datetime.datetime.utcnow()

    print("=" * 60)
    print("[Startup] EAS Stock Market Bot — Phase 1B (Dual-Database)")
    print(f"[Startup] Logged in as: {bot.user} (ID: {bot.user.id})")
    print(f"[Startup] Developer ID: {DEVELOPER_USER_ID}")
    print("=" * 60)

    # Initialise both database pools.
    await db_manager.init_pools()

    # Initialise main database tables.
    if db_manager.db_pool_main is not None:
        try:
            await init_db(db_manager.db_pool_main, "main")
            # Keep legacy db_pool pointing at main for backward-compat helpers.
            db_pool = db_manager.db_pool_main
            print(f"[Startup] ✅ Main Server ({MAIN_GUILD_ID}) — LIVE MODE")
        except Exception as exc:
            print(f"[Startup] ❌ Main database init failed: {exc}")
    else:
        print(f"[Startup] ⚠️  Main Server ({MAIN_GUILD_ID}) — database unavailable")

    # Initialise test database tables and seed fake players.
    if db_manager.db_pool_test is not None:
        try:
            await init_db(db_manager.db_pool_test, "test")
            print(f"[Startup] ✅ Test Server ({TEST_GUILD_ID}) — SIMULATION MODE")
            print("[Startup] ✅ Fake players seeded in test database")
        except Exception as exc:
            print(f"[Startup] ❌ Test database init failed: {exc}")
    else:
        print(f"[Startup] ⚠️  Test Server ({TEST_GUILD_ID}) — database unavailable")

    # Start the background Top 10 sync / simulation loop.
    if not top10_sync_loop.is_running():
        top10_sync_loop.start()
        print(f"[Startup] ✅ Market auto-update loop started (every {TOP10_SYNC_MINUTES} min)")
    else:
        print("[Startup] ℹ️  Market auto-update loop already running.")

    # Seed market shop items (once per process lifetime).
    if not _shop_seeded:
        _shop_seeded = True
        try:
            await seed_market_shop_items()
        except Exception as exc:
            print(f"[Shop] ❌ Shop seeding failed: {exc}")
    else:
        print("[Shop] ℹ️  Shop seeding already ran — skipping.")

    # Register badge definitions to the main database (once per process lifetime).
    if not _badges_registered:
        _badges_registered = True
        try:
            await register_badge_definitions()
        except Exception as exc:
            print(f"[Badges] ❌ Badge registration failed: {exc}")
    else:
        print("[Badges] ℹ️  Badge registration already ran — skipping.")

    # Register slash commands to the testing guild (once per process lifetime).
    if not _commands_registered:
        _commands_registered = True
        print("[Startup] 🔄 Registering slash commands to guilds...")
        all_commands = [cmd.name for cmd in bot.tree.get_commands()]
        print(f"[Startup] 📋 Commands to register ({len(all_commands)}): {', '.join(sorted(all_commands))}")

        # Testing guild — use register_guild_commands() for per-command logging.
        await register_guild_commands()

        # Main guild — sync directly.
        main_guild_obj = discord.Object(id=MAIN_GUILD_ID)
        main_guild_in_cache = bot.get_guild(MAIN_GUILD_ID)
        if main_guild_in_cache is None:
            print(f"[Startup] ⚠️  Bot is not in main server ({MAIN_GUILD_ID}) — skipping main guild sync.")
        else:
            try:
                synced = await bot.tree.sync(guild=main_guild_obj)
                print(f"[Startup] ✅ Registered {len(synced)} slash commands to main server ({MAIN_GUILD_ID})")
                for cmd in sorted(synced, key=lambda c: c.name):
                    print(f"[Startup]    /{cmd.name}")
            except discord.errors.Forbidden:
                print(f"[Startup] ❌ Missing applications.commands scope for main server ({MAIN_GUILD_ID}). "
                      "Re-invite the bot with scopes: bot, applications.commands")
            except Exception as exc:
                print(f"[Startup] ⚠️  Command sync failed for main server ({MAIN_GUILD_ID}): {exc}")
    else:
        print("[Startup] ℹ️  Command registration already ran — skipping.")

    print("=" * 60)
    print("[Startup] ✅ EAS Stock Market Bot is READY")
    print("=" * 60)


async def send_embed(interaction: discord.Interaction, title: str, description: str, color=discord.Color.blurple(), ephemeral=False):
    embed = discord.Embed(title=title, description=description, color=color)
    await interaction.response.send_message(embed=embed, ephemeral=ephemeral)


# ---------------------------------------------------------------------------
# Badge and role purchase handlers
# ---------------------------------------------------------------------------

async def handle_badge_purchase(
    guild_id: int,
    user_id: int,
    badge_id: str,
    db_pool: asyncpg.Pool,
) -> Tuple[bool, str]:
    """Award a badge to a user in player_badges.

    Returns (success, message) where success is True if the badge was awarded,
    False if the user already has it or an error occurred.
    """
    async with db_pool.acquire() as db:
        # Look up badge definition for the display name.
        badge_def = await db.fetchrow(
            "SELECT id, name FROM badge_definitions WHERE id = $1;",
            badge_id,
        )
        if badge_def is None:
            return False, f"Badge definition `{badge_id}` not found in the database."

        # Check if user already has this badge.
        already_has = await db.fetchval(
            "SELECT id FROM player_badges WHERE guild_id=$1 AND user_id=$2 AND badge_id=$3;",
            guild_id, user_id, badge_id,
        )
        if already_has is not None:
            return False, f"You already have the **{badge_def['name']}** badge."

        # Award the badge.
        await db.execute(
            """
            INSERT INTO player_badges (guild_id, user_id, badge_id, awarded_at, source)
            VALUES ($1, $2, $3, NOW(), 'shop')
            ON CONFLICT (guild_id, user_id, badge_id) DO NOTHING;
            """,
            guild_id, user_id, badge_id,
        )

    return True, f"**{badge_def['name']}** has been added to your profile!"


async def handle_role_purchase(
    guild_id: int,
    user_id: int,
    role_name_key: str,
    db_pool: asyncpg.Pool,
) -> Tuple[bool, str]:
    """Assign a Discord role to a member by searching for a role whose name
    matches the role_name_key (case-insensitive, underscores treated as spaces).

    Returns (success, message).
    """
    guild = bot.get_guild(guild_id)
    if guild is None:
        return False, "Could not find the Discord server. Please try again."

    member = guild.get_member(user_id)
    if member is None:
        try:
            member = await guild.fetch_member(user_id)
        except Exception:
            return False, "Could not find your Discord member profile."

    # Normalise the key: replace underscores with spaces, strip trailing "_role".
    normalised = role_name_key.replace("_", " ").strip()
    if normalised.lower().endswith(" role"):
        normalised = normalised[:-5].strip()

    # Find the matching role in the guild (case-insensitive).
    target_role: Optional[discord.Role] = None
    for role in guild.roles:
        if role.name.lower() == normalised.lower():
            target_role = role
            break
    # Fallback: partial match.
    if target_role is None:
        for role in guild.roles:
            if normalised.lower() in role.name.lower():
                target_role = role
                break

    if target_role is None:
        return False, (
            f"Could not find a Discord role matching `{role_name_key}`. "
            "Please ask a staff member to set up the role."
        )

    # Check if member already has the role.
    if target_role in member.roles:
        return False, f"You already have the **{target_role.name}** role."

    # Assign the role.
    try:
        await member.add_roles(target_role, reason="Shop purchase")
    except discord.Forbidden:
        return False, (
            f"I don't have permission to assign the **{target_role.name}** role. "
            "Please ask a staff member to check my role permissions."
        )
    except Exception as exc:
        return False, f"Failed to assign role: {exc}"

    return True, f"The **{target_role.name}** role has been assigned to you!"


# ---------------------------------------------------------------------------
# Registration helpers
# ---------------------------------------------------------------------------

async def is_registered(guild_id: int, user_id: int, pool: asyncpg.Pool) -> bool:
    """Check if user is registered and has accepted terms in this guild."""
    async with pool.acquire() as db:
        result = await db.fetchval(
            "SELECT accepted_terms FROM market_registrations WHERE guild_id=$1 AND user_id=$2;",
            guild_id, user_id,
        )
    return result is True


async def get_registration_info(guild_id: int, user_id: int, pool: asyncpg.Pool) -> Optional[Dict]:
    """Get user's registration info."""
    async with pool.acquire() as db:
        return await db.fetchrow(
            "SELECT * FROM market_registrations WHERE guild_id=$1 AND user_id=$2;",
            guild_id, user_id,
        )


NOT_REGISTERED_DESC = (
    "You must register for the EAS Stock Market first.\n\n"
    "Use `/register` to:\n"
    "✓ Read the Terms & Conditions\n"
    "✓ Accept the agreement\n"
    f"✓ Get your starting balance of ${STARTING_SP:,} SP\n\n"
    "Registration takes less than 1 minute!"
)


# ---------------------------------------------------------------------------
# Registration UI — Accept / Decline buttons
# ---------------------------------------------------------------------------

class RegistrationView(discord.ui.View):
    """View with Accept and Decline buttons for the /register command."""

    def __init__(self, user_id: int, guild_id: int, pool: asyncpg.Pool, username: str):
        super().__init__(timeout=300)  # 5-minute timeout
        self.user_id = user_id
        self.guild_id = guild_id
        self.pool = pool
        self.username = username

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only the user who invoked /register may click the buttons."""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "These buttons are not for you.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="✅ Accept", style=discord.ButtonStyle.green)
    async def accept_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Register the user and create their market account."""
        try:
            async with self.pool.acquire() as db:
                await db.execute(
                    """
                    INSERT INTO market_registrations
                        (guild_id, user_id, username, accepted_terms, accepted_at)
                    VALUES ($1, $2, $3, TRUE, NOW())
                    ON CONFLICT (guild_id, user_id)
                    DO UPDATE SET accepted_terms=TRUE, accepted_at=NOW(), username=$3;
                    """,
                    self.guild_id, self.user_id, self.username,
                )
            # Create market_users entry with starting balance.
            await ensure_user(self.guild_id, self.user_id, self.pool)
        except Exception as exc:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="❌ Registration Error",
                    description=f"An error occurred: {exc}",
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )
            return

        # Disable all buttons.
        for child in self.children:
            child.disabled = True  # type: ignore[attr-defined]
        await interaction.response.edit_message(view=self)

        desc = (
            "You have been registered and accepted the Terms & Conditions.\n\n"
            f"**Your Starting Balance:** ${STARTING_SP:,} SP\n\n"
            "You can now use all market commands:\n"
            "• `/balance` — Check your balance\n"
            "• `/daily` — Claim daily rewards\n"
            "• `/market` — View the Top 10\n"
            "• `/buy` — Purchase stocks\n"
            "• `/sell` — Sell stocks\n"
            "• `/portfolio` — View your holdings\n\n"
            "Good luck! 📈"
        )
        embed = discord.Embed(
            title="✅ Welcome to the EAS Stock Market!",
            description=desc,
            color=discord.Color.green(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="❌ Decline", style=discord.ButtonStyle.red)
    async def decline_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Decline registration — do not register the user."""
        for child in self.children:
            child.disabled = True  # type: ignore[attr-defined]
        await interaction.response.edit_message(view=self)

        desc = (
            "You have declined the Terms & Conditions.\n\n"
            "You can register at any time by using `/register` again."
        )
        embed = discord.Embed(
            title="ℹ️ Registration Declined",
            description=desc,
            color=discord.Color.orange(),
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def on_timeout(self) -> None:
        """Disable buttons when the view times out."""
        for child in self.children:
            child.disabled = True  # type: ignore[attr-defined]


async def validate_guild_and_get_pool(interaction: discord.Interaction) -> Optional[asyncpg.Pool]:
    """Validate that the interaction comes from a known guild and return its pool.

    Returns None (after sending an error embed) if the guild is not recognised.
    """
    if interaction.guild is None:
        await send_embed(
            interaction,
            "❌ Server Required",
            "This command must be used inside a server.",
            discord.Color.red(),
            True,
        )
        return None
    guild_id = interaction.guild.id
    if not validate_guild(guild_id):
        await send_embed(
            interaction,
            "❌ Invalid Server",
            f"This bot only works in the testing server (`{TEST_GUILD_ID}`) "
            f"or main server (`{MAIN_GUILD_ID}`).",
            discord.Color.red(),
            True,
        )
        return None
    try:
        return await db_manager.get_pool(guild_id)
    except RuntimeError as exc:
        await send_embed(
            interaction,
            "❌ Database Unavailable",
            str(exc),
            discord.Color.red(),
            True,
        )
        return None


@bot.tree.command(name="ping", description="Check if the EAS Stock Market Bot is online.")
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)

    # Per-database connection check.
    async def _check_pool(pool: Optional[asyncpg.Pool]) -> str:
        if pool is None:
            return "❌ Disconnected"
        try:
            async with pool.acquire() as _conn:
                await _conn.fetchval("SELECT 1")
            return "✅ Connected"
        except Exception:
            return "⚠️ Error"

    main_status = await _check_pool(db_manager.db_pool_main)
    test_status = await _check_pool(db_manager.db_pool_test)

    # Uptime calculation.
    if bot_start_time is not None:
        delta = datetime.datetime.utcnow() - bot_start_time
        total_seconds = int(delta.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{hours}h {minutes}m {seconds}s"
    else:
        uptime_str = "Unknown"

    guild_id = interaction.guild.id if interaction.guild else 0
    current_mode = mode_label(guild_id) if validate_guild(guild_id) else "⚠️ Unknown Server"

    desc = (
        f"**Latency:** {latency}ms\n"
        f"**Mode:** {current_mode}\n"
        f"**Main DB:** {main_status}\n"
        f"**Test DB:** {test_status}\n"
        f"**Uptime:** {uptime_str}"
    )
    await send_embed(interaction, "🏓 Pong", desc, discord.Color.green())


@bot.tree.command(name="register", description="Register for the EAS Stock Market and accept the Terms & Conditions.")
async def register(interaction: discord.Interaction):
    """Display Terms & Conditions and allow user to register for the market."""
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    user_id = interaction.user.id

    # Check if already registered.
    reg = await get_registration_info(guild_id, user_id, pool)
    if reg and reg["accepted_terms"]:
        reg_date = reg["registered_at"]
        date_str = reg_date.strftime("%Y-%m-%d at %H:%M UTC") if reg_date else "Unknown"
        desc = (
            f"You registered on: **{date_str}**\n\n"
            "You have already accepted the Terms & Conditions.\n"
            "Use `/balance` to check your account status."
        )
        await send_embed(
            interaction,
            "ℹ️ Already Registered",
            desc,
            discord.Color.blue(),
            True,
        )
        return

    # Show T&C embed with Accept/Decline buttons.
    embed = discord.Embed(
        title="📋 EAS Stock Market — Registration",
        description=TERMS_AND_CONDITIONS,
        color=discord.Color.gold(),
    )
    embed.set_footer(text="You have 5 minutes to accept or decline. Only you can click these buttons.")
    view = RegistrationView(
        user_id=user_id,
        guild_id=guild_id,
        pool=pool,
        username=str(interaction.user),
    )
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


@bot.tree.command(name="balance", description="View your StarPoints balance and wealth role.")
async def balance(interaction: discord.Interaction):
    """Show the caller's current balance, wealth role, and next wealth milestone."""
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    await ensure_user(guild_id, interaction.user.id, pool)
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT balance, wealth_role FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, interaction.user.id,
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
    embed.set_footer(text=f"Guild: {interaction.guild.name} • {mode_label(guild_id)}")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="daily", description="Claim your daily StarPoints reward.")
async def daily(interaction: discord.Interaction):
    """Claim the daily $50,000 SP reward (once per 24 hours)."""
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    await ensure_user(guild_id, interaction.user.id, pool)
    async with pool.acquire() as db:
        can_claim = await db.fetchval("""
        SELECT last_daily IS NULL OR last_daily < NOW() - INTERVAL '24 hours'
        FROM market_users WHERE guild_id=$1 AND user_id=$2;
        """, guild_id, interaction.user.id)
        if not can_claim:
            # Calculate time remaining until next claim.
            next_claim = await db.fetchval(
                "SELECT last_daily + INTERVAL '24 hours' FROM market_users WHERE guild_id=$1 AND user_id=$2;",
                guild_id, interaction.user.id,
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
            DAILY_SP, guild_id, interaction.user.id,
        )
        new_balance = await db.fetchval(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, interaction.user.id,
        )
        await db.execute(
            "INSERT INTO market_transactions (guild_id, investor_id, type, total) VALUES ($1,$2,'daily',$3);",
            guild_id, interaction.user.id, DAILY_SP,
        )
    # Update wealth role after balance change.
    new_role = await update_wealth_role(guild_id, interaction.user.id, int(new_balance), pool)
    desc = f"You received **{money(DAILY_SP)}**!\n**New Balance:** {money(int(new_balance))}"
    if new_role:
        desc += f"\n**Wealth Role:** {new_role}"
    await send_embed(interaction, "⭐ Daily Reward Claimed", desc, discord.Color.green())


@bot.tree.command(name="market", description="View the EAS Top 10 stock market.")
async def market(interaction: discord.Interaction):
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    await sync_top10_for_guild(interaction.guild)
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT * FROM market_stocks WHERE guild_id=$1 AND active=true ORDER BY rank_position ASC LIMIT 10;",
            guild_id,
        )
    if not rows:
        no_data_msg = (
            "No simulation data yet. Use `/marketsimulate` to seed fake players."
            if guild_id == TEST_GUILD_ID
            else "No top 10 stocks found yet. Staff can run `/syncmarket` after the ranked database is connected."
        )
        await send_embed(interaction, "📈 EAS Exchange", no_data_msg, discord.Color.red())
        return
    desc = f"*{mode_label(guild_id)}*\n\n"
    for r in rows:
        player_id = int(r["player_id"])
        # For fake players, show their configured name; for real players, resolve member.
        if 9000000001 <= player_id <= 9000000010:
            fake = next((p for p in FAKE_PLAYERS if p["user_id"] == player_id), None)
            name = fake["name"] if fake else f"Test Player {player_id}"
        else:
            member = interaction.guild.get_member(player_id)
            name = member.display_name if member else f"User {player_id}"
        desc += f"**#{r['rank_position']} {name}** — `{money(r['price'])}` | CR: `{r['cr']:,}`\n"
    await send_embed(interaction, "📈 EAS Exchange — Top 10 Market", desc, discord.Color.green())


@bot.tree.command(name="stock", description="View a top 10 player's stock.")
async def stock(interaction: discord.Interaction, user: discord.Member):
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    row = await get_stock(guild_id, user.id, pool)
    if not row:
        await send_embed(interaction, "❌ Not Listed", "That player is not currently in the top 10 market.", discord.Color.red(), True)
        return
    async with pool.acquire() as db:
        hist = await db.fetch(
            "SELECT old_price,new_price,reason FROM market_stock_history WHERE guild_id=$1 AND player_id=$2 ORDER BY created_at DESC LIMIT 5;",
            guild_id, user.id,
        )
    movement = "No recent movement."
    if hist:
        lines = []
        for h in hist:
            diff = h["new_price"] - h["old_price"]
            sign = "+" if diff >= 0 else ""
            lines.append(f"`{sign}{diff:,} SP` — {h['reason']}")
        movement = "\n".join(lines)
    desc = (
        f"**Price:** {money(row['price'])}\n**Top 10 Place:** #{row['rank_position']}\n"
        f"**CR:** {row['cr']:,}\n**Wins:** {row['wins']}\n**Losses:** {row['losses']}\n"
        f"**Kills:** {row['kills']}\n**MVPs:** {row['mvps']}\n**Streak:** {row['streak']}\n\n"
        f"**Recent Movement**\n{movement}"
    )
    await send_embed(interaction, f"📊 {user.display_name} Stock", desc, discord.Color.blue())


@bot.tree.command(name="buy", description="Buy shares of a top 10 player.")
@app_commands.describe(user="The top 10 player whose stock you want to buy", shares="Number of shares to purchase")
async def buy(interaction: discord.Interaction, user: discord.Member, shares: int):
    """Purchase shares of a top 10 player's stock."""
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    if not await market_is_open(guild_id, pool):
        await send_embed(interaction, "🔒 Market Closed", "Trading is currently closed.", discord.Color.red(), True); return
    if shares <= 0:
        await send_embed(interaction, "❌ Invalid Shares", "Shares must be more than 0.", discord.Color.red(), True); return
    if user.id == interaction.user.id:
        await send_embed(interaction, "❌ Not Allowed", "You cannot buy your own stock.", discord.Color.red(), True); return
    stock_row = await get_stock(guild_id, user.id, pool)
    if not stock_row:
        await send_embed(interaction, "❌ Not Listed", "That player is not in the top 10 market.", discord.Color.red(), True); return
    await ensure_user(guild_id, interaction.user.id, pool)
    total_cost = int(stock_row["price"]) * shares
    async with pool.acquire() as db:
        u = await db.fetchrow(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, interaction.user.id,
        )
        if u["balance"] < total_cost:
            await send_embed(interaction, "❌ Not Enough SP", f"You need **{money(total_cost)}** but have **{money(u['balance'])}**.", discord.Color.red(), True); return
        current_total = await total_shares(guild_id, user.id, pool)
        current_owned = safe_int(await db.fetchval(
            "SELECT shares FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;",
            guild_id, interaction.user.id, user.id,
        ))
        if (current_owned + shares) / max(1, current_total + shares) > MAX_OWNERSHIP_PERCENT:
            await send_embed(interaction, "❌ Ownership Limit", "You cannot own more than 25% of one player's market shares.", discord.Color.red(), True); return
        old = await db.fetchrow(
            "SELECT shares,average_price FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;",
            guild_id, interaction.user.id, user.id,
        )
        if old:
            new_shares = old["shares"] + shares
            new_avg = math.floor(((old["shares"] * old["average_price"]) + total_cost) / new_shares)
            await db.execute(
                "UPDATE market_holdings SET shares=$1, average_price=$2 WHERE guild_id=$3 AND investor_id=$4 AND player_id=$5;",
                new_shares, new_avg, guild_id, interaction.user.id, user.id,
            )
        else:
            await db.execute(
                "INSERT INTO market_holdings (guild_id,investor_id,player_id,shares,average_price) VALUES ($1,$2,$3,$4,$5);",
                guild_id, interaction.user.id, user.id, shares, stock_row["price"],
            )
        await db.execute(
            "UPDATE market_users SET balance=balance-$1 WHERE guild_id=$2 AND user_id=$3;",
            total_cost, guild_id, interaction.user.id,
        )
        new_balance = await db.fetchval(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, interaction.user.id,
        )
        await db.execute(
            "INSERT INTO market_transactions (guild_id,investor_id,player_id,type,shares,price,total) VALUES ($1,$2,$3,'buy',$4,$5,$6);",
            guild_id, interaction.user.id, user.id, shares, stock_row["price"], total_cost,
        )
    await update_wealth_role(guild_id, interaction.user.id, int(new_balance), pool)
    await send_embed(
        interaction, "✅ Shares Purchased",
        f"Bought **{shares} shares** of **{user.display_name}** for **{money(total_cost)}**.\n**Remaining Balance:** {money(int(new_balance))}",
        discord.Color.green(),
    )


@bot.tree.command(name="sell", description="Sell shares of a player stock.")
@app_commands.describe(user="The player whose shares you want to sell", shares="Number of shares to sell")
async def sell(interaction: discord.Interaction, user: discord.Member, shares: int):
    """Sell shares of a top 10 player's stock (3% sell tax applies)."""
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    if not await market_is_open(guild_id, pool):
        await send_embed(interaction, "🔒 Market Closed", "Trading is currently closed.", discord.Color.red(), True); return
    if shares <= 0:
        await send_embed(interaction, "❌ Invalid Shares", "Shares must be more than 0.", discord.Color.red(), True); return
    stock_row = await get_stock(guild_id, user.id, pool)
    if not stock_row:
        await send_embed(interaction, "❌ Not Listed", "That player is not in the top 10 market.", discord.Color.red(), True); return
    await ensure_user(guild_id, interaction.user.id, pool)
    async with pool.acquire() as db:
        h = await db.fetchrow(
            "SELECT shares FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;",
            guild_id, interaction.user.id, user.id,
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
                guild_id, interaction.user.id, user.id,
            )
        else:
            await db.execute(
                "UPDATE market_holdings SET shares=$1 WHERE guild_id=$2 AND investor_id=$3 AND player_id=$4;",
                remaining, guild_id, interaction.user.id, user.id,
            )
        await db.execute(
            "UPDATE market_users SET balance=balance+$1 WHERE guild_id=$2 AND user_id=$3;",
            net, guild_id, interaction.user.id,
        )
        new_balance = await db.fetchval(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, interaction.user.id,
        )
        await db.execute(
            "INSERT INTO market_transactions (guild_id,investor_id,player_id,type,shares,price,total) VALUES ($1,$2,$3,'sell',$4,$5,$6);",
            guild_id, interaction.user.id, user.id, shares, stock_row["price"], net,
        )
    await update_wealth_role(guild_id, interaction.user.id, int(new_balance), pool)
    await send_embed(
        interaction, "✅ Shares Sold",
        f"Sold **{shares} shares** of **{user.display_name}** for **{money(net)}** after {int(SELL_TAX*100)}% tax.\n**New Balance:** {money(int(new_balance))}",
        discord.Color.green(),
    )


@bot.tree.command(name="portfolio", description="View your stock portfolio.")
async def portfolio(interaction: discord.Interaction):
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    await ensure_user(guild_id, interaction.user.id, pool)
    async with pool.acquire() as db:
        bal = await db.fetchval(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, interaction.user.id,
        )
        rows = await db.fetch("""
        SELECT h.player_id, h.shares, h.average_price, s.price
        FROM market_holdings h
        JOIN market_stocks s ON h.guild_id=s.guild_id AND h.player_id=s.player_id
        WHERE h.guild_id=$1 AND h.investor_id=$2 AND s.active=true
        ORDER BY h.shares * s.price DESC;
        """, guild_id, interaction.user.id)
    if not rows:
        await send_embed(interaction, "📁 Portfolio", f"Wallet: **{money(bal)}**\nYou do not own any shares yet.", discord.Color.purple())
        return
    desc = ""
    value_total = 0
    for r in rows:
        player_id = int(r["player_id"])
        if 9000000001 <= player_id <= 9000000010:
            fake = next((p for p in FAKE_PLAYERS if p["user_id"] == player_id), None)
            name = fake["name"] if fake else f"Test Player {player_id}"
        else:
            member = interaction.guild.get_member(player_id)
            name = member.display_name if member else f"User {player_id}"
        value = int(r["shares"] * r["price"])
        cost = int(r["shares"] * r["average_price"])
        pl = value - cost
        sign = "+" if pl >= 0 else ""
        value_total += value
        desc += f"**{name}** — {r['shares']} shares\nValue: `{money(value)}` | P/L: `{sign}{pl:,} SP`\n\n"
    embed = discord.Embed(title=f"📁 {interaction.user.display_name}'s Portfolio", description=desc[:3900], color=discord.Color.purple())
    embed.add_field(name="Wallet", value=money(bal))
    embed.add_field(name="Portfolio", value=money(value_total))
    embed.add_field(name="Net Worth", value=money(bal + value_total))
    embed.set_footer(text=mode_label(guild_id))
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="marketleaderboard", description="View the top investors by total net worth.")
async def marketleaderboard(interaction: discord.Interaction):
    """Show the top 10 investors ranked by balance + portfolio value."""
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    async with pool.acquire() as db:
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
        """, guild_id)
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
    embed.set_footer(text=f"Ranked by balance + portfolio value • {mode_label(guild_id)}")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="shop", description="Browse the EAS investor shop.")
@app_commands.describe(page="Page number (each page shows 5 items)")
async def shop(interaction: discord.Interaction, page: int = 1):
    """Display the shop catalogue with purchasable items and badges.

    Items are loaded from market_shop_items.  Users can buy items with /buy
    (future Phase 2 command); this command is read-only browsing.
    """
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    async with pool.acquire() as db:
        total_items = await db.fetchval("SELECT COUNT(*) FROM market_shop_items WHERE active=TRUE;")
        items_per_page = 5
        total_pages = max(1, math.ceil(total_items / items_per_page))
        page = max(1, min(page, total_pages))
        offset = (page - 1) * items_per_page
        rows = await db.fetch(
            "SELECT * FROM market_shop_items WHERE active=TRUE ORDER BY price ASC LIMIT $1 OFFSET $2;",
            items_per_page, offset,
        )

    if not rows:
        await send_embed(interaction, "🛒 EAS Investor Shop", "The shop is currently empty.", discord.Color.blue())
        return

    # Display items with ID for /buyitem reference.
    category_icons = {"badge": "🏅", "title": "📛", "cosmetic": "🎨", "trophy": "🏆"}
    rarity_colors = {"common": "⬜", "uncommon": "🟩", "rare": "🟦", "epic": "🟪",
                     "legendary": "🟨", "mythic": "🟥", "exclusive": "🔶"}
    desc = ""
    for r in rows:
        icon = category_icons.get(r["category"], "🛍️")
        rarity_icon = rarity_colors.get(r["rarity"], "⬜")
        stock_tag = ""
        if r["limited"]:
            stock_left = r["current_stock"] if r["current_stock"] is not None else 0
            stock_tag = f" | 📦 {stock_left} left" if stock_left > 0 else " | ❌ Sold Out"
        desc += f"{icon} {rarity_icon} **[ID: {r['id']}] {r['item_name']}** — `{money(r['price'])}`{stock_tag}\n"
        if r["description"]:
            desc += f"  _{r['description']}_\n"
        desc += "\n"

    embed = discord.Embed(
        title="🛒 EAS Investor Shop",
        description=desc.strip(),
        color=discord.Color.blue(),
    )
    embed.set_footer(text=f"Page {page}/{total_pages} • Use /buyitem <item_name> to purchase • /shop page:{page+1} for more")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="topstocks", description="View highest priced top 10 stocks.")
async def topstocks(interaction: discord.Interaction):
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT * FROM market_stocks WHERE guild_id=$1 AND active=true ORDER BY price DESC LIMIT 10;",
            guild_id,
        )
    desc = ""
    for i, r in enumerate(rows, start=1):
        player_id = int(r["player_id"])
        if 9000000001 <= player_id <= 9000000010:
            fake = next((p for p in FAKE_PLAYERS if p["user_id"] == player_id), None)
            name = fake["name"] if fake else f"Test Player {player_id}"
        else:
            member = interaction.guild.get_member(player_id)
            name = member.display_name if member else f"User {player_id}"
        desc += f"**#{i} {name}** — `{money(r['price'])}` | Top 10 Place: `{r['rank_position']}`\n"
    await send_embed(interaction, "💹 Top Stocks", desc or "No stocks listed.", discord.Color.green())


async def movement_board(guild_id: int, positive: bool, pool: asyncpg.Pool):
    op = ">" if positive else "<"
    order = "DESC" if positive else "ASC"
    async with pool.acquire() as db:
        return await db.fetch(f"""
        SELECT player_id, old_price, new_price, reason
        FROM market_stock_history
        WHERE guild_id=$1 AND new_price - old_price {op} 0
        ORDER BY (new_price - old_price) {order}, created_at DESC
        LIMIT 10;
        """, guild_id)


@bot.tree.command(name="gainers", description="View recent biggest gaining stocks.")
async def gainers(interaction: discord.Interaction):
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    rows = await movement_board(guild_id, True, pool)
    desc = ""
    for r in rows:
        player_id = int(r["player_id"])
        if 9000000001 <= player_id <= 9000000010:
            fake = next((p for p in FAKE_PLAYERS if p["user_id"] == player_id), None)
            name = fake["name"] if fake else f"Test Player {player_id}"
        else:
            member = interaction.guild.get_member(player_id)
            name = member.display_name if member else f"User {player_id}"
        desc += f"**{name}** `{r['new_price'] - r['old_price']:+,} SP` — {r['reason']}\n"
    await send_embed(interaction, "📈 Biggest Gainers", desc or "No gainers yet.", discord.Color.green())


@bot.tree.command(name="losers", description="View recent biggest losing stocks.")
async def losers(interaction: discord.Interaction):
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    rows = await movement_board(guild_id, False, pool)
    desc = ""
    for r in rows:
        player_id = int(r["player_id"])
        if 9000000001 <= player_id <= 9000000010:
            fake = next((p for p in FAKE_PLAYERS if p["user_id"] == player_id), None)
            name = fake["name"] if fake else f"Test Player {player_id}"
        else:
            member = interaction.guild.get_member(player_id)
            name = member.display_name if member else f"User {player_id}"
        desc += f"**{name}** `{r['new_price'] - r['old_price']:+,} SP` — {r['reason']}\n"
    await send_embed(interaction, "📉 Biggest Losers", desc or "No losers yet.", discord.Color.red())


@bot.tree.command(name="transactions", description="View your recent market transactions.")
async def transactions(interaction: discord.Interaction):
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    async with pool.acquire() as db:
        rows = await db.fetch("""
        SELECT * FROM market_transactions
        WHERE guild_id=$1 AND investor_id=$2
        ORDER BY created_at DESC LIMIT 10;
        """, guild_id, interaction.user.id)
    desc = ""
    for r in rows:
        target = ""
        if r["player_id"]:
            player_id = int(r["player_id"])
            if 9000000001 <= player_id <= 9000000010:
                fake = next((p for p in FAKE_PLAYERS if p["user_id"] == player_id), None)
                target = f" {fake['name'] if fake else f'Test Player {player_id}'}"
            else:
                member = interaction.guild.get_member(player_id)
                target = f" {member.display_name if member else player_id}"
        shares = f" x{r['shares']}" if r["shares"] else ""
        total = money(r["total"] or 0)
        desc += f"`{r['type']}`{target}{shares} — **{total}**\n"
    await send_embed(interaction, "🧾 Recent Transactions", desc or "No transactions yet.", discord.Color.blurple())


@bot.tree.command(name="syncmarket", description="Staff: Sync top 10 market players from ranked database.")
async def syncmarket(interaction: discord.Interaction):
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if guild_id == TEST_GUILD_ID:
        await send_embed(interaction, "⚠️ Test Server", "Use `/marketsimulate` to advance the simulation in the test server.", discord.Color.orange(), True)
        return
    count = await sync_top10_for_guild(interaction.guild)
    await send_embed(interaction, "✅ Market Synced", f"Updated **{count}** top 10 market stocks from the ranked database.", discord.Color.green())


@bot.tree.command(name="marketopen", description="Staff: Open market trading.")
async def marketopen(interaction: discord.Interaction):
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO market_settings (guild_id, market_open) VALUES ($1,true) ON CONFLICT (guild_id) DO UPDATE SET market_open=true, updated_at=NOW();",
            interaction.guild.id,
        )
    await send_embed(interaction, "🔓 Market Open", "Trading is now open.", discord.Color.green())


@bot.tree.command(name="marketclose", description="Staff: Close market trading.")
async def marketclose(interaction: discord.Interaction):
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO market_settings (guild_id, market_open) VALUES ($1,false) ON CONFLICT (guild_id) DO UPDATE SET market_open=false, updated_at=NOW();",
            interaction.guild.id,
        )
    await send_embed(interaction, "🔒 Market Closed", "Trading is now closed.", discord.Color.red())


@bot.tree.command(name="logresult", description="Staff: Manually update stock from ranked performance.")
@app_commands.describe(result="win or loss")
async def logresult(interaction: discord.Interaction, user: discord.Member, result: str, mvp: bool = False, high_kills: bool = False, upset: bool = False):
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    result = result.lower().strip()
    if result not in {"win", "loss"}:
        await send_embed(interaction, "❌ Invalid Result", "Use `win` or `loss`.", discord.Color.red(), True); return
    stock_row = await get_stock(guild_id, user.id, pool)
    if not stock_row:
        await send_embed(interaction, "❌ Not Listed", "That player is not in the top 10 market.", discord.Color.red(), True); return
    pct = result_percent(result == "win", mvp, high_kills, upset)
    old = int(stock_row["price"])
    new = max(1000, math.floor(old * (1 + pct)))
    reason = result.upper()
    if mvp: reason += ", MVP"
    if high_kills: reason += ", High Kills"
    if upset: reason += ", Upset"
    old_price, new_price = await update_price(guild_id, user.id, new, reason, pool)
    async with pool.acquire() as db:
        if result == "win":
            await db.execute("UPDATE market_stocks SET wins=wins+1, streak=streak+1, mvps=mvps+$1 WHERE guild_id=$2 AND player_id=$3;", 1 if mvp else 0, guild_id, user.id)
        else:
            await db.execute("UPDATE market_stocks SET losses=losses+1, streak=0, mvps=mvps+$1 WHERE guild_id=$2 AND player_id=$3;", 1 if mvp else 0, guild_id, user.id)
    diff = new_price - old_price
    await send_embed(interaction, "📊 Stock Updated", f"**{user.display_name}**\nOld: `{money(old_price)}`\nNew: `{money(new_price)}`\nChange: `{diff:+,} SP`\nReason: **{reason}**", discord.Color.green() if diff >= 0 else discord.Color.red())


@bot.tree.command(name="freezeportfolio", description="Staff: Record a portfolio freeze action for a user.")
async def freezeportfolio(interaction: discord.Interaction, user: discord.Member):
    """Log a portfolio freeze action in the transaction history."""
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    await ensure_user(interaction.guild.id, user.id, pool)
    async with pool.acquire() as db:
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
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    await ensure_user(interaction.guild.id, user.id, pool)
    async with pool.acquire() as db:
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
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    await ensure_user(guild_id, user.id, pool)
    async with pool.acquire() as db:
        await db.execute("UPDATE market_users SET balance=balance+$1 WHERE guild_id=$2 AND user_id=$3;", amount, guild_id, user.id)
        await db.execute("INSERT INTO market_transactions (guild_id,investor_id,type,total) VALUES ($1,$2,'developer_give',$3);", guild_id, user.id, amount)
    await send_embed(interaction, "✅ StarPoints Given", f"Gave **{money(amount)}** to {user.mention}.", discord.Color.green())


@bot.tree.command(name="takepoints", description="Developer: Take StarPoints from a user.")
async def takepoints(interaction: discord.Interaction, user: discord.Member, amount: int):
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True); return
    if amount <= 0:
        await send_embed(interaction, "❌ Invalid Amount", "Amount must be more than 0.", discord.Color.red(), True); return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    await ensure_user(guild_id, user.id, pool)
    async with pool.acquire() as db:
        await db.execute("UPDATE market_users SET balance=GREATEST(balance-$1,0) WHERE guild_id=$2 AND user_id=$3;", amount, guild_id, user.id)
        await db.execute("INSERT INTO market_transactions (guild_id,investor_id,type,total) VALUES ($1,$2,'developer_take',$3);", guild_id, user.id, amount)
    await send_embed(interaction, "✅ StarPoints Taken", f"Took **{money(amount)}** from {user.mention}.", discord.Color.orange())


@bot.tree.command(name="resetbalance", description="Developer: Reset a user's StarPoints balance.")
async def resetbalance(interaction: discord.Interaction, user: discord.Member, amount: int = STARTING_SP):
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True); return
    if amount < 0:
        await send_embed(interaction, "❌ Invalid Amount", "Amount cannot be negative.", discord.Color.red(), True); return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    await ensure_user(guild_id, user.id, pool)
    async with pool.acquire() as db:
        await db.execute("UPDATE market_users SET balance=$1 WHERE guild_id=$2 AND user_id=$3;", amount, guild_id, user.id)
        await db.execute("INSERT INTO market_transactions (guild_id,investor_id,type,total) VALUES ($1,$2,'developer_reset',$3);", guild_id, user.id, amount)
    await send_embed(interaction, "✅ Balance Reset", f"Set {user.mention}'s balance to **{money(amount)}**.", discord.Color.green())


# ---------------------------------------------------------------------------
# New developer commands: /marketsimulate, /marketresettest, /marketstatus
# ---------------------------------------------------------------------------

@bot.tree.command(name="marketsimulate", description="Developer: Manually trigger market simulation (test server only).")
async def marketsimulate(interaction: discord.Interaction):
    """Advance fake player CRs and update stock prices in the test server."""
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True); return
    if interaction.guild is None or interaction.guild.id != TEST_GUILD_ID:
        await send_embed(interaction, "❌ Test Server Only", "This command only works in the test server.", discord.Color.red(), True); return
    if db_manager.db_pool_test is None:
        await send_embed(interaction, "❌ Test DB Unavailable", "The test database is not connected.", discord.Color.red(), True); return

    await interaction.response.defer()
    try:
        results = await simulate_market_movement(db_manager.db_pool_test)
    except Exception as exc:
        await interaction.followup.send(embed=discord.Embed(
            title="❌ Simulation Error",
            description=str(exc),
            color=discord.Color.red(),
        ))
        return

    if not results:
        await interaction.followup.send(embed=discord.Embed(
            title="⚠️ No Simulation Data",
            description="No fake players found. Run `/marketresettest` to seed them.",
            color=discord.Color.orange(),
        ))
        return

    desc = "**🧪 TEST MODE — Simulation Results**\n\n"
    for r in results:
        fake = next((p for p in FAKE_PLAYERS if p["user_id"] == r["player_id"]), None)
        name = fake["name"] if fake else f"Player {r['player_id']}"
        diff = r["new_price"] - r["old_price"]
        sign = "+" if diff >= 0 else ""
        pos_change = ""
        if r["old_position"] != r["new_position"]:
            pos_change = f" (#{r['old_position']}→#{r['new_position']})"
        desc += f"**{name}**{pos_change}: `{sign}{diff:,} SP` → `{money(r['new_price'])}`\n"

    embed = discord.Embed(title="🧪 Market Simulation Complete", description=desc[:3900], color=discord.Color.teal())
    embed.set_footer(text=f"Simulated {len(results)} fake players")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="marketresettest", description="Developer: Reset the test database to initial state.")
async def marketresettest(interaction: discord.Interaction):
    """Clear all test-server market data and re-seed fake players."""
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True); return
    if interaction.guild is None or interaction.guild.id != TEST_GUILD_ID:
        await send_embed(interaction, "❌ Test Server Only", "This command only works in the test server.", discord.Color.red(), True); return
    if db_manager.db_pool_test is None:
        await send_embed(interaction, "❌ Test DB Unavailable", "The test database is not connected.", discord.Color.red(), True); return

    await interaction.response.defer()
    try:
        await reset_test_database(db_manager.db_pool_test)
    except Exception as exc:
        await interaction.followup.send(embed=discord.Embed(
            title="❌ Reset Error",
            description=str(exc),
            color=discord.Color.red(),
        ))
        return

    desc = (
        "✅ Test database has been reset.\n\n"
        f"**Fake players re-seeded:** {len(FAKE_PLAYERS)}\n"
        "**User balances cleared:** Yes\n"
        "**Holdings cleared:** Yes\n"
        "**Transaction history cleared:** Yes\n"
        "**Price history cleared:** Yes"
    )
    embed = discord.Embed(title="🔄 Test Database Reset", description=desc, color=discord.Color.green())
    embed.set_footer(text="🧪 TEST MODE")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="marketstatus", description="Show current database and market status.")
async def marketstatus(interaction: discord.Interaction):
    """Show mode, database connection status, active stocks, users, and last sync."""
    guild_id = interaction.guild.id if interaction.guild else 0
    current_mode = mode_label(guild_id) if validate_guild(guild_id) else "⚠️ Unknown Server"

    # Check both pools.
    async def _check(pool: Optional[asyncpg.Pool]) -> str:
        if pool is None:
            return "❌ Not connected"
        try:
            async with pool.acquire() as c:
                await c.fetchval("SELECT 1")
            return "✅ Connected"
        except Exception:
            return "⚠️ Error"

    main_status = await _check(db_manager.db_pool_main)
    test_status = await _check(db_manager.db_pool_test)

    # Fetch stats from the guild's own pool (if available).
    active_stocks = 0
    user_count = 0
    last_sync: Optional[str] = None

    if validate_guild(guild_id):
        try:
            pool = await db_manager.get_pool(guild_id)
            async with pool.acquire() as db:
                active_stocks = safe_int(await db.fetchval(
                    "SELECT COUNT(*) FROM market_stocks WHERE guild_id=$1 AND active=true;", guild_id
                ))
                user_count = safe_int(await db.fetchval(
                    "SELECT COUNT(*) FROM market_users WHERE guild_id=$1;", guild_id
                ))
                last_ts = await db.fetchval(
                    "SELECT MAX(updated_at) FROM market_stocks WHERE guild_id=$1;", guild_id
                )
                if last_ts:
                    last_sync = last_ts.strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            pass

    desc = (
        f"**Mode:** {current_mode}\n"
        f"**Main DB:** {main_status}\n"
        f"**Test DB:** {test_status}\n"
        f"**Active Stocks:** {active_stocks}\n"
        f"**Registered Users:** {user_count}\n"
        f"**Last Sync:** {last_sync or 'Never'}"
    )
    embed = discord.Embed(title="📊 Market Status", description=desc, color=discord.Color.blurple())
    embed.set_footer(text=f"Guild: {interaction.guild.name if interaction.guild else 'DM'}")
    await interaction.response.send_message(embed=embed)


# ---------------------------------------------------------------------------
# /marketforceupdate — Developer: force an immediate top-10 sync + simulation
# ---------------------------------------------------------------------------

@bot.tree.command(name="marketforceupdate", description="Developer: Force an immediate market update cycle.")
async def marketforceupdate(interaction: discord.Interaction):
    """Force an immediate top-10 sync (main) or simulation tick (test)."""
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    await interaction.response.defer()
    try:
        if guild_id == TEST_GUILD_ID:
            if db_manager.db_pool_test is None:
                await interaction.followup.send(embed=discord.Embed(title="❌ Test DB Unavailable", color=discord.Color.red()))
                return
            results = await simulate_market_movement(db_manager.db_pool_test)
            desc = f"✅ Simulation tick complete — **{len(results)}** stocks updated."
        else:
            count = await sync_top10_for_guild(interaction.guild)
            desc = f"✅ Top 10 sync complete — **{count}** stocks updated."
        embed = discord.Embed(title="🔄 Market Force Update", description=desc, color=discord.Color.green())
        embed.set_footer(text=mode_label(guild_id))
        await interaction.followup.send(embed=embed)
    except Exception as exc:
        await interaction.followup.send(embed=discord.Embed(
            title="❌ Force Update Error", description=str(exc), color=discord.Color.red()
        ))


# ---------------------------------------------------------------------------
# /synccommands — Developer: manually re-register all slash commands
# ---------------------------------------------------------------------------

@bot.tree.command(name="synccommands", description="Developer: Re-register all slash commands to both guilds.")
async def synccommands(interaction: discord.Interaction):
    """Manually re-sync all slash commands to testing and main guilds.

    Calls register_guild_commands() for the testing guild (with per-command
    logging) then syncs the main guild directly.  Developer-only.
    """
    if not is_developer(interaction.user):
        await interaction.response.send_message("❌ Developer only", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    results: List[str] = []

    # Testing guild — use register_guild_commands() for detailed logging.
    print("[Commands] 🔄 /synccommands triggered — re-registering all commands...")
    test_count = await register_guild_commands()
    results.append(f"✅ **testing** ({TEST_GUILD_ID}): {test_count} commands registered.")

    # Main guild — sync directly.
    main_guild_obj = discord.Object(id=MAIN_GUILD_ID)
    main_guild_in_cache = bot.get_guild(MAIN_GUILD_ID)
    if main_guild_in_cache is None:
        results.append(f"⚠️ **main** ({MAIN_GUILD_ID}): Bot not in server — skipped.")
    else:
        try:
            synced = await bot.tree.sync(guild=main_guild_obj)
            results.append(f"✅ **main** ({MAIN_GUILD_ID}): {len(synced)} commands registered.")
            print(f"[Commands] ✅ Synced {len(synced)} commands to main guild {MAIN_GUILD_ID}")
        except discord.errors.Forbidden:
            results.append(f"❌ **main** ({MAIN_GUILD_ID}): Missing `applications.commands` scope.")
            print(f"[Commands] ❌ Missing applications.commands scope for main guild {MAIN_GUILD_ID}")
        except Exception as exc:
            results.append(f"⚠️ **main** ({MAIN_GUILD_ID}): {exc}")
            print(f"[Commands] ❌ Sync to main guild {MAIN_GUILD_ID} failed: {exc}")

    desc = "\n".join(results)
    embed = discord.Embed(title="🔄 Slash Command Sync", description=desc, color=discord.Color.blurple())
    embed.set_footer(text=f"✅ Synced {test_count} commands to testing guild")
    await interaction.followup.send(embed=embed, ephemeral=True)


# ---------------------------------------------------------------------------
# /starpoints — View your StarPoints balance (alias for /balance)
# ---------------------------------------------------------------------------

@bot.tree.command(name="starpoints", description="View your StarPoints balance and wealth role.")
async def starpoints(interaction: discord.Interaction):
    """Alias for /balance — shows SP balance and wealth role."""
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    await ensure_user(guild_id, interaction.user.id, pool)
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT balance, wealth_role FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, interaction.user.id,
        )
    bal = int(row["balance"])
    role = row["wealth_role"] or "None"
    next_role: Optional[str] = None
    next_threshold: Optional[int] = None
    for role_name, threshold in reversed(WEALTH_ROLES):
        if bal < threshold:
            next_role = role_name
            next_threshold = threshold
    desc = f"**Balance:** {money(bal)}\n**Wealth Role:** {role}\n"
    if next_role and next_threshold:
        needed = next_threshold - bal
        desc += f"**Next Role:** {next_role} (need {money(needed)} more)"
    else:
        desc += "**Status:** 🏆 Maximum wealth role achieved!"
    embed = discord.Embed(title="⭐ StarPoints Balance", description=desc, color=discord.Color.gold())
    embed.set_footer(text=f"Guild: {interaction.guild.name} • {mode_label(guild_id)}")
    await interaction.response.send_message(embed=embed)


# ---------------------------------------------------------------------------
# /starpointprice — View current StarPoint exchange rate info
# ---------------------------------------------------------------------------

@bot.tree.command(name="starpointprice", description="View StarPoint economy info and exchange rates.")
async def starpointprice(interaction: discord.Interaction):
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    desc = (
        f"**Starting Balance:** {money(STARTING_SP)}\n"
        f"**Daily Reward:** {money(DAILY_SP)}\n"
        f"**Sell Tax:** {int(SELL_TAX * 100)}%\n"
        f"**Max Ownership:** {int(MAX_OWNERSHIP_PERCENT * 100)}% per stock\n\n"
        "**Wealth Role Thresholds:**\n"
    )
    for role_name, threshold in WEALTH_ROLES:
        desc += f"• **{role_name}** — {money(threshold)}\n"
    embed = discord.Embed(title="💱 StarPoint Economy Info", description=desc, color=discord.Color.gold())
    embed.set_footer(text=mode_label(interaction.guild.id if interaction.guild else 0))
    await interaction.response.send_message(embed=embed)


# ---------------------------------------------------------------------------
# /buystarpoints — Placeholder (future feature)
# ---------------------------------------------------------------------------

@bot.tree.command(name="buystarpoints", description="Buy StarPoints (coming soon).")
async def buystarpoints(interaction: discord.Interaction):
    await send_embed(
        interaction,
        "🚧 Coming Soon",
        "The ability to purchase StarPoints will be available in a future update.\n\n"
        "For now, earn SP through:\n"
        "• `/daily` — Claim your daily reward\n"
        "• Selling stocks at a profit",
        discord.Color.orange(),
        True,
    )


# ---------------------------------------------------------------------------
# /sellstarpoints — Placeholder (future feature)
# ---------------------------------------------------------------------------

@bot.tree.command(name="sellstarpoints", description="Sell StarPoints (coming soon).")
async def sellstarpoints(interaction: discord.Interaction):
    await send_embed(
        interaction,
        "🚧 Coming Soon",
        "The ability to sell StarPoints will be available in a future update.",
        discord.Color.orange(),
        True,
    )


# ---------------------------------------------------------------------------
# /inventory — View your purchased shop items, badges, and roles
# ---------------------------------------------------------------------------

@bot.tree.command(name="inventory", description="View your badges, roles, and purchased shop items.")
async def inventory(interaction: discord.Interaction):
    """Display all owned badges (from player_badges), Discord roles, and shop items."""
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return

    rarity_icons = {
        "common": "⬜", "uncommon": "🟩", "rare": "🟦", "epic": "🟪",
        "legendary": "🟨", "mythic": "🟥", "exclusive": "🔶",
    }
    category_icons = {"badge": "🏅", "title": "📛", "cosmetic": "🎨", "trophy": "🏆", "role": "👑"}

    # ── Section 1: Badges from player_badges ─────────────────────────────
    async with pool.acquire() as db:
        badge_rows = await db.fetch("""
        SELECT pb.badge_id, pb.awarded_at, pb.source,
               bd.name, bd.rarity, bd.icon_url, bd.color
        FROM player_badges pb
        JOIN badge_definitions bd ON pb.badge_id = bd.id
        WHERE pb.guild_id=$1 AND pb.user_id=$2
        ORDER BY pb.awarded_at DESC;
        """, guild_id, interaction.user.id)

        # ── Section 2: Shop items (non-badge, non-role) ───────────────────
        shop_rows = await db.fetch("""
        SELECT ui.purchased_at, si.item_name, si.description, si.category, si.rarity
        FROM market_user_items ui
        JOIN market_shop_items si ON ui.item_id = si.id
        WHERE ui.guild_id=$1 AND ui.user_id=$2
        ORDER BY ui.purchased_at DESC;
        """, guild_id, interaction.user.id)

    # ── Section 3: Discord roles ──────────────────────────────────────────
    member = interaction.guild.get_member(interaction.user.id)
    if member is None:
        try:
            member = await interaction.guild.fetch_member(interaction.user.id)
        except Exception:
            member = None

    # Filter out @everyone and bot-managed roles.
    discord_roles: List[discord.Role] = []
    if member:
        discord_roles = [
            r for r in member.roles
            if r.name != "@everyone" and not r.managed
        ]
        discord_roles.sort(key=lambda r: r.position, reverse=True)

    total_items = len(badge_rows) + len(discord_roles) + len(shop_rows)

    if total_items == 0:
        await send_embed(
            interaction, "🎒 Inventory",
            "You have no badges, roles, or items yet.\nVisit the `/shop` to browse available items!",
            discord.Color.blue(),
        )
        return

    embed = discord.Embed(
        title=f"🎒 {interaction.user.display_name}'s Inventory",
        color=discord.Color.blue(),
    )

    # Badges field.
    if badge_rows:
        badge_lines = []
        for r in badge_rows:
            rarity_icon = rarity_icons.get(r["rarity"], "⬜")
            date_str = r["awarded_at"].strftime("%Y-%m-%d") if r["awarded_at"] else "?"
            source_tag = f" _{r['source']}_" if r["source"] != "system" else ""
            badge_lines.append(
                f"🏅 {rarity_icon} **{r['name']}** — _{r['rarity']}_{source_tag} | {date_str}"
            )
        embed.add_field(
            name=f"🏅 Badges ({len(badge_rows)})",
            value="\n".join(badge_lines)[:1024],
            inline=False,
        )

    # Discord roles field.
    if discord_roles:
        role_lines = [f"👑 {r.mention}" for r in discord_roles[:20]]
        if len(discord_roles) > 20:
            role_lines.append(f"_...and {len(discord_roles) - 20} more_")
        embed.add_field(
            name=f"👑 Discord Roles ({len(discord_roles)})",
            value="\n".join(role_lines)[:1024],
            inline=False,
        )

    # Other shop items field (titles, cosmetics, trophies).
    other_items = [r for r in shop_rows if r["category"] not in ("badge", "role")]
    if other_items:
        item_lines = []
        for r in other_items:
            icon = category_icons.get(r["category"], "🛍️")
            rarity_icon = rarity_icons.get(r["rarity"], "⬜")
            date_str = r["purchased_at"].strftime("%Y-%m-%d") if r["purchased_at"] else "?"
            item_lines.append(f"{icon} {rarity_icon} **{r['item_name']}** — _{r['rarity']}_ | {date_str}")
        embed.add_field(
            name=f"🛍️ Other Items ({len(other_items)})",
            value="\n".join(item_lines)[:1024],
            inline=False,
        )

    embed.set_footer(text=f"{total_items} total item(s) • {mode_label(guild_id)}")
    await interaction.response.send_message(embed=embed)


# ---------------------------------------------------------------------------
# /buyitem — Purchase an item from the shop
# ---------------------------------------------------------------------------

@bot.tree.command(name="buyitem", description="Purchase an item from the EAS shop by name.")
@app_commands.describe(item_name="The name of the item to purchase (from /shop)")
async def buyitem(interaction: discord.Interaction, item_name: str):
    """Purchase a shop item by name. Handles badge awards and Discord role assignments."""
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    await ensure_user(guild_id, interaction.user.id, pool)

    async with pool.acquire() as db:
        # Look up item by name (case-insensitive).
        item = await db.fetchrow(
            "SELECT * FROM market_shop_items WHERE LOWER(item_name)=LOWER($1) AND active=TRUE;",
            item_name.strip(),
        )
        if not item:
            await send_embed(
                interaction, "❌ Item Not Found",
                f"No active shop item named `{item_name}`. Use `/shop` to browse available items.",
                discord.Color.red(), True,
            )
            return

        item_id = item["id"]

        # Check stock for limited items.
        if item["limited"] and item["current_stock"] is not None and item["current_stock"] <= 0:
            await send_embed(interaction, "❌ Out of Stock",
                             f"**{item['item_name']}** is sold out.", discord.Color.red(), True)
            return

        # Check if already owned in market_user_items.
        already_owned = await db.fetchval(
            "SELECT id FROM market_user_items WHERE guild_id=$1 AND user_id=$2 AND item_id=$3;",
            guild_id, interaction.user.id, item_id,
        )
        if already_owned:
            await send_embed(interaction, "❌ Already Owned",
                             f"You already own **{item['item_name']}**.", discord.Color.orange(), True)
            return

        # Check balance.
        bal = await db.fetchval(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, interaction.user.id,
        )
        if bal is None or bal < item["price"]:
            await send_embed(
                interaction, "❌ Insufficient Funds",
                f"**{item['item_name']}** costs **{money(item['price'])}** but you only have **{money(bal or 0)}**.",
                discord.Color.red(), True,
            )
            return

        # Deduct balance.
        await db.execute(
            "UPDATE market_users SET balance=balance-$1 WHERE guild_id=$2 AND user_id=$3;",
            item["price"], guild_id, interaction.user.id,
        )
        # Record in market_user_items.
        await db.execute(
            "INSERT INTO market_user_items (guild_id, user_id, item_id) VALUES ($1,$2,$3);",
            guild_id, interaction.user.id, item_id,
        )
        # Log transaction.
        await db.execute(
            "INSERT INTO market_transactions (guild_id,investor_id,type,total) VALUES ($1,$2,'shop_purchase',$3);",
            guild_id, interaction.user.id, item["price"],
        )
        # Decrement stock for limited items and increment total_bought.
        if item["limited"] and item["current_stock"] is not None:
            await db.execute(
                "UPDATE market_shop_items SET current_stock=current_stock-1, total_bought=total_bought+1 WHERE id=$1;",
                item_id,
            )
        else:
            await db.execute(
                "UPDATE market_shop_items SET total_bought=total_bought+1 WHERE id=$1;",
                item_id,
            )
        new_bal = await db.fetchval(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, interaction.user.id,
        )

    await update_wealth_role(guild_id, interaction.user.id, int(new_bal), pool)

    # Handle category-specific side effects.
    category = item["category"]
    extra_msg = ""

    if category == "badge":
        badge_id = item["badge_id"]
        if badge_id:
            success, badge_msg = await handle_badge_purchase(guild_id, interaction.user.id, badge_id, pool)
            extra_msg = f"\n\n🏅 **Badge:** {badge_msg}"
        else:
            extra_msg = "\n\n🏅 Badge awarded to your profile!"

    elif category == "role":
        # Derive role name key from item_name (strip trailing _role suffix for display).
        role_key = item["item_name"]
        success, role_msg = await handle_role_purchase(guild_id, interaction.user.id, role_key, pool)
        if success:
            extra_msg = f"\n\n👑 **Role:** {role_msg}"
        else:
            extra_msg = f"\n\n⚠️ **Role:** {role_msg}"

    category_icons = {"badge": "🏅", "title": "📛", "cosmetic": "🎨", "trophy": "🏆", "role": "👑"}
    icon = category_icons.get(category, "🛍️")
    rarity_colors = {
        "common": discord.Color.light_grey(),
        "uncommon": discord.Color.green(),
        "rare": discord.Color.blue(),
        "epic": discord.Color.purple(),
        "legendary": discord.Color.gold(),
        "mythic": discord.Color.red(),
    }
    embed_color = rarity_colors.get(item["rarity"], discord.Color.green())

    desc = (
        f"{icon} **{item['item_name']}** purchased!\n\n"
        f"**Price Paid:** {money(item['price'])}\n"
        f"**New Balance:** {money(int(new_bal))}\n"
        f"**Rarity:** {item['rarity'].capitalize()}"
        f"{extra_msg}\n\n"
        f"_{item['description'] or 'No description.'}_"
    )
    embed = discord.Embed(title="✅ Item Purchased", description=desc, color=embed_color)
    embed.set_footer(text=mode_label(guild_id))
    await interaction.response.send_message(embed=embed)


# ---------------------------------------------------------------------------
# /resellitem — Resell an owned item back to the shop
# ---------------------------------------------------------------------------

@bot.tree.command(name="resellitem", description="Resell an owned item back to the shop for a partial refund.")
@app_commands.describe(item_id="The ID number of the item to resell (from /inventory)")
async def resellitem(interaction: discord.Interaction, item_id: int):
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    if not await is_registered(guild_id, interaction.user.id, pool):
        await send_embed(interaction, "❌ Not Registered", NOT_REGISTERED_DESC, discord.Color.red(), True)
        return
    async with pool.acquire() as db:
        owned = await db.fetchrow(
            "SELECT ui.id as own_id FROM market_user_items ui WHERE ui.guild_id=$1 AND ui.user_id=$2 AND ui.item_id=$3;",
            guild_id, interaction.user.id, item_id,
        )
        if not owned:
            await send_embed(interaction, "❌ Not Owned",
                             f"You don't own item ID `{item_id}`. Use `/inventory` to see your items.", discord.Color.red(), True)
            return
        item = await db.fetchrow("SELECT * FROM market_shop_items WHERE id=$1;", item_id)
        if not item:
            await send_embed(interaction, "❌ Item Not Found", "Item data not found.", discord.Color.red(), True)
            return
        resale_pct = item["resale_percent"] if item["resale_percent"] is not None else 70
        refund = math.floor(item["price"] * resale_pct / 100)
        # Remove from inventory and refund.
        await db.execute(
            "DELETE FROM market_user_items WHERE id=$1;", owned["own_id"]
        )
        await db.execute(
            "UPDATE market_users SET balance=balance+$1 WHERE guild_id=$2 AND user_id=$3;",
            refund, guild_id, interaction.user.id,
        )
        await db.execute(
            "INSERT INTO market_transactions (guild_id,investor_id,type,total) VALUES ($1,$2,'shop_resell',$3);",
            guild_id, interaction.user.id, refund,
        )
        # Restock limited items.
        if item["limited"] and item["current_stock"] is not None:
            await db.execute(
                "UPDATE market_shop_items SET current_stock=current_stock+1 WHERE id=$1;", item_id
            )
        new_bal = await db.fetchval(
            "SELECT balance FROM market_users WHERE guild_id=$1 AND user_id=$2;",
            guild_id, interaction.user.id,
        )
    await update_wealth_role(guild_id, interaction.user.id, int(new_bal), pool)
    desc = (
        f"**{item['item_name']}** has been resold.\n\n"
        f"**Refund ({resale_pct}%):** {money(refund)}\n"
        f"**New Balance:** {money(int(new_bal))}"
    )
    embed = discord.Embed(title="💰 Item Resold", description=desc, color=discord.Color.green())
    embed.set_footer(text=mode_label(guild_id))
    await interaction.response.send_message(embed=embed)


# ---------------------------------------------------------------------------
# /shopadmin — Admin command group for shop management
# ---------------------------------------------------------------------------

# Helper: log a shop audit entry.
async def _shop_audit(pool: asyncpg.Pool, item_id: Optional[int], item_name: Optional[str],
                      action: str, changed_by: int, field: Optional[str] = None,
                      old_value: Optional[str] = None, new_value: Optional[str] = None,
                      note: Optional[str] = None) -> None:
    try:
        async with pool.acquire() as db:
            await db.execute("""
            INSERT INTO market_shop_audit (item_id, item_name, action, changed_by, field, old_value, new_value, note)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
            """, item_id, item_name, action, changed_by, field, old_value, new_value, note)
    except Exception as exc:
        print(f"[ShopAudit] ⚠️  Failed to log audit: {exc}")


def _is_shop_admin(interaction: discord.Interaction) -> bool:
    """Return True if the user is the developer or a staff member."""
    return is_developer(interaction.user) or is_staff(interaction.user)


shopadmin_group = app_commands.Group(
    name="shopadmin",
    description="Shop administration commands (staff/developer only).",
)


@shopadmin_group.command(name="list", description="List all shop items with details.")
async def shopadmin_list(interaction: discord.Interaction):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT * FROM market_shop_items ORDER BY id ASC LIMIT 25;"
        )
    if not rows:
        await send_embed(interaction, "🛒 Shop Items", "No items in the shop.", discord.Color.blue(), True)
        return
    desc = ""
    for r in rows:
        status = "✅" if r["active"] else "❌"
        limited_tag = f" | 📦 {r['current_stock']}/{r['max_stock']}" if r["limited"] else ""
        badge_tag = f" | 🏅 `{r['badge_id']}`" if r["badge_id"] else ""
        role_tag = f" | 👥 <@&{r['role_id']}>" if r["role_id"] else ""
        desc += (
            f"{status} **[{r['id']}] {r['item_name']}** — `{money(r['price'])}` | "
            f"_{r['rarity']}_ | {r['category']}{limited_tag}{badge_tag}{role_tag}\n"
        )
    embed = discord.Embed(title="🛒 Shop Admin — Item List", description=desc[:3900], color=discord.Color.blue())
    embed.set_footer(text=f"{len(rows)} items shown (max 25)")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@shopadmin_group.command(name="create", description="Create a new shop item.")
@app_commands.describe(
    name="Item name",
    description="Item description",
    price="Price in StarPoints",
    category="Category: badge, title, cosmetic, trophy",
    rarity="Rarity: common, uncommon, rare, epic, legendary, mythic, exclusive",
    limited="Is this a limited stock item?",
    max_stock="Maximum stock (for limited items)",
    badge_id="Badge ID to award (from badge_manifest.json)",
    role_id="Discord role ID to award",
)
async def shopadmin_create(
    interaction: discord.Interaction,
    name: str,
    price: int,
    category: str,
    description: str = "",
    rarity: str = "common",
    limited: bool = False,
    max_stock: Optional[int] = None,
    badge_id: Optional[str] = None,
    role_id: Optional[str] = None,
):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    valid_categories = {"badge", "title", "cosmetic", "trophy"}
    valid_rarities = {"common", "uncommon", "rare", "epic", "legendary", "mythic", "exclusive"}
    if category not in valid_categories:
        await send_embed(interaction, "❌ Invalid Category",
                         f"Category must be one of: {', '.join(valid_categories)}", discord.Color.red(), True)
        return
    if rarity not in valid_rarities:
        await send_embed(interaction, "❌ Invalid Rarity",
                         f"Rarity must be one of: {', '.join(valid_rarities)}", discord.Color.red(), True)
        return
    role_id_int: Optional[int] = None
    if role_id:
        try:
            role_id_int = int(role_id)
        except ValueError:
            await send_embed(interaction, "❌ Invalid Role ID", "Role ID must be a number.", discord.Color.red(), True)
            return
    current_stock = max_stock if limited and max_stock else None
    try:
        async with pool.acquire() as db:
            new_id = await db.fetchval("""
            INSERT INTO market_shop_items
                (item_name, description, price, category, rarity, active, limited, max_stock, current_stock, badge_id, role_id)
            VALUES ($1,$2,$3,$4,$5,TRUE,$6,$7,$8,$9,$10)
            RETURNING id;
            """, name, description, price, category, rarity, limited, max_stock, current_stock, badge_id, role_id_int)
    except Exception as exc:
        await send_embed(interaction, "❌ Create Failed", str(exc), discord.Color.red(), True)
        return
    await _shop_audit(pool, new_id, name, "create", interaction.user.id,
                      note=f"price={price}, category={category}, rarity={rarity}, limited={limited}")
    desc = (
        f"**ID:** `{new_id}`\n"
        f"**Name:** {name}\n"
        f"**Price:** {money(price)}\n"
        f"**Category:** {category}\n"
        f"**Rarity:** {rarity}\n"
        f"**Limited:** {'Yes' if limited else 'No'}"
    )
    if limited and max_stock:
        desc += f"\n**Max Stock:** {max_stock}"
    if badge_id:
        desc += f"\n**Badge ID:** `{badge_id}`"
    if role_id_int:
        desc += f"\n**Role:** <@&{role_id_int}>"
    embed = discord.Embed(title="✅ Shop Item Created", description=desc, color=discord.Color.green())
    await interaction.response.send_message(embed=embed, ephemeral=True)


@shopadmin_group.command(name="edit", description="Edit an existing shop item's details.")
@app_commands.describe(
    item_id="Item ID to edit",
    name="New name",
    description="New description",
    rarity="New rarity",
    badge_id="New badge ID",
    role_id="New role ID",
)
async def shopadmin_edit(
    interaction: discord.Interaction,
    item_id: int,
    name: Optional[str] = None,
    description: Optional[str] = None,
    rarity: Optional[str] = None,
    badge_id: Optional[str] = None,
    role_id: Optional[str] = None,
):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        item = await db.fetchrow("SELECT * FROM market_shop_items WHERE id=$1;", item_id)
    if not item:
        await send_embed(interaction, "❌ Not Found", f"No item with ID `{item_id}`.", discord.Color.red(), True)
        return
    valid_rarities = {"common", "uncommon", "rare", "epic", "legendary", "mythic", "exclusive"}
    if rarity and rarity not in valid_rarities:
        await send_embed(interaction, "❌ Invalid Rarity",
                         f"Rarity must be one of: {', '.join(valid_rarities)}", discord.Color.red(), True)
        return
    role_id_int: Optional[int] = None
    if role_id:
        try:
            role_id_int = int(role_id)
        except ValueError:
            await send_embed(interaction, "❌ Invalid Role ID", "Role ID must be a number.", discord.Color.red(), True)
            return
    changes: List[str] = []
    async with pool.acquire() as db:
        if name and name != item["item_name"]:
            await db.execute("UPDATE market_shop_items SET item_name=$1, updated_at=NOW() WHERE id=$2;", name, item_id)
            await _shop_audit(pool, item_id, name, "edit", interaction.user.id, "item_name", item["item_name"], name)
            changes.append(f"Name: `{item['item_name']}` → `{name}`")
        if description is not None and description != item["description"]:
            await db.execute("UPDATE market_shop_items SET description=$1, updated_at=NOW() WHERE id=$2;", description, item_id)
            await _shop_audit(pool, item_id, item["item_name"], "edit", interaction.user.id, "description", item["description"], description)
            changes.append("Description updated.")
        if rarity and rarity != item["rarity"]:
            await db.execute("UPDATE market_shop_items SET rarity=$1, updated_at=NOW() WHERE id=$2;", rarity, item_id)
            await _shop_audit(pool, item_id, item["item_name"], "edit", interaction.user.id, "rarity", item["rarity"], rarity)
            changes.append(f"Rarity: `{item['rarity']}` → `{rarity}`")
        if badge_id is not None and badge_id != item["badge_id"]:
            await db.execute("UPDATE market_shop_items SET badge_id=$1, updated_at=NOW() WHERE id=$2;", badge_id or None, item_id)
            await _shop_audit(pool, item_id, item["item_name"], "edit", interaction.user.id, "badge_id", item["badge_id"], badge_id)
            changes.append(f"Badge ID: `{item['badge_id']}` → `{badge_id}`")
        if role_id is not None and role_id_int != item["role_id"]:
            await db.execute("UPDATE market_shop_items SET role_id=$1, updated_at=NOW() WHERE id=$2;", role_id_int, item_id)
            await _shop_audit(pool, item_id, item["item_name"], "edit", interaction.user.id, "role_id", str(item["role_id"]), str(role_id_int))
            changes.append(f"Role ID: `{item['role_id']}` → `{role_id_int}`")
    if not changes:
        await send_embed(interaction, "ℹ️ No Changes", "No fields were changed.", discord.Color.orange(), True)
        return
    desc = f"**Item [{item_id}] {item['item_name']}** updated:\n\n" + "\n".join(f"• {c}" for c in changes)
    embed = discord.Embed(title="✅ Item Edited", description=desc, color=discord.Color.green())
    await interaction.response.send_message(embed=embed, ephemeral=True)


@shopadmin_group.command(name="stock", description="View current stock and max stock for an item.")
@app_commands.describe(item_id="Item ID to check")
async def shopadmin_stock(interaction: discord.Interaction, item_id: int):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        item = await db.fetchrow("SELECT * FROM market_shop_items WHERE id=$1;", item_id)
    if not item:
        await send_embed(interaction, "❌ Not Found", f"No item with ID `{item_id}`.", discord.Color.red(), True)
        return
    if not item["limited"]:
        desc = f"**{item['item_name']}** is **unlimited** — no stock tracking."
    else:
        current = item["current_stock"] if item["current_stock"] is not None else "N/A"
        max_s = item["max_stock"] if item["max_stock"] is not None else "N/A"
        desc = (
            f"**Item:** {item['item_name']}\n"
            f"**Current Stock:** {current}\n"
            f"**Max Stock:** {max_s}\n"
            f"**Status:** {'✅ In Stock' if (item['current_stock'] or 0) > 0 else '❌ Out of Stock'}"
        )
    embed = discord.Embed(title=f"📦 Stock — [{item_id}] {item['item_name']}", description=desc, color=discord.Color.blue())
    await interaction.response.send_message(embed=embed, ephemeral=True)


@shopadmin_group.command(name="restock", description="Set max stock and current stock for a limited item.")
@app_commands.describe(item_id="Item ID to restock", max_stock="New max stock", current_stock="New current stock")
async def shopadmin_restock(interaction: discord.Interaction, item_id: int, max_stock: int, current_stock: int):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        item = await db.fetchrow("SELECT * FROM market_shop_items WHERE id=$1;", item_id)
        if not item:
            await send_embed(interaction, "❌ Not Found", f"No item with ID `{item_id}`.", discord.Color.red(), True)
            return
        await db.execute(
            "UPDATE market_shop_items SET limited=TRUE, max_stock=$1, current_stock=$2, updated_at=NOW() WHERE id=$3;",
            max_stock, current_stock, item_id,
        )
    await _shop_audit(pool, item_id, item["item_name"], "restock", interaction.user.id,
                      note=f"max_stock={max_stock}, current_stock={current_stock}")
    desc = (
        f"**{item['item_name']}** restocked:\n"
        f"**Max Stock:** {max_stock}\n"
        f"**Current Stock:** {current_stock}"
    )
    embed = discord.Embed(title="✅ Item Restocked", description=desc, color=discord.Color.green())
    await interaction.response.send_message(embed=embed, ephemeral=True)


@shopadmin_group.command(name="price", description="Update item price, min value, and max value.")
@app_commands.describe(
    item_id="Item ID to update",
    price="New current price",
    min_value="New minimum value",
    max_value="New maximum value",
)
async def shopadmin_price(
    interaction: discord.Interaction,
    item_id: int,
    price: Optional[int] = None,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        item = await db.fetchrow("SELECT * FROM market_shop_items WHERE id=$1;", item_id)
        if not item:
            await send_embed(interaction, "❌ Not Found", f"No item with ID `{item_id}`.", discord.Color.red(), True)
            return
        changes: List[str] = []
        if price is not None and price != item["price"]:
            old_price = item["price"]
            await db.execute("UPDATE market_shop_items SET price=$1, updated_at=NOW() WHERE id=$2;", price, item_id)
            await db.execute("""
            INSERT INTO market_item_value_history (item_id, old_price, new_price, changed_by, reason)
            VALUES ($1,$2,$3,$4,'admin price update');
            """, item_id, old_price, price, interaction.user.id)
            await _shop_audit(pool, item_id, item["item_name"], "price_change", interaction.user.id,
                              "price", str(old_price), str(price))
            changes.append(f"Price: `{money(old_price)}` → `{money(price)}`")
        if min_value is not None and min_value != item["min_value"]:
            await db.execute("UPDATE market_shop_items SET min_value=$1, updated_at=NOW() WHERE id=$2;", min_value, item_id)
            await _shop_audit(pool, item_id, item["item_name"], "edit", interaction.user.id,
                              "min_value", str(item["min_value"]), str(min_value))
            changes.append(f"Min Value: `{money(min_value)}`")
        if max_value is not None and max_value != item["max_value"]:
            await db.execute("UPDATE market_shop_items SET max_value=$1, updated_at=NOW() WHERE id=$2;", max_value, item_id)
            await _shop_audit(pool, item_id, item["item_name"], "edit", interaction.user.id,
                              "max_value", str(item["max_value"]), str(max_value))
            changes.append(f"Max Value: `{money(max_value)}`")
    if not changes:
        await send_embed(interaction, "ℹ️ No Changes", "No price fields were changed.", discord.Color.orange(), True)
        return
    desc = f"**Item [{item_id}] {item['item_name']}** price updated:\n\n" + "\n".join(f"• {c}" for c in changes)
    embed = discord.Embed(title="✅ Price Updated", description=desc, color=discord.Color.green())
    await interaction.response.send_message(embed=embed, ephemeral=True)


@shopadmin_group.command(name="resale", description="Set the resale percentage for an item.")
@app_commands.describe(item_id="Item ID", percent="Resale percent (0–100)")
async def shopadmin_resale(interaction: discord.Interaction, item_id: int, percent: int):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    if not 0 <= percent <= 100:
        await send_embed(interaction, "❌ Invalid Percent", "Resale percent must be between 0 and 100.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        item = await db.fetchrow("SELECT * FROM market_shop_items WHERE id=$1;", item_id)
        if not item:
            await send_embed(interaction, "❌ Not Found", f"No item with ID `{item_id}`.", discord.Color.red(), True)
            return
        old_pct = item["resale_percent"]
        await db.execute("UPDATE market_shop_items SET resale_percent=$1, updated_at=NOW() WHERE id=$2;", percent, item_id)
    await _shop_audit(pool, item_id, item["item_name"], "edit", interaction.user.id,
                      "resale_percent", str(old_pct), str(percent))
    desc = f"**{item['item_name']}** resale percent: `{old_pct}%` → `{percent}%`"
    embed = discord.Embed(title="✅ Resale Percent Updated", description=desc, color=discord.Color.green())
    await interaction.response.send_message(embed=embed, ephemeral=True)


@shopadmin_group.command(name="enable", description="Enable a disabled shop item.")
@app_commands.describe(item_id="Item ID to enable")
async def shopadmin_enable(interaction: discord.Interaction, item_id: int):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        item = await db.fetchrow("SELECT * FROM market_shop_items WHERE id=$1;", item_id)
        if not item:
            await send_embed(interaction, "❌ Not Found", f"No item with ID `{item_id}`.", discord.Color.red(), True)
            return
        if item["active"]:
            await send_embed(interaction, "ℹ️ Already Active", f"**{item['item_name']}** is already enabled.", discord.Color.orange(), True)
            return
        await db.execute("UPDATE market_shop_items SET active=TRUE, updated_at=NOW() WHERE id=$1;", item_id)
    await _shop_audit(pool, item_id, item["item_name"], "enable", interaction.user.id)
    embed = discord.Embed(
        title="✅ Item Enabled",
        description=f"**{item['item_name']}** is now active and visible in the shop.",
        color=discord.Color.green(),
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


class ConfirmDisableView(discord.ui.View):
    """Confirmation view for disabling a shop item."""

    def __init__(self, item_id: int, item_name: str, pool: asyncpg.Pool, admin_id: int):
        super().__init__(timeout=60)
        self.item_id = item_id
        self.item_name = item_name
        self.pool = pool
        self.admin_id = admin_id
        self.confirmed = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin_id:
            await interaction.response.send_message("Not your confirmation.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="✅ Confirm Disable", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True
        for child in self.children:
            child.disabled = True  # type: ignore[attr-defined]
        await interaction.response.edit_message(view=self)
        async with self.pool.acquire() as db:
            await db.execute("UPDATE market_shop_items SET active=FALSE, updated_at=NOW() WHERE id=$1;", self.item_id)
        await _shop_audit(self.pool, self.item_id, self.item_name, "disable", self.admin_id)
        await interaction.followup.send(
            embed=discord.Embed(
                title="✅ Item Disabled",
                description=f"**{self.item_name}** has been disabled and hidden from the shop.",
                color=discord.Color.orange(),
            ),
            ephemeral=True,
        )
        self.stop()

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True  # type: ignore[attr-defined]
        await interaction.response.edit_message(view=self)
        await interaction.followup.send("Disable cancelled.", ephemeral=True)
        self.stop()


@shopadmin_group.command(name="disable", description="Disable a shop item (hides it from the shop).")
@app_commands.describe(item_id="Item ID to disable")
async def shopadmin_disable(interaction: discord.Interaction, item_id: int):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        item = await db.fetchrow("SELECT * FROM market_shop_items WHERE id=$1;", item_id)
    if not item:
        await send_embed(interaction, "❌ Not Found", f"No item with ID `{item_id}`.", discord.Color.red(), True)
        return
    if not item["active"]:
        await send_embed(interaction, "ℹ️ Already Disabled", f"**{item['item_name']}** is already disabled.", discord.Color.orange(), True)
        return
    view = ConfirmDisableView(item_id, item["item_name"], pool, interaction.user.id)
    embed = discord.Embed(
        title="⚠️ Confirm Disable",
        description=f"Are you sure you want to disable **{item['item_name']}**?\nIt will be hidden from the shop.",
        color=discord.Color.orange(),
    )
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class ConfirmDeleteView(discord.ui.View):
    """Confirmation view for deleting a shop item."""

    def __init__(self, item_id: int, item_name: str, pool: asyncpg.Pool, admin_id: int):
        super().__init__(timeout=60)
        self.item_id = item_id
        self.item_name = item_name
        self.pool = pool
        self.admin_id = admin_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.admin_id:
            await interaction.response.send_message("Not your confirmation.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="🗑️ Confirm Delete", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True  # type: ignore[attr-defined]
        await interaction.response.edit_message(view=self)
        async with self.pool.acquire() as db:
            await db.execute("DELETE FROM market_shop_items WHERE id=$1;", self.item_id)
        await _shop_audit(self.pool, self.item_id, self.item_name, "delete", self.admin_id)
        await interaction.followup.send(
            embed=discord.Embed(
                title="🗑️ Item Deleted",
                description=f"**{self.item_name}** has been permanently deleted from the shop.",
                color=discord.Color.red(),
            ),
            ephemeral=True,
        )
        self.stop()

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True  # type: ignore[attr-defined]
        await interaction.response.edit_message(view=self)
        await interaction.followup.send("Delete cancelled.", ephemeral=True)
        self.stop()


@shopadmin_group.command(name="delete", description="Permanently delete a shop item.")
@app_commands.describe(item_id="Item ID to delete")
async def shopadmin_delete(interaction: discord.Interaction, item_id: int):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        item = await db.fetchrow("SELECT * FROM market_shop_items WHERE id=$1;", item_id)
    if not item:
        await send_embed(interaction, "❌ Not Found", f"No item with ID `{item_id}`.", discord.Color.red(), True)
        return
    view = ConfirmDeleteView(item_id, item["item_name"], pool, interaction.user.id)
    embed = discord.Embed(
        title="⚠️ Confirm Delete",
        description=(
            f"Are you sure you want to **permanently delete** **{item['item_name']}**?\n\n"
            "⚠️ This cannot be undone. Users who own this item will keep it in their inventory."
        ),
        color=discord.Color.red(),
    )
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


@shopadmin_group.command(name="history", description="View value history and audit log for a shop item.")
@app_commands.describe(item_id="Item ID to view history for")
async def shopadmin_history(interaction: discord.Interaction, item_id: int):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    async with pool.acquire() as db:
        item = await db.fetchrow("SELECT * FROM market_shop_items WHERE id=$1;", item_id)
        if not item:
            await send_embed(interaction, "❌ Not Found", f"No item with ID `{item_id}`.", discord.Color.red(), True)
            return
        price_hist = await db.fetch("""
        SELECT old_price, new_price, changed_by, reason, created_at
        FROM market_item_value_history WHERE item_id=$1
        ORDER BY created_at DESC LIMIT 10;
        """, item_id)
        audit_hist = await db.fetch("""
        SELECT action, field, old_value, new_value, changed_by, note, created_at
        FROM market_shop_audit WHERE item_id=$1
        ORDER BY created_at DESC LIMIT 10;
        """, item_id)
    embed = discord.Embed(
        title=f"📜 History — [{item_id}] {item['item_name']}",
        color=discord.Color.blurple(),
    )
    # Price history.
    if price_hist:
        ph_lines = []
        for r in price_hist:
            diff = r["new_price"] - r["old_price"]
            sign = "+" if diff >= 0 else ""
            date_str = r["created_at"].strftime("%m/%d %H:%M") if r["created_at"] else "?"
            ph_lines.append(f"`{date_str}` `{sign}{diff:,} SP` → `{money(r['new_price'])}` by <@{r['changed_by']}>")
        embed.add_field(name="💰 Price History", value="\n".join(ph_lines)[:1024], inline=False)
    else:
        embed.add_field(name="💰 Price History", value="No price changes recorded.", inline=False)
    # Audit log.
    if audit_hist:
        al_lines = []
        for r in audit_hist:
            date_str = r["created_at"].strftime("%m/%d %H:%M") if r["created_at"] else "?"
            field_info = f" `{r['field']}`" if r["field"] else ""
            note_info = f" — {r['note']}" if r["note"] else ""
            al_lines.append(f"`{date_str}` **{r['action']}**{field_info} by <@{r['changed_by']}>{note_info}")
        embed.add_field(name="📋 Audit Log", value="\n".join(al_lines)[:1024], inline=False)
    else:
        embed.add_field(name="📋 Audit Log", value="No audit entries.", inline=False)
    embed.set_footer(text=f"Current price: {money(item['price'])} | Rarity: {item['rarity']}")
    await interaction.response.send_message(embed=embed, ephemeral=True)


# Register the shopadmin group with the bot's command tree.
bot.tree.add_command(shopadmin_group)


# ---------------------------------------------------------------------------
# Badge admin helpers — /badgeadmin (developer only)
# ---------------------------------------------------------------------------

badgeadmin_group = app_commands.Group(
    name="badgeadmin",
    description="Badge administration commands (developer only).",
)


@badgeadmin_group.command(name="award", description="Award a badge to a user.")
@app_commands.describe(user="User to award the badge to", badge_id="Badge ID from badge_manifest.json")
async def badgeadmin_award(interaction: discord.Interaction, user: discord.Member, badge_id: str):
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    async with pool.acquire() as db:
        badge = await db.fetchrow("SELECT * FROM badge_definitions WHERE id=$1;", badge_id)
        if not badge:
            await send_embed(interaction, "❌ Badge Not Found",
                             f"No badge with ID `{badge_id}`. Check badge_manifest.json.", discord.Color.red(), True)
            return
        try:
            await db.execute("""
            INSERT INTO player_badges (guild_id, user_id, badge_id, awarded_by, source)
            VALUES ($1,$2,$3,$4,'admin')
            ON CONFLICT (guild_id, user_id, badge_id) DO NOTHING;
            """, guild_id, user.id, badge_id, interaction.user.id)
        except Exception as exc:
            await send_embed(interaction, "❌ Award Failed", str(exc), discord.Color.red(), True)
            return
    desc = (
        f"**Badge:** {badge['name']} (`{badge_id}`)\n"
        f"**Awarded to:** {user.mention}\n"
        f"**Rarity:** {badge['rarity']}\n"
        f"**Category:** {badge['category']}"
    )
    embed = discord.Embed(title="🏅 Badge Awarded", description=desc, color=discord.Color.gold())
    await interaction.response.send_message(embed=embed)


@badgeadmin_group.command(name="revoke", description="Revoke a badge from a user.")
@app_commands.describe(user="User to revoke the badge from", badge_id="Badge ID to revoke")
async def badgeadmin_revoke(interaction: discord.Interaction, user: discord.Member, badge_id: str):
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    async with pool.acquire() as db:
        result = await db.execute(
            "DELETE FROM player_badges WHERE guild_id=$1 AND user_id=$2 AND badge_id=$3;",
            guild_id, user.id, badge_id,
        )
    if result == "DELETE 0":
        await send_embed(interaction, "ℹ️ Not Found",
                         f"{user.mention} does not have badge `{badge_id}`.", discord.Color.orange(), True)
        return
    embed = discord.Embed(
        title="🗑️ Badge Revoked",
        description=f"Badge `{badge_id}` revoked from {user.mention}.",
        color=discord.Color.orange(),
    )
    await interaction.response.send_message(embed=embed)


@badgeadmin_group.command(name="list", description="List all badges a user has.")
@app_commands.describe(user="User to check badges for")
async def badgeadmin_list(interaction: discord.Interaction, user: discord.Member):
    if not _is_shop_admin(interaction):
        await send_embed(interaction, "❌ No Permission", "Staff or developer only.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    async with pool.acquire() as db:
        rows = await db.fetch("""
        SELECT pb.badge_id, pb.awarded_at, pb.source, bd.name, bd.rarity, bd.category
        FROM player_badges pb
        JOIN badge_definitions bd ON pb.badge_id = bd.id
        WHERE pb.guild_id=$1 AND pb.user_id=$2
        ORDER BY pb.awarded_at DESC;
        """, guild_id, user.id)
    if not rows:
        await send_embed(interaction, "🏅 Badges", f"{user.mention} has no badges.", discord.Color.blue(), True)
        return
    rarity_icons = {"common": "⬜", "uncommon": "🟩", "rare": "🟦", "epic": "🟪",
                    "legendary": "🟨", "mythic": "🟥", "exclusive": "🔶"}
    desc = ""
    for r in rows:
        icon = rarity_icons.get(r["rarity"], "⬜")
        date_str = r["awarded_at"].strftime("%Y-%m-%d") if r["awarded_at"] else "?"
        desc += f"{icon} **{r['name']}** (`{r['badge_id']}`) — _{r['rarity']}_ | {r['source']} | {date_str}\n"
    embed = discord.Embed(
        title=f"🏅 {user.display_name}'s Badges ({len(rows)})",
        description=desc[:3900],
        color=discord.Color.gold(),
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@badgeadmin_group.command(name="cleanup", description="Developer: Remove legacy badges not in badge_definitions.")
@app_commands.describe(user="Specific user to clean up (leave empty for all users)")
async def badgeadmin_cleanup(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    """Remove legacy/invalid badge entries from player_badges.

    For user saku (1257212661817671761): removes Content Creator and Tournament Winner,
    keeps R6 SuperStar Low and Ranked (if they exist in badge_definitions).
    """
    if not is_developer(interaction.user):
        await send_embed(interaction, "❌ Developer Only", "Only the developer can use this command.", discord.Color.red(), True)
        return
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    guild_id = interaction.guild.id
    await interaction.response.defer(ephemeral=True)
    async with pool.acquire() as db:
        if user:
            # Remove badges for a specific user that are not in badge_definitions.
            removed = await db.fetch("""
            DELETE FROM player_badges
            WHERE guild_id=$1 AND user_id=$2
              AND badge_id NOT IN (SELECT id FROM badge_definitions)
            RETURNING badge_id, user_id;
            """, guild_id, user.id)
        else:
            # Remove all orphaned badge entries across all users.
            removed = await db.fetch("""
            DELETE FROM player_badges
            WHERE guild_id=$1
              AND badge_id NOT IN (SELECT id FROM badge_definitions)
            RETURNING badge_id, user_id;
            """, guild_id)
    if not removed:
        await interaction.followup.send(
            embed=discord.Embed(title="✅ No Cleanup Needed",
                                description="All badge entries are valid.", color=discord.Color.green()),
            ephemeral=True,
        )
        return
    lines = [f"• <@{r['user_id']}> — `{r['badge_id']}`" for r in removed[:20]]
    if len(removed) > 20:
        lines.append(f"... and {len(removed) - 20} more.")
    desc = f"Removed **{len(removed)}** legacy badge(s):\n\n" + "\n".join(lines)
    embed = discord.Embed(title="🧹 Badge Cleanup Complete", description=desc, color=discord.Color.orange())
    await interaction.followup.send(embed=embed, ephemeral=True)


bot.tree.add_command(badgeadmin_group)


# ---------------------------------------------------------------------------
# /marketcommands — Paginated command reference (6 pages)
# ---------------------------------------------------------------------------

@bot.tree.command(name="marketcommands", description="View EAS stock market commands by page.")
@app_commands.describe(page="Page number: 1 Economy, 2 Market, 3 Trading, 4 Staff, 5 Developer, 6 Shop Admin")
async def marketcommands_updated(interaction: discord.Interaction, page: int = 1):
    """Paginated command reference for the EAS Stock Market Bot."""
    pages = {
        1: (
            "📘 Economy Commands",
            "`/register` — Accept Terms & Conditions and register\n"
            "`/ping` — Bot status, latency & uptime\n"
            "`/balance` — View your SP balance & wealth role\n"
            "`/starpoints` — View your StarPoints balance\n"
            "`/starpointprice` — View economy rates & wealth thresholds\n"
            "`/daily` — Claim your daily $50,000 SP reward\n"
            "`/marketcommands` — This command list\n\n"
            "**Starting Balance:** $250,000 SP\n"
            "**Daily Reward:** $50,000 SP\n\n"
            "**Note:** All market commands require registration via `/register`",
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
            "💹 Trading & Shop Commands",
            "`/buy <player> <shares>` — Buy shares of a top 10 player\n"
            "`/sell <player> <shares>` — Sell shares (3% tax)\n"
            "`/buyitem <item_name>` — Purchase an item from the shop by name\n"
            "`/resellitem <item_id>` — Resell an item for a partial refund\n"
            "`/inventory` — View your badges, roles, and purchased items\n"
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
            "`/resetbalance` — Reset a user's balance\n"
            "`/marketsimulate` — Trigger test-server simulation (test only)\n"
            "`/marketresettest` — Reset test database (test only)\n"
            "`/marketstatus` — Show database & mode status\n"
            "`/marketforceupdate` — Force immediate market update\n"
            "`/synccommands` — Re-register all slash commands\n\n"
            f"Only developer ID `{DEVELOPER_USER_ID}` can use these.",
        ),
        6: (
            "🛒 Shop Admin Commands",
            "`/shopadmin list` — List all shop items\n"
            "`/shopadmin create` — Create a new shop item\n"
            "`/shopadmin edit` — Edit item name, description, rarity, badge/role\n"
            "`/shopadmin stock` — View current stock levels\n"
            "`/shopadmin restock` — Set max and current stock\n"
            "`/shopadmin price` — Update price, min value, max value\n"
            "`/shopadmin resale` — Set resale percentage\n"
            "`/shopadmin enable` — Enable a disabled item\n"
            "`/shopadmin disable` — Disable an item (with confirmation)\n"
            "`/shopadmin delete` — Delete an item (with confirmation)\n"
            "`/shopadmin history` — View price history & audit log\n\n"
            "`/badgeadmin award` — Award a badge to a user\n"
            "`/badgeadmin revoke` — Revoke a badge from a user\n"
            "`/badgeadmin list` — List a user's badges\n"
            "`/badgeadmin cleanup` — Remove legacy badge data",
        ),
    }
    page = max(1, min(6, page))
    title, desc = pages[page]
    embed = discord.Embed(title=title, description=desc, color=discord.Color.gold())
    embed.set_footer(text=f"Page {page}/6 • Use /marketcommands page:{page % 6 + 1} for next page")
    await interaction.response.send_message(embed=embed)


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN is missing.")
    bot.run(DISCORD_TOKEN)
