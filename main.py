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

# ---------------------------------------------------------------------------
# Terms & Conditions — v1.0
# ---------------------------------------------------------------------------
TERMS_VERSION = "1.0"
TERMS_AND_CONDITIONS = """
📋 **EAS STOCK MARKET — TERMS & CONDITIONS**

By registering for the EAS Stock Market, you agree to:

**1. MARKET PARTICIPATION**
• This is a simulated market for testing purposes
• Prices are determined by player rankings and market simulation
• Your balance starts at $250,000 StarPoints (virtual currency)
• You may trade, buy, and sell stocks freely

**2. RISK DISCLAIMER**
• Stock prices can increase or decrease based on player performance
• You may lose StarPoints through trading
• Past performance does not guarantee future results
• The market may be reset for testing purposes without notice

**3. FAIR PLAY & CONDUCT**
• No exploiting bugs or glitches
• No market manipulation or collusion
• No harassment of other players
• Violations may result in account suspension or ban

**4. DATA & PRIVACY**
• Your trading data is stored in the database
• Data may be cleared during testing phases
• We do not sell or share your personal data
• Your Discord ID is used for account identification

**5. COMPLIANCE**
• You agree to follow Discord's Terms of Service
• You agree to follow server rules and staff decisions
• Staff decisions regarding violations are final
• The market may be modified or shut down at any time

**6. ACKNOWLEDGMENT**
By clicking **"I Agree"**, you confirm:
✓ You have read these terms
✓ You understand the risks
✓ You agree to follow all rules
✓ You are 13+ years old (Discord requirement)

Last Updated: 2026-06-04 | Version: 1.0
"""

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)
# Legacy single-pool reference — kept for backward compatibility with helper
# functions that haven't been migrated to pool-parameter style yet.
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
        # market_registrations — tracks users who have agreed to T&C.
        # ------------------------------------------------------------------ #
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_registrations (
            id              SERIAL,
            guild_id        BIGINT NOT NULL,
            user_id         BIGINT NOT NULL,
            registered_at   TIMESTAMP DEFAULT NOW(),
            terms_agreed_at TIMESTAMP,
            terms_version   TEXT DEFAULT '1.0',
            PRIMARY KEY (guild_id, user_id)
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
            print(f"[Shop/{mode}] ✅ Seeded {seeded} new shop items.")
        else:
            print(f"[Shop/{mode}] ✅ Shop items already present — no seeding needed.")

    print(f"[DB/{mode}] ✅ All market_ tables verified/created.")


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


@bot.event
async def on_ready():
    """Discord on_ready handler — runs once after the bot logs in.

    Initialises both database pools, starts the Top 10 sync loop, and syncs
    slash commands to both servers.  Comprehensive startup logs are printed
    so Railway deployment logs are easy to read.
    """
    global bot_start_time, db_pool
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

    # Registration system is always active once tables are initialised.
    print("[Startup] ✅ Registration system initialized")
    print(f"[Startup] ✅ Terms & Conditions loaded (v{TERMS_VERSION})")

    # Start the background Top 10 sync / simulation loop.
    if not top10_sync_loop.is_running():
        top10_sync_loop.start()
        print(f"[Startup] ✅ Market auto-update loop started (every {TOP10_SYNC_MINUTES} min)")
    else:
        print("[Startup] ℹ️  Market auto-update loop already running.")

    # Sync slash commands to both servers.
    synced_count = 0
    for guild_id in (TEST_GUILD_ID, MAIN_GUILD_ID):
        guild_obj = discord.Object(id=guild_id)
        try:
            synced = await bot.tree.sync(guild=guild_obj)
            synced_count = len(synced)
            label = "testing" if guild_id == TEST_GUILD_ID else "main"
            print(f"[Startup] ✅ Synced {synced_count} slash commands to {label} server ({guild_id})")
        except Exception as exc:
            print(f"[Startup] ⚠️  Command sync failed for guild {guild_id}: {exc}")

    print("=" * 60)
    print("[Startup] ✅ EAS Stock Market Bot is READY")
    print("=" * 60)


async def send_embed(interaction: discord.Interaction, title: str, description: str, color=discord.Color.blurple(), ephemeral=False):
    embed = discord.Embed(title=title, description=description, color=color)
    await interaction.response.send_message(embed=embed, ephemeral=ephemeral)


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


async def check_registration(interaction: discord.Interaction, pool: asyncpg.Pool) -> bool:
    """Check if user is registered and has agreed to T&C.

    Returns True if registered, False (after sending an error embed) otherwise.
    """
    guild_id = interaction.guild.id
    user_id = interaction.user.id
    async with pool.acquire() as db:
        registered = await db.fetchval(
            "SELECT user_id FROM market_registrations WHERE guild_id=$1 AND user_id=$2;",
            guild_id, user_id,
        )
    if not registered:
        await send_embed(
            interaction,
            "❌ Not Registered",
            "You must register first using `/register` to access the market.\n\n"
            "Use `/register` to view the Terms & Conditions and create your account.",
            discord.Color.red(),
            True,
        )
        return False
    return True


class TermsButtons(discord.ui.View):
    """Button view for the Terms & Conditions agreement flow."""

    def __init__(self, user_id: int, pool: asyncpg.Pool, guild_id: int) -> None:
        super().__init__(timeout=300)  # 5-minute window to respond.
        self.user_id = user_id
        self.pool = pool
        self.guild_id = guild_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only the user who triggered /register may click the buttons."""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "These buttons are not for you.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="I Agree", style=discord.ButtonStyle.green, emoji="✅")
    async def agree_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        """Register the user and initialise their market account."""
        try:
            async with self.pool.acquire() as db:
                # Check for duplicate registration (race condition guard).
                existing = await db.fetchval(
                    "SELECT user_id FROM market_registrations WHERE guild_id=$1 AND user_id=$2;",
                    self.guild_id, self.user_id,
                )
                if existing:
                    await interaction.response.edit_message(
                        embed=discord.Embed(
                            title="ℹ️ Already Registered",
                            description="You are already registered for the EAS Stock Market!",
                            color=discord.Color.orange(),
                        ),
                        view=None,
                    )
                    return

                # Insert registration record.
                await db.execute(
                    """
                    INSERT INTO market_registrations (guild_id, user_id, registered_at, terms_agreed_at, terms_version)
                    VALUES ($1, $2, NOW(), NOW(), $3)
                    ON CONFLICT (guild_id, user_id) DO NOTHING;
                    """,
                    self.guild_id, self.user_id, TERMS_VERSION,
                )

            # Initialise market_users balance (STARTING_SP).
            await ensure_user(self.guild_id, self.user_id, self.pool)

            print(f"[Register] ✅ User {self.user_id} registered in guild {self.guild_id} (T&C v{TERMS_VERSION})")

            welcome = (
                f"You are now registered and ready to trade!\n\n"
                f"📊 **Your Starting Balance:** {money(STARTING_SP)}\n\n"
                f"🎯 **Quick Start:**\n"
                f"• `/daily` — Claim ${DAILY_SP:,} daily reward\n"
                f"• `/market` — View Top 10 stocks\n"
                f"• `/buy <player> <shares>` — Buy shares\n"
                f"• `/sell <player> <shares>` — Sell shares\n"
                f"• `/portfolio` — View your holdings\n"
                f"• `/balance` — Check your balance\n\n"
                f"📋 **More Commands:**\n"
                f"• `/marketcommands` — Full command list\n"
                f"• `/shop` — Browse items\n"
                f"• `/marketleaderboard` — Top investors\n\n"
                f"Good luck trading! 📈"
            )
            embed = discord.Embed(
                title="✅ WELCOME TO THE EAS STOCK MARKET",
                description=welcome,
                color=discord.Color.green(),
            )
            embed.set_footer(text=f"Terms & Conditions v{TERMS_VERSION} agreed • {interaction.guild.name}")
            await interaction.response.edit_message(embed=embed, view=None)

        except Exception as exc:
            print(f"[Register] ❌ Registration error for user {self.user_id}: {exc}")
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="❌ Registration Error",
                    description="An error occurred during registration. Please try again later.",
                    color=discord.Color.red(),
                ),
                view=None,
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red, emoji="❌")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        """Cancel the registration flow."""
        embed = discord.Embed(
            title="❌ Registration Cancelled",
            description=(
                "You have cancelled registration.\n\n"
                "You can register at any time using `/register`.\n"
                "You must agree to the Terms & Conditions to access the market."
            ),
            color=discord.Color.red(),
        )
        await interaction.response.edit_message(embed=embed, view=None)


@bot.tree.command(name="register", description="Register for the EAS Stock Market and agree to Terms & Conditions.")
async def register(interaction: discord.Interaction) -> None:
    """Show T&C and register the user for the market."""
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return

    guild_id = interaction.guild.id
    user_id = interaction.user.id

    # Check if already registered.
    async with pool.acquire() as db:
        existing = await db.fetchval(
            "SELECT registered_at FROM market_registrations WHERE guild_id=$1 AND user_id=$2;",
            guild_id, user_id,
        )

    if existing:
        registered_ts = existing.strftime("%Y-%m-%d %H:%M UTC") if hasattr(existing, "strftime") else str(existing)
        embed = discord.Embed(
            title="ℹ️ Already Registered",
            description=(
                f"You are already registered for the EAS Stock Market!\n\n"
                f"**Registered:** {registered_ts}\n"
                f"**Terms Version:** v{TERMS_VERSION}\n\n"
                f"Use `/balance` to check your balance or `/market` to start trading."
            ),
            color=discord.Color.orange(),
        )
        embed.set_footer(text=f"✅ Registration Status: Registered • {mode_label(guild_id)}")
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    # Show Terms & Conditions with agree/cancel buttons.
    embed = discord.Embed(
        title="📋 EAS Stock Market — Terms & Conditions",
        description=TERMS_AND_CONDITIONS,
        color=discord.Color.blurple(),
    )
    embed.set_footer(text="Click 'I Agree' to register • This message expires in 5 minutes")
    view = TermsButtons(user_id=user_id, pool=pool, guild_id=guild_id)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


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

    # Check registration status for the calling user.
    reg_status = "⚠️ N/A"
    if interaction.guild and validate_guild(guild_id):
        try:
            pool = await db_manager.get_pool(guild_id)
            async with pool.acquire() as _conn:
                is_registered = await _conn.fetchval(
                    "SELECT user_id FROM market_registrations WHERE guild_id=$1 AND user_id=$2;",
                    guild_id, interaction.user.id,
                )
            reg_status = "✅ Registered" if is_registered else "❌ Not Registered"
        except Exception:
            reg_status = "⚠️ Unknown"

    desc = (
        f"**Latency:** {latency}ms\n"
        f"**Mode:** {current_mode}\n"
        f"**Main DB:** {main_status}\n"
        f"**Test DB:** {test_status}\n"
        f"**Uptime:** {uptime_str}\n"
        f"**Registration Status:** {reg_status}"
    )
    await send_embed(interaction, "🏓 Pong", desc, discord.Color.green())


@bot.tree.command(name="marketcommands", description="View EAS stock market commands by page.")
@app_commands.describe(page="Page number: 1 Economy, 2 Market, 3 Trading, 4 Staff, 5 Developer")
async def marketcommands(interaction: discord.Interaction, page: int = 1):
    """Paginated command reference for the EAS Stock Market Bot."""
    pages = {
        1: (
            "📘 Economy Commands",
            "`/register` — Register & agree to Terms & Conditions\n"
            "`/ping` — Bot status, latency & uptime\n"
            "`/balance` — View your SP balance & wealth role\n"
            "`/daily` — Claim your daily $50,000 SP reward\n"
            "`/marketcommands` — This command list\n\n"
            "**Starting Balance:** $250,000 SP\n"
            "**Daily Reward:** $50,000 SP\n"
            "**Registration required** to access market commands.",
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
            "`/resetbalance` — Reset a user's balance\n"
            "`/marketsimulate` — Trigger test-server simulation (test only)\n"
            "`/marketresettest` — Reset test database (test only)\n"
            "`/marketstatus` — Show database & mode status\n\n"
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
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    async with pool.acquire() as db:
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
    pool = await validate_guild_and_get_pool(interaction)
    if pool is None:
        return
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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
    if not await check_registration(interaction, pool):
        return
    guild_id = interaction.guild.id
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


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN is missing.")
    bot.run(DISCORD_TOKEN)
