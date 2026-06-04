import os
import math
import json
import asyncio
from typing import Optional, List, Dict, Any

import asyncpg
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
DEVELOPER_USER_ID = int(os.getenv("DEVELOPER_USER_ID", "733871667788644445"))

STAFF_ROLE_IDS = {
    1473033115818786909,
    1473033135003533352,
    1509317165650673694,
    1486144217079091352,
    1473033493771452579,
}

STARTING_SP = int(os.getenv("STARTING_SP", "5000"))
DAILY_SP = int(os.getenv("DAILY_SP", "1000"))
SELL_TAX = float(os.getenv("SELL_TAX", "0.03"))
MAX_OWNERSHIP_PERCENT = float(os.getenv("MAX_OWNERSHIP_PERCENT", "0.25"))
MAX_MARKET_PLAYERS = 10
TOP10_SYNC_MINUTES = int(os.getenv("TOP10_SYNC_MINUTES", "5"))

# Existing ranked bot database source.
# This expects the EAS ranked bot table format you used before:
# players(guild_id, user_id, data JSON/JSONB)
# data contains fields like cr, wins, losses, kills, mvps/mvps, streak.
RANKED_PLAYERS_TABLE = os.getenv("RANKED_PLAYERS_TABLE", "players")
RANKED_USER_ID_COLUMN = os.getenv("RANKED_USER_ID_COLUMN", "user_id")
RANKED_GUILD_ID_COLUMN = os.getenv("RANKED_GUILD_ID_COLUMN", "guild_id")
RANKED_DATA_COLUMN = os.getenv("RANKED_DATA_COLUMN", "data")

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)
db_pool: Optional[asyncpg.Pool] = None


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


async def init_db() -> None:
    global db_pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing.")
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with db_pool.acquire() as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_users (
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            balance BIGINT NOT NULL DEFAULT 5000,
            last_daily TIMESTAMP,
            frozen BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (guild_id, user_id)
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_stocks (
            guild_id BIGINT NOT NULL,
            player_id BIGINT NOT NULL,
            price BIGINT NOT NULL DEFAULT 1000,
            rank_position INTEGER NOT NULL,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            kills INTEGER NOT NULL DEFAULT 0,
            mvps INTEGER NOT NULL DEFAULT 0,
            streak INTEGER NOT NULL DEFAULT 0,
            cr INTEGER NOT NULL DEFAULT 0,
            previous_rank_position INTEGER,
            updated_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (guild_id, player_id)
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_holdings (
            guild_id BIGINT NOT NULL,
            investor_id BIGINT NOT NULL,
            player_id BIGINT NOT NULL,
            shares INTEGER NOT NULL DEFAULT 0,
            average_price BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, investor_id, player_id)
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_transactions (
            id SERIAL PRIMARY KEY,
            guild_id BIGINT NOT NULL,
            investor_id BIGINT NOT NULL,
            player_id BIGINT,
            type TEXT NOT NULL,
            shares INTEGER,
            price BIGINT,
            total BIGINT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_stock_history (
            id SERIAL PRIMARY KEY,
            guild_id BIGINT NOT NULL,
            player_id BIGINT NOT NULL,
            old_price BIGINT NOT NULL,
            new_price BIGINT NOT NULL,
            reason TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS market_settings (
            guild_id BIGINT PRIMARY KEY,
            market_open BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """)


async def ensure_user(guild_id: int, user_id: int) -> None:
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute("""
        INSERT INTO market_users (guild_id, user_id, balance)
        VALUES ($1, $2, $3)
        ON CONFLICT (guild_id, user_id) DO NOTHING;
        """, guild_id, user_id, STARTING_SP)


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
    """Pull top 10 by CR from the existing ranked players table and update market listings."""
    assert db_pool is not None
    guild_id = guild.id

    # Use dynamic table/columns from env. These names are config-only, not user input.
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
            print(f"Top 10 sync failed for guild {guild_id}: {exc}")
            return 0

        current_rows = await db.fetch("""
        SELECT player_id, price, rank_position FROM market_stocks
        WHERE guild_id=$1 AND active=true;
        """, guild_id)
        current = {int(r["player_id"]): r for r in current_rows}

        new_ids = []
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
                old_position = int(old["rank_position"])
                old_price = int(old["price"])
                final_price = max(1000, int((old_price * 0.75) + (base_price * 0.25)))

                if index < old_position:
                    # Player passed someone and moved up.
                    bump = 0.025 * (old_position - index)
                    final_price = int(final_price * (1 + bump))
                    reason = f"Top 10 sync: moved up from #{old_position} to #{index}"
                elif index > old_position:
                    drop = 0.02 * (index - old_position)
                    final_price = int(final_price * (1 - drop))
                    reason = f"Top 10 sync: moved down from #{old_position} to #{index}"
                else:
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
                await db.execute("""
                INSERT INTO market_stocks
                (guild_id, player_id, price, rank_position, previous_rank_position, active, wins, losses, kills, mvps, streak, cr)
                VALUES ($1, $2, $3, $4, NULL, true, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (guild_id, player_id)
                DO UPDATE SET price=$3, rank_position=$4, active=true, wins=$5, losses=$6, kills=$7, mvps=$8, streak=$9, cr=$10, updated_at=NOW();
                """, guild_id, player_id, base_price, index, wins, losses, kills, mvps, streak, cr)

            updated_count += 1

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
    for guild in bot.guilds:
        await sync_top10_for_guild(guild)


@bot.event
async def on_ready():
    await init_db()
    if not top10_sync_loop.is_running():
        top10_sync_loop.start()
    await bot.tree.sync()
    print(f"EAS Stock Market Bot online as {bot.user}")


async def send_embed(interaction: discord.Interaction, title: str, description: str, color=discord.Color.blurple(), ephemeral=False):
    embed = discord.Embed(title=title, description=description, color=color)
    await interaction.response.send_message(embed=embed, ephemeral=ephemeral)


@bot.tree.command(name="ping", description="Check if the EAS Stock Market Bot is online.")
async def ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)
    await send_embed(interaction, "🏓 Pong", f"Bot is online. Latency: **{latency}ms**", discord.Color.green())


@bot.tree.command(name="marketcommands", description="View EAS stock market commands by page.")
@app_commands.describe(page="Page number: 1 Public, 2 Trading, 3 Staff, 4 Developer")
async def marketcommands(interaction: discord.Interaction, page: int = 1):
    pages = {
        1: ("📘 Public Commands", "`/ping` - Check if bot is online\n`/balance` - View StarPoints\n`/daily` - Claim daily StarPoints\n`/market` - View top 10 market\n`/stock user` - View player stock\n`/portfolio` - View your holdings\n`/topinvestors` - Richest investors\n`/transactions` - Recent trades"),
        2: ("📈 Trading Commands", "`/buy user shares` - Buy shares\n`/sell user shares` - Sell shares\n`/topstocks` - Highest priced stocks\n`/gainers` - Recent biggest gainers\n`/losers` - Recent biggest losers"),
        3: ("🛡️ Staff Commands", "`/syncmarket` - Pull top 10 from ranked database\n`/marketopen` - Open trading\n`/marketclose` - Close trading\n`/logresult` - Manually log performance movement\n`/freezeportfolio` - Freeze user trading\n`/unfreezeportfolio` - Unfreeze user trading"),
        4: ("👑 Developer Commands", "`/givepoints` - Give StarPoints\n`/takepoints` - Take StarPoints\n`/resetbalance` - Reset a balance\nOnly developer ID `733871667788644445` can use these."),
    }
    page = max(1, min(4, page))
    title, desc = pages[page]
    embed = discord.Embed(title=title, description=desc, color=discord.Color.gold())
    embed.set_footer(text=f"Page {page}/4 • Use /marketcommands page:2 etc.")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="balance", description="View your StarPoints balance.")
async def balance(interaction: discord.Interaction):
    await ensure_user(interaction.guild.id, interaction.user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        row = await db.fetchrow("SELECT balance, frozen FROM market_users WHERE guild_id=$1 AND user_id=$2;", interaction.guild.id, interaction.user.id)
    status = "Frozen" if row["frozen"] else "Active"
    await send_embed(interaction, "⭐ StarPoints Balance", f"Balance: **{money(row['balance'])}**\nPortfolio Status: **{status}**", discord.Color.gold())


@bot.tree.command(name="daily", description="Claim your daily StarPoints.")
async def daily(interaction: discord.Interaction):
    await ensure_user(interaction.guild.id, interaction.user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        can_claim = await db.fetchval("""
        SELECT last_daily IS NULL OR last_daily < NOW() - INTERVAL '24 hours'
        FROM market_users WHERE guild_id=$1 AND user_id=$2;
        """, interaction.guild.id, interaction.user.id)
        if not can_claim:
            await send_embed(interaction, "⏳ Daily Already Claimed", "You can claim again after 24 hours.", discord.Color.orange(), True)
            return
        await db.execute("UPDATE market_users SET balance=balance+$1, last_daily=NOW() WHERE guild_id=$2 AND user_id=$3;", DAILY_SP, interaction.guild.id, interaction.user.id)
        await db.execute("INSERT INTO market_transactions (guild_id, investor_id, type, total) VALUES ($1,$2,'daily',$3);", interaction.guild.id, interaction.user.id, DAILY_SP)
    await send_embed(interaction, "⭐ Daily Claimed", f"You received **{money(DAILY_SP)}**.", discord.Color.green())


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
async def buy(interaction: discord.Interaction, user: discord.Member, shares: int):
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
        u = await db.fetchrow("SELECT balance,frozen FROM market_users WHERE guild_id=$1 AND user_id=$2;", interaction.guild.id, interaction.user.id)
        if u["frozen"]:
            await send_embed(interaction, "🧊 Frozen", "Your portfolio is frozen.", discord.Color.red(), True); return
        if u["balance"] < total_cost:
            await send_embed(interaction, "❌ Not Enough SP", f"You need **{money(total_cost)}**.", discord.Color.red(), True); return
        current_total = await total_shares(interaction.guild.id, user.id)
        current_owned = safe_int(await db.fetchval("SELECT shares FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;", interaction.guild.id, interaction.user.id, user.id))
        if (current_owned + shares) / max(1, current_total + shares) > MAX_OWNERSHIP_PERCENT:
            await send_embed(interaction, "❌ Ownership Limit", "You cannot own more than 25% of one player's market shares.", discord.Color.red(), True); return
        old = await db.fetchrow("SELECT shares,average_price FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;", interaction.guild.id, interaction.user.id, user.id)
        if old:
            new_shares = old["shares"] + shares
            new_avg = math.floor(((old["shares"] * old["average_price"]) + total_cost) / new_shares)
            await db.execute("UPDATE market_holdings SET shares=$1, average_price=$2 WHERE guild_id=$3 AND investor_id=$4 AND player_id=$5;", new_shares, new_avg, interaction.guild.id, interaction.user.id, user.id)
        else:
            await db.execute("INSERT INTO market_holdings (guild_id,investor_id,player_id,shares,average_price) VALUES ($1,$2,$3,$4,$5);", interaction.guild.id, interaction.user.id, user.id, shares, stock_row["price"])
        await db.execute("UPDATE market_users SET balance=balance-$1 WHERE guild_id=$2 AND user_id=$3;", total_cost, interaction.guild.id, interaction.user.id)
        await db.execute("INSERT INTO market_transactions (guild_id,investor_id,player_id,type,shares,price,total) VALUES ($1,$2,$3,'buy',$4,$5,$6);", interaction.guild.id, interaction.user.id, user.id, shares, stock_row["price"], total_cost)
    await send_embed(interaction, "✅ Shares Purchased", f"Bought **{shares} shares** of **{user.display_name}** for **{money(total_cost)}**.", discord.Color.green())


@bot.tree.command(name="sell", description="Sell shares of a player stock.")
async def sell(interaction: discord.Interaction, user: discord.Member, shares: int):
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
        u = await db.fetchrow("SELECT frozen FROM market_users WHERE guild_id=$1 AND user_id=$2;", interaction.guild.id, interaction.user.id)
        if u["frozen"]:
            await send_embed(interaction, "🧊 Frozen", "Your portfolio is frozen.", discord.Color.red(), True); return
        h = await db.fetchrow("SELECT shares FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;", interaction.guild.id, interaction.user.id, user.id)
        if not h or h["shares"] < shares:
            await send_embed(interaction, "❌ Not Enough Shares", "You do not own enough shares.", discord.Color.red(), True); return
        gross = stock_row["price"] * shares
        tax = math.floor(gross * SELL_TAX)
        net = gross - tax
        remaining = h["shares"] - shares
        if remaining <= 0:
            await db.execute("DELETE FROM market_holdings WHERE guild_id=$1 AND investor_id=$2 AND player_id=$3;", interaction.guild.id, interaction.user.id, user.id)
        else:
            await db.execute("UPDATE market_holdings SET shares=$1 WHERE guild_id=$2 AND investor_id=$3 AND player_id=$4;", remaining, interaction.guild.id, interaction.user.id, user.id)
        await db.execute("UPDATE market_users SET balance=balance+$1 WHERE guild_id=$2 AND user_id=$3;", net, interaction.guild.id, interaction.user.id)
        await db.execute("INSERT INTO market_transactions (guild_id,investor_id,player_id,type,shares,price,total) VALUES ($1,$2,$3,'sell',$4,$5,$6);", interaction.guild.id, interaction.user.id, user.id, shares, stock_row["price"], net)
    await send_embed(interaction, "✅ Shares Sold", f"Sold **{shares} shares** of **{user.display_name}** for **{money(net)}** after tax.", discord.Color.green())


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


@bot.tree.command(name="topinvestors", description="View the richest investors.")
async def topinvestors(interaction: discord.Interaction):
    assert db_pool is not None
    async with db_pool.acquire() as db:
        rows = await db.fetch("""
        SELECT u.user_id, u.balance + COALESCE(SUM(h.shares * s.price), 0) AS net_worth
        FROM market_users u
        LEFT JOIN market_holdings h ON u.guild_id=h.guild_id AND u.user_id=h.investor_id
        LEFT JOIN market_stocks s ON h.guild_id=s.guild_id AND h.player_id=s.player_id AND s.active=true
        WHERE u.guild_id=$1
        GROUP BY u.user_id, u.balance
        ORDER BY net_worth DESC
        LIMIT 10;
        """, interaction.guild.id)
    desc = ""
    for i, r in enumerate(rows, start=1):
        member = interaction.guild.get_member(int(r["user_id"]))
        name = member.display_name if member else f"User {r['user_id']}"
        desc += f"**#{i} {name}** — `{money(r['net_worth'])}`\n"
    await send_embed(interaction, "🏦 Top Investors", desc or "No investors yet.", discord.Color.gold())


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


@bot.tree.command(name="freezeportfolio", description="Staff: Freeze a user's portfolio.")
async def freezeportfolio(interaction: discord.Interaction, user: discord.Member):
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    await ensure_user(interaction.guild.id, user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute("UPDATE market_users SET frozen=true WHERE guild_id=$1 AND user_id=$2;", interaction.guild.id, user.id)
    await send_embed(interaction, "🧊 Portfolio Frozen", f"{user.mention}'s portfolio is now frozen.", discord.Color.blue())


@bot.tree.command(name="unfreezeportfolio", description="Staff: Unfreeze a user's portfolio.")
async def unfreezeportfolio(interaction: discord.Interaction, user: discord.Member):
    if not is_staff(interaction.user):
        await send_embed(interaction, "❌ No Permission", "Staff only.", discord.Color.red(), True); return
    await ensure_user(interaction.guild.id, user.id)
    assert db_pool is not None
    async with db_pool.acquire() as db:
        await db.execute("UPDATE market_users SET frozen=false WHERE guild_id=$1 AND user_id=$2;", interaction.guild.id, user.id)
    await send_embed(interaction, "✅ Portfolio Unfrozen", f"{user.mention}'s portfolio is now unfrozen.", discord.Color.green())


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
