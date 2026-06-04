# EAS Stock Market Bot

A separate Discord bot for the EAS player stock market using StarPoints (SP). Version 1 has no shop. Only the current top 10 ranked players are listed as stocks.

## Features

- Slash commands only
- PostgreSQL storage
- StarPoints economy
- Buy/sell top 10 player shares
- Portfolio and investor leaderboard
- Automatic top 10 sync from ranked database
- Market price movement when top 10 positions change
- Staff-only market controls
- Developer-only currency commands locked to `733871667788644445`
- `/ping` health check
- `/marketcommands` paged command list embeds

## Staff Role IDs Built In

These roles can use staff commands:

- `1473033115818786909`
- `1473033135003533352`
- `1509317165650673694`
- `1486144217079091352`
- `1473033493771452579`

Admins can also use staff commands.

## Developer-Only Commands

Only user ID `733871667788644445` can use:

- `/givepoints`
- `/takepoints`
- `/resetbalance`

## Public Commands

- `/ping`
- `/marketcommands page`
- `/balance`
- `/daily`
- `/market`
- `/stock user`
- `/buy user shares`
- `/sell user shares`
- `/portfolio`
- `/topinvestors`
- `/topstocks`
- `/gainers`
- `/losers`
- `/transactions`

## Staff Commands

- `/syncmarket`
- `/marketopen`
- `/marketclose`
- `/logresult`
- `/freezeportfolio`
- `/unfreezeportfolio`

## Railway Setup

1. Upload this folder to GitHub.
2. Create a new Railway project.
3. Choose **Deploy from GitHub Repo**.
4. Add PostgreSQL to the Railway project, or connect the same PostgreSQL database your ranked bot uses.
5. Add these Railway variables:

```env
DISCORD_TOKEN=your_bot_token
DATABASE_URL=your_railway_postgres_database_url
DEVELOPER_USER_ID=733871667788644445
STARTING_SP=5000
DAILY_SP=1000
SELL_TAX=0.03
MAX_OWNERSHIP_PERCENT=0.25
TOP10_SYNC_MINUTES=5
RANKED_PLAYERS_TABLE=players
RANKED_USER_ID_COLUMN=user_id
RANKED_GUILD_ID_COLUMN=guild_id
RANKED_DATA_COLUMN=data
```

6. Set the Railway start command to:

```bash
python main.py
```

Railway may also use the included `Procfile` automatically:

```bash
worker: python main.py
```

## Ranked Database Connection

By default, the bot expects your existing ranked bot table to be:

```sql
players(
  guild_id BIGINT,
  user_id BIGINT,
  data JSONB
)
```

And the `data` JSON should contain values like:

```json
{
  "cr": 3200,
  "wins": 50,
  "losses": 20,
  "kills": 500,
  "mvps": 8,
  "streak": 4
}
```

If your ranked bot uses different table or column names, change these Railway variables:

```env
RANKED_PLAYERS_TABLE=players
RANKED_USER_ID_COLUMN=user_id
RANKED_GUILD_ID_COLUMN=guild_id
RANKED_DATA_COLUMN=data
```

## How Top 10 Works

The bot syncs every 5 minutes by CR. Only the top 10 players by CR are active stocks. When a player moves up, their stock gets a boost. When a player gets passed and moves down, their stock loses value.

You can also run:

```txt
/syncmarket
```

## Notes

This is fake currency only. Do not allow real-money cashout, Robux cashout, Nitro cashout, or gift card cashout.
