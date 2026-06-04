# Prompt to give Railway

Set up this new Discord bot as the EAS Stock Market Bot.

The bot should run as a separate service from the ranked bot, but it must connect to the same PostgreSQL database so it can read the ranked players table and automatically list only the current top 10 ranked players as stocks.

Use these environment variables:

```env
DISCORD_TOKEN=PASTE_BOT_TOKEN_HERE
DATABASE_URL=PASTE_RAILWAY_POSTGRES_URL_HERE
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

Start command:

```bash
python main.py
```

Important behavior:

- Only top 10 ranked players by CR can be listed in the stock market.
- The bot should automatically sync the top 10 every 5 minutes.
- The bot should also allow staff to force sync with `/syncmarket`.
- If a player moves higher in the top 10, their stock price should increase a little extra.
- If a player gets passed and moves lower, their stock price should decrease a little.
- The shop is NOT included yet.
- All command responses should use embeds.
- `/ping` should show if the bot is online.
- `/marketcommands` should show paged command categories.
- Developer commands are not owner commands. They are developer commands and only this user ID can use them: `733871667788644445`.
- Developer-only commands: `/givepoints`, `/takepoints`, `/resetbalance`.
- Staff roles allowed:
  - `1473033115818786909`
  - `1473033135003533352`
  - `1509317165650673694`
  - `1486144217079091352`
  - `1473033493771452579`

Make sure the Railway service has access to PostgreSQL and that the `players` table exists from the ranked bot. If the ranked bot uses a different table name or data format, update the `RANKED_*` variables or patch the top 10 sync query in `main.py`.
