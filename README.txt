EAS Market Badge Pack

How to add:
1. Upload all SVG files to your website public folder:
   public/badges/

2. Add the badge_manifest.json entries to your badge_definitions table.

3. Make the profile page read player_badges joined with badge_definitions.

4. When a user buys a badge in Discord, insert a row into player_badges:
   guild_id, user_id, badge_id, source='market_shop', granted_by='system', granted_at=NOW()

5. On profile page, render:
   <img src={badge.icon_url} alt={badge.name} title={badge.description} />

These are SVG files, so they will stay crisp on the website.
