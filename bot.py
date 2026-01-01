import discord
from discord import app_commands
import os
import asyncio
import re
import logging
from urllib.parse import urlparse

# Configure logging early for opus loading
logging.basicConfig(level=logging.INFO)


# Load opus library for voice support with comprehensive search
from dotenv import load_dotenv
import os

load_dotenv()  # Loads variables from .env

# Access environment variables
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_SERVER_ID = os.getenv("DISCORD_SERVER_ID")
import time
import math
import requests
from scraper import Scraper, is_protected_site
from utils import create_epub, create_pdf
from manga_utils import upload_large_file, is_file_too_large_for_discord
from user_settings import settings_manager, get_user_settings, get_setting, set_setting, get_style_css, get_settings_display, EPUB_STYLES
from download_history import history_manager, add_download, get_history, get_last_download, get_library, check_duplicate, get_stats
from typing import Optional, Dict, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API endpoint for analytics/dashboard (set your API URL or leave empty to disable)
API_BASE_URL = os.environ.get('API_BASE_URL', '')


def _sync_log_download(data: dict):
    """Log a download to the console (API logging disabled)"""
    logger.info(f"Download completed: {data.get('novelTitle', 'Unknown')} - {data.get('chapterCount', 0)} chapters")

def _sync_log_error(data: dict):
    """Log an error to the console (API logging disabled)"""
    logger.error(f"Bot Error: {data.get('errorType', 'Unknown')} - {data.get('errorMessage', '')}")

async def log_download(data: dict, loop=None):
    """Log a download (non-blocking)"""
    _sync_log_download(data)

async def log_error(data: dict, loop=None):
    """Log an error (non-blocking)"""
    _sync_log_error(data)

async def log_site_health(data: dict, loop=None):
    """Site health logging disabled"""
    pass

def _sync_create_active_download(data: dict) -> Optional[int]:
    """Active download tracking via API disabled"""
    return None

def _sync_update_active_download(download_id: int, current_chapter: int, status: str = 'in_progress'):
    """Active download tracking via API disabled"""
    pass

def _sync_delete_active_download(download_id: int):
    """Active download tracking via API disabled"""
    pass

def _sync_register_worker(worker_id: str, status: str = 'online', current_task: str = None):
    """Worker registration disabled"""
    pass

def _sync_track_content_request(title: str, content_type: str, source: str, success: bool = None):
    """Content request tracking disabled"""
    pass

def _sync_log_site_failure(site_domain: str, error_type: str, error_message: str = None, novel_url: str = None, chapter_url: str = None, response_code: int = None, response_time: int = None):
    """Site failure logging to console"""
    logger.warning(f"Site failure on {site_domain}: {error_type}")

async def register_worker(worker_id: str, status: str = 'online', current_task: str = None, loop=None):
    pass

async def track_content_request(title: str, content_type: str, source: str, success: bool = None, loop=None):
    pass

async def log_site_failure(site_domain: str, error_type: str, error_message: str = None, novel_url: str = None, chapter_url: str = None, response_code: int = None, response_time: int = None, loop=None):
    _sync_log_site_failure(site_domain, error_type)

def _sync_log_to_sheets(data: dict):
    """Google Sheets logging disabled via API"""
    pass

async def log_to_sheets(data: dict, loop=None):
    pass


# Helper to get token securely
TOKEN = os.getenv('DISCORD_TOKEN')
SERVER_ID = int(os.getenv('DISCORD_SERVER_ID', '0'))

# Ad-free upgrade message for free users
AD_FREE_MESSAGE = (
    "Tired of annoying ads? Want to enjoy all your favorite webnovels or manga on the Bot?\n"
    "Support Meowi's Tea and Coffee on Patreon for just $1/month and enjoy an ad-free experience!\n"
    "https://www.patreon.com/c/meowisteaandcoffee/membership")


def shorten_with_shrinkme(long_url: str, service: str = "") -> str:
    """URL shortening disabled - returns direct URL."""
    return long_url

# Role names (case-insensitive matching)
VERIFIED_ROLE_NAME = "Verified"
COFFEE_ROLE_NAME = "Coffee"  # Formerly Patreon
CATNIP_ROLE_NAME = "Catnip"  # Formerly VIP
SPONSOR_ROLE_NAME = "Sponsor"
SUPPORTER_ROLE_NAME = "Supporter"  # Alias for Sponsor (same tier)
ADMIN_ROLE_NAME = "admin"

# Private chat category ID - reactions only work in this category
PRIVATE_CHAT_CATEGORY_ID = 1452964350233936103
# Channel where Cat Café welcome message is posted
CAT_CAFE_CHANNEL_ID = 1452964413379313685
# Channel where bot logs are posted
LOGS_CHANNEL_ID = 1452975292590063667
# Verification message for role assignment
VERIFICATION_MESSAGE_ID = 1453054196520718376
VERIFICATION_CHANNEL_ID = 1452902693784780891

# Small hint for back/cancel options (shown at bottom of interactive messages)
HINT_TEXT = "\n\n`back` - go back | `cancel` - cancel"

# Stat channel configuration
STAT_CATEGORY_NAME = "📊 ── sᴛᴀᴛꜱ ──"
STAT_CHANNELS = [
    {
        "key": "verified",
        "template": "👣 ᴠᴇʀɪꜰɪᴇᴅ • {count}"
    },
    {
        "key": "novels_today",
        "template": "📘 ɴᴏᴠᴇʟꜱ ᴛᴏᴅᴀʏ • {count}"
    },
    {
        "key": "novels_alltime",
        "template": "📚 ɴᴏᴠᴇʟꜱ ᴀʟʟ-ᴛɪᴍᴇ • {count}"
    },
    {
        "key": "active_jobs",
        "template": "⚡ ᴀᴄᴛɪᴠᴇ ᴊᴏʙꜱ • {count}"
    },
    {
        "key": "queue",
        "template": "⏳ ǫᴜᴇᴜᴇ • {count}"
    },
    {
        "key": "top_site",
        "template": "🔥 ᴛᴏᴘ ꜱɪᴛᴇ • {count}"
    },
    {
        "key": "failures_today",
        "template": "🛠️ ꜰᴀɪʟᴜʀᴇꜱ ᴛᴏᴅᴀʏ • {count}"
    },
]
STAT_UPDATE_INTERVAL = 600  # 10 minutes in seconds


def format_stat_number(num: int) -> str:
    """Format number for stat display: 999, 1.2k, 1.2M"""
    if num is None:
        return "—"
    try:
        num = int(num)
        if num >= 1_000_000:
            return f"{num / 1_000_000:.1f}M".rstrip('0').rstrip('.')
        elif num >= 1_000:
            return f"{num / 1_000:.1f}k".rstrip('0').rstrip('.')
        else:
            return str(num)
    except (ValueError, TypeError):
        return "—"


def get_gmt8_today() -> str:
    """Get today's date in GMT+8 as YYYY-MM-DD"""
    import pytz
    from datetime import datetime
    gmt8 = pytz.timezone('Asia/Singapore')
    return datetime.now(gmt8).strftime('%Y-%m-%d')


# Links
SERVER_INVITE = "https://discord.gg/CHnH7YUy"
PATREON_LINK = "https://www.patreon.com/c/MeowisTeaandCoffee"

# Role-based scraping speeds (parallel workers)
WORKERS_NORMAL = 5  # Verified: 5 chapters at a time
WORKERS_COFFEE = 15  # Coffee: 15 chapters at a time
WORKERS_CATNIP = 25  # Catnip: 25 chapters at a time
WORKERS_SPONSOR = 100  # Sponsor: 100 chapters at a time (maximum speed)

# Download limits per tier
# Verified: 20% of total chapters + daily bonus
# Coffee: 80% of total chapters (max 1000/day) + daily bonus
# Catnip/Sponsor: Unlimited
DAILY_BONUS_NOVEL_VERIFIED = 200  # +200 novel chapters/day for Verified
DAILY_BONUS_MANGA_VERIFIED = 30  # +30 manga chapters/day for Verified
DAILY_BONUS_NOVEL_COFFEE = 1000  # +1000 novel chapters/day for Coffee
DAILY_BONUS_MANGA_COFFEE = 150  # +150 manga chapters/day for Coffee

# GMT+8 timezone offset
import pytz

GMT8 = pytz.timezone('Asia/Singapore')


def get_gmt8_date() -> str:
    """Get current date in GMT+8 timezone as YYYY-MM-DD string"""
    from datetime import datetime
    return datetime.now(GMT8).strftime('%Y-%m-%d')


class NovelBot(discord.Client):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_states = {
        }  # {user_id: {'step': ..., 'data': ..., 'message': ..., 'cancelled': ..., 'user_tier': ...}}
        self.scraper = Scraper()
        self.scraping_tasks = {}  # {user_id: task}
        self.temporary_channels = {
        }  # {channel_id: user_id} - tracks temporary private channels
        self.bot_disabled_channels = set(
        )  # {channel_id} - channels where bot is disabled
        self.tree = app_commands.CommandTree(self)
        self.keep_alive_task = None  # Background keep-alive task
        self._setup_slash_commands()

    def _normalize_novel_title(self, raw_title: str, url: str) -> str:
        """Normalize novel titles so the same work from different sites groups together.

        Uses the URL slug when possible (e.g., /novel/the-way-of-restraint,
        /novel-book/the-way-of-restraint, /novels/1206857-the-way-of-restraint.html)
        and falls back to cleaning common patterns like
        "Read <title> novel online free - ReadNovelFull".
        """
        title = (raw_title or '').strip()

        # 1) Try to derive from URL slug
        try:
            parsed = urlparse(url)
            path = (parsed.path or '').strip('/')
            if path:
                seg = path.split('/')[-1]
                # Drop extension
                import re as _re
                seg = _re.sub(r'\.(html?|php)$', '', seg, flags=_re.I)
                # Remove known prefixes and numeric ids
                for prefix in ('novel-book-', 'novel-', 'book-'):
                    if seg.lower().startswith(prefix):
                        seg = seg[len(prefix):]
                        break
                seg = _re.sub(r'^\d+-', '', seg)
                slug = seg.replace('-', ' ').strip()
                if slug and len(slug) > 2:
                    return slug.title()
        except Exception:
            pass

        # 2) Handle "Read <title> novel online free - ..." style titles
        try:
            import re as _re
            lower = title.lower()
            m = _re.search(r'read\s+(.+?)\s+novel online free', lower)
            if m:
                core_lower = m.group(1).strip()
                idx = lower.find(core_lower)
                if idx != -1:
                    core = title[idx:idx + len(core_lower)]
                else:
                    core = core_lower
                core = core.strip()
                if core:
                    return core
        except Exception:
            pass

        # 3) Fallback: trim at first " - " to drop site tagline
        if ' - ' in title:
            return title.split(' - ', 1)[0].strip()

        return title or 'Unknown'

    def _setup_slash_commands(self):
        """Register slash commands"""

        @self.tree.command(name="edit",
                           description="Edit a bot message by ID (Admin only)",
                           guild=discord.Object(id=SERVER_ID))
        @app_commands.describe(new_text="The new text for the message",
                               message_id="The ID of the message to edit")
        async def edit_message(interaction: discord.Interaction, new_text: str,
                               message_id: str):
            # Check if user is admin
            member = interaction.user
            is_admin = any(role.name.lower() == ADMIN_ROLE_NAME.lower()
                           for role in member.roles)

            if not is_admin:
                await interaction.response.send_message(
                    "Only admins can edit bot messages.", ephemeral=True)
                return

            try:
                msg_id = int(message_id)
            except ValueError:
                await interaction.response.send_message(
                    "Invalid message ID. Please provide a valid number.",
                    ephemeral=True)
                return

            # Defer response while we search for the message
            await interaction.response.defer(ephemeral=True)

            # Search all channels the bot can access
            guild = interaction.guild
            message_found = False

            for channel in guild.text_channels:
                try:
                    msg = await channel.fetch_message(msg_id)
                    # Check if the bot authored this message
                    if msg.author.id != self.user.id:
                        await interaction.followup.send(
                            f"That message was not sent by me. I can only edit my own messages.",
                            ephemeral=True)
                        return

                    # Replace \\n with actual newlines
                    formatted_text = new_text.replace("\\n", "\n")
                    await msg.edit(content=formatted_text)
                    message_found = True
                    await interaction.followup.send(
                        f"Message edited successfully in #{channel.name}!",
                        ephemeral=True)

                    # Log the edit
                    await self.log_to_discord(
                        "Message Edited",
                        f"Admin {member.name} edited message {msg_id} in #{channel.name}",
                        discord.Color.blue())
                    break
                except discord.NotFound:
                    continue
                except discord.Forbidden:
                    continue
                except Exception as e:
                    logger.error(f"Error checking channel {channel.name}: {e}")
                    continue

            if not message_found:
                await interaction.followup.send(
                    "Message not found. Make sure the ID is correct and the message exists.",
                    ephemeral=True)

        @self.tree.command(
            name="sites",
            description="Post the list of supported novel sites",
            guild=discord.Object(id=SERVER_ID))
        async def post_sites(interaction: discord.Interaction):
            # Only allow in specific channel
            SITES_CHANNEL_ID = 1453320043252285490
            if interaction.channel_id != SITES_CHANNEL_ID:
                await interaction.response.send_message(
                    f"This command can only be used in <#{SITES_CHANNEL_ID}>.",
                    ephemeral=True)
                return

            # Check if user is admin
            member = interaction.user
            is_admin = any(role.name.lower() == ADMIN_ROLE_NAME.lower()
                           for role in member.roles)

            if not is_admin:
                await interaction.response.send_message(
                    "Only admins can use this command.", ephemeral=True)
                return

            # Novel sites list
            novel_sites = """**Supported Novel Sites (24+)**

**Primary Sites:**
- NovelBin (novelbin.me, novelbin.com, novelbin.cfd)
- RoyalRoad (royalroad.com)
- NovelFire (novelfire.net)
- FreeWebNovel (freewebnovel.com, freewebnovel.org)

**Additional Sites:**
- CreativeNovels
- BoxNovel
- Novel2
- NovelMTL
- Ranobes
- NovelBuddy
- LightNovelCave
- LibRead
- NovelUpdates
- EmpireNovel
- WTR-Lab
- LightNovelWorld
- LNMTL
- ReaderNovel
- FullNovels
- NiceNovel
- BedNovel
- AllNovelBook
- YongLibrary
- EnglishNovelsFree"""

            manga_sites = """**Supported Manga Sites (21)**

**Primary Sites:**
- AsuraComic (asuracomic.net)
- MangaDex (mangadex.org)
- MangaPark (mangapark.net)
- MangaPill (mangapill.com)
- MangaBuddy (mangabuddy.com)

**Additional Sites:**
- MangaHere
- MangaFox / FanFox
- Mangago
- Mangamya
- VIZ (viz.com)
- MagiManga
- MangaRead
- MangaToto
- ManhuaUS
- InfiniteMage
- MangaDass
- NovaManga
- MangaPaw
- DaoTranslate"""

            formats = """**Output Formats:**
- Novels: EPUB or PDF
- Manga: PDF (all images) or ZIP (organized by chapter)

**How to Use:**
1. DM the bot or use your private channel
2. Send a URL or search by title
3. Select chapter range
4. Choose format
5. Get your file!

Use `\\n` for new lines when using /edit command."""

            await interaction.response.defer(ephemeral=True)

            try:
                # Post each section as separate messages
                await interaction.channel.send(novel_sites)
                await interaction.channel.send(manga_sites)
                await interaction.channel.send(formats)

                await interaction.followup.send("Supported sites list posted!",
                                                ephemeral=True)

                await self.log_to_discord(
                    "Sites List Posted",
                    f"Admin {member.name} posted supported sites list in #{interaction.channel.name}",
                    discord.Color.blue())
            except discord.Forbidden:
                await interaction.followup.send(
                    "I don't have permission to post in this channel. Please give me 'Send Messages' permission or try in a different channel.",
                    ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"Error posting sites: {e}",
                                                ephemeral=True)

        @self.tree.command(
            name="analytics",
            description="View bot statistics and download analytics",
            guild=discord.Object(id=SERVER_ID))
        @app_commands.describe(view="Choose which analytics to view")
        @app_commands.choices(view=[
            app_commands.Choice(name="Summary - Overall stats",
                                value="summary"),
            app_commands.Choice(name="Trending - Most popular content",
                                value="trending"),
            app_commands.Choice(name="By Site - Downloads per site",
                                value="by-site"),
            app_commands.Choice(name="Recent - Latest downloads",
                                value="recent"),
            app_commands.Choice(name="Workers - Bot status", value="workers"),
        ])
        async def analytics_command(interaction: discord.Interaction,
                                    view: str = "summary"):
            # Check if user is admin or sponsor
            member = interaction.user
            is_admin = any(role.name.lower() == ADMIN_ROLE_NAME.lower()
                           for role in member.roles)
            is_sponsor = any(role.name.lower() in ['sponsor', 'catnip']
                             for role in member.roles)

            if not is_admin and not is_sponsor:
                await interaction.response.send_message(
                    "This command is only available for admins and sponsors.",
                    ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            try:
                if view == "summary":
                    # Fetch dashboard stats
                    dashboard_resp = requests.get(
                        f'{API_BASE_URL}/api/dashboard/stats', timeout=10)
                    dashboard = dashboard_resp.json(
                    ) if dashboard_resp.ok else {}

                    stats_resp = requests.get(
                        f'{API_BASE_URL}/api/downloads/stats', timeout=10)
                    stats = stats_resp.json() if stats_resp.ok else {}

                    queue = dashboard.get('queue', {})

                    embed = discord.Embed(title="Bot Analytics Summary",
                                          color=discord.Color.blue(),
                                          timestamp=discord.utils.utcnow())
                    embed.add_field(name="Today's Downloads",
                                    value=str(
                                        dashboard.get('todayDownloads', 0)),
                                    inline=True)
                    embed.add_field(
                        name="Today's Chapters",
                        value=f"{int(dashboard.get('todayChapters', 0)):,}",
                        inline=True)
                    embed.add_field(name="Online Workers",
                                    value=str(dashboard.get(
                                        'onlineWorkers', 0)),
                                    inline=True)
                    embed.add_field(name="Total Downloads",
                                    value=str(stats.get('totalDownloads', 0)),
                                    inline=True)
                    embed.add_field(
                        name="Total Chapters",
                        value=f"{int(stats.get('totalChapters', 0)):,}",
                        inline=True)
                    embed.add_field(name="Unique Users",
                                    value=str(stats.get('uniqueUsers', 0)),
                                    inline=True)
                    embed.add_field(
                        name="Queue Status",
                        value=
                        f"Queued: {queue.get('queued', 0)} | Processing: {queue.get('processing', 0)}",
                        inline=False)
                    embed.add_field(
                        name="Failure Rate (24h)",
                        value=f"{dashboard.get('failureRate', 0):.1f}%",
                        inline=True)

                    await interaction.followup.send(embed=embed,
                                                    ephemeral=True)

                elif view == "trending":
                    # Fetch trending content
                    resp = requests.get(
                        f'{API_BASE_URL}/api/analytics/trending', timeout=10)
                    trending = resp.json() if resp.ok else []

                    embed = discord.Embed(title="Trending Content (Top 10)",
                                          color=discord.Color.gold(),
                                          timestamp=discord.utils.utcnow())

                    if trending:
                        lines = []
                        for i, item in enumerate(trending[:10], 1):
                            title = item.get('novelTitle', 'Unknown')[:40]
                            if len(item.get('novelTitle', '')) > 40:
                                title += '...'
                            downloads = item.get('downloadCount', 0)
                            lines.append(
                                f"**{i}.** {title} ({downloads} downloads)")
                        embed.description = "\n".join(lines)
                    else:
                        embed.description = "No trending data available yet."

                    await interaction.followup.send(embed=embed,
                                                    ephemeral=True)

                elif view == "by-site":
                    # Fetch by-site stats
                    resp = requests.get(
                        f'{API_BASE_URL}/api/analytics/by-site', timeout=10)
                    by_site = resp.json() if resp.ok else []

                    embed = discord.Embed(title="Downloads by Site",
                                          color=discord.Color.green(),
                                          timestamp=discord.utils.utcnow())

                    if by_site:
                        lines = []
                        for item in by_site[:15]:
                            source = item.get('source', 'Unknown')
                            downloads = item.get('downloadCount', 0)
                            chapters = int(item.get('totalChapters', 0))
                            lines.append(
                                f"**{source}**: {downloads} downloads ({chapters:,} chapters)"
                            )
                        embed.description = "\n".join(lines)
                    else:
                        embed.description = "No site data available yet."

                    await interaction.followup.send(embed=embed,
                                                    ephemeral=True)

                elif view == "recent":
                    # Fetch recent activity
                    resp = requests.get(
                        f'{API_BASE_URL}/api/analytics/recent-activity',
                        timeout=10)
                    recent = resp.json() if resp.ok else []

                    embed = discord.Embed(title="Recent Downloads (Last 10)",
                                          color=discord.Color.purple(),
                                          timestamp=discord.utils.utcnow())

                    if recent:
                        lines = []
                        for item in recent[:10]:
                            title = item.get('novelTitle', 'Unknown')[:35]
                            if len(item.get('novelTitle', '')) > 35:
                                title += '...'
                            chapters = item.get('chapterCount', 0)
                            fmt = item.get('format', '').upper()
                            tier = item.get('userTier', 'normal')
                            lines.append(
                                f"**{title}**\n  {chapters} ch | {fmt} | {tier}"
                            )
                        embed.description = "\n".join(lines)
                    else:
                        embed.description = "No recent activity."

                    await interaction.followup.send(embed=embed,
                                                    ephemeral=True)

                elif view == "workers":
                    # Fetch worker status
                    resp = requests.get(f'{API_BASE_URL}/api/workers',
                                        timeout=10)
                    workers = resp.json() if resp.ok else []

                    embed = discord.Embed(title="Bot Workers Status",
                                          color=discord.Color.teal(),
                                          timestamp=discord.utils.utcnow())

                    if workers:
                        lines = []
                        for w in workers:
                            status = w.get('status', 'unknown')
                            status_icon = "Online" if status == 'online' else "Busy" if status == 'busy' else "Offline"
                            task = w.get('currentTask') or 'Idle'
                            completed = w.get('tasksCompleted', 0)
                            failed = w.get('tasksFailed', 0)
                            lines.append(
                                f"**{w.get('workerId', 'Unknown')}**\nStatus: {status_icon} | Task: {task}\nCompleted: {completed} | Failed: {failed}"
                            )
                        embed.description = "\n\n".join(lines)
                    else:
                        embed.description = "No workers registered."

                    await interaction.followup.send(embed=embed,
                                                    ephemeral=True)

            except Exception as e:
                logger.error(f"Analytics command error: {e}")
                await interaction.followup.send(
                    f"Error fetching analytics: {e}", ephemeral=True)

    async def _keep_alive_heartbeat(self):
        """Background task to keep bot awake every 5 minutes"""
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await asyncio.sleep(300)  # 5 minutes
                # Ping the Discord API to keep connection alive
                latency = self.latency
                logger.info(f"Keep-alive ping - Latency: {latency*1000:.2f}ms")
            except Exception as e:
                logger.warning(f"Keep-alive error: {e}")

    async def on_ready(self):
        """Sync slash commands when bot is ready"""
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')
        try:
            synced = await self.tree.sync(guild=discord.Object(id=SERVER_ID))
            logger.info(f"Synced {len(synced)} slash command(s)")
            print(f"Synced {len(synced)} slash command(s)")
        except Exception as e:
            logger.error(f"Error syncing commands: {e}")
            print(f"Error syncing commands: {e}")

        # Start keep-alive heartbeat if not already running
        if self.keep_alive_task is None or self.keep_alive_task.done():
            self.keep_alive_task = asyncio.create_task(
                self._keep_alive_heartbeat())
            logger.info("Started keep-alive heartbeat task")

        # Start worker registration heartbeat
        if not hasattr(
                self, 'worker_heartbeat_task'
        ) or self.worker_heartbeat_task is None or self.worker_heartbeat_task.done(
        ):
            self.worker_heartbeat_task = asyncio.create_task(
                self._worker_heartbeat())
            logger.info("Started worker heartbeat task")

        # Start stat channel update task
        if not hasattr(
                self, 'stat_channels_task'
        ) or self.stat_channels_task is None or self.stat_channels_task.done():
            self.stat_channels_task = asyncio.create_task(
                self._stat_channels_update_loop())
            logger.info("Started stat channels update task")

    async def on_disconnect(self):
        """Log bot disconnection events"""
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.critical(f"[{timestamp}] BOT DISCONNECTED from Discord")
        logger.critical(f"[{timestamp}] Bot will attempt automatic reconnection...")

    async def _worker_heartbeat(self):
        """Background task to register worker and send heartbeats"""
        await self.wait_until_ready()
        worker_id = f"bot-{self.user.id}" if self.user else "bot-unknown"

        while not self.is_closed():
            try:
                # Count active tasks
                active_count = len(
                    [s for s in self.user_states.values() if s.get('step')])
                status = 'busy' if active_count > 0 else 'online'
                current_task = f"{active_count} active downloads" if active_count > 0 else None

                await register_worker(worker_id, status, current_task,
                                      self.loop)
                logger.debug(f"Worker heartbeat sent: {worker_id} - {status}")
            except Exception as e:
                logger.warning(f"Worker heartbeat error: {e}")

            await asyncio.sleep(60)  # Send heartbeat every minute

    async def _stat_channels_update_loop(self):
        """Background task to update stat channels every 10 minutes"""
        await self.wait_until_ready()
        self._last_stat_values = {
        }  # Track last values to avoid unnecessary API calls
        self._stat_channel_ids = {}  # Cache channel IDs

        while not self.is_closed():
            try:
                await self._update_stat_channels()
            except Exception as e:
                logger.error(f"Stat channel update error: {e}")

            await asyncio.sleep(STAT_UPDATE_INTERVAL)

    async def _get_or_create_stat_category(
            self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        """Get or create the stat category with proper permissions"""
        # Look for existing category
        for category in guild.categories:
            if category.name == STAT_CATEGORY_NAME:
                return category

        # Create category with view-only permissions
        overwrites = {
            guild.default_role:
            discord.PermissionOverwrite(view_channel=True,
                                        connect=False,
                                        send_messages=False,
                                        add_reactions=False,
                                        create_public_threads=False,
                                        create_private_threads=False,
                                        manage_webhooks=False)
        }

        try:
            category = await guild.create_category(STAT_CATEGORY_NAME,
                                                   overwrites=overwrites)
            logger.info(f"Created stat category: {STAT_CATEGORY_NAME}")
            return category
        except discord.Forbidden:
            logger.error(f"Failed to create stat category: Missing 'Manage Channels' permission. Please grant this permission to the bot's role.")
            return None
        except Exception as e:
            logger.error(f"Failed to create stat category: {e}")
            return None

    async def _get_or_create_stat_channel(
            self, category: discord.CategoryChannel, template: str,
            current_value: str) -> Optional[discord.VoiceChannel]:
        """Get or create a stat voice channel under the category"""
        expected_name = template.format(count=current_value)

        # Look for existing channel with this template prefix
        template_prefix = template.split("{count}")[0]
        for channel in category.voice_channels:
            if channel.name.startswith(template_prefix):
                return channel

        # Create new voice channel
        overwrites = {
            category.guild.default_role:
            discord.PermissionOverwrite(view_channel=True,
                                        connect=False,
                                        send_messages=False,
                                        add_reactions=False)
        }

        try:
            channel = await category.create_voice_channel(
                expected_name, overwrites=overwrites)
            logger.info(f"Created stat channel: {expected_name}")
            return channel
        except Exception as e:
            logger.error(f"Failed to create stat channel: {e}")
            return None

    async def _fetch_stat_values(self) -> Dict[str, any]:
        """Fetch all stat values from various sources (API calls disabled)"""
        stats = {}
        guild = self.get_guild(SERVER_ID)

        # 1. Verified count - members with Verified role
        try:
            if guild:
                verified_role = discord.utils.get(guild.roles, name=VERIFIED_ROLE_NAME)
                stats['verified'] = len(verified_role.members) if verified_role else 0
            else:
                stats['verified'] = 0
        except Exception as e:
            logger.warning(f"Failed to get verified count: {e}")
            stats['verified'] = 0

        # API-based stats are set to 0 or placeholders since web server is disabled
        stats['novels_today'] = 0
        stats['manga_today'] = 0
        stats['novels_alltime'] = 0
        stats['manga_alltime'] = 0

        # 6. Active jobs - count from user_states
        try:
            active_count = len([
                s for s in self.user_states.values() if s.get('step')
                and 'scraping' in str(s.get('step', '')).lower()
            ])
            stats['active_jobs'] = active_count
        except:
            stats['active_jobs'] = 0

        stats['queue'] = 0
        stats['top_site'] = '—'
        stats['failures_today'] = 0

        return stats

    async def _update_stat_channels(self):
        """Update all stat channels with current values"""
        guild = self.get_guild(SERVER_ID)
        if not guild:
            logger.warning("Could not find guild for stat channel updates")
            return

        # Get or create category
        category = await self._get_or_create_stat_category(guild)
        if not category:
            return

        # Fetch all stats
        stats = await self._fetch_stat_values()

        # Update each channel
        for channel_config in STAT_CHANNELS:
            key = channel_config['key']
            template = channel_config['template']
            value = stats.get(key)

            # Format value
            if key == 'top_site':
                formatted = str(value) if value else '—'
            else:
                formatted = format_stat_number(
                    value) if value is not None else '—'

            expected_name = template.format(count=formatted)

            # Get or create channel
            channel = await self._get_or_create_stat_channel(
                category, template, formatted)
            if not channel:
                continue

            # Only rename if value changed
            if channel.name != expected_name:
                try:
                    await channel.edit(name=expected_name)
                    logger.info(f"Updated stat channel: {expected_name}")
                except discord.Forbidden:
                    logger.error(f"Missing Access (error code: 50001): Cannot edit channel name {channel.name}. Please ensure the bot has 'Manage Channels' permission and that its role is higher than the channel's permissions.")
                except discord.HTTPException as e:
                    if e.status == 429:  # Rate limited
                        logger.warning(
                            f"Rate limited updating {key}, will retry next cycle"
                        )
                    else:
                        logger.error(
                            f"Failed to update stat channel {key}: {e}")
                except Exception as e:
                    logger.error(f"Error updating stat channel {key}: {e}")

    async def log_to_discord(self,
                             title: str,
                             description: str,
                             color=discord.Color.blue()):
        """Log an event to the Discord logs channel"""
        try:
            guild = self.get_guild(SERVER_ID)
            if not guild:
                return

            logs_channel = guild.get_channel(LOGS_CHANNEL_ID)
            if not logs_channel:
                return

            embed = discord.Embed(title=title,
                                  description=description,
                                  color=color,
                                  timestamp=discord.utils.utcnow())
            await logs_channel.send(embed=embed)
        except Exception as e:
            logger.warning(f"Could not log to Discord: {e}")

    async def _get_member_in_server(self,
                                    user_id: int) -> Optional[discord.Member]:
        """Get the member object from the main server, returns None if not in server"""
        try:
            guild = self.get_guild(SERVER_ID)
            if not guild:
                guild = await self.fetch_guild(SERVER_ID)
            if guild:
                try:
                    member = await guild.fetch_member(user_id)
                    return member
                except discord.NotFound:
                    return None
        except Exception as e:
            logger.error(f"Error checking server membership: {e}")
        return None

    async def _get_daily_usage(self, user_id: str) -> Dict[str, int]:
        """Get user's daily chapter usage.

        Remote database / API calls are disabled; this always returns 0 usage
        so the bot can operate without an external backend.
        """
        return {
            'novel_used': 0,
            'manga_used': 0,
            'novel_bonus_used': 0,
            'manga_bonus_used': 0,
        }

    def _normalize_novel_key(self, url: str) -> str:
        """Create a normalized key from URL for tracking - immune to title variations"""
        # Use URL hostname + path to create a stable key
        import re
        from urllib.parse import urlparse
        try:
            parsed = urlparse(url)
            # Get hostname and path, normalize
            host = parsed.netloc.lower().replace('www.', '')
            path = parsed.path.lower().strip('/')
            # Remove trailing chapter info (e.g., /chapter-1, /1)
            path = re.sub(r'/chapter[-_]?\d+.*$', '', path)
            path = re.sub(r'/\d+$', '', path)
            # Remove special chars and create key
            key = f"{host}_{path}"
            key = re.sub(r'[^\w]', '_', key)
            key = re.sub(r'_+', '_', key).strip('_')
            return key[:150] if key else 'unknown'
        except:
            # Fallback: normalize as simple string
            key = url.lower()
            key = re.sub(r'[^\w]', '_', key)
            return key[:150] if key else 'unknown'

    async def _get_novel_usage(self, user_id: str, novel_key: str) -> Dict:
        """Get user's usage for a specific novel today"""
        today = get_gmt8_date()
        try:
            # URL encode the novel_key to handle special characters
            import urllib.parse
            encoded_key = urllib.parse.quote(novel_key, safe='')
            response = requests.get(
                f'{API_BASE_URL}/api/novel-usage/{user_id}/{encoded_key}/{today}',
                timeout=5)
            if response.status_code == 200:
                data = response.json()
                return {
                    'chapters_used': data.get('chaptersUsed', 0),
                    'total_chapters': data.get('totalChapters', 0)
                }
        except Exception as e:
            logger.warning(f"Error getting novel usage: {e}")
        return {'chapters_used': 0, 'total_chapters': 0}

    async def _update_novel_usage(self,
                                  user_id: str,
                                  novel_key: str,
                                  novel_title: str,
                                  total_chapters: int,
                                  chapters: int,
                                  content_type: str = 'novel'):
        """Update user's usage for a specific novel today"""
        today = get_gmt8_date()
        try:
            requests.post(f'{API_BASE_URL}/api/novel-usage',
                          json={
                              'discordUserId': user_id,
                              'novelKey': novel_key,
                              'novelTitle': novel_title,
                              'date': today,
                              'contentType': content_type,
                              'totalChapters': total_chapters,
                              'chapters': chapters
                          },
                          timeout=5)
        except Exception as e:
            logger.warning(f"Error updating novel usage: {e}")

    async def _update_daily_usage(self,
                                  user_id: str,
                                  novel_chapters: int = 0,
                                  manga_chapters: int = 0,
                                  novel_bonus_used: int = 0,
                                  manga_bonus_used: int = 0):
        """Update user's daily chapter usage in database including bonus usage tracking"""
        today = get_gmt8_date()
        try:
            requests.post(f'{API_BASE_URL}/api/daily-limits',
                          json={
                              'discordUserId': user_id,
                              'date': today,
                              'novelChapters': novel_chapters,
                              'mangaChapters': manga_chapters,
                              'novelBonusUsed': novel_bonus_used,
                              'mangaBonusUsed': manga_bonus_used
                          },
                          timeout=5)
        except Exception as e:
            logger.warning(f"Error updating daily usage: {e}")

    def _calculate_limit(self, tier: str, total_chapters: int,
                         content_type: str) -> Dict:
        """
        Calculate download limit based on tier and total chapters

        Returns: {max_allowed: int, daily_bonus: int, percent_limit: int, percent: int, is_unlimited: bool}

        NOTE: Currently all users have unlimited downloads. The percentage-based limits below
        are commented out for potential future implementation.
        """
        # UNLIMITED FOR ALL USERS - uncomment below to re-enable limits
        return {
            'max_allowed': 999999,
            'daily_bonus': 0,
            'percent_limit': 999999,
            'percent': 100,
            'is_unlimited': True
        }

        # === FUTURE LIMIT IMPLEMENTATION (commented out) ===
        # if tier in ('catnip', 'sponsor'):
        #     return {'max_allowed': 999999, 'daily_bonus': 0, 'percent_limit': 999999, 'percent': 100, 'is_unlimited': True}
        #
        # if content_type == 'novel':
        #     bonus = DAILY_BONUS_NOVEL_COFFEE if tier == 'coffee' else DAILY_BONUS_NOVEL_VERIFIED
        # else:  # manga
        #     bonus = DAILY_BONUS_MANGA_COFFEE if tier == 'coffee' else DAILY_BONUS_MANGA_VERIFIED
        #
        # if tier == 'coffee':
        #     # Coffee: 80% of total + bonus (no daily cap)
        #     percent = 80
        #     percent_limit = max(1, math.ceil(total_chapters * 0.8))  # Use ceiling, min 1
        #     max_allowed = percent_limit + bonus
        # else:
        #     # Verified: 20% of total + bonus
        #     percent = 20
        #     percent_limit = max(1, math.ceil(total_chapters * 0.2))  # Use ceiling, min 1
        #     max_allowed = percent_limit + bonus
        #
        # return {'max_allowed': max_allowed, 'daily_bonus': bonus, 'percent_limit': percent_limit, 'percent': percent, 'is_unlimited': False}

    def _calculate_bonus_used(self, chapters_downloaded: int,
                              limit_info: Dict) -> int:
        """
        Calculate how much of the download came from the shared daily bonus pool.

        Logic:
        - If download <= percent_remaining: all from percentage, 0 from bonus
        - If download > percent_remaining: excess comes from bonus

        Returns: number of chapters that count against daily bonus
        """
        if not limit_info:
            return 0
        if limit_info.get('is_unlimited', False):
            return 0

        percent_remaining = limit_info.get('percent_remaining', 0)

        if chapters_downloaded <= percent_remaining:
            # All from percentage, none from bonus
            return 0
        else:
            # Excess comes from bonus
            return chapters_downloaded - percent_remaining

    async def _check_download_allowed(self,
                                      user_id: str,
                                      tier: str,
                                      total_chapters: int,
                                      requested_chapters: int,
                                      content_type: str,
                                      novel_url: str = '',
                                      novel_title: str = '') -> Dict:
        """
        Check if download is allowed within per-novel limits

        NOTE: Currently all users have unlimited downloads. The limit logic below
        is commented out for potential future implementation.

        Returns: {allowed: bool, remaining: int, max_now: int, message: str, limit_info: Dict, needs_confirm: bool, novel_key: str}
        """
        # UNLIMITED FOR ALL USERS
        return {
            'allowed': True,
            'remaining': float('inf'),
            'max_now': requested_chapters,
            'message': None,
            'limit_info': None,
            'needs_confirm': False,
            'novel_key': ''
        }

        # === FUTURE LIMIT IMPLEMENTATION (commented out) ===
        # if tier in ('catnip', 'sponsor'):
        #     return {
        #         'allowed': True,
        #         'remaining': float('inf'),
        #         'max_now': requested_chapters,
        #         'message': None,
        #         'limit_info': None,
        #         'needs_confirm': False,
        #         'novel_key': ''
        #     }
        #
        # novel_key = self._normalize_novel_key(novel_url) if novel_url else 'unknown'
        # novel_usage = await self._get_novel_usage(user_id, novel_key)
        # used_this_novel = novel_usage.get('chapters_used', 0)
        # daily_usage = await self._get_daily_usage(user_id)
        # if content_type == 'novel':
        #     bonus_used_today = daily_usage.get('novel_bonus_used', 0)
        # else:
        #     bonus_used_today = daily_usage.get('manga_bonus_used', 0)
        # limit_info = self._calculate_limit(tier, total_chapters, content_type)
        # percent_limit = limit_info['percent_limit']
        # daily_bonus = limit_info['daily_bonus']
        # percent_remaining = max(0, percent_limit - used_this_novel)
        # bonus_remaining = max(0, daily_bonus - bonus_used_today)
        # remaining = percent_remaining + bonus_remaining
        # max_allowed = percent_limit + bonus_remaining
        # percent = limit_info['percent']
        # limit_breakdown = f"{percent}% of {total_chapters} = {percent_limit} + {bonus_remaining}/{daily_bonus} bonus = {max_allowed} max"
        # limit_info['bonus_remaining'] = bonus_remaining
        # limit_info['percent_remaining'] = percent_remaining
        #
        # if remaining == 0:
        #     upgrade_msg = "Upgrade to Coffee for more chapters!" if tier == 'normal' else "Upgrade to Catnip for unlimited downloads!"
        #     reason = ""
        #     if percent_remaining == 0 and bonus_remaining == 0:
        #         reason = f"You've used all {percent_limit} chapters from percentage limit on this novel AND all {daily_bonus} bonus chapters today."
        #     elif percent_remaining == 0:
        #         reason = f"You've used all {percent_limit} chapters from percentage limit on this novel."
        #     else:
        #         reason = f"You've used all {daily_bonus} bonus chapters today."
        #     return {
        #         'allowed': False,
        #         'remaining': 0,
        #         'max_now': 0,
        #         'message': f"**Limit reached for {novel_title}**\n\n{reason}\n\n{limit_breakdown}\n\n{upgrade_msg}\n\nTry a different novel for a fresh percentage limit!",
        #         'limit_info': limit_info,
        #         'needs_confirm': False,
        #         'novel_key': novel_key
        #     }
        #
        # if requested_chapters > remaining:
        #     return {
        #         'allowed': True,
        #         'remaining': remaining,
        #         'max_now': remaining,
        #         'message': f"**Limit for this novel:** {limit_breakdown}\n**Used (this novel):** {used_this_novel}\n**Bonus used today:** {bonus_used_today}/{daily_bonus}\n**Remaining:** {remaining}\n\nYou requested {requested_chapters} chapters but can only download {remaining} more.",
        #         'limit_info': limit_info,
        #         'needs_confirm': True,
        #         'novel_key': novel_key
        #     }
        #
        # return {
        #     'allowed': True,
        #     'remaining': remaining,
        #     'max_now': requested_chapters,
        #     'message': None,
        #     'limit_info': limit_info,
        #     'needs_confirm': False,
        #     'novel_key': novel_key
        # }

    def _format_remaining_text(self,
                               tier: str,
                               usage: Dict,
                               novel_total: int = 0,
                               manga_total: int = 0) -> str:
        """Format remaining chapters text for display"""
        if tier in ('catnip', 'sponsor'):
            return "**Unlimited** downloads available"

        novel_limit = self._calculate_limit(
            tier, novel_total, 'novel')['max_allowed'] if novel_total else 0
        manga_limit = self._calculate_limit(
            tier, manga_total, 'manga')['max_allowed'] if manga_total else 0

        novel_remaining = max(
            0, novel_limit -
            usage.get('novel_used', 0)) if novel_total else "N/A"
        manga_remaining = max(
            0, manga_limit -
            usage.get('manga_used', 0)) if manga_total else "N/A"

        return f"**Remaining today:** Novel: {novel_remaining} | Manga: {manga_remaining}"

    def _has_role(self, member: discord.Member, role_name: str) -> bool:
        """Check if member has a specific role (case-insensitive)"""
        if not member:
            return False
        for role in member.roles:
            if role.name.lower() == role_name.lower():
                return True
        return False

    async def _check_user_access(self,
                                 message: discord.Message) -> Tuple[bool, str]:
        """Check if user has access to use the bot.
        Returns: (has_access, user_tier) where tier is 'normal', 'patreon', or 'vip'
        """
        user_id = message.author.id

        # Get member from server
        member = await self._get_member_in_server(user_id)

        logger.info(f"Checking access for user {user_id}. Member found in guild {SERVER_ID}: {member is not None}")

        # Check 1: Must be in server
        if not member:
            logger.warning(f"Access denied: User {user_id} not in server. SERVER_ID: {SERVER_ID}")
            await message.channel.send(
                f"❌ You must join our server to use this bot: {SERVER_INVITE}")
            return (False, 'normal')

        # Check 2: Must be verified
        if not self._has_role(member, VERIFIED_ROLE_NAME):
            logger.info(f"User {user_id} in server but not verified. Role checked: {VERIFIED_ROLE_NAME}")
            await message.channel.send(
                f"❌ You must be verified to use this bot. Please verify first in the server: {SERVER_INVITE}"
            )
            await self.log_to_discord(
                "⚠️ Unauthorized Access Attempt",
                f"User {message.author.name} (ID: {message.author.id}) tried to use bot without verification",
                discord.Color.orange())
            return (False, 'normal')

        # Check 3: Determine user tier (Sponsor > Catnip > Coffee > Normal)
        if self._has_role(member, SPONSOR_ROLE_NAME):
            return (True, 'sponsor')
        elif self._has_role(member, CATNIP_ROLE_NAME):
            return (True, 'catnip')
        elif self._has_role(member, COFFEE_ROLE_NAME):
            return (True, 'coffee')
        else:
            return (True, 'normal')

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for deduplication (lowercase host, strip trailing slash)"""
        url = url.lower().strip()
        if url.endswith('/'):
            url = url[:-1]
        return url

    async def _handle_suggest_command(self, message: discord.Message):
        """Handle the !suggest command to submit a new site suggestion"""
        # Parse: !suggest <url> [name] [description]
        content = message.content.strip()
        # Remove command prefix
        if content.lower().startswith('!suggest '):
            content = content[9:]
        elif content.lower().startswith('suggest '):
            content = content[8:]

        parts = content.strip().split(' ', 2)
        if not parts or not parts[0]:
            await message.channel.send(
                "**How to suggest a site:**\n"
                "`!suggest <url> <site name> [description]`\n\n"
                "Example: `!suggest https://example.com ExampleNovels A great novel site with lots of content`"
            )
            return

        url = self._normalize_url(parts[0])
        site_name = parts[1] if len(parts) > 1 else url.split(
            '/')[2] if '/' in url else url
        description = parts[2] if len(parts) > 2 else None

        # Validate URL format
        if not url.startswith('http://') and not url.startswith('https://'):
            await message.channel.send(
                "Please provide a valid URL starting with http:// or https://")
            return

        try:
            data = {
                'siteUrl': url,
                'siteName': site_name,
                'description': description,
                'submittedBy': str(message.author.id),
                'submittedByName': str(message.author.name)
            }

            # Use run_in_executor to avoid blocking the event loop
            resp = await self.loop.run_in_executor(
                None, lambda: requests.post(
                    f"{API_BASE_URL}/api/suggestions", json=data, timeout=10))

            if resp.status_code == 201:
                result = resp.json()
                embed = discord.Embed(
                    title="Site Suggestion Submitted",
                    description=f"Thank you for suggesting **{site_name}**!",
                    color=0x00ff00)
                embed.add_field(name="URL", value=url, inline=False)
                if description:
                    embed.add_field(name="Description",
                                    value=description,
                                    inline=False)
                embed.add_field(name="Votes", value="1", inline=True)
                embed.set_footer(
                    text=
                    f"ID: {result.get('id', '?')} | Others can vote with: !vote {result.get('id', '?')}"
                )
                await message.channel.send(embed=embed)
            elif resp.status_code == 409:
                existing = resp.json().get('existing', {})
                await message.channel.send(
                    f"This site has already been suggested!\n"
                    f"**{existing.get('siteName', 'Unknown')}** - {existing.get('voteCount', 0)} votes\n"
                    f"Vote for it with: `!vote {existing.get('id', '?')}`")
            else:
                await message.channel.send(
                    "Failed to submit suggestion. Please try again.")
        except Exception as e:
            logger.error(f"Error submitting suggestion: {e}")
            await message.channel.send(
                "Failed to submit suggestion. Please try again later.")

    async def _handle_vote_command(self, message: discord.Message):
        """Handle the !vote command to vote for a site suggestion"""
        content = message.content.strip()
        # Remove command prefix
        if content.lower().startswith('!vote '):
            content = content[6:]
        elif content.lower().startswith('vote '):
            content = content[5:]

        try:
            suggestion_id = int(content.strip())
        except ValueError:
            await message.channel.send(
                "Please provide a valid suggestion ID. Example: `!vote 1`")
            return

        try:
            data = {'discordUserId': str(message.author.id)}
            # Use run_in_executor to avoid blocking the event loop
            resp = await self.loop.run_in_executor(
                None, lambda: requests.post(
                    f"{API_BASE_URL}/api/suggestions/{suggestion_id}/vote",
                    json=data,
                    timeout=10))

            if resp.status_code == 200:
                result = resp.json()
                embed = discord.Embed(
                    title="Vote Recorded",
                    description=
                    f"You voted for **{result.get('siteName', 'Unknown')}**",
                    color=0x00ff00)
                embed.add_field(name="Total Votes",
                                value=str(result.get('voteCount', 1)),
                                inline=True)
                await message.channel.send(embed=embed)
            elif resp.status_code == 409:
                await message.channel.send(
                    "You've already voted for this suggestion!")
            elif resp.status_code == 404:
                await message.channel.send(
                    "Suggestion not found. Use `!suggestions` to see available options."
                )
            else:
                await message.channel.send("Failed to vote. Please try again.")
        except Exception as e:
            logger.error(f"Error voting: {e}")
            await message.channel.send(
                "Failed to vote. Please try again later.")

    async def _handle_list_suggestions(self, message: discord.Message):
        """Handle the !suggestions command to list top site suggestions"""
        try:
            # Use run_in_executor to avoid blocking the event loop
            resp = await self.loop.run_in_executor(
                None, lambda: requests.get(f"{API_BASE_URL}/api/suggestions",
                                           timeout=10))

            if resp.status_code == 200:
                suggestions = resp.json()

                if not suggestions:
                    await message.channel.send(
                        "No site suggestions yet!\n"
                        "Be the first to suggest one: `!suggest <url> <name> [description]`"
                    )
                    return

                embed = discord.Embed(
                    title="Top Site Suggestions",
                    description=
                    "Vote for sites you want added! Use `!vote <id>` to vote.",
                    color=0x5865F2)

                for i, s in enumerate(suggestions[:10], 1):
                    status_emoji = {
                        'pending': '',
                        'approved': '',
                        'implemented': '',
                        'rejected': ''
                    }.get(s.get('status', 'pending'), '')

                    field_value = f"{s.get('siteUrl', 'N/A')}\n{s.get('description', 'No description')[:100]}"
                    embed.add_field(
                        name=
                        f"{status_emoji} #{s.get('id')} {s.get('siteName', 'Unknown')} ({s.get('voteCount', 0)} votes)",
                        value=field_value,
                        inline=False)

                embed.set_footer(
                    text=
                    "Suggest a new site: !suggest <url> <name> [description]")
                await message.channel.send(embed=embed)
            else:
                await message.channel.send(
                    "Failed to fetch suggestions. Please try again.")
        except Exception as e:
            logger.error(f"Error fetching suggestions: {e}")
            await message.channel.send(
                "Failed to fetch suggestions. Please try again later.")

    async def _handle_settings_command(self, message: discord.Message):
        """Handle !settings command for user preferences"""
        user_id = str(message.author.id)
        content = message.content.strip().lower()
        
        # Get user tier
        member = await self._get_member_in_server(message.author.id)
        if self._has_role(member, SPONSOR_ROLE_NAME):
            user_tier = 'sponsor'
        elif self._has_role(member, CATNIP_ROLE_NAME):
            user_tier = 'catnip'
        elif self._has_role(member, COFFEE_ROLE_NAME):
            user_tier = 'coffee'
        else:
            user_tier = 'verified'
        
        # Parse command arguments
        parts = content.split()
        
        # Just "!settings" - show current settings
        if len(parts) == 1:
            display = get_settings_display(user_id, user_tier)
            await message.channel.send(display)
            return
        
        arg = parts[1].lower()
        
        # Check tier access for premium features
        is_coffee_plus = user_tier in ('coffee', 'catnip', 'sponsor')
        
        # Handle "epub2" or "epub3"
        if arg == 'epub2':
            if settings_manager.set_epub_format(user_id, 'epub2'):
                await message.channel.send("✅ Default format set to **EPUB 2.0**")
            else:
                await message.channel.send("❌ Failed to update setting.")
            return
        
        if arg == 'epub3':
            if not is_coffee_plus:
                await message.channel.send("☕ EPUB 3.0 is available for **Coffee+** tiers.\n" + PATREON_LINK)
                return
            if settings_manager.set_epub_format(user_id, 'epub3'):
                await message.channel.send("✅ Default format set to **EPUB 3.0**")
            else:
                await message.channel.send("❌ Failed to update setting.")
            return
        
        # Handle "style [name]"
        if arg == 'style':
            if not is_coffee_plus:
                await message.channel.send("☕ Custom styles are available for **Coffee+** tiers.\n" + PATREON_LINK)
                return
            
            if len(parts) < 3:
                # Show available styles
                styles_text = "**Available Styles:**\n"
                for key, info in EPUB_STYLES.items():
                    styles_text += f"• `{key}` - {info['description']}\n"
                styles_text += "\n*Usage: !settings style classic*"
                await message.channel.send(styles_text)
                return
            
            style_name = parts[2].lower()
            if style_name not in EPUB_STYLES:
                await message.channel.send(f"❌ Unknown style: `{style_name}`\nAvailable: classic, modern, compact, cozy")
                return
            
            if settings_manager.set_style(user_id, style_name):
                await message.channel.send(f"✅ Style set to **{EPUB_STYLES[style_name]['name']}**")
            else:
                await message.channel.send("❌ Failed to update setting.")
            return
        
        # Handle "audio" toggle
        if arg == 'audio':
            if not is_coffee_plus:
                await message.channel.send("☕ Audio is available for **Coffee+** tiers.\n" + PATREON_LINK)
                return
            
            new_value = settings_manager.toggle_audio(user_id)
            status = "enabled" if new_value else "disabled"
            await message.channel.send(f"✅ Audio {status} for EPUB downloads.")
            return
        
        # Handle "notes" toggle (TL notes/footnotes)
        if arg == 'notes':
            if not is_coffee_plus:
                await message.channel.send("☕ Notes toggle is available for **Coffee+** tiers.\n" + PATREON_LINK)
                return
            
            new_value = settings_manager.toggle_notes(user_id)
            if new_value:
                await message.channel.send("✅ TL notes & footnotes will appear at **end of each chapter**.")
            else:
                await message.channel.send("✅ TL notes & footnotes will be **removed** from chapters.")
            return
        
        # Handle "voice" toggle or set (male/female)
        if arg == 'voice':
            if not is_coffee_plus:
                await message.channel.send("☕ Voice selection is available for **Coffee+** tiers.\n" + PATREON_LINK)
                return
            
            # Check if specific voice provided
            if len(parts) >= 3:
                voice = parts[2].lower()
                if voice in ['male', 'female']:
                    settings_manager.set_voice(user_id, voice)
                    await message.channel.send(f"✅ TTS voice set to **{voice.capitalize()}**.")
                else:
                    await message.channel.send("❌ Voice must be `male` or `female`.")
            else:
                # Toggle between male/female
                new_value = settings_manager.toggle_voice(user_id)
                await message.channel.send(f"✅ TTS voice set to **{new_value.capitalize()}**.")
            return
        
        # Handle "reset"
        if arg == 'reset':
            settings_manager.reset_settings(user_id)
            await message.channel.send("✅ Settings reset to defaults.")
            return
        
        # Unknown argument
        await message.channel.send("❌ Unknown option. Try: `!settings`, `!settings epub2`, `!settings style classic`, `!settings audio`, `!settings voice`, `!settings notes`, `!settings reset`")

    async def _handle_help_command(self, message: discord.Message):
        """Handle !help command"""
        # Get user tier
        member = await self._get_member_in_server(message.author.id)
        if self._has_role(member, SPONSOR_ROLE_NAME):
            user_tier = 'sponsor'
        elif self._has_role(member, CATNIP_ROLE_NAME):
            user_tier = 'catnip'
        elif self._has_role(member, COFFEE_ROLE_NAME):
            user_tier = 'coffee'
        else:
            user_tier = 'verified'
        
        is_coffee_plus = user_tier in ('coffee', 'catnip', 'sponsor')
        
        embed = discord.Embed(
            title="📚 Meowi's Tea and Coffee Bot",
            description="Download novels in EPUB or PDF format.",
            color=0x5865F2
        )
        
        # Basic Commands
        embed.add_field(
            name="📖 Download",
            value=(
                "• Send a **title** to search\n"
                "• Paste a **URL** for direct download\n"
                "• Type `cancel` to stop anytime"
            ),
            inline=False
        )
        
        # Library Commands
        embed.add_field(
            name="📚 Library & History",
            value=(
                "`!library` - Your saved novels\n"
                "`!history` - Recent downloads\n"
                "`!continue` - Resume last download\n"
                "`!stats` - Your download stats"
            ),
            inline=True
        )
        
        # Settings
        settings_value = "`!settings` - View/change preferences\n"
        if is_coffee_plus:
            settings_value += (
                "`!settings epub3` - Use EPUB 3.0\n"
                "`!settings style [name]` - Change style\n"
                "`!settings audio` - Toggle audio"
            )
        else:
            settings_value += "☕ *More options with Coffee tier*"
        
        embed.add_field(
            name="⚙️ Settings",
            value=settings_value,
            inline=True
        )
        
        # Format Options
        format_value = "**Default:** EPUB 2.0, Classic style\n"
        if is_coffee_plus:
            format_value += (
                "**Formats:** `epub2`, `epub3`\n"
                "**Styles:** classic, modern, compact, cozy\n"
                "**Audio:** Include TTS in EPUB"
            )
        else:
            format_value += "☕ *EPUB 3.0, styles, audio with Coffee+*"
        
        embed.add_field(
            name="📦 Format Options",
            value=format_value,
            inline=False
        )
        
        # Other Commands
        embed.add_field(
            name="ℹ️ Other",
            value=(
                "`!tiers` - Compare subscription tiers\n"
                "`!suggestions` - Suggest new sites\n"
                "`!ttshelp` - Voice chat commands"
            ),
            inline=False
        )
        
        embed.set_footer(text=f"Your tier: {user_tier.title()} | Join: {SERVER_INVITE}")
        
        await message.channel.send(embed=embed)

    async def _handle_history_command(self, message: discord.Message):
        """Handle !history command to show recent downloads"""
        user_id = str(message.author.id)
        history = get_history(user_id, limit=10)
        
        if not history:
            await message.channel.send("📭 No download history yet. Start by sending a novel title or URL!")
            return
        
        embed = discord.Embed(
            title="📜 Recent Downloads",
            color=0x5865F2
        )
        
        for i, entry in enumerate(history[:10], 1):
            title = entry.get('title', 'Unknown')[:40]
            if len(entry.get('title', '')) > 40:
                title += '...'
            
            ch_start = entry.get('chapter_start', '?')
            ch_end = entry.get('chapter_end', '?')
            fmt = entry.get('format', '?').upper()
            timestamp = entry.get('timestamp', '')[:10]  # Just the date
            
            embed.add_field(
                name=f"{i}. {title}",
                value=f"Ch. {ch_start}-{ch_end} | {fmt} | {timestamp}",
                inline=False
            )
        
        embed.set_footer(text="Use !continue to resume your last novel")
        await message.channel.send(embed=embed)

    async def _handle_library_command(self, message: discord.Message):
        """Handle !library command to show user's novel library"""
        user_id = str(message.author.id)
        library = get_library(user_id)
        
        if not library:
            await message.channel.send("📚 Your library is empty. Download some novels to get started!")
            return
        
        embed = discord.Embed(
            title="📚 Your Library",
            description=f"**{len(library)}** novels downloaded",
            color=0x5865F2
        )
        
        # Show up to 10 novels
        for i, (key, data) in enumerate(list(library.items())[:10], 1):
            title = data['title'][:35]
            if len(data['title']) > 35:
                title += '...'
            
            downloads = data['downloads']
            latest = downloads[0]
            ch_end = latest.get('chapter_end', '?')
            
            embed.add_field(
                name=f"{i}. {title}",
                value=f"Last: Ch. {ch_end} | {len(downloads)} downloads",
                inline=False
            )
        
        if len(library) > 10:
            embed.set_footer(text=f"...and {len(library) - 10} more novels")
        else:
            embed.set_footer(text="Use !check [title] to check for updates")
        
        await message.channel.send(embed=embed)

    async def _handle_continue_command(self, message: discord.Message):
        """Handle !continue command to resume last download"""
        user_id = str(message.author.id)
        last = get_last_download(user_id)
        
        if not last:
            await message.channel.send("No previous download found. Start by sending a novel title or URL!")
            return
        
        title = last.get('title', 'Unknown')
        novel_url = last.get('novel_url', '')
        ch_end = last.get('chapter_end', 0)
        
        if not novel_url:
            await message.channel.send(f"Can't continue **{title}** - no URL saved.")
            return
        
        await message.channel.send(
            f"📖 **Continue: {title}**\n\n"
            f"Last downloaded: Chapter {ch_end}\n"
            f"URL: {novel_url}\n\n"
            f"To download more chapters, paste the URL and specify the range:\n"
            f"`{ch_end + 1}-end` for all new chapters"
        )

    async def _handle_stats_command(self, message: discord.Message):
        """Handle !stats command to show user statistics"""
        user_id = str(message.author.id)
        stats = get_stats(user_id)
        
        embed = discord.Embed(
            title="📊 Your Download Stats",
            color=0x5865F2
        )
        
        embed.add_field(name="📥 Total Downloads", value=str(stats['total_downloads']), inline=True)
        embed.add_field(name="📚 Unique Novels", value=str(stats['unique_novels']), inline=True)
        embed.add_field(name="📖 Total Chapters", value=str(stats['total_chapters']), inline=True)
        
        if stats['favorite_format']:
            embed.add_field(name="❤️ Favorite Format", value=stats['favorite_format'].upper(), inline=True)
        
        if stats['first_download']:
            first_date = stats['first_download'][:10]
            embed.add_field(name="🗓️ First Download", value=first_date, inline=True)
        
        await message.channel.send(embed=embed)

    async def _handle_tiers_command(self, message: discord.Message):
        """Handle !tiers command to show tier comparison"""
        embed = discord.Embed(
            title="📊 Tier Comparison",
            description="Support the bot and unlock premium features!",
            color=0x5865F2
        )
        
        embed.add_field(
            name="🆓 Verified (Free)",
            value=(
                "• 5 parallel workers\n"
                "• EPUB 2.0 format\n"
                "• Classic style only\n"
                "• 1 download at a time\n"
                "• Forever history"
            ),
            inline=True
        )
        
        embed.add_field(
            name="☕ Coffee ($3/mo)",
            value=(
                "• 15 parallel workers\n"
                "• EPUB 2.0 & 3.0\n"
                "• All 4 styles\n"
                "• Audio TTS option\n"
                "• 3 batch downloads\n"
                "• Forever history"
            ),
            inline=True
        )
        
        embed.add_field(
            name="🐱 Catnip ($7/mo)",
            value=(
                "• 25 parallel workers\n"
                "• All Coffee features\n"
                "• **High priority queue**\n"
                "• Custom cover upload\n"
                "• Series pack downloads\n"
                "• 5 batch downloads"
            ),
            inline=True
        )
        
        embed.add_field(
            name="⭐ Sponsor ($15/mo)",
            value=(
                "• 100 parallel workers\n"
                "• All Catnip features\n"
                "• **Highest priority**\n"
                "• Custom CSS styles\n"
                "• Unlimited batch\n"
                "• Early access\n"
                "• Direct support"
            ),
            inline=True
        )
        
        embed.set_footer(text=f"Subscribe at: {PATREON_LINK}")
        
        await message.channel.send(embed=embed)

    async def _handle_check_command(self, message: discord.Message):
        """Handle !check command to check for novel updates"""
        user_id = str(message.author.id)
        
        # Parse: !check [title] or just !check for last novel
        content = message.content.strip()
        if content.lower().startswith('!check '):
            query = content[7:].strip()
            # Search in library
            novel = history_manager.find_novel(user_id, query)
            if not novel:
                await message.channel.send(f"No novel matching '{query}' in your library.")
                return
        else:
            # Check last novel
            novel = get_last_download(user_id)
            if not novel:
                await message.channel.send("No previous download found. Use `!check [title]` to check a specific novel.")
                return
        
        title = novel.get('title', 'Unknown')
        novel_url = novel.get('novel_url', '')
        last_ch = novel.get('chapter_end', 0)
        
        if not novel_url:
            await message.channel.send(f"Can't check **{title}** - no URL saved.")
            return
        
        await message.channel.send(f"🔍 Checking **{title}** for updates...")
        
        try:
            # Get current chapter count
            current_count = await self.loop.run_in_executor(
                None, self.scraper.get_chapter_count, novel_url
            )
            
            if current_count and current_count > last_ch:
                new_chapters = current_count - last_ch
                await message.channel.send(
                    f"✅ **{title}** has updates!\n\n"
                    f"Your last download: Chapter {last_ch}\n"
                    f"Latest available: Chapter {current_count}\n"
                    f"**{new_chapters} new chapters!**\n\n"
                    f"Use `!continue` to download new chapters."
                )
            else:
                await message.channel.send(
                    f"📖 **{title}** is up to date.\n"
                    f"Your last download: Chapter {last_ch}\n"
                    f"Latest available: Chapter {current_count or 'Unknown'}"
                )
        except Exception as e:
            logger.error(f"Error checking for updates: {e}")
            await message.channel.send(f"❌ Failed to check for updates: {e}")

    async def on_raw_reaction_add(self,
                                  payload: discord.RawReactionActionEvent):
        """Handle user reactions for verification and private channels"""
        try:
            # Get the guild, user, and channel
            guild = self.get_guild(payload.guild_id)
            if not guild:
                logger.debug("Guild not found")
                return

            user = await self.fetch_user(payload.user_id)
            if not user or user.bot:
                return

            channel = guild.get_channel(payload.channel_id)
            if not channel:
                logger.debug("Channel not found")
                return

            # Check if this is a verification reaction
            if payload.channel_id == VERIFICATION_CHANNEL_ID and payload.message_id == VERIFICATION_MESSAGE_ID:
                try:
                    member = await guild.fetch_member(payload.user_id)
                    verified_role = None
                    for role in guild.roles:
                        if role.name.lower() == VERIFIED_ROLE_NAME.lower():
                            verified_role = role
                            break

                    if verified_role and verified_role not in member.roles:
                        await member.add_roles(verified_role)
                        logger.info(
                            f"User {user.name} (ID: {user.id}) verified via reaction"
                        )
                        await self.log_to_discord(
                            "✅ User Verified",
                            f"{user.name} (ID: {user.id}) reacted to verification message and received Verified role",
                            discord.Color.green())
                    return
                except Exception as e:
                    logger.error(
                        f"Error processing verification reaction: {e}")
                    return

            # Check if reaction is in the correct category for private chats
            if channel.category_id != PRIVATE_CHAT_CATEGORY_ID:
                logger.debug(
                    f"Reaction in wrong category. Expected {PRIVATE_CHAT_CATEGORY_ID}, got {channel.category_id}"
                )
                return

            # IMPORTANT: Only create new channel if reacting in the Cat Café channel, NOT in existing chat channels
            # Skip if this is already a temporary private channel (prevents duplicate creation)
            if payload.channel_id in self.temporary_channels:
                logger.debug(
                    f"Reaction in existing temporary channel, skipping channel creation"
                )
                return

            # Only allow channel creation from Cat Café trigger channel
            if payload.channel_id != CAT_CAFE_CHANNEL_ID:
                logger.debug(
                    f"Reaction not in Cat Café channel, skipping. Channel ID: {payload.channel_id}"
                )
                return

            # Get the message
            try:
                message = await channel.fetch_message(payload.message_id)
            except Exception as e:
                logger.debug(f"Could not fetch message: {e}")
                return

            logger.info(
                f"User {user.name} reacted with {payload.emoji} to message in {channel.name}"
            )

            # Create a private text channel for this user
            # Simple overwrites: deny everyone by default, allow user and bot
            channel_name = f"chat-{user.name.lower()[:20]}"

            # Check if channel already exists (prevent duplicates from multiple bot instances)
            existing_channel = discord.utils.get(guild.text_channels,
                                                 name=channel_name)
            if existing_channel:
                logger.info(
                    f"Channel '{channel_name}' already exists, skipping creation"
                )
                return

            overwrites = {
                guild.default_role:
                discord.PermissionOverwrite(read_messages=False,
                                            send_messages=False),
                user:
                discord.PermissionOverwrite(read_messages=True,
                                            send_messages=True),
            }

            # Ensure bot has access
            bot_member = guild.me
            if bot_member:
                overwrites[bot_member] = discord.PermissionOverwrite(
                    read_messages=True, send_messages=True)

            # Add admin role access if it exists
            admin_role = None
            for role in guild.roles:
                if role.name.lower() == ADMIN_ROLE_NAME.lower():
                    admin_role = role
                    overwrites[admin_role] = discord.PermissionOverwrite(
                        read_messages=True, send_messages=True)
                    break

            logger.info(
                f"Creating channel '{channel_name}' with overwrites for user {user.name}"
            )

            # Get the category for the private channels
            category = guild.get_channel(PRIVATE_CHAT_CATEGORY_ID)

            temp_channel = await guild.create_text_channel(
                channel_name,
                category=category,
                overwrites=overwrites,
                reason=f"Temporary private chat for {user.name}")

            # Track the temporary channel
            self.temporary_channels[temp_channel.id] = user.id

            # Send welcome message (don't mention admin to avoid pinging)
            embed = discord.Embed(
                title="🐱 Private Chat Created",
                description=
                f"Welcome {user.mention}! This is your private chat channel.",
                color=discord.Color.blurple())
            embed.add_field(
                name="How to Download",
                value=("**Option 1 - Send a URL:**\n"
                       "`https://novelbin.com/b/solo-leveling`\n"
                       "`https://asuracomic.net/series/nano-machine`\n\n"
                       "**Option 2 - Send a title:**\n"
                       "`Solo Leveling`\n"
                       "`Nano Machine`"),
                inline=False)
            embed.add_field(name="Commands",
                            value=("`!stop` - Stop bot from responding\n"
                                   "`!start` - Resume bot responses\n"
                                   "`!close` - Delete this channel"),
                            inline=False)
            embed.set_footer(text="Need help? Contact @admin")
            await temp_channel.send(embed=embed)

            await self.log_to_discord(
                "🐱 Private Channel Created",
                f"User {user.name} (ID: {user.id}) created private channel: {temp_channel.name}",
                discord.Color.green())
            logger.info(
                f"Created temporary channel: {temp_channel.name} (ID: {temp_channel.id}) for user {user.name}"
            )

        except Exception as e:
            logger.error(f"Error handling reaction: {type(e).__name__}: {e}",
                         exc_info=True)

    def _detect_input_type(self, user_input: str) -> str:
        """Detect if input is a URL or title, and if URL is novel or manga"""
        url = user_input.strip()
        if re.match(r'^https?://', url):
            # Check if it's a manga URL
            return 'url'  # Novel URL
        return 'title'

    def _parse_chapter_range(self,
                             user_input: str) -> Tuple[int, Optional[int]]:
        """Parse chapter range input like '1-50', '1 to 500', '1 500', '50', 'all'
        Ignores trailing symbols like '/', spaces, etc.

        Returns: (start_chapter, end_chapter) or (1, None) if 'all'
        """
        user_input = user_input.strip().lower()
        logger.info(f"Parsing chapter range input: '{user_input}'")

        # Handle 'all' keyword
        if user_input == 'all' or user_input == 'all chapters':
            logger.info("Parsed as: ALL chapters")
            return (1, None)

        # Handle "X to Y" format: "1 to 500"
        if ' to ' in user_input:
            try:
                parts = user_input.split(' to ')
                parts = [p.strip() for p in parts if p.strip()]

                if len(parts) == 2:
                    start = int(re.sub(r'[^\d]', '', parts[0]))  # Extract only digits
                    end = int(re.sub(r'[^\d]', '', parts[1]))    # Extract only digits
                    logger.info(f"Parsed as: chapters {start} to {end}")
                    return (start, end)
            except (ValueError, AttributeError) as e:
                logger.warning(
                    f"Failed to parse 'to' format '{user_input}': {e}")
                return (1, None)

        # Handle space-separated format: "1 500"
        if ' ' in user_input and '-' not in user_input:
            try:
                parts = user_input.split()
                parts = [p.strip() for p in parts if p.strip()]

                if len(parts) == 2:
                    start = int(re.sub(r'[^\d]', '', parts[0]))  # Extract only digits
                    end = int(re.sub(r'[^\d]', '', parts[1]))    # Extract only digits
                    logger.info(f"Parsed as: chapters {start} {end}")
                    return (start, end)
            except (ValueError, AttributeError) as e:
                logger.warning(
                    f"Failed to parse space format '{user_input}': {e}")
                pass

        # Handle range format: "1-50" or "1-500" (support various dash types)
        if '-' in user_input or '–' in user_input or '—' in user_input:
            try:
                # Replace fancy dashes with regular dash
                normalized = user_input.replace('–', '-').replace('—', '-')
                parts = normalized.split('-')

                # Filter out empty parts and extract only digits
                parts = [re.sub(r'[^\d]', '', p.strip()) for p in parts if p.strip()]
                parts = [p for p in parts if p]  # Remove empty strings after digit extraction

                if len(parts) == 2:
                    start = int(parts[0])
                    end = int(parts[1])
                    logger.info(f"Parsed as: chapters {start}-{end}")
                    return (start, end)
            except (ValueError, AttributeError) as e:
                logger.warning(f"Failed to parse range '{user_input}': {e}")
                return (1, None)

        # Handle single number: "50" or "500" (extract only digits, ignore symbols)
        try:
            digits_only = re.sub(r'[^\d]', '', user_input)
            if digits_only:
                num = int(digits_only)
                logger.info(f"Parsed as: chapters 1-{num}")
                return (1, num)
        except (ValueError, AttributeError):
            pass

        logger.warning(f"Could not parse chapter range: '{user_input}'")
        return (1, None)

    async def _search_all_sites(
            self, query: str,
            message: discord.Message) -> Dict[str, Optional[str]]:
        """Search all supported sites for a novel by title"""
        try:
            await message.channel.send(
                f"🔍 Searching for '{query}' on multiple sites...")
            results = self.scraper.search_all_sites(query)
            return results
        except Exception as e:
            logger.error(f"Search error: {e}")
            await message.channel.send(f"❌ Search failed: {str(e)}")
            return {}

    async def _search_with_choices(self, query: str,
                                   message: discord.Message) -> list:
        """Search all sites including Google and return numbered choices"""
        try:
            raw_results = await self.loop.run_in_executor(
                None, self.scraper.search_all_sites_with_choices, query)

            if not raw_results:
                return []

            # Group flat site results into novels with aggregated sources
            grouped: Dict[str, Dict[str, object]] = {}
            for r in raw_results:
                title = (r.get('title') or 'Unknown').strip()
                url = r.get('url') or ''
                source_name = r.get('source') or 'Unknown'

                if not url:
                    continue

                # Determine the real site from the URL domain. DuckDuckGo is
                # used only as a search engine – we show the underlying
                # website (NovelBin, Ranobes, etc.), not "DuckDuckGo".
                domain = urlparse(url).netloc.lower()

                def map_domain_to_site_name(domain_str: str, fallback: str) -> str:
                    domain_str = domain_str or ''
                    mapping = [
                        ('novelbin', 'NovelBin'),
                        ('ranobes', 'Ranobes'),
                        ('royalroad', 'RoyalRoad'),
                        ('novelfire', 'NovelFire'),
                        ('freewebnovel', 'FreeWebNovel'),
                        ('creativenovels', 'CreativeNovels'),
                        ('boxnovel', 'BoxNovel'),
                        ('lightnovelworld', 'LightNovelWorld'),
                        ('lnmtl', 'LNMTL'),
                        ('readernovel', 'ReaderNovel'),
                        ('novelbuddy', 'NovelBuddy'),
                        ('lightnovelcave', 'LightNovelCave'),
                        ('libread', 'LibRead'),
                        ('wtr-lab', 'WTR-Lab'),
                        ('fullnovels', 'FullNovels'),
                        ('nicenovel', 'NiceNovel'),
                        ('bednovel', 'BedNovel'),
                        ('allnovelbook', 'AllNovelBook'),
                        ('yonglibrary', 'YongLibrary'),
                        ('englishnovelsfree', 'EnglishNovelsFree'),
                        ('readnovelfull', 'ReadNovelFull'),
                        ('novellive', 'NovelLive'),
                    ]
                    for needle, label in mapping:
                        if needle in domain_str:
                            return label
                    return fallback

                # If result came from DuckDuckGo, remap to the actual site
                if source_name == 'DuckDuckGo':
                    source_name = map_domain_to_site_name(domain, source_name)

                # Skip sites we know we cannot scrape (paid/protected)
                if self.scraper._is_paid_site(url) or is_protected_site(url):
                    continue

                # Use normalized title so the same novel from different
                # sites (NovelBin, ReadNovelFull, etc.) is grouped into one
                # entry with multiple sources.
                norm_title = self._normalize_novel_title(title, url)
                key = norm_title.lower()
                if key not in grouped:
                    grouped[key] = {
                        'title': norm_title,
                        'sources': []  # list of {source, url}
                    }

                sources_list = grouped[key]['sources']  # type: ignore[assignment]
                # Avoid duplicate URLs for the same novel
                if not any(s.get('url') == url for s in sources_list):
                    sources_list.append({'source': source_name, 'url': url})

            return list(grouped.values())
        except Exception as e:
            logger.error(f"Search error: {e}")
            # Don't show error to user - just return empty results
            return []

    async def _scrape_with_progress(
            self,
            url: str,
            message: discord.Message,
            user_id: int,
            chapter_start: int = 1,
            chapter_end: Optional[int] = None,
            user_tier: str = 'normal') -> Optional[Dict]:
        """Scrape novel with optional chapter range and live progress updates"""
        # Extract source domain for tracking
        source_domain = url.split('/')[2] if '/' in url else 'unknown'

        try:
            # Set parallel workers based on user tier
            if user_tier == 'sponsor':
                self.scraper.parallel_workers = WORKERS_SPONSOR  # 100
            elif user_tier == 'catnip':
                self.scraper.parallel_workers = WORKERS_CATNIP  # 25
            elif user_tier == 'coffee':
                self.scraper.parallel_workers = WORKERS_COFFEE  # 15
            else:
                self.scraper.parallel_workers = WORKERS_NORMAL  # 5

            chapter_range_str = f"{chapter_start}-{chapter_end}" if chapter_end else f"{chapter_start}-ALL"
            status_msg = await message.channel.send(f"⏳ Collecting links...")

            # Progress tracking (shared with state)
            state = self.user_states.get(user_id, {})
            progress_data = state.get('progress_data', {
                'current': 0,
                'total': 0,
                'phase': 'collecting'
            })

            # Store message for updates
            state['status_msg'] = status_msg
            update_complete = False

            def report_progress(current_or_msg, total=None):
                """Track scraping progress and update state
                Can be called as:
                - report_progress(current, total) for numeric progress
                - report_progress("message") for retry status messages
                """
                if total is None and isinstance(current_or_msg, str):
                    # String message (retry progress)
                    progress_data['retry_message'] = current_or_msg
                    progress_data['phase'] = 'retrying'
                else:
                    # Numeric progress
                    progress_data['current'] = current_or_msg
                    progress_data['total'] = total
                    progress_data['phase'] = 'scraping'
                    progress_data['retry_message'] = None
                # Keep state updated for on_message handler
                if user_id in self.user_states:
                    self.user_states[user_id]['progress_data'] = progress_data

            def report_link_progress(current, total, phase):
                """Track link collection progress"""
                progress_data['current'] = current
                progress_data['total'] = total
                progress_data['phase'] = phase
                if user_id in self.user_states:
                    self.user_states[user_id]['progress_data'] = progress_data

            # Set callbacks
            self.scraper.progress_callback = report_progress
            self.scraper.link_progress_callback = report_link_progress

            # Set cancellation check
            def check_cancel():
                """Check if user requested cancellation"""
                return self.user_states.get(user_id,
                                            {}).get('cancelled', False)

            self.scraper.cancel_check = check_cancel

            # Update message periodically (live progress)
            async def update_progress_msg():
                """Update Discord message with current progress"""
                last_updated = ""
                while not update_complete and user_id in self.user_states:
                    try:
                        current = progress_data.get('current', 0)
                        total = progress_data.get('total', 0)
                        phase = progress_data.get('phase', 'collecting')
                        retry_msg = progress_data.get('retry_message')

                        if phase == 'collecting':
                            msg_text = f"⏳ Collecting links: {current}/{total}" if total > 0 else "⏳ Collecting links..."
                        elif phase == 'retrying' and retry_msg:
                            msg_text = f"🔄 {retry_msg}"
                        else:
                            msg_text = f"⏳ Scraping: {current}/{total}"

                        # Only update if message changed
                        if msg_text != last_updated and status_msg:
                            await status_msg.edit(content=msg_text)
                            last_updated = msg_text
                    except discord.errors.NotFound:
                        break
                    except Exception as e:
                        logger.error(f"Failed to update progress: {e}")

                    await asyncio.sleep(0.5)  # Update every 0.5 seconds

            # Start progress update task
            update_task = asyncio.create_task(update_progress_msg())

            # Scrape in executor (non-blocking)
            novel_data = await self.loop.run_in_executor(
                None, self.scraper.scrape, url, chapter_start, chapter_end)

            # Mark update task as complete
            update_complete = True
            await update_task

            # Check if cancelled (message already sent in cancel handler)
            if self.user_states.get(user_id, {}).get('cancelled'):
                return None

            if not novel_data or not novel_data.get('chapters'):
                await message.channel.send(
                    "❌ No chapters were successfully scraped.")
                # Track failed content request
                title = novel_data.get('title',
                                       'Unknown') if novel_data else 'Unknown'
                await track_content_request(title,
                                            'novel',
                                            source_domain,
                                            success=False,
                                            loop=self.loop)
                await log_site_failure(source_domain,
                                       'no_chapters',
                                       'No chapters scraped',
                                       novel_url=url,
                                       loop=self.loop)
                return None

            chapter_count = len(novel_data['chapters'])
            await status_msg.edit(
                content=f"✅ Scraping complete: {chapter_count} chapters")

            # Track successful content request
            title = novel_data.get('title', 'Unknown')
            await track_content_request(title,
                                        'novel',
                                        source_domain,
                                        success=True,
                                        loop=self.loop)

            return novel_data

        except Exception as e:
            logger.error(f"Scraping error: {e}")
            await message.channel.send(f"❌ Scraping failed: {str(e)}")
            # Log failure
            await log_site_failure(source_domain,
                                   'scrape_error',
                                   str(e),
                                   novel_url=url,
                                   loop=self.loop)
            return None

    async def on_message(self, message: discord.Message):
        # Ignore bot's own messages
        if message.author.id == self.user.id:
            return

        # Deduplicate messages (prevent processing same message twice)
        if not hasattr(self, '_processed_messages'):
            self._processed_messages = set()

        if message.id in self._processed_messages:
            return  # Already processed
        self._processed_messages.add(message.id)

        # Cleanup old IDs (keep last 500)
        if len(self._processed_messages) > 1000:
            self._processed_messages = set(
                sorted(self._processed_messages)[-500:])

        user_id = message.author.id

        # Check for stop command (disable bot in this private channel)
        if message.content.strip().lower() == 'stop' or message.content.strip(
        ).lower() == '!stop':
            if message.channel.id in self.temporary_channels:
                self.bot_disabled_channels.add(message.channel.id)
                logger.info(
                    f"Bot disabled in channel {message.channel.name} (ID: {message.channel.id}) by {message.author.name}"
                )
                await message.channel.send(
                    "🤐 I'll stop responding now. Type `!start` if you want to chat again."
                )
            return

        # Check for start command (enable bot in this private channel)
        if message.content.strip().lower() == 'start' or message.content.strip(
        ).lower() == '!start':
            if message.channel.id in self.temporary_channels:
                self.bot_disabled_channels.discard(message.channel.id)
                logger.info(
                    f"Bot enabled in channel {message.channel.name} (ID: {message.channel.id}) by {message.author.name}"
                )
                await message.channel.send("👋 I'm back! Ready to chat.")
                return
            # If not a temporary channel, treat "start" as a regular message to trigger welcome

        # Check for close command (delete temporary private channel)
        if message.content.strip().lower() == 'close' or message.content.strip(
        ).lower() == '!close':
            # Check if this is a temporary channel
            if message.channel.id in self.temporary_channels:
                user_id = self.temporary_channels[message.channel.id]
                # Verify the user who issued the command owns this channel
                if message.author.id == user_id:
                    logger.info(
                        f"Closing temporary channel: {message.channel.name} (ID: {message.channel.id}) for user {message.author.name}"
                    )
                    try:
                        await message.channel.send("Closing channel...")
                        await asyncio.sleep(0.5)
                        await self.log_to_discord(
                            "🗑️ Private Channel Closed",
                            f"User {message.author.name} (ID: {message.author.id}) closed channel: {message.channel.name}",
                            discord.Color.red())
                        await message.channel.delete(
                            reason=f"Closed by user {message.author.name}")
                        del self.temporary_channels[message.channel.id]
                    except Exception as e:
                        logger.error(f"Error deleting channel: {e}")
                        await message.channel.send(
                            f"Error closing channel: {e}")
                else:
                    await message.channel.send(
                        "❌ Only the channel owner can close this channel.")
                    await self.log_to_discord(
                        "⚠️ Unauthorized Channel Closure Attempt",
                        f"User {message.author.name} (ID: {message.author.id}) tried to close channel they don't own: {message.channel.name}",
                        discord.Color.orange())
            return

        # Check for cancel command at any time
        if message.content.strip().lower() == 'cancel':
            if user_id in self.user_states:
                state = self.user_states[user_id]
                # If currently scraping, set cancelled flag and let scraper finish
                if state.get('step') == 'scraping_in_progress':
                    state['cancelled'] = True
                    await message.channel.send(
                        "⏹️ Cancelling... please wait for scraping to stop.")
                else:
                    # For all other steps, reset state completely
                    del self.user_states[user_id]
                    await message.channel.send(
                        "❌ Cancelled. Type `start` to begin again.")
                return
            return

        # If bot is disabled in this channel, don't process any further commands
        if message.channel.id in self.bot_disabled_channels:
            logger.debug(
                f"Bot disabled in channel {message.channel.id}, ignoring message"
            )
            return

        # If user is currently scraping, ignore any input except cancel (already handled above)
        if user_id in self.user_states:
            state = self.user_states[user_id]
            if state.get('step') == 'scraping_in_progress':
                # Show current progress instead
                progress_data = state.get('progress_data', {})
                current = progress_data.get('current', 0)
                total = progress_data.get('total', '?')
                await message.channel.send(
                    f"⏳ Currently scraping: {current}/{total}\nType `cancel` to stop."
                )
                return

        # Handle suggestion commands
        content_lower = message.content.lower().strip()

        if content_lower.startswith('!suggest ') or content_lower.startswith(
                'suggest '):
            await self._handle_suggest_command(message)
            return

        if content_lower.startswith('!vote ') or content_lower.startswith(
                'vote '):
            await self._handle_vote_command(message)
            return

        if content_lower in ['!suggestions', 'suggestions', '!sites', 'sites']:
            await self._handle_list_suggestions(message)
            return

        # New user settings and history commands
        if content_lower.startswith('!settings'):
            await self._handle_settings_command(message)
            return
        
        if content_lower in ['!help', 'help']:
            await self._handle_help_command(message)
            return
        
        if content_lower.startswith('!history'):
            await self._handle_history_command(message)
            return
        
        if content_lower.startswith('!library'):
            await self._handle_library_command(message)
            return
        
        if content_lower in ['!continue', 'continue']:
            await self._handle_continue_command(message)
            return
        
        if content_lower in ['!stats', 'stats']:
            await self._handle_stats_command(message)
            return
        
        if content_lower in ['!tiers', 'tiers']:
            await self._handle_tiers_command(message)
            return
        
        if content_lower.startswith('!check'):
            await self._handle_check_command(message)
            return

        # Post Cat Café welcome message
        if content_lower in ['!create', 'create']:
            await self._post_cat_cafe_message(message)
            return

        state = self.user_states.get(message.author.id)

        # Check if we're in allowed location for scraping (DM or private chat category)
        is_dm = isinstance(message.channel, discord.DMChannel)
        is_private_category = (hasattr(message.channel, 'category_id')
                               and message.channel.category_id
                               == PRIVATE_CHAT_CATEGORY_ID)

        # Debug logging for channel detection
        if hasattr(message.channel, 'category_id'):
            logger.debug(
                f"Channel category_id: {message.channel.category_id}, expected: {PRIVATE_CHAT_CATEGORY_ID}, match: {is_private_category}"
            )

        # Check if user has an active session in a different channel
        if state:
            session_channel_id = state.get('channel_id')
            if session_channel_id and session_channel_id != message.channel.id:
                # User has active session in another channel
                session_channel_name = state.get('channel_name',
                                                 'another location')
                step = state.get('step', 'unknown')

                # Show helpful message about active session
                if step == 'scraping_in_progress':
                    await message.channel.send(
                        f"You have an active download in progress in **{session_channel_name}**.\n"
                        f"Please wait for it to complete or type `cancel` there to stop it."
                    )
                else:
                    await message.channel.send(
                        f"You have an active session in **{session_channel_name}**.\n"
                        f"Please continue there or type `cancel` to start fresh here."
                    )
                return

        # Initialize state if not exists
        if not state:
            if not is_dm and not is_private_category:
                # Not in allowed location - ignore scraping commands
                # But still allow other bot features to work
                logger.debug(
                    f"Ignoring message from {message.author.name} - not in allowed location (DM: {is_dm}, category match: {is_private_category})"
                )
                return

            # Check user access first
            has_access, user_tier = await self._check_user_access(message)
            if not has_access:
                return

            # Get daily usage for display
            usage = await self._get_daily_usage(str(message.author.id))

            # Get channel name for display
            if is_dm:
                channel_name = "DM"
            else:
                channel_name = getattr(message.channel, 'name', 'this channel')

            self.user_states[message.author.id] = {
                'step': 'awaiting_welcome_ack',
                'data': {},
                'message': None,
                'cancelled': False,
                'user_tier':
                user_tier,  # 'normal', 'coffee', 'catnip', 'sponsor'
                'channel_id': message.channel.id,
                'channel_name': channel_name
            }

            # Build tier-specific limits display (currently unlimited for all users)
            tier_display = user_tier.title()
            limits_text = "**Unlimited downloads today**"

            await message.channel.send(
                "═══════════════════════════════════════\n"
                "☕ **Meowi's Tea and Coffee** ☕\n"
                "═══════════════════════════════════════\n\n"
                f"**Tier:** {tier_display} | {limits_text}\n\n"
                "**What would you like to do?**\n\n"
                "📖 **Option 1: Search by Title**\n"
                "  Type a title (e.g., `Global Lord`)\n\n"
                "🔗 **Option 2: Direct Link**\n"
                "  Paste a URL from any supported site\n\n"
                "📚 **Supported:** Novels + Manga\n"
                "⏹️  Type `cancel` anytime to stop")
            return

        step = state['step']

        if step == 'awaiting_welcome_ack':
            # User has seen the welcome message, now process their input
            user_input = message.content.strip()
            user_tier = state.get('user_tier', 'normal')

            if not user_input:
                await message.channel.send("Please provide a title or link.")
                return

            input_type = self._detect_input_type(user_input)
            state['step'] = 'waiting_for_input' # Transition to waiting_for_input immediately

            if input_type == 'url':
                logger.info(f"Novel URL received: {user_input}")

                # Check if WuxiaWorld/WebNovel URL - sponsor only
                url_lower = user_input.lower()
                is_sponsor_only_site = ('wuxiaworld.com' in url_lower
                                        or 'wuxiaworld.eu' in url_lower
                                        or 'wuxia.city' in url_lower or
                                        ('webnovel.com' in url_lower
                                         and 'freewebnovel' not in url_lower))
                if is_sponsor_only_site and user_tier != 'sponsor':
                    await message.channel.send(
                        "This site requires **Sponsor** tier due to heavy protection.\n"
                        "Upgrade at: https://www.patreon.com/c/meowisteaandcoffee/membership\n\n"
                        "Or try a different source like:\n"
                        "NovelBin, RoyalRoad, FreeWebNovel, LightNovelCave, etc."
                    )
                    return

                state['data']['url'] = user_input
                state['data']['content_type'] = 'novel'
                state['step'] = 'waiting_for_format'

                # Fetch metadata to get chapter count and title
                metadata_result = await self.loop.run_in_executor(
                    None, self.scraper.get_novel_metadata, user_input)

                title = metadata_result.get('title', 'Unknown Novel')
                chapter_count = metadata_result.get('total_chapters', 0)

                if chapter_count:
                    state['data']['total_chapters'] = chapter_count
                    count_info = f"\n**Available:** All {chapter_count} chapters\n"
                else:
                    count_info = ""

                await message.channel.send(
                    f"✅ **Selected: {title}**{count_info}\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "**Select Your Preferred Format**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "**1️⃣  EPUB** - Universal format (recommended)\n"
                    "  Best for: E-readers, phones, tablets\n\n"
                    "**2️⃣  PDF** - Print-ready format\n"
                    "  Best for: Desktop, printing\n\n"
                    "Reply with `1` or `2`" + HINT_TEXT)

            else:  # input_type == 'title'
                # Direct novel search (manga disabled)
                logger.info(f"Title search: {user_input}")
                state['data']['search_query'] = user_input
                await message.channel.send(f"🔍 Searching for '{user_input}'...")
                
                results = await self._search_with_choices(user_input, message)
                if results:
                    if len(results) == 1:
                        novel = results[0]
                        state['data']['selected_novel'] = novel
                        state['step'] = 'waiting_for_source_choice'
                        
                        sources = novel.get('sources', [])
                        if not sources:
                            await message.channel.send(
                                f"No sources found for '{novel.get('title', 'Unknown')}'.")
                            del self.user_states[message.author.id]
                            return
                        sources_text = f"**{novel.get('title', 'Unknown')}**\n\nSources:\n"
                        for i, src in enumerate(sources, 1):
                            sources_text += f"**{i}.** {src.get('source', 'Unknown')}\n"
                        sources_text += f"\nReply 1-{len(sources)}" + HINT_TEXT
                        await message.channel.send(sources_text)
                    else:
                        state['data']['search_results'] = results
                        state['step'] = 'waiting_for_novel_choice'
                        
                        choices_text = "**Found novels:**\n\n"
                        for i, result in enumerate(results, 1):
                            sources_list = ", ".join([src['source'] for src in result.get('sources', [])])
                            choices_text += f"**{i}.** {result['title']} ({sources_list})\n"
                        choices_text += f"\nReply 1-{len(results)}" + HINT_TEXT
                        await message.channel.send(choices_text)
                else:
                    await message.channel.send("Not found. Try a different title or URL.")
                    del self.user_states[message.author.id]
            return

        if step == 'waiting_for_novel_choice':
            # User is picking a novel from search results
            user_choice = message.content.strip().lower()
            search_results = state['data'].get('search_results', [])

            # Check for back command - go back to title input
            if user_choice == 'back':
                state['step'] = 'awaiting_welcome_ack'
                await message.channel.send("↩️ Going back. Please enter a title or URL:")
                return

            try:
                choice_num = int(user_choice) - 1
                if 0 <= choice_num < len(search_results):
                    selected_novel = search_results[choice_num]

                    # Always show sources
                    state['data']['selected_novel'] = selected_novel
                    state['step'] = 'waiting_for_source_choice'

                    sources_text = f"✅ **Selected: {selected_novel['title']}**\n\nAvailable sources:\n"
                    for i, src in enumerate(selected_novel['sources'], 1):
                        sources_text += f"**{i}.** {src['source']}\n   {src['url']}\n"
                    sources_text += f"\nReply with a number (1-{len(selected_novel['sources'])})" + HINT_TEXT
                    await message.channel.send(sources_text)
                else:
                    await message.channel.send(
                        f"❌ Please reply with a number between 1 and {len(search_results)}."
                    )
            except ValueError:
                await message.channel.send(
                    f"❌ Please reply with a number between 1 and {len(search_results)}."
                )

        elif step == 'waiting_for_source_choice':
            # User is picking a source for the selected novel
            user_choice = message.content.strip().lower()
            selected_novel = state['data'].get('selected_novel', {})
            sources = selected_novel.get('sources', [])

            # Check for back command
            if user_choice == 'back':
                search_results = state['data'].get('search_results', [])
                if search_results:
                    state['step'] = 'waiting_for_novel_choice'
                    choices_text = "↩️ Going back.\n\n✅ **Found multiple novels!** Pick one:\n\n"
                    # Show up to 10 titles with source count
                    for i, result in enumerate(search_results[:10], 1):
                        source_count = len(result.get('sources', []))
                        sources_list = ", ".join([src['source'] for src in result.get('sources', [])])
                        choices_text += f"**{i}.** {result['title']}\n   ({source_count} {'site' if source_count == 1 else 'sites'}: {sources_list})\n\n"
                    choices_text += f"Reply with a number (1-{min(10, len(search_results))})" + HINT_TEXT
                    await message.channel.send(choices_text)
                else:
                    state['step'] = 'awaiting_title'
                    await message.channel.send(
                        "↩️ Going back. Please enter a novel title or URL:")
                return

            try:
                choice_num = int(user_choice) - 1
                if 0 <= choice_num < len(sources):
                    selected_source = sources[choice_num]

                    # Check if WuxiaWorld/WebNovel - sponsor only
                    source_url = selected_source.get('url', '').lower()
                    user_tier = state.get('user_tier', 'normal')
                    is_sponsor_only_site = ('wuxiaworld.com' in source_url
                                            or 'wuxiaworld.eu' in source_url
                                            or 'wuxia.city' in source_url or
                                            ('webnovel.com' in source_url and
                                             'freewebnovel' not in source_url))
                    if is_sponsor_only_site and user_tier != 'sponsor':
                        await message.channel.send(
                            "This site requires **Sponsor** tier. Please select a different source."
                        )
                        return

                    state['data']['url'] = selected_source['url']
                    state['step'] = 'waiting_for_format'

                    # Get chapter count for selected source and save to state
                    chapter_count = await self.loop.run_in_executor(
                        None, self.scraper.get_chapter_count,
                        selected_source['url'])
                    count_info = f"\n**Chapters available:** {chapter_count}\n" if chapter_count else ""
                    if chapter_count:
                        state['data'][
                            'total_chapters'] = chapter_count  # Save for later use

                    await message.channel.send(
                        f"✅ **Selected: {selected_source['source']}**{count_info}\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "**Select Your Preferred Format**\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "**1️⃣  EPUB** - Universal format (recommended)\n"
                        "  Best for: E-readers, phones, tablets\n\n"
                        "**2️⃣  PDF** - Print-ready format\n"
                        "  Best for: Desktop, printing\n\n"
                        "Reply with `1` or `2`")
                else:
                    await message.channel.send(
                        f"❌ Please reply with a number between 1 and {len(sources)}."
                    )
            except ValueError:
                await message.channel.send(
                    f"❌ Please reply with a number between 1 and {len(sources)}."
                )

        elif step == 'waiting_for_website_choice':
            # Legacy handler - kept for backward compatibility
            user_choice = message.content.strip()
            search_results = state['data'].get('search_results', [])

            try:
                choice_num = int(user_choice) - 1
                if 0 <= choice_num < len(search_results):
                    selected = search_results[choice_num]
                    state['data']['url'] = selected['url']
                    state['step'] = 'waiting_for_format'
                    await message.channel.send(
                        f"✅ **Selected: {selected['source']}**\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "**Select Your Preferred Format**\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "**1️⃣  EPUB** - Universal format (recommended)\n"
                        "  Best for: E-readers, phones, tablets\n\n"
                        "**2️⃣  PDF** - Print-ready format\n"
                        "  Best for: Desktop, printing\n\n"
                        "Reply with `1` or `2`")
                else:
                    await message.channel.send(
                        f"❌ Please reply with a number between 1 and {len(search_results)}."
                    )
            except ValueError:
                await message.channel.send(
                    f"❌ Please reply with a number between 1 and {len(search_results)}."
                )

        elif step == 'waiting_for_format':
            user_input = message.content.strip().lower()

            # Handle back/cancel
            if user_input == 'back':
                state['step'] = 'awaiting_welcome_ack'
                await message.channel.send(
                    "↩️ Going back. Please enter a novel title or URL:")
                return
            if user_input == 'cancel':
                del self.user_states[message.author.id]
                await message.channel.send(
                    "Cancelled. Type anything to start again.")
                return

            # Accept 1 for EPUB, 2 for PDF, or the full word
            format_map = {
                '1': 'epub',
                'epub': 'epub',
                '2': 'pdf',
                'pdf': 'pdf'
            }

            fmt = format_map.get(user_input)
            if not fmt:
                await message.channel.send("❌ Invalid format!\n\n"
                                           "Please reply with:\n"
                                           "  `1` for EPUB\n"
                                           "  `2` for PDF")
                return

            state['data']['format'] = fmt
            state['step'] = 'waiting_for_chapter_range'

            # Use saved chapter count or fetch if not available
            url = state['data'].get('url', '')
            user_tier = state.get('user_tier', 'normal')

            # Check if we already have chapter count from source selection
            total_chapters = state['data'].get('total_chapters', 0)

            if total_chapters == 0:
                # Only fetch if not already known
                loading_msg = await message.channel.send(
                    "Checking chapter info...")
                try:
                    novel_metadata = await self.loop.run_in_executor(
                        None, self.scraper.get_novel_metadata, url)
                    total_chapters = novel_metadata.get('total_chapters', 0)
                    state['data']['novel_metadata'] = novel_metadata
                    state['data']['total_chapters'] = total_chapters
                except:
                    total_chapters = 0
                loading_msg_exists = True
            else:
                loading_msg = await message.channel.send("Loading...")
                loading_msg_exists = True

            # All users have unlimited downloads
            limit_text = "**Downloads:** Unlimited"
            if total_chapters > 0:
                remaining_text = f"\n**Available:** All {total_chapters} chapters"
            else:
                remaining_text = ""

            # === FUTURE LIMIT IMPLEMENTATION (commented out) ===
            # if user_tier in ('catnip', 'sponsor'):
            #     limit_text = "**Your Limit:** Unlimited"
            #     remaining_text = ""
            # else:
            #     novel_key = self._normalize_novel_key(url)
            #     novel_usage = await self._get_novel_usage(str(message.author.id), novel_key)
            #     used = novel_usage.get('chapters_used', 0)
            #     daily_usage = await self._get_daily_usage(str(message.author.id))
            #     bonus_used_today = daily_usage.get('novel_bonus_used', 0)
            #     limit_info = self._calculate_limit(user_tier, total_chapters, 'novel')
            #     percent = limit_info['percent']
            #     percent_limit = limit_info['percent_limit']
            #     daily_bonus = limit_info['daily_bonus']
            #     percent_remaining = max(0, percent_limit - used)
            #     bonus_remaining = max(0, daily_bonus - bonus_used_today)
            #     max_available = percent_limit + bonus_remaining
            #     total_remaining = percent_remaining + bonus_remaining
            #     if total_chapters > 0:
            #         limit_text = f"**Your Limit:** {percent}% of {total_chapters} chapters = {percent_limit} + {bonus_remaining}/{daily_bonus} bonus = **{max_available} max**"
            #         if used > 0:
            #             remaining_text = f"\n**Already Downloaded (this novel):** {used}\n**Bonus Used Today:** {bonus_used_today}/{daily_bonus}\n**Remaining:** {total_remaining} chapters"
            #         else:
            #             if bonus_used_today > 0:
            #                 remaining_text = f"\n**Bonus Used Today:** {bonus_used_today}/{daily_bonus}\n**You Can Download:** {max_available} chapters from this novel"
            #             else:
            #                 remaining_text = f"\n**You Can Download:** {max_available} chapters from this novel"
            #     else:
            #         limit_text = f"**Your Limit:** {percent}% of novel's chapters + {bonus_remaining}/{daily_bonus} bonus *(per novel)*"
            #         remaining_text = ""

            format_display = "📘 EPUB" if fmt == 'epub' else "📄 PDF"
            await loading_msg.edit(
                content=f"✅ **Format Selected: {format_display}**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "**How Many Chapters?**\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{limit_text}{remaining_text}\n\n"
                "**Examples:**\n"
                "  `1-50`   → Chapters 1 to 50\n"
                "  `1-500`  → Chapters 1 to 500\n"
                "  `50`     → First 50 chapters\n"
                "  `all`    → All chapters available\n\n"
                "Your choice:" + HINT_TEXT)

        elif step == 'waiting_for_chapter_range':
            chapter_input = message.content.strip().lower()
            user_tier = state.get('user_tier', 'normal')

            # Handle back/cancel
            if chapter_input == 'back':
                state['step'] = 'waiting_for_format'
                await message.channel.send("↩️ Going back.\n\n"
                                           "**Select Your Preferred Format**\n"
                                           "**1️⃣  EPUB** | **2️⃣  PDF**" +
                                           HINT_TEXT)
                return
            if chapter_input == 'cancel':
                del self.user_states[message.author.id]
                await message.channel.send(
                    "Cancelled. Type anything to start again.")
                return

            try:
                chapter_start, chapter_end = self._parse_chapter_range(
                    chapter_input)

                state['data']['chapter_start'] = chapter_start
                state['data']['chapter_end'] = chapter_end

                logger.info(
                    f"Chapter range: {chapter_start}-{chapter_end} (Tier: {user_tier})"
                )
            except:
                await message.channel.send(
                    "❌ Invalid chapter range. Try: `1-50`, `50`, or `all`")
                return

            url = state['data'].get('url')
            fmt = state['data'].get('format')

            if not url or not fmt:
                await message.channel.send(
                    "❌ Error: Data lost. Please start over with `start`.")
                del self.user_states[message.author.id]
                return

            # Use saved chapter count or fetch if not available
            total_chapters = state['data'].get('total_chapters', 0)
            novel_metadata = state['data'].get('novel_metadata', {})

            if total_chapters == 0:
                await message.channel.send("Checking novel info...")
                novel_metadata = await self.loop.run_in_executor(
                    None, self.scraper.get_novel_metadata, url)
                total_chapters = novel_metadata.get('total_chapters', 500)
                state['data']['novel_metadata'] = novel_metadata
                state['data']['total_chapters'] = total_chapters

            # Calculate requested chapters
            if chapter_end:
                requested_chapters = chapter_end - chapter_start + 1
            else:
                # "all" request
                requested_chapters = total_chapters
                chapter_end = total_chapters
                state['data']['chapter_end'] = chapter_end

            # Check download limits with real total (per-novel tracking)
            novel_title = novel_metadata.get('title', 'Unknown Novel')
            limit_check = await self._check_download_allowed(
                str(message.author.id), user_tier, total_chapters,
                requested_chapters, 'novel', url, novel_title)
            state['data']['novel_key'] = limit_check.get('novel_key', '')
            state['data']['novel_title'] = novel_title

            if not limit_check['allowed']:
                await message.channel.send(
                    f"**Daily Limit Reached**\n{limit_check['message']}")
                del self.user_states[message.author.id]
                return

            # Always store limit_check for bonus calculation later
            state['data']['limit_check'] = limit_check
            state['data']['total_chapters'] = total_chapters

            # If limit applies, ask for confirmation
            if limit_check['needs_confirm']:
                state['step'] = 'novel_confirm_limit'
                state['data']['original_chapter_end'] = chapter_end

                limited_end = chapter_start + limit_check['max_now'] - 1
                await message.channel.send(
                    f"**Download Limit Warning**\n\n"
                    f"{limit_check['message']}\n\n"
                    f"**Will download:** Chapters {chapter_start}-{limited_end}\n\n"
                    f"Type **yes** to continue or **cancel** to abort.")
                return

            chapter_range_display = f"{chapter_start}-{chapter_end}" if chapter_end else f"{chapter_start}-ALL"
            logger.info(
                f"Starting scrape: {url} -> {fmt.upper()} -> chapters {chapter_range_display}"
            )

            try:
                # Mark as scraping in progress
                state['step'] = 'scraping_in_progress'
                # Calculate actual chapter count for progress display
                actual_count = (chapter_end - chapter_start +
                                1) if chapter_end else 0
                state['progress_data'] = {'current': 0, 'total': actual_count}
                start_time = time.time()

                # Scrape with progress updates
                novel_data = await self._scrape_with_progress(
                    url, message, message.author.id, chapter_start,
                    chapter_end, user_tier)

                if not novel_data:
                    # Clean up state
                    if message.author.id in self.user_states:
                        del self.user_states[message.author.id]
                    return

                # Check for Cloudflare-blocked chapters
                cloudflare_chapters = novel_data.get('cloudflare_chapters', [])
                if cloudflare_chapters:
                    cf_count = len(cloudflare_chapters)
                    chapter_count_so_far = len(novel_data.get('chapters', []))

                    # Store data for retry
                    state['step'] = 'cloudflare_retry_prompt'
                    state['data']['novel_data'] = novel_data
                    state['data']['cloudflare_chapters'] = cloudflare_chapters
                    state['data']['start_time'] = start_time

                    cf_chapter_nums = [
                        str(num) for num, url in cloudflare_chapters[:10]
                    ]
                    cf_preview = ", ".join(cf_chapter_nums)
                    if cf_count > 10:
                        cf_preview += f" ... and {cf_count - 10} more"

                    await message.channel.send(
                        f"**Cloudflare Protection Detected**\n\n"
                        f"Downloaded: **{chapter_count_so_far}** chapters successfully\n"
                        f"Blocked: **{cf_count}** chapters (Cloudflare challenge)\n"
                        f"Chapters: {cf_preview}\n\n"
                        f"Would you like to **retry** downloading the {cf_count} blocked chapters?\n"
                        f"Type **yes** to retry or **no** to get the file with current chapters.\n\n"
                        f"*Hint: Type `back` to go back or `cancel` to abort*")
                    return

                # Generate file
                title = novel_data.get('title', 'Novel')
                chapter_count = len(novel_data.get('chapters', []))

                await message.channel.send(
                    f"📝 **Generating {fmt.upper()}...**\n"
                    f"Chapters: {chapter_count}")

                if fmt == 'epub':
                    user_id_str = str(message.author.id)
                    filename = await self.loop.run_in_executor(
                        None, lambda: create_epub(novel_data, user_id_str, user_tier))
                else:
                    user_id_str = str(message.author.id)
                    filename = await self.loop.run_in_executor(
                        None, lambda: create_pdf(novel_data, user_id_str, user_tier))

                # Send file with rich embed
                if os.path.exists(filename):
                    file_size_mb = os.path.getsize(filename) / (1024 * 1024)

                    # Check if file is too large for Discord (proactive upload)
                    if is_file_too_large_for_discord(filename):
                        progress_msg = await message.channel.send(
                            f"**Uploading to external host...**\n"
                            f"File size: {file_size_mb:.1f}MB\n"
                            f"Trying: Litterbox...")

                        # Track upload progress
                        upload_status = {"current": "Litterbox", "tried": []}

                        def update_upload_progress(service, status):
                            if status == "uploading":
                                upload_status["current"] = service
                            elif status == "failed":
                                upload_status["tried"].append(service)

                        async def update_progress_message():
                            tried = ", ".join(
                                upload_status["tried"]
                            ) if upload_status["tried"] else "None"
                            current = upload_status["current"]
                            await progress_msg.edit(
                                content=(f"**Uploading to external host...**\n"
                                         f"File size: {file_size_mb:.1f}MB\n"
                                         f"Trying: {current}...\n"
                                         f"Failed: {tried}"))

                        # Run upload with progress updates
                        import functools
                        upload_func = functools.partial(
                            upload_large_file, filename,
                            update_upload_progress)

                        # Start upload task
                        upload_task = self.loop.run_in_executor(
                            None, upload_func)

                        # Update message periodically while uploading
                        while not upload_task.done():
                            await update_progress_message()
                            await asyncio.sleep(3)

                        upload_url, service = await upload_task

                        if upload_url:
                            await progress_msg.edit(
                                content=f"Upload complete to {service}!")
                            # Get proper filename for user reference
                            proper_filename = os.path.basename(filename)

                            # Wrap URL with ShrinkMe ads for free users only (skip hosts with own redirects)
                            display_url = upload_url
                            has_ads = False
                            if user_tier == 'normal':
                                shortened = await self.loop.run_in_executor(
                                    None, lambda: shorten_with_shrinkme(
                                        upload_url, service))
                                if shortened != upload_url:
                                    display_url = shortened
                                    has_ads = True

                            embed = discord.Embed(title="Download Complete",
                                                  description=f"**{title}**",
                                                  color=discord.Color.green())
                            embed.add_field(name="Chapters",
                                            value=str(chapter_count),
                                            inline=True)
                            embed.add_field(name="Format",
                                            value=fmt.upper(),
                                            inline=True)
                            embed.add_field(name="Size",
                                            value=f"{file_size_mb:.1f}MB",
                                            inline=True)
                            embed.add_field(name="Save As",
                                            value=f"`{proper_filename}`",
                                            inline=False)
                            embed.add_field(name="Download Link",
                                            value=display_url,
                                            inline=False)

                            if has_ads:
                                embed.add_field(name="Ad Supported",
                                                value=AD_FREE_MESSAGE,
                                                inline=False)

                            embed.set_footer(
                                text=
                                f"Hosted on {service} | Rename file after downloading"
                            )
                            await message.channel.send(embed=embed)

                            # Log to database
                            duration = int(time.time() - start_time)
                            await log_download({
                                'discordUserId':
                                str(message.author.id),
                                'discordUsername':
                                str(message.author.name),
                                'novelTitle':
                                title,
                                'source':
                                url.split('/')[2] if '/' in url else 'unknown',
                                'chapterStart':
                                chapter_start,
                                'chapterEnd':
                                chapter_end or chapter_count,
                                'chapterCount':
                                chapter_count,
                                'format':
                                fmt,
                                'userTier':
                                user_tier,
                                'durationSeconds':
                                duration,
                                'status':
                                'completed'
                            })
                            
                            # Add to user download history
                            add_download(
                                str(message.author.id), title, url,
                                chapter_start, chapter_end or chapter_count, fmt
                            )
                            
                            # Track per-novel usage (use URL-based key to prevent bypass)
                            novel_key = state['data'].get(
                                'novel_key') or self._normalize_novel_key(url)
                            novel_title_stored = state['data'].get(
                                'novel_title', title)
                            total_ch = state['data'].get(
                                'total_chapters', chapter_count)
                            limit_info = state['data'].get(
                                'limit_check', {}).get('limit_info', {})
                            bonus_used = self._calculate_bonus_used(
                                chapter_count, limit_info)
                            await self._update_novel_usage(
                                str(message.author.id), novel_key,
                                novel_title_stored, total_ch, chapter_count,
                                'novel')
                            await self._update_daily_usage(
                                str(message.author.id),
                                novel_chapters=chapter_count,
                                novel_bonus_used=bonus_used)
                            os.remove(filename)
                        else:
                            await progress_msg.edit(
                                content=
                                f"Failed to upload to all hosts. File saved locally: `{filename}`"
                            )

                        # Reset state
                        if message.author.id in self.user_states:
                            del self.user_states[message.author.id]
                        return

                    try:
                        # For free users, always use external upload + ad links
                        if user_tier == 'normal':
                            progress_msg = await message.channel.send(
                                f"**Uploading...**\n"
                                f"File size: {file_size_mb:.1f}MB")

                            upload_status = {
                                "current": "Litterbox",
                                "tried": []
                            }

                            def update_upload_progress_small(service, status):
                                if status == "uploading":
                                    upload_status["current"] = service
                                elif status == "failed":
                                    upload_status["tried"].append(service)

                            import functools
                            upload_func = functools.partial(
                                upload_large_file, filename,
                                update_upload_progress_small)
                            upload_url, service = await self.loop.run_in_executor(
                                None, upload_func)

                            if upload_url:
                                await progress_msg.delete()
                                proper_filename = os.path.basename(filename)

                                # Wrap with ShrinkMe ads
                                shortened = await self.loop.run_in_executor(
                                    None, lambda: shorten_with_shrinkme(
                                        upload_url, service))
                                display_url = shortened
                                has_ads = shortened != upload_url

                                paywall_count = novel_data.get(
                                    'paywall_count', 0)
                                embed_color = discord.Color.orange(
                                ) if paywall_count > 0 else discord.Color.green(
                                )

                                embed = discord.Embed(
                                    title="Download Complete",
                                    description=f"**{title}**",
                                    color=embed_color)
                                embed.add_field(name="Chapters",
                                                value=str(chapter_count),
                                                inline=True)
                                embed.add_field(name="Format",
                                                value=fmt.upper(),
                                                inline=True)
                                embed.add_field(name="Size",
                                                value=f"{file_size_mb:.1f}MB",
                                                inline=True)
                                embed.add_field(name="Save As",
                                                value=f"`{proper_filename}`",
                                                inline=False)
                                embed.add_field(name="Download Link",
                                                value=display_url,
                                                inline=False)

                                if has_ads:
                                    embed.add_field(name="Ad Supported",
                                                    value=AD_FREE_MESSAGE,
                                                    inline=False)

                                if paywall_count > 0:
                                    embed.add_field(
                                        name="Paywall Warning",
                                        value=
                                        f"{paywall_count} chapter(s) were locked/premium and skipped",
                                        inline=False)

                                metadata = novel_data.get('metadata', {})
                                cover_url = metadata.get('cover_image', '')
                                if cover_url and not cover_url.endswith(
                                        'placeholder.jpg'):
                                    embed.set_thumbnail(url=cover_url)

                                embed.set_footer(
                                    text=
                                    f"Hosted on {service} | Rename file after downloading"
                                )
                                await message.channel.send(embed=embed)

                                # Log and cleanup
                                duration = int(time.time() - start_time)
                                await log_download({
                                    'discordUserId':
                                    str(message.author.id),
                                    'discordUsername':
                                    str(message.author.name),
                                    'novelTitle':
                                    title,
                                    'source':
                                    url.split('/')[2]
                                    if '/' in url else 'unknown',
                                    'chapterStart':
                                    chapter_start,
                                    'chapterEnd':
                                    chapter_end or chapter_count,
                                    'chapterCount':
                                    chapter_count,
                                    'format':
                                    fmt,
                                    'userTier':
                                    user_tier,
                                    'durationSeconds':
                                    duration,
                                    'status':
                                    'completed'
                                })
                                novel_key = state['data'].get(
                                    'novel_key') or self._normalize_novel_key(
                                        url)
                                novel_title_stored = state['data'].get(
                                    'novel_title', title)
                                total_ch = state['data'].get(
                                    'total_chapters', chapter_count)
                                limit_info = state['data'].get(
                                    'limit_check', {}).get('limit_info', {})
                                bonus_used = self._calculate_bonus_used(
                                    chapter_count, limit_info)
                                await self._update_novel_usage(
                                    str(message.author.id), novel_key,
                                    novel_title_stored, total_ch,
                                    chapter_count, 'novel')
                                await self._update_daily_usage(
                                    str(message.author.id),
                                    novel_chapters=chapter_count,
                                    novel_bonus_used=bonus_used)
                                os.remove(filename)
                                logger.info(
                                    f"File uploaded with ads for free user: {filename}"
                                )

                                if message.author.id in self.user_states:
                                    del self.user_states[message.author.id]
                                return
                            else:
                                await progress_msg.edit(
                                    content="Upload failed, sending directly..."
                                )
                                # Fall through to direct send below

                        # Create rich embed for completion (paid users or fallback)
                        paywall_count = novel_data.get('paywall_count', 0)
                        embed_color = discord.Color.orange(
                        ) if paywall_count > 0 else discord.Color.green()

                        embed = discord.Embed(title="Download Complete",
                                              description=f"**{title}**",
                                              color=embed_color)
                        embed.add_field(name="Chapters",
                                        value=str(chapter_count),
                                        inline=True)
                        embed.add_field(name="Format",
                                        value=fmt.upper(),
                                        inline=True)
                        embed.add_field(name="Tier",
                                        value=user_tier.upper(),
                                        inline=True)

                        # Show paywall warning if any chapters were locked
                        if paywall_count > 0:
                            embed.add_field(
                                name="Paywall Warning",
                                value=
                                f"{paywall_count} chapter(s) were locked/premium and skipped",
                                inline=False)

                        # Add cover image if available
                        metadata = novel_data.get('metadata', {})
                        cover_url = metadata.get('cover_image', '')
                        if cover_url and not cover_url.endswith(
                                'placeholder.jpg'):
                            embed.set_thumbnail(url=cover_url)

                        embed.set_footer(text="Novel Scraper Bot")

                        await message.channel.send(embed=embed,
                                                   file=discord.File(filename))
                        logger.info(f"File sent successfully: {filename}")

                        # Log to database
                        duration = int(time.time() - start_time)
                        await log_download({
                            'discordUserId':
                            str(message.author.id),
                            'discordUsername':
                            str(message.author.name),
                            'novelTitle':
                            title,
                            'source':
                            url.split('/')[2] if '/' in url else 'unknown',
                            'chapterStart':
                            chapter_start,
                            'chapterEnd':
                            chapter_end or chapter_count,
                            'chapterCount':
                            chapter_count,
                            'format':
                            fmt,
                            'userTier':
                            user_tier,
                            'durationSeconds':
                            duration,
                            'status':
                            'completed'
                        })
                        
                        # Add to user download history
                        add_download(
                            str(message.author.id), title, url,
                            chapter_start, chapter_end or chapter_count, fmt
                        )
                        
                        # Track per-novel usage (use URL-based key to prevent bypass)
                        novel_key = state['data'].get(
                            'novel_key') or self._normalize_novel_key(url)
                        novel_title_stored = state['data'].get(
                            'novel_title', title)
                        total_ch = state['data'].get('total_chapters',
                                                     chapter_count)
                        limit_info = state['data'].get('limit_check', {}).get(
                            'limit_info', {})
                        bonus_used = self._calculate_bonus_used(
                            chapter_count, limit_info)
                        await self._update_novel_usage(str(message.author.id),
                                                       novel_key,
                                                       novel_title_stored,
                                                       total_ch, chapter_count,
                                                       'novel')
                        await self._update_daily_usage(
                            str(message.author.id),
                            novel_chapters=chapter_count,
                            novel_bonus_used=bonus_used)

                        # Show Patreon reminder for normal users only
                        if state.get('user_tier', 'normal') == 'normal':
                            patreon_embed = discord.Embed(
                                title="Support the Bot",
                                description=
                                f"Please support me to maintain the scraper!",
                                color=discord.Color.pink(),
                                url=PATREON_LINK)
                            await message.channel.send(embed=patreon_embed)

                        # Cleanup
                        os.remove(filename)
                    except discord.errors.HTTPException as e:
                        logger.error(f"Failed to send file: {e}")
                        # Try external upload with progress
                        file_size_mb = os.path.getsize(filename) / (1024 *
                                                                    1024)
                        progress_msg = await message.channel.send(
                            f"**Uploading to external host...**\n"
                            f"File size: {file_size_mb:.1f}MB\n"
                            f"Trying: Litterbox...")

                        upload_status = {"current": "Litterbox", "tried": []}

                        def update_upload_progress(service, status):
                            if status == "uploading":
                                upload_status["current"] = service
                            elif status == "failed":
                                upload_status["tried"].append(service)

                        import functools
                        upload_func = functools.partial(
                            upload_large_file, filename,
                            update_upload_progress)
                        upload_task = self.loop.run_in_executor(
                            None, upload_func)

                        while not upload_task.done():
                            tried = ", ".join(
                                upload_status["tried"]
                            ) if upload_status["tried"] else "None"
                            await progress_msg.edit(content=(
                                f"**Uploading to external host...**\n"
                                f"File size: {file_size_mb:.1f}MB\n"
                                f"Trying: {upload_status['current']}...\n"
                                f"Failed: {tried}"))
                            await asyncio.sleep(3)

                        upload_url, service = await upload_task
                        if upload_url:
                            await progress_msg.edit(
                                content=f"Upload complete to {service}!")
                            proper_filename = os.path.basename(filename)

                            # Wrap URL with ShrinkMe ads for free users only (skip hosts with own redirects)
                            display_url = upload_url
                            has_ads = False
                            if user_tier == 'normal':
                                shortened = await self.loop.run_in_executor(
                                    None, lambda: shorten_with_shrinkme(
                                        upload_url, service))
                                if shortened != upload_url:
                                    display_url = shortened
                                    has_ads = True

                            embed = discord.Embed(title="Download Complete",
                                                  description=f"**{title}**",
                                                  color=discord.Color.green())
                            embed.add_field(name="Chapters",
                                            value=str(chapter_count),
                                            inline=True)
                            embed.add_field(name="Format",
                                            value=fmt.upper(),
                                            inline=True)
                            embed.add_field(name="Size",
                                            value=f"{file_size_mb:.1f}MB",
                                            inline=True)
                            embed.add_field(name="Save As",
                                            value=f"`{proper_filename}`",
                                            inline=False)
                            embed.add_field(name="Download Link",
                                            value=display_url,
                                            inline=False)

                            if has_ads:
                                embed.add_field(name="Ad Supported",
                                                value=AD_FREE_MESSAGE,
                                                inline=False)

                            embed.set_footer(
                                text=
                                f"Hosted on {service} | Rename file after downloading"
                            )
                            await message.channel.send(embed=embed)
                            os.remove(filename)
                        else:
                            await progress_msg.edit(
                                content=
                                f"Failed to upload to all hosts. File saved locally: `{filename}`"
                            )
                else:
                    await message.channel.send(
                        "Error: Failed to generate file.")

                # Reset state
                if message.author.id in self.user_states:
                    del self.user_states[message.author.id]

            except Exception as e:
                logger.error(f"Error during scrape/generation: {e}")
                await message.channel.send(
                    f"**Error:** {str(e)}\n\nPlease try again with `start`")
                if message.author.id in self.user_states:
                    del self.user_states[message.author.id]

        # ========== CLOUDFLARE RETRY PROMPT ==========
        elif step == 'cloudflare_retry_prompt':
            user_input = message.content.strip().lower()

            if user_input == 'cancel':
                del self.user_states[message.author.id]
                await message.channel.send(
                    "Cancelled. Type anything to start again.")
                return

            if user_input == 'back':
                state['step'] = 'novel_format'
                await message.channel.send(
                    "**Choose format:**\n"
                    "1. EPUB\n"
                    "2. PDF\n\n"
                    "*Hint: Type `back` to go back or `cancel` to abort*")
                return

            novel_data = state['data'].get('novel_data')
            cloudflare_chapters = state['data'].get('cloudflare_chapters', [])
            url = state['data'].get('url')
            fmt = state['data'].get('format')
            user_tier = state.get('user_tier', 'normal')
            start_time = state['data'].get('start_time', time.time())
            chapter_start = state['data'].get('chapter_start', 1)
            chapter_end = state['data'].get('chapter_end')

            if user_input in ('yes', 'y', 'retry'):
                # Retry Cloudflare chapters
                cf_count = len(cloudflare_chapters)
                await message.channel.send(
                    f"**Retrying {cf_count} Cloudflare-blocked chapters...**")

                from playwright_scraper import fetch_webnovel_chapters_parallel_sync

                # Retry fetching the blocked chapters
                results = await self.loop.run_in_executor(
                    None, lambda: fetch_webnovel_chapters_parallel_sync(
                        cloudflare_chapters, concurrency=3))

                recovered = 0
                still_blocked = []
                for chap_num, chap_url, html in results:
                    if html and html != "CLOUDFLARE_BLOCKED":
                        # Parse the recovered chapter
                        result = await self.loop.run_in_executor(
                            None,
                            lambda h=html, u=chap_url: self.scraper.
                            _scrape_webnovel_chapter(u, prefetched_html=h))
                        if result and result.get('content') and len(
                                result['content']) > 100:
                            result['chapter_num'] = chap_num
                            result['url'] = chap_url
                            novel_data['chapters'].append(result)
                            recovered += 1
                        else:
                            still_blocked.append((chap_num, chap_url))
                    else:
                        still_blocked.append((chap_num, chap_url))

                # Sort chapters by number
                novel_data['chapters'].sort(
                    key=lambda c: c.get('chapter_num', 999999))

                if recovered > 0:
                    await message.channel.send(
                        f"Recovered **{recovered}** chapters! Still blocked: **{len(still_blocked)}**"
                    )
                else:
                    await message.channel.send(
                        f"Could not recover any chapters. Cloudflare protection is active."
                    )

                # Update cloudflare chapters for another potential retry
                if still_blocked:
                    state['data']['cloudflare_chapters'] = still_blocked
                    await message.channel.send(
                        f"**{len(still_blocked)}** chapters still blocked.\n"
                        f"Type **retry** to try again, or **no** to proceed with current chapters."
                    )
                    return

            elif user_input not in ('no', 'n'):
                await message.channel.send(
                    "Please type **yes** to retry or **no** to proceed with current chapters."
                )
                return

            # User said no or all retries done - proceed with file generation
            title = novel_data.get('title', 'Novel')
            chapter_count = len(novel_data.get('chapters', []))

            if chapter_count == 0:
                await message.channel.send(
                    "No chapters available. All were blocked by Cloudflare.")
                del self.user_states[message.author.id]
                return

            await message.channel.send(f"**Generating {fmt.upper()}...**\n"
                                       f"Chapters: {chapter_count}")

            try:
                if fmt == 'epub':
                    user_id_str = str(message.author.id)
                    filename = await self.loop.run_in_executor(
                        None, lambda: create_epub(novel_data, user_id_str, user_tier))
                else:
                    user_id_str = str(message.author.id)
                    filename = await self.loop.run_in_executor(
                        None, lambda: create_pdf(novel_data, user_id_str, user_tier))

                if os.path.exists(filename):
                    file_size_mb = os.path.getsize(filename) / (1024 * 1024)

                    if is_file_too_large_for_discord(filename):
                        upload_url, service = await self.loop.run_in_executor(
                            None, lambda: upload_large_file(filename))
                        if upload_url:
                            display_url = upload_url
                            if user_tier == 'normal':
                                shortened = await self.loop.run_in_executor(
                                    None, lambda: shorten_with_shrinkme(
                                        upload_url, service))
                                if shortened != upload_url:
                                    display_url = shortened

                            proper_filename = os.path.basename(filename)
                            embed = discord.Embed(title="Download Complete",
                                                  description=f"**{title}**",
                                                  color=discord.Color.green())
                            embed.add_field(name="Chapters",
                                            value=str(chapter_count),
                                            inline=True)
                            embed.add_field(name="Format",
                                            value=fmt.upper(),
                                            inline=True)
                            embed.add_field(name="Size",
                                            value=f"{file_size_mb:.1f}MB",
                                            inline=True)
                            embed.add_field(name="Download Link",
                                            value=display_url,
                                            inline=False)
                            embed.set_footer(text=f"Hosted on {service}")
                            await message.channel.send(embed=embed)
                            os.remove(filename)
                    else:
                        embed = discord.Embed(title="Download Complete",
                                              description=f"**{title}**",
                                              color=discord.Color.green())
                        embed.add_field(name="Chapters",
                                        value=str(chapter_count),
                                        inline=True)
                        embed.add_field(name="Format",
                                        value=fmt.upper(),
                                        inline=True)
                        await message.channel.send(embed=embed,
                                                   file=discord.File(filename))
                        os.remove(filename)

                    # Log download
                    duration = int(time.time() - start_time)
                    await log_download({
                        'discordUserId':
                        str(message.author.id),
                        'discordUsername':
                        str(message.author.name),
                        'novelTitle':
                        title,
                        'source':
                        url.split('/')[2] if '/' in url else 'unknown',
                        'chapterStart':
                        chapter_start,
                        'chapterEnd':
                        chapter_end or chapter_count,
                        'chapterCount':
                        chapter_count,
                        'format':
                        fmt,
                        'userTier':
                        user_tier,
                        'durationSeconds':
                        duration,
                        'status':
                        'completed'
                    })
                else:
                    await message.channel.send("Error generating file.")
            except Exception as e:
                logger.error(f"Error in cloudflare retry handler: {e}")
                await message.channel.send(f"Error: {str(e)}")

            # Clean up state
            if message.author.id in self.user_states:
                del self.user_states[message.author.id]

        # ========== NOVEL LIMIT CONFIRMATION ==========
        elif step == 'novel_confirm_limit':
            user_input = message.content.strip().lower()

            if user_input == 'cancel':
                del self.user_states[message.author.id]
                await message.channel.send(
                    "Cancelled. Type anything to start again.")
                return

            if user_input != 'yes':
                await message.channel.send(
                    "Please type **yes** to continue or **cancel** to abort.")
                return

            # User confirmed, proceed with limited download
            limit_check = state['data'].get('limit_check', {})
            chapter_start = state['data'].get('chapter_start', 1)
            chapter_end = chapter_start + limit_check.get('max_now', 1) - 1
            state['data']['chapter_end'] = chapter_end

            url = state['data'].get('url')
            fmt = state['data'].get('format')
            user_tier = state.get('user_tier', 'normal')
            total_chapters = state['data'].get('total_chapters', 500)

            chapter_range_display = f"{chapter_start}-{chapter_end}"
            logger.info(
                f"Confirmed limited download: {url} -> {fmt.upper()} -> chapters {chapter_range_display}"
            )

            await message.channel.send(
                f"Proceeding with chapters {chapter_range_display}...")

            try:
                # Mark as scraping in progress
                state['step'] = 'scraping_in_progress'
                # Calculate actual chapter count for progress display
                actual_count = chapter_end - chapter_start + 1
                state['progress_data'] = {'current': 0, 'total': actual_count}
                start_time = time.time()

                # Scrape with progress updates
                novel_data = await self._scrape_with_progress(
                    url, message, message.author.id, chapter_start,
                    chapter_end, user_tier)

                if not novel_data:
                    if message.author.id in self.user_states:
                        del self.user_states[message.author.id]
                    return

                # Generate file
                title = novel_data.get('title', 'Novel')
                chapter_count = len(novel_data.get('chapters', []))

                await message.channel.send(f"Generating {fmt.upper()}...\n"
                                           f"Chapters: {chapter_count}")

                if fmt == 'epub':
                    user_id_str = str(message.author.id)
                    filename = await self.loop.run_in_executor(
                        None, lambda: create_epub(novel_data, user_id_str, user_tier))
                else:
                    user_id_str = str(message.author.id)
                    filename = await self.loop.run_in_executor(
                        None, lambda: create_pdf(novel_data, user_id_str, user_tier))

                # Send file
                if os.path.exists(filename):
                    file_size_mb = os.path.getsize(filename) / (1024 * 1024)

                    if is_file_too_large_for_discord(filename):
                        upload_url, service = await self.loop.run_in_executor(
                            None, upload_large_file, filename)
                        if upload_url:
                            embed = discord.Embed(title="Download Complete",
                                                  description=f"**{title}**",
                                                  color=discord.Color.green())
                            embed.add_field(name="Chapters",
                                            value=str(chapter_count),
                                            inline=True)
                            embed.add_field(name="Format",
                                            value=fmt.upper(),
                                            inline=True)
                            embed.add_field(name="Download Link",
                                            value=upload_url,
                                            inline=False)
                            await message.channel.send(embed=embed)
                            os.remove(filename)
                        else:
                            await message.channel.send("Failed to upload file."
                                                       )
                    else:
                        embed = discord.Embed(title="Download Complete",
                                              description=f"**{title}**",
                                              color=discord.Color.green())
                        embed.add_field(name="Chapters",
                                        value=str(chapter_count),
                                        inline=True)
                        embed.add_field(name="Format",
                                        value=fmt.upper(),
                                        inline=True)
                        await message.channel.send(embed=embed,
                                                   file=discord.File(filename))
                        os.remove(filename)

                    # Track per-novel usage (use URL-based key to prevent bypass)
                    novel_key = state['data'].get(
                        'novel_key') or self._normalize_novel_key(url)
                    novel_title_stored = state['data'].get(
                        'novel_title', title)
                    total_ch = state['data'].get('total_chapters',
                                                 chapter_count)
                    limit_info = state['data'].get('limit_check',
                                                   {}).get('limit_info', {})
                    bonus_used = self._calculate_bonus_used(
                        chapter_count, limit_info)
                    await self._update_novel_usage(str(message.author.id),
                                                   novel_key,
                                                   novel_title_stored,
                                                   total_ch, chapter_count,
                                                   'novel')
                    await self._update_daily_usage(
                        str(message.author.id),
                        novel_chapters=chapter_count,
                        novel_bonus_used=bonus_used)

                    # Log download
                    await log_download({
                        'discordUserId':
                        str(message.author.id),
                        'discordUsername':
                        str(message.author.name),
                        'novelTitle':
                        title,
                        'source':
                        url.split('/')[2] if '/' in url else 'unknown',
                        'chapterStart':
                        chapter_start,
                        'chapterEnd':
                        chapter_end,
                        'chapterCount':
                        chapter_count,
                        'format':
                        fmt,
                        'userTier':
                        user_tier,
                        'durationSeconds':
                        int(time.time() - start_time),
                        'status':
                        'completed'
                    })
                else:
                    await message.channel.send(
                        "Error: Failed to generate file.")

                if message.author.id in self.user_states:
                    del self.user_states[message.author.id]

            except Exception as e:
                logger.error(f"Error during confirmed scrape: {e}")
                await message.channel.send(f"**Error:** {str(e)}")
                if message.author.id in self.user_states:
                    del self.user_states[message.author.id]

        # ========== MANGA/NOVEL TYPE SELECTION ==========
        elif step == 'waiting_for_content_type':
            user_choice = message.content.strip().lower()
            search_query = state['data'].get('search_query', '')

            # Handle back/cancel
            if user_choice == 'back':
                state['step'] = 'awaiting_welcome_ack'
                await message.channel.send(
                    "↩️ Going back. Please enter a title or URL:")
                return
            if user_choice == 'cancel':
                del self.user_states[message.author.id]
                await message.channel.send(
                    "Cancelled. Type anything to start again.")
                return

            if user_choice == '1':
                # Novel search
                state['data']['content_type'] = 'novel'
                await message.channel.send(
                    f"Searching for '{search_query}' in novels...")
                results = await self._search_with_choices(
                    search_query, message)

                if results:
                    if len(results) == 1:
                        novel = results[0]
                        state['data']['selected_novel'] = novel
                        state['step'] = 'waiting_for_source_choice'

                        sources = novel.get('sources', [])
                        if not sources:
                            await message.channel.send(
                                f"No sources found for '{novel.get('title', 'Unknown')}'. Please try another search or provide a direct URL."
                            )
                            del self.user_states[message.author.id]
                            return
                        sources_text = f"**Found: {novel.get('title', 'Unknown')}**\n\nAvailable sources:\n"
                        for i, src in enumerate(sources, 1):
                            sources_text += f"**{i}.** {src.get('source', 'Unknown')}\n   {src.get('url', '')}\n"
                        sources_text += f"\nReply with a number (1-{len(sources)})" + HINT_TEXT
                        await message.channel.send(sources_text)
                    else:
                        state['data']['search_results'] = results
                        state['step'] = 'waiting_for_novel_choice'

                        choices_text = "**Found multiple novels!** Pick one:\n\n"
                        for i, result in enumerate(results, 1):
                            source_count = len(result.get('sources', []))
                            sources_list = ", ".join([src['source'] for src in result.get('sources', [])])
                            choices_text += f"**{i}.** {result['title']}\n   ({source_count} {'site' if source_count == 1 else 'sites'}: {sources_list})\n\n"
                        choices_text += f"Reply with a number (1-{len(results)})" + HINT_TEXT
                        await message.channel.send(choices_text)
                else:
                    await message.channel.send(
                        "Novel not found. Please try another search or provide a direct URL."
                    )
                    del self.user_states[message.author.id]

            elif user_choice == '2':
                # Manga search
                state['data']['content_type'] = 'manga'
                await message.channel.send(
                    f"Searching for '{search_query}' in manga sites...")

                try:
                    manga_results = await self.loop.run_in_executor(
                        None, self.manga_scraper.search_manga, search_query)

                    if manga_results:
                        state['data']['manga_search_results'] = manga_results
                        state['step'] = 'waiting_for_manga_choice'

                        choices_text = "**Found manga!** Pick one:\n\n"
                        for i, result in enumerate(manga_results, 1):
                            choices_text += f"**{i}.** {result['title']}\n   Source: {result.get('source', 'Unknown')}\n"
                        choices_text += f"\nReply with a number (1-{len(manga_results)})" + HINT_TEXT
                        await message.channel.send(choices_text)
                    else:
                        await message.channel.send(
                            "Manga not found. Please try another search or provide a direct URL."
                        )
                        del self.user_states[message.author.id]
                except Exception as e:
                    logger.error(f"Manga search error: {e}")
                    await message.channel.send(f"Search failed: {e}")
                    del self.user_states[message.author.id]
            else:
                await message.channel.send(
                    "Please reply with `1` for Novel or `2` for Manga.")

        # ========== MANGA CHOICE SELECTION ==========
        elif step == 'waiting_for_manga_choice':
            user_choice = message.content.strip().lower()
            manga_results = state['data'].get('manga_search_results', [])

            if user_choice == 'back':
                state['step'] = 'awaiting_title'
                await message.channel.send(
                    "Going back. Please enter a title or URL:")
                return

            try:
                choice_num = int(user_choice) - 1
                if 0 <= choice_num < len(manga_results):
                    selected_manga = manga_results[choice_num]
                    state['data']['url'] = selected_manga['url']
                    state['data']['manga_title'] = selected_manga['title']
                    state['step'] = 'waiting_for_manga_format'

                    await message.channel.send(
                        f"**Selected: {selected_manga['title']}**\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "**Select Your Preferred Format**\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "**1️⃣  PDF** - All images in one file\n"
                        "  Best for: Reading, sharing\n\n"
                        "**2️⃣  ZIP** - Images organized by chapter\n"
                        "  Best for: Archiving, offline viewing\n\n"
                        "Reply with `1` or `2`" + HINT_TEXT)
                else:
                    await message.channel.send(
                        f"Please reply with a number between 1 and {len(manga_results)}."
                    )
            except ValueError:
                await message.channel.send(
                    f"Please reply with a number between 1 and {len(manga_results)}."
                )

        # ========== MANGA FORMAT SELECTION ==========
        elif step == 'waiting_for_manga_format':
            user_choice = message.content.strip().lower()

            # Handle back/cancel
            if user_choice == 'back':
                state['step'] = 'awaiting_welcome_ack'
                await message.channel.send(
                    "↩️ Going back. Please enter a manga title or URL:")
                return
            if user_choice == 'cancel':
                del self.user_states[message.author.id]
                await message.channel.send(
                    "Cancelled. Type anything to start again.")
                return

            if user_choice == '1':
                state['data']['format'] = 'pdf'
            elif user_choice == '2':
                state['data']['format'] = 'zip'
            else:
                await message.channel.send(
                    "Please reply with `1` for PDF or `2` for ZIP." +
                    HINT_TEXT)
                return

            # Get manga info and chapter count
            url = state['data'].get('url')
            await message.channel.send("Fetching manga info...")

            try:
                manga_info = await self.loop.run_in_executor(
                    None, self.manga_scraper.get_manga_info, url)

                if 'error' in manga_info:
                    await message.channel.send(f"Error: {manga_info['error']}")
                    del self.user_states[message.author.id]
                    return

                total_chapters = len(manga_info.get('chapters', []))
                state['data']['manga_info'] = manga_info
                state['data']['total_chapters'] = total_chapters
                state['step'] = 'waiting_for_manga_chapters'

                # All users have unlimited downloads
                limit_text = "**Downloads:** Unlimited"
                remaining_text = f"\n**Available:** All {total_chapters} chapters"

                # === FUTURE LIMIT IMPLEMENTATION (commented out) ===
                # user_tier = state.get('user_tier', 'normal')
                # if user_tier in ('catnip', 'sponsor'):
                #     limit_text = "**Your Limit:** Unlimited"
                #     remaining_text = ""
                # else:
                #     novel_key = self._normalize_novel_key(url)
                #     novel_usage = await self._get_novel_usage(str(message.author.id), novel_key)
                #     used = novel_usage.get('chapters_used', 0)
                #     daily_usage = await self._get_daily_usage(str(message.author.id))
                #     bonus_used_today = daily_usage.get('manga_bonus_used', 0)
                #     limit_info = self._calculate_limit(user_tier, total_chapters, 'manga')
                #     percent = limit_info['percent']
                #     percent_limit = limit_info['percent_limit']
                #     daily_bonus = limit_info['daily_bonus']
                #     percent_remaining = max(0, percent_limit - used)
                #     bonus_remaining = max(0, daily_bonus - bonus_used_today)
                #     max_available = percent_limit + bonus_remaining
                #     total_remaining = percent_remaining + bonus_remaining
                #     limit_text = f"**Your Limit:** {percent}% of {total_chapters} chapters = {percent_limit} + {bonus_remaining}/{daily_bonus} bonus = **{max_available} max**"
                #     if used > 0:
                #         remaining_text = f"\n**Already Downloaded (this manga):** {used}\n**Bonus Used Today:** {bonus_used_today}/{daily_bonus}\n**Remaining:** {total_remaining} chapters"
                #     else:
                #         if bonus_used_today > 0:
                #             remaining_text = f"\n**Bonus Used Today:** {bonus_used_today}/{daily_bonus}\n**You Can Download:** {max_available} chapters from this manga"
                #         else:
                #             remaining_text = f"\n**You Can Download:** {max_available} chapters from this manga"

                await message.channel.send(
                    f"**{manga_info.get('title', 'Manga')}**\n"
                    f"Chapters available: {total_chapters}\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "**Select Chapter Range**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"{limit_text}{remaining_text}\n\n"
                    "**Examples:**\n"
                    "  `1-10` - Chapters 1 to 10\n"
                    "  `5` - Just chapter 5\n"
                    "  `all` - All chapters\n\n"
                    "Reply with a chapter range:" + HINT_TEXT)
            except Exception as e:
                logger.error(f"Error getting manga info: {e}")
                await message.channel.send(f"Error: {e}")
                del self.user_states[message.author.id]

        # ========== MANGA CHAPTER RANGE ==========
        elif step == 'waiting_for_manga_chapters':
            user_input = message.content.strip().lower()
            manga_info = state['data'].get('manga_info', {})
            total_chapters = state['data'].get('total_chapters', 0)
            fmt = state['data'].get('format', 'pdf')
            user_tier = state.get('user_tier', 'normal')

            # Handle back/cancel
            if user_input == 'back':
                state['step'] = 'waiting_for_manga_format'
                await message.channel.send("↩️ Going back.\n\n"
                                           "**Select Your Preferred Format**\n"
                                           "**1️⃣  PDF** | **2️⃣  ZIP**" +
                                           HINT_TEXT)
                return
            if user_input == 'cancel':
                del self.user_states[message.author.id]
                await message.channel.send(
                    "Cancelled. Type anything to start again.")
                return

            try:
                chapter_start, chapter_end = self._parse_chapter_range(
                    user_input)

                # Handle 'all' case
                if chapter_end is None:
                    chapter_end = total_chapters

                # Validate range
                if chapter_start < 1 or chapter_end > total_chapters:
                    await message.channel.send(
                        f"Invalid range. Available: 1-{total_chapters}")
                    return

                state['data']['chapter_start'] = chapter_start
                state['data']['chapter_end'] = chapter_end

            except Exception as e:
                await message.channel.send(
                    "Invalid chapter range. Try: `1-10`, `5`, or `all`")
                return

            # Check download limits (per-manga tracking)
            requested_chapters = chapter_end - chapter_start + 1
            manga_title = manga_info.get('title', 'Unknown Manga')
            manga_url = state['data'].get('url', '')
            limit_check = await self._check_download_allowed(
                str(message.author.id), user_tier, total_chapters,
                requested_chapters, 'manga', manga_url, manga_title)
            state['data']['novel_key'] = limit_check.get('novel_key', '')
            state['data']['novel_title'] = manga_title
            state['data'][
                'limit_check'] = limit_check  # Store for bonus calculation

            if not limit_check['allowed']:
                await message.channel.send(
                    f"**Daily Limit Reached**\n{limit_check['message']}")
                del self.user_states[message.author.id]
                return

            # Apply limit if needed
            if limit_check['max_now'] < requested_chapters:
                chapter_end = chapter_start + limit_check['max_now'] - 1
                state['data']['chapter_end'] = chapter_end
                await message.channel.send(
                    f"**Note:** {limit_check['message']}")

            # Start manga download
            state['step'] = 'scraping_in_progress'
            url = state['data'].get('url')
            title = manga_info.get('title', 'Manga')

            chapter_range = f"{chapter_start}-{chapter_end}" if chapter_end != chapter_start else str(
                chapter_start)
            await message.channel.send(
                f"**Downloading manga...**\n"
                f"Title: {title}\n"
                f"Chapters: {chapter_range}\n"
                f"Format: {fmt.upper()}\n\n"
                "This may take a while for large chapters...")

            try:
                start_time = time.time()
                last_update_time = [start_time]
                progress_message = [None
                                    ]  # Store the progress message to edit it

                # Progress callback that edits a single message
                async def send_progress(msg):
                    current_time = time.time()
                    # Throttle updates to every 3 seconds to avoid rate limits
                    if progress_message[0] is None or (current_time -
                                                       last_update_time[0]
                                                       >= 3):
                        last_update_time[0] = current_time
                        try:
                            if progress_message[0] is None:
                                # Create the first progress message
                                progress_message[
                                    0] = await message.channel.send(
                                        f"**Progress:** {msg}")
                            else:
                                # Edit the existing progress message
                                await progress_message[0].edit(
                                    content=f"**Progress:** {msg}")
                        except Exception as e:
                            logger.debug(f"Progress update error: {e}")
                    logger.info(f"Manga progress: {msg}")

                def progress_callback(msg):
                    # Schedule async update on event loop
                    asyncio.run_coroutine_threadsafe(send_progress(msg),
                                                     self.loop)

                manga_data = await self.loop.run_in_executor(
                    None, lambda: self.manga_scraper.download_manga(
                        url, chapter_start, chapter_end, progress_callback))

                # Extract source domain for tracking
                manga_source = url.split('/')[2] if '/' in url else 'unknown'

                if 'error' in manga_data:
                    await message.channel.send(f"Error: {manga_data['error']}")
                    # Track failed manga request
                    await track_content_request(title,
                                                'manga',
                                                manga_source,
                                                success=False,
                                                loop=self.loop)
                    await log_site_failure(manga_source,
                                           'manga_error',
                                           manga_data['error'],
                                           novel_url=url,
                                           loop=self.loop)
                    del self.user_states[message.author.id]
                    return

                if not manga_data.get('chapters'):
                    await message.channel.send(
                        "No chapters were downloaded successfully.")
                    # Track failed manga request
                    await track_content_request(title,
                                                'manga',
                                                manga_source,
                                                success=False,
                                                loop=self.loop)
                    await log_site_failure(manga_source,
                                           'no_chapters',
                                           'No manga chapters downloaded',
                                           novel_url=url,
                                           loop=self.loop)
                    del self.user_states[message.author.id]
                    return

                # Track successful manga request
                await track_content_request(title,
                                            'manga',
                                            manga_source,
                                            success=True,
                                            loop=self.loop)

                # Generate file
                chapter_count = len(manga_data.get('chapters', []))
                await message.channel.send(
                    f"Generating {fmt.upper()} with {chapter_count} chapters..."
                )

                if fmt == 'pdf':
                    filename = await self.loop.run_in_executor(
                        None, create_manga_pdf, manga_data)
                else:
                    filename = await self.loop.run_in_executor(
                        None, create_manga_zip, manga_data)

                # Cleanup temp directory
                output_dir = manga_data.get('output_dir', '')
                if output_dir:
                    await self.loop.run_in_executor(None, cleanup_manga_temp,
                                                    output_dir)

                # Send file
                if os.path.exists(filename):
                    file_size_mb = os.path.getsize(filename) / (1024 * 1024)

                    # Check if file is too large for Discord (proactive upload)
                    if is_file_too_large_for_discord(filename):
                        progress_msg = await message.channel.send(
                            f"**Uploading to external host...**\n"
                            f"File size: {file_size_mb:.1f}MB\n"
                            f"Trying: Litterbox...")

                        # Track upload progress
                        upload_status = {"current": "Litterbox", "tried": []}

                        def update_upload_progress(service, status):
                            if status == "uploading":
                                upload_status["current"] = service
                            elif status == "failed":
                                upload_status["tried"].append(service)

                        async def update_progress_message():
                            tried = ", ".join(
                                upload_status["tried"]
                            ) if upload_status["tried"] else "None"
                            current = upload_status["current"]
                            await progress_msg.edit(
                                content=(f"**Uploading to external host...**\n"
                                         f"File size: {file_size_mb:.1f}MB\n"
                                         f"Trying: {current}...\n"
                                         f"Failed: {tried}"))

                        # Run upload with progress updates
                        import functools
                        upload_func = functools.partial(
                            upload_large_file, filename,
                            update_upload_progress)

                        # Start upload task
                        upload_task = self.loop.run_in_executor(
                            None, upload_func)

                        # Update message periodically while uploading
                        while not upload_task.done():
                            await update_progress_message()
                            await asyncio.sleep(3)

                        upload_url, service = await upload_task

                        if upload_url:
                            await progress_msg.edit(
                                content=f"Upload complete to {service}!")
                            proper_filename = os.path.basename(filename)

                            # Wrap URL with ShrinkMe ads for free users only (skip hosts with own redirects)
                            display_url = upload_url
                            has_ads = False
                            if user_tier == 'normal':
                                shortened = await self.loop.run_in_executor(
                                    None, lambda: shorten_with_shrinkme(
                                        upload_url, service))
                                if shortened != upload_url:
                                    display_url = shortened
                                    has_ads = True

                            embed = discord.Embed(
                                title="Manga Download Complete",
                                description=f"**{title}**",
                                color=discord.Color.blue())
                            embed.add_field(name="Chapters",
                                            value=str(chapter_count),
                                            inline=True)
                            embed.add_field(name="Format",
                                            value=fmt.upper(),
                                            inline=True)
                            embed.add_field(name="Size",
                                            value=f"{file_size_mb:.1f}MB",
                                            inline=True)
                            embed.add_field(name="Save As",
                                            value=f"`{proper_filename}`",
                                            inline=False)
                            embed.add_field(name="Download Link",
                                            value=display_url,
                                            inline=False)

                            if has_ads:
                                embed.add_field(name="Ad Supported",
                                                value=AD_FREE_MESSAGE,
                                                inline=False)

                            embed.set_footer(
                                text=
                                f"Hosted on {service} | Rename file after downloading"
                            )
                            try:
                                await message.channel.send(embed=embed)
                                logger.info(
                                    f"Manga embed sent to {message.channel} with link: {display_url}"
                                )
                            except Exception as e:
                                logger.error(
                                    f"Failed to send manga embed: {e}")
                                await message.channel.send(
                                    f"Download complete! Link: {display_url}")

                            # Ask if download worked - offer retry with different host
                            retry_prompt = await message.channel.send(
                                "**Did the file download successfully?**\n"
                                "Press `1` to retry with a different file host\n"
                                "Press `2` or wait 30 seconds to finish")

                            tried_hosts = upload_status.get("tried",
                                                            []) + [service]

                            def check_retry(m):
                                return m.author == message.author and m.channel == message.channel and m.content.strip(
                                ) in ['1', '2']

                            try:
                                retry_response = await self.wait_for(
                                    'message', check=check_retry, timeout=30)
                                if retry_response.content.strip() == '1':
                                    await retry_prompt.delete()
                                    retry_msg = await message.channel.send(
                                        "Retrying with different host...")

                                    # Try alternative hosts
                                    alt_upload_url, alt_service = await self.loop.run_in_executor(
                                        None, lambda: upload_large_file(
                                            filename, skip_hosts=tried_hosts))

                                    if alt_upload_url:
                                        await retry_msg.delete()
                                        alt_display_url = alt_upload_url
                                        if user_tier == 'normal':
                                            shortened = await self.loop.run_in_executor(
                                                None,
                                                lambda: shorten_with_shrinkme(
                                                    alt_upload_url, alt_service
                                                ))
                                            if shortened != alt_upload_url:
                                                alt_display_url = shortened

                                        alt_embed = discord.Embed(
                                            title="Alternative Download Link",
                                            description=f"**{title}**",
                                            color=discord.Color.green())
                                        alt_embed.add_field(
                                            name="Download Link",
                                            value=alt_display_url,
                                            inline=False)
                                        alt_embed.set_footer(
                                            text=f"Hosted on {alt_service}")
                                        await message.channel.send(
                                            embed=alt_embed)
                                        logger.info(
                                            f"Alternative link sent: {alt_display_url} via {alt_service}"
                                        )
                                    else:
                                        await retry_msg.edit(
                                            content=
                                            "No more alternative hosts available. Please try again later."
                                        )
                                else:
                                    await retry_prompt.delete()
                            except asyncio.TimeoutError:
                                try:
                                    await retry_prompt.delete()
                                except:
                                    pass

                            # Log to database
                            duration = int(time.time() - start_time)
                            await log_download({
                                'discordUserId':
                                str(message.author.id),
                                'discordUsername':
                                str(message.author.name),
                                'novelTitle':
                                f"[MANGA] {title}",
                                'source':
                                manga_data.get('source', 'manga'),
                                'chapterStart':
                                chapter_start,
                                'chapterEnd':
                                chapter_end,
                                'chapterCount':
                                chapter_count,
                                'format':
                                fmt,
                                'userTier':
                                user_tier,
                                'durationSeconds':
                                duration,
                                'status':
                                'completed'
                            })
                            # Track per-manga usage (use URL-based key to prevent bypass)
                            manga_url = state['data'].get('url', '')
                            novel_key = state['data'].get(
                                'novel_key') or self._normalize_novel_key(
                                    manga_url)
                            novel_title_stored = state['data'].get(
                                'novel_title', title)
                            total_ch = state['data'].get(
                                'total_chapters', chapter_count)
                            limit_info = state['data'].get(
                                'limit_check', {}).get('limit_info', {})
                            bonus_used = self._calculate_bonus_used(
                                chapter_count, limit_info)
                            await self._update_novel_usage(
                                str(message.author.id), novel_key,
                                novel_title_stored, total_ch, chapter_count,
                                'manga')
                            await self._update_daily_usage(
                                str(message.author.id),
                                manga_chapters=chapter_count,
                                manga_bonus_used=bonus_used)
                            os.remove(filename)
                        else:
                            await progress_msg.edit(
                                content=
                                f"Failed to upload to all hosts. File saved locally: `{filename}`"
                            )

                        # Reset state
                        if message.author.id in self.user_states:
                            del self.user_states[message.author.id]
                        return

                    try:
                        # For free users, always use external upload + ad links
                        if user_tier == 'normal':
                            progress_msg = await message.channel.send(
                                f"**Uploading...**\n"
                                f"File size: {file_size_mb:.1f}MB")

                            upload_status = {
                                "current": "Litterbox",
                                "tried": []
                            }

                            def update_upload_progress_manga_small(
                                    service, status):
                                if status == "uploading":
                                    upload_status["current"] = service
                                elif status == "failed":
                                    upload_status["tried"].append(service)

                            import functools
                            upload_func = functools.partial(
                                upload_large_file, filename,
                                update_upload_progress_manga_small)
                            upload_url, service = await self.loop.run_in_executor(
                                None, upload_func)

                            if upload_url:
                                await progress_msg.delete()
                                proper_filename = os.path.basename(filename)

                                # Wrap with ShrinkMe ads
                                shortened = await self.loop.run_in_executor(
                                    None, lambda: shorten_with_shrinkme(
                                        upload_url, service))
                                display_url = shortened
                                has_ads = shortened != upload_url

                                embed = discord.Embed(
                                    title="Manga Download Complete",
                                    description=f"**{title}**",
                                    color=discord.Color.blue())
                                embed.add_field(name="Chapters",
                                                value=str(chapter_count),
                                                inline=True)
                                embed.add_field(name="Format",
                                                value=fmt.upper(),
                                                inline=True)
                                embed.add_field(name="Size",
                                                value=f"{file_size_mb:.1f}MB",
                                                inline=True)
                                embed.add_field(name="Save As",
                                                value=f"`{proper_filename}`",
                                                inline=False)
                                embed.add_field(name="Download Link",
                                                value=display_url,
                                                inline=False)

                                if has_ads:
                                    embed.add_field(name="Ad Supported",
                                                    value=AD_FREE_MESSAGE,
                                                    inline=False)

                                cover_url = manga_data.get('cover', '')
                                if cover_url:
                                    embed.set_thumbnail(url=cover_url)

                                embed.set_footer(
                                    text=
                                    f"Hosted on {service} | Rename file after downloading"
                                )
                                await message.channel.send(embed=embed)

                                # Ask if download worked - offer retry with different host
                                retry_prompt = await message.channel.send(
                                    "**Did the file download successfully?**\n"
                                    "Press `1` to retry with a different file host\n"
                                    "Press `2` or wait 30 seconds to finish")

                                tried_hosts_small = upload_status.get(
                                    "tried", []) + [service]

                                def check_retry_small(m):
                                    return m.author == message.author and m.channel == message.channel and m.content.strip(
                                    ) in ['1', '2']

                                try:
                                    retry_response = await self.wait_for(
                                        'message',
                                        check=check_retry_small,
                                        timeout=30)
                                    if retry_response.content.strip() == '1':
                                        await retry_prompt.delete()
                                        retry_msg = await message.channel.send(
                                            "Retrying with different host...")

                                        alt_upload_url, alt_service = await self.loop.run_in_executor(
                                            None, lambda: upload_large_file(
                                                filename,
                                                skip_hosts=tried_hosts_small))

                                        if alt_upload_url:
                                            await retry_msg.delete()
                                            alt_display_url = alt_upload_url
                                            shortened = await self.loop.run_in_executor(
                                                None,
                                                lambda: shorten_with_shrinkme(
                                                    alt_upload_url, alt_service
                                                ))
                                            if shortened != alt_upload_url:
                                                alt_display_url = shortened

                                            alt_embed = discord.Embed(
                                                title=
                                                "Alternative Download Link",
                                                description=f"**{title}**",
                                                color=discord.Color.green())
                                            alt_embed.add_field(
                                                name="Download Link",
                                                value=alt_display_url,
                                                inline=False)
                                            alt_embed.set_footer(
                                                text=f"Hosted on {alt_service}"
                                            )
                                            await message.channel.send(
                                                embed=alt_embed)
                                            logger.info(
                                                f"Alternative link sent: {alt_display_url} via {alt_service}"
                                            )
                                        else:
                                            await retry_msg.edit(
                                                content=
                                                "No more alternative hosts available. Please try again later."
                                            )
                                    else:
                                        await retry_prompt.delete()
                                except asyncio.TimeoutError:
                                    try:
                                        await retry_prompt.delete()
                                    except:
                                        pass

                                # Log and cleanup
                                duration = int(time.time() - start_time)
                                await log_download({
                                    'discordUserId':
                                    str(message.author.id),
                                    'discordUsername':
                                    str(message.author.name),
                                    'novelTitle':
                                    f"[MANGA] {title}",
                                    'source':
                                    manga_data.get('source', 'manga'),
                                    'chapterStart':
                                    chapter_start,
                                    'chapterEnd':
                                    chapter_end,
                                    'chapterCount':
                                    chapter_count,
                                    'format':
                                    fmt,
                                    'userTier':
                                    user_tier,
                                    'durationSeconds':
                                    duration,
                                    'status':
                                    'completed'
                                })
                                manga_url = state['data'].get('url', '')
                                novel_key = state['data'].get(
                                    'novel_key') or self._normalize_novel_key(
                                        manga_url)
                                novel_title_stored = state['data'].get(
                                    'novel_title', title)
                                total_ch = state['data'].get(
                                    'total_chapters', chapter_count)
                                limit_info = state['data'].get(
                                    'limit_check', {}).get('limit_info', {})
                                bonus_used = self._calculate_bonus_used(
                                    chapter_count, limit_info)
                                await self._update_novel_usage(
                                    str(message.author.id), novel_key,
                                    novel_title_stored, total_ch,
                                    chapter_count, 'manga')
                                await self._update_daily_usage(
                                    str(message.author.id),
                                    manga_chapters=chapter_count,
                                    manga_bonus_used=bonus_used)
                                os.remove(filename)
                                logger.info(
                                    f"Manga uploaded with ads for free user: {filename}"
                                )

                                if message.author.id in self.user_states:
                                    del self.user_states[message.author.id]
                                return
                            else:
                                await progress_msg.edit(
                                    content="Upload failed, sending directly..."
                                )
                                # Fall through to direct send below

                        # Paid users or fallback - send directly
                        embed = discord.Embed(title="Manga Download Complete",
                                              description=f"**{title}**",
                                              color=discord.Color.blue())
                        embed.add_field(name="Chapters",
                                        value=str(chapter_count),
                                        inline=True)
                        embed.add_field(name="Format",
                                        value=fmt.upper(),
                                        inline=True)

                        cover_url = manga_data.get('cover', '')
                        if cover_url:
                            embed.set_thumbnail(url=cover_url)

                        embed.set_footer(text="Manga Scraper Bot")

                        await message.channel.send(embed=embed,
                                                   file=discord.File(filename))
                        logger.info(f"Manga file sent: {filename}")

                        # Log to database
                        duration = int(time.time() - start_time)
                        await log_download({
                            'discordUserId':
                            str(message.author.id),
                            'discordUsername':
                            str(message.author.name),
                            'novelTitle':
                            f"[MANGA] {title}",
                            'source':
                            manga_data.get('source', 'manga'),
                            'chapterStart':
                            chapter_start,
                            'chapterEnd':
                            chapter_end,
                            'chapterCount':
                            chapter_count,
                            'format':
                            fmt,
                            'userTier':
                            user_tier,
                            'durationSeconds':
                            duration,
                            'status':
                            'completed'
                        })
                        # Track per-manga usage (use URL-based key to prevent bypass)
                        manga_url = state['data'].get('url', '')
                        novel_key = state['data'].get(
                            'novel_key') or self._normalize_novel_key(
                                manga_url)
                        novel_title_stored = state['data'].get(
                            'novel_title', title)
                        total_ch = state['data'].get('total_chapters',
                                                     chapter_count)
                        limit_info = state['data'].get('limit_check', {}).get(
                            'limit_info', {})
                        bonus_used = self._calculate_bonus_used(
                            chapter_count, limit_info)
                        await self._update_novel_usage(str(message.author.id),
                                                       novel_key,
                                                       novel_title_stored,
                                                       total_ch, chapter_count,
                                                       'manga')
                        await self._update_daily_usage(
                            str(message.author.id),
                            manga_chapters=chapter_count,
                            manga_bonus_used=bonus_used)

                        # Cleanup
                        os.remove(filename)
                    except discord.errors.HTTPException as e:
                        logger.error(f"Failed to send manga file: {e}")
                        # Try external upload with progress
                        file_size_mb = os.path.getsize(filename) / (1024 *
                                                                    1024)
                        progress_msg = await message.channel.send(
                            f"**Uploading to external host...**\n"
                            f"File size: {file_size_mb:.1f}MB\n"
                            f"Trying: Litterbox...")

                        upload_status = {"current": "Litterbox", "tried": []}

                        def update_upload_progress(service, status):
                            if status == "uploading":
                                upload_status["current"] = service
                            elif status == "failed":
                                upload_status["tried"].append(service)

                        import functools
                        upload_func = functools.partial(
                            upload_large_file, filename,
                            update_upload_progress)
                        upload_task = self.loop.run_in_executor(
                            None, upload_func)

                        while not upload_task.done():
                            tried = ", ".join(
                                upload_status["tried"]
                            ) if upload_status["tried"] else "None"
                            await progress_msg.edit(content=(
                                f"**Uploading to external host...**\n"
                                f"File size: {file_size_mb:.1f}MB\n"
                                f"Trying: {upload_status['current']}...\n"
                                f"Failed: {tried}"))
                            await asyncio.sleep(3)

                        upload_url, service = await upload_task
                        if upload_url:
                            await progress_msg.edit(
                                content=f"Upload complete to {service}!")
                            proper_filename = os.path.basename(filename)

                            # Wrap URL with ShrinkMe ads for free users only (skip hosts with own redirects)
                            display_url = upload_url
                            has_ads = False
                            if user_tier == 'normal':
                                shortened = await self.loop.run_in_executor(
                                    None, lambda: shorten_with_shrinkme(
                                        upload_url, service))
                                if shortened != upload_url:
                                    display_url = shortened
                                    has_ads = True

                            embed = discord.Embed(
                                title="Manga Download Complete",
                                description=f"**{title}**",
                                color=discord.Color.blue())
                            embed.add_field(name="Chapters",
                                            value=str(chapter_count),
                                            inline=True)
                            embed.add_field(name="Format",
                                            value=fmt.upper(),
                                            inline=True)
                            embed.add_field(name="Size",
                                            value=f"{file_size_mb:.1f}MB",
                                            inline=True)
                            embed.add_field(name="Save As",
                                            value=f"`{proper_filename}`",
                                            inline=False)
                            embed.add_field(name="Download Link",
                                            value=display_url,
                                            inline=False)

                            if has_ads:
                                embed.add_field(name="Ad Supported",
                                                value=AD_FREE_MESSAGE,
                                                inline=False)

                            embed.set_footer(
                                text=
                                f"Hosted on {service} | Rename file after downloading"
                            )
                            await message.channel.send(embed=embed)
                            os.remove(filename)
                        else:
                            await progress_msg.edit(
                                content=
                                f"Failed to upload to all hosts. File saved locally: `{filename}`"
                            )
                else:
                    await message.channel.send(
                        "Error: Failed to generate file.")

                # Reset state
                if message.author.id in self.user_states:
                    del self.user_states[message.author.id]

            except Exception as e:
                logger.error(f"Manga download error: {e}")
                await message.channel.send(
                    f"Error: {str(e)}\n\nPlease try again with `start`")
                if message.author.id in self.user_states:
                    del self.user_states[message.author.id]


if __name__ == '__main__':
    if not TOKEN:
        print("Error: DISCORD_TOKEN not found in environment variables.")
        exit(1)
    else:
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True  # Required to fetch member info for role checks
        intents.reactions = True  # Required for reaction events
        intents.guilds = True  # Required for channel creation
        client = NovelBot(intents=intents)

        # Register slash command
        @client.tree.command(
            name="create",
            description="Post a message with a reaction (Admin only)",
            guild=discord.Object(id=SERVER_ID))
        @app_commands.describe(text="The message to post",
                               emoji="The reaction emoji (default: 🐾)")
        async def create_command(interaction: discord.Interaction,
                                 text: str,
                                 emoji: str = "🐾"):
            """Slash command: /create text emoji"""
            try:
                # Defer the response immediately to prevent timeout
                await interaction.response.defer(ephemeral=True)

                # Check if user is admin
                member = await client._get_member_in_server(interaction.user.id
                                                            )
                if not member or not client._has_role(member, ADMIN_ROLE_NAME):
                    await interaction.followup.send(
                        "❌ Only admins can use this command.", ephemeral=True)
                    await client.log_to_discord(
                        "⚠️ Unauthorized Admin Command",
                        f"User {interaction.user.name} (ID: {interaction.user.id}) tried to use /create without admin role",
                        discord.Color.orange())
                    return

                # Post the message to the channel
                posted_msg = await interaction.channel.send(text)

                # Add the reaction
                try:
                    await posted_msg.add_reaction(emoji)
                except Exception as e:
                    await interaction.followup.send(
                        f"⚠️ Could not add reaction {emoji}: {e}",
                        ephemeral=True)
                    return

                await client.log_to_discord(
                    "📝 Custom Message Posted",
                    f"Admin {interaction.user.name} posted message with {emoji} reaction in <#{interaction.channel.id}>",
                    discord.Color.blurple())

                await interaction.followup.send(
                    f"✅ Message posted with {emoji} reaction!", ephemeral=True)
            except Exception as e:
                logger.error(f"Error in /create command: {e}")
                try:
                    await interaction.followup.send(f"Error: {e}",
                                                    ephemeral=True)
                except:
                    pass

        @client.tree.command(
            name="resetlimit",
            description="Reset a user's daily download limits (Admin only)",
            guild=discord.Object(id=SERVER_ID))
        @app_commands.describe(user="The user to reset limits for")
        async def resetlimit_command(interaction: discord.Interaction,
                                     user: discord.Member):
            """Slash command: /resetlimit @user"""
            try:
                await interaction.response.defer(ephemeral=True)

                # Check if user is admin
                member = await client._get_member_in_server(interaction.user.id
                                                            )
                if not member or not client._has_role(member, ADMIN_ROLE_NAME):
                    await interaction.followup.send(
                        "❌ Only admins can use this command.", ephemeral=True)
                    await client.log_to_discord(
                        "⚠️ Unauthorized Admin Command",
                        f"User {interaction.user.name} tried to use /resetlimit without admin role",
                        discord.Color.orange())
                    return

                # Call the API to reset limits
                try:
                    resp = requests.delete(
                        f'{API_BASE_URL}/api/daily-limits/{user.id}',
                        timeout=10)

                    if resp.status_code == 200:
                        await interaction.followup.send(
                            f"✅ Reset daily limits for **{user.name}** ({user.id})\n"
                            f"They can now download again today.",
                            ephemeral=True)
                        await client.log_to_discord(
                            "🔄 Limits Reset",
                            f"Admin {interaction.user.name} reset limits for {user.name} ({user.id})",
                            discord.Color.green())
                    else:
                        await interaction.followup.send(
                            f"❌ Failed to reset limits: {resp.text}",
                            ephemeral=True)
                except Exception as e:
                    await interaction.followup.send(f"❌ Error: {str(e)}",
                                                    ephemeral=True)

            except Exception as e:
                logger.error(f"Error in /resetlimit command: {e}")
                try:
                    await interaction.followup.send(f"Error: {e}",
                                                    ephemeral=True)
                except:
                    pass

        # Run bot with error handling
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                logger.info("Starting Discord bot...")
                client.run(TOKEN)
                break  # If run() exits normally
            except discord.errors.GatewayNotFound:
                logger.error("Discord Gateway not found. Retrying...")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(5)
            except discord.errors.PrivilegedIntentsRequired:
                logger.error(
                    "Missing required intents. Check bot permissions.")
                break
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                break
            except Exception as e:
                logger.error(f"Bot error: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(
                        f"Retrying in 5 seconds... ({retry_count}/{max_retries})"
                    )
                    time.sleep(5)
                else:
                    logger.critical("Max retries reached. Exiting.")
                    exit(1)
