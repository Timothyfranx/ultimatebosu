import discord
from discord.ext import commands, tasks
from discord import app_commands
import asyncio
import logging
import os
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List
from pathlib import Path

from database import DatabaseManager
from utils.url_validation import validate_reply_link, extract_urls_bulk_optimized, extract_username_from_x_url
from utils.excel_template import ExcelTemplateManager

logger = logging.getLogger(__name__)


class ReplyTrackerBot(commands.Bot):
    """Replit-optimized Reply Tracker Bot"""

    def __init__(self, config):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True

        super().__init__(command_prefix='!',
                         intents=intents,
                         help_command=None,
                         case_insensitive=True,
                         activity=discord.Activity(
                             type=discord.ActivityType.watching,
                             name="for reply submissions"),
                         status=discord.Status.online)

        self.config = config
        self.start_time = datetime.utcnow()
        self.db = DatabaseManager(config.database_path)
        self.excel_manager = ExcelTemplateManager(config.excel_directory)
        self.onboarding_sessions: Dict[int, Dict[str, Any]] = {}
        self.user_cache: Dict[int, Dict[str, Any]] = {}
        self.cache_ttl = config.cache_ttl_seconds

    async def setup_hook(self):
        logger.info("Bot setup hook started")

        try:
            await self.db.initialize()
            await self.load_extension('commands.admin_commands')
            await self.load_extension('commands.user_commands')

            if self.config.guild_id:
                guild = discord.Object(id=self.config.guild_id)
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
                logger.info(
                    f"Synced {len(synced)} commands to guild {self.config.guild_id}"
                )
            else:
                synced = await self.tree.sync()
                logger.info(f"Synced {len(synced)} commands globally")

            if not self.daily_reminder.is_running():
                self.daily_reminder.start()
            if not self.cleanup_task.is_running():
                self.cleanup_task.start()

        except Exception as e:
            logger.error(f"Error in setup_hook: {e}", exc_info=True)
            raise

    async def on_ready(self):
        logger.info(f'{self.user} is ready!')
        logger.info(f'Bot ID: {self.user.id}')
        logger.info(f'Connected to {len(self.guilds)} guilds')
        await self.health_check()
        await self.restore_tracking_channels()

    async def restore_tracking_channels(self):
        logger.info("Restoring tracking channels after restart...")
        try:
            async with self.db.get_db() as db:
                async with db.execute('''
                    SELECT u.discord_id, u.channel_id, u.username, u.x_username
                    FROM users u
                    JOIN tracking_sessions ts ON u.id = ts.user_id
                    WHERE ts.status = 'active' AND u.channel_id IS NOT NULL
                ''') as cursor:
                    active_users = await cursor.fetchall()

            restored_count = 0
            missing_channels = []
            missing_users = []

            for guild in self.guilds:
                for user_data in active_users:
                    discord_id = user_data['discord_id']
                    channel_id = user_data['channel_id']
                    username = user_data['username']
                    member = guild.get_member(discord_id)
                    if not member:
                        missing_users.append(username)
                        continue
                    channel = guild.get_channel(channel_id)
                    if not channel:
                        missing_channels.append({
                            'member': member,
                            'username': username,
                            'old_channel_id': channel_id
                        })
                        continue
                    restored_count += 1

            logger.info(f"Restored tracking for {restored_count} users")
            if missing_channels:
                logger.warning(
                    f"Found {len(missing_channels)} users with missing channels"
                )
                await self.recreate_missing_channels(missing_channels)
            if missing_users:
                logger.info(
                    f"Found {len(missing_users)} users who left the server")
                await self.cleanup_left_users(missing_users)

        except Exception as e:
            logger.error(f"Error restoring tracking channels: {e}",
                         exc_info=True)

    async def recreate_missing_channels(self, missing_channels):
        logger.info(f"Recreating {len(missing_channels)} missing channels...")
        recreated_count = 0
        for channel_info in missing_channels:
            member = channel_info['member']
            username = channel_info['username']
            old_channel_id = channel_info['old_channel_id']
            try:
                reply_role = discord.utils.get(
                    member.roles, name=self.config.reply_role_name)
                if not reply_role:
                    logger.info(
                        f"Skipping {username} - no longer has reply role")
                    continue
                guild = member.guild
                category = discord.utils.get(
                    guild.categories, name=self.config.tracking_category_name)
                if not category:
                    category = await guild.create_category(
                        self.config.tracking_category_name)
                overwrites = {
                    guild.default_role:
                    discord.PermissionOverwrite(read_messages=False,
                                                send_messages=False),
                    member:
                    discord.PermissionOverwrite(read_messages=True,
                                                send_messages=True,
                                                attach_files=True),
                    guild.me:
                    discord.PermissionOverwrite(read_messages=True,
                                                send_messages=True,
                                                attach_files=True)
                }
                admin_role = discord.utils.get(
                    guild.roles, name=self.config.admin_role_name)
                if admin_role:
                    overwrites[admin_role] = discord.PermissionOverwrite(
                        read_messages=True,
                        send_messages=True,
                        attach_files=True)
                channel_name = f"tracking-{member.display_name.lower().replace(' ', '-')}"
                new_channel = await guild.create_text_channel(
                    channel_name, category=category, overwrites=overwrites)
                async with self.db.get_db() as db:
                    await db.execute(
                        'UPDATE users SET channel_id = ? WHERE discord_id = ?',
                        (new_channel.id, member.id))
                    await db.commit()
                embed = discord.Embed(
                    title="Channel Restored",
                    description=
                    "Your tracking channel was recreated after a bot restart. You can continue submitting links here!",
                    color=discord.Color.blue())
                embed.add_field(name="Note",
                                value="All your previous data is safe",
                                inline=False)
                await new_channel.send(f"{member.mention}", embed=embed)
                recreated_count += 1
                logger.info(
                    f"Recreated channel for {username}: {new_channel.name}")
            except Exception as e:
                logger.error(f"Failed to recreate channel for {username}: {e}")
        logger.info(f"Successfully recreated {recreated_count} channels")
        if recreated_count > 0:
            await self.send_restoration_report(recreated_count,
                                               len(missing_channels))

    async def cleanup_left_users(self, missing_users):
        logger.info(
            f"Cleaning up {len(missing_users)} users who left the server")
        try:
            async with self.db.get_db() as db:
                for username in missing_users:
                    await db.execute(
                        '''
                        UPDATE tracking_sessions 
                        SET status = 'left_server'
                        WHERE user_id IN (
                            SELECT id FROM users WHERE username = ?
                        )
                    ''', (username, ))
                await db.commit()
        except Exception as e:
            logger.error(f"Error cleaning up left users: {e}")

    async def send_restoration_report(self, recreated_count, total_missing):
        try:
            for guild in self.guilds:
                admin_channel = await self.get_admin_channel(guild)
                if admin_channel:
                    embed = discord.Embed(
                        title="Bot Restart - Channel Restoration Report",
                        description="Automatic channel recovery completed",
                        color=discord.Color.green())
                    embed.add_field(name="Channels Recreated",
                                    value=str(recreated_count),
                                    inline=True)
                    embed.add_field(name="Total Missing",
                                    value=str(total_missing),
                                    inline=True)
                    success_rate = f"{(recreated_count/total_missing)*100:.1f}%" if total_missing > 0 else "100%"
                    embed.add_field(name="Success Rate",
                                    value=success_rate,
                                    inline=True)
                    embed.set_footer(
                        text=
                        f"Restoration completed at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
                    )
                    await admin_channel.send(
                        "🔄 **Bot Restart Recovery Report**", embed=embed)
                    break
        except Exception as e:
            logger.error(f"Error sending restoration report: {e}")

    async def health_check(self):
        issues = []
        try:
            async with self.db.get_db() as db:
                await db.execute("SELECT 1")
            excel_dir = Path(self.config.excel_directory)
            if not excel_dir.exists():
                excel_dir.mkdir(exist_ok=True)
                logger.info(f"Created directory: {excel_dir}")
            if self.config.guild_id:
                guild = self.get_guild(self.config.guild_id)
                if guild:
                    reply_role = discord.utils.get(
                        guild.roles, name=self.config.reply_role_name)
                    admin_role = discord.utils.get(
                        guild.roles, name=self.config.admin_role_name)
                    if not reply_role:
                        issues.append(
                            f"Reply role '{self.config.reply_role_name}' not found"
                        )
                    if not admin_role:
                        issues.append(
                            f"Admin role '{self.config.admin_role_name}' not found"
                        )
                else:
                    issues.append(
                        f"Cannot access guild {self.config.guild_id}")
            if issues:
                logger.warning(f"Health check issues: {issues}")
            else:
                logger.info("Health check passed")
        except Exception as e:
            logger.error(f"Health check failed: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        logger.info(f"Member left server: {member.display_name}")
        try:
            async with self.db.get_db() as db:
                async with db.execute(
                        '''
                    SELECT u.id, u.username, u.x_username, u.channel_id, 
                           ts.id as session_id, ts.excel_path, ts.target_replies, 
                           ts.start_date, ts.end_date, COUNT(r.id) as total_replies
                    FROM users u
                    LEFT JOIN tracking_sessions ts ON u.id = ts.user_id AND ts.status = 'active'
                    LEFT JOIN replies r ON ts.id = r.session_id AND r.is_valid = 1
                    WHERE u.discord_id = ?
                    GROUP BY u.id, ts.id
                    LIMIT 1
                ''', (member.id, )) as cursor:
                    user_data = await cursor.fetchone()
            if not user_data:
                logger.info(
                    f"No tracking data found for {member.display_name}")
                return
            admin_channel = await self.get_admin_channel(member.guild)
            if not admin_channel:
                logger.warning(
                    "Admin channel not found, cannot send departure report")
                return
            embed = discord.Embed(
                title="User Left Server - Auto Cleanup",
                description=f"**{user_data['username']}** has left the server",
                color=discord.Color.orange())
            embed.add_field(name="User Info",
                            value=f"@{user_data['x_username']}",
                            inline=True)
            embed.add_field(name="Total Replies",
                            value=str(user_data['total_replies'] or 0),
                            inline=True)
            embed.add_field(name="Daily Target",
                            value=str(user_data['target_replies'] or 'N/A'),
                            inline=True)
            if user_data['start_date'] and user_data['end_date']:
                embed.add_field(
                    name="Period",
                    value=
                    f"{user_data['start_date']} to {user_data['end_date']}",
                    inline=False)
            embed.set_footer(
                text=
                f"Auto-cleanup performed at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
            )
            channel_deleted = False
            if user_data['channel_id']:
                channel = member.guild.get_channel(user_data['channel_id'])
                if channel:
                    try:
                        await channel.delete(
                            reason=f"User {member.display_name} left the server"
                        )
                        channel_deleted = True
                        logger.info(f"Deleted channel: {channel.name}")
                    except Exception as e:
                        logger.warning(f"Failed to delete channel: {e}")
            embed.add_field(name="Channel",
                            value="Deleted" if channel_deleted else
                            "Not found/Already deleted",
                            inline=True)
            excel_sent = False
            if user_data['excel_path'] and os.path.exists(
                    user_data['excel_path']):
                try:
                    filename = f"DEPARTED_{user_data['username']}_{user_data['x_username']}_tracking.xlsx"
                    await admin_channel.send(embed=embed,
                                             file=discord.File(
                                                 user_data['excel_path'],
                                                 filename=filename))
                    excel_sent = True
                    os.remove(user_data['excel_path'])
                except Exception as e:
                    logger.warning(f"Failed to send Excel file: {e}")
            if not excel_sent:
                await admin_channel.send(embed=embed)
            if user_data['session_id']:
                async with self.db.get_db() as db:
                    await db.execute(
                        'UPDATE tracking_sessions SET status = ? WHERE id = ?',
                        ('left_server', user_data['session_id']))
                    await db.commit()
            logger.info(f"Auto-cleanup completed for {member.display_name}")
        except Exception as e:
            logger.error(
                f"Error in auto-cleanup for {member.display_name}: {e}")

    async def get_admin_channel(self, guild):
        try:
            admin_category = discord.utils.get(
                guild.categories, name=self.config.admin_category_name)
            if not admin_category:
                logger.warning(
                    f"Admin category '{self.config.admin_category_name}' not found"
                )
                return None
            admin_channel = discord.utils.get(
                admin_category.channels, name=self.config.admin_channel_name)
            if not admin_channel:
                logger.warning(
                    f"Admin channel '{self.config.admin_channel_name}' not found"
                )
                return None
            return admin_channel
        except Exception as e:
            logger.error(f"Error finding admin channel: {e}")
            return None

    @commands.Cog.listener()
    async def on_member_update(self, before, after):
        """Detect when someone gets the reply role"""
        logger.info(f"Member update detected for {after.display_name}")
        reply_role = discord.utils.get(after.guild.roles,
                                       name=self.config.reply_role_name)
        if not reply_role:
            logger.warning(
                f"Reply role '{self.config.reply_role_name}' not found in guild"
            )
            return
        if reply_role in after.roles and reply_role not in before.roles:
            logger.info(f"{after.display_name} got the reply role!")
            await self.setup_new_reply_user(after)

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return
        if message.author.id in self.onboarding_sessions:
            onboarding_data = self.onboarding_sessions[message.author.id]
            if message.channel.id == onboarding_data.get('channel_id'):
                await self.handle_onboarding_response(message)
                return
        if message.channel.name and message.channel.name.startswith(
                'tracking-'):
            if await self.verify_user_channel(message.author.id,
                                              message.channel.id):
                if message.author.id not in self.onboarding_sessions:
                    await self.handle_reply_submission(message)
            else:
                logger.warning(
                    f"{message.author.display_name} tried to submit in wrong tracking channel"
                )
            return
        await self.process_commands(message)

    async def verify_user_channel(self, discord_id: int,
                                  channel_id: int) -> bool:
        try:
            async with self.db.get_db() as db:
                async with db.execute(
                        '''
                    SELECT channel_id FROM users WHERE discord_id = ?
                ''', (discord_id, )) as cursor:
                    result = await cursor.fetchone()
                    if result and result['channel_id'] == channel_id:
                        return True
                    elif result and result['channel_id'] != channel_id:
                        await db.execute(
                            '''
                            UPDATE users SET channel_id = ? WHERE discord_id = ?
                        ''', (channel_id, discord_id))
                        await db.commit()
                        logger.info(
                            f"Updated channel ID for user {discord_id}: {result['channel_id']} -> {channel_id}"
                        )
                        return True
                    return False
        except Exception as e:
            logger.error(f"Error verifying user channel: {e}")
            return False

    async def setup_new_reply_user(self, member):
        logger.info(f"Setting up new reply user: {member.display_name}")
        guild = member.guild
        category = discord.utils.get(guild.categories,
                                     name=self.config.tracking_category_name)
        if not category:
            try:
                category = await guild.create_category(
                    self.config.tracking_category_name)
                logger.info(
                    f"Created category: {self.config.tracking_category_name}")
            except discord.Forbidden:
                logger.error("Bot lacks permission to create categories")
                try:
                    await member.send(
                        "❌ I couldn't create your tracking channel due to permission issues. Please contact an admin."
                    )
                except:
                    pass
                return
            except Exception as e:
                logger.error(f"Error creating category: {e}")
                return
        overwrites = {
            guild.default_role:
            discord.PermissionOverwrite(read_messages=False,
                                        send_messages=False),
            member:
            discord.PermissionOverwrite(read_messages=True,
                                        send_messages=True,
                                        attach_files=True),
            guild.me:
            discord.PermissionOverwrite(read_messages=True,
                                        send_messages=True,
                                        attach_files=True)
        }
        admin_role = discord.utils.get(guild.roles,
                                       name=self.config.admin_role_name)
        if admin_role:
            overwrites[admin_role] = discord.PermissionOverwrite(
                read_messages=True, send_messages=True, attach_files=True)
        channel_name = f"tracking-{member.display_name.lower().replace(' ', '-')}"
        try:
            channel = await guild.create_text_channel(channel_name,
                                                      category=category,
                                                      overwrites=overwrites)
            logger.info(f"Created channel: {channel.name}")
            await self.db.update_user_channel(member.id, channel.id)
            await self.start_onboarding(member, channel)
        except discord.Forbidden:
            logger.error("Bot lacks permission to create channels")
            try:
                await member.send(
                    "❌ I couldn't create your tracking channel due to permission issues. Please contact an admin."
                )
            except:
                pass
        except Exception as e:
            logger.error(f"Error creating channel: {e}")

    async def start_onboarding(self, member, channel):
        logger.info(
            f"Starting onboarding for {member.display_name} in {channel.name}")
        embed = discord.Embed(
            title="Welcome to Reply Tracking!",
            description=
            f"Hi {member.mention}! I'm here to save you from stress - don't forget to drop those links! The Lord is watching",
            color=0x1DA1F2)
        embed.add_field(
            name="QUICK SETUP GUIDE",
            value=
            "1. Your X Username (without @)\n2. Daily reply target (how many per day)\n3. Start date (I'll create 60 days automatically)",
            inline=False)
        embed.add_field(
            name="Step 1 of 3",
            value="What's your **X (Twitter) username**? (without @)",
            inline=False)
        embed.add_field(
            name="Example",
            value="If your X profile is @johndoe123, just type: `johndoe123`",
            inline=False)
        try:
            await channel.send(embed=embed)
            self.onboarding_sessions[member.id] = {
                'step': 'x_username',
                'channel_id': channel.id,
                'is_setup_channel': True,
                'data': {},
                'created_at': time.time()
            }
            logger.info(f"Onboarding started for {member.display_name}")
        except Exception as e:
            logger.error(f"Error starting onboarding: {e}")

    async def handle_onboarding_response(self, message):
        user_id = message.author.id
        onboarding_data = self.onboarding_sessions[user_id]
        step = onboarding_data['step']
        try:
            if step == 'x_username':
                import re
                username = message.content.strip().replace('@', '')
                if not re.match(r'^[A-Za-z0-9_]{1,15}$', username):
                    embed = discord.Embed(
                        title="Invalid Username",
                        description=
                        "Please enter a valid X username (letters, numbers, underscore only, max 15 characters)",
                        color=0xff0000)
                    await message.reply(embed=embed)
                    return
                onboarding_data['data']['x_username'] = username
                onboarding_data['step'] = 'target_replies'
                embed = discord.Embed(title="Username Saved!",
                                      description=f"X Username: @{username}",
                                      color=0x00ff00)
                embed.add_field(
                    name="Step 2 of 3",
                    value="How many replies do you want to track **per day**?",
                    inline=False)
                embed.add_field(
                    name="Example",
                    value="If you want to track 50 replies daily, type: `50`",
                    inline=False)
                await message.reply(embed=embed)
            elif step == 'target_replies':
                try:
                    target = int(message.content.strip())
                    if target <= 0 or target > 500:
                        embed = discord.Embed(
                            title="Invalid Number",
                            description=
                            "Please enter a number between 1 and 500",
                            color=0xff0000)
                        await message.reply(embed=embed)
                        return
                    onboarding_data['data']['target_replies'] = target
                    onboarding_data['step'] = 'start_date'
                    embed = discord.Embed(
                        title="Daily Target Saved!",
                        description=f"Daily Target: {target} replies",
                        color=0x00ff00)
                    embed.add_field(
                        name="Step 3 of 3",
                        value=
                        "What's your **start date**? (Format: YYYY-MM-DD)",
                        inline=False)
                    embed.add_field(
                        name="Example",
                        value="For March 25th, 2025, type: `2025-03-25`",
                        inline=False)
                    await message.reply(embed=embed)
                except ValueError:
                    embed = discord.Embed(
                        title="Invalid Input",
                        description="Please enter a valid number",
                        color=0xff0000)
                    await message.reply(embed=embed)
                    return
            elif step == 'start_date':
                try:
                    start_date = datetime.strptime(message.content.strip(),
                                                   '%Y-%m-%d').date()
                    if start_date < datetime.now().date():
                        embed = discord.Embed(
                            title="Invalid Date",
                            description="Start date cannot be in the past",
                            color=0xff0000)
                        await message.reply(embed=embed)
                        return
                    end_date = start_date + timedelta(days=60)
                    onboarding_data['data']['start_date'] = start_date
                    onboarding_data['data']['end_date'] = end_date
                    embed = discord.Embed(
                        title="Setup Complete!",
                        description=
                        f"Perfect! Your 60-day tracking period is set up.",
                        color=0x00ff00)
                    embed.add_field(name="Start Date",
                                    value=str(start_date),
                                    inline=True)
                    embed.add_field(name="End Date",
                                    value=str(end_date),
                                    inline=True)
                    embed.add_field(name="Duration",
                                    value="60 days",
                                    inline=True)
                    await message.reply(embed=embed)
                    await self.complete_onboarding(message.author,
                                                   message.channel,
                                                   onboarding_data['data'])
                except ValueError:
                    embed = discord.Embed(
                        title="Invalid Date Format",
                        description=
                        "Please use format YYYY-MM-DD (e.g., 2025-03-25)",
                        color=0xff0000)
                    await message.reply(embed=embed)
                    return
        except Exception as e:
            logger.error(f"Error in onboarding: {e}")
            embed = discord.Embed(
                title="Error",
                description=
                "Something went wrong. Please try again or contact an admin.",
                color=0xff0000)
            await message.reply(embed=embed)

    async def complete_onboarding(self, member, channel, data):
        logger.info(f"Completing onboarding for {member.display_name}")
        try:
            user_id = await self.db.save_user(member.id, member.display_name,
                                              data['x_username'], channel.id)
            session_id = await self.db.create_session(user_id,
                                                      data['target_replies'],
                                                      data['start_date'],
                                                      data['end_date'])
            excel_path = await self.excel_manager.create_excel_template(
                session_id, data, member.display_name)
            if excel_path:
                await self.db.update_session_excel_path(session_id, excel_path)
            embed = discord.Embed(
                title="Setup Complete!",
                description="Your reply tracking system is ready!",
                color=0x00ff00)
            embed.add_field(name="X Username",
                            value=f"@{data['x_username']}",
                            inline=True)
            embed.add_field(name="Daily Target",
                            value=f"{data['target_replies']} replies",
                            inline=True)
            embed.add_field(name="Duration",
                            value="60 days (auto-calculated)",
                            inline=False)
            embed.add_field(
                name="How to Submit",
                value="Just paste your X reply links in this channel!",
                inline=False)
            embed.add_field(
                name="Valid Link Format",
                value=f"https://x.com/{data['x_username']}/status/1234567890",
                inline=False)
            embed.set_footer(
                text=
                "I'll validate each link and update your Excel automatically")
            await channel.send(embed=embed)
            if excel_path and os.path.exists(excel_path):
                await channel.send(
                    "Here's your tracking spreadsheet:",
                    file=discord.File(
                        excel_path,
                        filename=f"{member.display_name}_tracking.xlsx"))
            self.onboarding_sessions.pop(member.id, None)
            logger.info(f"Onboarding completed for {member.display_name}")
        except Exception as e:
            logger.error(f"Error completing onboarding: {e}")
            embed = discord.Embed(
                title="Setup Failed",
                description=
                "Something went wrong during setup. Please contact an admin.",
                color=0xff0000)
            await channel.send(embed=embed)

    async def handle_reply_submission(self, message):
        logger.info(
            f"Reply submission from {message.author.display_name}: {len(message.content)} characters"
        )
        logger.info(f"Message content preview: {message.content[:200]}...")
        urls = extract_urls_bulk_optimized(message.content)
        logger.info(f"Extracted URLs: {len(urls)} found")
        for i, url in enumerate(urls, 1):
            logger.info(f"  URL {i}: {url}")
        user_data = await self.db.get_user_session(message.author.id)
        if not user_data:
            embed = discord.Embed(
                title="No Active Session",
                description=
                "No active tracking session found. Contact an admin if this is a mistake.",
                color=0xff0000)
            await message.reply(embed=embed)
            return
        logger.info(
            f"User session found: {user_data['x_username']}, target: {user_data['target_replies']}"
        )
        start_date = datetime.strptime(user_data['start_date'],
                                       '%Y-%m-%d').date()
        end_date = datetime.strptime(user_data['end_date'], '%Y-%m-%d').date()
        today = datetime.now().date()
        if today < start_date:
            embed = discord.Embed(
                title="Tracking Not Started",
                description=
                f"Tracking hasn't started yet. Start date: {start_date}",
                color=0xff0000)
            await message.reply(embed=embed)
            return
        elif today > end_date:
            embed = discord.Embed(
                title="Tracking Ended",
                description=f"Tracking period has ended. End date: {end_date}",
                color=0xff0000)
            await message.reply(embed=embed)
            return
        if not urls:
            logger.info("No URLs found in message, ignoring")
            return
        valid_urls = []
        invalid_urls = []
        for url in urls:
            is_valid = validate_reply_link(url, user_data['x_username'])
            logger.info(
                f"Validating {url}: {'VALID' if is_valid else 'INVALID'}")
            if is_valid:
                valid_urls.append(url)
            else:
                invalid_urls.append(url)
        logger.info(
            f"Validation results: {len(valid_urls)} valid, {len(invalid_urls)} invalid"
        )
        if not valid_urls and invalid_urls:
            await message.add_reaction("❌")
            embed = discord.Embed(
                title="Invalid Links",
                description=
                f"These links are not from your registered X account (@{user_data['x_username']}) or not proper status links",
                color=0xff0000)
            embed.add_field(
                name="Valid format example:",
                value=
                f"https://x.com/{user_data['x_username']}/status/1234567890",
                inline=False)
            await message.reply(embed=embed)
            return
        existing_count = await self.db.get_daily_reply_count(
            user_data['session_id'], today)
        logger.info(f"Existing replies today: {existing_count}")
        new_total = existing_count + len(valid_urls)
        if new_total > user_data['target_replies']:
            await message.add_reaction("⚠️")
            remaining = user_data['target_replies'] - existing_count
            embed = discord.Embed(title="Daily Limit Exceeded", color=0xffaa00)
            if remaining > 0:
                embed.description = f"You can only submit {remaining} more replies today (target: {user_data['target_replies']})"
            else:
                embed.description = f"You've already reached your daily target of {user_data['target_replies']} replies!"
            await message.reply(embed=embed)
            return
        logger.info(f"Saving {len(valid_urls)} URLs to database...")
        try:
            await self.db.save_replies(user_data['session_id'], today,
                                       valid_urls, existing_count)
            logger.info("Database save completed successfully")
        except Exception as e:
            logger.error(f"Database save failed: {e}")
            await message.add_reaction("❌")
            await message.reply(
                "Failed to save replies to database. Contact an admin.")
            return
        if user_data['excel_path']:
            logger.info(f"Updating Excel file: {user_data['excel_path']}")
            try:
                await self.excel_manager.update_excel_file(
                    user_data['excel_path'], user_data['session_id'], today,
                    valid_urls, user_data['target_replies'],
                    datetime.strptime(user_data['start_date'],
                                      '%Y-%m-%d').date(),
                    user_data['x_username'])
                logger.info("Excel update completed successfully")
            except Exception as e:
                logger.error(f"Excel update failed: {e}")
        await message.add_reaction("✅")
        new_count = existing_count + len(valid_urls)
        embed = discord.Embed(
            title="Replies Logged!",
            description=f"Successfully added {len(valid_urls)} reply(s)",
            color=0x00ff00)
        embed.add_field(
            name="Today's Progress",
            value=f"{new_count}/{user_data['target_replies']} replies",
            inline=True)
        embed.add_field(name="Date",
                        value=today.strftime('%Y-%m-%d'),
                        inline=True)
        if invalid_urls:
            embed.add_field(
                name="Invalid Links",
                value=
                f"{len(invalid_urls)} link(s) rejected (wrong account or format)",
                inline=False)
        progress_percent = (new_count / user_data['target_replies']) * 100
        progress_bar = "█" * int(
            progress_percent / 10) + "░" * (10 - int(progress_percent / 10))
        embed.add_field(name="Progress",
                        value=f"`{progress_bar}` {progress_percent:.1f}%",
                        inline=False)
        await message.reply(embed=embed)
        logger.info(
            f"Reply submission completed: {new_count}/{user_data['target_replies']}"
        )

    @tasks.loop(time=time(9, 0))
    async def daily_reminder(self):
        try:
            today = date.today()
            async with self.db.get_db() as db:
                async with db.execute(
                        '''
                    SELECT u.discord_id, u.channel_id, u.username, ts.target_replies,
                           COALESCE(COUNT(r.id), 0) as todays_replies
                    FROM users u
                    JOIN tracking_sessions ts ON u.id = ts.user_id
                    LEFT JOIN replies r ON ts.id = r.session_id AND r.date = ? AND r.is_valid = 1
                    WHERE ts.status = 'active' AND ts.start_date <= ? AND ts.end_date >= ?
                    GROUP BY u.id, ts.id
                    HAVING todays_replies < ts.target_replies
                ''', (today, today, today)) as cursor:
                    users_to_remind = await cursor.fetchall()
            reminders_sent = 0
            for user_data in users_to_remind:
                try:
                    channel = self.get_channel(user_data['channel_id'])
                    if channel:
                        user = self.get_user(user_data['discord_id'])
                        remaining = user_data['target_replies'] - user_data[
                            'todays_replies']
                        embed = discord.Embed(
                            title="Daily Reminder",
                            description=
                            "Don't forget to submit your reply links!",
                            color=discord.Color.orange())
                        embed.add_field(
                            name="Progress",
                            value=
                            f"{user_data['todays_replies']}/{user_data['target_replies']}"
                        )
                        embed.add_field(name="Remaining",
                                        value=f"{remaining} replies needed")
                        if user:
                            await channel.send(f"{user.mention}", embed=embed)
                        else:
                            await channel.send(embed=embed)
                        reminders_sent += 1
                        await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(
                        f"Failed to send reminder to {user_data['username']}: {e}"
                    )
            logger.info(f"Sent {reminders_sent} daily reminders")
        except Exception as e:
            logger.error(f"Error in daily reminder task: {e}")

    @daily_reminder.before_loop
    async def before_daily_reminder(self):
        await self.wait_until_ready()

    @tasks.loop(hours=1)
    async def cleanup_task(self):
        try:
            current_time = time.time()
            expired_cache = []
            for user_id, cache_data in list(self.user_cache.items()):
                if current_time - cache_data.get('cached_at',
                                                 0) > self.cache_ttl:
                    expired_cache.append(user_id)
            for user_id in expired_cache:
                del self.user_cache[user_id]
            expired_sessions = []
            for user_id, session_data in list(
                    self.onboarding_sessions.items()):
                if current_time - session_data.get('created_at', 0) > 3600:
                    expired_sessions.append(user_id)
            for user_id in expired_sessions:
                del self.onboarding_sessions[user_id]
            if expired_cache or expired_sessions:
                logger.info(
                    f"Cleanup: removed {len(expired_cache)} cache entries, {len(expired_sessions)} sessions"
                )
        except Exception as e:
            logger.error(f"Error in cleanup task: {e}", exc_info=True)

    @cleanup_task.before_loop
    async def before_cleanup(self):
        await self.wait_until_ready()

    async def close(self):
        logger.info("Bot is shutting down...")
        self.daily_reminder.cancel()
        self.cleanup_task.cancel()
        await super().close()
