import discord
from discord import app_commands
from discord.ext import commands
import logging
import os
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)


class UserCommands(commands.Cog):
    """User commands for the Reply Tracker Bot"""

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="progress",
        description="Check your reply tracking progress and get your Excel file"
    )
    async def progress_slash(self, interaction: discord.Interaction):
        """Slash command for checking progress"""
        await interaction.response.defer(ephemeral=True)
        try:
            user_data = await self.bot.db.get_user_session(interaction.user.id)
            if not user_data:
                embed = discord.Embed(
                    title="No Active Session",
                    description=
                    "No active tracking session found. Make sure you have the Light Warriors role!",
                    color=discord.Color.red())
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            # Get stats using async method
            total_replies = await self.bot.db.get_total_user_replies(user_data['session_id'])
            active_days = await self.bot.db.get_active_days_count(user_data['session_id'])

            start_date = datetime.strptime(user_data['start_date'],
                                           '%Y-%m-%d').date()
            end_date = datetime.strptime(user_data['end_date'],
                                         '%Y-%m-%d').date()
            today = date.today()
            total_days = (end_date - start_date).days + 1

            if today < start_date:
                days_elapsed = 0
                status = "Not Started"
            elif today > end_date:
                days_elapsed = total_days
                status = "Period Completed"
            else:
                days_elapsed = (today - start_date).days + 1
                status = "Active"

            expected_replies = days_elapsed * user_data['target_replies']
            completion_rate = (total_replies / expected_replies *
                               100) if expected_replies > 0 else 0
            todays_replies = await self.bot.db.get_daily_reply_count(
                user_data['session_id'], today)

            embed = discord.Embed(
                title="Your Progress Report",
                description=f"Tracking progress for @{user_data['x_username']}",
                color=discord.Color.blue())
            embed.add_field(
                name="Period",
                value=
                f"{user_data['start_date']} to {user_data['end_date']} (60 days)",
                inline=False)
            embed.add_field(name="Daily Target",
                            value=str(user_data['target_replies']),
                            inline=True)
            embed.add_field(name="Status", value=status, inline=True)
            embed.add_field(name="Completion Rate",
                            value=f"{completion_rate:.1f}%",
                            inline=True)
            embed.add_field(name="Total Replies",
                            value=f"{total_replies}/{expected_replies}",
                            inline=True)
            embed.add_field(name="Active Days",
                            value=f"{active_days}/{days_elapsed}",
                            inline=True)
            embed.add_field(
                name="Today's Progress",
                value=f"{todays_replies}/{user_data['target_replies']}",
                inline=True)
            progress_bar = "█" * int(
                completion_rate / 10) + "░" * (10 - int(completion_rate / 10))
            embed.add_field(name="Overall Progress",
                            value=f"`{progress_bar}` {completion_rate:.1f}%",
                            inline=False)

            if completion_rate >= 80:
                embed.color = discord.Color.green()
            elif completion_rate >= 50:
                embed.color = discord.Color.orange()
            else:
                embed.color = discord.Color.red()

            excel_sent = False
            excel_path = user_data.get('excel_path')
            if excel_path and os.path.exists(excel_path):
                user_display = interaction.user.display_name.replace(' ', '_')
                filename = f"{user_display}_progress_{today.strftime('%Y%m%d')}.xlsx"
                await interaction.followup.send(embed=embed,
                                                file=discord.File(
                                                    excel_path,
                                                    filename=filename),
                                                ephemeral=True)
                excel_sent = True
            else:
                logger.warning(
                    f"Excel file not found for user {interaction.user.id}: {excel_path}"
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                await interaction.followup.send(
                    "Excel file not found. Contact an admin.", ephemeral=True)

        except Exception as e:
            logger.error(f"Error in progress command: {e}", exc_info=True)
            embed = discord.Embed(
                title="Error",
                description=
                "Something went wrong getting your progress. Contact an admin.",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed, ephemeral=True)

    # ... (other commands unchanged, type hints can be added similarly)
    @app_commands.command(name="change_target", description="Change your daily reply target")
    async def change_daily_target(self, interaction: discord.Interaction, new_target: int):
            """Allow users to change their daily target"""
            if new_target <= 0 or new_target > 500:
                embed = discord.Embed(
                    title="Invalid Target",
                    description="Daily target must be between 1 and 500.",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            try:
                # Get user's active session using async method
                user_data = await self.bot.db.get_user_session(interaction.user.id)
                if not user_data:
                    embed = discord.Embed(
                        title="No Active Session",
                        description="You don't have an active tracking session.",
                        color=discord.Color.red()
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return

                old_target = user_data['target_replies']
                session_id = user_data['session_id']

                # Update target using async method
                await self.bot.db.update_session_target_replies(session_id, new_target)

                embed = discord.Embed(
                    title="Target Updated",
                    description=f"Daily target changed from {old_target} to {new_target} replies",
                    color=discord.Color.green()
                )
                embed.add_field(name="Note", value="This change applies to all remaining days in your tracking period", inline=False)

                await interaction.followup.send(embed=embed, ephemeral=True)

            except Exception as e:
                logger.error(f"Error changing target: {e}", exc_info=True)
                embed = discord.Embed(
                    title="Update Failed",
                    description="Failed to update daily target.",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="pause_tracking", description="Temporarily pause your tracking (vacation mode)")
    async def pause_tracking(self, interaction: discord.Interaction):
            """Pause tracking session - FIXED VERSION"""
            await interaction.response.defer(ephemeral=True)

            try:
                # Get user's active session using async method
                user_data = await self.bot.db.get_user_session(interaction.user.id)
                if not user_data or user_data.get('status') != 'active':
                    embed = discord.Embed(
                        title="No Active Session",
                        description="You don't have an active tracking session to pause.",
                        color=discord.Color.red()
                    )
                else:
                    session_id = user_data['session_id']
                    
                    # Update the session using async method
                    await self.bot.db.update_session_status(session_id, 'paused')

                    embed = discord.Embed(
                        title="Tracking Paused",
                        description="Your tracking is now paused. Use `/resume_tracking` to continue.",
                        color=discord.Color.orange()
                    )
                    embed.add_field(name="Note", value="You won't receive daily reminders while paused", inline=False)

                await interaction.followup.send(embed=embed, ephemeral=True)

            except Exception as e:
                logger.error(f"Error pausing tracking: {e}", exc_info=True)
                embed = discord.Embed(
                    title="Pause Failed",
                    description=f"Failed to pause tracking: {str(e)}",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="resume_tracking", description="Resume your paused tracking")
    async def resume_tracking(self, interaction: discord.Interaction):
            """Resume tracking session - FIXED VERSION"""
            await interaction.response.defer(ephemeral=True)

            try:
                # Get user's paused session using async method
                user_data = await self.bot.db.get_user_session_by_status(interaction.user.id, 'paused')
                if not user_data:
                    embed = discord.Embed(
                        title="No Paused Session",
                        description="You don't have a paused tracking session to resume.",
                        color=discord.Color.red()
                    )
                else:
                    session_id = user_data['session_id']
                    
                    # Update the session using async method
                    await self.bot.db.update_session_status(session_id, 'active')

                    embed = discord.Embed(
                        title="Tracking Resumed",
                        description="Your tracking is now active again. Welcome back!",
                        color=discord.Color.green()
                    )
                    embed.add_field(name="Note", value="Daily reminders will resume tomorrow", inline=False)

                await interaction.followup.send(embed=embed, ephemeral=True)

            except Exception as e:
                logger.error(f"Error resuming tracking: {e}", exc_info=True)
                embed = discord.Embed(
                    title="Resume Failed",
                    description=f"Failed to resume tracking: {str(e)}",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)

    # Additional methods that would need to be implemented in DatabaseManager
    async def get_total_user_replies(self, session_id: int) -> int:
        """Get total replies for a user session"""
        try:
            async with self.bot.db.get_db() as db:
                async with db.execute('''
                    SELECT COUNT(r.id) as total_replies
                    FROM replies r
                    WHERE r.session_id = ? AND r.is_valid = 1
                ''', (session_id,)) as cursor:
                    result = await cursor.fetchone()
                    return result['total_replies'] if result else 0
        except Exception as e:
            logger.error(f"Error getting total user replies: {e}")
            return 0

    async def get_active_days_count(self, session_id: int) -> int:
        """Get count of active days for a user session"""
        try:
            async with self.bot.db.get_db() as db:
                async with db.execute('''
                    SELECT COUNT(DISTINCT r.date) as active_days
                    FROM replies r
                    WHERE r.session_id = ? AND r.is_valid = 1
                ''', (session_id,)) as cursor:
                    result = await cursor.fetchone()
                    return result['active_days'] if result else 0
        except Exception as e:
            logger.error(f"Error getting active days count: {e}")
            return 0

    async def update_session_target_replies(self, session_id: int, new_target: int):
        """Update session target replies"""
        try:
            async with self.bot.db.get_db() as db:
                await db.execute('UPDATE tracking_sessions SET target_replies = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?', 
                              (new_target, session_id))
                await db.commit()
        except Exception as e:
            logger.error(f"Error updating session target replies: {e}")
            raise

    async def get_user_session_by_status(self, discord_id: int, status: str):
        """Get user session by specific status"""
        try:
            async with self.bot.db.get_db() as db:
                async with db.execute('''
                    SELECT u.id, u.username, u.x_username, u.channel_id,
                           ts.id as session_id, ts.target_replies, ts.start_date, 
                           ts.end_date, ts.excel_path, ts.status
                    FROM users u
                    JOIN tracking_sessions ts ON u.id = ts.user_id
                    WHERE u.discord_id = ? AND ts.status = ?
                    ORDER BY ts.created_at DESC
                    LIMIT 1
                ''', (discord_id, status)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting user session by status: {e}")
            return None


async def setup(bot):
    """Required function to add this cog to the bot"""
    await bot.add_cog(UserCommands(bot))
