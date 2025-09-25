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

            async with self.bot.db.get_db() as db:
                async with db.execute(
                        '''
                    SELECT COUNT(r.id) as total_replies,
                           COUNT(DISTINCT r.date) as active_days
                    FROM replies r
                    WHERE r.session_id = ? AND r.is_valid = 1
                ''', (user_data['session_id'], )) as cursor:
                    stats = await cursor.fetchone()
                total_replies = stats['total_replies'] if stats else 0
                active_days = stats['active_days'] if stats else 0

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
            if user_data['excel_path'] and os.path.exists(
                    user_data['excel_path']):
                user_display = interaction.user.display_name.replace(' ', '_')
                filename = f"{user_display}_progress_{today.strftime('%Y%m%d')}.xlsx"
                await interaction.followup.send(embed=embed,
                                                file=discord.File(
                                                    user_data['excel_path'],
                                                    filename=filename),
                                                ephemeral=True)
                excel_sent = True
            else:
                logger.warning(
                    f"Excel file not found for user {interaction.user.id}: {user_data.get('excel_path')}"
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
