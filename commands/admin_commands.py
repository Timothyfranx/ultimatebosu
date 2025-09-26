import discord
from discord import app_commands
from discord.ext import commands
import logging
import os
import asyncio
from datetime import datetime
from typing import Optional, List

from utils.excel_manager import CombinedExcelReportGenerator
from utils.duplicate_scanner import AdvancedDuplicateScanner

logger = logging.getLogger(__name__)


class AdminCommands(commands.Cog):
    """Admin-only commands for the Reply Tracker Bot."""

    def __init__(self, bot):
        self.bot = bot

    async def cog_check(self, ctx) -> bool:
        """Check if user has admin role before any command in this cog."""
        try:
            user = ctx.interaction.user if hasattr(
                ctx, 'interaction') else ctx.author
            admin_role_name = self.bot.config.admin_role_name
            has_admin_role = any(role.name == admin_role_name
                                 for role in getattr(user, "roles", []))
            if not has_admin_role:
                logger.warning(
                    f"Access denied for {getattr(user, 'display_name', 'Unknown')}: Missing admin role '{admin_role_name}'"
                )
            return has_admin_role
        except Exception as e:
            logger.error(f"Error in cog_check: {e}")
            return False

    # --- Combined Report ---
    @app_commands.command(
        name="get_all_reports",
        description="[ADMIN] Generate combined Excel report with all user data"
    )
    async def get_all_reports_combined(self, interaction: discord.Interaction):
        """Combines all sheets in one Excel file for all users."""
        await interaction.response.defer()
        try:
            report_generator = CombinedExcelReportGenerator(self.bot.db)
            filepath = await report_generator.generate_combined_report()

            if not filepath or not os.path.exists(filepath):
                embed = discord.Embed(
                    title="No Data Found",
                    description=
                    "No active tracking sessions found to generate report.",
                    color=discord.Color.orange())
                await interaction.followup.send(embed=embed)
                return

            file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
            embed = discord.Embed(
                title="Combined Tracking Report Generated",
                description=
                "All user tracking data compiled into a single Excel file.",
                color=discord.Color.green())
            embed.add_field(name="File Size",
                            value=f"{file_size:.2f} MB",
                            inline=True)
            embed.add_field(name="Generated",
                            value=datetime.now().strftime("%Y-%m-%d %H:%M"),
                            inline=True)
            embed.add_field(
                name="Contents",
                value=
                "📊 Summary Sheet\n👥 Individual User Sheets\n📈 Analytics Sheet",
                inline=False)

            if file_size > 20:  # Discord 25MB limit with buffer
                embed.add_field(
                    name="⚠️ File Too Large",
                    value=
                    "File exceeds Discord limits. Consider date filtering.",
                    inline=False)
                await interaction.followup.send(embed=embed)
            else:
                filename = f"combined_tracking_report_{datetime.now().strftime('%Y%m%d')}.xlsx"
                await interaction.followup.send(embed=embed,
                                                file=discord.File(
                                                    filepath,
                                                    filename=filename))
            try:
                os.remove(filepath)
            except Exception as cleanup_error:
                logger.warning(
                    f"Failed to delete temporary report: {cleanup_error}")

        except Exception as e:
            logger.error(f"Error in get_all_reports_combined: {e}",
                         exc_info=True)
            embed = discord.Embed(
                title="Report Generation Failed",
                description=
                f"Failed to generate combined report. Error: {str(e)}",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed)

    # --- Channel Restoration ---
    @app_commands.command(
        name="restore_channels",
        description="[ADMIN] Manually restore missing tracking channels")
    async def restore_channels(self, interaction: discord.Interaction):
        """Manually trigger channel restoration."""
        await interaction.response.defer()
        try:
            # Get active users from database using async method
            active_users = await self.bot.db.get_users_with_missing_channels([])
            
            missing_channels, existing_channels = [], 0
            for guild in self.bot.guilds:
                # Get all members with reply role
                reply_role = discord.utils.get(
                    guild.roles, name=self.bot.config.reply_role_name)
                if not reply_role:
                    continue
                    
                reply_members = [member for member in guild.members 
                               if not member.bot and reply_role in member.roles]
                
                for member in reply_members:
                    user_data = await self.bot.db.get_user_session(member.id)
                    if user_data:
                        channel_id = await self.bot.db.get_tracking_channel(str(member.id))
                        if channel_id:
                            channel = guild.get_channel(int(channel_id))
                            if not channel:
                                missing_channels.append({
                                    'member': member,
                                    'username': user_data.get('username', member.display_name),
                                    'old_channel_id': channel_id
                                })
                            else:
                                existing_channels += 1

            if not missing_channels:
                embed = discord.Embed(
                    title="All Channels Present",
                    description=
                    f"All {existing_channels} tracking channels are working correctly.",
                    color=discord.Color.green())
                await interaction.followup.send(embed=embed)
                return

            embed = discord.Embed(
                title="Channel Restoration",
                description=
                f"Found {len(missing_channels)} missing channels to restore",
                color=discord.Color.orange())
            embed.add_field(name="Existing Channels",
                            value=str(existing_channels),
                            inline=True)
            embed.add_field(name="Missing Channels",
                            value=str(len(missing_channels)),
                            inline=True)
            missing_list = [
                f"• {info['username']}" for info in missing_channels[:5]
            ]
            if len(missing_channels) > 5:
                missing_list.append(
                    f"• ... and {len(missing_channels) - 5} more")
            embed.add_field(name="Users Affected",
                            value="\n".join(missing_list),
                            inline=False)
            await interaction.followup.send(embed=embed)

            recreated_count = 0
            for channel_info in missing_channels:
                member = channel_info['member']
                username = channel_info['username']
                try:
                    reply_role = discord.utils.get(
                        member.roles, name=self.bot.config.reply_role_name)
                    if not reply_role:
                        continue
                    await self.bot.setup_new_reply_user(member)
                    recreated_count += 1
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(
                        f"Failed to restore channel for {username}: {e}")

            final_embed = discord.Embed(title="Channel Restoration Complete",
                                        color=discord.Color.green())
            final_embed.add_field(name="Channels Recreated",
                                  value=str(recreated_count),
                                  inline=True)
            final_embed.add_field(name="Total Attempted",
                                  value=str(len(missing_channels)),
                                  inline=True)
            success_rate = (recreated_count / len(missing_channels)
                            ) * 100 if missing_channels else 100
            final_embed.add_field(name="Success Rate",
                                  value=f"{success_rate:.1f}%",
                                  inline=True)
            await interaction.followup.send(embed=final_embed)

        except Exception as e:
            logger.error(f"Error in restore_channels: {e}", exc_info=True)
            embed = discord.Embed(
                title="Restoration Failed",
                description=f"Failed to restore channels. Error: {str(e)}",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed)

    # --- Bulk Setup ---
    @app_commands.command(
        name="setup_all_role_holders",
        description="[ADMIN] Set up tracking for ALL users with the reply role"
    )
    async def setup_all_role_holders(self, interaction: discord.Interaction):
        """Bulk setup for all users with the reply role."""
        await interaction.response.defer()
        try:
            guild = interaction.guild
            reply_role = discord.utils.get(
                guild.roles, name=self.bot.config.reply_role_name)
            if not reply_role:
                embed = discord.Embed(
                    title="Role Not Found",
                    description=
                    f"Role '{self.bot.config.reply_role_name}' not found in this server.",
                    color=discord.Color.red())
                await interaction.followup.send(embed=embed)
                return

            role_holders = [
                member for member in reply_role.members if not member.bot
            ]
            if not role_holders:
                embed = discord.Embed(
                    title="No Role Holders",
                    description=
                    f"No users found with the {self.bot.config.reply_role_name} role.",
                    color=discord.Color.red())
                await interaction.followup.send(embed=embed)
                return

            needs_setup, already_setup = [], []
            for member in role_holders:
                user_data = await self.bot.db.get_user_session(member.id)
                if user_data:
                    already_setup.append(member.display_name)
                else:
                    needs_setup.append(member)

            if not needs_setup:
                embed = discord.Embed(
                    title="All Users Already Set Up",
                    description=
                    "All role holders already have active tracking sessions.",
                    color=discord.Color.blue())
                if already_setup:
                    embed.add_field(name="Existing Users",
                                    value="\n".join(already_setup[:10]),
                                    inline=False)
                await interaction.followup.send(embed=embed)
                return

            embed = discord.Embed(
                title="Bulk Setup Starting",
                description=f"Setting up {len(needs_setup)} users...",
                color=discord.Color.blue())
            await interaction.followup.send(embed=embed)

            success_count, failed_users = 0, []
            for member in needs_setup:
                try:
                    await self.bot.setup_new_reply_user(member)
                    success_count += 1
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Failed to setup {member.display_name}: {e}")
                    failed_users.append(member.display_name)

            result_embed = discord.Embed(
                title="Bulk Setup Complete",
                color=discord.Color.green()
                if not failed_users else discord.Color.orange())
            result_embed.add_field(name="Successfully Set Up",
                                   value=str(success_count),
                                   inline=True)
            result_embed.add_field(name="Already Had Sessions",
                                   value=str(len(already_setup)),
                                   inline=True)
            result_embed.add_field(name="Failed",
                                   value=str(len(failed_users)),
                                   inline=True)
            if already_setup:
                result_embed.add_field(name="Existing Users",
                                       value="\n".join(already_setup[:5]),
                                       inline=False)
            if failed_users:
                result_embed.add_field(name="Failed Users",
                                       value="\n".join(failed_users),
                                       inline=False)
            await interaction.followup.send(embed=result_embed)

        except Exception as e:
            logger.error(f"Error in bulk setup: {e}", exc_info=True)
            embed = discord.Embed(
                title="Bulk Setup Failed",
                description=f"Failed to complete bulk setup. Error: {str(e)}",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed)

    # --- Dashboard ---
    @app_commands.command(
        name="dashboard",
        description="[ADMIN] Live dashboard with all user statistics")
    async def dashboard(self, interaction: discord.Interaction):
        """Show comprehensive admin dashboard."""
        await interaction.response.defer()
        try:
            today = datetime.now().date()
            
            # Get statistics using async database methods
            total_users = await self.bot.db.get_total_users_count()
            active_sessions = await self.bot.db.get_active_sessions_count()
            
            # Get today's statistics
            total_replies_today = await self.bot.db.get_replies_count_for_date(today)
            active_today = await self.bot.db.get_active_users_count_for_date(today)
            
            # Get user performance data
            user_performance = await self.bot.db.get_user_performance_for_date(today)
            
            embed = discord.Embed(title="Admin Dashboard",
                                  description=f"Live statistics for {today}",
                                  color=discord.Color.blue())
            embed.add_field(name="Total Users",
                            value=str(total_users or 0),
                            inline=True)
            embed.add_field(name="Active Sessions",
                            value=str(active_sessions or 0),
                            inline=True)
            embed.add_field(name="Active Today",
                            value=str(active_today or 0),
                            inline=True)
            embed.add_field(name="Replies Today",
                            value=str(total_replies_today or 0),
                            inline=True)
            if user_performance:
                avg_completion = sum(
                    perf.get('completion_pct', 0)
                    for perf in user_performance) / len(user_performance)
                embed.add_field(name="Avg Completion",
                                value=f"{avg_completion:.1f}%",
                                inline=True)

            if user_performance:
                top_5 = user_performance[:5]
                top_text = ""
                for perf in top_5:
                    username = perf.get('username', 'Unknown')
                    target = perf.get('target_replies', 0)
                    replies = perf.get('todays_replies', 0)
                    pct = perf.get('completion_pct', 0)
                    status = "✅" if replies >= target else "⏳" if replies > 0 else "❌"
                    top_text += f"{status} **{username}**: {replies}/{target} ({pct}%)\n"
                embed.add_field(name="Top Performers Today",
                                value=top_text or "No data",
                                inline=False)

                inactive = [
                    perf for perf in user_performance
                    if perf.get('todays_replies', 0) == 0
                ]
                if inactive:
                    inactive_text = "\n".join([
                        f"❌ **{perf.get('username', 'Unknown')}**: 0/{perf.get('target_replies', 0)}"
                        for perf in inactive[:5]
                    ])
                    if len(inactive) > 5:
                        inactive_text += f"\n... and {len(inactive) - 5} more"
                    embed.add_field(name="Needs Attention",
                                    value=inactive_text,
                                    inline=False)

            embed.set_footer(
                text=f"Updated: {datetime.now().strftime('%H:%M:%S')}")
            await interaction.followup.send(embed=embed)
        except Exception as e:
            logger.error(f"Error generating dashboard: {e}", exc_info=True)
            embed = discord.Embed(
                title="Dashboard Error",
                description=f"Failed to generate dashboard. Error: {str(e)}",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed)

    # --- You can add more admin commands below as needed, following the same error handling and logging patterns ---

    @app_commands.command(
        name="delete_user_channel",
        description=
        "[ADMIN] Delete user's tracking channel and send Excel to admin channel"
    )
    async def delete_user_channel(self, interaction: discord.Interaction,
                                  member: discord.Member):
        """Delete user's tracking channel and send Excel to admin channel."""
        await interaction.response.defer()
        try:
            user_data = await self.bot.db.get_user_session(member.id)
            
            if not user_data:
                embed = discord.Embed(
                    title="User Not Found",
                    description=
                    f"{member.mention} has no tracking data in the database.",
                    color=discord.Color.red())
                await interaction.followup.send(embed=embed)
                return

            # Delete the channel if it exists
            channel_deleted = False
            channel_id = await self.bot.db.get_tracking_channel(str(member.id))
            if channel_id:
                channel = interaction.guild.get_channel(int(channel_id))
                if channel:
                    try:
                        await channel.delete(
                            reason=
                            f"Deleted by admin {interaction.user.display_name}"
                        )
                        channel_deleted = True
                        logger.info(f"Deleted channel: {channel.name}")
                    except Exception as e:
                        logger.error(f"Failed to delete channel: {e}")

            # Prepare summary for admin channel
            summary_embed = discord.Embed(
                title="User Channel Deleted",
                description=
                f"Tracking channel for **{user_data.get('username', member.display_name)}** has been deleted",
                color=discord.Color.orange())
            summary_embed.add_field(
                name="User",
                value=f"{member.mention} (@{user_data.get('x_username', 'N/A')})",
                inline=True)
            summary_embed.add_field(name="Deleted By",
                                    value=interaction.user.mention,
                                    inline=True)
            summary_embed.add_field(
                name="Channel Deleted",
                value="Yes" if channel_deleted else "No/Already deleted",
                inline=True)
            if user_data.get('session_id') and user_data.get('target_replies'):
                summary_embed.add_field(
                    name="Progress",
                    value=
                    f"{user_data.get('total_replies', 0)} replies submitted",
                    inline=True)
                summary_embed.add_field(name="Daily Target",
                                        value=str(user_data.get('target_replies', 'N/A')),
                                        inline=True)
                summary_embed.add_field(
                    name="Period",
                    value=
                    f"{user_data.get('start_date', 'N/A')} to {user_data.get('end_date', 'N/A')}",
                    inline=True)
            summary_embed.set_footer(
                text=f"Deleted at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

            # Send Excel file to admin channel if it exists
            admin_channel = await self.bot.get_admin_channel(interaction.guild)
            excel_sent = False
            excel_path = user_data.get('excel_path')
            if admin_channel and excel_path and os.path.exists(excel_path):
                try:
                    filename = f"DELETED_{user_data.get('username', member.display_name)}_{user_data.get('x_username', 'N/A')}_tracking.xlsx"
                    await admin_channel.send(embed=summary_embed,
                                             file=discord.File(
                                                 excel_path,
                                                 filename=filename))
                    excel_sent = True
                    os.remove(excel_path)
                except Exception as e:
                    logger.error(f"Error sending Excel to admin channel: {e}")

            if not excel_sent and admin_channel:
                await admin_channel.send(embed=summary_embed)

            # Mark session as deleted in database
            session_id = user_data.get('session_id')
            if session_id:
                await self.bot.db.update_session_status(session_id, 'deleted')

            # Response to the admin who ran the command
            response_embed = discord.Embed(
                title="Channel Deletion Complete",
                description=
                f"Successfully processed deletion for {member.mention}",
                color=discord.Color.green())
            response_embed.add_field(
                name="Channel Deleted",
                value="Yes" if channel_deleted else "No/Not found",
                inline=True)
            response_embed.add_field(
                name="Excel Sent to Admin",
                value="Yes" if excel_sent else "No Excel file found",
                inline=True)
            response_embed.add_field(name="Database Updated",
                                     value="Yes",
                                     inline=True)
            await interaction.followup.send(embed=response_embed)
        except Exception as e:
            logger.error(f"Error in delete_user_channel: {e}")
            embed = discord.Embed(
                title="Deletion Failed",
                description=f"Failed to delete user channel. Error: {str(e)}",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed)

    @app_commands.command(
        name="setup_user",
        description="[ADMIN] Manually set up tracking for a user with the role"
    )
    async def setup_user(self, interaction: discord.Interaction,
                         member: discord.Member):
        """Manually setup tracking for a specific user."""
        await interaction.response.defer()
        try:
            reply_role = discord.utils.get(
                member.roles, name=self.bot.config.reply_role_name)
            if not reply_role:
                embed = discord.Embed(
                    title="User Missing Role",
                    description=
                    f"{member.mention} doesn't have the {self.bot.config.reply_role_name} role.",
                    color=discord.Color.red())
                await interaction.followup.send(embed=embed)
                return

            user_data = await self.bot.db.get_user_session(member.id)
            if user_data:
                embed = discord.Embed(
                    title="User Already Set Up",
                    description=
                    f"{member.mention} already has an active tracking session.",
                    color=discord.Color.red())
                await interaction.followup.send(embed=embed)
                return

            await self.bot.setup_new_reply_user(member)
            embed = discord.Embed(
                title="Manual Setup Complete",
                description=
                f"Successfully created tracking setup for {member.mention}",
                color=discord.Color.green())
            embed.add_field(
                name="Next Steps",
                value=
                "The user should now complete their onboarding in their new channel",
                inline=False)
            await interaction.followup.send(embed=embed)
        except Exception as e:
            logger.error(f"Error in manual setup: {e}")
            embed = discord.Embed(
                title="Setup Failed",
                description=f"Failed to set up user. Error: {str(e)}",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed)

    @app_commands.command(
        name="check_user_status",
        description="[ADMIN] Check setup status of a specific user")
    async def check_user_status(self, interaction: discord.Interaction,
                                member: discord.Member):
        """Check the setup status of a user."""
        await interaction.response.defer()
        try:
            user_data = await self.bot.db.get_user_session(member.id)
            embed = discord.Embed(
                title=f"Status Report for {member.display_name}",
                color=discord.Color.blue())
            reply_role = discord.utils.get(
                member.roles, name=self.bot.config.reply_role_name)
            embed.add_field(name="Has Required Role",
                            value="Yes" if reply_role else "No",
                            inline=True)

            if not user_data:
                embed.add_field(name="Database Record",
                                value="None",
                                inline=True)
                embed.add_field(name="Tracking Session",
                                value="None",
                                inline=True)
                embed.add_field(name="Recommendation",
                                value="Use `/setup_user` to create setup",
                                inline=False)
                embed.color = discord.Color.red()
            else:
                embed.add_field(name="Database Record",
                                value="Exists",
                                inline=True)
                embed.add_field(name="X Username",
                                value=f"@{user_data.get('x_username', 'N/A')}",
                                inline=True)
                embed.add_field(name="Active Session",
                                value="Yes",
                                inline=True)
                embed.add_field(name="Daily Target",
                                value=str(user_data.get('target_replies', 'N/A')),
                                inline=True)
                embed.add_field(
                    name="Period",
                    value=
                    f"{user_data.get('start_date', 'N/A')} to {user_data.get('end_date', 'N/A')}",
                    inline=False)
                embed.color = discord.Color.green()
            await interaction.followup.send(embed=embed)
        except Exception as e:
            logger.error(f"Error checking user status: {e}")
            embed = discord.Embed(
                title="Status Check Failed",
                description=f"Failed to check user status. Error: {str(e)}",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed)

    @app_commands.command(
        name="scan_duplicates",
        description="[ADMIN] Scan users for duplicate link submissions")
    async def scan_duplicates(self,
                              interaction: discord.Interaction,
                              user1: discord.Member,
                              user2: Optional[discord.Member] = None,
                              user3: Optional[discord.Member] = None,
                              user4: Optional[discord.Member] = None,
                              user5: Optional[discord.Member] = None):
        """Scan users for duplicate link submissions."""
        await interaction.response.defer()
        users_to_scan = [user1.id]
        user_mentions = [user1.mention]
        for user in [user2, user3, user4, user5]:
            if user:
                users_to_scan.append(user.id)
                user_mentions.append(user.mention)
        try:
            scanner = AdvancedDuplicateScanner(self.bot.db)
            results = await scanner.scan_users_for_duplicates(users_to_scan)
            if 'error' in results:
                embed = discord.Embed(
                    title="Scan Failed",
                    description=f"Error during scan: {results['error']}",
                    color=discord.Color.red())
                await interaction.followup.send(embed=embed)
                return
            summary = results['summary']
            embed = discord.Embed(
                title="Duplicate Scan Results",
                description=f"Scanned {len(user_mentions)} users",
                color=discord.Color.red()
                if summary['users_with_issues'] > 0 else discord.Color.green())
            embed.add_field(name="Users Scanned",
                            value=summary['total_users_scanned'],
                            inline=True)
            embed.add_field(name="Internal Duplicates",
                            value=summary['total_internal_duplicates'],
                            inline=True)
            embed.add_field(name="Cross-User Duplicates",
                            value=summary['cross_user_duplicates'],
                            inline=True)
            user_results = []
            for user_data in results['users_scanned']:
                status = "CLEAN" if user_data.get('duplicate_count', 0) == 0 else f"{user_data.get('duplicate_count', 0)} DUPLICATES"
                user_results.append(f"**{user_data['username']}**: {status}")
            if user_results:
                embed.add_field(name="Individual Results",
                                value='\n'.join(user_results),
                                inline=False)
            await interaction.followup.send(embed=embed)
            if summary['total_internal_duplicates'] > 0 or summary[
                    'cross_user_duplicates'] > 0:
                report_path = await scanner.generate_duplicate_report_file(
                    results)
                if report_path and os.path.exists(report_path):
                    filename = f"duplicate_report_{datetime.now().strftime('%Y%m%d')}.xlsx"
                    await interaction.followup.send(
                        "Detailed duplicate analysis report:",
                        file=discord.File(report_path, filename=filename))
                    try:
                        os.remove(report_path)
                    except Exception as cleanup_error:
                        logger.warning(
                            f"Failed to delete duplicate report: {cleanup_error}"
                        )
        except Exception as e:
            logger.error(f"Error in scan_duplicates: {e}", exc_info=True)
            embed = discord.Embed(
                title="Scan Error",
                description=
                f"An error occurred during the duplicate scan. Error: {str(e)}",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed)

    @app_commands.command(name="daily_summary",
                          description="[ADMIN] Get today's progress summary")
    async def daily_summary(self, interaction: discord.Interaction):
        """Get today's progress summary (admin only)."""
        await interaction.response.defer()
        try:
            today = datetime.now().date()
            
            # Get today's performance data using async method
            results = await self.bot.db.get_user_performance_for_date(today)
            
            if not results:
                embed = discord.Embed(title="No Active Users",
                                      description="No active users for today.",
                                      color=discord.Color.orange())
                await interaction.followup.send(embed=embed)
                return
            
            embed = discord.Embed(title=f"Daily Summary - {today}",
                                  color=discord.Color.blue())
            completed = partial = none = 0
            summary_text = ""
            for row in results:
                username = row.get('username', 'Unknown')
                x_username = row.get('x_username', 'N/A')
                target_replies = row.get('target_replies', 0)
                todays_replies = row.get('todays_replies', 0)
                if todays_replies == target_replies:
                    emoji = "✅"
                    completed += 1
                elif todays_replies > 0:
                    emoji = "🟡"
                    partial += 1
                else:
                    emoji = "❌"
                    none += 1
                progress_percent = (todays_replies / target_replies
                                    ) * 100 if target_replies else 0
                summary_text += f"{emoji} **{username}** (@{x_username}): {todays_replies}/{target_replies} ({progress_percent:.0f}%)\n"
            # Truncate if too long
            if len(summary_text) > 1024:
                summary_text = summary_text[:1000] + f"... and {len(results) - summary_text[:1000].count('**')} more"
            embed.add_field(name="Today's Performance",
                            value=summary_text or "No data",
                            inline=False)
            embed.add_field(name="✅ Completed",
                            value=str(completed),
                            inline=True)
            embed.add_field(name="🟡 In Progress",
                            value=str(partial),
                            inline=True)
            embed.add_field(name="❌ Not Started", value=str(none), inline=True)
            completion_rate = (completed /
                               len(results)) * 100 if results else 0
            embed.add_field(name="Overall Completion",
                            value=f"{completion_rate:.1f}%",
                            inline=False)
            await interaction.followup.send(embed=embed)
        except Exception as e:
            logger.error(f"Error in daily_summary: {e}", exc_info=True)
            embed = discord.Embed(
                title="Summary Failed",
                description=
                f"Failed to generate daily summary. Error: {str(e)}",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed)

    # Additional database method calls that should be in your DatabaseManager
    # These are the missing methods that would have been in your original bot file
    async def update_session_status(self, session_id: int, status: str):
        """Update session status in database"""
        try:
            async with self.bot.db.get_db() as db:
                await db.execute(
                    'UPDATE tracking_sessions SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                    (status, session_id))
                await db.commit()
        except Exception as e:
            logger.error(f"Error updating session status: {e}")

    async def get_total_users_count(self):
        """Get total number of users in database"""
        try:
            async with self.bot.db.get_db() as db:
                async with db.execute('SELECT COUNT(*) as count FROM users') as cursor:
                    result = await cursor.fetchone()
                    return result['count'] if result else 0
        except Exception as e:
            logger.error(f"Error getting total users count: {e}")
            return 0

    async def get_active_sessions_count(self):
        """Get number of active sessions"""
        try:
            async with self.bot.db.get_db() as db:
                async with db.execute('SELECT COUNT(*) as count FROM tracking_sessions WHERE status = "active"') as cursor:
                    result = await cursor.fetchone()
                    return result['count'] if result else 0
        except Exception as e:
            logger.error(f"Error getting active sessions count: {e}")
            return 0

    async def get_replies_count_for_date(self, date_obj):
        """Get total replies count for a specific date"""
        try:
            async with self.bot.db.get_db() as db:
                async with db.execute('''
                    SELECT COUNT(*) as count FROM replies 
                    WHERE date = ? AND is_valid = 1
                ''', (date_obj,)) as cursor:
                    result = await cursor.fetchone()
                    return result['count'] if result else 0
        except Exception as e:
            logger.error(f"Error getting replies count for date: {e}")
            return 0

    async def get_active_users_count_for_date(self, date_obj):
        """Get number of users who submitted replies on a specific date"""
        try:
            async with self.bot.db.get_db() as db:
                async with db.execute('''
                    SELECT COUNT(DISTINCT session_id) as count FROM replies 
                    WHERE date = ? AND is_valid = 1
                ''', (date_obj,)) as cursor:
                    result = await cursor.fetchone()
                    return result['count'] if result else 0
        except Exception as e:
            logger.error(f"Error getting active users count for date: {e}")
            return 0

    async def get_user_performance_for_date(self, date_obj):
        """Get user performance data for a specific date"""
        try:
            async with self.bot.db.get_db() as db:
                async with db.execute('''
                    SELECT u.username, u.x_username, ts.target_replies,
                           COUNT(r.id) as todays_replies,
                           ROUND((COUNT(r.id) * 100.0 / ts.target_replies), 1) as completion_pct
                    FROM users u
                    JOIN tracking_sessions ts ON u.id = ts.user_id AND ts.status = 'active'
                    LEFT JOIN replies r ON ts.id = r.session_id AND r.date = ? AND r.is_valid = 1
                    WHERE ts.start_date <= ? AND ts.end_date >= ?
                    GROUP BY u.id, ts.id
                    ORDER BY completion_pct DESC, todays_replies DESC
                ''', (date_obj, date_obj, date_obj)) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting user performance for date: {e}")
            return []


async def setup(bot):
    """Required function to add this cog to the bot."""
    await bot.add_cog(AdminCommands(bot))
