"""
Discord Reply Tracker Bot - Main Entry Point for Replit
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Configure logging for output visibility
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Import configuration first
from config import get_config

# Start keep-alive server immediately
from keep_alive import keep_alive

keep_alive()

# Import bot components
from bot import ReplyTrackerBot


async def main():
    """Main async function to run the bot"""
    try:
        # Get configuration
        config = get_config()

        # Create necessary directories
        Path(config.excel_directory).mkdir(exist_ok=True)

        # Initialize and run bot
        bot = ReplyTrackerBot(config)

        logging.info("Starting Discord Reply Tracker Bot...")
        logging.info(f"Python version: {sys.version}")
        logging.info(f"Working directory: {os.getcwd()}")

        async with bot:
            await bot.start(config.discord_token)

    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        # Clean shutdown for production
        sys.exit(1)


def run_bot():
    """Run the bot with proper error handling for Replit"""
    try:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("Bot shutdown requested")
    except Exception as e:
        logging.error(f"Critical error in main loop: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run_bot()
