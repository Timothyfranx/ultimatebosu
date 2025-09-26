import asyncio
import logging
import os
import sys
from pathlib import Path

# Configure logging for Railway
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout  # Railway captures stdout
)

# Import your components
from config import get_config
from bot import ReplyTrackerBot
from database_v2 import create_database_manager


async def main():
    """Main function optimized for Railway"""
    try:
        config = get_config()

        # Create database manager (will auto-detect PostgreSQL from DATABASE_URL)
        db_manager = create_database_manager()

        # Initialize bot with Railway database
        bot = ReplyTrackerBot(config)
        bot.db = db_manager  # Override with Railway database

        # Create necessary directories
        Path(config.excel_directory).mkdir(exist_ok=True)

        logging.info("Starting Discord Reply Tracker Bot on Railway...")
        logging.info(f"Python version: {sys.version}")
        logging.info(f"Database type: {db_manager.config.db_type.value}")

        async with bot:
            await bot.start(config.discord_token)

    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Railway compatibility
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot shutdown requested")
    except Exception as e:
        logging.error(f"Critical error: {e}", exc_info=True)
        sys.exit(1)
