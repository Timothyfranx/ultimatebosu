import asyncio
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from discord.ext import commands
from config import Config
from bot import ReplyTrackerBot

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """Main entry point for the bot"""
    logger.info("Starting bot initialization...")
    
    # Initialize config
    config = Config()
    
    # Create directories if they don't exist
    excel_dir = Path(config.excel_directory)
    excel_dir.mkdir(exist_ok=True)
    logger.info(f"Excel directory: {excel_dir}")
    
    # Check if running on Railway (has DATABASE_URL)
    is_on_railway = bool(os.getenv('DATABASE_URL'))
    if is_on_railway:
        logger.info("Detected Railway environment - using PostgreSQL")
    else:
        logger.info("Using local SQLite database")
    
    # Initialize bot
    bot = ReplyTrackerBot(config)
    
    try:
        # Start the bot
        logger.info("Starting bot connection...")
        await bot.start(config.discord_token)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error during bot execution: {e}", exc_info=True)
    finally:
        logger.info("Closing bot...")
        await bot.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot shutdown interrupted by user")
    except Exception as e:
        logger.error(f"Error running bot: {e}", exc_info=True)
