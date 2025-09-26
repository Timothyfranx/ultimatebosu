import os
import logging
from dataclasses import dataclass
from typing import Optional

# Configure logging for Replit
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Only console output on Replit
    ]
)

@dataclass
class BotConfig:
    """Configuration management for Replit environment with PostgreSQL/SQLite support"""

    # Required settings
    discord_token: str
    guild_id: Optional[int] = None

    # Role and channel names
    reply_role_name: str = "Light Warriors"
    admin_role_name: str = "Admin"
    tracking_category_name: str = "Light Tracking"
    admin_category_name: str = "✧ Dictators"
    admin_channel_name: str = "innerchambers"

    # Database configuration - Updated for PostgreSQL/SQLite compatibility
    database_url: Optional[str] = None  # For PostgreSQL (Railway)
    sqlite_database_path: str = "reply_tracker.db"  # For SQLite (local)
    
    # Excel and file paths
    excel_directory: str = "excel_files"

    # Bot settings
    default_tracking_days: int = 60
    max_daily_target: int = 500
    reminder_hour: int = 9

    # Performance settings for Replit
    max_urls_per_message: int = 30  # Lower for Replit limits
    bulk_processing_threshold: int = 15
    cache_ttl_seconds: int = 300

    @classmethod
    def from_environment(cls) -> 'BotConfig':
        """Load configuration from Replit environment variables"""

        # Get required token
        discord_token = os.getenv('DISCORD_TOKEN')
        if not discord_token:
            raise ValueError("DISCORD_TOKEN environment variable is required")

        # Get optional guild ID
        guild_id = None
        if os.getenv('GUILD_ID'):
            try:
                guild_id = int(os.getenv('GUILD_ID'))
            except ValueError:
                logging.warning("Invalid GUILD_ID format, ignoring")

        # Get database configuration
        database_url = os.getenv('DATABASE_URL')  # PostgreSQL for Railway
        sqlite_database_path = os.getenv('SQLITE_DATABASE_PATH', 'reply_tracker.db')

        return cls(
            discord_token=discord_token,
            guild_id=guild_id,
            reply_role_name=os.getenv('REPLY_ROLE_NAME', 'Light Warriors'),
            admin_role_name=os.getenv('ADMIN_ROLE_NAME', 'Admin'),
            tracking_category_name=os.getenv('TRACKING_CATEGORY', 'Light Tracking'),
            admin_category_name=os.getenv('ADMIN_CATEGORY', '✧ Dictators'),
            admin_channel_name=os.getenv('ADMIN_CHANNEL', 'innerchambers'),
            database_url=database_url,
            sqlite_database_path=sqlite_database_path,
            excel_directory=os.getenv('EXCEL_DIRECTORY', 'excel_files'),
        )

    @property
    def is_postgresql(self) -> bool:
        """Check if using PostgreSQL database"""
        return bool(self.database_url)

    @property
    def is_sqlite(self) -> bool:
        """Check if using SQLite database"""
        return not self.is_postgresql

    @property
    def database_path(self) -> str:
        """Get the appropriate database path/URL"""
        if self.is_postgresql:
            return self.database_url
        else:
            return self.sqlite_database_path

    def validate(self) -> bool:
        """Validate critical configuration"""
        if not self.discord_token:
            logging.error("Discord token is missing")
            return False

        if len(self.discord_token) < 50:
            logging.error("Discord token appears invalid (too short)")
            return False

        logging.info("Configuration validation passed")
        return True

    def log_config(self):
        """Log current configuration (without sensitive data)"""
        logging.info("Bot Configuration:")
        logging.info(f"  Guild ID: {self.guild_id or 'All servers'}")
        logging.info(f"  Reply Role: {self.reply_role_name}")
        logging.info(f"  Admin Role: {self.admin_role_name}")
        logging.info(f"  Tracking Category: {self.tracking_category_name}")
        logging.info(f"  Admin Category: {self.admin_category_name}")
        logging.info(f"  Database Type: {'PostgreSQL' if self.is_postgresql else 'SQLite'}")
        logging.info(f"  Database Path: {self.database_path}")
        logging.info(f"  Excel Directory: {self.excel_directory}")

# Create an alias so main.py can import Config
Config = BotConfig

def get_config() -> BotConfig:
    """Get validated configuration"""
    config = BotConfig.from_environment()

    if not config.validate():
        raise RuntimeError("Configuration validation failed")

    config.log_config()
    return config
