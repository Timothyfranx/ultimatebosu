import aiosqlite
import logging
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any
from datetime import date, datetime

logger = logging.getLogger(__name__)


class DatabaseManager:

    def __init__(self, db_path: str = "reply_tracker.db"):
        self.db_path = db_path
        self._initialized = False

    async def initialize(self):
        """Initialize database with proper constraints and indexes"""
        if self._initialized:
            return

        async with aiosqlite.connect(self.db_path) as db:
            # Enable foreign keys
            await db.execute("PRAGMA foreign_keys = ON")

            # Users table
            await db.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_id INTEGER UNIQUE NOT NULL,
                    username TEXT NOT NULL,
                    x_username TEXT NOT NULL,
                    channel_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP
                )
            ''')

            # Tracking sessions
            await db.execute('''
                CREATE TABLE IF NOT EXISTS tracking_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    target_replies INTEGER NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    status TEXT DEFAULT 'active',
                    excel_path TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            ''')

            # Replies table with tweet_id
            await db.execute('''
                CREATE TABLE IF NOT EXISTS replies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    date DATE NOT NULL,
                    url TEXT NOT NULL,
                    x_username_extracted TEXT,
                    is_valid BOOLEAN DEFAULT TRUE,
                    reply_number INTEGER,
                    tweet_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES tracking_sessions (id)
                )
            ''')

            # Create indexes for better performance
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_users_discord_id ON users(discord_id)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_sessions_user_status ON tracking_sessions(user_id, status)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_replies_session_date ON replies(session_id, date)"
            )

            await db.commit()

        self._initialized = True
        logger.info("Database initialized successfully")

    @asynccontextmanager
    async def get_db(self):
        """Get database connection with proper cleanup"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA foreign_keys = ON")
            db.row_factory = aiosqlite.Row
            yield db

    async def get_user_session(self, discord_id: int) -> Optional[Dict]:
        """Get user's active session"""
        async with self.get_db() as db:
            async with db.execute(
                    '''
                SELECT u.id, u.x_username, ts.id as session_id, ts.target_replies, 
                       ts.start_date, ts.end_date, ts.excel_path
                FROM users u
                JOIN tracking_sessions ts ON u.id = ts.user_id
                WHERE u.discord_id = ? AND ts.status = 'active'
                ORDER BY ts.created_at DESC
                LIMIT 1
            ''', (discord_id, )) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def save_user(self, discord_id: int, username: str, x_username: str,
                        channel_id: int) -> int:
        """Save user"""
        async with self.get_db() as db:
            await db.execute(
                '''
                INSERT OR REPLACE INTO users (discord_id, username, x_username, channel_id, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (discord_id, username, x_username, channel_id))

            async with db.execute('SELECT id FROM users WHERE discord_id = ?',
                                  (discord_id, )) as cursor:
                row = await cursor.fetchone()
                await db.commit()
                return row[0] if row else None

    async def create_session(self, user_id: int, target_replies: int,
                             start_date: date, end_date: date) -> int:
        """Create tracking session"""
        async with self.get_db() as db:
            cursor = await db.execute(
                '''
                INSERT INTO tracking_sessions (user_id, target_replies, start_date, end_date)
                VALUES (?, ?, ?, ?)
            ''', (user_id, target_replies, start_date, end_date))

            session_id = cursor.lastrowid
            await db.commit()
            return session_id

    async def update_session_excel_path(self, session_id: int,
                                        excel_path: str):
        """Update session with Excel file path"""
        async with self.get_db() as db:
            await db.execute(
                'UPDATE tracking_sessions SET excel_path = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                (excel_path, session_id))
            await db.commit()

    async def save_replies(self, session_id: int, date_obj: date,
                           urls: List[str], existing_count: int):
        """Save multiple replies"""
        async with self.get_db() as db:
            for idx, url in enumerate(urls):
                try:
                    tweet_id = self._extract_tweet_id_from_url(url)
                    await db.execute(
                        '''
                        INSERT INTO replies (session_id, date, url, x_username_extracted, is_valid, reply_number, tweet_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''',
                        (
                            session_id,
                            date_obj.strftime('%Y-%m-%d'),
                            url,
                            self._extract_username_from_url(url),
                            1,  # is_valid = True
                            existing_count + idx + 1,
                            tweet_id))
                except Exception as e:
                    logger.error(f"Error saving individual reply {url}: {e}")
                    continue
            await db.commit()
            logger.info(
                f"Saved {len(urls)} replies to database for session {session_id}"
            )

    async def update_user_channel(self, discord_id: int, channel_id: int):
        """Update user's channel ID"""
        async with self.get_db() as db:
            await db.execute(
                '''
                UPDATE users SET channel_id = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE discord_id = ?
            ''', (channel_id, discord_id))
            await db.commit()
            logger.info(
                f"Updated channel ID for user {discord_id}: {channel_id}")

    async def get_users_with_missing_channels(
            self, guild_member_ids: List[int]) -> List[Dict]:
        """Get users whose channels might be missing"""
        async with self.get_db() as db:
            placeholders = ','.join('?' * len(guild_member_ids))
            async with db.execute(
                    f'''
                SELECT u.discord_id, u.channel_id, u.username, u.x_username,
                       ts.status as session_status
                FROM users u
                JOIN tracking_sessions ts ON u.id = ts.user_id
                WHERE u.discord_id IN ({placeholders}) 
                AND ts.status = 'active' 
                AND u.channel_id IS NOT NULL
            ''', guild_member_ids) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def mark_user_left_server(self, discord_id: int):
        """Mark user as having left the server"""
        async with self.get_db() as db:
            await db.execute(
                '''
                UPDATE tracking_sessions 
                SET status = 'left_server', updated_at = CURRENT_TIMESTAMP
                WHERE user_id = (
                    SELECT id FROM users WHERE discord_id = ?
                )
            ''', (discord_id, ))
            await db.commit()
            logger.info(f"Marked user {discord_id} as left server")

    def _extract_tweet_id_from_url(self, url: str) -> Optional[str]:
        """Extract tweet ID from URL"""
        import re
        match = re.search(r'/status/(\d+)', url)
        return match.group(1) if match else None

    async def get_daily_reply_count(self, session_id: int,
                                    date_obj: date) -> int:
        """Get count of replies for specific date"""
        async with self.get_db() as db:
            async with db.execute(
                    '''
                SELECT COUNT(*) FROM replies 
                WHERE session_id = ? AND date = ? AND is_valid = 1
            ''', (session_id, date_obj)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    def _extract_username_from_url(self, url: str) -> Optional[str]:
        """Extract username from URL"""
        import re
        patterns = [
            r'https?://(?:www\.)?(?:twitter\.com|x\.com)/([^/\?]+)(?:/status/\d+|/\d+)',
            r'https?://(?:www\.)?(?:twitter\.com|x\.com)/([^/\?]+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                username = match.group(1).lower()
                if username not in [
                        'home', 'search', 'notifications', 'messages', 'i',
                        'explore', 'settings'
                ]:
                    return username
        return None
