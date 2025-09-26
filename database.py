import os
import aiosqlite
import asyncpg
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
import logging
import asyncio
from datetime import date

logger = logging.getLogger('bot')

class DatabaseManager:
    def __init__(self, db_path='bot_database.db'):
        self.db_type = 'postgresql' if os.getenv('DATABASE_URL') else 'sqlite'
        self.pool = None
        self.db_path = os.getenv('LOCAL_DB_PATH', db_path)
        
        if self.db_type == 'postgresql':
            logger.info("Using PostgreSQL database")
        else:
            logger.info("Using SQLite database")
    
    async def initialize(self):
        """Initialize the database connection and tables"""
        await self.init_database()
    
    async def init_database(self):
        """Initialize the database connection and tables"""
        if self.db_type == 'postgresql':
            await self._init_postgresql()
        else:
            await self._init_sqlite()
    
    async def _init_postgresql(self):
        """Initialize PostgreSQL database"""
        try:
            self.pool = await asyncpg.create_pool(os.getenv('DATABASE_URL'))
            
            async with self.pool.acquire() as conn:
                # Create users table
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        discord_id BIGINT UNIQUE NOT NULL,
                        username TEXT NOT NULL,
                        x_username TEXT,
                        channel_id BIGINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create tracking_sessions table
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS tracking_sessions (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        target_replies INTEGER DEFAULT 0,
                        start_date DATE,
                        end_date DATE,
                        status TEXT DEFAULT 'active',
                        excel_path TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create replies table
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS replies (
                        id SERIAL PRIMARY KEY,
                        session_id INTEGER REFERENCES tracking_sessions(id) ON DELETE CASCADE,
                        date DATE NOT NULL,
                        url TEXT NOT NULL,
                        x_username_extracted TEXT,
                        is_valid BOOLEAN DEFAULT TRUE,
                        reply_number INTEGER,
                        tweet_id TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create indexes
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_discord_id ON users(discord_id)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON tracking_sessions(user_id)')
                await conn.execute('CREATE INDEX IF NOT EXISTS idx_replies_session_date ON replies(session_id, date)')
                
            logger.info("PostgreSQL database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL: {e}")
            raise
    
    async def _init_sqlite(self):
        """Initialize SQLite database using aiosqlite"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Create users table
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        discord_id INTEGER UNIQUE NOT NULL,
                        username TEXT NOT NULL,
                        x_username TEXT,
                        channel_id INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create tracking_sessions table
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS tracking_sessions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        target_replies INTEGER DEFAULT 0,
                        start_date TEXT,
                        end_date TEXT,
                        status TEXT DEFAULT 'active',
                        excel_path TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create replies table
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS replies (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id INTEGER REFERENCES tracking_sessions(id) ON DELETE CASCADE,
                        date TEXT NOT NULL,
                        url TEXT NOT NULL,
                        x_username_extracted TEXT,
                        is_valid INTEGER DEFAULT 1,
                        reply_number INTEGER,
                        tweet_id TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                await db.commit()
            logger.info("SQLite database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize SQLite: {e}")
            raise
    
    @asynccontextmanager
    async def get_db(self):
        """Get database connection with proper async handling"""
        if self.db_type == 'postgresql':
            conn = await self.pool.acquire()
            try:
                yield conn
            finally:
                await self.pool.release(conn)
        else:
            # Use aiosqlite for SQLite operations
            async with aiosqlite.connect(self.db_path) as conn:
                # Set row factory to get dict-like rows (similar to PostgreSQL)
                conn.row_factory = aiosqlite.Row
                yield conn
    
    # All the methods that your original bot.py calls
    async def get_user_session(self, discord_id: int):
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
            await db.commit()

            async with db.execute('SELECT id FROM users WHERE discord_id = ?',
                                  (discord_id, )) as cursor:
                row = await cursor.fetchone()
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
                    x_username = self._extract_username_from_url(url)
                    
                    await db.execute('''
                        INSERT INTO replies (session_id, date, url, x_username_extracted, is_valid, reply_number, tweet_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (session_id, date_obj.strftime('%Y-%m-%d'), url, 
                          x_username, 1, existing_count + idx + 1, tweet_id))
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
        if not guild_member_ids:
            return []
        
        async with self.get_db() as db:
            placeholders = ','.join('?' * len(guild_member_ids))
            async with db.execute(
                f'''
                SELECT u.discord_id, u.channel_id, u.username, u.x_username
                FROM users u
                JOIN tracking_sessions ts ON u.id = ts.user_id
                WHERE u.discord_id IN ({placeholders}) 
                AND ts.status = 'active' 
                AND u.channel_id IS NOT NULL
            ''', guild_member_ids) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

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

    async def get_total_users_count(self):
        """Get total number of users in database"""
        async with self.get_db() as db:
            async with db.execute('SELECT COUNT(*) as count FROM users') as cursor:
                result = await cursor.fetchone()
                return result['count'] if result else 0

    async def get_active_sessions_count(self):
        """Get number of active sessions"""
        async with self.get_db() as db:
            async with db.execute('SELECT COUNT(*) as count FROM tracking_sessions WHERE status = "active"') as cursor:
                result = await cursor.fetchone()
                return result['count'] if result else 0

    async def get_replies_count_for_date(self, date_obj):
        """Get total replies count for a specific date"""
        async with self.get_db() as db:
            async with db.execute('''
                SELECT COUNT(*) as count FROM replies 
                WHERE date = ? AND is_valid = 1
            ''', (date_obj,)) as cursor:
                result = await cursor.fetchone()
                return result['count'] if result else 0

    async def get_active_users_count_for_date(self, date_obj):
        """Get number of users who submitted replies on a specific date"""
        async with self.get_db() as db:
            async with db.execute('''
                SELECT COUNT(DISTINCT session_id) as count FROM replies 
                WHERE date = ? AND is_valid = 1
            ''', (date_obj,)) as cursor:
                result = await cursor.fetchone()
                return result['count'] if result else 0

    async def get_user_performance_for_date(self, date_obj):
        """Get user performance data for a specific date"""
        async with self.get_db() as db:
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

    async def get_all_tracking_channels(self) -> Dict[str, str]:
        """Get all user-channel mappings"""
        async with self.get_db() as db:
            async with db.execute('SELECT discord_id, channel_id FROM users WHERE channel_id IS NOT NULL') as cursor:
                rows = await cursor.fetchall()
                return {str(row[0]): str(row[1]) for row in rows if row[1]}

    async def get_tracking_channel(self, user_id: str) -> Optional[str]:
        """Get the tracking channel for a user"""
        async with self.get_db() as db:
            async with db.execute('SELECT channel_id FROM users WHERE discord_id = ?', (int(user_id),)) as cursor:
                result = await cursor.fetchone()
                return str(result[0]) if result and result[0] else None

    async def set_tracking_channel(self, user_id: str, channel_id: str, guild_id: str = None):
        """Set or update the tracking channel for a user"""
        await self.update_user_channel(int(user_id), int(channel_id))

    async def update_user_data(self, user_id: str, client_username: str, rest_ str):
        """Update user's submission data"""
        async with self.get_db() as db:
            await db.execute('''
                INSERT OR REPLACE INTO users (discord_id, x_username, updated_at) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (int(user_id), client_username))
            await db.commit()

    async def update_session_status(self, session_id: int, status: str):
        """Update session status in database"""
        async with self.get_db() as db:
            await db.execute(
                'UPDATE tracking_sessions SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                (status, session_id))
            await db.commit()

    async def get_session_replies(self, session_id: int):
        """Get all replies for a session"""
        async with self.get_db() as db:
            async with db.execute('''
                SELECT * FROM replies 
                WHERE session_id = ? 
                ORDER BY date, reply_number
            ''', (session_id,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_user_by_id(self, user_id: int):
        """Get user by ID"""
        async with self.get_db() as db:
            async with db.execute('SELECT * FROM users WHERE id = ?', (user_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_all_active_sessions(self):
        """Get all active sessions"""
        async with self.get_db() as db:
            async with db.execute('SELECT * FROM tracking_sessions WHERE status = "active"') as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_user_all_replies(self, session_id: int):
        """Get all replies for a user session"""
        async with self.get_db() as db:
            async with db.execute('''
                SELECT * FROM replies 
                WHERE session_id = ? AND is_valid = 1
                ORDER BY date, reply_number
            ''', (session_id,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_users_with_url(self, current_user_id: int, url: str):
        """Get other users who have submitted the same URL"""
        async with self.get_db() as db:
            async with db.execute('''
                SELECT DISTINCT u.username 
                FROM replies r
                JOIN tracking_sessions ts ON r.session_id = ts.id
                JOIN users u ON ts.user_id = u.id
                WHERE r.url = ? AND u.discord_id != ?
            ''', (url, current_user_id)) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def get_total_user_replies(self, session_id: int) -> int:
        """Get total replies for a user session"""
        async with self.get_db() as db:
            async with db.execute('''
                SELECT COUNT(r.id) as total_replies
                FROM replies r
                WHERE r.session_id = ? AND r.is_valid = 1
            ''', (session_id,)) as cursor:
                result = await cursor.fetchone()
                return result['total_replies'] if result else 0

    async def get_active_days_count(self, session_id: int) -> int:
        """Get count of active days for a user session"""
        async with self.get_db() as db:
            async with db.execute('''
                SELECT COUNT(DISTINCT r.date) as active_days
                FROM replies r
                WHERE r.session_id = ? AND r.is_valid = 1
            ''', (session_id,)) as cursor:
                result = await cursor.fetchone()
                return result['active_days'] if result else 0

    async def update_session_target_replies(self, session_id: int, new_target: int):
        """Update session target replies"""
        async with self.get_db() as db:
            await db.execute('UPDATE tracking_sessions SET target_replies = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?', 
                          (new_target, session_id))
            await db.commit()

    async def get_user_session_by_status(self, discord_id: int, status: str):
        """Get user session by specific status"""
        async with self.get_db() as db:
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

    async def get_replies_for_multiple_users(self, user_ids: List[int]) -> List[Dict]:
        """Get replies for multiple users"""
        async with self.get_db() as db:
            placeholders = ','.join('?' * len(user_ids))
            async with db.execute(f'''
                SELECT u.username, u.x_username, r.url, r.date, r.reply_number
                FROM replies r
                JOIN tracking_sessions ts ON r.session_id = ts.id
                JOIN users u ON ts.user_id = u.id
                WHERE u.discord_id IN ({placeholders}) AND r.is_valid = 1
                ORDER BY r.url
            ''', user_ids) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def close(self):
        """Close database connections"""
        if self.pool:
            await self.pool.close()
