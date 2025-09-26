import os
import sqlite3
import asyncpg
from typing import Optional, Dict, Any, list
from contextlib import asynccontextmanager
import logging
import asyncio
from datetime import date

logger = logging.getLogger('bot')

class DatabaseManager:
    def __init__(self):
        self.db_type = 'postgresql' if os.getenv('DATABASE_URL') else 'sqlite'
        self.pool = None
        self.db_path = os.getenv('LOCAL_DB_PATH', 'bot_database.db')
        
        # For SQLite connection pooling to prevent "database is locked" errors
        self._sqlite_lock = asyncio.Lock()
        
        if self.db_type == 'postgresql':
            logger.info("Using PostgreSQL database")
        else:
            logger.info("Using SQLite database")
            self._init_sqlite()
    
    async def initialize(self):
        """Alias for init_database to maintain compatibility with original bot code"""
        await self.init_database()
    
    async def init_database(self):
        """Initialize the database connection and tables"""
        if self.db_type == 'postgresql':
            await self._init_postgresql()
        else:
            self._init_sqlite()
    
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
    
    def _init_sqlite(self):
        """Initialize SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create users table
            cursor.execute('''
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
            cursor.execute('''
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
            cursor.execute('''
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
            
            conn.commit()
            conn.close()
            logger.info("SQLite database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize SQLite: {e}")
            raise
    
    @asynccontextmanager
    async def get_db(self):
        """Get database connection with proper concurrency handling"""
        if self.db_type == 'postgresql':
            conn = await self.pool.acquire()
            try:
                yield conn
            finally:
                await self.pool.release(conn)
        else:
            # For SQLite, use a lock to prevent "database is locked" errors
            async with self._sqlite_lock:
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row  # Similar to aiosqlite.Row
                try:
                    yield conn
                finally:
                    conn.close()
    
    async def get_user_session(self, discord_id: int) -> Optional[Dict]:
        """Get user's active session"""
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                query = '''
                    SELECT u.id, u.x_username, ts.id as session_id, ts.target_replies, 
                           ts.start_date, ts.end_date, ts.excel_path
                    FROM users u
                    JOIN tracking_sessions ts ON u.id = ts.user_id
                    WHERE u.discord_id = $1 AND ts.status = 'active'
                    ORDER BY ts.created_at DESC
                    LIMIT 1
                '''
                row = await db.fetchrow(query, discord_id)
            else:
                query = '''
                    SELECT u.id, u.x_username, ts.id as session_id, ts.target_replies, 
                           ts.start_date, ts.end_date, ts.excel_path
                    FROM users u
                    JOIN tracking_sessions ts ON u.id = ts.user_id
                    WHERE u.discord_id = ? AND ts.status = 'active'
                    ORDER BY ts.created_at DESC
                    LIMIT 1
                '''
                cursor = db.execute(query, (discord_id,))
                row = cursor.fetchone()
            
            if row:
                return dict(row) if isinstance(row, dict) else {row.keys()[i]: row[i] for i in range(len(row))}
            return None

    async def save_user(self, discord_id: int, username: str, x_username: str,
                        channel_id: int) -> int:
        """Save user"""
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                # PostgreSQL version with ON CONFLICT
                await db.execute('''
                    INSERT INTO users (discord_id, username, x_username, channel_id, updated_at)
                    VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                    ON CONFLICT (discord_id) DO UPDATE 
                    SET username = $2, x_username = $3, channel_id = $4, updated_at = CURRENT_TIMESTAMP
                ''', discord_id, username, x_username, channel_id)
                
                # Get the user ID
                row = await db.fetchrow('SELECT id FROM users WHERE discord_id = $1', discord_id)
            else:
                # SQLite version with INSERT OR REPLACE
                await db.execute('''
                    INSERT OR REPLACE INTO users (discord_id, username, x_username, channel_id, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (discord_id, username, x_username, channel_id))
                
                # Get the user ID
                cursor = db.execute('SELECT id FROM users WHERE discord_id = ?', (discord_id,))
                row = cursor.fetchone()
            
            if self.db_type == 'postgresql':
                user_id = row['id'] if row else None
            else:
                user_id = row[0] if row else None
            
            if self.db_type != 'postgresql':
                db.commit()
            
            return user_id

    async def create_session(self, user_id: int, target_replies: int,
                             start_date: date, end_date: date) -> int:
        """Create tracking session"""
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                result = await db.fetchval('''
                    INSERT INTO tracking_sessions (user_id, target_replies, start_date, end_date)
                    VALUES ($1, $2, $3, $4)
                    RETURNING id
                ''', user_id, target_replies, start_date, end_date)
            else:
                cursor = db.execute('''
                    INSERT INTO tracking_sessions (user_id, target_replies, start_date, end_date)
                    VALUES (?, ?, ?, ?)
                ''', (user_id, target_replies, start_date, end_date))
                result = cursor.lastrowid
                db.commit()
            
            return result

    async def update_session_excel_path(self, session_id: int,
                                        excel_path: str):
        """Update session with Excel file path"""
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                await db.execute('''
                    UPDATE tracking_sessions 
                    SET excel_path = $1, updated_at = CURRENT_TIMESTAMP 
                    WHERE id = $2
                ''', excel_path, session_id)
            else:
                await db.execute('''
                    UPDATE tracking_sessions 
                    SET excel_path = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE id = ?
                ''', (excel_path, session_id))
                db.commit()

    async def save_replies(self, session_id: int, date_obj: date,
                           urls: List[str], existing_count: int):
        """Save multiple replies"""
        async with self.get_db() as db:
            for idx, url in enumerate(urls):
                try:
                    tweet_id = self._extract_tweet_id_from_url(url)
                    x_username = self._extract_username_from_url(url)
                    
                    if self.db_type == 'postgresql':
                        await db.execute('''
                            INSERT INTO replies (session_id, date, url, x_username_extracted, is_valid, reply_number, tweet_id)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ''', session_id, date_obj.strftime('%Y-%m-%d'), url, 
                             x_username, True, existing_count + idx + 1, tweet_id)
                    else:
                        await db.execute('''
                            INSERT INTO replies (session_id, date, url, x_username_extracted, is_valid, reply_number, tweet_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (session_id, date_obj.strftime('%Y-%m-%d'), url, 
                              x_username, 1, existing_count + idx + 1, tweet_id))
                except Exception as e:
                    logger.error(f"Error saving individual reply {url}: {e}")
                    continue
            
            if self.db_type != 'postgresql':
                db.commit()
            
            logger.info(f"Saved {len(urls)} replies to database for session {session_id}")

    async def update_user_channel(self, discord_id: int, channel_id: int):
        """Update user's channel ID"""
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                await db.execute('''
                    UPDATE users 
                    SET channel_id = $1, updated_at = CURRENT_TIMESTAMP 
                    WHERE discord_id = $2
                ''', channel_id, discord_id)
            else:
                await db.execute('''
                    UPDATE users 
                    SET channel_id = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE discord_id = ?
                ''', (channel_id, discord_id))
                db.commit()
            
            logger.info(f"Updated channel ID for user {discord_id}: {channel_id}")

    async def get_users_with_missing_channels(
            self, guild_member_ids: List[int]) -> List[Dict]:
        """Get users whose channels might be missing"""
        if not guild_member_ids:
            return []
        
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                placeholders = ','.join([f'${i+1}' for i in range(len(guild_member_ids))])
                query = f'''
                    SELECT u.discord_id, u.channel_id, u.username, u.x_username,
                           ts.status as session_status
                    FROM users u
                    JOIN tracking_sessions ts ON u.id = ts.user_id
                    WHERE u.discord_id = ANY($1) 
                    AND ts.status = 'active' 
                    AND u.channel_id IS NOT NULL
                '''
                rows = await db.fetch(query, guild_member_ids)
            else:
                placeholders = ','.join(['?' for _ in guild_member_ids])
                query = f'''
                    SELECT u.discord_id, u.channel_id, u.username, u.x_username,
                           ts.status as session_status
                    FROM users u
                    JOIN tracking_sessions ts ON u.id = ts.user_id
                    WHERE u.discord_id IN ({placeholders}) 
                    AND ts.status = 'active' 
                    AND u.channel_id IS NOT NULL
                '''
                cursor = db.execute(query, guild_member_ids)
                rows = cursor.fetchall()
            
            return [dict(row) if isinstance(row, dict) else {row.keys()[i]: row[i] for i in range(len(row))} for row in rows]

    async def mark_user_left_server(self, discord_id: int):
        """Mark user as having left the server"""
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                await db.execute('''
                    UPDATE tracking_sessions 
                    SET status = 'left_server', updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = (
                        SELECT id FROM users WHERE discord_id = $1
                    )
                ''', discord_id)
            else:
                await db.execute('''
                    UPDATE tracking_sessions 
                    SET status = 'left_server', updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = (
                        SELECT id FROM users WHERE discord_id = ?
                    )
                ''', (discord_id,))
                db.commit()
            
            logger.info(f"Marked user {discord_id} as left server")

    def _extract_tweet_id_from_url(self, url: str) -> Optional[str]:
        """Extract tweet ID from URL"""
        match = re.search(r'/status/(\d+)', url)
        return match.group(1) if match else None

    async def get_daily_reply_count(self, session_id: int,
                                    date_obj: date) -> int:
        """Get count of replies for specific date"""
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                query = '''
                    SELECT COUNT(*) FROM replies 
                    WHERE session_id = $1 AND date = $2 AND is_valid = true
                '''
                row = await db.fetchrow(query, session_id, date_obj)
                count = row[0] if row else 0
            else:
                query = '''
                    SELECT COUNT(*) FROM replies 
                    WHERE session_id = ? AND date = ? AND is_valid = 1
                '''
                cursor = db.execute(query, (session_id, date_obj))
                row = cursor.fetchone()
                count = row[0] if row else 0
            
            return count

    def _extract_username_from_url(self, url: str) -> Optional[str]:
        """Extract username from URL"""
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
    
    # Additional methods for tracking channels (from your original issue)
    async def get_tracking_channel(self, user_id: str) -> Optional[str]:
        """Get the tracking channel for a user (for your original issue)"""
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                result = await db.fetchrow(
                    'SELECT channel_id FROM users WHERE discord_id = $1', 
                    int(user_id)
                )
                return str(result['channel_id']) if result and result['channel_id'] else None
            else:
                cursor = db.execute('SELECT channel_id FROM users WHERE discord_id = ?', (int(user_id),))
                result = cursor.fetchone()
                return str(result[0]) if result and result[0] else None

    async def set_tracking_channel(self, user_id: str, channel_id: str, guild_id: str = None):
        """Set or update the tracking channel for a user"""
        await self.update_user_channel(int(user_id), int(channel_id))

    async def get_all_tracking_channels(self) -> Dict[str, str]:
        """Get all user-channel mappings"""
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                rows = await db.fetch('SELECT discord_id, channel_id FROM users WHERE channel_id IS NOT NULL')
                return {str(row['discord_id']): str(row['channel_id']) for row in rows if row['channel_id']}
            else:
                cursor = db.execute('SELECT discord_id, channel_id FROM users WHERE channel_id IS NOT NULL')
                rows = cursor.fetchall()
                return {str(row[0]): str(row[1]) for row in rows if row[1]}

    async def update_user_data(self, user_id: str, client_username: str, rest_data: str):
        """Update user's submission data"""
        async with self.get_db() as db:
            if self.db_type == 'postgresql':
                # Add the columns if they don't exist (first time setup)
                await db.execute('''
                    ALTER TABLE users 
                    ADD COLUMN IF NOT EXISTS client_username TEXT,
                    ADD COLUMN IF NOT EXISTS rest_data TEXT,
                    ADD COLUMN IF NOT EXISTS submission_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ''')
                
                await db.execute('''
                    INSERT INTO users (discord_id, client_username, rest_data, submission_time) 
                    VALUES ($1, $2, $3, CURRENT_TIMESTAMP) 
                    ON CONFLICT (discord_id) DO UPDATE 
                    SET client_username = $2, rest_data = $3, submission_time = CURRENT_TIMESTAMP
                ''', int(user_id), client_username, rest_data)
            else:
                # Add columns to SQLite if they don't exist
                try:
                    await db.execute('ALTER TABLE users ADD COLUMN client_username TEXT')
                except:
                    pass  # Column might already exist
                try:
                    await db.execute('ALTER TABLE users ADD COLUMN rest_data TEXT')
                except:
                    pass  # Column might already exist
                try:
                    await db.execute('ALTER TABLE users ADD COLUMN submission_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
                except:
                    pass  # Column might already exist
                
                await db.execute('''
                    INSERT OR REPLACE INTO users (discord_id, client_username, rest_data, submission_time) 
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (int(user_id), client_username, rest_data))
                db.commit()
    async def close(self):
        """Close database connections"""
        if self.pool:
            await self.pool.close()
