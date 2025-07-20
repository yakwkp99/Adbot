import asyncio
import logging
import sqlite3
import json
import os
import time
import random
import pyfiglet
import shutil
from termcolor import colored
from colorama import init
import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, quote

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, 
    BotCommand, InputMediaPhoto, InputMediaVideo, InputMediaDocument
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, 
    MessageHandler, filters, ContextTypes
)
from telegram.helpers import escape_markdown
from telegram.error import TelegramError, BadRequest

from telethon import TelegramClient, events, functions
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError,
    PhoneNumberInvalidError, FloodWaitError, ChatAdminRequiredError,
    UserAlreadyParticipantError, InviteHashExpiredError,
    UserBannedInChannelError, ChannelPrivateError,
    ChatWriteForbiddenError
)
from telethon.tl.functions.messages import ForwardMessagesRequest
from telethon.tl.functions.channels import GetForumTopicsRequest
from telethon.tl.types import (
    Channel, Chat, User, PeerChannel, PeerChat, PeerUser, ForumTopic
)

# Read custom DB and session names from environment variables
DB_NAME = os.environ.get("ADBOT_DB", "adbot.db")
SESSION_NAME = os.environ.get("ADBOT_SESSION", "userbot_session")
SUPPORT_USER_IDS = [7759982300]  # Example support user


CRITICAL_SETTINGS_KEYS = [
    "bot_token", "admin_user_id", "subscription_start", "subscription_duration"
]

# Protect these tables entirely (if any)
CRITICAL_TABLES = []


def show_banner():
    init()
    term_width = shutil.get_terminal_size().columns

    title = pyfiglet.figlet_format("AEGIS", font="slant")
    subtitle = "@Distroying"

    def center_ascii(text):
        return "\n".join(line.center(term_width) for line in text.splitlines())

    def framed_text(text):
        border = "+-" + "-".join(["-"] * len(text)) + "-+"
        content = "| " + " ".join(text) + " |"
        return f"{border}\n{content}\n{border}"

    print(colored(center_ascii(title), "cyan", attrs=["bold"]))
    print(colored(center_ascii(framed_text(subtitle)), "magenta", attrs=["bold"]))

# Database setup
class Database:
    def __init__(self, db_path: str = DB_NAME):
        self.db_path = db_path
        self.init_database()
    
    def fetch_one(self, query, params=()):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()
        conn.close()
        return result
    
    def init_database(self):
        """Initialize the database with required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Userbot session table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS userbot_session (
                id INTEGER PRIMARY KEY,
                api_id INTEGER,
                api_hash TEXT,
                phone_number TEXT,
                session_string TEXT,
                is_active BOOLEAN DEFAULT 0,
                last_activity TIMESTAMP,
                forward_mode TEXT DEFAULT 'show',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute("PRAGMA table_info(userbot_session)")
        columns = [col[1] for col in cursor.fetchall()]
        if "forward_mode" not in columns:
            cursor.execute("ALTER TABLE userbot_session ADD COLUMN forward_mode TEXT DEFAULT 'show'")
        
        # Groups and forums table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY,
                chat_id INTEGER UNIQUE,
                title TEXT,
                username TEXT,
                chat_type TEXT,
                is_selected BOOLEAN DEFAULT 1,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Forwarding messages table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS forwarding_messages (
                id INTEGER PRIMARY KEY,
                message_link TEXT,
                channel_id INTEGER,
                message_id INTEGER,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Trace log table (for live trace, auto-purged after 1 hour)
        
        # System settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS afk_replies (
                user_id INTEGER PRIMARY KEY,
                last_replied_at TIMESTAMP
            )
        ''')
        
        # Logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY,
                level TEXT,
                message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_joins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_link TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute("DROP TABLE IF EXISTS forward_trace")
        cursor.execute('''
            CREATE TABLE forward_trace (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_title TEXT,
                group_link TEXT,
                chat_id INTEGER,
                message_id INTEGER,
                forwarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute("SELECT value FROM settings WHERE key = 'per_message_delay'")
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
                ('per_message_delay', '30', datetime.now())
            )

            # Forwarded cache to prevent duplication
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS forwarded_cycle_cache (
                chat_id INTEGER,
                message_hash TEXT,
                cycle_id TEXT,
                PRIMARY KEY (chat_id, message_hash, cycle_id)
            )
        ''')

        # State persistence
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS forwarding_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        cursor.execute("SELECT value FROM settings WHERE key = 'group_join_enabled'")
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
                ('group_join_enabled', 'on', datetime.now())
            )

        conn.commit()
        conn.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """Execute a query and return results (only for SELECT)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        query_type = query.strip().upper()
        if query_type.startswith("SELECT"):
            results = cursor.fetchall()
        else:
            conn.commit()
            results = []

        conn.close()
        return results
    
    def get_forwarding_state(self, key: str, default: str = None) -> Optional[str]:
        result = self.execute_query("SELECT value FROM forwarding_state WHERE key = ?", (key,))
        return result[0][0] if result else default

    def set_forwarding_state(self, key: str, value: str):
        self.execute_query(
            "INSERT OR REPLACE INTO forwarding_state (key, value) VALUES (?, ?)",
            (key, value)
        )

    def was_forwarded_this_cycle(self, chat_id: int, message_hash: str, cycle_id: str) -> bool:
        result = self.execute_query(
            "SELECT 1 FROM forwarded_cycle_cache WHERE chat_id = ? AND message_hash = ? AND cycle_id = ?",
            (chat_id, message_hash, cycle_id)
        )
        return bool(result)

    def mark_forwarded_this_cycle(self, chat_id: int, message_hash: str, cycle_id: str):
        self.execute_query(
            "INSERT OR IGNORE INTO forwarded_cycle_cache (chat_id, message_hash, cycle_id) VALUES (?, ?, ?)",
            (chat_id, message_hash, cycle_id)
        )

    def get_afk_last_reply_time(self, user_id: int) -> Optional[datetime]:
        result = self.fetch_one("SELECT last_replied_at FROM afk_replies WHERE user_id = ?", (user_id,))
        return datetime.fromisoformat(result[0]) if result else None

    def set_afk_last_reply_time(self, user_id: int, reply_time: datetime):
        self.execute_query(
            "INSERT OR REPLACE INTO afk_replies (user_id, last_replied_at) VALUES (?, ?)",
            (user_id, reply_time.isoformat())
        )
    
    def get_setting(self, key: str, default: str = None) -> str:
        """Get a setting value."""
        result = self.execute_query("SELECT value FROM settings WHERE key = ?", (key,))
        return result[0][0] if result else default
    
    def set_setting(self, key: str, value: str):
        """Set a setting value."""
        self.execute_query(
            "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            (key, value, datetime.now())
        )
    
    def log_event(self, level: str, message: str):
        """Log an event to the database."""
        self.execute_query(
            "INSERT INTO logs (level, message) VALUES (?, ?)",
            (level, message)
        )

class UserbotManager:
    def __init__(self, database: Database):
        self.db = database
        self.client: Optional[TelegramClient] = None
        self.is_authenticated = False
        self.session_file = os.environ.get("ADBOT_SESSION", "userbot_session")

    
    async def add_userbot(self, api_id: int, api_hash: str, phone_number: str) -> Tuple[bool, str]:
        """Add a new userbot session."""
        try:
            # Remove existing session
            await self.remove_userbot()
            
            # Create new client
            self.client = TelegramClient(self.session_file, api_id, api_hash)
            await self.client.connect()
            
            # Send code request
            await self.client.send_code_request(phone_number)
            
            # Store session info
            self.db.execute_query(
                "INSERT OR REPLACE INTO userbot_session (id, api_id, api_hash, phone_number, is_active) VALUES (1, ?, ?, ?, 0)",
                (api_id, api_hash, phone_number)
            )
            
            return True, "Code sent! Please provide the OTP code."
            
        except Exception as e:
            self.db.log_event("ERROR", f"Failed to add userbot: {str(e)}")
            return False, f"Error: {str(e)}"
        
    async def restore_session(self):
        """Restore userbot session from database and session file on bot startup."""
        session_data = self.db.execute_query(
            "SELECT api_id, api_hash, phone_number, is_active FROM userbot_session WHERE id = 1"
        )
        if not session_data:
            return  # No session to restore

        api_id, api_hash, phone_number, is_active = session_data[0]
        if not is_active:
            return  # Userbot was not logged in previously

        try:
            self.client = TelegramClient(self.session_file, api_id, api_hash)
            await self.client.connect()
            if await self.client.is_user_authorized():
                self.is_authenticated = True
                await self.start_monitoring()
                print("[Userbot] Restored previous session and connected.")
            else:
                print("[Userbot] Session file found but not authorized. Please re-login.")
        except Exception as e:
            print(f"[Userbot] Could not auto-restore userbot: {e}")
    
    async def verify_code(self, phone_number: str, code: str, password: str = None) -> Tuple[bool, str]:
        """Verify OTP code and complete authentication."""
        try:
            if not self.client:
                return False, "No active session found."

            try:
                # Step 1: Try signing in with phone and code
                await self.client.sign_in(phone_number, code)
            except SessionPasswordNeededError:
                if not password:
                    return False, "Two-factor authentication required. Please provide your password."
                # Step 2: Sign in using only the password (after code is accepted)
                await self.client.sign_in(password=password)

            # Final check
            if await self.client.is_user_authorized():
                self.is_authenticated = True
                self.db.execute_query(
                    "UPDATE userbot_session SET is_active = 1, last_activity = ? WHERE id = 1",
                    (datetime.now(),)
                )
                await self.start_monitoring()
                print("[Userbot] Authenticated and monitoring started!")
                return True, "✅ Userbot authenticated successfully!"
            else:
                return False, "Authentication failed."

        except PhoneCodeInvalidError:
            return False, "Invalid verification code."
        except Exception as e:
            self.db.log_event("ERROR", f"Verification failed: {str(e)}")
            return False, f"Error: {str(e)}"
    
    async def remove_userbot(self) -> bool:
        """Remove the current userbot session."""
        try:
            if self.client:
                await self.client.disconnect()
                self.client = None
            
            # Clear database
            self.db.execute_query("DELETE FROM userbot_session WHERE id = 1")
            self.db.execute_query("DELETE FROM groups")
            
            self.is_authenticated = False
            
            # Remove session file
            import os
            session_files = [f for f in os.listdir('.') if f.startswith(self.session_file)]
            for file in session_files:
                os.remove(file)
            
            return True
            
        except Exception as e:
            self.db.log_event("ERROR", f"Failed to remove userbot: {str(e)}")
            return False
    
    async def start_monitoring(self):
        """Start monitoring userbot status and handle AFK auto-reply."""
        if not self.client or not self.is_authenticated:
            return

        @self.client.on(events.NewMessage(incoming=True))
        async def handler(event):
            # Update last activity
            
            self.db.execute_query(
                "UPDATE userbot_session SET last_activity = ? WHERE id = 1",
                (datetime.now(),)
            )

            if event.is_private and not event.out:
                afk_enabled = self.db.get_setting("afk_enabled", "off")
                afk_message = self.db.get_setting("afk_message", "")
                owner_id_str = self.db.get_setting("admin_user_id", "")
                OWNER_ID = int(owner_id_str) if owner_id_str.isdigit() else None
                sender_id = event.sender_id

                if (
                    afk_enabled == "on"
                    and OWNER_ID
                    and sender_id != OWNER_ID
                    and afk_message
                ):
                    last_reply_time = self.db.get_afk_last_reply_time(sender_id)
                    now = datetime.now()

                    if not last_reply_time or (now - last_reply_time).total_seconds() >= 7200:
                        await event.reply(afk_message)
                        self.db.set_afk_last_reply_time(sender_id, now)
    

    async def get_status(self) -> Dict:
        """Get current userbot status."""
        if not self.client or not self.is_authenticated:
            return {
                "status": "❌ Disconnected",
                "last_activity": "Never",
                "user_info": "Not authenticated"
            }
        
        try:
            # Get user info
            me = await self.client.get_me()
            
            # Get last activity
            result = self.db.execute_query(
                "SELECT last_activity FROM userbot_session WHERE id = 1"
            )
            last_activity = result[0][0] if result else "Unknown"
            
            return {
                "status": "✅ Connected",
                "last_activity": last_activity,
                "user_info": f"@{me.username}" if me.username else me.first_name
            }
            
        except Exception as e:
            return {
                "status": "❌ Error",
                "last_activity": "Unknown",
                "user_info": f"Error: {str(e)}"
            }
    
    async def fetch_groups(self) -> Dict:
        """Fetch all groups and forums the userbot is member of, and count new vs existing."""
        if not self.client or not self.is_authenticated:
            return {"groups": [], "new": 0, "existing": 0, "total": 0}

        try:
            groups = []
            new_count = 0
            existing_count = 0
            seen_ids = set()

            async for dialog in self.client.iter_dialogs():
                entity = dialog.entity
                title = dialog.title
                username = getattr(entity, "username", None)

                # Determine type
                if isinstance(entity, Chat):
                    chat_type = "basic_group"
                elif isinstance(entity, Channel):
                    if getattr(entity, "broadcast", False):
                        chat_type = "channel"
                    elif getattr(entity, "megagroup", False):
                        chat_type = "forum" if getattr(entity, "forum", False) else "supergroup"
                    else:
                        chat_type = "unknown"
                else:
                    continue  # skip if not group or channel

                chat_id = entity.id
                if chat_id in seen_ids:
                    continue  # avoid duplicates from dialogs pointing to same chat
                seen_ids.add(chat_id)

                chat_info = {
                    "id": chat_id,
                    "title": title,
                    "username": username,
                    "type": chat_type
                }
                groups.append(chat_info)

                # Check if already in database
                exists = self.db.fetch_one("SELECT 1 FROM groups WHERE chat_id = ?", (chat_id,))
                if exists:
                    existing_count += 1
                else:
                    new_count += 1
                    self.db.execute_query(
                        "INSERT INTO groups (chat_id, title, username, chat_type) VALUES (?, ?, ?, ?)",
                        (chat_id, title, username, chat_type)
                    )

            return {
                "groups": groups,
                "new": new_count,
                "existing": existing_count,
                "total": new_count + existing_count
            }

        except Exception as e:
            self.db.log_event("ERROR", f"Failed to fetch groups: {str(e)}")
            return {"groups": [], "new": 0, "existing": 0, "total": 0}
    
    async def join_group(self, group_link: str) -> Tuple[bool, str]:
        """Join a group using invite link or username."""
        if not self.client or not self.is_authenticated:
            return False, "Userbot not authenticated."
        
        try:
            # Parse different link formats
            if group_link.startswith('@'):
                entity = await self.client.get_entity(group_link)
            elif 'joinchat' in group_link:
                # Extract invite hash
                invite_hash = group_link.split('/')[-1]
                entity = await self.client.get_entity(f"https://t.me/joinchat/{invite_hash}")
            elif 't.me/' in group_link:
                # Extract username
                username = group_link.split('/')[-1]
                entity = await self.client.get_entity(f"@{username}")
            else:
                return False, "Invalid group link format."
            
            # Join the group
            await self.client(functions.channels.JoinChannelRequest(entity))
            
            # Add to database
            self.db.execute_query(
                "INSERT OR REPLACE INTO groups (chat_id, title, username, chat_type, is_selected) VALUES (?, ?, ?, ?, 1)",
                (entity.id, entity.title, getattr(entity, 'username', None), 'channel' if hasattr(entity, 'broadcast') else 'group')
            )
            
            return True, f"✅ Successfully joined {entity.title}"
            
        except UserAlreadyParticipantError:
            return False, "Already a member of this group."
        except InviteHashExpiredError:
            return False, "Invite link has expired."
        except ChatAdminRequiredError:
            return False, "Admin rights required to join this group."
        except FloodWaitError as e:
            return False, f"Rate limited. Please wait {e.seconds} seconds."
        except Exception as e:
            self.db.log_event("ERROR", f"Failed to join group: {str(e)}")
            return False, f"Error: {str(e)}"

class ForwardingManager:
    def __init__(self, database: Database, userbot_manager: UserbotManager, bot: 'TelegramBot'):
        self.db = database
        self.userbot = userbot_manager
        self.bot = bot
        self.is_running = False
        self.is_paused = False
        self.forwarding_task = None
        self.current_cycle_id = None  # ⏱ Tracked per cycle (from DB)

        self.stats = {
            "total_forwarded": 0,
            "last_forward_time": None,
            "next_forward_time": None,
            "errors": 0
        }

        self.forward_index = 0
        self.forwarded_groups_set = set()

        # 🚀 Load duration settings
        self.hold_duration = int(self.db.get_setting("hold_duration", "15"))
        self.cycle_length = int(self.db.get_setting("cycle_length", "120"))

        # 🔁 Restore forwarding index
        try:
            self.forward_index = int(self.db.get_forwarding_state("forward_index", "0"))
        except Exception:
            self.forward_index = 0

        # 🌗 Restore phase and timestamp
        self.current_phase = self.db.get_forwarding_state("current_phase", "forwarding")
        phase_time_str = self.db.get_forwarding_state("phase_start_time")
        try:
            self.phase_start_time = datetime.fromisoformat(phase_time_str) if phase_time_str else datetime.now()
        except Exception:
            self.phase_start_time = datetime.now()

        # 🌐 Restore last cycle ID (deduplication scope)
        self.current_cycle_id = self.db.get_forwarding_state("current_cycle_id", None)

    def compute_message_hash(self, message) -> str:
        """Compute a hash of the message's content to detect duplicates across channels."""
        base = ""

        if message.message:
            base += message.message

        if message.media:
            media_repr = str(message.media)
            base += media_repr

        return hashlib.sha256(base.encode('utf-8')).hexdigest()

    async def get_group_topic_ids(self, chat_id: int) -> List[int]:
        """Get all available open forum topics for a chat"""
        try:
            # First, verify this is actually a forum
            try:
                entity = await self.userbot.client.get_entity(chat_id)
                if not (getattr(entity, "megagroup", False) and getattr(entity, "forum", False)):
                    self.db.log_event("INFO", f"Chat {chat_id} is not a forum group")
                    return []
            except Exception as e:
                self.db.log_event("ERROR", f"Failed to get entity for {chat_id}: {e}")
                return []

            # Try different approaches to get forum topics
            open_topic_ids = []
            
            # Method 1: Try GetForumTopicsRequest with different parameters
            try:
                result = await self.userbot.client(GetForumTopicsRequest(
                    channel=chat_id,
                    offset_date=None,
                    offset_id=0,
                    offset_topic=0,
                    limit=100
                ))
                
                self.db.log_event("INFO", f"GetForumTopicsRequest returned {len(result.topics)} topics for {chat_id}")
                
                for topic in result.topics:
                    if not isinstance(topic, ForumTopic):
                        continue
                    
                    topic_id = getattr(topic, "id", None)
                    topic_title = getattr(topic, "title", "Unknown")
                    is_closed = getattr(topic, "closed", False)
                    is_hidden = getattr(topic, "hidden", False)
                    
                    self.db.log_event("INFO", f"Topic {topic_id} ({topic_title}): closed={is_closed}, hidden={is_hidden}")
                    
                    if not is_closed and not is_hidden and topic_id:
                        open_topic_ids.append(topic_id)
                        
            except Exception as e:
                self.db.log_event("ERROR", f"GetForumTopicsRequest failed for {chat_id}: {e}")
            
            # Method 2: If no topics found, try getting recent messages to find topic IDs
            if not open_topic_ids:
                try:
                    self.db.log_event("INFO", f"Trying to get topics from recent messages for {chat_id}")
                    messages = await self.userbot.client.get_messages(chat_id, limit=50)
                    
                    found_topics = set()
                    for msg in messages:
                        if hasattr(msg, 'reply_to') and msg.reply_to:
                            if hasattr(msg.reply_to, 'forum_topic') and msg.reply_to.forum_topic:
                                topic_id = getattr(msg.reply_to, 'reply_to_top_id', None)
                                if topic_id and topic_id not in found_topics:
                                    found_topics.add(topic_id)
                                    self.db.log_event("INFO", f"Found topic {topic_id} from recent messages")
                    
                    open_topic_ids.extend(list(found_topics))
                    
                except Exception as e:
                    self.db.log_event("ERROR", f"Failed to get topics from messages for {chat_id}: {e}")
            
            # Method 3: If still no topics, try common topic IDs (1 is usually General)
            if not open_topic_ids:
                self.db.log_event("WARNING", f"No topics found via API, trying common topic IDs for {chat_id}")
                # Try common topic IDs - forums usually start with 1 (General topic)
                test_topics = [1, 2, 3, 4, 5]
                for test_id in test_topics:
                    try:
                        # Try to send a test message to see if topic exists and is writable
                        await self.userbot.client(ForwardMessagesRequest(
                            from_peer=chat_id,  # Use same chat as source for test
                            id=[1],  # Use message ID 1 as test
                            to_peer=chat_id,
                            top_msg_id=test_id,
                            drop_author=True
                        ))
                        # If we get here, topic exists and is writable
                        open_topic_ids.append(test_id)
                        self.db.log_event("INFO", f"Confirmed topic {test_id} exists and is writable in {chat_id}")
                    except Exception as e:
                        err_str = str(e).lower()
                        if "topic_closed" in err_str:
                            self.db.log_event("INFO", f"Topic {test_id} exists but is closed in {chat_id}")
                        elif "invalid peer" in err_str or "message not found" in err_str:
                            self.db.log_event("INFO", f"Topic {test_id} doesn't exist in {chat_id}")
                        else:
                            self.db.log_event("INFO", f"Topic {test_id} test failed in {chat_id}: {e}")
            
            # Remove duplicates and sort
            open_topic_ids = sorted(list(set(open_topic_ids)))
            
            self.db.log_event("INFO", f"Final result: {len(open_topic_ids)} open topics found for {chat_id}: {open_topic_ids}")
            return open_topic_ids
            
        except Exception as e:
            self.stats["errors"] += 1
            self.db.log_event("ERROR", f"Complete failure getting topics for {chat_id}: {str(e)}")
            return []


    async def forward_to_forum_group(self, message, chat_id: int, group_title: str, group_link: str) -> bool:
        """Handle forwarding to a forum group with forward_mode support"""
        try:
            forward_mode = self.bot.get_forward_mode()  # ✅ Fixed scope

            topic_ids = await self.get_group_topic_ids(chat_id)
            if not topic_ids:
                self.db.log_event("WARNING", f"No valid forum topics found in {group_title}")
                return False

            for i, topic_id in enumerate(topic_ids):
                try:
                    if forward_mode == "show":
                        await self.userbot.client(ForwardMessagesRequest(
                            from_peer=message.chat_id,
                            id=[message.id],
                            to_peer=chat_id,
                            top_msg_id=topic_id
                        ))
                    else:
                        # Hide mode – native send
                        me = await self.userbot.client.get_me()
                        has_premium = getattr(me, "premium", False)

                        if message.media:
                            file = await message.download_media()
                            await self.userbot.client.send_file(
                                chat_id,
                                file,
                                caption=message.message,
                                formatting_entities=message.entities,
                                reply_to=topic_id
                            )
                        elif message.text:
                            await self.userbot.client.send_message(
                                entity=chat_id,
                                message=message.message,
                                formatting_entities=message.entities,
                                reply_to=topic_id
                            )

                        if not has_premium and message.text and "🪄" in message.text:
                            print("⚠️ Premium emoji may be downgraded (non-premium account)")

                    self.db.execute_query("DELETE FROM forward_trace WHERE forwarded_at < ?", 
                                        (datetime.now() - timedelta(hours=1),))
                    self.db.execute_query(
                        "INSERT INTO forward_trace (group_title, group_link, chat_id, message_id, forwarded_at) VALUES (?, ?, ?, ?, ?)",
                        (group_title, group_link, chat_id, message.id, datetime.now(timezone.utc).isoformat())
                    )
                    self.stats["total_forwarded"] += 1
                    self.stats["last_forward_time"] = datetime.now()
                    self.forwarded_groups_set.add(chat_id)
                    return True

                except Exception as e:
                    self.db.log_event("ERROR", f"Failed to forward to topic {topic_id} in {group_title}: {e}")
                    continue

            self.db.log_event("WARNING", f"Failed to forward to any topic in forum {group_title}")
            return False

        except Exception as e:
            self.stats["errors"] += 1
            self.db.log_event("ERROR", f"General failure in forum forwarding for {group_title}: {e}")
            return False

    
    async def add_forwarding_message(self, message_link: str) -> Tuple[bool, str]:
        """Add a forwarding message after validating it exists."""
        try:
            # Enforce 2-message limit
            existing = self.db.execute_query("SELECT COUNT(*) FROM forwarding_messages WHERE is_active = 1")
            if existing[0][0] >= 2: 
                return False, "Maximum 2 forwarding messages allowed."

            # Parse link
            channel_id, message_id = self.parse_message_link(message_link)
            if not channel_id or not message_id:
                return False, "Invalid message link format."

            # Validate via userbot client
            if not self.userbot.client or not self.userbot.is_authenticated:
                return False, "Userbot not connected."

            message = await self.userbot.client.get_messages(channel_id, ids=message_id)
            if not message:
                return False, "❌ Message not found. Check the link or visibility."

            # Insert into DB
            self.db.execute_query(
                "INSERT INTO forwarding_messages (message_link, channel_id, message_id) VALUES (?, ?, ?)",
                (message_link, channel_id, message_id)
            )
            return True, "✅ Forwarding message added successfully!"

        except Exception as e:
            self.db.log_event("ERROR", f"Failed to add forwarding message: {str(e)}")
            return False, f"Error: {str(e)}"
    
    def parse_message_link(self, link: str) -> Tuple[Optional[int], Optional[int]]:
        """Parse Telegram message link to extract channel and message ID."""
        try:
            # Pattern: https://t.me/channel_name/message_id
            pattern = r't\.me/([^/]+)/(\d+)'
            match = re.search(pattern, link)
            
            if match:
                channel_name = match.group(1)
                message_id = int(match.group(2))
                return channel_name, message_id
            
            return None, None
            
        except Exception:
            return None, None
    
    def remove_forwarding_message(self, message_id: int) -> bool:
        """Remove a forwarding message."""
        try:
            self.db.execute_query(
                "UPDATE forwarding_messages SET is_active = 0 WHERE id = ?",
                (message_id,)
            )
            return True
        except Exception:
            return False
    
    def get_forwarding_messages(self) -> List[Dict]:
        """Get all active forwarding messages."""
        results = self.db.execute_query(
            "SELECT id, message_link, channel_id, message_id FROM forwarding_messages WHERE is_active = 1"
        )
        
        return [
            {
                "id": row[0],
                "link": row[1],
                "channel_id": row[2],
                "message_id": row[3]
            }
            for row in results
        ]
    
    def get_selected_groups(self) -> List[Dict]:
        """Get groups selected for forwarding."""
        results = self.db.execute_query(
            "SELECT chat_id, title, username, chat_type FROM groups WHERE is_selected = 1"
        )
        
        return [
            {
                "chat_id": row[0],
                "title": row[1],
                "username": row[2],
                "type": row[3]
            }
            for row in results
        ]
    
    async def start_forwarding(self) -> Tuple[bool, str]:
        # Check subscription status
        active, status_msg = self.bot.get_subscription_status()
        if not active:
            return False, status_msg  # Don’t start if subscription is not active
        # Check other preconditions
        if not self.userbot.is_authenticated:
            return False, "❌ Userbot not authenticated."

        messages = self.get_forwarding_messages()
        if not messages:
            return False, "❌ No forwarding messages configured."

        groups = self.get_selected_groups()
        if not groups:
            return False, "❌ No groups selected for forwarding."

        if self.is_running:
            return False, "ℹ️ Forwarding is already running."

        self.is_running = True
        self.is_paused = False

        # 🚀 3. Start forwarding loop
        self.forwarding_task = asyncio.create_task(self.forwarding_loop())

        return True, "✅ Forwarding started successfully!"
    
    def stop_forwarding(self) -> Tuple[bool, str]:
        """Stop the forwarding process."""
        if not self.is_running:
            return False, "ℹ️ Forwarding is not running."
        
        self.is_running = False
        self.is_paused = False
        
        if self.forwarding_task:
            self.forwarding_task.cancel()
        
        return True, "⏹️ Forwarding stopped."
    
    def pause_forwarding(self) -> Tuple[bool, str]:
        """Pause the forwarding process."""
        if not self.is_running:
            return False, "ℹ️ Forwarding is not running."
        
        if self.is_paused:
            return False, "ℹ️ Forwarding is already paused."
        
        self.is_paused = True
        return True, "⏸️ Forwarding paused."
    
    def resume_forwarding(self) -> Tuple[bool, str]:
        """Resume the forwarding process."""
        if not self.is_running:
            return False, "ℹ️ Forwarding is not running."
        
        if not self.is_paused:
            return False, "ℹ️ Forwarding is not paused."
        
        self.is_paused = False

        # ✅ Clear next_forward_time since the cycle loop now controls timing
        try:
            self.stats["next_forward_time"] = None
        except Exception as e:
            self.db.log_event("ERROR", f"Failed to reset next_forward_time on resume: {str(e)}")

        return True, "▶️ Forwarding resumed."
    
    async def forwarding_loop(self):
        """Main time-based ad forwarding cycle with HOLD/ACTIVE phases."""
        try:
            # (Optional) Reset state at the start of a clean run
            self.phase_start_time = datetime.now()
            self.current_phase = "forwarding"
            print("[Cycle] 🚀 Entering FORWARDING phase.")

            # 💾 Save initial state
            self.db.set_forwarding_state("phase_start_time", self.phase_start_time.isoformat())
            self.db.set_forwarding_state("current_phase", self.current_phase)
            self.db.set_forwarding_state("forward_index", str(self.forward_index))

            while self.is_running:
                now = datetime.now()

                # Check subscription status
                active, _ = self.bot.get_subscription_status()
                if not active:
                    self.db.log_event("INFO", "Subscription expired. Stopping forwarding loop.")
                    self.is_running = False
                    return

                # Refresh group list every cycle
                all_groups = self.get_selected_groups()
                if not all_groups:
                    self.db.log_event("WARNING", "No groups selected. Forwarding loop halted.")
                    self.is_running = False
                    return

                # Load settings dynamically
                self.cycle_length = int(self.db.get_setting("cycle_length", "120"))
                self.hold_duration = int(self.db.get_setting("hold_duration", "15"))
                delay = max(15, int(self.db.get_setting("per_message_delay", "30")))

                if self.current_phase == "forwarding":
                    elapsed = (now - self.phase_start_time).total_seconds()
                    if elapsed >= self.cycle_length * 60:
                        self.current_phase = "holding"
                        self.phase_start_time = now
                        print("[Cycle] ⏸ Entering HOLD phase.")

                        # 💾 Save phase switch
                        self.db.set_forwarding_state("phase_start_time", self.phase_start_time.isoformat())
                        self.db.set_forwarding_state("current_phase", self.current_phase)
                        continue

                    # Forward next group
                    await self.forward_next_group(all_groups)
                    await asyncio.sleep(delay)

                elif self.current_phase == "holding":
                    elapsed = (now - self.phase_start_time).total_seconds()
                    if elapsed >= self.hold_duration * 60:
                        self.current_phase = "forwarding"
                        self.phase_start_time = now
                        print("[Cycle] 🔁 Resuming FORWARDING phase.")

                        # 💾 Save phase switch
                        self.db.set_forwarding_state("phase_start_time", self.phase_start_time.isoformat())
                        self.db.set_forwarding_state("current_phase", self.current_phase)
                        continue

                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            print("[Cycle] 🔻 Forwarding loop cancelled.")
        except Exception as e:
            self.db.log_event("ERROR", f"Critical forwarding loop failure: {e}")
        finally:
            self.is_running = False
    
    async def forward_next_group(self, all_groups: List[Dict]):
        """Forward one message to the next group, checking deduplication scoped to current cycle ID."""
        if not self.userbot.client or not self.userbot.is_authenticated:
            return

        messages = self.get_forwarding_messages()
        if not messages:
            return

        total_groups = len(all_groups)
        attempts = 0

        while attempts < total_groups:
            self.forward_index = self.forward_index % total_groups
            target_group = all_groups[self.forward_index]
            self.forward_index += 1
            attempts += 1

            self.db.set_forwarding_state("forward_index", str(self.forward_index))

            chat_id = target_group["chat_id"]
            group_title = target_group["title"]
            username = target_group.get("username")
            group_link = f"@{username}" if username else "N/A"

            for msg in messages:
                try:
                    tg_msg = await self.userbot.client.get_messages(msg["channel_id"], ids=msg["message_id"])
                    if not tg_msg:
                        continue

                    msg_hash = self.compute_message_hash(tg_msg)

                    # Deduplication for this cycle
                    if self.db.was_forwarded_this_cycle(chat_id, msg_hash, self.current_cycle_id):
                        print(f"[DUPLICATE] Skipping message already sent in current cycle to {chat_id}")
                        continue

                    # Detect if group is a forum
                    try:
                        entity = await self.userbot.client.get_entity(chat_id)
                        is_forum = getattr(entity, "megagroup", False) and getattr(entity, "forum", False)
                    except Exception as e:
                        self.stats["errors"] += 1
                        self.db.log_event("ERROR", f"Forum detection failed for group {chat_id}: {e}")
                        continue

                    if is_forum:
                        success = await self.forward_to_forum_group(tg_msg, chat_id, group_title, group_link)
                        if success:
                            self.db.mark_forwarded_this_cycle(chat_id, msg_hash, self.current_cycle_id)
                            return
                        continue  # Try next group

                    # Non-forum group — apply forward_mode logic
                    forward_mode = self.bot.get_forward_mode()
                    sent_msg = None

                    if forward_mode == "show":
                        result = await self.userbot.client.forward_messages(chat_id, tg_msg)
                        sent_msg = result[0] if isinstance(result, list) else result
                    else:
                        me = await self.userbot.client.get_me()
                        has_premium = getattr(me, "premium", False)
                        if tg_msg.media:
                            file_path = await tg_msg.download_media()
                            sent_msg = await self.userbot.client.send_file(
                                chat_id,
                                file_path,
                                caption=tg_msg.message,
                                formatting_entities=tg_msg.entities
                            )
                        else:
                            sent_msg = await self.userbot.client.send_message(
                                chat_id,
                                message=tg_msg.message,
                                formatting_entities=tg_msg.entities
                            )
                        if not has_premium and tg_msg.text and "🪄" in tg_msg.text:
                            print("⚠️ Premium emoji may be downgraded (non-premium account)")

                    if sent_msg:
                        self.db.execute_query(
                            "INSERT INTO forward_trace (group_title, group_link, chat_id, message_id, forwarded_at) VALUES (?, ?, ?, ?, ?)",
                            (group_title, group_link, chat_id, sent_msg.id, datetime.now(timezone.utc).isoformat())
                        )
                        self.db.mark_forwarded_this_cycle(chat_id, msg_hash, self.current_cycle_id)
                        self.stats["total_forwarded"] += 1
                        self.stats["last_forward_time"] = datetime.now()
                        self.forwarded_groups_set.add(chat_id)

                        print(f"[FORWARD] ✅ Sent message {sent_msg.id} to {group_title} ({chat_id})")
                        return

                except FloodWaitError as e:
                    print(f"[WAIT] Sleeping for {e.seconds}s due to rate limit.")
                    await asyncio.sleep(e.seconds)
                    return
                except (ChatWriteForbiddenError, ChatAdminRequiredError):
                    self.stats["errors"] += 1
                    self.db.log_event("ERROR", f"No permission to write to group {group_title}")
                    break
                except Exception as e:
                    self.stats["errors"] += 1
                    self.db.log_event("ERROR", f"Failed to forward to {group_title}: {e}")
                    break

        print("[FORWARD] ❌ All groups skipped or duplicates — nothing sent this round.")

    async def forward_messages(self):
        if not self.userbot.client or not self.userbot.is_authenticated:
            self.db.log_event("ERROR", "Userbot not authenticated.")
            return

        messages = self.get_forwarding_messages()
        if not messages:
            self.db.log_event("ERROR", "No active forwarding messages.")
            return

        groups = self.get_selected_groups()
        if not groups:
            self.db.log_event("ERROR", "No selected groups for forwarding.")
            return

        dialogs = await self.userbot.client.get_dialogs()
        groups_to_forward = [g for g in groups if g["chat_id"] not in self.forwarded_groups_set]
        print(f"[Forwarding] Starting cycle with {len(groups_to_forward)} unvisited groups.")

        for message_info in messages:
            try:
                message = await self.userbot.client.get_messages(
                    message_info["channel_id"],
                    ids=message_info["message_id"]
                )
                if not message:
                    self.db.log_event("ERROR", f"Message ID {message_info['message_id']} not found in channel {message_info['channel_id']}")
                    continue

                for group in groups_to_forward:
                    now = datetime.now()
                    elapsed = (now - self.phase_start_time).total_seconds()
                    if self.current_phase == "forwarding" and elapsed >= self.cycle_length * 60:
                        print(f"[Forwarding] ⏳ Time limit of {self.cycle_length} min reached. Exiting early.")
                        return

                    chat_id = group["chat_id"]
                    group_title = group["title"]
                    username = group.get("username")
                    group_link = username if username else None

                    try:
                        # Determine if this is a forum with better error handling
                        is_forum = False
                        try:
                            dialog = next((d for d in dialogs if d.id == chat_id), None)
                            entity = dialog.entity if dialog else await self.userbot.client.get_entity(chat_id)
                            is_forum = getattr(entity, "megagroup", False) and getattr(entity, "forum", False)
                            self.db.log_event("INFO", f"Group {group_title} (ID: {chat_id}) is_forum: {is_forum}")
                        except Exception as e:
                            self.db.log_event("ERROR", f"Failed to determine forum status for {group_title}: {e}")
                            continue

                        # Handle non-forum groups
                        if not is_forum:
                            try:
                                forward_mode = self.bot.get_forward_mode()  # ✅ FIXED SCOPE
                                sent_msg = None
                                
                                if forward_mode == "show":
                                    result = await self.userbot.client.forward_messages(entity=chat_id, messages=message)
                                    sent_msg = result[0] if isinstance(result, list) else result
                                else:
                                    me = await self.userbot.client.get_me()
                                    has_premium = getattr(me, "premium", False)

                                    if message.media:
                                        file = await message.download_media()
                                        sent_msg = await self.userbot.client.send_file(
                                            chat_id,
                                            file,
                                            caption=message.message,
                                            formatting_entities=message.entities
                                        )
                                    elif message.text:
                                        sent_msg = await self.userbot.client.send_message(
                                            entity=chat_id,
                                            message=message.message,
                                            formatting_entities=message.entities
                                        )

                                    if not has_premium and message.text and "🪄" in message.text:
                                        print("⚠️ Premium emoji may be downgraded (non-premium account)")

                                if sent_msg:
                                    print(f"[DEBUG] FORWARD MODE: {forward_mode}")
                                    print(f"[DEBUG] Original Msg ID: {message.id}")
                                    print(f"[DEBUG] Sent Msg Obj: {sent_msg!r}")
                                    print(f"[DEBUG] Sent Msg ID (used in trace): {getattr(sent_msg, 'id', '??')}")
                                    self.stats["total_forwarded"] += 1
                                    self.stats["last_forward_time"] = datetime.now()
                                    self.forwarded_groups_set.add(chat_id)

                                    delay_setting = self.db.get_setting("per_message_delay", "30")
                                    delay_seconds = max(15, int(delay_setting))
                                    await asyncio.sleep(delay_seconds)

                                    print(f"[TRACE] Logging trace: chat_id={chat_id}, msg_id={sent_msg.id}")
                                    self.db.execute_query("DELETE FROM forward_trace WHERE forwarded_at < ?", 
                                                        (datetime.now() - timedelta(hours=1),))
                                    self.db.execute_query(
                                        "INSERT INTO forward_trace (group_title, group_link, chat_id, message_id, forwarded_at) VALUES (?, ?, ?, ?, ?)",
                                        (group_title, group_link, chat_id, sent_msg.id, datetime.now(timezone.utc).isoformat())
                                    )

                            except (ChatAdminRequiredError, UserBannedInChannelError, ChannelPrivateError, ChatWriteForbiddenError) as e:
                                self.stats["errors"] += 1
                                self.db.log_event("ERROR", f"Permission issue forwarding to group {group_title}: {e}")
                                self.db.execute_query("DELETE FROM groups WHERE chat_id = ?", (chat_id,))
                                self.db.log_event("ERROR", f"🚫 Auto-removed bad group {group_title} (ID {chat_id}) from database.")
                                try:
                                    entity = await self.userbot.client.get_entity(chat_id)
                                    await self.userbot.client(functions.channels.LeaveChannelRequest(entity))
                                except Exception as leave_err:
                                    self.db.log_event("ERROR", f"Failed to leave group {group_title} (ID {chat_id}): {leave_err}")
                                continue
                            except Exception as e:
                                self.stats["errors"] += 1
                                if "can't write in this chat" in str(e).lower():
                                    self.db.log_event("ERROR", f"🚫 Write error: removing {group_title} (ID {chat_id})")
                                    self.db.execute_query("DELETE FROM groups WHERE chat_id = ?", (chat_id,))
                                    continue
                                self.db.log_event("ERROR", f"Failed to forward to group {group_title}: {e}")
                                continue

                        # Handle forum groups
                        else:
                            success = await self.forward_to_forum_group(message, chat_id, group_title, group_link)
                            if success:
                                delay_setting = self.db.get_setting("per_message_delay", "30")
                                delay_seconds = max(15, int(delay_setting))
                                await asyncio.sleep(delay_seconds)

                    except Exception as e:
                        self.stats["errors"] += 1
                        self.db.log_event("ERROR", f"General failure forwarding to group {group_title}: {e}")

            except Exception as e:
                self.stats["errors"] += 1
                self.db.log_event("ERROR", f"Message fetch failure (ID {message_info['message_id']}): {e}")

        if len(self.forwarded_groups_set) >= len(groups):
            print("[Forwarding] ✅ All groups covered this round. Resetting set.")
            self.forwarded_groups_set = set()

    def get_status(self) -> Dict:
        """Get current forwarding bot status, including dynamic countdown to next phase event."""
        now = datetime.now()

        # ⏱️ Compute next event time only if bot is running
        if self.is_running:
            if self.current_phase == "forwarding":
                elapsed = (now - self.phase_start_time).total_seconds()
                seconds_left = self.cycle_length * 60 - elapsed
                if seconds_left > 0:
                    mins, secs = divmod(int(seconds_left), 60)
                    next_time_str = f"{mins}m {secs}s until HOLD"
                else:
                    next_time_str = "imminent (switching to HOLD)"
            elif self.current_phase == "holding":
                elapsed = (now - self.phase_start_time).total_seconds()
                seconds_left = self.hold_duration * 60 - elapsed
                if seconds_left > 0:
                    mins, secs = divmod(int(seconds_left), 60)
                    next_time_str = f"{mins}m {secs}s until FORWARDING"
                else:
                    next_time_str = "imminent (switching to FORWARDING)"
            else:
                next_time_str = "N/A"
        else:
            next_time_str = "N/A"

        return {
            "is_running": self.is_running,
            "is_paused": self.is_paused,
            "total_forwarded": self.stats["total_forwarded"],
            "last_forward_time": self.stats["last_forward_time"],
            "next_forward_in": next_time_str,
            "errors": self.stats["errors"],
            "selected_groups": len(self.get_selected_groups()),
            "forwarding_messages": len(self.get_forwarding_messages())
        }

class TelegramBot:
    def __init__(self, bot_token: str, admin_id: int):
        self.BOT_TOKEN = bot_token
        self.ADMIN_USER_ID = admin_id

        self.db = Database()
        self.userbot_manager = UserbotManager(self.db)
        self.forwarding_manager = ForwardingManager(self.db, self.userbot_manager, self)
        self.application = None
        self.temp_data = {}  # For storing temporary data during flows
        self.progress_containers = {}  # For live group-join progress containers
        self.last_group_join_time = None
        stored = self.db.get_setting("last_group_join_time")
        self.last_group_join_time = float(stored) if stored else None

    def set_forward_mode(self, mode: str):
        self.db.execute_query(
            "UPDATE userbot_session SET forward_mode = ? WHERE id = 1", (mode,)
        )

    def get_forward_mode(self):
        result = self.db.fetch_one("SELECT forward_mode FROM userbot_session WHERE id = 1")
        return result[0] if result else "show"

    HELP_PAGES = [
    {
        "title": "🤖 What is AdBot?",
        "content": (
            "*AdBot* is your personal Telegram automation assistant, hosted 24/7 on your VPS.\n\n"
            "Built for creators, marketers, and admins, it uses your actual Telegram account (userbot) to:\n"
            "• Forward messages to unlimited groups & channels\n"
            "• Auto-reply to DMs using smart AFK\n"
            "• Join groups automatically using links\n"
            "• Run in background with full local control\n\n"
            "*Why it's different:*\n"
            "• No third-party servers — hosted on your own VPS\n"
            "• Uses official Telegram API via session login\n"
            "• Hands-free cycles and privacy-first design"
        )
    },
    {
        "title": "🚀 How to Set Up",
        "content": (
            "1. Go to [my.telegram.org](https://my.telegram.org) → API Development\n"
            "2. Get your API ID and API Hash\n"
            "3. In the bot, tap `➕ Add Userbot`\n"
            "4. Enter:\n"
            "   - API ID\n"
            "   - API Hash\n"
            "   - Phone number (linked to your Telegram account)\n"
            "   - OTP code (Telegram login)\n"
            "   - 2FA password (if enabled)\n\n"
            "🎯 AdBot saves a session file and starts running *permanently* from your VPS."
        )
    },
    {
        "title": "👥 Groups & Auto Join",
        "content": (
            "Manage and automate your groups from the `👥 Groups` menu:\n\n"
            "• Add groups via @username, t.me links, or invite URLs\n"
            "• Auto-join queue runs every 5 minutes\n"
            "• Track join status, approval errors, or bans\n"
            "• Toggle groups on/off for forwarding\n"
            "• Remove groups to leave them automatically\n\n"
            "✅ All groups persist between reboots."
        )
    },
    {
        "title": "📤 Forwarding System",
        "content": (
            "From `🔁 Forwarding`, you can:\n"
            "• Add up to 2 messages to forward\n"
            "• Choose destination groups\n"
            "• Set delay via `Alter`\n"
            "• Start/Stop forwarding loop\n\n"
            "💡 Uses Telegram’s `forward_messages` API\n"
            "⏱ Adds random delay between sends\n"
            "🎯 Designed to mimic human behavior and avoid detection"
        )
    },
    {
        "title": "🔄 Forwarding Logic & Cycles",
        "content": (
            "The bot runs on auto cycles:\n\n"
            "• Active phase: 120 mins forwarding\n"
            "• Hold phase: 15+ mins pause (configurable)\n"
            "• Resumes automatically\n\n"
            "⏳ Countdown shown in UI\n"
            "🕹 Modify cycles at runtime\n"
            "🔁 Resets on pause, stop, or restart"
        )
    },
    {
        "title": "💬 AFK Auto-Reply Logic",
        "content": (
            "Smart AFK replies are perfect for passive DM handling:\n\n"
            "• Set message in `⚙️ Settings > 💤 AFK Message`\n"
            "• Turn ON → replies to all new DM users\n"
            "• One reply per user every 2 hours\n"
            "• Admins are excluded from replies\n"
            "• Toggle AFK OFF any time\n\n"
            "💤 Works great while you're away or sleeping"
        )
    },
    {
        "title": "🧠 Live Status & Tracing",
        "content": (
            "Go to `📊 Status` to see:\n\n"
            "• Current session info (userbot active/inactive)\n"
            "• List of selected groups\n"
            "• Next forward countdown\n"
            "• Last message per group (Live Trace)\n"
            "• Pagination & refresh\n\n"
            "🔄 Use `Refresh` to update logs in real-time"
        )
    },
    {
        "title": "⚙️ Settings & Control Panel",
        "content": (
            "Settings include:\n\n"
            "• `Alter`: custom per-message delay (min 30s)\n"
            "• `AFK`: configure & enable/disable reply logic\n"
            "• `Forward Mode`: show/hide sender tag\n"
            "• `Wipe All`: clear all settings & session\n"
            "• `Sync Groups`: reload joined group list\n"
            "• `Support Access`: allow developer debug (optional)\n\n"
            "✅ All changes take effect instantly"
        )
    },
    {
        "title": "🛡️ Emoji & Premium Mode",
        "content": (
            "When hiding sender tags (native send):\n\n"
            "• Premium emojis may break unless you have Telegram Premium\n"
            "• Bot detects this and strips unsupported emojis\n\n"
            "✅ Forward Mode is toggleable:\n"
            "• *Show* → uses official forward method\n"
            "• *Hide* → sends content manually"
        )
    },
    {
        "title": "🔐 Support Access Mode",
        "content": (
            "If you need help, you can enable temporary debug access:\n\n"
            "• Go to `⚙️ Settings > Support Access`\n"
            "• This grants developer limited remote debug rights\n"
            "• You’ll be notified when it’s active\n\n"
            "🔒 You can revoke this any time\n"
            "🛠 Only use if you're actively getting support"
        )
    },
    {
        "title": "📁 Session & Data Info",
        "content": (
            "All your data is stored *locally on the VPS*:\n\n"
            "• Telegram session (userbot)\n"
            "• Group list & join queue\n"
            "• Messages to forward\n"
            "• Logs & live traces\n\n"
            "No external API calls. No cloud sync. 100% local."
        )
    },
    {
        "title": "🚧 Troubleshooting",
        "content": (
            "If forwarding isn't working:\n"
            "• Make sure a message is added\n"
            "• Select at least one group\n"
            "• Ensure forwarding is active (not paused)\n\n"
            "If joining fails:\n"
            "• The group may be private, full, or need approval\n"
            "• Try alternate link (invite vs username)\n\n"
            "If you're banned:\n"
            "• Review delay settings — were they too low?\n"
            "• Contact Telegram support for appeal"
        )
    },
    {
        "title": "📚 Use Cases & Ideas",
        "content": (
            "• Forward CPA/ad posts to 100+ groups\n"
            "• Manage multiple forums passively\n"
            "• Auto-reply to warm leads at night\n"
            "• Promote offers without touching the app\n"
            "• Hands-free DM interaction with rate-limited accounts\n"
            "• Let clients monitor groups with logs"
        )
    },
    {
        "title": "📜 Terms of Service",
        "content": (
            "By using AdBot, you agree:\n\n"
            "• This is a self-hosted tool. You own the risks.\n"
            "• Your Telegram account is under your control\n"
            "• We are NOT responsible for bans, limits, or lockouts\n"
            "• You must follow Telegram’s Terms of Use\n"
            "• Logs are private and stored locally only\n\n"
            "💡 *Always get suggestions before risky changes*\n"
            "Spam at your own risk — automation ≠ abuse."
        )
    },
    {
        "title": "💼 Safety Guidelines",
        "content": (
            "To stay safe:\n\n"
            "• Use delays > 1 mins per forward\n"
            "• Avoid forwarding same post 20+ times quickly\n"
            "• Avoid keywords: ‘free’, ‘earn’, etc.\n"
            "• Prefer native content formats\n"
            "• Don’t mass-forward during Telegram updates\n\n"
            "✅ Best Practice: Ask us before going aggressive"
        )
    },
    {
        "title": "📞 Need Help?",
        "content": (
            "• Telegram support: [@Distroying](https://t.me/Distroying)\n"
            "• Logs, screenshots help us debug fast\n\n"
            "*Thanks for trusting AdBot 🤖*\n"
            "We’re building for creators who want speed, scale, and silence."
        )
    }
]


    def start_bot(self):
        self.application = Application.builder().token(self.BOT_TOKEN).build()

        # Restore userbot session synchronously
        import asyncio
        asyncio.get_event_loop().run_until_complete(self.userbot_manager.restore_session())

        # Register handlers
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        # Set bot commands
        asyncio.get_event_loop().run_until_complete(self.application.bot.set_my_commands([
            BotCommand("start", "Start the bot"),
            BotCommand("menu", "Show main menu"),
            BotCommand("status", "Show system status")
        ]))

        # Start background group joiner task
        loop = asyncio.get_event_loop()
        loop.create_task(self.background_group_joiner())

        print("Bot started successfully!")
        self.application.run_polling()



    
    def is_admin(self, user_id: int) -> bool:
        if user_id == self.ADMIN_USER_ID:
            return True
        if user_id in SUPPORT_USER_IDS:
            support_status = self.db.get_setting("support_access", "off")
            return support_status == "on"
        return False
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id

        if user_id == self.ADMIN_USER_ID:
            await self.show_main_menu(update, context)

        elif user_id in SUPPORT_USER_IDS:
            support_status = self.db.get_setting("support_access", "off")
            if support_status == "on":
                await self.show_main_menu(update, context)  # access to full user panel
            else:
                await self.show_support_menu(update, context)  # limited support tools

        else:
            await self.show_public_menu(update, context)

    async def show_support_menu(self, update, context):
        keyboard = [
            [InlineKeyboardButton("📄 View Subscription", callback_data="support_subscription")],
            [
                InlineKeyboardButton("➕ Add Minutes", callback_data="support_add_minutes"),
                InlineKeyboardButton("➖ Subtract Minutes", callback_data="support_subtract_minutes")
            ],
            [InlineKeyboardButton("👤 View User ID", callback_data="support_userid")],
            [InlineKeyboardButton("📶 Ping AdBot", callback_data="support_ping")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        text = "🛠 *Support Panel*\n\nChoose an action to help the user."

        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        elif update.message:
            await update.message.reply_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )


    def get_subscription_status(self) -> Tuple[bool, str]:
        start_str = self.db.get_setting("subscription_start")
        duration_str = self.db.get_setting("subscription_duration", "0")

        if not start_str or not duration_str:
            return False, "❌ Subscription not configured."

        try:
            start_time = datetime.fromisoformat(start_str)
            duration = int(duration_str)
            end_time = start_time + timedelta(minutes=duration)
            now = datetime.now()

            remaining = end_time - now

            if remaining.total_seconds() <= 0:
                return False, "⛔️ Subscription expired."

            days, rem = divmod(remaining.total_seconds(), 86400)
            hours, rem = divmod(rem, 3600)
            minutes, seconds = divmod(rem, 60)

            return True, (
                f"⏳ *Subscription Status:*\n"
                f"• Start: `{start_time.strftime('%Y-%m-%d %H:%M:%S')}`\n"
                f"• Duration: `{duration}` minutes\n"
                f"• Time left: `{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s`\n"
                f"• Expires at: `{end_time.strftime('%Y-%m-%d %H:%M:%S')}`"
            )
        except Exception as e:
            return False, f"⚠️ Error: {str(e)}"

    async def show_subscription_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        active, status = self.get_subscription_status()

        keyboard = [
            [InlineKeyboardButton("🔄 Refresh", callback_data="subscription_refresh")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_back")]
        ]

        if not active:
            keyboard.insert(0, [InlineKeyboardButton("📩 Contact Developer", url="https://t.me/m/nc4hp1LXNGFk")])
            await update.callback_query.edit_message_text(
                text=f"{status}\n\n*Access Denied.* Please contact @distroying.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.callback_query.edit_message_text(
                text=status,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )


    async def show_public_menu(self, update, context):
        keyboard = [
            [InlineKeyboardButton("🆘 Help", callback_data="public_help")],
            [InlineKeyboardButton("📩 Contact", url="https://t.me/m/HJCHVFUYZjZk")]
        ]
        text = (
            "👋 Welcome to AdBot!\n\n"
            "This bot helps manage and auto-forward Telegram posts.\n"
            "If you want to learn more or contact the owner, use the buttons below."
        )

        if hasattr(update, "callback_query") and update.callback_query:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show the main menu."""
        keyboard = [
            [InlineKeyboardButton("🛠 Userbot", callback_data="menu_userbot"),
            InlineKeyboardButton("👥 Groups", callback_data="menu_groups")],
            [InlineKeyboardButton("🔁 Forwarding", callback_data="menu_forwarding"),
            InlineKeyboardButton("📊 Status", callback_data="menu_status")],
            [InlineKeyboardButton("🆘 Help", callback_data="menu_help"),
            InlineKeyboardButton("⚙️ Settings", callback_data="menu_settings")],
            [InlineKeyboardButton("🕒 Subscription", callback_data="subscription")],
            [InlineKeyboardButton("📩 Contact", url="https://t.me/m/EamveViiOWI0")]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        text = (
    "👋 <b>Welcome to AdBot</b>  — <i>Version 1.5 (16 July 2024)</i>\n\n"
    "Manage your Telegram groups and automate message forwarding with speed, control, and precision.\n\n"
    "• Configure and monitor ad flows\n"
    "• Set forwarding intervals and hold logic\n"
    "• Track logs, adjust settings, and stay in command\n\n"
    "Select an option below to get started."
)


        if update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    raise
        else:
            await update.message.reply_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )

    async def show_dynamic_delete_menu(self, update, context):
        user_id = update.effective_user.id
        db = self.db

        # Start new session if not already active
        if user_id not in self.temp_data or self.temp_data[user_id].get("flow_type") != "delete_dynamic":
            self.temp_data[user_id] = {
                "flow_type": "delete_dynamic",
                "selected": {}
            }

        selected = self.temp_data[user_id]["selected"]

        # Fetch all table names from SQLite
        conn = sqlite3.connect(db.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()

        keyboard = []
        for table in tables:
            if table in CRITICAL_TABLES:
                continue

            is_selected = selected.get(table, False)
            status = "🟢" if is_selected else "🔴"
            label = f"{status} {table}"
            keyboard.append([
                InlineKeyboardButton(label, callback_data=f"toggle_delete_{table}")
            ])

        keyboard.append([
            InlineKeyboardButton("✅ Confirm", callback_data="delete_confirm"),
            InlineKeyboardButton("❌ Cancel", callback_data="delete_cancel")
        ])

        text = (
            "🧹 *Select Tables to Wipe*\n\n"
            "• Tap to toggle\n"
            "• 🟢 = selected for deletion\n"
            "• 🔴 = skipped\n\n"
            "*Some protected settings will never be deleted.*"
        )

        await update.callback_query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )


    async def show_settings_menu(self, update, context):
        """Show settings menu."""

        # ✅ Check subscription status
        if not await self.ensure_subscription_valid(update, context):
            return

        # Get current forward mode from settings
        forward_mode = self.get_forward_mode()
        mode_label = "Show Sender Info" if forward_mode == "show" else "Hide Sender Info"

        text = (
            "⚙️ *Settings*\n\n"
            f"🔁 *Forward Mode:* _{mode_label}_\n"
            + (
                "⚠️ _Note: Hiding sender info may cause premium emojis to downgrade if your account is not Telegram Premium._\n\n"
                if forward_mode == "hide" else "\n"
            )
            + "Choose a setting to configure:"
        )

        mode_label = "Show" if forward_mode == "show" else "Hide"
        support_status = self.db.get_setting("support_access", "off")
        dot = "🟢" if support_status == "on" else "🔴"
        support_label = f"{dot} Support Access"

        keyboard = [
            [InlineKeyboardButton(f"🔁 Forward Style: {mode_label}", callback_data="toggle_forward_mode")],
            [InlineKeyboardButton("💤 AFK Message", callback_data="settings_afk")],
            [InlineKeyboardButton("🔄 Sync All Groups", callback_data="confirm_sync_groups")],
            [InlineKeyboardButton("🧹 Custom Wipe (Advanced)", callback_data="show_custom_wipe")],
            [InlineKeyboardButton("🧹 Clear All Data", callback_data="settings_clear_all")],
            [InlineKeyboardButton(support_label, callback_data="toggle_support_access")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)

        if hasattr(update, "callback_query") and update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    pass  # Ignore harmless error
                else:
                    raise
        else:
            await update.message.reply_text(
                text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN
            )

    async def update_group_join_progress(self, user_id):
        from telegram.constants import ParseMode
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        data = self.progress_containers.get(user_id)
        if not data or "progress_msg_id" not in data or "progress_chat_id" not in data:
            return

        rows = self.db.execute_query(
            "SELECT group_link, status FROM pending_joins ORDER BY id"
        )
        total = len(rows)
        joined = sum(1 for _, s in rows if s == "joined")
        failed = sum(1 for _, s in rows if s == "failed")
        pending = sum(1 for _, s in rows if s == "pending")

        # ✅ If nothing is pending, clear timer and optionally exit early
        if pending == 0:
            self.last_group_join_time = None
            self.db.set_setting("last_group_join_time", "")
            self.progress_containers.pop(user_id, None)

            done_msg = [
                escape_markdown("✅ All groups joined or failed. Nothing pending.", version=2),
                "",
                f"{escape_markdown('Groups queued', version=2)}: {escape_markdown(str(total), version=2)}",
                f"{escape_markdown('Joined', version=2)}: {escape_markdown(str(joined), version=2)} / {escape_markdown(str(total), version=2)}",
                f"{escape_markdown('Failed', version=2)}: {escape_markdown(str(failed), version=2)}",
                f"{escape_markdown('Pending', version=2)}: 0",
                "",
                escape_markdown("Use the button below to go back.", version=2)
            ]

            reply_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="menu_groups")]
            ])

            try:
                await self.application.bot.edit_message_text(
                    text="\n".join(done_msg),
                    chat_id=data["progress_chat_id"],
                    message_id=data["progress_msg_id"],
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    pass
                else:
                    raise

            return

        # Continue with normal rendering if pending > 0
        page = data.get("progress_page", 0)
        page_size = 10
        total_pages = (total + page_size - 1) // page_size
        start = page * page_size
        end = start + page_size
        page_groups = rows[start:end]

        progress_lines = [
            escape_markdown('🟢 Joining Groups Progress', version=2),
            "",
            f"{escape_markdown('Groups queued', version=2)}: {escape_markdown(str(total), version=2)}",
            f"{escape_markdown('Joined', version=2)}: {escape_markdown(str(joined), version=2)} / {escape_markdown(str(total), version=2)}",
            f"{escape_markdown('Failed', version=2)}: {escape_markdown(str(failed), version=2)}",
            f"{escape_markdown('Pending', version=2)}: {escape_markdown(str(pending), version=2)}",
            ""
        ]

        # ⏳ Join timer or paused
        join_status = self.db.get_setting("group_join_enabled", "on")
        if join_status == "off":
            progress_lines.append(escape_markdown("⏸ Joining is currently paused", version=2))
        elif hasattr(self, "last_group_join_time") and self.last_group_join_time:
            elapsed = time.time() - self.last_group_join_time
            remaining = max(0, 300 - int(elapsed))
            mins = remaining // 60
            secs = remaining % 60
            timer_str = f"{mins}:{secs:02d}"
            progress_lines.append(f"⏳ Next group join in: {escape_markdown(timer_str, version=2)}")
        else:
            progress_lines.append(escape_markdown("⏳ Next group join: pending…", version=2))

        progress_lines.append("")
        for idx, (group, status) in enumerate(page_groups, start=start + 1):
            symbol = {"joined": "✅", "failed": "❌", "pending": "⏳"}.get(status, "❓")
            idx_str = escape_markdown(str(idx), version=2)
            group_str = escape_markdown(group, version=2)
            progress_lines.append(f"{idx_str}\\.{chr(32)}{group_str} {symbol}")
        progress_lines.append("")
        progress_lines.append(escape_markdown("Use the arrows to scroll, or tap Refresh.", version=2))

        # ⏯ Pause/Resume label
        toggle_label = "⏸ Pause Joining" if join_status == "on" else "▶️ Resume Joining"

        # Pagination buttons
        pagination_row = []
        if page > 0:
            pagination_row.append(InlineKeyboardButton("⬅️ Prev", callback_data="progress_prevpage"))
        if page < total_pages - 1:
            pagination_row.append(InlineKeyboardButton("Next ➡️", callback_data="progress_nextpage"))

        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="groups_processes")],
            pagination_row,
            [InlineKeyboardButton(toggle_label, callback_data="toggle_group_join")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_groups")]
        ])

        try:
            await self.application.bot.edit_message_text(
                text="\n".join(progress_lines),
                chat_id=data["progress_chat_id"],
                message_id=data["progress_msg_id"],
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Ignore harmless message
            else:
                raise
        
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle callback queries."""
        query = update.callback_query
        data = query.data
        data = update.callback_query.data

        if data == "public_help" or data.startswith("help_page_"):
            # Figure out which page to show
            if data == "public_help":
                page = 0
            else:
                page = int(data.split("_")[-1])
            await self.show_help_menu(update, context, page=page)
            return

        if data == "public_back":
            await self.show_public_menu(update, context)
            return
                
        print("[DEBUG] handle_callback is running!")
        print(f"[DEBUG] handle_callback received: {data}")
        
        await query.answer()
        
        user_id = update.effective_user.id
        support_status = self.db.get_setting("support_access", "off")

        # Only allow valid users to proceed
        if not (
            user_id == self.ADMIN_USER_ID or 
            (user_id in SUPPORT_USER_IDS and (support_status == "on" or data.startswith("support_")))
        ):
            await query.edit_message_text("❌ Unauthorized access.")
            return
        
        data = query.data


        
        # Main menu handlers
        if data == "menu_userbot":
            await self.show_userbot_menu(update, context)
        elif data == "menu_groups":
            await self.show_groups_menu(update, context)
        elif data == "menu_forwarding":
            await self.show_forwarding_menu(update, context)
        elif data == "menu_status":
            await self.show_status_menu(update, context)
        elif data == "status_live":
            await self.show_live_status(update, context)
        elif data == "status_logs":
            await self.show_logs(update, context)
        elif data == "status_trace":
            await self.show_live_trace(update, context, page=0)
        elif data.startswith("help_page_"):
            page_num = int(data.split("_")[-1])
            await self.show_help_menu(update, context, page=page_num)

        elif data == "menu_help":
            await self.show_help_menu(update, context, page=0)
        elif data == "menu_settings":
            await self.show_settings_menu(update, context)
        elif data == "back_main":
            await self.show_main_menu(update, context)
        
        elif data == "forwarding_preview":
            await self.show_forwarding_preview(update, context)
        
        elif data == "support_menu":
            await self.show_support_menu(update, context)
        
        elif data == "support_subscription":
            active, status = self.get_subscription_status()
            keyboard = [
                [InlineKeyboardButton("🔙 Back", callback_data="support_menu")]
            ]
            await update.callback_query.edit_message_text(
                text=status,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        elif data == "set_hold_duration":
            current = self.db.get_setting("hold_duration", "15")
            await update.callback_query.edit_message_text(
                f"🕒 *Set Hold Duration*\n\n"
                f"Current hold time: `{current}` minutes\n\n"
                "Please enter a new hold duration (minimum: 15 minutes):",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                ])
            )
            self.temp_data[update.effective_user.id] = {"flow_type": "set_hold_duration"}

        elif data == "set_cycle_length":
            current = self.db.get_setting("cycle_length", "120")
            await update.callback_query.edit_message_text(
                f"♻️ *Set Cycle Length*\n\n"
                f"Current cycle duration: `{current}` minutes\n\n"
                f"Enter a new cycle length (minimum: 10 minutes):",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                ])
            )
            self.temp_data[update.effective_user.id] = {"flow_type": "set_cycle_length"}

        elif data == "support_add_minutes":
            self.temp_data[user_id] = {"flow_type": "support_add"}
            current = self.db.get_setting("subscription_duration", "0")
            await update.callback_query.edit_message_text(
                f"✏️ *Add Minutes*\n\nCurrent Duration: `{current}` minutes\n\nEnter how many minutes to add:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="support_menu")]])
            )

        elif data == "support_subtract_minutes":
            self.temp_data[update.effective_user.id] = {"flow_type": "support_subtract"}
            await update.callback_query.edit_message_text(
                "✏️ Enter how many minutes to *subtract* from the subscription:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="support_menu")]
                ])
            )
            
        elif data == "support_userid":
            uid = self.db.get_setting("admin_user_id", "Unknown")
            await update.callback_query.edit_message_text(
                f"👤 Admin User ID: `{uid}`",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="support_menu")]])
            )

        elif data == "support_ping":
            userbot_status = await self.userbot_manager.get_status()
            await update.callback_query.edit_message_text(
                f"📶 *Ping Result:*\n\n"
                f"Status: {userbot_status['status']}\n"
                f"User: {userbot_status['user_info']}\n"
                f"Last Active: {userbot_status['last_activity']}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="support_menu")]])
            )

        elif data == "settings_alter_delay":
            current = self.db.get_setting("per_message_delay", "30")
            await update.callback_query.edit_message_text(
                f"⏱ *Alter Per-Message Delay*\n\n"
                f"Current delay: `{current}` seconds\n\n"
                "Please enter a new delay (minimum: 30 seconds):",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                ])
            )
            self.temp_data[update.effective_user.id] = {"flow_type": "alter_delay"}
            

        elif data == "confirm_sync_groups":
            text = (
                "⚠️ *Sync All Groups*\n\n"
                "This will scan all groups and channels the userbot is currently joined to, "
                "and update the internal group list. Any missing groups will be added.\n\n"
                "*Are you sure you want to continue?*"
            )
            keyboard = [
                [InlineKeyboardButton("✅ Yes, Sync Now", callback_data="sync_groups_now")],
                [InlineKeyboardButton("❌ Cancel", callback_data="menu_settings")]
            ]
            await update.callback_query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )

        elif data == "sync_groups_now":
            await update.callback_query.edit_message_text("🔄 Syncing groups... Please wait.")

            try:
                result = await self.userbot_manager.fetch_groups()

                await update.callback_query.edit_message_text(
                    f"✅ Group sync completed successfully.\n"
                    f"✅ Synced {result['total']} group(s).\n"
                    f"🆕 {result['new']} new, ♻️ {result['existing']} already existed.\n\n"
                    "What do you want to do next?",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 View Groups", callback_data="groups_view")],
                        [InlineKeyboardButton("🔁 Sync Again", callback_data="sync_groups_now")],
                        [InlineKeyboardButton("🔙 Back", callback_data="menu_settings")]
                    ])
                )
            except Exception as e:
                await update.callback_query.edit_message_text(
                    f"❌ Failed to sync groups.\n\nError: `{str(e)}`",
                    parse_mode=ParseMode.MARKDOWN
                )

        elif data == "userbot_retry_api_id":
            user_id = update.effective_user.id
            if user_id in self.temp_data:
                self.temp_data[user_id]["step"] = "api_id"
                await update.callback_query.edit_message_text(
                    "🔢 Please re-enter your API ID:",
                    parse_mode=ParseMode.MARKDOWN
                )

        elif data == "toggle_support_access":
            current = self.db.get_setting("support_access", "off")
            if current == "on":
                # Confirm turning OFF
                text = (
                    "⚠️ *Disable Support Access*\n\n"
                    "Turning this OFF will revoke panel access from the support team.\n\n"
                    "*Are you sure?*"
                )
                confirm_cb = "confirm_support_off"
            else:
                # Confirm turning ON
                text = (
                    "⚠️ *Enable Support Access*\n\n"
                    "This will grant full control of your panel to our support staff.\n"
                    "They can view your groups, messages, logs, and settings to assist you.\n\n"
                    "*Are you sure you want to continue?*"
                )
                confirm_cb = "confirm_support_on"

            keyboard = [
                [InlineKeyboardButton("✅ Yes, Proceed", callback_data=confirm_cb)],
                [InlineKeyboardButton("❌ Cancel", callback_data="menu_settings")]
            ]

            await update.callback_query.edit_message_text(
                text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
            )

        elif data == "confirm_support_on":
            self.db.set_setting("support_access", "on")
            for uid in SUPPORT_USER_IDS:
                try:
                    await self.application.bot.send_message(uid, "✅ Support access has been granted. You can now access the user's panel.")
                except Exception as e:
                    print(f"[Support Access] Notify error: {e}")
            await update.callback_query.answer("✅ Support access enabled.")
            await self.show_settings_menu(update, context)

        elif data == "confirm_support_off":
            self.db.set_setting("support_access", "off")
            for uid in SUPPORT_USER_IDS:
                try:
                    await self.application.bot.send_message(uid, "🚫 Support access has been revoked. You no longer have access.")
                except Exception as e:
                    print(f"[Support Access] Notify error: {e}")
            await update.callback_query.answer("🚫 Support access disabled.")
            await self.show_settings_menu(update, context)

        elif data == "userbot_retry_api_hash":
            user_id = update.effective_user.id
            if user_id in self.temp_data:
                self.temp_data[user_id]["step"] = "api_hash"
                await update.callback_query.edit_message_text(
                    "🔑 Please re-enter your API Hash:",
                    parse_mode=ParseMode.MARKDOWN
                )
        
        elif data.startswith("logs_page_"):
            page = int(data.split("_")[-1])
            await self.show_logs(update, context, page=page)

        elif data == "groups_processes":
            user_id = update.effective_user.id

            # Load all pending joins
            rows = self.db.execute_query(
                "SELECT group_link, status FROM pending_joins ORDER BY id"
            )
            total = len(rows)
            joined = sum(1 for _, s in rows if s == "joined")
            failed = sum(1 for _, s in rows if s == "failed")
            pending = sum(1 for _, s in rows if s == "pending")

            page = 0
            page_size = 10
            total_pages = (total + page_size - 1) // page_size
            start = page * page_size
            end = start + page_size
            page_groups = rows[start:end]

            progress_lines = [
                escape_markdown("🟢 Joining Groups Progress", version=2),
                "",
                f"{escape_markdown('Groups queued', version=2)}: {escape_markdown(str(total), version=2)}",
                f"{escape_markdown('Joined', version=2)}: {escape_markdown(str(joined), version=2)}",
                f"{escape_markdown('Failed', version=2)}: {escape_markdown(str(failed), version=2)}",
                f"{escape_markdown('Pending', version=2)}: {escape_markdown(str(pending), version=2)}",
                ""
            ]

            # Show timer or pause status
            interval = 300
            join_status = self.db.get_setting("group_join_enabled", "on")
            if join_status == "off":
                progress_lines.append(escape_markdown("⏸ Group joining is currently paused.", version=2))
            elif hasattr(self, "last_group_join_time") and self.last_group_join_time:
                elapsed = time.time() - self.last_group_join_time
                remaining = max(0, interval - int(elapsed))
                mins = remaining // 60
                secs = remaining % 60
                timer_str = f"{mins}:{secs:02d}"
                progress_lines.append(escape_markdown(f"⏳ Next group join in: {timer_str}", version=2))
            else:
                progress_lines.append(escape_markdown("⏳ Next group join: pending…", version=2))

            progress_lines.append("")

            # Add group lines with safe escaping
            for idx, (group, status) in enumerate(page_groups, start=start + 1):
                symbol = {"joined": "✅", "failed": "❌", "pending": "⏳"}.get(status, "❓")
                try:
                    idx_str = escape_markdown(str(idx), version=2)
                    group_str = escape_markdown(group, version=2)
                    progress_lines.append(f"{idx_str}\\.{chr(32)}{group_str} {symbol}")
                except Exception as e:
                    print(f"[MarkdownEscapeError] Failed escaping group {group}: {e}")
                    safe_group = group.replace(".", "\\.").replace("-", "\\-")
                    progress_lines.append(f"{idx}\\.{chr(32)}{safe_group} {symbol}")

            progress_lines.append("")
            progress_lines.append(escape_markdown("Use the arrows to scroll, or tap Refresh.", version=2))

            # UI buttons
            toggle_label = "⏸ Pause Joining" if join_status == "on" else "▶️ Resume Joining"

            pagination_row = []
            if page > 0:
                pagination_row.append(InlineKeyboardButton("⬅️ Prev", callback_data="progress_prevpage"))
            if page < total_pages - 1:
                pagination_row.append(InlineKeyboardButton("Next ➡️", callback_data="progress_nextpage"))

            reply_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="groups_processes")],
                pagination_row,
                [InlineKeyboardButton(toggle_label, callback_data="toggle_group_join")],
                [InlineKeyboardButton("🔙 Back", callback_data="menu_groups")]
            ])

            text = "\n".join(progress_lines)
            original_text = update.callback_query.message.text or ""

            try:
                if original_text.startswith("👥 Groups Management") or "Select an option" in original_text:
                    msg = await update.effective_chat.send_message(
                        text=text,
                        reply_markup=reply_markup,
                        parse_mode="MarkdownV2"
                    )
                else:
                    msg = await update.callback_query.edit_message_text(
                        text=text,
                        reply_markup=reply_markup,
                        parse_mode="MarkdownV2"
                    )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    return  # Silent ignore
                else:
                    raise

            # Save progress pointer
            self.progress_containers[user_id] = {
                "progress_msg_id": msg.message_id,
                "progress_chat_id": msg.chat_id,
                "progress_page": 0
            }

        elif data.startswith("trace_page_"):
            page = int(data.split("_")[-1])
            await self.show_live_trace(update, context, page=page)

        elif data == "trace_clear":
            self.db.execute_query("DELETE FROM forward_trace")
            await update.callback_query.answer("🧹 Cleared all trace logs.")
            await self.show_live_trace(update, context, page=0)

        elif data == "userbot_retry_password":
            user_id = update.effective_user.id
            if user_id in self.temp_data:
                self.temp_data[user_id]["step"] = "password"
                await update.callback_query.edit_message_text(
                    "🔐 Please re-enter your 2FA password:",
                    parse_mode=ParseMode.MARKDOWN
                )

        elif data == "userbot_retry_phone":
            user_id = update.effective_user.id
            if user_id in self.temp_data:
                self.temp_data[user_id]["step"] = "phone_number"
                await update.callback_query.edit_message_text(
                    "📱 Please re-enter your phone number (with country code):",
                    parse_mode=ParseMode.MARKDOWN
                )


        elif data == "show_custom_wipe":
            await self.show_dynamic_delete_menu(update, context)

        elif data.startswith("toggle_delete_"):
            table = data.replace("toggle_delete_", "")
            flow = self.temp_data.get(user_id, {})
            if flow.get("flow_type") == "delete_dynamic":
                selected = flow["selected"]
                selected[table] = not selected.get(table, False)
                await self.show_dynamic_delete_menu(update, context)

        elif data == "delete_confirm":
            flow = self.temp_data.get(user_id, {})
            selected = flow.get("selected", {})
            deleted_tables = []

            for table, should_delete in selected.items():
                if not should_delete:
                    continue

                # Handle settings table specially
                if table == "settings":
                    self.db.execute_query(
                        "DELETE FROM settings WHERE key NOT IN (?, ?, ?, ?)",
                        tuple(CRITICAL_SETTINGS_KEYS)
                    )
                else:
                    self.db.execute_query(f"DELETE FROM {table}")

                deleted_tables.append(table)

            del self.temp_data[user_id]

            await update.callback_query.edit_message_text(
                "✅ *Deleted tables:*\n" +
                "\n".join([f"• `{t}`" for t in deleted_tables]) +
                "\n\nSubscription/admin settings preserved.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back to Settings", callback_data="menu_settings")]
                ])
            )

        elif data == "delete_cancel":
            del self.temp_data[user_id]
            await update.callback_query.edit_message_text(
                "❌ Cancelled wipe.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="menu_settings")]
                ])
            )

        elif data == "settings_clear_all":
            keyboard = [
                [InlineKeyboardButton("✅ Yes, clear EVERYTHING", callback_data="settings_clear_all_confirm")],
                [InlineKeyboardButton("❌ Cancel", callback_data="menu_settings")]
            ]
            text = (
                "⚠️ *Are you sure you want to clear ALL bot data?*\n\n"
                "This will delete:\n"
                "• All userbot sessions\n"
                "• All groups\n"
                "• All forwarding messages\n"
                "• All settings\n"
                "• All logs\n"
                "• All pending joins\n\n"
                "*This cannot be undone!*"
            )
            await update.callback_query.edit_message_text(
                text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
            )

        elif data == "toggle_forward_mode":
            current_mode = self.get_forward_mode()
            new_mode = "hide" if current_mode == "show" else "show"
            self.set_forward_mode(new_mode)

            me = await self.userbot_manager.client.get_me()
            has_premium = getattr(me, "premium", False)

            if new_mode == "hide" and not has_premium:
                await query.answer(
                    "⚠️ Sender info is hidden.\nBut you're not a Premium user — premium emojis may be downgraded.",
                    show_alert=True
                )
            else:
                await query.answer(f"✅ Sender info set to: {new_mode.upper()}", show_alert=False)

            await self.show_settings_menu(update, context)

        elif data == "settings_clear_all_confirm":
            # 1. Delete from all non-critical tables
            tables_to_clear = [
                "userbot_session", "groups", "forwarding_messages",
                "logs", "pending_joins", "forward_trace", "afk_replies"
            ]
            for t in tables_to_clear:
                self.db.execute_query(f"DELETE FROM {t}")

            # 2. Keep subscription + bot token info only
            self.db.execute_query(
                "DELETE FROM settings WHERE key NOT IN (?, ?, ?, ?)",
                ("bot_token", "admin_user_id", "subscription_start", "subscription_duration")
            )

            # 3. Reset forwarding state
            self.forwarding_manager.forwarded_groups_set = set()
            self.forwarding_manager.stats = {
                "total_forwarded": 0,
                "last_forward_time": None,
                "next_forward_time": None,
                "errors": 0
            }

            # 4. Disconnect and remove userbot session
            try:
                if self.userbot_manager.client:
                    await self.userbot_manager.client.disconnect()
                self.userbot_manager.client = None
                self.userbot_manager.is_authenticated = False
            except Exception as e:
                self.db.log_event("ERROR", f"Error disconnecting userbot: {str(e)}")

            # 5. Remove session file(s)
            import os
            session_prefix = self.userbot_manager.session_file  # e.g., "liam_adbot"
            session_files = [f for f in os.listdir('.') if f.startswith(session_prefix)]
            for file in session_files:
                try:
                    os.remove(file)
                except Exception as e:
                    self.db.log_event("ERROR", f"Failed to remove session file {file}: {str(e)}")

            # 6. Notify user
            try:
                await update.callback_query.edit_message_text(
                    "🧹 *All bot data has been permanently cleared!*\n\n"
                    "✅ Subscription info and bot config have been preserved.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu_settings")]]),
                    parse_mode=ParseMode.MARKDOWN
                )
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    raise

        elif data == "settings_afk":
            afk_enabled = self.db.get_setting("afk_enabled", "off")
            afk_message = self.db.get_setting("afk_message", "Hello, this is my adbot account. Please contact me at @username.")
            on_off_button = InlineKeyboardButton(
                "✅ Turn OFF" if afk_enabled == "on" else "✅ Turn ON",
                callback_data="settings_afk_toggle"
            )
            keyboard = [
                [InlineKeyboardButton("📝 Set Message", callback_data="settings_afk_set")],
                [InlineKeyboardButton("🗑️ Remove Message", callback_data="settings_afk_remove")],
                [on_off_button],
                [InlineKeyboardButton("🔙 Back", callback_data="menu_settings")]
            ]
            text = (
                f"💤 *AFK Message Settings*\n\n"
                f"Status: {'ON' if afk_enabled == 'on' else 'OFF'}\n"
                f"Current message:\n"
                f"_{afk_message}_"
            )
            try:
                await update.callback_query.edit_message_text(
                    text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    # Ignore harmless error
                    pass
                else:
                    raise

        elif data == "settings_afk_set":
            self.temp_data[update.effective_user.id] = {"flow_type": "afk_set"}
            try:
                await update.callback_query.edit_message_text(
                    "✏️ *Send the new AFK message now:*",
                    parse_mode=ParseMode.MARKDOWN
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    pass
                else:
                    raise

        elif data == "settings_afk_remove":
            self.db.set_setting("afk_message", "")
            try:
                await update.callback_query.edit_message_text(
                    "🗑️ *AFK message removed!*",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="settings_afk")]]),
                    parse_mode=ParseMode.MARKDOWN
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    pass
                else:
                    raise

        elif data == "settings_afk_toggle":
            afk_enabled = self.db.get_setting("afk_enabled", "off")
            new_status = "off" if afk_enabled == "on" else "on"
            self.db.set_setting("afk_enabled", new_status)
            try:
                await update.callback_query.edit_message_text(
                    f"{'☑️ AFK is now ON.' if new_status == 'on' else '❌ AFK is now OFF.'}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="settings_afk")]]),
                    parse_mode=ParseMode.MARKDOWN
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    pass
                else:
                    raise


        elif data == "subscription":
            await self.show_subscription_status(update, context)
        elif data == "subscription_refresh":
            await self.show_subscription_status(update, context)
        elif data == "menu_back":
            await self.show_main_menu(update, context)

        # Userbot handlers
        elif data == "userbot_add":
            await self.start_userbot_add_flow(update, context)
        elif data == "userbot_remove":
            await self.userbot_remove_confirm(update, context)
        elif data == "userbot_remove_confirm":
            await self.userbot_remove_execute(update, context)
        elif data == "userbot_status":
            await self.show_userbot_status(update, context)
        

        elif data == "userbot_retry_code":
            # Let user retry the verification code
            user_id = update.effective_user.id
            if user_id in self.temp_data:
                self.temp_data[user_id]["step"] = "code"
                await update.callback_query.edit_message_text(
                    "🔁 Please re-enter the verification code:",
                    parse_mode=ParseMode.MARKDOWN
                )

        elif data == "userbot_resend_code":
            # Re-send the login code
            user_id = update.effective_user.id
            flow_data = self.temp_data.get(user_id)
            if flow_data:
                try:
                    await self.userbot_manager.client.send_code_request(flow_data["phone_number"])
                    await update.callback_query.edit_message_text(
                        "📲 New verification code sent!\nPlease enter the new code:",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    self.temp_data[user_id]["step"] = "code"
                except Exception as e:
                    await update.callback_query.edit_message_text(
                        f"❌ Failed to resend code:\n{str(e)}",
                        parse_mode=ParseMode.MARKDOWN
                    )


        # Groups handlers
        elif data == "groups_add":
            await self.start_groups_add_flow(update, context)
        elif data == "groups_confirm_join":
            await self.process_start_joining(update, context)
        elif data == "start_joining":
            await self.process_start_joining(update, context)
        elif data == "groups_prevpage":
            user_id = update.effective_user.id
            flow_data = self.temp_data.get(user_id)
            if flow_data:
                flow_data["current_page"] = max(0, flow_data["current_page"] - 1)
                await self.send_group_preview(update, context, user_id, edit=True)
        elif data.startswith("groups_page_"):
            page_num = int(data.split("_")[-1])
            await self.show_groups_list(update, context, page=page_num)

        elif data == "groups_nextpage":
            user_id = update.effective_user.id
            flow_data = self.temp_data.get(user_id)
            if flow_data:
                page = flow_data.get("current_page", 0)
                total_pages = (len(flow_data["groups_to_add"]) + 9) // 10
                flow_data["current_page"] = min(total_pages - 1, page + 1)
                await self.send_group_preview(update, context, user_id, edit=True)
        elif data == "groups_view":
            await self.show_groups_list(update, context)

        elif data == "groups_remove":
            await self.show_groups_remove_menu(update, context)

        elif data.startswith("groups_remove_page_"):
            page_num = int(data.split("_")[-1])
            await self.show_groups_remove_menu(update, context, page=page_num)

        elif data.startswith("group_toggle_"):
            await self.toggle_group_selection(update, context, data)
    
        elif data.startswith("group_remove_exec_"):
            chat_id = int(data[len("group_remove_exec_"):])
            # Get group info from DB
            group_info = self.db.execute_query(
                "SELECT title, username, chat_type FROM groups WHERE chat_id = ?", (chat_id,)
            )
            if not group_info:
                await update.callback_query.edit_message_text("❌ Group not found or already removed.")
                return

            title, username, chat_type = group_info[0]
            # --- Attempt to leave the group using the userbot ---
            try:
                if self.userbot_manager.is_authenticated and self.userbot_manager.client:
                    # Leave group using Telethon
                    try:
                        entity = await self.userbot_manager.client.get_entity(chat_id)
                        await self.userbot_manager.client(functions.channels.LeaveChannelRequest(entity))
                    except Exception as leave_exc:
                        # Log error but proceed with DB cleanup
                        self.db.log_event("ERROR", f"Failed to leave group {title}: {leave_exc}")
                else:
                    # Userbot not connected; just remove from DB
                    pass
            except Exception as e:
                self.db.log_event("ERROR", f"Error in group removal for {title}: {e}")

            # Remove from DB
            self.db.execute_query("DELETE FROM groups WHERE chat_id = ?", (chat_id,))
            # Optionally, remove from forwarding_targets or other tables

            await update.callback_query.edit_message_text(
                f"✅ <b>Removed group:</b> <code>{title}</code>\n"
                f"Userbot has left the group (if possible).\n",
                parse_mode="HTML"
            )
            # Optionally show updated group list (paginated)
            await self.show_groups_remove_menu(update, context)


        elif data.startswith("group_remove_"):
            chat_id = int(data[len("group_remove_"):])
            print(f"[DEBUG] User selected group_remove_ for chat_id: {chat_id}")
            group_info = self.db.execute_query(
                "SELECT title, username, chat_type FROM groups WHERE chat_id = ?", (chat_id,)
            )
            if not group_info:
                await update.callback_query.edit_message_text("❌ Group not found.")
                return
            title, username, chat_type = group_info[0]
            text = (
                f"❌ <b>Remove Group</b>\n\n"
                f"<b>Name:</b> {title}\n"
                f"<b>Chat ID:</b> <code>{chat_id}</code>\n"
                f"<b>Username:</b> @{username if username else 'N/A'}\n"
                f"<b>Type:</b> {chat_type}\n\n"
                f"Do you want to remove this group? This will immediately leave and delete the group from your bot."
            )
            keyboard = [
                [InlineKeyboardButton("🚫 Remove Group", callback_data=f"group_remove_exec_{chat_id}")],
                [InlineKeyboardButton("🔙 Back", callback_data="groups_remove_page_0")]
            ]
            await update.callback_query.edit_message_text(
                text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML"
            )   

        elif data == "progress_refresh":
            user_id = update.effective_user.id
            await self.update_group_join_progress(user_id)

        elif data == "progress_prevpage":
            user_id = update.effective_user.id
            if user_id in self.progress_containers:
                self.progress_containers[user_id]["progress_page"] = max(
                    0, self.progress_containers[user_id].get("progress_page", 0) - 1
                )
                await self.update_group_join_progress(user_id)

        elif data == "progress_nextpage":
            user_id = update.effective_user.id
            if user_id in self.progress_containers:
                self.progress_containers[user_id]["progress_page"] = (
                    self.progress_containers[user_id].get("progress_page", 0) + 1
                )
                await self.update_group_join_progress(user_id)

        elif data == "toggle_group_join":
            user_id = update.effective_user.id
            current = self.db.get_setting("group_join_enabled", "on")
            new_value = "off" if current == "on" else "on"
            self.db.set_setting("group_join_enabled", new_value)

            await update.callback_query.answer(
                "✅ Group joining has been " + ("paused." if new_value == "off" else "resumed.")
            )

            # Refresh the current group join progress message
            await self.update_group_join_progress(user_id)

        # Forwarding handlers
        elif data == "forwarding_messages":
            await self.show_forwarding_messages_menu(update, context)
        elif data == "forwarding_controls":
            await self.show_forwarding_controls_menu(update, context)
        elif data == "forwarding_destinations":
            await self.show_forwarding_destinations_menu(update, context)
        elif data == "forwarding_start":
            await self.start_forwarding(update, context)
        elif data == "forwarding_stop":
            await self.stop_forwarding(update, context)
        elif data == "forwarding_pause":
            await self.pause_forwarding(update, context)
        elif data == "forwarding_resume":
            await self.resume_forwarding(update, context)
        elif data == "message_add":
            await self.start_message_add_flow(update, context)
        elif data.startswith("message_remove_"):
            await self.remove_forwarding_message(update, context, data)
        elif data == "logs_clear":
            self.db.execute_query("DELETE FROM logs")
            await self.show_logs(update, context)
        
    async def process_start_joining(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        flow_data = self.temp_data.get(user_id)
        if not flow_data or "groups_to_add" not in flow_data or not flow_data["groups_to_add"]:
            await update.callback_query.edit_message_text(
                escape_markdown("❌ No groups to join. Please add at least one group.", version=2),
                parse_mode="MarkdownV2"
            )
            return

        # --- NEW: Get all usernames and @links of already joined groups ---
        already_joined_groups = set()
        group_rows = self.db.execute_query("SELECT username FROM groups WHERE username IS NOT NULL")
        already_joined_groups = set(r[0].lower() for r in group_rows if r[0])

        # Track which links were skipped
        skipped = []
        queued = []

        for group_link in flow_data["groups_to_add"]:
            username = None
            # Extract username from @ or t.me/ link
            if group_link.startswith('@'):
                username = group_link[1:].lower()
            elif 't.me/' in group_link:
                part = group_link.split('t.me/')[-1]
                username = part.split('/')[0].lower() if '/' in part else part.lower()
                # skip joinchat links here, those don't have username
                if username.startswith("joinchat"):
                    username = None

            if username and username in already_joined_groups:
                skipped.append(group_link)
                continue  # skip, already joined

            # Only add to join queue if not skipped
            self.db.execute_query(
                "INSERT INTO pending_joins (group_link, status) VALUES (?, ?)",
                (group_link, "pending")
            )
            queued.append(group_link)

        total_groups = len(queued)

        progress_lines = [
            escape_markdown('🟢 Joining Groups Progress', version=2),
            "",
            f"{escape_markdown('Groups queued', version=2)}: {escape_markdown(str(total_groups), version=2)}",
            f"{escape_markdown('Skipped (already joined)', version=2)}: {escape_markdown(str(len(skipped)), version=2)}",
            ""
        ]
        if skipped:
            progress_lines.append(escape_markdown("Already joined:", version=2))
            progress_lines.extend([escape_markdown(g, version=2) for g in skipped])
            progress_lines.append("")

        progress_lines.append(escape_markdown('Next join will happen automatically every 5 minutes.', version=2))
        progress_lines.append(escape_markdown('This message will update as joins complete.', version=2))

        progress_text = "\n".join(progress_lines)

        try:
            msg = await update.callback_query.edit_message_text(
                progress_text,
                parse_mode="MarkdownV2"
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                # Silently ignore this expected error
                pass
            else:
                raise

        # Save for later live updating
        self.progress_containers[user_id] = {
            "progress_msg_id": msg.message_id,
            "progress_chat_id": msg.chat_id,
            "progress_page": 0
        }

    async def ensure_subscription_valid(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        active, _ = self.get_subscription_status()
        if not active:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📩 Contact Developer", url="https://t.me/distroying")]
            ])
            await update.callback_query.edit_message_text(
            text=(
                "⛔ *Subscription ended.*\n"
                "Thanks for subscribing!\n\n"
                "Please contact [@distroying](https://t.me/distroying) to renew access."
            ),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard
        )
            return False
        return True

    async def show_groups_remove_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
        groups = self.db.execute_query(
            "SELECT chat_id, title, username FROM groups ORDER BY title"
        )
        page_size = 10
        total_pages = (len(groups) + page_size - 1) // page_size
        start_idx = page * page_size
        end_idx = min(start_idx + page_size, len(groups))
        page_groups = groups[start_idx:end_idx]

        if not groups:
            text = "❌ **No Groups Found**\n\nNo groups to remove."
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu_groups")]]
        else:
            text = f"❌ **Remove from Groups** (Page {page + 1}/{total_pages})\n\nSelect a group to leave:"
            keyboard = []

            for group in page_groups:
                chat_id, title, username = group
                display_name = f"{title} (@{username})" if username else title
                keyboard.append([
                    InlineKeyboardButton(
                        f"❌ {display_name[:40]}...",
                        callback_data=f"group_remove_{chat_id}"
                    )
                ])

            # Pagination row
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"groups_remove_page_{page-1}"))
            if page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"groups_remove_page_{page+1}"))
            if nav_buttons:
                keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="menu_groups")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            print(f"[DEBUG] Exception in show_groups_remove_menu: {e}")

    async def background_group_joiner(self):
        while True:
            # ⏸️ Check if group joining is enabled
            join_enabled = self.db.get_setting("group_join_enabled", "on")
            if join_enabled != "on":
                print("[Joiner] Paused by user. Skipping this cycle.")
                self.last_group_join_time = None  # 🔧 Stop the countdown
                self.db.set_setting("last_group_join_time", "")  # 🔧 Clear saved value
                await asyncio.sleep(60)
                continue

            # 🔐 Check subscription validity
            active, _ = self.get_subscription_status()
            if not active:
                print("[Subscription] Expired. Stopping background group joiner.")
                self.db.log_event("INFO", "Subscription expired. Group joiner stopped.")
                admin_user_id = self.ADMIN_USER_ID
                self.progress_containers.pop(admin_user_id, None)
                self.last_group_join_time = None
                self.db.set_setting("last_group_join_time", "")
                return  # ⛔ Stop the loop completely

            # 🧭 Get the first pending group (status='pending')
            pending = self.db.execute_query(
                "SELECT id, group_link FROM pending_joins WHERE status = 'pending' ORDER BY id LIMIT 1"
            )

            if pending:
                group_id, group_link = pending[0]
                print(f"[JoinQueue] Attempting to join: {group_link}")
                success, msg = await self.userbot_manager.join_group(group_link)
                new_status = "joined" if success else "failed"
                self.db.execute_query(
                    "UPDATE pending_joins SET status = ? WHERE id = ?",
                    (new_status, group_id)
                )
                print(f"[JoinQueue] {group_link} => {msg}")

                # 🕒 Persist join time
                now = time.time()
                self.last_group_join_time = now
                self.db.set_setting("last_group_join_time", str(now))

                # 📡 Update UI for admin
                admin_user_id = self.ADMIN_USER_ID
                await self.update_group_join_progress(admin_user_id)

                await asyncio.sleep(300)  # Wait 5 minutes before next attempt

            else:
                # 🧹 All joins done, clean up UI & timer
                admin_user_id = self.ADMIN_USER_ID
                self.progress_containers.pop(admin_user_id, None)
                self.last_group_join_time = None
                self.db.set_setting("last_group_join_time", "")
                await asyncio.sleep(60)  # Retry in 1 minute

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages."""
        user_id = update.effective_user.id
        support_status = self.db.get_setting("support_access", "off")

        # Allow admin OR support agent who is in a support flow (add/subtract)
        flow_type = self.temp_data.get(user_id, {}).get("flow_type", "")
        if not (
            user_id == self.ADMIN_USER_ID or 
            (user_id in SUPPORT_USER_IDS and support_status == "off" and flow_type.startswith("support_"))
        ):
            await update.message.reply_text("❌ Unauthorized access.")
            return

        user_id = update.effective_user.id
        message_text = update.message.text

        # Handle different flows based on stored state
        if user_id in self.temp_data:
            flow_type = self.temp_data[user_id].get("flow_type")

            if flow_type == "userbot_add":
                await self.handle_userbot_add_flow(update, context, message_text)

            elif flow_type == "groups_add":
                await self.handle_groups_add_flow(update, context, message_text)

            elif flow_type == "message_add":
                await self.handle_message_add_flow(update, context, message_text)

            elif flow_type == "afk_set":
                afk_message = message_text.strip()
                self.db.set_setting("afk_message", afk_message)
                del self.temp_data[user_id]
                await update.message.reply_text(
                    "✅ *AFK message updated!*",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Back", callback_data="settings_afk")]]
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            elif flow_type == "set_cycle_length":
                try:
                    cycle = int(message_text.strip())
                    if cycle < 10:
                        await update.message.reply_text(
                            "❌ Minimum allowed cycle length is 10 minutes.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔁 Retry", callback_data="set_cycle_length")],
                                [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                            ])
                        )
                    else:
                        self.db.set_setting("cycle_length", str(cycle))
                        await update.message.reply_text(
                            f"✅ Cycle length set to {cycle} minutes.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                            ])
                        )
                        del self.temp_data[user_id]
                except ValueError:
                    await update.message.reply_text(
                        "❌ Invalid input. Please enter a number.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔁 Retry", callback_data="set_cycle_length")],
                            [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                        ])
                    )
                        
            elif flow_type == "support_add":
                try:
                    minutes = int(message_text.strip())
                    current = int(self.db.get_setting("subscription_duration", "0"))
                    new_value = max(0, current + minutes)
                    self.db.set_setting("subscription_duration", str(new_value))
                    self.temp_data.pop(user_id, None)

                    await update.message.reply_text(
                        f"✅ {minutes} minutes added successfully.\n\nNew Duration: `{new_value}` minutes.",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Back to Support Panel", callback_data="support_menu")]
                        ])
                    )

                except ValueError:
                    await update.message.reply_text("❌ Invalid input. Please send a number.")

            elif flow_type == "support_subtract":
                try:
                    minutes = int(message_text.strip())
                    current = int(self.db.get_setting("subscription_duration", "0"))
                    new_value = max(0, current - minutes)
                    self.db.set_setting("subscription_duration", str(new_value))
                    self.temp_data.pop(user_id, None)

                    await update.message.reply_text(
                        f"✅ {minutes} minutes subtracted successfully.\n\nNew Duration: `{new_value}` minutes.",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Back to Support Panel", callback_data="support_menu")]
                        ])
                    )

                except ValueError:
                    await update.message.reply_text("❌ Invalid input. Please send a number.")

            elif flow_type == "set_hold_duration":
                try:
                    hold = int(message_text.strip())
                    if hold < 15:
                        await update.message.reply_text(
                            "❌ Minimum hold duration is 15 minutes.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔁 Retry", callback_data="set_hold_duration")],
                                [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                            ])
                        )
                    else:
                        # Save to DB
                        self.db.set_setting("hold_duration", str(hold))

                        # 🆕 If forwarding is running, update its timing immediately
                        if self.forwarding_manager and self.forwarding_manager.is_running:
                            now = datetime.now()
                            self.forwarding_manager.phase_start_time = now

                            if self.forwarding_manager.current_phase == "holding":
                                self.forwarding_manager.stats["next_forward_time"] = now + timedelta(minutes=hold)
                            else:
                                # Forwarding phase continues unchanged
                                pass

                            print("[Forwarding] Hold duration updated live.")

                        # Notify user
                        await update.message.reply_text(
                            f"✅ Hold duration set to {hold} minutes.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔙 Back to Controls", callback_data="forwarding_controls")]
                            ])
                        )
                        del self.temp_data[user_id]

                except ValueError:
                    await update.message.reply_text(
                        "❌ Invalid input. Please enter a number.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔁 Retry", callback_data="set_hold_duration")],
                            [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                        ])
                    )

            elif flow_type == "alter_delay":
                try:
                    delay = int(message_text.strip())
                    if delay < 30:
                        await update.message.reply_text(
                            "❌ Minimum allowed delay is 30 seconds.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔁 Retry", callback_data="settings_alter_delay")],
                                [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                            ])
                        )
                    else:
                        self.db.set_setting("per_message_delay", str(delay))
                        await update.message.reply_text(
                            f"✅ Delay set to {delay} seconds.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                            ])
                        )
                        del self.temp_data[user_id]
                except ValueError:
                    await update.message.reply_text(
                        "❌ Invalid input. Please enter a number.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔁 Retry", callback_data="settings_alter_delay")],
                            [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
                        ])
                    )
                return

        # --- AFK auto-reply for private DMs ---
        afk_enabled = self.db.get_setting("afk_enabled", "off")
        afk_message = self.db.get_setting("afk_message", "")
        OWNER_ID = 7538794191   # <-- your Telegram user ID

        if (
            afk_enabled == "on"
            and update.effective_chat.type == "private"
            and update.effective_user.id != OWNER_ID
        ):
            if afk_message:
                await update.message.reply_text(afk_message)
    
    # Userbot Menu Methods
    async def show_userbot_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show userbot management menu (context-aware)."""
        # ✅ Check subscription status
        if not await self.ensure_subscription_valid(update, context):
            return
        is_connected = self.userbot_manager.is_authenticated

        keyboard = []
        if is_connected:
            # Only show remove/status when connected
            keyboard.append([InlineKeyboardButton("❌ Remove Userbot", callback_data="userbot_remove")])
            keyboard.append([InlineKeyboardButton("ℹ️ Status", callback_data="userbot_status")])
        else:
            # Only show add when NOT connected
            keyboard.append([InlineKeyboardButton("➕ Add Userbot", callback_data="userbot_add")])

        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_main")])

        text = "🛠 *Userbot Management*\n\nSelect an option:"
        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="MarkdownV2"
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                # Ignore the harmless error
                pass
            else:
                raise
    
    async def start_userbot_add_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start userbot addition flow."""
        self.temp_data[update.effective_user.id] = {
            "flow_type": "userbot_add",
            "step": "api_id"
        }
        
        text = "🔧 **Add Userbot**\n\nPlease provide your API ID:"
        try:
            await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)
        except BadRequest as e:
            if "Message is not modified" in str(e):
                # Silently ignore this harmless error
                pass
            else:
                raise

    async def handle_userbot_add_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_text: str):
        """Handle userbot addition flow."""
        user_id = update.effective_user.id
        flow_data = self.temp_data[user_id]
        step = flow_data["step"]

        if step == "api_id":
            try:
                api_id = int(message_text)
                flow_data["api_id"] = api_id
                flow_data["step"] = "api_hash"

                await update.message.reply_text(
                    "✅ API ID received.\n\nNow please provide your API Hash:",
                    parse_mode=ParseMode.MARKDOWN
                )
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid API ID. Please provide a valid number.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔁 Retry API ID", callback_data="userbot_retry_api_id")],
                        [InlineKeyboardButton("❌ Cancel", callback_data="back_main")]
                    ])
                )

        elif step == "api_hash":
            api_hash = message_text.strip()

            # 🛡 Validate hash: must be 32-char hex
            import re
            if not re.fullmatch(r"[a-fA-F0-9]{32}", api_hash):
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔁 Retry API Hash", callback_data="userbot_retry_api_hash")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="back_main")]
                ])
                await update.message.reply_text(
                    "❌ *Invalid API Hash.*\nIt must be a 32-character hexadecimal string (0–9, a–f).\n\nPlease try again.",
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN
                    
                )
                return

            flow_data["api_hash"] = api_hash
            flow_data["step"] = "phone_number"

            await update.message.reply_text(
                "✅ API Hash received.\n\nNow please provide your phone number (with country code, e.g., +1234567890):",
                parse_mode=ParseMode.MARKDOWN
            )

        elif step == "phone_number":
            flow_data["phone_number"] = message_text
            flow_data["step"] = "code"

            success, message = await self.userbot_manager.add_userbot(
                flow_data["api_id"],
                flow_data["api_hash"],
                message_text
            )

            if success:
                await update.message.reply_text(
                    f"✅ {message}\n\nPlease enter the verification code:",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔁 Retry Phone Number", callback_data="userbot_retry_phone")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="back_main")]
                ])
                await update.message.reply_text(f"❌ {message}", reply_markup=keyboard)

        elif step == "code":
            flow_data["code"] = message_text

            success, message = await self.userbot_manager.verify_code(
                flow_data["phone_number"],
                message_text
            )

            if success:
                await update.message.reply_text(
                    f"✅ {message}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Main Menu", callback_data="back_main")]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                del self.temp_data[user_id]
                await self.userbot_manager.fetch_groups()

            elif "Two-factor authentication required" in message:
                flow_data["step"] = "password"
                await update.message.reply_text(
                    "🔐 Two-factor authentication required.\n\nPlease enter your password:",
                    parse_mode=ParseMode.MARKDOWN
                )

            else:
                # ❌ Invalid code or error — offer retry
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔁 Retry Code", callback_data="userbot_retry_code")],
                    [InlineKeyboardButton("📲 Resend Code", callback_data="userbot_resend_code")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="back_main")]
                ])
                await update.message.reply_text(f"❌ {message}", reply_markup=keyboard)

        elif step == "password":
            success, message = await self.userbot_manager.verify_code(
                flow_data["phone_number"],
                flow_data["code"],
                message_text
            )

            if success:
                await update.message.reply_text(
                    f"✅ {message}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Main Menu", callback_data="back_main")]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                await self.userbot_manager.fetch_groups()
            else:
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔁 Retry Password", callback_data="userbot_retry_password")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="back_main")]
                ])
                await update.message.reply_text(f"❌ {message}", reply_markup=keyboard)

            del self.temp_data[user_id]
    
    async def userbot_remove_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show userbot removal confirmation."""
        keyboard = [
            [InlineKeyboardButton("✅ Yes, Remove", callback_data="userbot_remove_confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="menu_userbot")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        text = "⚠️ **Confirm Userbot Removal**\n\nThis will remove the userbot session and all associated data. Are you sure?"
        
        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                # Ignore this harmless error
                pass
            else:
                raise
    
    async def userbot_remove_execute(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Execute userbot removal."""
        success = await self.userbot_manager.remove_userbot()
        
        if success:
            text = "✅ **Userbot Removed**\n\nUserbot session has been successfully removed."
        else:
            text = "❌ **Error**\n\nFailed to remove userbot session."
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu_userbot")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                # Ignore this harmless error
                pass
            else:
                raise
    
    async def show_userbot_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        status = await self.userbot_manager.get_status()

        if not self.userbot_manager.is_authenticated:
            status_text = escape_markdown("❌ Disconnected", version=2)
            user_info = escape_markdown("Not authenticated", version=2)
            last_activity = escape_markdown("Never", version=2)

            text = f"""📊 *Userbot Status*

    *Status:* {status_text}
    *User:* {user_info}
    *Last Activity:* {last_activity}
    """
        else:
            me = await self.userbot_manager.client.get_me()
            user_info = f"@{me.username}" if me.username else f"{me.first_name} {me.last_name or ''}".strip()
            user_info = escape_markdown(user_info, version=2)
            status_text = escape_markdown("✅ Connected", version=2)
            premium = "✅ Yes" if getattr(me, "premium", False) else "❌ No"
            premium = escape_markdown(premium, version=2)
            user_id = escape_markdown(str(me.id), version=2)
            phone = escape_markdown(getattr(me, "phone", "Unknown"), version=2)
            last_activity = escape_markdown(str(status.get("last_activity", "Unknown")), version=2)

            text = f"""📊 *Userbot Status*

    *Status:* {status_text}
    *User:* `{user_info}`
    *Premium:* {premium}
    *User ID:* `{user_id}`
    *Phone:* `{phone}`
    *Last Activity:* `{last_activity}`
    """

        keyboard = [
            [InlineKeyboardButton("🔄 Refresh", callback_data="userbot_status")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_userbot")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise


    
    # Groups Menu Methods
    async def show_groups_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show groups management menu."""
        
        # ✅ 1. Check subscription validity
        if not await self.ensure_subscription_valid(update, context):
            return

        keyboard = [
            [InlineKeyboardButton("➕ Add to Groups", callback_data="groups_add")],
            [InlineKeyboardButton("📋 View Groups", callback_data="groups_view")],
            [InlineKeyboardButton("❌ Remove from Group", callback_data="groups_remove")],
            [InlineKeyboardButton("🔄 Processes", callback_data="groups_processes")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        text = "👥 **Groups Management**\n\nSelect an option:"

        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                raise
    
    async def start_groups_add_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start groups addition flow."""
        self.temp_data[update.effective_user.id] = {
            "flow_type": "groups_add",
            "groups_to_add": [],
            "current_index": 0
        }
        
        text = """➕ **Add to Groups**

You can add multiple groups/channels. Supported formats:
• `@username`
• `https://t.me/username`
• `https://t.me/joinchat/invite_hash`

Please send the first group link or username:"""
        
        await update.callback_query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="menu_groups")]
            ])
        )
    
    async def handle_groups_add_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_text: str):
        """Handle groups addition flow, now supports multi-line input and pagination preview, with validation and user feedback."""
        from telegram.helpers import escape_markdown

        user_id = update.effective_user.id
        flow_data = self.temp_data[user_id]

        # Split lines, trim, filter non-empty
        new_links = [line.strip() for line in message_text.strip().splitlines() if line.strip()]
        existing = set(flow_data.get("groups_to_add", []))
        invalid = []
        added = []

        for link in new_links:
            # Basic Telegram link validation
            if link in existing or not (link.startswith('@') or 't.me/' in link):
                invalid.append(link)
                continue
            flow_data.setdefault("groups_to_add", []).append(link)
            existing.add(link)
            added.append(link)

        # Build feedback message
        msg_lines = []
        if added:
            msg_lines.append("✅ *Added:* " + ", ".join([escape_markdown(l, version=2) for l in added]))
        if invalid:
            msg_lines.append("❌ *Invalid or duplicate:* " + ", ".join([escape_markdown(l, version=2) for l in invalid]))

        # Always show back/cancel button!
        await update.message.reply_text(
            "\n".join(msg_lines) if msg_lines else "Nothing new added.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu_groups")]]),
            parse_mode="MarkdownV2",
            disable_web_page_preview=True
        )

        # Save current page (if not already set)
        flow_data.setdefault("current_page", 0)

        # Show the group preview with all entered so far
        await self.send_group_preview(update, context, user_id)


    async def send_group_preview(self, update, context, user_id, edit=False):
        flow_data = self.temp_data[user_id]
        groups = flow_data["groups_to_add"]
        page = flow_data.get("current_page", 0)
        page_size = 10
        total_pages = (len(groups) + page_size - 1) // page_size
        start = page * page_size
        end = start + page_size
        page_groups = groups[start:end]

        # Escape the list number, the dot, and the group
        preview = "\n".join([
            f"{escape_markdown(str(start + idx + 1), version=2)}\\."
            f" {escape_markdown(g, version=2)}"
            for idx, g in enumerate(page_groups)
        ]) or escape_markdown("_No groups entered yet._", version=2)

        # ESCAPE ALL STATIC TEXT and any punctuation!
        title = escape_markdown("📝 Confirm Groups to Join", version=2)
        page_str = escape_markdown(f"(Page {page+1}/{total_pages or 1})", version=2)
        instructions = escape_markdown(
            "Paste all group links/usernames above, review, then press ✅ to confirm or add more.",
            version=2
        )

        text = f"{title} {page_str}\n\n{preview}\n\n{instructions}"

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data="groups_prevpage"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data="groups_nextpage"))

        keyboard = []
        if nav:
            keyboard.append(nav)
        keyboard.append([
            InlineKeyboardButton("✅ Confirm Join", callback_data="groups_confirm_join"),
            InlineKeyboardButton("❌ Cancel", callback_data="menu_groups"),
        ])
        reply_markup = InlineKeyboardMarkup(keyboard)

        if edit and update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    text, reply_markup=reply_markup, parse_mode="MarkdownV2"
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    # Ignore harmless error
                    pass
                else:
                    raise
        else:
            await update.message.reply_text(
                text, reply_markup=reply_markup, parse_mode="MarkdownV2"
            )

    
    async def start_group_joining_process(self, update: Update, context: ContextTypes.DEFAULT_TYPE, groups_list: List[str]):
        """Start the group joining process with progress updates."""
        successful_joins = 0
        total_groups = len(groups_list)

        # Create progress message
        progress_text = (
            f"🔄 **Joining Groups Progress**\n\n"
            f"**Total Groups:** {total_groups}\n"
            f"**Current:** Starting...\n"
            f"**Status:** Initializing"
        )

        if update.message:
            message = await update.message.reply_text(progress_text, parse_mode=ParseMode.MARKDOWN)
        else:
            # For CallbackQuery context
            message = await update.callback_query.message.reply_text(progress_text, parse_mode=ParseMode.MARKDOWN)

        for i, group_link in enumerate(groups_list, 1):
            # Update progress
            progress_text = (
                f"🔄 **Joining Groups Progress**\n\n"
                f"**Total Groups:** {total_groups}\n"
                f"**Current:** {i}/{total_groups}\n"
                f"**Joining:** {group_link}\n"
                f"**Status:** Attempting to join..."
            )

            await message.edit_text(progress_text, parse_mode=ParseMode.MARKDOWN)

            # Attempt to join
            success, result_message = await self.userbot_manager.join_group(group_link)
            if success:
                successful_joins += 1

            # Print to your console for debug
            print(f"[Join Debug] {group_link} — {success} — {result_message}")

            # Update with result
            status_emoji = "✅" if success else "❌"
            progress_text = (
                f"🔄 **Joining Groups Progress**\n\n"
                f"**Total Groups:** {total_groups}\n"
                f"**Current:** {i}/{total_groups}\n"
                f"**Last Group:** {group_link}\n"
                f"**Result:** {status_emoji} {result_message}\n\n"
                f"{'⏳ Waiting 5 minutes before next join...' if i < total_groups else '🎉 Process completed!'}"
            )

            await message.edit_text(progress_text, parse_mode=ParseMode.MARKDOWN)

            # Wait 10 minutes between joins (except for the last one)
            if i < total_groups:
                await asyncio.sleep(600)  # 10 minutes

        # Final message
        await message.edit_text(
            f"🎉 **Group Joining Completed!**\n\n"
            f"Tried to join {total_groups} groups.\n"
            f"✅ Successfully joined {successful_joins} group(s).\n"
            f"❌ Failed to join {total_groups - successful_joins} group(s).\n"
            "Check the groups list to see results.",
            parse_mode=ParseMode.MARKDOWN
        )

        # Refresh groups list
        await self.userbot_manager.fetch_groups()

    
    async def show_groups_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
        """Show groups list with pagination and chat type labeling."""
        groups = self.db.execute_query(
            "SELECT chat_id, title, username, chat_type, is_selected FROM groups ORDER BY title"
        )
        
        if not groups:
            text = "📋 **Groups List**\n\nNo groups found. Add some groups first!"
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu_groups")]]
        else:
            page_size = 10
            total_pages = (len(groups) + page_size - 1) // page_size
            start_idx = page * page_size
            end_idx = min(start_idx + page_size, len(groups))
            page_groups = groups[start_idx:end_idx]

            type_map = {
                "channel": "📢 Channel",
                "basic_group": "👥 Group",
                "supergroup": "👥 Supergroup",
                "forum": "🧵 Forum",
                "unknown": "❓ Unknown"
            }

            groups_text = ""
            keyboard = []

            for group in page_groups:
                chat_id, title, username, chat_type, is_selected = group
                status_emoji = "✅" if is_selected else "❌"
                safe_title = escape_markdown(str(title), version=2)
                safe_username = escape_markdown(str(username), version=2) if username else "Private"
                safe_chat_type = escape_markdown(type_map.get(chat_type, "❓"), version=2)

                groups_text += f"{status_emoji} *{safe_title}*\n   {safe_username} • {safe_chat_type}\n\n"

                # Toggle button
                button_label = f"{status_emoji} {title[:30]}..."
                keyboard.append([
                    InlineKeyboardButton(button_label, callback_data=f"group_toggle_{chat_id}")
                ])

            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"groups_page_{page-1}"))
            if page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"groups_page_{page+1}"))
            if nav_buttons:
                keyboard.append(nav_buttons)

            keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="menu_groups")])

            text = f"""📋 **Groups List** (Page {page + 1}/{total_pages})

    {groups_text}

    **Legend:**
    ✅ = Selected for forwarding
    ❌ = Not selected

    Click a group to toggle selection:"""

        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # harmless
            else:
                raise
    
    async def toggle_group_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
        """Toggle group selection for forwarding."""
        chat_id = int(callback_data.split("_")[-1])
        
        # Toggle selection
        current_selection = self.db.execute_query(
            "SELECT is_selected FROM groups WHERE chat_id = ?", (chat_id,)
        )
        
        if current_selection:
            new_selection = 1 - current_selection[0][0]
            self.db.execute_query(
                "UPDATE groups SET is_selected = ? WHERE chat_id = ?",
                (new_selection, chat_id)
            )
        
        # Refresh the groups list
        await self.show_groups_list(update, context)
    
    

    async def safe_edit_message_text(self, cbq, *args, **kwargs):
        try:
            await cbq.edit_message_text(*args, **kwargs)
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Silently ignore harmless error
            else:
                raise
    
    # Forwarding Menu Methods
    async def show_forwarding_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show forwarding management menu."""
        # ✅ Check subscription status
        if not await self.ensure_subscription_valid(update, context):
            return
        keyboard = [
            [InlineKeyboardButton("✏️ Manage Messages", callback_data="forwarding_messages")],
            [InlineKeyboardButton("⚙️ Forwarding Controls", callback_data="forwarding_controls")],
            [InlineKeyboardButton("📑 Manage Destinations", callback_data="forwarding_destinations")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        text = "📤 **Forwarding Management**\n\nSelect an option:"
        
        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                # Ignore harmless error
                pass
            else:
                raise
    
    async def show_forwarding_messages_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show forwarding messages management menu."""
        messages = self.forwarding_manager.get_forwarding_messages()
        
        keyboard = []
        messages_text = ""
        
        if messages:
            for i, msg in enumerate(messages, 1):
                messages_text += f"{i}. {msg['link']}\n"
                keyboard.append([
                    InlineKeyboardButton(
                        f"❌ Remove #{i}",
                        callback_data=f"message_remove_{msg['id']}"
                    )
                ])
        else:
            messages_text = "No forwarding messages configured."
        
        if len(messages) < 2:
            keyboard.append([InlineKeyboardButton("➕ Add Message", callback_data="message_add")])
        
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="menu_forwarding")])
        
        text = f"""✏️ **Forwarding Messages** ({len(messages)}/2)

        {messages_text}

        ⚠️ _Note: Tier 1 subscription allows only 1 active message._
        Please add only one message.  
        Thanks for your attention!

        Select an action:"""

        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                # Ignore harmless error
                pass
            else:
                raise
    
    async def start_message_add_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start message addition flow."""
        self.temp_data[update.effective_user.id] = {
            "flow_type": "message_add"
        }
        
        text = """➕ **Add Forwarding Message**

Please send the message link in this format:
`https://t.me/channel_name/message_id`

Example: `https://t.me/mychannel/123`"""
        
        try:
            await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Ignore harmless error
            else:
                raise
    
    async def handle_message_add_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_text: str):
        """Handle message addition flow."""
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        user_id = update.effective_user.id

        success, message = await self.forwarding_manager.add_forwarding_message(message_text)

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Forwarding", callback_data="menu_forwarding")]
        ])

        if success:
            await update.message.reply_text(f"✅ {message}", reply_markup=keyboard)
        else:
            await update.message.reply_text(f"❌ {message}", reply_markup=keyboard)

        if user_id in self.temp_data:
            del self.temp_data[user_id]
    
    async def remove_forwarding_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
        """Remove a forwarding message."""
        message_id = int(callback_data.split("_")[-1])
        
        success = self.forwarding_manager.remove_forwarding_message(message_id)
        
        if success:
            await update.callback_query.answer("✅ Message removed successfully!")
        else:
            await update.callback_query.answer("❌ Failed to remove message.")
        
        # Refresh the messages menu
        await self.show_forwarding_messages_menu(update, context)
    
    async def show_forwarding_controls_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show forwarding controls menu."""
        status = self.forwarding_manager.get_status()

        # Phase details
        current_phase = getattr(self.forwarding_manager, "current_phase", "N/A").capitalize()
        next_event = status["next_forward_in"] or "N/A"
        hold_duration = self.db.get_setting("hold_duration", "15")
        per_msg_delay = self.db.get_setting("per_message_delay", "30")
        cycle_length = self.db.get_setting("cycle_length", "120")

        # Build status text
        if status["is_running"]:
            if status["is_paused"]:
                status_text = "⏸️ **Paused**"
                control_buttons = [
                    InlineKeyboardButton("▶️ Resume", callback_data="forwarding_resume"),
                    InlineKeyboardButton("⏹️ Stop", callback_data="forwarding_stop")
                ]
            else:
                status_text = "▶️ **Running**"
                control_buttons = [
                    InlineKeyboardButton("⏸️ Pause", callback_data="forwarding_pause"),
                    InlineKeyboardButton("⏹️ Stop", callback_data="forwarding_stop")
                ]
        else:
            status_text = "⏹️ **Stopped**"
            control_buttons = [
                InlineKeyboardButton("▶️ Start", callback_data="forwarding_start")
            ]

        keyboard = [
            control_buttons,
            [InlineKeyboardButton("⏱ Per-Message Delay", callback_data="settings_alter_delay")],
            [InlineKeyboardButton("🕒 Set Hold Duration", callback_data="set_hold_duration")],
            [InlineKeyboardButton("♻️ Set Cycle Length", callback_data="set_cycle_length")],
            [InlineKeyboardButton("👁 Preview Plan", callback_data="forwarding_preview")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="forwarding_controls")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_forwarding")]
        ]

        text = f"""⚙️ **Forwarding Controls**

    **Status:** {status_text}
    **Total Forwarded:** {status['total_forwarded']}
    **Errors / Info:** {status['errors']}
    **Selected Groups:** {status['selected_groups']}
    **Forwarding Messages:** {status['forwarding_messages']}
    **Cycle Phase:** {current_phase}
    **Next Event:** {next_event}
    **Cycle Length:** {cycle_length} minutes
    **Hold Duration:** {hold_duration} minutes
    **Per-Message Delay:** {per_msg_delay} seconds
    **Last Forward:** {status['last_forward_time'] or 'Never'}

    Select an action:"""

        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                raise
            
    async def start_forwarding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start forwarding process."""
        success, message = await self.forwarding_manager.start_forwarding()
        
        await update.callback_query.answer(message)
        await self.show_forwarding_controls_menu(update, context)
    
    async def stop_forwarding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Stop forwarding process."""
        success, message = self.forwarding_manager.stop_forwarding()
        
        await update.callback_query.answer(message)
        await self.show_forwarding_controls_menu(update, context)
    
    async def pause_forwarding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Pause forwarding process."""
        success, message = self.forwarding_manager.pause_forwarding()
        
        await update.callback_query.answer(message)
        await self.show_forwarding_controls_menu(update, context)
    
    async def resume_forwarding(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Resume forwarding process."""
        success, message = self.forwarding_manager.resume_forwarding()
        
        await update.callback_query.answer(message)
        await self.show_forwarding_controls_menu(update, context)
    
    
    async def show_forwarding_destinations_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show forwarding destinations (same as groups list)."""
        await self.show_groups_list(update, context)
    
    # Status Menu Methods
    async def show_status_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show system status menu."""
    # ✅ Check subscription status
        if not await self.ensure_subscription_valid(update, context):
            return

        keyboard = [
            [InlineKeyboardButton("📈 Live Status", callback_data="status_live")],
            [InlineKeyboardButton("📊 Live Trace", callback_data="status_trace")],
            [InlineKeyboardButton("📄 Logs", callback_data="status_logs")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        text = "📊 **System Status**\n\nSelect an option:"
        
        try:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # harmless, ignore
            else:
                raise
    
    async def show_live_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show live system status."""
        # Get userbot status
        userbot_status = await self.userbot_manager.get_status()
        forwarding_status = self.forwarding_manager.get_status()
        groups_count = self.db.execute_query("SELECT COUNT(*) FROM groups")[0][0]
        selected_groups = self.db.execute_query("SELECT COUNT(*) FROM groups WHERE is_selected = 1")[0][0]
        messages_count = self.db.execute_query("SELECT COUNT(*) FROM forwarding_messages WHERE is_active = 1")[0][0]

        # Escape all dynamic/user content!
        userbot_status_text = escape_markdown(
            "✅ Connected" if userbot_status["status"] == "✅ Connected" else "❌ Disconnected", version=2)
        user_info = escape_markdown(str(userbot_status.get("user_info", "")), version=2)
        last_activity = escape_markdown(str(userbot_status.get("last_activity", "")), version=2)

        if forwarding_status["is_running"]:
            forwarding_status_text = escape_markdown(
                "⏸️ Paused" if forwarding_status["is_paused"] else "▶️ Running", version=2)
        else:
            forwarding_status_text = escape_markdown("⏹️ Stopped", version=2)

        total_forwarded = escape_markdown(str(forwarding_status["total_forwarded"]), version=2)
        errors = escape_markdown(str(forwarding_status["errors"]), version=2)
        last_forward_time = escape_markdown(
            str(forwarding_status["last_forward_time"] or "Never"), version=2)

        groups_count_str = escape_markdown(str(groups_count), version=2)
        selected_groups_str = escape_markdown(str(selected_groups), version=2)
        messages_count_str = escape_markdown(str(messages_count), version=2)

        last_update = escape_markdown(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), version=2)

        text = (
            f"{escape_markdown('📈 Live System Status', version=2)}\n\n"
            f"{escape_markdown('🛠 Userbot:', version=2)}\n"
            f"Status: {userbot_status_text}\n"
            f"User: {user_info}\n"
            f"Last Activity: {last_activity}\n\n"
            f"{escape_markdown('📤 Forwarding:', version=2)}\n"
            f"Status: {forwarding_status_text}\n"
            f"Total Forwarded: {total_forwarded}\n"
            f"Errors: {errors}\n"
            f"Last Forward: {last_forward_time}\n\n"
            f"{escape_markdown('👥 Groups:', version=2)}\n"
            f"Total Groups: {groups_count_str}\n"
            f"Selected for Forwarding: {selected_groups_str}\n\n"
            f"{escape_markdown('📝 Messages:', version=2)}\n"
            f"Forwarding Messages: {messages_count_str}/2\n\n"
            f"{escape_markdown('⏰ Last Update:', version=2)} {last_update}"
        )

        MAX_LEN = 3900
        if len(text) > MAX_LEN:
            text = text[:MAX_LEN] + escape_markdown("\n...truncated...", version=2)

        keyboard = [
            [InlineKeyboardButton("🔄 Refresh", callback_data="status_live")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_status")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await update.callback_query.edit_message_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                raise
    
    async def show_forwarding_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show live preview of forwarding plan."""
        messages = self.forwarding_manager.get_forwarding_messages()
        groups = self.forwarding_manager.get_selected_groups()
        status = self.forwarding_manager.get_status()

        lines = [escape_markdown("📤 Forwarding Preview", version=2), ""]

        if not messages:
            lines.append(escape_markdown("No messages added.", version=2))
        else:
            for i, m in enumerate(messages, start=1):
                link = escape_markdown(m.get("link", "N/A"), version=2)
                lines.append(f"{i}\\: {link}")

        lines.append("")
        if not groups:
            lines.append(escape_markdown("No destination groups selected.", version=2))
        else:
            lines.append(escape_markdown(f"{len(groups)} groups selected:", version=2))
            for g in groups[:5]:
                title = escape_markdown(g.get("title", "Unnamed"), version=2)
                lines.append(f"• {title}")
            if len(groups) > 5:
                lines.append(escape_markdown(f"...and {len(groups)-5} more", version=2))

        lines.append("")
        status_text = escape_markdown("▶️ Running" if status.get("is_running") else "⏹️ Stopped", version=2)
        lines.append(f"Status: {status_text}")
        lines.append(f"Total Forwarded: {escape_markdown(str(status.get('total_forwarded', 0)), version=2)}")
        lines.append(f"Errors: {escape_markdown(str(status.get('errors', 0)), version=2)}")

        last_time = status.get("last_forward_time")
        last_time_str = escape_markdown(str(last_time), version=2) if last_time else "Never"
        lines.append(f"Last Forward: {last_time_str}")

        text = "\n".join(lines).strip()

        keyboard = [
            [InlineKeyboardButton("🔄 Refresh", callback_data="forwarding_preview")],
            [InlineKeyboardButton("🔙 Back", callback_data="forwarding_controls")]
        ]
        new_markup = InlineKeyboardMarkup(keyboard)
        new_markup_json = json.dumps(new_markup.to_dict(), sort_keys=True)

        current_text = update.callback_query.message.text.strip()
        current_markup = update.callback_query.message.reply_markup
        current_markup_json = json.dumps(current_markup.to_dict(), sort_keys=True) if current_markup else ""

        # Safe comparison
        text_changed = text != current_text
        markup_changed = new_markup_json != current_markup_json

        try:
            if text_changed:
                await update.callback_query.edit_message_text(
                    text=text,
                    reply_markup=new_markup,
                    parse_mode="MarkdownV2"
                )
            elif markup_changed:
                await update.callback_query.edit_message_reply_markup(
                    reply_markup=new_markup
                )
        except Exception as e:
            # Log silently if the error is 'Message is not modified'
            if "Message is not modified" not in str(e):
                raise e

    async def show_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
        """Show system logs with pagination."""
        PAGE_SIZE = 20
        offset = page * PAGE_SIZE

        logs = self.db.execute_query(
            "SELECT level, message, timestamp FROM logs ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            (PAGE_SIZE, offset)
        )

        total_logs = self.db.fetch_one("SELECT COUNT(*) FROM logs")[0]
        total_pages = (total_logs + PAGE_SIZE - 1) // PAGE_SIZE

        if not logs:
            logs_text = escape_markdown("No logs found.", version=2)
        else:
            logs_text = ""
            for level, message, timestamp in logs:
                emoji = "❌" if level == "ERROR" else "ℹ️"
                logs_text += (
                    f"{emoji} "
                    f"`{escape_markdown(str(timestamp), version=2)}`"
                    f" \\- "
                    f"{escape_markdown(str(message), version=2)}\n"
                )

        text = (
            f"{escape_markdown(f'📄 System Logs (Page {page + 1} of {total_pages})', version=2)}\n\n"
            f"{logs_text}\n"
            f"{escape_markdown('Legend:', version=2)}\n"
            f"{escape_markdown('❌ = Error', version=2)}\n"
            f"{escape_markdown('ℹ️ = Info', version=2)}"
        )

        MAX_LEN = 3900
        if len(text) > MAX_LEN:
            text = text[:MAX_LEN] + escape_markdown("\n...truncated...", version=2)

        # Pagination buttons
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"logs_page_{page - 1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"logs_page_{page + 1}"))

        keyboard = [
            nav_buttons if nav_buttons else [],
            [InlineKeyboardButton("🔄 Refresh", callback_data=f"logs_page_{page}")],
            [InlineKeyboardButton("🗑️ Clear Logs", callback_data="logs_clear")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_status")]
        ]
        reply_markup = InlineKeyboardMarkup([row for row in keyboard if row])  # Remove empty rows

        if hasattr(update, "callback_query") and update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    text, reply_markup=reply_markup, parse_mode="MarkdownV2"
                )
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    pass
                else:
                    raise
        else:
            await update.message.reply_text(
                text, reply_markup=reply_markup, parse_mode="MarkdownV2"
            )
    async def show_live_trace(self, update, context, page=0):
        """Show recent forwarded traces (only from last 1 hour)."""
        self.db.execute_query(
            "DELETE FROM forward_trace WHERE forwarded_at < datetime('now', '-1 hour')"
        )

        all_traces = self.db.execute_query(
            "SELECT group_title, chat_id, message_id, forwarded_at FROM forward_trace ORDER BY forwarded_at DESC"
        )

        per_page = 10
        total_pages = (len(all_traces) + per_page - 1) // per_page
        traces_page = all_traces[page * per_page : (page + 1) * per_page]

        if not traces_page:
            text = "⚠️ *No trace records found in the last 1 hour.*"
            parse_mode = ParseMode.MARKDOWN
        else:
            text = escape_markdown("📡 Live Trace Logs (past 1 hour)", version=2) + "\n\n"
            for title, chat_id, msg_id, ts in traces_page:
                try:
                    clean_chat_id = str(chat_id).replace("-100", "")
                    raw_link = f"https://t.me/c/{clean_chat_id}/{msg_id}"
                    link = quote(raw_link, safe=':/')

                    time_obj = datetime.fromisoformat(ts)
                    if time_obj.tzinfo is None:
                        time_obj = time_obj.replace(tzinfo=timezone.utc).astimezone()
                    else:
                        time_obj = time_obj.astimezone()

                    ago_delta = datetime.now().astimezone() - time_obj
                    if ago_delta < timedelta(minutes=1):
                        ago = "Just now"
                    elif ago_delta < timedelta(hours=1):
                        ago = f"{int(ago_delta.total_seconds() // 60)} min ago"
                    elif ago_delta < timedelta(days=1):
                        ago = f"{int(ago_delta.total_seconds() // 3600)} hour ago"
                    else:
                        ago = f"{ago_delta.days} day ago"

                    title_md = escape_markdown(title or "Unknown", version=2)
                    ago_md = escape_markdown(ago, version=2)

                    text += f"• [{title_md}]({link})\n  Time: {ago_md}\n\n"

                except Exception:
                    fallback = escape_markdown(title or "Unknown", version=2)
                    text += f"• *{fallback}* \\(error parsing time or link\\)\n\n"

            parse_mode = ParseMode.MARKDOWN_V2

        # Navigation buttons
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"trace_page_{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"trace_page_{page+1}"))

        keyboard = [
            nav_buttons if nav_buttons else [],
            [InlineKeyboardButton("🔄 Refresh", callback_data="status_trace")],
            [InlineKeyboardButton("🗑️ Clear Traces", callback_data="trace_clear")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_status")]
        ]
        reply_markup = InlineKeyboardMarkup([row for row in keyboard if row])

        try:
            if hasattr(update, "callback_query") and update.callback_query:
                await update.callback_query.edit_message_text(
                    text=text, reply_markup=reply_markup, parse_mode=parse_mode
                )
            else:
                await update.message.reply_text(
                    text=text, reply_markup=reply_markup, parse_mode=parse_mode
                )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                await update.callback_query.answer("✅ Already up to date.", show_alert=False)
            elif "Can't parse entities" in str(e):
                await update.callback_query.answer("⚠️ Formatting issue. Please check log entries.", show_alert=True)
            else:
                raise

    async def show_help_menu(self, update, context, page=0):
        total_pages = len(self.HELP_PAGES)
        page = max(0, min(page, total_pages - 1))
        data = self.HELP_PAGES[page]
        text = f"{data['title']}\n\n{data['content']}\n\n_Page {page + 1} of {total_pages}_"

        user_id = update.effective_user.id
        is_admin = self.is_admin(user_id)

        buttons = []
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"help_page_{page - 1}"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"help_page_{page + 1}"))
        if nav:
            buttons.append(nav)
        # Show "Back" only for public (non-admin) users
        if not is_admin:
            buttons.append([InlineKeyboardButton("🔙 Back", callback_data="public_back")])
        else:
            buttons.append([InlineKeyboardButton("🔙 Back", callback_data="back_main")])

        # Use callback or normal message
        if hasattr(update, "callback_query") and update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode="Markdown"
                )
            except Exception:
                pass
        else:
            await update.message.reply_text(
                text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode="Markdown"
            )

if __name__ == "__main__":
    show_banner()

    # 1. Create database connection
    db = Database()
    old_token = db.get_setting("bot_token", "")
    old_admin = db.get_setting("admin_user_id", "")
    old_duration = db.get_setting("subscription_duration", "")

    # Ask for input
    bot_token = input(f"Enter your Bot Token [{old_token}]: ").strip() or old_token
    admin_id = input(f"Enter Admin User ID [{old_admin}]: ").strip() or old_admin
    sub_minutes = input(f"Enter Subscription Time (in minutes) [{old_duration}]: ").strip() or old_duration

    # 3. Save everything in the database
    db.set_setting("bot_token", bot_token)
    db.set_setting("admin_user_id", admin_id)
    db.set_setting("subscription_duration", sub_minutes)
    db.set_setting("subscription_start", datetime.now().isoformat())

    # 4. Start bot with entered values
    bot = TelegramBot(bot_token=bot_token, admin_id=int(admin_id))
    try:
        bot.start_bot()
    except KeyboardInterrupt:
        print("Bot stopped by user.")
    except Exception as e:
        print(f"Bot error: {e}")