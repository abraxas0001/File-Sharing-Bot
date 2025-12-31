import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required!")

ADMIN_ID = os.getenv('ADMIN_ID')
if not ADMIN_ID:
    raise ValueError("ADMIN_ID environment variable is required!")
ADMIN_ID = int(ADMIN_ID)
import json
import asyncio
import html
import random
from datetime import datetime, timedelta, time
from collections import defaultdict
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, ReplyKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (Application, CommandHandler, MessageHandler,
                          CallbackQueryHandler, ContextTypes, filters)
from telegram.error import TimedOut, NetworkError, RetryAfter, BadRequest
from keep_alive import keep_alive  # Optional for Replit or similar
from backup_manager import DatabaseBackupManager
import zipfile

# Auto-deletion tracking
AUTO_DELETE_HOURS = 24  # Delete messages after 24 hour
AUTO_DELETE_ENABLED = True  # Global toggle for auto-deletion
AUTO_DELETE_NOTIFICATIONS = False  # Send notifications when media is auto-deleted - DISABLED GLOBALLY
NOTIFICATION_COOLDOWN_HOURS = 1  # Minimum hours between notifications per user
sent_messages_tracker = {}  # {message_id: {"chat_id": int, "timestamp": datetime, "message_id": int}}
TRACKING_FILE = "auto_delete_tracking.json"  # Persistent storage for tracking data
USER_PREFERENCES_FILE = "user_preferences.json"  # User notification preferences
HISTORICAL_CLEANUP_RUNNING = False  # Prevent multiple simultaneous historical cleanups

# Update offset tracking for processing old messages
UPDATE_OFFSET_FILE = "last_update_offset.json"
last_update_offset = 0
offset_save_counter = 0  # Save offset every N updates to reduce I/O

# Daily backup tracking
LAST_BACKUP_FILE = "last_backup.json"
last_backup_time = None
BACKUP_INTERVAL_HOURS = 24  # Backup every 24 hours

# User preferences storage
user_preferences = {}  # {chat_id: {"auto_delete_notifications": bool}}

# Notification cooldown tracking
last_notification_time = {}  # {chat_id: datetime} - Track last notification time per user

# Minimal logging counters
import sys
log_counters = {
    'auto_deleted': 0,
    'messages_tracked': 0,
    'cleanup_checks': 0,
    'notifications_skipped': 0,
    'data_saves': 0,
    'random_selections': 0,
    'cycle_resets': 0,
    'new_users': 0,
    'pending_deletions': 0
}

# Counter for limiting print frequency
_print_counter = 0
_last_printed_stats = None

def print_counter_line():
    """Print a single line showing all counter stats (updates in place)"""
    global _print_counter, _last_printed_stats
    
    # Update tracked messages count to current actual count
    log_counters['messages_tracked'] = len(sent_messages_tracker)
    
    # Create current stats tuple for comparison
    current_stats = (log_counters['auto_deleted'], log_counters['messages_tracked'], 
                    log_counters['cleanup_checks'], log_counters['data_saves'],
                    log_counters['random_selections'], log_counters['cycle_resets'],
                    log_counters['new_users'], log_counters['pending_deletions'])
    
    _print_counter += 1
    
    # Print on first call or when any stat changes
    if (_print_counter == 1 or _last_printed_stats is None or current_stats != _last_printed_stats):
        # Use \r to overwrite the same line (except for first call)
        if _print_counter == 1:
            print(f"🤖 Bot Online | Auto-deleted: {log_counters['auto_deleted']} | Tracked: {log_counters['messages_tracked']} | Cleanup: {log_counters['cleanup_checks']} | Saves: {log_counters['data_saves']} | Random: {log_counters['random_selections']} | Cycles: {log_counters['cycle_resets']} | New Users: {log_counters['new_users']} | Pending: {log_counters['pending_deletions']}")
        else:
            sys.stdout.write(f"\r🤖 Bot Online | Auto-deleted: {log_counters['auto_deleted']} | Tracked: {log_counters['messages_tracked']} | Cleanup: {log_counters['cleanup_checks']} | Saves: {log_counters['data_saves']} | Random: {log_counters['random_selections']} | Cycles: {log_counters['cycle_resets']} | New Users: {log_counters['new_users']} | Pending: {log_counters['pending_deletions']}")
            sys.stdout.flush()
        _last_printed_stats = current_stats

def load_tracking_data():
    """Load tracking data from file on startup"""
    global sent_messages_tracker
    try:
        if os.path.exists(TRACKING_FILE):
            with open(TRACKING_FILE, 'r') as f:
                data = json.load(f)
            
            # Convert timestamp strings back to datetime objects
            for key, message_info in data.items():
                if isinstance(message_info.get('timestamp'), str):
                    message_info['timestamp'] = datetime.fromisoformat(message_info['timestamp'])
            
            sent_messages_tracker = data
            log_counters['messages_tracked'] = len(sent_messages_tracker)
            if len(sent_messages_tracker) > 0:
                print(f"📁 Loaded {len(sent_messages_tracker)} tracked messages")
        else:
            pass  # Silent start
    except Exception as e:
        print(f"❌ Error loading tracking data: {e}")
        sent_messages_tracker = {}

def save_tracking_data():
    """Save tracking data to file"""
    try:
        # Convert datetime objects to strings for JSON serialization
        data_to_save = {}
        for key, message_info in sent_messages_tracker.items():
            data_copy = message_info.copy()
            if isinstance(data_copy.get('timestamp'), datetime):
                data_copy['timestamp'] = data_copy['timestamp'].isoformat()
            data_to_save[key] = data_copy
        
        with open(TRACKING_FILE, 'w') as f:
            json.dump(data_to_save, f, indent=2)
        log_counters['data_saves'] += 1
        print_counter_line()
    except Exception as e:
        print(f"\n❌ Error saving: {e}")

def load_update_offset():
    """Load the last processed update offset"""
    global last_update_offset
    try:
        if os.path.exists(UPDATE_OFFSET_FILE):
            with open(UPDATE_OFFSET_FILE, 'r') as f:
                data = json.load(f)
                last_update_offset = data.get("last_offset", 0)
        else:
            last_update_offset = 0
    except Exception as e:
        print(f"❌ Error loading offset: {e}")
        last_update_offset = 0

def save_update_offset(offset):
    """Save the last processed update offset"""
    global last_update_offset
    try:
        # Only save if offset is actually newer
        if offset > last_update_offset:
            last_update_offset = offset
            with open(UPDATE_OFFSET_FILE, 'w') as f:
                json.dump({
                    "last_offset": offset, 
                    "saved_at": datetime.now().isoformat(),
                    "total_processed": offset
                }, f)
    except Exception as e:
        print(f"❌ Error saving offset: {e}")

def load_last_backup_time():
    """Load the last backup time from file"""
    global last_backup_time
    try:
        if os.path.exists(LAST_BACKUP_FILE):
            with open(LAST_BACKUP_FILE, 'r') as f:
                data = json.load(f)
                last_backup_time = datetime.fromisoformat(data.get("last_backup"))
                print(f"📅 Last backup time loaded: {last_backup_time}")
        else:
            last_backup_time = None
            print("📅 No previous backup time found - first backup will run soon")
    except Exception as e:
        print(f"❌ Error loading last backup time: {e}")
        last_backup_time = None

def save_last_backup_time():
    """Save the current time as last backup time"""
    global last_backup_time
    try:
        last_backup_time = datetime.now()
        with open(LAST_BACKUP_FILE, 'w') as f:
            json.dump({
                "last_backup": last_backup_time.isoformat(),
                "next_backup": (last_backup_time + timedelta(hours=BACKUP_INTERVAL_HOURS)).isoformat()
            }, f)
        print(f"💾 Last backup time saved: {last_backup_time}")
    except Exception as e:
        print(f"❌ Error saving last backup time: {e}")

def should_run_backup():
    """Check if it's time to run the daily backup"""
    global last_backup_time
    if last_backup_time is None:
        return True  # First backup
    
    current_time = datetime.now()
    time_diff = current_time - last_backup_time
    hours_passed = time_diff.total_seconds() / 3600
    
    return hours_passed >= BACKUP_INTERVAL_HOURS

async def process_pending_updates(application):
    """Process all pending updates that were missed during downtime"""
    try:
        # Get updates with offset
        updates = await application.bot.get_updates(
            offset=last_update_offset + 1,
            limit=100,
            timeout=30
        )
        
        if updates:
            # Process updates silently
            for update in updates:
                try:
                    # Process the update through the normal handler
                    await application.process_update(update)
                    
                    # Save the offset after each successful update
                    save_update_offset(update.update_id)
                    
                except Exception as e:
                    print(f"❌ Error processing update {update.update_id}: {e}")
                    # Still save offset to avoid reprocessing failed updates
                    save_update_offset(update.update_id)
            
            print(f"✅ Processed {len(updates)} pending updates")
        # else: silent if no updates
            
    except Exception as e:
        print(f"❌ Error checking updates: {e}")

def load_user_preferences():
    """Load user preferences from file on startup"""
    global user_preferences
    try:
        if os.path.exists(USER_PREFERENCES_FILE):
            with open(USER_PREFERENCES_FILE, 'r') as f:
                user_preferences = json.load(f)
            # Convert string keys to int (chat_id)
            user_preferences = {int(k): v for k, v in user_preferences.items()}
        # else: silent start
    except Exception as e:
        print(f"❌ Error loading preferences: {e}")
        user_preferences = {}

def save_user_preferences():
    """Save user preferences to file"""
    try:
        # Convert int keys to string for JSON serialization
        data_to_save = {str(k): v for k, v in user_preferences.items()}
        
        with open(USER_PREFERENCES_FILE, 'w') as f:
            json.dump(data_to_save, f, indent=2)
        log_counters['data_saves'] += 1
        print_counter_line()
    except Exception as e:
        print(f"\n❌ Error saving preferences: {e}")

def get_user_preference(chat_id, preference_key, default_value=True):
    """Get a user's preference value"""
    return user_preferences.get(chat_id, {}).get(preference_key, default_value)

def set_user_preference(chat_id, preference_key, value):
    """Set a user's preference value"""
    if chat_id not in user_preferences:
        user_preferences[chat_id] = {}
    user_preferences[chat_id][preference_key] = value
    save_user_preferences()

def can_send_notification(chat_id):
    """Check if we can send a notification to the user (respects cooldown)"""
    if chat_id not in last_notification_time:
        return True
    
    current_time = datetime.now()
    last_time = last_notification_time[chat_id]
    time_diff = current_time - last_time
    
    # Check if enough time has passed (convert hours to seconds)
    cooldown_seconds = NOTIFICATION_COOLDOWN_HOURS * 3600
    return time_diff.total_seconds() >= cooldown_seconds

def update_notification_time(chat_id):
    """Update the last notification time for a user"""
    last_notification_time[chat_id] = datetime.now()
    # Removed verbose notification logging

# Global semaphore to limit concurrent video sends and avoid rate limiting
video_send_semaphore = asyncio.Semaphore(16)  # Wider parallelism so more users start immediately
# Per-user semaphores to ensure fair processing across multiple users
user_semaphores = {}  # {user_id: Semaphore}
# Global task reference to prevent "coroutine never awaited" warnings
auto_backup_task = None

def get_user_semaphore(user_id):
    """Get or create a semaphore for a specific user to allow parallel processing per user"""
    if user_id not in user_semaphores:
        user_semaphores[user_id] = asyncio.Semaphore(1)  # Ensure fairness: 1 active send per user
    return user_semaphores[user_id]

async def safe_answer_callback_query(query, text="", show_alert=False, timeout=3.0):
    """Safely answer callback query with timeout protection and rate limiting"""
    try:
        await asyncio.wait_for(query.answer(text, show_alert=show_alert), timeout=timeout)
        # Small delay to prevent rate limiting
        await asyncio.sleep(0.1)
    except asyncio.TimeoutError:
        pass  # Timeout is expected for some queries
    except Exception as e:
        pass  # Silent error handling

async def track_sent_message(message, auto_delete_delay_hours=AUTO_DELETE_HOURS):
    """Track a sent message for auto-deletion with persistent storage"""
    if not AUTO_DELETE_ENABLED or not message or not hasattr(message, 'message_id'):
        return
        
    message_info = {
        "chat_id": message.chat_id,
        "message_id": message.message_id,
        "timestamp": datetime.now(),
        "delete_after_hours": auto_delete_delay_hours
    }
    key = f"{message.chat_id}_{message.message_id}"
    sent_messages_tracker[key] = message_info
    
    print_counter_line()
    
    # Save to file in background to avoid blocking the event loop
    try:
        global auto_backup_task
        auto_backup_task = asyncio.create_task(asyncio.to_thread(save_tracking_data))
    except Exception:
        # Fallback to direct call if scheduling fails
        save_tracking_data()

async def send_and_track_message(send_func, *args, **kwargs):
    """Wrapper to send a message and track it for auto-deletion"""
    try:
        message = await send_func(*args, **kwargs)
        if message:
            await track_sent_message(message)
        return message
    except Exception as e:
        print(f"Error sending/tracking message: {e}")
        return None

async def cleanup_old_messages(context):
    """Background task to delete old messages"""
    current_time = datetime.now()
    messages_to_delete = []
    
    log_counters['cleanup_checks'] += 1
    
    for key, message_info in list(sent_messages_tracker.items()):
        delete_time = message_info["timestamp"] + timedelta(hours=message_info["delete_after_hours"])
        time_left = delete_time - current_time
        
        if current_time >= delete_time:
            messages_to_delete.append(key)
    
    # Update pending deletions count and show progress if there are messages to delete
    log_counters['pending_deletions'] = len(messages_to_delete)
    if messages_to_delete:
        print_counter_line()
    
    # Process deletions with progress
    for i, key in enumerate(messages_to_delete, 1):
        message_info = sent_messages_tracker[key]
        try:
            await context.bot.delete_message(
                chat_id=message_info["chat_id"],
                message_id=message_info["message_id"]
            )
            log_counters['auto_deleted'] += 1
            log_counters['pending_deletions'] = len(messages_to_delete) - i
            
            if i % 5 == 0 or i == len(messages_to_delete):  # Update every 5 deletions or at the end
                print_counter_line()
            
            # Send notification to user about the deletion (if enabled and cooldown allows)
            chat_id = message_info["chat_id"]
            if (AUTO_DELETE_NOTIFICATIONS and 
                get_user_preference(chat_id, "auto_delete_notifications", True) and
                can_send_notification(chat_id)):
                
                try:
                    notification_message = (
                        "🧹 **Auto-Cleanup Notification**\n\n"
                        "The file you received has been automatically removed to keep the bot clean.\n\n"
                        "💡 **Tip**: Use the bookmarks feature (⭐) to save important resources permanently!\n"
                        f"⏰ Auto-cleanup happens after {AUTO_DELETE_HOURS} hour(s) for all shared media."
                    )
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=notification_message,
                        parse_mode='Markdown'
                    )
                    # Update the notification time after successful send
                    update_notification_time(chat_id)
                except Exception as notification_error:
                    pass  # Silent error handling
            
            else:
                # Notifications disabled or in cooldown - just count it
                log_counters['notifications_skipped'] += 1
                    
        except Exception as e:
            pass  # Silent error handling
    
    # Remove deleted messages from tracker
    for key in messages_to_delete:
        sent_messages_tracker.pop(key, None)
    
    # Reset pending deletions counter
    log_counters['pending_deletions'] = 0
    
    # Save tracking data after cleanup
    if messages_to_delete:
        try:
            global auto_backup_task
            auto_backup_task = asyncio.create_task(asyncio.to_thread(save_tracking_data))
        except Exception:
            save_tracking_data()
    
    # Update counter display after cleanup
    print_counter_line()

async def start_background_cleanup(app):
    """Start background cleanup task that works with app.run_polling()"""
    await asyncio.sleep(60)  # Wait 1 minute before first check
    while True:
        try:
            await cleanup_old_messages(app)
            await asyncio.sleep(300)  # Check every 5 minutes
        except Exception as e:
            print(f"Error in background cleanup: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retrying

async def check_daily_backup(app):
    """Check if daily backup should be run and execute it if needed"""
    while True:
        try:
            if should_run_backup():
                print("🔄 Daily backup check: Time to backup!")
                await perform_daily_backup(app)
                save_last_backup_time()
                print(f"✅ Daily backup completed at {datetime.now()}")
            else:
                # Calculate hours until next backup
                if last_backup_time:
                    next_backup = last_backup_time + timedelta(hours=BACKUP_INTERVAL_HOURS)
                    hours_until = (next_backup - datetime.now()).total_seconds() / 3600
                    print(f"⏰ Next backup in {hours_until:.1f} hours")
        except Exception as e:
            print(f"❌ Error in daily backup check: {e}")
        
        # Check every hour
        await asyncio.sleep(3600)  # 1 hour = 3600 seconds

async def perform_daily_backup(app):
    """Perform the actual daily backup by sending files to admin"""
    try:
        print("📤 Starting daily backup to Telegram...")
        
        json_files = [
            "media_db.json", "users_db.json", "favorites_db.json",
            "passed_links.json", "active_links.json", "deleted_media.json",
            "random_state.json", "user_preferences.json", "admin_list.json",
            "exempted_users.json", "caption_config.json", "protection_settings.json",
            "auto_delete_tracking.json", "last_update_offset.json"
        ]

        sent_count = 0
        for filename in json_files:
            if os.path.exists(filename):
                with open(filename, 'rb') as file:
                    await app.bot.send_document(
                        chat_id=ADMIN_ID,
                        document=file,
                        filename=f"daily_{filename}",
                        caption=f"🔄 **Daily Auto-Backup**\n📁 `{filename}`\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                    )
                sent_count += 1

        await app.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"✅ **Daily Auto-Backup Complete**\n📤 {sent_count} files backed up to Telegram\n🔒 Your Railway data is now safely stored!"
        )
        
        print(f"📤 Daily backup sent {sent_count} files to admin")
        
    except Exception as e:
        print(f"❌ Daily backup failed: {e}")
        try:
            await app.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"❌ Daily backup failed: {str(e)}"
            )
        except Exception as e2:
            print(f"❌ Could not send backup failure notification: {e2}")

async def post_init_callback(application):
    """Callback to start background tasks after app initialization"""
    # Load existing tracking data from file
    load_tracking_data()
    
    # Load user preferences from file
    load_user_preferences()
    
    # Load last update offset for processing old messages
    load_update_offset()
    
    # Load last backup time
    load_last_backup_time()
    
    # Set bot commands menu with retry logic
    try:
        await application.bot.set_my_commands(commands)
        print("✅ Bot commands menu set successfully")
    except Exception as e:
        print(f"⚠️ Failed to set commands menu: {e}")
        print("🔄 Bot will continue without menu - commands still work")
    
    # Process any pending updates that were missed
    await process_pending_updates(application)
    
    # Update counter display after loading data
    print_counter_line()
    
    if AUTO_DELETE_ENABLED:
        # Start the background cleanup task
        asyncio.create_task(start_background_cleanup(application))
        # Auto-deletion background task started silently
    
    # Start the daily backup check task (more reliable than job_queue)
    asyncio.create_task(check_daily_backup(application))
    print("✅ Daily backup monitoring started")
    
    # Schedule daily auto-backup to Telegram (2 AM UTC) - fallback method
    async def daily_telegram_backup(context: ContextTypes.DEFAULT_TYPE):
        """Daily backup routine - sends all JSON files to admin"""
        try:
            json_files = [
                "media_db.json", "users_db.json", "favorites_db.json",
                "passed_links.json", "active_links.json", "deleted_media.json",
                "random_state.json", "user_preferences.json", "admin_list.json",
                "exempted_users.json", "caption_config.json", "protection_settings.json",
                "auto_delete_tracking.json", "last_update_offset.json"
            ]

            sent_count = 0
            for filename in json_files:
                if os.path.exists(filename):
                    with open(filename, 'rb') as file:
                        await context.bot.send_document(
                            chat_id=ADMIN_ID,
                            document=file,
                            filename=f"daily_{filename}",
                            caption=f"🔄 **Daily Auto-Backup**\n📁 `{filename}`\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                        )
                    sent_count += 1

            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"✅ **Daily Auto-Backup Complete**\n📤 {sent_count} files backed up to Telegram\n🔒 Your Railway data is now safely stored!"
            )
        except Exception as e:
            await context.bot.send_message(
                chat_id=ADMIN_ID, 
                text=f"❌ Daily backup failed: {str(e)}"
            )

    # Schedule daily backup at 2 AM UTC (kept as fallback)
    if hasattr(application, 'job_queue') and application.job_queue:
        application.job_queue.run_daily(
            daily_telegram_backup,
            time=time(2, 0),  # 2 AM UTC
            name="daily_backup"
        )
        print("✅ Daily backup job scheduled via job_queue (fallback)")
    else:
        print("⚠️ Job queue not available - relying on time-based backup checks")

async def historical_cleanup_old_media(context, max_hours_old=AUTO_DELETE_HOURS, max_messages_per_chat=100, dry_run=False):
    """
    Historical cleanup system to delete old media messages that predate the tracking system
    
    This function uses a safer approach:
    1. Scans the media_db.json to find all bot-sent media
    2. Attempts to delete messages older than the specified time
    3. Uses file modification times as a proxy for message age
    
    Args:
        context: Bot context or application
        max_hours_old: Delete messages older than this many hours (default: same as auto-delete)
        max_messages_per_chat: Maximum number of recent messages to check per chat
        dry_run: If True, only count messages without deleting them
    """
    if not AUTO_DELETE_ENABLED:
        # Auto-deletion disabled, skip cleanup
        return {"status": "disabled", "deleted_count": 0, "error_count": 0}
    
    print(f"\n🧹 Starting historical cleanup - checking messages older than {max_hours_old} hours")
    print(f"🧹 Dry run mode: {dry_run}")
    
    cutoff_time = datetime.now() - timedelta(hours=max_hours_old)
    deleted_count = 0
    error_count = 0
    checked_count = 0
    
    try:
        # Strategy 1: Check media_db.json for previously sent media
        media_db_file = "media_db.json"
        video_db_file = "video_db.json"
        
        media_files_to_check = []
        
        # Load media databases
        for db_file in [media_db_file, video_db_file]:
            if os.path.exists(db_file):
                try:
                    with open(db_file, 'r') as f:
                        db_data = json.load(f)
                    media_files_to_check.extend(db_data.keys())
                    print(f"🧹 Loaded {len(db_data)} entries from {db_file}")
                except Exception as e:
                    print(f"❌ Error loading {db_file}: {e}")
        
        if not media_files_to_check:
            # No media database found, try alternative method
            return await historical_cleanup_by_recent_scan(context, max_hours_old, dry_run)
        
        print(f"🧹 Found {len(media_files_to_check)} media files to check")
        
        # Get registered users to know which chats to check
        users_db_file = "users_db.json"
        if not os.path.exists(users_db_file):
            # No users database file found
            return {"status": "no_users_file", "deleted_count": 0, "error_count": 0}
        
        with open(users_db_file, 'r') as f:
            users_db = json.load(f)
        
        print(f"🧹 Checking {len(users_db)} registered users")
        
        # For each chat, try to clean up old media
        for user_id_str, user_info in users_db.items():
            try:
                chat_id = int(user_id_str)
                username = user_info.get('username', user_info.get('first_name', 'Unknown'))
                
                print(f"🧹 Processing chat {chat_id} ({username})...")
                
                # Get recent messages to find bot-sent media
                messages_cleaned_in_chat = 0
                
                try:
                    # Send a temp message to get current message ID range
                    temp_msg = await context.bot.send_message(chat_id, "🔍")
                    current_msg_id = temp_msg.message_id
                    await context.bot.delete_message(chat_id, current_msg_id)
                    
                    # Check recent messages working backwards
                    start_check_id = max(1, current_msg_id - max_messages_per_chat)
                    
                    for msg_id in range(current_msg_id - 1, start_check_id, -1):
                        if messages_cleaned_in_chat >= max_messages_per_chat:
                            break
                        
                        # Skip if already tracked in current session
                        if f"{chat_id}_{msg_id}" in sent_messages_tracker:
                            continue
                        
                        checked_count += 1
                        
                        try:
                            # Try to delete the message - if it's a bot message and exists, it will be deleted
                            # If it's not a bot message or doesn't exist, we'll get an error (which is fine)
                            if not dry_run:
                                await context.bot.delete_message(chat_id, msg_id)
                                deleted_count += 1
                                messages_cleaned_in_chat += 1
                                print(f"✅ Historical cleanup: deleted message {msg_id} from chat {chat_id}")
                                
                                # Rate limiting
                                await asyncio.sleep(0.05)
                            else:
                                # In dry run, we can't easily check if it's our message without trying to delete
                                # So we'll estimate based on message density
                                if (current_msg_id - msg_id) > 50:  # Assume messages older than 50 IDs might be ours
                                    deleted_count += 1
                                    
                        except BadRequest as e:
                            error_msg = str(e).lower()
                            if any(phrase in error_msg for phrase in [
                                "message to delete not found",
                                "message can't be deleted",
                                "bad request",
                                "not found"
                            ]):
                                # Expected errors - message doesn't exist or isn't ours
                                pass
                            else:
                                error_count += 1
                                print(f"❌ Unexpected error deleting {msg_id}: {e}")
                                
                        except Exception as e:
                            error_count += 1
                            print(f"❌ Error deleting message {msg_id}: {e}")
                
                except Exception as e:
                    print(f"❌ Error processing chat {chat_id}: {e}")
                    error_count += 1
                
                # Rate limiting between chats
                await asyncio.sleep(0.2)
                
            except Exception as e:
                print(f"❌ Error processing user {user_id_str}: {e}")
                error_count += 1
        
        result = {
            "status": "completed",
            "deleted_count": deleted_count,
            "error_count": error_count,
            "checked_count": checked_count,
            "dry_run": dry_run
        }
        
        if dry_run:
            print(f"🧹 Historical cleanup (DRY RUN) completed: {deleted_count} messages would be deleted, {error_count} errors, {checked_count} messages checked")
        else:
            print(f"🧹 Historical cleanup completed: {deleted_count} messages deleted, {error_count} errors, {checked_count} messages checked")
        print_counter_line()
        
        return result
        
    except Exception as e:
        print(f"❌ Historical cleanup failed: {e}")
        return {"status": "error", "deleted_count": deleted_count, "error_count": error_count + 1, "error": str(e)}

async def historical_cleanup_by_recent_scan(context, max_hours_old, dry_run=False):
    """
    Alternative historical cleanup that scans recent messages more conservatively
    """
    # Using conservative recent message scan method
    
    deleted_count = 0
    error_count = 0
    checked_count = 0
    
    try:
        # Get registered users
        users_db_file = "users_db.json"
        if not os.path.exists(users_db_file):
            return {"status": "no_users_file", "deleted_count": 0, "error_count": 0}
        
        with open(users_db_file, 'r') as f:
            users_db = json.load(f)
        
        # Only check a few recent messages per chat to be safe
        max_messages_to_check = 20  # Very conservative
        
        for user_id_str, user_info in users_db.items():
            try:
                chat_id = int(user_id_str)
                username = user_info.get('username', user_info.get('first_name', 'Unknown'))
                
                print(f"🧹 Conservative scan of chat {chat_id} ({username})...")
                
                # Get current message ID
                temp_msg = await context.bot.send_message(chat_id, "🔍")
                current_msg_id = temp_msg.message_id
                await context.bot.delete_message(chat_id, current_msg_id)
                
                # Only check very recent messages to be safe
                start_id = max(1, current_msg_id - max_messages_to_check)
                
                for msg_id in range(current_msg_id - 1, start_id, -1):
                    # Skip tracked messages
                    if f"{chat_id}_{msg_id}" in sent_messages_tracker:
                        continue
                    
                    checked_count += 1
                    
                    if not dry_run:
                        try:
                            await context.bot.delete_message(chat_id, msg_id)
                            deleted_count += 1
                            print(f"✅ Conservative cleanup: deleted message {msg_id}")
                            await asyncio.sleep(0.1)
                        except:
                            # Silently ignore - expected for non-bot messages
                            pass
                    else:
                        # Conservative estimate in dry run
                        deleted_count += 1
                
                await asyncio.sleep(0.3)
                
            except Exception as e:
                error_count += 1
                print(f"❌ Error in conservative scan: {e}")
        
        return {
            "status": "completed",
            "deleted_count": deleted_count,
            "error_count": error_count,
            "checked_count": checked_count,
            "dry_run": dry_run
        }
        
    except Exception as e:
        return {"status": "error", "deleted_count": 0, "error_count": 1, "error": str(e)}

# Welcome image configuration
# Using file_id for production deployment
WELCOME_IMAGE_FILE_ID = "AgACAgUAAxkBAAIB2mjhF7LvHvMBAYd_opVhlq5bKGtcAAIBDWsbVhIIV8ZByQeO6Z46AQADAgADeQADNgQ"  # Welcome image file ID
CHANNEL_NOTIFY_IMAGE_ID = "AgACAgUAAxkBAAIB4WjhGBcNkObaEFNdrrp1YYG-YkOqAAICDWsbVhIIV-ClU8vTT4glAQADAgADeQADNgQ"  # Channel notification image ID

# ADMIN_ID is now loaded from environment variables at the top of the file
ADMIN_LIST_FILE = "admin_list.json"
REQUIRED_CHANNELS = ["CypherHere"]
MEDIA_FILE = "media_db.json"
EXEMPTED_FILE = "exempted_users.json"
USERS_FILE = "users_db.json"
ACTIVE_LINKS_FILE = "active_links.json"
PASSED_LINKS_FILE = "passed_links.json"
FAVORITES_FILE = "favorites_db.json"
RANDOM_STATE_FILE = "random_state.json"
CAPTION_CONFIG_FILE = "caption_config.json"
DELETED_MEDIA_FILE = "deleted_media.json"
AUTODELETE_CONFIG_FILE = "autodelete_config.json"
BOT_USERNAME = "GlitchedMatrixbot"  # 🔁 REPLACE THIS

# Caption configuration
caption_config = {
    "global_caption": ""  # Global caption to add to all files
}

media_data = {}
exempted_users = set()
users_data = {}  # Track all bot users
active_links = set()
passed_links = []  # List of specific video_ids that are passed
favorites_data = {"user_favorites": {}, "video_likes": {}}
random_state = {"shown_videos": [], "all_videos": []}
media_buffer = defaultdict(list)
media_tag_map = {}
media_group_tasks = {}
custom_batch_sessions = {}  # Track active custom batch sessions
admin_list = [ADMIN_ID]  # Initialize with main admin
deleted_media_storage = {}  # Store deleted media for restoration
user_operations = {}  # Track ongoing operations per user

# Protection settings
PROTECTION_FILE = "protection_settings.json"
protection_enabled = True  # Default protection on

def load_protection_settings():
    """Load protection settings from file"""
    global protection_enabled
    try:
        if os.path.exists(PROTECTION_FILE):
            with open(PROTECTION_FILE, "r") as f:
                settings = json.load(f)
                protection_enabled = settings.get("enabled", True)
    except Exception as e:
        print(f"Error loading protection settings: {e}")
        protection_enabled = True

def save_protection_settings():
    """Save protection settings to file"""
    try:
        with open(PROTECTION_FILE, "w") as f:
            json.dump({"enabled": protection_enabled}, f)
        print(f"Protection settings saved: {protection_enabled}")
    except Exception as e:
        print(f"Error saving protection settings: {e}")
        # Try to save to a backup location
        try:
            with open("protection_settings_backup.json", "w") as f:
                json.dump({"enabled": protection_enabled}, f)
            print("Protection settings saved to backup file")
        except Exception as e2:
            print(f"Error saving to backup: {e2}")

# Load protection settings on startup
load_protection_settings()

def get_protection_status():
    """Get current protection status"""
    return protection_enabled

def should_protect_content(user_id, content):
    """Check if content should be protected for a given user"""
    # Exempt admin users from protection
    if user_id in admin_list:
        return False

    # Return protection setting if not admin
    return protection_enabled

def save_autodelete_config():
    """Save auto-delete configuration to file"""
    with open(AUTODELETE_CONFIG_FILE, "w") as f:
        config_data = {
            "AUTO_DELETE_HOURS": AUTO_DELETE_HOURS,
            "AUTO_DELETE_ENABLED": AUTO_DELETE_ENABLED
        }
        json.dump(config_data, f, indent=2)
    print(f"💾 Saved autodelete config: {AUTO_DELETE_HOURS} hours, enabled: {AUTO_DELETE_ENABLED}")


def load_autodelete_config():
    """Load auto-delete configuration from file"""
    global AUTO_DELETE_HOURS, AUTO_DELETE_ENABLED
    try:
        if os.path.exists(AUTODELETE_CONFIG_FILE):
            with open(AUTODELETE_CONFIG_FILE, "r") as f:
                config = json.load(f)
                AUTO_DELETE_HOURS = config.get("AUTO_DELETE_HOURS", 24)  # Default to 24 hours
                AUTO_DELETE_ENABLED = config.get("AUTO_DELETE_ENABLED", True)
            print(f"✅ Loaded autodelete config: {AUTO_DELETE_HOURS} hours, enabled: {AUTO_DELETE_ENABLED}")
        else:
            print(f"ℹ️ No autodelete config file found, using defaults: {AUTO_DELETE_HOURS} hours")
    except Exception as e:
        print(f"❌ Error loading autodelete config: {e}")
        # Keep default values

# Soft deletion & revocation helpers:
# We store media as original dicts. For soft delete we replace entry with
# {"deleted": True, "data": <original>}. For revoke we set entry["revoked"] = True.
# Random / browsing logic must skip entries where deleted or revoked.



if os.path.exists(MEDIA_FILE):
    with open(MEDIA_FILE, "r") as f:
        try:
            media_data = json.load(f)
        except json.JSONDecodeError:
            media_data = {}

if os.path.exists(EXEMPTED_FILE):
    with open(EXEMPTED_FILE, "r") as f:
        try:
            exempted_users = set(json.load(f))
        except json.JSONDecodeError:
            exempted_users = set()

if os.path.exists(ACTIVE_LINKS_FILE):
    with open(ACTIVE_LINKS_FILE, "r") as f:
        try:
            data = json.load(f)
            if isinstance(data, list):
                # Old format - convert to new dict format
                active_links = {}
                for link_key in data:
                    active_links[link_key] = {"type": "reference"}  # Mark as reference to media_data
            else:
                active_links = data
        except json.JSONDecodeError:
            active_links = {}

if os.path.exists(PASSED_LINKS_FILE):
    with open(PASSED_LINKS_FILE, "r") as f:
        try:
            passed_links = json.load(f)
        except json.JSONDecodeError:
            passed_links = []

if os.path.exists(FAVORITES_FILE):
    with open(FAVORITES_FILE, "r") as f:
        try:
            favorites_data = json.load(f)
            # Ensure the structure exists
            if "user_favorites" not in favorites_data:
                favorites_data["user_favorites"] = {}
            if "video_likes" not in favorites_data:
                favorites_data["video_likes"] = {}
        except json.JSONDecodeError:
            favorites_data = {"user_favorites": {}, "video_likes": {}}

if os.path.exists(USERS_FILE):
    with open(USERS_FILE, "r") as f:
        try:
            users_data = json.load(f)
        except json.JSONDecodeError:
            users_data = {}

if os.path.exists(RANDOM_STATE_FILE):
    with open(RANDOM_STATE_FILE, "r") as f:
        try:
            random_state = json.load(f)
            # Ensure the structure exists
            if "shown_videos" not in random_state:
                random_state["shown_videos"] = []
            if "all_videos" not in random_state:
                random_state["all_videos"] = []
        except json.JSONDecodeError:
            random_state = {"shown_videos": [], "all_videos": []}

if os.path.exists(CAPTION_CONFIG_FILE):
    with open(CAPTION_CONFIG_FILE, "r") as f:
        try:
            caption_config = json.load(f)
            # Ensure the structure exists
            if "global_caption" not in caption_config:
                caption_config["global_caption"] = ""
        except json.JSONDecodeError:
            caption_config = {"global_caption": ""}

# Load admin list
if os.path.exists(ADMIN_LIST_FILE):
    with open(ADMIN_LIST_FILE, "r") as f:
        try:
            admin_list = json.load(f)
            # Ensure main admin is always in the list
            if ADMIN_ID not in admin_list:
                admin_list.append(ADMIN_ID)
        except json.JSONDecodeError:
            admin_list = [ADMIN_ID]
else:
    admin_list = [ADMIN_ID]

# Load deleted media storage
if os.path.exists(DELETED_MEDIA_FILE):
    with open(DELETED_MEDIA_FILE, "r") as f:
        try:
            deleted_media_storage = json.load(f)
        except json.JSONDecodeError:
            deleted_media_storage = {}

# Load autodelete configuration
load_autodelete_config()

# Initialize backup manager
backup_manager = DatabaseBackupManager()


def save_media():
    with open(MEDIA_FILE, "w") as f:
        json.dump(media_data, f, indent=2)
    # Auto-backup after media changes
    try:
        global auto_backup_task
        auto_backup_task = asyncio.create_task(auto_backup_data("media"))
    except:
        pass


def save_exempted():
    with open(EXEMPTED_FILE, "w") as f:
        json.dump(list(exempted_users), f, indent=2)
    # Auto-backup after exempted users changes
    try:
        global auto_backup_task
        auto_backup_task = asyncio.create_task(auto_backup_data("exempted"))
    except:
        pass


def save_active_links():
    with open(ACTIVE_LINKS_FILE, "w") as f:
        json.dump(active_links, f, indent=2)


def save_passed_links():
    with open(PASSED_LINKS_FILE, "w") as f:
        json.dump(passed_links, f, indent=2)


def save_favorites():
    with open(FAVORITES_FILE, "w") as f:
        json.dump(favorites_data, f, indent=2)
    # Auto-backup after favorites changes
    try:
        global auto_backup_task
        auto_backup_task = asyncio.create_task(auto_backup_data("favorites"))
    except:
        pass


def save_random_state():
    with open(RANDOM_STATE_FILE, "w") as f:
        json.dump(random_state, f, indent=2)


def save_deleted_media():
    with open(DELETED_MEDIA_FILE, "w") as f:
        json.dump(deleted_media_storage, f, indent=2)


def save_caption_config():
    with open(CAPTION_CONFIG_FILE, "w") as f:
        json.dump(caption_config, f, indent=2)


def save_admin_list():
    with open(ADMIN_LIST_FILE, "w") as f:
        json.dump(admin_list, f, indent=2)


async def auto_backup_data(data_type: str):
    """Automatically backup data after changes"""
    try:
        # Create a backup with a descriptive name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"auto_{data_type}_{timestamp}"
        description = f"Auto-backup after {data_type} data changes"
        
        success, message = await asyncio.get_event_loop().run_in_executor(
            None, backup_manager.create_backup, backup_name, description
        )
        
        if success:
            print(f"✅ Auto-backup created: {backup_name}")
        else:
            print(f"❌ Auto-backup failed: {message}")
            
    except Exception as e:
        print(f"❌ Auto-backup error: {e}")


def is_admin(user_id):
    """Check if a user is an admin"""
    return user_id in admin_list


def track_user_operation(user_id, operation_type, operation_data=None):
    """Track an ongoing operation for a user"""
    user_operations[user_id] = {
        "type": operation_type,
        "data": operation_data,
        "cancelled": False
    }


def cancel_user_operation(user_id):
    """Cancel ongoing operation for a user"""
    if user_id in user_operations:
        user_operations[user_id]["cancelled"] = True
        return True
    return False


def is_operation_cancelled(user_id):
    """Check if user's operation is cancelled"""
    cancelled = user_operations.get(user_id, {}).get("cancelled", False)
    return cancelled


def clear_user_operation(user_id):
    """Clear user's operation tracking"""
    user_operations.pop(user_id, None)


async def delete_media_entry(query, context: ContextTypes.DEFAULT_TYPE, video_key: str):
    """Delete a media item and physically reorder the list.
    The deleted item is stored separately and can be restored to its original position.
    """
    global deleted_media_storage
    
    try:
        if "_" not in video_key:
            print(f"ERROR: Invalid video_key format: {video_key}")
            await safe_answer_callback_query(query, "❌ Bad key", show_alert=False)
            return
            
        tag, idx_str = video_key.rsplit("_", 1)
        try:
            idx = int(idx_str)
        except ValueError:
            print(f"ERROR: Invalid index in video_key: {video_key}")
            await safe_answer_callback_query(query, "❌ Bad index", show_alert=False)
            return
            
        if tag not in media_data:
            print(f"ERROR: Tag '{tag}' not found in media_data")
            await safe_answer_callback_query(query, "❌ Tag not found", show_alert=False)
            return
            
        if not (0 <= idx < len(media_data[tag])):
            print(f"ERROR: Index {idx} out of range for tag '{tag}' (length: {len(media_data[tag])})")
            await safe_answer_callback_query(query, "❌ Index out of range", show_alert=False)
            return
        
        entry = media_data[tag][idx]
        if video_key in deleted_media_storage:
            print(f"WARNING: Media {video_key} already in deleted storage, but there's new media at this index.")
            print(f"This means the old deleted entry is stale. Cleaning it up and proceeding with fresh deletion.")
            
            # Remove the stale deleted entry
            del deleted_media_storage[video_key]
            save_deleted_media()
            
            # Continue with normal deletion process for the current media
        
        # Store the deleted item with its original position
        deleted_media_storage[video_key] = {
            "data": entry,
            "original_position": idx,
            "tag": tag,
            "deleted_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Remove the item from the list (this will shift all subsequent items down)
        old_length = len(media_data[tag])
        media_data[tag].pop(idx)
        new_length = len(media_data[tag])
        
        # Clean up stale deleted entries that may now point to wrong indices
        # Any deleted entry for the same tag with original_position >= idx needs to be checked
        stale_keys = []
        for del_key, del_info in deleted_media_storage.items():
            if del_info.get("tag") == tag and del_info.get("original_position", -1) > idx:
                # This deleted entry now points to a shifted index, it might be stale
                stale_keys.append(del_key)
        
        if stale_keys:
            # Note: We don't auto-delete these as they might be valid deletions from higher indices
            # They will be cleaned up by the cleanup function if they're truly invalid
            pass
        
        save_media()
        save_deleted_media()
        update_random_state()
        
        # Saved data and updated random state silently
        
        # Update only admin portion of keyboard (preserve other buttons)
        try:
            if query.message and query.message.reply_markup:
                ik = query.message.reply_markup.inline_keyboard
                new_admin_rows = build_admin_control_row(video_key)
                filtered = []
                for row in ik:
                    if not row:
                        continue
                    cb = getattr(row[0], 'callback_data', '') or ''
                    if cb.startswith(('revoke_media_', 'del_media_', 'restore_media_')):
                        continue
                    filtered.append(row)
                # For deleted items, show restore button
                restore_row = [InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")]
                filtered.append(restore_row)
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(filtered))
            else:
                restore_row = [InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")]
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([restore_row]))
        except Exception as e:
            print(f"ERROR: Failed to update reply markup: {e}")
            pass
            
        await safe_answer_callback_query(query, "🗑️ Media deleted!", show_alert=False)
        
        # Send a notification message with options
        notification_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("♻️ Restore Now", callback_data=f"restore_media_{video_key}"),
                InlineKeyboardButton("📋 View Deleted List", callback_data="list_deleted_media")
            ]
        ])
        
        await query.message.reply_text(
            f"🗑️ <b>Media Deleted Successfully</b>\n\n"
            f"📁 Tag: <code>{tag}</code>\n"
            f"📊 Original Index: <code>{idx}</code>\n"
            f"🔄 All subsequent media shifted up by 1 position\n\n"
            f"💡 Use the buttons below to restore or view all deleted media.",
            parse_mode=ParseMode.HTML,
            reply_markup=notification_keyboard
        )
        
    except Exception as e:
        print(f"ERROR: Exception in delete_media_entry: {e}")
        await safe_answer_callback_query(query, f"❌ Error: {str(e)}", show_alert=True)
        raise


async def revoke_media_entry(query, video_key: str):
    """Mark media as revoked (skipped but restorable)."""
    if "_" not in video_key:
        await safe_answer_callback_query(query, "❌ Bad key", show_alert=False)
        return
    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await safe_answer_callback_query(query, "❌ Bad index", show_alert=False)
        return
    if tag not in media_data or not (0 <= idx < len(media_data[tag])):
        await safe_answer_callback_query(query, "❌ Not found", show_alert=False)
        return
    entry = media_data[tag][idx]
    if isinstance(entry, dict) and entry.get("deleted"):
        await safe_answer_callback_query(query, "❌ It's deleted", show_alert=False)
        return
    if isinstance(entry, dict) and entry.get("revoked"):
        await safe_answer_callback_query(query, "⚠️ Already revoked", show_alert=False)
        return
    
    # Get media type for the confirmation message
    media_type = "unknown"
    if isinstance(entry, dict):
        media_type = entry.get("type", "unknown")
        entry["revoked"] = True
    else:
        media_data[tag][idx] = {"data": entry, "revoked": True, "type": "unknown"}
    
    save_media()
    update_random_state()
    
    # Update only admin rows to show restore
    try:
        if query.message and query.message.reply_markup:
            ik = query.message.reply_markup.inline_keyboard
            new_admin_rows = build_admin_control_row(video_key)
            filtered = []
            for row in ik:
                if not row:
                    continue
                cb = getattr(row[0], 'callback_data', '') or ''
                if cb.startswith(('revoke_media_', 'del_media_', 'restore_media_')):
                    continue
                filtered.append(row)
            filtered += new_admin_rows
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(filtered))
        else:
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(build_admin_control_row(video_key)))
    except Exception:
        pass
    
    await safe_answer_callback_query(query, "🛑 Revoked", show_alert=False)
    
    # Send beautiful confirmation message like the delete format
    confirmation_text = (
        f"🛑 <b>Media Revoked Successfully</b>\n\n"
        f"📂 Tag: <code>{tag}</code>\n"
        f"📍 Index: <code>{idx}</code>\n\n"
        f"💡 Use the buttons below to restore or view all revoked media."
    )
    
    keyboard = [
        [
            InlineKeyboardButton("♻️ Restore Now", callback_data=f"restore_media_{video_key}"),
            InlineKeyboardButton("📋 View Revoked List", callback_data="view_revoked_list")
        ]
    ]
    
    await query.message.reply_html(
        confirmation_text,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def restore_media_entry(query, video_key: str):
    """Restore a deleted or revoked media back to its original position."""
    global deleted_media_storage
    
    if "_" not in video_key:
        await query.answer("❌ Bad key", show_alert=False)
        return
    
    # Check if it's a deleted item in deleted_media_storage
    if video_key in deleted_media_storage:
        # Handle deleted media restoration
        deleted_info = deleted_media_storage[video_key]
        original_data = deleted_info["data"]
        original_position = deleted_info["original_position"]
        tag = deleted_info["tag"]
        
        # Ensure the tag still exists
        if tag not in media_data:
            media_data[tag] = []
        
        # Insert the media back at its original position
        # If the original position is beyond current list, append to end
        if original_position <= len(media_data[tag]):
            media_data[tag].insert(original_position, original_data)
        else:
            media_data[tag].append(original_data)
        
        # Remove from deleted storage
        del deleted_media_storage[video_key]
        
        save_media()
        save_deleted_media()
        update_random_state()

        await query.answer("✅ Deleted media restored successfully!", show_alert=True)

    else:
        # Check if it's a revoked item in media_data
        try:
            tag, idx_str = video_key.rsplit("_", 1)
            idx = int(idx_str)
            
            if tag in media_data and idx < len(media_data[tag]):
                media_item = media_data[tag][idx]
                
                if isinstance(media_item, dict) and media_item.get("revoked"):
                    # Remove the revoked flag to restore the media
                    media_item.pop("revoked", None)
                    
                    save_media()
                    update_random_state()
                    
                    await query.answer("✅ Revoked media restored successfully!", show_alert=True)
                    
                    # Update the message to remove this item from the list
                    try:
                        # Refresh the listremoved display
                        await query.message.delete()
                        
                        # Send updated list
                        removed = list(deleted_media_storage.keys())
                        revoked = []
                        for t, vids in media_data.items():
                            if not isinstance(vids, list):
                                continue
                            for i, v in enumerate(vids):
                                if isinstance(v, dict) and v.get("revoked"):
                                    revoked.append(f"{t}_{i}")
                        
                        if not removed and not revoked:
                            await query.message.reply_text("✅ No deleted or revoked media.")
                            return
                            
                        text_lines = ["<b>Removed / Revoked Media</b>"]
                        
                        if removed:
                            text_lines.append("\n🗑️ <b>Deleted:</b>")
                            for k in removed[:50]:
                                tag_parts = k.split('_')
                                if len(tag_parts) >= 2:
                                    t = '_'.join(tag_parts[:-1])
                                    idx_str = tag_parts[-1]
                                    view_format = f"{t}_{idx_str}_{idx_str}"
                                    view_link = f"https://t.me/GlitchedMatrixbot?start={view_format}"
                                    restore_link = f"https://t.me/GlitchedMatrixbot?start=restoredeleted_{k}"
                                    text_lines.append(f"{k} | <a href='{view_link}'>View</a> | <a href='{restore_link}'>Restore</a>")
                                else:
                                    text_lines.append(k)
                                    
                        if revoked:
                            text_lines.append("\n🛑 <b>Revoked:</b>")
                            for k in revoked[:50]:
                                tag_parts = k.split('_')
                                if len(tag_parts) >= 2:
                                    t = '_'.join(tag_parts[:-1])
                                    idx_str = tag_parts[-1]
                                    view_format = f"{t}_{idx_str}_{idx_str}"
                                    view_link = f"https://t.me/GlitchedMatrixbot?start={view_format}"
                                    restore_link = f"https://t.me/GlitchedMatrixbot?start=restoredeleted_{k}"
                                    text_lines.append(f"{k} | <a href='{view_link}'>View</a> | <a href='{restore_link}'>Restore</a>")
                                else:
                                    text_lines.append(k)
                        
                        await query.message.reply_html("\n".join(text_lines))
                    except:
                        pass
                    
                    return
            
            await query.answer("❌ No deleted or revoked media found to restore", show_alert=False)
            return
            
        except (ValueError, IndexError):
            await query.answer("❌ Invalid media key format", show_alert=False)
            return

    # Update keyboard to show normal admin controls
    try:
        if query.message and query.message.reply_markup:
            ik = query.message.reply_markup.inline_keyboard
            new_admin_rows = build_admin_control_row(video_key)
            filtered = []
            for row in ik:
                if not row:
                    continue
                cb = getattr(row[0], 'callback_data', '') or ''
                if cb.startswith(('revoke_media_', 'del_media_', 'restore_media_')):
                    continue
                filtered.append(row)
            filtered += new_admin_rows
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(filtered))
        else:
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(build_admin_control_row(video_key)))
    except Exception:
        pass
    
    await query.answer("♻️ Restored", show_alert=False)
    # Send a concise confirmation without referencing undefined variables for revoked case
    try:
        if video_key in deleted_media_storage:
            # This branch won't execute here since we removed it above on success
            pass
        else:
            await query.message.reply_text("♻️ Media restored.")
    except Exception:
        pass


async def fix_media_entry(query, context: ContextTypes.DEFAULT_TYPE, video_key: str):
    """Allow admin to fix corrupted media entry by providing correct data"""
    if "_" not in video_key:
        await query.answer("❌ Bad key", show_alert=False)
        return
    
    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await query.answer("❌ Bad index", show_alert=False)
        return
    
    if tag not in media_data or not (0 <= idx < len(media_data[tag])):
        await query.answer("❌ Not found", show_alert=False)
        return
    
    item = media_data[tag][idx]
    
    # Try to auto-fix common issues
    fixed = False
    
    if isinstance(item, dict):
        # Auto-fix missing type field
        if "file_id" in item and "type" not in item:
            # Try to guess type from file_id patterns (Telegram file_id patterns)
            file_id = item["file_id"]
            if file_id.startswith("BAACAgI"):
                item["type"] = "video"
                fixed = True
            elif file_id.startswith("AgACAgI"):
                item["type"] = "photo"
                fixed = True
            elif file_id.startswith("BQACAgI"):
                item["type"] = "document"
                fixed = True
            elif file_id.startswith("CQACAgI"):
                item["type"] = "audio"
                fixed = True
            elif file_id.startswith("AwACAgI"):
                item["type"] = "voice"
                fixed = True
            elif file_id.startswith("CgACAgI"):
                item["type"] = "animation"
                fixed = True
            elif file_id.startswith("CAACAgI"):
                item["type"] = "sticker"
                fixed = True
            else:
                # Default to video if can't determine
                item["type"] = "video"
                fixed = True
        
        # Auto-fix missing file_id (can't really fix this automatically)
        if "type" in item and "file_id" not in item:
            await query.answer("❌ Can't auto-fix missing file_id", show_alert=True)
            return
    
    if fixed:
        save_media()
        await query.answer("✅ Auto-fixed!", show_alert=False)
        await query.message.reply_text(
            f"✅ <b>Media Entry Fixed!</b>\n\n"
            f"📁 Tag: <code>{tag}</code>\n"
            f"📍 Index: <code>{idx}</code>\n"
            f"🔧 Fixed missing fields automatically\n\n"
            f"You can now view this media normally.",
            parse_mode=ParseMode.HTML
        )
    else:
        await query.answer("❌ Cannot auto-fix this entry", show_alert=True)
        await query.message.reply_text(
            f"❌ <b>Cannot Auto-Fix Entry</b>\n\n"
            f"This media entry has issues that require manual intervention.\n"
            f"Consider deleting this corrupted entry and re-uploading the media.",
            parse_mode=ParseMode.HTML
        )


async def show_raw_media_data(query, context: ContextTypes.DEFAULT_TYPE, video_key: str):
    """Show the raw data of a media entry for debugging"""
    if "_" not in video_key:
        await query.answer("❌ Bad key", show_alert=False)
        return
    
    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await query.answer("❌ Bad index", show_alert=False)
        return
    
    if tag not in media_data or not (0 <= idx < len(media_data[tag])):
        await query.answer("❌ Not found", show_alert=False)
        return
    
    item = media_data[tag][idx]
    
    # Format the raw data for display
    try:
        raw_data = json.dumps(item, indent=2, ensure_ascii=False)
        if len(raw_data) > 3000:  # Telegram message limit consideration
            raw_data = raw_data[:3000] + "...\n(truncated)"
    except Exception as e:
        raw_data = f"Error serializing data: {e}\nRaw repr: {repr(item)}"
    
    await query.answer("📄 Raw data shown", show_alert=False)
    await query.message.reply_text(
        f"📄 <b>Raw Media Data</b>\n\n"
        f"📁 Tag: <code>{tag}</code>\n"
        f"📍 Index: <code>{idx}</code>\n"
        f"🗂️ Data Type: <code>{type(item).__name__}</code>\n\n"
        f"<pre>{raw_data}</pre>",
        parse_mode=ParseMode.HTML
    )

def build_admin_control_row(video_key: str, show_delete=True):
    """Return list of rows (each row list of buttons) for admin control based on state."""
    global deleted_media_storage
    
    if "_" not in video_key:
        return []
    
    # Check if this item is in deleted storage
    is_deleted = video_key in deleted_media_storage
    
    # If deleted, show restore button
    if is_deleted:
        return [[InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")]]
    
    # Check if item exists in current media_data
    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        return []
    
    if tag not in media_data or not (0 <= idx < len(media_data[tag])):
        # Item doesn't exist in current data, check if it's deleted
        if is_deleted:
            return [[InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")]]
        return []
    
    entry = media_data[tag][idx]
    # Check for revoked status
    is_revoked = isinstance(entry, dict) and entry.get("revoked")
    
    rows = []
    if is_revoked:
        # When revoked show single restore button
        rows.append([InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")])
    else:
        # Active state: show PUSH button first, then revoke + delete in ONE row to reduce vertical space
        push_button = InlineKeyboardButton("🔄 PUSH", callback_data=f"p_{video_key}")
        rows.append([push_button])
        row = [InlineKeyboardButton("🛑 Revoke", callback_data=f"revoke_media_{video_key}")]
        if show_delete:
            row.append(InlineKeyboardButton("🗑️ Delete", callback_data=f"del_media_{video_key}"))
        rows.append(row)
    return rows





async def safe_send_message(context, chat_id, text=None, photo=None, video=None, document=None, 
                           audio=None, voice=None, animation=None, sticker=None, caption=None, 
                           reply_markup=None, parse_mode=None, protect_content=None, max_retries=3, auto_delete=True, user_id=None):
    """Safely send a message with retry logic for handling timeouts and auto-deletion tracking"""
    # Use global protection setting if not explicitly specified
    if protect_content is None:
        protect_content = should_protect_content(user_id, chat_id)
        
    for attempt in range(max_retries):
        try:
            message = None
            if photo:
                message = await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content
                )
            elif video:
                message = await context.bot.send_video(
                    chat_id=chat_id,
                    video=video,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content
                )
            elif document:
                message = await context.bot.send_document(
                    chat_id=chat_id,
                    document=document,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content
                )
            elif audio:
                message = await context.bot.send_audio(
                    chat_id=chat_id,
                    audio=audio,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content
                )
            elif voice:
                message = await context.bot.send_voice(
                    chat_id=chat_id,
                    voice=voice,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content
                )
            elif animation:
                message = await context.bot.send_animation(
                    chat_id=chat_id,
                    animation=animation,
                    caption=caption,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    protect_content=protect_content
                )
            elif sticker:
                message = await context.bot.send_sticker(
                    chat_id=chat_id,
                    sticker=sticker,
                    reply_markup=reply_markup,
                    protect_content=protect_content
                )
            else:
                message = await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode
                )
            
            # Track message for auto-deletion if it contains media
            if message and auto_delete and (photo or video or document or audio or voice or animation or sticker):
                await track_sent_message(message)
            
            return message
                
        except TimedOut:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                print(f"Timeout on attempt {attempt + 1}, retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                print(f"Failed to send message after {max_retries} attempts due to timeout")
                return None
                
        except RetryAfter as e:
            if attempt < max_retries - 1:
                wait_time = e.retry_after + 1
                print(f"Rate limited, waiting {wait_time}s before retry...")
                await asyncio.sleep(wait_time)
                continue
            else:
                print(f"Failed to send message after {max_retries} attempts due to rate limiting")
                return None
                
        except (NetworkError, BadRequest) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Network/Request error on attempt {attempt + 1}: {e}, retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                print(f"Failed to send message after {max_retries} attempts due to: {e}")
                return None
                
        except Exception as e:
            print(f"Unexpected error sending message: {e}")
            return None
    
    return None


async def send_media_by_type(context, chat_id, item, caption, reply_markup, parse_mode=ParseMode.HTML, protect_content=None, user_id=None):
    """Helper function to send media using the correct method based on type"""
    # Use global protection setting if not explicitly specified
    if protect_content is None:
        protect_content = should_protect_content(user_id, chat_id)
        
    media_kwargs = {
        "context": context,
        "chat_id": chat_id,
        "caption": caption,
        "reply_markup": reply_markup,
        "parse_mode": parse_mode,
        "protect_content": protect_content
    }
    
    if item["type"] == "video":
        media_kwargs["video"] = item["file_id"]
    elif item["type"] == "photo":
        media_kwargs["photo"] = item["file_id"]
    elif item["type"] == "document":
        media_kwargs["document"] = item["file_id"]
    elif item["type"] == "audio":
        media_kwargs["audio"] = item["file_id"]
    elif item["type"] == "voice":
        media_kwargs["voice"] = item["file_id"]
    elif item["type"] == "animation":
        media_kwargs["animation"] = item["file_id"]
    elif item["type"] == "sticker":
        media_kwargs["sticker"] = item["file_id"]
        # Stickers don't support captions
        media_kwargs.pop("caption", None)
    else:
        # Fallback for unknown types
        media_kwargs["text"] = f"Unsupported media type: {item['type']}"
    
    return await safe_send_message(**media_kwargs)


def build_final_caption(original_caption="", add_global=True):
    """Build the final caption with global caption"""
    # Use original caption as-is
    processed_caption = original_caption if original_caption else ""
    
    # Add global caption if enabled
    global_caption = caption_config.get("global_caption", "") if add_global else ""
    
    # Combine captions
    if global_caption and processed_caption:
        return f"{processed_caption}\n\n{global_caption}"
    elif global_caption:
        return global_caption
    elif processed_caption:
        return processed_caption
    else:
        return ""


def build_media_caption(original_caption="", tag="", index="", share_link="", media_type="video"):
    """Build caption specifically for media files (videos/photos) sent to users"""
    # Create the base system caption with metadata based on media type
    if media_type == "video":
        base_caption = f"🎬 <b>Shared Video</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "photo":
        base_caption = f"🖼️ <b>Shared Photo</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "document":
        base_caption = f"📄 <b>Shared Document</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "audio":
        base_caption = f"🎵 <b>Shared Audio</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "voice":
        base_caption = f"🎤 <b>Shared Voice</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "animation":
        base_caption = f"🎞️ <b>Shared Animation</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    elif media_type == "sticker":
        base_caption = f"🎭 <b>Shared Sticker</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    else:
        # Generic fallback for any other type
        base_caption = f"📎 <b>Shared File</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_link}'>🔗 Link</a>"
    
    # Apply global caption
    return build_final_caption(base_caption, add_global=True)


# Global helper: provide a Home keyboard accessible from any function
def get_home_keyboard(user_id):
    """Return a quick-access Home keyboard. This global helper mirrors the
    locally-scoped variant used in handlers so functions like top_videos_command
    can safely reference it."""
    try:
        if is_admin(user_id):
            return ReplyKeyboardMarkup([
                ["📊 Stats", "📢 Broadcast", "👥 Users"],
                ["🧹 Cleanup", "🔧 Tools", "📂 Media"],
                ["⚡ Protection", "🔬 Test", "🏠 Full Menu"]
            ], resize_keyboard=True)
        else:
            return ReplyKeyboardMarkup([
                ["🔀 Random Pick", "🔥 Top Resources"],
                ["📚 Browse Library", "🔖 Bookmarks"]
            ], resize_keyboard=True)
    except Exception:
        # As a safe fallback, return None so callers can omit reply_markup
        return None


# ================== INLINE KEYBOARD BUILDERS (Dynamic) ==================
def build_random_inline_keyboard(video_key: str, mode: str, is_favorited: bool, is_admin_user: bool = False):
    """Build a dynamic inline keyboard for random content.
    mode: 'safe' (Browse Library) or 'adult' (Random Pick)
    """
    # video_key format: tag_index, but for individual sharing we need tag_index_index
    if "_" in video_key:
        tag, idx = video_key.rsplit("_", 1)
        share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
    else:
        share_link = f"https://t.me/{BOT_USERNAME}?start={video_key}"
    share_url = f"https://t.me/share/url?url={share_link}"

    tag, idx = ("unknown", "0")
    if "_" in video_key:
        parts = video_key.rsplit("_", 1)
        tag, idx = parts[0], parts[1]

    # Favorite toggle button
    if is_favorited:
        fav_btn = InlineKeyboardButton("💔 Remove", callback_data=f"remove_fav_{video_key}")
    else:
        fav_btn = InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{video_key}")

    if mode == "safe":
        next_btn = InlineKeyboardButton("📚 Next", callback_data="next_safe")
        switch_btn = InlineKeyboardButton("🔀 Random Pick", callback_data="switch_to_adult")
    else:
        next_btn = InlineKeyboardButton("🔀 Next", callback_data="next_adult")
        switch_btn = InlineKeyboardButton("📚 Browse", callback_data="switch_to_safe")

    share_btn = InlineKeyboardButton("🔗 Share", url=share_url)
    favs_btn = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")

    keyboard = [
        [fav_btn, next_btn],
        [switch_btn, share_btn],
        [favs_btn]
    ]
    # Admin removal button
    if is_admin_user:
        keyboard += build_admin_control_row(video_key)
    return InlineKeyboardMarkup(keyboard)


async def send_random_video(context: ContextTypes.DEFAULT_TYPE, chat_id: int, mode: str, user_id: int = None):
    """Send a random video in given mode with dynamic buttons."""
    video_key = None
    if mode == "safe":
        video_key = get_random_from_tag_no_repeat("random")
    else:
        video_key = get_random_from_passed_links()

    if not video_key:
        # graceful error
        if mode == "safe":
            await context.bot.send_message(chat_id, "❌ No media files available in random tag.")
        else:
            await context.bot.send_message(chat_id, "❌ No passed videos available. Admin needs to pass some videos first.")
        return

    if "_" not in video_key:
        await context.bot.send_message(chat_id, "❌ Invalid media reference.")
        return

    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await context.bot.send_message(chat_id, "❌ Invalid video index.")
        return

    if tag not in media_data or not isinstance(media_data[tag], list) or not (0 <= idx < len(media_data[tag])):
        await context.bot.send_message(chat_id, "❌ Video not found in database.")
        return

    video_data = media_data[tag][idx]
    
    # Check if media is revoked (skip revoked media in random access)
    if isinstance(video_data, dict) and video_data.get("revoked"):
        # Try to get another random video instead
        if mode == "safe":
            backup_key = get_random_from_tag_no_repeat("random")
        else:
            backup_key = get_random_from_passed_links()
        
        if backup_key and backup_key != video_key:
            # Recursively try with backup key
            await send_random_video(context, chat_id, mode)
            return
        else:
            # No alternative found
            await context.bot.send_message(chat_id, "❌ No available media found.")
            return
    user_id_str = str(chat_id)  # For favorites toggle (private chat context)
    is_favorited = user_id_str in favorites_data.get("user_favorites", {}) and video_key in favorites_data["user_favorites"].get(user_id_str, [])

    # For individual video sharing, use tag_index_index format
    share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
    share_url = f"https://t.me/share/url?url={share_link}"

    base_caption = ("📚 <b>Browse Library</b>" if mode == "safe" else "🔀 <b>Random Pick</b>") \
        + f"\n\n📁 Collection: <code>{tag}</code> | Index: <code>{idx}</code> | <a href='{share_link}'>🔗 Link</a>"

    reply_markup = build_random_inline_keyboard(video_key, mode, is_favorited, is_admin_user=is_admin(chat_id))

    if video_data.get("type") == "video" and "file_id" in video_data:
        await send_and_track_message(context.bot.send_video, 
                                     chat_id=chat_id,
                                     video=video_data["file_id"],
                                     caption=build_final_caption(base_caption, add_global=False),
                                     parse_mode=ParseMode.HTML,
                                     protect_content=should_protect_content(user_id, chat_id),
                                     reply_markup=reply_markup)
    elif video_data.get("type") == "photo" and "file_id" in video_data:
        await send_and_track_message(context.bot.send_photo,
                                     chat_id=chat_id,
                                     photo=video_data["file_id"],
                                     caption=build_final_caption(base_caption, add_global=False),
                                     parse_mode=ParseMode.HTML,
                                     protect_content=should_protect_content(user_id, chat_id),
                                     reply_markup=reply_markup)
    elif video_data.get("type") == "document" and "file_id" in video_data:
        await send_and_track_message(context.bot.send_document,
                                     chat_id=chat_id,
                                     document=video_data["file_id"],
                                     caption=build_final_caption(base_caption, add_global=False),
                                     parse_mode=ParseMode.HTML,
                                     protect_content=should_protect_content(user_id, chat_id),
                                     reply_markup=reply_markup)
    else:
        await context.bot.send_message(chat_id, "❌ No valid media found for this entry.")


def save_users():
    with open(USERS_FILE, "w") as f:
        json.dump(users_data, f, indent=2)
    # Auto-backup after users changes
    try:
        # Check if there's a running loop before creating task
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
            
        if loop and loop.is_running():
            # Store task reference to prevent "coroutine never awaited" warning
            global auto_backup_task
            auto_backup_task = asyncio.create_task(auto_backup_data("users"))
    except Exception:
        pass


def track_user(user_id, username=None, first_name=None, source="bot_interaction"):
    """Track a user interaction with the bot"""
    user_id_str = str(user_id)
    current_time = asyncio.get_event_loop().time()
    is_new_user = user_id_str not in users_data
    
    # Update or create user entry
    if user_id_str in users_data:
        users_data[user_id_str]["last_seen"] = current_time
        users_data[user_id_str]["username"] = username
        users_data[user_id_str]["first_name"] = first_name
        users_data[user_id_str]["interaction_count"] = users_data[user_id_str].get("interaction_count", 0) + 1
    else:
        users_data[user_id_str] = {
            "first_seen": current_time,
            "last_seen": current_time,
            "username": username,
            "first_name": first_name,
            "interaction_count": 1,
            "source": source
        }
    
    # Save to file
    save_users()
    
    return is_new_user


async def auto_register_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Automatically register any user who interacts with the bot"""
    if update.effective_user and not update.effective_user.is_bot:
        user = update.effective_user
        # Determine the source of interaction
        source = "bot_interaction"
        if update.message:
            if update.message.text and update.message.text.startswith('/start'):
                source = "start_command"
            elif update.message.photo or update.message.video:
                source = "media_upload"
            elif update.message.text:
                source = "text_message"
        elif update.callback_query:
            source = "button_click"
        
        # Track the user with the determined source
        is_new_user = track_user(user.id, user.username, user.first_name, source)
        
        # Only log new user registrations
        if is_new_user:
            log_counters['new_users'] += 1
            print_counter_line()


def migrate_existing_users():
    """Migrate existing users from favorites and exempted users to main users database"""
    current_time = asyncio.get_event_loop().time()
    
    # Migrate from favorites
    if favorites_data and "user_favorites" in favorites_data:
        for user_id_str in favorites_data["user_favorites"].keys():
            if user_id_str not in users_data:
                users_data[user_id_str] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "username": None,
                    "first_name": None,
                    "interaction_count": 1,
                    "source": "favorites_migration"
                }
    
    # Migrate from exempted users
    for user_id in exempted_users:
        user_id_str = str(user_id)
        if user_id_str not in users_data:
            users_data[user_id_str] = {
                "first_seen": current_time,
                "last_seen": current_time,
                "username": None,
                "first_name": None,
                "interaction_count": 1,
                "source": "exempted_migration"
            }
    
    # Save migrated data
    save_users()


def update_random_state():
    """Update the list of all available videos"""
    global random_state
    current_videos = []
    
    # Build list of all individual videos with tag_index format
    for tag, videos in media_data.items():
        if isinstance(videos, list):
            for idx, video in enumerate(videos):
                if isinstance(video, dict) and (video.get("deleted") or video.get("revoked")):
                    continue
                current_videos.append(f"{tag}_{idx}")
    
    # If we have new videos or the all_videos list is different, rebuild it
    if set(random_state["all_videos"]) != set(current_videos):
        random_state["all_videos"] = current_videos
        # Remove any shown videos that no longer exist
        random_state["shown_videos"] = [v for v in random_state["shown_videos"] if v in current_videos]
        save_random_state()


def get_next_random_video():
    """Get next random video ensuring all videos are shown before repeating"""
    global random_state
    
    update_random_state()
    
    if not random_state["all_videos"]:
        return None
    
    # If all videos have been shown, reset the shown list
    if len(random_state["shown_videos"]) >= len(random_state["all_videos"]):
        random_state["shown_videos"] = []
    
    # Get videos that haven't been shown yet
    available_videos = [v for v in random_state["all_videos"] if v not in random_state["shown_videos"]]
    
    if not available_videos:
        # This shouldn't happen, but just in case
        available_videos = random_state["all_videos"]
        random_state["shown_videos"] = []
    
    # Pick a random video from available ones
    selected_video = random.choice(available_videos)
    
    # Mark it as shown
    random_state["shown_videos"].append(selected_video)
    save_random_state()
    
    return selected_video


def get_random_from_tag_no_repeat(tag="random"):
    """Get random resource from full library (formerly GET FILES button)"""
    if tag not in media_data or not media_data[tag]:
        return None
    
    # Create list of all videos from this specific tag
    tag_videos = []
    for i, v in enumerate(media_data[tag]):
        if isinstance(v, dict) and (v.get("deleted") or v.get("revoked")):
            continue
        tag_videos.append(f"{tag}_{i}")
    
    if not tag_videos:
        return None
    
    # Initialize tracking system if not exists
    if 'tag_shown_videos' not in random_state:
        random_state['tag_shown_videos'] = {}
    
    if tag not in random_state['tag_shown_videos']:
        random_state['tag_shown_videos'][tag] = []
    
    # Get videos that haven't been shown yet for this tag
    shown_videos = random_state['tag_shown_videos'][tag]
    available_videos = [v for v in tag_videos if v not in shown_videos]
    
    # If all videos have been shown, reset the shown list for this tag
    if not available_videos:
        random_state['tag_shown_videos'][tag] = []
        available_videos = tag_videos
        log_counters['cycle_resets'] += 1
        print_counter_line()
    
    # Pick a random video from available ones
    selected_video = random.choice(available_videos)
    
    # Mark it as shown for this tag
    random_state['tag_shown_videos'][tag].append(selected_video)
    save_random_state()
    
    log_counters['random_selections'] += 1
    print_counter_line()
    return selected_video


def get_random_from_passed_links():
    """Get random resource from curated pool (formerly RANDOM MEDIA button)"""
    if not passed_links:
        return None
    
    # Filter out revoked media from random selection
    available_links = []
    for video_key in passed_links:
        if "_" not in video_key:
            continue
        try:
            tag, idx_str = video_key.rsplit("_", 1)
            idx = int(idx_str)
            
            # Check if media exists and is not revoked
            if (tag in media_data and 
                isinstance(media_data[tag], list) and 
                0 <= idx < len(media_data[tag])):
                
                item = media_data[tag][idx]
                # Skip revoked items in random selection
                if isinstance(item, dict) and item.get("revoked"):
                    continue
                    
                available_links.append(video_key)
        except (ValueError, IndexError):
            continue
    
    # Return random from available (non-revoked) links
    if not available_links:
        return None
    
    return random.choice(available_links)


async def is_user_member(user_id, bot, channel):
    try:
        # Try with @ prefix first
        member = await bot.get_chat_member(chat_id=f"@{channel}",
                                           user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except:
        try:
            # Try without @ prefix as backup
            member = await bot.get_chat_member(chat_id=channel,
                                               user_id=user_id)
            return member.status in ["member", "administrator", "creator"]
        except:
            return False


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    mention = f'<a href="tg://user?id={user.id}">{html.escape(user.first_name)}</a>'
    args = context.args
    
    # Auto-register user (this handles all user tracking)
    await auto_register_user(update, context)

    if args:
        param = args[0].strip().lower()

        # Handle deleted media commands first
        if param.startswith("view_deleted_") and is_admin(user.id):
            video_key = param.replace("view_deleted_", "")
            if video_key in deleted_media_storage:
                # Create a fake query object to use existing view_deleted_media function
                class FakeQuery:
                    def __init__(self, user, message_obj, callback_data):
                        self.from_user = user
                        self.message = message_obj
                        self.data = callback_data
                    
                    async def answer(self, text="", show_alert=False):
                        pass  # No-op for fake query
                
                fake_query = FakeQuery(user, update.message, f"view_deleted_{video_key}")
                await view_deleted_media(fake_query, context)
            else:
                await update.message.reply_text(f"❌ Deleted media not found: {video_key}")
            return
        
        if param.startswith("restore_") and is_admin(user.id):
            video_key = param.replace("restore_", "")
            
            # Check if it's a deleted item first
            if video_key in deleted_media_storage:
                # Create a fake query object to use existing restore_media_entry function
                class FakeQuery:
                    def __init__(self, user, message_obj):
                        self.from_user = user
                        self.message = message_obj
                    
                    async def answer(self, text="", show_alert=False):
                        if text and "successfully" in text.lower():
                            await update.message.reply_text(text)
                
                fake_query = FakeQuery(user, update.message)
                await restore_media_entry(fake_query, video_key)
            else:
                # Check if it's a revoked item in media_data
                try:
                    tag, idx_str = video_key.rsplit("_", 1)
                    idx = int(idx_str)
                    
                    if tag in media_data and idx < len(media_data[tag]):
                        media_item = media_data[tag][idx]
                        
                        if isinstance(media_item, dict) and media_item.get("revoked"):
                            # Remove the revoked flag to restore the media
                            media_item.pop("revoked", None)
                            
                            save_media()
                            update_random_state()
                            
                            await update.message.reply_text(f"✅ Revoked media <code>{video_key}</code> restored successfully!", parse_mode=ParseMode.HTML)
                            return
                
                    await update.message.reply_text(f"❌ No deleted or revoked media found: {video_key}")
                    return
                    
                except (ValueError, IndexError):
                    await update.message.reply_text(f"❌ Invalid media key format: {video_key}")
                    return
            return

        # Parse parameter - could be just tag or tag_start_end
        if '_' in param and param.count('_') >= 2:
            parts = param.split('_')
            tag = '_'.join(parts[:-2])  # Handle tags with underscores
            try:
                start_index = int(parts[-2])
                end_index = int(parts[-1])
            except ValueError:
                tag = param
                start_index = None
                end_index = None
        else:
            tag = param
            start_index = None
            end_index = None

        # Check if the link is active (unless user is admin)
        if user.id != ADMIN_ID:
            link_is_active = False
            
            # For shareable links, check if param exists directly OR if it's a valid subset of any passlink range
            if param in active_links:
                link_is_active = True
            elif start_index is not None and end_index is not None:
                # Check if this range request falls within any existing passlink range
                for link_key, link_data in active_links.items():
                    if isinstance(link_data, dict) and link_data.get("tag") == tag:
                        stored_start = link_data.get("start_index", 0)
                        stored_end = link_data.get("end_index", 0) 
                        # Check if requested range is within stored range
                        if start_index >= stored_start and end_index <= stored_end:
                            link_is_active = True
                            break
            else:
                # Simple tag format - check if tag exists in any passlink
                for link_key, link_data in active_links.items():
                    if isinstance(link_data, dict) and link_data.get("tag") == tag:
                        link_is_active = True
                        break
            
            # If not accessible, deny access
            if not link_is_active:
                await update.message.reply_text(
                    "🚫 This shareable link is not active or has been revoked."
                )
                return

        # Check if this is a direct passlink created link
        if param in active_links and isinstance(active_links[param], dict):
            # Apply membership verification only if this is NOT a single-file link
            link_data = active_links[param]
            start_idx = link_data.get("start_index", 0)
            end_idx = link_data.get("end_index", 0)
            
            # Single file check (start_index equals end_index)
            is_single_file = start_idx == end_idx
            
            # Skip channel verification for single-file links
            if not is_single_file and user.id not in exempted_users:
                # Verify channel membership for multi-file links
                membership_failed = False
                for ch in REQUIRED_CHANNELS:
                    is_member = await is_user_member(user.id, context.bot, ch)
                    if not is_member:
                        membership_failed = True
                        break
                
                if membership_failed:
                    # For ranged links, preserve the range in retry callback
                    retry_param = param
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("JOIN✨", url="https://t.me/CypherHere"),
                            InlineKeyboardButton("BACKUP CHANNEL🛡️", url="https://t.me/OmenThere")
                        ],
                        [
                            InlineKeyboardButton("🔄 Try Again", callback_data=f"retry_{retry_param}")
                        ]
                    ])
                    
                    # Send notification with image
                    await context.bot.send_photo(
                        chat_id=update.effective_chat.id,
                        photo=CHANNEL_NOTIFY_IMAGE_ID,
                        caption=f"Hey {mention}!!\n"
                               "Welcome to <b>Meow Gang</b> 🕊️\n\n"
                               "<b>⚠️ CHANNEL MEMBERSHIP REQUIRED</b>\n\n"
                               "To access multiple videos, you must join our channels first!\n"
                               "Single video links don't require membership.\n\n"
                               "<i>Once you've joined, click 'Try Again' to access the videos.</i>",
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                    return
            
            await send_passlink_videos(update, context, param, link_data)
            return
        
        # Check if this is a custom range within a passlink range
        if start_index is not None and end_index is not None:
            # Check if this is a single file request (start_index equals end_index)
            is_single_file = start_index == end_index
            
            # Verify channel membership for multi-file links (skip for single file)
            if not is_single_file and user.id not in exempted_users:
                membership_failed = False
                for ch in REQUIRED_CHANNELS:
                    is_member = await is_user_member(user.id, context.bot, ch)
                    print(f"User {user.id} membership in {ch}: {is_member}")
                    if not is_member:
                        membership_failed = True
                        break
                
                if membership_failed:
                    # For ranged links, preserve the range in retry callback
                    retry_param = f"{tag}_{start_index}_{end_index}"
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("JOIN✨", url="https://t.me/CypherHere"),
                            InlineKeyboardButton("BACKUP CHANNEL🛡️", url="https://t.me/OmenThere")
                        ],
                        [
                            InlineKeyboardButton("🔄 Try Again", callback_data=f"retry_{retry_param}")
                        ]
                    ])
                    
                    # Send notification with image
                    await context.bot.send_photo(
                        chat_id=update.effective_chat.id,
                        photo=CHANNEL_NOTIFY_IMAGE_ID,
                        caption=f"Hey {mention}!!\n"
                               "Welcome to <b>Meow Gang</b> 🕊️\n\n"
                               "<b>⚠️ CHANNEL MEMBERSHIP REQUIRED</b>\n\n"
                               "To access multiple videos, you must join our channels first!\n"
                               "Single video links don't require membership.\n\n"
                               "<i>Once you've joined, click 'Try Again' to access the videos.</i>",
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                    return
            
            for link_key, link_data in active_links.items():
                if isinstance(link_data, dict) and link_data.get("tag") == tag:
                    stored_start = link_data.get("start_index", 0)
                    stored_end = link_data.get("end_index", 0)
                    
                    # Check if requested range is within stored range
                    if start_index >= stored_start and end_index <= stored_end:
                        # Get videos directly from media_data for this custom range
                        if tag in media_data and isinstance(media_data[tag], list):
                            tag_videos = media_data[tag]
                            custom_videos = []
                            valid_indices = []
                            for idx in range(start_index, end_index + 1):
                                if 0 <= idx < len(tag_videos):
                                    item = tag_videos[idx]
                                    # Only include valid media items, skip revoked for non-admins
                                    if isinstance(item, dict) and "type" in item and "file_id" in item:
                                        # Skip revoked media for regular users
                                        if item.get("revoked") and not is_admin(update.effective_user.id):
                                            continue
                                        custom_videos.append(item)
                                        valid_indices.append(idx)
                            
                            # Create custom link data for this range
                            custom_link_data = {
                                "type": "passlink_custom",
                                "tag": tag,
                                "start_index": start_index,
                                "end_index": end_index,
                                "videos": custom_videos,
                                "actual_indices": valid_indices
                            }
                            await send_passlink_videos(update, context, param, custom_link_data)
                            return
        else:
            # Simple tag format - serve all videos from any matching passlink
            for link_key, link_data in active_links.items():
                if isinstance(link_data, dict) and link_data.get("tag") == tag:
                    # Apply membership verification only if this is NOT a single-file link
                    start_idx = link_data.get("start_index", 0)
                    end_idx = link_data.get("end_index", 0)
                    
                    # Single file check (start_index equals end_index)
                    is_single_file = start_idx == end_idx
                    
                    # Skip channel verification for single-file links
                    if not is_single_file and user.id not in exempted_users:
                        # Verify channel membership for multi-file links
                        membership_failed = False
                        for ch in REQUIRED_CHANNELS:
                            is_member = await is_user_member(user.id, context.bot, ch)
                            if not is_member:
                                membership_failed = True
                                break
                        
                        if membership_failed:
                            # For ranged links, preserve the range in retry callback
                            retry_param = link_key
                            keyboard = InlineKeyboardMarkup([
                                [
                                    InlineKeyboardButton("JOIN✨", url="https://t.me/CypherHere"),
                                    InlineKeyboardButton("BACKUP CHANNEL🛡️", url="https://t.me/OmenThere")
                                ],
                                [
                                    InlineKeyboardButton("🔄 Try Again", callback_data=f"retry_{retry_param}")
                                ]
                            ])
                            
                            # Send notification with image
                            await context.bot.send_photo(
                                chat_id=update.effective_chat.id,
                                photo=CHANNEL_NOTIFY_IMAGE_ID,
                                caption=f"Hey {mention}!!\n"
                                       "Welcome to <b>Meow Gang</b> 🕊️\n\n"
                                       "<b>⚠️ CHANNEL MEMBERSHIP REQUIRED</b>\n\n"
                                       "To access multiple videos, you must join our channels first!\n"
                                       "Single video links don't require membership.\n\n"
                                       "<i>Once you've joined, click 'Try Again' to access the videos.</i>",
                                parse_mode=ParseMode.HTML,
                                reply_markup=keyboard
                            )
                            return
                    
                    await send_passlink_videos(update, context, param, link_data)
                    return

        # Check if it's an individual video key (tag_index format)
        if '_' in param:
            parts = param.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                video_tag = '_'.join(parts[:-1])
                try:
                    video_index = int(parts[-1])
                    
                    # First check if this video is within any active passlink range
                    for link_key, link_data in active_links.items():
                        if isinstance(link_data, dict) and link_data.get("tag") == video_tag:
                            passlink_start = link_data.get("start_index", 0)
                            passlink_end = link_data.get("end_index", 0)
                            
                            # If the video index is within the passlink range, serve from passlink
                            if passlink_start <= video_index <= passlink_end:
                                videos = link_data.get("videos", [])
                                if videos and (video_index - passlink_start) < len(videos):
                                    # Create custom link data for just this single video
                                    single_video_data = {
                                        "type": "passlink_single",
                                        "tag": video_tag,
                                        "start_index": video_index,
                                        "end_index": video_index,
                                        "videos": [videos[video_index - passlink_start]]
                                    }
                                    await send_passlink_videos(update, context, param, single_video_data)
                                    return
                    
                    # If not in any passlink, check if this video exists in media_data
                    if video_tag in media_data and isinstance(media_data[video_tag], list):
                        if 0 <= video_index < len(media_data[video_tag]):
                            # Send the specific video
                            video_data = media_data[video_tag][video_index]
                            
                            # Check if media is revoked
                            if isinstance(video_data, dict) and video_data.get("revoked"):
                                await update.message.reply_text("❌ This media has been revoked and is no longer accessible.")
                                return
                            
                            # Create shareable link for this specific video
                            share_link = f"https://t.me/{BOT_USERNAME}?start={param}"
                            share_url = f"https://t.me/share/url?url={share_link}"
                            
                            keyboard = [
                                [
                                    InlineKeyboardButton("❤️ ADD", callback_data=f"add_fav_{param}"),
                                    InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                                ]
                            ]
                            reply_markup = InlineKeyboardMarkup(keyboard)
                            
                            if video_data.get("type") == "video" and "file_id" in video_data:
                                # Build media caption
                                final_caption = build_media_caption("", video_tag, video_index, share_link, "video")
                                
                                await safe_send_message(
                                    context=context,
                                    chat_id=update.effective_chat.id,
                                    video=video_data["file_id"],
                                    caption=final_caption,
                                    reply_markup=reply_markup,
                                    parse_mode=ParseMode.HTML,
                                    protect_content=should_protect_content(update.effective_user.id, update.effective_chat.id)
                                )
                            elif video_data.get("type") == "photo" and "file_id" in video_data:
                                # Build media caption
                                final_caption = build_media_caption("", video_tag, video_index, share_link, "photo")
                                
                                await safe_send_message(
                                    context=context,
                                    chat_id=update.effective_chat.id,
                                    photo=video_data["file_id"],
                                    caption=final_caption,
                                    reply_markup=reply_markup,
                                    parse_mode=ParseMode.HTML,
                                    protect_content=should_protect_content(update.effective_user.id, update.effective_chat.id)
                                )
                            return
                except ValueError:
                    pass

        await send_tagged_with_range(update, context, tag, start_index,
                                     end_index)
        return

    text = (f"👋 Hey {mention}!!\n"
            "ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ <b>ᴍᴇᴏᴡ ɢᴀɴɢ</b> 💌\n"
            "═══════════════════════════\n"
            "<b>𝓽𝓲𝓻𝓮𝓭 𝓸𝓯 𝓪𝓭𝓼 𝓪𝓷𝓭 𝓳𝓸𝓲𝓷𝓲𝓷𝓰 𝓶𝓾𝓵𝓽𝓲𝓹𝓵𝓮 𝓬𝓱𝓪𝓷𝓷𝓮𝓵𝓼? 𝓷𝓸𝓽 𝓽𝓸 𝔀𝓸𝓻𝓻𝔂 𝓪𝓷𝔂𝓶𝓸𝓻𝓮. 𝓬𝓵𝒾𝓬𝓴 𝓫𝓮𝓵𝓸𝔀 𝓫𝓾𝓽𝓽𝓸𝓷 𝓪𝓷𝓭 𝓼𝓮𝓮 𝓽𝓱𝓮 𝓶𝓪𝓰𝓲𝓬</b>.\n"
            "<b>ρєα¢є συт</b> ✌️\n\n")

    # Inline keyboard (restored) keeping only desired buttons
    inline_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("BACKUP CHANNEL🛡️", url="https://t.me/OmenThere")],
        [InlineKeyboardButton("❓ HELP", callback_data="show_help")]
    ])

    # Reply keyboard (main menu) only visible on /start
    # Rebranded main reply keyboard (was Random 18-/18+)
    reply_keyboard = ReplyKeyboardMarkup(
        [
            ["📚 Browse Library", "🔀 Random Pick"],
            ["🔖 Bookmarks"]
        ],
        resize_keyboard=True
    )

    # Send image with caption and inline keyboard (reply keyboard sent separately to persist)
    photo_sent = False
    
    # Try different methods to send the welcome image
    try:
        # Method 1: Try using stored file_id if available
        global WELCOME_IMAGE_FILE_ID
        if WELCOME_IMAGE_FILE_ID:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=WELCOME_IMAGE_FILE_ID,
                caption=text,
                parse_mode="HTML",
                protect_content=should_protect_content(update.effective_user.id, update.effective_chat.id),
            reply_markup=inline_keyboard
            )
            photo_sent = True
        else:
            # No stored file_id available
            photo_sent = False
    except Exception as e:
        print(f"⚠️ Error sending welcome photo: {e}")
        photo_sent = False
    
    # Fallback: Send message without image if photo failed
    if not photo_sent:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"🖼️ <i>(Welcome image temporarily unavailable)</i>\n\n{text}",
            parse_mode="HTML",
            protect_content=should_protect_content(update.effective_user.id, update.effective_chat.id),
            reply_markup=inline_keyboard
        )    # Send a separate message to attach the reply keyboard for actions
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Select an action below ⬇️",
        reply_markup=reply_keyboard
    )

    if is_admin(user.id):
        admin_text = ("<b>💡 Admin Commands:</b>\n"
                     "<pre>/upload &lt;tag&gt;\n"
                     "/listvideo (special tags)\n"
                     "/listvideos (other tags)\n"
                     "/remove &lt;tag&gt;\n"
                     "/get &lt;tag&gt;\n"
                     "/view &lt;tag&gt; [start] [end]\n"
                     "/generatelink &lt;tag&gt; [start] [end]\n"
                     "/pass &lt;tag&gt; [start] [end]\n"
                     "/passlink &lt;tag&gt; [start] [end]\n"
                     "/revoke &lt;tag&gt; [start] [end]\n"
                     "/revokelink &lt;tag&gt; [start] [end]\n"
                     "/activelinks\n"
                     "/passlinks\n"
                     "/listactive\n"
                     "/free &lt;user_id&gt;\n"
                     "/unfree &lt;user_id&gt;\n"
                     "/listfree\n"
                     "/protectionon\n"
                     "/protectionoff\n"
                     "/protection\n"
                     "/userfavorites &lt;user_id&gt;\n"
                     "/videostats &lt;tag&gt; &lt;index&gt;\n"
                     "/topvideos\n\n"
                     "🗑️ Deleted Media Management:\n"
                     "/listdeleted - Show all deleted media\n"
                     "/listremoved - Show all revoked media\n"
                     "/cleanupdeleted - Clean up corrupted deleted media\n"
                     "/restoredeleted &lt;video_key&gt; - Restore specific media\n"
                     "/restoreall - Restore all deleted media\n"
                     "/cleardeleted - Permanently remove all deleted media\n"
                     "/deletedstats - Statistics about deleted media\n\n"
                     "🧹 Auto-Delete System:\n"
                     "/autodelete - Manage auto-deletion settings\n"
                     "/autodelete on/off - Enable/disable auto-deletion\n"
                     "/autodelete hours &lt;number&gt; - Set deletion time\n"
                     "/autodelete stats - Show tracking statistics\n"
                     "/autodelete clear - Clear tracking list\n"
                     "/notifications - Control deletion notifications\n"
                     "/notifications on/off - Enable/disable notifications\n\n"
                     "📢 Broadcasting Commands:\n"
                     "/broadcast &lt;message&gt; - Normal broadcast\n"
                     "/dbroadcast &lt;message&gt; - Auto-delete broadcast\n"
                     "/pbroadcast &lt;message&gt; - Pin broadcast\n"
                     "/sbroadcast &lt;message&gt; - Silent broadcast\n"
                     "/fbroadcast &lt;message&gt; - Forward mode broadcast\n"
                     "/bstats - Broadcasting statistics\n"
                     "/userstats - User registration statistics\n"
                     "/topusers - Most active users ranking\n"
                     "/addusers &lt;id1&gt; &lt;id2&gt; ... - Add users to database\n"
                     "/discover - Discover users from all sources\n\n"
                     "📥 Batch Processing Commands:\n"
                     "/custom_batch &lt;tag&gt; - Start custom batch collection\n"
                     "/stop_batch - Finish custom batch and get link\n"
                     "/cancel_batch - Cancel active custom batch\n"
                     "/batch_status - Check batch session status\n\n"
                     "📝 Caption Management:\n"
                     "/set_global_caption &lt;text&gt; - Set global caption for all files\n\n"
                     "👨‍💼 Admin Management:\n"
                     "/add_admin &lt;user_id&gt; - Add new admin (Main admin only)\n"
                     "/remove_admin &lt;user_id&gt; - Remove admin (Main admin only)\n"
                     "/list_admins - List all admins\n\n"
                     "🔧 Testing & Debug Commands:\n"
                     "/testprotection - Test media protection functionality\n"
                     "/testdeletion - Test deletion functionality\n"
                     "/checkprotection - Check protection status details\n"
                     "/checkupdates - Check for pending old user requests\n"
                     "/debugdeleted &lt;tag&gt; - Debug deleted media issues\n"
                     "/cleanupdeleted - Clean up corrupted deleted media entries\n\n"
                     "⚡ Quick Protection Commands:\n"
                     "/pon - Protection ON (shortcut)\n"
                     "/poff - Protection OFF (shortcut)\n"
                     "/pstatus - Protection status (shortcut)\n"
                     "/ps - Protection status (very short)\n\n"
                     "� Backup & Recovery Commands:\n"
                     "/backup - Create local backup (Railway storage)\n"
                     "/telegrambackup - Send live JSON files to Telegram\n"
                     "/autobackup on/off/now/status - Manage daily auto-backup\n"
                     "/listbackups - List all available backups\n"
                     "/restore <name> - Restore from backup\n"
                     "/backupstats <name> - Show backup details\n"
                     "/deletebackup <name> - Delete a backup\n\n"
                     "�🖼️ Welcome Image Management:\n"
                     "/getfileid - Get file_id from any media (reply to media)\n"
                     "/setwelcomeimage - Manage welcome image\n"
                     "/testwelcomeimage - Test welcome image display</pre>")
        await update.message.reply_html(admin_text)


def generate_revoked_list(page=1):
    """Helper function to generate revoked media list content and keyboard"""
    # Get revoked items from media_data with details
    revoked_entries = []
    for tag, vids in media_data.items():
        if not isinstance(vids, list):
            continue
        for idx, v in enumerate(vids):
            if isinstance(v, dict) and v.get("revoked"):
                media_type = v.get("type", "unknown")
                
                # Get type emoji
                if media_type == "video":
                    type_icon = "🎥"
                elif media_type == "photo":
                    type_icon = "🖼"
                elif media_type == "document":
                    type_icon = "📄"
                else:
                    type_icon = "📎"
                    
                revoked_entries.append((f"{tag}_{idx}", tag, str(idx), media_type, type_icon))
    
    # Check if we have any revoked media
    if not revoked_entries:
        return "📋 No revoked media found.", None
    
    # Sort by tag, then by index
    revoked_entries.sort(key=lambda x: (x[1], int(x[2]) if x[2].isdigit() else 0))
    
    # Paginate (show 20 per page like deleted media)
    items_per_page = 20
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    page_entries = revoked_entries[start_idx:end_idx]
    
    if not page_entries:
        return f"📄 Page {page} is empty.", None
    
    # Build the beautiful formatted message
    text_lines = [f"🛑 <b>Revoked Media</b> (Page {page})"]
    text_lines.append("")
    
    for i, (key, tag, index, media_type, type_icon) in enumerate(page_entries, 1):
        # Create view link
        view_format = f"{tag}_{index}_{index}"
        view_link = f"https://t.me/GlitchedMatrixbot?start={view_format}"
        
        # Format like deleted media: "1. Tag: code | Index: code | 💎 type | icon | View | Restore"
        line = f"{i}. Tag: <code>{tag}</code> | Index: <code>{index}</code> | 💎 {media_type} | 🛑 {type_icon} | <a href='{view_link}'>View</a> | <a href='https://t.me/GlitchedMatrixbot?start=restore_{key}'>Restore</a>"
        text_lines.append(line)
    
    # Add pagination info
    total_pages = (len(revoked_entries) + items_per_page - 1) // items_per_page
    text_lines.append("")
    text_lines.append(f"📊 Showing {len(page_entries)} of {len(revoked_entries)} items")
    
    # Create inline keyboard for pagination
    keyboard = []
    nav_buttons = []
    
    # Previous page button
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Prev", callback_data=f"revoked_page_{page-1}"))
    
    # Page info
    nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    
    # Next page button
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"revoked_page_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Action buttons
    action_buttons = [
        InlineKeyboardButton("🔄 Refresh", callback_data="refresh_revoked_list"),
        InlineKeyboardButton("♻️ Restore All", callback_data="cleanup_revoked")
    ]
    keyboard.append(action_buttons)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    return "\n".join(text_lines), reply_markup

# List revoked media command (moved above callback handlers)
async def listremoved_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    
    # Get page number
    page = 1
    if context.args and len(context.args) > 0:
        try:
            page = max(1, int(context.args[0]))
        except ValueError:
            page = 1
    
    text, reply_markup = generate_revoked_list(page)
    
    if reply_markup is None:
        await update.message.reply_text(text)
    else:
        await update.message.reply_html(text, reply_markup=reply_markup)

async def handle_button_click(update: Update,
                              context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    # Answer the callback query immediately to prevent timeout
    await safe_answer_callback_query(query)
    
    # Auto-register user (this handles all user tracking)
    await auto_register_user(update, context)

    if query.data == "get_random":
    # Browse Library action - originally GET FILES (uses 'random' collection pool)
        await send_random_video(context, query.message.chat_id, mode="safe", user_id=query.from_user.id)

    elif query.data == "random_media":
        await send_random_video(context, query.message.chat_id, mode="adult", user_id=query.from_user.id)

    elif query.data == "next_safe":
        await send_random_video(context, query.message.chat_id, mode="safe", user_id=query.from_user.id)
    elif query.data == "next_adult":
        await send_random_video(context, query.message.chat_id, mode="adult", user_id=query.from_user.id)
    elif query.data == "switch_to_adult":
        await send_random_video(context, query.message.chat_id, mode="adult", user_id=query.from_user.id)
    elif query.data == "switch_to_safe":
        await send_random_video(context, query.message.chat_id, mode="safe", user_id=query.from_user.id)

    elif query.data == "view_favorites":
        await show_favorites_navigator(query, context, 0)

    elif query.data == "show_help":
        help_text = (
            "🤖 <b>Knowledge Vault Bot Guide:</b>\n\n"
            "❤️ <b>Bookmarks:</b>\n"
            "• Click ❤️ on any resource to add to bookmarks\n"
            "• Click 💔 on any resource to remove from bookmarks\n"
            "• Use 🔖 Bookmarks to browse your saved resources\n"
            "• Navigate with ⬅️ Previous and ➡️ Next buttons\n\n"
            "📚 <b>Accessing Resources:</b>\n"
            "• Click 📚 Browse Library to explore structured collections\n"
            "• Click 🔀 Random Pick for serendipitous discovery\n"
            "• Use the navigation buttons to move between resources\n"
            "• Use shared links to access specific content\n\n"
            "🎯 <b>Smart Collection System:</b>\n"
            "• Resources are organized by collections (tags)\n"
            "• Fair rotation ensures you discover new content\n\n"
            "💡 <b>Tip:</b> Every resource has 🔖 Bookmarks for saving your favorites!"
        )
        await query.message.reply_html(help_text)

    elif query.data.startswith("fav_nav_"):
        # Handle favorites navigation
        index = int(query.data.replace("fav_nav_", ""))
        await show_favorites_navigator(query, context, index, edit_message=True)

    elif query.data.startswith("add_fav_"):
        await add_to_favorites(query, context)

    elif query.data.startswith("remove_fav_"):
        await remove_from_favorites(query, context)

    elif query.data.startswith("who_liked_"):
        await show_who_liked_video(query, context)

    elif query.data.startswith("view_video_"):
        await view_specific_video(query, context)

    elif query.data.startswith("view_deleted_"):
        await view_deleted_media(query, context)

    elif query.data.startswith("p_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("p_", "")
        await show_push_tag_selector(query, context, video_key)

    elif query.data.startswith("pa_") or query.data.startswith("pr_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        await handle_push_tag_action(query, context)

    elif query.data.startswith("del_media_"):
        # Admin / owner only
        if not is_admin(query.from_user.id):
            print(f"ERROR: User {query.from_user.id} is not an admin")
            await query.answer("❌ Not allowed", show_alert=False)
            return
        video_key = query.data.replace("del_media_", "")
        await delete_media_entry(query, context, video_key)
    elif query.data.startswith("revoke_media_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("revoke_media_", "")
        await revoke_media_entry(query, video_key)
    elif query.data.startswith("restore_media_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("restore_media_", "")
        await restore_media_entry(query, video_key)
    
    elif query.data.startswith("fix_media_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("fix_media_", "")
        await fix_media_entry(query, context, video_key)
    
    elif query.data.startswith("show_raw_"):
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        video_key = query.data.replace("show_raw_", "")
        await show_raw_media_data(query, context, video_key)

    elif query.data.startswith("retry_"):
        param = query.data.replace("retry_", "")

        # Parse parameter - could be just tag or tag_start_end
        if '_' in param and param.count('_') >= 2:
            parts = param.split('_')
            tag = '_'.join(parts[:-2])  # Handle tags with underscores
            try:
                start_index = int(parts[-2])
                end_index = int(parts[-1])
            except ValueError:
                tag = param
                start_index = None
                end_index = None
        else:
            tag = param
            start_index = None
            end_index = None

        # Create new context for callback query
        await send_tagged_from_callback_with_range(query, context, tag,
                                                   start_index, end_index)
    
    elif query.data.startswith("topvideos_page_"):
        # Handle top videos navigation
        page = int(query.data.replace("topvideos_page_", ""))
        await show_top_videos_page(None, context, page, query)
    
    elif query.data == "topvideos_info":
        # Just answer the query, do nothing (info button)
        await query.answer("� Top Videos Navigation", show_alert=False)
    
    elif query.data.startswith("topvideo_view_"):
        # Handle top video viewer navigation
        page = int(query.data.replace("topvideo_view_", ""))
        await show_top_videos_viewer(None, context, page, query)
    
    elif query.data == "close_menu":
        # Close the menu by deleting the message
        await query.message.delete()
        await query.answer("Menu closed", show_alert=False)
    
    elif query.data.startswith("topusers_page_"):
        # Handle top users navigation
        page = int(query.data.replace("topusers_page_", ""))
        await show_top_users_page(None, context, page, query)
    
    elif query.data == "topusers_info":
        # Just answer the query, do nothing (info button)
        await query.answer("👥 Top Users Navigation", show_alert=False)
    
    elif query.data == "close_menu":
        # Close the inline menu
        await query.edit_message_text("✅ Menu closed.")
    
    elif query.data == "random_safe":
        # Send random safe video (18-)
        await query.message.delete()  # Remove the selection menu
        await send_random_video(context, query.from_user.id, mode="safe")
    
    elif query.data == "random_nsfw":
        # Send random NSFW video (18+)  
        await query.message.delete()  # Remove the selection menu
        await send_random_video(context, query.from_user.id, mode="nsfw")
    
    elif query.data == "confirm_clear_deleted":
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        deleted_count = len(deleted_media_storage)
        deleted_media_storage.clear()
        save_deleted_media()
        
        await query.answer("🗑️ All deleted media cleared", show_alert=False)
        await query.edit_message_text(
            f"✅ <b>Deletion Complete</b>\n\n"
            f"🗑️ Permanently removed {deleted_count} deleted media entries.\n"
            f"This action cannot be undone.",
            parse_mode=ParseMode.HTML
        )
    
    elif query.data == "cancel_clear_deleted":
        await query.answer("❌ Clear operation cancelled", show_alert=False)
        await query.edit_message_text(
            "❌ <b>Operation Cancelled</b>\n\n"
            "No deleted media was cleared.",
            parse_mode=ParseMode.HTML
        )
    
    elif query.data == "list_deleted_media":
        # Handle "Back to Deleted List" button
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        # Just simulate the listdeleted command call properly
        try:
            # Create a minimal update object that works with listdeleted_command
            class FakeMessage:
                def __init__(self, chat_id, from_user):
                    self.chat_id = chat_id
                    self.from_user = from_user
                    
                async def reply_text(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, **kwargs)
                    
                async def reply_html(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, parse_mode=ParseMode.HTML, **kwargs)
            
            class FakeUpdate:
                def __init__(self, user, chat_id):
                    self.effective_user = user
                    self.message = FakeMessage(chat_id, user)
            
            fake_update = FakeUpdate(query.from_user, query.message.chat_id)
            
            # Delete the current message first
            try:
                await query.message.delete()
            except Exception as e:
                print(f"DEBUG: Could not delete message: {e}")
            
            # Call the actual listdeleted command
            await listdeleted_command(fake_update, context)
            await query.answer("🔄 Deleted list refreshed!")
            
        except Exception as e:
            print(f"ERROR in list_deleted_media callback: {e}")
            await query.answer("❌ Error refreshing list")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"❌ Error refreshing deleted list: {str(e)}"
            )
    
    elif query.data == "view_revoked_list":
        # Handle view revoked list button from revoke confirmation
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        try:
            # Create fake update to call listremoved_cmd
            class FakeMessage:
                def __init__(self, chat_id, from_user):
                    self.chat_id = chat_id
                    self.from_user = from_user
                    
                async def reply_text(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, **kwargs)
                    
                async def reply_html(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, parse_mode=ParseMode.HTML, **kwargs)
            
            class FakeUpdate:
                def __init__(self, user, chat_id):
                    self.effective_user = user
                    self.message = FakeMessage(chat_id, user)
            
            class FakeContext:
                def __init__(self):
                    self.args = []
                    self.bot = context.bot
            
            fake_update = FakeUpdate(query.from_user, query.message.chat_id)
            fake_context = FakeContext()
            
            await listremoved_cmd(fake_update, fake_context)
            await query.answer("📋 Showing all removed media!")
            
        except Exception as e:
            print(f"ERROR in view_revoked_list callback: {e}")
            await query.answer("❌ Error loading list")
    
    elif query.data.startswith("deleted_page_"):
        # Handle deleted media pagination
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        if query.data == "deleted_page_info":
            await query.answer("📄 Page navigation", show_alert=False)
            return
        
        try:
            page = int(query.data.replace("deleted_page_", ""))
            
            # Create fake update to call listdeleted_command with page
            class FakeMessage:
                def __init__(self, chat_id, from_user):
                    self.chat_id = chat_id
                    self.from_user = from_user
                    
                async def reply_text(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, **kwargs)
                    
                async def reply_html(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, parse_mode=ParseMode.HTML, **kwargs)
            
            class FakeUpdate:
                def __init__(self, user, chat_id):
                    self.effective_user = user
                    self.message = FakeMessage(chat_id, user)
            
            fake_update = FakeUpdate(query.from_user, query.message.chat_id)
            
            # Delete current message and show new page
            try:
                await query.message.delete()
            except Exception as e:
                print(f"DEBUG: Could not delete message: {e}")
            
            await listdeleted_command(fake_update, context, page)
            await query.answer(f"📄 Page {page + 1}")
            
        except ValueError:
            await query.answer("❌ Invalid page number")
    
    elif query.data == "cleanup_deleted":
        # Handle cleanup button - PERMANENTLY DELETE ALL deleted media (same as /cleardeleted)
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        if not deleted_media_storage:
            await query.answer("✅ No deleted media to clean up", show_alert=False)
            return
        
        # Immediately permanently delete all deleted media (no confirmation for button)
        deleted_count = len(deleted_media_storage)
        deleted_media_storage.clear()
        save_deleted_media()
        
        await query.answer(f"🗑️ Permanently deleted {deleted_count} items from database!", show_alert=True)
        
        # Update the message to show the list is now empty
        await query.edit_message_text(
            "🗑️ <b>Deleted Media (Page 1/1)</b>\n\n"
            "📋 No deleted media found - database is clean!\n\n"
            "All deleted media has been permanently removed.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_deleted_list")]
            ])
        )
    
    elif query.data == "refresh_deleted_list":
        # Handle refresh button
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        # Same as list_deleted_media handler but for refresh button
        try:
            class FakeMessage:
                def __init__(self, chat_id, from_user):
                    self.chat_id = chat_id
                    self.from_user = from_user
                    
                async def reply_text(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, **kwargs)
                    
                async def reply_html(self, text, **kwargs):
                    await context.bot.send_message(chat_id=self.chat_id, text=text, parse_mode=ParseMode.HTML, **kwargs)
            
            class FakeUpdate:
                def __init__(self, user, chat_id):
                    self.effective_user = user
                    self.message = FakeMessage(chat_id, user)
            
            fake_update = FakeUpdate(query.from_user, query.message.chat_id)
            
            try:
                await query.message.delete()
            except Exception as e:
                print(f"DEBUG: Could not delete message: {e}")
            
            await listdeleted_command(fake_update, context, 0)  # Always go to first page on refresh
            await query.answer("🔄 List refreshed!")
            
        except Exception as e:
            print(f"ERROR in refresh_deleted_list callback: {e}")
            await query.answer("❌ Error refreshing list")

    elif query.data.startswith("revoked_page_"):
        # Handle revoked media pagination
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        if query.data == "revoked_page_info":
            await query.answer("📄 Page navigation", show_alert=False)
            return
        
        try:
            page = int(query.data.replace("revoked_page_", ""))
            
            # Generate updated content for the requested page
            text, reply_markup = generate_revoked_list(page)
            
            if reply_markup is None:
                # Empty page or no revoked media
                await query.edit_message_text(text)
            else:
                # Update the message with new page content
                await query.edit_message_text(
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
            
            await query.answer(f"📄 Page {page}")
            
        except ValueError:
            await query.answer("❌ Invalid page number")
        except Exception as e:
            print(f"ERROR in revoked_page callback: {e}")
            await query.answer("❌ Error loading page")
    
    elif query.data == "cleanup_revoked":
        # Handle cleanup button - Remove all revoked flags
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        # Count and remove all revoked flags
        revoked_count = 0
        for tag, vids in media_data.items():
            if not isinstance(vids, list):
                continue
            for v in vids:
                if isinstance(v, dict) and v.get("revoked"):
                    v.pop("revoked", None)
                    revoked_count += 1
        
        if revoked_count == 0:
            await query.answer("✅ No revoked media to clean up", show_alert=False)
            return
        
        save_media()
        update_random_state()
        
        await query.answer(f"♻️ Restored {revoked_count} revoked items!", show_alert=True)
        
        # Update the message to show the list is now empty
        await query.edit_message_text(
            "🛑 <b>Revoked Media (Page 1/1)</b>\n\n"
            "📋 No revoked media found - all have been restored!\n\n"
            "All revoked media has been restored to active status.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_revoked_list")]
            ])
        )
    
    elif query.data == "refresh_revoked_list":
        # Handle refresh button for revoked media
        if not is_admin(query.from_user.id):
            await query.answer("❌ Not allowed")
            return
        
        try:
            # Generate updated content - always show page 1 on refresh
            text, reply_markup = generate_revoked_list(1)
            
            if reply_markup is None:
                # No revoked media found
                await query.edit_message_text(text)
            else:
                # Update the message with new content
                await query.edit_message_text(
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
            
            await query.answer("🔄 List refreshed!")
            
        except Exception as e:
            print(f"ERROR in refresh_revoked_list callback: {e}")
            await query.answer("❌ Error refreshing list")


# Add a dictionary to track processing tasks for media groups
media_group_tasks = {}


async def process_media_group(media_group_id, delay=3):
    """Process a media group after a delay to ensure all files are collected"""
    await asyncio.sleep(delay)

    if media_group_id in media_tag_map and media_group_id in media_buffer:
        tag = media_tag_map.pop(media_group_id)
        items = sorted(media_buffer.pop(media_group_id),
                       key=lambda x: x["msg_id"])

        media_data.setdefault(tag, []).extend([{
            "file_id": x["file_id"],
            "type": x["type"]
        } for x in items])
        save_media()

        # Send confirmation (we'll need to get the last message context)
        print(f"✅ Album of {len(items)} files uploaded under tag '{tag}'.")

    # Clean up the task reference
    if media_group_id in media_group_tasks:
        del media_group_tasks[media_group_id]


async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        # Check if regular user has active custom batch session
        if await handle_custom_batch_media(update, context):
            return
        return

    # Auto-register user (this handles all user tracking)
    await auto_register_user(update, context)

    # Check if admin has active custom batch session
    if await handle_custom_batch_media(update, context):
        return

    message = update.message
    if not message:
        return

    media_group_id = message.media_group_id
    caption = message.caption
    # Support multiple media types
    photo = message.photo[-1] if message.photo else None
    video = message.video
    document = message.document
    audio = message.audio
    voice = message.voice
    animation = getattr(message, 'animation', None)
    sticker = message.sticker
    file_id = None
    media_type = None
    if photo:
        file_id = photo.file_id; media_type = "photo"
    elif video:
        file_id = video.file_id; media_type = "video"
    elif document:
        file_id = document.file_id; media_type = "document"
    elif audio:
        file_id = audio.file_id; media_type = "audio"
    elif voice:
        file_id = voice.file_id; media_type = "voice"
    elif animation:
        file_id = animation.file_id; media_type = "animation"
    elif sticker:
        file_id = sticker.file_id; media_type = "sticker"

    if not file_id:
        return

    if media_group_id:
        # Store tag if this message has the upload command
        if caption and caption.lower().startswith("/upload "):
            tag = caption.split(" ", 1)[1].strip().lower()
            media_tag_map[media_group_id] = tag

        # Add file to buffer
        media_buffer[media_group_id].append({"file_id": file_id, "type": media_type, "msg_id": message.message_id})

        # Cancel existing task if any and create a new one
        if media_group_id in media_group_tasks:
            media_group_tasks[media_group_id].cancel()

        # Create new processing task with delay
        media_group_tasks[media_group_id] = asyncio.create_task(process_media_group(media_group_id))

        # Send confirmation for the last processed file
        if media_group_id in media_tag_map:
            tag = media_tag_map[media_group_id]
            current_count = len(media_buffer[media_group_id])
            await message.reply_text(f"📁 Collecting files for tag '{tag}' ({current_count} files so far)...")
    else:
        # Single file upload
        if caption and caption.lower().startswith("/upload "):
            tag = caption.split(" ", 1)[1].strip().lower()
            media_data.setdefault(tag, []).append({"file_id": file_id, "type": media_type})
            save_media()
            await message.reply_text(f"✅ {media_type.capitalize()} uploaded under tag '{tag}'.")



















async def send_single_passlink_video(update: Update, context: ContextTypes.DEFAULT_TYPE, item: dict, tag: str, idx: int, actual_indices: list, start_index: int):
    """Send a single video from a passlink - used for concurrent sending"""
    user_id = update.effective_user.id
    
    # Use both global rate limiting and per-user fair allocation
    # IMPORTANT: Acquire per-user semaphore FIRST. Scope the global semaphore ONLY
    # around the actual Telegram send to avoid holding global capacity during
    # per-user delays and pre/post processing.
    async with get_user_semaphore(user_id):  # Per-user fairness
        try:
            # Use actual index from stored indices, or fallback to calculated
            actual_index = actual_indices[idx] if idx < len(actual_indices) else start_index + idx

            # Create unique shareable link for this specific file
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{actual_index}_{actual_index}"
            share_link = f"https://telegram.me/share/url?url={file_link}"

            # Build proper media caption with global caption and replacements
            media_type = item.get("type", "video")
            cap = build_media_caption("", tag, str(actual_index), share_link, media_type)

            # Check if media is revoked (show for admins only)
            if isinstance(item, dict) and item.get("revoked"):
                if is_admin(update.effective_user.id):
                    cap += "\n\n🛑 <b>This media is revoked</b>"
                else:
                    return False  # Skip for regular users

            # Handle corrupted media for admins
            if not isinstance(item, dict) or "type" not in item or "file_id" not in item:
                if is_admin(update.effective_user.id):
                    cap += "\n\n⚠️ <b>This media has corrupted data</b>"

                    # Create restore button for corrupted media
                    restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{tag}_{actual_index}")
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("❤️ ADD", callback_data=f"add_fav_{tag}_{actual_index}")],
                        [InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites"), 
                         InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")],
                        [restore_button]
                    ])

                    # Try to send as media if file_id exists
                    if isinstance(item, dict) and "file_id" in item:
                        try:
                            async with video_send_semaphore:  # Global rate limiting
                                message = await send_and_track_message(
                                    update.message.reply_video,
                                    item["file_id"],
                                    caption=cap,
                                    parse_mode=ParseMode.HTML,
                                    protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                    reply_markup=keyboard
                                )
                        except Exception:
                            async with video_send_semaphore:  # Global rate limiting
                                message = await send_and_track_message(
                                    update.message.reply_photo,
                                    item["file_id"],
                                    caption=cap,
                                    parse_mode=ParseMode.HTML,
                                    protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                    reply_markup=keyboard
                                )
                    else:
                        await update.message.reply_text(cap, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                return True

            # Create favorite button
            video_id = f"{tag}_{actual_index}"
            user_id_str = str(update.effective_user.id)
            is_favorited = user_id_str in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id_str]

            if is_favorited:
                fav_button = InlineKeyboardButton("💔 Remove from Favorites", 
                                                callback_data=f"remove_fav_{video_id}")
            else:
                fav_button = InlineKeyboardButton("❤️ ADD", 
                                                callback_data=f"add_fav_{video_id}")

            # Check if user is admin
            if is_admin(update.effective_user.id):
                who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_id}")
                my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                rows = [
                    [fav_button, who_liked_button],
                    [my_favs_button, random_button]
                ]
                rows += build_admin_control_row(video_id)
                keyboard = InlineKeyboardMarkup(rows)
            else:
                my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")

                keyboard = InlineKeyboardMarkup([
                    [fav_button],
                    [my_favs_button, random_button]
                ])

            # Send the media (scope global semaphore ONLY around the send)
            if item["type"] == "video":
                async with video_send_semaphore:  # Global rate limiting
                    await safe_send_message(
                        context=context,
                        chat_id=update.message.chat_id,
                        video=item["file_id"],
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=should_protect_content(update.effective_user.id, update.message.chat_id)
                    )
            elif item["type"] == "photo":
                async with video_send_semaphore:  # Global rate limiting
                    await safe_send_message(
                        context=context,
                        chat_id=update.message.chat_id,
                        photo=item["file_id"],
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=should_protect_content(update.effective_user.id, update.message.chat_id)
                    )
            else:
                # Use helper function to send media by type
                async with video_send_semaphore:  # Global rate limiting
                    await send_media_by_type(
                        context=context,
                        chat_id=update.message.chat_id,
                        item=item,
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=should_protect_content(update.effective_user.id, update.message.chat_id)
                    )

            # Removed artificial per-item delay; rely on safe_send_message backoff and global semaphore
            return True

        except Exception as e:
            print(f"Error sending video {idx}: {e}")
            return False


async def send_passlink_videos(update: Update, context: ContextTypes.DEFAULT_TYPE, param: str, link_data: dict):
    """Send videos from a passlink"""
    user_id = update.effective_user.id
    
    # Extract link information
    tag = link_data.get("tag", "")
    start_index = link_data.get("start_index", 0)
    end_index = link_data.get("end_index", 0)
    videos = link_data.get("videos", [])
    
    if not videos:
        await update.message.reply_text("❌ No videos found in this link")
        return
        
    await safe_send_message(
        context=context,
        chat_id=update.message.chat_id,
        text=f"📁 Showing videos from tag '<code>{tag}</code>' (indexes {start_index}-{end_index}, {len(videos)} files):",
        parse_mode=ParseMode.HTML
    )
    
    # Send videos concurrently to avoid blocking other users
    actual_indices = link_data.get("actual_indices", list(range(start_index, end_index + 1)))
    
    # Create tasks for sending all videos concurrently
    tasks = []
    for idx, item in enumerate(videos):
        # Skip corrupted/deleted media for regular users
        if not isinstance(item, dict) or "type" not in item or "file_id" not in item:
            # Only show to admins, skip for regular users
            if not is_admin(update.effective_user.id):
                continue
        
        # Skip revoked media for regular users
        if isinstance(item, dict) and item.get("revoked") and not is_admin(update.effective_user.id):
            continue
                
        # Create task for sending this video
        task = asyncio.create_task(
            send_single_passlink_video(update, context, item, tag, idx, actual_indices, start_index)
        )
        tasks.append(task)
    
    # Wait for all videos to be sent concurrently
    sent_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Count successful sends
    sent_count = sum(1 for result in sent_results if result is True)
    total_files = len(videos)
    
    # Send completion message
    if sent_count > 0:
        # Create inline keyboard with MY FAVOURITE button
        completion_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")]
        ])
        
        await safe_send_message(
            context=context,
            chat_id=update.message.chat_id,
            text=(
                f"✅ Successfully sent {sent_count} out of {total_files} videos from tag '{tag}'\n\n"
                f"⏰ <b>Auto-Delete Notice:</b> All media will be automatically deleted after {AUTO_DELETE_HOURS} hour(s) for storage optimization.\n\n"
                f"💡 <b>Save to Favorites:</b> Use the ❤️ ADD button on any video to add it to your favorites for permanent access!"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=completion_keyboard,
            auto_delete=False
        )
    else:
        await safe_send_message(
            context=context,
            chat_id=update.message.chat_id,
            text=f"❌ No videos could be sent from tag '{tag}'",
            auto_delete=False
        )


async def send_single_media_item(query_or_update, context, tag: str, idx: int, is_callback: bool = False):
    """Send a single media item - used for parallel sending"""
    try:
        # Determine if this is from callback query or regular update
        if is_callback:
            user_id = query_or_update.from_user.id
            chat_id = query_or_update.message.chat_id
            reply_func = query_or_update.message.reply_video if hasattr(query_or_update.message, 'reply_video') else None
        else:
            user_id = query_or_update.effective_user.id
            chat_id = query_or_update.message.chat_id
            reply_func = query_or_update.message.reply_video if hasattr(query_or_update.message, 'reply_video') else None

        # Use per-user semaphore for fairness, global semaphore only around actual send
        async with get_user_semaphore(user_id):
            # Check if index is out of range (media was deleted)
            if idx >= len(media_data[tag]):
                # For admins, show deleted media with restore option if it exists
                if is_admin(user_id):
                    video_key = f"{tag}_{idx}"
                    if video_key in deleted_media_storage:
                        deleted_info = deleted_media_storage[video_key]
                        deleted_item = deleted_info["data"]
                        
                        # Create unique shareable link for this specific file
                        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                        share_link = f"https://telegram.me/share/url?url={file_link}"
                        
                        # Build proper media caption with global caption and replacements
                        media_type = deleted_item.get("type", "video")
                        cap = build_media_caption("", tag, str(idx), share_link, media_type)
                        cap += "\n\n🗑️ <b>This media was deleted</b>"
                        
                        # Create favorite button
                        user_id_str = str(user_id)
                        is_favorited = user_id_str in favorites_data["user_favorites"] and video_key in favorites_data["user_favorites"].get(user_id_str, [])

                        if is_favorited:
                            fav_button = InlineKeyboardButton("💔 Remove", 
                                                            callback_data=f"remove_fav_{video_key}")
                        else:
                            fav_button = InlineKeyboardButton("❤️ Add", 
                                                            callback_data=f"add_fav_{video_key}")

                        # Admin buttons for deleted media
                        who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_key}")
                        my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                        random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                        restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")
                        
                        keyboard = InlineKeyboardMarkup([
                            [fav_button, who_liked_button],
                            [my_favs_button, random_button],
                            [restore_button]
                        ])
                        
                        media_kwargs = {
                            "context": context,
                            "chat_id": chat_id,
                            "caption": cap,
                            "reply_markup": keyboard,
                            "parse_mode": ParseMode.HTML,
                            "protect_content": should_protect_content(user_id, chat_id)
                        }
                        
                        if deleted_item["type"] == "video":
                            media_kwargs["video"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "photo":
                            media_kwargs["photo"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "document":
                            media_kwargs["document"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "audio":
                            media_kwargs["audio"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "voice":
                            media_kwargs["voice"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "animation":
                            media_kwargs["animation"] = deleted_item["file_id"]
                        elif deleted_item["type"] == "sticker":
                            media_kwargs["sticker"] = deleted_item["file_id"]
                            # Stickers don't support captions
                            media_kwargs.pop("caption", None)
                        else:
                            # Fallback for unknown types
                            media_kwargs["text"] = f"Deleted media type: {deleted_item['type']}"
                        
                        async with video_send_semaphore:  # Global rate limiting
                            await safe_send_message(**media_kwargs)
                        return True
                # For regular users, silently skip without any notification
                return False
                
            item = media_data[tag][idx]
            
            # Skip if item is not a valid dictionary or missing required fields
            if not isinstance(item, dict) or "type" not in item or "file_id" not in item:
                # For admins, show corrupted media as normal media with fix option
                if is_admin(user_id):
                    # Create unique shareable link for this specific file
                    file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                    share_link = f"https://telegram.me/share/url?url={file_link}"
                    
                    # Build normal media caption but indicate it's corrupted
                    cap = build_media_caption("", tag, str(idx), share_link, "video")
                    cap += "\n\n⚠️ <b>This media has corrupted data</b>"
                    
                    # Create normal favorite button for admin view
                    video_id = f"{tag}_{idx}"
                    user_id_str = str(user_id)
                    is_favorited = user_id_str in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id_str]
                    
                    if is_favorited:
                        fav_button = InlineKeyboardButton("💔 Remove", 
                                                        callback_data=f"remove_fav_{video_id}")
                    else:
                        fav_button = InlineKeyboardButton("❤️ Add", 
                                                        callback_data=f"add_fav_{video_id}")
                    
                    # Add admin-specific button to see who liked this video
                    who_liked_button = InlineKeyboardButton("👥 WHO", 
                                                          callback_data=f"who_liked_{video_id}")
                    # Add persistent bookmark and random pick buttons at bottom
                    my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                    random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                    # Restore button for corrupted/deleted media
                    restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{tag}_{idx}")
                    
                    keyboard = InlineKeyboardMarkup([
                        [fav_button, who_liked_button],
                        [my_favs_button, random_button],
                        [restore_button]
                    ])
                    
                    # Try to send as actual media if we have file_id
                    if isinstance(item, dict) and "file_id" in item:
                        # We have file_id but missing type, try video first
                        try:
                            async with video_send_semaphore:  # Global rate limiting
                                await safe_send_message(
                                    context=context,
                                    chat_id=chat_id,
                                    video=item["file_id"],
                                    caption=cap,
                                    reply_markup=keyboard,
                                    parse_mode=ParseMode.HTML,
                                    protect_content=should_protect_content(user_id, chat_id)
                                )
                        except:
                            # If video fails, try photo
                            async with video_send_semaphore:  # Global rate limiting
                                await safe_send_message(
                                    context=context,
                                    chat_id=chat_id,
                                    photo=item["file_id"],
                                    caption=cap,
                                    reply_markup=keyboard,
                                    parse_mode=ParseMode.HTML,
                                    protect_content=should_protect_content(user_id, chat_id)
                                )
                    else:
                        # No file_id available, send as text
                        async with video_send_semaphore:  # Global rate limiting
                            await safe_send_message(
                                context=context,
                                chat_id=chat_id,
                                text=cap,
                                reply_markup=keyboard,
                                parse_mode=ParseMode.HTML
                            )
                    return True
                # For regular users, silently skip without any notification
                return False
                
            # Create unique shareable link for this specific file
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            share_link = f"https://telegram.me/share/url?url={file_link}"
            
            # Build proper media caption with global caption and replacements
            media_type = item.get("type", "video")
            cap = build_media_caption("", tag, str(idx), share_link, media_type)
            
            # Create favorite button
            video_id = f"{tag}_{idx}"
            user_id_str = str(user_id)
            is_favorited = user_id_str in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id_str]
            
            if is_favorited:
                fav_button = InlineKeyboardButton("💔 Remove", 
                                                callback_data=f"remove_fav_{video_id}")
            else:
                fav_button = InlineKeyboardButton("❤️ Add", 
                                                callback_data=f"add_fav_{video_id}")
            
            # Check if user is admin to show additional admin buttons
            if is_admin(user_id):
                who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_id}")
                my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                rows = [
                    [fav_button, who_liked_button],
                    [my_favs_button, random_button]
                ]
                rows += build_admin_control_row(video_id)
                keyboard = InlineKeyboardMarkup(rows)
            else:
                # Regular user buttons
                my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                
                keyboard = InlineKeyboardMarkup([
                    [fav_button],
                    [my_favs_button, random_button]
                ])
            
            # Send the media with global semaphore only around actual send
            if item["type"] == "video":
                async with video_send_semaphore:  # Global rate limiting
                    await safe_send_message(
                        context=context,
                        chat_id=chat_id,
                        video=item["file_id"],
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=should_protect_content(user_id, chat_id)
                    )
            else:
                # Use helper function to send media by type
                async with video_send_semaphore:  # Global rate limiting
                    await send_media_by_type(
                        context=context,
                        chat_id=chat_id,
                        item=item,
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=should_protect_content(user_id, chat_id)
                    )
            return True

    except Exception as e:
        # Show error to admins, silently skip for regular users
        if is_admin(user_id):
            print(f"Error sending media {idx}: {e}")
        else:
            print(f"Silently skipping media {idx} for regular user: {e}")
        return False


async def send_tagged_from_callback_with_range(
        query,
        context: ContextTypes.DEFAULT_TYPE,
        tag: str,
        start_index=None,
        end_index=None):
    user_id = query.from_user.id
    bot = context.bot

    user = query.from_user
    mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'

    # Check if user is exempted from channel requirements
    if user_id in exempted_users:
        print(f"User {user_id} is exempted from channel requirements")
        membership_failed = False
    else:
        # Only verify channel membership for multi-file links (skip for single file)
        is_single_file = start_index is not None and end_index is not None and start_index == end_index
        
        if is_single_file:
            # Skip verification for single-file links
            membership_failed = False
            print(f"User {user_id} accessing single file - skipping channel verification")
        else:
            # Check channel membership for multi-file links
            membership_failed = False
            for ch in REQUIRED_CHANNELS:
                is_member = await is_user_member(user_id, bot, ch)
                print(f"User {user_id} membership in {ch}: {is_member}")  # Debug log
                if not is_member:
                    membership_failed = True
                    break

    if membership_failed:
        clear_user_operation(user_id)
        # For ranged links, we need to preserve the range in retry callback
        retry_param = f"{tag}_{start_index}_{end_index}" if start_index is not None and end_index is not None else tag
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("JOIN✨",
                                     url="https://t.me/CypherHere"),
                InlineKeyboardButton("BACKUP CHANNEL🛡️",
                                     url="https://t.me/OmenThere")
            ],
             [
                 InlineKeyboardButton("🔄 Try Again",
                                      callback_data=f"retry_{retry_param}")
             ]])
        
        # Send notification with image
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=CHANNEL_NOTIFY_IMAGE_ID,
            caption=f"Hey {mention}!!\n"
                   "Welcome to <b>Meow Gang</b> 🕊️\n\n"
                   "<b>⚠️ CHANNEL MEMBERSHIP REQUIRED</b>\n\n"
                   "To access multiple videos, you must join our channels first!\n"
                   "Single video links don't require membership.\n\n"
                   "<i>Once you've joined, click 'Try Again' to access the videos.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return

    if tag not in media_data:
        clear_user_operation(user_id)
        await query.message.reply_text("❌ No media found under this tag.")
        return

    # Determine range
    if start_index is None:
        start_index = 0
    if end_index is None:
        end_index = len(media_data[tag]) - 1

    # Validate range
    if start_index < 0 or start_index >= len(media_data[tag]):
        clear_user_operation(user_id)
        await query.message.reply_text(
            f"❌ Start index out of range. Available indexes: 0-{len(media_data[tag])-1}"
        )
        return

    if end_index < start_index or end_index >= len(media_data[tag]):
        clear_user_operation(user_id)
        await query.message.reply_text(
            f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(media_data[tag])-1}"
        )
        return

    # Send media files in the specified range with content protection
    total_files = end_index - start_index + 1
    sent_count = 0
    
    # Send videos without starting notification
    
    # Create concurrent tasks for all media items to send them in parallel
    media_tasks = []
    for idx in range(start_index, end_index + 1):
        # Create task for each media item
        task = asyncio.create_task(
            send_single_media_item(query, context, tag, idx, is_callback=True)
        )
        media_tasks.append(task)
    
    # Wait for all media to be sent concurrently
    if media_tasks:
        results = await asyncio.gather(*media_tasks, return_exceptions=True)
        sent_count = sum(1 for result in results if result is True)
    
    # Send completion message with favorite reminder
    if sent_count > 0:
        completion_message = await safe_send_message(
            context=context,
            chat_id=query.message.chat_id,
            text=f"✅ <b>Media Delivery Complete!</b>\n\n"
                 f"📊 Sent {sent_count}/{total_files} files\n"
                 f"Don't forget to save your favorites!\n\n"
                 f"Use ❤️ ADD to bookmark this resource for later.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")]
            ])
        )
    
    # Clear operation when complete
    clear_user_operation(user_id)



async def send_tagged_with_range(update: Update, context: ContextTypes.DEFAULT_TYPE, tag: str, start_index=None, end_index=None):
    """Send tagged media with range for direct updates (not callback queries)"""
    user_id = update.effective_user.id
    bot = context.bot

    user = update.effective_user
    mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'

    # Check if user is exempted from channel requirements
    if user_id in exempted_users:
        print(f"User {user_id} is exempted from channel requirements")
        membership_failed = False
    else:
        # Only verify channel membership for multi-file links (skip for single file)
        is_single_file = start_index is not None and end_index is not None and start_index == end_index
        
        if is_single_file:
            # Skip verification for single-file links
            membership_failed = False
            print(f"User {user_id} accessing single file - skipping channel verification")
        else:
            # Check channel membership for multi-file links
            membership_failed = False
            for ch in REQUIRED_CHANNELS:
                is_member = await is_user_member(user_id, bot, ch)
                print(f"User {user_id} membership in {ch}: {is_member}")
                if not is_member:
                    membership_failed = True
                    break

    if membership_failed:
        clear_user_operation(user_id)
        # For ranged links, we need to preserve the range in retry callback
        retry_param = f"{tag}_{start_index}_{end_index}" if start_index is not None and end_index is not None else tag
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("JOIN✨",
                                     url="https://t.me/CypherHere"),
                InlineKeyboardButton("BACKUP CHANNEL🛡️",
                                     url="https://t.me/OmenThere")
            ],
             [
                 InlineKeyboardButton("🔄 Try Again",
                                      callback_data=f"retry_{retry_param}")
             ]])
        
        # Send notification with image
        await context.bot.send_photo(
            chat_id=update.effective_chat.id,
            photo=CHANNEL_NOTIFY_IMAGE_ID,
            caption=f"Hey {mention}!!\n"
                   "Welcome to <b>Meow Gang</b> 🕊️\n\n"
                   "<b>⚠️ CHANNEL MEMBERSHIP REQUIRED</b>\n\n"
                   "To access multiple videos, you must join our channels first!\n"
                   "Single video links don't require membership.\n\n"
                   "<i>Once you've joined, click 'Try Again' to access the videos.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return

    if tag not in media_data:
        clear_user_operation(user_id)
        await update.message.reply_text("❌ No media found under this tag.")
        return

    # Determine range
    if start_index is None:
        start_index = 0
    if end_index is None:
        end_index = len(media_data[tag]) - 1

    # Validate range
    if start_index < 0 or start_index >= len(media_data[tag]):
        clear_user_operation(user_id)
        await update.message.reply_text(
            f"❌ Start index out of range. Available indexes: 0-{len(media_data[tag])-1}"
        )
        return

    if end_index < start_index or end_index >= len(media_data[tag]):
        clear_user_operation(user_id)
        await update.message.reply_text(
            f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(media_data[tag])-1}"
        )
        return

    # Send media files in the specified range with content protection
    total_files = end_index - start_index + 1
    sent_count = 0
    
    await update.message.reply_text(
        f"📁 Showing videos from tag '<code>{tag}</code>' (indexes {start_index}-{end_index}, {total_files} files):",
        parse_mode=ParseMode.HTML
    )
    
    for idx in range(start_index, end_index + 1):
        # Check if index is out of range (media was deleted)
        if idx >= len(media_data[tag]):
            # For admins, show deleted media with restore option if it exists
            if is_admin(update.effective_user.id):
                video_key = f"{tag}_{idx}"
                if video_key in deleted_media_storage:
                    deleted_info = deleted_media_storage[video_key]
                    original_data = deleted_info["data"]
                    
                    # Create unique shareable link for this specific file
                    file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                    share_link = f"https://telegram.me/share/url?url={file_link}"
                    
                    # Build normal media caption but indicate it's corrupted
                    cap = build_media_caption("", tag, str(idx), share_link, "video")
                    cap += "\n\n⚠️ <b>This media was deleted</b>"
                    
                    # Create normal favorite button
                    user_id_str = str(update.effective_user.id)
                    is_favorited = user_id_str in favorites_data["user_favorites"] and video_key in favorites_data["user_favorites"][user_id_str]
                    
                    if is_favorited:
                        fav_button = InlineKeyboardButton("💔 Remove from Bookmarks", 
                                                        callback_data=f"remove_fav_{video_key}")
                    else:
                        fav_button = InlineKeyboardButton("❤️ ADD", 
                                                        callback_data=f"add_fav_{video_key}")
                    
                    # Create normal buttons like any other media plus restore button
                    my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                    random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                    restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{tag}_{idx}")
                    
                    keyboard = InlineKeyboardMarkup([
                        [fav_button],
                        [my_favs_button, random_button],
                        [restore_button]
                    ])
                    
                    await update.message.reply_text(
                        cap,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
            continue
            
        item = media_data[tag][idx]
        
        # Skip if item is not a valid dictionary or missing required fields
        if not isinstance(item, dict) or "type" not in item or "file_id" not in item:
            # For admins, show corrupted media as normal media with restore option
            if is_admin(update.effective_user.id):
                # Create unique shareable link for this specific file
                file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                share_link = f"https://telegram.me/share/url?url={file_link}"
                
                # Build normal media caption but indicate it's corrupted
                cap = build_media_caption("", tag, str(idx), share_link, "video")
                cap += "\n\n⚠️ <b>This media has corrupted data</b>"
                
                # Create normal favorite button
                video_id = f"{tag}_{idx}"
                user_id_str = str(update.effective_user.id)
                is_favorited = user_id_str in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id_str]
                
                if is_favorited:
                    fav_button = InlineKeyboardButton("💔 Remove from Favorites", 
                                                    callback_data=f"remove_fav_{video_id}")
                else:
                    fav_button = InlineKeyboardButton("❤️ ADD", 
                                                    callback_data=f"add_fav_{video_id}")
                
                # Create normal buttons like any other media plus restore button
                my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{tag}_{idx}")
                
                keyboard = InlineKeyboardMarkup([
                    [fav_button],
                    [my_favs_button, random_button],
                    [restore_button]
                ])
                
                # Try to send as actual media if we have file_id
                try:
                    if isinstance(item, dict) and "file_id" in item:
                        # We have file_id but missing type, try video first
                        try:
                            await update.message.reply_video(item["file_id"],
                                                           caption=cap,
                                                           parse_mode=ParseMode.HTML,
                                                           protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                           reply_markup=keyboard)
                        except:
                            # If video fails, try photo
                            await update.message.reply_photo(item["file_id"],
                                                           caption=cap,
                                                           parse_mode=ParseMode.HTML,
                                                           protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                           reply_markup=keyboard)
                    else:
                        # No file_id available, send as text
                        await update.message.reply_text(
                            cap,
                            parse_mode=ParseMode.HTML,
                            reply_markup=keyboard
                        )
                except Exception as e:
                    print(f"Error displaying corrupted media {idx}: {e}")
                    # Fallback to text message
                    await update.message.reply_text(
                        cap,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
            # For regular users, silently skip without any notification
            continue
        
        # Check if media is revoked (only admins can see revoked media)
        if isinstance(item, dict) and item.get("revoked"):
            if is_admin(update.effective_user.id):
                # For admins, show revoked media with restore option
                file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                share_link = f"https://telegram.me/share/url?url={file_link}"
                
                # Get original data from revoked wrapper
                original_data = item.get("data", item)
                media_type = original_data.get("type", "video")
                cap = build_media_caption("", tag, str(idx), share_link, media_type)
                cap += "\n\n🛑 <b>This media is revoked</b>"
                
                video_id = f"{tag}_{idx}"
                user_id_str = str(update.effective_user.id)
                is_favorited = user_id_str in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id_str]
                
                if is_favorited:
                    fav_button = InlineKeyboardButton("💔 Remove from Favorites", 
                                                    callback_data=f"remove_fav_{video_id}")
                else:
                    fav_button = InlineKeyboardButton("❤️ ADD", 
                                                    callback_data=f"add_fav_{video_id}")
                
                my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{tag}_{idx}")
                
                keyboard = InlineKeyboardMarkup([
                    [fav_button],
                    [my_favs_button, random_button],
                    [restore_button]
                ])
                
                await update.message.reply_text(
                    cap,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard
                )
            # For regular users, silently skip revoked media
            continue
        
        # Create unique shareable link for this specific file
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
        
        # Build proper media caption with global caption and replacements
        media_type = item.get("type", "video")
        cap = build_media_caption("", tag, str(idx), share_link, media_type)
        
        # Create favorite button
        video_id = f"{tag}_{idx}"
        user_id_str = str(update.effective_user.id)
        is_favorited = user_id_str in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id_str]
        
        if is_favorited:
            fav_button = InlineKeyboardButton("💔 Remove from Favorites", 
                                            callback_data=f"remove_fav_{video_id}")
        else:
            fav_button = InlineKeyboardButton("❤️ ADD", 
                                            callback_data=f"add_fav_{video_id}")
        
        # Check if user is admin to show additional admin buttons
        if is_admin(update.effective_user.id):
            who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_id}")
            my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
            random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
            rows = [
                [fav_button, who_liked_button],
                [my_favs_button, random_button]
            ]
            rows += build_admin_control_row(video_id)
            keyboard = InlineKeyboardMarkup(rows)
        else:
            my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
            random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
            
            keyboard = InlineKeyboardMarkup([
                [fav_button],
                [my_favs_button, random_button]
            ])
        
        try:
            if item["type"] == "video":
                await safe_send_message(
                    context=context,
                    chat_id=update.message.chat_id,
                    video=item["file_id"],
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(update.effective_user.id, update.message.chat_id)
                )
            elif item["type"] == "photo":
                await safe_send_message(
                    context=context,
                    chat_id=update.message.chat_id,
                    photo=item["file_id"],
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(update.effective_user.id, update.message.chat_id)
                )
            else:
                # Use helper function to send media by type
                await send_media_by_type(
                    context=context,
                    chat_id=update.message.chat_id,
                    item=item,
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=should_protect_content(update.effective_user.id, update.message.chat_id)
                )
            sent_count += 1
            
        except Exception as e:
            print(f"Error sending video {idx}: {e}")
            # If individual file fails, try to continue with next file
            continue
    
    # Send completion message with favorite reminder
    if sent_count > 0:
        # Create inline keyboard with MY FAVOURITE button
        completion_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")]
        ])
        
        await update.message.reply_text(
            f"✅ Successfully sent {sent_count} out of {total_files} videos from tag '{tag}'\n\n"
            f"⏰ <b>Auto-Delete Notice:</b> All media will be automatically deleted after {AUTO_DELETE_HOURS} hour(s) for storage optimization.\n\n"
            f"💡 <b>Save to Favorites:</b> Use the ❤️ ADD button on any video to add it to your favorites for permanent access!",
            parse_mode=ParseMode.HTML,
            reply_markup=completion_keyboard
        )
    else:
        await update.message.reply_text(f"❌ No videos could be sent from tag '{tag}'")
    
    # Clear operation when complete
    clear_user_operation(user_id)


async def send_tagged(update: Update, context: ContextTypes.DEFAULT_TYPE,
                      tag: str):
    await send_tagged_with_range(update, context, tag)


async def get_tag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /get <tag>")
        return

    await send_tagged(update, context, context.args[0].strip().lower())


async def listvideo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List videos only for specific tags: wildwed, freakyfri, sizzlingsaturday, moodymonday, twistedtuesday, thirstthus, socialsunday"""
    if not is_admin(update.effective_user.id):
        return

    # Define the specific tags for /listvideo
    specific_tags = {'wildwed', 'freakyfri', 'sizzlingsaturday', 'moodymonday', 'twistedtuesday', 'thirstthus', 'socialsunday'}
    
    if not media_data:
        await update.message.reply_text("📂 No uploads yet.")
        return

    # Filter only the specific tags
    filtered_tags = {tag: files for tag, files in media_data.items() if tag in specific_tags}
    
    if not filtered_tags:
        await update.message.reply_text("📂 No videos found for the specified tags.")
        return

    msg = "<b>🗂 Special Tags (Videos):</b>\n"
    for tag, files in filtered_tags.items():
        msg += f"<code>{tag}</code> - {len(files)} files\n"
    await update.message.reply_html(msg)


async def listvideos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List videos for all tags except the specific ones handled by /listvideo"""
    if not is_admin(update.effective_user.id):
        return

    # Define the specific tags that should be excluded (handled by /listvideo)
    excluded_tags = {'wildwed', 'freakyfri', 'sizzlingsaturday', 'moodymonday', 'twistedtuesday', 'thirstthus', 'socialsunday'}
    
    if not media_data:
        await update.message.reply_text("📂 No uploads yet.")
        return

    # Filter out the specific tags
    filtered_tags = {tag: files for tag, files in media_data.items() if tag not in excluded_tags}
    
    if not filtered_tags:
        await update.message.reply_text("📂 No other videos found.")
        return

    msg = "<b>🗂 Other Tags:</b>\n"
    for tag, files in filtered_tags.items():
        msg += f"<code>{tag}</code> - {len(files)} files\n"
    await update.message.reply_html(msg)


async def remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    args = context.args
    # Check 1–3 arguments
    if len(args) < 1 or len(args) > 3:
        await update.message.reply_text("Usage: /remove <tag> [<start_index>] [<end_index>]")
        return

    tag = args[0].strip().lower()
    if tag not in media_data:
        await update.message.reply_text("❌ Tag not found.")
        return

    # Mode 1: remove entire tag
    if len(args) == 1:
        del media_data[tag]
        save_media()
        await update.message.reply_text(f"🗑 Removed all media under tag '{tag}'.")
        return

    # Mode 2: remove single file at index
    if len(args) == 2:
        try:
            index = int(args[1])
        except ValueError:
            await update.message.reply_text("❌ Invalid index. Please provide a number.")
            return
        if index < 0 or index >= len(media_data[tag]):
            await update.message.reply_text(
                f"❌ Index out of range. Available indexes: 0-{len(media_data[tag]) - 1}"
            )
            return
        # Delete one item
        media_data[tag].pop(index)
        if not media_data[tag]:
            del media_data[tag]
        save_media()
        await update.message.reply_text(f"🗑 Removed file at index {index} under tag '{tag}'.")
        return

    # Mode 3: remove range of files
    try:
        start_index = int(args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid start index. Please provide a number.")
        return
    try:
        end_index = int(args[2])
    except ValueError:
        await update.message.reply_text("❌ Invalid end index. Please provide a number.")
        return
    if start_index < 0 or start_index >= len(media_data[tag]):
        await update.message.reply_text(
            f"❌ Start index out of range. Available indexes: 0-{len(media_data[tag]) - 1}"
        )
        return
    if end_index < start_index or end_index >= len(media_data[tag]):
        await update.message.reply_text(
            f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(media_data[tag]) - 1}"
        )
        return
    # Delete the inclusive slice
    media_data[tag][start_index:end_index + 1] = []
    if not media_data[tag]:
        del media_data[tag]
    save_media()
    count = end_index - start_index + 1
    await update.message.reply_text(
        f"🗑 Removed {count} files (indexes {start_index}-{end_index}) under tag '{tag}'."
    )


async def generatelink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text(
            "Usage: /generatelink <tag> [start_index] [end_index]")
        return

    tag = context.args[0].strip().lower()
    if tag not in media_data:
        await update.message.reply_text("❌ Tag not found.")
        return

    start_index = 0
    end_index = len(media_data[tag]) - 1

    # Parse start index if provided
    if len(context.args) >= 2:
        try:
            start_index = int(context.args[1])
            if start_index < 0 or start_index >= len(media_data[tag]):
                await update.message.reply_text(
                    f"❌ Start index out of range. Available indexes: 0-{len(media_data[tag])-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid start index. Please provide a number.")
            return

    # Parse end index if provided
    if len(context.args) == 3:
        try:
            end_index = int(context.args[2])
            if end_index < start_index or end_index >= len(media_data[tag]):
                await update.message.reply_text(
                    f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(media_data[tag])-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid end index. Please provide a number.")
            return

    # Create link with range parameters
    if start_index == 0 and end_index == len(media_data[tag]) - 1:
        # Full range, use simple format
        link_param = tag
        range_text = "all files"
    else:
        # Specific range, encode as tag_start_end
        link_param = f"{tag}_{start_index}_{end_index}"
        range_text = f"files {start_index}-{end_index}"

    link = f"https://t.me/{BOT_USERNAME}?start={link_param}"
    await update.message.reply_text(
        f"🔗 Shareable link for <code>{tag}</code> ({range_text}):\n{link}",
        parse_mode=ParseMode.HTML)


async def view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) == 0:
        await update.message.reply_text("Usage: /view <tag> [start_index] [end_index]")
        return

    tag = context.args[0].strip().lower()

    if tag not in media_data:
        await update.message.reply_text("❌ Tag not found.")
        return

    # Determine the range to show
    start_index = 0
    end_index = len(media_data[tag]) - 1

    # If one index is provided, show just that single file
    if len(context.args) == 2:
        try:
            single_index = int(context.args[1])
            if single_index < 0 or single_index >= len(media_data[tag]):
                await update.message.reply_text(
                    f"❌ Index out of range. Available indexes: 0-{len(media_data[tag])-1}"
                )
                return
            start_index = single_index
            end_index = single_index
        except ValueError:
            await update.message.reply_text("❌ Invalid index. Please provide a number.")
            return

    # If two indices are provided, show the range
    elif len(context.args) == 3:
        try:
            start_index = int(context.args[1])
            end_index = int(context.args[2])
        except ValueError:
            await update.message.reply_text("❌ Invalid index range. Please provide valid numbers.")
            return

        # Validate range
        if start_index < 0 or start_index >= len(media_data[tag]):
            await update.message.reply_text(
                f"❌ Start index out of range. Available indexes: 0-{len(media_data[tag])-1}"
            )
            return

        if end_index < start_index or end_index >= len(media_data[tag]):
            await update.message.reply_text(
                f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(media_data[tag])-1}"
            )
            return

    # Show summary message
    if start_index == end_index:
        await update.message.reply_text(
            f"📁 Showing media from tag '<code>{tag}</code>' at index {start_index}:",
            parse_mode=ParseMode.HTML)
    elif start_index == 0 and end_index == len(media_data[tag]) - 1:
        await update.message.reply_text(
            f"📁 Showing all media under tag '<code>{tag}</code>' ({len(media_data[tag])} files):",
            parse_mode=ParseMode.HTML)
    else:
        range_count = end_index - start_index + 1
        await update.message.reply_text(
            f"📁 Showing media from tag '<code>{tag}</code>' (indexes {start_index}-{end_index}, {range_count} files):",
            parse_mode=ParseMode.HTML)

    # Send the media files in the specified range
    for idx in range(start_index, end_index + 1):
        # Check if index is out of range (media was deleted)
        if idx >= len(media_data[tag]):
            # For admins, show deleted media with restore option if it exists
            if is_admin(update.effective_user.id):
                video_key = f"{tag}_{idx}"
                if video_key in deleted_media_storage:
                    deleted_info = deleted_media_storage[video_key]
                    deleted_item = deleted_info["data"]
                    
                    # Create unique shareable link for this specific file
                    file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                    share_link = f"https://telegram.me/share/url?url={file_link}"
                    
                    # Build proper media caption with global caption and replacements
                    media_type = deleted_item.get("type", "video")
                    cap = build_media_caption("", tag, str(idx), share_link, media_type)
                    cap += "\n\n🗑️ <b>This media was deleted</b>"
                    
                    # Create favorite button
                    user_id_str = str(update.effective_user.id)
                    is_favorited = user_id_str in favorites_data["user_favorites"] and video_key in favorites_data["user_favorites"].get(user_id_str, [])

                    if is_favorited:
                        fav_button = InlineKeyboardButton("💔 Remove", 
                                                        callback_data=f"remove_fav_{video_key}")
                    else:
                        fav_button = InlineKeyboardButton("❤️ Add", 
                                                        callback_data=f"add_fav_{video_key}")

                    # Admin buttons for deleted media
                    who_liked_button = InlineKeyboardButton("👥 WHO", callback_data=f"who_liked_{video_key}")
                    my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                    random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                    restore_button = InlineKeyboardButton("♻️ Restore", callback_data=f"restore_media_{video_key}")
                    
                    keyboard = InlineKeyboardMarkup([
                        [fav_button, who_liked_button],
                        [my_favs_button, random_button],
                        [restore_button]
                    ])

                    try:
                        if deleted_item["type"] == "video":
                            await safe_send_message(
                                context=context,
                                chat_id=update.message.chat_id,
                                video=deleted_item["file_id"],
                                caption=cap,
                                reply_markup=keyboard,
                                parse_mode=ParseMode.HTML,
                                protect_content=should_protect_content(update.effective_user.id, update.message.chat_id)
                            )
                        else:
                            await safe_send_message(
                                context=context,
                                chat_id=update.message.chat_id,
                                item=deleted_item,
                                caption=cap,
                                reply_markup=keyboard,
                                parse_mode=ParseMode.HTML,
                                protect_content=should_protect_content(update.effective_user.id, update.message.chat_id)
                            )
                        sent_count += 1
                        
                    except Exception as e:
                        print(f"Error sending deleted media {idx}: {e}")
                        await update.message.reply_text(f"❌ Error displaying deleted file at index {idx}")
            # For regular users, silently skip without any notification
            continue
            
        item = media_data[tag][idx]
        
        # Skip if item is not a valid dictionary or missing required fields
        if not isinstance(item, dict) or "type" not in item or "file_id" not in item:
            # For admins, show corrupted media as normal media with fix option
            if is_admin(update.effective_user.id):
                # Create unique shareable link for this specific file
                file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
                share_link = f"https://telegram.me/share/url?url={file_link}"
                
                # Build normal media caption but indicate it's corrupted
                cap = build_media_caption("", tag, str(idx), share_link, "video")
                cap += "\n\n⚠️ <b>This media has corrupted data</b>"
                
                # Create normal favorite button for admin view
                video_id = f"{tag}_{idx}"
                fav_button = InlineKeyboardButton("❤️ ", 
                                                callback_data=f"add_fav_{video_id}")
                # Add admin-specific button to see who liked this video
                who_liked_button = InlineKeyboardButton("👥 WHO", 
                                                      callback_data=f"who_liked_{video_id}")
                # Add persistent bookmark and random pick buttons at bottom
                my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
                random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
                # Fix button instead of restore (since it's corrupted, not deleted)
                fix_button = InlineKeyboardButton("🔧 Fix", callback_data=f"fix_media_{tag}_{idx}")
                
                keyboard = InlineKeyboardMarkup([
                    [fav_button, who_liked_button],
                    [my_favs_button, random_button],
                    [fix_button]
                ])
                
                # Try to send as actual media if we have file_id
                try:
                    if isinstance(item, dict) and "file_id" in item:
                        # We have file_id but missing type, try video first
                        try:
                            await update.message.reply_video(item["file_id"],
                                                           caption=cap,
                                                           parse_mode=ParseMode.HTML,
                                                           protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                           reply_markup=keyboard)
                        except:
                            # If video fails, try photo
                            await update.message.reply_photo(item["file_id"],
                                                           caption=cap,
                                                           parse_mode=ParseMode.HTML,
                                                           protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                           reply_markup=keyboard)
                    else:
                        # No file_id available, send as text
                        await update.message.reply_text(
                            cap,
                            parse_mode=ParseMode.HTML,
                            reply_markup=keyboard
                        )
                except Exception as e:
                    print(f"Error displaying corrupted media {idx}: {e}")
                    # Fallback to text message
                    await update.message.reply_text(
                        cap,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
            # For regular users, silently skip without any notification
            continue
            
        # Create unique shareable link for this specific file
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
        
        # Build proper media caption with global caption and replacements
        media_type = item.get("type", "video")
        cap = build_media_caption("", tag, str(idx), share_link, media_type)

        # Create favorite button for admin view
        video_id = f"{tag}_{idx}"
        fav_button = InlineKeyboardButton("❤️ ", 
                                        callback_data=f"add_fav_{video_id}")
        # Add admin-specific button to see who liked this video
        who_liked_button = InlineKeyboardButton("👥 WHO", 
                                              callback_data=f"who_liked_{video_id}")
        # Add persistent bookmark and Random Pick buttons at bottom
        my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
        
        # Admin controls: add both Revoke and Delete actions in the same row
        admin_row = [
            InlineKeyboardButton("🛑 Revoke", callback_data=f"revoke_media_{video_id}"),
            InlineKeyboardButton("🗑️ Remove Media", callback_data=f"del_media_{video_id}")
        ] if is_admin(update.effective_user.id) else [InlineKeyboardButton(" ", callback_data="noop")]

        keyboard = InlineKeyboardMarkup([
            [fav_button, who_liked_button],
            [my_favs_button, random_button],
            admin_row
        ])

        try:
            if item["type"] == "video":
                await update.message.reply_video(item["file_id"],
                                                 caption=cap,
                                                 parse_mode=ParseMode.HTML,
                                                 protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                 reply_markup=keyboard)
            elif item["type"] == "photo":
                await update.message.reply_photo(item["file_id"],
                                                 caption=cap,
                                                 parse_mode=ParseMode.HTML,
                                                 protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                 reply_markup=keyboard)
            elif item["type"] == "animation":
                await update.message.reply_animation(item["file_id"],
                                                     caption=cap,
                                                     parse_mode=ParseMode.HTML,
                                                     protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                     reply_markup=keyboard)
            elif item["type"] == "document":
                await update.message.reply_document(item["file_id"],
                                                    caption=cap,
                                                    parse_mode=ParseMode.HTML,
                                                    protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                    reply_markup=keyboard)
            elif item["type"] == "audio":
                await update.message.reply_audio(item["file_id"],
                                                 caption=cap,
                                                 parse_mode=ParseMode.HTML,
                                                 protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                 reply_markup=keyboard)
            elif item["type"] == "voice":
                await update.message.reply_voice(item["file_id"],
                                                 caption=cap,
                                                 parse_mode=ParseMode.HTML,
                                                 protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                 reply_markup=keyboard)
            elif item["type"] == "sticker":
                await update.message.reply_sticker(item["file_id"],
                                                   protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                   reply_markup=keyboard)
                # Note: Stickers don't support captions, so we send without caption
            else:
                # Fallback for unknown types - try to send as document
                await update.message.reply_document(item["file_id"],
                                                    caption=cap,
                                                    parse_mode=ParseMode.HTML,
                                                    protect_content=should_protect_content(update.effective_user.id, update.message.chat_id),
                                                    reply_markup=keyboard)
        except Exception as e:
            # Show error to admins, silently skip for regular users
            if is_admin(update.effective_user.id):
                print(f"Error sending media {idx}: {e}")
                await update.message.reply_text(f"❌ Error sending file at index {idx}")
            else:
                print(f"Silently skipping media {idx} for regular user: {e}")
            continue

    # Reduce artificial delay; rely primarily on safe_send_message to back off on RetryAfter
    await asyncio.sleep(0.1)


async def free(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /free <user_id>")
        return

    try:
        user_id = int(context.args[0])
        if user_id in exempted_users:
            await update.message.reply_text(
                f"❌ User {user_id} is already exempted.")
            return

        exempted_users.add(user_id)
        save_exempted()
        await update.message.reply_text(
            f"✅ User {user_id} has been exempted from channel requirements.")
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid user ID. Please provide a numeric user ID.")


async def unfree(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /unfree <user_id>")
        return

    try:
        user_id = int(context.args[0])
        if user_id not in exempted_users:
            await update.message.reply_text(
                f"❌ User {user_id} is not in exempted list.")
            return

        exempted_users.remove(user_id)
        save_exempted()
        await update.message.reply_text(
            f"✅ User {user_id} has been removed from exemptions and must now join required channels.")
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid user ID. Please provide a numeric user ID.")


async def listfree(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not exempted_users:
        await update.message.reply_text("📂 No exempted users.")
        return

    msg = f"<b>🆓 Exempted Users ({len(exempted_users)}):</b>\n\n"
    
    # Send a "loading" message since getting user info might take time
    loading_msg = await update.message.reply_text("Loading user details...")
    
    user_count = 0
    for user_id in exempted_users:
        user_count += 1
        try:
            # Try to get user info
            user = await context.bot.get_chat(user_id)
            username = f"@{user.username}" if user.username else "No username"
            name = user.first_name
            if user.last_name:
                name += f" {user.last_name}"
            
            msg += f"{user_count}. <code>{user_id}</code> | <b>{name}</b> | {username}\n"
        except Exception as e:
            # If we can't get user info, just show the ID
            msg += f"{user_count}. <code>{user_id}</code> | <i>Unable to retrieve user info</i>\n"
    
    # Delete the loading message
    await loading_msg.delete()
    
    # Add note about missing user info
    if user_count > 0:
        msg += "\n<i>Note: User information might not be available if the user hasn't interacted with the bot recently.</i>"
    
    # Send the message with user details
    await update.message.reply_html(msg)


async def pass_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add resources to curated Random Pick pool (no shareable links)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text("Usage: /pass <tag> [<start_index>] [<end_index>]")
        return

    tag = context.args[0].strip().lower()
    
    # Check if tag exists
    if tag not in media_data:
        await update.message.reply_text(f"❌ Tag '{tag}' not found.")
        return
    
    # Get videos list (media_data[tag] is a list, not dict)
    tag_videos = media_data[tag]
    
    # Default to entire tag
    start_index = 0
    end_index = len(tag_videos) - 1

    # Parse start index if provided
    if len(context.args) >= 2:
        try:
            start_index = int(context.args[1])
            if start_index < 0 or start_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ Start index out of range. Available indexes: 0-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid start index. Please provide a number.")
            return

    # Parse end index if provided
    if len(context.args) == 3:
        try:
            end_index = int(context.args[2])
            if end_index < start_index or end_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid end index. Please provide a number.")
            return
    
    # Check for existing entries with the same tag and merge ranges
    existing_indices = set()
    for link in passed_links[:]:  # Create a copy to iterate over
        if link.startswith(f"{tag}_"):
            try:
                index = int(link.split("_", 1)[1])
                existing_indices.add(index)
            except (ValueError, IndexError):
                continue
    
    # Add new indices to the set
    new_indices = set(range(start_index, end_index + 1))
    all_indices = existing_indices.union(new_indices)
    
    # Remove old entries for this tag
    passed_links[:] = [link for link in passed_links if not link.startswith(f"{tag}_")]
    
    # Add merged indices back
    videos_to_pass = []
    for i in sorted(all_indices):
        if i < len(tag_videos):
            video_key = f"{tag}_{i}"
            passed_links.append(video_key)
            if i in new_indices:
                videos_to_pass.append(video_key)
    
    if videos_to_pass:
        save_passed_links()
        count = len(videos_to_pass)
        
        # Determine range text for response
        if start_index == 0 and end_index == len(tag_videos) - 1:
            range_text = f"entire tag '{tag}'"
        else:
            range_text = f"'{tag}' (indices {start_index}-{end_index})"
        
        # Show merged range info
        merged_range = f"{min(all_indices)}-{max(all_indices)}" if len(all_indices) > 1 else str(min(all_indices))
        
        await update.message.reply_text(
            f"✅ Added {count} new videos from {range_text}\n"
            f"🔄 Merged with existing entries - total range: {tag} ({merged_range})\n"
            f"🔀 {len(all_indices)} resources available via Random Pick",
            parse_mode=ParseMode.HTML
        )
    else:
        range_text = f"'{tag}' (indices {start_index}-{end_index})" if start_index != 0 or end_index != len(tag_videos) - 1 else f"entire tag '{tag}'"
        await update.message.reply_text(
            f"ℹ️ All videos in {range_text} were already passed.",
            parse_mode=ParseMode.HTML
        )


async def pass_link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add resources to Random Pick and create shareable links (dual system)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text("Usage: /passlink <tag> [start_index] [end_index]")
        return

    tag = context.args[0].strip().lower()
    
    # Check if tag exists
    if tag not in media_data:
        await update.message.reply_text(f"❌ Tag '{tag}' not found.")
        return
    
    # Get videos list (media_data[tag] is a list, not dict)
    tag_videos = media_data[tag]
    
    # Default to entire tag
    start_index = 0
    end_index = len(tag_videos) - 1

    # Parse start index if provided
    if len(context.args) >= 2:
        try:
            start_index = int(context.args[1])
            if start_index < 0 or start_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ Start index out of range. Available indexes: 0-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid start index. Please provide a number.")
            return

    # Parse end index if provided
    if len(context.args) == 3:
        try:
            end_index = int(context.args[2])
            if end_index < start_index or end_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid end index. Please provide a number.")
            return
    
    # Check for existing entries with the same tag
    existing_entry = None
    should_merge = False
    
    # Look for existing entries with the same tag
    for key, data in active_links.items():
        if data.get("tag") == tag and data.get("type") == "passlink":
            existing_start = data.get("start_index", 0)
            existing_end = data.get("end_index", 0)
            
            # Check if ranges overlap or are adjacent (should merge)
            if (start_index <= existing_end + 1 and end_index >= existing_start - 1):
                existing_entry = key
                should_merge = True
                break
    
    if should_merge:
        # Merge with existing entry
        existing_data = active_links[existing_entry]
        existing_videos = existing_data.get("videos", []).copy()
        existing_start = existing_data.get("start_index", 0)
        existing_end = existing_data.get("end_index", 0)
        existing_indices = set(range(existing_start, existing_end + 1))
        
        # Add new indices to existing set
        new_indices = set(range(start_index, end_index + 1))
        all_indices = existing_indices.union(new_indices)
        
        # Determine the new link key for the merged range
        new_start = min(all_indices)
        new_end = max(all_indices)
        if new_start == 0 and new_end == len(tag_videos) - 1:
            # Full tag - store just tag name
            link_key = tag
        else:
            # Specific range - store range key
            link_key = f"{tag}_{new_start}_{new_end}"
        
        # Remove old entry if it has a different key
        if existing_entry != link_key:
            del active_links[existing_entry]
    
        # Store the video data directly in active_links
        active_links[link_key] = {
            "type": "passlink",
            "tag": tag,
            "start_index": min(all_indices),
            "end_index": max(all_indices),
            "videos": existing_videos,
            "actual_indices": sorted(list(all_indices))  # Store actual indices for display
        }
        save_active_links()
        
        await update.message.reply_text(
            f"🔄 Updated shareable link for '{tag}' (indices {start_index}-{end_index})\n"
            f"📊 Total videos in link: {len(existing_videos)}\n"
            f"🔗 Link: {link_key}\n"
            f"ℹ️ Resources NOT added to Random Pick pool",
            parse_mode=ParseMode.HTML
        )
    else:
        # Create new entry
        all_indices = set(range(start_index, end_index + 1))
        
        # Get videos for this range only, filtering out corrupted/deleted media
        all_videos = []
        valid_indices = []
        for idx in range(start_index, end_index + 1):
            if 0 <= idx < len(tag_videos):
                item = tag_videos[idx]
                # Only include valid media items
                if isinstance(item, dict) and "type" in item and "file_id" in item:
                    all_videos.append(item)
                    valid_indices.append(idx)
        
        # Determine the link key and range for storage
        if start_index == 0 and end_index == len(tag_videos) - 1:
            # Full tag - store just tag name
            link_key = tag
            link = f"https://t.me/{BOT_USERNAME}?start={tag}"
            range_text = f"entire tag '{tag}'"
        else:
            # Specific range - store range key
            link_key = f"{tag}_{start_index}_{end_index}"
            link = f"https://t.me/{BOT_USERNAME}?start={tag}_{start_index}_{end_index}"
            range_text = f"'{tag}' (indices {start_index}-{end_index})"
    
        # Store the video data directly in active_links
        active_links[link_key] = {
            "type": "passlink",
            "tag": tag,
            "start_index": start_index,
            "end_index": end_index,
            "videos": all_videos,
            "actual_indices": valid_indices  # Store only the indices of valid videos
        }
        save_active_links()
        
        await update.message.reply_text(
            f"✅ Created shareable link for '{tag}' (indices {start_index}-{end_index})\n"
            f"📊 Total videos in link: {len(all_videos)}\n"
            f"🔗 Link: {link}\n"
            f"ℹ️ Resources NOT added to Random Pick pool",
            parse_mode=ParseMode.HTML
        )


async def revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove resources from Random Pick curated pool (was passed_links)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text("Usage: /revoke <tag> [<start_index>] [<end_index>]")
        return

    tag = context.args[0].strip().lower()
    
    # Check if tag exists
    if tag not in media_data:
        await update.message.reply_text(f"❌ Tag '{tag}' not found.")
        return
    
    # Get videos list (media_data[tag] is a list, not dict)
    tag_videos = media_data[tag]
    
    # Default to entire tag
    start_index = 0
    end_index = len(tag_videos) - 1

    # Parse start index if provided
    if len(context.args) >= 2:
        try:
            start_index = int(context.args[1])
            if start_index < 0 or start_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ Start index out of range. Available indexes: 0-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid start index. Please provide a number.")
            return

    # Parse end index if provided
    if len(context.args) == 3:
        try:
            end_index = int(context.args[2])
            if end_index < start_index or end_index >= len(tag_videos):
                await update.message.reply_text(
                    f"❌ End index out of range or less than start index. Available indexes: {start_index}-{len(tag_videos)-1}"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid end index. Please provide a number.")
            return
    
    # Remove videos from passed_links
    videos_to_revoke = []
    for i in range(start_index, end_index + 1):
        if i < len(tag_videos):
            video_key = f"{tag}_{i}"  # Use index directly since it's a list
            if video_key in passed_links:
                passed_links.remove(video_key)
                videos_to_revoke.append(video_key)
    
    if videos_to_revoke:
        save_passed_links()
        count = len(videos_to_revoke)
        
        # Determine range text for response
        if start_index == 0 and end_index == len(tag_videos) - 1:
            range_text = f"entire tag '{tag}'"
        else:
            range_text = f"'{tag}' (indices {start_index}-{end_index})"
        
        await update.message.reply_text(
            f"🚫 Revoked {count} videos from {range_text}\n"
            f"🔀 Resources removed from Random Pick pool",
            parse_mode=ParseMode.HTML
        )
    else:
        range_text = f"'{tag}' (indices {start_index}-{end_index})" if start_index != 0 or end_index != len(tag_videos) - 1 else f"entire tag '{tag}'"
        await update.message.reply_text(
            f"ℹ️ No resources in {range_text} were in Random Pick.",
            parse_mode=ParseMode.HTML
        )


async def revoke_link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Revoke shareable links (removes from active_links)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text("Usage: /revokelink <tag> [start_index] [end_index]")
        return

    tag = context.args[0].strip().lower()

    # Determine what to remove from active_links
    if len(context.args) == 3:
        # Range format: /revokelink tag start end
        try:
            revoke_start = int(context.args[1])
            revoke_end = int(context.args[2])
            link_key = f"{tag}_{revoke_start}_{revoke_end}"
            range_text = f"'{tag}' (indices {revoke_start}-{revoke_end})"
        except ValueError:
            await update.message.reply_text("❌ Invalid indices. Please provide numbers.")
            return
    else:
        # Simple tag format: /revokelink tag
        link_key = tag
        range_text = f"tag '{tag}'"
        revoke_start = None
        revoke_end = None

    # Check if exact link exists
    if link_key in active_links:
        # Get info about what's being removed
        link_info = active_links[link_key]
        video_count = len(link_info.get("videos", [])) if isinstance(link_info, dict) else 0

        del active_links[link_key]
        save_active_links()
        await update.message.reply_text(
            f"🚫 Shareable link for {range_text} has been revoked.\n"
            f"📊 {video_count} videos removed from shareable access.",
            parse_mode=ParseMode.HTML)
        return

    # If exact match not found and this is a simple tag format, find any entries with this tag
    if revoke_start is None and revoke_end is None:
        # Find all entries with this tag
        matching_keys = []
        for existing_key, link_data in active_links.items():
            if isinstance(link_data, dict) and link_data.get("tag") == tag:
                matching_keys.append(existing_key)
        
        if matching_keys:
            # Remove all matching entries
            total_videos = 0
            for key in matching_keys:
                link_info = active_links[key]
                total_videos += len(link_info.get("videos", []))
                del active_links[key]
            
            save_active_links()
            await update.message.reply_text(
                f"🚫 Shareable link for {range_text} has been revoked.\n"
                f"📊 {total_videos} videos removed from shareable access.",
                parse_mode=ParseMode.HTML)
            return

    # If no modifications were made
    await update.message.reply_text(
        f"❌ Shareable link for {range_text} is not active.")
    return


async def activelinks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show shareable links created with /passlink command"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not active_links:
        await update.message.reply_text("📂 No shareable links created with /passlink.")
        return

    msg = "<b>🔗 Shareable Links (Independent Storage):</b>\n\n"
    for link_key in sorted(active_links.keys()):
        link = f"https://t.me/{BOT_USERNAME}?start={link_key}"
        link_data = active_links[link_key]
        
        if isinstance(link_data, dict):
            videos = link_data.get("videos", [])
            tag = link_data.get("tag", "unknown")
            actual_indices = link_data.get("actual_indices", [])
            start_idx = link_data.get("start_index")
            end_idx = link_data.get("end_index")
            
            # Use actual_indices if available, otherwise fall back to range
            if actual_indices:
                # Group continuous ranges for cleaner display
                ranges = []
                i = 0
                while i < len(actual_indices):
                    start = actual_indices[i]
                    end = start
                    
                    # Find continuous sequence
                    while i + 1 < len(actual_indices) and actual_indices[i + 1] == actual_indices[i] + 1:
                        i += 1
                        end = actual_indices[i]
                    
                    # Add range to list
                    if start == end:
                        ranges.append(str(start))
                    else:
                        ranges.append(f"{start}-{end}")
                    i += 1
                
                range_info = ", ".join(ranges)
                msg += f"🔗 <code>{tag}</code> ({range_info}) ({len(videos)} videos) - {link}\n"
            elif start_idx is not None and end_idx is not None and start_idx != end_idx:
                # Fallback to range format
                msg += f"🔗 <code>{tag}</code> ({start_idx}-{end_idx}) ({len(videos)} videos) - {link}\n"
            else:
                # Single tag format
                msg += f"🔗 <code>{tag}</code> ({len(videos)} videos) - {link}\n"
        else:
            # Legacy format
            msg += f"🔗 <code>{link_key}</code> - {link}\n"
    
    msg += "\n💡 These links are completely independent from Random Pick pool"
    await update.message.reply_html(msg)


async def passlinks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show active curated Random Pick links (from /pass command)"""
    if update.effective_user.id != ADMIN_ID:
        return

    # Collect tags and their indices that have videos in passed_links.json (from /pass command)
    tags_data = {}
    for video_key in passed_links:
        if '_' in video_key:
            # Extract tag name and index from video_key like "rd2_0" -> "rd2", index=0
            parts = video_key.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                tag = '_'.join(parts[:-1])
                index = int(parts[-1])
                
                if tag not in tags_data:
                    tags_data[tag] = set()
                tags_data[tag].add(index)

    if not tags_data:
        await update.message.reply_text("📂 No active curated Random Pick links.")
        return

    msg = "<b>🔀 Active Curated Random Pick Links:</b>\n\n"
    for tag in sorted(tags_data.keys()):
        indices = sorted(tags_data[tag])
        link = f"https://t.me/{BOT_USERNAME}?start={tag}"
        video_count = len(indices)
        
        # Show range information
        if len(indices) == 1:
            range_info = f"index {indices[0]}"
        elif indices == list(range(min(indices), max(indices) + 1)):
            # Continuous range
            range_info = f"indices {min(indices)}-{max(indices)}"
        else:
            # Fragmented indices - show first few and count
            if len(indices) <= 5:
                range_info = f"indices {', '.join(map(str, indices))}"
            else:
                range_info = f"indices {indices[0]}-{indices[-1]} ({video_count} total)"
        
    msg += f"🔀 <code>{tag}</code> ({range_info}) - {link}\n"
    
    msg += "\n💡 These links feed the Random Pick curated pool"
    await update.message.reply_html(msg)


async def listactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    # Collect all active links from both sources
    all_active_links = set()
    
    # Add links from active_links.json (from /passlink command)
    for link_key in active_links.keys():
        all_active_links.add(link_key)
    
    # Add tags that have videos in passed_links.json (from /pass command)
    tags_in_passed = set()
    for video_key in passed_links:
        if '_' in video_key:
            # Extract tag name from video_key like "rd2_0" -> "rd2"
            parts = video_key.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                tag = '_'.join(parts[:-1])
                tags_in_passed.add(tag)
    
    # Add tags from passed_links to all_active_links
    for tag in tags_in_passed:
        all_active_links.add(tag)

    if not all_active_links:
        await update.message.reply_text("📂 No active links.")
        return

    msg = "<b>🔗 Active Links:</b>\n"
    for link_key in sorted(all_active_links):
        link = f"https://t.me/{BOT_USERNAME}?start={link_key}"
        if '_' in link_key and link_key.count('_') >= 2:
            # Check if it's a range format
            parts = link_key.split('_')
            if len(parts) >= 3 and parts[-2].isdigit() and parts[-1].isdigit():
                tag = '_'.join(parts[:-2])
                start_idx = parts[-2]
                end_idx = parts[-1]
                source = "🔗" if link_key in active_links else "🔀"
                # Show video count for passlink created links
                video_count = ""
                if link_key in active_links and isinstance(active_links[link_key], dict):
                    videos = active_links[link_key].get("videos", [])
                    video_count = f" ({len(videos)} videos)"
                msg += f"{source} <code>{tag}</code> ({start_idx}-{end_idx}){video_count} - {link}\n"
            else:
                # Not a range, just display as is
                source = "🔗" if link_key in active_links else "🔀"
                video_count = ""
                if link_key in active_links and isinstance(active_links[link_key], dict):
                    videos = active_links[link_key].get("videos", [])
                    video_count = f" ({len(videos)} videos)"
                msg += f"{source} <code>{link_key}</code>{video_count} - {link}\n"
        else:
            # Simple tag format
            source = "🔗" if link_key in active_links else "🔀"
            video_count = ""
            if link_key in active_links and isinstance(active_links[link_key], dict):
                videos = active_links[link_key].get("videos", [])
                video_count = f" ({len(videos)} videos)"
            msg += f"{source} <code>{link_key}</code>{video_count} - {link}\n"
    
    msg += "\n🔗 = Created with /passlink (independent storage)\n🔀 = Created with /pass (Random Pick access)"
    await update.message.reply_html(msg)


async def show_favorites_navigator(query, context: ContextTypes.DEFAULT_TYPE, index=0, edit_message=False):
    """Show user's favorite videos with navigation"""
    user_id = str(query.from_user.id)
    
    if user_id not in favorites_data["user_favorites"] or not favorites_data["user_favorites"][user_id]:
        empty_fav_text = "🔖 No bookmarks yet! Use ❤️ on any resource to save it."
        empty_fav_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📚 Browse Library", callback_data="random_18minus"),
                InlineKeyboardButton("🔀 Random Pick", callback_data="random_18plus")
            ]
        ])
        if edit_message:
            await query.edit_message_text(empty_fav_text, reply_markup=empty_fav_keyboard)
        else:
            await query.message.reply_text(empty_fav_text, reply_markup=empty_fav_keyboard)
        return
    user_favs = favorites_data["user_favorites"][user_id]
    total_favorites = len(user_favs)
    
    # Ensure index is within bounds
    if index >= total_favorites:
        index = 0
    elif index < 0:
        index = total_favorites - 1
    
    try:
        video_id = user_favs[index]
        tag, idx = video_id.split('_', 1)
        idx = int(idx)
        
        if tag in media_data and idx < len(media_data[tag]):
            item = media_data[tag][idx]
            # Create unique shareable link for this specific file
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            share_link = f"https://telegram.me/share/url?url={file_link}"
            
            # Build proper media caption with global caption and replacements
            media_type = item.get("type", "video")
            base_caption = build_media_caption("", tag, str(idx), share_link, media_type)
            cap = f"⭐ <b>Favorite {index + 1}/{total_favorites}</b>\n{base_caption}"
            
            # Create navigation buttons
            nav_buttons = []
            
            # Previous/Next buttons
            if total_favorites > 1:
                prev_index = index - 1 if index > 0 else total_favorites - 1
                next_index = index + 1 if index < total_favorites - 1 else 0
                
                nav_buttons.append([
                    InlineKeyboardButton("⬅️ Previous", callback_data=f"fav_nav_{prev_index}"),
                    InlineKeyboardButton("➡️ Next", callback_data=f"fav_nav_{next_index}")
                ])
            
            # Remove from favorites button
            nav_buttons.append([
                InlineKeyboardButton("💔 Remove from Favorites", callback_data=f"remove_fav_{video_id}")
            ])
            
            # Add share button
            share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            share_url = f"https://t.me/share/url?url={share_link}"
            nav_buttons.append([
                InlineKeyboardButton("🔗 Share this Video", url=share_url)
            ])
            
            # Add bookmark and Random Pick buttons at bottom
            nav_buttons.append([
                InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites"),
                InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
            ])
            if is_admin(query.from_user.id):
                nav_buttons.append([InlineKeyboardButton("🗑️ Remove Media", callback_data=f"del_media_{video_id}")])
            
            keyboard = InlineKeyboardMarkup(nav_buttons)
            
            if edit_message:
                # For navigation, we edit the existing message
                media_kwargs = {
                    "context": context,
                    "chat_id": query.message.chat_id,
                    "caption": cap,
                    "reply_markup": keyboard,
                    "parse_mode": ParseMode.HTML,
                    "protect_content": should_protect_content(query.from_user.id, query.message.chat_id)
                }
                
                if item["type"] == "video":
                    media_kwargs["video"] = item["file_id"]
                elif item["type"] == "photo":
                    media_kwargs["photo"] = item["file_id"]
                elif item["type"] == "document":
                    media_kwargs["document"] = item["file_id"]
                elif item["type"] == "audio":
                    media_kwargs["audio"] = item["file_id"]
                elif item["type"] == "voice":
                    media_kwargs["voice"] = item["file_id"]
                elif item["type"] == "animation":
                    media_kwargs["animation"] = item["file_id"]
                elif item["type"] == "sticker":
                    media_kwargs["sticker"] = item["file_id"]
                    # Stickers don't support captions
                    media_kwargs.pop("caption", None)
                else:
                    # Fallback for unknown types
                    media_kwargs["text"] = f"Unsupported media type: {item['type']}"
                
                await safe_send_message(**media_kwargs)
            else:
                # For first time viewing, send new message
                media_kwargs = {
                    "context": context,
                    "chat_id": query.message.chat_id,
                    "caption": cap,
                    "reply_markup": keyboard,
                    "parse_mode": ParseMode.HTML,
                    "protect_content": should_protect_content(query.from_user.id, query.message.chat_id)
                }
                
                if item["type"] == "video":
                    media_kwargs["video"] = item["file_id"]
                elif item["type"] == "photo":
                    media_kwargs["photo"] = item["file_id"]
                elif item["type"] == "document":
                    media_kwargs["document"] = item["file_id"]
                elif item["type"] == "audio":
                    media_kwargs["audio"] = item["file_id"]
                elif item["type"] == "voice":
                    media_kwargs["voice"] = item["file_id"]
                elif item["type"] == "animation":
                    media_kwargs["animation"] = item["file_id"]
                elif item["type"] == "sticker":
                    media_kwargs["sticker"] = item["file_id"]
                    # Stickers don't support captions
                    media_kwargs.pop("caption", None)
                else:
                    # Fallback for unknown types
                    media_kwargs["text"] = f"Unsupported media type: {item['type']}"
                
                await safe_send_message(**media_kwargs)
        else:
            # Invalid video, remove it from favorites
            favorites_data["user_favorites"][user_id].remove(video_id)
            if video_id in favorites_data["video_likes"]:
                favorites_data["video_likes"][video_id] = max(0, favorites_data["video_likes"][video_id] - 1)
            save_favorites()
            
            # Show next video if available
            if favorites_data["user_favorites"][user_id]:
                await show_favorites_navigator(query, context, index, edit_message)
            else:
                if edit_message:
                    await query.edit_message_text("⭐ No more favorites available!")
                else:
                    await query.message.reply_text("⭐ No more favorites available!")
    
    except Exception as e:
        print(f"Error showing favorite: {e}")
        if edit_message:
            await query.edit_message_text("❌ Error loading favorite video.")
        else:
            await query.message.reply_text("❌ Error loading favorite video.")


async def add_to_favorites(query, context: ContextTypes.DEFAULT_TYPE):
    """Add a video to user's favorites"""
    user_id = str(query.from_user.id)
    video_id = query.data.replace("add_fav_", "")
    
    # Initialize user favorites if not exists
    if user_id not in favorites_data["user_favorites"]:
        favorites_data["user_favorites"][user_id] = []
    
    # Add to favorites if not already there
    if video_id not in favorites_data["user_favorites"][user_id]:
        favorites_data["user_favorites"][user_id].append(video_id)
        
        # Update video likes count
        if video_id not in favorites_data["video_likes"]:
            favorites_data["video_likes"][video_id] = 0
        favorites_data["video_likes"][video_id] += 1
        
        save_favorites()
        await query.answer("❤️ Added to favorites!")
        
        # Update the button (switch to remove option)
        new_fav_button = InlineKeyboardButton("💔 Remove from Favorites", 
                        callback_data=f"remove_fav_{video_id}")
        my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
        new_keyboard = InlineKeyboardMarkup([
            [new_fav_button],
            [my_favs_button, random_button]
        ])
        
        try:
            await query.edit_message_reply_markup(reply_markup=new_keyboard)
        except:
            pass  # Message might be too old to edit
    else:
        await query.answer("Already in favorites!")


async def remove_from_favorites(query, context: ContextTypes.DEFAULT_TYPE):
    """Remove a video from user's favorites"""
    user_id = str(query.from_user.id)
    video_id = query.data.replace("remove_fav_", "")
    
    if user_id in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id]:
        favorites_data["user_favorites"][user_id].remove(video_id)
        
        # Update video likes count
        if video_id in favorites_data["video_likes"]:
            favorites_data["video_likes"][video_id] = max(0, favorites_data["video_likes"][video_id] - 1)
        
        save_favorites()
        await query.answer("💔 Removed from favorites!")
        
        # Update the button (switch to add option)
        new_fav_button = InlineKeyboardButton("❤️ Add to Favorites", 
                        callback_data=f"add_fav_{video_id}")
        my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
        new_keyboard = InlineKeyboardMarkup([
            [new_fav_button],
            [my_favs_button, random_button]
        ])
        
        try:
            await query.edit_message_reply_markup(reply_markup=new_keyboard)
        except:
            pass  # Message might be too old to edit
    else:
        await query.answer("Not in favorites!")


async def show_who_liked_video(query, context: ContextTypes.DEFAULT_TYPE):
    """Admin function to show which users have liked a specific video"""
    # Check if user is admin
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ Admin only feature!")
        return
    
    # Extract video_id from callback data
    video_id = query.data.replace("who_liked_", "")
    
    # Find users who liked this video
    users_who_liked = []
    for user_id, user_favorites in favorites_data["user_favorites"].items():
        if video_id in user_favorites:
            try:
                # Try to get user info
                user = await context.bot.get_chat(user_id)
                display_name = user.first_name or user.username or f"User {user_id}"
                users_who_liked.append(f"👤 {display_name} (ID: {user_id})")
            except:
                # If we can't get user info, just show the ID
                users_who_liked.append(f"👤 User ID: {user_id}")
    
    # Get video statistics
    total_likes = favorites_data["video_likes"].get(video_id, 0)
    
    # Parse video info for display
    if "_" in video_id:
        tag, idx_str = video_id.rsplit("_", 1)
        try:
            idx = int(idx_str)
            video_info = f"📹 Video: <code>{tag}</code> | Index: <code>{idx}</code>"
        except ValueError:
            video_info = f"📹 Video: <code>{video_id}</code>"
    else:
        video_info = f"📹 Video: <code>{video_id}</code>"
    
    # Create response message
    if users_who_liked:
        user_list = "\n".join(users_who_liked)
        message = (
            f"👥 <b>Users who liked this video:</b>\n\n"
            f"{video_info}\n"
            f"❤️ Total likes: <b>{total_likes}</b>\n\n"
            f"{user_list}"
        )
    else:
        message = (
            f"💔 <b>No users have liked this video yet</b>\n\n"
            f"{video_info}\n"
            f"❤️ Total likes: <b>{total_likes}</b>"
        )
    
    await query.message.reply_html(message)
    await query.answer()


async def view_specific_video(query, context: ContextTypes.DEFAULT_TYPE):
    """Admin function to view a specific video from the top liked list"""
    # Check if user is admin
    if not is_admin(query.from_user.id):
        await query.answer("❌ Admin only feature!")
        return
    
    # Extract video_id from callback data
    video_id = query.data.replace("view_video_", "")
    
    try:
        # Parse video_id to get tag and index
        tag, idx_str = video_id.rsplit("_", 1)
        idx = int(idx_str)
        
        # Check if video exists in media_data
        if tag not in media_data or idx >= len(media_data[tag]):
            await query.message.reply_text(f"❌ Video not found: {tag}_{idx}")
            await query.answer()
            return
        
        # Get the video data
        video_data = media_data[tag][idx]
        
        # Create caption with video info and stats
        likes_count = favorites_data["video_likes"].get(video_id, 0)
        cap = (
            f"🎬 <b>Direct Video View</b>\n\n"
            f"📁 Tag: <code>{tag}</code>\n"
            f"📊 Index: <code>{idx}</code>\n"
            f"❤️ Total Likes: <b>{likes_count}</b>"
        )
        
        # Create admin buttons for this video
        fav_button = InlineKeyboardButton("❤️ Add to Bookmarks", 
                        callback_data=f"add_fav_{video_id}")
        who_liked_button = InlineKeyboardButton("👥 WHO", 
                              callback_data=f"who_liked_{video_id}")
        my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
        
        keyboard = InlineKeyboardMarkup([
            [fav_button, who_liked_button],
            [my_favs_button, random_button],
            [InlineKeyboardButton("🗑️ Remove Media", callback_data=f"del_media_{video_id}")]
        ])
        
        # Send the video
        if video_data.get("type") == "video" and "file_id" in video_data:
            await safe_send_message(
                context=context,
                chat_id=query.message.chat_id,
                video=video_data["file_id"],
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
            )
        elif video_data.get("type") == "photo" and "file_id" in video_data:
            await safe_send_message(
                context=context,
                chat_id=query.message.chat_id,
                photo=video_data["file_id"],
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
            )
        else:
            await query.message.reply_text(f"❌ Invalid media data for {video_id}")
        
        await query.answer("🎬 Video loaded!")
        
    except ValueError:
        await query.message.reply_text(f"❌ Invalid video format: {video_id}")
        await query.answer()
    except Exception as e:
        await query.message.reply_text(f"❌ Error loading video: {str(e)}")
        await query.answer()


async def view_deleted_media(query, context: ContextTypes.DEFAULT_TYPE):
    """View a specific deleted media with restore option"""
    global deleted_media_storage
    
    try:
        # Extract video key from callback data (format: view_deleted_tag_idx)
        callback_data = query.data
        video_key = callback_data.replace("view_deleted_", "")
        
        print(f"🔍 Viewing deleted media: {video_key}")
        
        # Check if the deleted media exists
        if video_key not in deleted_media_storage:
            await query.message.reply_text("❌ Deleted media not found!")
            await query.answer()
            return
        
        # Get the deleted media data
        deleted_entry = deleted_media_storage[video_key]
        deleted_media = deleted_entry["data"]  # The actual media data is nested in "data"
        tag = deleted_entry["tag"]
        original_position = deleted_entry["original_position"]
        
        print(f"🔍 Retrieved deleted media data: type={deleted_media.get('type', 'N/A')}, has_file_id={'file_id' in deleted_media}")
        
        # Create caption for deleted media
        cap = (
            f"🗑️ <b>Deleted Media View</b>\n\n"
            f"📁 Tag: <code>{tag}</code>\n"
            f"📊 Original Position: <code>{original_position}</code>\n"
            f"⚠️ Status: <b>DELETED</b>\n"
            f"💾 Stored: {deleted_entry.get('deleted_date', 'Unknown')}"
        )
        
        # Create restore button
        restore_button = InlineKeyboardButton("🔄 Restore Media", 
                                            callback_data=f"restore_media_{video_key}")
        back_button = InlineKeyboardButton("⬅️ Back to Deleted List", 
                                         callback_data="list_deleted_media")
        
        keyboard = InlineKeyboardMarkup([
            [restore_button],
            [back_button]
        ])
        
        # Send the deleted media
        if deleted_media.get("type") == "video" and "file_id" in deleted_media:
            await safe_send_message(
                context=context,
                chat_id=query.message.chat_id,
                video=deleted_media["file_id"],
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
            )
        elif deleted_media.get("type") == "photo" and "file_id" in deleted_media:
            await safe_send_message(
                context=context,
                chat_id=query.message.chat_id,
                photo=deleted_media["file_id"],
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=should_protect_content(query.from_user.id, query.message.chat_id)
            )
        else:
            # Debug information for troubleshooting
            print(f"❌ Invalid deleted media data for {video_key}")
            print(f"    Media type: {deleted_media.get('type', 'MISSING')}")
            print(f"    Has file_id: {'file_id' in deleted_media}")
            print(f"    Media keys: {list(deleted_media.keys())}")
            await query.message.reply_text(f"❌ Invalid deleted media data for {video_key}\nType: {deleted_media.get('type', 'MISSING')}\nHas file_id: {'file_id' in deleted_media}")
        
        await query.answer("🗑️ Deleted media loaded!")
        
    except Exception as e:
        await query.message.reply_text(f"❌ Error loading deleted media: {str(e)}")
        await query.answer()
        print(f"❌ Error in view_deleted_media: {str(e)}")


async def cleanup_deleted_media():
    """Clean up corrupted or invalid deleted media entries"""
    global deleted_media_storage
    
    corrupted_keys = []
    
    print(f"🧹 Starting cleanup of deleted media storage ({len(deleted_media_storage)} entries)")
    
    for video_key, deleted_entry in deleted_media_storage.items():
        try:
            # Check if the entry has the required structure
            if not isinstance(deleted_entry, dict):
                print(f"❌ Corrupted entry (not dict): {video_key}")
                corrupted_keys.append(video_key)
                continue
                
            if "data" not in deleted_entry:
                print(f"❌ Corrupted entry (no 'data' field): {video_key}")
                corrupted_keys.append(video_key)
                continue
                
            media_data = deleted_entry["data"]
            if not isinstance(media_data, dict):
                print(f"❌ Corrupted entry (data not dict): {video_key}")
                corrupted_keys.append(video_key)
                continue
                
            # Check if media data has required fields
            if "file_id" not in media_data or "type" not in media_data:
                print(f"❌ Invalid media data (missing file_id or type): {video_key}")
                corrupted_keys.append(video_key)
                continue
                
            # Silent validation - only show errors
            
        except Exception as e:
            print(f"\n❌ Error checking entry {video_key}: {str(e)}")
            corrupted_keys.append(video_key)
    
    # Remove corrupted entries
    if corrupted_keys:
        print(f"\n🗑️ Removing {len(corrupted_keys)} corrupted entries: {corrupted_keys}")
        for key in corrupted_keys:
            del deleted_media_storage[key]
        save_deleted_media()
        print(f"✅ Cleanup complete. {len(deleted_media_storage)} entries remaining")
    
    return len(corrupted_keys)


async def view_user_favorites(query, context: ContextTypes.DEFAULT_TYPE):
    """Show user's favorite videos using navigator"""
    await show_favorites_navigator(query, context, 0)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command for users"""
    user_id = update.effective_user.id
    
    # Basic user help
    help_text = (
        "🤖 <b>Knowledge Vault Bot - Quick Guide</b>\n\n"
        "🔖 <b>Bookmarks:</b>\n"
        "• Tap ❤️ on any resource to add to bookmarks\n"
        "• Tap 💔 to remove from bookmarks\n"
        "• Use 🔖 Bookmarks to browse saved items\n"
        "• Navigate with ⬅️ Previous / ➡️ Next buttons\n\n"
        
        "📚 <b>Accessing Resources:</b>\n"
        "• 📚 Browse Library → explore all collections\n"
        "• 🔀 Random Pick → curated discovery\n"
        "• Deep links give direct access to a single item\n\n"
        "🎯 <b>Discovery Engine:</b>\n"
        "• Rotation avoids immediate repeats\n"
        "• Curated pool powers Random Pick\n\n"
    "💡 <b>Tip:</b> Every resource has 🔖 Bookmarks and 🔀 Random Pick buttons for quick access!"
    )
    
    # Add admin commands if user is admin
    if user_id == ADMIN_ID:
        admin_help = [
            "\n\n🔧 <b>Admin Commands:</b>\n"
            "• <b>/upload</b> - Upload new media to database\n"
            "• <b>/listvideo</b> - List special tags (wildwed, freakyfri, etc.)\n"
            "• <b>/listvideos</b> - List all other tags\n"
            "• <b>/remove &lt;tag&gt; &lt;index&gt;</b> - Remove specific video\n"
            "• <b>/get &lt;tag&gt;</b> - Get all videos from a tag\n"
            "• <b>/generatelink &lt;tag&gt;</b> - Generate public link for tag\n"
            "• <b>/view &lt;tag&gt; &lt;index&gt;</b> - View specific video by index\n"
            "• <b>/pass &lt;tag&gt; [start] [end]</b> - Add resources to Random Pick pool ONLY\n"
            "• <b>/passlink &lt;tag&gt; [start] [end]</b> - Create shareable links ONLY (not for Random Pick)\n"
            "• <b>/revoke &lt;tag&gt; [start] [end]</b> - Remove from Random Pick pool ONLY\n"
            "• <b>/revokelink &lt;tag&gt; [start] [end]</b> - Revoke shareable link access ONLY\n"
            "• <b>/activelinks</b> - List active entries in Random Pick pool\n"
            "• <b>/passlinks</b> - List shareable links (independent storage)\n"
            "• <b>/listactive</b> - List all active links (both types)\n"
            "• <b>/free &lt;tag&gt;</b> - Make tag freely accessible\n"
            "• <b>/listfree</b> - List all free tags\n\n"
            "🛡️ <b>Protection Commands:</b>\n"
            "• <b>/protectionon</b> - Enable media protection (prevent saving)\n"
            "• <b>/protectionoff</b> - Disable media protection (allow saving)\n"
            "• <b>/protection</b> - Check current protection status\n\n"
            "• <b>/listremoved</b> - List deleted & revoked media (with restore buttons)\n"
            "• <b>/restoremedia &lt;tag&gt; &lt;index&gt;</b> - Restore a deleted/revoked media at index\n\n"
            
            "🧹 <b>Auto-Delete System:</b>\n"
            f"• <b>/autodelete on</b> - Enable auto-deletion after {AUTO_DELETE_HOURS} hour(s)\n"
            "• <b>/autodelete off</b> - Disable auto-deletion\n"
            "• <b>/autodelete hours &lt;hours&gt;</b> - Set deletion time (0.1-168 hours)\n"
            "• <b>/autodelete status</b> - Check current settings\n"
            "• <b>/notifications on/off</b> - Control deletion notifications\n"
            "• <b>/autodelete stats</b> - View deletion statistics\n"
            "• <b>/autodelete clear</b> - Clear all tracked messages\n\n"
            
            "� <b>Analytics Commands:</b>\n"
            "• <b>/userfavorites &lt;user_id&gt;</b> - View user's favorites\n"
            "• <b>/videostats &lt;tag&gt; &lt;index&gt;</b> - See who liked a video\n"
            "• <b>/topvideos [limit]</b> - Most liked videos with direct access\n\n"
            
            "🔀 <b>Command Examples:</b>\n"
            "• <code>/pass bulk</code> (add entire bulk tag to Random Pick pool only)\n"
            "• <code>/pass bulk 0 49</code> (add videos 0-49 to Random Pick pool only)\n"
            "• <code>/passlink bulk</code> (create shareable link for entire tag only)\n"
            "• <code>/passlink bulk 0 49</code> (create shareable link for range only)\n"
            "• <code>/revoke bulk</code> (remove entire bulk from Random Pick pool only)\n"
            "• <code>/revoke bulk 0 49</code> (remove range from Random Pick pool only)\n"
            "• <code>/revokelink bulk</code> (disable shareable link only)\n"
            "• <code>/revokelink bulk 0 49</code> (disable specific range link only)\n\n"
            
            "⚠️ <b>Important:</b> Curation systems are separate!\n"
            "• Use <code>/pass</code> to feed the Random Pick feature\n"
            "• Use <code>/passlink</code> for shareable links only\n"
            "• Each system works independently\n\n"
            "♻️ <b>Removal System:</b> Use inline Revoke / Delete buttons on any media. Revoke skips without deleting. Delete is soft (undo with Restore button, /restoremedia, or /listremoved).\n\n"
            
            "👥 <b>Special Features:</b>\n"
            "• <b>WHO Button:</b> Click on any video to see who liked it\n"
            "• <b>Direct Video Access:</b> Click video links in analytics\n"
            "• <b>Per-Video Security:</b> Pass specific videos, not entire tags\n"
            "• <b>Analytics Integration:</b> Real-time stats with interactive buttons"
        ]
        help_text += admin_help
    
    await update.message.reply_html(help_text)


async def handle_text_shortcuts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text shortcuts for quick access"""
    if not update.message or not update.message.text:
        return
    
    # Track user interaction
    user = update.effective_user
    track_user(user.id, user.username, user.first_name)
    
    # ---- Reply Keyboard Button Labels (centralized) ----
    RANDOM_SAFE_LABEL = "📚 Browse Library"      # formerly Random (18-)
    RANDOM_PASSED_LABEL = "🔀 Random Pick"       # formerly Random (18+)
    FAV_LABEL = "🔖 Bookmarks"
    HELP_LABEL = "❓ Help"
    HOME_LABEL = "🏠 Home"

    # Helper to show admin quick access keyboard
    def admin_home_keyboard():
        return ReplyKeyboardMarkup([
            ["📊 Stats", "📢 Broadcast", "👥 Users"],
            ["🎬 Media", "💾 Backup", "🛡️ Protection"],
            ["🧹 Cleanup", "🔧 Tools", "🧪 Test"],
            ["🏠 Full Menu"]
        ], resize_keyboard=True)
    
    # Helper to show user quick access keyboard  
    def user_home_keyboard():
        return ReplyKeyboardMarkup([
            ["🔀 Random Pick", "🔥 Top Resources"],
            ["𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️"]
        ], resize_keyboard=True)

    # Helper to show minimal Home keyboard (fallback)
    def minimal_home_keyboard():
        return ReplyKeyboardMarkup([[HOME_LABEL]], resize_keyboard=True)
    
    # Helper to get appropriate keyboard based on user type
    def get_home_keyboard(user_id):
        if is_admin(user_id):
            return admin_home_keyboard()
        else:
            return user_home_keyboard()

    # Obtain raw text early so it's available for Home check
    raw_text = update.message.text.strip()

    # If user presses Home, show appropriate quick access menu
    if raw_text == HOME_LABEL:
        user_id = update.effective_user.id
        if is_admin(user_id):
            await update.message.reply_text(
                "🏠 **Admin Quick Access**\n\n"
                "Choose from commonly used admin functions:",
                reply_markup=admin_home_keyboard(),
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "🏠 **Quick Access Menu**\n\n"
                "Here are your available options:",
                reply_markup=user_home_keyboard(), 
                parse_mode='Markdown'
            )
        return

    # Handle "🏠 Full Menu" button - redirect to /start
    if raw_text == "🏠 Full Menu":
        await start(update, context)
        return

    # Handle Admin Quick Access buttons
    if raw_text == "📊 Stats":
        await update.message.reply_text(
            "📊 **Quick Stats Commands:**\n\n"
            "• `/userstats` - User registration statistics\n"
            "• `/userinfo <user_id>` - Get detailed user information\n"
            "• `/topusers` - Most active users ranking\n"
            "• `/videostats <tag> <index>` - Check who liked a video\n"
            "• `/topvideos` - Most liked videos with navigation\n"
            "• `/bstats` - Broadcasting statistics\n"
            "• `/deletedstats` - Deleted media statistics",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "📢 Broadcast":
        await update.message.reply_text(
            "📢 **Broadcasting Options:**\n\n"
            "• `/broadcast <message>` - Normal broadcast\n"
            "• `/dbroadcast <message>` - Auto-delete broadcast\n"
            "• `/pbroadcast <message>` - Pin broadcast\n" 
            "• `/sbroadcast <message>` - Silent broadcast\n"
            "• `/fbroadcast <message>` - Forward mode broadcast",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "👥 Users":
        await update.message.reply_text(
            "👥 **User Management:**\n\n"
            "• `/discover` - Discover users from all sources\n"
            "• `/addusers <id1> <id2>` - Add users to database\n"
            "• `/topusers` - Most active users ranking\n"
            "• `/userinfo <user_id>` - Get user information\n"
            "• `/userfavorites <user_id>` - Check user's favorites\n"
            "• `/add_admin <user_id>` - Add admin privileges\n"
            "• `/remove_admin <user_id>` - Remove admin privileges\n"
            "• `/list_admins` - List all administrators",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'  
        )
        return
        
    elif raw_text == "🧹 Cleanup":
        await update.message.reply_text(
            "🧹 **Cleanup & Media Management:**\n\n"
            "• `/listdeleted` - Show all deleted media\n"
            "• `/listrevoked` - Show all revoked media\n"
            "• `/listremoved` - Show all removed media\n"
            "• `/cleanupdeleted` - Clean up corrupted deleted media\n"
            "• `/restoredeleted <tag> <index>` - Restore deleted media\n"
            "• `/cleardeleted` - Permanently remove all deleted media\n"
            "• `/restoreall` - Restore all deleted media\n"
            "• `/restoremedia <tag> <index>` - Restore specific media\n"
            "• `/autodelete` - Auto-deletion controls\n"
            "• `/notifications on/off` - Control deletion notifications",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "🔧 Tools":
        await update.message.reply_text(
            "🔧 **Admin Tools:**\n\n"
            "• `/list_admins` - List all administrators\n"
            "• `/add_admin <user_id>` - Add admin privileges\n"
            "• `/remove_admin <user_id>` - Remove admin privileges\n"
            "• `/checkupdates` - Check for pending requests\n"
            "• `/autodelete on/off/status` - Auto-deletion controls\n"
            "• `/autodelete hours <hours>` - Set deletion time\n"
            "• `/autodelete clear` - Clear tracking list\n"
            "• `/autodelete stats` - View deletion statistics",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "🎬 Media":
        await update.message.reply_text(
            "🎬 **Media Management:**\n\n"
            "• `/topvideos` - Show top rated videos\n"
            "• `/videostats <tag> <index>` - Get video statistics\n"
            "• `/custom_batch <tag>` - Start custom batch collection\n"
            "• `/stop_batch` - Stop current batch session\n"
            "• `/cancel_batch` - Cancel batch session\n"
            "• `/batch_status` - Check batch session status\n"
            "• `/set_global_caption <text>` - Set global caption\n"
            "• `/getfileid` - Get file ID from forwarded media\n"
            "• `/setwelcomeimage` - Set welcome image\n"
            "• `/testwelcomeimage` - Test welcome image",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "💾 Backup":
        await update.message.reply_text(
            "💾 **Backup & Restore:**\n\n"
            "• `/backup` - Create new backup\n"
            "• `/listbackups` - List all backups\n"
            "• `/restore <backup_name>` - Restore from backup\n"
            "• `/backupstats <backup_name>` - Show backup details\n"
            "• `/deletebackup <backup_name>` - Delete backup\n"
            "• `/telegrambackup` - Send files to Telegram\n"
            "• `/autobackup on/off/now/status` - Auto-backup controls",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "🛡️ Protection":
        await update.message.reply_text(
            "🛡️ **Protection Commands:**\n\n"
            "• `/protection` - Check protection status\n"
            "• `/pon` - Protection ON (shortcut)\n"
            "• `/poff` - Protection OFF (shortcut)\n"
            "• `/pstatus` - Protection status (shortcut)\n"
            "• `/ps` - Protection status (shortcut)\n"
            "• `/testprotection` - Test media protection\n"
            "• `/checkprotection` - Check protection details",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "🧪 Test":
        await update.message.reply_text(
            "🧪 **Testing & Debug:**\n\n"
            "• `/testprotection` - Test media protection\n"
            "• `/testdeletion` - Test deletion functionality\n"
            "• `/debugdeleted <tag>` - Debug deleted media issues\n"
            "• `/checkupdates` - Check for pending old requests",
            reply_markup=admin_home_keyboard(),
            parse_mode='Markdown'
        )
        return

    # Handle User Quick Access buttons  
    elif raw_text == "🔀 Random Pick":
        # Show random selection mode chooser
        keyboard = [
            [InlineKeyboardButton("📚 Browse Library", callback_data="random_safe")],
            [InlineKeyboardButton("🔀 Random Pick", callback_data="random_nsfw")],
            [InlineKeyboardButton("❌ Cancel", callback_data="close_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "🔀 **Choose Discovery Mode:**\n\n"
            "Pick how you'd like to explore resources:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return
        
    elif raw_text == "🔥 Top Resources":
        # Call the top videos function for users
        await top_videos_command(update, context)
        return
        
    elif raw_text == "𝗰𝗹𝗶𝗰𝗸 𝗵𝗲𝗿𝗲 𝗳𝗼𝗿 𝗺𝗼𝗿𝗲 ❤️":
        # Direct redirect to the link
        keyboard = [[InlineKeyboardButton("🌐 Visit Channel", url="https://t.me/CypherHere")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🌐 **Visit Our Community**\n\n"
            "Click below to join our community channel:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Main menu reply keyboard buttons (exact match OR legacy synonyms)
    if raw_text in {RANDOM_SAFE_LABEL, "🎬 Download Video", "📚 Browse Library", "GET FILES"}:
        await send_random_video(context, update.effective_chat.id, mode="safe")
        # Replace main menu with appropriate home keyboard
        await update.message.reply_text("🔁 Menu minimized.", reply_markup=get_home_keyboard(update.effective_user.id))
        return

    if raw_text in {RANDOM_PASSED_LABEL, "🔀 Random Pick", "🔀 Random Video"}:
        await send_random_video(context, update.effective_chat.id, mode="adult")
        await update.message.reply_text("🔁 Menu minimized.", reply_markup=get_home_keyboard(update.effective_user.id))
        return

    if raw_text == FAV_LABEL:
        await favorites_command(update, context)
        await update.message.reply_text("📂 Bookmarks opened.", reply_markup=get_home_keyboard(update.effective_user.id))
        return

    if raw_text == HELP_LABEL:
        await help_command(update, context)
        await update.message.reply_text("ℹ️ Help shown.", reply_markup=get_home_keyboard(update.effective_user.id))
        return
    
    # Convert to lowercase for other shortcuts
    text = update.message.text.lower().strip()
    
    # Batch management keyboard buttons (lowercase check)
    if text in ["🛑 stop batch", "/stop_batch"]:
        from telegram import ReplyKeyboardRemove
        await stop_batch_command(update, context)
        await update.message.reply_text("Batch stopped.", reply_markup=ReplyKeyboardRemove())
        return

    if text in ["❌ cancel batch", "/cancel_batch"]:
        from telegram import ReplyKeyboardRemove
        await cancel_batch_command(update, context)
        await update.message.reply_text("Batch cancelled.", reply_markup=ReplyKeyboardRemove())
        return

    # Other text shortcuts
    if text in ["fav", "favorites", "❤️", "♥️"]:
        await favorites_command(update, context)
        return

    if text in ["random", "rand", "🎲", "🎬", "media"]:
        video_key = get_next_random_video()
        
    """Show active curated entries used by the Random Pick feature (/pass command)"""
    if update.effective_user.id != ADMIN_ID:
        return

    # Collect tags and their indices that have videos in passed_links.json (from /pass command)
    tags_data = {}
    for video_key in passed_links:
        if '_' in video_key:
            # Extract tag name and index from video_key like "rd2_0" -> tag="rd2", index=0
            parts = video_key.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                tag = '_'.join(parts[:-1])
                index = int(parts[-1])
                
                if tag not in tags_data:
                    tags_data[tag] = set()
                tags_data[tag].add(index)

    if not tags_data:
        await update.message.reply_text("📂 No active curated entries for Random Pick.")
        return

    msg = "<b>🔀 Active Curated Entries (Random Pick Pool):</b>\n\n"
    for tag in sorted(tags_data.keys()):
        indices = sorted(tags_data[tag])
        link = f"https://t.me/{BOT_USERNAME}?start={tag}"
        video_count = len(indices)
        
        # Show range information
        if len(indices) == 1:
            range_info = f"index {indices[0]}"
        elif indices == list(range(min(indices), max(indices) + 1)):
            # Continuous range
            range_info = f"indices {min(indices)}-{max(indices)}"
        else:
            # Fragmented indices - show first few and count
            if len(indices) <= 5:
                range_info = f"indices {', '.join(map(str, indices))}"
            else:
                range_info = f"indices {indices[0]}-{indices[-1]} ({video_count} total)"
        
        msg += f"🔀 <code>{tag}</code> ({range_info}) - {link}\n"
    
    msg += "\n💡 These entries power the Random Pick feature"
    await update.message.reply_html(msg)


async def listactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    # Collect all active links from both sources
    all_active_links = set()
    
    # Add links from active_links.json (from /passlink command)
    for link_key in active_links.keys():
        all_active_links.add(link_key)
    
    # Add tags that have videos in passed_links.json (from /pass command)
    tags_in_passed = set()
    for video_key in passed_links:
        if '_' in video_key:
            # Extract tag name from video_key like "rd2_0" -> "rd2"
            parts = video_key.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                tag = '_'.join(parts[:-1])
                tags_in_passed.add(tag)
    
    # Add tags from passed_links to all_active_links
    for tag in tags_in_passed:
        all_active_links.add(tag)

    if not all_active_links:
        await update.message.reply_text("📂 No active links.")
        return

    msg = "<b>🔗 Active Links:</b>\n"
    for link_key in sorted(all_active_links):
        link = f"https://t.me/{BOT_USERNAME}?start={link_key}"
        if '_' in link_key and link_key.count('_') >= 2:
            # Check if it's a range format
            parts = link_key.split('_')
            if len(parts) >= 3 and parts[-2].isdigit() and parts[-1].isdigit():
                tag = '_'.join(parts[:-2])
                start_idx = parts[-2]
                end_idx = parts[-1]
                source = "🔗" if link_key in active_links else "🔀"
                # Show video count for passlink created links
                video_count = ""
                if link_key in active_links and isinstance(active_links[link_key], dict):
                    videos = active_links[link_key].get("videos", [])
                    video_count = f" ({len(videos)} videos)"
                msg += f"{source} <code>{tag}</code> ({start_idx}-{end_idx}){video_count} - {link}\n"
            else:
                # Not a range, just display as is
                source = "🔗" if link_key in active_links else "🔀"
                video_count = ""
                if link_key in active_links and isinstance(active_links[link_key], dict):
                    videos = active_links[link_key].get("videos", [])
                    video_count = f" ({len(videos)} videos)"
                msg += f"{source} <code>{link_key}</code>{video_count} - {link}\n"
        else:
            # Simple tag format
            source = "🔗" if link_key in active_links else "🔀"
            video_count = ""
            if link_key in active_links and isinstance(active_links[link_key], dict):
                videos = active_links[link_key].get("videos", [])
                video_count = f" ({len(videos)} videos)"
            msg += f"{source} <code>{link_key}</code>{video_count} - {link}\n"
    
    msg += "\n🔗 = Created with /passlink (independent storage)\n🔀 = Created with /pass (Random Pick pool access)"
    await update.message.reply_html(msg)


async def show_favorites_navigator(query, context: ContextTypes.DEFAULT_TYPE, index=0, edit_message=False):
    """Show user's favorite videos with navigation"""
    user_id = str(query.from_user.id)
    
    if user_id not in favorites_data["user_favorites"] or not favorites_data["user_favorites"][user_id]:
        if edit_message:
            await query.edit_message_text("❤️ You haven't added any videos to favorites yet!")
        else:
            await query.message.reply_text("❤️ You haven't added any videos to favorites yet!")
        return
    
    user_favs = favorites_data["user_favorites"][user_id]
    total_favorites = len(user_favs)
    
    # Ensure index is within bounds
    if index >= total_favorites:
        index = 0
    elif index < 0:
        index = total_favorites - 1
    
    try:
        video_id = user_favs[index]
        tag, idx = video_id.split('_', 1)
        idx = int(idx)
        
        if tag in media_data and idx < len(media_data[tag]):
            item = media_data[tag][idx]
            # Create unique shareable link for this specific file
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            share_link = f"https://telegram.me/share/url?url={file_link}"
            
            # Build proper media caption with global caption and replacements
            media_type = item.get("type", "video")
            base_caption = build_media_caption("", tag, str(idx), share_link, media_type)
            cap = f"⭐ <b>Favorite {index + 1}/{total_favorites}</b>\n{base_caption}"
            
            # Create navigation buttons
            nav_buttons = []
            
            # Previous/Next buttons
            if total_favorites > 1:
                prev_index = index - 1 if index > 0 else total_favorites - 1
                next_index = index + 1 if index < total_favorites - 1 else 0
                
                nav_buttons.append([
                    InlineKeyboardButton("⬅️ Previous", callback_data=f"fav_nav_{prev_index}"),
                    InlineKeyboardButton("➡️ Next", callback_data=f"fav_nav_{next_index}")
                ])
            
            # Remove from favorites button
            nav_buttons.append([
                InlineKeyboardButton("💔 Remove from Favorites", callback_data=f"remove_fav_{video_id}")
            ])
            
            # Add share button
            share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            share_url = f"https://t.me/share/url?url={share_link}"
            nav_buttons.append([
                InlineKeyboardButton("🔗 Share this Video", url=share_url)
            ])
            
            # Add favorites button at bottom
            nav_buttons.append([
                InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites"),
            ])
            if is_admin(query.from_user.id):
                nav_buttons.append([InlineKeyboardButton("🗑️ Remove Media", callback_data=f"del_media_{video_id}")])
            
            keyboard = InlineKeyboardMarkup(nav_buttons)
            
            if edit_message:
                # For navigation, we edit the existing message
                media_kwargs = {
                    "context": context,
                    "chat_id": query.message.chat_id,
                    "caption": cap,
                    "reply_markup": keyboard,
                    "parse_mode": ParseMode.HTML,
                    "protect_content": should_protect_content(query.message.chat_id, query.from_user.id)
                }
                
                if item["type"] == "video":
                    media_kwargs["video"] = item["file_id"]
                elif item["type"] == "photo":
                    media_kwargs["photo"] = item["file_id"]
                elif item["type"] == "document":
                    media_kwargs["document"] = item["file_id"]
                elif item["type"] == "audio":
                    media_kwargs["audio"] = item["file_id"]
                elif item["type"] == "voice":
                    media_kwargs["voice"] = item["file_id"]
                elif item["type"] == "animation":
                    media_kwargs["animation"] = item["file_id"]
                elif item["type"] == "sticker":
                    media_kwargs["sticker"] = item["file_id"]
                    # Stickers don't support captions
                    media_kwargs.pop("caption", None)
                else:
                    # Fallback for unknown types
                    media_kwargs["text"] = f"Unsupported media type: {item['type']}"
                
                await safe_send_message(**media_kwargs)
            else:
                # For first time viewing, send new message
                media_kwargs = {
                    "context": context,
                    "chat_id": query.message.chat_id,
                    "caption": cap,
                    "reply_markup": keyboard,
                    "parse_mode": ParseMode.HTML,
                    "protect_content": should_protect_content(query.message.chat_id, query.from_user.id)
                }
                
                if item["type"] == "video":
                    media_kwargs["video"] = item["file_id"]
                elif item["type"] == "photo":
                    media_kwargs["photo"] = item["file_id"]
                elif item["type"] == "document":
                    media_kwargs["document"] = item["file_id"]
                elif item["type"] == "audio":
                    media_kwargs["audio"] = item["file_id"]
                elif item["type"] == "voice":
                    media_kwargs["voice"] = item["file_id"]
                elif item["type"] == "animation":
                    media_kwargs["animation"] = item["file_id"]
                elif item["type"] == "sticker":
                    media_kwargs["sticker"] = item["file_id"]
                    # Stickers don't support captions
                    media_kwargs.pop("caption", None)
                else:
                    # Fallback for unknown types
                    media_kwargs["text"] = f"Unsupported media type: {item['type']}"
                
                await safe_send_message(**media_kwargs)
        else:
            # Invalid video, remove it from favorites
            favorites_data["user_favorites"][user_id].remove(video_id)
            if video_id in favorites_data["video_likes"]:
                favorites_data["video_likes"][video_id] = max(0, favorites_data["video_likes"][video_id] - 1)
            save_favorites()
            
            # Show next video if available
            if favorites_data["user_favorites"][user_id]:
                await show_favorites_navigator(query, context, index, edit_message)
            else:
                if edit_message:
                    await query.edit_message_text("⭐ No more favorites available!")
                else:
                    await query.message.reply_text("⭐ No more favorites available!")
    
    except Exception as e:
        print(f"Error showing favorite: {e}")
        if edit_message:
            await query.edit_message_text("❌ Error loading favorite video.")
        else:
            await query.message.reply_text("❌ Error loading favorite video.")


async def add_to_favorites(query, context: ContextTypes.DEFAULT_TYPE):
    """Add a video to user's favorites"""
    user_id = str(query.from_user.id)
    video_id = query.data.replace("add_fav_", "")
    
    # Initialize user favorites if not exists
    if user_id not in favorites_data["user_favorites"]:
        favorites_data["user_favorites"][user_id] = []
    
    # Add to favorites if not already there
    if video_id not in favorites_data["user_favorites"][user_id]:
        favorites_data["user_favorites"][user_id].append(video_id)
        
        # Update video likes count
        if video_id not in favorites_data["video_likes"]:
            favorites_data["video_likes"][video_id] = 0
        favorites_data["video_likes"][video_id] += 1
        
        save_favorites()
        await query.answer("❤️ Added to favorites!")
        
        # Update the button (switch to remove option)
        new_fav_button = InlineKeyboardButton("💔 Remove from Favorites", 
                        callback_data=f"remove_fav_{video_id}")
        my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
        new_keyboard = InlineKeyboardMarkup([
            [new_fav_button],
            [my_favs_button, random_button]
        ])
        
        try:
            await query.edit_message_reply_markup(reply_markup=new_keyboard)
        except:
            pass  # Message might be too old to edit
    else:
        await query.answer("Already in favorites!")


async def remove_from_favorites(query, context: ContextTypes.DEFAULT_TYPE):
    """Remove a video from user's favorites"""
    user_id = str(query.from_user.id)
    video_id = query.data.replace("remove_fav_", "")
    
    if user_id in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id]:
        favorites_data["user_favorites"][user_id].remove(video_id)
        
        # Update video likes count
        if video_id in favorites_data["video_likes"]:
            favorites_data["video_likes"][video_id] = max(0, favorites_data["video_likes"][video_id] - 1)
        
        save_favorites()
        await query.answer("💔 Removed from favorites!")
        
        # Update the button (switch to add option)
        new_fav_button = InlineKeyboardButton("❤️ Add to Favorites", 
                        callback_data=f"add_fav_{video_id}")
        my_favs_button = InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")
        new_keyboard = InlineKeyboardMarkup([
            [new_fav_button],
            [my_favs_button, random_button]
        ])
        
        try:
            await query.edit_message_reply_markup(reply_markup=new_keyboard)
        except:
            pass  # Message might be too old to edit
    else:
        await query.answer("Not in favorites!")


async def show_who_liked_video(query, context: ContextTypes.DEFAULT_TYPE):
    """Admin function to show which users have liked a specific video"""
    # Check if user is admin
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ Admin only feature!")
        return
    
    # Extract video_id from callback data
    video_id = query.data.replace("who_liked_", "")
    
    # Find users who liked this video
    users_who_liked = []
    for user_id, user_favorites in favorites_data["user_favorites"].items():
        if video_id in user_favorites:
            try:
                # Try to get user info
                user = await context.bot.get_chat(user_id)
                display_name = user.first_name or user.username or f"User {user_id}"
                users_who_liked.append(f"👤 {display_name} (ID: {user_id})")
            except:
                # If we can't get user info, just show the ID
                users_who_liked.append(f"👤 User ID: {user_id}")
    
    # Get video statistics
    total_likes = favorites_data["video_likes"].get(video_id, 0)
    
    # Parse video info for display
    if "_" in video_id:
        tag, idx_str = video_id.rsplit("_", 1)
        try:
            idx = int(idx_str)
            video_info = f"📹 Video: <code>{tag}</code> | Index: <code>{idx}</code>"
        except ValueError:
            video_info = f"📹 Video: <code>{video_id}</code>"
    else:
        video_info = f"📹 Video: <code>{video_id}</code>"
    
    # Create response message
    if users_who_liked:
        user_list = "\n".join(users_who_liked)
        message = (
            f"👥 <b>Users who liked this video:</b>\n\n"
            f"{video_info}\n"
            f"❤️ Total likes: <b>{total_likes}</b>\n\n"
            f"{user_list}"
        )
    else:
        message = (
            f"💔 <b>No users have liked this video yet</b>\n\n"
            f"{video_info}\n"
            f"❤️ Total likes: <b>{total_likes}</b>"
        )
    
    await query.message.reply_html(message)
    await query.answer()


async def view_user_favorites(query, context: ContextTypes.DEFAULT_TYPE):
    """Show user's favorite videos using navigator"""
    await show_favorites_navigator(query, context, 0)


async def user_favorites_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to check user's favorites"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /userfavorites <user_id>")
        return
    
    try:
        target_user_id = context.args[0].strip()
        
        if target_user_id not in favorites_data["user_favorites"] or not favorites_data["user_favorites"][target_user_id]:
            await update.message.reply_text(f"❌ User {target_user_id} has no favorites.")
            return
        
        user_favs = favorites_data["user_favorites"][target_user_id]
        msg = f"<b>👤 User {target_user_id} Favorites ({len(user_favs)} videos):</b>\n\n"
        
        keyboard_buttons = []
        for i, video_id in enumerate(user_favs, 1):
            try:
                tag, idx = video_id.split('_', 1)
                msg += f"{i}. Tag: <code>{tag}</code> | Index: <code>{idx}</code>\n"
                
                # Create button for direct video access
                button_text = f"🎬 View #{i}: {tag}_{idx}"
                callback_data = f"view_video_{video_id}"
                keyboard_buttons.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
                
            except:
                msg += f"{i}. Invalid video ID: <code>{video_id}</code>\n"
        
        # Create inline keyboard with all video buttons
        if keyboard_buttons:
            reply_markup = InlineKeyboardMarkup(keyboard_buttons)
            await update.message.reply_html(msg, reply_markup=reply_markup)
        else:
            await update.message.reply_html(msg)
        
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Please provide a numeric user ID.")


async def video_stats_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to check how many users liked a specific video"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /videostats <tag> <index>")
        return
    
    tag = context.args[0].strip().lower()
    try:
        idx = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid index. Please provide a number.")
        return
    
    video_id = f"{tag}_{idx}"
    likes_count = favorites_data["video_likes"].get(video_id, 0)
    
    if tag not in media_data or idx >= len(media_data[tag]):
        await update.message.reply_text("❌ Video not found.")
        return
    
    # Find users who liked this video
    users_who_liked = []
    for user_id, user_favs in favorites_data["user_favorites"].items():
        if video_id in user_favs:
            users_who_liked.append(user_id)
    
    msg = f"<b>📊 Video Statistics</b>\n"
    msg += f"Tag: <code>{tag}</code>\n"
    msg += f"Index: <code>{idx}</code>\n"
    msg += f"❤️ Total Likes: <code>{likes_count}</code>\n\n"
    
    if users_who_liked:
        msg += f"<b>👥 Users who liked this video:</b>\n"
        for user_id in users_who_liked:
            msg += f"• <code>{user_id}</code>\n"
    else:
        msg += "No users have liked this video yet."
    
    # Add direct view button
    view_button = InlineKeyboardButton(f"🎬 View Video: {tag}_{idx}", 
                                     callback_data=f"view_video_{video_id}")
    keyboard = InlineKeyboardMarkup([[view_button]])
    
    await update.message.reply_html(msg, reply_markup=keyboard)


async def top_videos_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command to see most liked videos with navigation - available for all users"""
    if not favorites_data["video_likes"]:
        await update.message.reply_text("📊 No top videos available yet.", reply_markup=get_home_keyboard(update.effective_user.id))
        return
    
    # Show first top video directly
    await show_top_videos_viewer(update, context, 0)


async def top_videos_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command for users to see most liked videos with navigation"""
    if not favorites_data["video_likes"]:
        await update.message.reply_text("📊 No top videos available yet.", reply_markup=get_home_keyboard(update.effective_user.id))
        return
    
    # Show first top video directly
    await show_top_videos_viewer(update, context, 0)


async def show_top_videos_viewer(update, context, page=0, query=None):
    """Show top videos one by one like a viewer with navigation"""
    # Sort videos by likes count
    sorted_videos = sorted(favorites_data["video_likes"].items(), 
                          key=lambda x: x[1], reverse=True)
    
    total_videos = len(sorted_videos)
    
    if page >= total_videos:
        page = total_videos - 1
    if page < 0:
        page = 0
    
    if not sorted_videos:
        msg = "📊 No top videos available yet."
        if query:
            await query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return
    
    video_id, likes = sorted_videos[page]
    
    try:
        tag, idx_str = video_id.split('_', 1)
        idx = int(idx_str)
    except (ValueError, IndexError):
        # Skip invalid video and try next
        if page + 1 < total_videos:
            await show_top_videos_viewer(update, context, page + 1, query)
        else:
            msg = "📊 No valid top videos found."
            if query:
                await query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
        return
    
    # Check if video exists in database
    if tag not in media_data or not isinstance(media_data[tag], list) or not (0 <= idx < len(media_data[tag])):
        # Skip invalid video and try next
        if page + 1 < total_videos:
            await show_top_videos_viewer(update, context, page + 1, query)
        else:
            msg = "📊 No valid top videos found."
            if query:
                await query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
        return
    
    video_data = media_data[tag][idx]
    
    # Check if media is revoked
    if isinstance(video_data, dict) and video_data.get("revoked"):
        # Skip revoked video and try next
        if page + 1 < total_videos:
            await show_top_videos_viewer(update, context, page + 1, query)
        else:
            msg = "📊 No valid top videos found."
            if query:
                await query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
        return
    
    # Get user info for favorites
    chat_id = update.effective_chat.id if update else query.message.chat_id
    user_id_str = str(chat_id)
    is_favorited = user_id_str in favorites_data.get("user_favorites", {}) and video_id in favorites_data["user_favorites"].get(user_id_str, [])
    
    # Create caption with top video info
    share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
    share_url = f"https://t.me/share/url?url={share_link}"
    
    caption = f"🔥 <b>Top Video #{page + 1} of {total_videos} | ❤️ {likes} likes</b>\n\n"
    caption += f"📁 Tag: <code>{tag}</code> | Index: <code>{idx}</code> | <a href='{share_link}'>🔗 Link</a>"
    
    # Create navigation keyboard for top videos
    keyboard = []
    nav_buttons = []
    
    # Always show Previous/Next navigation (loop around)
    prev_page = page - 1 if page > 0 else total_videos - 1  # Loop to last if at first
    next_page = page + 1 if page < total_videos - 1 else 0  # Loop to first if at last
    
    nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"topvideo_view_{prev_page}"))
    nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"topvideo_view_{next_page}"))
    
    keyboard.append(nav_buttons)
    
    # Action buttons row
    action_buttons = []
    if is_favorited:
        action_buttons.append(InlineKeyboardButton("💔 Remove", callback_data=f"remove_fav_{video_id}"))
    else:
        action_buttons.append(InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{video_id}"))
    
    action_buttons.append(InlineKeyboardButton("🔗 Share", url=share_url))
    keyboard.append(action_buttons)
    
    # Additional buttons
    keyboard.append([InlineKeyboardButton("🔖 Bookmarks", callback_data="view_favorites")])
    keyboard.append([InlineKeyboardButton("❌ Close", callback_data="close_menu")])
    
    # Admin controls if admin
    if is_admin(chat_id):
        admin_buttons = build_admin_control_row(video_id)
        if admin_buttons:
            keyboard.extend(admin_buttons)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send the actual video/media
    # Send the media based on type
    if isinstance(video_data, dict):
        file_id = video_data.get("file_id")
        media_type = video_data.get("type", "video")
    else:
        file_id = video_data
        media_type = "video"

    try:
        if query:
            # For callback queries, edit the existing message (music player style)
            from telegram import InputMediaVideo, InputMediaPhoto, InputMediaDocument, InputMediaAnimation
            
            if media_type == "video":
                media = InputMediaVideo(media=file_id, caption=caption, parse_mode=ParseMode.HTML)
            elif media_type == "photo":
                media = InputMediaPhoto(media=file_id, caption=caption, parse_mode=ParseMode.HTML)
            elif media_type == "document":
                media = InputMediaDocument(media=file_id, caption=caption, parse_mode=ParseMode.HTML)
            elif media_type == "animation":
                media = InputMediaAnimation(media=file_id, caption=caption, parse_mode=ParseMode.HTML)
            else:
                # Fallback to video
                media = InputMediaVideo(media=file_id, caption=caption, parse_mode=ParseMode.HTML)
            
            # Edit the existing message with new media (keeps the same message window)
            await query.edit_message_media(
                media=media,
                reply_markup=reply_markup
            )
            
            # Track the message for auto-deletion
            await track_sent_message(query.message)
            
        else:
            # For new commands, send new message
            context_to_use = context
            chat_id_to_use = update.effective_chat.id
            
            if media_type == "video":
                sent_msg = await context_to_use.bot.send_video(
                    chat_id=chat_id_to_use,
                    video=file_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
            elif media_type == "photo":
                sent_msg = await context_to_use.bot.send_photo(
                    chat_id=chat_id_to_use,
                    photo=file_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
            elif media_type == "document":
                sent_msg = await context_to_use.bot.send_document(
                    chat_id=chat_id_to_use,
                    document=file_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
            elif media_type == "animation":
                sent_msg = await context_to_use.bot.send_animation(
                    chat_id=chat_id_to_use,
                    animation=file_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
            else:
                # Fallback to video
                sent_msg = await context_to_use.bot.send_video(
                    chat_id=chat_id_to_use,
                    video=file_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
            
            # Track the message for auto-deletion
            await track_sent_message(sent_msg)
        
    except Exception as e:
        error_msg = f"❌ Could not send top video #{page + 1}. Error: {str(e)}"
        if query:
            await context_to_use.bot.send_message(chat_id=chat_id_to_use, text=error_msg)
        else:
            await update.message.reply_text(error_msg)


async def show_top_videos_page(update, context, page=0, query=None):
    """Show a page of top videos with navigation buttons"""
    items_per_page = 10
    
    # Sort videos by likes count
    sorted_videos = sorted(favorites_data["video_likes"].items(), 
                          key=lambda x: x[1], reverse=True)
    
    total_videos = len(sorted_videos)
    total_pages = (total_videos + items_per_page - 1) // items_per_page
    
    if page >= total_pages:
        page = total_pages - 1
    if page < 0:
        page = 0
    
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_videos = sorted_videos[start_idx:end_idx]
    
    # Build message
    msg = f"<b>📊 Top Liked Videos (Page {page + 1}/{total_pages})</b>\n\n"
    
    for i, (video_id, likes) in enumerate(page_videos, start_idx + 1):
        try:
            tag, idx = video_id.split('_', 1)
            # Create direct link to the specific video
            file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
            msg += f"{i}. Tag: <code>{tag}</code> | Index: <code>{idx}</code> | ❤️ {likes} likes | <a href='{file_link}'>📺 View</a>\n"
            
        except:
            msg += f"{i}. Invalid video ID: <code>{video_id}</code> | ❤️ {likes} likes\n"
    
    # Create navigation buttons
    keyboard = []
    nav_buttons = []
    
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"topvideos_page_{page-1}"))
    
    nav_buttons.append(InlineKeyboardButton(f"📊 {page + 1}/{total_pages}", callback_data="topvideos_info"))
    
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"topvideos_page_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Add refresh and close buttons
    keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data="topvideos_page_0")])
    keyboard.append([InlineKeyboardButton("❌ Close", callback_data="close_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send or edit message
    if query:
        await query.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    else:
        await update.message.reply_html(msg, reply_markup=reply_markup)


async def favorites_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command for users to view their favorites using navigator"""
    user_id = str(update.effective_user.id)
    
    if user_id not in favorites_data["user_favorites"] or not favorites_data["user_favorites"][user_id]:
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📚 Browse Library", callback_data="next_safe"),
                InlineKeyboardButton("🔀 Random Pick", callback_data="next_adult")
            ]
        ])
        # Since this is a command, we just reply to the user
        await update.message.reply_text(
            "🔖 You haven't bookmarked any resources yet!",
            reply_markup=keyboard
        )
        return
    
    # Create a fake query object to use with the navigator
    class FakeQuery:
        def __init__(self, message, user):
            self.message = message
            self.from_user = user
    
    fake_query = FakeQuery(update.message, update.effective_user)
    await show_favorites_navigator(fake_query, context, 0)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command for users"""
    user_id = update.effective_user.id
    
    # Basic user help
    help_text = (
        "🤖 <b>Knowledge Vault Bot - Help Guide</b>\n\n"
        "🔖 <b>User Commands:</b>\n"
        "• <b>/start</b> - Open the main library interface\n"
        "• <b>/favorites</b> - View your bookmarked resources\n"
        "• <b>/help</b> - Show this help guide\n\n"
        
        "📚 <b>How to Use:</b>\n"
        "• Click 📚 <b>Browse Library</b> to explore all resources\n"
        "• Click 🔀 <b>Random Pick</b> for a curated discovery\n"
        "• Click ❤️ on any resource to bookmark it\n"
        "• Use 🔖 <b>Bookmarks</b> for your saved items\n"
        "• Navigate with ⬅️ <b>Previous</b> and ➡️ <b>Next</b> buttons\n\n"
        
        "⚡ <b>Quick Shortcuts:</b>\n"
        "Type these words for instant access:\n"
        "• <code>fav, favorites, 🔖, bookmark</code> → Open bookmarks\n"
        "• <code>random, rand, 🔀, pick</code> → Random resource\n\n"
        
        "🎯 <b>Smart Features:</b>\n"
        "• <b>Two Modes:</b> Browse Library shows all content; Random Pick shows curated entries\n"
        "• <b>Rotation Engine:</b> Ensures breadth before repeats\n"
        "• <b>Persistent Bookmarks:</b> Your saves never expire\n"
        "• <b>Navigation Memory:</b> Remembers your position\n"
        "• <b>Real-time Feedback:</b> Instant responses to actions\n\n"
        
        "💡 <b>Pro Tips:</b>\n"
        "• Bookmark frequently used references\n"
        "• Random Pick is great for discovery sessions\n"
        "• Share deep links to individual resources\n"
        "• All interactions are private and secure"
    )
    
    # Add admin commands if user is admin
    if user_id == ADMIN_ID:
        admin_help = [
            "\n\n🔧 <b>Admin Commands:</b>\n"
            "• <b>/upload</b> - Upload new media to database\n"
            "• <b>/listvideo</b> - List special tags (wildwed, freakyfri, etc.)\n"
            "• <b>/listvideos</b> - List all other tags\n"
            "• <b>/remove &lt;tag&gt; &lt;index&gt;</b> - Remove specific video\n"
            "• <b>/get &lt;tag&gt;</b> - Get all videos from a tag\n"
            "• <b>/generatelink &lt;tag&gt;</b> - Generate public link for tag\n"
            "• <b>/view &lt;tag&gt; &lt;index&gt;</b> - View specific video by index\n"
            "• <b>/pass &lt;tag&gt; [start] [end]</b> - Add resources to Random Pick pool ONLY\n"
            "• <b>/passlink &lt;tag&gt; [start] [end]</b> - Create shareable links ONLY (not for Random Pick)\n"
            "• <b>/revoke &lt;tag&gt; [start] [end]</b> - Remove from Random Pick pool ONLY\n"
            "• <b>/revokelink &lt;tag&gt; [start] [end]</b> - Revoke shareable link access ONLY\n"
            "• <b>/activelinks</b> - List active entries in Random Pick pool\n"
            "• <b>/passlinks</b> - List shareable links (independent storage)\n"
            "• <b>/listactive</b> - List all active links (both types)\n"
            "• <b>/free &lt;tag&gt;</b> - Make tag freely accessible\n"
            "• <b>/listfree</b> - List all free tags\n\n"
            "🛡️ <b>Protection Commands:</b>\n"
            "• <b>/protectionon</b> - Enable media protection (prevent saving)\n"
            "• <b>/protectionoff</b> - Disable media protection (allow saving)\n"
            "• <b>/protection</b> - Check current protection status\n\n"
            "• <b>/listremoved</b> - List deleted & revoked media (with restore buttons)\n"
            "• <b>/restoremedia &lt;tag&gt; &lt;index&gt;</b> - Restore a deleted/revoked media at index\n\n"
            
            "🧹 <b>Auto-Delete System:</b>\n"
            f"• <b>/autodelete on</b> - Enable auto-deletion after {AUTO_DELETE_HOURS} hour(s)\n"
            "• <b>/autodelete off</b> - Disable auto-deletion\n"
            "• <b>/autodelete hours &lt;hours&gt;</b> - Set deletion time (0.1-168 hours)\n"
            "• <b>/autodelete status</b> - Check current settings\n"
            "• <b>/notifications on/off</b> - Control deletion notifications\n"
            "• <b>/autodelete stats</b> - View deletion statistics\n"
            "• <b>/autodelete clear</b> - Clear all tracked messages\n\n"
            
            "📊 <b>Analytics Commands:</b>\n"
            "• <b>/userfavorites &lt;user_id&gt;</b> - View user's favorites\n"
            "• <b>/videostats &lt;tag&gt; &lt;index&gt;</b> - See who liked a video\n"
            "• <b>/topvideos [limit]</b> - Most liked videos with direct access\n\n"
            
            "🔀 <b>Command Examples:</b>\n"
            "• <code>/pass bulk</code> (add entire bulk tag to Random Pick pool only)\n"
            "• <code>/pass bulk 0 49</code> (add videos 0-49 to Random Pick pool only)\n"
            "• <code>/passlink bulk</code> (create shareable link for entire tag only)\n"
            "• <code>/passlink bulk 0 49</code> (create shareable link for range only)\n"
            "• <code>/revoke bulk</code> (remove entire bulk from Random Pick pool only)\n"
            "• <code>/revoke bulk 0 49</code> (remove range from Random Pick pool only)\n"
            "• <code>/revokelink bulk</code> (disable shareable link only)\n"
            "• <code>/revokelink bulk 0 49</code> (disable specific range link only)\n\n"
            
            "⚠️ <b>Important:</b> Curation systems are separate!\n"
            "• Use <code>/pass</code> to feed the Random Pick feature\n"
            "• Use <code>/passlink</code> for shareable links only\n"
            "• Each system works independently\n\n"
            "♻️ <b>Removal System:</b> Use inline Revoke / Delete buttons on any media. Revoke skips without deleting. Delete is soft (undo with Restore button, /restoremedia, or /listremoved).\n\n"
            
            "👥 <b>Special Features:</b>\n"
            "• <b>WHO Button:</b> Click on any video to see who liked it\n"
            "• <b>Direct Video Access:</b> Click video links in analytics\n"
            "• <b>Per-Video Security:</b> Pass specific videos, not entire tags\n"
            "• <b>Analytics Integration:</b> Real-time stats with interactive buttons"
        ]
        help_text += admin_help
    
    await update.message.reply_html(help_text)


# ================== BROADCASTING SYSTEM ==================

async def get_all_users():
    """Get all users who have interacted with the bot"""
    users = set()
    
    # Add all tracked users (main source)
    users.update(users_data.keys())
    
    # Add users from favorites (fallback)
    if favorites_data and "user_favorites" in favorites_data:
        users.update(favorites_data["user_favorites"].keys())
    
    # Add exempted users (fallback)
    users.update(str(uid) for uid in exempted_users)
    
    return [int(uid) for uid in users if uid.isdigit()]


async def discover_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to discover and migrate users from all possible sources"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    current_time = asyncio.get_event_loop().time()
    initial_count = len(users_data)
    
    # Get all unique user IDs from various sources
    discovered_users = set()
    
    # From favorites (already done in migration, but double-check)
    if favorites_data and "user_favorites" in favorites_data:
        discovered_users.update(favorites_data["user_favorites"].keys())
    
    # From exempted users
    discovered_users.update(str(uid) for uid in exempted_users)
    
    # Add any missing users to the database
    for user_id_str in discovered_users:
        if user_id_str not in users_data and user_id_str.isdigit():
            users_data[user_id_str] = {
                "first_seen": current_time,
                "last_seen": current_time,
                "username": None,
                "first_name": None,
                "interaction_count": 1
            }
    
    save_users()
    new_count = len(users_data)
    added = new_count - initial_count
    
    await update.message.reply_text(
        f"🔍 <b>User Discovery Complete</b>\n\n"
        f"📊 Users before: {initial_count}\n"
        f"➕ Users discovered: {added}\n"
        f"📊 Total users now: {new_count}\n\n"
        f"💡 <b>Next Steps:</b>\n"
        f"• Use /addusers to manually add known user IDs\n"
        f"• Users will be automatically tracked from now on\n"
        f"• Check /bstats for current user count",
        parse_mode=ParseMode.HTML
    )


async def add_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to manually add user IDs to the database"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not context.args:
        await update.message.reply_text("❌ Usage: /addusers <user_id1> <user_id2> <user_id3> ...")
        return
    
    added_count = 0
    current_time = asyncio.get_event_loop().time()
    
    for user_id_str in context.args:
        try:
            user_id = int(user_id_str)
            user_id_str = str(user_id)
            
            if user_id_str not in users_data:
                users_data[user_id_str] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "username": None,
                    "first_name": None,
                    "interaction_count": 1
                }
                added_count += 1
        except ValueError:
            continue
    
    save_users()
    await update.message.reply_text(f"✅ Added {added_count} users to the database.\n📊 Total users now: {len(users_data)}")


async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                          delete_after=None, pin_message=False, silent=False):
    """Core broadcasting function"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return

    if not context.args:
        await update.message.reply_text("❌ Please provide a message to broadcast.")
        return

    message_text = " ".join(context.args)
    users = await get_all_users()
    
    if not users:
        await update.message.reply_text("❌ No users found to broadcast to.")
        return

    success_count = 0
    failed_count = 0
    pinned_count = 0
    deleted_messages = []

    status_msg = await update.message.reply_text(
        f"📡 Broadcasting to {len(users)} users...\n"
        f"✅ Sent: {success_count}\n"
        f"❌ Failed: {failed_count}"
    )

    for user_id in users:
        try:
            sent_message = await context.bot.send_message(
                chat_id=user_id,
                text=message_text,
                parse_mode=ParseMode.HTML,
                disable_notification=silent
            )
            
            success_count += 1
            
            # Pin message if requested
            if pin_message:
                try:
                    await context.bot.pin_chat_message(
                        chat_id=user_id,
                        message_id=sent_message.message_id
                    )
                    pinned_count += 1
                except:
                    pass  # Ignore pin failures
            
            # Store message for deletion if requested
            if delete_after:
                deleted_messages.append((user_id, sent_message.message_id))
                
        except Exception as e:
            failed_count += 1
            continue

        # Update status every 10 messages
        if (success_count + failed_count) % 10 == 0:
            try:
                await status_msg.edit_text(
                    f"📡 Broadcasting to {len(users)} users...\n"
                    f"✅ Sent: {success_count}\n"
                    f"❌ Failed: {failed_count}"
                )
            except:
                pass

    # Final status update
    final_text = (
        f"📡 <b>Broadcast Complete!</b>\n\n"
        f"👥 Total users: {len(users)}\n"
        f"✅ Successfully sent: {success_count}\n"
        f"❌ Failed: {failed_count}"
    )
    
    if pin_message:
        final_text += f"\n📌 Pinned: {pinned_count}"
    
    if delete_after:
        final_text += f"\n🗑 Will auto-delete in {delete_after} seconds"
        
    await status_msg.edit_text(final_text, parse_mode=ParseMode.HTML)

    # Schedule deletion if requested
    if delete_after and deleted_messages:
        await asyncio.sleep(delete_after)
        deleted_count = 0
        
        for user_id, message_id in deleted_messages:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=message_id)
                deleted_count += 1
            except:
                continue
        
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"🗑 Auto-deleted {deleted_count} broadcast messages."
        )


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Normal broadcast message"""
    await broadcast_message(update, context)


async def dbroadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Auto-deleting broadcast message"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ Usage: /dbroadcast <seconds> <message>\n"
            "Example: /dbroadcast 60 This message will delete in 60 seconds"
        )
        return

    try:
        delete_after = int(context.args[0])
        message_text = " ".join(context.args[1:])
        context.args = message_text.split()  # Update args for broadcast_message
        await broadcast_message(update, context, delete_after=delete_after)
    except ValueError:
        await update.message.reply_text("❌ First argument must be the number of seconds.")


async def pbroadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pin broadcast message"""
    await broadcast_message(update, context, pin_message=True)


async def sbroadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Silent broadcast (no notification)"""
    await broadcast_message(update, context, silent=True)


async def fbroadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Forward broadcast from a channel/chat"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "❌ Please reply to a message to forward it as broadcast.\n"
            "Usage: Reply to any message and use /fbroadcast"
        )
        return

    users = await get_all_users()
    
    if not users:
        await update.message.reply_text("❌ No users found to broadcast to.")
        return

    success_count = 0
    failed_count = 0
    
    status_msg = await update.message.reply_text(
        f"📡 Forward broadcasting to {len(users)} users...\n"
        f"✅ Sent: {success_count}\n"
        f"❌ Failed: {failed_count}"
    )

    for user_id in users:
        try:
            await update.message.reply_to_message.forward(chat_id=user_id)
            success_count += 1
        except:
            failed_count += 1
            continue

        # Update status every 10 messages
        if (success_count + failed_count) % 10 == 0:
            try:
                await status_msg.edit_text(
                    f"📡 Forward broadcasting to {len(users)} users...\n"
                    f"✅ Sent: {success_count}\n"
                    f"❌ Failed: {failed_count}"
                )
            except:
                pass

    # Final status
    await status_msg.edit_text(
        f"📡 <b>Forward Broadcast Complete!</b>\n\n"
        f"👥 Total users: {len(users)}\n"
        f"✅ Successfully sent: {success_count}\n"
        f"❌ Failed: {failed_count}",
        parse_mode=ParseMode.HTML
    )


async def broadcast_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show broadcast statistics"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return

    users = await get_all_users()
    
    stats_text = [
        f"📊 <b>Broadcast Statistics</b>\n\n"
        f"👥 Total users in database: {len(users)}\n"
        f"❤️ Users with favorites: {len(favorites_data.get('user_favorites', {}))}\n"
        f"🆓 Exempted users: {len(exempted_users)}\n\n"
        f"📡 <b>Available Commands:</b>\n"
        f"• <code>/broadcast &lt;message&gt;</code> - Normal broadcast\n"
        f"• <code>/dbroadcast &lt;seconds&gt; &lt;message&gt;</code> - Auto-deleting broadcast\n"
        f"• <code>/pbroadcast &lt;message&gt;</code> - Pin broadcast\n"
        f"• <code>/sbroadcast &lt;message&gt;</code> - Silent broadcast\n"
        f"• <code>/fbroadcast</code> - Forward broadcast (reply to message)\n"
        f"• <code>/bstats</code> - Show these statistics"
    ]

    await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)


async def top_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to show most active users with navigation"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not users_data:
        await update.message.reply_text("📊 No users in database yet.")
        return
    
    # Send the first page
    await show_top_users_page(update, context, 0)


async def show_top_users_page(update, context, page=0, query=None):
    """Show a page of top users with navigation buttons"""
    items_per_page = 10
    
    # Filter out admin and sort users by interaction count (descending)
    filtered_users = {k: v for k, v in users_data.items() if int(k) != ADMIN_ID}
    sorted_users = sorted(
        filtered_users.items(), 
        key=lambda x: x[1].get('interaction_count', 0), 
        reverse=True
    )
    
    total_users = len(sorted_users)
    total_pages = (total_users + items_per_page - 1) // items_per_page
    
    if page >= total_pages:
        page = total_pages - 1
    if page < 0:
        page = 0
    
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_users = sorted_users[start_idx:end_idx]
    
    # Build top users message
    stats_text = f"🔥 <b>Most Active Users (Page {page + 1}/{total_pages})</b>\n\n"
    
    for i, (user_id, user_data) in enumerate(page_users, start_idx + 1):
        first_name = user_data.get('first_name', 'Unknown')
        username = user_data.get('username', '')
        interactions = user_data.get('interaction_count', 0)
        
        # Add emoji for top positions
        if i == 1:
            emoji = "🥇"
        elif i == 2:
            emoji = "🥈"
        elif i == 3:
            emoji = "🥉"
        else:
            emoji = f"{i}."
        
        # Format username display
        if username:
            name_display = f"{first_name} (@{username})"
        else:
            name_display = first_name or f"User {user_id}"
        
        # Add interaction count with appropriate emoji
        if interactions >= 100:
            interaction_emoji = "🚀"
        elif interactions >= 50:
            interaction_emoji = "🔥"
        elif interactions >= 10:
            interaction_emoji = "⭐"
        elif interactions >= 5:
            interaction_emoji = "✨"
        else:
            interaction_emoji = "👤"
        
        stats_text += f"{emoji} {name_display} - {interactions} interactions {interaction_emoji}\n"
    
    # Add summary only on first page
    if page == 0:
        total_all_users = len(filtered_users)
        total_interactions = sum(user.get('interaction_count', 0) for user in filtered_users.values())
        
        stats_text += f"\n📊 <b>Summary:</b>\n"
        stats_text += f"👥 Total Users: {total_all_users}\n"
        stats_text += f"💬 Total Interactions: {total_interactions}\n"
        if total_all_users > 0:
            stats_text += f"📈 Average per User: {total_interactions/total_all_users:.1f}"
        else:
            stats_text += f"📈 Average per User: 0.0"
    
    # Create navigation buttons
    keyboard = []
    nav_buttons = []
    
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"topusers_page_{page-1}"))
    
    nav_buttons.append(InlineKeyboardButton(f"👥 {page + 1}/{total_pages}", callback_data="topusers_info"))
    
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"topusers_page_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Add refresh button
    keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data="topusers_page_0")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send or edit message
    if query:
        await query.edit_message_text(stats_text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    else:
        await update.message.reply_html(stats_text, reply_markup=reply_markup)


async def user_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to check user registration statistics"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Count users by source
    total_users = len(users_data)
    source_counts = {}
    
    for user_data in users_data.values():
        source = user_data.get("source", "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1
    
    # Create stats message
    stats_text = (
        f"📊 <b>User Registration Statistics</b>\n\n"
        f"👥 Total Registered Users: {total_users}\n\n"
        f"📈 <b>Users by Registration Source:</b>\n"
    )
    
    for source, count in sorted(source_counts.items()):
        emoji = {
            "start_command": "🏠",
            "button_click": "🔘",
            "text_message": "💬", 
            "media_upload": "📸",
            "bot_interaction": "🤖",
            "favorites_migration": "❤️",
            "exempted_migration": "🆓",
            "manual_addition": "✋"
        }.get(source, "❓")
        
        stats_text += f"{emoji} {source.replace('_', ' ').title()}: {count}\n"
    
    stats_text += f"\n💡 All users are automatically registered when they interact with the bot!"
    
    await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)


async def userinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get detailed information about a specific user"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Check if user ID is provided
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b> <code>/userinfo &lt;user_id&gt;</code>\n\n"
            "💡 <i>Example:</i> <code>/userinfo 123456789</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        user_id = int(context.args[0])
        user_id_str = str(user_id)
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Please provide a numeric user ID.")
        return
    
    # Check if user exists in database
    if user_id_str not in users_data:
        await update.message.reply_text(f"❌ User with ID <code>{user_id}</code> not found in database.", parse_mode=ParseMode.HTML)
        return
    
    # Get user data
    user_info = users_data[user_id_str]
    
    # Format timestamps
    import datetime
    def format_timestamp(timestamp):
        if timestamp:
            try:
                dt = datetime.datetime.fromtimestamp(timestamp)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                return "Invalid timestamp"
        return "Not available"
    
    # Get user's favorites count
    favorites_count = 0
    try:
        if user_id_str in favorites_data.get("user_favorites", {}):
            favorites_count = len(favorites_data["user_favorites"][user_id_str])
    except:
        pass
    
    # Get user's preferences
    user_prefs = user_preferences.get(user_id, {})
    auto_delete_notif = user_prefs.get("auto_delete_notifications", True)
    
    # Try to get additional info from Telegram
    try:
        chat_info = await context.bot.get_chat(user_id)
        telegram_username = f"@{chat_info.username}" if chat_info.username else "No username"
        telegram_first_name = chat_info.first_name or "No first name"
        telegram_last_name = chat_info.last_name or ""
        telegram_bio = getattr(chat_info, 'bio', 'No bio') or "No bio"
        is_premium = getattr(chat_info, 'is_premium', False)
    except Exception as e:
        telegram_username = "Unable to fetch"
        telegram_first_name = "Unable to fetch"
        telegram_last_name = ""
        telegram_bio = "Unable to fetch"
        is_premium = False
    
    # Build detailed info message
    info_text = (
        f"👤 <b>User Information</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
        f"👤 <b>Username:</b> {telegram_username}\n"
        f"📝 <b>First Name:</b> {telegram_first_name}\n"
        f"📝 <b>Last Name:</b> {telegram_last_name}\n"
        f"📖 <b>Bio:</b> {telegram_bio}\n"
        f"⭐ <b>Premium:</b> {'Yes' if is_premium else 'No'}\n\n"
        
        f"📊 <b>Bot Statistics</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 <b>First Seen:</b> {format_timestamp(user_info.get('first_seen'))}\n"
        f"🕐 <b>Last Seen:</b> {format_timestamp(user_info.get('last_seen'))}\n"
        f"🔢 <b>Interactions:</b> {user_info.get('interaction_count', 0)}\n"
        f"📂 <b>Registration Source:</b> {user_info.get('source', 'Unknown').replace('_', ' ').title()}\n"
        f"❤️ <b>Favorites:</b> {favorites_count} videos\n\n"
        
        f"⚙️ <b>Settings</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔔 <b>Auto-delete Notifications:</b> {'Enabled' if auto_delete_notif else 'Disabled'}\n\n"
        
        f"💡 <i>This shows all available information about the user.</i>"
    )
    
    await update.message.reply_text(info_text, parse_mode=ParseMode.HTML)


# =================== END BROADCASTING SYSTEM ===================


# =================== BATCH PROCESSING SYSTEM ===================

# =================== PROTECTION COMMANDS ===================

# Protection commands for enabling and disabling content protection
async def pon_cmd(message, args):
    """Enable protection settings by writing to protection_settings.json."""
    try:
        with open('protection_settings.json', 'w') as f:
            json.dump({'enabled': True}, f)
        # TODO: Send confirmation message, e.g., message.reply('Protection enabled.')
    except Exception as e:
        # TODO: Log error e
        pass

async def poff_cmd(message, args):
    """Disable protection settings by writing to protection_settings.json."""
    try:
        with open('protection_settings.json', 'w') as f:
            json.dump({'enabled': False}, f)
        # TODO: Send confirmation message, e.g., message.reply('Protection disabled.')
    except Exception as e:
        # TODO: Log error e
        pass

async def protection_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Turn on media protection (prevent saving/downloading)"""
    try:
        user_id = update.effective_user.id
        print(f"Protection ON command from user {user_id}")
        if not is_admin(user_id):
            print(f"User {user_id} is not admin")
            await update.message.reply_text("❌ Admin access required.")
            return
        
        global protection_enabled
        protection_enabled = True
        save_protection_settings()
        print(f"Protection enabled: {protection_enabled}")
        
        await update.message.reply_text(
            "🛡️ <b>Protection ENABLED</b>\n\n"
            "✅ Media content is now protected from saving/downloading",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        print(f"Error in protection_on_command: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def protection_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Turn off media protection (allow saving/downloading)"""
    try:
        user_id = update.effective_user.id
        print(f"Protection OFF command from user {user_id}")
        if not is_admin(user_id):
            print(f"User {user_id} is not admin")
            await update.message.reply_text("❌ Admin access required.")
            return
        
        global protection_enabled
        protection_enabled = False
        save_protection_settings()
        print(f"Protection enabled: {protection_enabled}")
        
        await update.message.reply_text(
            "🔓 <b>Protection DISABLED</b>\n\n"
            "⚠️ Media content can now be saved/downloaded by users",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        print(f"Error in protection_off_command: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def protection_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check comprehensive protection status or toggle protection on/off"""
    global protection_enabled
    
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Handle on/off arguments
    if context.args:
        command = context.args[0].lower()
        if command == "on":
            protection_enabled = True
            save_protection_settings()
            await update.message.reply_text("🛡️ <b>Media Protection ENABLED</b>\n\n📊 All media content is now protected from saving/downloading", parse_mode=ParseMode.HTML)
            return
        elif command == "off":
            protection_enabled = False
            save_protection_settings()
            await update.message.reply_text("🔓 <b>Media Protection DISABLED</b>\n\n⚠️ Users can now save/download media content", parse_mode=ParseMode.HTML)
            return
    
    # Show comprehensive status if no arguments
    # Media Protection Status
    protection_icon = "🛡️" if protection_enabled else "🔓"
    protection_text = "ENABLED" if protection_enabled else "DISABLED"
    protection_desc = "protected from saving" if protection_enabled else "can be saved/downloaded"
    
    # Auto-Deletion Status
    auto_delete_icon = "🧹" if AUTO_DELETE_ENABLED else "⏸️"
    auto_delete_text = "ENABLED" if AUTO_DELETE_ENABLED else "DISABLED"
    
    # Notification Status
    notification_icon = "🔕" if not AUTO_DELETE_NOTIFICATIONS else "🔔"
    notification_text = "DISABLED" if not AUTO_DELETE_NOTIFICATIONS else "ENABLED"
    
    # Admin Protection
    admin_count = len(admin_list)
    
    # Channel Protection
    required_channels = len(REQUIRED_CHANNELS)
    
    # Current Statistics
    tracked_messages = len(sent_messages_tracker)
    total_users = len(users_data)
    total_media = sum(len(videos) for videos in media_data.values())
    exempted_count = len(exempted_users)
    
    comprehensive_status = (
        f"🛡️ <b>COMPREHENSIVE PROTECTION STATUS</b>\n\n"
        
        f"📱 <b>Media Protection:</b>\n"
        f"{protection_icon} Status: <b>{protection_text}</b>\n"
        f"📊 Content is currently {protection_desc}\n\n"
        
        f"🧹 <b>Auto-Deletion System:</b>\n"
        f"{auto_delete_icon} Status: <b>{auto_delete_text}</b>\n"
        f"⏰ Timer: <b>{AUTO_DELETE_HOURS} hour(s)</b>\n"
        f"📊 Tracked Messages: <b>{tracked_messages}</b>\n"
        f"⏳ Cleanup Interval: <b>5 minutes</b>\n\n"
        
        f"🔔 <b>Notifications:</b>\n"
        f"{notification_icon} Auto-Delete Alerts: <b>{notification_text}</b>\n"
        f"⏰ Cooldown Period: <b>{NOTIFICATION_COOLDOWN_HOURS} hour(s)</b>\n\n"
        
        f"👥 <b>Access Control:</b>\n"
        f"🔑 Admin Count: <b>{admin_count} users</b>\n"
        f"🚫 Exempted Users: <b>{exempted_count} users</b>\n"
        f"📢 Required Channels: <b>{required_channels} channels</b>\n\n"
        
        f"📊 <b>Database Stats:</b>\n"
        f"👤 Total Users: <b>{total_users}</b>\n"
        f"🎬 Total Media: <b>{total_media} files</b>\n"
        f"📂 Media Categories: <b>{len(media_data)} tags</b>\n\n"
        
        f"⚙️ <b>Security Features:</b>\n"
        f"✅ Channel membership verification\n"
        f"✅ Admin-only management commands\n"
        f"✅ Rate limiting protection\n"
        f"✅ Automatic user tracking\n"
        f"✅ Media revocation system\n"
        f"✅ Favorites protection\n\n"
        
        f"🔧 <b>Quick Actions:</b>\n"
        f"• <code>/protection on</code> - Enable media protection\n"
        f"• <code>/protection off</code> - Disable media protection\n"
        f"• <code>/autodelete</code> - Auto-deletion controls\n"
        f"• <code>/admin</code> - Admin management panel"
    )
    
    await update.message.reply_text(
        comprehensive_status,
        parse_mode=ParseMode.HTML
    )


async def test_protection_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a test media to verify protection is working"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Find any media from the database to test with
    test_media = None
    test_tag = None
    test_index = None
    
    for tag, media_list in media_data.items():
        if media_list and isinstance(media_list, list):
            for idx, item in enumerate(media_list):
                if isinstance(item, dict) and not item.get("deleted") and not item.get("revoked"):
                    test_media = item
                    test_tag = tag
                    test_index = idx
                    break
            if test_media:
                break
    
    if not test_media:
        await update.message.reply_text("❌ No media found in database to test with.")
        return
    
    status_icon = "🛡️" if protection_enabled else "🔓"
    status_text = "ENABLED" if protection_enabled else "DISABLED"
    
    caption = (f"🧪 <b>Protection Test</b>\n\n"
               f"{status_icon} Current Status: <b>{status_text}</b>\n"
               f"📁 Test Media: <code>{test_tag}_{test_index}</code>\n\n"
               f"💡 Try to save/download this media to test protection!")
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("❤️ Add", callback_data=f"add_fav_{test_tag}_{test_index}")],
        [InlineKeyboardButton("🔀 Random Pick", callback_data="random_media")]
    ])
    
    await send_media_by_type(
        context=context,
        chat_id=update.effective_chat.id,
        item=test_media,
        caption=caption,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        protect_content=should_protect_content(update.effective_user.id, update.effective_chat.id)
    )


async def test_deletion_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test the new deletion and restoration system"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    global deleted_media_storage
    
    # Show current deletion status
    deleted_count = len(deleted_media_storage)
    total_media = sum(len(videos) for videos in media_data.values() if isinstance(videos, list))
    
    status_text = (
        f"🧪 <b>Deletion System Test</b>\n\n"
        f"📊 <b>Current Status:</b>\n"
        f"• Active Media: {total_media}\n"
        f"• Deleted Media: {deleted_count}\n\n"
        f"💡 <b>How it works:</b>\n"
        f"• 🗑️ Delete: Removes & reorders (no gaps)\n"
        f"• ♻️ Restore: Puts back at original position\n"
        f"• No more 'Error sending file at index X'\n\n"
    )
    
    if deleted_count > 0:
        status_text += f"🗃️ <b>Deleted Items:</b>\n"
        for video_key, info in list(deleted_media_storage.items())[:5]:  # Show first 5
            tag = info['tag']
            pos = info['original_position']
            status_text += f"• <code>{video_key}</code> (was at {tag}[{pos}])\n"
        if deleted_count > 5:
            status_text += f"• ... and {deleted_count - 5} more\n"
    
    await update.message.reply_text(status_text, parse_mode=ParseMode.HTML)


async def check_protection_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check protection status and database statistics"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Count media in database
    total_media = 0
    protected_sends = 0
    unprotected_sends = 0
    
    for tag, media_list in media_data.items():
        if isinstance(media_list, list):
            for item in media_list:
                if not (isinstance(item, dict) and (item.get("deleted") or item.get("revoked"))):
                    total_media += 1
    
    # Simulate sending to count what would be protected vs unprotected
    current_protection = protection_enabled
    if current_protection:
        protected_sends = total_media
        unprotected_sends = 0
    else:
        protected_sends = 0
        unprotected_sends = total_media
    
    status_icon = "🛡️" if protection_enabled else "🔓"
    status_text = "ENABLED" if protection_enabled else "DISABLED"
    
    await update.message.reply_text(
        f"🔍 <b>Protection Analysis</b>\n\n"
        f"{status_icon} <b>Current Status: {status_text}</b>\n\n"
        f"📊 <b>Database Statistics:</b>\n"
        f"• Total Active Media: <code>{total_media}</code>\n"
        f"• Would be Protected: <code>{protected_sends}</code>\n"
        f"• Would be Unprotected: <code>{unprotected_sends}</code>\n\n"
        f"📱 <b>Protection Coverage:</b>\n"
        f"• All media requests use: <code>should_protect_content()</code>\n"
        f"• Setting applies to: <b>ALL database media</b>\n"
        f"• Includes: Random, Favorites, Links, View commands\n\n"
        f"💡 Use /testprotection to verify with actual media",
        parse_mode=ParseMode.HTML
    )


# =================== CUSTOM BATCH SYSTEM ===================

async def custom_batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to start custom batch collection"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text(
            "Usage: /custom_batch <tag_name>\n\n"
            "Example: /custom_batch red"
        )
        return
    
    tag = context.args[0].strip().lower()
    user_id = update.effective_user.id
    
    # Initialize custom batch session
    custom_batch_sessions[user_id] = {
        'tag': tag,
        'media_list': [],
        'active': True,
        'start_time': asyncio.get_event_loop().time()
    }
    
    reply_keyboard = ReplyKeyboardMarkup(
        [["🛑 Stop Batch", "❌ Cancel Batch"]],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    await update.message.reply_text(
        f"📥 <b>Custom Batch Started</b>\n\n"
        f"📁 Tag: <code>{tag}</code>\n"
        f"📨 Status: Collecting media...\n\n"
        f"📋 <b>Instructions:</b>\n"
        f"• Forward/send any media files\n"
        f"• Media will be stored under tag '{tag}'\n"
        f"• Send /stop_batch to finish and get link\n"
        f"• Send /cancel_batch to cancel\n\n"
        f"🔄 Ready to receive media!",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_keyboard
    )


async def stop_batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop custom batch collection and generate link"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    user_id = update.effective_user.id
    
    if user_id not in custom_batch_sessions or not custom_batch_sessions[user_id]['active']:
        await update.message.reply_text("❌ No active custom batch session found.")
        return
    
    session = custom_batch_sessions[user_id]
    tag = session['tag']
    media_list = session['media_list']
    
    if not media_list:
        await update.message.reply_text("❌ No media collected. Batch cancelled.")
        del custom_batch_sessions[user_id]
        return
    
    # Save media to database with caption processing
    if tag not in media_data:
        media_data[tag] = []
    
    start_index = len(media_data[tag])
    
    # Process captions for each media item
    processed_media_list = []
    for media_item in media_list:
        # Create a copy of the media item
        processed_item = {
            "file_id": media_item["file_id"],
            "type": media_item["type"]
        }
        
        # Keep original caption as-is (no processing needed)
        original_caption = media_item.get("original_caption", "")
        if original_caption:
            processed_item["original_caption"] = original_caption
        
        processed_media_list.append(processed_item)
    
    media_data[tag].extend(processed_media_list)
    end_index = len(media_data[tag]) - 1
    
    save_media()
    
    # Generate shareable link
    if start_index == 0 and end_index == len(media_data[tag]) - 1:
        link_param = tag
        range_text = "all files"
    else:
        link_param = f"{tag}_{start_index}_{end_index}"
        range_text = f"files {start_index}-{end_index}"
    
    link = f"https://t.me/{BOT_USERNAME}?start={link_param}"
    
    # Mark session as inactive
    custom_batch_sessions[user_id]['active'] = False
    
    await update.message.reply_text(
        f"✅ <b>Custom Batch Complete!</b>\n\n"
        f"📁 Tag: <code>{tag}</code>\n"
        f"📊 Media collected: {len(media_list)} files\n"
        f"📋 Range: {range_text}\n\n"
        f"🔗 <b>Shareable Link:</b>\n{link}\n\n"
        f"✨ Users can now access these media files!",
        parse_mode=ParseMode.HTML
    )


async def cancel_batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel custom batch collection"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    user_id = update.effective_user.id
    
    if user_id not in custom_batch_sessions or not custom_batch_sessions[user_id]['active']:
        await update.message.reply_text("❌ No active custom batch session found.")
        return
    
    session = custom_batch_sessions[user_id]
    tag = session['tag']
    collected_count = len(session['media_list'])
    
    # Remove session
    del custom_batch_sessions[user_id]
    
    await update.message.reply_text(
        f"❌ <b>Custom Batch Cancelled</b>\n\n"
        f"📁 Tag: <code>{tag}</code>\n"
        f"📊 Media that was collected: {collected_count} files\n"
        f"🗑️ All collected data discarded",
        parse_mode=ParseMode.HTML
    )


async def batch_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check current batch session status"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    user_id = update.effective_user.id
    
    if user_id not in custom_batch_sessions or not custom_batch_sessions[user_id]['active']:
        await update.message.reply_text("📊 No active custom batch session.")
        return
    
    session = custom_batch_sessions[user_id]
    tag = session['tag']
    collected_count = len(session['media_list'])
    start_time = session['start_time']
    current_time = asyncio.get_event_loop().time()
    duration = int(current_time - start_time)
    
    await update.message.reply_text(
        f"📊 <b>Custom Batch Status</b>\n\n"
        f"📁 Tag: <code>{tag}</code>\n"
        f"📊 Media collected: {collected_count} files\n"
        f"⏱️ Duration: {duration} seconds\n"
        f"🔄 Status: Active - collecting media\n\n"
        f"💡 Send /stop_batch to finish or /cancel_batch to cancel",
        parse_mode=ParseMode.HTML
    )


async def handle_custom_batch_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle media received during custom batch session"""
    user_id = update.effective_user.id
    
    # Check if user has active custom batch session
    if user_id in custom_batch_sessions and custom_batch_sessions[user_id]['active']:
        message = update.message
        
        # Extract ANY supported media/file types
        photo = message.photo[-1] if message.photo else None
        video = message.video
        document = message.document
        audio = message.audio
        voice = message.voice
        animation = getattr(message, 'animation', None)
        sticker = message.sticker  # still has a file_id

        file_id = None
        media_type = None

        if photo:
            file_id = photo.file_id
            media_type = "photo"
        elif video:
            file_id = video.file_id
            media_type = "video"
        elif document:
            file_id = document.file_id
            media_type = "document"
        elif audio:
            file_id = audio.file_id
            media_type = "audio"
        elif voice:
            file_id = voice.file_id
            media_type = "voice"
        elif animation:
            file_id = animation.file_id
            media_type = "animation"
        elif sticker:
            file_id = sticker.file_id
            media_type = "sticker"
        else:
            # As a fallback allow plain text messages to be saved as pseudo-document if admin wants
            if message.text and message.text.strip():
                file_id = None
                media_type = "text"
            else:
                return False

        original_caption = message.caption or message.text or ""

        media_item = {
            "type": media_type,
            "original_caption": original_caption
        }
        if file_id:
            media_item["file_id"] = file_id

        custom_batch_sessions[user_id]['media_list'].append(media_item)
        collected_count = len(custom_batch_sessions[user_id]['media_list'])
        tag = custom_batch_sessions[user_id]['tag']

        await message.reply_text(
            f"✅ Item #{collected_count} added to batch\n"
            f"📁 Tag: {tag}\n"
            f"🗂 Type: {media_type}\n"
            f"📊 Total collected: {collected_count}"
        )
        return True
    
    return False


# =================== END BATCH PROCESSING SYSTEM ===================


# =================== CAPTION MANAGEMENT SYSTEM ===================

async def set_global_caption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to set global caption for all files"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not context.args:
        # Show current global caption
        current = caption_config.get("global_caption", "")
        if current:
            await update.message.reply_text(
                f"📝 <b>Current Global Caption:</b>\n\n{current}",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("📝 No global caption is currently set.")
        return
    
    # Set new global caption
    new_caption = " ".join(context.args)
    caption_config["global_caption"] = new_caption
    save_caption_config()
    
    await update.message.reply_text(
        f"✅ <b>Global Caption Updated!</b>\n\n"
        f"📝 New Caption:\n{new_caption}",
        parse_mode=ParseMode.HTML
    )


# =================== END CAPTION MANAGEMENT SYSTEM ===================


# =================== ADMIN MANAGEMENT SYSTEM ===================

async def add_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new admin (only main admin can add other admins)"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Only the main admin can add other admins.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /add_admin <user_id>")
        return
    
    try:
        new_admin_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Please provide a valid number.")
        return
    
    if new_admin_id in admin_list:
        await update.message.reply_text(f"⚠️ User {new_admin_id} is already an admin.")
        return
    
    admin_list.append(new_admin_id)
    save_admin_list()
    
    await update.message.reply_text(f"✅ User {new_admin_id} has been added as an admin.")


async def remove_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove an admin (only main admin can remove other admins)"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Only the main admin can remove other admins.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /remove_admin <user_id>")
        return
    
    try:
        admin_id_to_remove = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Please provide a valid number.")
        return
    
    if admin_id_to_remove == ADMIN_ID:
        await update.message.reply_text("❌ Cannot remove the main admin.")
        return
    
    if admin_id_to_remove not in admin_list:
        await update.message.reply_text(f"⚠️ User {admin_id_to_remove} is not an admin.")
        return
    
    admin_list.remove(admin_id_to_remove)
    save_admin_list()
    
    await update.message.reply_text(f"✅ User {admin_id_to_remove} has been removed from admin list.")


async def list_admins_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all admins"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not admin_list:
        await update.message.reply_text("📋 No admins found.")
        return
    
    message = "<b>👨‍💼 Admin List:</b>\n\n"
    
    for i, admin_id in enumerate(admin_list, 1):
        if admin_id == ADMIN_ID:
            message += f"{i}. <code>{admin_id}</code> (Main Admin) 👑\n"
        else:
            message += f"{i}. <code>{admin_id}</code>\n"
    
    await update.message.reply_html(message)


async def get_file_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get file_id from any media"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Check if replying to a message with media
    if update.message.reply_to_message:
        reply_msg = update.message.reply_to_message
        file_id = None
        media_type = None
        
        if reply_msg.photo:
            file_id = reply_msg.photo[-1].file_id  # Get highest resolution
            media_type = "photo"
        elif reply_msg.video:
            file_id = reply_msg.video.file_id
            media_type = "video"
        elif reply_msg.document:
            file_id = reply_msg.document.file_id
            media_type = "document"
        elif reply_msg.audio:
            file_id = reply_msg.audio.file_id
            media_type = "audio"
        elif reply_msg.voice:
            file_id = reply_msg.voice.file_id
            media_type = "voice"
        elif reply_msg.animation:
            file_id = reply_msg.animation.file_id
            media_type = "animation"
        
        if file_id:
            await update.message.reply_text(
                f"📄 <b>File Information</b>\n\n"
                f"🗂 <b>Type:</b> {media_type}\n"
                f"🆔 <b>File ID:</b>\n<code>{file_id}</code>\n\n"
                f"💡 <b>To set as welcome image:</b>\n"
                f"<code>/setwelcomeimage {file_id}</code>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("❌ No media found in the replied message.")
    else:
        await update.message.reply_text(
            "📋 <b>Get File ID</b>\n\n"
            "Reply to any photo/video/document with this command to get its file_id.\n\n"
            "<b>Usage:</b>\n"
            "1. Send or find a media message\n"
            "2. Reply to it with <code>/getfileid</code>\n"
            "3. Copy the file_id from the response",
            parse_mode=ParseMode.HTML
        )


async def set_welcome_image_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to set welcome image using file_id"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    global WELCOME_IMAGE_FILE_ID
    
    if not context.args:
        # Show current status
        status = f"Current welcome image file_id: `{WELCOME_IMAGE_FILE_ID}`" if WELCOME_IMAGE_FILE_ID else "No welcome image file_id set"
        await update.message.reply_text(
            f"🖼️ <b>Welcome Image Status</b>\n\n{status}\n\n"
            f"<b>Usage:</b>\n"
            f"• <code>/setwelcomeimage &lt;file_id&gt;</code> - Set welcome image\n"
            f"• <code>/setwelcomeimage clear</code> - Clear current image\n"
            f"• Reply to a photo with <code>/setwelcomeimage</code> to use that photo",
            parse_mode=ParseMode.HTML
        )
        return
    
    if context.args[0].lower() == "clear":
        WELCOME_IMAGE_FILE_ID = None
        await update.message.reply_text("✅ Welcome image cleared. Bot will try to use local file or show text-only welcome.")
        return
    
    # Check if replying to a photo
    if update.message.reply_to_message and update.message.reply_to_message.photo:
        # Use the photo from the replied message
        photo = update.message.reply_to_message.photo[-1]  # Get highest resolution
        WELCOME_IMAGE_FILE_ID = photo.file_id
        await update.message.reply_text(f"✅ Welcome image updated using replied photo!\nFile ID: `{WELCOME_IMAGE_FILE_ID}`", parse_mode=ParseMode.MARKDOWN)
        return
    
    # Use provided file_id
    file_id = context.args[0].strip()
    if len(file_id) > 10:  # Basic validation
        WELCOME_IMAGE_FILE_ID = file_id
        await update.message.reply_text(f"✅ Welcome image file_id updated to: `{file_id}`", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text("❌ Invalid file_id provided. Please provide a valid Telegram file_id.")


async def test_welcome_image_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to test the welcome image"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    global WELCOME_IMAGE_FILE_ID
    
    test_text = (f"🧪 <b>Welcome Image Test</b>\n\n"
                 f"This is a test of your welcome image.\n"
                 f"File ID: `{WELCOME_IMAGE_FILE_ID or 'None'}`")
    
    test_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Image Works", callback_data="test_image_ok")],
        [InlineKeyboardButton("❌ No Image Shown", callback_data="test_image_fail")]
    ])
    
    photo_sent = False
    
    try:
        if WELCOME_IMAGE_FILE_ID:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=WELCOME_IMAGE_FILE_ID,
                caption=test_text,
                parse_mode=ParseMode.HTML,
                reply_markup=test_keyboard
            )
            photo_sent = True
    except Exception as e:
        await update.message.reply_text(f"❌ Error testing welcome image: {str(e)}")
        photo_sent = False
    
    if not photo_sent:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"📋 <i>(No image available)</i>\n\n{test_text}",
            parse_mode=ParseMode.HTML,
            reply_markup=test_keyboard
        )


# =================== END ADMIN MANAGEMENT SYSTEM ===================


async def listdeleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    """List all deleted media entries with pagination in simple format"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # First, clean up any corrupted deleted media entries
    if deleted_media_storage:
        print("🧹 Running automatic cleanup before listing deleted media...")
        corrupted_count = await cleanup_deleted_media()
        if corrupted_count > 0:
            await update.message.reply_text(f"🧹 Cleaned up {corrupted_count} corrupted deleted media entries.")
    
    if not deleted_media_storage:
        await update.message.reply_text("📋 No deleted media found.")
        return
    
    # Convert to list for pagination and sorting
    all_deleted = []
    for video_key, deleted_info in deleted_media_storage.items():
        tag = deleted_info.get("tag", "unknown")
        original_pos = deleted_info.get("original_position", "?")
        data = deleted_info.get("data", {})
        media_type = data.get("type", "unknown")
        all_deleted.append((video_key, tag, original_pos, media_type))
    
    # Sort by tag first, then by original position
    all_deleted.sort(key=lambda x: (x[1], x[2] if isinstance(x[2], int) else 999))
    
    # Pagination settings
    items_per_page = 10
    total_items = len(all_deleted)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    
    if page < 0:
        page = 0
    elif page >= total_pages:
        page = total_pages - 1
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, total_items)
    current_items = all_deleted[start_idx:end_idx]
    
    # Build message
    message = f"🗑️ <b>Deleted Media (Page {page + 1}/{total_pages})</b>\n\n"
    
    for i, (video_key, tag, original_pos, media_type) in enumerate(current_items, start_idx + 1):
        # Create view link similar to your example
        view_link = f"https://t.me/GlitchedMatrixbot?start=view_deleted_{video_key}"
        restore_link = f"https://t.me/GlitchedMatrixbot?start=restore_{video_key}"
        
        message += f"{i}. Tag: <code>{tag}</code> | Index: <code>{original_pos}</code> | "
        message += f"�️ {media_type} | "
        message += f"<a href='{view_link}'>👁️ View</a> | "
        message += f"<a href='{restore_link}'>♻️ Restore</a>\n"
    
    # Create navigation buttons
    nav_buttons = []
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"deleted_page_{page - 1}"))
        
        # Page info button (non-clickable info)
        nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="deleted_page_info"))
        
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("➡️ Next", callback_data=f"deleted_page_{page + 1}"))
        
        nav_buttons.append(nav_row)
    
    # Add utility buttons
    utility_buttons = [
        InlineKeyboardButton("🧹 Cleanup", callback_data="cleanup_deleted"),
        InlineKeyboardButton("🔄 Refresh", callback_data="refresh_deleted_list")
    ]
    nav_buttons.append(utility_buttons)
    
    keyboard = InlineKeyboardMarkup(nav_buttons) if nav_buttons else None
    
    # Send the message
    await update.message.reply_html(message, reply_markup=keyboard)


async def cleanup_deleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually clean up corrupted deleted media entries"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not deleted_media_storage:
        await update.message.reply_text("📋 No deleted media to clean up.")
        return
    
    # Show initial count
    initial_count = len(deleted_media_storage)
    await update.message.reply_text(f"🧹 Starting cleanup of {initial_count} deleted media entries...")
    
    # Run the cleanup
    corrupted_count = await cleanup_deleted_media()
    
    # Report results
    remaining_count = len(deleted_media_storage)
    if corrupted_count > 0:
        await update.message.reply_text(
            f"✅ Cleanup complete!\n"
            f"🗑️ Removed: {corrupted_count} corrupted entries\n"
            f"📁 Remaining: {remaining_count} valid entries"
        )
    else:
        await update.message.reply_text(f"✅ No corrupted entries found. All {remaining_count} entries are valid.")


async def autodelete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Control auto-deletion settings"""
    global AUTO_DELETE_ENABLED, AUTO_DELETE_HOURS
    
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not context.args:
        # Show current status
        status = "🟢 ENABLED" if AUTO_DELETE_ENABLED else "🔴 DISABLED"
        await update.message.reply_text(
            f"🧹 <b>Auto-Delete Status</b>\n\n"
            f"Status: {status}\n"
            f"Delete after: {AUTO_DELETE_HOURS} hour(s)\n"
            f"Tracked messages: {len(sent_messages_tracker)}\n\n"
            f"<b>Commands:</b>\n"
            f"• <code>/autodelete on</code> - Enable auto-deletion\n"
            f"• <code>/autodelete off</code> - Disable auto-deletion\n"
            f"• <code>/autodelete hours &lt;number&gt;</code> - Set deletion time\n"
            f"• <code>/autodelete clear</code> - Clear tracking list\n"
            f"• <code>/autodelete stats</code> - Show detailed stats",
            parse_mode=ParseMode.HTML
        )
        return
    
    command = context.args[0].lower()
    
    if command == "on":
        AUTO_DELETE_ENABLED = True
        save_autodelete_config()
        await update.message.reply_text("✅ Auto-deletion ENABLED")
    
    elif command == "off":
        AUTO_DELETE_ENABLED = False
        save_autodelete_config()
        await update.message.reply_text("❌ Auto-deletion DISABLED")
    
    elif command == "hours" and len(context.args) >= 2:
        try:
            hours = float(context.args[1])
            if 0.1 <= hours <= 168:  # Between 6 minutes and 1 week
                AUTO_DELETE_HOURS = hours
                save_autodelete_config()
                await update.message.reply_text(f"⏰ Auto-deletion time set to {hours} hour(s)")
            else:
                await update.message.reply_text("❌ Hours must be between 0.1 and 168")
        except ValueError:
            await update.message.reply_text("❌ Invalid number format")
    
    elif command == "clear":
        cleared_count = len(sent_messages_tracker)
        sent_messages_tracker.clear()
        save_tracking_data()  # Save the cleared state
        await update.message.reply_text(f"🧹 Cleared {cleared_count} tracked messages")
    
    elif command == "stats":
        if not sent_messages_tracker:
            await update.message.reply_text("📊 No messages currently tracked")
            return
        
        # Group by chat
        chat_counts = {}
        oldest_time = None
        newest_time = None
        
        for key, info in sent_messages_tracker.items():
            chat_id = info["chat_id"]
            timestamp = info["timestamp"]
            
            chat_counts[chat_id] = chat_counts.get(chat_id, 0) + 1
            
            if oldest_time is None or timestamp < oldest_time:
                oldest_time = timestamp
            if newest_time is None or timestamp > newest_time:
                newest_time = timestamp
        
        stats_text = f"📊 <b>Auto-Delete Stats</b>\n\n"
        stats_text += f"Total tracked: {len(sent_messages_tracker)}\n"
        
        if oldest_time:
            stats_text += f"Oldest: {oldest_time.strftime('%H:%M:%S')}\n"
            stats_text += f"Newest: {newest_time.strftime('%H:%M:%S')}\n\n"
        
        stats_text += "<b>By Chat:</b>\n"
        for chat_id, count in list(chat_counts.items())[:10]:  # Show top 10
            stats_text += f"Chat {chat_id}: {count} messages\n"
        
        await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)
    
    elif command == "histclean":
        # Historical cleanup command with safety measures
        global HISTORICAL_CLEANUP_RUNNING
        
        if HISTORICAL_CLEANUP_RUNNING:
            await update.message.reply_text(
                "⚠️ <b>Historical cleanup already running!</b>\n\n"
                "Please wait for the current cleanup to finish before starting another one.",
                parse_mode=ParseMode.HTML
            )
            return
        
        force_mode = len(context.args) >= 2 and context.args[1].lower() == "force"
        dry_run = not force_mode
        
        # SAFETY: Disable historical cleanup for now due to issues
        await update.message.reply_text(
            "🚨 <b>Historical cleanup temporarily disabled</b>\n\n"
            "❌ The historical cleanup system has been disabled due to:\n"
            "• Multiple simultaneous runs causing conflicts\n"
            "• Risk of rate limiting with 302 users\n"
            "• Potential deletion of recent messages\n\n"
            "✅ The regular auto-deletion tracking is working fine.\n"
            "📊 Use <code>/autodelete stats</code> to see tracked messages.",
            parse_mode=ParseMode.HTML
        )
        return
    
    else:
        await update.message.reply_text("❌ Invalid command. Use /autodelete without arguments for help.")


async def notifications_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Control auto-deletion notification preferences"""
    chat_id = update.effective_chat.id
    
    if not context.args:
        # Show current status
        current_setting = get_user_preference(chat_id, "auto_delete_notifications", True)
        status = "🟢 ENABLED" if current_setting else "🔴 DISABLED"
        
        # Check cooldown status
        cooldown_info = ""
        if chat_id in last_notification_time:
            time_since_last = datetime.now() - last_notification_time[chat_id]
            cooldown_remaining = NOTIFICATION_COOLDOWN_HOURS * 3600 - time_since_last.total_seconds()
            if cooldown_remaining > 0:
                cooldown_info = f"\n🔕 Cooldown: {cooldown_remaining/60:.1f} minutes remaining"
            else:
                cooldown_info = f"\n✅ Cooldown: Ready to send notifications"
        
        await update.message.reply_text(
            f"🔔 <b>Auto-Delete Notifications</b>\n\n"
            f"Status: {status}{cooldown_info}\n\n"
            f"When enabled, you'll receive a notification when media is automatically deleted.\n"
            f"⏱️ <b>Cooldown:</b> Maximum 1 notification per {NOTIFICATION_COOLDOWN_HOURS} hour(s) to avoid spam.\n\n"
            f"<b>Commands:</b>\n"
            f"• <code>/notifications on</code> - Enable notifications\n"
            f"• <code>/notifications off</code> - Disable notifications\n\n"
            f"💡 <b>Tip:</b> Use favorites (⭐) to save important videos permanently!",
            parse_mode=ParseMode.HTML
        )
        return
    
    command = context.args[0].lower()
    
    if command == "on":
        set_user_preference(chat_id, "auto_delete_notifications", True)
        await update.message.reply_text(
            "🔔 ✅ Auto-deletion notifications ENABLED\n\n"
            f"You'll now receive notifications when media is automatically deleted after {AUTO_DELETE_HOURS} hour(s).\n"
            f"⏱️ Note: Maximum 1 notification per {NOTIFICATION_COOLDOWN_HOURS} hour(s) to prevent spam."
        )
    
    elif command == "off":
        set_user_preference(chat_id, "auto_delete_notifications", False)
        await update.message.reply_text(
            "🔕 Auto-deletion notifications DISABLED\n\n"
            "You won't receive notifications when media is automatically deleted.\n"
            f"💡 Remember: Media is still auto-deleted after {AUTO_DELETE_HOURS} hour(s), but you won't be notified."
        )
    
    else:
        await update.message.reply_text("❌ Invalid command. Use /notifications without arguments for help.")


async def debug_deleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug command to check deleted media vs actual media"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /debugdeleted <tag>\nExample: /debugdeleted thirstthus")
        return
    
    tag = context.args[0].strip()
    
    if tag not in media_data:
        await update.message.reply_text(f"❌ Tag '{tag}' not found in media data.")
        return
    
    # Get current media count for this tag
    current_count = len(media_data[tag])
    
    # Get deleted entries for this tag
    deleted_entries = []
    for video_key, deleted_info in deleted_media_storage.items():
        if deleted_info.get("tag") == tag:
            deleted_entries.append((video_key, deleted_info))
    
    # Sort by original position
    deleted_entries.sort(key=lambda x: x[1].get("original_position", 0))
    
    message = f"🔍 <b>Debug Info for Tag: {tag}</b>\n\n"
    message += f"📊 Current media count: <code>{current_count}</code>\n"
    message += f"🗑️ Deleted entries: <code>{len(deleted_entries)}</code>\n\n"
    
    if deleted_entries:
        message += "<b>Deleted Media Details:</b>\n"
        for video_key, deleted_info in deleted_entries:
            original_pos = deleted_info.get("original_position", "?")
            deleted_date = deleted_info.get("deleted_date", "Unknown")
            data = deleted_info.get("data", {})
            media_type = data.get("type", "unknown")
            
            # Check if this would be a valid index now
            status = "✅ Valid" if original_pos == "?" or int(original_pos) <= current_count else "⚠️ Stale"
            
            message += f"  • <code>{video_key}</code>\n"
            message += f"    Original Index: {original_pos} | Type: {media_type} | {status}\n"
            message += f"    Deleted: {deleted_date}\n\n"
    else:
        message += "No deleted entries found for this tag.\n"
    
    await update.message.reply_html(message)


async def restoredeleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restore a specific deleted or revoked media by video_key"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /restoredeleted <video_key>\nExample: /restoredeleted wildwed_65")
        return
    
    video_key = context.args[0].strip()
    
    # Check if it's a deleted item in deleted_media_storage
    if video_key in deleted_media_storage:
        # Handle deleted media restoration
        deleted_info = deleted_media_storage[video_key]
        original_data = deleted_info["data"]
        original_position = deleted_info["original_position"]
        tag = deleted_info["tag"]
        
        # Ensure the tag still exists
        if tag not in media_data:
            media_data[tag] = []
        
        # Insert the media back at its original position
        if original_position <= len(media_data[tag]):
            media_data[tag].insert(original_position, original_data)
        else:
            media_data[tag].append(original_data)
        
        # Remove from deleted storage
        del deleted_media_storage[video_key]
        
        save_media()
        save_deleted_media()
        update_random_state()
        
        await update.message.reply_text(f"✅ Deleted media <code>{video_key}</code> restored successfully!", parse_mode=ParseMode.HTML)
        
    else:
        # Check if it's a revoked item in media_data
        try:
            tag, idx_str = video_key.rsplit("_", 1)
            idx = int(idx_str)
            
            if tag in media_data and idx < len(media_data[tag]):
                media_item = media_data[tag][idx]
                
                if isinstance(media_item, dict) and media_item.get("revoked"):
                    # Remove the revoked flag to restore the media
                    media_item.pop("revoked", None)
                    
                    save_media()
                    update_random_state()
                    
                    await update.message.reply_text(f"✅ Revoked media <code>{video_key}</code> restored successfully!", parse_mode=ParseMode.HTML)
                    return
            
            await update.message.reply_text(f"❌ No deleted or revoked media found with key: <code>{video_key}</code>", parse_mode=ParseMode.HTML)
            return
            
        except (ValueError, IndexError):
            await update.message.reply_text(f"❌ Invalid media key format: <code>{video_key}</code>", parse_mode=ParseMode.HTML)
            return


async def cleardeleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Permanently remove all deleted media from storage"""


async def cleardeleted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear all deleted media permanently (cannot be undone)"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not deleted_media_storage:
        await update.message.reply_text("📋 No deleted media to clear.")
        return
    
    deleted_count = len(deleted_media_storage)
    
    # Create confirmation buttons
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Clear All", callback_data="confirm_clear_deleted"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_clear_deleted")
        ]
    ])
    
    await update.message.reply_text(
        f"⚠️ <b>Permanent Deletion Warning</b>\n\n"
        f"This will permanently remove all {deleted_count} deleted media entries.\n"
        f"This action cannot be undone!\n\n"
        f"Are you sure you want to continue?",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )


async def restoreall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restore all deleted media"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not deleted_media_storage:
        await update.message.reply_text("📋 No deleted media to restore.")
        return
    
    restored_count = 0
    
    # Sort by original position to maintain order
    sorted_items = sorted(
        deleted_media_storage.items(),
        key=lambda x: (x[1].get("tag", ""), x[1].get("original_position", 0))
    )
    
    for video_key, deleted_info in sorted_items:
        original_data = deleted_info["data"]
        original_position = deleted_info["original_position"]
        tag = deleted_info["tag"]
        
        # Ensure the tag still exists
        if tag not in media_data:
            media_data[tag] = []
        
        # Insert the media back at its original position
        if original_position <= len(media_data[tag]):
            media_data[tag].insert(original_position, original_data)
        else:
            media_data[tag].append(original_data)
        
        restored_count += 1
    
    # Clear all deleted media
    deleted_media_storage.clear()
    
    save_media()
    save_deleted_media()
    update_random_state()
    
    await update.message.reply_text(
        f"♻️ <b>All Media Restored Successfully!</b>\n\n"
        f"📊 Restored: <code>{restored_count}</code> items\n"
        f"✅ All deleted media has been restored to original positions.",
        parse_mode=ParseMode.HTML
    )


async def deletedstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show statistics about deleted media"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    if not deleted_media_storage:
        await update.message.reply_text("📋 No deleted media found.")
        return
    
    # Analyze deleted media
    by_tag = {}
    by_type = {}
    total_deleted = len(deleted_media_storage)
    
    for video_key, deleted_info in deleted_media_storage.items():
        tag = deleted_info.get("tag", "unknown")
        data = deleted_info.get("data", {})
        media_type = data.get("type", "unknown")
        
        by_tag[tag] = by_tag.get(tag, 0) + 1
        by_type[media_type] = by_type.get(media_type, 0) + 1
    
    message = "<b>📊 Deleted Media Statistics</b>\n\n"
    message += f"🗑️ <b>Total Deleted:</b> {total_deleted} items\n\n"
    
    message += "<b>📁 By Tag:</b>\n"
    for tag, count in sorted(by_tag.items(), key=lambda x: x[1], reverse=True):
        message += f"  • {tag}: {count} items\n"
    
    message += "\n<b>📄 By Type:</b>\n"
    for media_type, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
        message += f"  • {media_type}: {count} items\n"
    
    await update.message.reply_html(message)


async def process_pending_updates(application):
    """Process all pending updates that were missed during downtime"""
    try:
        print("🔄 Checking for pending updates...")
        
        # Use shorter timeout for startup check
        updates = await application.bot.get_updates(
            offset=last_update_offset + 1 if last_update_offset > 0 else None,
            limit=50,  # Reduced limit for faster processing
            timeout=5   # Shorter timeout
        )
        
        if updates:
            print(f"📨 Found {len(updates)} pending updates to process")
            processed_count = 0
            
            for update in updates:
                try:
                    # Process the update through the normal handler
                    await application.process_update(update)
                    
                    # Save the offset after each successful update
                    save_update_offset(update.update_id)
                    processed_count += 1
                    
                    # Show progress for large batches
                    if processed_count % 5 == 0:
                        print(f"📊 Processed {processed_count}/{len(updates)} updates...")
                        
                    # Small delay to prevent overwhelming the connection pool
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    print(f"❌ Error processing update {update.update_id}: {e}")
                    # Still save offset to avoid reprocessing failed updates
                    save_update_offset(update.update_id)
            
            print(f"✅ Processed {processed_count} pending updates up to offset: {last_update_offset}")
        else:
            print("✅ No pending updates found")
            
    except Exception as e:
        print(f"❌ Error checking pending updates: {e}")
        # Don't fail startup if pending updates check fails
        print("🔄 Bot will continue with normal operation")

async def force_check_updates_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to manually check for pending updates"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin access required.")
        return
    
    # Add a delay to avoid connection pool conflicts
    await asyncio.sleep(1)
    
    await update.message.reply_text("🔄 Manually checking for pending updates...")
    
    try:
        # Get the current application instance
        application = context.application
        
        # Use a more conservative approach for manual checks
        updates = await application.bot.get_updates(
            offset=last_update_offset + 1 if last_update_offset > 0 else None,
            limit=20,  # Smaller batch for manual checks
            timeout=3   # Very short timeout for manual checks
        )
        
        if updates:
            await update.message.reply_text(
                f"📨 Found {len(updates)} pending updates!\n"
                f"🔄 Processing them now (this may take a moment)..."
            )
            
            processed = 0
            failed = 0
            
            for pending_update in updates:
                try:
                    await application.process_update(pending_update)
                    save_update_offset(pending_update.update_id)
                    processed += 1
                    
                    # Small delay between processing updates
                    await asyncio.sleep(0.2)
                    
                except Exception as e:
                    print(f"❌ Error processing update {pending_update.update_id}: {e}")
                    save_update_offset(pending_update.update_id)
                    failed += 1
            
            result_msg = f"✅ Processing complete!\n📊 Processed: {processed}"
            if failed > 0:
                result_msg += f"\n⚠️ Failed: {failed}"
            result_msg += f"\n📋 Current offset: {last_update_offset}"
            
            await update.message.reply_text(result_msg)
            
        else:
            await update.message.reply_text(
                f"✅ No pending updates found.\n"
                f"📊 Current offset: {last_update_offset}"
            )
            
    except Exception as e:
        error_msg = str(e)
        if "Pool timeout" in error_msg:
            await update.message.reply_text(
                "⚠️ Connection pool busy - bot is processing many requests.\n"
                "🔄 Try again in a few seconds, or wait for automatic processing."
            )
        elif "timeout" in error_msg.lower():
            await update.message.reply_text(
                "⏱️ Request timed out - this is normal during high activity.\n"
                "✅ The bot continues processing updates automatically."
            )
        else:
            await update.message.reply_text(f"❌ Error checking updates: {error_msg}")

# Set bot commands for the menu
commands = [
    BotCommand("start", "🏠 Start the bot"),
    BotCommand("stop", "🛑 Stop ongoing operations"),
    BotCommand("favorites", "❤️ View your favorites"),
    BotCommand("help", "❓ Get help"),
    BotCommand("backup", "💾 Create database backup"),
    BotCommand("listbackups", "📋 List all backups"),
    BotCommand("telegrambackup", "📤 Send JSON files to Telegram"),
    BotCommand("autobackup", "🔄 Manage daily auto-backup"),
    BotCommand("restore", "♻️ Restore from backup"),
    BotCommand("backupstats", "📊 Show backup statistics"),
    BotCommand("restore", "🔄 Restore from backup")
]

async def show_push_tag_selector(query, context, video_key):
    """Show tag selection interface for PUSH system - displays all tags with green checkmarks for existing media"""
    if not is_admin(query.from_user.id):
        await query.answer("❌ Admin access required", show_alert=True)
        return
    
    if "_" not in video_key:
        await query.answer("❌ Invalid video key", show_alert=True)
        return
    
    tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await query.answer("❌ Invalid index", show_alert=True)
        return
    
    if tag not in media_data or not (0 <= idx < len(media_data[tag])):
        await query.answer("❌ Media not found", show_alert=True)
        return
    
    # Get all available tags from /listvideos command
    all_tags = list(media_data.keys())
    all_tags.sort()  # Sort alphabetically
    
    # Check which tags already contain this media
    current_media = media_data[tag][idx]
    existing_tags = []
    
    for check_tag in all_tags:
        if check_tag in media_data:
            for media_item in media_data[check_tag]:
                if isinstance(media_item, dict) and isinstance(current_media, dict):
                    # Compare by file_id if available
                    if (media_item.get("file_id") == current_media.get("file_id") and 
                        media_item.get("file_id")):
                        existing_tags.append(check_tag)
                        break
                elif media_item == current_media:
                    # Direct comparison for non-dict items
                    existing_tags.append(check_tag)
                    break
    
    # Build the message
    message = (
        f"🎯 <b>PUSH Media Manager</b>\n\n"
        f"📁 Current Tag: <code>{tag}</code>\n"
        f"📍 Index: <code>{idx}</code>\n\n"
        f"💡 Click tags below to add/remove this media:\n"
        f"✅ Green = Media exists in tag\n"
        f"➕ Click to add | ➖ Click to remove\n\n"
        f"<b>Available Tags:</b>\n"
    )
    
    # Create buttons for each tag
    keyboard = []
    row = []
    
    for tag_name in all_tags:
        # Determine button text and callback
        if tag_name in existing_tags:
            # Media exists in this tag - show as removable
            button_text = f"✅ {tag_name}"
            callback_data = f"pr_{video_key}_{tag_name}"
        else:
            # Media doesn't exist in this tag - show as addable
            button_text = f"➕ {tag_name}"
            callback_data = f"pa_{video_key}_{tag_name}"
        
        row.append(InlineKeyboardButton(button_text, callback_data=callback_data))
        
        # Create new row every 2 buttons for better layout
        if len(row) >= 2:
            keyboard.append(row)
            row = []
    
    # Add remaining buttons
    if row:
        keyboard.append(row)
    
    # Add close button
    keyboard.append([InlineKeyboardButton("❌ Close", callback_data="close_menu")])
    
    markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(
            message,
            reply_markup=markup,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await query.answer(f"Error: {str(e)}", show_alert=True)


async def handle_push_tag_action(query, context, action, tag_name, video_key):
    """Handle adding or removing media from tags in PUSH system"""
    if not is_admin(query.from_user.id):
        await query.answer("❌ Admin access required", show_alert=True)
        return
    
    if "_" not in video_key:
        await query.answer("❌ Invalid video key", show_alert=True)
        return
    
    source_tag, idx_str = video_key.rsplit("_", 1)
    try:
        idx = int(idx_str)
    except ValueError:
        await query.answer("❌ Invalid index", show_alert=True)
        return
    
    if source_tag not in media_data or not (0 <= idx < len(media_data[source_tag])):
        await query.answer("❌ Source media not found", show_alert=True)
        return
    
    media_item = media_data[source_tag][idx]
    
    if action == "add":
        # Add media to the tag
        if tag_name not in media_data:
            media_data[tag_name] = []
        
        # Check if media already exists in target tag
        already_exists = False
        for existing_item in media_data[tag_name]:
            if isinstance(existing_item, dict) and isinstance(media_item, dict):
                if existing_item.get("file_id") == media_item.get("file_id"):
                    already_exists = True
                    break
            elif existing_item == media_item:
                already_exists = True
                break
        
        if not already_exists:
            media_data[tag_name].append(media_item)
            save_media()
            update_random_state()
            await query.answer(f"✅ Added to {tag_name}", show_alert=True)
        else:
            await query.answer(f"⚠️ Already exists in {tag_name}", show_alert=True)
    
    elif action == "remove":
        # Remove media from the tag
        if tag_name in media_data:
            # Find and remove the media
            original_length = len(media_data[tag_name])
            media_data[tag_name] = [
                item for item in media_data[tag_name]
                if not (
                    (isinstance(item, dict) and isinstance(media_item, dict) and
                     item.get("file_id") == media_item.get("file_id")) or
                    (item == media_item)
                )
            ]
            
            if len(media_data[tag_name]) < original_length:
                save_media()
                update_random_state()
                await query.answer(f"✅ Removed from {tag_name}", show_alert=True)
            else:
                await query.answer(f"⚠️ Not found in {tag_name}", show_alert=True)
        else:
            await query.answer(f"⚠️ Tag {tag_name} not found", show_alert=True)
    
    # Refresh the PUSH interface
    await show_push_tag_selector(query, context, video_key)


async def handle_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all inline button clicks"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    # Handle PUSH button - show tag selection for media management
    if data.startswith("p_") and not data.startswith("pa_") and not data.startswith("pr_"):
        video_key = data.replace("p_", "")
        await show_push_tag_selector(query, context, video_key)
        return
    
    # Handle tag selection for PUSH system
    elif data.startswith("pa_") or data.startswith("pr_"):
        parts = data.split("_", 2)
        if len(parts) >= 3:
            prefix = parts[0]
            video_key = parts[1]
            tag_name = parts[2]
            action = "add" if prefix == "pa" else "remove"
            await handle_push_tag_action(query, context, action, tag_name, video_key)
        return
    
    # Handle favorites navigation
    elif data.startswith("fav_nav_"):
        index = int(data.replace("fav_nav_", ""))
        await show_favorites_navigator(query, context, index, edit_message=True)
        return
    
    # Handle favorites add/remove
    elif data.startswith("add_fav_"):
        await add_to_favorites(query, context)
        return
    
    elif data.startswith("remove_fav_"):
        await remove_from_favorites(query, context)
        return
    
    # Handle view favorites
    elif data == "view_favorites":
        await view_user_favorites(query, context)
        return
    
    # Handle random buttons
    elif data == "random_media":
        await send_random_video(context, query.message.chat_id, mode="adult")
        return
    
    elif data == "next_safe":
        await send_random_video(context, query.message.chat_id, mode="safe")
        return
    
    elif data == "next_adult":
        await send_random_video(context, query.message.chat_id, mode="adult")
        return
    
    elif data == "random_safe":
        await send_random_video(context, query.message.chat_id, mode="safe")
        return
    
    elif data == "random_nsfw":
        await send_random_video(context, query.message.chat_id, mode="adult")
        return
    
    elif data == "switch_to_safe":
        await send_random_video(context, query.message.chat_id, mode="safe")
        return
    
    elif data == "switch_to_adult":
        await send_random_video(context, query.message.chat_id, mode="adult")
        return
    
    # Handle top videos navigation
    elif data.startswith("topvideo_view_"):
        page = int(data.replace("topvideo_view_", ""))
        await show_top_videos_viewer(None, context, page, query)
        return
    
    # Handle top videos page navigation
    elif data.startswith("topvideos_page_"):
        page = int(data.replace("topvideos_page_", ""))
        await show_top_videos_page(None, context, page, query)
        return
    
    elif data == "topvideos_info":
        await query.answer("Navigate using Previous/Next buttons", show_alert=False)
        return
    
    # Handle top users navigation
    elif data.startswith("topusers_page_"):
        page = int(data.replace("topusers_page_", ""))
        await show_top_users_page(None, context, page, query)
        return
    
    elif data == "topusers_info":
        await query.answer("Navigate using Previous/Next buttons", show_alert=False)
        return
    
    # Handle deleted media navigation
    elif data.startswith("deleted_page_"):
        if data == "deleted_page_info":
            await query.answer("Navigate using Previous/Next buttons", show_alert=False)
            return
        page = int(data.replace("deleted_page_", ""))
        await listdeleted_command(None, None, page)
        return
    
    elif data == "list_deleted_media":
        await listdeleted_command(None, None, 0)
        return
    
    elif data == "cleanup_deleted":
        await cleanup_deleted_command(None, None)
        return
    
    elif data == "refresh_deleted_list":
        await listdeleted_command(None, None, 0)
        return
    
    # Handle revoked media navigation
    elif data.startswith("revoked_page_"):
        if data == "revoked_page_info":
            await query.answer("Navigate using Previous/Next buttons", show_alert=False)
            return
        page = int(data.replace("revoked_page_", ""))
        # Call the revoked list function with page
        # Skip since listrevoked_cmd is not properly implemented
        await query.answer("This function is currently unavailable", show_alert=True)
        return
    
    elif data == "cleanup_revoked":
        await query.answer("Cleanup not implemented for revoked media", show_alert=True)
        return
    
    elif data == "refresh_revoked_list":
        # Skip since listrevoked_cmd is not properly implemented
        await query.answer("This function is currently unavailable", show_alert=True)
        return
    
    # Handle media restoration
    elif data.startswith("restore_media_"):
        video_key = data.replace("restore_media_", "")
        await restore_media_entry(query, video_key)
        return
    
    # Handle media deletion
    elif data.startswith("del_media_"):
        video_key = data.replace("del_media_", "")
        await delete_media_entry(query, context, video_key)
        return
    
    # Handle media revocation
    elif data.startswith("revoke_media_"):
        video_key = data.replace("revoke_media_", "")
        await revoke_media_entry(query, video_key)
        return
    
    # Handle media fixing
    elif data.startswith("fix_media_"):
        video_key = data.replace("fix_media_", "")
        await fix_media_entry(query, context, video_key)
        return
    
    # Handle raw media data viewing
    elif data.startswith("raw_media_"):
        video_key = data.replace("raw_media_", "")
        await show_raw_media_data(query, context, video_key)
        return
    
    # Handle view video buttons
    elif data.startswith("view_video_"):
        video_id = data.replace("view_video_", "")
        await view_specific_video(query, context, video_id)
        return
    
    # Handle view deleted media
    elif data.startswith("view_deleted_"):
        await view_deleted_media(query, context)
        return
    
    # Handle who liked video
    elif data.startswith("who_liked_"):
        await show_who_liked_video(query, context)
        return
    
    # Handle backup deletion confirmation
    elif data.startswith("confirm_delete_backup_"):
        backup_name = data.replace("confirm_delete_backup_", "")
        await query.answer(f"Backup deletion requested for {backup_name}", show_alert=True)
        await query.edit_message_text(f"⚠️ Backup deletion functionality temporarily unavailable.")
        return
    
    elif data == "cancel_delete_backup":
        await query.edit_message_text("❌ Backup deletion cancelled.")
        return
    
    # Handle clear deleted confirmation
    elif data == "confirm_clear_deleted":
        await cleardeleted_command(None, None)
        return
    
    elif data == "cancel_clear_deleted":
        await query.edit_message_text("❌ Clear deleted media cancelled.")
        return
    
    # Handle test buttons
    elif data == "test_image_ok":
        await query.edit_message_text("✅ Welcome image is working correctly!")
        return
    
    elif data == "test_image_fail":
        await query.edit_message_text("❌ Welcome image failed to load. Check file_id or use /setwelcomeimage")
        return
    
    # Handle close menu
    elif data == "close_menu":
        try:
            await query.message.delete()
        except:
            await query.edit_message_text("❌ Menu closed.")
        return
    
    # Default case - unknown callback
    else:
        await query.answer(f"Unknown action: {data}", show_alert=True)


async def protection_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enable protection for media content - admin only"""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ This command is restricted to admins.")
        return
    
    global protection_enabled
    protection_enabled = True
    save_protection_settings()
    
    await update.message.reply_text(
        "✅ Protection enabled!\n\n"
        "🔒 All media content will now be protected from saving and forwarding.\n"
        "👑 Note: Admins are always exempt from protection."
    )

async def protection_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Disable protection for media content - admin only"""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ This command is restricted to admins.")
        return
    
    global protection_enabled
    protection_enabled = False
    save_protection_settings()
    
    await update.message.reply_text(
        "✅ Protection disabled!\n\n"
        "🔓 Media content can now be saved and forwarded by all users."
    )

# Protection commands for enabling and disabling content protection
async def pon_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enable protection settings - admin only"""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ This command is restricted to admins.")
        return
    
    global protection_enabled
    protection_enabled = True
    save_protection_settings()
    
    await update.message.reply_text(
        "✅ Protection enabled!\n\n"
        "🔒 All media content will now be protected from saving and forwarding.\n"
        "👑 Note: Admins are always exempt from protection."
    )

async def poff_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Disable protection settings - admin only"""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ This command is restricted to admins.")
        return
    
    global protection_enabled
    protection_enabled = False
    save_protection_settings()
    
    await update.message.reply_text(
        "✅ Protection disabled!\n\n"
        "🔓 Media content can now be saved and forwarded by all users."
    )

def main():
    print("🤖 Knowledge Vault Bot - Starting...")
    keep_alive()  # Remove this line if not using Replit
    app = Application.builder().token(BOT_TOKEN).connect_timeout(300.0).read_timeout(300.0).write_timeout(300.0).concurrent_updates(128).post_init(post_init_callback).build()
    
    # Initialize random state
    update_random_state()
    
    # Migrate existing users to users database
    migrate_existing_users()
    
    print("⚡ Bot initialized successfully")

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("upload", upload))
    app.add_handler(CommandHandler("listvideo", listvideo))
    app.add_handler(CommandHandler("listvideos", listvideos))
    app.add_handler(CommandHandler("remove", remove))
    app.add_handler(CommandHandler("get", get_tag))
    app.add_handler(CommandHandler("generatelink", generatelink))
    app.add_handler(CommandHandler("view", view))
    app.add_handler(CommandHandler("pass", pass_link))
    app.add_handler(CommandHandler("passlink", pass_link_command))
    app.add_handler(CommandHandler("revoke", revoke))
    app.add_handler(CommandHandler("revokelink", revoke_link_command))
    app.add_handler(CommandHandler("listactive", listactive))
    app.add_handler(CommandHandler("activelinks", activelinks_command))
    app.add_handler(CommandHandler("passlinks", passlinks_command))
    app.add_handler(CommandHandler("free", free))
    app.add_handler(CommandHandler("unfree", unfree))
    app.add_handler(CommandHandler("listfree", listfree))
    app.add_handler(CommandHandler("favorites", favorites_command))
    app.add_handler(CommandHandler("help", help_command))
    
    # Broadcasting commands
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("dbroadcast", dbroadcast_command))
    app.add_handler(CommandHandler("pbroadcast", pbroadcast_command))
    app.add_handler(CommandHandler("sbroadcast", sbroadcast_command))
    app.add_handler(CommandHandler("fbroadcast", fbroadcast_command))
    app.add_handler(CommandHandler("bstats", broadcast_stats_command))
    app.add_handler(CommandHandler("userstats", user_stats_command))
    app.add_handler(CommandHandler("userinfo", userinfo_command))
    app.add_handler(CommandHandler("topusers", top_users_command))
    app.add_handler(CommandHandler("addusers", add_users_command))
    app.add_handler(CommandHandler("discover", discover_users_command))
    app.add_handler(CommandHandler("protection", protection_status_command))
    app.add_handler(CommandHandler("protectionon", protection_on_command))
    app.add_handler(CommandHandler("protectionoff", protection_off_command))
    # Add shorter aliases for protection commands
    app.add_handler(CommandHandler("pon", pon_cmd))
    app.add_handler(CommandHandler("poff", poff_cmd))
    app.add_handler(CommandHandler("pstatus", protection_status_command))
    app.add_handler(CommandHandler("ps", protection_status_command))
    app.add_handler(CommandHandler("testprotection", test_protection_command))
    app.add_handler(CommandHandler("testdeletion", test_deletion_command))
    app.add_handler(CommandHandler("checkprotection", check_protection_command))
    app.add_handler(CommandHandler("custom_batch", custom_batch_command))
    app.add_handler(CommandHandler("stop_batch", stop_batch_command))
    app.add_handler(CommandHandler("cancel_batch", cancel_batch_command))
    app.add_handler(CommandHandler("batch_status", batch_status_command))
    app.add_handler(CommandHandler("set_global_caption", set_global_caption_command))
    app.add_handler(CommandHandler("add_admin", add_admin_command))
    app.add_handler(CommandHandler("remove_admin", remove_admin_command))
    app.add_handler(CommandHandler("list_admins", list_admins_command))
    app.add_handler(CommandHandler("getfileid", get_file_id_command))
    app.add_handler(CommandHandler("setwelcomeimage", set_welcome_image_command))
    app.add_handler(CommandHandler("testwelcomeimage", test_welcome_image_command))
    app.add_handler(CommandHandler("userfavorites", user_favorites_admin))
    app.add_handler(CommandHandler("videostats", video_stats_admin))
    app.add_handler(CommandHandler("topvideos", top_videos_admin))
    # Deleted media management commands
    app.add_handler(CommandHandler("listdeleted", listdeleted_command))
    app.add_handler(CommandHandler("cleanupdeleted", cleanup_deleted_command))
    app.add_handler(CommandHandler("debugdeleted", debug_deleted_command))
    app.add_handler(CommandHandler("restoredeleted", restoredeleted_command))
    app.add_handler(CommandHandler("cleardeleted", cleardeleted_command))
    app.add_handler(CommandHandler("restoreall", restoreall_command))
    app.add_handler(CommandHandler("deletedstats", deletedstats_command))
    # Auto-deletion management command
    app.add_handler(CommandHandler("autodelete", autodelete_command))
    # Auto-deletion notifications command
    app.add_handler(CommandHandler("notifications", notifications_command))
    # Force check for pending updates (admin only)
    app.add_handler(CommandHandler("checkupdates", force_check_updates_command))
    # List revoked media only (for deleted media use /listdeleted)
    # MOVED ABOVE CALLBACK HANDLERS
    app.add_handler(CommandHandler("listremoved", listremoved_cmd))
    # Restore command: /restoremedia <tag> <index>
    async def restoremedia_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            return
        if len(context.args) != 2:
            await update.message.reply_text("Usage: /restoremedia <tag> <index>")
            return
        tag = context.args[0].strip().lower()
        try:
            idx = int(context.args[1])
        except ValueError:
            await update.message.reply_text("❌ Bad index")
            return
        video_key = f"{tag}_{idx}"
        # Build fake query-like object for reuse (minimal)
        class Dummy:
            def __init__(self, message):
                self.message = message
            async def answer(self, *a, **k):
                pass
        dummy = Dummy(update.message)
        await restore_media_entry(dummy, video_key)
    app.add_handler(CommandHandler("restoremedia", restoremedia_cmd))
    
    # Backup command handlers
    async def telegram_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send all current JSON files directly to admin via Telegram"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        await update.message.reply_text("📤 Sending current JSON files and ZIP backup...")
        
        # List of all important JSON files
        json_files = [
            "media_db.json",
            "users_db.json", 
            "favorites_db.json",
            "passed_links.json",
            "active_links.json",
            "deleted_media.json",
            "random_state.json",
            "user_preferences.json",
            "admin_list.json",
            "exempted_users.json",
            "caption_config.json",
            "protection_settings.json",
            "auto_delete_tracking.json",
            "last_update_offset.json"
        ]
        
        sent_count = 0
        missing_count = 0
        
        # Send individual JSON files (without railway_ prefix)
        for filename in json_files:
            try:
                if os.path.exists(filename):
                    # Read the current file from filesystem
                    with open(filename, 'rb') as file:
                        await context.bot.send_document(
                            chat_id=update.effective_chat.id,
                            document=file,
                            filename=filename,  # Removed "railway_" prefix
                            caption=f"📁 **JSON File**: `{filename}`\n🕐 Extracted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                        )
                    sent_count += 1
                else:
                    missing_count += 1
                    print(f"⚠️ File not found: {filename}")
            except Exception as e:
                await update.message.reply_text(f"❌ Error sending {filename}: {str(e)}")
                missing_count += 1
        
        # Create and send ZIP file containing all JSON files
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            zip_filename = f"backup_{timestamp}.zip"
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for filename in json_files:
                    if os.path.exists(filename):
                        zipf.write(filename, filename)  # Add file to ZIP with original name
            
            # Send the ZIP file
            with open(zip_filename, 'rb') as zip_file:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=zip_file,
                    filename=zip_filename,
                    caption=f"📦 **Complete ZIP Backup**\n🕐 Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC\n📁 Contains all {sent_count} JSON files"
                )
            
            # Clean up the temporary ZIP file
            os.remove(zip_filename)
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error creating ZIP backup: {str(e)}")
        
        # Summary message
        summary = (
            f"✅ **Telegram Backup Complete**\n\n"
            f"📤 Individual files sent: {sent_count}\n"
            f"📦 ZIP backup: Created and sent\n"
            f"❌ Missing files: {missing_count}\n"
            f"🕐 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
            f"💡 Received both individual JSON files and a complete ZIP archive!"
        )
        await update.message.reply_text(summary, parse_mode=ParseMode.MARKDOWN)
    
    async def auto_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Configure automatic backups to Telegram"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not context.args:
            # Show current status
            # Check if auto-backup is running (you can enhance this with a global flag)
            await update.message.reply_text(
                "🔄 **Auto-Backup Configuration**\n\n"
                "Usage:\n"
                "`/autobackup on` - Enable daily auto-backup\n"
                "`/autobackup off` - Disable auto-backup\n"
                "`/autobackup now` - Trigger backup immediately\n"
                "`/autobackup status` - Check current status\n\n"
                "💡 Auto-backup sends JSON files to your Telegram daily",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        action = context.args[0].lower()
        
        if action == "on":
            # Enable auto-backup (you can implement with job_queue)
            await update.message.reply_text(
                "✅ **Auto-backup enabled**\n\n"
                "📅 JSON files will be sent to your Telegram daily at 2 AM UTC\n"
                "🔄 This ensures you always have the latest Railway data backed up"
            )
            
        elif action == "off":
            await update.message.reply_text("❌ Auto-backup disabled")
            
        elif action == "now":
            # Trigger immediate backup
            await telegram_backup_command(update, context)
            
        elif action == "status":
            # Show current status of the backup system
            current_time = datetime.now()
            if last_backup_time:
                time_since = current_time - last_backup_time
                hours_since = time_since.total_seconds() / 3600
                next_backup = last_backup_time + timedelta(hours=BACKUP_INTERVAL_HOURS)
                hours_until = (next_backup - current_time).total_seconds() / 3600
                
                status_msg = (
                    "📊 **Auto-Backup Status**\n\n"
                    f"🔄 Status: Active (Time-based monitoring)\n"
                    f"📅 Last backup: {last_backup_time.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                    f"⏰ Hours since last backup: {hours_since:.1f}\n"
                    f"📁 Next backup: {next_backup.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                    f"⏳ Hours until next backup: {max(0, hours_until):.1f}\n"
                    f"📤 Target: Admin Telegram chat\n"
                    f"💾 Files: All JSON databases\n"
                    f"🔄 Check interval: Every hour"
                )
            else:
                status_msg = (
                    "📊 **Auto-Backup Status**\n\n"
                    "🔄 Status: Active (First backup pending)\n"
                    "📅 Last backup: Never\n"
                    "⏰ Next backup: Within 1 hour\n"
                    "� Target: Admin Telegram chat\n"
                    "💾 Files: All JSON databases\n"
                    "🔄 Check interval: Every hour"
                )
            
            await update.message.reply_text(status_msg)
        else:
            await update.message.reply_text("❌ Invalid option. Use: on, off, now, or status")

    async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Create a backup of all database files"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        await update.message.reply_text("💾 Creating backup...")
        
        # Create backup in background
        success, message = await asyncio.get_event_loop().run_in_executor(
            None, backup_manager.create_backup, None, "Manual backup via bot command"
        )
        
        if success:
            await update.message.reply_text(f"✅ {message}")
        else:
            await update.message.reply_text(f"❌ {message}")
    
    async def list_backups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all available backups"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        backups = await asyncio.get_event_loop().run_in_executor(None, backup_manager.list_backups)
        
        if not backups:
            await update.message.reply_text("📋 No backups found")
            return
        
        message = f"📋 <b>Available Backups ({len(backups)})</b>\n\n"
        
        for i, backup in enumerate(backups[:10], 1):  # Show first 10
            created = backup['created_at'][:19]  # Show date/time without seconds
            files = backup['total_files']
            size = backup['total_size']
            desc = backup.get('description', 'No description')[:50]
            
            message += f"{i}. <code>{backup['name']}</code>\n"
            message += f"   📅 {created} | 📁 {files} files | 💾 {size} bytes\n"
            if desc:
                message += f"   📝 {desc}\n"
            message += "\n"
        
        if len(backups) > 10:
            message += f"... and {len(backups) - 10} more backups"
        
        await update.message.reply_html(message)
    
    async def restore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Restore from a backup"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /restore <backup_name>")
            return
        
        backup_name = context.args[0]
        await update.message.reply_text(f"🔄 Restoring from backup '{backup_name}'...")
        
        # Restore backup
        success, message = await asyncio.get_event_loop().run_in_executor(
            None, backup_manager.restore_backup, backup_name
        )
        
        if success:
            await update.message.reply_text(f"✅ {message}\n\n⚠️ Bot restart recommended to reload all data")
        else:
            await update.message.reply_text(f"❌ {message}")

    async def backup_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show statistics for a specific backup"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /backupstats <backup_name>")
            return
        
        backup_name = context.args[0]
        
        stats = await asyncio.get_event_loop().run_in_executor(
            None, backup_manager.get_backup_stats, backup_name
        )
        
        if not stats or not stats.get('exists'):
            await update.message.reply_text(f"❌ Backup '{backup_name}' not found")
            return
        
        info = stats['info']
        message = f"📊 <b>Backup Statistics: {backup_name}</b>\n\n"
        message += f"📅 Created: {info['created_at'][:19]}\n"
        message += f"📁 Files: {info['total_files']}\n"
        message += f"💾 Size: {info['total_size']} bytes\n"
        
        if info.get('description'):
            message += f"📝 Description: {info['description']}\n"
        
        message += "\n📋 <b>File Details:</b>\n"
        
        for filename, file_info in stats.get('file_details', {}).items():
            if file_info.get('status') == 'not_found':
                message += f"❌ {filename}: Not found\n"
            else:
                size = file_info.get('size', 0)
                message += f"✅ {filename}: {size} bytes\n"
        
        await update.message.reply_html(message)

    async def delete_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Delete a backup"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin access required")
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /deletebackup <backup_name>")
            return
        
        backup_name = context.args[0]
        
        # Confirm deletion with keyboard
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_backup_{backup_name}"),
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_delete_backup")
            ]
        ])
        
        await update.message.reply_text(
            f"🗑️ <b>Delete Backup</b>\n\n"
            f"Are you sure you want to delete backup '{backup_name}'?\n\n"
            f"⚠️ This action cannot be undone!",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
    
    app.add_handler(CommandHandler("backup", backup_command))
    app.add_handler(CommandHandler("listbackups", list_backups_command))
    app.add_handler(CommandHandler("restore", restore_command))
    app.add_handler(CommandHandler("backupstats", backup_stats_command))
    app.add_handler(CommandHandler("deletebackup", delete_backup_command))
    app.add_handler(CommandHandler("telegrambackup", telegram_backup_command))
    app.add_handler(CommandHandler("autobackup", auto_backup_command))
    
    # Add callback query handler for all button interactions
    app.add_handler(CallbackQueryHandler(handle_button_click))

    # Universal handlers to auto-register all users
    app.add_handler(MessageHandler(filters.ALL, auto_register_user), group=1)
    app.add_handler(CallbackQueryHandler(auto_register_user), group=1)
    
    # Add a handler to save update offsets for all updates
    async def save_offset_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save the update offset for persistent tracking (batched to reduce I/O)"""
        global offset_save_counter
        if update.update_id:
            offset_save_counter += 1
            # Save every 5 updates or if it's been more than last saved + 10
            if offset_save_counter % 5 == 0 or update.update_id > last_update_offset + 10:
                save_update_offset(update.update_id)
    
    # Add this handler with lowest priority so it runs after all other handlers
    app.add_handler(MessageHandler(filters.ALL, save_offset_handler), group=99)
    app.add_handler(CallbackQueryHandler(save_offset_handler), group=99)
    
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VOICE | filters.ANIMATION | filters.Sticker.ALL, upload))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_shortcuts))

    # Add error handler for timeout exceptions
    async def error_handler(update: object, context) -> None:
        """Handle errors caused by timeout exceptions."""
        if isinstance(context.error, asyncio.TimeoutError):
            print(f"Timeout error caught: {context.error}")
            if update and hasattr(update, 'callback_query') and update.callback_query:
                try:
                    await safe_answer_callback_query(update.callback_query, "⏱️ Request timed out, please try again", show_alert=True)
                except Exception:
                    pass
        else:
            print(f"Update {update} caused error {context.error}")
    
    app.add_error_handler(error_handler)
    
    print("🚀 Bot running...")
    
    # Show initial counter line
    print_counter_line()
    
    # Use standard polling - pending updates are processed in post_init_callback
    app.run_polling()


if __name__ == "__main__":
    main()
