import os
import json
import asyncio
import random
import urllib.parse
import time
from collections import defaultdict
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.constants import ParseMode
from telegram.ext import (Application, CommandHandler, MessageHandler,
                          CallbackQueryHandler, ContextTypes, filters)
from telegram.error import TimedOut, NetworkError, RetryAfter, BadRequest
from keep_alive import keep_alive  # Optional for Replit or similar


# ================== USER CONFIGURATION (imported from config.py) ==================
from config import IMG_URL as img_url, ADMIN_ID, BOT_USERNAME, REQUIRED_CHANNELS, BOT_TOKEN, BACKUP_CHANNEL_URL, JOIN_CHANNEL_URL
# ================== END USER CONFIGURATION ==================

ADMIN_LIST_FILE = "admin_list.json"
MEDIA_FILE = "media_db.json"
EXEMPTED_FILE = "exempted_users.json"
USERS_FILE = "users_db.json"
ACTIVE_LINKS_FILE = "active_links.json"
PASSED_LINKS_FILE = "passed_links.json"
FAVORITES_FILE = "favorites_db.json"
RANDOM_STATE_FILE = "random_state.json"
CAPTION_CONFIG_FILE = "caption_config.json"

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
user_operations = {}  # Track ongoing operations per user



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


def save_media():
    with open(MEDIA_FILE, "w") as f:
        json.dump(media_data, f, indent=2)


def save_exempted():
    with open(EXEMPTED_FILE, "w") as f:
        json.dump(list(exempted_users), f, indent=2)


def save_active_links():
    with open(ACTIVE_LINKS_FILE, "w") as f:
        json.dump(active_links, f, indent=2)


def save_passed_links():
    with open(PASSED_LINKS_FILE, "w") as f:
        json.dump(passed_links, f, indent=2)


def save_favorites():
    with open(FAVORITES_FILE, "w") as f:
        json.dump(favorites_data, f, indent=2)


def save_random_state():
    with open(RANDOM_STATE_FILE, "w") as f:
        json.dump(random_state, f, indent=2)


def save_caption_config():
    with open(CAPTION_CONFIG_FILE, "w") as f:
        json.dump(caption_config, f, indent=2)


def save_admin_list():
    with open(ADMIN_LIST_FILE, "w") as f:
        json.dump(admin_list, f, indent=2)


def is_admin(user_id):
    """Check if a user is an admin"""
    return user_id in admin_list


def track_user_operation(user_id, operation_type, operation_data=None):
    """Track an ongoing operation for a user"""
    print(f"DEBUG: Tracking operation for user {user_id}: {operation_type}")
    user_operations[user_id] = {
        "type": operation_type,
        "data": operation_data,
        "cancelled": False
    }


def cancel_user_operation(user_id):
    """Cancel ongoing operation for a user"""
    if user_id in user_operations:
        print(f"DEBUG: Cancelling operation for user {user_id}")
        user_operations[user_id]["cancelled"] = True
        return True
    return False


def is_operation_cancelled(user_id):
    """Check if user's operation is cancelled"""
    cancelled = user_operations.get(user_id, {}).get("cancelled", False)
    if cancelled:
        print(f"DEBUG: Operation is cancelled for user {user_id}")
    return cancelled


def clear_user_operation(user_id):
    """Clear user's operation tracking"""
    if user_id in user_operations:
        operation_type = user_operations[user_id].get("type", "unknown")
        print(f"DEBUG: Clearing operation for user {user_id}: {operation_type}")
    user_operations.pop(user_id, None)





async def safe_send_message(context, chat_id, text=None, photo=None, video=None, caption=None, 
                           reply_markup=None, parse_mode=None, protect_content=False, max_retries=3):
    """Safely send a message with retry logic for handling timeouts"""
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
            else:
                message = await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode
                )
            
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


def build_media_caption(original_caption="", tag="", index="", share_url="", media_type="video"):
    """Build caption specifically for media files (videos/photos) sent to users"""
    # Create the base system caption with metadata
    if media_type == "video":
        base_caption = f"🎬 <b>Shared Video</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_url}'>🔗 Shareable Link</a>"
    else:
        base_caption = f"🖼️ <b>Shared Photo</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{index}</code> | <a href='{share_url}'>🔗 Shareable Link</a>"
    
    # Apply global caption
    return build_final_caption(base_caption, add_global=True)


def save_users():
    with open(USERS_FILE, "w") as f:
        json.dump(users_data, f, indent=2)


def track_user(user_id, username=None, first_name=None, source="bot_interaction"):
    """Track a user interaction with the bot"""
    user_id_str = str(user_id)
    current_time = asyncio.get_event_loop().time()
    
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
        track_user(user.id, user.username, user.first_name, source)
        
        # Log the registration for admin visibility
        print(f"Auto-registered user: {user.id} ({user.first_name}) - Source: {source}")


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


def get_random_from_tag(tag="random"):
    """Get random video from specific tag with weighted probability system (for GET FILES button)"""
    if tag not in media_data or not media_data[tag]:
        return None
    
    # Create list of all videos from this specific tag
    tag_videos = [f"{tag}_{i}" for i in range(len(media_data[tag]))]
    
    # Initialize weights system if not exists
    if 'video_weights' not in random_state:
        random_state['video_weights'] = {}
    
    # Initialize weights for tag videos if not exists
    for video in tag_videos:
        if video not in random_state['video_weights']:
            random_state['video_weights'][video] = 1.0  # Equal probability initially
    
    # Get current weights for this tag's videos
    current_weights = []
    for video in tag_videos:
        weight = random_state['video_weights'].get(video, 1.0)
        current_weights.append(weight)
    
    # Weighted random selection
    selected_video = random.choices(tag_videos, weights=current_weights, k=1)[0]
    
    # Update weights: reduce selected video's weight, increase others slightly
    for video in tag_videos:
        if video == selected_video:
            # Recently shown video gets much lower probability
            random_state['video_weights'][video] = 0.1
        else:
            # Other videos get slightly higher probability (recovery factor)
            current_weight = random_state['video_weights'].get(video, 1.0)
            # Gradually restore weight, max cap at 2.0 for balance
            random_state['video_weights'][video] = min(current_weight * 1.15, 2.0)
    
    save_random_state()
    return selected_video


def get_random_from_passed_links():
    """Get random video from passed links only (for RANDOM MEDIA button)"""
    if not passed_links:
        return None
    
    return random.choice(passed_links)


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
    mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'
    args = context.args
    
    # Auto-register user (this handles all user tracking)
    await auto_register_user(update, context)

    if args:
        param = args[0].strip().lower()

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
            await send_passlink_videos(update, context, param, active_links[param])
            return
        
        # Check if this is a custom range within a passlink range
        if start_index is not None and end_index is not None:
            for link_key, link_data in active_links.items():
                if isinstance(link_data, dict) and link_data.get("tag") == tag:
                    stored_start = link_data.get("start_index", 0)
                    stored_end = link_data.get("end_index", 0)
                    stored_videos = link_data.get("videos", [])
                    
                    # Check if requested range is within stored range
                    if start_index >= stored_start and end_index <= stored_end:
                        # Extract the specific videos for this custom range
                        video_start_offset = start_index - stored_start
                        video_end_offset = end_index - stored_start + 1
                        custom_videos = stored_videos[video_start_offset:video_end_offset]
                        
                        # Create custom link data for this range
                        custom_link_data = {
                            "type": "passlink_custom",
                            "tag": tag,
                            "start_index": start_index,
                            "end_index": end_index,
                            "videos": custom_videos
                        }
                        await send_passlink_videos(update, context, param, custom_link_data)
                        return
        else:
            # Simple tag format - serve all videos from any matching passlink
            for link_key, link_data in active_links.items():
                if isinstance(link_data, dict) and link_data.get("tag") == tag:
                    await send_passlink_videos(update, context, param, link_data)
                    return

        # Check if it's an individual video key (tag_index format)
        if '_' in param:
            parts = param.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                video_tag = '_'.join(parts[:-1])
                try:
                    video_index = int(parts[-1])
                    
                    # Check if this video exists in media_data
                    if video_tag in media_data and isinstance(media_data[video_tag], list):
                        if 0 <= video_index < len(media_data[video_tag]):
                            # Send the specific video
                            video_data = media_data[video_tag][video_index]
                            
                            # Create shareable link for this specific video
                            share_link = f"https://t.me/{BOT_USERNAME}?start={param}"
                            share_url = f"https://t.me/share/url?url={share_link}"
                            
                            keyboard = [
                                [
                                    InlineKeyboardButton("❤️ ADD", callback_data=f"add_fav_{param}"),
                                    InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
                                ]
                            ]
                            reply_markup = InlineKeyboardMarkup(keyboard)
                            
                            if video_data.get("type") == "video" and "file_id" in video_data:
                                # Build media caption
                                final_caption = build_media_caption("", video_tag, video_index, share_url, "video")
                                
                                await safe_send_message(
                                    context=context,
                                    chat_id=update.effective_chat.id,
                                    video=video_data["file_id"],
                                    caption=final_caption,
                                    reply_markup=reply_markup,
                                    parse_mode=ParseMode.HTML,
                                    protect_content=True
                                )
                            elif video_data.get("type") == "photo" and "file_id" in video_data:
                                # Build media caption
                                final_caption = build_media_caption("", video_tag, video_index, share_url, "photo")
                                
                                await safe_send_message(
                                    context=context,
                                    chat_id=update.effective_chat.id,
                                    photo=video_data["file_id"],
                                    caption=final_caption,
                                    reply_markup=reply_markup,
                                    parse_mode=ParseMode.HTML,
                                    protect_content=True
                                )
                            return
                except ValueError:
                    pass

        await send_tagged_with_range(update, context, tag, start_index,
                                     end_index)
        return

    text = (f"👋 Hey {mention}!!\n"
            "ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ <b>ᴍᴇᴏᴡ ɢᴀɴɢ</b> 💌\n"
            "══════════════════════════\n"
            "<b>ρєα¢є συт</b> ✌️\n\n")
    keyboard = InlineKeyboardMarkup([ [
    InlineKeyboardButton("🎬 GET FILES", callback_data="get_random"),
    InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
    ],
      [
        InlineKeyboardButton("❤️ MY FAVORITES", callback_data="view_favorites"),
    ], 
    [
    InlineKeyboardButton("BACKUP CHANNEL🛡️",
                 url=BACKUP_CHANNEL_URL)
    ],
    [
        InlineKeyboardButton("❓ HELP", callback_data="show_help"),
    ]])

     # Send image with caption instead of plain text
    await context.bot.send_photo(
        chat_id=update.effective_chat.id,
        photo=img_url,  # <-- Replace with your image URL or open("file.jpg","rb")
        caption=text,
        parse_mode="HTML",
        reply_markup=keyboard
    )

    if is_admin(user.id):
        admin_text = ("<b>💡 Admin Commands:</b>\n"
                     "<pre>/upload &lt;tag&gt;\n"
                     "/listvideos\n"
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
                     "/listfree\n"
                     "/userfavorites &lt;user_id&gt;\n"
                     "/videostats &lt;tag&gt; &lt;index&gt;\n"
                     "/topvideos\n\n"
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
                     "/list_admins - List all admins</pre>")
        await update.message.reply_html(admin_text)


async def handle_button_click(update: Update,
                              context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # Auto-register user (this handles all user tracking)
    await auto_register_user(update, context)

    if query.data == "get_random":
        # GET FILES button - only from "random" tag
        video_key = get_random_from_tag("random")
        
        if video_key and "_" in video_key:
            # Parse tag and index from video_key (format: tag_index)
            tag, idx_str = video_key.rsplit("_", 1)
            try:
                idx = int(idx_str)
                if tag in media_data and isinstance(media_data[tag], list) and 0 <= idx < len(media_data[tag]):
                    video_data = media_data[tag][idx]
                    
                    # Create shareable link for this specific video
                    share_link = f"https://t.me/{BOT_USERNAME}?start={video_key}"
                    share_url = f"https://t.me/share/url?url={share_link}"
                    
                    # Create inline keyboard with favorites, get files, and random media buttons
                    keyboard = [
                        [InlineKeyboardButton("❤️ ADD", callback_data=f"add_fav_{video_key}")],
                        [InlineKeyboardButton("🎬 GET FILES", callback_data="get_random")],
                        [InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    if video_data.get("type") == "video" and "file_id" in video_data:
                        base_caption = f"🎬 <b>GET FILES</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{idx}</code> | <a href='{share_url}'>🔗 Shareable Link</a>"
                        # Don't apply global caption to GET FILES button - just show the basic info
                        final_caption = build_final_caption(base_caption, add_global=False)
                        
                        await safe_send_message(
                            context=context,
                            chat_id=query.message.chat_id,
                            video=video_data["file_id"],
                            caption=final_caption,
                            reply_markup=reply_markup,
                            parse_mode=ParseMode.HTML,
                            protect_content=True
                        )
                    elif video_data.get("type") == "photo" and "file_id" in video_data:
                        base_caption = f"🖼️ <b>GET FILES</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{idx}</code> | <a href='{share_url}'>🔗 Shareable Link</a>"
                        # Don't apply global caption to GET FILES button - just show the basic info
                        final_caption = build_final_caption(base_caption, add_global=False)
                        
                        await safe_send_message(
                            context=context,
                            chat_id=query.message.chat_id,
                            photo=video_data["file_id"],
                            caption=final_caption,
                            reply_markup=reply_markup,
                            parse_mode=ParseMode.HTML,
                            protect_content=True
                        )
                    else:
                        await query.message.reply_text("❌ No valid media found for this entry.")
                else:
                    await query.message.reply_text("❌ Video not found in database.")
            except ValueError:
                await query.message.reply_text("❌ Invalid video format.")
        else:
            await query.message.reply_text("❌ No media files available in random tag.")

    elif query.data == "random_media":
        # RANDOM MEDIA button - only from passed links
        video_key = get_random_from_passed_links()
        
        if not video_key:
            await query.answer("❌ No passed videos available. Admin needs to pass some videos first.", show_alert=True)
            return
        
        if video_key and "_" in video_key:
            # Parse tag and index from video_key (format: tag_index)
            tag, idx_str = video_key.rsplit("_", 1)
            try:
                idx = int(idx_str)
                if tag in media_data and isinstance(media_data[tag], list) and 0 <= idx < len(media_data[tag]):
                    video_data = media_data[tag][idx]
                    
                    # Create shareable link for this specific video
                    share_link = f"https://t.me/{BOT_USERNAME}?start={video_key}"
                    share_url = f"https://t.me/share/url?url={share_link}"
                    
                    # Create inline keyboard with favorites and random media buttons
                    keyboard = [
                        [
                            InlineKeyboardButton("❤️ ADD", callback_data=f"add_fav_{video_key}"),
                            InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
                        ]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    if video_data.get("type") == "video" and "file_id" in video_data:
                        base_caption = f"🎬 <b>Random</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{idx}</code> | <a href='{share_url}'>🔗 Shareable Link</a>"
                        # Don't apply global caption to random button - just show the basic info
                        final_caption = build_final_caption(base_caption, add_global=False)
                        
                        await safe_send_message(
                            context=context,
                            chat_id=query.message.chat_id,
                            video=video_data["file_id"],
                            caption=final_caption,
                            reply_markup=reply_markup,
                            parse_mode=ParseMode.HTML,
                            protect_content=True
                        )
                    elif video_data.get("type") == "photo" and "file_id" in video_data:
                        base_caption = f"🖼️ <b>Random</b>\n\n📁 Tag: <code>{tag}</code> | Index: <code>{idx}</code> | <a href='{share_url}'>🔗 Shareable Link</a>"
                        # Don't apply global caption to random button - just show the basic info
                        final_caption = build_final_caption(base_caption, add_global=False)
                        
                        await safe_send_message(
                            context=context,
                            chat_id=query.message.chat_id,
                            photo=video_data["file_id"],
                            caption=final_caption,
                            reply_markup=reply_markup,
                            parse_mode=ParseMode.HTML,
                            protect_content=True
                        )
                    else:
                        await query.message.reply_text("❌ No valid media found for this entry.")
                else:
                    await query.message.reply_text("❌ Video not found in database.")
            except ValueError:
                await query.message.reply_text("❌ Invalid video format.")
        else:
            await query.message.reply_text("❌ No passed links available.")

    elif query.data == "view_favorites":
        await show_favorites_navigator(query, context, 0)

    elif query.data == "show_help":
        help_text = (
            "🤖 <b>How to Use the Bot:</b>\n\n"
            "❤️ <b>Favorites:</b>\n"
            "• Click ❤️ on any video to add to favorites\n"
            "• Click 💔 on any video to remove from favorites\n"
            "• Use ❤️ MY FAVORITES button to browse your collection\n"
            "• Navigate with ⬅️ Previous and ➡️ Next buttons\n\n"
            "🎬 <b>Getting Videos:</b>\n"
            "• Click 🎬 GET FILES for smart random videos\n"
            "• Click 🎲 RANDOM MEDIA for instant random content\n"
            "• Use shared links to access specific content\n\n"
            "🎯 <b>Smart Random System:</b>\n"
            "• Fair rotation ensures variety\n\n"
            "💡 <b>Tip:</b> Every video has ❤️ MY FAVORITES and 🎲 RANDOM MEDIA buttons for quick access!"
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
        await query.answer("📊 Top Videos Navigation", show_alert=False)
    
    elif query.data.startswith("topusers_page_"):
        # Handle top users navigation
        page = int(query.data.replace("topusers_page_", ""))
        await show_top_users_page(None, context, page, query)
    
    elif query.data == "topusers_info":
        # Just answer the query, do nothing (info button)
        await query.answer("👥 Top Users Navigation", show_alert=False)


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
    photo = message.photo[-1] if message.photo else None
    video = message.video
    file_id = photo.file_id if photo else video.file_id if video else None
    media_type = "photo" if photo else "video" if video else None

    if not file_id:
        return

    if media_group_id:
        # Store tag if this message has the upload command
        if caption and caption.lower().startswith("/upload "):
            tag = caption.split(" ", 1)[1].strip().lower()
            media_tag_map[media_group_id] = tag

        # Add file to buffer
        media_buffer[media_group_id].append({
            "file_id": file_id,
            "type": media_type,
            "msg_id": message.message_id
        })

        # Cancel existing task if any and create a new one
        if media_group_id in media_group_tasks:
            media_group_tasks[media_group_id].cancel()

        # Create new processing task with delay
        media_group_tasks[media_group_id] = asyncio.create_task(
            process_media_group(media_group_id))

        # Send confirmation for the last processed file
        if media_group_id in media_tag_map:
            tag = media_tag_map[media_group_id]
            current_count = len(media_buffer[media_group_id])
            await message.reply_text(
                f"📁 Collecting files for tag '{tag}' ({current_count} files so far)..."
            )
    else:
        # Single file upload
        if caption and caption.lower().startswith("/upload "):
            tag = caption.split(" ", 1)[1].strip().lower()
            media_data.setdefault(tag, []).append({
                "file_id": file_id,
                "type": media_type
            })
            save_media()
            await message.reply_text(
                f"✅ {media_type.capitalize()} uploaded under tag '{tag}'.")



















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
        # Check channel membership
        membership_failed = False
        for ch in REQUIRED_CHANNELS:
            is_member = await is_user_member(user_id, bot, ch)
            print(
                f"User {user_id} membership in {ch}: {is_member}")  # Debug log
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
                                     url=JOIN_CHANNEL_URL),
                InlineKeyboardButton("BACKUP CHANNEL🛡️",
                                     url=BACKUP_CHANNEL_URL)
            ],
             [
                 InlineKeyboardButton("🔄 Try Again",
                                      callback_data=f"retry_{retry_param}")
             ]])
        await query.message.reply_html(
            f"Hey {mention}!!\n"
            "Welcome to <b>Meow Gang</b> 🕊️\n\n"
            "To access videos/photos, make sure to join the listed channels and then click 'Try Again':",
            reply_markup=keyboard)
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
    
    for idx in range(start_index, end_index + 1):
            
        item = media_data[tag][idx]
        # Create unique shareable link for this specific file
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}_{idx}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
        
        # Build proper media caption with global caption and replacements
        media_type = item.get("type", "video")
        cap = build_media_caption("", tag, str(idx), share_link, media_type)
        
        # Create favorite button
        video_id = f"{tag}_{idx}"
        user_id_str = str(query.from_user.id)
        is_favorited = user_id_str in favorites_data["user_favorites"] and video_id in favorites_data["user_favorites"][user_id_str]
        
        if is_favorited:
            fav_button = InlineKeyboardButton("💔 Remove", 
                                            callback_data=f"remove_fav_{video_id}")
        else:
            fav_button = InlineKeyboardButton("❤️ ADD", 
                                            callback_data=f"add_fav_{video_id}")
        
        # Check if user is admin to show additional admin buttons
        if is_admin(query.from_user.id):
            who_liked_button = InlineKeyboardButton("👥 WHO", 
                                                  callback_data=f"who_liked_{video_id}")
            # Add persistent favorites and random media buttons at bottom
            my_favs_button = InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites")
            random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
            
            keyboard = InlineKeyboardMarkup([
                [fav_button, who_liked_button],
                [my_favs_button, random_button]
            ])
        else:
            # Regular user buttons
            my_favs_button = InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites")
            random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
            
            keyboard = InlineKeyboardMarkup([
                [fav_button],
                [my_favs_button, random_button]
            ])
        
        try:
            if item["type"] == "video":
                await safe_send_message(
                    context=context,
                    chat_id=query.message.chat_id,
                    video=item["file_id"],
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=True
                )
            else:
                await safe_send_message(
                    context=context,
                    chat_id=query.message.chat_id,
                    photo=item["file_id"],
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=True
                )
            sent_count += 1
            
        except Exception as e:
            print(f"Error sending media {idx}: {e}")
            await query.message.reply_text(f"❌ Error sending file {idx}")

        # Small delay to avoid rate limits
        await asyncio.sleep(2.0)
    
    # Send completion message with favorite reminder
    if sent_count > 0:
        completion_message = await safe_send_message(
            context=context,
            chat_id=query.message.chat_id,
            text=f"✅ <b>Media Delivery Complete!</b>\n\n"
                 f"📊 Sent {sent_count}/{total_files} files\n"
                 f"❤️ Don't forget to save your favorites!\n\n"
                 f"💡 Use ❤️ ADD button to save videos permanently.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❤️ MY FAVORITES", callback_data="view_favorites")]
            ])
        )
        
    
    # Clear operation when complete
    clear_user_operation(user_id)


async def send_tagged_from_callback(query, context: ContextTypes.DEFAULT_TYPE,
                                    tag: str):
    await send_tagged_from_callback_with_range(query, context, tag)


async def send_passlink_videos(update: Update, context: ContextTypes.DEFAULT_TYPE, link_key: str, link_data: dict):
    """Send videos stored in active_links for /passlink created links"""
    user_id = update.effective_user.id
    bot = context.bot
    user = update.effective_user
    mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'

    # Check if user is exempted from channel requirements
    if user_id in exempted_users:
        print(f"User {user_id} is exempted from channel requirements")
        membership_failed = False
    else:
        # Check channel membership
        membership_failed = False
        for ch in REQUIRED_CHANNELS:
            is_member = await is_user_member(user_id, bot, ch)
            print(f"User {user_id} membership in {ch}: {is_member}")
            if not is_member:
                membership_failed = True
                break

    if membership_failed:
        clear_user_operation(user_id)
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("JOIN✨", url=JOIN_CHANNEL_URL),
                InlineKeyboardButton("BACKUP CHANNEL🛡️", url=BACKUP_CHANNEL_URL)
            ],
             [
                 InlineKeyboardButton("🔄 Try Again", callback_data=f"retry_{link_key}")
             ]])
        await update.message.reply_html(
            f"Hey {mention}!!\n"
            "Welcome to <b>Meow Gang</b> 🕊️\n\n"
            "To access videos/photos, make sure to join the listed channels and then click 'Try Again':",
            reply_markup=keyboard)
        return

    videos = link_data.get("videos", [])
    tag = link_data.get("tag", "unknown")
    start_idx = link_data.get("start_index", 0)
    
    if not videos:
        await update.message.reply_text("❌ No videos found in this shareable link.")
        # Don't clear operation immediately - give user time to see the error message
        await asyncio.sleep(2)  # Give user time to test /stop command
        clear_user_operation(user_id)
        return

    # Send each video with proper indexing
    total_files = len(videos)
    sent_count = 0
    
    # Send initial progress message for large collections
    # No starting notification for passlinks
    
    for idx, item in enumerate(videos):
            
        actual_index = start_idx + idx
        # Create unique shareable link for this specific file
        file_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{actual_index}_{actual_index}"
        share_link = f"https://telegram.me/share/url?url={file_link}"
        
        # Build proper media caption with global caption and replacements
        media_type = item.get("type", "video")
        cap = build_media_caption("", tag, str(actual_index), share_link, media_type)
        
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
            who_liked_button = InlineKeyboardButton("👥 WHO", 
                                                  callback_data=f"who_liked_{video_id}")
            my_favs_button = InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites")
            random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
            
            keyboard = InlineKeyboardMarkup([
                [fav_button, who_liked_button],
                [my_favs_button, random_button]
            ])
        else:
            my_favs_button = InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites")
            random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
            
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
                    protect_content=True
                )
            else:
                await safe_send_message(
                    context=context,
                    chat_id=update.message.chat_id,
                    photo=item["file_id"],
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=True
                )
            sent_count += 1
            
                
        except Exception as e:
            await update.message.reply_text(f"❌ Error sending media: {str(e)}")
            continue

        # Add delay to avoid rate limiting
        await asyncio.sleep(1.5)
    
    # Send completion message with favorite reminder
    if sent_count > 0:
        completion_message = await safe_send_message(
            context=context,
            chat_id=update.message.chat_id,
            text=f"📊 Sent {sent_count}/{total_files} files\n"
                 f"❤️ Don't forget to save your favorites!\n\n"
                 f"💡 Use ❤️ ADD button to save videos permanently.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❤️ MY FAVORITES", callback_data="view_favorites")]
            ])
        )
        
    
    # Clear operation when complete
    clear_user_operation(user_id)


async def send_tagged_with_range(update: Update,
                                 context: ContextTypes.DEFAULT_TYPE,
                                 tag: str,
                                 start_index=None,
                                 end_index=None):
    user_id = update.effective_user.id
    bot = context.bot

    user = update.effective_user
    mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'

    # Check if user is exempted from channel requirements
    if user_id in exempted_users:
        print(f"User {user_id} is exempted from channel requirements")
        membership_failed = False
    else:
        # Check channel membership
        membership_failed = False
        for ch in REQUIRED_CHANNELS:
            is_member = await is_user_member(user_id, bot, ch)
            print(
                f"User {user_id} membership in {ch}: {is_member}")  # Debug log
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
                                     url="https://t.me/BeingHumanAssociation"),
                InlineKeyboardButton("BACKUP CHANNEL🛡️",
                                     url="https://t.me/bhaicharabackup")
            ],
             [
                 InlineKeyboardButton("🔄 Try Again",
                                      callback_data=f"retry_{retry_param}")
             ]])
        await update.message.reply_html(
            f"Hey {mention}!!\n"
            "Welcome to <b>Meow Gang</b> 🕊️\n\n"
            "To access videos/photos, make sure to join the listed channels and then click 'Try Again':",
            reply_markup=keyboard)
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

    # Send media files in the specified range
    total_files = end_index - start_index + 1
    sent_count = 0
    
    # Send videos without starting notification
    
    for idx in range(start_index, end_index + 1):
            
        item = media_data[tag][idx]
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
            who_liked_button = InlineKeyboardButton("👥 WHO", 
                                                  callback_data=f"who_liked_{video_id}")
            # Add persistent favorites and random media buttons at bottom
            my_favs_button = InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites")
            random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
            
            keyboard = InlineKeyboardMarkup([
                [fav_button, who_liked_button],
                [my_favs_button, random_button]
            ])
        else:
            # Regular user buttons
            my_favs_button = InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites")
            random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
            
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
                    protect_content=True
                )
            else:
                await safe_send_message(
                    context=context,
                    chat_id=update.message.chat_id,
                    photo=item["file_id"],
                    caption=cap,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    protect_content=True
                )
            sent_count += 1
                
        except Exception as e:
            print(f"Error sending media {idx}: {e}")
            await safe_send_message(
                context=context,
                chat_id=update.message.chat_id,
                text=f"❌ Error sending file {idx}"
            )

        # Small delay to avoid rate limits
        await asyncio.sleep(1.5)
    
    # Send completion message with favorite reminder
    if sent_count > 0:
        completion_message = await safe_send_message(
            context=context,
            chat_id=update.message.chat_id,
            text=f"✅ <b>Media Delivery Complete!</b>\n\n"
                 f"📊 Sent {sent_count}/{total_files} files\n"
                 f"❤️ Don't forget to save your favorites!\n\n"
                 f"💡 Use ❤️ ADD button to save videos permanently.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❤️ MY FAVORITES", callback_data="view_favorites")]
            ])
        )
        
    
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


async def listvideos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    if not media_data:
        await update.message.reply_text("📂 No uploads yet.")
        return

    msg = "<b>🗂 Uploaded Tags:</b>\n"
    for tag, files in media_data.items():
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
        del media_data[tag][index]
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
    del media_data[tag][start_index:end_index + 1]
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
        item = media_data[tag][idx]
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
        # Add persistent favorites and random media buttons at bottom
        my_favs_button = InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
        
        keyboard = InlineKeyboardMarkup([
            [fav_button, who_liked_button],
            [my_favs_button, random_button]
        ])

        try:
            if item["type"] == "video":
                await update.message.reply_video(item["file_id"],
                                                 caption=cap,
                                                 parse_mode=ParseMode.HTML,
                                                 protect_content=True,
                                                 reply_markup=keyboard)
            else:
                await update.message.reply_photo(item["file_id"],
                                                 caption=cap,
                                                 parse_mode=ParseMode.HTML,
                                                 protect_content=True,
                                                 reply_markup=keyboard)
        except Exception as e:
            print(f"Error sending media {idx}: {e}")
            await update.message.reply_text(f"❌ Error sending file at index {idx}")

        # Small delay to avoid rate limits (and allow for stop command processing)
        await asyncio.sleep(2.0)  # Increased delay for better /stop testing


async def free(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /free <user_id>")
        return

    try:
        user_id = int(context.args[0].strip())
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


async def listfree(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not exempted_users:
        await update.message.reply_text("📂 No exempted users.")
        return

    msg = "<b>🆓 Exempted Users:</b>\n"
    for user_id in exempted_users:
        msg += f"<code>{user_id}</code>\n"
    await update.message.reply_html(msg)


async def pass_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pass videos to RANDOM MEDIA button only (no shareable links)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text("Usage: /pass <tag> [start_index] [end_index]")
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
            f"🎲 {len(all_indices)} videos available via RANDOM MEDIA button",
            parse_mode=ParseMode.HTML
        )
    else:
        range_text = f"'{tag}' (indices {start_index}-{end_index})" if start_index != 0 or end_index != len(tag_videos) - 1 else f"entire tag '{tag}'"
        await update.message.reply_text(
            f"ℹ️ All videos in {range_text} were already passed.",
            parse_mode=ParseMode.HTML
        )


async def pass_link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pass videos and create shareable links (for both RANDOM MEDIA button and generated links)"""
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
        
        # Get videos for all indices
        all_videos = []
        for idx in sorted(all_indices):
            if 0 <= idx < len(tag_videos):
                all_videos.append(tag_videos[idx])
        
        # Determine the link key and range for storage
        min_index = min(all_indices)
        max_index = max(all_indices)
        
        if min_index == 0 and max_index == len(tag_videos) - 1:
            # Full tag - store just tag name
            link_key = tag
            link = f"https://t.me/{BOT_USERNAME}?start={tag}"
            range_text = f"entire tag '{tag}'"
        else:
            # Specific range - store range key
            link_key = f"{tag}_{min_index}_{max_index}"
            link = f"https://t.me/{BOT_USERNAME}?start={tag}_{min_index}_{max_index}"
            range_text = f"'{tag}' (updated collection {min_index}-{max_index})"
        
        # Remove old entry if it has a different key
        if existing_entry != link_key:
            del active_links[existing_entry]
    else:
        # Create separate entry (no merging)
        all_indices = set(range(start_index, end_index + 1))
        
        # Get videos for this range only
        all_videos = []
        for idx in range(start_index, end_index + 1):
            if 0 <= idx < len(tag_videos):
                all_videos.append(tag_videos[idx])
        
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
    
    # Remove old entry if it exists and has a different key
    if should_merge and existing_entry and existing_entry != link_key and existing_entry in active_links:
        del active_links[existing_entry]
    
    was_new_entry = link_key not in active_links
    # Store the video data directly in active_links
    if should_merge:
        min_index = min(all_indices)
        max_index = max(all_indices)
    else:
        min_index = start_index
        max_index = end_index
        
    active_links[link_key] = {
        "type": "passlink",
        "tag": tag,
        "start_index": min_index,
        "end_index": max_index,
        "videos": all_videos,
        "actual_indices": sorted(list(all_indices))  # Store actual indices for display
    }
    save_active_links()
    
    if was_new_entry or (should_merge and existing_entry):
        action_text = "✅ Created" if was_new_entry and not should_merge else "🔄 Updated"
        await update.message.reply_text(
            f"{action_text} shareable link for {range_text}\n"
            f"📱 Shareable link: {link}\n"
            f"🔗 Link activated for sharing ({len(all_videos)} videos stored)\n"
            f"ℹ️ Videos NOT added to RANDOM MEDIA button",
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            f"ℹ️ Shareable link for {range_text} already exists.",
            parse_mode=ParseMode.HTML
        )


async def revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Revoke videos from RANDOM MEDIA button (removes from passed_links)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) < 1 or len(context.args) > 3:
        await update.message.reply_text("Usage: /revoke <tag> [start_index] [end_index]")
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
            f"🎲 Videos removed from RANDOM MEDIA button",
            parse_mode=ParseMode.HTML
        )
    else:
        range_text = f"'{tag}' (indices {start_index}-{end_index})" if start_index != 0 or end_index != len(tag_videos) - 1 else f"entire tag '{tag}'"
        await update.message.reply_text(
            f"ℹ️ No videos in {range_text} were in RANDOM MEDIA.",
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

    # If exact match not found and this is a range revocation, check if we need to modify existing ranges
    if revoke_start is not None and revoke_end is not None:
        modified_any = False
        keys_to_remove = []
        keys_to_add = {}
        
        for existing_key, link_data in active_links.items():
            if isinstance(link_data, dict) and link_data.get("tag") == tag:
                stored_start = link_data.get("start_index", 0)
                stored_end = link_data.get("end_index", 0)
                stored_videos = link_data.get("videos", [])
                actual_indices = link_data.get("actual_indices", list(range(stored_start, stored_end + 1)))
                
                # Check if any of the revoke indices are in the actual stored indices
                revoke_indices = set(range(revoke_start, revoke_end + 1))
                stored_indices_set = set(actual_indices)
                
                if revoke_indices.intersection(stored_indices_set):
                    # There's an overlap - we need to modify this range
                    keys_to_remove.append(existing_key)
                    
                    # Calculate what remains after revoking
                    remaining_indices = sorted(list(stored_indices_set - revoke_indices))
                    
                    if remaining_indices:
                        # Get videos for remaining indices
                        remaining_videos = []
                        for idx in remaining_indices:
                            # Find the position of this index in the original actual_indices
                            if idx in actual_indices:
                                original_pos = actual_indices.index(idx)
                                if original_pos < len(stored_videos):
                                    remaining_videos.append(stored_videos[original_pos])
                        
                        if remaining_videos:
                            # Create new entry with remaining data
                            new_start = min(remaining_indices)
                            new_end = max(remaining_indices)
                            
                            if new_start == new_end:
                                new_key = f"{tag}_{new_start}_{new_end}"
                            else:
                                new_key = f"{tag}_{new_start}_{new_end}"
                            
                            keys_to_add[new_key] = {
                                "type": "passlink",
                                "tag": tag,
                                "start_index": new_start,
                                "end_index": new_end,
                                "videos": remaining_videos,
                                "actual_indices": remaining_indices
                            }
                    
                    modified_any = True
        
        # Apply the modifications
        for key in keys_to_remove:
            del active_links[key]
        
        for key, data in keys_to_add.items():
            active_links[key] = data
        
        if modified_any:
            save_active_links()
            revoked_count = (revoke_end - revoke_start + 1)
            await update.message.reply_text(
                f"🚫 Shareable link access for {range_text} has been revoked.\n"
                f"📊 {revoked_count} videos removed from shareable access.\n"
                f"✅ Existing ranges have been updated to exclude the revoked indices.",
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
    
    msg += "\n💡 These links are completely independent from RANDOM MEDIA button"
    await update.message.reply_html(msg)


async def passlinks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show active links for RANDOM MEDIA button (from /pass command)"""
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
        await update.message.reply_text("📂 No active links for RANDOM MEDIA button.")
        return

    msg = "<b>🎲 Active Links for RANDOM MEDIA Button:</b>\n\n"
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
        
        msg += f"🎲 <code>{tag}</code> ({range_info}) - {link}\n"
    
    msg += "\n💡 These links affect the RANDOM MEDIA button"
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
                source = "🔗" if link_key in active_links else "🎲"
                # Show video count for passlink created links
                video_count = ""
                if link_key in active_links and isinstance(active_links[link_key], dict):
                    videos = active_links[link_key].get("videos", [])
                    video_count = f" ({len(videos)} videos)"
                msg += f"{source} <code>{tag}</code> ({start_idx}-{end_idx}){video_count} - {link}\n"
            else:
                # Not a range, just display as is
                source = "🔗" if link_key in active_links else "🎲"
                video_count = ""
                if link_key in active_links and isinstance(active_links[link_key], dict):
                    videos = active_links[link_key].get("videos", [])
                    video_count = f" ({len(videos)} videos)"
                msg += f"{source} <code>{link_key}</code>{video_count} - {link}\n"
        else:
            # Simple tag format
            source = "🔗" if link_key in active_links else "🎲"
            video_count = ""
            if link_key in active_links and isinstance(active_links[link_key], dict):
                videos = active_links[link_key].get("videos", [])
                video_count = f" ({len(videos)} videos)"
            msg += f"{source} <code>{link_key}</code>{video_count} - {link}\n"
    
    msg += "\n🔗 = Created with /passlink (independent storage)\n🎲 = Created with /pass (RANDOM MEDIA access)"
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
            cap = f"❤️ <b>Favorite {index + 1}/{total_favorites}</b>\n{base_caption}"
            
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
            share_link = f"https://t.me/{BOT_USERNAME}?start={tag}_{idx}"
            share_url = f"https://t.me/share/url?url={share_link}"
            nav_buttons.append([
                InlineKeyboardButton("🔗 Share this Video", url=share_url)
            ])
            
            # Add favorites and random media buttons at bottom
            nav_buttons.append([
                InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites"),
                InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
            ])
            
            keyboard = InlineKeyboardMarkup(nav_buttons)
            
            if edit_message:
                # For navigation, we edit the existing message
                if item["type"] == "video":
                    await safe_send_message(
                        context=context,
                        chat_id=query.message.chat_id,
                        video=item["file_id"],
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=True
                    )
                else:
                    await safe_send_message(
                        context=context,
                        chat_id=query.message.chat_id,
                        photo=item["file_id"],
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=True
                    )
            else:
                # For first time viewing, send new message
                if item["type"] == "video":
                    await safe_send_message(
                        context=context,
                        chat_id=query.message.chat_id,
                        video=item["file_id"],
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=True
                    )
                else:
                    await safe_send_message(
                        context=context,
                        chat_id=query.message.chat_id,
                        photo=item["file_id"],
                        caption=cap,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        protect_content=True
                    )
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
                    await query.edit_message_text("❤️ No more favorites available!")
                else:
                    await query.message.reply_text("❤️ No more favorites available!")
    
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
        
        # Update the button
        new_fav_button = InlineKeyboardButton("💔 Remove from Favorites", 
                                        callback_data=f"remove_fav_{video_id}")
        my_favs_button = InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
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
        
        # Update the button
        new_fav_button = InlineKeyboardButton("❤️ Add to Favorites", 
                                        callback_data=f"add_fav_{video_id}")
        my_favs_button = InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
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
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ Admin only feature!")
        return
    
    # Extract video_id from callback data
    video_id = query.data.replace("view_video_", "")
    
    try:
        # Parse video_id to get tag and index
        tag, idx_str = video_id.rsplit("_", 1)
        idx = int(idx_str)
        
        # Check if video exists in database
        if tag not in media_data or not isinstance(media_data[tag], list) or idx >= len(media_data[tag]):
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
        fav_button = InlineKeyboardButton("❤️ Add to Favorites", 
                                        callback_data=f"add_fav_{video_id}")
        who_liked_button = InlineKeyboardButton("👥 WHO", 
                                              callback_data=f"who_liked_{video_id}")
        my_favs_button = InlineKeyboardButton("❤️ MY FAV", callback_data="view_favorites")
        random_button = InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
        
        keyboard = InlineKeyboardMarkup([
            [fav_button, who_liked_button],
            [my_favs_button, random_button]
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
                protect_content=True
            )
        elif video_data.get("type") == "photo" and "file_id" in video_data:
            await safe_send_message(
                context=context,
                chat_id=query.message.chat_id,
                photo=video_data["file_id"],
                caption=cap,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                protect_content=True
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
    """Admin command to see most liked videos with navigation"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not favorites_data["video_likes"]:
        await update.message.reply_text("📊 No video likes data yet.")
        return
    
    # Send the first page
    await show_top_videos_page(update, context, 0)


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
    
    # Add refresh button
    keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data="topvideos_page_0")])
    
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
        await update.message.reply_text("❤️ You haven't added any videos to favorites yet!")
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
        "🤖 <b>Bhaichara Bot - Help Guide</b>\n\n"
        "❤️ <b>User Commands:</b>\n"
        "• <b>/start</b> - Start the bot and see main menu\n"
        "• <b>/favorites</b> - View your favorite videos with navigation\n"
        "• <b>/help</b> - Show this help message\n\n"
        
        "🎬 <b>How to Use:</b>\n"
        "• Click 🗂️ <b>GET FILES</b> for smart random videos from all content\n"
        "• Click 🎲 <b>RANDOM MEDIA</b> for random videos from passed collection\n"
        "• Click ❤️ on any video to add to your favorites\n"
        "• Use ❤️ <b>MY FAVORITES</b> button for quick access to your collection\n"
        "• Navigate with ⬅️ <b>Previous</b> and ➡️ <b>Next</b> buttons\n\n"
        
        "⚡ <b>Quick Shortcuts:</b>\n"
        "Type these words for instant access:\n"
        "• <code>fav, favorites, ❤️</code> → Open favorites\n"
        "• <code>random, rand, 🎲, media</code> → Get random video\n\n"
        
        "🎯 <b>Smart Features:</b>\n"
        "• <b>Two-Tier Random System:</b> GET FILES shows all content, RANDOM MEDIA shows curated content\n"
        "• <b>Smart Rotation:</b> All videos shown before any repeats\n"
        "• <b>Persistent Favorites:</b> Your favorites are saved permanently\n"
        "• <b>Navigation Memory:</b> Bot remembers your position in favorites\n"
        "• <b>Real-time Updates:</b> Instant feedback on all actions\n\n"
        
        "💡 <b>Pro Tips:</b>\n"
        "• Every video has quick access buttons for favorites and random content\n"
        "• Use favorites to build your personal collection\n"
        "• Random system ensures fair variety without repetition\n"
        "• All interactions are private and secure"
    )
    
    # Add admin commands if user is admin
    if user_id == ADMIN_ID:
        admin_help = [
            "\n\n🔧 <b>Admin Commands:</b>\n"
            "• <b>/upload</b> - Upload new media to database\n"
            "• <b>/listvideos [tag]</b> - List all videos in a tag\n"
            "• <b>/remove &lt;tag&gt; &lt;index&gt;</b> - Remove specific video\n"
            "• <b>/get &lt;tag&gt;</b> - Get all videos from a tag\n"
            "• <b>/generatelink &lt;tag&gt;</b> - Generate public link for tag\n"
            "• <b>/view &lt;tag&gt; &lt;index&gt;</b> - View specific video by index\n"
            "• <b>/pass &lt;tag&gt; [start] [end]</b> - Pass videos to RANDOM MEDIA button ONLY\n"
            "• <b>/passlink &lt;tag&gt; [start] [end]</b> - Create shareable links ONLY (not for RANDOM MEDIA)\n"
            "• <b>/revoke &lt;tag&gt; [start] [end]</b> - Remove from RANDOM MEDIA button ONLY\n"
            "• <b>/revokelink &lt;tag&gt; [start] [end]</b> - Revoke shareable link access ONLY\n"
            "• <b>/activelinks</b> - List active links for RANDOM MEDIA button\n"
            "• <b>/passlinks</b> - List shareable links (independent storage)\n"
            "• <b>/listactive</b> - List all active links (both types)\n"
            "• <b>/free &lt;tag&gt;</b> - Make tag freely accessible\n"
            "• <b>/listfree</b> - List all free tags\n\n"
            
            "� <b>Analytics Commands:</b>\n"
            "• <b>/userfavorites &lt;user_id&gt;</b> - View user's favorites\n"
            "• <b>/videostats &lt;tag&gt; &lt;index&gt;</b> - See who liked a video\n"
            "• <b>/topvideos [limit]</b> - Most liked videos with direct access\n\n"
            
            "🎲 <b>Command Examples:</b>\n"
            "• <code>/pass bulk</code> (add entire bulk tag to RANDOM MEDIA only)\n"
            "• <code>/pass bulk 0 49</code> (add videos 0-49 to RANDOM MEDIA only)\n"
            "• <code>/passlink bulk</code> (create shareable link for entire tag only)\n"
            "• <code>/passlink bulk 0 49</code> (create shareable link for range only)\n"
            "• <code>/revoke bulk</code> (remove entire bulk from RANDOM MEDIA only)\n"
            "• <code>/revoke bulk 0 49</code> (remove range from RANDOM MEDIA only)\n"
            "• <code>/revokelink bulk</code> (disable shareable link only)\n"
            "• <code>/revokelink bulk 0 49</code> (disable specific range link only)\n\n"
            
            "⚠️ <b>Important:</b> Commands are now completely separate!\n"
            "• Use <code>/pass</code> for RANDOM MEDIA button access\n"
            "• Use <code>/passlink</code> for shareable links only\n"
            "• Each system works independently\n\n"
            
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
    text = update.message.text.lower().strip()
    
    # Track user interaction
    user = update.effective_user
    track_user(user.id, user.username, user.first_name)
    
    if text in ["fav", "favorites", "❤️", "♥️"]:
        # Redirect to favorites
        await favorites_command(update, context)
        return
    
    if text in ["random", "rand", "🎲", "🎬", "media"]:
        # Send random media
        video_key = get_next_random_video()
        
        if video_key and "_" in video_key:
            # Parse tag and index from video_key (format: tag_index)
            tag, idx_str = video_key.rsplit("_", 1)
            try:
                idx = int(idx_str)
                if tag in media_data and isinstance(media_data[tag], list) and 0 <= idx < len(media_data[tag]):
                    video_data = media_data[tag][idx]
                    
                    # Create inline keyboard with favorites and random media buttons
                    keyboard = [
                        [
                            InlineKeyboardButton("❤️ ADD", callback_data=f"add_fav_{video_key}"),
                            InlineKeyboardButton("🎲 RANDOM", callback_data="random_media")
                        ]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    if video_data.get("type") == "video" and "file_id" in video_data:
                        await update.message.reply_video(
                            video_data["file_id"],
                            caption=f"🎬 <b>Random Video</b>\n\n📁 <i>{tag}</i> | Index: <i>{idx}</i>",
                            parse_mode=ParseMode.HTML,
                            protect_content=True,
                            reply_markup=reply_markup
                        )
                    elif video_data.get("type") == "photo" and "file_id" in video_data:
                        await update.message.reply_photo(
                            video_data["file_id"],
                            caption=f"🖼️ <b>Random Photo</b>\n\n📁 <i>{tag}</i> | Index: <i>{idx}</i>",
                            parse_mode=ParseMode.HTML,
                            protect_content=True,
                            reply_markup=reply_markup
                        )
                    else:
                        await update.message.reply_text("❌ No valid media found.")
                else:
                    await update.message.reply_text("❌ Video not found in database.")
            except ValueError:
                await update.message.reply_text("❌ Invalid video format.")
        else:
            await update.message.reply_text("❌ No media files available.")
        return
    
    if text in ["help", "?"]:
        # Redirect to help
        await help_command(update, context)
        return


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


# =================== END BROADCASTING SYSTEM ===================


# =================== BATCH PROCESSING SYSTEM ===================

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
        parse_mode=ParseMode.HTML
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
        
        # Extract media information
        photo = message.photo[-1] if message.photo else None
        video = message.video
        file_id = photo.file_id if photo else video.file_id if video else None
        media_type = "photo" if photo else "video" if video else None
        
        if file_id and media_type:
            # Capture original caption for replacement processing
            original_caption = message.caption or ""
            
            # Add to batch session
            media_item = {
                "file_id": file_id,
                "type": media_type,
                "original_caption": original_caption
            }
            
            custom_batch_sessions[user_id]['media_list'].append(media_item)
            collected_count = len(custom_batch_sessions[user_id]['media_list'])
            tag = custom_batch_sessions[user_id]['tag']
            
            await message.reply_text(
                f"✅ Media #{collected_count} added to batch\n"
                f"📁 Tag: {tag}\n"
                f"📊 Total collected: {collected_count} files"
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


# =================== END ADMIN MANAGEMENT SYSTEM ===================


def main():
    keep_alive()  # Remove this line if not using Replit
    # ================== BOT TOKEN (imported from config.py) ==================
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Initialize random state
    update_random_state()
    
    # Migrate existing users to users database
    migrate_existing_users()
    
    # Set bot commands for the menu
    commands = [
        BotCommand("start", "🏠 Start the bot"),
        BotCommand("stop", "🛑 Stop ongoing operations"),
        BotCommand("favorites", "❤️ View your favorites"),
        BotCommand("help", "❓ Get help")
    ]

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("upload", upload))
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
    app.add_handler(CommandHandler("topusers", top_users_command))
    app.add_handler(CommandHandler("addusers", add_users_command))
    app.add_handler(CommandHandler("discover", discover_users_command))
    app.add_handler(CommandHandler("custom_batch", custom_batch_command))
    app.add_handler(CommandHandler("stop_batch", stop_batch_command))
    app.add_handler(CommandHandler("cancel_batch", cancel_batch_command))
    app.add_handler(CommandHandler("batch_status", batch_status_command))
    app.add_handler(CommandHandler("set_global_caption", set_global_caption_command))
    app.add_handler(CommandHandler("add_admin", add_admin_command))
    app.add_handler(CommandHandler("remove_admin", remove_admin_command))
    app.add_handler(CommandHandler("list_admins", list_admins_command))
    app.add_handler(CommandHandler("userfavorites", user_favorites_admin))
    app.add_handler(CommandHandler("videostats", video_stats_admin))
    app.add_handler(CommandHandler("topvideos", top_videos_admin))
    app.add_handler(CallbackQueryHandler(handle_button_click))
    
    # Universal handlers to auto-register all users
    app.add_handler(MessageHandler(filters.ALL, auto_register_user), group=1)
    app.add_handler(CallbackQueryHandler(auto_register_user), group=1)
    
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO, upload))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_shortcuts))

    # Set bot commands menu
    asyncio.get_event_loop().run_until_complete(
        app.bot.set_my_commands(commands)
    )
    print("🤖 Bot running...")
    app.run_polling()


if __name__ == "__main__":
    main()
