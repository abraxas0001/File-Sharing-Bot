"""
Knowledge Vault Bot - Database Initialization Script
---------------------------------------------------
This script creates empty JSON database files for first-time setup.
"""

import os
import json

# List of essential database files with their default structure
DB_FILES = {
    "media_db.json": {},
    "users_db.json": {},
    "favorites_db.json": {"user_favorites": {}},
    "active_links.json": {},
    "passed_links.json": [],
    "admin_list.json": [],
    "deleted_media.json": {},
    "user_preferences.json": {},
    "exempted_users.json": [],
    "autodelete_config.json": {"enabled": False, "hours": 24},
    "caption_config.json": {"global_caption": ""},
    "last_update_offset.json": {"offset": 0},
    "random_state.json": {"all_videos": [], "shown_videos": [], "tag_shown_videos": {}}
}

def initialize_database():
    """Create empty database files if they don't exist."""
    files_created = 0
    files_skipped = 0
    
    print("🛠️ Initializing Knowledge Vault Bot database files...")
    
    for filename, default_data in DB_FILES.items():
        if not os.path.exists(filename):
            try:
                with open(filename, 'w') as f:
                    json.dump(default_data, f)
                print(f"✅ Created: {filename}")
                files_created += 1
            except Exception as e:
                print(f"❌ Error creating {filename}: {e}")
        else:
            print(f"ℹ️ Skipped: {filename} (already exists)")
            files_skipped += 1
    
    print("\n🏁 Database initialization completed!")
    print(f"   - {files_created} files created")
    print(f"   - {files_skipped} files skipped (already exist)")
    
    if files_created > 0:
        print("\n⚠️ IMPORTANT: Your bot now has empty database files.")
        print("   - Add at least one admin ID to admin_list.json")
        print("   - Set up your environment variables (BOT_TOKEN, ADMIN_ID)")
        print("   - Upload some content to populate the media database")
    
    print("\nYour Knowledge Vault Bot is now ready for setup!")

if __name__ == "__main__":
    initialize_database()