import os
import shutil

# Files to be removed
files_to_remove = [
    "test_connectivity.py",    # Empty file
    "reupload_bot.py",         # Empty file
    "check_files.py",          # Empty file
    "manage_media.py",         # Empty file
    "temp_fix.py",             # Temporary fix script
    "debug_bot.py",            # Debugging utility
    "test_bot_token.py"        # Testing utility
]

# Count for reporting
removed_count = 0
error_count = 0

print("🧹 Cleaning up unnecessary files...")

for file in files_to_remove:
    try:
        if os.path.exists(file):
            os.remove(file)
            print(f"✅ Removed: {file}")
            removed_count += 1
        else:
            print(f"⚠️ File not found: {file}")
    except Exception as e:
        print(f"❌ Error removing {file}: {e}")
        error_count += 1

# Replace the old README with the new one
try:
    if os.path.exists("README.md.new"):
        # Backup the old README first
        if os.path.exists("README.md"):
            shutil.copy2("README.md", "README.md.backup")
            print("✅ Created backup of old README.md")
        
        # Replace README
        shutil.move("README.md.new", "README.md")
        print("✅ Updated README.md with new version")
except Exception as e:
    print(f"❌ Error updating README: {e}")
    error_count += 1

print("\n🏁 Cleanup completed!")
print(f"   - {removed_count} files removed")
print(f"   - {error_count} errors encountered")
print("\nYour Knowledge Vault Bot is now cleaner and better organized!")