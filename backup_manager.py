#!/usr/bin/env python3
"""
Backup Utility for Bhaichara Bot Database
Handles backup and restore of all JSON database files
"""

import os
import json
import shutil
import datetime
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple

class DatabaseBackupManager:
    """Manages backup and restore operations for bot database files"""

    def __init__(self, data_dir: str = "data", backup_dir: str = "backups"):
        """
        Initialize backup manager

        Args:
            data_dir: Directory containing JSON files (default: 'data')
            backup_dir: Directory to store backups (default: 'backups')
        """
        self.data_dir = Path(data_dir)
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)

        # List of all database files to backup
        self.db_files = [
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

        # Create data directory if it doesn't exist
        self.data_dir.mkdir(exist_ok=True)

    def get_file_hash(self, file_path: Path) -> str:
        """Get SHA256 hash of a file"""
        if not file_path.exists():
            return ""

        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    def get_backup_info(self, backup_name: str) -> Dict:
        """Get information about a backup"""
        backup_path = self.backup_dir / backup_name
        info_file = backup_path / "backup_info.json"

        if not info_file.exists():
            return {}

        try:
            with open(info_file, 'r') as f:
                return json.load(f)
        except:
            return {}

    def create_backup(self, backup_name: Optional[str] = None, description: str = "") -> Tuple[bool, str]:
        """
        Create a backup of all database files

        Args:
            backup_name: Name for the backup (auto-generated if None)
            description: Description of the backup

        Returns:
            Tuple of (success, message)
        """
        try:
            # Generate backup name if not provided
            if not backup_name:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_name = f"backup_{timestamp}"

            backup_path = self.backup_dir / backup_name
            backup_path.mkdir(exist_ok=True)

            backup_info = {
                "name": backup_name,
                "created_at": datetime.datetime.now().isoformat(),
                "description": description,
                "files": {},
                "total_files": 0,
                "total_size": 0
            }

            files_backed_up = 0
            total_size = 0

            # Backup each database file
            for filename in self.db_files:
                src_path = self.data_dir / filename

                # Check if file exists (some might not exist yet)
                if src_path.exists():
                    # Copy file to backup directory
                    dst_path = backup_path / filename
                    shutil.copy2(src_path, dst_path)

                    # Get file info
                    file_size = src_path.stat().st_size
                    file_hash = self.get_file_hash(src_path)

                    backup_info["files"][filename] = {
                        "size": file_size,
                        "hash": file_hash,
                        "backed_up": True
                    }

                    files_backed_up += 1
                    total_size += file_size
                else:
                    backup_info["files"][filename] = {
                        "size": 0,
                        "hash": "",
                        "backed_up": False,
                        "reason": "file_not_found"
                    }

            backup_info["total_files"] = files_backed_up
            backup_info["total_size"] = total_size

            # Save backup info
            info_file = backup_path / "backup_info.json"
            with open(info_file, 'w') as f:
                json.dump(backup_info, f, indent=2)

            return True, f"Backup '{backup_name}' created successfully. {files_backed_up} files backed up ({total_size} bytes)"

        except Exception as e:
            return False, f"Backup failed: {str(e)}"

    def restore_backup(self, backup_name: str, files_to_restore: Optional[List[str]] = None) -> Tuple[bool, str]:
        """
        Restore files from a backup

        Args:
            backup_name: Name of the backup to restore from
            files_to_restore: List of specific files to restore (None = all files)

        Returns:
            Tuple of (success, message)
        """
        try:
            backup_path = self.backup_dir / backup_name

            if not backup_path.exists():
                return False, f"Backup '{backup_name}' not found"

            # Load backup info
            backup_info = self.get_backup_info(backup_name)
            if not backup_info:
                return False, f"Could not load backup info for '{backup_name}'"

            files_restored = 0
            total_size = 0

            # Determine which files to restore
            if files_to_restore is None:
                files_to_restore = self.db_files

            # Create pre-restore backup
            pre_restore_name = f"pre_restore_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.create_backup(pre_restore_name, f"Pre-restore backup before restoring from {backup_name}")

            # Restore each file
            for filename in files_to_restore:
                if filename not in backup_info.get("files", {}):
                    continue

                backup_file_path = backup_path / filename
                target_file_path = self.data_dir / filename

                if backup_file_path.exists():
                    # Backup current version if it exists
                    if target_file_path.exists():
                        current_backup = target_file_path.with_suffix('.current')
                        shutil.copy2(target_file_path, current_backup)

                    # Restore from backup
                    shutil.copy2(backup_file_path, target_file_path)

                    file_size = backup_file_path.stat().st_size
                    files_restored += 1
                    total_size += file_size

                    # Remove temporary current backup
                    if current_backup.exists():
                        current_backup.unlink()

                else:
                    return False, f"Backup file '{filename}' not found in backup '{backup_name}'"

            return True, f"Restored {files_restored} files from backup '{backup_name}' ({total_size} bytes)"

        except Exception as e:
            return False, f"Restore failed: {str(e)}"

    def list_backups(self) -> List[Dict]:
        """List all available backups"""
        backups = []

        if not self.backup_dir.exists():
            return backups

        for backup_dir in sorted(self.backup_dir.iterdir()):
            if backup_dir.is_dir():
                info = self.get_backup_info(backup_dir.name)
                if info:
                    backups.append(info)
                else:
                    # Create basic info for backups without info file
                    backups.append({
                        "name": backup_dir.name,
                        "created_at": datetime.datetime.fromtimestamp(backup_dir.stat().st_mtime).isoformat(),
                        "description": "Legacy backup",
                        "total_files": len(list(backup_dir.glob("*.json"))),
                        "total_size": sum(f.stat().st_size for f in backup_dir.glob("*.json") if f.is_file())
                    })

        # Sort by creation date (newest first)
        backups.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return backups

    def delete_backup(self, backup_name: str) -> Tuple[bool, str]:
        """Delete a backup"""
        try:
            backup_path = self.backup_dir / backup_name

            if not backup_path.exists():
                return False, f"Backup '{backup_name}' not found"

            shutil.rmtree(backup_path)
            return True, f"Backup '{backup_name}' deleted successfully"

        except Exception as e:
            return False, f"Delete failed: {str(e)}"

    def get_backup_stats(self, backup_name: str) -> Dict:
        """Get detailed statistics for a backup"""
        backup_info = self.get_backup_info(backup_name)
        if not backup_info:
            return {}

        backup_path = self.backup_dir / backup_name

        stats = {
            "name": backup_name,
            "info": backup_info,
            "exists": backup_path.exists(),
            "file_details": {}
        }

        if backup_path.exists():
            for filename in self.db_files:
                file_path = backup_path / filename
                if file_path.exists():
                    stats["file_details"][filename] = {
                        "size": file_path.stat().st_size,
                        "modified": datetime.datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
                        "hash": self.get_file_hash(file_path)
                    }
                else:
                    stats["file_details"][filename] = {"status": "not_found"}

        return stats

    def compare_backups(self, backup1: str, backup2: str) -> Dict:
        """Compare two backups"""
        info1 = self.get_backup_info(backup1)
        info2 = self.get_backup_info(backup2)

        if not info1 or not info2:
            return {"error": "One or both backups not found"}

        comparison = {
            "backup1": backup1,
            "backup2": backup2,
            "files_comparison": {}
        }

        all_files = set(info1.get("files", {}).keys()) | set(info2.get("files", {}).keys())

        for filename in all_files:
            file1_info = info1.get("files", {}).get(filename, {})
            file2_info = info2.get("files", {}).get(filename, {})

            comparison["files_comparison"][filename] = {
                "backup1": {
                    "exists": file1_info.get("backed_up", False),
                    "size": file1_info.get("size", 0),
                    "hash": file1_info.get("hash", "")
                },
                "backup2": {
                    "exists": file2_info.get("backed_up", False),
                    "size": file2_info.get("size", 0),
                    "hash": file2_info.get("hash", "")
                },
                "changed": file1_info.get("hash") != file2_info.get("hash") if file1_info.get("hash") and file2_info.get("hash") else True
            }

        return comparison

    def export_backup(self, backup_name: str, export_path: str) -> Tuple[bool, str]:
        """Export a backup to a compressed archive"""
        try:
            import zipfile

            backup_path = self.backup_dir / backup_name
            if not backup_path.exists():
                return False, f"Backup '{backup_name}' not found"

            export_file = Path(export_path)
            export_file.parent.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(export_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in backup_path.rglob('*'):
                    if file_path.is_file():
                        zipf.write(file_path, file_path.relative_to(backup_path))

            return True, f"Backup '{backup_name}' exported to {export_path}"

        except ImportError:
            return False, "zipfile module not available for export"
        except Exception as e:
            return False, f"Export failed: {str(e)}"

    def import_backup(self, import_path: str, backup_name: Optional[str] = None) -> Tuple[bool, str]:
        """Import a backup from a compressed archive"""
        try:
            import zipfile

            import_file = Path(import_path)
            if not import_file.exists():
                return False, f"Import file '{import_path}' not found"

            if not backup_name:
                backup_name = f"imported_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

            backup_path = self.backup_dir / backup_name
            backup_path.mkdir(exist_ok=True)

            with zipfile.ZipFile(import_file, 'r') as zipf:
                zipf.extractall(backup_path)

            # Verify the backup
            if not (backup_path / "backup_info.json").exists():
                # Create basic backup info for imported backup
                backup_info = {
                    "name": backup_name,
                    "created_at": datetime.datetime.now().isoformat(),
                    "description": f"Imported from {import_path}",
                    "files": {},
                    "total_files": 0,
                    "total_size": 0
                }

                total_files = 0
                total_size = 0

                for filename in self.db_files:
                    file_path = backup_path / filename
                    if file_path.exists():
                        file_size = file_path.stat().st_size
                        backup_info["files"][filename] = {
                            "size": file_size,
                            "hash": self.get_file_hash(file_path),
                            "backed_up": True
                        }
                        total_files += 1
                        total_size += file_size

                backup_info["total_files"] = total_files
                backup_info["total_size"] = total_size

                with open(backup_path / "backup_info.json", 'w') as f:
                    json.dump(backup_info, f, indent=2)

            return True, f"Backup imported as '{backup_name}'"

        except ImportError:
            return False, "zipfile module not available for import"
        except Exception as e:
            return False, f"Import failed: {str(e)}"


def main():
    """Command line interface for backup manager"""
    import argparse

    parser = argparse.ArgumentParser(description="Bhaichara Bot Database Backup Manager")
    parser.add_argument("--data-dir", default=".", help="Data directory (default: .)")
    parser.add_argument("--backup-dir", default="backups", help="Backup directory (default: backups)")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Create backup
    create_parser = subparsers.add_parser("create", help="Create a backup")
    create_parser.add_argument("--name", help="Backup name")
    create_parser.add_argument("--description", default="", help="Backup description")

    # Restore backup
    restore_parser = subparsers.add_parser("restore", help="Restore from backup")
    restore_parser.add_argument("backup_name", help="Name of backup to restore")
    restore_parser.add_argument("--files", nargs="*", help="Specific files to restore")

    # List backups
    subparsers.add_parser("list", help="List all backups")

    # Delete backup
    delete_parser = subparsers.add_parser("delete", help="Delete a backup")
    delete_parser.add_argument("backup_name", help="Name of backup to delete")

    # Stats
    stats_parser = subparsers.add_parser("stats", help="Show backup statistics")
    stats_parser.add_argument("backup_name", help="Name of backup")

    # Compare
    compare_parser = subparsers.add_parser("compare", help="Compare two backups")
    compare_parser.add_argument("backup1", help="First backup name")
    compare_parser.add_argument("backup2", help="Second backup name")

    # Export
    export_parser = subparsers.add_parser("export", help="Export backup to archive")
    export_parser.add_argument("backup_name", help="Name of backup to export")
    export_parser.add_argument("export_path", help="Path for exported archive")

    # Import
    import_parser = subparsers.add_parser("import", help="Import backup from archive")
    import_parser.add_argument("import_path", help="Path to archive to import")
    import_parser.add_argument("--name", help="Name for imported backup")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    manager = DatabaseBackupManager(args.data_dir, args.backup_dir)

    if args.command == "create":
        success, message = manager.create_backup(args.name, args.description)
        print(message)

    elif args.command == "restore":
        success, message = manager.restore_backup(args.backup_name, args.files)
        print(message)

    elif args.command == "list":
        backups = manager.list_backups()
        if not backups:
            print("No backups found")
        else:
            print(f"Found {len(backups)} backups:")
            for backup in backups:
                print(f"  {backup['name']} - {backup['created_at']} ({backup['total_files']} files, {backup['total_size']} bytes)")
                if backup.get('description'):
                    print(f"    {backup['description']}")

    elif args.command == "delete":
        success, message = manager.delete_backup(args.backup_name)
        print(message)

    elif args.command == "stats":
        stats = manager.get_backup_stats(args.backup_name)
        if not stats:
            print(f"Backup '{args.backup_name}' not found")
        else:
            print(f"Backup: {stats['name']}")
            print(f"Created: {stats['info']['created_at']}")
            print(f"Files: {stats['info']['total_files']}")
            print(f"Size: {stats['info']['total_size']} bytes")
            if stats['info'].get('description'):
                print(f"Description: {stats['info']['description']}")

    elif args.command == "compare":
        comparison = manager.compare_backups(args.backup1, args.backup2)
        if "error" in comparison:
            print(comparison["error"])
        else:
            print(f"Comparing {comparison['backup1']} vs {comparison['backup2']}:")
            for filename, comp in comparison["files_comparison"].items():
                if comp["changed"]:
                    print(f"  {filename}: CHANGED")
                    print(f"    {comparison['backup1']}: {comp['backup1']['size']} bytes")
                    print(f"    {comparison['backup2']}: {comp['backup2']['size']} bytes")
                else:
                    print(f"  {filename}: unchanged")

    elif args.command == "export":
        success, message = manager.export_backup(args.backup_name, args.export_path)
        print(message)

    elif args.command == "import":
        success, message = manager.import_backup(args.import_path, args.name)
        print(message)


if __name__ == "__main__":
    main()