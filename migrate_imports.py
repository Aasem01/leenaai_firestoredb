#!/usr/bin/env python3
"""
Migration script to update imports from the old database structure to the new package.
This script should be run from the main project directory (leenaai_backend).
"""

import os
import re
import glob
from pathlib import Path

def find_python_files():
    """Find all Python files in the project."""
    python_files = []
    
    # Directories to search (excluding the database directory itself)
    search_dirs = [
        'src',
        'tests',
        '.'
    ]
    
    for search_dir in search_dirs:
        if os.path.exists(search_dir):
            for root, dirs, files in os.walk(search_dir):
                # Skip the database directory
                dirs[:] = [d for d in dirs if d != 'database']
                
                # Skip certain directories
                dirs[:] = [d for d in dirs if d not in ['.git', '__pycache__', 'node_modules', 'venv', 'env']]
                
                for file in files:
                    if file.endswith('.py'):
                        python_files.append(os.path.join(root, file))
    
    return python_files

def update_imports_in_file(file_path):
    """Update imports in a single file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        changed = False
        
        # Update import patterns
        import_patterns = [
            # Old: from leenaai_firestoredb.firestore.xxx import yyy
            # New: from leenaai_firestoredb.firestore.xxx import yyy
            (r'from database\.firestore\.([^ ]+) import ([^ ]+)', r'from leenaai_firestoredb.firestore.\1 import \2'),
            
            # Old: from leenaai_firestoredb.vector.xxx import yyy
            # New: from leenaai_firestoredb.vector.xxx import yyy
            (r'from database\.vector\.([^ ]+) import ([^ ]+)', r'from leenaai_firestoredb.vector.\1 import \2'),
            
            # Old: from leenaai_firestoredb.schemas.xxx import yyy
            # New: from leenaai_firestoredb.schemas.xxx import yyy
            (r'from database\.schemas\.([^ ]+) import ([^ ]+)', r'from leenaai_firestoredb.schemas.\1 import \2'),
            
            # Old: from leenaai_firestoredb.utils.xxx import yyy
            # New: from leenaai_firestoredb.utils.xxx import yyy
            (r'from database\.utils\.([^ ]+) import ([^ ]+)', r'from leenaai_firestoredb.utils.\1 import \2'),
            
            # Old: import leenaai_firestoredb
            # New: import leenaai_firestoredb
            (r'import leenaai_firestoredb', r'import leenaai_firestoredb'),
            
            # Old: from leenaai_firestoredb import xxx
            # New: from leenaai_firestoredb import xxx
            (r'from leenaai_firestoredb import ([^ ]+)', r'from leenaai_firestoredb import \1'),
        ]
        
        for pattern, replacement in import_patterns:
            new_content = re.sub(pattern, replacement, content)
            if new_content != content:
                content = new_content
                changed = True
        
        if changed:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✅ Updated: {file_path}")
            return True
        else:
            print(f"⏭️  No changes needed: {file_path}")
            return False
            
    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
        return False

def main():
    """Main migration process."""
    print("🔄 Starting import migration process...")
    print("This will update all database imports to use the new package structure.")
    
    # Check if we're in the right directory
    if not os.path.exists('src'):
        print("❌ Error: This script should be run from the main project directory (leenaai_backend)")
        return False
    
    python_files = find_python_files()
    print(f"Found {len(python_files)} Python files to check")
    
    updated_count = 0
    total_count = 0
    
    for file_path in python_files:
        total_count += 1
        if update_imports_in_file(file_path):
            updated_count += 1
    
    print(f"\n📊 Migration Results:")
    print(f"✅ Files updated: {updated_count}")
    print(f"📁 Total files checked: {total_count}")
    
    if updated_count > 0:
        print(f"\n🎉 Migration completed successfully!")
        print(f"\n📋 Next steps:")
        print(f"1. Build and install the database package: cd database && python build_package.py")
        print(f"2. Test your application to ensure all imports work correctly")
        print(f"3. Once confirmed working, you can remove the database directory from this project")
    else:
        print(f"\nℹ️  No files needed updating - imports are already correct!")
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 