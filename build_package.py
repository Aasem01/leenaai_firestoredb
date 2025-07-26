#!/usr/bin/env python3
"""
Build script for the LeenaAI Database Package
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def run_command(command, cwd=None):
    """Run a command and return the result."""
    print(f"Running: {command}")
    result = subprocess.run(command, shell=True, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    print(f"Success: {result.stdout}")
    return True

def clean_build():
    """Clean previous build artifacts."""
    print("🧹 Cleaning previous build artifacts...")
    dirs_to_clean = ["build", "dist", "*.egg-info"]
    for dir_name in dirs_to_clean:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)
            print(f"Removed {dir_name}")

def build_package():
    """Build the package."""
    print("🔨 Building package...")
    return run_command("python -m build")

def install_package():
    """Install the package in development mode."""
    print("📦 Installing package in development mode...")
    return run_command("pip install -e .")

def install_package_production():
    """Install the package in production mode."""
    print("📦 Installing package in production mode...")
    return run_command("pip install .")

def test_import():
    """Test if the package can be imported."""
    print("🧪 Testing package import...")
    try:
        import leenaai_firestoredb
        print("✅ Package imported successfully!")
        
        # Test some key modules
        from leenaai_firestoredb.firestore.client import FirestoreClient
        print("✅ FirestoreClient imported successfully!")
        
        from leenaai_firestoredb.vector.chroma_sync import chromaSyncronizer
        print("✅ chromaSyncronizer imported successfully!")
        
        return True
    except Exception as e:
        print(f"❌ Import test failed: {e}")
        return False

def main():
    """Main build process."""
    print("🚀 Starting LeenaAI Database Package build process...")
    
    # Clean previous builds
    clean_build()
    
    # Build the package
    if not build_package():
        print("❌ Build failed!")
        return False
    
    # Install in development mode
    if not install_package():
        print("❌ Installation failed!")
        return False
    
    # Test import
    if not test_import():
        print("❌ Import test failed!")
        return False
    
    print("🎉 Package built and installed successfully!")
    print("\n📋 Next steps:")
    print("1. The package is now installed in development mode")
    print("2. You can import it in your project using: from leenaai_firestoredb import ...")
    print("3. To install in production mode, run: pip install .")
    print("4. To uninstall, run: pip uninstall leenaai-firestoredb")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 