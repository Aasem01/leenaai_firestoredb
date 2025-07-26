# Installation Guide

## 🚀 Quick Installation

### Step 1: Build the Package

```bash
cd database
python build_package.py
```

This will:
- Clean previous build artifacts
- Build the package
- Install it in development mode
- Test the installation

### Step 2: Update Your Project Imports

```bash
# From the main project directory (leenaai_backend)
python database/migrate_imports.py
```

This will update all imports from `database.` to `leenaai_firestoredb.`

### Step 3: Test Your Application

```bash
# Test that your application still works
uvicorn src.main:app --reload
```

### Step 4: Remove Old Database Directory (Optional)

Once you've confirmed everything works:

```bash
# Remove the old database directory
rm -rf database/
```

## 📦 Package Management

### Development Mode (Recommended)

```bash
cd database
pip install -e .
```

This installs the package in "editable" mode, so changes to the source code are immediately reflected.

### Production Mode

```bash
cd database
python -m build
pip install dist/leenaai_firestoredb-1.0.0.tar.gz
```

### Uninstall

```bash
pip uninstall leenaai-firestoredb
```

## 🔧 Manual Installation

If the automated scripts don't work:

### 1. Install Dependencies

```bash
cd database
pip install -r requirements.txt
```

### 2. Build Package

```bash
python -m build
```

### 3. Install Package

```bash
pip install -e .
```

### 4. Test Import

```python
python -c "
import leenaai_firestoredb
from leenaai_firestoredb.firestore.client import FirestoreClient
from leenaai_firestoredb.vector.chroma_sync import chromaSyncronizer
print('✅ Package installed successfully!')
"
```

## 🐛 Troubleshooting

### Import Errors

If you get import errors:

1. **Check package installation**:
   ```bash
   pip list | grep leenaai
   ```

2. **Reinstall the package**:
   ```bash
   pip uninstall leenaai-firestoredb
   cd database
   pip install -e .
   ```

3. **Check Python path**:
   ```python
   import sys
   print(sys.path)
   ```

### Build Errors

If build fails:

1. **Update setuptools**:
   ```bash
   pip install --upgrade setuptools wheel
   ```

2. **Install build tools**:
   ```bash
   pip install build
   ```

3. **Clean and rebuild**:
   ```bash
   rm -rf build/ dist/ *.egg-info/
   python -m build
   ```

### Runtime Errors

If the application fails to start:

1. **Check all imports are updated**:
   ```bash
   grep -r "from database" src/
   ```

2. **Test individual imports**:
   ```python
   from leenaai_firestoredb.firestore.client import FirestoreClient
   from leenaai_firestoredb.vector.chroma_sync import chromaSyncronizer
   ```

## 📋 Verification Checklist

- [ ] Package builds successfully
- [ ] Package installs without errors
- [ ] Import test passes
- [ ] Application starts without import errors
- [ ] All database operations work correctly
- [ ] Vector sync operations work
- [ ] No references to old `database.` imports remain

## 🆘 Getting Help

If you encounter issues:

1. Check the error messages carefully
2. Verify all dependencies are installed
3. Ensure Python version is 3.8+
4. Check that Google Cloud credentials are set up
5. Create an issue with detailed error information 