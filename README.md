# LeenaAI FirestoreDB Package

A comprehensive Firestore and ChromaDB package for the LeenaAI real estate chatbot, providing database integration with vector search capabilities.

## 🚀 Features

- **Firestore Integration**: Complete CRUD operations for real estate data
- **ChromaDB Vector Database**: Semantic search and similarity matching
- **Data Synchronization**: Automatic sync between Firestore and ChromaDB
- **Schema Management**: Pydantic-based data validation
- **Async Support**: Full async/await support for high performance
- **Caching**: Built-in caching mechanisms for improved performance

## 📦 Installation

### Development Installation

```bash
# Clone the repository
git clone <repository-url>
cd leenaai-firestoredb

# Install in development mode
pip install -e .
```

### Production Installation

```bash
# Install from PyPI (when published)
pip install leenaai-firestoredb

# Or install from local build
python -m build
pip install dist/leenaai_firestoredb-1.0.0.tar.gz
```

## 🔧 Quick Start

```python
from leenaai_firestoredb.firestore.client import FirestoreClient
from leenaai_firestoredb.vector.chroma_sync import chromaSyncronizer

# Initialize Firestore client
client = FirestoreClient.shared()

# Initialize ChromaDB synchronizer
sync = chromaSyncronizer.shared()

# Sync data between Firestore and ChromaDB
await sync.sync_firestore_to_chroma()
```

## 📚 Usage Examples

### Firestore Operations

```python
from leenaai_firestoredb.firestore.units import propertiesDB
from leenaai_firestoredb.firestore.projects import FirestoreProjectsDB

# Get properties
properties = await propertiesDB.get_units_by_client("client_id")

# Get projects
projects = await FirestoreProjectsDB.shared().getAllProjects()
```

### Vector Database Operations

```python
from leenaai_firestoredb.vector.sync import dbToVectorSync

# Sync data to vector database
await dbToVectorSync.sync_firestore_to_chroma()
```

### Schema Management

```python
from leenaai_firestoredb.schemas.collection_names import DatabaseCollectionNames
from leenaai_firestoredb.schemas.dashboard_record import DashboardRecord

# Use collection names
collection = DatabaseCollectionNames.UNITS

# Create dashboard record
record = DashboardRecord(
    user_id="user123",
    action="property_view",
    data={"property_id": "prop456"}
)
```

## 🏗️ Package Structure

```
leenaai_firestoredb/
├── firestore/           # Firestore database operations
│   ├── client.py       # Firestore client initialization
│   ├── units.py        # Property/unit operations
│   ├── projects.py     # Project operations
│   ├── users.py        # User operations
│   └── ...
├── vector/             # Vector database operations
│   ├── sync.py         # Data synchronization
│   ├── chroma_sync.py  # ChromaDB operations
│   └── project_sync.py # Project-specific sync
├── schemas/            # Data schemas and models
│   ├── collection_names.py
│   ├── dashboard_record.py
│   └── keys.py
└── utils/              # Utility functions
    ├── developer_resolver.py
    └── insert_units.py
```

## 🔄 Migration from Local Database

If you're migrating from a local `database/` directory in your project:

1. **Update imports** in your main project:
   ```bash
   python database/migrate_imports.py
   ```

2. **Build and install** the package:
   ```bash
   cd database
   python build_package.py
   ```

3. **Test your application** to ensure everything works

4. **Remove the old database directory** from your main project

## 🧪 Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=leenaai_firestoredb
```

## 📋 Requirements

- Python 3.8+
- Google Cloud Firestore
- ChromaDB
- Pydantic
- AsyncIO support

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For support and questions:
- Create an issue on GitHub
- Contact: info@leenaai.com

## 🔄 Changelog

### Version 1.0.0
- Initial release
- Complete Firestore integration
- ChromaDB vector database support
- Data synchronization capabilities
- Schema management with Pydantic 