"""
Database module for LeenaAI backend.

This module contains all database-related functionality including:
- Firestore database operations
- Vector database operations (ChromaDB)
- Database schemas and models
- Database utilities and helpers
"""

# Core database classes
from .firestore.client import FirestoreClient
# Lazy imports to avoid circular dependencies
# from .firestore.units import FirestoreUnitsDB, propertiesDB
from .firestore.users import FirestoreUsersDB
from .firestore.clients import FirestoreClientsDB, clientsDB
# Lazy imports to avoid circular dependencies
# from .firestore.projects import FirestoreProjectsDB
# from .firestore.project_phases import FirestoreProjectPhasesDB
from .firestore.dashboard import FirestoreDashboardDB, dashboardDB
from .firestore.actions import FirestoreActionsDB, actionsDB
from .firestore.requirements import FirestoreRequirementsDB, requirementsDB
from .firestore.developers import FirestoreDevelopersDB
from .firestore.images import FirestoreImagesDB
from .firestore.bookings import BookingSystem, bookingsDB
from .firestore.sales_employees import SalesEmployeesDB
from .firestore.share import FirestoreSharedLinksDB, sharedLinksDB
from .firestore.sharing_data import FirestoreSharingDataDB
from .firestore.requesting_demo import FirestoreRequestingDemo, demoDB

# Vector database operations
# Lazy imports to avoid circular dependencies
# from .vector.sync import DBToVectorSync, dbToVectorSync
# from .vector.project_sync import ProjectDBToVectorSync, projectDBToVectorSync
from .vector.chroma_sync import ChromaSyncronizer, chromaSyncronizer

# Schemas and models
from .schemas.dashboard_record import DashboardRecord
from .schemas.collection_names import DatabaseCollectionNames
from .schemas.keys import FireStoreKeys

# Utilities
from .utils.developer_resolver import DeveloperNameResolver
from .utils.insert_units import InsertUnitsIntoFirestore

# Export all public classes and instances
__all__ = [
    # Firestore classes
    "FirestoreClient",
    # "FirestoreUnitsDB",  # Lazy import
    # "propertiesDB",  # Lazy import
    "FirestoreUsersDB",
    "FirestoreClientsDB",
    "clientsDB",
    # "FirestoreProjectsDB",  # Lazy import
    # "FirestoreProjectPhasesDB",  # Lazy import
    "FirestoreDashboardDB",
    "dashboardDB",
    "FirestoreActionsDB",
    "actionsDB",
    "FirestoreRequirementsDB",
    "requirementsDB",
    "FirestoreDevelopersDB",
    "FirestoreImagesDB",
    "BookingSystem",
    "bookingsDB",
    "SalesEmployeesDB",
    "FirestoreSharedLinksDB",
    "sharedLinksDB",
    "FirestoreSharingDataDB",
    "FirestoreRequestingDemo",
    "demoDB",
    
    # Vector database
    # "DBToVectorSync",  # Lazy import
    # "dbToVectorSync",  # Lazy import
    # "ProjectDBToVectorSync",  # Lazy import
    # "projectDBToVectorSync",  # Lazy import
    "ChromaSyncronizer",
    "chromaSyncronizer",
    
    # Schemas
    "DashboardRecord",
    "DatabaseCollectionNames",
    "FireStoreKeys",
    
    # Utilities
    "DeveloperNameResolver",
    "InsertUnitsIntoFirestore",
] 