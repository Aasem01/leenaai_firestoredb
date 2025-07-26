"""
Firestore database operations module.

This module contains all Firestore-related database operations including:
- Client initialization and connection management
- CRUD operations for all data entities
- Database synchronization and caching
"""

from .client import FirestoreClient
# Lazy imports to avoid circular dependencies
# from .units import FirestoreUnitsDB, propertiesDB
from .users import FirestoreUsersDB
from .clients import FirestoreClientsDB, clientsDB
# Lazy imports to avoid circular dependencies
# from .projects import FirestoreProjectsDB
# Lazy imports to avoid circular dependencies
# from .project_phases import FirestoreProjectPhasesDB
from .dashboard import FirestoreDashboardDB, dashboardDB
from .actions import FirestoreActionsDB, actionsDB
from .requirements import FirestoreRequirementsDB, requirementsDB
from .developers import FirestoreDevelopersDB
from .images import FirestoreImagesDB
from .bookings import BookingSystem, bookingsDB
from .sales_employees import SalesEmployeesDB
from .share import FirestoreSharedLinksDB, sharedLinksDB
from .sharing_data import FirestoreSharingDataDB
from .requesting_demo import FirestoreRequestingDemo, demoDB

__all__ = [
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
] 