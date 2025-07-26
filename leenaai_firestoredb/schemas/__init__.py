"""
Database schemas and models module.

This module contains all database schemas, models, and data structures including:
- Dashboard records
- Collection names enumeration
- Database keys and constants
"""

from .dashboard_record import DashboardRecord
from .collection_names import DatabaseCollectionNames
from .keys import FireStoreKeys

__all__ = [
    "DashboardRecord",
    "DatabaseCollectionNames",
    "FireStoreKeys",
] 