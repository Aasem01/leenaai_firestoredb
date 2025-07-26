from difflib import get_close_matches
import uuid
from typing import List, Optional

from google.cloud.firestore_v1.base_query import FieldFilter, Or

from ..schemas.collection_names import DatabaseCollectionNames
from ..utils.developer_resolver import DeveloperNameResolver
from .units import propertiesDB
from .client import FirestoreClient
from src.utils.developer_schema_classes import DeveloperCreate, DeveloperUpdate
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger
from src.utils.standard_response import StandardResponse
from src.utils.time_now import TimeManager
from src.utils.user_language_normalization import LanguageNormalizer


# indexing: client_id, city
class FirestoreDevelopersDB:
    _instance = None

    def __init__(self):
        self.developers_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.DEVELOPERS_COLLECTION_NAME.value)

    @classmethod
    def shared(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def _validate_developer_data(self, developer: DeveloperCreate) -> Optional[StandardResponse]:
        if not developer:
            return StandardResponse.bad_request("Developer data is required")
        if not developer.client_id:
            return StandardResponse.bad_request("Developer client ID is required")
        return None

    def _handle_error(self, error: Exception, operation: str) -> StandardResponse:
        logger.error(f"❌ Error {operation}: {str(error)}")
        return StandardResponse.internal_error(str(error))

    async def create_developer(self, developer: DeveloperCreate) -> StandardResponse:
        try:
            if validation_error := self._validate_developer_data(developer):
                return validation_error

            # Create new document with auto-generated ID first
            doc_ref = self.developers_collection.document(str(uuid.uuid4()))

            # Get the data and set the ID
            data = developer.model_dump()
            data["id"] = doc_ref.id

            # Save to Firestore
            await doc_ref.set(data)

            return StandardResponse.success(data=data, message="Developer created successfully")
        except Exception as e:
            return self._handle_error(e, "creating developer")

    async def get_developer(self, developer_id: str) -> StandardResponse:
        try:
            if not developer_id:
                return StandardResponse.bad_request("Developer ID is required")

            doc = await self.developers_collection.document(developer_id).get()
            if not doc.exists:
                return StandardResponse.not_found("Developer not found")

            return StandardResponse.success(data=doc.to_dict(), message="Developer retrieved successfully")
        except Exception as e:
            return self._handle_error(e, f"getting developer '{developer_id}'")

    async def update_developer(self, developer_id: str, developer: DeveloperUpdate) -> StandardResponse:
        try:
            if not developer_id:
                return StandardResponse.bad_request("Developer ID is required")

            doc_ref = self.developers_collection.document(developer_id)
            doc = await doc_ref.get()
            if not doc.exists:
                return StandardResponse.not_found("Developer not found")

            developer_data = doc.to_dict()
            old_developer_name = developer_data.get("name") or developer_data.get("en_name")
            old_developer_name_ar = developer_data.get("ar_name")

            # Get only the fields that were provided in the update
            update_data = developer.model_dump(exclude_unset=True, exclude={"id"})

            # If no fields were provided to update, return error
            if not update_data:
                return StandardResponse.bad_request("No fields provided for update")

            # Always set updated_at to current time
            update_data["updated_at"] = TimeManager.get_time_now_isoformat()

            await doc_ref.update(update_data)
            updated = {**doc.to_dict(), **update_data}

            await propertiesDB.update_units_by_developer_name_and_sync(old_developer_name, update_data)

            return StandardResponse.success(data=updated, message="Developer updated successfully")
        except Exception as e:
            return self._handle_error(e, f"updating developer '{developer_id}'")

    async def delete_developer(self, developer_id: str, client_id: str) -> StandardResponse:
        try:
            if not developer_id:
                return StandardResponse.bad_request("Developer ID is required")

            doc_ref = self.developers_collection.document(developer_id)
            doc = await doc_ref.get()
            if not doc.exists:
                return StandardResponse.not_found("Developer not found")

            # Check if developer belongs to the client
            developer_data = doc.to_dict()
            if developer_data.get("client_id") != client_id:
                return StandardResponse.failure(
                    code=ErrorCodes.UNAUTHORIZED, error_message="You don't have permission to delete this developer"
                )

            # Get developer name for checking associated projects
            developer_name = developer_data.get("en_name")

            # Check if any units are associated with this developer
            units_response = await propertiesDB.get_filtered_by_client_units({"developer_name": developer_name})

            if units_response.status and units_response.data:
                associated_units = [u for u in units_response.data.get("units") if u.get("developer") == developer_name]
                if associated_units:
                    return StandardResponse.failure(
                        code=ErrorCodes.CONFLICT,
                        error_message="This developer is associated with multiple units. Kindly contact support to delete it",
                    )

            await doc_ref.delete()
            return StandardResponse.success(message="Developer deleted successfully")
        except Exception as e:
            return self._handle_error(e, f"deleting developer '{developer_id}'")

    async def get_all_developers(
        self,
        client_id: Optional[str] = None,
        city: Optional[str] = None,
        district: Optional[str] = None,
        developer_name: Optional[str] = None,
    ) -> StandardResponse:
        """
        Return all developer documents from the database as a list of dictionaries.
        If client_id is provided, only return developers belonging to that client.
        """
        try:
            # Start with base query using only indexed fields
            query = self.developers_collection

            # Apply indexed filters first (client_id and city)
            
            if client_id:
                query = query.where("client_id", "==", client_id)

            if city:
                query = query.where("city", "==", city.lower())

            # Get all records first
            docs = await query.get()
            developers = [doc.to_dict() for doc in docs if doc.exists]

            # Apply memory-level filters
            if district:
                developers = [developer for developer in developers if developer.get("district", "").lower() == district.lower()]

            if developer_name:
                developers = DeveloperNameResolver.filter_developers_by_name(developers, developer_name)
        
            return StandardResponse.success(data=developers, message="Developers retrieved successfully")
        except Exception as e:
            return self._handle_error(e, "listing developers")

    async def migrate_en_name_to_ar_name(self):
        """
        For all developers, set 'ar_name' = 'en_name'.
        Returns a summary of the migration.
        """
        try:
            docs = await self.developers_collection.get()
            updated = 0
            for doc in docs:
                if not doc.exists:
                    continue
                data = doc.to_dict()
                en_name = data.get("en_name")
                if en_name is not None:
                    update_data = {"ar_name": en_name}
                    await doc.reference.update(update_data)
                    updated += 1
            return StandardResponse.success(data={"updated": updated}, message=f"Migration completed. Updated {updated} developers.")
        except Exception as e:
            return self._handle_error(e, "migrating en_name to ar_name")

    async def get_developer_by_name(self, en_name: str, ar_name: Optional[str] = None) -> StandardResponse:
        try:

            # Build filters for OR query
            filters = [FieldFilter("en_name", "==", en_name)]
            if ar_name:
                filters.append(FieldFilter("ar_name", "==", ar_name))

            # Single OR query to check both en_name and ar_name
            docs = await self.developers_collection.where(filter=Or(filters)).get()

            if docs:
                return StandardResponse.success(data=docs[0].to_dict(), message="Developer retrieved successfully")

            return StandardResponse.not_found("Developer not found")
        except Exception as e:
            return self._handle_error(e, "getting developer")


# ✅ Initialize DB
developers_db = FirestoreDevelopersDB()
