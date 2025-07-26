import asyncio
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from google.cloud.firestore_v1.base_query import FieldFilter

from ..schemas.collection_names import DatabaseCollectionNames
from .client import FirestoreClient
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger
from src.utils.requirement_property_schema import RequirementPropertySchema
from src.utils.standard_response import StandardResponse

USER_FILTERS_COLLECTION_NAME = DatabaseCollectionNames.USER_FILTERS_COLLECTION_NAME.value


class FirestoreRequirementsDB:
    def __init__(self):
        self.requirements_collection = FirestoreClient.shared().collection(USER_FILTERS_COLLECTION_NAME)

    def _validate_user_id(self, user_id: str) -> Optional[StandardResponse]:
        if not user_id:
            return StandardResponse.bad_request("Requirement ID is required")
        return None

    def _validate_requirements_data(self, requirements: RequirementPropertySchema) -> Optional[StandardResponse]:
        if not requirements:
            return StandardResponse.bad_request("Requirements data is required")
        if not requirements.user_id:
            return StandardResponse.bad_request("User ID is required")
        return None

    def _validate_document_exists(self, doc, user_id: str) -> Optional[StandardResponse]:
        if not doc.exists:
            return StandardResponse.not_found(f"Requirement '{user_id}' not found")
        return None

    def _handle_error(self, error: Exception, operation: str) -> StandardResponse:
        logger.error(f"❌ Error {operation}: {str(error)}")
        return StandardResponse.internal_error(str(error))

    async def _get_document(self, user_id: str) -> tuple:
        validation_error = self._validate_user_id(user_id)
        if validation_error:
            return None, validation_error

        doc_ref = self.requirements_collection.document(user_id)
        doc = await doc_ref.get()

        validation_error = self._validate_document_exists(doc, user_id)
        if validation_error:
            return None, validation_error

        return doc, None

    async def update_user_requirements(self, user_id: str, new_filters: Dict[str, Any]) -> StandardResponse:
        """Update user filters."""
        try:
            existing_requirements = await self.get_requirements(user_id)
            existing_data = existing_requirements.data if existing_requirements and existing_requirements.data else {}

            allReq = {**existing_data, **new_filters}

            req = RequirementPropertySchema(**allReq)
            if existing_data:
                return await self.update_requirements(user_id, req)
            else:
                return await self._create_requirements(req)

        except Exception as e:
            logger.error(f"❌ Error updating user requirements: {str(e)}")
            return self._handle_error("update_user_requirements", e)

    def _merge_filters(self, existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """Merge new filters with existing ones."""
        merged = existing.copy()
        for key, value in new.items():
            if key in merged:
                if isinstance(merged[key], list):
                    if isinstance(value, list):
                        merged[key].extend(value)
                    else:
                        merged[key].append(value)
                    if not any(isinstance(item, dict) for item in merged[key]):
                        merged[key] = list(set(merged[key]))
                else:
                    merged[key] = [merged[key], value] if not isinstance(value, list) else [merged[key]] + value
            else:
                merged[key] = value if isinstance(value, list) else [value]
        return merged

    async def _create_requirements(self, requirements: RequirementPropertySchema) -> StandardResponse:
        try:
            if validation_error := self._validate_requirements_data(requirements):
                return validation_error

            data = requirements.model_dump(mode="json")
            uid = data["user_id"]

            if validation_error := self._validate_user_id(uid):
                return validation_error

            doc_ref = self.requirements_collection.document(uid)
            data["id"] = doc_ref.id
            await doc_ref.set(data)

            return StandardResponse.success(data=data, message="Requirement created successfully")
        except Exception as e:
            return self._handle_error(e, "creating requirement")

    async def get_requirements(self, user_id: str) -> StandardResponse:
        try:
            doc, error = await self._get_document(user_id)
            if error:
                return error
            return StandardResponse.success(data=doc.to_dict(), message="Requirement retrieved successfully")
        except Exception as e:
            return self._handle_error(e, f"fetching requirement '{user_id}'")

    async def update_requirements(self, user_id: str, requirement: RequirementPropertySchema) -> StandardResponse:
        try:
            if validation_error := self._validate_requirements_data(requirement):
                return validation_error

            doc, error = await self._get_document(user_id)
            if error:
                return error

            existing_data = doc.to_dict()
            update_data = requirement.model_dump(mode="json", exclude_unset=True)
            merged_data = {**existing_data, **update_data}

            await doc.reference.set(merged_data, merge=True)
            return StandardResponse.success(data=merged_data, message="Requirement updated successfully")
        except Exception as e:
            return self._handle_error(e, f"updating requirement '{user_id}'")

    async def delete_requirement(self, user_id: str) -> StandardResponse:
        try:
            doc, error = await self._get_document(user_id)
            if error:
                return error
            await doc.reference.delete()
            return StandardResponse.success(message="Requirement deleted successfully")
        except Exception as e:
            return self._handle_error(e, f"deleting requirement '{user_id}'")

    async def delete_requirements_client(self, client_id: str) -> StandardResponse:
        try:
            doc_ref = self.requirements_collection.where(filter=FieldFilter("client_id", "==", client_id))
            docs = [doc async for doc in doc_ref.stream()]
            delete_tasks = [doc.reference.delete() for doc in docs]
            await asyncio.gather(*delete_tasks)
            logger.info(f"✅ Requirements deleted successfully for client {client_id}")
            return StandardResponse.success(message="Requirement deleted successfully")
        except Exception as e:
            logger.error(f"❌ Error deleting requirements for client {client_id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def list_all_requirements(self) -> StandardResponse:
        try:
            docs = await self.requirements_collection.get()
            items = [doc.to_dict() for doc in docs if doc.exists]
            return StandardResponse.success(data=items or [], message="Requirements listed successfully")
        except Exception as e:
            return self._handle_error(e, "listing all requirements")


requirementsDB = FirestoreRequirementsDB()
