import asyncio
import subprocess
from typing import Any, Dict, List, Optional
from uuid import uuid4

from async_lru import alru_cache
from google.auth import default
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.oauth2.credentials import Credentials
from pydantic import BaseModel, EmailStr

from src.auth.client_model import Client, ClientModel, ClientProfileUpdate
from ..schemas.collection_names import DatabaseCollectionNames
from .client import FirestoreClient
from src.utils.config import GOOGLE_CLOUD_PROJECT_ID, LOCAL_ENV
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger
from src.utils.sale_property_schema import SalePropertyDetails
from src.utils.sharing_policy_enum import SharingPolicy
from src.utils.standard_response import StandardResponse
from src.utils.time_now import TimeManager


class FirestoreClientsDB:

    def __init__(self):
        self.client_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.CLIENT_COLLECTION_NAME.value)
        self._cold_start_lock = asyncio.Lock()  # avoids thundering-herd

    # ------------------------------------------------------------------ #
    #  ─── Internal caches ─────────────── #
    # ------------------------------------------------------------------ #
    @alru_cache(maxsize=1)
    async def _load_all_clients_raw(self) -> List[dict]:
        """
        Fetch all client documents once, cache in RAM until invalidated.
        Uses a cold start lock to prevent multiple simultaneous fetches.
        """
        async with self._cold_start_lock:
            try:
                docs = await self.client_collection.get()
                data = [d.to_dict() for d in docs if d.exists]
                logger.info(f"✅ cached clients = {len(data)} clients")
                return data
            except Exception as e:
                logger.error(f"❌ Failed to fetch clients from Firestore: {str(e)}")
                raise

    async def invalidate_caches(self) -> None:
        """
        Clear all in-memory caches after mutations.
        This is called after any write operation to ensure data consistency.
        """

        def _clear_caches():
            try:
                self._load_all_clients_raw.cache_clear()
                self.get_client_by_id.cache_clear()
                logger.info("🔄 Invalidate caches")
            except Exception as e:
                logger.error(f"❌ Error clearing caches: {str(e)}")

        # Run cache clearing in background thread
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _clear_caches)
        except Exception as e:
            logger.error(f"❌ Error in cache invalidation: {str(e)}")
            # Don't re-raise - we want to continue even if cache clearing fails

    async def get_all_clients(self):
        """
        Retrieves all clients from cache.

        Returns:
            List of client dictionaries or error information
        """
        try:
            clients = await self._load_all_clients_raw()
            return StandardResponse.success(data=clients, message="All clients retrieved successfully")
        except Exception as e:
            error_msg = f"Failed to get all clients: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg).to_dict()

    async def create_client(self, client: Client):
        """
        Creates a new client or updates an existing one.

        Returns:
            Dict containing status and error information if any
        """
        try:
            # Convert client model to dict and ensure UUID is serialized to string
            client_data = client.dict()
            client_data["id"] = str(client.id)  # Convert UUID to string

            client_ref = self.client_collection.document(str(client.id))
            client_doc = await client_ref.get()

            if client_doc.exists:
                error_msg = f"Client {client.email} already exists"
                logger.error(f"❌ {error_msg}")
                return StandardResponse.conflict(error_msg).to_dict()

            await client_ref.set(client_data, merge=True)  # merge=True updates only changed fields
            logger.info(f"Client {client.email} saved successfully")

            await self.invalidate_caches()

            return StandardResponse.success(data=client_data, message="Client saved successfully").to_dict()
        except Exception as e:
            error_msg = f"Failed to save client {client.email}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg).to_dict()

    async def get_client_sharing_policy(self, client_id: str):
        try:
            # Get from cache first
            clients = await self._load_all_clients_raw()
            client_data = next((client for client in clients if client.get("client_id") == client_id), None)

            if not client_data:
                return StandardResponse.failure(code=ErrorCodes.NOT_FOUND, error_message="Client not found").to_dict()

            logger.debug(f"✅ Client found: {client_id}")
            sharing_policy = client_data.get("sharing_policy", SharingPolicy.PULL_FROM_OTHER_CLIENTS.value)
            return StandardResponse.success(
                data={"sharing_policy": sharing_policy}, message="Sharing unit status retrieved successfully"
            ).to_dict()
        except Exception as e:
            error_msg = f"Failed to check if client {client_id} is sharing units: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg).to_dict()

    async def get_client(self, client_email: EmailStr):
        """
        Retrieves a client's details by client email from cache.

        Returns:
            Dict containing client data or error information
        """
        try:
            # Normalize email to lowercase since we use it as document ID
            normalized_email = client_email.lower()

            # Get from cache first
            clients = await self._load_all_clients_raw()
            client_data = next((client for client in clients if client.get("email", "").lower() == normalized_email), None)

            if not client_data:
                error_msg = f"Client not found: {normalized_email}"
                logger.error(f":alert {error_msg}")
                return StandardResponse.not_found(error_msg).to_dict()

            logger.debug(f"✅ Client found: {normalized_email}")
            return StandardResponse.success(data=client_data, message="Client retrieved successfully").to_dict()
        except Exception as e:
            error_msg = f"Failed to fetch client {client_email}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg).to_dict()

    @alru_cache(maxsize=1)
    async def get_client_by_id(self, client_id: str):
        """
        Retrieves a client's details by client ID from cache.

        Returns:
            Dict containing client data or error information
        """
        try:
            # Get from cache first
            clients = await self._load_all_clients_raw()
            client_data = next((client for client in clients if client.get("client_id") == client_id), None)

            if not client_data:
                return StandardResponse.failure(code=ErrorCodes.NOT_FOUND, error_message="Client not found").to_dict()

            logger.debug(f"✅ Client found: {client_id}")
            return StandardResponse.success(data=client_data, message="Client retrieved successfully").to_dict()
        except Exception as e:
            error_msg = f"Failed to fetch client {client_id}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg).to_dict()

    async def delete_client(self, email: EmailStr):
        """
        Deletes a client from Firestore.

        Args:
            email: The email of the client to delete

        Returns:
            Dict containing status and error information if any
        """
        try:
            client_docs = await self.client_collection.where(filter=FieldFilter("email", "==", email)).get()

            if not client_docs or len(client_docs) == 0:
                error_msg = f"Client not found: {email}"
                logger.error(f"❌ {error_msg}")
                return StandardResponse.not_found(error_msg).to_dict()

            client_doc = client_docs[0]
            await client_doc.reference.delete()

            logger.info(f"✅ Client {email} deleted successfully")

            await self.invalidate_caches()

            return StandardResponse.success(message="Client deleted successfully").to_dict()
        except Exception as e:
            error_msg = f"Failed to delete client {email}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg).to_dict()

    async def set_client_device_token(self, client_email: EmailStr, device_token: str, device_model: Optional[str] = None):
        """
        Adds or updates a device token for a client, supporting multiple devices.

        Args:
            client_email: The email of the client
            device_token: The device token to set
            device_model: Optional device model name

        Returns:
            Dict containing status and error information if any
        """
        try:
            device_token = device_token.strip()
            client_ref = self.client_collection.where(filter=FieldFilter("email", "==", client_email)).get()
            client_doc = client_ref[0]

            if not client_doc.exists:
                error_msg = f"Client not found: {client_email}"
                logger.error(f"❌ {error_msg}")
                return StandardResponse.not_found(error_msg).to_dict()

            client_data = client_doc.to_dict()
            tokens = client_data.get("device_tokens", [])
            if not isinstance(tokens, list):
                tokens = []

            existing_token = next((token for token in tokens if token.get("device_token") == device_token), None)

            if not existing_token:
                tokens.append(
                    {
                        "device_id": str(uuid4()),
                        "device_token": device_token,
                        "device_model": device_model,
                        "created_at": TimeManager.get_time_now_isoformat(),
                    }
                )

                await client_ref.update({"device_tokens": tokens})
                logger.info(f"✅ Device token set for client {client_email}")

            else:
                logger.info(f"⚠️ Device token already exists for client {client_email}, skipping insert.")

            await self.invalidate_caches()

            return StandardResponse.success(message="Device token set successfully").to_dict()

        except Exception as e:
            error_msg = f"Failed to set device token for client {client_email}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg).to_dict()

    async def unset_client_device_token(self, client_email: EmailStr, device_token: str):
        """
        Removes a specific device token from the client's device_tokens list.

        Args:
            client_email: The email of the client.
            device_token: The device token string to remove.

        Returns:
            Dict containing status and error information if any.
        """
        try:
            if not device_token:
                return StandardResponse.bad_request("device_token must be provided.").to_dict()

            device_token = device_token.strip()
            client_ref = self.client_collection.where(filter=FieldFilter("email", "==", client_email)).get()
            client_doc = client_ref[0]

            if not client_doc.exists:
                error_msg = f"Client not found: {client_email}"
                logger.error(f"❌ {error_msg}")
                return StandardResponse.not_found(error_msg).to_dict()

            client_data = client_doc.to_dict()
            tokens = client_data.get("device_tokens", [])
            if not isinstance(tokens, list):
                tokens = []

            updated_tokens = [token for token in tokens if token.get("device_token") != device_token]

            await client_ref.update({"device_tokens": updated_tokens})
            logger.info(f"✅ Device token removed for client {client_email}")

            await self.invalidate_caches()

            return StandardResponse.success(message="Device token removed successfully").to_dict()

        except Exception as e:
            error_msg = f"Failed to remove device token for client {client_email}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg).to_dict()

    async def get_client_price_percentage(self, client_id: str):
        """
        Gets the price percentage for a client by their ID from cache.

        Args:
            client_id: The ID of the client

        Returns:
            Dict containing the price percentage or error information
        """
        try:
            # Get from cache first
            clients = await self._load_all_clients_raw()

            client_data = next((client for client in clients if client.get("client_id") == client_id), None)

            if not client_data:
                logger.warning(f"Client {client_id} not found, returning default price percentage")
                return StandardResponse.success(data={"price_percentage": 30}, message="Using default price percentage")
            price_percentage = client_data.get("price_percentage", 30)
            logger.debug(f"✅ Retrieved price percentage for client {client_id}: {price_percentage}")
            return StandardResponse.success(data={"price_percentage": price_percentage}, message="Price percentage retrieved successfully")

        except Exception as e:
            error_msg = f"Failed to get price percentage for client {client_id}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg)

    async def update_client_profile(self, update_data: ClientProfileUpdate, client_id: str):
        """
        Updates a client's price percentage and/or sharing policy and/or phone number.

        Args:
            update_data: ClientProfileUpdate object containing the fields to update
            client_id: The ID of the client to update

        Returns:
            Dict containing updated client data or error information
        """
        try:
            # Query for the client document by client_id field
            client_query = await self.client_collection.where(filter=FieldFilter("client_id", "==", client_id)).get()

            if not client_query or len(client_query) == 0:
                error_msg = f"Client not found with ID: {client_id}"
                logger.error(f"❌ {error_msg}")
                return StandardResponse.not_found(error_msg).to_dict()

            # Get the first matching document
            client_doc = client_query[0]
            client_ref = client_doc.reference

            # Prepare update data
            update_dict = {}
            if update_data.price_percentage is not None:
                update_dict["price_percentage"] = update_data.price_percentage
            if update_data.sharing_policy is not None:
                update_dict["sharing_policy"] = update_data.sharing_policy
            if update_data.phone_number is not None:
                update_dict["phone_number"] = update_data.phone_number

            if not update_dict:
                return StandardResponse.bad_request("No fields to update provided").to_dict()

            # Update the client document
            await client_ref.update(update_dict)

            # Get the updated document
            updated_doc = await client_ref.get()
            logger.info(f"✅ Client profile updated successfully for client_id: {client_id}")

            await self.invalidate_caches()

            return StandardResponse.success(data=updated_doc.to_dict(), message="Client profile updated successfully").to_dict()

        except Exception as e:
            error_msg = f"Failed to update client profile: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg).to_dict()

    async def get_client_device_tokens(self, client_id: str):
        try:
            # Fetch client data from cache
            client_response = await self.get_client_by_id(client_id)
            if not client_response or client_response.get("status") != True:
                error_msg = f"Client {client_id} not found"
                logger.error(f"❌ {error_msg}")
                return StandardResponse.not_found(error_msg).to_dict()

            client_data = client_response.get("data")
            device_tokens = client_data.get("device_tokens", [])

            if not device_tokens or not isinstance(device_tokens, list):
                return StandardResponse.failure(code=ErrorCodes.NOT_FOUND, error_message="No device tokens found for client").to_dict()

            return StandardResponse.success(data=device_tokens, message="Device tokens retrieved successfully").to_dict()

        except Exception as e:
            error_msg = f"Failed to get client device tokens: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return StandardResponse.internal_error(error_msg).to_dict()


clientsDB = FirestoreClientsDB()
