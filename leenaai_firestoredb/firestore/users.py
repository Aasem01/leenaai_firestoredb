import asyncio
import time
import uuid
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from typing_extensions import deprecated

from src.cache import cache
from ..schemas.collection_names import DatabaseCollectionNames
from .client import FirestoreClient
from src.schemas.message_schemas import MessageContent, SubMessage
from src.utils.redis_cache_manager import redis_cache_manager
from src.utils.cache_config import CacheConfig
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger
from src.utils.role_validator import validate_user_client_access
from src.utils.standard_response import StandardResponse
from src.utils.time_it import time_it
from src.vector_db.models import LenaMessage
from src.utils.schema_classes import WhatsAppTemporaryMessageSchema


class FirestoreUsersDB:
    _shared = None
    _initialized = False

    def __new__(cls):
        if cls._shared is None:
            cls._shared = super(FirestoreUsersDB, cls).__new__(cls)
        return cls._shared

    def __init__(self):
        if not self._initialized:
            self.messages_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.MESSAGES_COLLECTION_NAME.value)
            self.sub_messages_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.SUB_MESSAGES_COLLECTION_NAME.value)
            self.user_profile_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.USER_PROFILE_COLLECTION_NAME.value)
            self.client_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.CLIENT_COLLECTION_NAME.value)
            self.whatsapp_temp_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.WHATSAPP_TEMPORARY_MESSAGE_COLLECTION_NAME.value)
            self._initialized = True

    @classmethod
    def shared(cls):
        if cls._shared is None:
            cls._shared = cls()
        return cls._shared

    async def _handle_firestore_error(self, operation: str, error: Exception) -> StandardResponse:
        """Handle Firestore errors consistently."""
        logger.error(f"❌ Firestore Error ({operation}): {str(error)}")
        return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(error))

    async def _update_document(self, collection, doc_id: str, data: Dict) -> StandardResponse:
        """Safely update a document in Firestore."""
        try:
            doc_ref = collection.document(doc_id)
            await doc_ref.set(data, merge=True)
            return StandardResponse.success(data=data)
        except Exception as e:
            return await self._handle_firestore_error("update_document", e)

    async def get_all_crm_links(self) -> StandardResponse:
        """Get all CRM links from Firestore."""
        try:
            docs = await self.client_collection.get()
            crm_links = {}

            for doc in docs:
                data = doc.to_dict()
                if data.get("client_id") and data.get("crm_link"):
                    crm_links[data["client_id"]] = data["crm_link"]

            return StandardResponse.success(data=crm_links)
        except Exception as e:
            return StandardResponse.failure(ErrorCodes.get_http_status_code(e), str(e))

    async def save_message_to_firestore(self, lena_msg: LenaMessage, client_id: str) -> StandardResponse:
        """Save a message to Firestore."""
        try:
            # Only include name and phone if they are provided
            message_data = {
                "user_id": lena_msg.user_id,
                "client_id": client_id,
                "messages": firestore.ArrayUnion(
                    [
                        {
                            "user_message": lena_msg.user_message,
                            "bot_response": lena_msg.bot_response,
                            "source": lena_msg.source,
                            "platform": lena_msg.platform,
                            "properties": lena_msg.properties,
                            "timestamp": lena_msg.timestamp.isoformat(),
                        }
                    ]
                ),
            }
            # Only update device data if device token is provided
            if lena_msg.device_token:
                message_data["device_data"] = firestore.ArrayUnion(
                    [{"device_token": lena_msg.device_token, "created_at": lena_msg.timestamp.isoformat()}]
                )

            # Only update name and phone if they are provided
            if lena_msg.name:
                message_data["name"] = lena_msg.name
            if lena_msg.phone_number:
                message_data["phone_number"] = lena_msg.phone_number
                # Invalidate phone cache when phone number is updated
                await self._invalidate_phone_cache(lena_msg.phone_number)

            response = await self._update_document(self.messages_collection, lena_msg.user_id, message_data)

            return response

        except Exception as e:
            logger.error(f"❌ Error saving message to Firestore - user_id={lena_msg.user_id}, client_id={client_id}", exc_info=True)
            return await self._handle_firestore_error("save_message_to_firestore", e)

    @deprecated("Use get_messages_for_conversation instead")
    async def _get_messages_for_conversation_v0(self, user_id: str, client_id: str) -> StandardResponse:
        # Get old schema data first
        old_data = None
        try:
            doc_ref = self.messages_collection.document(user_id)
            doc = await doc_ref.get()

            if doc.exists:
                old_data = doc.to_dict()
                # Validate user's access to client data
                old_response = await validate_user_client_access(user_id, client_id, old_data)
                if old_response.status:
                    old_data = old_response.data

                    # Check if we have messages_ref and fetch the actual messages
                    messages_ref = old_data.get("messages_ref", [])

                    if messages_ref:
                        # Fetch all messages in parallel
                        message_tasks = [self.fetch_message(msg_ref) for msg_ref in messages_ref]
                        message_results = await asyncio.gather(*message_tasks, return_exceptions=True)

                        # Process results and extract only message content
                        messages = []
                        temp_messages = []

                        # First collect all valid messages
                        for msg in message_results:
                            if msg is not None and not isinstance(msg, Exception):
                                temp_messages.append(msg)

                        # Sort by message_index
                        temp_messages.sort(key=lambda x: x.get("message_index", 1))

                        # Extract only the message content
                        for msg_data in temp_messages:
                            message_content = msg_data.get("message", {})
                            if message_content:
                                messages.append(message_content)

                        old_data["messages"] = messages
                    else:
                        old_data["messages"] = []
                else:
                    old_data = None
        except Exception as old_error:
            logger.warning(f"⚠️ Failed to get messages using old schema: {str(old_error)}")

    @deprecated("Use get_messages_for_conversation instead")
    def combine_v0_v1_data(self, old_data: dict, v1_data: dict) -> dict:
        # Combine data if both exist
        if old_data and v1_data:
            # Both schemas exist, prefer v1 schema but use old schema messages if v1 has none
            v1_messages = v1_data.get("messages", [])
            old_messages = old_data.get("messages", [])

            # Use v1 messages if available, otherwise fall back to old messages
            final_messages = v1_messages if v1_messages else old_messages

            # Get unread_message_count from v1_data if available, otherwise from old_data
            unread_count = v1_data.get("unread_messages_count", old_data.get("unread_messages_count", 0))

            return StandardResponse.success(
                data={**v1_data, "messages": final_messages, "unread_messages_count": unread_count}  # Use v1 data as base
            )

    @time_it
    async def get_messages_for_conversation(self, user_id: str, client_id: str) -> StandardResponse:
        """Get messages for a specific conversation with caching."""
        try:
            v1_response = await self.get_messages_for_conversation_v1(user_id, client_id)
            if v1_response.status:
                v1_data = v1_response.data
        except Exception as v1_error:
            logger.warning(f"⚠️ Failed to get messages using v1 schema: {str(v1_error)}")

        return StandardResponse.success(data=v1_data if v1_data else [])

    async def fetch_message(self, msg_ref: str):
        """Fetch individual message from sub_messages collection."""
        try:
            # Fetch from the global sub_messages collection, not from a subcollection
            msg_doc = await self.sub_messages_collection.document(msg_ref).get()
            if msg_doc.exists:
                return msg_doc.to_dict()
            else:
                logger.warning(f"⚠️ Message document {msg_ref} does not exist in sub_messages collection")
                return None
        except Exception as msg_error:
            logger.warning(f"⚠️ Failed to fetch message {msg_ref}: {str(msg_error)}")
            return None

    async def fetch_message(self, msg_ref: str):
        """Fetch individual message from sub_messages collection."""
        try:
            # Fetch from the global sub_messages collection, not from a subcollection
            msg_doc = await self.sub_messages_collection.document(msg_ref).get()
            if msg_doc.exists:
                return msg_doc.to_dict()
            else:
                logger.warning(f"⚠️ Message document {msg_ref} does not exist in sub_messages collection")
                return None
        except Exception as msg_error:
            logger.warning(f"⚠️ Failed to fetch message {msg_ref}: {str(msg_error)}")
            return None

    async def get_all_conversations_for_client(self, client_id: str) -> StandardResponse:
        """Get all conversations for a client."""
        try:
            query = self.messages_collection.where(filter=FieldFilter("client_id", "==", client_id))
            results = [doc.to_dict() async for doc in query.stream()]
            return StandardResponse.success(data=results)
        except Exception as e:
            return await self._handle_firestore_error("get_all_conversations_for_client", e)

    async def delete_conversation(self, user_id: str, client_id: str) -> StandardResponse:
        """Delete an entire conversation with fallback to old schema."""
        try:
            # First try v1 schema
            try:
                return await self.delete_conversation_v1(user_id, client_id)
            except Exception as v1_error:
                logger.warning(f"⚠️ Failed to delete conversation using v1 schema, falling back to old schema: {str(v1_error)}")

                # Fallback to old schema
                doc_ref = self.messages_collection.document(user_id)
                await doc_ref.delete()
                return StandardResponse.success(data=True)

        except Exception as e:
            return await self._handle_firestore_error("delete_conversation", e)

    async def get_user_profile(self, user_id: str) -> StandardResponse:
        """Get user profile."""
        try:
            doc = await self.user_profile_collection.document(user_id).get()
            if not doc.exists:
                return StandardResponse.success(data={})

            data = doc.to_dict()
            # Defensive check for None data
            if data is None:
                logger.warning(f"⚠️ User profile document exists but data is None for user_id: {user_id}")
                return StandardResponse.success(data={})

            object = data if data else {}
            return StandardResponse.success(data=object)
        except Exception as e:
            logger.error(f"❌ Error getting user profile: {str(e)}")
            return StandardResponse.failure(ErrorCodes.get_http_status_code(e), str(e))

    async def _invalidate_auto_reply_cache(self, user_id: str):
        """Invalidate auto-reply cache for a specific user."""
        try:
            # Invalidate auto-reply cache
            cache_key = CacheConfig.format_auto_reply_key(user_id)  # Use centralized config
            if cache_key in cache:
                del cache[cache_key]
                logger.debug(f"🗑️ Invalidated auto-reply cache for user: {user_id}")
        except Exception as e:
            logger.error(f"❌ Error invalidating cache for user {user_id}: {str(e)}")

    async def _invalidate_phone_cache(self, phone_number: str):
        """Invalidate phone number cache when user data changes."""
        try:
            cache_key = f"phone_to_user_id:{phone_number}"
            if cache_key in cache:
                del cache[cache_key]
                logger.debug(f"🗑️ Invalidated phone cache for: {phone_number}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to invalidate phone cache: {str(e)}")

    async def _invalidate_conversation_cache(self, user_id: str, client_id: str):
        """Invalidate conversation cache when new messages are added."""
        try:

            # Invalidate full conversation cache
            cache_key = CacheConfig.format_conversation_key(user_id, client_id)  # Use centralized config
            if cache_key in cache:
                del cache[cache_key]
                logger.debug(f"🗑️ Invalidated conversation cache for user_id={user_id}, client_id={client_id}")

            # Invalidate parent document cache
            parent_cache_key = CacheConfig.format_parent_doc_key(user_id, client_id)  # Use centralized config
            if parent_cache_key in cache:
                del cache[parent_cache_key]
                logger.debug(f"🗑️ Invalidated parent document cache for user_id={user_id}")

            # Invalidate user requirements cache
            requirements_cache_key = CacheConfig.format_user_requirements_key(user_id)  # Use centralized config
            if requirements_cache_key in cache:
                del cache[requirements_cache_key]
                logger.debug(f"🗑️ Invalidated user requirements cache for user_id={user_id}")

            # Note: We don't invalidate sub-message caches as they are still valid
            # Only the conversation structure changed, not the individual messages

        except Exception as e:
            logger.warning(f"⚠️ Failed to invalidate conversation cache: {str(e)}")

    async def update_user_profile(self, user_id: str, client_id: str, toggle_ai_auto_reply: bool, platform: str) -> StandardResponse:
        """Update user profile."""
        try:
            new_data = {
                "toggle_ai_auto_reply": toggle_ai_auto_reply,
                "client_id": client_id,
                "last_updated": firestore.SERVER_TIMESTAMP,
                "platform": platform,
            }
            result = await self._update_document(self.user_profile_collection, user_id, new_data)

            # Invalidate auto-reply cache when settings are updated
            await self._invalidate_auto_reply_cache(user_id)
            logger.debug(f"🗑️ Invalidated auto-reply cache for user: {user_id}")

            return result
        except Exception as e:
            return await self._handle_firestore_error("update_user_profile", e)

    async def update_user_profile_lang(self, user_id: str, lang: str) -> StandardResponse:
        """Update user language."""
        try:
            update_data = {"user_lang": firestore.ArrayUnion([lang]), "last_updated": firestore.SERVER_TIMESTAMP}
            return await self._update_document(self.user_profile_collection, user_id, update_data)
        except Exception as e:
            return await self._handle_firestore_error("update_user_profile_lang", e)

    async def delete_user_profile(self, user_id: str) -> StandardResponse:
        """Delete user profile."""
        try:
            doc_ref = self.user_profile_collection.document(user_id)
            await doc_ref.delete()
            return StandardResponse.success(data=True)
        except Exception as e:
            return await self._handle_firestore_error("delete_user_profile", e)

    async def delete_all_client_conversations(self, client_id: str) -> StandardResponse:
        """Delete all conversations for a client."""
        try:
            query = self.messages_collection.where(filter=FieldFilter("client_id", "==", client_id))
            docs = [doc async for doc in query.stream()]
            delete_tasks = [doc.reference.delete() for doc in docs]
            await asyncio.gather(*delete_tasks)
            return StandardResponse.success(data=True)
        except Exception as e:
            return await self._handle_firestore_error("delete_all_client_conversations", e)

    async def get_user_device_token(self, user_id: str, client_id: str):
        try:
            logger.debug(f"🔍 Attempting to get device tokens for user {user_id}")

            # Fetch user data from Firestore
            user_response = await self.get_messages_for_conversation(user_id, client_id)
            if not user_response or not user_response.status:
                error_msg = f"User {user_id} not found"
                logger.error(f"❌ {error_msg}")
                return {"status": False, "error_message": error_msg}

            user_data = user_response.data
            logger.debug(f"📦 User data retrieved: {user_data}")

            # Get device data array
            device_data = user_data.get("device_data", [])
            if not device_data or not isinstance(device_data, list):
                logger.warning(f"⚠️ No device data found for user {user_id}")
                return {"status": False, "error_message": "No device data found"}

            # Extract all device tokens
            device_tokens = [device.get("device_token") for device in device_data if device.get("device_token")]

            if not device_tokens:
                logger.warning(f"⚠️ No valid device tokens found for user {user_id}")
                return {"status": False, "error_message": "No device tokens found for client"}

            return {"status": True, "data": device_tokens, "message": "Device tokens retrieved successfully"}

        except Exception as e:
            error_msg = f"Failed to get client device tokens: {str(e)}"
            logger.error(f"❌ {error_msg}")
            logger.error("🔍 Stack trace:", exc_info=True)
            return StandardResponse.internal_error(error_msg).to_dict()

    async def sync_unread_messages_count(self, user_id: str, unread_messages_count: int = 0, increment: bool = False) -> StandardResponse:
        """Sync unread messages count between messages and dashboard collections.

        Args:
            user_id: The user ID to sync
            unread_messages_count: The count to set (if increment=False) or amount to increment by (if increment=True)
            increment: If True, increment the current count by unread_messages_count. If False, set to unread_messages_count.
        """
        try:
            # Update messages collection with the same count
            doc_ref = self.messages_collection.document(user_id)
            doc = await doc_ref.get()

            if doc.exists:
                # Update existing document
                if increment:
                    await doc_ref.update({"unread_messages_count": firestore.Increment(unread_messages_count)})
                else:
                    await doc_ref.update({"unread_messages_count": unread_messages_count})

            logger.debug(f"✅ Synced unread messages count for user {user_id}: {unread_messages_count} (increment={increment})")
            return StandardResponse.success(data={"unread_messages_count": unread_messages_count})

        except Exception as e:
            logger.error(f"❌ Error syncing unread messages count for user {user_id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.get_http_status_code(e), str(e))

    async def save_message_to_firestore_v1(self, lena_msg: LenaMessage, client_id: str) -> StandardResponse:
        """Optimized: Save a message to Firestore (v1 schema) with improved scalability and performance."""
        try:
            logger.debug(f"💾 Saving message to Firestore v1 - user_id={lena_msg.user_id}, client_id={client_id}")

            # === 1. Fetch parent document ONCE ===
            doc_ref = self.messages_collection.document(lena_msg.user_id)
            doc_snapshot = await doc_ref.get()
            doc_data = doc_snapshot.to_dict() if doc_snapshot.exists else {}

            # === 2. Generate a Firestore-safe atomic increment ===
            index_field = firestore.Increment(1)

            # === 3. Create message content and sub-message ===
            message_content = MessageContent(
                user_message=lena_msg.user_message,
                bot_response=lena_msg.bot_response,
                source=lena_msg.source,
                platform=lena_msg.platform,
                properties=lena_msg.properties,
                project_data=lena_msg.project_data,
                crm_link=lena_msg.crm_link,
                project_phases=lena_msg.project_phases,
                timestamp=lena_msg.timestamp,
            )

            # === 4. Generate sub-message ID first ===
            sub_message_id = str(uuid.uuid4())

            # === 5. Prepare parent doc update (NO ARRAYUNION) ===
            update_data = {
                "client_id": client_id,
                "user_id": lena_msg.user_id,
                "last_message_index": index_field,
                "messages_ref": firestore.ArrayUnion([sub_message_id]),
                "source": lena_msg.source,
            }

            if lena_msg.phone_number:
                update_data["phone_number"] = lena_msg.phone_number
            if lena_msg.name:
                update_data["name"] = lena_msg.name

            if lena_msg.device_token:
                existing_tokens = [device.get("device_token") for device in doc_data.get("device_data", [])]
                if lena_msg.device_token not in existing_tokens:
                    update_data["device_data"] = firestore.ArrayUnion(
                        [{"device_token": lena_msg.device_token, "created_at": lena_msg.timestamp}]
                    )

            # === 6. Run update, get incremented index ===
            await doc_ref.set(update_data, merge=True)
            updated_doc = await doc_ref.get()
            updated_index = updated_doc.to_dict().get("last_message_index", 1)

            # === 7. Save sub-message separately (outside transaction) ===
            sub_message = SubMessage(user_id=lena_msg.user_id, client_id=client_id, message_index=updated_index, message=message_content)

            sub_msg_ref = self.sub_messages_collection.document(sub_message_id)
            await sub_msg_ref.set(sub_message.model_dump())

            # === 8. Increment unread count ===
            await self.sync_unread_messages_count(user_id=lena_msg.user_id, unread_messages_count=1, increment=True)

            # === 9. Invalidate conversation cache ===
            await self._invalidate_conversation_cache(lena_msg.user_id, client_id)

            logger.debug(f"✅ Message v1 saved for user_id={lena_msg.user_id} with sub_message_id={sub_message_id}")

            return StandardResponse.success(data={"message_index": updated_index, "sub_message_id": sub_message_id})

        except Exception as e:
            logger.error(f"❌ Error saving message v1 - user_id={lena_msg.user_id}, client_id={client_id}", exc_info=True)
            return await self._handle_firestore_error("save_message_to_firestore_v1", e)

    @time_it
    async def get_messages_for_conversation_v1(self, user_id: str, client_id: str) -> StandardResponse:
        """Get messages for a conversation using the new schema (v1)."""
        try:
            logger.debug(f"📥 Fetching conversation v1 for user_id={user_id}, client_id={client_id}")

            # === 1. Check cache first ===
            cache_key = CacheConfig.format_conversation_key(user_id, client_id)

            if cache_key in cache:
                logger.debug(f"✅ Retrieved conversation from cache for user_id={user_id}, client_id={client_id}")
                return StandardResponse.success(data=cache[cache_key])

            # === 2. Fetch parent document ===
            doc_ref = self.messages_collection.document(user_id)
            doc_snapshot = await doc_ref.get()

            if not doc_snapshot.exists:
                logger.warning(f"⚠️ No conversation document found for user_id={user_id}")
                return StandardResponse.success(data=[])

            parent_data = doc_snapshot.to_dict()

            # === 3. Get sub-messages using the messages_ref list ===
            messages_ref = parent_data.get("messages_ref", [])
            if not messages_ref:
                logger.debug(f"📭 No messages_ref found for user_id={user_id}")
                return StandardResponse.success(data=parent_data)

            # === 4. Get all sub-messages in parallel from global collection ===
            sub_messages = []
            sub_message_tasks = []

            for msg_id in messages_ref:
                task = self.sub_messages_collection.document(msg_id).get()
                sub_message_tasks.append(task)

            sub_message_results = await asyncio.gather(*sub_message_tasks, return_exceptions=True)

            # === 5. Process results and extract only message content ===
            temp_messages = []
            for result in sub_message_results:
                if isinstance(result, Exception):
                    logger.warning(f"⚠️ Failed to fetch sub-message: {str(result)}")
                    continue
                if result.exists:
                    msg_data = result.to_dict()
                    # Keep the full message data temporarily for sorting
                    temp_messages.append(msg_data)

            # === 6. Sort messages by message_index ===
            temp_messages.sort(key=lambda x: x.get("message_index", 1))

            # === 7. Extract only the message content after sorting ===
            for msg_data in temp_messages:
                message_content = msg_data.get("message", {})
                if message_content:
                    sub_messages.append(message_content)

            # === 8. Combine data ===
            result = {**parent_data, "messages": sub_messages}

            # === 9. Validate access ===
            access_response = await validate_user_client_access(user_id, client_id, result)
            if not access_response.status:
                return access_response

            # === 10. Cache the result ===
            cache[cache_key] = result

            logger.debug(f"✅ Retrieved {len(sub_messages)} messages for user_id={user_id}, client_id={client_id}")
            return StandardResponse.success(data=result)

        except Exception as e:
            logger.error(f"❌ Error fetching conversation v1: {str(e)}")
            return StandardResponse.failure(ErrorCodes.get_http_status_code(e), str(e))

    async def fetch_sub_message(self, user_id: str, msg_id: str):
        """Fetch individual sub-message."""
        sub_cache_key = CacheConfig.format_sub_msg_key(msg_id)  # Use centralized config

        # Check sub-message cache first
        if sub_cache_key in cache:
            return cache[sub_cache_key]

        # Fetch from Firestore
        sub_doc_ref = self.messages_collection.document(user_id).collection("sub_messages").document(msg_id)
        sub_doc = await sub_doc_ref.get()

        if sub_doc.exists:
            message_content = sub_doc.to_dict()
            # Cache sub-message
            cache[sub_cache_key] = message_content
            return message_content
        return None

    async def delete_conversation_v1(self, user_id: str, client_id: str) -> StandardResponse:
        """Delete an entire conversation using the new schema (v1)."""
        try:
            # 1. Delete sub-messages
            query = self.sub_messages_collection.where(filter=FieldFilter("user_id", "==", user_id)).where(
                filter=FieldFilter("client_id", "==", client_id)
            )
            sub_messages = [doc async for doc in query.stream()]
            delete_tasks = [doc.reference.delete() for doc in sub_messages]
            await asyncio.gather(*delete_tasks)

            # 2. Delete parent document
            doc_ref = self.messages_collection.document(user_id)
            await doc_ref.delete()

            return StandardResponse.success(data=True)
        except Exception as e:
            return await self._handle_firestore_error("delete_conversation_v1", e)

    async def delete_device_token(self, user_id: str, client_id: str, device_token: str = None) -> StandardResponse:
        """Delete a device token from the user collection. If device_token is None, delete all tokens."""
        try:
            doc_ref = self.messages_collection.document(user_id)
            doc = await doc_ref.get()
            if not doc.exists:
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, "User not found")

            doc_data = doc.to_dict()

            # Validate user access before proceeding
            access_validation = await validate_user_client_access(user_id, client_id, doc_data)
            if not access_validation.status:
                return access_validation

            if device_token:
                # Handle specific token deletion
                device_data = doc_data.get("device_data", [])
                if not device_data:
                    return StandardResponse.failure(ErrorCodes.NOT_FOUND, "Device data not found")

                # Find and delete the specific device token
                original_length = len(device_data)
                device_data = [device for device in device_data if device.get("device_token") != device_token]

                if len(device_data) == original_length:
                    return StandardResponse.failure(ErrorCodes.NOT_FOUND, "Device token not found")

                # Update the device_data field
                await doc_ref.update({"device_data": device_data})
                return StandardResponse.success(message="Device token deleted successfully")

            await doc_ref.update({"device_data": []})
            return StandardResponse.success(message="All device tokens deleted successfully")

        except Exception as e:
            return await self._handle_firestore_error("delete_device_token", e)

    @time_it
    async def get_user_id_from_phone_number(self, phone_number: str) -> str:
        """Get user ID from phone number with caching."""
        try:
            # Check cache first
            cache_key = f"phone_to_user_id:{phone_number}"
            if cache_key in cache:
                cached_user_id = cache[cache_key]
                return cached_user_id

            # Query Firestore if not in cache
            query = self.messages_collection.where(filter=FieldFilter("phone_number", "==", phone_number))
            docs = [doc async for doc in query.stream()]

            if docs:
                doc = docs[0]
                doc_data = doc.to_dict()
                if not doc.id:
                    return None
                if not doc_data or doc_data.get("phone_number") != phone_number:
                    return None

                user_id = doc.id
                # Cache the result with TTL (15 minutes like other user data)
                cache[cache_key] = user_id
                return user_id
            else:
                cache[cache_key] = None
                return None

        except Exception as e:
            return await self._handle_firestore_error("get_user_id_from_phone_number", e)

    async def save_whatsapp_temporary_message(self, message_data: WhatsAppTemporaryMessageSchema) -> StandardResponse:
        try:
            doc_ref = self.whatsapp_temp_collection.document(message_data['whatsapp_message_id'])
            await doc_ref.set(message_data)
            return StandardResponse.success(data={"id": doc_ref.id})
        except Exception as e:
            return await self._handle_firestore_error("save_whatsapp_temporary_message", e)

    async def get_whatsapp_temporary_message_by_id(self, whatsapp_message_id: str) -> StandardResponse:
        try:
            query = self.whatsapp_temp_collection.where(filter=FieldFilter("whatsapp_message_id", "==", whatsapp_message_id))
            docs = [doc async for doc in query.stream()]
            if docs:
                return StandardResponse.success(data=docs[0].to_dict())
            else:
                return StandardResponse.success(data={})
        except Exception as e:
            return await self._handle_firestore_error("get_whatsapp_temporary_message_by_id", e)

    async def save_whatsapp_temporary_message_batch(self, messages: list) -> StandardResponse:
        try:
            batch = FirestoreClient.shared().batch()
            ids = []
            for msg in messages:
                doc_ref = self.whatsapp_temp_collection.document(msg['whatsapp_message_id'])
                batch.set(doc_ref, msg)
                ids.append(msg['whatsapp_message_id'])
            await batch.commit()
            return StandardResponse.success(data={"ids": ids})
        except Exception as e:
            return await self._handle_firestore_error("save_whatsapp_temporary_message_batch", e)
