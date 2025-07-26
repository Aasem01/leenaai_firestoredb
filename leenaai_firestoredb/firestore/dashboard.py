import asyncio
import datetime
from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

from fastapi import HTTPException
from google.cloud.firestore_v1.base_query import FieldFilter

from ..schemas.dashboard_record import DashboardRecord
from ..schemas.collection_names import DatabaseCollectionNames
from ..schemas.keys import FireStoreKeys
from .users import FirestoreUsersDB
from .client import FirestoreClient
from src.utils.date_formatter import DateFormatter
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger
from src.utils.role_validator import validate_user_client_access
from src.utils.standard_response import StandardResponse
from src.utils.time_it import time_it
from src.utils.time_now import TimeManager
from src.utils.user_actions_schema import ChatActions


# indexing: client_id, updated_at,
# limit: 1000
# get_price_range_units_by_attributes_manual_filter
class FirestoreDashboardDB:
    def __init__(self):
        self.dashboardCollection = FirestoreClient.shared().collection("DASHBOARD_COLLECTION")

    @time_it
    async def create_dashboard_record(self, record: DashboardRecord) -> StandardResponse:
        try:
            # Get existing record
            doc_ref = self.dashboardCollection.document(record.user_id)
            doc = await doc_ref.get()

            if doc.exists:
                # Keep existing last_action only if the new record doesn't have one
                existing_data = doc.to_dict()
                if existing_data is None:
                    existing_data = {}

                record_dict = {
                    "name": record.name,
                    "client_id": record.client_id,
                    "user_id": record.user_id,
                    "phone_number": record.phone_number,
                    "last_action": (record.last_action if record.last_action else existing_data.get("last_action")),
                    "requirement_name": record.requirement_name,
                    "updated_at": record.updated_at,
                    "messages_count": record.messages_count,
                    "unread_messages_count": (1 if existing_data.get("unread_messages_count", 0) == 1 else record.unread_messages_count),
                    "score": record.score,
                }
                await doc_ref.set(record_dict)
            else:
                # Create new record
                record_dict = {
                    "name": record.name,
                    "client_id": record.client_id,
                    "user_id": record.user_id,
                    "phone_number": record.phone_number,
                    "last_action": record.last_action,
                    "requirement_name": record.requirement_name,
                    "updated_at": record.updated_at,
                    "messages_count": record.messages_count,
                    "unread_messages_count": record.unread_messages_count,
                    "score": record.score,
                }
                await doc_ref.set(record_dict)

            logger.debug(f"🔄 Dashboard record created successfully for user {record.user_id}")
            return StandardResponse.success(data=record_dict, message="Dashboard record created successfully")
        except Exception as e:
            logger.error(f"❌ Error creating dashboard record for user {record.user_id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.BAD_REQUEST, str(e))

    async def update_dashboard_action_record(self, client_id: str, user_id: str, last_action: str) -> StandardResponse:
        try:
            doc_ref = self.dashboardCollection.document(user_id)
            doc = await doc_ref.get()

            if doc.exists:
                # Update existing record
                existing_data = doc.to_dict() or {}
                updated_record = {
                    "name": existing_data.get("name", ""),
                    "client_id": existing_data.get("client_id", ""),
                    "user_id": existing_data.get("user_id", user_id),
                    "phone_number": existing_data.get("phone_number", ""),
                    "last_action": last_action,
                    "requirement_name": existing_data.get("requirement_name", ""),
                    "updated_at": TimeManager.get_time_now_isoformat(),
                    "messages_count": existing_data.get("messages_count", 0),
                    "unread_messages_count": existing_data.get("unread_messages_count", 0),
                    "score": existing_data.get("score", 0),
                }
                await doc_ref.set(updated_record)
                logger.debug(f"✅ Dashboard record updated for user {user_id}")
                return StandardResponse.success(data=updated_record, message="Dashboard record updated successfully")
            else:
                # Create new record
                new_record = DashboardRecord(
                    name="",
                    client_id=client_id,
                    user_id=user_id,
                    phone_number="",
                    last_action=last_action,
                    requirement_name="",
                    updated_at=TimeManager.get_time_now_isoformat(),
                    messages_count=0,
                    unread_messages_count=0,
                    score=0,
                )
                await doc_ref.set(new_record.dict())
                logger.debug(f"✅ Dashboard record created for user {user_id} by setting the last action")
                return StandardResponse.success(
                    data=new_record.dict(), message="Dashboard record created successfully by setting the last action"
                )

        except Exception as e:
            logger.error(f"❌ Error updating/creating dashboard record for user {user_id} by setting the last action: {str(e)}")
            return StandardResponse.failure(ErrorCodes.BAD_REQUEST, str(e))

    @time_it
    async def get_dashboard_records(
        self,
        clientId: str,
        action: Optional[ChatActions],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        unread_messages: Optional[int] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        direction: Optional[str] = "forward",
    ) -> StandardResponse:
        try:
            if not clientId:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Client ID is required")

            # Validate date formats
            start_datetime = None
            end_datetime = None
            date_error = StandardResponse.failure(
                ErrorCodes.BAD_REQUEST, f"Invalid format. Expected format: YYYY-MM-DDTHH:MM:SS.SSSZ (e.g. 2024-03-20T15:30:45.123Z)"
            )
            if start_date:
                start_datetime = TimeManager().convert_str_to_iso_date(start_date)
                if not start_datetime:
                    return date_error

            if end_date:
                end_datetime = TimeManager().convert_str_to_iso_date(end_date)
                if not end_datetime:
                    return date_error

            # Main query using only indexed fields (clientId and updated_at)
            query = self.dashboardCollection.where(filter=FieldFilter(FireStoreKeys.clientId, "==", clientId))

            # Apply date range filters at Firestore level if both dates are provided
            if start_datetime and end_datetime:
                query = query.where(filter=FieldFilter("updated_at", ">=", start_datetime))
                query = query.where(filter=FieldFilter("updated_at", "<=", end_datetime))
            elif start_datetime:
                query = query.where(filter=FieldFilter("updated_at", ">=", start_datetime))
            elif end_datetime:
                query = query.where(filter=FieldFilter("updated_at", "<=", end_datetime))

            # Get all records first
            async_docs = query.stream()
            all_records = []
            async for doc in async_docs:
                record = doc.to_dict()
                record["user_id"] = doc.id  # Attach Firestore doc ID for cursoring
                all_records.append(record)

            # Apply remaining filters in memory
            if action:
                all_records = [record for record in all_records if record.get("last_action") == action]

            # Apply date filters in memory if not already applied at Firestore level
            if start_datetime and not (start_datetime and end_datetime):
                all_records = [record for record in all_records if record.get("updated_at") >= start_datetime]

            if end_datetime and not (start_datetime and end_datetime):
                all_records = [record for record in all_records if record.get("updated_at") <= end_datetime]

            # Apply unread/read filter in memory
            if unread_messages is not None:
                logger.info(f"🔄 Applying unread_messages filter: {unread_messages}")
                all_records = [
                    record
                    for record in all_records
                    if (unread_messages == 0 and record.get("unread_messages_count", 0) == 0)
                    or (unread_messages > 0 and record.get("unread_messages_count", 0) >= unread_messages)
                ]

            # Sort by updated_at in memory (descending)
            all_records.sort(key=lambda x: x.get("updated_at", ""), reverse=True)

            page_size = limit or len(all_records)
            users = []
            next_cursor = None
            prev_cursor = None
            has_more_next = False
            has_more_prev = False

            if direction == "backward" and cursor:
                users, next_cursor, prev_cursor, has_more_next, has_more_prev = self._paginate_backward(all_records, page_size, cursor)
            else:
                users, next_cursor, prev_cursor, has_more_next, has_more_prev = self._paginate_forward(all_records, page_size, cursor)


            if not users:
                return StandardResponse.success(
                    data={
                        "users": [],
                        "count": 0,
                        "pagination": {"next_cursor": None, "prev_cursor": None, "has_more_next": False, "has_more_prev": False},
                    },
                    message="No records found in the database",
                ).to_dict()

            logger.debug(f"🔄 Dashboard records retrieved successfully for client {clientId}")
            return StandardResponse.success(
                data={
                    "users": users,
                    "count": len(users),
                    "pagination": {"next_cursor": next_cursor, "prev_cursor": prev_cursor, "has_more_next": has_more_next, "has_more_prev": has_more_prev},
                },
                message="Records retrieved successfully",
            )
        except Exception as e:
            logger.error(f"❌ Error retrieving dashboard records for client {clientId}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.BAD_REQUEST, str(e))

    def _paginate_forward(self, all_records, page_size, cursor):
        start_index = 0
        if cursor:
            for i, record in enumerate(all_records):
                if str(record.get("user_id")) == str(cursor):
                    start_index = i + 1
                    break
        paginated_records = all_records[start_index:start_index + page_size + 1]
        has_more_next = len(paginated_records) > page_size
        users = paginated_records[:page_size]
        next_cursor = users[-1]["user_id"] if has_more_next and users else None
        has_more_prev = start_index > 0
        prev_cursor = users[0]["user_id"] if has_more_prev and users else None
        return users, next_cursor, prev_cursor, has_more_next, has_more_prev

    def _paginate_backward(self, all_records, page_size, cursor):
        end_index = 0
        for i, record in enumerate(all_records):
            if str(record.get("user_id")) == str(cursor):
                end_index = i
                break
        start_index = max(0, end_index - page_size)
        users = all_records[start_index:end_index]
        has_more_prev = start_index > 0
        prev_cursor = users[0]["user_id"] if has_more_prev and users else None
        has_more_next = end_index < len(all_records)
        next_cursor = users[-1]["user_id"] if has_more_next and users else None
        return users, next_cursor, prev_cursor, has_more_next, has_more_prev

    @time_it
    async def deleteRecordsForClient(self, clientId: str) -> StandardResponse:
        try:
            if not clientId:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Client ID is required")

            doc_ref = self.dashboardCollection.where(filter=FieldFilter(FireStoreKeys.clientId, "==", clientId))
            docs = [doc async for doc in doc_ref.stream()]
            delete_tasks = [doc.reference.delete() for doc in docs]
            await asyncio.gather(*delete_tasks)
            logger.info(f"✅ Dashboard records deleted successfully for client {clientId}")
            return StandardResponse.success(message="Dashboard records deleted successfully")
        except Exception as e:
            logger.error(f"❌ Error retrieving dashboard records for client {clientId}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.BAD_REQUEST, str(e))

    @time_it
    async def get_user_info(self, user_id: str) -> tuple[str, str]:
        """
        Retrieve user name and phone number from dashboard document.

        Args:
            user_id (str): The user ID to look up

        Returns:
            tuple[str, str]: A tuple containing (name, phone_number)
        """
        try:
            doc_ref = self.dashboardCollection.document(user_id)
            doc = await doc_ref.get()

            if not doc.exists:
                logger.debug(f"📝 No dashboard record found for user {user_id}")
                return "", ""

            data = doc.to_dict()
            # Defensive check for None data
            if data is None:
                logger.warning(f"⚠️ Dashboard document exists but data is None for user_id: {user_id}")
                return "", ""

            data = data or {}
            name = data.get("name", "")
            phone = data.get("phone_number", "")

            logger.debug(f"📦 Retrieved user info for {user_id} - Name: {name}, Phone: {phone}")
            return name, phone

        except Exception as e:
            logger.error(f"❌ Error retrieving user info for {user_id}: {str(e)}")
            return "", ""

    @time_it
    async def update_user_info(self, user_id: str, name: str = "", phone_number: str = "") -> StandardResponse:
        try:
            doc_ref = self.dashboardCollection.document(user_id)
            doc = await doc_ref.get()

            if doc.exists:
                # Update existing record
                existing_data = doc.to_dict() or {}
                updated_record = {
                    "name": name if name else existing_data.get("name", ""),
                    "client_id": existing_data.get("client_id", ""),
                    "user_id": user_id,
                    "phone_number": (phone_number if phone_number else existing_data.get("phone_number", "")),
                    "last_action": existing_data.get("last_action", ""),
                    "requirement_name": existing_data.get("requirement_name", ""),
                    "updated_at": TimeManager.get_time_now_isoformat(),
                    "messages_count": existing_data.get("messages_count", 0),
                    "unread_messages_count": existing_data.get("unread_messages_count", 0),
                    "score": existing_data.get("score", 0),
                }
                await doc_ref.set(updated_record)
                logger.debug(f"✅ User info updated in dashboard for user {user_id}")
                return StandardResponse.success(data=updated_record, message="User info updated successfully")
            else:
                # Create new record
                new_record = DashboardRecord(
                    name=name,
                    client_id="",
                    user_id=user_id,
                    phone_number=phone_number,
                    last_action="",
                    requirement_name="",
                    updated_at=TimeManager.get_time_now_isoformat(),
                    messages_count=0,
                    unread_messages_count=0,
                    score=0,
                )
                await doc_ref.set(new_record.dict())
                logger.debug(f"✅ New user info created in dashboard for user {user_id}")
                return StandardResponse.success(data=new_record.dict(), message="User info created successfully")

        except Exception as e:
            logger.error(f"❌ Error updating/creating user info for user {user_id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.BAD_REQUEST, str(e))

    @time_it
    async def mark_as_read(self, user_id: str, client_id: str) -> StandardResponse:
        try:
            doc_ref = self.dashboardCollection.document(user_id)
            doc = await doc_ref.get()

            if not doc.exists:
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, "User not found")

            existing_data = doc.to_dict() or {}

            await validate_user_client_access(user_id, client_id, existing_data)

            updated_record = {**existing_data, "unread_messages_count": 0}

            await doc_ref.set(updated_record)
            logger.debug(f"✅ Unread count reset for user {user_id}")

            # Set unread count to 0 in messages collection
            await FirestoreUsersDB.shared().sync_unread_messages_count(user_id=user_id, unread_messages_count=0, increment=False)

            logger.debug(f"✅ Unread count reset for user {user_id} in both collections")
            return StandardResponse.success(message="Marked as read successfully")

        except Exception as e:
            logger.error(f"❌ Error marking as read for user {user_id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.BAD_REQUEST, str(e))


# Initialize instance
dashboardDB = FirestoreDashboardDB()
