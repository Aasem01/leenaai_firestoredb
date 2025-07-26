import asyncio
from datetime import datetime
from typing import List, Optional

from fastapi import HTTPException
from google.cloud.firestore_v1.base_query import FieldFilter

from src.chatbots.user_actions_enum import ChatActions
from ..schemas.collection_names import DatabaseCollectionNames
from .dashboard import dashboardDB
from .client import FirestoreClient
from src.schemas.sales_employee_schema import SalesEmployee
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger
from src.utils.standard_response import StandardResponse
from src.utils.time_now import TimeManager
from src.utils.user_actions_schema import Action

USER_ACTIONS_COLLECTION_NAME = DatabaseCollectionNames.USER_ACTIONS_COLLECTION_NAME.value
from datetime import datetime, timedelta

import pytz


class FirestoreActionsDB:
    def __init__(self):
        self.collection = FirestoreClient.shared().collection(USER_ACTIONS_COLLECTION_NAME)

    def _validate_action_data(self, action: Action) -> Optional[StandardResponse]:
        if not action:
            return StandardResponse.bad_request("Action data is required")
        if not action.user_id:
            return StandardResponse.bad_request("User Id is required")
        return None

    def _handle_error(self, error: Exception, operation: str) -> StandardResponse:
        logger.error(f"❌ Error {operation}: {str(error)}")
        return StandardResponse.internal_error(str(error))

    async def create_action(self, action: Action) -> StandardResponse:
        try:
            logger.debug(f"🔄 Starting create_action for user_id: {action.user_id}")

            if validation_error := self._validate_action_data(action):
                logger.warning(f"❌ Validation failed for action: {validation_error}")
                return validation_error

            # Convert action to dict
            action_data = action.model_dump(mode="json")
            logger.debug(f"📝 Converted action to dict: {action_data}")

            # Extract user info for document level
            user_info = {
                "user_id": action_data["user_id"],
                "client_id": action_data["client_id"],
                "phone_number": action_data["phone_number"],
                "name": action_data["name"],
            }

            # Get the user's document reference
            user_doc_ref = self.collection.document(action.user_id)
            logger.debug(f"📄 Got document reference for user_id: {action.user_id}")

            # Get current document if it exists
            user_doc = await user_doc_ref.get()
            logger.debug(f"🔍 Retrieved document exists: {user_doc.exists}")

            if user_doc.exists:
                # Document exists, update only the actions array
                current_data = user_doc.to_dict()
                actions = current_data.get("actions", [])
                actions.append(action_data)
                logger.debug(f"📝 Updating existing document with new action. Total actions: {len(actions)}")
                try:
                    await user_doc_ref.update({"actions": actions})
                    logger.debug("✅ Document updated successfully")
                except Exception as e:
                    logger.error(f"❌ Failed to update document: {str(e)}", exc_info=True)
                    raise
            else:
                # Create new document with initial action and user info
                logger.debug(f"📝 Creating new document with initial action {action_data} for user_id: {action.user_id}")
                try:
                    await user_doc_ref.set(
                        {
                            **user_info,  # Add user info at document level
                            "actions": [action_data],  # Keep full action data including user info
                        }
                    )
                    logger.debug("✅ New document created successfully")
                except Exception as e:
                    logger.error(f"❌ Failed to create new document: {str(e)}", exc_info=True)
                    raise

            # Update dashboard action record
            logger.debug(f"📊 Updating dashboard for user {action.user_id} with action {action.action.value}")
            await dashboardDB.update_dashboard_action_record(action.client_id, action.user_id, action.action.value)
            logger.debug("✅ Dashboard updated successfully")

            logger.debug("✅ Action created successfully")
            return StandardResponse.success(data=action_data, message="Action created successfully")
        except Exception as e:
            logger.error(f"❌ Exception in create_action: {str(e)}", exc_info=True)
            return self._handle_error(e, "creating action")

    async def get_actions(self, user_id: str) -> StandardResponse:
        try:
            logger.debug(f"🔍 Getting actions for user_id: {user_id}")
            user_doc = await self.collection.document(user_id).get()

            if not user_doc.exists:
                logger.debug(f"ℹ️ No document found for user_id: {user_id}")
                return StandardResponse.success(data=[], message="No actions found for user")

            user_data = user_doc.to_dict()
            actions = user_data.get("actions", [])
            logger.debug(f"✅ Found {len(actions)} actions for user_id: {user_id}")
            return StandardResponse.success(data=user_data, message="User actions retrieved successfully")
        except Exception as e:
            logger.error(f"❌ Error retrieving user actions: {str(e)}", exc_info=True)
            return self._handle_error(e, "retrieving user actions")

    async def delete_client_actions(self, client_id: str) -> StandardResponse:
        try:
            logger.debug(f"🔍 Getting actions for user_id: {client_id}")
            actions = await self.collection.where(filter=FieldFilter("client_id", "==", client_id)).get()
            delete_tasks = [doc.reference.delete() for doc in actions]
            await asyncio.gather(*delete_tasks)
            logger.debug(f"✅ Actions deleted successfully for client {client_id}")

            return StandardResponse.success(message="Actions deleted successfully")
        except Exception as e:
            logger.error(f"❌ Error deleting client actions: {str(e)}", exc_info=True)
            return self._handle_error(e, "deleting client actions")

    async def delete_user_actions(self, user_id: str) -> StandardResponse:
        try:
            logger.debug(f"🔍 Deleting actions for user_id: {user_id}")
            doc_ref = self.collection.document(user_id)
            await doc_ref.delete()
            logger.debug(f"✅ Actions deleted successfully for user {user_id}")

            return StandardResponse.success(message="User actions deleted successfully")
        except Exception as e:
            logger.error(f"❌ Error deleting user actions: {str(e)}", exc_info=True)
            return self._handle_error(e, "deleting user actions")

    async def get_actions_by_date(
        self, start_date: datetime, end_date: datetime, client_id: str, action_type: Optional[str] = None
    ) -> StandardResponse:
        try:
            logger.info(
                f"Retrieving actions for client {client_id} between {start_date.date()} and {end_date.date()}"
                + (f" with action type: {action_type}" if action_type else "")
            )

            actions = await self.collection.where(filter=FieldFilter("client_id", "==", client_id)).get()
            logger.info(f"Found {len(actions)} documents for client {client_id}")

            filtered_actions = []
            for doc in actions:
                user_data = doc.to_dict()
                if not user_data:
                    continue

                actions_list = user_data.get("actions", [])
                if not isinstance(actions_list, list):
                    continue

                for action in actions_list:
                    action_date = action.get("meeting_time")
                    if not action_date:
                        continue

                    try:
                        if isinstance(action_date, str):
                            action_date = datetime.fromisoformat(action_date.replace("Z", "+00:00"))

                        if start_date.date() <= action_date.date() <= end_date.date():
                            if action_type is None or action.get("action") == action_type:
                                filtered_actions.append(action)
                    except ValueError:
                        continue

            logger.info(f"Found {len(filtered_actions)} actions within date range")
            return StandardResponse.success(data={"actions": filtered_actions}, message="Actions retrieved successfully")
        except Exception as e:
            logger.error(f"Error retrieving actions by date: {str(e)}", exc_info=True)
            return self._handle_error(e, "retrieving actions by date")

    async def get_scheduled_actions_by_date(
        self, start_datetime: datetime, end_datetime: datetime, client_id: str, action_type: Optional[str] = None
    ) -> StandardResponse:
        try:
            logger.info(f"🔄 Starting get_scheduled_actions_by_date for client {client_id}")
            logger.info(
                f"📅 Date range: {start_datetime.date()} to {end_datetime.date()}"
                + (f" with action type: {action_type}" if action_type else "")
            )

            # Get all documents for the client
            logger.debug(f"🔍 Creating Firestore query for client_id: {client_id}")
            query = self.collection.where(filter=FieldFilter("client_id", "==", client_id))
            query_snapshot = await query.get()
            logger.info(f"📄 Found {len(query_snapshot)} documents for client {client_id}")

            # Dictionary to store latest action for each user
            latest_actions = {}
            processed_docs = 0
            processed_actions = 0

            for doc_snapshot in query_snapshot:
                processed_docs += 1
                logger.debug(f"📝 Processing document {processed_docs}/{len(query_snapshot)}")

                if not doc_snapshot.exists:
                    logger.debug(f"⚠️ Document {processed_docs} does not exist, skipping")
                    continue

                user_data = doc_snapshot.to_dict()
                if not user_data:
                    logger.debug(f"⚠️ Document {processed_docs} has no data, skipping")
                    continue

                actions_list = user_data.get("actions", [])
                if not isinstance(actions_list, list):
                    logger.debug(f"⚠️ Document {processed_docs} has invalid actions format, skipping")
                    continue

                logger.debug(f"📋 Found {len(actions_list)} actions in document {processed_docs}")

                # Sort actions by created_at in descending order to get the latest first
                logger.debug(f"🔄 Sorting actions by created_at for document {processed_docs}")

                def get_created_at(action):
                    created_at = action.get("created_at")
                    if not created_at:
                        return datetime.min
                    try:
                        if isinstance(created_at, str):
                            return datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                        return created_at
                    except ValueError:
                        return datetime.min

                sorted_actions = sorted(actions_list, key=get_created_at, reverse=True)

                # Get the latest action that matches our criteria
                for action in sorted_actions:
                    processed_actions += 1
                    action_date = action.get("meeting_time")
                    if not action_date:
                        logger.debug(f"⚠️ Action {processed_actions} has no meeting_time, skipping")
                        continue

                    try:
                        if isinstance(action_date, str):
                            action_date = datetime.fromisoformat(action_date.replace("Z", "+00:00"))

                        if start_datetime.date() <= action_date.date() <= end_datetime.date():
                            if action_type is None or action.get("action") == action_type:
                                user_id = user_data.get("user_id")
                                if user_id and user_id not in latest_actions:
                                    logger.debug(f"✅ Found matching action for user {user_id} on {action_date.date()}")
                                    latest_actions[user_id] = action
                                    break  # We found the latest action for this user, move to next user
                                else:
                                    logger.debug(f"ℹ️ Skipping action for user {user_id} as we already have a later one")
                        else:
                            logger.debug(f"ℹ️ Action date {action_date.date()} outside range, skipping")
                    except ValueError as e:
                        logger.warning(f"⚠️ Error processing action date: {str(e)}")
                        continue

            # Convert dictionary values to list
            filtered_actions = list(latest_actions.values())

            logger.info(f"📊 Processing complete:")
            logger.info(f"   - Processed {processed_docs} documents")
            logger.info(f"   - Processed {processed_actions} total actions")
            logger.info(f"   - Found {len(filtered_actions)} latest actions within date range")

            return StandardResponse.success(data={"actions": filtered_actions}, message="Latest actions retrieved successfully")
        except Exception as e:
            logger.error(f"❌ Error retrieving actions by date: {str(e)}", exc_info=True)
            return self._handle_error(e, "retrieving actions by date")

    async def get_followup_actions_for_notification(self) -> StandardResponse:
        """
        Get all follow-up actions scheduled for today and tomorrow.
        Returns actions that need notifications sent.
        """
        try:
            followup_actions = [ChatActions.FOLLOW_UP_LATER.value]
            # Get current date in UTC
            now = datetime.now(pytz.UTC)
            today = now.date()
            tomorrow = today + timedelta(days=1)

            # Get all actions
            actions = await self.collection.get()

            followup_actions = []
            for doc in actions:
                user_data = doc.to_dict()
                if not user_data:
                    continue

                actions_list = user_data.get("actions", [])
                if not isinstance(actions_list, list):
                    continue

                for action in actions_list:
                    # Check if it's a follow-up action
                    if action.get("action") not in followup_actions:
                        continue

                    action_date = action.get("meeting_time")
                    if not action_date:
                        continue

                    try:
                        if isinstance(action_date, str):
                            action_date = TimeManager().convert_str_to_iso_date(action_date)

                        # Check if the action is scheduled for today or tomorrow
                        if action_date.date() in [today, tomorrow]:
                            followup_actions.append(
                                {
                                    **action,
                                    "user_id": user_data.get("user_id"),
                                    "client_id": user_data.get("client_id"),
                                    "phone_number": user_data.get("phone_number"),
                                }
                            )
                    except ValueError:
                        continue

            logger.info(f"Found {len(followup_actions)} follow-up actions for today and tomorrow")
            return StandardResponse.success(data={"actions": followup_actions}, message="Follow-up actions retrieved successfully")
        except Exception as e:
            logger.error(f"Error retrieving follow-up actions: {str(e)}", exc_info=True)
            return self._handle_error(e, "retrieving follow-up actions")

    async def update_action_assigned_sales(self, user_id: str, assigned_sales: List[SalesEmployee]) -> StandardResponse:
        """Update the assigned_sales field of the last action for a user"""
        try:
            logger.debug(f"🔄 Starting update_action_assigned_sales for user_id: {user_id}")

            # Get the user's document reference
            user_doc_ref = self.collection.document(user_id)

            # Get current document
            user_doc = await user_doc_ref.get()
            if not user_doc.exists:
                return StandardResponse.failure(code=404, error_message=f"No actions found for user {user_id}")

            # Get current actions
            current_data = user_doc.to_dict()
            actions = current_data.get("actions", [])

            # Check if there are any actions
            if not actions:
                return StandardResponse.failure(code=400, error_message=f"No actions found for user {user_id}")

            # Create clean sales employee data without tasks
            clean_sales_data = []
            for sales in assigned_sales:
                sales_dict = sales.model_dump(mode="json")
                if "tasks" in sales_dict:
                    del sales_dict["tasks"]
                clean_sales_data.append(sales_dict)

            # Update the assigned_sales field of the last action
            last_action_index = len(actions) - 1
            actions[last_action_index]["assigned_sales"] = clean_sales_data

            # Update the document
            try:
                await user_doc_ref.update({"actions": actions})
                logger.debug("✅ Action assigned_sales updated successfully")
                return StandardResponse.success(data=actions[last_action_index], message="Action assigned_sales updated successfully")
            except Exception as e:
                logger.error(f"❌ Failed to update action assigned_sales: {str(e)}", exc_info=True)
                raise

        except Exception as e:
            logger.error(f"❌ Exception in update_action_assigned_sales: {str(e)}", exc_info=True)
            return self._handle_error(e, "updating action assigned_sales")

    async def remove_assigned_sales(self, user_id: str, sales_employee_id: str) -> StandardResponse:
        """Remove a sales employee from the assigned_sales list of the last action"""
        try:
            logger.debug(f"🔄 Starting remove_assigned_sales for user_id: {user_id}, sales_employee_id: {sales_employee_id}")

            # Get the user's document reference
            user_doc_ref = self.collection.document(user_id)

            # Get current document
            user_doc = await user_doc_ref.get()
            if not user_doc.exists:
                return StandardResponse.failure(code=404, error_message=f"No actions found for user {user_id}")

            # Get current actions
            current_data = user_doc.to_dict()
            actions = current_data.get("actions", [])

            # Check if there are any actions
            if not actions:
                return StandardResponse.failure(code=400, error_message=f"No actions found for user {user_id}")

            # Get the last action
            last_action_index = len(actions) - 1
            last_action = actions[last_action_index]

            # Check if assigned_sales exists and is a list
            if not isinstance(last_action.get("assigned_sales"), list):
                return StandardResponse.failure(code=400, error_message="No assigned sales found in the last action")

            # Remove the sales employee from assigned_sales
            assigned_sales = last_action["assigned_sales"]
            original_length = len(assigned_sales)
            last_action["assigned_sales"] = [sales for sales in assigned_sales if sales.get("id") != sales_employee_id]

            # Check if any sales employee was actually removed
            if len(last_action["assigned_sales"]) == original_length:
                return StandardResponse.failure(code=404, error_message=f"Sales employee {sales_employee_id} not found in assigned sales")

            # Update the document
            try:
                await user_doc_ref.update({"actions": actions})
                logger.debug("✅ Sales employee removed from assigned_sales successfully")
                return StandardResponse.success(data=last_action, message="Sales employee removed from assigned_sales successfully")
            except Exception as e:
                logger.error(f"❌ Failed to remove sales employee from assigned_sales: {str(e)}", exc_info=True)
                raise

        except Exception as e:
            logger.error(f"❌ Exception in remove_assigned_sales: {str(e)}", exc_info=True)
            return self._handle_error(e, "removing assigned sales")


# Initialize instance
actionsDB = FirestoreActionsDB()
