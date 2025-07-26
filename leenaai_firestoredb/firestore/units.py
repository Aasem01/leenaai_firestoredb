import asyncio
import logging
from datetime import datetime, timedelta, timezone
from difflib import get_close_matches
from functools import lru_cache
from typing import List, Optional, Union

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from typing_extensions import deprecated

from src.agents_llm_component.delivery_date_extractor import DeliveryDateExtractor
# Lazy import to avoid circular dependency
# from ..vector.chroma_sync import chromaSyncronizer
from ..schemas.collection_names import DatabaseCollectionNames
from .client import FirestoreClient
from .projects import FirestoreProjectsDB
from src.tools.required_user_prop_requirements import PropertyMustHaveRequirements
from src.utils.error_codes import ErrorCodes
from src.utils.language_helper import normalize_user_purpose
from src.utils.logger import logger
from src.utils.price_priority_enum import PricePriority
from src.utils.rental_property_schema import RentPropertyDetails
from src.utils.sale_property_schema import PropertyDetails, SalePropertyDetails
from src.utils.schema_classes import UnitFilters
from src.utils.standard_response import StandardResponse
from src.utils.time_it import time_it
from src.utils.time_now import TimeManager
from src.utils.config import AUTO_SET_DOWNPAYMENT


# indexing: clientId, city,
# limit, timeout
class FirestoreUnitsDB:
    def __init__(self):
        self.units_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.UNITS_COLLECTION_NAME.value)
        # Pre-warm the collection reference to avoid lazy loading overhead
        self._prewarm_collection()

    def _prewarm_collection(self):
        """Pre-warm the collection reference to avoid lazy loading overhead."""
        try:
            # This forces the collection to be initialized immediately
            _ = self.units_collection._client
            logger.debug("✅ Units collection pre-warmed successfully")
        except Exception as e:
            logger.warning(f"⚠️ Failed to pre-warm units collection: {e}")

    def _check_and_override_downpayment(self, unit_data: dict) -> dict:
        """
        Check if downpayment should be overridden based on conditions.
        If downpayment <= 0 and totalPrice >= 2 million, override downpayment with totalPrice.
        This method is controlled by the AUTO_SET_DOWNPAYMENT flag.
        """
        if not AUTO_SET_DOWNPAYMENT:
            return unit_data

        try:
            # Get the values
            downpayment = unit_data.get("downPayment", 0)
            total_price = unit_data.get("totalPrice", 0)

            # Check conditions: downpayment <= 0 and totalPrice >= 2 million
            delivery_date = unit_data.get("deliveryDate")
            today = TimeManager.get_time_now_isoformat()

            # Only compare delivery_date if it's not None
            off_plan_unit = False
            if delivery_date is not None:
                off_plan_unit = delivery_date > today

            if off_plan_unit:
                unit_data["downPayment"] = total_price * 0.05  # 5% of total price
            elif downpayment <= 0:
                # Override downpayment with totalPrice
                unit_data["downPayment"] = total_price
                logger.debug(f"🔄 Auto-override: Set downpayment to totalPrice ({total_price}) for unit")

            return unit_data
        except Exception as e:
            logger.warning(f"⚠️ Error in downpayment override check: {str(e)}")
            return unit_data

    def _extract_image_ids_from_unit(self, unit_data: dict) -> List[str]:
        """
        Extract image fileIds from unit data.
        Unit images are stored as: images = [{"fileId": "...", "url": "..."}, ...]
        """
        image_ids = []

        if "images" in unit_data and isinstance(unit_data["images"], list):
            for image in unit_data["images"]:
                if isinstance(image, dict) and "fileId" in image:
                    image_ids.append(image["fileId"])

        return image_ids

    async def _mark_unit_images_as_linked(self, unit_data: dict) -> None:
        """
        Extract image IDs from unit data and mark them as linked.
        """
        try:
            image_ids = self._extract_image_ids_from_unit(unit_data)
            unit_id = unit_data.get("unitId")

            if not image_ids:
                logger.info("ℹ️ No images found in unit data to mark as linked")
                return

            logger.info(f"🔍 Found {len(image_ids)} images in unit to mark as linked")

            from src.database.firestore_images_db import FirestoreImagesDB

            marked_count = 0

            for image_id in image_ids:
                try:
                    result = await FirestoreImagesDB.shared().mark_image_as_linked(image_id, linked_with="unit", linked_with_id=unit_id)
                    if result.status:
                        marked_count += 1
                        logger.debug(f"✅ Marked image {image_id} as linked")
                    else:
                        logger.warning(f"⚠️ Failed to mark image {image_id} as linked: {result.error_message}")
                except Exception as e:
                    logger.error(f"❌ Error marking image {image_id} as linked: {str(e)}")

            logger.info(f"🔗 Successfully marked {marked_count} out of {len(image_ids)} images as linked")

        except Exception as e:
            logger.error(f"❌ Error in _mark_unit_images_as_linked: {str(e)}")

    async def create_sale_unit(self, unit: SalePropertyDetails):
        return await self._create_unit(unit)

    async def create_rent_unit(self, unit: RentPropertyDetails):
        return await self._create_unit(unit)

    async def _create_unit(self, unit: PropertyDetails):
        try:
            logger.info(f"🔄 Starting unit creation in Firestore for unit ID: {unit.unitId}")

            if not unit.clientId:
                logger.error("❌ Unit creation failed: Client ID is missing")
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Client ID is required")

            # Check if unit already exists
            logger.info(f"🔍 Checking if unit {unit.unitId} already exists")
            unit_ref = self.units_collection.document(unit.unitId)
            existing_doc = await unit_ref.get()

            if existing_doc.exists:
                logger.warning(f"⚠️ Unit creation failed: Unit {unit.unitId} already exists")
                return StandardResponse.failure(ErrorCodes.CONFLICT, f"Unit with ID {unit.unitId} already exists")

            # Convert unit to dictionary and ensure updatedAt is set
            logger.info(f"📝 Preparing unit data for insertion: {unit.unitId}")
            unit_data = unit.model_dump()
            if "updatedAt" not in unit_data:
                unit_data["updatedAt"] = TimeManager.get_time_now_isoformat()
                logger.info(f"⏰ Added updatedAt timestamp: {unit_data['updatedAt']}")

            # Create the unit
            logger.info(f"💾 Writing unit {unit.unitId} to Firestore")
            await unit_ref.set(unit_data, merge=True)
            logger.info(f"✅ Unit {unit.unitId} created successfully in Firestore")

            # Mark images as linked
            await self._mark_unit_images_as_linked(unit_data)

            # Sync with Chroma
            logger.info(f"🔄 Initiating Chroma sync for client: {unit.clientId}")
            await self._sync_with_chroma(unit.clientId)
            logger.info(f"✅ Chroma sync initiated for client: {unit.clientId}")

            return StandardResponse.success(data={"unitId": unit.unitId}, message="Unit created successfully")

        except Exception as e:
            logger.error(f"❌ Error creating unit {unit.unitId if unit else 'unknown'}: {str(e)}", exc_info=True)
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def update_unit(self, unit_id: str, update_data: dict):
        try:
            if not unit_id or not update_data:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Unit ID and update data are required")

            # Check if unit exists
            unit_ref = self.units_collection.document(unit_id)
            unit_doc = await unit_ref.get()

            if not unit_doc.exists:
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, f"Unit with ID {unit_id} not found")

            # Validate clientId if present in update_data
            if "clientId" in update_data and not update_data["clientId"]:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Client ID cannot be empty")
            update_data["updatedAt"] = TimeManager.get_time_now_isoformat()
            # Perform the update
            await unit_ref.update(update_data)
            logging.info(f"Unit {unit_id} updated successfully.")

            # Mark images as linked if images are being updated
            if "images" in update_data:
                await self._mark_unit_images_as_linked(update_data)

            # Sync with Chroma if clientId is provided
            if "clientId" in update_data:
                await self._sync_with_chroma(update_data["clientId"])
            else:
                # Get clientId from existing document if not in update_data
                existing_data = unit_doc.to_dict()
                if existing_data and "clientId" in existing_data:
                    await self._sync_with_chroma(existing_data["clientId"])

            return StandardResponse.success(data={"unitId": unit_id}, message="Unit updated successfully")

        except Exception as e:
            logging.error(f"❌Error updating unit {unit_id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def _sync_with_chroma(self, client_id: str):
        """
        Synchronizes units from Firestore to Chroma in the background.
        """
        try:
            if not client_id:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Client ID is required")

            async def sync_task():
                try:
                    # Add timeout to prevent indefinite hanging
                    units_response = await asyncio.wait_for(self.get_units_by_client(client_id), timeout=30.0)  # 30 second timeout
                    units = []
                    if hasattr(units_response, "status") and units_response.status:
                        units = units_response.data.get("units", [])
                    else:
                        logging.error(
                            f"❌ Failed to fetch units for Chroma sync: {getattr(units_response, 'error_message', 'Unknown error')}"
                        )
                        return
                    logging.info(f"✅ {len(units)} units fetched from Firestore for client_id={client_id}")
                    # Lazy import to avoid circular dependency
                    from ..vector.chroma_sync import chromaSyncronizer
                    await chromaSyncronizer.sync_firestore_to_chroma(units, client_id)
                except asyncio.TimeoutError:
                    logging.error(f"❌ Chroma sync task timed out for client_id={client_id}")
                except asyncio.CancelledError:
                    logging.warning(f"⚠️ Chroma sync task was cancelled for client_id={client_id}")
                except Exception as e:
                    logging.error(f"❌ Error in Chroma sync task: {str(e)}")

            # Create and store the task to prevent it from being garbage collected
            sync_task_obj = asyncio.create_task(sync_task())

            # Add error handling for the task
            def handle_task_completion(task):
                try:
                    if task.cancelled():
                        logging.warning(f"⚠️ Chroma sync task was cancelled for client_id={client_id}")
                    elif task.exception():
                        logging.error(f"❌ Chroma sync task failed: {str(task.exception())}")
                except Exception as e:
                    logging.error(f"❌ Error handling task completion: {str(e)}")

            sync_task_obj.add_done_callback(handle_task_completion)

            return StandardResponse.success(message="Started background sync of units to Chroma")

        except Exception as e:
            logging.error(f"❌ Error initiating Chroma sync: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def update_units_by_new_project_name_and_sync(self, old_project_name: str, update_data: dict):
        try:
            new_project_name = update_data.get("en_name", None)
            new_city = update_data.get("city", None)
            new_district = update_data.get("district", None)

            # Get all units with the old project name
            units_query = await self.units_collection.where(filter=FieldFilter("project", "==", old_project_name)).get()

            # Store unique client IDs from the units
            client_ids = set()
            for unit_doc in units_query:
                client_id = unit_doc.get("clientId")
                if client_id:
                    client_ids.add(client_id)

                # Create update dictionary with only non-None values
                update_dict = {}
                if new_project_name is not None:
                    update_dict["project"] = new_project_name
                if new_city is not None:
                    update_dict["city"] = new_city
                if new_district is not None:
                    update_dict["district"] = new_district

                # Only update if there are fields to update
                if update_dict:
                    await unit_doc.reference.update(update_dict)

            # Apply sync for each unique client
            for client_id in client_ids:
                await self._sync_with_chroma(client_id)

            logger.info(
                f"✅ Updated {len(units_query)} units from the old project name {old_project_name} to the new project name {new_project_name} and synced for {len(client_ids)} clients"
            )

            return StandardResponse.success(message=f"Updated {len(units_query)} units and synced for {len(client_ids)} clients")
        except Exception as e:
            logging.error(f"❌ Error searching units by project name and applying sync: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def update_units_by_developer_name_and_sync(self, old_developer_name: str, update_data: dict):
        try:
            new_developer_name = update_data.get("name", None)
            new_en_name = update_data.get("en_name", None)
            new_ar_name = update_data.get("ar_name", None)

            # Get all units with the old developer name
            units_query = await self.units_collection.where(filter=FieldFilter("developer", "==", old_developer_name)).get()

            # Store unique client IDs from the units
            client_ids = set()
            for unit_doc in units_query:
                client_id = unit_doc.get("clientId")
                if client_id:
                    client_ids.add(client_id)

                if new_developer_name:
                    await unit_doc.reference.update({"developer": new_developer_name})
                if new_en_name:
                    await unit_doc.reference.update({"developer": new_en_name})
                if new_ar_name:
                    await unit_doc.reference.update({"developer_ar": new_ar_name})
            # Apply sync for each unique client
            for client_id in client_ids:
                await self._sync_with_chroma(client_id)

            logger.info(
                f"✅ Updated {len(units_query)} units from the old developer name {old_developer_name} to the new developer name {new_developer_name} / {new_en_name} / {new_ar_name} and synced for {len(client_ids)} clients"
            )

            return StandardResponse.success(message=f"Updated {len(units_query)} units and synced for {len(client_ids)} clients")
        except Exception as e:
            logging.error(f"❌ Error searching units by developer name and applying sync: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def get_unit(self, unit_id: str):
        """
        Retrieves a unit's details by unit ID with optimized performance.
        """
        try:
            if not unit_id:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Unit ID is required")

            # Use optimized document reference with pre-warmed collection
            unit_ref = self.units_collection.document(unit_id)

            # Add timeout to prevent hanging requests
            try:
                unit_doc = await asyncio.wait_for(unit_ref.get(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.error(f"❌ Timeout fetching unit {unit_id} after 5 seconds")
                return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, "Request timeout")

            if unit_doc.exists:
                # Convert to dict immediately to avoid repeated calls
                unit_data = unit_doc.to_dict()
                # Apply downpayment override if enabled
                unit_data = self._check_and_override_downpayment(unit_data)
                return StandardResponse.success(data=unit_data)
            else:
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, "Unit not found")
        except Exception as e:
            logging.error(f"❌Error fetching unit {unit_id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def delete_unit(self, unit_id: str):
        """
        Deletes a unit from Firestore and its associated images from GCS.
        """
        try:
            if not unit_id:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Unit ID is required")

            unit_ref = self.units_collection.document(unit_id)
            unit_doc = await unit_ref.get()
            if not unit_doc.exists:
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, "Unit not found")

            unit_data = unit_doc.to_dict()
            client_id = unit_data.get("clientId")

            # Extract and delete images from GCS
            deleted_images_count = await self._delete_unit_images(unit_data)

            # Delete the unit from Firestore
            await unit_ref.delete()
            logging.info(f"🗑️ Unit {unit_id} deleted successfully.")

            # Sync with Chroma
            if client_id:
                await self._sync_with_chroma(client_id)

            return StandardResponse.success(
                data={"unitId": unit_id, "deletedImagesCount": deleted_images_count},
                message=f"Unit deleted successfully. {deleted_images_count} images were also deleted from GCS.",
            )
        except Exception as e:
            logging.error(f"❌Error deleting unit {unit_id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def _delete_unit_images(self, unit_data: dict) -> int:
        """
        Extracts image URLs from unit data and deletes them from GCS.
        Expected structure: images = [{"fileId": "...", "url": "..."}, ...]
        Returns the count of deleted images.
        """
        try:
            deleted_count = 0

            # Get images list from unit data
            images = unit_data.get("images")
            if not images or not isinstance(images, list):
                logging.info(f"ℹ️ No images found for unit or images is not a list")
                return 0

            logging.info(f"🔍 Found {len(images)} images to delete for unit")

            for image_data in images:
                if not isinstance(image_data, dict):
                    logging.warning(f"⚠️ Skipping invalid image data: {image_data}")
                    continue

                # Extract fileId and url from image dict
                file_id = image_data.get("fileId")
                image_url = image_data.get("url")

                if not file_id:
                    logging.warning(f"⚠️ Skipping image with no fileId: {image_data}")
                    continue

                try:
                    # Import GCS service here to avoid circular imports
                    from src.image_crud.gcs_service_provider import gcs_service

                    # Delete image from GCS using fileId
                    result = gcs_service.delete_image(file_id)
                    if result.get("message") == "Image deleted successfully":
                        deleted_count += 1
                        logging.info(f"🗑️ Deleted image: {file_id} (URL: {image_url})")
                    else:
                        logging.warning(f"⚠️ Failed to delete image: {file_id}")

                except Exception as e:
                    logging.error(f"❌ Error deleting image {file_id}: {str(e)}")

            logging.info(f"🗑️ Successfully deleted {deleted_count} out of {len(images)} images from GCS for unit")
            return deleted_count

        except Exception as e:
            logging.error(f"❌ Error in _delete_unit_images: {str(e)}")
            return 0

    @time_it
    async def get_units_by_client(self, client_id: str, page_size: int = 100, cursor: str = None):
        try:
            if not client_id:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Client ID is required")

            query = self.units_collection.where(filter=FieldFilter("clientId", "==", client_id))

            if cursor:
                cursor_doc = await self.units_collection.document(cursor).get()
                if cursor_doc.exists:
                    query = query.start_after(cursor_doc)

            query = query.order_by("updatedAt", direction=firestore.Query.DESCENDING).limit(page_size + 1)
            docs = await query.get()

            units = [doc.to_dict() for doc in docs[:page_size]]
            # Apply downpayment override to each unit if enabled
            units = [self._check_and_override_downpayment(unit) for unit in units]
            next_cursor = docs[page_size].id if len(docs) > page_size else None

            if not units:
                return StandardResponse.success({"units": [], "next_cursor": None})

            return StandardResponse.success({"units": units, "next_cursor": next_cursor})

        except Exception as e:
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def get_filtered_by_client_units(self, filters: Union[UnitFilters, dict]):
        try:
            if isinstance(filters, dict):
                filters = UnitFilters(**filters)

            page_size = filters.page_size or 0
            cursor = filters.cursor
            direction = filters.direction or "forward"

            # Start with base query using only indexed fields
            query = self.units_collection.order_by("updatedAt", direction=firestore.Query.DESCENDING)

            # Apply indexed filters first (clientId and city)
            if filters.client_id:
                query = query.where(filter=FieldFilter("clientId", "==", filters.client_id))

            if filters.city:
                query = query.where(filter=FieldFilter("city", "==", filters.city))


            # Use a larger limit to account for memory-level filtering
            fetch_limit = min(page_size * 100, 1000)  # Fetch more to account for filtering and in-memory pagination
            query = query.limit(fetch_limit)
            docs = await query.get()

            # Apply all other filters in memory
            filtered_units = []
            for doc in docs:
                unit = doc.to_dict()
                unit["unitId"] = doc.id  # Attach Firestore doc ID for cursoring
                if self._matches_memory_filters(unit, filters):
                    unit = self._check_and_override_downpayment(unit)
                    filtered_units.append(unit)

            units = []
            next_cursor = None
            prev_cursor = None
            has_more_next = False
            has_more_prev = False
            start_index = 0
            end_index = 0

            if direction == "backward" and cursor:
                units, prev_cursor, next_cursor, has_more_prev, has_more_next = self._paginate_backward(filtered_units, page_size, cursor)
            else:
                units, prev_cursor, next_cursor, has_more_prev, has_more_next = self._paginate_forward(filtered_units, page_size, cursor)

            return StandardResponse.success({
                "units": units,
                "count": len(units),
                "pagination": {
                    "next_cursor": next_cursor,
                    "prev_cursor": prev_cursor,
                    "has_more_next": has_more_next,
                    "has_more_prev": has_more_prev,
                },
            })

        except Exception as e:
            logger.error(f"❌ Error in get_filtered_by_client_units: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    def _paginate_forward(self, filtered_units, page_size, cursor):
        start_index = 0
        if cursor:
            for i, unit in enumerate(filtered_units):
                if str(unit.get("unitId")) == str(cursor):
                    start_index = i + 1
                    break
        paginated_units = filtered_units[start_index:start_index + page_size + 1]
        has_more_next = len(paginated_units) > page_size
        users = paginated_units[:page_size]
        has_more_prev = start_index > 0
        prev_cursor = users[0]["unitId"] if has_more_prev and users else None
        next_cursor = users[-1]["unitId"] if has_more_next and users else None
        return users, prev_cursor, next_cursor, has_more_prev, has_more_next

    def _paginate_backward(self, filtered_units, page_size, cursor):
        end_index = 0
        for i, unit in enumerate(filtered_units):
            if str(unit.get("unitId")) == str(cursor):
                end_index = i
                break
        start_index = max(0, end_index - page_size)
        users = filtered_units[start_index:end_index]
        has_more_prev = start_index > 0
        prev_cursor = users[0]["unitId"] if has_more_prev and users else None
        has_more_next = end_index < len(filtered_units)
        next_cursor = users[-1]["unitId"] if has_more_next and users else None
        return users, prev_cursor, next_cursor, has_more_prev, has_more_next

    @time_it
    async def get_units_by_filters(
        self,
        city: Optional[str] = None,
        user_district: Optional[str] = None,
        project_name: Optional[str] = None,
        client_id: Optional[str] = None,
        purpose: Optional[str] = None,
        delivery_date: Optional[str] = None,
        finishing_type: Optional[str] = None,
        duration_type: Optional[str] = None,
        min_rooms: Optional[int] = None,
        min_area: Optional[float] = None,
        furnished_type: Optional[str] = None,
        phase: Optional[str] = None,
        property_type: Optional[str] = None,
        down_payment: Optional[int] = None,
        monthly_installment: Optional[int] = None,
        max_target_price: Optional[float] = None,
        min_target_price: Optional[float] = None,
        max_down_payment: Optional[int] = None,
        limit: int = 1000,
    ):
        """
        Get units using only client_id and city for Firestore indexing,
        then apply manual filtering for all other criteria.
        """
        try:
            # Convert purpose enum to string if needed
            if hasattr(purpose, "value"):
                purpose = purpose.value.lower()
            elif isinstance(purpose, str):
                purpose = purpose.lower()

            # Start with indexed fields only (client_id and optionally city)
            query = self.units_collection
            if client_id:
                query = query.where(filter=FieldFilter("clientId", "==", client_id))

            if city:
                query = query.where(filter=FieldFilter("city", "==", city))

            # Order by updatedAt for consistent results
            query = query.order_by("updatedAt", direction=firestore.Query.DESCENDING)

            # Apply limit
            query = query.limit(limit)

            docs = await query.get()

            # Apply manual filtering
            filtered_units = []
            for doc in docs:
                unit = doc.to_dict()
                unit["id"] = doc.id  # Add document ID

                # Apply downpayment override if enabled
                unit = self._check_and_override_downpayment(unit)

                # Apply manual filters
                if self._matches_manual_filters(
                    unit=unit,
                    user_district=user_district,
                    project_name=project_name,
                    purpose=purpose,
                    delivery_date=delivery_date,
                    finishing_type=finishing_type,
                    duration_type=duration_type,
                    min_rooms=min_rooms,
                    min_area=min_area,
                    furnished_type=furnished_type,
                    phase=phase,
                    property_type=property_type,
                    down_payment=down_payment,
                    monthly_installment=monthly_installment,
                    max_target_price=max_target_price,
                    min_target_price=min_target_price,
                    max_down_payment=max_down_payment,
                ):
                    filtered_units.append(unit)

            logger.info(
                f"🔍 get_units_by_filters: {client_id} {city} Returned {len(filtered_units)} filtered units out of {len(docs)} fetched"
            )

            return StandardResponse.success({"units": filtered_units, "total_fetched": len(docs), "total_filtered": len(filtered_units)})

        except Exception as e:
            logger.error(f"❌ Error in get_units_by_filters: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    def _matches_memory_filters(self, unit: dict, filters: UnitFilters) -> bool:
        """
        Check if a unit matches all the memory-level filters.
        """
        # Property type filter
        if filters.property_type and unit.get("buildingType") != filters.property_type:
            return False

        # Purpose filter
        if filters.purpose and unit.get("purpose") != filters.purpose:
            return False

        # Project name filter
        if filters.project_name and unit.get("project") != filters.project_name:
            return False

        # Developer name filter
        if filters.developer_name and unit.get("developer") != filters.developer_name:
            return False

        # Developer name Arabic filter
        if filters.developer_name_ar and unit.get("developer_ar") != filters.developer_name_ar:
            return False

        # Floor filter
        if filters.floor and unit.get("floor") != filters.floor:
            return False

        # Bedrooms filter
        if filters.bedrooms and unit.get("roomsCount") != filters.bedrooms:
            return False

        # Bathrooms filter
        if filters.bathrooms and unit.get("bathroomCount") != filters.bathrooms:
            return False

        # Price filters
        unit_price = self._get_unit_price(unit)
        if filters.min_price and unit_price < filters.min_price:
            return False
        if filters.max_price and unit_price > filters.max_price:
            return False

        return True

    def _get_unit_price(self, unit):
        if unit.get("purpose") == "rent" and "rentDurationType" in unit:
            monthly_rent = unit["rentDurationType"].get("monthly", {})
            return monthly_rent.get("totalPrice") or monthly_rent.get("price")
        return unit.get("price") or unit.get("priceUSD") or unit.get("totalPrice")

    async def get_unit_by_id(self, unit_id: str):
        """
        Retrieves a specific unit by its ID with enhanced error handling.
        """
        try:
            if not unit_id:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Unit ID is required")

            unit_ref = self.units_collection.document(unit_id)
            doc = await unit_ref.get()

            if doc.exists:
                unit_data = doc.to_dict()
                # Apply downpayment override if enabled
                unit_data = self._check_and_override_downpayment(unit_data)
                return StandardResponse.success(data=unit_data, message=f"Unit {unit_id} found successfully")
            else:
                logging.info(f"Unit {unit_id} not found in database")
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, f"Unit with ID {unit_id} not found in our database")

        except Exception as e:
            logging.error(f"❌ Error retrieving unit {unit_id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def get_unit_by_code(self, unit_code: str):
        """
        Retrieves a specific unit by its code with enhanced error handling.
        """
        try:
            if not unit_code:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Unit code is required")

            query = self.units_collection.where(filter=FieldFilter("code", "==", unit_code))
            docs = await query.get()

            if docs and len(docs) > 0:
                unit_data = docs[0].to_dict()
                # Apply downpayment override if enabled
                unit_data = self._check_and_override_downpayment(unit_data)
                return StandardResponse.success(data=unit_data, message=f"Unit with code {unit_code} found successfully")
            else:
                logging.info(f"Unit with code {unit_code} not found in database")
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, f"Unit with code {unit_code} not found in our database")

        except Exception as e:
            logging.error(f"❌ Error retrieving unit with code {unit_code}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def get_unit_by_title(self, unit_title: str):
        """
        Retrieves a specific unit by its title with enhanced error handling.
        """
        try:
            if not unit_title:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Unit title is required")

            query = self.units_collection.where(filter=FieldFilter("unitTitle", "==", unit_title))
            docs = await query.get()

            if docs and len(docs) > 0:
                unit_data = docs[0].to_dict()
                # Apply downpayment override if enabled
                unit_data = self._check_and_override_downpayment(unit_data)
                return StandardResponse.success(data=unit_data, message=f"Unit with title {unit_title} found successfully")
            else:
                logging.info(f"Unit with title {unit_title} not found in database")
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, f"Unit with title {unit_title} not found in our database")

        except Exception as e:
            logging.error(f"❌ Error retrieving unit with title {unit_title}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def update_missing_project_ar_fields(self) -> dict:
        """
        For all units, if project_ar is missing or empty, set it to the correct Arabic project name
        by mapping the unit's project field to the ar_name from the projects collection.
        Returns a summary dict.
        """
        # Import here to avoid circular import
        # from src.database.firestore_projects_db import FirestoreProjectsDB

        updated_count = 0
        skipped_count = 0
        error_count = 0
        error_units = []
        unmapped_projects = set()
        total_units = 0

        # 1. Get all projects and build mapping {en_name: ar_name}
        projects_response = await FirestoreProjectsDB.shared().getAllProjects()
        if not projects_response.status:
            return {"status": False, "error": "Failed to fetch projects", "details": projects_response.error_message}
        projects = projects_response.data
        project_map = {p.get("en_name"): p.get("ar_name", "") for p in projects if p.get("en_name")}

        # 2. Get all units
        all_units_docs = await self.units_collection.get()
        total_units = len(all_units_docs)

        for doc in all_units_docs:
            unit = doc.to_dict()
            unit_id = doc.id
            project_en = unit.get("project")
            project_ar = unit.get("project_ar")

            # Only update if project_ar is missing or empty and project_en exists
            if (not project_ar or not project_ar.strip()) and project_en:
                ar_name = project_map.get(project_en)
                if ar_name:
                    try:
                        await doc.reference.update({"project_ar": ar_name})
                        # Call sync with chroma for this unit's clientId if available
                        client_id = unit.get("clientId")
                        if client_id:
                            await self._sync_with_chroma(client_id)
                        updated_count += 1
                    except Exception as e:
                        error_count += 1
                        error_units.append({"unit_id": unit_id, "error": str(e)})
                else:
                    unmapped_projects.add(project_en)
                    skipped_count += 1
            else:
                skipped_count += 1

        return {
            "status": True,
            "updated_count": updated_count,
            "skipped_count": skipped_count,
            "error_count": error_count,
            "error_units": error_units,
            "unmapped_projects": sorted(list(unmapped_projects)),
            "total_units": total_units,
        }

    def _get_unit_price_by_priority(
        self, unit: dict, price_priority: PricePriority, expected_delivery_date: Optional[float] = None
    ) -> Optional[float]:
        """
        Extract unit price based on the specified PricePriority enum.
        If no priority is specified, falls back to totalPrice/targetPrice.
        """

        try:
            payment_plans = unit.get("paymentPlans", [])

            if price_priority == PricePriority.downPayment:
                # Always return minimum downpayment from payment plans
                min_down_payment = float("inf")
                for plan in payment_plans:
                    if isinstance(plan, dict):
                        plan_down_payment = plan.get("downPayment", float("inf"))
                        if plan_down_payment < min_down_payment:
                            min_down_payment = plan_down_payment
                return min_down_payment if min_down_payment != float("inf") else unit.get("downPayment", 0)

            elif price_priority == PricePriority.monthlyInstallment:
                # For monthly installment, check paymentPlans for monthly installment
                # logger.info(f"🔍 db_price_range> payment_plans: {payment_plans}")
                for plan in payment_plans:
                    # Check if plan has monthly installment directly
                    if "monthly" in plan and isinstance(plan["monthly"], dict):
                        monthly_installment = plan["monthly"].get("installment")
                        if monthly_installment:
                            return monthly_installment

                    # Calculate monthly installment from yearly amount
                    yearly_installment = plan.get("installment_amount_yearly")
                    if yearly_installment:
                        # Convert yearly installment to monthly (divide by 12)
                        monthly_installment = yearly_installment / 12
                        # logger.info(f"🔍 Calculated monthly installment: {monthly_installment} from yearly: {yearly_installment}")
                        return monthly_installment
                return None

            elif price_priority == PricePriority.totalPrice:
                return unit.get("totalPrice")

            else:
                # Unknown priority, fallback to original logic
                return self._get_unit_price(unit)

        except Exception as e:
            logger.warning(f"⚠️ Error extracting price for priority {price_priority}: {str(e)}")
            # Fallback to original logic
            return self._get_unit_price(unit)

    async def get_price_range_units_by_attributes_manual_filter(
        self,
        property_type: str,
        city: str,
        district: str,
        user_purpose: str,
        price_priority: PricePriority,
        client_id: Optional[str] = None,
        project: Optional[str] = None,
        area: Optional[float] = None,
        bedrooms: Optional[int] = None,
        expected_delivery_date: Optional[str] = None,
        limit: int = 1000,  # Limit for initial city-based fetch
    ):
        """
        Get price range units by attributes using manual filtering instead of Firestore indexing.
        Fetches units by city first, then applies additional filters in memory.
        Requires a PricePriority enum value for price calculation.
        """
        try:
            normalized_user_purpose = normalize_user_purpose(user_input=user_purpose)

            # Start with city-based query (most restrictive to reduce initial dataset)
            query = self.units_collection
            if city:
                query = query.where(filter=FieldFilter("city", "==", city))
            if client_id:
                query = query.where(filter=FieldFilter("clientId", "==", client_id))

            # Add limit to prevent excessive data fetching
            query = query.limit(limit)
            docs = await query.get()

            if not docs:
                return StandardResponse.success({"highest_unit": None, "lowest_unit": None, "matching_units": []})

            delivery_date = DeliveryDateExtractor.shared().build_delivery_date_filter(expected_delivery_date)

            # Apply manual filtering in memory
            filtered_units = []
            priced_units = []
            matching_units = []  # All units from query (to match original behavior)

            for doc in docs:
                unit = doc.to_dict()
                # Apply downpayment override if enabled
                unit = self._check_and_override_downpayment(unit)
                matching_units.append(unit)  # Add all units to match original behavior

                # Apply all filters manually
                if self._matches_filters(unit, property_type, district, normalized_user_purpose, client_id, project, area, bedrooms):
                    filtered_units.append(unit)

                    # Calculate price based on priority
                    delivery_date_value = None
                    if delivery_date and isinstance(delivery_date, dict):
                        delivery_date_value = delivery_date.get("deliveryDate", None)

                    price = self._get_unit_price_by_priority(unit, price_priority, delivery_date_value)
                    if price is not None:
                        priced_units.append((price, unit))

            if not priced_units:
                return StandardResponse.success({"highest_unit": None, "lowest_unit": None, "matching_units": matching_units})

            # Find min/max without full sort for better performance
            min_price_unit = min(priced_units, key=lambda x: x[0])
            max_price_unit = max(priced_units, key=lambda x: x[0])

            lowest_price, lowest_unit = min_price_unit
            highest_price, highest_unit = max_price_unit
            logger.info(
                f"🔍 db_price_range- Total units fetched: {len(docs)}, Filtered units: {len(filtered_units)}, lowest_price: {lowest_price} -price_priority {price_priority}"
            )

            return StandardResponse.success(
                {
                    "highest_unit": highest_unit,
                    "lowest_unit": lowest_unit,
                    "price_range": {"lowest": lowest_price, "highest": highest_price},
                    "matching_units": filtered_units,
                }
            )

        except Exception as e:
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    def _matches_filters(
        self,
        unit: dict,
        property_type: str,
        district: str,
        normalized_user_purpose: str,
        client_id: Optional[str],
        project: Optional[str],
        area: Optional[float],
        bedrooms: Optional[int],
        delivery_date: Optional[str] = None,
    ) -> bool:
        """
        Check if a unit matches all the specified filters.
        """
        # Client ID filter
        if client_id and client_id.strip():
            if unit.get("clientId") != client_id:
                return False

        # District filter
        if district:
            if unit.get("district") != district:
                return False

        # Project filter
        if project:
            if unit.get("project") != project:
                return False

        # Property type filter
        if property_type:
            if unit.get("buildingType") != property_type:
                return False

        # Purpose filter
        if normalized_user_purpose:
            if unit.get("purpose") != normalized_user_purpose:
                return False

        # Area filter
        if area:
            unit_area = unit.get("landArea")
            if unit_area is None or unit_area < area:
                return False

        # Bedrooms filter
        if bedrooms:
            unit_bedrooms = unit.get("roomsCount")
            if unit_bedrooms is None or unit_bedrooms < bedrooms:
                return False

        # Delivery date filter
        # if delivery_date:
        #     if unit.get("deliveryDate") < delivery_date:
        #         return False

        return True

    def _matches_manual_filters(
        self,
        unit: dict,
        user_district: Optional[str] = None,
        project_name: Optional[str] = None,
        purpose: Optional[str] = None,
        delivery_date: Optional[str] = None,
        finishing_type: Optional[str] = None,
        duration_type: Optional[str] = None,
        min_rooms: Optional[int] = None,
        min_area: Optional[float] = None,
        furnished_type: Optional[str] = None,
        phase: Optional[str] = None,
        property_type: Optional[str] = None,
        down_payment: Optional[int] = None,
        monthly_installment: Optional[int] = None,
        max_target_price: Optional[float] = None,
        min_target_price: Optional[float] = None,
        max_down_payment: Optional[int] = None,
    ) -> bool:
        """
        Check if a unit matches all the manual filter criteria.
        """
        # District filter
        if user_district and unit.get("district") != user_district:
            return False

        # Project filter
        if project_name and unit.get("project") != project_name:
            return False

        # Purpose filter
        if purpose and unit.get("purpose") != purpose:
            return False

        # Property type filter
        if property_type and unit.get("buildingType") != property_type:
            return False

        # Phase filter
        if phase and unit.get("phase") != phase:
            return False

        # Finishing type filter
        if finishing_type and unit.get("finishing") != finishing_type:
            return False

        # Furnished type filter
        if furnished_type and unit.get("furnishing") != furnished_type:
            return False

        # Duration type filter (for rent properties)
        if duration_type and purpose == "rent":
            rent_duration = unit.get("rentDurationType", {})
            if duration_type not in rent_duration:
                return False

        # Area filter
        if min_area:
            unit_area = unit.get("landArea")
            if unit_area is None or unit_area < min_area:
                return False

        # Bedrooms filter
        if min_rooms:
            unit_bedrooms = unit.get("roomsCount")
            if unit_bedrooms is None or unit_bedrooms < min_rooms:
                return False

        # Price filters
        unit_total_price = unit.get("totalPrice")
        if min_target_price and (unit_total_price is None or unit_total_price < min_target_price):
            return False

        if max_target_price and (unit_total_price is None or unit_total_price > max_target_price):
            return False

        # Down payment filters
        unit_down_payment = unit.get("downPayment")
        if down_payment and unit_down_payment != down_payment:
            return False

        if max_down_payment and (unit_down_payment is None or unit_down_payment > max_down_payment):
            return False

        # Monthly installment filter (check payment plans)
        if monthly_installment:
            payment_plans = unit.get("paymentPlans", [])
            has_matching_installment = False
            for plan in payment_plans:
                if isinstance(plan, dict):
                    # Check yearly installment converted to monthly
                    yearly_installment = plan.get("installment_amount_yearly")
                    if yearly_installment and abs(yearly_installment / 12 - monthly_installment) < 100:  # Allow small variance
                        has_matching_installment = True
                        break
            if not has_matching_installment:
                return False

        # Delivery date filter (basic implementation)
        if delivery_date:
            unit_delivery = unit.get("deliveryDate")
            if not unit_delivery:
                return False
            # Add specific delivery date logic if needed

        return True


propertiesDB = FirestoreUnitsDB()


async def list_project_and_units_created_recently():
    # Get today's and yesterday's dates at start of day (UTC)
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)

    # Get all units
    units_response = await propertiesDB.get_units_by_client(client_id="demo")
    units = units_response.data.get("units", [])

    # Get all projects
    projects_response = await FirestoreProjectsDB.shared().getAllProjects()
    projects = projects_response.data

    print("Checking units and projects created today and yesterday...")

    # Group data by client_id
    today_projects_by_client = {}
    yesterday_projects_by_client = {}
    today_units_by_client = {}
    yesterday_units_by_client = {}

    # Collect today's projects
    for project in projects:
        created_at = project.get("updated_at")
        if created_at:
            if hasattr(created_at, "replace"):
                created_date = created_at.replace(hour=0, minute=0, second=0, microsecond=0)
                if created_date == today:
                    client_id = project.get("client_id", "Unknown")
                    if client_id not in today_projects_by_client:
                        today_projects_by_client[client_id] = []
                    today_projects_by_client[client_id].append({"name": project.get("en_name"), "city": project.get("city", "Unknown")})

    # Collect yesterday's projects
    for project in projects:
        created_at = project.get("updated_at")
        if created_at:
            if hasattr(created_at, "replace"):
                created_date = created_at.replace(hour=0, minute=0, second=0, microsecond=0)
                if created_date == yesterday:
                    client_id = project.get("client_id", "Unknown")
                    if client_id not in yesterday_projects_by_client:
                        yesterday_projects_by_client[client_id] = []
                    yesterday_projects_by_client[client_id].append({"name": project.get("en_name"), "city": project.get("city", "Unknown")})

    # Collect today's units
    for unit in units:
        created_at = unit.get("updatedAt")
        if created_at:
            if hasattr(created_at, "replace"):
                created_date = created_at.replace(hour=0, minute=0, second=0, microsecond=0)
                if created_date == today:
                    client_id = unit.get("clientId", "Unknown")
                    if client_id not in today_units_by_client:
                        today_units_by_client[client_id] = []
                    today_units_by_client[client_id].append(
                        {"title": unit.get("title"), "project": unit.get("project"), "city": unit.get("city", "Unknown")}
                    )

    # Collect yesterday's units
    for unit in units:
        created_at = unit.get("updatedAt")
        if created_at:
            if hasattr(created_at, "replace"):
                created_date = created_at.replace(hour=0, minute=0, second=0, microsecond=0)
                if created_date == yesterday:
                    client_id = unit.get("clientId", "Unknown")
                    if client_id not in yesterday_units_by_client:
                        yesterday_units_by_client[client_id] = []
                    yesterday_units_by_client[client_id].append(
                        {"title": unit.get("title"), "project": unit.get("project"), "city": unit.get("city", "Unknown")}
                    )

    # Display results grouped by client
    print("\n" + "=" * 80)
    print("PROJECTS AND UNITS CREATED TODAY AND YESTERDAY (GROUPED BY CLIENT)")
    print("=" * 80)

    all_clients = set(
        list(today_projects_by_client.keys())
        + list(yesterday_projects_by_client.keys())
        + list(today_units_by_client.keys())
        + list(yesterday_units_by_client.keys())
    )

    for client_id in sorted(all_clients):
        print(f"\n📋 CLIENT: {client_id}")
        print("-" * 50)

        # Today's data for this client
        if client_id in today_projects_by_client or client_id in today_units_by_client:
            print(f"  📅 TODAY:")
            if client_id in today_projects_by_client:
                print(f"    🏗️  Projects ({len(today_projects_by_client[client_id])}):")
                for project in today_projects_by_client[client_id]:
                    print(f"      • {project['name']} (City: {project['city']})")

            if client_id in today_units_by_client:
                print(f"    🏠 Units ({len(today_units_by_client[client_id])}):")
                for unit in today_units_by_client[client_id]:
                    print(f"      • {unit['title']} (Project: {unit['project']}, City: {unit['city']})")

        # Yesterday's data for this client
        if client_id in yesterday_projects_by_client or client_id in yesterday_units_by_client:
            print(f"  📅 YESTERDAY:")
            if client_id in yesterday_projects_by_client:
                print(f"    🏗️  Projects ({len(yesterday_projects_by_client[client_id])}):")
                for project in yesterday_projects_by_client[client_id]:
                    print(f"      • {project['name']} (City: {project['city']})")

            if client_id in yesterday_units_by_client:
                print(f"    🏠 Units ({len(yesterday_units_by_client[client_id])}):")
                for unit in yesterday_units_by_client[client_id]:
                    print(f"      • {unit['title']} (Project: {unit['project']}, City: {unit['city']})")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    total_today_projects = sum(len(projects) for projects in today_projects_by_client.values())
    total_yesterday_projects = sum(len(projects) for projects in yesterday_projects_by_client.values())
    total_today_units = sum(len(units) for units in today_units_by_client.values())
    total_yesterday_units = sum(len(units) for units in yesterday_units_by_client.values())

    print(f"📊 Total Projects created today: {total_today_projects}")
    print(f"📊 Total Projects created yesterday: {total_yesterday_projects}")
    print(f"📊 Total Units created today: {total_today_units}")
    print(f"📊 Total Units created yesterday: {total_yesterday_units}")
    print(f"👥 Total Clients with activity: {len(all_clients)}")

    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>. end of script")


if __name__ == "__main__":
    asyncio.run(list_project_and_units_created_recently())
