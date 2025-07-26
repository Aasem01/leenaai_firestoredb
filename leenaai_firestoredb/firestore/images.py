import asyncio
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from ..schemas.collection_names import DatabaseCollectionNames
from .client import FirestoreClient
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger
from src.utils.standard_response import StandardResponse
from src.utils.time_now import TimeManager


class FirestoreImagesDB:
    """Firestore database class for managing image metadata."""

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirestoreImagesDB, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.images_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.IMAGES_COLLECTION_NAME.value)
            self._initialized = True

    @classmethod
    def shared(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def _handle_firestore_error(self, operation: str, error: Exception) -> StandardResponse:
        """Handle Firestore errors consistently."""
        logger.error(f"❌ Firestore Error ({operation}): {str(error)}")
        return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(error))

    async def create_image_metadata(
        self,
        image_id: str,
        original_name: str,
        image_url: str,
        original_content_type: str,
        detected_content_type: str,
        extension: str,
        image_path: str,
        client_id: str,
        is_linked: bool = False,
        width: int = None,
        height: int = None,
        size_kb: float = None,
    ) -> StandardResponse:
        """Create a new image metadata document in Firestore."""
        try:
            logger.debug(f"💾 Creating image metadata for image_id: {image_id}")

            # Validate required fields
            if not image_id:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Image ID is required")

            if not original_name:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Original name is required")

            # Create the image metadata document
            image_data = {
                "image_id": image_id,
                "original_name": original_name,
                "image_url": image_url,
                "original_content_type": original_content_type,
                "detected_content_type": detected_content_type,
                "extension": extension,
                "image_path": image_path,
                "client_id": client_id,
                "is_linked": is_linked,
                "width": width,
                "height": height,
                "size_kb": size_kb,
                "created_at": TimeManager.get_time_now_isoformat(),
                "updated_at": TimeManager.get_time_now_isoformat(),
            }

            # Use image_id as the document ID
            doc_ref = self.images_collection.document(image_id)
            await doc_ref.set(image_data)

            logger.info(f"✅ Image metadata created successfully for image_id: {image_id}")
            return StandardResponse.success(data=image_data)

        except Exception as e:
            return await self._handle_firestore_error("create_image_metadata", e)

    async def get_image_metadata(self, image_id: str) -> StandardResponse:
        """Get image metadata by image_id."""
        try:
            logger.debug(f"🔍 Retrieving image metadata for image_id: {image_id}")

            doc_ref = self.images_collection.document(image_id)
            doc = await doc_ref.get()

            if not doc.exists:
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, f"Image metadata not found: {image_id}")

            image_data = doc.to_dict()
            logger.debug(f"✅ Image metadata retrieved for image_id: {image_id}")
            return StandardResponse.success(data=image_data)

        except Exception as e:
            return await self._handle_firestore_error("get_image_metadata", e)

    async def update_image_metadata(self, image_id: str, update_data: Dict) -> StandardResponse:
        """Update image metadata by image_id."""
        try:
            logger.debug(f"🔄 Updating image metadata for image_id: {image_id}")

            # Add updated_at timestamp
            update_data["updated_at"] = TimeManager.get_time_now_isoformat()

            doc_ref = self.images_collection.document(image_id)
            await doc_ref.update(update_data)

            logger.info(f"✅ Image metadata updated successfully for image_id: {image_id}")
            return StandardResponse.success(data={"image_id": image_id, "updated": True})

        except Exception as e:
            return await self._handle_firestore_error("update_image_metadata", e)

    async def delete_image_metadata(self, image_id: str) -> StandardResponse:
        """Delete image metadata by image_id."""
        try:
            logger.debug(f"🗑️ Deleting image metadata for image_id: {image_id}")

            doc_ref = self.images_collection.document(image_id)
            doc = await doc_ref.get()

            if not doc.exists:
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, f"Image metadata not found: {image_id}")

            await doc_ref.delete()

            logger.info(f"✅ Image metadata deleted successfully for image_id: {image_id}")
            return StandardResponse.success(data={"image_id": image_id, "deleted": True})

        except Exception as e:
            return await self._handle_firestore_error("delete_image_metadata", e)

    async def get_images_by_client(self, client_id: str) -> StandardResponse:
        """Get all images for a specific client."""
        try:
            logger.debug(f"🔍 Retrieving images for client_id: {client_id}")

            query = self.images_collection.where(filter=FieldFilter("client_id", "==", client_id))
            results = [doc.to_dict() async for doc in query.stream()]

            logger.info(f"✅ Retrieved {len(results)} images for client_id: {client_id}")
            return StandardResponse.success(data=results)

        except Exception as e:
            return await self._handle_firestore_error("get_images_by_client", e)

    async def get_linked_images(self, client_id: str) -> StandardResponse:
        """Get all linked images for a specific client."""
        try:
            logger.debug(f"🔍 Retrieving linked images for client_id: {client_id}")

            query = self.images_collection.where(filter=FieldFilter("client_id", "==", client_id)).where(
                filter=FieldFilter("is_linked", "==", True)
            )
            results = [doc.to_dict() async for doc in query.stream()]

            logger.info(f"✅ Retrieved {len(results)} linked images for client_id: {client_id}")
            return StandardResponse.success(data=results)

        except Exception as e:
            return await self._handle_firestore_error("get_linked_images", e)

    async def get_unlinked_images(self, client_id: str) -> StandardResponse:
        """Get all unlinked images for a specific client."""
        try:
            logger.debug(f"🔍 Retrieving unlinked images for client_id: {client_id}")

            query = self.images_collection.where(filter=FieldFilter("client_id", "==", client_id)).where(
                filter=FieldFilter("is_linked", "==", False)
            )
            results = [doc.to_dict() async for doc in query.stream()]

            logger.info(f"✅ Retrieved {len(results)} unlinked images for client_id: {client_id}")
            return StandardResponse.success(data=results)

        except Exception as e:
            return await self._handle_firestore_error("get_linked_images", e)

    async def mark_image_as_linked(self, image_id: str, linked_with: str = None, linked_with_id: str = None) -> StandardResponse:
        """Mark an image as linked, optionally with context (unit/project and id)."""
        try:
            logger.debug(f"🔗 Marking image as linked: {image_id}")

            update_data = {"is_linked": True, "updated_at": TimeManager.get_time_now_isoformat()}
            if linked_with:
                update_data["linked_with"] = linked_with
            if linked_with_id:
                update_data["linked_with_id"] = linked_with_id

            return await self.update_image_metadata(image_id, update_data)

        except Exception as e:
            return await self._handle_firestore_error("mark_image_as_linked", e)

    async def mark_image_as_unlinked(self, image_id: str) -> StandardResponse:
        """Mark an image as unlinked."""
        try:
            logger.debug(f"🔗 Marking image as unlinked: {image_id}")

            update_data = {"is_linked": False, "updated_at": TimeManager.get_time_now_isoformat()}

            return await self.update_image_metadata(image_id, update_data)

        except Exception as e:
            return await self._handle_firestore_error("mark_image_as_unlinked", e)

    async def get_all_clients_stats(self) -> StandardResponse:
        """Get comprehensive statistics about images for all clients."""
        try:
            logger.debug(f"🔍 Retrieving image statistics for all clients")

            # Get all images from all clients
            query = self.images_collection
            all_images = [doc.to_dict() async for doc in query.stream()]

            logger.debug(f"📊 Retrieved {len(all_images)} total images")

            # Group images by client_id
            client_images = {}
            for img in all_images:
                client_id = img.get("client_id")
                if client_id:
                    if client_id not in client_images:
                        client_images[client_id] = []
                    client_images[client_id].append(img)

            logger.debug(f"📊 Grouped into {len(client_images)} clients")

            # Calculate statistics for each client
            all_clients_stats = []

            for client_id, images in client_images.items():
                try:
                    logger.debug(f"📊 Processing client: {client_id} with {len(images)} images")

                    # Calculate statistics for this client
                    total_images = len(images)
                    linked_images = len([img for img in images if img.get("is_linked", False)])
                    unlinked_images = total_images - linked_images

                    # Calculate total size with safe conversion
                    total_size_kb = 0
                    for img in images:
                        size_kb = img.get("size_kb")
                        if size_kb is not None and size_kb != "":
                            try:
                                total_size_kb += float(size_kb)
                            except (ValueError, TypeError) as e:
                                logger.warning(f"⚠️ Invalid size_kb value for image {img.get('image_id')}: {size_kb}")

                    # Calculate average dimensions with safe conversion
                    valid_dimensions = []
                    for img in images:
                        width = img.get("width")
                        height = img.get("height")
                        if width is not None and height is not None and width != "" and height != "":
                            try:
                                valid_dimensions.append((float(width), float(height)))
                            except (ValueError, TypeError) as e:
                                logger.warning(f"⚠️ Invalid dimensions for image {img.get('image_id')}: width={width}, height={height}")

                    avg_width = sum(w for w, h in valid_dimensions) / len(valid_dimensions) if valid_dimensions else 0
                    avg_height = sum(h for w, h in valid_dimensions) / len(valid_dimensions) if valid_dimensions else 0

                    # Get file type distribution
                    extensions = {}
                    for img in images:
                        ext = img.get("extension", "unknown")
                        extensions[ext] = extensions.get(ext, 0) + 1

                    # Get size distribution with safe conversion
                    small_count = 0
                    medium_count = 0
                    large_count = 0
                    valid_sizes = []

                    for img in images:
                        size_kb = img.get("size_kb")
                        if size_kb is not None and size_kb != "":
                            try:
                                size_float = float(size_kb)
                                valid_sizes.append(size_float)
                                if size_float < 100:
                                    small_count += 1
                                elif size_float < 500:
                                    medium_count += 1
                                else:
                                    large_count += 1
                            except (ValueError, TypeError) as e:
                                logger.warning(f"⚠️ Invalid size_kb for size distribution: {size_kb}")

                    size_ranges = {"small": small_count, "medium": medium_count, "large": large_count}

                    # Get recent uploads (last 30 days)
                    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
                    recent_uploads = 0

                    for img in images:
                        created_at = img.get("created_at")
                        if created_at:
                            try:
                                # Handle both string and datetime objects from Firestore
                                if isinstance(created_at, str):
                                    # Handle string datetime formats
                                    if created_at.endswith("Z"):
                                        created_at = created_at.replace("Z", "+00:00")
                                    parsed_date = datetime.fromisoformat(created_at)
                                else:
                                    # Handle Firestore DatetimeWithNanoseconds object
                                    parsed_date = created_at

                                # Ensure both dates are timezone-aware for comparison
                                if parsed_date.tzinfo is None:
                                    parsed_date = parsed_date.replace(tzinfo=timezone.utc)
                                if thirty_days_ago.tzinfo is None:
                                    thirty_days_ago = thirty_days_ago.replace(tzinfo=timezone.utc)

                                # Debug logging for date comparison
                                logger.debug(
                                    f"📅 Image {img.get('image_id')}: parsed_date={parsed_date}, thirty_days_ago={thirty_days_ago}"
                                )
                                logger.debug(f"📅 Comparison: {parsed_date} > {thirty_days_ago} = {parsed_date > thirty_days_ago}")

                                # Calculate and log the time difference
                                time_diff = parsed_date - thirty_days_ago
                                logger.debug(f"📅 Time difference: {time_diff} (days: {time_diff.days})")

                                if parsed_date > thirty_days_ago:
                                    recent_uploads += 1
                                    logger.debug(f"✅ Counted as recent upload for image {img.get('image_id')}")
                                else:
                                    logger.debug(f"❌ Not counted as recent upload for image {img.get('image_id')}")
                            except (ValueError, TypeError, AttributeError) as e:
                                logger.warning(
                                    f"⚠️ Invalid created_at for image {img.get('image_id')}: {created_at} (type: {type(created_at)})"
                                )
                                continue
                        else:
                            # Skip images without created_at
                            continue

                    # Log the final count for debugging
                    logger.debug(f"📊 Client {client_id}: Found {recent_uploads} recent uploads out of {total_images} total images")

                    # Compile statistics for this client with safe rounding
                    try:
                        client_stats = {
                            "client_id": client_id,
                            "total_images": int(total_images),
                            "linked_images": int(linked_images),
                            "unlinked_images": int(unlinked_images),
                            "total_size_kb": round(float(total_size_kb), 2),
                            "total_size_mb": round(float(total_size_kb) / 1024, 2),
                            "average_width": round(float(avg_width), 0),
                            "average_height": round(float(avg_height), 0),
                            "file_type_distribution": extensions,
                            "size_distribution": size_ranges,
                            "recent_uploads_30_days": int(recent_uploads),
                            "upload_rate_per_month": round(float(recent_uploads) / 1, 2),  # Assuming 1 month period
                            "average_image_size_kb": (round(float(total_size_kb) / int(total_images), 2) if total_images > 0 else 0),
                            "largest_image_kb": (float(max(valid_sizes)) if valid_sizes else 0),
                            "smallest_image_kb": (float(min(valid_sizes)) if valid_sizes else 0),
                        }
                    except (ValueError, TypeError) as e:
                        logger.error(f"❌ Error creating stats dict for client {client_id}: {str(e)}")
                        # Create a minimal stats dict with safe defaults
                        client_stats = {
                            "client_id": client_id,
                            "total_images": 0,
                            "linked_images": 0,
                            "unlinked_images": 0,
                            "total_size_kb": 0,
                            "total_size_mb": 0,
                            "average_width": 0,
                            "average_height": 0,
                            "file_type_distribution": {},
                            "size_distribution": {"small": 0, "medium": 0, "large": 0},
                            "recent_uploads_30_days": 0,
                            "upload_rate_per_month": 0,
                            "average_image_size_kb": 0,
                            "largest_image_kb": 0,
                            "smallest_image_kb": 0,
                        }

                    all_clients_stats.append(client_stats)

                except Exception as e:
                    logger.error(f"❌ Error processing client {client_id}: {str(e)}")
                    logger.error(f"❌ Error type: {type(e).__name__}")
                    logger.error(f"❌ Full traceback: {traceback.format_exc()}")
                    # Continue with next client instead of failing completely
                    continue

            # Sort by total images (descending) with safe sorting
            try:
                all_clients_stats.sort(key=lambda x: int(x["total_images"]), reverse=True)
            except (ValueError, TypeError) as e:
                logger.warning(f"⚠️ Error sorting clients by total_images: {str(e)}")
                # Fallback: sort by client_id if total_images sorting fails
                all_clients_stats.sort(key=lambda x: str(x["client_id"]))

            logger.debug(f"✅ Retrieved image statistics for {len(all_clients_stats)} clients")
            return StandardResponse.success(data=all_clients_stats)

        except Exception as e:
            return await self._handle_firestore_error("get_all_clients_stats", e)
