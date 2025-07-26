import asyncio
from datetime import datetime, timezone

from fastapi import HTTPException
from google.cloud.firestore_v1.base_query import FieldFilter

from src.chatbots.share_text_generator import socailMediaGenerator
from ..schemas.collection_names import DatabaseCollectionNames
from .share import sharedLinksDB
from .units import propertiesDB
from .client import FirestoreClient
from src.utils.config import LOCAL_ENV
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger


class FirestoreSharingDataDB:
    _instance = None
    _cache = {}

    def __init__(self):
        self.sharing_data_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.SHARING_DATA_COLLECTION_NAME.value)
        if not LOCAL_ENV:
            # Preload cache in production environment
            asyncio.create_task(self._preload_cache())

    @classmethod
    def shared(cls) -> "FirestoreSharingDataDB":
        """Get the shared instance of FirestoreSharingDataDB"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def _preload_cache(self):
        """Preload all valid share links into the cache"""
        try:
            logger.info("🔄 Preloading share links cache...")
            # Query all share links
            query = sharedLinksDB.shared_collection
            docs = await query.get()

            current_time = datetime.now(timezone.utc)
            loaded_count = 0

            for doc in docs:
                data = doc.to_dict()
                # Convert expire_date to datetime if it's a string
                expire_date = data.get("expire_date")
                if isinstance(expire_date, str):
                    try:
                        expire_date = datetime.fromisoformat(expire_date.replace("Z", "+00:00"))
                    except ValueError:
                        logger.warning(f"Skipping invalid expire_date format: {expire_date}")
                        continue

                # Only cache valid (non-expired) links
                if expire_date and current_time < expire_date:
                    unit_id = data.get("unit_id")
                    if unit_id:
                        # Get post content from the post field
                        post_data = data.get("post", {})
                        # Create a post-like structure to match the cache format
                        cache_data = {
                            "arabic_post_text": post_data.get("arabic_post_text"),
                            "english_post_text": post_data.get("english_post_text"),
                            "share_link": data.get("link"),
                            "is_cached": True,
                            "expire_date": expire_date,
                        }
                        self._cache[unit_id] = cache_data
                        loaded_count += 1

            logger.info(f"✅ Preloaded {loaded_count} valid share links into cache")
        except Exception as e:
            logger.error(f"❌ Error preloading share links cache: {str(e)}")

    async def _get_existing_share_link(self, unit_id: str) -> dict:
        """Get existing share link from cache or database"""
        # First check cache
        if unit_id in self._cache:
            cached_data = self._cache[unit_id]
            logger.debug(f"📦 Cache data for unit {unit_id}: {cached_data}")
            # Check if the cached link is still valid
            expire_date = cached_data.get("expire_date")
            if isinstance(expire_date, str):
                try:
                    expire_date = datetime.fromisoformat(expire_date.replace("Z", "+00:00"))
                except ValueError:
                    logger.warning(f"Invalid expire_date format in cache: {expire_date}")
                    return None

            if expire_date and datetime.now(timezone.utc) < expire_date:
                logger.debug(f"✅ Using valid cached data for unit {unit_id}")
                return cached_data
            else:
                # Remove expired cache entry
                logger.debug(f"🔄 Removing expired cache entry for unit {unit_id}")
                del self._cache[unit_id]

        # If not in cache or expired, check database
        try:
            # Query the share links collection for this unit_id
            query = sharedLinksDB.shared_collection.where(filter=FieldFilter("unit_id", "==", unit_id)).limit(1)
            docs = await query.get()

            for doc in docs:
                data = doc.to_dict()
                logger.debug(f"📄 Database data for unit {unit_id}: {data}")
                # Check if the link is still valid
                expire_date = data.get("expire_date")
                if isinstance(expire_date, str):
                    try:
                        expire_date = datetime.fromisoformat(expire_date.replace("Z", "+00:00"))
                    except ValueError:
                        logger.warning(f"Invalid expire_date format in database: {expire_date}")
                        continue

                if expire_date and datetime.now(timezone.utc) < expire_date:
                    post_data = data.get("post", {})
                    cache_data = {
                        "arabic_post_text": post_data.get("arabic_post_text"),
                        "english_post_text": post_data.get("english_post_text"),
                        "share_link": data.get("link"),
                        "is_cached": True,
                        "expire_date": expire_date,
                    }
                    # Update cache
                    self._cache[unit_id] = cache_data
                    logger.debug(f"✅ Updated cache with database data for unit {unit_id}")
                    return cache_data
        except Exception as e:
            logger.error(f"Error checking existing share link: {str(e)}")

        return None

    async def create_sharing_link(self, client_id: str, unit_id: str) -> dict:
        try:
            # Check for existing share link
            existing_link = await self._get_existing_share_link(unit_id)
            if existing_link and existing_link.get("arabic_post_text") and existing_link.get("english_post_text"):
                logger.debug(f"Using existing share link for unit {unit_id}")
                share_link = existing_link.get("share_link")
                return {
                    "arabic_post_text": f"اتكلم مع لينا:\n{share_link}\n\n{existing_link.get('arabic_post_text')}",
                    "english_post_text": f"Talk to Lena:\n{share_link}\n\n{existing_link.get('english_post_text')}",
                    "share_link": share_link,
                    "is_cached": True,
                }

            # If no existing link, create new one
            property_doc = await propertiesDB.get_unit(unit_id)
            if not property_doc:
                raise HTTPException(status_code=ErrorCodes.NOT_FOUND, detail="Property not found")

            property_doc = property_doc.to_dict()

            property_data = property_doc.get("data", {})
            property = property_data if isinstance(property_data, dict) else property_data.to_dict()
            share_object = {
                "title": property.get("title"),
                "description": property.get("description"),
                "rooms_count": property.get("roomsCount"),
                "bathrooms_count": property.get("bathroomCount"),
                "area": property.get("landArea"),
                "city": property.get("city"),
                "project": property.get("project"),
                "view": property.get("view"),
                "floor": property.get("floor"),
                "building_type": property.get("buildingType"),
                "purpose": property.get("purpose"),
                "garden_size": property.get("gardenSize"),
            }
            # images = property.get("images", [])
            # images_urls = [image.get("url") for image in images if image]
            post = await socailMediaGenerator.generate_social_media_post(share_object)
            if post:
                # post["images"] = images_urls
                # share_object["images"] = images_urls
                sharable_link = await sharedLinksDB.create_unique_shared_link(
                    client_id=client_id, unit_id=unit_id, unit=share_object, post=post
                )
                link = sharable_link.data["link"]

                # Update cache with complete post data
                self._cache[unit_id] = {
                    "arabic_post_text": post["arabic_post_text"],
                    "english_post_text": post["english_post_text"],
                    "share_link": link,
                    "is_cached": False,
                    "expire_date": sharable_link.data.get("expire_date"),
                }

                # Return the post data with share link appended
                return {
                    "arabic_post_text": f"اتكلم مع لينا:\n{link}\n\n{post['arabic_post_text']}",
                    "english_post_text": f"Talk to Lena:\n{link}\n\n{post['english_post_text']}",
                    "share_link": link,
                    "is_cached": False,
                }

            return None

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating share link: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def get_shared_property(self, share_token: str) -> dict:
        try:
            doc_ref = self.sharing_data_collection.document(share_token)
            doc = await doc_ref.get()
            if not doc.exists:
                raise HTTPException(status_code=404, detail="Share token not found")

            share_data = doc.to_dict()

            # Expiry check
            expires_at = share_data.get("expires_at")
            if datetime.now(timezone.utc) > expires_at:
                await doc_ref.delete()
                raise HTTPException(status_code=410, detail="Share link has expired")

            # Get property data
            property_id = share_data.get("property_id")
            property_doc = await self.units_collection.document(property_id).get()
            if not property_doc.exists:
                raise HTTPException(status_code=404, detail="Property not found")
            return property_doc.to_dict()
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"❌ Error getting shared property: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
