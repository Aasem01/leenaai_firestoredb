from __future__ import annotations

import asyncio
import json
from threading import Lock
from typing import List, Optional
from uuid import uuid4

from async_lru import alru_cache  # pip install async-lru
from google.cloud.firestore_v1.base_query import FieldFilter, Or

from ..schemas.collection_names import DatabaseCollectionNames
from .client import FirestoreClient
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger
from src.utils.projects_schema_classes import ProjectCreate, ProjectUpdate
from src.utils.standard_response import StandardResponse
from src.utils.storage_project_details import project_data_store
from src.utils.time_it import time_it
from src.utils.time_now import TimeManager


class FirestoreProjectsDB:
    """
    Firestore wrapper with read-through cache.

    Access it everywhere via:
        db = FirestoreProjectsDB.shared()
    """

    # ------------------------------------------------------------------ #
    #  ─── Singleton plumbing (per interpreter / per worker) ──────────── #
    # ------------------------------------------------------------------ #
    _instance: "FirestoreProjectsDB | None" = None
    _instance_lock = Lock()  # thread-safe construction guard

    @classmethod
    def shared(cls) -> "FirestoreProjectsDB":
        """
        Return the per-worker singleton instance.

        Thread-safe and re-entrant: the first caller creates the object,
        subsequent callers get the cached one.
        """
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------ #
    #  ─── Initialisation ─────────────── #
    # ------------------------------------------------------------------ #
    def __init__(self) -> None:
        # ❗ do NOT call this directly → always use .shared()
        self._coll = FirestoreClient.shared().collection(DatabaseCollectionNames.PROJECTS_COLLECTION_NAME.value)
        self._cold_start_lock = asyncio.Lock()  # avoids thundering-herd

    # ------------------------------------------------------------------ #
    #  ─── Internal caches ─────────────── #
    # ------------------------------------------------------------------ #
    @alru_cache(maxsize=1)
    async def _load_all_raw(self) -> List[dict]:
        """
        Fetch all documents once, cache in RAM until invalidated.
        Uses a cold start lock to prevent multiple simultaneous fetches.
        """
        async with self._cold_start_lock:
            try:
                docs = await self._coll.get()
                data = [d.to_dict() for d in docs if d.exists]
                return data
            except Exception as e:
                logger.error(f"❌ Failed to fetch projects from Firestore: {str(e)}")
                raise

    async def invalidate_caches(self) -> None:
        """
        Clear all in-memory caches after mutations.
        This is called after any write operation to ensure data consistency.
        """

        def _clear_caches():
            try:
                self._load_all_raw.cache_clear()
                self._build_location_tree.cache_clear()
                logger.info("🔄 Invalidate caches")
            except Exception as e:
                logger.error(f"❌ Error clearing caches: {str(e)}")

        # Run cache clearing in background thread
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _clear_caches)

            # Also invalidate CityNormalizeResolver caches to ensure data consistency
            # Import here to avoid circular import
            from src.utils.cached_egypt_locations import CachedEgyptLocations

            CachedEgyptLocations.shared().invalidate_caches()
        except Exception as e:
            logger.error(f"❌ Error in cache invalidation: {str(e)}")
            # Don't re-raise - we want to continue even if cache clearing fails

    # ------------------------------------------------------------------ #
    #  ─── Public read helpers (cached) ─────────────── #
    # ------------------------------------------------------------------ #
    async def getAllProjects(
        self, client_id: Optional[str] = None, city: Optional[str] = None, district: Optional[str] = None
    ) -> StandardResponse:
        """
        Return all project documents from the database as a list of dictionaries.
        If client_id is provided, only return projects belonging to that client.
        """
        try:
            records = await self._load_all_raw()
            if city:
                records = [record for record in records if record.get("city") == city]
            if district:
                records = [record for record in records if record.get("district") == district]
            # Ensure phases, payment_plans, and properties_types are always lists
            for record in records:
                record["phases"] = record.get("phases", [])
                record["payment_plans"] = record.get("payment_plans", [])
                record["properties_types"] = record.get("properties_types", [])

            # If client_id is provided, filter projects for that client
            if client_id:
                records = [record for record in records if record.get("client_id") == client_id]
                if not records:
                    return StandardResponse.failure(code=ErrorCodes.NOT_FOUND, error_message=f"No projects found for client {client_id}")

            return StandardResponse.success(data=records, message="Projects retrieved successfully")
        except Exception as e:
            return self._handle_error(e, "getting all projects")

    @alru_cache(maxsize=1)
    async def _build_location_tree(self) -> dict:
        """
        Build {Country → {City → {District → [Projects]}}} dictionary, then cache it.
        """
        records = await self._load_all_raw()

        location_tree = {"egypt": {}}
        for p in records:
            country = p.get("country", "egypt").lower()
            city = p.get("city", "")
            district = p.get("district", "")
            en_name = p.get("en_name", "")
            ar_name = p.get("ar_name", "")
            phases = p.get("phases", [])

            if not all([city, district, en_name]):
                continue

            if country not in location_tree:
                location_tree[country] = {}

            if city not in location_tree[country]:
                location_tree[country][city] = {}

            if district not in location_tree[country][city]:
                location_tree[country][city][district] = []

            if not any(proj.get("en_name") == en_name for proj in location_tree[country][city][district]):
                location_tree[country][city][district].append({"en_name": en_name, "ar_name": ar_name, "phases": phases})

        return location_tree

    @alru_cache(maxsize=1)
    async def getLocationTree(self) -> StandardResponse:
        """Cached location resolver."""
        try:
            tree = await self._build_location_tree()
            return StandardResponse.success(data=tree, message="Projects tree retrieved successfully")
        except Exception as e:
            return self._handle_error(e, "building location tree")

    # ------------------------------------------------------------------ #
    #  ─── CRUD (invalidate caches) ─────────────── #
    # ------------------------------------------------------------------ #
    def _extract_image_ids_from_project(self, project_data: dict) -> List[str]:
        """
        Extract image fileIds from project data including phases and master_plan.
        Project images are stored as:
        - images = [{"fileId": "...", "url": "..."}, ...]
        - phases = [{"images": [{"fileId": "...", "url": "..."}, ...]}, ...]
        - master_plan = {"fileId": "...", "url": "..."}
        """
        image_ids = []

        # Extract images from main project images
        if "images" in project_data and isinstance(project_data["images"], list):
            for image in project_data["images"]:
                if isinstance(image, dict) and "fileId" in image:
                    image_ids.append(image["fileId"])

        # Extract images from phases
        if "phases" in project_data and isinstance(project_data["phases"], list):
            for phase in project_data["phases"]:
                if isinstance(phase, dict) and "images" in phase and isinstance(phase["images"], list):
                    for image in phase["images"]:
                        if isinstance(image, dict) and "fileId" in image:
                            image_ids.append(image["fileId"])

        # Extract image from master_plan
        if "master_plan" in project_data and isinstance(project_data["master_plan"], dict):
            master_plan = project_data["master_plan"]
            if "fileId" in master_plan:
                image_ids.append(master_plan["fileId"])

        return image_ids

    async def _mark_project_images_as_linked(self, project_data: dict) -> None:
        """
        Extract image IDs from project data and mark them as linked.
        """
        try:
            image_ids = self._extract_image_ids_from_project(project_data)
            project_id = project_data.get("id")

            if not image_ids:
                logger.info("ℹ️ No images found in project data to mark as linked")
                return

            logger.info(f"🔍 Found {len(image_ids)} images in project to mark as linked")

            from src.database.firestore_images_db import FirestoreImagesDB

            marked_count = 0

            for image_id in image_ids:
                try:
                    result = await FirestoreImagesDB.shared().mark_image_as_linked(
                        image_id, linked_with="project", linked_with_id=project_id
                    )
                    if result.status:
                        marked_count += 1
                        logger.debug(f"✅ Marked image {image_id} as linked")
                    else:
                        logger.warning(f"⚠️ Failed to mark image {image_id} as linked: {result.error_message}")
                except Exception as e:
                    logger.error(f"❌ Error marking image {image_id} as linked: {str(e)}")

            logger.info(f"🔗 Successfully marked {marked_count} out of {len(image_ids)} images as linked")

        except Exception as e:
            logger.error(f"❌ Error in _mark_project_images_as_linked: {str(e)}")

    async def create_project(self, project: ProjectCreate) -> StandardResponse:
        try:
            if not project.id:
                project.id = str(uuid4())
            # Convert project data to dict and ensure id is string
            project_dict = project.model_dump()
            project_dict["id"] = str(project_dict["id"])
            # Ensure city and district are lowercase
            if "city" in project_dict:
                project_dict["city"] = project_dict["city"].lower()
            if "district" in project_dict:
                project_dict["district"] = project_dict["district"].lower()
            await self._coll.document(str(project.id)).set(project_dict)
            await self.invalidate_caches()

            # Mark images as linked
            await self._mark_project_images_as_linked(project_dict)

            # Sync with Chroma
            await self._sync_with_chroma(project_dict.get("client_id"))

            return StandardResponse.success(data=project_dict, message="Project created successfully")
        except Exception as e:
            return self._handle_error(e, "creating project")

    async def delete_project(self, project_id: str, client_id: str) -> StandardResponse:
        try:
            docs = await self._coll.where(filter=FieldFilter("id", "==", project_id)).get()
            if not docs:
                return StandardResponse.not_found("Project not found")

            for doc in docs:
                if doc.to_dict().get("client_id") != client_id:
                    return StandardResponse.failure(
                        code=ErrorCodes.UNAUTHORIZED, error_message="You don't have permission to delete this project"
                    )

                # Get project data for image deletion
                project_data = doc.to_dict()
                project_name = project_data.get("en_name")

                # Check if any units are associated with this project
                from src.database.firestore_units_db import propertiesDB
                units_response = await propertiesDB.get_filtered_by_client_units({"project_name": project_name})

                if units_response.status and units_response.data.get("units"):
                    return StandardResponse.failure(
                        code=ErrorCodes.CONFLICT,
                        error_message="This project is associated with multiple units. Kindly contact support to delete it",
                    )

                # Delete project images and master_plan files from GCS
                deleted_images_count = await self._delete_project_images(project_data)
                deleted_master_plan_count = await self._delete_project_master_plan_files(project_data)

                # Delete the project from Firestore
                await doc.reference.delete()

            await self.invalidate_caches()

            # Sync with Chroma
            await self._sync_with_chroma(client_id)

            return StandardResponse.success(
                data={
                    "projectId": project_id,
                    "deletedImagesCount": deleted_images_count,
                    "deletedMasterPlanCount": deleted_master_plan_count,
                },
                message=f"Project deleted successfully. {deleted_images_count} images and {deleted_master_plan_count} master plan files were also deleted from GCS.",
            )
        except Exception as e:
            return self._handle_error(e, "deleting project")

    async def _delete_project_images(self, project_data: dict) -> int:
        """
        Extracts image URLs from project data and deletes them from GCS.
        Expected structure: images = [{"fileId": "...", "url": "..."}, ...]
        Returns the count of deleted images.
        """
        try:
            deleted_count = 0

            # Get images list from project data
            images = project_data.get("images")
            if not images or not isinstance(images, list):
                logger.info(f"ℹ️ No images found for project or images is not a list")
                return 0

            logger.info(f"🔍 Found {len(images)} images to delete for project")

            for image_data in images:
                if not isinstance(image_data, dict):
                    logger.warning(f"⚠️ Skipping invalid image data: {image_data}")
                    continue

                # Extract fileId and url from image dict
                file_id = image_data.get("fileId")
                image_url = image_data.get("url")

                if not file_id:
                    logger.warning(f"⚠️ Skipping image with no fileId: {image_data}")
                    continue

                try:
                    # Import GCS service here to avoid circular imports
                    from src.image_crud.gcs_service_provider import gcs_service

                    # Delete image from GCS using fileId
                    result = gcs_service.delete_image(file_id)
                    if result.get("message") == "Image deleted successfully":
                        deleted_count += 1
                        logger.info(f"🗑️ Deleted project image: {file_id} (URL: {image_url})")
                    else:
                        logger.warning(f"⚠️ Failed to delete project image: {file_id}")

                except Exception as e:
                    logger.error(f"❌ Error deleting project image {file_id}: {str(e)}")

            logger.info(f"🗑️ Successfully deleted {deleted_count} out of {len(images)} project images from GCS")
            return deleted_count

        except Exception as e:
            logger.error(f"❌ Error in _delete_project_images: {str(e)}")
            return 0

    async def _delete_project_master_plan_files(self, project_data: dict) -> int:
        """
        Extracts master_plan file URL from project data and deletes it from GCS.
        Expected structure: master_plan = {"fileId": "...", "url": "..."}
        Returns 1 if deleted, 0 otherwise.
        """
        try:
            master_plan_file = project_data.get("master_plan")
            if not master_plan_file or not isinstance(master_plan_file, dict):
                logger.info(f"ℹ️ No master plan file found for project or master_plan is not a dict")
                return 0
            file_id = master_plan_file.get("fileId")
            file_url = master_plan_file.get("url")
            if not file_id:
                logger.warning(f"⚠️ Skipping master plan file with no fileId: {master_plan_file}")
                return 0
            try:
                from src.image_crud.gcs_service_provider import gcs_service

                result = gcs_service.delete_image(file_id)
                if result.get("message") == "Image deleted successfully":
                    logger.info(f"🗑️ Deleted project master plan file: {file_id} (URL: {file_url})")
                    return 1
                else:
                    logger.warning(f"⚠️ Failed to delete project master plan file: {file_id}")
                    return 0
            except Exception as e:
                logger.error(f"❌ Error deleting project master plan file {file_id}: {str(e)}")
                return 0
        except Exception as e:
            logger.error(f"❌ Error in _delete_project_master_plan_files: {str(e)}")
            return 0

    async def get_project(self, project_id: str) -> StandardResponse:
        try:
            docs = await self._coll.where(filter=FieldFilter("id", "==", project_id)).get()
            if not docs:
                return StandardResponse.not_found("Project not found")

            doc = docs[0]
            data = doc.to_dict()
            # Ensure phases, payment_plans, and properties_types are always lists
            data["phases"] = data.get("phases", [])
            data["payment_plans"] = data.get("payment_plans", [])
            data["properties_types"] = data.get("properties_types", [])
            return StandardResponse.success(data=data, message="Project retrieved successfully")
        except Exception as e:
            return self._handle_error(e, "getting project")

    async def get_project_by_name(self, en_name: str, ar_name: Optional[str] = None) -> StandardResponse:
        try:
            # Build filters for OR query
            filters = [FieldFilter("en_name", "==", en_name)]
            if ar_name:
                filters.append(FieldFilter("ar_name", "==", ar_name))

            # Single OR query to check both en_name and ar_name
            docs = await self._coll.where(filter=Or(filters)).get()

            if docs:
                data = docs[0].to_dict()
                # Ensure phases, payment_plans, and properties_types are always lists
                data["phases"] = data.get("phases", [])
                data["payment_plans"] = data.get("payment_plans", [])
                data["properties_types"] = data.get("properties_types", [])
                return StandardResponse.success(data=data, message="Project retrieved successfully")

            return StandardResponse.not_found("Project not found")
        except Exception as e:
            return self._handle_error(e, "getting project")

    async def get_projects_by_city_and_district(self, city: str, district: str) -> StandardResponse:
        try:
            # Convert city and district to lowercase before querying
            city = city.lower()
            district = district.lower()
            records = await self._load_all_raw()
            # Filter projects by city and district and return complete project data
            projects = [p for p in records if p.get("city", "").lower() == city and p.get("district", "").lower() == district]
            # Ensure phases, payment_plans, and properties_types are always lists for each project
            for project in projects:
                project["phases"] = project.get("phases", [])
                project["payment_plans"] = project.get("payment_plans", [])
                project["properties_types"] = project.get("properties_types", [])
            return StandardResponse.success(data=projects, message="Projects retrieved successfully")
        except Exception as e:
            return self._handle_error(e, "getting projects by city and district")

    @alru_cache(maxsize=1)
    async def getAllCitiesAndDistricts(self) -> StandardResponse:
        try:
            # Load cities and districts from egypt_localized_cities.json file
            with open("src/utils/egypt_localized_cities.json", "r", encoding="utf-8") as f:
                localized_cities = json.loads(f.read())

            # Transform the data into the desired format
            cities = []
            result = {"cities": cities}

            # Process each city from the localized cities data
            for city_data in localized_cities:
                city_name = city_data.get("city", "").lower()
                if city_name:
                    cities.append(city_name)

                    # Extract district names from the districts array
                    districts = []
                    for district in city_data.get("districts", []):
                        en_name = district.get("en_name", "")
                        if en_name:
                            districts.append(en_name.lower())

                    result[city_name] = districts

            return StandardResponse.success(data=result, message="Location tree retrieved successfully")
        except Exception as e:
            return self._handle_error(e, "getting location tree data")

    async def update_project_fields(self, project_id: str, update_data: ProjectUpdate, client_id: str) -> StandardResponse:
        """
        Update specific fields of a project.
        Requires the project to belong to the specified client_id.
        """
        try:
            # Get the project first to ensure it exists and check ownership
            docs = await self._coll.where(filter=FieldFilter("id", "==", project_id)).get()
            if not docs:
                return StandardResponse.not_found("Project not found")

            doc = docs[0]
            project_data = doc.to_dict()

            # Get old name, preferring en_name if it exists
            old_project_name = project_data.get("en_name") or project_data.get("name", "")

            # Validate client ownership
            if project_data.get("client_id") != client_id:
                return StandardResponse.failure(
                    code=ErrorCodes.UNAUTHORIZED, error_message="You don't have permission to update this project"
                )

            ref = doc.reference

            # Only allow specific fields to be updated
            allowed_fields = set(ProjectUpdate.model_fields.keys())
            update_dict = {k: v for k, v in update_data.items() if k in allowed_fields and v is not None}

            if not update_dict:
                return StandardResponse.bad_request("No valid fields to update provided")

            # Add updated_at timestamp
            update_dict["updated_at"] = TimeManager.get_time_now_isoformat()

            await ref.update(update_dict)
            await self.invalidate_caches()

            # Mark images as linked if images or phases are being updated
            if "images" in update_dict or "phases" in update_dict:
                await self._mark_project_images_as_linked(update_dict)

            # Get the updated document
            updated_doc = await ref.get()
            updated_data = updated_doc.to_dict()

            # Ensure phases, payment_plans, and properties_types are always lists
            updated_data["phases"] = updated_data.get("phases", [])
            updated_data["payment_plans"] = updated_data.get("payment_plans", [])
            updated_data["properties_types"] = updated_data.get("properties_types", [])

            # Sync with Chroma
            await self._sync_with_chroma(client_id)

            # Check if en_name exists in update_data and update related units
            if any(field in update_dict for field in ["en_name", "city", "district"]):
                logger.info(f"Updating project name from '{old_project_name}' to '{update_dict['en_name']}'")

                # Update units with the old project name
                from src.database.firestore_units_db import propertiesDB
                await propertiesDB.update_units_by_new_project_name_and_sync(old_project_name=old_project_name, update_data=update_dict)

            return StandardResponse.success(data=updated_data, message="Project updated successfully")
        except Exception as e:
            return self._handle_error(e, "updating project fields")

    async def _sync_with_chroma(self, client_id: str):
        """
        Synchronizes projects from Firestore to Chroma in the background.
        """
        try:
            if not client_id:
                return StandardResponse.failure(ErrorCodes.BAD_REQUEST, "Client ID is required")

            async def sync_task():
                try:
                    # Add timeout to prevent indefinite hanging
                    projects_response = await asyncio.wait_for(self.getAllProjects(client_id), timeout=30.0)  # 30 second timeout
                    projects = []
                    if hasattr(projects_response, "status") and projects_response.status:
                        projects = projects_response.data
                    else:
                        logger.error(
                            f"❌ Failed to fetch projects for Chroma sync: {getattr(projects_response, 'error_message', 'Unknown error')}"
                        )
                        return
                    logger.info(f"✅ {len(projects)} projects fetched from Firestore for client_id={client_id}")

                    # Lazy import to avoid circular dependency
                    from ..vector.project_sync import projectDBToVectorSync

                    await projectDBToVectorSync.sync_firestore_to_chroma(client_id)
                except asyncio.TimeoutError:
                    logger.error(f"❌ Chroma sync task timed out for client_id={client_id}")
                except asyncio.CancelledError:
                    logger.warning(f"⚠️ Chroma sync task was cancelled for client_id={client_id}")
                except Exception as e:
                    logger.error(f"❌ Error in Chroma sync task: {str(e)}")

            # Create and store the task to prevent it from being garbage collected
            sync_task_obj = asyncio.create_task(sync_task())

            # Add error handling for the task
            def handle_task_completion(task):
                try:
                    if task.cancelled():
                        logger.warning(f"⚠️ Chroma sync task was cancelled for client_id={client_id}")
                    elif task.exception():
                        logger.error(f"❌ Chroma sync task failed: {str(task.exception())}")
                except Exception as e:
                    logger.error(f"❌ Error handling task completion: {str(e)}")

            sync_task_obj.add_done_callback(handle_task_completion)

            return StandardResponse.success(message="Started background sync of projects to Chroma")

        except Exception as e:
            logger.error(f"❌ Error initiating Chroma sync: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def update_missing_timestamps(self) -> StandardResponse:
        """
        Updates all projects that don't have an updated_at field with the current timestamp.
        Returns a summary of how many projects were updated.
        """
        try:
            from datetime import datetime

            from google.cloud.firestore_v1 import SERVER_TIMESTAMP

            # Get all projects
            projects_response = await self.getAllProjects()
            if not projects_response.status:
                return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, "Failed to fetch projects")

            projects = projects_response.data
            updated_count = 0
            skipped_count = 0
            error_count = 0
            error_projects = []

            logger.info(f"🔍 Checking {len(projects)} projects for missing timestamps...")

            for project in projects:
                project_id = project.get("id")
                project_name = project.get("name", "Unknown")

                if not project_id:
                    logger.warning(f"⚠️ Project without ID found: {project_name}, skipping")
                    error_count += 1
                    error_projects.append(f"{project_name} (no ID)")
                    continue

                # Check if updated_at is missing or None
                if not project.get("updated_at"):
                    try:
                        # Find the document by ID field instead of using it as document ID
                        docs = await self._coll.where(filter=FieldFilter("id", "==", project_id)).get()
                        if not docs:
                            logger.error(f"❌ Project not found in Firestore: {project_name} (ID: {project_id})")
                            error_count += 1
                            error_projects.append(f"{project_name} (ID: {project_id})")
                            continue

                        doc = docs[0]
                        # Update the project with current timestamp
                        await doc.reference.update({"updated_at": SERVER_TIMESTAMP})
                        updated_count += 1
                        logger.info(f"✅ Updated timestamp for project {project_name} (ID: {project_id})")
                    except Exception as e:
                        logger.error(f"❌ Failed to update timestamp for project {project_name} (ID: {project_id}): {str(e)}")
                        error_count += 1
                        error_projects.append(f"{project_name} (ID: {project_id})")
                else:
                    skipped_count += 1
                    logger.debug(f"⏭️ Project {project_name} (ID: {project_id}) already has timestamp: {project.get('updated_at')}")

            # Invalidate caches after updates
            await self.invalidate_caches()

            return StandardResponse.success(
                data={
                    "updated_count": updated_count,
                    "skipped_count": skipped_count,
                    "error_count": error_count,
                    "error_projects": error_projects,
                    "total_projects": len(projects),
                },
                message=f"Updated {updated_count} projects with missing timestamps. {error_count} errors occurred.",
            )

        except Exception as e:
            logger.error(f"❌ Error updating project timestamps: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def update_project_name_fields(self) -> StandardResponse:
        """
        Updates all projects to use en_name and ar_name fields instead of name.
        Sets ar_name to 'مشروعي' for all projects.
        Deletes the old name field after updating.
        Returns a summary of how many projects were updated.
        """
        try:
            # Get all projects
            projects_response = await self.getAllProjects()
            if not projects_response.status:
                return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, "Failed to fetch projects")

            projects = projects_response.data
            updated_count = 0
            skipped_count = 0
            error_count = 0
            error_projects = []

            logger.info(f"🔍 Checking {len(projects)} projects for name field updates...")

            for project in projects:
                project_id = project.get("id")
                project_name = project.get("name", "Unknown")

                if not project_id:
                    logger.warning(f"⚠️ Project without ID found: {project_name}, skipping")
                    error_count += 1
                    error_projects.append(f"{project_name} (no ID)")
                    continue

                try:
                    # Find the document by ID field
                    docs = await self._coll.where(filter=FieldFilter("id", "==", project_id)).get()
                    if not docs:
                        logger.error(f"❌ Project not found in Firestore: {project_name} (ID: {project_id})")
                        error_count += 1
                        error_projects.append(f"{project_name} (ID: {project_id})")
                        continue

                    doc = docs[0]
                    project_data = doc.to_dict()

                    # Check if project already has en_name and ar_name
                    if "en_name" in project_data and "ar_name" in project_data:
                        # If it has the new fields but still has the old name field, delete it
                        if "name" in project_data:
                            await doc.reference.update({"name": None})  # This will delete the field in Firestore
                            logger.info(f"✅ Deleted old name field for project {project_name} (ID: {project_id})")
                        skipped_count += 1
                        logger.debug(f"⏭️ Project {project_name} (ID: {project_id}) already has en_name and ar_name fields")
                        continue

                    # Update the project with new name fields and delete the old name field
                    update_data = {
                        "en_name": project_name,  # Use existing name as English name
                        "ar_name": "",  # Set Arabic name
                        "name": None,  # This will delete the old name field
                        "updated_at": TimeManager.get_time_now_isoformat(),
                    }

                    await doc.reference.update(update_data)
                    updated_count += 1
                    logger.info(f"✅ Updated name fields and deleted old name for project {project_name} (ID: {project_id})")

                except Exception as e:
                    logger.error(f"❌ Failed to update name fields for project {project_name} (ID: {project_id}): {str(e)}")
                    error_count += 1
                    error_projects.append(f"{project_name} (ID: {project_id})")

            # Invalidate caches after updates
            await self.invalidate_caches()

            return StandardResponse.success(
                data={
                    "updated_count": updated_count,
                    "skipped_count": skipped_count,
                    "error_count": error_count,
                    "error_projects": error_projects,
                    "total_projects": len(projects),
                },
                message=f"Updated {updated_count} projects with new name fields and deleted old name field. {error_count} errors occurred.",
            )

        except Exception as e:
            logger.error(f"❌ Error updating project name fields: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    # ------------------------------------------------------------------ #
    #  ─── Error helper ─────────────── #
    # ------------------------------------------------------------------ #
    def _handle_error(self, err: Exception, op: str) -> StandardResponse:
        logger.exception("❌ Error %s: %s", op, err)
        return StandardResponse.internal_error(str(err))
