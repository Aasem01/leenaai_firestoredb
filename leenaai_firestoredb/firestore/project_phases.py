from __future__ import annotations

import asyncio
from threading import Lock
from typing import List
from uuid import uuid4

from ..schemas.collection_names import DatabaseCollectionNames

# Remove the direct import
# from src.database.firestore_projects_db import FirestoreProjectsDB
from .projects import FirestoreProjectsDB
from .client import FirestoreClient
from src.utils.error_codes import CustomError, ErrorCodes
from src.utils.logger import logger
from src.utils.projects_schema_classes import PhaseUpdate, ProjectPhase
from src.utils.standard_response import StandardResponse
from src.utils.time_now import TimeManager


class FirestoreProjectPhasesDB:
    """
    Firestore wrapper for Project Phase operations.

    Handles all CRUD operations for project phases within projects.
    Access it everywhere via:
        phase_db = FirestoreProjectPhasesDB.shared()
    """

    # ------------------------------------------------------------------ #
    #  ─── Singleton plumbing (per interpreter / per worker) ──────────── #
    # ------------------------------------------------------------------ #
    _instance: "FirestoreProjectPhasesDB | None" = None
    _instance_lock = Lock()  # thread-safe construction guard

    @classmethod
    def shared(cls) -> "FirestoreProjectPhasesDB":
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
                    projects_response = await asyncio.wait_for(
                        FirestoreProjectsDB.shared().getAllProjects(client_id), timeout=30.0  # 30 second timeout
                    )
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

    # ------------------------------------------------------------------ #
    #  ─── Project Phase CRUD Operations ─────────────── #
    # ------------------------------------------------------------------ #

    async def create_phase(self, project_id: str, phase: ProjectPhase) -> StandardResponse:
        """
        Add a new phase to an existing project.
        """
        try:
            # Get the existing project
            project_doc = await self._coll.document(project_id).get()
            if not project_doc.exists:
                return StandardResponse.not_found("Project not found")

            project_data = project_doc.to_dict()

            # Ensure phase has an ID
            if not phase.id:
                phase.id = uuid4()

            # Convert phase to dict and ensure id is string
            phase_dict = phase.model_dump()
            phase_dict["id"] = str(phase_dict["id"])

            # Add the new phase to the project's phases list
            if "phases" not in project_data:
                project_data["phases"] = []

            # Check if phase with same ID already exists
            existing_ids = [p.get("id") for p in project_data["phases"]]
            if phase_dict["id"] in existing_ids:
                raise StandardResponse.conflict(error_message="Phase with this ID already exists")

            project_data["phases"].append(phase_dict)

            # Update the project in Firestore
            await self._coll.document(project_id).update({"phases": project_data["phases"]})
            await self.invalidate_cache_in_BG()

            # Sync with Chroma
            await self._sync_with_chroma(project_data.get("client_id"))

            logger.info("✅ Created phase %s for project %s", phase_dict["id"], project_id)
            return StandardResponse.success(data=phase_dict, message="Project phase created successfully")

        except Exception as e:
            return self._handle_error(e, "creating project phase")

    async def invalidate_cache_in_BG(self) -> None:
        await FirestoreProjectsDB.shared().invalidate_caches()

    async def get_all_phases(self, project_id: str) -> StandardResponse:
        """
        Get all phases for a specific project.
        """
        try:
            project_doc = await self._coll.document(project_id).get()
            if not project_doc.exists:
                return StandardResponse.not_found("Project not found")

            project_data = project_doc.to_dict()
            phases = project_data.get("phases", [])

            return StandardResponse.success(data=phases, message=f"Retrieved {len(phases)} project phases successfully")

        except Exception as e:
            return self._handle_error(e, "getting project phases")

    async def get_phase(self, project_id: str, phase_id: str) -> StandardResponse:
        """
        Get a specific phase by project ID and phase ID.
        """
        try:
            project_doc = await self._coll.document(project_id).get()
            if not project_doc.exists:
                return StandardResponse.not_found("Project not found")

            project_data = project_doc.to_dict()
            phases = project_data.get("phases", [])

            # Find the specific phase
            phase = next((p for p in phases if p.get("id") == phase_id), None)
            if not phase:
                return StandardResponse.not_found("Phase not found")

            return StandardResponse.success(data=phase, message="Project phase retrieved successfully")

        except Exception as e:
            return self._handle_error(e, "getting project phase")

    async def update_phase(self, project_id: str, phase_id: str, phase_update: PhaseUpdate) -> StandardResponse:
        """
        Update a specific phase within a project.
        Only description and master_plan can be updated.
        """
        try:
            project_doc = await self._coll.document(project_id).get()
            if not project_doc.exists:
                return StandardResponse.not_found("Project not found")

            project_data = project_doc.to_dict()
            phases = project_data.get("phases", [])

            # Find and update the specific phase
            phase_index = next((i for i, p in enumerate(phases) if p.get("id") == phase_id), None)
            if phase_index is None:
                return StandardResponse.not_found("Phase not found")

            # Update only the allowed fields
            update_dict = phase_update.model_dump(exclude_unset=True)

            # Only allow updating description and master_plan
            allowed_fields = {"description", "master_plan", "images"}
            update_dict = {k: v for k, v in update_dict.items() if k in allowed_fields}

            if not update_dict:
                return StandardResponse.bad_request("No valid fields to update provided")

            # Add updated_at timestamp
            update_dict["updated_at"] = TimeManager.get_time_now_isoformat()

            # Merge the updates with existing phase data
            phases[phase_index].update(update_dict)

            # Update the project in Firestore
            await self._coll.document(project_id).update({"phases": phases})
            await self.invalidate_cache_in_BG()

            # Sync with Chroma
            await self._sync_with_chroma(project_data.get("client_id"))

            logger.info("✅ Updated phase %s for project %s", phase_id, project_id)
            return StandardResponse.success(data=phases[phase_index], message="Project phase updated successfully")

        except Exception as e:
            return self._handle_error(e, "updating project phase")

    async def delete_phase(self, project_id: str, phase_id: str, client_id: str) -> StandardResponse:
        """
        Delete a specific phase from a project.
        """
        try:
            project_doc = await self._coll.document(project_id).get()
            if not project_doc.exists:
                return StandardResponse.not_found("Project not found")

            project_data = project_doc.to_dict()

            # Check if project belongs to the client
            if project_data.get("client_id") != client_id:
                return StandardResponse.failure(
                    code=ErrorCodes.UNAUTHORIZED, error_message="You don't have permission to delete phases from this project"
                )

            phases = project_data.get("phases", [])

            # Find the specific phase to delete its files
            phase_to_delete = None
            for phase in phases:
                if phase.get("id") == phase_id:
                    phase_to_delete = phase
                    break

            if not phase_to_delete:
                return StandardResponse.not_found("Phase not found")

            # Delete phase images and master_plan files from GCS
            deleted_images_count = await self._delete_phase_images(phase_to_delete)
            deleted_master_plan_count = await self._delete_phase_master_plan_files(phase_to_delete)

            # Remove the phase from the phases list
            original_length = len(phases)
            phases = [p for p in phases if p.get("id") != phase_id]

            if len(phases) == original_length:
                return StandardResponse.not_found("Phase not found")

            # Update the project in Firestore
            await self._coll.document(project_id).update({"phases": phases})
            await self.invalidate_cache_in_BG()

            # Sync with Chroma
            await self._sync_with_chroma(client_id)

            logger.info("✅ Deleted phase %s from project %s", phase_id, project_id)
            return StandardResponse.success(
                data={
                    "projectId": project_id,
                    "phaseId": phase_id,
                    "deletedImagesCount": deleted_images_count,
                    "deletedMasterPlanCount": deleted_master_plan_count,
                },
                message=f"Project phase deleted successfully. {deleted_images_count} images and {deleted_master_plan_count} master plan files were also deleted from GCS.",
            )

        except Exception as e:
            return self._handle_error(e, "deleting project phase")

    async def _delete_phase_images(self, phase_data: dict) -> int:
        """
        Extracts image URLs from phase data and deletes them from GCS.
        Expected structure: images = [{"fileId": "...", "url": "..."}, ...]
        Returns the count of deleted images.
        """
        try:
            deleted_count = 0

            # Get images list from phase data
            images = phase_data.get("images")
            if not images or not isinstance(images, list):
                logger.info(f"ℹ️ No images found for phase or images is not a list")
                return 0

            logger.info(f"🔍 Found {len(images)} images to delete for phase")

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
                        logger.info(f"🗑️ Deleted phase image: {file_id} (URL: {image_url})")
                    else:
                        logger.warning(f"⚠️ Failed to delete phase image: {file_id}")

                except Exception as e:
                    logger.error(f"❌ Error deleting phase image {file_id}: {str(e)}")

            logger.info(f"🗑️ Successfully deleted {deleted_count} out of {len(images)} phase images from GCS")
            return deleted_count

        except Exception as e:
            logger.error(f"❌ Error in _delete_phase_images: {str(e)}")
            return 0

    async def _delete_phase_master_plan_files(self, phase_data: dict) -> int:
        """
        Extracts master_plan file URL from phase data and deletes it from GCS.
        Expected structure: master_plan = {"fileId": "...", "url": "..."}
        Returns 1 if deleted, 0 otherwise.
        """
        try:
            master_plan_file = phase_data.get("master_plan")
            if not master_plan_file or not isinstance(master_plan_file, dict):
                logger.info(f"ℹ️ No master plan file found for phase or master_plan is not a dict")
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
                    logger.info(f"🗑️ Deleted phase master plan file: {file_id} (URL: {file_url})")
                    return 1
                else:
                    logger.warning(f"⚠️ Failed to delete phase master plan file: {file_id}")
                    return 0
            except Exception as e:
                logger.error(f"❌ Error deleting phase master plan file {file_id}: {str(e)}")
                return 0
        except Exception as e:
            logger.error(f"❌ Error in _delete_phase_master_plan_files: {str(e)}")
            return 0

    async def get_phases_by_name(self, project_id: str, phase_name: str) -> StandardResponse:
        """
        Get phases by name within a specific project.
        """
        try:
            project_doc = await self._coll.document(project_id).get()
            if not project_doc.exists:
                return StandardResponse.not_found("Project not found")

            project_data = project_doc.to_dict()
            phases = project_data.get("phases", [])

            # Filter phases by name (case-insensitive)
            matching_phases = [p for p in phases if p.get("name", "").lower() == phase_name.lower()]

            return StandardResponse.success(data=matching_phases, message=f"Found {len(matching_phases)} matching phases")

        except Exception as e:
            return self._handle_error(e, "getting phases by name")

    async def count_phases(self, project_id: str) -> StandardResponse:
        """
        Get the count of phases for a specific project.
        """
        try:
            project_doc = await self._coll.document(project_id).get()
            if not project_doc.exists:
                return StandardResponse.not_found("Project not found")

            project_data = project_doc.to_dict()
            phases = project_data.get("phases", [])

            return StandardResponse.success(data={"count": len(phases)}, message=f"Project has {len(phases)} phases")

        except Exception as e:
            return self._handle_error(e, "counting project phases")

    async def get_all_phases_across_projects(self) -> StandardResponse:
        """
        Get all phases from all projects (useful for analytics or global operations).
        """
        try:
            # Get all projects
            projects_docs = await self._coll.get()
            all_phases = []

            for doc in projects_docs:
                if not doc.exists:
                    continue

                project_data = doc.to_dict()
                project_phases = project_data.get("phases", [])

                # Add project context to each phase
                for phase in project_phases:
                    phase_with_context = {**phase, "project_id": project_data.get("id"), "project_name": project_data.get("name")}
                    all_phases.append(phase_with_context)

            return StandardResponse.success(data=all_phases, message=f"Retrieved {len(all_phases)} phases across all projects")

        except Exception as e:
            return self._handle_error(e, "getting all project phases")

    async def duplicate_phase(self, project_id: str, phase_id: str, new_name: str = None) -> StandardResponse:
        """
        Duplicate an existing phase within the same project.
        """
        try:
            # First get the existing phase
            phase_response = await self.get_phase(project_id, phase_id)
            if not phase_response.success:
                return phase_response

            existing_phase_data = phase_response.data

            # Create a new phase with the same data but new ID and optionally new name
            new_phase_data = existing_phase_data.copy()
            new_phase_data["id"] = str(uuid4())

            if new_name:
                new_phase_data["name"] = new_name
            else:
                new_phase_data["name"] = f"{existing_phase_data['name']} (Copy)"

            # Create ProjectPhase instance
            new_phase = ProjectPhase(**new_phase_data)

            # Create the duplicated phase
            return await self.create_phase(project_id, new_phase)

        except Exception as e:
            return self._handle_error(e, "duplicating project phase")

    # ------------------------------------------------------------------ #
    #  ─── Error helper ─────────────── #
    # ------------------------------------------------------------------ #
    def _handle_error(self, err: Exception, op: str) -> StandardResponse:
        logger.exception("❌ Error %s: %s", op, err)
        raise CustomError(str(err), ErrorCodes.INTERNAL_SERVER_ERROR)
