import asyncio
import datetime
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import chromadb
import pandas as pd
from dateutil.parser import isoparse
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings

from ..schemas.collection_names import DatabaseCollectionNames
# Lazy import to avoid circular dependency
# from ..firestore.projects import FirestoreProjectsDB
from ..firestore.client import FirestoreClient
from src.utils.config import LOCAL_ENV, OPENAI_API_KEY
from src.utils.logger import logger
from src.utils.projects_schema_classes import ProjectCreate
from src.utils.test_accounts import test_client_ids
from src.utils.time_it import time_it
from src.utils.time_now import TimeManager
from src.vector_db.base_data_loader import BaseDataLoader
from src.vector_db.db_api_helper import DataBaseAPI
from src.vector_db.local_file_data_loader import LocalFileDataLoader
from src.vector_db.projects_chroma import ProjectsChromaDBHandler

CLIENT_COLLECTION_NAME_VALUE = DatabaseCollectionNames.CLIENT_COLLECTION_NAME.value


class ProjectDBToVectorSync(DataBaseAPI):
    async def delete_nonexistent_records(self, projects, existing_metadata, deleted_records):
        """Delete records from ChromaDB that no longer exist in Firestore"""
        firestore_ids = {project.get("id") for project in projects}
        for project_id in existing_metadata.keys():
            if project_id not in firestore_ids:
                ProjectsChromaDBHandler.shared().delete_project(project_id)
                deleted_records += 1

    @time_it
    async def sync_firestore_to_chroma(self, clientId: str):
        """Main method to sync projects from Firestore to ChromaDB"""
        try:
            if clientId == "":
                # 🔹 Fetch all projects from Firestore
                # Lazy import to avoid circular dependency
                from ..firestore.projects import FirestoreProjectsDB
                standard_response = await FirestoreProjectsDB.shared().getAllProjects()
                if not standard_response or not standard_response.status:
                    logger.error(f"❌ Error from Firestore: {standard_response.error_message if standard_response else 'No response'}")
                    return {
                        "status": "error",
                        "error": (standard_response.error_message if standard_response else "No response from Firestore"),
                    }

                projects = standard_response.data
                if not isinstance(projects, list):
                    logger.error(f"❌ Invalid projects data type: {type(projects)}")
                    return {"status": "error", "error": "Invalid projects data type"}

                logger.info(f"✅ {len(projects)} projects fetched from Firestore")
                ProjectsChromaDBHandler.shared().ensure_collection_exists()

                all_chroma_data = ProjectsChromaDBHandler.shared().vectorstore.get()
                existing_metadata = {meta.get("id"): meta for meta in all_chroma_data.get("metadatas", [])}

                successful_updates = 0
                new_inserts = 0
                deleted_records = 0
                skipped_updates = 0

                await self._add_all_projects_to_chroma(projects, existing_metadata, successful_updates, new_inserts, skipped_updates)

                await self._remove_deleted_projects_from_chroma(projects, "")

                return {
                    "status": "success",
                    "new_inserts": new_inserts,
                    "successful_updates": successful_updates,
                    "deleted_records": deleted_records,
                    "skipped_updates": skipped_updates,
                }

            # 🔹 Sync for a specific clientId
            # Lazy import to avoid circular dependency
            from ..firestore.projects import FirestoreProjectsDB
            standard_response = await FirestoreProjectsDB.shared().getAllProjects(clientId)
            if not standard_response or not standard_response.status:
                logger.error(
                    f"❌ Error from Firestore for client_id={clientId}: {standard_response.error_message if standard_response else 'No response'}"
                )
                return {
                    "status": "error",
                    "error": (standard_response.error_message if standard_response else "No response from Firestore"),
                    "client_id": clientId,
                }

            projects = standard_response.data
            if not isinstance(projects, list):
                logger.error(f"❌ Invalid projects data type for client_id={clientId}: {type(projects)}")
                return {"status": "error", "error": "Invalid projects data type", "client_id": clientId}

            if not projects:
                logger.info(f"ℹ️ No projects found for client_id={clientId}")
                return {"status": "success", "client_id": clientId, "message": "No projects to sync"}
            logger.info(f"✅ {len(projects)} projects fetched from Firestore for client_id={clientId}")
            ProjectsChromaDBHandler.shared().ensure_collection_exists()

            all_chroma_data = ProjectsChromaDBHandler.shared().vectorstore.get()
            existing_metadata = {meta.get("id"): meta for meta in all_chroma_data.get("metadatas", [])}

            successful_updates = 0
            new_inserts = 0
            deleted_records = 0
            skipped_updates = 0

            await self._add_all_projects_to_chroma(projects, existing_metadata, successful_updates, new_inserts, skipped_updates)

            await self._remove_deleted_projects_from_chroma(projects, clientId)

            return {
                "status": "success",
                "client_id": clientId,
                "new_inserts": new_inserts,
                "successful_updates": successful_updates,
                "deleted_records": deleted_records,
                "skipped_updates": skipped_updates,
            }

        except Exception as e:
            error_msg = f"❌ Exception in ProjectDBToVectorSync.sync_firestore_to_chroma: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "error": error_msg, "client_id": clientId}

    async def _add_all_projects_to_chroma(
        self,
        projects: List[Dict[str, Any]],
        existing_metadata: Dict[str, Any],
        successful_updates: int,
        new_inserts: int,
        skipped_updates: int,
    ):
        """Add or update all projects in ChromaDB"""
        for project in projects:
            project_id = project.get("id")
            if not project_id:
                continue
            if project.get("client_id") in test_client_ids:
                continue
            if project_id in existing_metadata:
                await self._update_project_if_changed(project_id, project, existing_metadata, successful_updates, skipped_updates)
            else:
                try:
                    # Add raw project data directly without validation
                    ProjectsChromaDBHandler.shared().add_project_to_db(project)
                    new_inserts += 1
                except Exception as e:
                    logger.error(f"Failed to add project {project_id} to ChromaDB: {str(e)}")
                    continue

        logger.debug(f"✅ Sync completed: {new_inserts} new inserts, " f"{successful_updates} updates, {skipped_updates} skipped updates.")

    async def _update_project_if_changed(
        self, project_id: str, project: Dict[str, Any], existing_metadata: Dict[str, Any], successful_updates: int, skipped_updates: int
    ):
        """Update project in ChromaDB if it has changed"""
        try:
            logger.debug(f"🔄 Checking project {project_id} for updates...")

            # Get timestamps
            firestore_updated_at = project.get("updated_at")
            existing_project = existing_metadata.get(project_id)
            chromadb_updated_at = existing_project.get("updated_at") if existing_project else None

            logger.debug(
                f"📅 Timestamps - Firestore: {firestore_updated_at} (type: {type(firestore_updated_at).__name__}), ChromaDB: {chromadb_updated_at} (type: {type(chromadb_updated_at).__name__})"
            )

            # If both timestamps exist, compare them
            if firestore_updated_at and chromadb_updated_at:
                # Convert both to datetime objects if they're strings
                if isinstance(firestore_updated_at, str):
                    firestore_time = isoparse(firestore_updated_at)
                    logger.debug(
                        f"⏰ Converted Firestore string timestamp to datetime: {firestore_time} (type: {type(firestore_time).__name__})"
                    )
                else:
                    firestore_time = firestore_updated_at
                    logger.debug(f"⏰ Using Firestore datetime object: {firestore_time} (type: {type(firestore_time).__name__})")

                if isinstance(chromadb_updated_at, str):
                    chroma_time = isoparse(chromadb_updated_at)
                    logger.debug(f"⏰ Converted ChromaDB string timestamp to datetime: {chroma_time} (type: {type(chroma_time).__name__})")
                else:
                    chroma_time = chromadb_updated_at
                    logger.debug(f"⏰ Using ChromaDB datetime object: {chroma_time} (type: {type(chroma_time).__name__})")

                # If Firestore timestamp is older or equal, skip update
                if firestore_time <= chroma_time:
                    logger.debug(f"⏭️ Skipping update for project {project_id} - ChromaDB version is newer or same")
                    logger.debug(f"⏰ Timestamp comparison - Firestore: {firestore_time} <= ChromaDB: {chroma_time}")
                    skipped_updates += 1
                    return
                else:
                    logger.debug(f"🔄 Update needed - Firestore timestamp ({firestore_time}) is newer than ChromaDB ({chroma_time})")
            else:
                if not firestore_updated_at:
                    logger.warning(f"⚠️ No Firestore timestamp for project {project_id}")
                if not chromadb_updated_at:
                    logger.warning(f"⚠️ No ChromaDB timestamp for project {project_id}")
                logger.debug(f"🔄 Proceeding with update due to missing timestamp(s)")

            # If we get here, either:
            # 1. One or both timestamps are missing
            # 2. Firestore timestamp is newer
            # In either case, we should update
            logger.debug(f"📝 Updating project {project_id} in ChromaDB...")
            ProjectsChromaDBHandler.shared().update_project(project_id, project)
            successful_updates += 1
            logger.debug(f"✅ Successfully updated project {project_id} in ChromaDB")

        except Exception as e:
            logger.error(f"❌ Failed to update project {project_id} in ChromaDB: {str(e)}")
            logger.error(f"🔍 Project data: {project}")
            logger.error(f"🔍 Existing metadata: {existing_metadata}")
            raise

    async def _remove_deleted_projects_from_chroma(self, projects: List[Dict[str, Any]], client_id: str) -> int:
        """
        Deletes records from ChromaDB whose project IDs no longer exist in Firestore for the given client.

        Returns:
            int: Number of records deleted from ChromaDB.
        """
        try:
            firestore_project_ids = {project["id"] for project in projects}
            logger.info(f"📥 Firestore project count: {len(firestore_project_ids)} for client_id={client_id}")

            chroma_data = ProjectsChromaDBHandler.shared().vectorstore.get()
            metadatas = chroma_data.get("metadatas", [])
            vector_ids = chroma_data.get("ids", [])

            projectid_to_vectorid = {}
            for meta, vid in zip(metadatas, vector_ids):
                pid = meta.get("id")
                cid = meta.get("client_id")
                if pid and cid == client_id:
                    projectid_to_vectorid[pid] = vid
                elif pid and cid != client_id:
                    logger.debug(f"⛔ Skipping vector {vid}: client_id mismatch ({cid} != {client_id})")
                elif not pid:
                    logger.warning(f"⚠️ Missing project ID in metadata for vector {vid}")

            chroma_project_ids = set(projectid_to_vectorid.keys())
            to_delete_project_ids = chroma_project_ids - firestore_project_ids
            to_delete_vector_ids = [projectid_to_vectorid[pid] for pid in to_delete_project_ids]

            # Enhanced log: Total Chroma vectors *for this client*
            logger.info(f"🧠 Chroma vector count: {len(chroma_project_ids)} for client_id={client_id}")

            if to_delete_vector_ids:
                logger.info(
                    f"🧹 {len(to_delete_vector_ids)} vectors to delete for client_id={client_id} "
                    f"with ID(s) = {list(to_delete_project_ids)}"
                )
                logger.debug(f"🗑️ Deleting vector IDs from ChromaDB: {to_delete_vector_ids}")
                ProjectsChromaDBHandler.shared().vectorstore.delete(ids=to_delete_vector_ids)
            else:
                logger.info(f"✅ No obsolete vectors to delete for client_id={client_id}")

            return len(to_delete_vector_ids)

        except Exception as e:
            logger.error(f"❌ Error in _remove_deleted_projects_from_chroma for client_id={client_id}: {str(e)}")
            return 0


projectDBToVectorSync = ProjectDBToVectorSync()
