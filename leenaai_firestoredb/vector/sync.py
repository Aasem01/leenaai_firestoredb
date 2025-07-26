import asyncio
import datetime  # Ensure datetime is imported
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import chromadb
import pandas as pd  # Required for handling timestamps
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings

from src.data_extractors.google_sheets_data_loader import GoogleSheetsDataLoader
from ..schemas.collection_names import DatabaseCollectionNames
# Lazy import to avoid circular dependency
# from ..firestore.units import propertiesDB
from ..firestore.client import FirestoreClient
from src.utils.config import LOCAL_ENV, OPENAI_API_KEY  # Ensure API key is loaded
from src.utils.logger import logger
from src.utils.sale_property_schema import UnitKeys
from src.utils.test_accounts import test_client_ids
from src.utils.time_it import time_it
from src.utils.time_now import TimeManager
from src.vector_db.base_data_loader import BaseDataLoader
from src.vector_db.db_api_helper import DataBaseAPI
from src.vector_db.db_chroma_helper import ChromaDBHandler
from src.vector_db.local_file_data_loader import LocalFileDataLoader

CLIENT_COLLECTION_NAME_VALUE = DatabaseCollectionNames.CLIENT_COLLECTION_NAME.value


class DBToVectorSync(DataBaseAPI):

    async def delete_nonexistent_records(self, units, existing_metadata, deleted_records):
        """Delete records from ChromaDB that no longer exist in Firestore"""
        firestore_ids = {unit.get(UnitKeys.unitId) for unit in units}
        for unit_id in existing_metadata.keys():
            if unit_id not in firestore_ids:
                ChromaDBHandler.shared().delete_property(unit_id)
                deleted_records += 1

    @time_it
    async def sync_firestore_to_chroma(self, clientId: str):
        try:
            if clientId == "":
                # 🔹 Fetch all client IDs from Firestore
                self.client_collection = FirestoreClient.shared().collection(CLIENT_COLLECTION_NAME_VALUE)
                logger.debug(f"🚀 Fetching client_id from collection: {self.client_collection.id}")

                docs = await self.client_collection.get()
                total_docs = len(docs)
                logger.debug(f"📄 Total documents found in '{self.client_collection.id}': {total_docs}")

                client_ids = []

                async def process_doc(doc):
                    try:
                        data = doc.to_dict()
                        logger.debug(f"📄 Document {doc.id} data: {data}")
                        client_id = data.get("client_id")
                        if client_id and client_id not in test_client_ids:
                            client_ids.append(client_id)
                    except Exception as doc_error:
                        logger.error(f"❌ Error processing document {doc.id}: {doc_error}")

                await asyncio.gather(*(process_doc(doc) for doc in docs))
                logger.info("✅ Finished fetching all client IDs from Firestore")

                # 🔄 Recursively call self for each client_id
                for id in client_ids:
                    await self.sync_firestore_to_chroma(clientId=id)
                return {"status": "success", "synced_client_ids": len(client_ids)}

            # 🔹 Sync for a specific clientId
            # Lazy import to avoid circular dependency
            from ..firestore.units import propertiesDB
            standard_response = await propertiesDB.get_units_by_client(clientId)
            units = standard_response.data.get("units", [])
            logger.info(f"✅ {len(units)} units fetched from Firestore for client_id={clientId}")
            ChromaDBHandler.shared().ensure_collection_exists()

            all_chroma_data = ChromaDBHandler.shared().vectorstore.get()
            existing_metadata = {meta.get(UnitKeys.unitId): meta for meta in all_chroma_data.get("metadatas", [])}

            successful_updates = 0
            new_inserts = 0
            deleted_records = 0
            skipped_updates = 0

            ChromaDBHandler.shared().add_all_units_to_chroma(units, existing_metadata, successful_updates, new_inserts, skipped_updates)

            await self.remove_deleted_units_from_chroma(units, clientId)

            return {
                "status": "success",
                "client_id": clientId,
                "new_inserts": new_inserts,
                "successful_updates": successful_updates,
                "deleted_records": deleted_records,
                "skipped_updates": skipped_updates,
            }

        except Exception as e:
            error_msg = f"❌ Exception in DBToVectorSync sync_firestore_to_chroma: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "error": error_msg, "client_id": clientId}

    async def remove_deleted_units_from_chroma(self, units: List[Dict[str, Any]], client_id: str) -> int:
        """
        Deletes records from ChromaDB whose unitIds no longer exist in Firestore for the given client.

        Returns:
            int: Number of records deleted from ChromaDB.
        """
        try:
            firestore_unit_ids = {unit[UnitKeys.unitId] for unit in units}
            logger.info(f"📥 Firestore unit count: {len(firestore_unit_ids)} for client_id={client_id}")

            chroma_data = ChromaDBHandler.shared().vectorstore.get()
            metadatas = chroma_data.get("metadatas", [])
            vector_ids = chroma_data.get("ids", [])

            unitid_to_vectorid = {}
            for meta, vid in zip(metadatas, vector_ids):
                uid = meta.get(UnitKeys.unitId)
                cid = meta.get("clientId")
                if uid and cid == client_id:
                    unitid_to_vectorid[uid] = vid
                elif uid and cid != client_id:
                    logger.debug(f"⛔ Skipping vector {vid}: client_id mismatch ({cid} != {client_id})")
                elif not uid:
                    logger.warning(f"⚠️ Missing unitId in metadata for vector {vid}")

            chroma_unit_ids = set(unitid_to_vectorid.keys())
            to_delete_unit_ids = chroma_unit_ids - firestore_unit_ids
            to_delete_vector_ids = [unitid_to_vectorid[uid] for uid in to_delete_unit_ids]

            # Enhanced log: Total Chroma vectors *for this client*
            logger.info(f"🧠 Chroma vector count: {len(chroma_unit_ids)} for client_id={client_id}")

            if to_delete_vector_ids:
                logger.info(
                    f"🧹 {len(to_delete_vector_ids)} vectors to delete for client_id={client_id} "
                    f"with ID(s) = {list(to_delete_unit_ids)}"
                )
                logger.debug(f"🗑️ Deleting vector IDs from ChromaDB: {to_delete_vector_ids}")
                ChromaDBHandler.shared().vectorstore.delete(ids=to_delete_vector_ids)
            else:
                logger.info(f"✅ No obsolete vectors to delete for client_id={client_id}")

            return len(to_delete_vector_ids)

        except Exception as e:
            logger.error(f"❌ Error in remove_deleted_units_from_chroma for client_id={client_id}: {str(e)}")
            return 0


dbToVectorSync = DBToVectorSync()
