from typing import List

from src.utils.logger import logger
from src.utils.sale_property_schema import UnitKeys
from src.utils.time_it import time_it
from src.vector_db.db_api_helper import DataBaseAPI
from src.vector_db.db_chroma_helper import ChromaDBHandler


class ChromaSyncronizer(DataBaseAPI):
    @time_it
    async def sync_firestore_to_chroma(self, units: List, clientId):
        try:
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
            error_msg = f"❌ Exception in ChromaSyncronizer.sync_firestore_to_chroma: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "error": error_msg, "client_id": clientId}

    async def remove_deleted_units_from_chroma(self, units: List, client_id: str) -> int:
        """
        Deletes records from ChromaDB whose unitIds no longer exist in Firestore for the given client.

        Returns:
            int: Number of records deleted from ChromaDB.
        """
        try:
            firestore_unit_ids = {unit[UnitKeys.unitId] for unit in units}

            chroma_data = ChromaDBHandler.shared().vectorstore.get()
            metadatas = chroma_data.get("metadatas", [])
            vector_ids = chroma_data.get("ids", [])

            unitid_to_vectorid = {}
            for meta, vid in zip(metadatas, vector_ids):
                uid = meta.get(UnitKeys.unitId)
                cid = meta.get(UnitKeys.clientId)
                if uid and cid == client_id:
                    unitid_to_vectorid[uid] = vid
                elif uid and cid != client_id:
                    logger.debug(f"⛔ Skipping vector {vid}: client_id mismatch ({cid} != {client_id})")
                elif not uid:
                    logger.warning(f"⚠️ Missing unitId in metadata for vector {vid}")

            chroma_unit_ids = set(unitid_to_vectorid.keys())
            to_delete_unit_ids = chroma_unit_ids - firestore_unit_ids
            to_delete_vector_ids = [unitid_to_vectorid[uid] for uid in to_delete_unit_ids]

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


chromaSyncronizer = ChromaSyncronizer()
