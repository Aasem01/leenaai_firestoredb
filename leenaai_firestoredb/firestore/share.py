import logging
from datetime import datetime
from typing import Optional

from dateutil.relativedelta import relativedelta

from ..schemas.collection_names import DatabaseCollectionNames
from .clients import clientsDB
from .client import FirestoreClient
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger
from src.utils.schema_classes import SharedLink
from src.utils.standard_response import StandardResponse
from src.utils.time_now import TimeManager


class FirestoreSharedLinksDB:

    def __init__(self):
        self.shared_collection = FirestoreClient.shared().collection(DatabaseCollectionNames.SHARE_COLLECTION_NAME.value)

    async def create_unique_shared_link(
        self,
        client_id: str,
        document_id: Optional[str] = None,
        phone_number: Optional[str] = None,
        client_name: Optional[str] = None,
        unit_id: Optional[str] = None,
        unit: Optional[dict] = None,
        post: Optional[dict] = None,
        expire_date: Optional[datetime] = None,
    ):
        try:
            if phone_number is None:
                client = await clientsDB.get_client_by_id(client_id)
                phone_number = client.get("data", {}).get("phone_number", "01002891933")
                client_name = client.get("data", {}).get("client_name", client_id)
            link_id = document_id
            if link_id is None:
                link_id = SharedLink.generate_unique_id()

            if expire_date is None:
                months = 60
                if document_id is not None:
                    months = 1
                expire_date = TimeManager.get_time_now_isoformat() + relativedelta(months=months)

            sharableObject = SharedLink(
                link=f"https://chat.lenaai.net/{link_id}",
                client_id=client_id,
                client_name=client_name,
                unit_id=unit_id,
                phone_number=phone_number,
                created_at=TimeManager.get_time_now_isoformat(),
                updated_at=TimeManager.get_time_now_isoformat(),
                unit=unit,
                expire_date=expire_date,
                post=post,
            )
            linkDocument = self.shared_collection.document(link_id)
            logger.info(f"link id : {link_id}")
            existing_doc = await linkDocument.get()
            if existing_doc.exists:
                return StandardResponse.success(data=existing_doc.to_dict(), message="Share link created successfully")

            # Convert unit to dictionary and ensure updatedAt is set
            link_data = sharableObject.model_dump()
            logger.info(f"link_data: {link_data}")
            # Create the unit
            await linkDocument.set(link_data, merge=True)
            return StandardResponse.success(data=link_data, message="Share link created successfully")

        except Exception as e:
            logging.error(f"❌Error creating share link with ID {link_id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def get_share_link(self, id: str):
        """
        Retrieves a share link's details by share link ID.
        """
        try:
            share_link_ref = await self.shared_collection.document(id).get()
            if share_link_ref.exists:
                return StandardResponse.success(data=share_link_ref.to_dict())
            else:
                return StandardResponse.failure(ErrorCodes.NOT_FOUND, "Share link not found")
        except Exception as e:
            logging.error(f"❌Error fetching share link {id}: {str(e)}")
            return StandardResponse.failure(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))


sharedLinksDB = FirestoreSharedLinksDB()
