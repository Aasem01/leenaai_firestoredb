from google.cloud.firestore_v1.base_query import FieldFilter

from .client import FirestoreClient
from src.utils.logger import logger

REQUESTING_DEMO_COLLECTION = "REQUESTING_DEMO"


class FirestoreRequestingDemo:
    def __init__(self):
        self.collection = FirestoreClient.shared().collection(REQUESTING_DEMO_COLLECTION)

    async def get_all(self):
        """Retrieves all records from the collection."""
        try:
            async_docs = self.collection.stream()
            records = [doc.to_dict() async for doc in async_docs]  # Async iteration
            return records if records else {"message": "No records found"}
        except Exception as e:
            logger.error(f"Error fetching all records: {str(e)}")
            return {"error": str(e)}

    async def get_by_phone(self, phone: str):
        """Retrieves a record by phone number."""
        try:
            async_docs = self.collection.where(filter=FieldFilter("phone", "==", phone)).stream()
            records = [doc.to_dict() async for doc in async_docs]  # Async iteration
            return records[0] if records else {"error": "Record not found"}
        except Exception as e:
            logger.error(f"Error fetching record by phone {phone}: {str(e)}")
            return {"error": str(e)}

    async def delete_by_phone(self, phone: str):
        """Deletes a record by phone number."""
        try:
            async_docs = self.collection.where(filter=FieldFilter("phone", "==", phone)).stream()
            docs = [doc async for doc in async_docs]  # Async iteration to collect docs

            if not docs:
                return {"error": "Record not found"}

            for doc in docs:
                await doc.reference.delete()  # Correctly delete each document

            logger.info(f"Record with phone {phone} deleted successfully.")
            return {"status": "Record deleted successfully"}
        except Exception as e:
            logger.error(f"Error deleting record by phone {phone}: {str(e)}")
            return {"error": str(e)}

    async def create_by_phone(self, email: str, phone: str, name: str):
        """Creates a record by phone number."""
        try:
            # Check if record already exists
            existing_record = await self.get_by_phone(phone)
            if "error" not in existing_record:
                return {"status": "request already exists"}

            record_data = {"email": email, "phone": phone, "name": name}
            await self.collection.document(phone).set(record_data)
            logger.info(f"Record with phone {phone} created successfully.")
            return {"status": "Record created successfully", "phone": phone}
        except Exception as e:
            logger.error(f"Error creating record by phone {phone}: {str(e)}")
            return {"error": str(e)}


# Initialize the FirestoreRequestingDemo class
demoDB = FirestoreRequestingDemo()
