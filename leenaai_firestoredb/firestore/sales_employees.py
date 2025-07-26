import uuid
from datetime import datetime
from typing import List, Optional

from ..schemas.collection_names import DatabaseCollectionNames
from .client import FirestoreClient
from src.schemas.sales_employee_schema import SalesEmployee
from src.utils.error_codes import ErrorCodes
from src.utils.logger import logger
from src.utils.standard_response import StandardResponse


class SalesEmployeesDB:
    _instance = None

    def __init__(self):
        self.collection = FirestoreClient.shared().collection(DatabaseCollectionNames.SALES_EMPLOYEES_COLLECTION_NAME.value)

    @classmethod
    def shared(cls) -> "SalesEmployeesDB":
        """Get the shared instance of SalesEmployeesDB"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _generate_employee_id(self, client_id: str) -> str:
        """Generate a composite ID in format {client_id}_{uuid4}"""
        # Generate a full UUID4
        uuid4 = str(uuid.uuid4())
        return f"{client_id}_{uuid4}"

    async def create_sales_employee(self, employee: SalesEmployee) -> StandardResponse:
        """Create a new sales employee with composite ID"""
        try:
            # Generate the composite ID and create document
            employee_id = self._generate_employee_id(employee.client_id)
            employee_dict = employee.model_dump()
            employee_dict["id"] = employee_id  # Add ID to dict directly

            doc_ref = self.collection.document(employee_id)
            await doc_ref.set(employee_dict)
            logger.info(f"✅ Sales employee created successfully with ID: {employee_id}")

            return StandardResponse.success(data={"id": employee_id}, message="Sales employee created successfully")
        except Exception as e:
            logger.error(f"❌ Error creating sales employee: {str(e)}")
            return StandardResponse.failure(code=ErrorCodes.get_http_status_code(e), error_message=str(e))

    async def get_sales_employee(self, employee_id: str) -> StandardResponse:
        """Get a sales employee by ID"""
        try:
            doc = await self.collection.document(employee_id).get()
            if not doc.exists:
                return StandardResponse.not_found("Sales employee not found")

            employee = SalesEmployee(**doc.to_dict())
            return StandardResponse.success(data=employee, message="Sales employee retrieved successfully")
        except Exception as e:
            logger.error(f"❌ Error getting sales employee: {str(e)}")
            return StandardResponse.failure(code=ErrorCodes.get_http_status_code(e), error_message=str(e))

    async def get_all_sales_employees(self) -> StandardResponse:
        """Get all sales employees"""
        try:
            docs = await self.collection.get()
            employees = [SalesEmployee(**doc.to_dict()) for doc in docs]
            return StandardResponse.success(data=employees, message="Sales employees retrieved successfully")
        except Exception as e:
            logger.error(f"❌ Error getting all sales employees: {str(e)}")
            return StandardResponse.failure(code=ErrorCodes.get_http_status_code(e), error_message=str(e))

    async def update_sales_employee(self, employee_id: str, employee: SalesEmployee) -> StandardResponse:
        """Update a sales employee"""
        try:
            # Check if employee exists
            doc_ref = self.collection.document(employee_id)
            doc = await doc_ref.get()
            if not doc.exists:
                return StandardResponse.not_found("Sales employee not found")

            # Update employee data
            update_data = employee.model_dump(exclude={"id"})
            await doc_ref.update(update_data)

            return StandardResponse.success(message="Sales employee updated successfully")
        except Exception as e:
            logger.error(f"❌ Error updating sales employee: {str(e)}")
            return StandardResponse.failure(code=ErrorCodes.get_http_status_code(e), error_message=str(e))

    async def delete_sales_employee(self, employee_id: str) -> StandardResponse:
        """Delete a sales employee"""
        try:
            doc_ref = self.collection.document(employee_id)
            doc = await doc_ref.get()
            if not doc.exists:
                return StandardResponse.not_found("Sales employee not found")

            await doc_ref.delete()
            return StandardResponse.success(message="Sales employee deleted successfully")
        except Exception as e:
            logger.error(f"❌ Error deleting sales employee: {str(e)}")
            return StandardResponse.failure(code=ErrorCodes.get_http_status_code(e), error_message=str(e))
