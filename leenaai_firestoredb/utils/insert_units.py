import json
import logging
import uuid
from typing import Any, Dict, List, Optional

from google.cloud import firestore
from pydantic import BaseModel, Field

from src.data_extractors.image_data import ImageData
# Lazy import to avoid circular dependency
# from ..firestore.units import propertiesDB
from src.utils.sale_property_schema import SalePropertyDetails
from src.utils.time_now import TimeManager


class InsertUnitsIntoFirestore:

    def safe_str(self, value: Any) -> Optional[str]:
        """Safely convert value to string, returning None for null values."""
        return str(value) if value is not None else ""

    def safe_float(self, value: Any) -> float:
        """Safely convert value to float, returning -1 for invalid values."""
        if value is None:
            return 0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0

    def safe_int(self, value: Any) -> int:
        """Safely convert value to int, returning -1 for invalid values."""
        if value is None:
            return 0
        try:
            # Handle Arabic numerals or text
            if isinstance(value, str) and not value.isdigit():
                return 0
            return int(float(value))
        except (ValueError, TypeError):
            return 0

    def createSalePropertyDetailsFromDic(self, item: Dict, images: List[Dict] = []) -> SalePropertyDetails:
        # Handle property price
        total_price = self.safe_float(item.get("total_price"))
        price_data = {"years": 0, "price": total_price, "maintanance": total_price * 0.1 if total_price > 0 else 0}
        price_data_str = json.dumps(price_data)
        # Handle unit ID - use file_name if unitId is not available
        unit_id = uuid.uuid4()
        print(unit_id)
        if not unit_id:
            file_name = item.get("file_name", "")
            unit_id = file_name.replace(".pdf", "").strip()
            if not unit_id:
                unit_id = str(uuid.uuid4())

        # Create property details with safe conversions
        return SalePropertyDetails(
            clientName=self.safe_str(item.get("clientName")),
            clientId=self.safe_str(item.get("clientId")),
            country="egypt",
            city=self.safe_str(item.get("city")),
            project=self.safe_str(item.get("project")),
            developer=self.safe_str(item.get("developer")),
            unitId=self.safe_str(unit_id),  # Ensure unitId is always provided
            deliveryDate=self.safe_str(item.get("deliveryDate")),
            bathroomCount=self.safe_int(item.get("bathroomCount")),
            buildingType=self.safe_str(item.get("buildingType")),
            floor=self.safe_int(item.get("floor")),
            roomsCount=self.safe_int(item.get("roomsCount")),
            landArea=self.safe_float(item.get("landArea")),
            gardenSize=self.safe_float(item.get("gardenSize")),
            finishing=self.safe_str(item.get("finishing")),
            dataSource=self.safe_str(item.get("real_path")),
            view=self.safe_str(item.get("view")),
            purpose=self.safe_str(item.get("purpose")),
            garageArea=self.safe_float(item.get("garageArea")),
            paymentPlans=price_data_str,
            images=images,
            updatedAt=TimeManager.get_time_now_isoformat(),
        )

    def parse_and_store_properties(self, json_data: List[Dict[str, Any]]):
        results = []

        for item in json_data:
            try:
                property_details = self.createSalePropertyDetailsFromDic(item)
                # Store in Firestore
                # Lazy import to avoid circular dependency
                from ..firestore.units import propertiesDB
                propertiesDB.create_or_update_unit(property_details)

            except Exception as e:
                error_msg = str(e)
                logging.error(f"Error processing property: {item.get('file_name', 'Unknown')}")
                logging.error(f"Error details: {error_msg}")
                continue

    def store_json_data_into_firestore(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Load JSON file and process properties with error handling.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                json_data = json.load(file)
            return self.parse_and_store_properties(json_data)
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Error loading file: {error_msg}")


firestoreWriter = InsertUnitsIntoFirestore()
# Example usage
if __name__ == "__main__":
    firestoreWriter.store_json_data_into_firestore("property_data.json")
