import calendar
from datetime import datetime, time
from uuid import uuid4

import pytz
from google.cloud.firestore_v1.base_query import FieldFilter

from ..schemas.collection_names import DatabaseCollectionNames
from .client import FirestoreClient
from src.schemas.booking_schemas import BookingsRequest, CancelBookingRequest
from src.utils.calendar_util import calendarUtil
from src.utils.error_codes import ErrorCodes
from src.utils.google_calendar_util import google_calendar
from src.utils.logger import logger
from src.utils.standard_response import StandardResponse

BOOKED_MEETINGS_COLLECTION = DatabaseCollectionNames.BOOKED_MEETINGS_COLLECTION.value
AVAILABLE_MEETINGS_COLLECTION = DatabaseCollectionNames.AVAILABLE_MEETINGS_COLLECTION.value
EGYPT_TIMEZONE = pytz.timezone("Africa/Cairo")


class BookingSystem:

    def __init__(self):
        self.bookings_collection = FirestoreClient.shared().collection(BOOKED_MEETINGS_COLLECTION)
        self.availability_collection = FirestoreClient.shared().collection(AVAILABLE_MEETINGS_COLLECTION)

    def _get_current_egypt_time(self) -> datetime:
        """Get current time in Egypt timezone."""
        return datetime.now(EGYPT_TIMEZONE)

    def _filter_future_time_slots(self, time_slots: list, current_time: time) -> list:
        """Filter out past time slots for the current day."""
        return [slot for slot in time_slots if datetime.strptime(slot, "%H:%M").time() > current_time]

    def _get_month_days_count(self, year: int, month: int) -> int:
        """Get the number of days in the specified month."""
        return calendar.monthrange(year, month)[1]

    def _extract_date_components(self, date_str: str) -> tuple[str, str]:
        """Extract month and day from date string (YYYY-MM-DD)."""
        return date_str[:7], date_str.split("-")[2]  # Returns (month_str, day_str)

    def _is_month_in_past(self, year: int, month: int) -> bool:
        """Check if the given month is completely in the past."""
        current_date = self._get_current_egypt_time()
        logger.debug(f"🔍 Month comparison - Input: year={year}, month={month}")
        logger.debug(f"🔍 Month comparison - Current: year={current_date.year}, month={current_date.month}")

        is_past = year < current_date.year or (year == current_date.year and month < current_date.month)

        logger.debug(f"🔍 Month comparison result: {is_past}")
        return is_past

    def _is_date_in_past(self, year: int, month: int, day: int) -> bool:
        """Check if the given date is in the past."""
        current_date = self._get_current_egypt_time()
        logger.debug(f"🔍 Date comparison - Input: year={year}, month={month}, day={day}")
        logger.debug(f"🔍 Date comparison - Current: year={current_date.year}, month={current_date.month}, day={current_date.day}")

        is_past = (
            year < current_date.year
            or (year == current_date.year and month < current_date.month)
            or (year == current_date.year and month == current_date.month and day < current_date.day)
        )

        logger.debug(f"🔍 Date comparison result: {is_past}")
        return is_past

    def _is_time_in_past(self, time_str: str) -> bool:
        """Check if the given time is in the past for current day."""
        current_time = self._get_current_egypt_time()
        return datetime.strptime(time_str, "%H:%M").time() <= current_time.time()

    async def _get_availability_document(self, month_str: str):
        """Get availability document for the specified month."""
        doc_ref = self.availability_collection.document(month_str)
        return await doc_ref.get()

    async def _update_availability_document(self, month_str: str, availability_data: dict):
        """Update availability document in Firestore."""
        doc_ref = self.availability_collection.document(month_str)
        await doc_ref.set(availability_data)

    async def _create_booking_document(self, booking_id: str, booking_data: dict):
        """Create a new booking document in Firestore."""
        doc_ref = self.bookings_collection.document(booking_id)
        await doc_ref.set(booking_data)

    async def create_booking(self, booking_data: BookingsRequest):
        """
        Create a new booking after validating availability.

        Args:
            booking_data: Booking request containing client and time slot information

        Returns:
            StandardResponse with booking details or error message
        """
        try:
            # Extract date components
            month_str, selected_day = self._extract_date_components(booking_data.selected_date)

            # Get availability document
            availability_doc = await self._get_availability_document(month_str)
            if not availability_doc.exists:
                StandardResponse.raise_http_exception(ErrorCodes.NOT_FOUND, "Availability data for the selected month not found")

            # Get available slots for the selected day
            available_slots = availability_doc.to_dict().get("days", {}).get(selected_day, [])
            if not available_slots:
                StandardResponse.raise_http_exception(ErrorCodes.NOT_FOUND, f"No availability data for the selected day ({selected_day})")

            # Validate time slot availability
            if booking_data.selected_time not in available_slots:
                StandardResponse.raise_http_exception(ErrorCodes.CONFLICT, "The selected time slot is already booked")

            # Create Google Calendar event
            try:
                event_id = google_calendar.create_meeting_event(booking_data.model_dump())
                booking_data.google_calendar_event_id = event_id
            except Exception as e:
                logger.error(f"❌ Error creating Google Calendar event: {str(e)}")
                # Continue with booking creation even if calendar event fails
                pass

            # Create booking
            booking_id = str(uuid4())
            availability_data = availability_doc.to_dict()
            availability_data["days"][selected_day].remove(booking_data.selected_time)

            # Update both documents
            await self._create_booking_document(booking_id, booking_data.model_dump())
            await self._update_availability_document(month_str, availability_data)

            logger.debug(f"✅ Booking created successfully with ID: {booking_id}")
            return StandardResponse.success(
                data={"booking_id": booking_id, **booking_data.model_dump()}, message="Booking created successfully"
            )

        except Exception as e:
            logger.error(f"❌ Error creating booking: {str(e)}")
            StandardResponse.raise_http_exception(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def get_available_slots(self, month_str: str) -> StandardResponse:
        """
        Get available time slots for a specific month.

        Args:
            month_str: Month in YYYY-MM format

        Returns:
            StandardResponse with available slots or error message
        """
        try:
            # Parse and validate month
            logger.debug(f"📅 Getting available slots for month: {month_str}")
            parsed_date = datetime.strptime(month_str, "%Y-%m")
            parsed_date = EGYPT_TIMEZONE.localize(parsed_date)
            logger.debug(f"📅 Parsed date: {parsed_date}")

            current_date = self._get_current_egypt_time()
            logger.debug(f"📅 Current date: {current_date}")

            # Use month comparison instead of date comparison
            is_past = self._is_month_in_past(parsed_date.year, parsed_date.month)
            logger.debug(f"📅 Is month in past: {is_past}")

            if is_past:
                StandardResponse.raise_http_exception(ErrorCodes.NOT_FOUND, "Cannot get availability for past months")

            # Get or create availability document
            availability_doc = await self._get_availability_document(month_str)
            if not availability_doc.exists:
                logger.debug(f"📅 Creating new availability document for {month_str}")
                creation_response = await self.create_availability_document(month_str)
                if not creation_response.status:
                    StandardResponse.raise_http_exception(
                        ErrorCodes.INTERNAL_SERVER_ERROR, creation_response.error_message or creation_response.message
                    )
                availability_doc = await self._get_availability_document(month_str)

            # Process available slots
            availability_data = availability_doc.to_dict()
            filtered_days = self._filter_available_days(availability_data.get("days", {}), parsed_date, current_date)

            return StandardResponse.success(
                data={"month": month_str, "days": filtered_days, "timezone": "Africa/Cairo"}, message="Available slots fetched successfully"
            )

        except Exception as e:
            logger.error(f"❌ Error fetching available slots: {str(e)}")
            StandardResponse.raise_http_exception(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    def _filter_available_days(self, days: dict, target_date: datetime, current_date: datetime) -> dict:
        """Filter available days based on current date and time."""
        filtered_days = {}

        for day, slots in days.items():
            day_int = int(day)

            # For current day, filter out past time slots
            if target_date.year == current_date.year and target_date.month == current_date.month and day_int == current_date.day:
                filtered_slots = self._filter_future_time_slots(slots, current_date.time())
                if filtered_slots:
                    filtered_days[day] = filtered_slots
            # For future days, include all slots
            elif not self._is_date_in_past(target_date.year, target_date.month, day_int):
                filtered_days[day] = slots

        return filtered_days

    async def create_availability_document(self, month: str):
        """
        Creates an availability document for the entire month.
        Each day of the month will have an array of available time slots.
        Only includes dates from the current day onwards and future time slots for the current day.
        All times are in Egypt timezone.
        """
        try:
            availability_ref = self.availability_collection.document(month)

            # Get current date and time in Egypt timezone
            current_date = self._get_current_egypt_time()
            current_day = current_date.day
            current_month = current_date.month
            current_year = current_date.year
            current_time = current_date.time()

            # Parse the month string and convert to Egypt timezone
            parsed_month = datetime.strptime(month, "%Y-%m")
            parsed_month = EGYPT_TIMEZONE.localize(parsed_month)

            # Get the number of days in the month
            days_in_month = self._get_month_days_count(parsed_month.year, parsed_month.month)

            month_slots = {}
            for day in range(1, days_in_month + 1):
                day_str = str(day).zfill(2)

                # Skip past dates
                if (
                    parsed_month.year < current_year
                    or (parsed_month.year == current_year and parsed_month.month < current_month)
                    or (parsed_month.year == current_year and parsed_month.month == current_month and day < current_day)
                ):
                    continue

                # Get all possible slots for the day
                available_slots = calendarUtil.get_possible_slots(f"{month}-{day_str}")

                # Only filter time slots for current day
                if parsed_month.year == current_year and parsed_month.month == current_month and day == current_day:
                    available_slots = self._filter_future_time_slots(available_slots, current_time)

                if available_slots:  # Only add the day if there are available slots
                    month_slots[day_str] = available_slots

            await availability_ref.set({"month": month, "days": month_slots, "timezone": "Africa/Cairo"})

            return StandardResponse.success(
                data={"message": "Availability document for the month created successfully"}, message="Availability document created"
            )

        except Exception as e:
            logger.error(f"❌ Error creating availability document: {str(e)}")
            StandardResponse.raise_http_exception(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))

    async def cancel_booking(self, cancel_data: CancelBookingRequest):
        """
        Cancel an existing booking and return the time slot to availability.

        Args:
            cancel_data: Cancellation request containing client email and meeting details

        Returns:
            StandardResponse with cancellation details or error message
        """
        try:
            # Find the booking
            bookings_query = (
                self.bookings_collection.where(filter=FieldFilter("client_email", "==", cancel_data.client_email))
                .where(filter=FieldFilter("selected_date", "==", cancel_data.selected_date))
                .where(filter=FieldFilter("selected_time", "==", cancel_data.selected_time))
                .where(filter=FieldFilter("status", "==", "booked"))
            )

            bookings = await bookings_query.get()
            bookings_list = [doc for doc in bookings]

            if not bookings_list:
                StandardResponse.raise_http_exception(ErrorCodes.NOT_FOUND, "No active booking found for the specified details")

            if len(bookings_list) > 1:
                StandardResponse.raise_http_exception(
                    ErrorCodes.CONFLICT, "Multiple bookings found for the specified details. Please contact support."
                )

            booking_doc = bookings_list[0]
            booking_data = booking_doc.to_dict()

            # Delete Google Calendar event if it exists
            if booking_data.get("google_calendar_event_id"):
                try:
                    google_calendar.delete_meeting_event(booking_data["google_calendar_event_id"])
                except Exception as e:
                    logger.error(f"❌ Error deleting Google Calendar event: {str(e)}")
                    # Continue with cancellation even if calendar event deletion fails
                    pass

            # Extract date components for availability update
            month_str, selected_day = self._extract_date_components(cancel_data.selected_date)

            # Get availability document
            availability_doc = await self._get_availability_document(month_str)
            if not availability_doc.exists:
                StandardResponse.raise_http_exception(ErrorCodes.NOT_FOUND, "Availability data for the selected month not found")

            # Update availability
            availability_data = availability_doc.to_dict()
            if selected_day not in availability_data["days"]:
                availability_data["days"][selected_day] = []

            # Add the time slot back to available slots
            if cancel_data.selected_time not in availability_data["days"][selected_day]:
                availability_data["days"][selected_day].append(cancel_data.selected_time)
                availability_data["days"][selected_day].sort()  # Keep slots sorted

            # Update booking status
            booking_data["status"] = "canceled"
            booking_data["updated_at"] = self._get_current_egypt_time().isoformat()
            if cancel_data.cancellation_note:
                booking_data["cancellation_note"] = cancel_data.cancellation_note

            # Update both documents
            await booking_doc.reference.set(booking_data)
            await self._update_availability_document(month_str, availability_data)

            logger.debug(f"✅ Booking canceled successfully: {booking_doc.id}")
            return StandardResponse.success(data={"booking_id": booking_doc.id, **booking_data}, message="Booking canceled successfully")

        except Exception as e:
            logger.error(f"❌ Error canceling booking: {str(e)}")
            StandardResponse.raise_http_exception(ErrorCodes.INTERNAL_SERVER_ERROR, str(e))


# Singleton export
bookingsDB = BookingSystem()
