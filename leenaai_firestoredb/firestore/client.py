import asyncio
import logging
import os
import shutil
import subprocess
from typing import Optional
from unittest.mock import AsyncMock, Mock

from google.auth import default
from google.cloud import firestore
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials

from src.utils.config import GOOGLE_CLOUD_PROJECT_ID, LOCAL_ENV


class FirestoreClient:
    is_initialized = None
    _client_pool = []
    _max_pool_size = 5
    _pool_lock = asyncio.Lock()

    def __init__(self):
        # Check if we're in a testing environment
        if os.getenv("TESTING", "false").lower() == "true":
            logging.info("🧪 Test environment detected - using mock Firestore client")
            self.client = self._create_mock_client()
            return

        try:
            if LOCAL_ENV:
                # Find gcloud dynamically, fallback to "gcloud" if not found
                gcloud_cmd = shutil.which("gcloud") or "gcloud"

                if not gcloud_cmd:
                    raise FileNotFoundError("❌ gcloud command not found. Ensure Google Cloud SDK is installed and added to PATH.")

                # Use local gcloud CLI token
                access_token = subprocess.check_output([gcloud_cmd, "auth", "print-access-token"]).decode("utf-8").strip()
                credentials = Credentials(access_token)
            else:
                # Try to use Application Default Credentials first
                try:
                    credentials, _ = default()
                    logging.info("✅ Using Application Default Credentials (ADC).")
                except Exception as adc_error:
                    logging.warning(f"⚠️ ADC not available: {adc_error}")

                    # Fallback to service account files
                    service_account_files = []

                    # Check for standard Google credentials environment variable first
                    google_creds_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                    if google_creds_file and os.path.exists(google_creds_file):
                        service_account_files.append(google_creds_file)

                    # Check for FCM service account environment variable
                    fcm_sa_file = os.getenv("FCM_SERVICE_ACCOUNT_JSON")
                    if fcm_sa_file and os.path.exists(fcm_sa_file):
                        service_account_files.append(fcm_sa_file)

                    # Add default service account files
                    service_account_files.extend(
                        [
                            "firebase_cred.json",
                            "chat-history-449709-firebase-adminsdk-fbsvc-f97f621c97.json",
                            "chat-history-449709-8754a08730d2.json",
                            "calendar-sa-key.json",
                        ]
                    )

                    credentials = None
                    for sa_file in service_account_files:
                        if os.path.exists(sa_file):
                            try:
                                credentials = service_account.Credentials.from_service_account_file(
                                    sa_file, scopes=["https://www.googleapis.com/auth/cloud-platform"]
                                )
                                logging.info(f"✅ Using service account file: {sa_file}")
                                break
                            except Exception as sa_error:
                                logging.warning(f"⚠️ Failed to load {sa_file}: {sa_error}")
                                continue

                    if credentials is None:
                        raise Exception(
                            "❌ No valid authentication method found. "
                            "Please ensure either ADC is set up or a valid service account file is available."
                        )

            # Create client with optimized settings
            self.client = firestore.AsyncClient(
                project=GOOGLE_CLOUD_PROJECT_ID,
                credentials=credentials,
                # Add connection pooling settings
                client_options={"api_endpoint": "firestore.googleapis.com", "quota_project_id": GOOGLE_CLOUD_PROJECT_ID},
            )

            # Pre-warm the client connection
            asyncio.create_task(self._prewarm_client())

        except Exception as e:
            logging.error(f"❌ FIRESTORE CLIENT Failed to authenticate: {e}")
            # In test environments, don't exit, just create a mock client
            if os.getenv("TESTING", "false").lower() == "true":
                logging.warning("🔄 Falling back to mock client for testing")
                self.client = self._create_mock_client()
            else:
                exit(1)

    async def _prewarm_client(self):
        """Pre-warm the client connection to avoid cold start delays."""
        try:
            # Make a simple query to warm up the connection
            collection = self.client.collection("_warmup")
            await collection.limit(1).get()
            logging.debug("✅ Firestore client pre-warmed successfully")
        except Exception as e:
            logging.debug(f"⚠️ Firestore client pre-warm failed (non-critical): {e}")

    async def get_client_from_pool(self) -> firestore.AsyncClient:
        """Get a client from the connection pool."""
        async with self._pool_lock:
            if self._client_pool:
                return self._client_pool.pop()
            else:
                # Create new client if pool is empty
                return self.client

    async def return_client_to_pool(self, client: firestore.AsyncClient):
        """Return a client to the connection pool."""
        async with self._pool_lock:
            if len(self._client_pool) < self._max_pool_size:
                self._client_pool.append(client)

    def _create_mock_client(self):
        """Create a mock Firestore client for testing."""
        # Create a more comprehensive mock that handles async operations
        def mock_collection(collection_name):
            mock_collection_instance = Mock()

            # Mock document method
            def mock_document(doc_id):
                mock_doc = Mock()
                mock_doc.get = AsyncMock(return_value=Mock())
                mock_doc.set = AsyncMock()
                mock_doc.update = AsyncMock()
                mock_doc.delete = AsyncMock()
                mock_doc.reference = mock_doc
                return mock_doc

            mock_collection_instance.document = Mock(side_effect=mock_document)

            # Mock where method for queries
            def mock_where(field, op, value):
                mock_query = Mock()
                mock_query.stream = AsyncMock(return_value=[])
                mock_query.get = AsyncMock(return_value=[])
                mock_query.order_by = Mock(return_value=mock_query)
                mock_query.limit = Mock(return_value=mock_query)
                mock_query.start_after = Mock(return_value=mock_query)
                return mock_query

            mock_collection_instance.where = Mock(side_effect=mock_where)

            # Mock async methods
            mock_collection_instance.stream = AsyncMock(return_value=[])
            mock_collection_instance.get = AsyncMock(return_value=[])

            return mock_collection_instance

        mock_client = Mock()
        mock_client.collection = Mock(side_effect=mock_collection)
        return mock_client

    @classmethod
    def shared(cls):
        if cls.is_initialized is None:
            cls.is_initialized = FirestoreClient()
        return cls.is_initialized.client
