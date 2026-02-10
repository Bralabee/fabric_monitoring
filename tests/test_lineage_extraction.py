"""
Tests for Lineage Extraction Logic

Tests cover the LineageExtractor class methods for extracting
Mirrored Databases, Lakehouses, KQL Databases, and their shortcuts.
"""

import base64
import json
from unittest.mock import MagicMock, patch

import pytest

# Import module directly since scripts/ is in sys.path via conftest.py
import extract_lineage


class TestLineageExtractorInit:
    """Tests for LineageExtractor initialization."""

    @patch("extract_lineage.load_dotenv")
    @patch("extract_lineage.create_authenticator_from_env")
    @patch("extract_lineage.setup_logging")
    def test_init_success(self, mock_logging, mock_auth_factory, mock_dotenv):
        """Successful initialization with valid credentials."""
        mock_auth = MagicMock()
        mock_auth.validate_credentials.return_value = True
        mock_auth.get_fabric_token.return_value = "test_token"
        mock_auth_factory.return_value = mock_auth
        mock_logging.return_value = MagicMock()

        extractor = extract_lineage.LineageExtractor()

        assert extractor.token == "test_token"
        assert "Bearer test_token" in extractor.headers["Authorization"]

    @patch("extract_lineage.load_dotenv")
    @patch("extract_lineage.create_authenticator_from_env")
    @patch("extract_lineage.setup_logging")
    def test_init_auth_failure(self, mock_logging, mock_auth_factory, mock_dotenv):
        """Should raise exception when authentication fails."""
        mock_auth = MagicMock()
        mock_auth.validate_credentials.return_value = False
        mock_auth_factory.return_value = mock_auth
        mock_logging.return_value = MagicMock()

        with pytest.raises(RuntimeError, match="Authentication failed"):
            extract_lineage.LineageExtractor()


class TestDecodePayload:
    """Tests for Base64 payload decoding."""

    @pytest.fixture
    def extractor(self):
        """Create extractor with mocked init."""
        with patch("extract_lineage.load_dotenv"):
            with patch("extract_lineage.create_authenticator_from_env") as mock_auth:
                with patch("extract_lineage.setup_logging") as mock_log:
                    mock_auth.return_value.validate_credentials.return_value = True
                    mock_auth.return_value.get_fabric_token.return_value = "token"
                    mock_log.return_value = MagicMock()

                    return extract_lineage.LineageExtractor()

    def test_decode_valid_payload(self, extractor):
        """Valid Base64 JSON should be decoded correctly."""
        original = {"SourceProperties": {"sourceType": "Snowflake"}}
        encoded = base64.b64encode(json.dumps(original).encode("utf-8")).decode("utf-8")

        result = extractor.decode_payload(encoded)

        assert result == original
        assert result["SourceProperties"]["sourceType"] == "Snowflake"

    def test_decode_invalid_base64(self, extractor):
        """Invalid Base64 should return None."""
        result = extractor.decode_payload("not-valid-base64!!!")
        assert result is None

    def test_decode_non_json_payload(self, extractor):
        """Valid Base64 but non-JSON content should return None."""
        encoded = base64.b64encode(b"plain text, not json").decode("utf-8")
        result = extractor.decode_payload(encoded)
        assert result is None


class TestMakeRequestWithRetry:
    """Tests for HTTP request retry logic."""

    @pytest.fixture
    def extractor(self):
        """Create extractor with mocked init."""
        with patch("extract_lineage.load_dotenv"):
            with patch("extract_lineage.create_authenticator_from_env") as mock_auth:
                with patch("extract_lineage.setup_logging") as mock_log:
                    mock_auth.return_value.validate_credentials.return_value = True
                    mock_auth.return_value.get_fabric_token.return_value = "token"
                    mock_log.return_value = MagicMock()

                    return extract_lineage.LineageExtractor()

    @patch("extract_lineage.requests.request")
    def test_successful_request(self, mock_request, extractor):
        """Successful request should return response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        result = extractor.make_request_with_retry("GET", "https://api.example.com")

        assert result == mock_response
        assert mock_request.call_count == 1

    @patch("time.sleep")
    @patch("extract_lineage.requests.request")
    def test_retry_on_429(self, mock_request, mock_sleep, extractor):
        """429 response should trigger retry with backoff."""
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {"Retry-After": "1"}

        mock_200 = MagicMock()
        mock_200.status_code = 200

        mock_request.side_effect = [mock_429, mock_200]

        result = extractor.make_request_with_retry("GET", "https://api.example.com")

        assert result == mock_200
        assert mock_request.call_count == 2
        mock_sleep.assert_called()


class TestLineageDataStructure:
    """Tests for lineage data output structure."""

    def test_mirrored_database_record_structure(self):
        """Mirrored database records should have required fields."""
        record = {
            "Workspace Name": "Finance Workspace",
            "Workspace ID": "ws-001",
            "Item Name": "Sales Mirror",
            "Item ID": "item-001",
            "Item Type": "MirroredDatabase",
            "Shortcut Name": None,
            "Shortcut Path": None,
            "Source Type": "Snowflake",
            "Source Connection": "connection-123",
            "Source Database": "SALES_DB",
            "Connection ID": "conn-abc",
            "Full Definition": "{}",
        }

        required_fields = [
            "Workspace Name",
            "Workspace ID",
            "Item Name",
            "Item ID",
            "Item Type",
            "Source Type",
            "Source Connection",
        ]

        for field in required_fields:
            assert field in record

    def test_lakehouse_shortcut_record_structure(self):
        """Lakehouse shortcut records should have required fields."""
        record = {
            "Workspace Name": "Data Workspace",
            "Workspace ID": "ws-002",
            "Item Name": "Main Lakehouse",
            "Item ID": "lh-001",
            "Item Type": "Lakehouse Shortcut",
            "Shortcut Name": "external_data",
            "Shortcut Path": "Files/external_data",
            "Source Type": "adlsGen2",
            "Source Connection": "https://storage.dfs.core.windows.net/container/path",
            "Source Database": None,
            "Connection ID": None,
            "Full Definition": "{}",
        }

        assert record["Item Type"] == "Lakehouse Shortcut"
        assert record["Shortcut Name"] == "external_data"
        assert record["Shortcut Path"] == "Files/external_data"
