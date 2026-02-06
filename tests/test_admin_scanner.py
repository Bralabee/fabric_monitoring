"""
Tests for Admin Scanner API Client

Tests cover:
- Client initialization
- Scan workflow (initiate, poll, get results)
- Result normalization
- Error handling and fallback
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestAdminScannerClient:
    """Tests for AdminScannerClient class."""

    @pytest.fixture
    def client(self):
        """Create a scanner client with test token."""
        from usf_fabric_monitoring.core.admin_scanner import AdminScannerClient

        return AdminScannerClient(token="test_token_123")

    def test_init_sets_headers(self, client):
        """Client should set authorization headers correctly."""
        assert "Authorization" in client.headers
        assert "Bearer test_token_123" in client.headers["Authorization"]

    def test_batch_size_is_100(self, client):
        """Batch size should be 100 (API limit)."""
        assert client.batch_size == 100

    @patch("usf_fabric_monitoring.core.admin_scanner.requests.request")
    def test_initiate_scan_returns_scan_id(self, mock_request, client):
        """Successful scan initiation should return scan ID."""
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_response.json.return_value = {"id": "scan-123"}
        mock_request.return_value = mock_response

        result = client._initiate_scan(
            workspace_ids=["ws-1", "ws-2"],
            lineage=True,
            datasource_details=True,
            dataset_schema=False,
            dataset_expressions=False,
        )

        assert result == "scan-123"

    @patch("usf_fabric_monitoring.core.admin_scanner.requests.request")
    def test_get_scan_status_returns_status(self, mock_request, client):
        """Should return status string from API."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "Running"}
        mock_request.return_value = mock_response

        result = client._get_scan_status("scan-123")

        assert result == "Running"

    @patch("usf_fabric_monitoring.core.admin_scanner.requests.request")
    def test_retry_on_429(self, mock_request, client):
        """Should retry on 429 rate limit response."""
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.headers = {"Retry-After": "1"}

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"status": "Succeeded"}

        mock_request.side_effect = [mock_429, mock_200]

        with patch("usf_fabric_monitoring.core.admin_scanner.time.sleep"):
            result = client._get_scan_status("scan-123")

        assert result == "Succeeded"
        assert mock_request.call_count == 2


class TestResultNormalization:
    """Tests for normalizing scanner results to iterative format."""

    @pytest.fixture
    def client(self):
        from usf_fabric_monitoring.core.admin_scanner import AdminScannerClient

        return AdminScannerClient(token="test_token")

    def test_normalize_empty_result(self, client):
        """Empty scan result should return empty list."""
        result = client.normalize_lineage_results({"workspaces": []})
        assert result == []

    def test_normalize_lakehouse_with_lineage(self, client):
        """Should extract lakehouse with upstream/downstream."""
        scan_result = {
            "workspaces": [
                {
                    "id": "ws-001",
                    "name": "Finance Workspace",
                    "lakehouses": [
                        {
                            "id": "lh-001",
                            "name": "Sales Lakehouse",
                            "upstreamDataflows": [{"id": "df-001"}],
                            "downstreamDataflows": [],
                            "datasourceUsages": [],
                        }
                    ],
                }
            ]
        }

        result = client.normalize_lineage_results(scan_result)

        assert len(result) == 1
        assert result[0]["Workspace Name"] == "Finance Workspace"
        assert result[0]["Item Name"] == "Sales Lakehouse"
        assert result[0]["Item Type"] == "Lakehouse"
        assert result[0]["Upstream Count"] == 1


class TestHybridExtractor:
    """Tests for hybrid mode selection logic."""

    @patch("scripts.extract_lineage.load_dotenv")
    @patch("scripts.extract_lineage.create_authenticator_from_env")
    @patch("scripts.extract_lineage.setup_logging")
    def test_mode_selection_auto_below_threshold(self, mock_log, mock_auth, mock_dotenv):
        """Auto mode with few workspaces should use iterative."""
        mock_auth.return_value.validate_credentials.return_value = True
        mock_auth.return_value.get_fabric_token.return_value = "token"
        mock_log.return_value = MagicMock()

        import sys

        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from extract_lineage import HybridLineageExtractor

        extractor = HybridLineageExtractor(mode="auto", threshold=50)

        # Mock workspace count below threshold
        with patch.object(extractor, "_count_workspaces", return_value=30):
            with patch.object(extractor, "_run_iterative", return_value=Path("/tmp/test.json")) as mock_iter:
                extractor.extract("/tmp/test")
                mock_iter.assert_called_once()

    @patch("scripts.extract_lineage.load_dotenv")
    @patch("scripts.extract_lineage.create_authenticator_from_env")
    @patch("scripts.extract_lineage.setup_logging")
    def test_mode_selection_auto_above_threshold(self, mock_log, mock_auth, mock_dotenv):
        """Auto mode with many workspaces should use scanner."""
        mock_auth.return_value.validate_credentials.return_value = True
        mock_auth.return_value.get_fabric_token.return_value = "token"
        mock_log.return_value = MagicMock()

        import sys

        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from extract_lineage import HybridLineageExtractor

        extractor = HybridLineageExtractor(mode="auto", threshold=50)

        # Mock workspace count above threshold
        with patch.object(extractor, "_count_workspaces", return_value=100):
            with patch.object(extractor, "_run_scanner", return_value=Path("/tmp/test.json")) as mock_scan:
                extractor.extract("/tmp/test")
                mock_scan.assert_called_once()

    @patch("scripts.extract_lineage.load_dotenv")
    @patch("scripts.extract_lineage.create_authenticator_from_env")
    @patch("scripts.extract_lineage.setup_logging")
    def test_scanner_fallback_on_error(self, mock_log, mock_auth, mock_dotenv):
        """Scanner failure should fall back to iterative."""
        mock_auth.return_value.validate_credentials.return_value = True
        mock_auth.return_value.get_fabric_token.return_value = "token"
        mock_log.return_value = MagicMock()

        import sys

        sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
        from extract_lineage import HybridLineageExtractor

        extractor = HybridLineageExtractor(mode="scanner")

        # Mock scanner to fail, should fall back
        with patch.object(extractor, "_get_all_workspace_ids", return_value=["ws-1"]):
            with patch("usf_fabric_monitoring.core.admin_scanner.AdminScannerClient") as mock_client:
                mock_client.return_value.scan_workspaces.side_effect = Exception("API Error")
                with patch.object(extractor, "_run_iterative", return_value=Path("/tmp/test.json")) as mock_iter:
                    extractor.extract("/tmp/test")
                    mock_iter.assert_called_once()
