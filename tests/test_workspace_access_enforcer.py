"""
Tests for Workspace Access Enforcer.

Tests cover:
- AccessRequirement and SuppressionRules data classes
- WorkspaceAccessEnforcer initialisation and config validation
- Loading access requirements and suppressions from JSON
- Assess / dry-run mode — reports without modifying
- Enforce mode — makes API calls to assign groups
- Suppression filtering — suppressed workspaces are skipped
- Workspace fetching (Fabric and Power BI admin endpoints)
- Error handling for API failures
- HTTP retry / rate-limit logic
"""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from usf_fabric_monitoring.core.workspace_access_enforcer import (
    AccessRequirement,
    SuppressionRules,
    WorkspaceAccessEnforcer,
    WorkspaceAccessError,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def default_requirement():
    """A single access requirement."""
    return AccessRequirement(
        object_id="aaa-bbb-ccc",
        display_name="Fabric Admins",
        role="Admin",
    )


@pytest.fixture
def requirements(default_requirement):
    """Tuple of access requirements (minimum one)."""
    return [default_requirement]


@pytest.fixture
def suppression_rules():
    """SuppressionRules with one workspace ID and one name suppressed."""
    return SuppressionRules.from_payload(
        {
            "workspaceIds": ["suppressed-id-111"],
            "workspaceNames": ["Sandbox Workspace"],
        }
    )


@pytest.fixture
def mock_authenticator():
    """MagicMock that quacks like a FabricAuthenticator."""
    auth = MagicMock()
    auth.get_fabric_headers.return_value = {
        "Authorization": "Bearer fabric-tok",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    auth.get_powerbi_headers.return_value = {
        "Authorization": "Bearer pbi-tok",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    return auth


@pytest.fixture
def enforcer(requirements, suppression_rules, mock_authenticator):
    """A fully configured enforcer in dry_run mode (fabric-only API)."""
    return WorkspaceAccessEnforcer(
        access_requirements=requirements,
        suppressions=suppression_rules,
        authenticator=mock_authenticator,
        dry_run=True,
        api_preference="fabric",
    )


@pytest.fixture
def enforcer_live(requirements, suppression_rules, mock_authenticator):
    """Enforcer with dry_run=False for testing real assignment path."""
    return WorkspaceAccessEnforcer(
        access_requirements=requirements,
        suppressions=suppression_rules,
        authenticator=mock_authenticator,
        dry_run=False,
        api_preference="fabric",
    )


@pytest.fixture
def sample_workspaces_payload():
    """Simulated JSON response from workspace listing endpoints."""
    return {
        "value": [
            {
                "id": "ws-001",
                "name": "Finance Workspace",
                "displayName": "Finance Workspace",
                "state": "Active",
                "capacityId": "cap-001",
                "isOnDedicatedCapacity": True,
            },
            {
                "id": "ws-002",
                "name": "HR Workspace",
                "displayName": "HR Workspace",
                "state": "Active",
                "capacityId": "cap-001",
                "isOnDedicatedCapacity": True,
            },
        ]
    }


@pytest.fixture
def sample_users_payload(default_requirement):
    """Simulated response from workspace users endpoint (group already assigned)."""
    return {
        "value": [
            {
                "identifier": default_requirement.object_id,
                "displayName": default_requirement.display_name,
                "groupUserAccessRight": "Admin",
                "principalType": "Group",
            }
        ]
    }


@pytest.fixture
def empty_users_payload():
    """Workspace with no users."""
    return {"value": []}


# ===========================================================================
# Tests — AccessRequirement
# ===========================================================================


class TestAccessRequirement:
    """Tests for the AccessRequirement dataclass."""

    def test_defaults(self):
        """Default principal types should be AadGroup / Group."""
        req = AccessRequirement(object_id="id", display_name="Name", role="Admin")
        assert req.fabric_principal_type == "AadGroup"
        assert req.powerbi_principal_type == "Group"

    def test_custom_principal_types(self):
        """Should accept non-default principal types."""
        req = AccessRequirement(
            object_id="id",
            display_name="Name",
            role="Contributor",
            fabric_principal_type="ServicePrincipal",
            powerbi_principal_type="App",
        )
        assert req.fabric_principal_type == "ServicePrincipal"
        assert req.powerbi_principal_type == "App"
        assert req.role == "Contributor"


# ===========================================================================
# Tests — SuppressionRules
# ===========================================================================


class TestSuppressionRules:
    """Tests for SuppressionRules data class and filtering."""

    def test_from_payload_none(self):
        """None payload should produce empty suppressions."""
        rules = SuppressionRules.from_payload(None)
        assert len(rules.workspace_ids) == 0
        assert len(rules.workspace_names) == 0

    def test_from_payload_with_data(self):
        """Should parse workspace IDs and names."""
        rules = SuppressionRules.from_payload(
            {
                "workspaceIds": ["AAA", "BBB"],
                "workspaceNames": ["Sandbox"],
            }
        )
        assert "aaa" in rules.workspace_ids  # stored lowercased
        assert "sandbox" in rules.workspace_names

    def test_is_suppressed_by_id(self, suppression_rules):
        """Should suppress by ID (case-insensitive)."""
        assert suppression_rules.is_suppressed("SUPPRESSED-ID-111", "Some Name") is True

    def test_is_suppressed_by_name(self, suppression_rules):
        """Should suppress by name (case-insensitive)."""
        assert suppression_rules.is_suppressed("unknown-id", "sandbox workspace") is True

    def test_not_suppressed(self, suppression_rules):
        """Non-matching workspace should not be suppressed."""
        assert suppression_rules.is_suppressed("other-id", "Production WS") is False

    def test_is_suppressed_with_none_values(self):
        """Should handle None workspace_id / workspace_name gracefully."""
        rules = SuppressionRules.from_payload({"workspaceIds": ["x"], "workspaceNames": ["y"]})
        assert rules.is_suppressed(None, None) is False


# ===========================================================================
# Tests — WorkspaceAccessEnforcer Initialisation
# ===========================================================================


class TestEnforcerInit:
    """Tests for WorkspaceAccessEnforcer construction."""

    def test_init_stores_requirements(self, requirements, mock_authenticator):
        """Should store requirements as tuple."""
        enf = WorkspaceAccessEnforcer(
            access_requirements=requirements,
            authenticator=mock_authenticator,
        )
        assert len(enf.access_requirements) == 1

    def test_empty_requirements_raises(self, mock_authenticator):
        """Should raise WorkspaceAccessError when no requirements supplied."""
        with pytest.raises(WorkspaceAccessError, match="No access requirements"):
            WorkspaceAccessEnforcer(
                access_requirements=[],
                authenticator=mock_authenticator,
            )

    def test_invalid_api_preference_raises(self, requirements, mock_authenticator):
        """Should reject unknown api_preference values."""
        with pytest.raises(WorkspaceAccessError, match="api_preference"):
            WorkspaceAccessEnforcer(
                access_requirements=requirements,
                authenticator=mock_authenticator,
                api_preference="invalid",
            )

    def test_valid_api_preferences(self, requirements, mock_authenticator):
        """auto, fabric, powerbi should all be accepted."""
        for pref in ("auto", "fabric", "powerbi"):
            enf = WorkspaceAccessEnforcer(
                access_requirements=requirements,
                authenticator=mock_authenticator,
                api_preference=pref,
            )
            assert enf.api_preference == pref

    def test_default_suppressions_empty(self, requirements, mock_authenticator):
        """When no suppressions given, should default to empty rules."""
        enf = WorkspaceAccessEnforcer(
            access_requirements=requirements,
            authenticator=mock_authenticator,
        )
        assert enf.suppressions.is_suppressed("any-id", "any-name") is False

    def test_dry_run_flag(self, requirements, mock_authenticator):
        """dry_run parameter should be stored."""
        enf = WorkspaceAccessEnforcer(
            access_requirements=requirements,
            authenticator=mock_authenticator,
            dry_run=True,
        )
        assert enf.dry_run is True


# ===========================================================================
# Tests — load_access_requirements / load_suppressions
# ===========================================================================


class TestLoadConfig:
    """Tests for static configuration loaders."""

    def test_load_access_requirements_valid(self, tmp_path):
        """Should parse groups array from JSON file."""
        config = {
            "groups": [
                {
                    "objectId": "group-id-1",
                    "displayName": "Admins",
                    "role": "Admin",
                },
                {
                    "objectId": "group-id-2",
                    "displayName": "Contributors",
                    "role": "Contributor",
                    "fabricPrincipalType": "ServicePrincipal",
                    "powerBiPrincipalType": "App",
                },
            ]
        }
        path = tmp_path / "targets.json"
        path.write_text(json.dumps(config))

        reqs = WorkspaceAccessEnforcer.load_access_requirements(path)

        assert len(reqs) == 2
        assert reqs[0].display_name == "Admins"
        assert reqs[1].role == "Contributor"
        assert reqs[1].fabric_principal_type == "ServicePrincipal"

    def test_load_access_requirements_missing_file(self, tmp_path):
        """Should raise when file doesn't exist."""
        with pytest.raises(WorkspaceAccessError, match="not found"):
            WorkspaceAccessEnforcer.load_access_requirements(tmp_path / "missing.json")

    def test_load_access_requirements_no_groups(self, tmp_path):
        """Should raise when JSON has no groups key."""
        path = tmp_path / "empty.json"
        path.write_text(json.dumps({"description": "empty"}))
        with pytest.raises(WorkspaceAccessError, match="No groups"):
            WorkspaceAccessEnforcer.load_access_requirements(path)

    def test_load_suppressions_valid(self, tmp_path):
        """Should parse suppressions from JSON."""
        path = tmp_path / "suppressions.json"
        path.write_text(
            json.dumps(
                {
                    "workspaceIds": ["aaa"],
                    "workspaceNames": ["Sandbox"],
                }
            )
        )
        rules = WorkspaceAccessEnforcer.load_suppressions(path)
        assert rules.is_suppressed("aaa", "") is True

    def test_load_suppressions_missing_file(self, tmp_path):
        """Missing file should return empty suppressions (not raise)."""
        rules = WorkspaceAccessEnforcer.load_suppressions(tmp_path / "nope.json")
        assert rules.is_suppressed("anything", "anything") is False

    def test_load_suppressions_none_path(self):
        """None path should return empty suppressions."""
        rules = WorkspaceAccessEnforcer.load_suppressions(None)
        assert rules.is_suppressed("x", "y") is False


# ===========================================================================
# Tests — _is_fabric_workspace
# ===========================================================================


class TestIsFabricWorkspace:
    """Tests for workspace type detection."""

    def test_with_capacity_id(self, enforcer):
        """Workspace with a non-null capacityId is Fabric/Premium."""
        ws = {"capacityId": "real-cap-id"}
        assert enforcer._is_fabric_workspace(ws) is True

    def test_with_zero_capacity_id(self, enforcer):
        """All-zero GUID capacity means non-Fabric."""
        ws = {"capacityId": "00000000-0000-0000-0000-000000000000"}
        assert enforcer._is_fabric_workspace(ws) is False

    def test_with_dedicated_capacity_flag(self, enforcer):
        """isOnDedicatedCapacity=True should mark as Fabric."""
        ws = {"isOnDedicatedCapacity": True}
        assert enforcer._is_fabric_workspace(ws) is True

    def test_non_fabric_workspace(self, enforcer):
        """Workspace with no capacity indicators should return False."""
        ws = {"id": "ws-free"}
        assert enforcer._is_fabric_workspace(ws) is False


# ===========================================================================
# Tests — _locate_existing_role
# ===========================================================================


class TestLocateExistingRole:
    """Tests for matching a principal in existing workspace users."""

    def test_match_by_identifier(self, enforcer):
        """Should match via 'identifier' field."""
        users = [{"identifier": "AAA-BBB-CCC", "groupUserAccessRight": "Admin"}]
        assert enforcer._locate_existing_role(users, "aaa-bbb-ccc") == "Admin"

    def test_match_by_objectId(self, enforcer):
        """Should match via 'objectId' field."""
        users = [{"objectId": "xyz", "role": "Contributor"}]
        assert enforcer._locate_existing_role(users, "XYZ") == "Contributor"

    def test_no_match_returns_none(self, enforcer):
        """Should return None when no matching user found."""
        users = [{"identifier": "other-id", "groupUserAccessRight": "Admin"}]
        assert enforcer._locate_existing_role(users, "target-id") is None

    def test_empty_users_returns_none(self, enforcer):
        """Should return None for empty user list."""
        assert enforcer._locate_existing_role([], "any") is None


# ===========================================================================
# Tests — _select_workspace_source
# ===========================================================================


class TestSelectWorkspaceSource:
    """Tests for source preference ordering."""

    def test_prefer_fabric(self):
        """Fabric source should be preferred."""
        assert WorkspaceAccessEnforcer._select_workspace_source(["powerbi", "fabric"]) == "fabric"

    def test_fallback_to_powerbi(self):
        """Without fabric, powerbi should be chosen."""
        assert WorkspaceAccessEnforcer._select_workspace_source(["powerbi"]) == "powerbi"

    def test_empty_returns_powerbi(self):
        """Empty list edge case (should not happen, but returns powerbi)."""
        assert WorkspaceAccessEnforcer._select_workspace_source([]) == "powerbi"


# ===========================================================================
# Tests — Dry-Run / Assess Mode
# ===========================================================================


class TestDryRun:
    """Tests for dry_run (assess) mode — no modifications."""

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_dry_run_does_not_post(self, mock_request, enforcer, sample_workspaces_payload, empty_users_payload):
        """In dry_run mode, POST assignment calls should NOT happen."""
        responses = []
        for payload in [sample_workspaces_payload, empty_users_payload, empty_users_payload]:
            r = MagicMock(status_code=200)
            r.json.return_value = payload
            responses.append(r)
        mock_request.side_effect = responses

        summary = enforcer.enforce()

        # Verify only GET requests were made (no POST for assignment)
        for call_args in mock_request.call_args_list:
            assert call_args[0][0] == "GET"

        assert summary["dry_run"] is True

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_dry_run_action_label(self, mock_request, enforcer, empty_users_payload):
        """Dry-run actions should be labelled 'dry_run'."""
        ws_list = {"value": [{"id": "ws-test", "name": "Test", "state": "Active", "capacityId": "c"}]}
        responses = []
        for payload in [ws_list, empty_users_payload]:
            r = MagicMock(status_code=200)
            r.json.return_value = payload
            responses.append(r)
        mock_request.side_effect = responses

        summary = enforcer.enforce()

        actions = summary["actions"][0]["actions"]
        assert any(a["action"] == "dry_run" for a in actions)


# ===========================================================================
# Tests — Enforce Mode (live assignment)
# ===========================================================================


class TestEnforceMode:
    """Tests for live enforcement that makes API calls."""

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_assigns_missing_group(self, mock_request, enforcer_live, empty_users_payload):
        """Should POST to assign group when missing from workspace."""
        ws_list = {"value": [{"id": "ws-new", "name": "New WS", "state": "Active", "capacityId": "c"}]}

        r_ws = MagicMock(status_code=200)
        r_ws.json.return_value = ws_list
        r_users = MagicMock(status_code=200)
        r_users.json.return_value = empty_users_payload
        r_post = MagicMock(status_code=200)
        r_post.json.return_value = {}

        mock_request.side_effect = [r_ws, r_users, r_post]

        enforcer_live.enforce()

        # At least one POST should have been made for assignment
        post_calls = [c for c in mock_request.call_args_list if c[0][0] == "POST"]
        assert len(post_calls) >= 1

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_already_compliant_no_post(self, mock_request, enforcer_live, sample_users_payload):
        """Should NOT POST when group already has correct role."""
        ws_list = {"value": [{"id": "ws-ok", "name": "OK WS", "state": "Active", "capacityId": "c"}]}
        r_ws = MagicMock(status_code=200)
        r_ws.json.return_value = ws_list
        r_users = MagicMock(status_code=200)
        r_users.json.return_value = sample_users_payload
        mock_request.side_effect = [r_ws, r_users]

        summary = enforcer_live.enforce()

        actions = summary["actions"][0]["actions"]
        assert actions[0]["action"] == "already_compliant"
        # Only GETs should have been issued
        for c in mock_request.call_args_list:
            assert c[0][0] == "GET"

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_role_mismatch_detected(self, mock_request, enforcer_live, default_requirement):
        """Should detect role mismatch (e.g., Contributor vs required Admin)."""
        ws_list = {"value": [{"id": "ws-mm", "name": "Mismatch WS", "state": "Active", "capacityId": "c"}]}
        mismatch_users = {
            "value": [
                {
                    "identifier": default_requirement.object_id,
                    "groupUserAccessRight": "Viewer",  # wrong role
                }
            ]
        }
        r_ws = MagicMock(status_code=200)
        r_ws.json.return_value = ws_list
        r_users = MagicMock(status_code=200)
        r_users.json.return_value = mismatch_users
        mock_request.side_effect = [r_ws, r_users]

        summary = enforcer_live.enforce()

        actions = summary["actions"][0]["actions"]
        assert actions[0]["action"] == "role_mismatch"
        assert actions[0]["current_role"] == "Viewer"
        assert actions[0]["target_role"] == "Admin"


# ===========================================================================
# Tests — Suppression Filtering
# ===========================================================================


class TestSuppressionFiltering:
    """Tests for workspace suppression during enforcement."""

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_suppressed_workspace_skipped(self, mock_request, enforcer):
        """Suppressed workspace should be marked 'suppressed' and skipped."""
        ws_list = {
            "value": [
                {"id": "suppressed-id-111", "name": "Suppressed WS", "state": "Active", "capacityId": "c"},
                {"id": "ws-normal", "name": "Normal WS", "state": "Active", "capacityId": "c"},
            ]
        }
        users_payload = {"value": []}

        r_ws = MagicMock(status_code=200)
        r_ws.json.return_value = ws_list
        r_users = MagicMock(status_code=200)
        r_users.json.return_value = users_payload
        mock_request.side_effect = [r_ws, r_users]

        summary = enforcer.enforce()

        suppressed = [a for a in summary["actions"] if a["status"] == "suppressed"]
        evaluated = [a for a in summary["actions"] if a["status"] == "evaluated"]
        assert len(suppressed) == 1
        assert len(evaluated) == 1

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_suppressed_by_name(self, mock_request, enforcer):
        """Should suppress workspace that matches by name."""
        ws_list = {
            "value": [
                {"id": "ws-xyz", "name": "Sandbox Workspace", "state": "Active", "capacityId": "c"},
            ]
        }
        get_resp = MagicMock(status_code=200)
        get_resp.json.return_value = ws_list
        mock_request.return_value = get_resp

        summary = enforcer.enforce()

        assert summary["actions"][0]["status"] == "suppressed"


# ===========================================================================
# Tests — Workspace Filtering
# ===========================================================================


class TestWorkspaceFiltering:
    """Tests for workspace_filter and max_workspaces parameters."""

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_workspace_filter_by_name(self, mock_request, enforcer, empty_users_payload):
        """workspace_filter should limit to matching workspaces."""
        ws_list = {
            "value": [
                {"id": "ws-a", "name": "Alpha", "state": "Active", "capacityId": "c"},
                {"id": "ws-b", "name": "Beta", "state": "Active", "capacityId": "c"},
            ]
        }
        r_ws = MagicMock(status_code=200)
        r_ws.json.return_value = ws_list
        r_users = MagicMock(status_code=200)
        r_users.json.return_value = empty_users_payload
        mock_request.side_effect = [r_ws, r_users]

        summary = enforcer.enforce(workspace_filter=["alpha"])

        assert summary["workspace_count"] == 1

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_max_workspaces_limit(self, mock_request, enforcer, empty_users_payload):
        """max_workspaces should truncate the list."""
        ws_list = {
            "value": [{"id": f"ws-{i}", "name": f"WS {i}", "state": "Active", "capacityId": "c"} for i in range(10)]
        }
        responses = []
        r_ws = MagicMock(status_code=200)
        r_ws.json.return_value = ws_list
        responses.append(r_ws)
        for _ in range(3):
            r = MagicMock(status_code=200)
            r.json.return_value = empty_users_payload
            responses.append(r)
        mock_request.side_effect = responses

        summary = enforcer.enforce(max_workspaces=3)

        assert summary["workspace_count"] == 3


# ===========================================================================
# Tests — _enforce_workspace edge cases
# ===========================================================================


class TestEnforceWorkspaceEdgeCases:
    """Edge-case tests for individual workspace enforcement."""

    def test_missing_workspace_id_skipped(self, enforcer):
        """Workspace dict with no 'id' should be skipped."""
        result = enforcer._enforce_workspace({"name": "No-ID WS"})
        assert result["status"] == "skipped"
        assert result["reason"] == "missing_id"


# ===========================================================================
# Tests — HTTP Error Handling
# ===========================================================================


class TestHttpErrorHandling:
    """Tests for API failure scenarios and retry logic."""

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_fetch_workspaces_failure_raises(self, mock_request, enforcer_live):
        """If all workspace endpoints fail, should raise WorkspaceAccessError."""
        mock_request.side_effect = requests.RequestException("network down")

        with pytest.raises(WorkspaceAccessError, match="Failed to enumerate"):
            enforcer_live.enforce()

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.time.sleep")
    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_429_retry(self, mock_request, mock_sleep, enforcer):
        """Should retry on HTTP 429 rate-limit response."""
        rate_resp = MagicMock(status_code=429, headers={"Retry-After": "1"}, text="rate limited")
        r_ws = MagicMock(status_code=200)
        r_ws.json.return_value = {"value": [{"id": "ws-1", "name": "WS", "state": "Active", "capacityId": "c"}]}
        r_users = MagicMock(status_code=200)
        r_users.json.return_value = {"value": []}
        mock_request.side_effect = [rate_resp, r_ws, r_users]

        # Should not raise — first 429 retried then succeeds
        enforcer.enforce()

        mock_sleep.assert_called()

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.time.sleep")
    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_server_error_retry(self, mock_request, mock_sleep, enforcer):
        """Should retry on 5xx server errors."""
        err_resp = MagicMock(status_code=503, headers={}, text="service unavailable")
        r_ws = MagicMock(status_code=200)
        r_ws.json.return_value = {"value": [{"id": "ws-1", "name": "WS", "state": "Active", "capacityId": "c"}]}
        r_users = MagicMock(status_code=200)
        r_users.json.return_value = {"value": []}
        mock_request.side_effect = [err_resp, r_ws, r_users]

        enforcer.enforce()
        mock_sleep.assert_called()

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_non_retryable_error_raises(self, mock_request, enforcer):
        """Non-retryable HTTP errors (e.g., 403) should raise immediately."""
        forbidden_resp = MagicMock(status_code=403, headers={}, text="forbidden")
        mock_request.return_value = forbidden_resp

        with pytest.raises(WorkspaceAccessError, match="403"):
            enforcer.enforce()

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_invalid_json_raises(self, mock_request, enforcer):
        """Should raise when API returns invalid JSON."""
        bad_resp = MagicMock(status_code=200)
        bad_resp.json.side_effect = ValueError("Expecting value")
        mock_request.return_value = bad_resp

        with pytest.raises(WorkspaceAccessError, match="invalid JSON"):
            enforcer.enforce()


# ===========================================================================
# Tests — Fetch workspace users error paths
# ===========================================================================


class TestFetchWorkspaceUsers:
    """Tests for _fetch_workspace_users error paths."""

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_404_returns_empty(self, mock_request, enforcer):
        """404 from all sources should return empty list (graceful skip)."""
        resp_404 = MagicMock(status_code=404, text="not found", headers={})
        mock_request.return_value = resp_404

        result = enforcer._fetch_workspace_users("ws-missing")

        assert result == []


# ===========================================================================
# Tests — Assignment via different APIs
# ===========================================================================


class TestAssignment:
    """Tests for _assign_group via Fabric and Power BI APIs."""

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_assign_via_fabric_api(self, mock_request, enforcer_live, default_requirement):
        """Should POST assignment via Fabric API endpoint."""
        post_resp = MagicMock(status_code=200)
        post_resp.json.return_value = {}
        mock_request.return_value = post_resp

        result = enforcer_live._assign_group("ws-test", default_requirement)

        assert result["action"] == "added_via_fabric"
        mock_request.assert_called_once()
        call_args = mock_request.call_args
        assert call_args[0][0] == "POST"
        payload = call_args[1]["json"]
        assert payload["userObjectId"] == default_requirement.object_id
        assert payload["role"] == "Admin"

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_assign_falls_back_to_powerbi(
        self, mock_request, requirements, suppression_rules, mock_authenticator, default_requirement
    ):
        """If Fabric API fails, should fall back to Power BI API."""
        # Need api_preference="auto" to exercise the Fabric→PowerBI fallback
        enf = WorkspaceAccessEnforcer(
            access_requirements=requirements,
            suppressions=suppression_rules,
            authenticator=mock_authenticator,
            dry_run=False,
            api_preference="auto",
        )
        fabric_err = MagicMock(status_code=403, text="forbidden", headers={})
        pbi_ok = MagicMock(status_code=200)
        pbi_ok.json.return_value = {}
        mock_request.side_effect = [fabric_err, pbi_ok]

        result = enf._assign_group("ws-test", default_requirement)

        assert result["action"] == "added_via_powerbi"

    def test_dry_run_assign_returns_dry_label(self, enforcer, default_requirement):
        """In dry_run, assignment should return action='dry_run' without HTTP."""
        result = enforcer._assign_group("ws-test", default_requirement)
        assert result["action"] == "dry_run"


# ===========================================================================
# Tests — Fabric-only workspace filtering
# ===========================================================================


class TestFabricOnlyFiltering:
    """Tests for fabric_only parameter in enforce()."""

    @patch("usf_fabric_monitoring.core.workspace_access_enforcer.requests.request")
    def test_fabric_only_filters_non_premium(self, mock_request, enforcer, empty_users_payload):
        """fabric_only=True should exclude workspaces without capacity."""
        ws_list = {
            "value": [
                {"id": "ws-fab", "name": "Fabric WS", "state": "Active", "capacityId": "cap-1"},
                {"id": "ws-free", "name": "Free WS", "state": "Active"},
            ]
        }
        r_ws = MagicMock(status_code=200)
        r_ws.json.return_value = ws_list
        r_users = MagicMock(status_code=200)
        r_users.json.return_value = empty_users_payload
        mock_request.side_effect = [r_ws, r_users]

        summary = enforcer.enforce(fabric_only=True)

        assert summary["workspace_count"] == 1
