"""
Tests for Fabric Authentication Module.

Tests cover:
- FabricAuthenticator initialisation with various credential combos
- create_authenticator_from_env() with mocked env vars (present and missing)
- Token acquisition via Azure SDK credential
- Token caching — cached token reused within expiry window
- Token refresh — expired token triggers new acquisition
- Error handling — missing credentials, failed token requests
- notebookutils fallback path
- Header generation
- Credential validation
"""

import os
import time
import types
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sp_credentials():
    """Valid service-principal credential dict."""
    return {
        "tenant_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "client_id": "11111111-2222-3333-4444-555555555555",
        "client_secret": "super-secret-value",
    }


@pytest.fixture
def env_vars(sp_credentials):
    """Mapping suitable for ``os.environ`` to set Azure SP env vars."""
    return {
        "AZURE_TENANT_ID": sp_credentials["tenant_id"],
        "AZURE_CLIENT_ID": sp_credentials["client_id"],
        "AZURE_CLIENT_SECRET": sp_credentials["client_secret"],
    }


@pytest.fixture
def _mock_azure_sdk():
    """Patch ``_AZURE_SDK_AVAILABLE`` to ``True`` and mock Azure SDK classes."""
    with (
        patch("usf_fabric_monitoring.core.auth._AZURE_SDK_AVAILABLE", True),
        patch("usf_fabric_monitoring.core.auth.ClientSecretCredential") as mock_csc,
        patch("usf_fabric_monitoring.core.auth.DefaultAzureCredential") as mock_dac,
    ):
        yield mock_csc, mock_dac


@pytest.fixture
def _mock_azure_sdk_unavailable():
    """Patch ``_AZURE_SDK_AVAILABLE`` to ``False``."""
    with patch("usf_fabric_monitoring.core.auth._AZURE_SDK_AVAILABLE", False):
        yield


def _make_token(token_str: str = "fake-token", expires_in_secs: float = 3600):
    """Return an object that looks like an ``azure.core.credentials.AccessToken``."""
    tok = MagicMock()
    tok.token = token_str
    tok.expires_on = time.time() + expires_in_secs
    return tok


# ===========================================================================
# Tests — FabricAuthenticator.__init__
# ===========================================================================


class TestFabricAuthenticatorInit:
    """Tests for FabricAuthenticator construction."""

    def test_init_with_full_sp_credentials(self, sp_credentials, _mock_azure_sdk):
        """Should use ClientSecretCredential when all three SP params are given."""
        mock_csc, _mock_dac = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        auth = FabricAuthenticator(**sp_credentials)

        assert auth.tenant_id == sp_credentials["tenant_id"]
        assert auth.client_id == sp_credentials["client_id"]
        assert auth.client_secret == sp_credentials["client_secret"]
        mock_csc.assert_called_once_with(
            tenant_id=sp_credentials["tenant_id"],
            client_id=sp_credentials["client_id"],
            client_secret=sp_credentials["client_secret"],
        )

    def test_init_without_credentials_uses_default_credential(self, _mock_azure_sdk):
        """Should fall back to DefaultAzureCredential when no SP params."""
        _mock_csc, mock_dac = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        auth = FabricAuthenticator()

        mock_dac.assert_called_once()
        assert auth.credential is not None

    def test_init_with_partial_credentials_uses_default(self, _mock_azure_sdk):
        """Providing only tenant_id (missing client_id/secret) should fall back."""
        _mock_csc, mock_dac = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        _auth = FabricAuthenticator(tenant_id="some-tenant")  # noqa: F841

        mock_dac.assert_called_once()

    def test_init_no_sdk_no_credentials_sets_credential_none(self, _mock_azure_sdk_unavailable):
        """When Azure SDK is unavailable and no SP, credential should be None."""
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        auth = FabricAuthenticator()

        assert auth.credential is None

    def test_init_no_sdk_with_sp_sets_credential_none(self, sp_credentials, _mock_azure_sdk_unavailable):
        """When Azure SDK is unavailable but SP supplied, credential is None."""
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        auth = FabricAuthenticator(**sp_credentials)

        assert auth.credential is None

    def test_token_cache_initialised_empty(self, _mock_azure_sdk):
        """Token cache fields should be None on fresh instance."""
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        auth = FabricAuthenticator()

        assert auth._fabric_token is None
        assert auth._powerbi_token is None
        assert auth._fabric_token_expires is None
        assert auth._powerbi_token_expires is None


# ===========================================================================
# Tests — create_authenticator_from_env
# ===========================================================================


class TestCreateAuthenticatorFromEnv:
    """Tests for the ``create_authenticator_from_env`` factory function."""

    def test_returns_sp_auth_when_all_env_set(self, env_vars, _mock_azure_sdk):
        """Should create authenticator with SP credentials from env."""
        from usf_fabric_monitoring.core.auth import create_authenticator_from_env

        with patch.dict(os.environ, env_vars, clear=False):
            auth = create_authenticator_from_env()

        assert auth.tenant_id == env_vars["AZURE_TENANT_ID"]
        assert auth.client_id == env_vars["AZURE_CLIENT_ID"]
        assert auth.client_secret == env_vars["AZURE_CLIENT_SECRET"]

    def test_falls_back_to_default_when_env_missing(self, _mock_azure_sdk):
        """Should return authenticator without SP when env vars absent."""
        from usf_fabric_monitoring.core.auth import create_authenticator_from_env

        clean_env = {
            k: v
            for k, v in os.environ.items()
            if k
            not in {
                "AZURE_TENANT_ID",
                "AZURE_CLIENT_ID",
                "AZURE_CLIENT_SECRET",
                "TENANT_ID",
                "CLIENT_ID",
                "CLIENT_SECRET",
            }
        }
        with patch.dict(os.environ, clean_env, clear=True):
            auth = create_authenticator_from_env()

        assert auth.tenant_id is None
        assert auth.client_id is None
        assert auth.client_secret is None

    def test_uses_legacy_env_var_aliases(self, sp_credentials, _mock_azure_sdk):
        """Should accept TENANT_ID / CLIENT_ID / CLIENT_SECRET aliases."""
        from usf_fabric_monitoring.core.auth import create_authenticator_from_env

        legacy_env = {
            "TENANT_ID": sp_credentials["tenant_id"],
            "CLIENT_ID": sp_credentials["client_id"],
            "CLIENT_SECRET": sp_credentials["client_secret"],
        }
        # Remove the AZURE_* variants so only legacy ones are present
        clean_env = {
            k: v
            for k, v in os.environ.items()
            if k
            not in {
                "AZURE_TENANT_ID",
                "AZURE_CLIENT_ID",
                "AZURE_CLIENT_SECRET",
                "TENANT_ID",
                "CLIENT_ID",
                "CLIENT_SECRET",
            }
        }
        clean_env.update(legacy_env)
        with patch.dict(os.environ, clean_env, clear=True):
            auth = create_authenticator_from_env()

        assert auth.tenant_id == sp_credentials["tenant_id"]
        assert auth.client_id == sp_credentials["client_id"]

    def test_partial_env_falls_back(self, _mock_azure_sdk):
        """If only some env vars are set, should fall back to default."""
        from usf_fabric_monitoring.core.auth import create_authenticator_from_env

        partial_env = {
            k: v
            for k, v in os.environ.items()
            if k
            not in {
                "AZURE_TENANT_ID",
                "AZURE_CLIENT_ID",
                "AZURE_CLIENT_SECRET",
                "TENANT_ID",
                "CLIENT_ID",
                "CLIENT_SECRET",
            }
        }
        partial_env["AZURE_TENANT_ID"] = "some-tenant"
        with patch.dict(os.environ, partial_env, clear=True):
            auth = create_authenticator_from_env()

        assert auth.tenant_id is None  # falls back to default


# ===========================================================================
# Tests — Token Acquisition
# ===========================================================================


class TestTokenAcquisition:
    """Tests for get_fabric_token / get_powerbi_token via Azure SDK."""

    def test_fabric_token_acquired_via_sp(self, sp_credentials, _mock_azure_sdk):
        """get_fabric_token should call credential.get_token with Fabric scope."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        fake_token = _make_token("fabric-tok-123")
        mock_csc.return_value.get_token.return_value = fake_token

        auth = FabricAuthenticator(**sp_credentials)
        token = auth.get_fabric_token()

        assert token == "fabric-tok-123"
        mock_csc.return_value.get_token.assert_called_once_with("https://api.fabric.microsoft.com/.default")

    def test_powerbi_token_acquired_via_sp(self, sp_credentials, _mock_azure_sdk):
        """get_powerbi_token should call credential.get_token with Power BI scope."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        fake_token = _make_token("pbi-tok-456")
        mock_csc.return_value.get_token.return_value = fake_token

        auth = FabricAuthenticator(**sp_credentials)
        token = auth.get_powerbi_token()

        assert token == "pbi-tok-456"
        mock_csc.return_value.get_token.assert_called_once_with("https://analysis.windows.net/powerbi/api/.default")

    def test_default_credential_fabric_token(self, _mock_azure_sdk):
        """Without SP, fabric token should be acquired via DefaultAzureCredential."""
        _, mock_dac = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        fake_token = _make_token("default-fab")
        mock_dac.return_value.get_token.return_value = fake_token

        auth = FabricAuthenticator()
        token = auth.get_fabric_token()

        assert token == "default-fab"

    def test_default_credential_powerbi_token(self, _mock_azure_sdk):
        """Without SP, power BI token should be acquired via DefaultAzureCredential."""
        _, mock_dac = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        fake_token = _make_token("default-pbi")
        mock_dac.return_value.get_token.return_value = fake_token

        auth = FabricAuthenticator()
        token = auth.get_powerbi_token()

        assert token == "default-pbi"


# ===========================================================================
# Tests — Token Caching
# ===========================================================================


class TestTokenCaching:
    """Tests for token caching and expiry behaviour."""

    def test_cached_token_reused(self, sp_credentials, _mock_azure_sdk):
        """Second call should return cached token without re-acquiring."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        fake_token = _make_token("cached-tok", expires_in_secs=3600)
        mock_csc.return_value.get_token.return_value = fake_token

        auth = FabricAuthenticator(**sp_credentials)
        tok1 = auth.get_fabric_token()
        tok2 = auth.get_fabric_token()

        assert tok1 == tok2 == "cached-tok"
        # get_token should only have been called ONCE
        assert mock_csc.return_value.get_token.call_count == 1

    def test_expired_token_triggers_refresh(self, sp_credentials, _mock_azure_sdk):
        """Expired token should cause a new acquisition."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        expired_token = _make_token("old-tok", expires_in_secs=-60)  # already expired
        new_token = _make_token("new-tok", expires_in_secs=3600)
        mock_csc.return_value.get_token.side_effect = [expired_token, new_token]

        auth = FabricAuthenticator(**sp_credentials)
        tok1 = auth.get_fabric_token()
        tok2 = auth.get_fabric_token()

        assert tok1 == "old-tok"
        assert tok2 == "new-tok"
        assert mock_csc.return_value.get_token.call_count == 2

    def test_force_refresh_bypasses_cache(self, sp_credentials, _mock_azure_sdk):
        """force_refresh=True should always call get_token."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        tok_a = _make_token("tok-a")
        tok_b = _make_token("tok-b")
        mock_csc.return_value.get_token.side_effect = [tok_a, tok_b]

        auth = FabricAuthenticator(**sp_credentials)
        auth.get_fabric_token()
        auth.get_fabric_token(force_refresh=True)

        assert mock_csc.return_value.get_token.call_count == 2

    def test_token_within_buffer_triggers_refresh(self, sp_credentials, _mock_azure_sdk):
        """Token expiring within the 5-minute buffer should be refreshed."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        # Expires in 4 minutes — within the 5-minute buffer
        soon_token = _make_token("soon-tok", expires_in_secs=240)
        fresh_token = _make_token("fresh-tok", expires_in_secs=3600)
        mock_csc.return_value.get_token.side_effect = [soon_token, fresh_token]

        auth = FabricAuthenticator(**sp_credentials)
        auth.get_fabric_token()
        tok = auth.get_fabric_token()

        assert tok == "fresh-tok"

    def test_powerbi_cache_independent_of_fabric(self, sp_credentials, _mock_azure_sdk):
        """Fabric and Power BI tokens should be cached independently."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        fab_tok = _make_token("fab-tok")
        pbi_tok = _make_token("pbi-tok")
        mock_csc.return_value.get_token.side_effect = [fab_tok, pbi_tok]

        auth = FabricAuthenticator(**sp_credentials)
        ft = auth.get_fabric_token()
        pt = auth.get_powerbi_token()

        assert ft == "fab-tok"
        assert pt == "pbi-tok"
        assert mock_csc.return_value.get_token.call_count == 2


# ===========================================================================
# Tests — _is_token_valid
# ===========================================================================


class TestIsTokenValid:
    """Tests for the private ``_is_token_valid`` helper."""

    def test_none_is_invalid(self, _mock_azure_sdk):
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        auth = FabricAuthenticator()
        assert auth._is_token_valid(None) is False

    def test_future_expiry_is_valid(self, _mock_azure_sdk):
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        auth = FabricAuthenticator()
        future = datetime.now() + timedelta(hours=1)
        assert auth._is_token_valid(future) is True

    def test_past_expiry_is_invalid(self, _mock_azure_sdk):
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        auth = FabricAuthenticator()
        past = datetime.now() - timedelta(hours=1)
        assert auth._is_token_valid(past) is False

    def test_within_buffer_is_invalid(self, _mock_azure_sdk):
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        auth = FabricAuthenticator()
        # 4 minutes into the future — still within the 5-min buffer
        almost = datetime.now() + timedelta(minutes=4)
        assert auth._is_token_valid(almost) is False


# ===========================================================================
# Tests — Notebookutils Fallback
# ===========================================================================


class TestNotebookutilsFallback:
    """Tests for the notebookutils-based token acquisition path."""

    def test_fabric_token_via_notebookutils(self, _mock_azure_sdk):
        """When no SP and notebookutils available, should use it for fabric token."""
        _, mock_dac = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        # Make DefaultAzureCredential fail so we fall through to notebookutils
        # Actually, without SP credentials, the code tries notebookutils first
        # then DefaultAzureCredential. Let's mock notebookutils.
        fake_creds_module = MagicMock()
        fake_creds_module.getToken.return_value = "notebook-fabric-token"

        fake_notebookutils = types.ModuleType("notebookutils")
        fake_notebookutils.credentials = fake_creds_module

        auth = FabricAuthenticator()  # no SP credentials

        with patch.dict(
            "sys.modules", {"notebookutils": fake_notebookutils, "notebookutils.credentials": fake_creds_module}
        ):
            token = auth.get_fabric_token()

        assert token == "notebook-fabric-token"
        fake_creds_module.getToken.assert_called_once_with("pbi")

    def test_powerbi_token_via_notebookutils(self, _mock_azure_sdk):
        """Notebookutils path should also work for Power BI tokens."""
        _, mock_dac = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        fake_creds_module = MagicMock()
        fake_creds_module.getToken.return_value = "notebook-pbi-token"

        fake_notebookutils = types.ModuleType("notebookutils")
        fake_notebookutils.credentials = fake_creds_module

        auth = FabricAuthenticator()

        with patch.dict(
            "sys.modules", {"notebookutils": fake_notebookutils, "notebookutils.credentials": fake_creds_module}
        ):
            token = auth.get_powerbi_token()

        assert token == "notebook-pbi-token"


# ===========================================================================
# Tests — Error Handling
# ===========================================================================


class TestErrorHandling:
    """Tests for authentication error scenarios."""

    def test_sp_credential_failure_raises(self, sp_credentials, _mock_azure_sdk):
        """If SP credential.get_token fails, error should propagate."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import ClientAuthenticationError, FabricAuthenticator

        mock_csc.return_value.get_token.side_effect = ClientAuthenticationError("invalid secret")

        auth = FabricAuthenticator(**sp_credentials)

        with pytest.raises(ClientAuthenticationError):
            auth.get_fabric_token()

    def test_default_credential_failure_raises(self, _mock_azure_sdk):
        """If DefaultAzureCredential fails and no notebookutils, should raise."""
        _, mock_dac = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import ClientAuthenticationError, FabricAuthenticator

        mock_dac.return_value.get_token.side_effect = Exception("no managed identity")

        auth = FabricAuthenticator()

        with pytest.raises(ClientAuthenticationError, match="Fabric authentication failed"):
            auth.get_fabric_token()

    def test_powerbi_default_credential_failure_raises(self, _mock_azure_sdk):
        """Power BI token path should also raise on failure."""
        _, mock_dac = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import ClientAuthenticationError, FabricAuthenticator

        mock_dac.return_value.get_token.side_effect = Exception("auth error")

        auth = FabricAuthenticator()

        with pytest.raises(ClientAuthenticationError, match="Power BI authentication failed"):
            auth.get_powerbi_token()

    def test_require_azure_sdk_raises_when_unavailable(self, sp_credentials, _mock_azure_sdk_unavailable):
        """_require_azure_sdk should raise when SDK not installed."""
        from usf_fabric_monitoring.core.auth import ClientAuthenticationError, FabricAuthenticator

        auth = FabricAuthenticator(**sp_credentials)

        with pytest.raises(ClientAuthenticationError, match="Azure SDK dependencies"):
            auth.get_fabric_token()


# ===========================================================================
# Tests — Headers
# ===========================================================================


class TestHeaders:
    """Tests for HTTP header generation helpers."""

    def test_get_fabric_headers(self, sp_credentials, _mock_azure_sdk):
        """get_fabric_headers should return dict with Bearer token."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        mock_csc.return_value.get_token.return_value = _make_token("hdr-tok")

        auth = FabricAuthenticator(**sp_credentials)
        headers = auth.get_fabric_headers()

        assert headers["Authorization"] == "Bearer hdr-tok"
        assert headers["Content-Type"] == "application/json"
        assert headers["Accept"] == "application/json"

    def test_get_powerbi_headers(self, sp_credentials, _mock_azure_sdk):
        """get_powerbi_headers should return dict with Bearer token."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        mock_csc.return_value.get_token.return_value = _make_token("pbi-hdr-tok")

        auth = FabricAuthenticator(**sp_credentials)
        headers = auth.get_powerbi_headers()

        assert headers["Authorization"] == "Bearer pbi-hdr-tok"


# ===========================================================================
# Tests — validate_credentials
# ===========================================================================


class TestValidateCredentials:
    """Tests for the ``validate_credentials`` method."""

    def test_returns_true_on_success(self, sp_credentials, _mock_azure_sdk):
        """Should return True when both tokens are acquired."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        fab = _make_token("fab")
        pbi = _make_token("pbi")
        mock_csc.return_value.get_token.side_effect = [fab, pbi]

        auth = FabricAuthenticator(**sp_credentials)
        assert auth.validate_credentials() is True

    def test_returns_false_on_failure(self, sp_credentials, _mock_azure_sdk):
        """Should return False when token acquisition fails."""
        mock_csc, _ = _mock_azure_sdk
        from usf_fabric_monitoring.core.auth import FabricAuthenticator

        mock_csc.return_value.get_token.side_effect = Exception("boom")

        auth = FabricAuthenticator(**sp_credentials)
        assert auth.validate_credentials() is False
