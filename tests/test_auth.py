import pytest
from unittest.mock import patch, MagicMock
from src.auth import get_google_credentials


def test_get_google_credentials_returns_valid_credentials(tmp_path):
    """get_google_credentials returns credentials when token file exists and is valid."""
    mock_creds = MagicMock()
    mock_creds.valid = True
    mock_creds.expired = False

    with patch("src.auth.os.path.exists", return_value=True), \
         patch("src.auth.Credentials") as MockCreds:
        MockCreds.from_authorized_user_file.return_value = mock_creds
        result = get_google_credentials(
            credentials_file="creds.json",
            token_file=str(tmp_path / "token.json"),
        )

    assert result == mock_creds


def test_get_google_credentials_refreshes_expired_token(tmp_path):
    """get_google_credentials refreshes credentials when token is expired."""
    mock_creds = MagicMock()
    mock_creds.valid = False
    mock_creds.expired = True
    mock_creds.refresh_token = "refresh123"

    with patch("src.auth.os.path.exists", return_value=True), \
         patch("src.auth.Credentials") as MockCreds, \
         patch("src.auth.Request"), \
         patch("builtins.open", MagicMock()):
        MockCreds.from_authorized_user_file.return_value = mock_creds

        result = get_google_credentials(
            credentials_file="creds.json",
            token_file=str(tmp_path / "token.json"),
        )

    mock_creds.refresh.assert_called_once()
    assert result == mock_creds


def test_get_google_credentials_runs_oauth_flow_when_no_token(tmp_path):
    """get_google_credentials runs OAuth flow when no token file exists."""
    mock_creds = MagicMock()

    with patch("src.auth.os.path.exists", return_value=False), \
         patch("src.auth.InstalledAppFlow") as MockFlow, \
         patch("builtins.open", MagicMock()):
        MockFlow.from_client_secrets_file.return_value.run_local_server.return_value = mock_creds

        result = get_google_credentials(
            credentials_file="creds.json",
            token_file=str(tmp_path / "token.json"),
        )

    MockFlow.from_client_secrets_file.assert_called_once()
    assert result == mock_creds
