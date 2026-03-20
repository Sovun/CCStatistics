import pytest
from unittest.mock import MagicMock
from src.drive_client import DriveClient


@pytest.fixture
def mock_creds():
    return MagicMock()


@pytest.fixture
def client(mock_creds):
    c = DriveClient(mock_creds)
    return c


def _attach_mock_service(client):
    mock_service = MagicMock()
    client._service = mock_service
    return mock_service


def test_list_sheets_in_folder_returns_file_list(client):
    """list_sheets_in_folder returns [{id, name}] for Google Sheets in the folder."""
    mock_service = _attach_mock_service(client)
    mock_service.files().list().execute.return_value = {
        "files": [
            {"id": "abc123", "name": "Alice Stats"},
            {"id": "def456", "name": "Bob Stats"},
        ]
    }

    result = client.list_sheets_in_folder("folder123")

    assert result == [
        {"id": "abc123", "name": "Alice Stats"},
        {"id": "def456", "name": "Bob Stats"},
    ]


def test_list_sheets_in_folder_returns_empty_when_none(client):
    """list_sheets_in_folder returns [] when no sheets exist."""
    mock_service = _attach_mock_service(client)
    mock_service.files().list().execute.return_value = {"files": []}

    result = client.list_sheets_in_folder("folder123")
    assert result == []


def test_find_subfolder_returns_folder_id(client):
    """find_subfolder returns the Drive folder ID for a named subfolder."""
    mock_service = _attach_mock_service(client)
    mock_service.files().list().execute.return_value = {
        "files": [{"id": "sub999", "name": "Aggregated Info"}]
    }

    result = client.find_subfolder("parent123", "Aggregated Info")
    assert result == "sub999"


def test_find_subfolder_raises_when_not_found(client):
    """find_subfolder raises ValueError when subfolder does not exist."""
    mock_service = _attach_mock_service(client)
    mock_service.files().list().execute.return_value = {"files": []}

    with pytest.raises(ValueError, match="Subfolder 'Aggregated Info' not found"):
        client.find_subfolder("parent123", "Aggregated Info")


def test_get_or_create_sheet_returns_existing_id(client):
    """get_or_create_sheet returns existing sheet ID without creating a new one."""
    mock_service = _attach_mock_service(client)
    mock_service.files().list().execute.return_value = {
        "files": [{"id": "existing123", "name": "CC Statistics Aggregated"}]
    }

    result = client.get_or_create_sheet("folder456", "CC Statistics Aggregated")

    assert result == "existing123"
    mock_service.files().create.assert_not_called()


def test_get_or_create_sheet_creates_new_when_absent(client):
    """get_or_create_sheet creates a new Google Sheet when none exists with that name."""
    mock_service = _attach_mock_service(client)
    mock_service.files().list().execute.return_value = {"files": []}
    mock_service.files().create().execute.return_value = {"id": "new789"}

    result = client.get_or_create_sheet("folder456", "CC Statistics Aggregated")

    assert result == "new789"


def test_list_sheets_in_folder_follows_pagination(client):
    """list_sheets_in_folder follows nextPageToken to retrieve all pages."""
    mock_service = _attach_mock_service(client)

    page1 = {"nextPageToken": "token_abc", "files": [{"id": "id1", "name": "Sheet1"}]}
    page2 = {"files": [{"id": "id2", "name": "Sheet2"}]}
    mock_service.files().list().execute.side_effect = [page1, page2]

    result = client.list_sheets_in_folder("folder123")
    assert result == [{"id": "id1", "name": "Sheet1"}, {"id": "id2", "name": "Sheet2"}]


def test_find_subfolder_handles_single_quote_in_name(client):
    """find_subfolder escapes single quotes in subfolder name for Drive query."""
    mock_service = _attach_mock_service(client)
    mock_service.files().list().execute.return_value = {
        "files": [{"id": "sub777", "name": "Engineer's Stats"}]
    }

    result = client.find_subfolder("parent123", "Engineer's Stats")
    assert result == "sub777"

    # Verify the query contained the escaped name
    call_kwargs = mock_service.files().list.call_args.kwargs
    assert "Engineer\\'s Stats" in call_kwargs["q"]
