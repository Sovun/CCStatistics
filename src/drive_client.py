from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials


class DriveClient:
    def __init__(self, creds: Credentials):
        self._service = build("drive", "v3", credentials=creds)

    def list_sheets_in_folder(self, folder_id: str) -> list[dict]:
        """Return [{id, name}] for all Google Sheets files in the given folder (non-recursive)."""
        query = (
            f"'{folder_id}' in parents"
            " and mimeType='application/vnd.google-apps.spreadsheet'"
            " and trashed=false"
        )
        response = (
            self._service.files()
            .list(q=query, fields="files(id, name)", pageSize=100)
            .execute()
        )
        return response.get("files", [])

    def find_subfolder(self, parent_folder_id: str, subfolder_name: str) -> str:
        """Return the Drive folder ID for a named subfolder. Raises ValueError if not found."""
        query = (
            f"'{parent_folder_id}' in parents"
            " and mimeType='application/vnd.google-apps.folder'"
            f" and name='{subfolder_name}'"
            " and trashed=false"
        )
        response = (
            self._service.files()
            .list(q=query, fields="files(id, name)")
            .execute()
        )
        files = response.get("files", [])
        if not files:
            raise ValueError(
                f"Subfolder '{subfolder_name}' not found in folder {parent_folder_id}"
            )
        return files[0]["id"]

    def get_or_create_sheet(self, folder_id: str, sheet_name: str) -> str:
        """Return spreadsheet ID for a named sheet in folder, creating it if absent."""
        query = (
            f"'{folder_id}' in parents"
            " and mimeType='application/vnd.google-apps.spreadsheet'"
            f" and name='{sheet_name}'"
            " and trashed=false"
        )
        response = (
            self._service.files()
            .list(q=query, fields="files(id, name)")
            .execute()
        )
        files = response.get("files", [])
        if files:
            return files[0]["id"]

        metadata = {
            "name": sheet_name,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        }
        created = self._service.files().create(body=metadata, fields="id").execute()
        return created["id"]
