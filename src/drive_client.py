from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials


class DriveClient:
    def __init__(self, creds: Credentials):
        self._service = build("drive", "v3", credentials=creds)

    def list_sheets_in_folder(self, folder_id: str) -> list[dict]:
        """Return [{id, name}] for all Google Sheets files in the given folder (all pages)."""
        query = (
            f"'{folder_id}' in parents"
            " and mimeType='application/vnd.google-apps.spreadsheet'"
            " and trashed=false"
        )
        files = []
        page_token = None
        while True:
            kwargs = dict(q=query, fields="nextPageToken, files(id, name)", pageSize=1000)
            if page_token:
                kwargs["pageToken"] = page_token
            response = self._service.files().list(**kwargs).execute()
            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return files

    @staticmethod
    def _escape_query_string(value: str) -> str:
        """Escape single quotes in Drive query string values."""
        return value.replace("'", "\\'")

    def find_subfolder(self, parent_folder_id: str, subfolder_name: str) -> str:
        """Return the Drive folder ID for a named subfolder. Raises ValueError if not found."""
        safe_name = self._escape_query_string(subfolder_name)
        query = (
            f"'{parent_folder_id}' in parents"
            " and mimeType='application/vnd.google-apps.folder'"
            f" and name='{safe_name}'"
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
        safe_name = self._escape_query_string(sheet_name)
        query = (
            f"'{folder_id}' in parents"
            " and mimeType='application/vnd.google-apps.spreadsheet'"
            f" and name='{safe_name}'"
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
