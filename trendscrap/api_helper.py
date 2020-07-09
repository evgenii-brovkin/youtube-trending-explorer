import os
from typing import Dict, NoReturn, Any, Optional

from google.oauth2 import service_account

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


def build_drive_service(
    serice_account_credentials_file: Optional[str] = "service_account_credentials.json"
):
    """
    Connect and authenticate to a Google API service account 
    using info from `serice_account_credentials_file` JSON file.
    
    Return a drive resource object for interacting with Drive API
    """
    
    serice_account_credentials = os.environ.get('SERVICE_ACC_CREDS', None)
    if serice_account_credentials is not None:
        serice_account_credentials = eval(serice_account_credentials)
        credentials = service_account.Credentials.from_service_account_info(
            serice_account_credentials
        )
    else:
        credentials = service_account.Credentials.from_service_account_file(
            serice_account_credentials_file
        )
    return build("drive", "v3", credentials=credentials)


def upload_file(
    path_to_local_file: str,
    destination_path: str,
    drive_service=None,
    file_mimetype: str = None,
) -> str:
    """
    Upload a file located in `path_to_local_file` to `destination_path`. 
    Destination path must be in format "parent_dir1/parent_dir2/filename".
    Missing parent dirs must exists, otherwise error will be raised
    
    Returns a file_id of a newly uploaded file.
    """
    if drive_service is None:
        drive_service = build_drive_service()
    *parent_dirs, filename = destination_path.split("/")
    directories = (
        drive_service.files()
        .list(
            q="mimeType = 'application/vnd.google-apps.folder'",
            pageSize=10,
            fields="files(name, id)",
        )
        .execute()
    )
    parents = [
        dir["id"] for dir in directories["files"] if dir["name"] == parent_dirs[-1]
    ]
    file_metadata = {"name": filename, "parents": parents}
    media = MediaFileUpload(path_to_local_file, mimetype=file_mimetype)
    file = (
        drive_service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )
    return file["id"]


def upload_image(
    path_to_local_file: str, destination_path: str, drive_service=None
) -> str:
    """
    Upload a JPEG image located in `path_to_local_file` to `destination_path`. 
    Destination path must be in format "parent_dir1/parent_dir2/filename".
    Missing parent dirs must exists, otherwise error will be raised
    
    Returns a file_id of a newly uploaded file.
    """
    return upload_file(
        path_to_local_file, destination_path, drive_service, "image/jpeg"
    )


def delete_file(file_id: str, drive_service=None) -> NoReturn:
    """
    Delete a file with `file_id`.
    """
    if drive_service is None:
        drive_service = build_drive_service()
    drive_service.files().delete(fileId=file_id).execute()


def build_youtube_service(api_key_file: Optional[str] = "api_key.txt"):
    """
    Connect and authenticate to a Youtube Data API  
    using key from `api_key_file` file.
    
    Return a youtube resource object for interacting with Youtube API
    """
    api_key = os.environ.get('YT_API_KEY', None)
    if api_key is None:
        with open(api_key_file, "r") as f:
            api_key = f.read()
    youtube_service = build("youtube", "v3", developerKey=api_key)
    return youtube_service


def get_video_data_from_trending(
    region: str = "US", top_n_videos: int = 50
) -> Dict[str, Any]:
    """
    Fetch `top_n_videos` for a `region` from a Youtube trending page.

    Region must be a two character country code. Number of videos can not exceed 50.
    """
    youtube = build_youtube_service(api_key_file="api_key.txt")

    response = (
        youtube.videos()
        .list(
            part="snippet,contentDetails,statistics",
            chart="mostPopular",
            maxResults=top_n_videos,
            regionCode=region,
        )
        .execute()
    )
    return response


def exists(file_name: str, drive_service=None) -> bool:
    if drive_service is None:
        drive_service = build_drive_service()
    responce = (
        drive_service.files()
        .list(q=f"name = '{file_name}'", fields="files(name)")
        .execute()
    )
    if len(responce["files"]) > 0:
        return True
    return False
