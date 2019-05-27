from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

def init_service(name, version, keyfile):
    """Initializes Google API service object

    Returns:
        An authorized Google API service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile)

    # Build the service object.
    service = build(name, version, credentials=credentials)

    return service
