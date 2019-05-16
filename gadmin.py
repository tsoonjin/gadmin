"""
GA Admin for batch processing via Google Analytics API

Features
--------
Bulk add users
Bulk delete users
Bulk add GTM tags with scheduled duration
"""

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError


GA_SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/analytics.manage.users",
]
TM_SCOPES = ["https://www.googleapis.com/auth/tagmanager.edit.containers"]
GA_KEY_FILE_LOCATION = "./secrets/client_secrets.json"
VIEW_ID = "86412644"
ACCOUNT_ID = "20278225"


def init_service(name, version, scopes, keyfile_location):
    """Initializes Google API service object

    Returns:
        An authorized Google API service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        keyfile_location, scopes
    )

    # Build the service object.
    service = build(name, version, credentials=credentials)

    return service


def list_account_users(service, accountId):
    """ Get all users linked to an account

    Returns:
        users (list): list of users linked to the accountId
    """
    try:
        account_links = (
            service.management().accountUserLinks().list(accountId=accountId).execute()
        )
        return account_links

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def list_view_users(service, accountId, webPropertyId, profileId):
    """ List all users for a view (profile)

    Returns:
        view_users (list): list of users linked to the viewId
    """
    try:
        profile_links = (
            service.management()
            .profileUserLinks()
            .list(accountId=accountId, webPropertyId=webPropertyId, profileId=profileId)
            .execute()
        )
        for profileUserLink in profile_links.get("items", []):
            userRef = profileUserLink.get("userRef", {})
            permissions = profileUserLink.get("permissions", {})

            print(
                f'Email: {userRef.get("email")}, LinkId: {profileUserLink.get("id")}, Permissions: {permissions.get("effective")}'
            )

        return profile_links

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def delete_user_from_view(service, accountId, webPropertyId, profileId, linkId):
    """ Delete user from a view (profile)
    """
    try:
        service.management().profileUserLinks().delete(
            accountId=accountId,
            webPropertyId=webPropertyId,
            profileId=profileId,
            linkId=linkId,
        ).execute()

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def add_user_to_view(service, accountId, webPropertyId, profileId, config):
    """ Add user to a view (profile)
    """
    try:
        service.management().profileUserLinks().insert(
            accountId=accountId,
            webPropertyId=webPropertyId,
            profileId=profileId,
            body={
                "permissions": {"local": config["permissions"]},
                "userRef": {"email": config["email"]},
            },
        ).execute()

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def list_containers(service, accountId):
    """ List all containers under a Tag Manager account

    Returns:
        containers (list): list of containers under the accountId
    """
    account_path = f"accounts/{accountId}"

    try:
        container_wrapper = (
            service.accounts().containers().list(parent=account_path).execute()
        )
        for container in container_wrapper["container"]:
            print(f'Name: {container["name"]}')
        return container_wrapper

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def get_container(service, accountId, containerName):
    """ Retrieve specific container under a Tag Manager account

    Returns:
        containers (obj): container
    """
    account_path = f"accounts/{accountId}"

    try:
        container_wrapper = (
            service.accounts().containers().list(parent=account_path).execute()
        )
        for container in container_wrapper["container"]:
            if container["name"] == containerName:
                return container
        return None

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def create_workspace(service, container, workspace_name):
    """Creates a workspace named 'my workspace'.

    Args:
      service:          the Tag Manager service object.
      container:        the container to insert the workspace within.
      workspace_name:   name of the workspace

    Returns:
      The created workspace.
    """
    try:
        return (
            service.accounts()
            .containers()
            .workspaces()
            .create(parent=container["path"], body={"name": workspace_name})
            .execute()
        )

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def create_tag(service, accountId, containerId, workspaceId, tag_body):
    """Create the Universal Analytics Hello World Tag.

    Args:
        service: the Tag Manager service object.
        workspace: the workspace to create a tag within.
        tracking_id: the Universal Analytics tracking ID to use.

    Returns:
        The created tag.
    """

    path = f"accounts/{accountId}/containers/{containerId}/workspaces/{workspaceId}"

    try:
        return (
            service.accounts()
            .containers()
            .workspaces()
            .tags()
            .create(parent=path, body=tag_body)
            .execute()
        )

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def list_tags(service, accountId, containerId, workspaceId):
    """Create the Universal Analytics Hello World Tag.

    Args:
        service: the Tag Manager service object.
        workspace: the workspace to create a tag within.
        tracking_id: the Universal Analytics tracking ID to use.

    Returns:
        The created tag.
    """

    path = f"accounts/{accountId}/containers/{containerId}/workspaces/{workspaceId}"

    try:
        tags = (
            service.accounts()
            .containers()
            .workspaces()
            .tags()
            .list(parent=path)
            .execute()
        )
        for tag in tags.get("tag", []):
            print(f'Tag Name: {tag.get("name")}')

        return tags

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def main():
    AWANI_PROPERTY_ID = "UA-28458950-9"
    AWANI_WEBSITE_COMBINED_ID = "86412644"
    DPAPAMON_LINKID = "86412644:102876439727337172502"
    TAGMANAGER_ACCOUNT_ID = "123287"
    ACM_CONTAINER_NAME = "01 - Astro - astro.com.my"
    ACM_CONTAINER_ID = "139462"
    WORKSPACE_ID = "478"

    ga_service = init_service("analytics", "v3", GA_SCOPES, GA_KEY_FILE_LOCATION)
    tm_service = init_service("tagmanager", "v2", TM_SCOPES, GA_KEY_FILE_LOCATION)

    # # List Account Users
    # account_users = list_account_users(service, ACCOUNT_ID)

    # # Add user
    # add_user_to_view(
    #     ga_service,
    #     ACCOUNT_ID,
    #     AWANI_PROPERTY_ID,
    #     AWANI_WEBSITE_COMBINED_ID,
    #     {"permissions": ["READ_AND_ANALYZE"], "email": "dpapamon@gmail.com"},
    # )

    # # Delete user
    # delete_user_from_view(
    #     ga_service,
    #     ACCOUNT_ID,
    #     AWANI_PROPERTY_ID,
    #     AWANI_WEBSITE_COMBINED_ID,
    #     DPAPAMON_LINKID,
    # )

    # # List View Users for AWANI
    # view_users = list_view_users(
    #     ga_service, ACCOUNT_ID, AWANI_PROPERTY_ID, AWANI_WEBSITE_COMBINED_ID
    # )

    # # Get container
    # container = get_container(tm_service, TAGMANAGER_ACCOUNT_ID, ACM_CONTAINER_NAME)

    # # Create workspace
    # workspace = create_workspace(tm_service, container, "middleware test")

    # # Create the hello world tag.

    # tag_body = {
    #     "name": "Universal Analytics Hello World",
    #     "type": "ua",
    #     "parameter": [
    #         {"key": "trackingId", "type": "template", "value": AWANI_PROPERTY_ID}
    #     ],
    # }
    # create_tag(
    #     tm_service, TAGMANAGER_ACCOUNT_ID, ACM_CONTAINER_ID, WORKSPACE_ID, tag_body
    # )

    tags = list_tags(tm_service, TAGMANAGER_ACCOUNT_ID, ACM_CONTAINER_ID, WORKSPACE_ID)


if __name__ == "__main__":
    main()
