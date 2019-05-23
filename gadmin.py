"""
GA Admin for batch processing via Google Analytics API

Features
--------
Bulk add users
Bulk delete users
Bulk add GTM tags with scheduled duration
"""

import time
import multiprocessing
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest
from datetime import datetime


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

        for accountUserLink in account_links.get("items", []):
            userRef = accountUserLink.get("userRef", {})
            print(f'User Email: {userRef.get("email")}')

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

        return profile_links

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def batch_delete_users(service, accountId, users, views=None):
    """ Batch delete users

    Parameters:
        users (list): list of user_email
    """

    def delete_user_from_property(property, account_id, batch):
        property_id = property.get("id")

        for view in property.get("profiles", []):
            view_id = view.get("id")
            links = (
                service.management()
                .profileUserLinks()
                .list(
                    accountId=account_id, webPropertyId=property_id, profileId=view_id
                )
                .execute()
            )
            links = [
                x.get("id")
                for x in links.get("items", [])
                if x.get("userRef", {})["email"] == user
            ]
            print(f"Property: {property_id}, View: {view_id}, Links: {links}")
            for link in links:
                batch.add(
                    service.management()
                    .profileUserLinks()
                    .delete(
                        accountId=account_id,
                        webPropertyId=property_id,
                        profileId=view_id,
                        linkId=link,
                    )
                )

    def handle_delete_user(requestId, response, exception):
        if exception is not None:
            print(f"There was an error: {exception}")
        else:
            print(f"Request ID: {requestId}, Deleted user: {response}")

    start_time = time.time()

    # Get the a full set of account summaries.
    account_summaries = service.management().accountSummaries().list().execute()

    # Loop through each account.
    for account in account_summaries.get("items", []):
        account_id = account.get("id")

        for user in users:
            processes = []
            batch = BatchHttpRequest(callback=handle_delete_user)

            for property_summary in account.get("webProperties", []):
                p = multiprocessing.Process(
                    target=delete_user_from_property,
                    args=(property_summary, account_id, batch),
                )
                processes.append(p)
                p.start()

            for process in processes:
                process.join()
            batch.execute()

    print(f"That took {(time.time() - start_time) / 60} min")


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


def batch_add_users(service, accountId, users):
    """ Batch add users

    Parameters:
        users (tuple): (user_email, list of requests)
    """

    def handle_create_user(requestId, response, exception):
        if exception is not None:
            print(f"There was an error: {exception}")
        else:
            print(f"Request ID: {requestId}, Created user: {response}")

    batch = BatchHttpRequest(callback=handle_create_user)

    for user in users:
        user_email, requests = user
        for request in requests:
            webPropertyId, profileId, permissions = request
            batch.add(
                service.management()
                .profileUserLinks()
                .insert(
                    accountId=accountId,
                    webPropertyId=webPropertyId,
                    profileId=profileId,
                    body={
                        "permissions": {"local": permissions},
                        "userRef": {"email": user_email},
                    },
                )
            )

    batch.execute()


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


def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


def batch_delete_tags(service, accountId, containerId, workspaceId, tags, LIMIT=1000):
    """ Batch create tags.

    Parameters:
        tags (list): list of tag id (limited to 1000 requests).
    """

    def handle_delete_tag(requestId, response, exception):
        if exception is not None:
            print(f"There was an error: {exception}")

    if len(tags) > LIMIT:
        print(f"Number of requests exceeded > {LIMIT}")

    path = f"accounts/{accountId}/containers/{containerId}/workspaces/{workspaceId}"
    batch = service.new_batch_http_request(callback=handle_delete_tag)

    for tag in tags:
        path = f"accounts/{accountId}/containers/{containerId}/workspaces/{workspaceId}/tags/{tag}"
        batch.add(service.accounts().containers().workspaces().tags().delete(path=path))
    batch.execute()


def batch_create_tags(service, accountId, containerId, workspaceId, tags, LIMIT=1000):
    """ Batch create tags.

    Parameters:
        tags (list): list of tag config (limited to 1000 requests).
    """

    def handle_create_tag(requestId, response, exception):
        if exception is not None:
            print(f"There was an error: {exception}")
        else:
            print(f"Request ID: {requestId}, Created tag: {response}")

    if len(tags) > LIMIT:
        print(f"Number of requests exceeded > {LIMIT}")

    path = f"accounts/{accountId}/containers/{containerId}/workspaces/{workspaceId}"
    batch = service.new_batch_http_request(callback=handle_create_tag)

    for tag in tags:
        tag_body, startDate, endDate = tag
        tag_body["scheduleStartMs"] = unix_time_millis(
            datetime.strptime(startDate, "%d%m%Y")
        )
        tag_body["scheduleEndMs"] = unix_time_millis(
            datetime.strptime(endDate, "%d%m%Y")
        )
        batch.add(
            service.accounts()
            .containers()
            .workspaces()
            .tags()
            .create(parent=path, body=tag_body)
        )
    batch.execute()


def create_tag(
    service, accountId, containerId, workspaceId, tag_body, startDate, endDate
):
    """ Create Google Tag.

    Args:
        service: the Tag Manager service object.
        workspace: the workspace to create a tag within.
        tracking_id: the Universal Analytics tracking ID to use.

    Returns:
        The created tag.
    """

    path = f"accounts/{accountId}/containers/{containerId}/workspaces/{workspaceId}"
    tag_body["scheduleStartMs"] = unix_time_millis(
        datetime.strptime(startDate, "%d%m%Y")
    )
    tag_body["scheduleEndMs"] = unix_time_millis(datetime.strptime(endDate, "%d%m%Y"))

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


def delete_tag(service, accountId, containerId, workspaceId, tagId):
    """Delete a tag.
    """

    path = f"accounts/{accountId}/containers/{containerId}/workspaces/{workspaceId}/tags/{tagId}"

    try:
        service.accounts().containers().workspaces().tags().delete(path=path).execute()

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
            print(tag)

        return tags

    except TypeError as error:
        # Handle errors in constructing a query.
        print(f"There was an error in constructing your query : {error}")

    except HttpError as error:
        # Handle API errors.
        print(f"There was an API error : {error.resp.status} : {error.resp.reason}")


def list_triggers(service, accountId, containerId, workspaceId):
    """ List all triggers for the container

    Returns:
        triggers (list): list of triggers
    """

    path = f"accounts/{accountId}/containers/{containerId}/workspaces/{workspaceId}"

    try:
        triggers = (
            service.accounts()
            .containers()
            .workspaces()
            .triggers()
            .list(parent=path)
            .execute()
        )
        for trigger in triggers.get("trigger", []):
            print(f'Trigger Name: {trigger.get("name")}')
            print(trigger)

        return triggers

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
    WORKSPACE_ID = "478"  # Default workspace id

    ga_service = init_service("analytics", "v3", GA_SCOPES, GA_KEY_FILE_LOCATION)
    tm_service = init_service("tagmanager", "v2", TM_SCOPES, GA_KEY_FILE_LOCATION)

    # # Add user
    # add_user_to_view(
    #     ga_service,
    #     ACCOUNT_ID,
    #     AWANI_PROPERTY_ID,
    #     AWANI_WEBSITE_COMBINED_ID,
    #     {"permissions": ["READ_AND_ANALYZE"], "email": "dpapamon@gmail.com"},
    # )

    # # Batch add user

    # batch_add_users(
    #     ga_service,
    #     ACCOUNT_ID,
    #     [
    #         (
    #             "sjtsoonj@astro.com.my",
    #             [(AWANI_PROPERTY_ID, AWANI_WEBSITE_COMBINED_ID, ["READ_AND_ANALYZE"])],
    #         ),
    #         (
    #             "jinified@gmail.com",
    #             [(AWANI_PROPERTY_ID, AWANI_WEBSITE_COMBINED_ID, ["READ_AND_ANALYZE"])],
    #         ),
    #     ],
    # )

    # # List Account Users
    # account_users = list_account_users(ga_service, ACCOUNT_ID)

    # # Delete user
    # delete_user_from_view(
    #     ga_service,
    #     ACCOUNT_ID,
    #     AWANI_PROPERTY_ID,
    #     AWANI_WEBSITE_COMBINED_ID,
    #     DPAPAMON_LINKID,
    # )

    # # Batch delete users

    # batch_delete_users(ga_service, ACCOUNT_ID, ["jinified@gmail.com"])

    # # List View Users for AWANI
    # view_users = list_view_users(
    #     ga_service, ACCOUNT_ID, AWANI_PROPERTY_ID, AWANI_WEBSITE_COMBINED_ID
    # )

    # for profileUserLink in view_users.get("items", []):
    #     userRef = profileUserLink.get("userRef", {})
    #     permissions = profileUserLink.get("permissions", {})

    #     print(
    #         f'Email: {userRef.get("email")}, LinkId: {profileUserLink.get("id")}, Permissions: {permissions.get("effective")}'
    #     )

    # # Get container
    # container = get_container(tm_service, TAGMANAGER_ACCOUNT_ID, ACM_CONTAINER_NAME)

    # # Create workspace
    # workspace = create_workspace(tm_service, container, "middleware test")

    # # Create the hello world tag.

    tag_body = {
        "name": "Test Tag 1",
        "type": "ua",
        "parameter": [
            {"key": "trackingId", "type": "template", "value": AWANI_PROPERTY_ID}
        ],
    }

    tag_body2 = {
        "name": "Test Tag 2",
        "type": "ua",
        "parameter": [
            {"key": "trackingId", "type": "template", "value": AWANI_PROPERTY_ID}
        ],
    }

    # # Batch creation of tags

    # batch_create_tags(
    #     tm_service,
    #     TAGMANAGER_ACCOUNT_ID,
    #     ACM_CONTAINER_ID,
    #     WORKSPACE_ID,
    #     [(tag_body, "23052019", "26052019"), (tag_body2, "26052019", "28052019")],
    # )

    # create_tag(
    #     tm_service,
    #     TAGMANAGER_ACCOUNT_ID,
    #     ACM_CONTAINER_ID,
    #     WORKSPACE_ID,
    #     tag_body,
    #     "21052019",
    #     "25052019",
    # )

    # # Delete a tag
    # delete_tag(tm_service, TAGMANAGER_ACCOUNT_ID, ACM_CONTAINER_ID, WORKSPACE_ID, 493)

    # # Batch delete tags
    # batch_delete_tags(
    #     tm_service, TAGMANAGER_ACCOUNT_ID, ACM_CONTAINER_ID, WORKSPACE_ID, [495, 496]
    # )

    # # List all tags
    # tags = list_tags(tm_service, TAGMANAGER_ACCOUNT_ID, ACM_CONTAINER_ID, WORKSPACE_ID)

    # # List all triggers
    # triggers = list_triggers(
    #     tm_service, TAGMANAGER_ACCOUNT_ID, ACM_CONTAINER_ID, WORKSPACE_ID
    # )


if __name__ == "__main__":
    main()
