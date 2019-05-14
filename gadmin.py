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


SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
KEY_FILE_LOCATION = "./secrets/client_secrets.json"
VIEW_ID = "86412644"
ACCOUNT_ID = "20278225"


def initialize_usermanagement():
    """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES
    )

    # Build the service object.
    analytics = build("analytics", "v3", credentials=credentials)

    return analytics


def get_report(analytics):
    """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
    return (
        analytics.reports()
        .batchGet(
            body={
                "reportRequests": [
                    {
                        "viewId": VIEW_ID,
                        "dateRanges": [{"startDate": "7daysAgo", "endDate": "today"}],
                        "metrics": [{"expression": "ga:sessions"}],
                        "dimensions": [{"name": "ga:country"}],
                    }
                ]
            }
        )
        .execute()
    )


def print_response(response):
    """Parses and prints the Analytics Reporting API V4 response.

  Args:
    response: An Analytics Reporting API V4 response.
  """
    for report in response.get("reports", []):
        columnHeader = report.get("columnHeader", {})
        dimensionHeaders = columnHeader.get("dimensions", [])
        metricHeaders = columnHeader.get("metricHeader", {}).get(
            "metricHeaderEntries", []
        )

        for row in report.get("data", {}).get("rows", []):
            dimensions = row.get("dimensions", [])
            dateRangeValues = row.get("metrics", [])

            for header, dimension in zip(dimensionHeaders, dimensions):
                print(header + ": " + dimension)

            for i, values in enumerate(dateRangeValues):
                print("Date range: " + str(i))
                for metricHeader, value in zip(metricHeaders, values.get("values")):
                    print(metricHeader.get("name") + ": " + value)


def get_first_profile_id(service):
    # Use the Analytics service object to get the first profile id.

    # Get a list of all Google Analytics accounts for this user
    accounts = service.management().accounts().list().execute()

    if accounts.get("items"):
        # Get the first Google Analytics account.
        account = accounts.get("items")[0].get("id")

        # Get a list of all the properties for the first account.
        properties = (
            service.management().webproperties().list(accountId=account).execute()
        )

        if properties.get("items"):
            # Get the first property id.
            property = properties.get("items")[0].get("id")

            # Get a list of all views (profiles) for the first property.
            profiles = (
                service.management()
                .profiles()
                .list(accountId=account, webPropertyId=property)
                .execute()
            )

            if profiles.get("items"):
                # return the first view (profile) id.
                return profiles.get("items")[0].get("id")

    return None


def main():
    service = initialize_usermanagement()
    response = get_first_profile_id(service)
    print(response)


if __name__ == "__main__":
    main()
