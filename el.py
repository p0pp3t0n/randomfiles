from elasticsearch import (
    Elasticsearch,
    NotFoundError,
    ConnectionError,
    AuthenticationException,
)
from tabulate import tabulate
import sys

try:
    # Connect to Elasticsearch cluster
    es = Elasticsearch(
        hosts=["http://localhost:9200"],  # Update with your cluster URL
        http_auth=("username", "password"),  # Omit if not required
    )

    # Check if the connection is successful
    if not es.ping():
        raise ConnectionError("Failed to connect to Elasticsearch cluster.")

    # Search for data views in the .kibana index
    try:
        response = es.search(
            index=".kibana",
            body={"query": {"term": {"type.keyword": {"value": "index-pattern"}}}},
        )
    except NotFoundError:
        print("Index '.kibana' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred while searching: {e}")
        sys.exit(1)

    # Process and format results
    data_views = []
    for hit in response["hits"]["hits"]:
        try:
            attributes = hit["_source"]["attributes"]
            data_views.append(
                {
                    "ID": hit["_id"],
                    "Title": attributes["title"],
                    "Time Field": attributes.get("timeFieldName", "N/A"),
                    "Field Count": len(attributes.get("fields", [])),
                }
            )
        except KeyError as e:
            print(f"Missing expected key in document: {e}")
            continue

    # Print table
    print(tabulate(data_views, headers="keys", tablefmt="grid"))

except ConnectionError as e:
    print(f"Connection error: {e}")
    sys.exit(1)
except AuthenticationException as e:
    print(f"Authentication failed: {e}")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    sys.exit(1)
