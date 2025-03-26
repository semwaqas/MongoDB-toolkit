import sys
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure, ConfigurationError
from typing import List, Dict, Optional, Any, Tuple

def execute_mongodb_query(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    query_filter: Dict[str, Any],
    projection: Optional[Dict[str, Any]] = None,
    limit: int = 0, # 0 means no limit
    skip: int = 0,
    sort: Optional[List[Tuple[str, int]]] = None # e.g., [('field1', ASCENDING), ('field2', DESCENDING)]
) -> List[Dict[str, Any]]:
    """
    Executes a MongoDB find query against a specified collection.

    Args:
        mongo_uri (str): The MongoDB connection URI.
        db_name (str): The name of the database.
        collection_name (str): The name of the collection.
        query_filter (Dict[str, Any]): The filter document for the find query.
        projection (Optional[Dict[str, Any]], optional): The projection document
            to specify which fields to include or exclude. Defaults to None (all fields).
            Example: {'_id': 0, 'name': 1, 'email': 1}
        limit (int, optional): The maximum number of documents to return.
            Defaults to 0 (no limit).
        skip (int, optional): The number of documents to skip before returning results.
            Defaults to 0.
        sort (Optional[List[Tuple[str, int]]], optional): A list of (key, direction) pairs
            to sort the results. Direction should be pymongo.ASCENDING (1) or
            pymongo.DESCENDING (-1). Defaults to None (no sorting).
            Example: [('age', DESCENDING), ('name', ASCENDING)]

    Returns:
        List[Dict[str, Any]]: A list of documents matching the query. Returns an empty
                              list if no documents match or if an error occurs during
                              connection or query execution.

    Raises:
        ConnectionFailure: If unable to connect to the MongoDB server.
        OperationFailure: If the database operation fails (e.g., authentication).
        ConfigurationError: If the URI is invalid.
        Exception: For other unexpected errors during execution.
        TypeError: If input types are incorrect (e.g., query_filter not a dict).
        ValueError: If limit or skip are negative.

    """
    if not isinstance(query_filter, dict):
        raise TypeError("query_filter must be a dictionary.")
    if limit < 0:
        raise ValueError("limit cannot be negative.")
    if skip < 0:
        raise ValueError("skip cannot be negative.")

    client: Optional[MongoClient] = None
    results: List[Dict[str, Any]] = []

    print(f"Attempting to connect to MongoDB at {mongo_uri}...")
    try:
        # Connect to MongoDB - added timeout for robustness
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)

        # The ismaster command is cheap and does not require auth.
        # Checks if the server is available.
        client.admin.command('ismaster')
        print("Connection successful.")

        db = client[db_name]
        collection = db[collection_name]
        print(f"Executing find on {db_name}.{collection_name}...")
        print(f"  Filter: {query_filter}")
        if projection:
            print(f"  Projection: {projection}")
        if limit > 0:
            print(f"  Limit: {limit}")
        if skip > 0:
            print(f"  Skip: {skip}")
        if sort:
            print(f"  Sort: {sort}")

        # Build the find command dynamically
        cursor = collection.find(query_filter, projection if projection else None)

        if sort:
            cursor = cursor.sort(sort)
        if skip > 0:
            cursor = cursor.skip(skip)
        if limit > 0:
            cursor = cursor.limit(limit)

        # Execute the query and retrieve results
        # Converting to list executes the query immediately
        results = list(cursor)
        print(f"Query executed. Found {len(results)} documents.")

    except ConnectionFailure as e:
        print(f"Error: Could not connect to MongoDB server at {mongo_uri}.", file=sys.stderr)
        print(f"  Details: {e}", file=sys.stderr)
        raise # Re-raise the exception for the caller to handle

    except OperationFailure as e:
        print(f"Error: MongoDB operation failed.", file=sys.stderr)
        print(f"  Details: {e}", file=sys.stderr)
        # Check for common issues like authentication
        if "Authentication failed" in str(e):
            print("  Hint: Check your username/password in the connection URI.", file=sys.stderr)
        raise # Re-raise

    except ConfigurationError as e:
         print(f"Error: Invalid MongoDB URI configuration.", file=sys.stderr)
         print(f"  Details: {e}", file=sys.stderr)
         raise # Re-raise

    except Exception as e:
        # Catch any other unexpected errors during query execution
        print(f"Error: An unexpected error occurred during query execution.", file=sys.stderr)
        print(f"  Details: {e}", file=sys.stderr)
        raise # Re-raise

    finally:
        # Ensure the client connection is closed
        if client:
            print("Closing MongoDB connection.")
            client.close()

    return results