import sys
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from bson import ObjectId, DBRef, MinKey, MaxKey, Timestamp, Int64, Decimal128, Binary, Code, Regex

# REQUIRED: Set the name of the database you want to inspect
DB_TO_INSPECT = "database"

# REQUIRED: Set your MongoDB connection URI
MONGO_CONNECTION_URI = "mongodb+srv://ateeqdafi:Wua8kURcfkf0EAA4@cluster0.gnlrb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# Example with authentication: "mongodb://user:password@host:port/"
# Example with Atlas: "mongodb+srv://user:password@clustername.mongodb.net/"

# Optional: Set the number of documents to sample from each collection
# A larger number gives a more accurate schema but takes longer.
SCHEMA_SAMPLE_SIZE = 10

# Optional: Set to a specific collection name if you only want to analyze one
# Set to None to analyze all collections in the database
TARGET_COLLECTION_NAME = None # Example: "users" or "products"


# Type Mapping Helper
def get_bson_type_name(value):
    """Maps Python types to BSON type names for clarity."""
    if isinstance(value, str): return "string"
    if isinstance(value, bool): return "bool"
    if isinstance(value, int): return "int" # Could be Int32 or Int64 in BSON
    if isinstance(value, Int64): return "long"
    if isinstance(value, float): return "double"
    if isinstance(value, Decimal128): return "decimal"
    if isinstance(value, list): return "array"
    if isinstance(value, dict): return "object"
    if isinstance(value, ObjectId): return "objectId"
    if isinstance(value, DBRef): return "dbRef"
    if isinstance(value, Timestamp): return "timestamp"
    if isinstance(value, type(None)): return "null"
    if isinstance(value, MinKey): return "minKey"
    if isinstance(value, MaxKey): return "maxKey"
    if isinstance(value, Binary): return "binData"
    if isinstance(value, Code): return "javascript"
    if isinstance(value, Regex): return "regex"
    return type(value).__name__

# Schema Inference Logic
def infer_schema_recursive(obj):
    """Recursively infers the schema of a Python object (dict, list, or primitive)."""
    bson_type = get_bson_type_name(obj)

    if bson_type == "object":
        # It's a dictionary (nested document)
        nested_schema = {}
        for key, value in obj.items():
            nested_schema[key] = infer_schema_recursive(value)
        # Return the structure for merging
        return {"types": {bson_type}, "schema": nested_schema}

    elif bson_type == "array":
        # It's a list
        if not obj: # Empty list
            return {"types": {bson_type}, "element_schema": {"types": {"empty_array"}}} # Mark as empty

        # Infer schema for elements by merging schemas of all items
        merged_element_schema = None
        for item in obj:
            item_schema_info = infer_schema_recursive(item)
            if merged_element_schema is None:
                merged_element_schema = item_schema_info
            else:
                merged_element_schema = merge_schema_info(merged_element_schema, item_schema_info)

        return {"types": {bson_type}, "element_schema": merged_element_schema}

    else:
        # Primitive type
        return {"types": {bson_type}}

def merge_schema_info(existing_info, new_info):
    """Merges two schema information dictionaries."""
    if not existing_info: return new_info
    if not new_info: return existing_info

    merged_info = existing_info.copy()

    # Merge types
    merged_info["types"].update(new_info.get("types", set()))

    # Merge nested schemas ('schema' for objects)
    if "schema" in new_info:
        if "schema" not in merged_info:
            merged_info["schema"] = new_info["schema"]
        else:
            # Recursively merge nested schemas
            schema1 = merged_info["schema"]
            schema2 = new_info["schema"]
            merged_nested_schema = schema1.copy()
            for key, value2 in schema2.items():
                if key not in merged_nested_schema:
                    merged_nested_schema[key] = value2
                else:
                    # Key exists in both, merge recursively
                    merged_nested_schema[key] = merge_schema_info(merged_nested_schema[key], value2)
            merged_info["schema"] = merged_nested_schema

    # Merge array element schemas ('element_schema' for arrays)
    if "element_schema" in new_info:
        if "element_schema" not in merged_info:
            # If existing didn't think it was an array, but new one does, types should reflect that
             merged_info["element_schema"] = new_info["element_schema"]
        else:
            # Both have element schemas, merge them
            merged_info["element_schema"] = merge_schema_info(
                merged_info["element_schema"],
                new_info["element_schema"]
            )
            # Handle case where one list was empty initially
            if "empty_array" in merged_info["element_schema"]["types"] and len(merged_info["element_schema"]["types"]) > 1:
                 merged_info["element_schema"]["types"].discard("empty_array")

    return merged_info


def get_collection_schema(collection, sample_size):
    """Infers the schema of a MongoDB collection by sampling documents."""
    print(f"  Sampling up to {sample_size} documents from '{collection.name}'...")
    try:
        # Use find().limit() for simplicity and predictability
        documents = list(collection.find(limit=sample_size))

        if not documents:
            print("  Collection is empty or no documents found in sample.")
            return None

    except OperationFailure as e:
        print(f"  Error sampling collection '{collection.name}': {e}")
        return None
    except Exception as e:
        print(f"  Unexpected error accessing collection '{collection.name}': {e}")
        return None


    merged_collection_schema = {}
    doc_count = 0
    for doc in documents:
        doc_count += 1
        # Remove _id for potentially cleaner schema, keep if you prefer
        # doc.pop('_id', None)
        doc_schema_info = infer_schema_recursive(doc) # Expecting {"types": {"object"}, "schema": {...}}

        # The top level is always an object, merge its inner schema
        if "schema" in doc_schema_info:
             doc_inner_schema = doc_schema_info["schema"]
             temp_merged = merged_collection_schema.copy() # Start with current merged state
             for key, value_info in doc_inner_schema.items():
                 if key not in temp_merged:
                     temp_merged[key] = value_info
                 else:
                     temp_merged[key] = merge_schema_info(temp_merged[key], value_info)
             merged_collection_schema = temp_merged

    print(f"  Analyzed {doc_count} documents.")
    return merged_collection_schema

# Main Schema Generation Function
def generate_db_schema(db_name, mongo_uri, sample_size, target_collection_name=None):
    """Connects to MongoDB, analyzes collections, and returns the inferred schema."""

    print(f"Connecting to MongoDB at {mongo_uri}...")
    client = None # Initialize client to None for finally block
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print("Connection successful.")
    except ConnectionFailure as e:
        print(f"Error: Could not connect to MongoDB at {mongo_uri}", file=sys.stderr)
        print(e, file=sys.stderr)
        return None # Return None on connection failure
    except Exception as e:
         print(f"An unexpected error occurred during connection: {e}", file=sys.stderr)
         return None

    try:
        db = client[db_name]
        print(f"Inspecting database: '{db_name}'")

        collections_to_inspect = []
        if target_collection_name:
            # Check if the specific collection exists
            if target_collection_name not in db.list_collection_names():
                 print(f"Error: Collection '{target_collection_name}' not found in database '{db_name}'.", file=sys.stderr)
                 return None # Return None if specific collection not found
            collections_to_inspect = [db[target_collection_name]]
            print(f"Targeting specific collection: '{target_collection_name}'")
        else:
            # Get all collections
            collection_names = db.list_collection_names()
            if not collection_names:
                 print("Database contains no collections.")
                 return {} # Return empty dict if no collections
            collections_to_inspect = [db[name] for name in collection_names]
            print(f"Found collections: {', '.join(collection_names)}")

        # Perform Schema Inference
        database_schema = {}
        for collection in collections_to_inspect:
            print("-" * 40)
            print(f"Analyzing collection: '{collection.name}'")
            collection_schema = get_collection_schema(collection, sample_size)
            if collection_schema is not None: # Only add if schema inference was successful
                database_schema[collection.name] = collection_schema

        return database_schema # Return the final schema dictionary

    except OperationFailure as e:
         print(f"\nAn error occurred during database operations: {e}", file=sys.stderr)
         if "Authentication failed" in str(e):
             print("Please check your MongoDB connection URI and credentials.", file=sys.stderr)
         return None # Return None on operation failure
    except Exception as e:
        print(f"\nAn unexpected error occurred during schema generation: {e}", file=sys.stderr)
        return None # Return None on other errors
    finally:
        if client:
            print("\nClosing MongoDB connection.")
            client.close()