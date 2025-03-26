import os
from functools import partial
from langchain.tools import Tool
from pydantic.v1 import BaseModel, Field # Use Pydantic for typed inputs
from typing import List, Dict, Optional, Any, Tuple

# Import your core functions
from .get_schema import generate_db_schema
from .validate_query_syntax import validate_mongodb_query_syntax
from .validate_query_schema import validate_query
from .execute_query import execute_mongodb_query, ASCENDING, DESCENDING # Import constants if needed

# Example using environment variables
MONGO_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
DB_NAME = os.environ.get("MONGODB_DB_NAME", "testdb") # Default to 'testdb' if not set

print(f"Using MongoDB URI: {'***' if 'password@' in MONGO_URI else MONGO_URI}") # Basic masking
print(f"Using Database Name: {DB_NAME}")

# --- Define Input Schemas using Pydantic (Strongly Recommended) ---

class GetSchemaInput(BaseModel):
    target_collection_name: Optional[str] = Field(None, description="Optional: Name of a specific collection to get the schema for. If None, schemas for all collections are returned.")
    sample_size: int = Field(10, description="Number of documents to sample for schema inference.")

class ValidateSyntaxInput(BaseModel):
    query_doc: Dict[str, Any] = Field(..., description="The MongoDB query filter document (as a dictionary) to validate.")

class ValidateQueryInput(BaseModel):
    query_doc: Dict[str, Any] = Field(..., description="The MongoDB query filter document (as a dictionary) to validate.")
    expected_schema: Dict[str, Any] = Field(..., description="The expected schema dictionary (usually obtained from get_schema tool).")

class ExecuteQueryInput(BaseModel):
    collection_name: str = Field(..., description="The name of the collection to query.")
    query_filter: Dict[str, Any] = Field(..., description="The filter document for the find query.")
    projection: Optional[Dict[str, Any]] = Field(None, description="Optional: Projection document (e.g., {'_id': 0, 'name': 1}).")
    limit: int = Field(0, description="Optional: Maximum number of documents to return (0 for no limit).")
    skip: int = Field(0, description="Optional: Number of documents to skip.")
    # Represent sort as a list of lists/tuples for JSON compatibility if needed by LLM, then convert if necessary
    sort: Optional[List[Tuple[str, int]]] = Field(None, description="Optional: Sort specification (e.g., [['age', -1], ['name', 1]] for pymongo.DESCENDING, pymongo.ASCENDING).")


# --- Create Partial Functions with Configuration Baked In ---

# Note: We pass DB_NAME here as it's static config for this setup
get_schema_partial = partial(generate_db_schema, mongo_uri=MONGO_URI, db_name=DB_NAME)

# Validation functions don't usually need DB access, only the query/schema
# validate_syntax_partial = validate_mongodb_query_syntax # No config needed
# validate_schema_partial = validate_query # No config needed

# Execute query needs URI and DB Name
execute_query_partial = partial(execute_mongodb_query, mongo_uri=MONGO_URI, db_name=DB_NAME)


# --- Create LangChain Tools using Partials and Input Schemas ---

get_schema_tool = Tool.from_function(
    name='get_database_schema', # Renamed for clarity
    description=f"Generates and returns the inferred schema for collections within the '{DB_NAME}' MongoDB database. Useful for understanding data structure before creating queries.",
    func=get_schema_partial,
    args_schema=GetSchemaInput
)

validate_query_syntax_tool = Tool.from_function(
    name='validate_query_syntax',
    description='Validates the basic syntax of a MongoDB query filter document (dictionary). Checks for known operators and correct structure (e.g., $in expects an array). Does NOT check against actual data types or field existence.',
    func=validate_mongodb_query_syntax, # Direct function, no config needed
    args_schema=ValidateSyntaxInput
)

validate_query_tool = Tool.from_function(
    name='validate_query_against_schema', # Renamed for clarity
    description=f"Validates a MongoDB query filter document (dictionary) against a provided schema dictionary (typically from 'get_database_schema'). Checks for field existence, type compatibility, and operator usage based on the schema.",
    func=validate_query, # Direct function, no config needed
    args_schema=ValidateQueryInput
)

execute_query_tool = Tool.from_function(
    name='execute_find_query', # Renamed for clarity
    description=f"Executes a MongoDB 'find' query against a specified collection within the '{DB_NAME}' database. Returns a list of matching documents.",
    func=execute_query_partial,
    args_schema=ExecuteQueryInput
)

# Expose the tools
__all__ = [
    'get_schema_tool',
    'validate_query_syntax_tool',
    'validate_query_tool',
    'execute_query_tool',
    'MONGO_URI', # Optionally expose config if needed elsewhere, but usually not
    'DB_NAME'
]