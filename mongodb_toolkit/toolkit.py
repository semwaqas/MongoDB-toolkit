# mongodb_toolkit/toolkit.py
import sys
from functools import lru_cache
from typing import List, Dict, Optional, Any, Tuple

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure, ConfigurationError
from langchain.tools import Tool

from .models import GetSchemaInput, ValidateSyntaxInput, SortItem, ExecuteQueryInput
from .utils import generate_collection_schema, validate_query_syntax_recursive
from .exceptions import ConfigurationError, SchemaError, ValidationError, ExecutionError

class MongoToolkit:
    """
    A toolkit for interacting with a specific MongoDB database, providing
    LangChain tools for schema discovery, query syntax validation, and execution.

    Instantiate this class with your MongoDB connection URI and database name.
    Then, use the get_tools() method to retrieve configured LangChain tools.
    """
    _client: Optional[MongoClient] = None
    _db: Optional[Database] = None

    def __init__(self, mongo_uri: str, db_name: str):
        """
        Initializes the toolkit with connection details.

        Args:
            mongo_uri (str): The MongoDB connection URI (e.g., "mongodb://...", "mongodb+srv://...").
                             Should be loaded securely (e.g., from environment variables).
            db_name (str): The name of the target database.
        """
        if not mongo_uri:
            raise ConfigurationError("mongo_uri cannot be empty.")
        if not db_name:
            raise ConfigurationError("db_name cannot be empty.")

        self.mongo_uri = mongo_uri
        self.db_name = db_name
        print(f"MongoToolkit initialized for database '{self.db_name}'. Connection will be established on first use.")

    def _get_db(self) -> Database:
        """Establishes connection (if needed) and returns the Database object."""
        if self._client is None or self._db is None:
            print(f"Establishing new MongoDB connection to database '{self.db_name}'...")
            try:
                self._client = MongoClient(
                    self.mongo_uri,
                    serverSelectionTimeoutMS=5000 # Timeout for connection attempt
                )
                # Test connection
                self._client.admin.command('ping') # 'ping' is lightweight
                self._db = self._client[self.db_name]
                print("MongoDB connection successful.")
            except ConfigurationError as e:
                self._client = None
                self._db = None
                print(f"Error: Invalid MongoDB URI configuration: {e}", file=sys.stderr)
                raise ConfigurationError(f"Invalid MongoDB URI configuration: {e}") from e
            except ConnectionFailure as e:
                self._client = None
                self._db = None
                print(f"Error: Could not connect to MongoDB server at {self.mongo_uri}. Details: {e}", file=sys.stderr)
                raise ConfigurationError(f"Could not connect to MongoDB: {e}") from e
            except Exception as e: # Catch other potential errors during init
                self._client = None
                self._db = None
                print(f"Error: An unexpected error occurred during MongoDB connection: {e}", file=sys.stderr)
                raise ConfigurationError(f"Unexpected error connecting to MongoDB: {e}") from e

        # Type checking reassurance
        if self._db is None:
             raise RuntimeError("Database object (_db) is unexpectedly None after connection attempt.") # Should not happen
        return self._db

    def close(self):
        """Closes the MongoDB client connection, if open."""
        if self._client:
            print("Closing MongoDB connection.")
            self._client.close()
            self._client = None
            self._db = None

    def get_db_schema(
        self,
        target_collection_name: Optional[str] = None,
        sample_size: int = 100
    ) -> Dict[str, Any]:
        """
        Generates and returns the inferred schema for collections within the database.
        """
        print(f"Getting schema for database: '{self.db_name}'")
        db = self._get_db()
        database_schema = {}

        try:
            if target_collection_name:
                if target_collection_name not in db.list_collection_names():
                    raise SchemaError(f"Collection '{target_collection_name}' not found in database '{self.db_name}'.")
                collections_to_inspect = [db[target_collection_name]]
                print(f"Targeting specific collection: '{target_collection_name}'")
            else:
                collection_names = db.list_collection_names()
                if not collection_names:
                    print("Database contains no collections.")
                    return {}
                collections_to_inspect = [db[name] for name in collection_names]
                print(f"Found collections: {', '.join(collection_names)}")

            for collection in collections_to_inspect:
                print("-" * 20)
                print(f"Analyzing collection: '{collection.name}'")
                collection_schema = generate_collection_schema(collection, sample_size)
                if collection_schema is not None:
                    database_schema[collection.name] = collection_schema

            return database_schema

        except OperationFailure as e:
            msg = f"MongoDB operation failed during schema generation: {e}"
            print(msg, file=sys.stderr)
            raise SchemaError(msg) from e
        except Exception as e:
            msg = f"An unexpected error occurred during schema generation: {e}"
            print(msg, file=sys.stderr)
            # Log the full traceback here if possible
            raise SchemaError(msg) from e


    def validate_mongodb_query_syntax(self, query_doc: Dict[str, Any]) -> str:
        """
        Validates the basic syntax of a MongoDB query filter document.

        Returns:
            str: "Syntax is valid." or a string listing the syntax errors found.
        """
        errors = []
        if not isinstance(query_doc, dict):
            return "Validation Error: Query root must be a dictionary."

        validate_query_syntax_recursive(query_doc, errors, path_prefix="")

        if not errors:
            return "Syntax is valid."
        else:
            error_string = "Syntax validation errors found:\n" + "\n".join(f"- {e}" for e in errors)
            # Raise error instead of returning string? Could be better for agent flow.
            # raise ValidationError(error_string)
            # For now, return string as per common tool patterns
            return error_string

    def execute_mongodb_query(
        self,
        collection_name: str,
        query_filter: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        limit: int = 0,
        skip: int = 0,
        sort: Optional[List[Dict[str, Any]]] = None # Receives list of dicts from SortItem model
    ) -> List[Dict[str, Any]]:
        """
        Executes a MongoDB find query against a specified collection.
        """
        if not collection_name:
            raise ExecutionError("collection_name cannot be empty.")
        if not isinstance(query_filter, dict):
            # Basic check, syntax validation tool should catch more
            raise ExecutionError("query_filter must be a dictionary.")
        if limit < 0: raise ValueError("limit cannot be negative.")
        if skip < 0: raise ValueError("skip cannot be negative.")

        db = self._get_db()
        try:
            collection = db[collection_name]
        except Exception as e:
            raise ExecutionError(f"Failed to get collection '{collection_name}': {e}") from e

        print(f"Executing find on {self.db_name}.{collection_name}")
        print(f"  Filter: {query_filter}")
        # (Add optional print statements for projection, limit, skip, sort)

        processed_sort: Optional[List[Tuple[str, int]]] = None
        if sort:
            processed_sort = []
            try:
                for item in sort:
                    field = item['field']
                    direction = item['direction']
                    if direction not in [ASCENDING, DESCENDING]:
                        raise ValueError(f"Invalid sort direction {direction}")
                    processed_sort.append((field, direction))
                print(f"  Sort: {processed_sort}")
            except (KeyError, ValueError, TypeError) as e:
                raise ExecutionError(f"Invalid sort format provided: {sort}. Error: {e}") from e

        try:
            cursor = collection.find(query_filter, projection if projection else None)
            if processed_sort:
                cursor = cursor.sort(processed_sort)
            if skip > 0:
                cursor = cursor.skip(skip)
            if limit > 0: # Apply limit if greater than 0
                cursor = cursor.limit(limit)
            elif limit == 0:
                # If limit is explicitly 0, pymongo defaults to no limit, which is fine.
                # Some APIs might expect a large number for "all", but 0 is standard no-limit here.
                 print("  Limit: No limit (0)")


            results = list(cursor) # Execute query
            print(f"Query executed. Found {len(results)} documents.")
            return results

        except OperationFailure as e:
            msg = f"MongoDB operation failed during query execution: {e}"
            print(msg, file=sys.stderr)
            raise ExecutionError(msg) from e
        except Exception as e:
            msg = f"An unexpected error occurred during query execution: {e}"
            print(msg, file=sys.stderr)
            raise ExecutionError(msg) from e

    @lru_cache(maxsize=1) # Cache the tools list once generated
    def get_tools(self) -> List[Tool]:
        """
        Returns a list of configured LangChain tools bound to this toolkit instance.
        """
        print("Generating LangChain tools for MongoToolkit...")
        schema_tool = Tool.from_function(
            name="get_mongodb_database_schema",
            description=f"Use this tool to get the schema of collections within the '{self.db_name}' MongoDB database. Provide an optional 'target_collection_name' to get schema for only one collection, and 'sample_size' to control accuracy vs speed.",
            func=self.get_db_schema, # Bound method
            args_schema=GetSchemaInput
        )

        validate_tool = Tool.from_function(
            name="validate_mongodb_query_syntax",
            description="Use this tool to validate the basic syntax of a MongoDB query filter document (dictionary) before execution. Checks for valid operators and structure. Input is the 'query_doc'. Returns 'Syntax is valid.' or lists errors.",
            func=self.validate_mongodb_query_syntax, # Bound method
            args_schema=ValidateSyntaxInput
        )

        execute_tool = Tool.from_function(
            name="execute_mongodb_find_query",
            description=f"Use this tool to execute a MongoDB 'find' query against a specific collection in the '{self.db_name}' database after validating its syntax. Provide 'collection_name' and 'query_filter'. Optionally provide 'projection', 'limit', 'skip', or 'sort'. Returns a list of matching documents.",
            func=self.execute_mongodb_query, # Bound method
            args_schema=ExecuteQueryInput
        )

        return [schema_tool, validate_tool, execute_tool]