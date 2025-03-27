## API Reference

This section details the main class and the underlying logic used by the LangChain tools.

### `MongoToolkit` Class

This class encapsulates the connection and provides methods for interacting with the specified MongoDB database.

*   **`__init__(self, mongo_uri: str, db_name: str)`**
    *   **Purpose:** Initializes the toolkit. Requires the MongoDB connection URI and the target database name.
    *   **Parameters:**
        *   `mongo_uri` (str): The full MongoDB connection string (e.g., `mongodb://...`, `mongodb+srv://...`).
        *   `db_name` (str): The specific database name to interact with.
    *   **Behavior:** Stores configuration. Validates that inputs are not empty. Establishes the actual MongoDB connection *lazily* (only when a database operation is first requested via a tool) to avoid immediate connection overhead. Prints status messages during initialization and connection.
    *   **Raises:** `mongodb_toolkit.ConfigurationError` if `mongo_uri` or `db_name` is empty, or if the connection fails due to invalid URI, authentication issues, or network problems during the initial connection attempt.

*   **`get_tools(self) -> List[Union[Tool, StructuredTool]]`**
    *   **Purpose:** Generates and returns the list of configured LangChain tools ready to be used by an agent.
    *   **Behavior:** This method calls the internal wrappers (`_get_db_schema_wrapper`, `_execute_query_wrapper`) or direct methods (`validate_mongodb_query_syntax`) and binds them to `StructuredTool` or `Tool` instances. It uses `@lru_cache` to generate the tool list only once per `MongoToolkit` instance for efficiency. The descriptions provided to the tools guide the LLM on their usage.
    *   **Returns:** A list containing instances of `langchain.tools.StructuredTool` and `langchain.tools.Tool`.

*   **`close(self)`**
    *   **Purpose:** Explicitly closes the underlying `pymongo.MongoClient` connection to release database resources.
    *   **Behavior:** If a connection is active, it will be closed. It's crucial to call this method when the toolkit is no longer needed, typically in a `finally` block, to prevent resource leaks or idle connections.

### Core Logic Methods (Called by Tools)

These are the methods within `MongoToolkit` that perform the actual database operations. The LangChain tools primarily act as interfaces to these methods via wrappers.

1.  **`get_db_schema(self, target_collection_name: Optional[str] = None, sample_size: int = 10) -> Dict[str, Any]`**
    *   **(Called by `get_mongodb_database_schema` tool via `_get_db_schema_wrapper`)**
    *   **Purpose:** Infers and returns the schema of collections within the configured database.
    *   **Parameters:**
        *   `target_collection_name` (Optional[str]): If provided, only this collection's schema is generated. If `None`, schemas for all collections in the database are generated.
        *   `sample_size` (int): The number of documents to sample from each collection to infer the schema. Larger samples may yield more accurate schemas for collections with diverse documents but take longer.
    *   **Behavior:** Connects to the database (if not already connected). Lists collections. For each collection (or the target one), it samples documents using `collection.find().limit()` and analyzes their structure recursively to determine field names and encountered BSON data types. It merges schema information from different documents. Includes robust checks for handling `None` values or inconsistent structures during inference and merging.
    *   **Returns:** A dictionary where keys are collection names and values are their inferred schema dictionaries. The schema dictionary maps field names to information about their types (e.g., `{'fieldName': {'types': {'string', 'null'}}}`). Nested objects and arrays are represented recursively.
    *   **Raises:** `mongodb_toolkit.SchemaError` if a specified collection is not found, or if a database operation fails during sampling or listing collections. May also propagate `mongodb_toolkit.ConfigurationError` if the connection fails.

2.  **`validate_mongodb_query_syntax(self, query_doc: Dict[str, Any]) -> str`**
    *   **(Called directly by `validate_mongodb_query_syntax` tool)**
    *   **Purpose:** Performs a basic syntactic validation of a MongoDB query filter document *without* reference to any schema or actual data.
    *   **Parameters:**
        *   `query_doc` (Dict[str, Any]): The MongoDB query filter to validate (represented as a Python dictionary).
    *   **Behavior:** Recursively checks:
        *   If the overall structure is a dictionary.
        *   If keys starting with `$` are known MongoDB query operators (from `KNOWN_QUERY_OPERATORS` in `utils.py`).
        *   If the values provided to operators match the expected *structure* (e.g., `$in` expects a list, `$and`/`$or` expect a list of dictionaries, `$not` expects a dictionary or regex, `$exists` expects a boolean).
        *   If field names are valid (not empty, don't start with `$`).
        *   If operators and field names are not improperly mixed within the same dictionary level (e.g., `{'field': 1, '$gt': 0}`).
    *   **Returns:** The string `"Syntax is valid."` if no errors are found. Otherwise, returns a multi-line string detailing the specific syntax errors encountered and their location within the query document.
    *   **Raises:** `mongodb_toolkit.ValidationError` (indirectly, via the return string format currently) if the root input `query_doc` is not a dictionary.

3.  **`execute_mongodb_query(self, collection_name: str, query_filter: Dict[str, Any], projection: Optional[Dict[str, Any]] = None, limit: int = 0, skip: int = 0, sort: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]`**
    *   **(Called by `execute_mongodb_find_query` tool via `_execute_query_wrapper`)**
    *   **Purpose:** Executes a MongoDB `find` command against the specified collection using the provided parameters.
    *   **Parameters:**
        *   `collection_name` (str): The name of the target collection.
        *   `query_filter` (Dict[str, Any]): The filter criteria for the query.
        *   `projection` (Optional[Dict]): A dictionary specifying which fields to include (1) or exclude (0). If `None`, all fields are returned.
        *   `limit` (int): The maximum number of documents to return. If `0`, no limit is applied by `pymongo`. (Note: the tool wrapper defaults this to 10 if not provided by the LLM).
        *   `skip` (int): The number of documents to skip at the beginning of the result set.
        *   `sort` (Optional[List[Dict]]): A list of dictionaries specifying sort order (e.g., `[{'field': 'age', 'direction': -1}, {'field': 'name', 'direction': 1}]`). The method internally converts this to the `pymongo` tuple format `[('age', -1), ('name', 1)]`.
    *   **Behavior:** Connects to the database (if needed). Accesses the specified collection. Constructs and executes the `collection.find()` command incorporating the filter, projection, sort, skip, and limit parameters. It immediately converts the cursor result to a list.
    *   **Returns:** A list of dictionaries, where each dictionary represents a MongoDB document found by the query. Returns an empty list if no documents match.
    *   **Raises:**
        *   `mongodb_toolkit.ExecutionError` if the `collection_name` is invalid, if a MongoDB operation fails during execution (e.g., invalid operator usage caught by the server), or if the provided `sort` format is invalid.
        *   `ValueError` if `limit` or `skip` are negative.
        *   May also propagate `mongodb_toolkit.ConfigurationError` if the connection fails.

### Internal Wrappers (Implementation Detail)

*   **`_get_db_schema_wrapper(self, **kwargs)`**
    *   Called by the `get_mongodb_database_schema` `StructuredTool`.
    *   Validates `kwargs` against the `GetSchemaInput` Pydantic model.
    *   Calls `get_db_schema` with the validated, unpacked arguments.
*   **`_execute_query_wrapper(self, **kwargs)`**
    *   Called by the `execute_mongodb_find_query` `StructuredTool`.
    *   Validates `kwargs` against the `ExecuteQueryInput` Pydantic model.
    *   Calls `execute_mongodb_query` with the validated, unpacked arguments (converting `sort` from Pydantic models back to dicts).

### Pydantic Models (`models.py`)

These models define the expected structure and types for the arguments passed to the LangChain tools (and subsequently to the wrappers/core methods). They ensure data consistency and help the LLM format its tool calls correctly.

*   **`GetSchemaInput`**: Defines arguments for `get_mongodb_database_schema`.
*   **`ValidateSyntaxInput`**: Defines arguments for `validate_mongodb_query_syntax`.
*   **`ExecuteQueryInput`**: Defines arguments for `execute_mongodb_find_query`.
*   **`SortItem`**: Defines the structure for a single sort criterion within `ExecuteQueryInput`.

Refer to `mongodb_toolkit/models.py` for the exact field definitions, types, and descriptions.