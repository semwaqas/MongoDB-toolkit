from pydantic.v1 import BaseModel, Field
from typing import List, Dict, Optional, Any, Tuple

# Re-import ASCENDING/DESCENDING or use literals 1/-1
from pymongo import ASCENDING, DESCENDING

class GetSchemaInput(BaseModel):
    target_collection_name: Optional[str] = Field(None, description="Optional: Name of a specific collection to get the schema for. If None, schemas for all collections are returned.")
    sample_size: int = Field(100, description="Number of documents to sample for schema inference.")

class ValidateSyntaxInput(BaseModel):
    query_doc: Dict[str, Any] = Field(..., description="The MongoDB query filter document (as a dictionary) to validate.")

class SortItem(BaseModel):
    """Represents a single field and direction for sorting."""
    field: str = Field(..., description="Field name to sort by.")
    direction: int = Field(..., description=f"Sort direction: {ASCENDING} for ascending, {DESCENDING} for descending.")

class ExecuteQueryInput(BaseModel):
    collection_name: str = Field(..., description="The name of the collection to query.")
    query_filter: Dict[str, Any] = Field(..., description="The filter document for the find query.")
    projection: Optional[Dict[str, Any]] = Field(None, description="Optional: Projection document (e.g., {'_id': 0, 'name': 1}).")
    limit: int = Field(0, description="Optional: Maximum number of documents to return (0 for no limit).")
    skip: int = Field(0, description="Optional: Number of documents to skip.")
    sort: Optional[List[SortItem]] = Field(None, description="Optional: List of sort criteria. Each item should be an object with 'field' (string) and 'direction' (integer: 1 for ascending, -1 for descending).")