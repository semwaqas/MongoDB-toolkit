import re
from bson import ObjectId, DBRef, MinKey, MaxKey, Timestamp, Int64, Decimal128, Binary, Code, Regex
from pymongo import ASCENDING, DESCENDING # Import directly
from pymongo.collection import Collection
from pymongo.errors import OperationFailure
from collections.abc import Mapping, Sequence
from typing import List, Dict, Any, Set, Optional

from .exceptions import SchemaError

# --- Constants ---
KNOWN_QUERY_OPERATORS = {
    # Comparison
    '$eq', '$gt', '$gte', '$in', '$lt', '$lte', '$ne', '$nin',
    # Logical
    '$and', '$or', '$not', '$nor',
    # Element
    '$exists', '$type',
    # Evaluation
    '$expr', '$jsonSchema', '$mod', '$regex', '$options', '$text', '$where', '$search',
    # Geospatial
    '$geoIntersects', '$geoWithin', '$near', '$nearSphere', '$box', '$center',
    '$centerSphere', '$geometry', '$maxDistance', '$minDistance', '$polygon',
    # Array
    '$all', '$elemMatch', '$size',
    # Bitwise
    '$bitsAllClear', '$bitsAllSet', '$bitsAnyClear', '$bitsAnySet',
    # Comments
    '$comment',
    # Projection - Technically not query filter, but might appear
    # '$', '$elemMatch', '$meta', '$slice',
}

REGEX_TYPES = (re.Pattern, )
try:
    REGEX_TYPES = (re.Pattern, Regex)
except ImportError:
    pass

# === Schema Inference Helpers ===

def get_bson_type_name(value):
    """Maps Python types to BSON type names for clarity."""
    # ... (copy from previous schema script) ...
    if isinstance(value, str): return "string"
    if isinstance(value, bool): return "bool"
    if isinstance(value, Int64): return "long"
    if isinstance(value, int): return "int"
    if isinstance(value, float): return "double"
    if isinstance(value, Decimal128): return "decimal"
    if isinstance(value, list): return "array"
    if isinstance(value, dict): return "object"
    if isinstance(value, ObjectId): return "objectId"
    # ... add other types ...
    if isinstance(value, type(None)): return "null"
    return type(value).__name__

def _infer_schema_recursive(obj):
    """Recursively infers the schema of a Python object (dict, list, or primitive)."""
    # ... (copy from previous schema script) ...
    bson_type = get_bson_type_name(obj)
    # ... (handle object, array, primitive cases) ...

def _merge_schema_info(existing_info, new_info):
    """Merges two schema information dictionaries."""
    # ... (copy from previous schema script) ...

def generate_collection_schema(collection: Collection, sample_size: int) -> Optional[Dict[str, Any]]:
    """Infers the schema of a single MongoDB collection by sampling documents."""
    print(f"  Sampling up to {sample_size} documents from '{collection.name}'...")
    try:
        documents = list(collection.find(limit=sample_size))
        if not documents:
            print("  Collection is empty or no documents found in sample.")
            return None
    except OperationFailure as e:
        print(f"  Error sampling collection '{collection.name}': {e}")
        raise SchemaError(f"Operation failed while sampling {collection.name}: {e}") from e
    except Exception as e:
        print(f"  Unexpected error accessing collection '{collection.name}': {e}")
        raise SchemaError(f"Unexpected error sampling {collection.name}: {e}") from e

    merged_collection_schema = {}
    doc_count = 0
    for doc in documents:
        doc_count += 1
        doc_schema_info = _infer_schema_recursive(doc)
        if "schema" in doc_schema_info:
            doc_inner_schema = doc_schema_info["schema"]
            temp_merged = merged_collection_schema.copy()
            for key, value_info in doc_inner_schema.items():
                if key not in temp_merged:
                    temp_merged[key] = value_info
                else:
                    temp_merged[key] = _merge_schema_info(temp_merged[key], value_info)
            merged_collection_schema = temp_merged

    print(f"  Analyzed {doc_count} documents.")
    return merged_collection_schema

# === Syntax Validation Helpers ===

def validate_query_syntax_recursive(current_part, errors, path_prefix):
    """Recursive helper for syntax validation."""

    if not isinstance(current_part, Mapping):
        # This case can happen inside $and, $or, $elemMatch etc. if structure is wrong
        errors.append(f"Invalid structure at '{path_prefix}': Expected a dictionary, but found {type(current_part).__name__}.")
        return

    for key, value in current_part.items():
        current_path = f"{path_prefix}.{key}" if path_prefix else key

        # --- Check 1: Key is an Operator ---
        if key.startswith('$'):
            if key not in KNOWN_QUERY_OPERATORS:
                errors.append(f"Unknown operator '{key}' used at '{current_path}'.")
                # Continue checking other keys even if one operator is unknown

            # Check structural type of the value based on the operator
            if key in ('$and', '$or', '$nor'):
                if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                    errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an array of query documents.")
                elif not value:
                     errors.append(f"Warning: Operator '{key}' at '{current_path}' has an empty array.")
                else:
                    # Validate each sub-document in the array
                    for i, sub_doc in enumerate(value):
                        validate_query_syntax_recursive(sub_doc, errors, path_prefix=f"{current_path}[{i}]")

            elif key == '$not':
                # $not typically expects an operator expression block (dict) or a regex
                if not isinstance(value, Mapping) and not isinstance(value, REGEX_TYPES):
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an operator expression block (dictionary) or a regex pattern.")
                elif isinstance(value, Mapping):
                    # Validate the inner expression block
                     validate_query_syntax_recursive(value, errors, path_prefix=current_path)
                # If it's a regex, syntax is okay

            elif key in ('$in', '$nin', '$all'):
                 if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                    errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an array.")
                 # Cannot validate types *within* the array without schema

            elif key == '$elemMatch':
                if not isinstance(value, Mapping):
                    errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected a query document (dictionary).")
                else:
                     validate_query_syntax_recursive(value, errors, path_prefix=current_path)

            elif key == '$exists':
                 if not isinstance(value, bool):
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected a boolean (true/false).")

            elif key == '$type':
                 # Can be string alias or BSON type number (int) or array of these
                is_valid_type = False
                if isinstance(value, (str, int)):
                    is_valid_type = True
                elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                    is_valid_type = all(isinstance(item, (str, int)) for item in value)

                if not is_valid_type:
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected a BSON type string, number, or an array of strings/numbers.")

            elif key == '$size':
                 if not isinstance(value, int):
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an integer.")

            elif key == '$regex':
                 # Value should be string or regex pattern. $options might be separate or within value dict
                 if not isinstance(value, (str, ) + REGEX_TYPES):
                      errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected a string or regex pattern.")

            elif key == '$mod':
                 if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != 2 or not all(isinstance(v, (int, float)) for v in value):
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an array of two numbers [divisor, remainder].")

            # Add structural checks for other operators ($geo*, $text, $where etc.) if needed
            # For many comparison operators ($gt, $lt etc.), any primitive value is syntactically okay.

        # --- Check 2: Key is a Field Name (or potential dot notation) ---
        else:
            # Basic check for field name validity (cannot be empty, cannot start with $)
            if not key:
                errors.append(f"Empty field name found at '{path_prefix}'.")
                continue
            if key.startswith('$'):
                # This shouldn't happen if the first check catches operators, but as safeguard
                errors.append(f"Invalid field name '{key}' starting with '$' at '{current_path}'.")
                continue

            # Check the structure of the value associated with the field
            if isinstance(value, Mapping):
                # Value is a dictionary. Could be:
                # 1. Operator block: {'$gt': 5, '$lt': 10}
                # 2. Nested document match: {'subfield': 'value'}
                # 3. Invalid mix: {'subfield': 'value', '$gt': 5}
                sub_keys = list(value.keys())
                has_operators = any(k.startswith('$') for k in sub_keys)
                has_fields = any(not k.startswith('$') for k in sub_keys)

                if has_operators and has_fields:
                    errors.append(f"Invalid query structure at '{current_path}': Cannot mix operators (like '{[k for k in sub_keys if k.startswith('$')][0]}') and field names (like '{[k for k in sub_keys if not k.startswith('$')][0]}') at the same level within a field's value.")
                elif has_operators:
                    # Assumed to be an operator block, validate recursively
                    validate_query_syntax_recursive(value, errors, path_prefix=current_path)
                elif has_fields:
                    # Assumed to be a nested document match, validate recursively
                     validate_query_syntax_recursive(value, errors, path_prefix=current_path)
                # else: empty dictionary value, syntactically okay ({field: {}})

            # If value is a list, primitive, regex pattern etc., it's syntactically fine
            # as an implicit $eq or direct match. No further *syntax* check needed here.