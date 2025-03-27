import re
from bson import ObjectId, DBRef, MinKey, MaxKey, Timestamp, Int64, Decimal128, Binary, Code, Regex
from pymongo import ASCENDING, DESCENDING # Import directly
from pymongo.collection import Collection
from pymongo.errors import OperationFailure
# Use Mapping, Sequence from collections.abc for broader compatibility
from collections.abc import Mapping, Sequence
from typing import List, Dict, Any, Set, Optional
import sys # For stderr printing

from .exceptions import SchemaError

# Constants
# See: https://www.mongodb.com/docs/manual/reference/operator/query/
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
}
REGEX_TYPES = (re.Pattern, )
try:
    REGEX_TYPES = (re.Pattern, Regex)
except ImportError:
    pass

# === Schema Inference Helpers ===

def get_bson_type_name(value):
    """Maps Python types to BSON type names for clarity."""
    if isinstance(value, str): return "string"
    if isinstance(value, bool): return "bool"
    if isinstance(value, Int64): return "long"
    if isinstance(value, int): return "int"
    if isinstance(value, float): return "double"
    if isinstance(value, Decimal128): return "decimal"
    # Use Sequence check for list-like, exclude str/bytes
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)): return "array"
    if isinstance(value, Mapping): return "object" # Use Mapping check for dict-like
    if isinstance(value, ObjectId): return "objectId"
    if isinstance(value, DBRef): return "dbRef"
    if isinstance(value, Timestamp): return "timestamp"
    if isinstance(value, type(None)): return "null"
    if isinstance(value, MinKey): return "minKey"
    if isinstance(value, MaxKey): return "maxKey"
    if isinstance(value, (bytes, Binary)): return "binData"
    if isinstance(value, Code): return "javascript"
    if isinstance(value, Regex) or isinstance(value, re.Pattern): return "regex"
    # Add other specific BSON types if needed (e.g., datetime)
    return type(value).__name__

def _infer_schema_recursive(obj):
    """Recursively infers the schema of a Python object (dict, list, or primitive)."""
    bson_type = get_bson_type_name(obj)

    if bson_type == "object":
        nested_schema = {}
        for key, value in obj.items():
            nested_schema[key] = _infer_schema_recursive(value)
        return {"types": {bson_type}, "schema": nested_schema}

    elif bson_type == "array":
        if not obj:
            return {"types": {bson_type}, "element_schema": {"types": {"empty_array"}}}

        merged_element_schema = None
        for item in obj:
            item_schema_info = _infer_schema_recursive(item)
            # Defensive check for item_schema_info
            if item_schema_info is None:
                 print(f"Warning: _infer_schema_recursive returned None for item '{item}' in array. Skipping.", file=sys.stderr)
                 continue # Skip if inference fails for an item

            if merged_element_schema is None:
                merged_element_schema = item_schema_info
            else:
                merged_element_schema = _merge_schema_info(merged_element_schema, item_schema_info)
                # Defensive check after merge
                if merged_element_schema is None:
                     print(f"Warning: _merge_schema_info returned None while merging array element schemas. Resetting.", file=sys.stderr)
                     # Decide how to handle: maybe reset to current item_schema_info or skip?
                     # For now, let's try resetting to the current item's schema to avoid None propagation
                     merged_element_schema = item_schema_info


        # Ensure merged_element_schema is not None before returning
        if merged_element_schema is None:
            print(f"Warning: Could not determine merged element schema for array, possibly empty or containing only uninferrable items. Defaulting to unknown.", file=sys.stderr)
            # Provide a default placeholder if all items failed or list was effectively empty after skips
            merged_element_schema = {"types": {"unknown_array_element"}}

        return {"types": {bson_type}, "element_schema": merged_element_schema}

    else:
        # Primitive type
        return {"types": {bson_type}}

def _merge_schema_info(existing_info, new_info):
    """Merges two schema information dictionaries more robustly."""
    # Handle cases where one input might be invalid/None (shouldn't happen often with checks above)
    if not isinstance(existing_info, Mapping):
        print(f"Warning: Invalid 'existing_info' in _merge_schema_info: {existing_info}. Returning 'new_info'.", file=sys.stderr)
        return new_info if isinstance(new_info, Mapping) else None
    if not isinstance(new_info, Mapping):
        print(f"Warning: Invalid 'new_info' in _merge_schema_info: {new_info}. Returning 'existing_info'.", file=sys.stderr)
        return existing_info # existing_info is known to be a Mapping here

    # Now both existing_info and new_info are known to be Mappings
    merged_info = existing_info.copy()

    # Merge types
    merged_info["types"] = set(merged_info.get("types", set())) # Ensure it's a set
    merged_info["types"].update(new_info.get("types", set()))

    # Merge nested schemas ('schema' for objects)
    if "schema" in new_info:
        new_nested_schema = new_info["schema"]
        # Check if new_nested_schema is iterable
        if isinstance(new_nested_schema, Mapping):
            if "schema" not in merged_info or not isinstance(merged_info.get("schema"), Mapping):
                # If existing doesn't have a schema or it's invalid, just take the new one
                merged_info["schema"] = new_nested_schema
            else:
                # Both have valid schemas, merge recursively
                schema1 = merged_info["schema"] # Known to be Mapping here
                schema2 = new_nested_schema      # Known to be Mapping here
                merged_nested = schema1.copy()
                for key, value2 in schema2.items():
                    # Ensure value2 is valid before merging
                    if not isinstance(value2, Mapping):
                        print(f"Warning: Invalid value found for key '{key}' in nested schema merge. Skipping.", file=sys.stderr)
                        continue

                    if key not in merged_nested:
                        merged_nested[key] = value2
                    else:
                        merged_nested_item = merged_nested[key]
                        # Ensure existing item is valid before merging
                        if not isinstance(merged_nested_item, Mapping):
                             print(f"Warning: Overwriting invalid existing schema for key '{key}' during merge.", file=sys.stderr)
                             merged_nested[key] = value2
                        else:
                             merged_result = _merge_schema_info(merged_nested_item, value2)
                             if merged_result is not None: # Only update if merge was successful
                                 merged_nested[key] = merged_result
                             else:
                                 print(f"Warning: Recursive merge for key '{key}' failed. Keeping existing.", file=sys.stderr)

                merged_info["schema"] = merged_nested
        else:
            print(f"Warning: 'schema' key found in new_info, but value is not a dictionary: {new_nested_schema}. Skipping schema merge.", file=sys.stderr)

    # Merge array element schemas ('element_schema' for arrays)
    if "element_schema" in new_info:
        new_element_schema = new_info["element_schema"]
        # Check if new_element_schema is valid
        if isinstance(new_element_schema, Mapping):
            if "element_schema" not in merged_info or not isinstance(merged_info.get("element_schema"), Mapping):
                # If existing doesn't have element_schema or it's invalid, take the new one
                merged_info["element_schema"] = new_element_schema
            else:
                # Both have potentially valid element_schemas, merge recursively
                existing_element_schema = merged_info["element_schema"] # Known Mapping
                merged_element = _merge_schema_info(existing_element_schema, new_element_schema)
                if merged_element is not None: # Only update if merge was successful
                    merged_info["element_schema"] = merged_element
                    # Clean up 'empty_array' if other types are now present
                    merged_types = merged_element.get("types", set())
                    if "empty_array" in merged_types and len(merged_types) > 1:
                        merged_element["types"].discard("empty_array")
                else:
                     print(f"Warning: Recursive merge for element_schema failed. Keeping existing.", file=sys.stderr)
        else:
             print(f"Warning: 'element_schema' key found in new_info, but value is not a dictionary: {new_element_schema}. Skipping element_schema merge.", file=sys.stderr)

    return merged_info

# (generate_collection_schema remains mostly the same, but benefits from robust merge)
def generate_collection_schema(collection: Collection, sample_size: int) -> Optional[Dict[str, Any]]:
    """Infers the schema of a single MongoDB collection by sampling documents."""
    print(f"  Sampling up to {sample_size} documents from '{collection.name}'...")
    try:
        documents = list(collection.find(limit=sample_size))
        if not documents:
            print("  Collection is empty or no documents found in sample.")
            return None
    # (error handling remains the same)
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
        try:
            doc_schema_info = _infer_schema_recursive(doc)

            # Defensive check: Ensure inference returned a dict
            if not isinstance(doc_schema_info, Mapping):
                print(f"Warning: Schema inference for document (ID: {doc.get('_id', 'N/A')}) failed, returned non-dict. Skipping doc.", file=sys.stderr)
                continue

            # The top level is always an object for a MongoDB doc
            if "schema" in doc_schema_info and isinstance(doc_schema_info["schema"], Mapping):
                doc_inner_schema = doc_schema_info["schema"]
                temp_merged = merged_collection_schema.copy()

                for key, value_info in doc_inner_schema.items():
                    # Defensive check: Ensure value_info is a dict
                    if not isinstance(value_info, Mapping):
                        print(f"Warning: Invalid schema info for key '{key}' in document (ID: {doc.get('_id', 'N/A')}). Skipping key.", file=sys.stderr)
                        continue

                    existing_value_info = temp_merged.get(key)
                    if existing_value_info is None:
                        temp_merged[key] = value_info
                    elif not isinstance(existing_value_info, Mapping):
                         print(f"Warning: Overwriting previously invalid schema for key '{key}' with new info.", file=sys.stderr)
                         temp_merged[key] = value_info
                    else:
                        # Both existing and new are valid Mappings, merge them
                        merged_result = _merge_schema_info(existing_value_info, value_info)
                        if merged_result is not None: # Only update if merge successful
                            temp_merged[key] = merged_result
                        else:
                             print(f"Warning: Merge failed for key '{key}'. Keeping previous merged state.", file=sys.stderr)

                merged_collection_schema = temp_merged
            elif "schema" not in doc_schema_info:
                 print(f"Warning: Inference for document (ID: {doc.get('_id', 'N/A')}) did not produce a 'schema' key, though type was object. Skipping doc.", file=sys.stderr)
            else: # schema exists but is not a Mapping
                print(f"Warning: Inference for document (ID: {doc.get('_id', 'N/A')}) produced a non-dictionary 'schema'. Skipping doc.", file=sys.stderr)

        except Exception as e:
             # Catch errors during processing of a single document
             print(f"Error processing schema for document (ID: {doc.get('_id', 'N/A')}): {e}. Skipping doc.", file=sys.stderr)
             # Optionally log the traceback here for deeper debugging
             # import traceback; traceback.print_exc()

    print(f"  Analyzed {doc_count} documents.")
    return merged_collection_schema


# === Syntax Validation Helpers ===
def validate_query_syntax_recursive(current_part, errors, path_prefix):
    """Recursive helper for syntax validation."""
    if not isinstance(current_part, Mapping):
        errors.append(f"Invalid structure at '{path_prefix}': Expected a dictionary, but found {type(current_part).__name__}.")
        return

    for key, value in current_part.items():
        current_path = f"{path_prefix}.{key}" if path_prefix else key

        # Check 1: Key is an Operator
        if key.startswith('$'):
            if key not in KNOWN_QUERY_OPERATORS:
                errors.append(f"Unknown operator '{key}' used at '{current_path}'.")
                continue # Don't validate value structure for unknown ops

            # Check structural type of the value based on the operator
            if key in ('$and', '$or', '$nor'):
                if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                    errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an array of query documents.")
                elif not value:
                     # It's syntactically valid, maybe add a warning if desired
                     # errors.append(f"Warning: Operator '{key}' at '{current_path}' has an empty array.")
                     pass
                else:
                    for i, sub_doc in enumerate(value):
                        validate_query_syntax_recursive(sub_doc, errors, path_prefix=f"{current_path}[{i}]")

            elif key == '$not':
                if not isinstance(value, Mapping) and not isinstance(value, REGEX_TYPES):
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an operator expression block (dictionary) or a regex pattern.")
                elif isinstance(value, Mapping):
                     validate_query_syntax_recursive(value, errors, path_prefix=current_path)

            elif key in ('$in', '$nin', '$all'):
                 if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                    errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an array.")

            elif key == '$elemMatch':
                if not isinstance(value, Mapping):
                    errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected a query document (dictionary).")
                else:
                     validate_query_syntax_recursive(value, errors, path_prefix=current_path)

            elif key == '$exists':
                 if not isinstance(value, bool):
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected a boolean (true/false).")

            elif key == '$type':
                is_valid_type = False
                if isinstance(value, (str, int)): is_valid_type = True
                elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                    is_valid_type = all(isinstance(item, (str, int)) for item in value)
                if not is_valid_type:
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected a BSON type string, number, or an array of strings/numbers.")

            elif key == '$size':
                 if not isinstance(value, int):
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an integer.")

            elif key == '$regex':
                 if not isinstance(value, (str, ) + REGEX_TYPES):
                      errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected a string or regex pattern.")

            elif key == '$mod':
                 if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != 2 or not all(isinstance(v, (int, float)) for v in value):
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an array of two numbers [divisor, remainder].")

            # Add more structural checks if needed

        # Check 2: Key is a Field Name
        else:
            if not key: errors.append(f"Empty field name found at '{path_prefix}'.")
            elif key.startswith('$'): errors.append(f"Invalid field name '{key}' starting with '$' at '{current_path}'.")
            elif isinstance(value, Mapping):
                sub_keys = list(value.keys())
                has_operators = any(k.startswith('$') for k in sub_keys)
                has_fields = any(not k.startswith('$') for k in sub_keys)

                if has_operators and has_fields:
                    errors.append(f"Invalid query structure at '{current_path}': Cannot mix operators and field names at the same level within a field's value.")
                # Recursively validate if it's purely operators or purely fields
                elif has_operators or has_fields:
                    validate_query_syntax_recursive(value, errors, path_prefix=current_path)
                # Empty dict {} is fine