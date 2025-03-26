import re
from bson import ObjectId, DBRef, MinKey, MaxKey, Timestamp, Int64, Decimal128, Binary, Code, Regex
from collections.abc import Mapping, Sequence # Use abc for broader type checks

def get_value_type_name(value):
    """Maps Python types commonly found in queries to BSON type names."""
    if isinstance(value, str): return "string"
    if isinstance(value, bool): return "bool"
    # Important: Check Int64 before int if you might have large numbers
    if isinstance(value, Int64): return "long"
    if isinstance(value, int): return "int" # Could be Int32 or Int64 in BSON
    if isinstance(value, float): return "double"
    if isinstance(value, Decimal128): return "decimal"
    # Check Sequence *before* Mapping/dict, but exclude str/bytes
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)): return "array"
    if isinstance(value, Mapping): return "object" # Check Mapping for dict-like
    if isinstance(value, ObjectId): return "objectId"
    if isinstance(value, DBRef): return "dbRef"
    if isinstance(value, Timestamp): return "timestamp"
    if isinstance(value, type(None)): return "null"
    if isinstance(value, MinKey): return "minKey"
    if isinstance(value, MaxKey): return "maxKey"
    if isinstance(value, (bytes, Binary)): return "binData" # Treat bytes as binData
    if isinstance(value, Code): return "javascript"
    # isinstance(value, Regex) doesn't work directly for re.Pattern
    if isinstance(value, Regex) or hasattr(value, 'pattern'): return "regex"
    # Add datetime, etc. if needed
    # Fallback
    return type(value).__name__

# List of common MongoDB query operators (add more if needed)
QUERY_OPERATORS = {
    '$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin',
    '$exists', '$type',
    '$mod', '$regex', '$options', '$text', '$search', '$where',
    # Array operators
    '$all', '$elemMatch', '$size',
    # Logical operators
    '$and', '$or', '$not', '$nor',
    # Geospatial, Bitwise, etc. can be added here
}


# Validation Logic

def validate_query(query_doc, expected_schema):
    """
    Validates a MongoDB query document against an expected schema definition.

    Args:
        query_doc (dict): The MongoDB query filter document.
        expected_schema (dict): The schema definition (output similar to the inference script).

    Returns:
        list: A list of strings describing validation errors. An empty list means valid.
    """
    if not isinstance(query_doc, Mapping):
        return ["Query document must be a dictionary-like object."]
    if not isinstance(expected_schema, Mapping):
         return ["Expected schema must be a dictionary-like object."]

    errors = []
    _validate_recursive(query_doc, expected_schema, errors, path_prefix="", full_schema=expected_schema)
    return errors

def _validate_recursive(query_part, schema_part, errors, path_prefix, full_schema):
    """Recursive helper for validation."""

    if not isinstance(query_part, Mapping):
        # This case should ideally not be hit for the top-level query_doc,
        # but might occur in $not or other nested scenarios incorrectly.
        errors.append(f"Invalid query structure at '{path_prefix}': Expected a dictionary, got {type(query_part).__name__}.")
        return

    for key, query_value in query_part.items():
        current_path = f"{path_prefix}.{key}" if path_prefix else key

        # Handle Logical Operators
        if key in ('$and', '$or', '$nor'):
            if not isinstance(query_value, Sequence) or isinstance(query_value, (str, bytes)):
                errors.append(f"Invalid value for operator '{key}' at '{current_path}': Expected an array of query documents.")
                continue
            if not query_value:
                 errors.append(f"Warning: Operator '{key}' at '{current_path}' has an empty array.")
                 continue
            # Validate each sub-query against the *full schema*
            for i, sub_query in enumerate(query_value):
                sub_path = f"{current_path}[{i}]"
                if not isinstance(sub_query, Mapping):
                     errors.append(f"Invalid element in '{key}' array at '{sub_path}': Expected a query document (dict).")
                     continue
                # Recursive call to the *top-level* validator for each item in $and/$or/$nor
                _validate_recursive(sub_query, full_schema, errors, path_prefix=f"{sub_path}", full_schema=full_schema)
            continue # Handled this logical operator key

        if key == '$not':
             # $not can contain a regex or an operator expression
             # We need the schema context of the *field* it applies to, which isn't directly here.
             # This requires rethinking how $not is handled, maybe pass parent schema context?
             # For now, let's do a basic check if it's a dict
             if not isinstance(query_value, Mapping):
                 errors.append(f"Invalid value for operator '$not' at '{current_path}': Expected an operator expression (dict).")
             else:
                 # We can't fully validate the inner part without knowing which field's schema applies.
                 # A simple heuristic: check if keys inside are operators. This is weak.
                 inner_keys = list(query_value.keys())
                 if not all(k.startswith('$') for k in inner_keys):
                      errors.append(f"Warning: Value for '$not' at '{current_path}' contains non-operator keys. Validation might be incomplete.")
                 # A full implementation would need the schema_part of the field being negated.
             continue


        # Handle Field Names (Potentially with Dot Notation)
        field_schema_info = None
        current_schema_level = schema_part

        # Handle dot notation (e.g., "address.city")
        parts = key.split('.')
        valid_path = True
        temp_path_prefix = path_prefix # Track path within dot notation traversal

        for i, part in enumerate(parts):
            if not isinstance(current_schema_level, Mapping):
                 errors.append(f"Invalid query path '{current_path}': Trying to access field '{part}' within a non-object schema part at '{temp_path_prefix}'.")
                 valid_path = False
                 break

            if part not in current_schema_level:
                # Check if the key is actually an operator applied to the *parent* object/doc
                # This happens if schema_part is the schema for a document, and key is like '$expr'
                if part.startswith('$') and i == 0: # Only check operators at the first level of split
                     # Let operator handling below deal with it, but need parent context. Difficult here.
                     # For simplicity, we'll assume dot notation *only* refers to nested fields for now.
                      errors.append(f"Invalid query key '{current_path}': Field '{part}' not found in schema at '{temp_path_prefix}'. Is it a misplaced operator?")

                else:
                    errors.append(f"Invalid query key '{current_path}': Field '{part}' not found in schema at '{temp_path_prefix}'.")

                valid_path = False
                break

            # Get the schema for this part
            field_schema_info = current_schema_level[part]

            # Check if we have the necessary nested schema info ('schema' for objects)
            if i < len(parts) - 1: # If not the last part, we need to traverse deeper
                temp_path_prefix = f"{temp_path_prefix}.{part}" if temp_path_prefix else part
                if 'object' not in field_schema_info.get('types', set()):
                    errors.append(f"Invalid query path '{current_path}': Field '{part}' at '{temp_path_prefix}' is not defined as an 'object' in the schema, cannot traverse further.")
                    valid_path = False
                    break
                if 'schema' not in field_schema_info:
                    errors.append(f"Schema definition error: Field '{part}' at '{temp_path_prefix}' is an 'object' but lacks a 'schema' definition.")
                    valid_path = False
                    break
                current_schema_level = field_schema_info['schema']
            else:
                 # This is the final part of the key, field_schema_info holds its definition
                 pass

        if not valid_path:
            continue # Skip validation for this key if path was invalid

        # We found the schema definition for the final field ('field_schema_info')

        # Check if the query value is a direct match or uses operators
        if isinstance(query_value, Mapping) and any(k.startswith('$') for k in query_value.keys()):
            # Value contains operators ($eq, $gt, $in, $elemMatch, etc.)
            for op, op_value in query_value.items():
                op_path = f"{current_path}.{op}"

                if op not in QUERY_OPERATORS:
                    errors.append(f"Unknown operator '{op}' used at '{op_path}'.")
                    continue

                # Operator-Specific Validation
                allowed_types = field_schema_info.get('types', set())
                element_schema = field_schema_info.get('element_schema', None) # For array fields

                if op in ('$eq', '$ne', '$gt', '$gte', '$lt', '$lte'):
                    op_value_type = get_value_type_name(op_value)
                    if not allowed_types:
                         errors.append(f"Schema definition error at '{current_path}': Field lacks 'types' definition.")
                    elif op_value_type not in allowed_types and 'null' not in allowed_types : # Allow comparison with null if null is allowed type
                         # Special case: Allow int/long/double/decimal to be compared somewhat interchangeably if any numeric type is allowed
                         numeric_types = {'int', 'long', 'double', 'decimal'}
                         if not (op_value_type in numeric_types and bool(allowed_types.intersection(numeric_types))):
                             errors.append(f"Type mismatch for operator '{op}' at '{op_path}': Query uses type '{op_value_type}', but schema expects {allowed_types}.")

                elif op in ('$in', '$nin'):
                    if not isinstance(op_value, Sequence) or isinstance(op_value, (str, bytes)):
                        errors.append(f"Invalid value for operator '{op}' at '{op_path}': Expected an array.")
                        continue
                    if not allowed_types:
                         errors.append(f"Schema definition error at '{current_path}': Field lacks 'types' definition.")
                         continue
                    for i, item in enumerate(op_value):
                        item_type = get_value_type_name(item)
                        item_path = f"{op_path}[{i}]"
                        if item_type not in allowed_types and not (item_type == 'null' and 'null' in allowed_types):
                             numeric_types = {'int', 'long', 'double', 'decimal'}
                             if not (item_type in numeric_types and bool(allowed_types.intersection(numeric_types))):
                                errors.append(f"Type mismatch for item in '{op}' array at '{item_path}': Item type is '{item_type}', but schema expects {allowed_types}.")

                elif op == '$exists':
                    if not isinstance(op_value, bool):
                        errors.append(f"Invalid value for operator '{op}' at '{op_path}': Expected boolean (true/false).")

                elif op == '$type':
                    # Value can be BSON type string or number
                    valid_type_spec = False
                    if isinstance(op_value, str): # BSON type alias
                        valid_type_spec = True # Assume string alias is potentially valid
                    elif isinstance(op_value, int): # BSON type number
                         valid_type_spec = True # Assume number is potentially valid
                    else:
                         errors.append(f"Invalid value for operator '{op}' at '{op_path}': Expected BSON type string (e.g., 'string') or number (e.g., 2).")

                    if valid_type_spec and allowed_types:
                         # Simple check: if $type requests a type not listed in schema's *possible* types, it's likely an issue.
                         # Note: This is tricky as $type checks the *actual* BSON type.
                         requested_type_str = str(op_value) # Crude check
                         if requested_type_str not in allowed_types and op_value not in allowed_types:
                            # Basic check - might need refinement based on BSON numbers vs names
                            errors.append(f"Warning: Operator '{op}' at '{op_path}' checks for type '{op_value}', which might not be among the expected schema types {allowed_types}.")

                elif op == '$regex':
                     if 'string' not in allowed_types:
                         errors.append(f"Usage warning for operator '{op}' at '{op_path}': Field type is not 'string' in schema ({allowed_types}), $regex might not work as expected.")
                     if not isinstance(op_value, (str, Regex, re.Pattern)):
                          errors.append(f"Invalid value for operator '{op}' at '{op_path}': Expected a string or regex pattern.")
                     # Could also validate '$options' if present in query_value dict

                elif op == '$size':
                     if 'array' not in allowed_types:
                         errors.append(f"Usage error for operator '{op}' at '{op_path}': Field type is not 'array' in schema ({allowed_types}).")
                     if not isinstance(op_value, int):
                          errors.append(f"Invalid value for operator '{op}' at '{op_path}': Expected an integer size.")

                elif op == '$all':
                     if 'array' not in allowed_types:
                         errors.append(f"Usage error for operator '{op}' at '{op_path}': Field type is not 'array' in schema ({allowed_types}).")
                     elif not isinstance(op_value, Sequence) or isinstance(op_value, (str, bytes)):
                         errors.append(f"Invalid value for operator '{op}' at '{op_path}': Expected an array of elements.")
                     elif element_schema:
                         # Validate each item in $all against the element schema
                         elem_allowed_types = element_schema.get('types', set())
                         if not elem_allowed_types:
                             errors.append(f"Schema definition error at '{current_path}': Array field lacks 'element_schema' with 'types'.")
                             continue
                         for i, item in enumerate(op_value):
                             item_type = get_value_type_name(item)
                             item_path = f"{op_path}[{i}]"
                             if item_type not in elem_allowed_types and not (item_type == 'null' and 'null' in elem_allowed_types):
                                 numeric_types = {'int', 'long', 'double', 'decimal'}
                                 if not (item_type in numeric_types and bool(elem_allowed_types.intersection(numeric_types))):
                                     errors.append(f"Type mismatch for item in '{op}' array at '{item_path}': Item type is '{item_type}', but array element schema expects {elem_allowed_types}.")
                     else:
                          errors.append(f"Schema definition error at '{current_path}': Array field lacks 'element_schema' definition needed to validate '{op}'.")


                elif op == '$elemMatch':
                     if 'array' not in allowed_types:
                         errors.append(f"Usage error for operator '{op}' at '{op_path}': Field type is not 'array' in schema ({allowed_types}).")
                     elif not isinstance(op_value, Mapping):
                         errors.append(f"Invalid value for operator '{op}' at '{op_path}': Expected a query document (dict) for element matching.")
                     elif element_schema:
                         # The element schema might be a primitive type or an object
                         elem_types = element_schema.get('types', set())
                         if 'object' in elem_types:
                             # Validate the $elemMatch query against the element's object schema
                             nested_elem_schema = element_schema.get('schema')
                             if nested_elem_schema:
                                 _validate_recursive(op_value, nested_elem_schema, errors, path_prefix=f"{op_path}", full_schema=full_schema) # Pass full_schema for logical operators within $elemMatch
                             else:
                                  errors.append(f"Schema definition error at '{current_path}': Array element is 'object' but lacks 'schema' in 'element_schema'.")
                         elif elem_types:
                              # If element schema is primitive, $elemMatch query should use operators applicable to that type
                              # We need to validate the operators inside op_value against the primitive element_schema
                              _validate_recursive_operators_against_schema(op_value, element_schema, errors, op_path, full_schema)

                         else:
                             errors.append(f"Schema definition error at '{current_path}': Array field 'element_schema' lacks 'types'.")

                     else:
                         errors.append(f"Schema definition error at '{current_path}': Array field lacks 'element_schema' definition needed to validate '{op}'.")

                # Add more operator checks ($mod, $text, $where, geo, etc.) here if needed

        else:
            # Value is a direct match (implicit $eq)
            query_value_type = get_value_type_name(query_value)
            allowed_types = field_schema_info.get('types', set())

            if not allowed_types:
                errors.append(f"Schema definition error at '{current_path}': Field lacks 'types' definition.")
            elif query_value_type not in allowed_types:
                 # Allow null match if 'null' is an allowed type
                 if query_value_type == 'null' and 'null' in allowed_types:
                     pass # Valid null match
                 else:
                     # Special case: Allow int/long/double/decimal to match if any numeric type is allowed
                     numeric_types = {'int', 'long', 'double', 'decimal'}
                     if not (query_value_type in numeric_types and bool(allowed_types.intersection(numeric_types))):
                         errors.append(f"Type mismatch for field '{current_path}': Query uses type '{query_value_type}', but schema expects {allowed_types}.")


def _validate_recursive_operators_against_schema(operator_query, field_schema, errors, path_prefix, full_schema):
     """
     Helper specifically for validating an operator block (like inside $elemMatch for primitives)
     against a specific field schema definition.
     """
     if not isinstance(operator_query, Mapping):
         errors.append(f"Invalid structure at '{path_prefix}': Expected an operator dictionary.")
         return

     # Simulate the structure needed by the main validator by wrapping the schema
     # This is a bit of a hack, cleaner ways might exist
     temp_wrapper_schema = {"_field_": field_schema}
     temp_wrapper_query = {"_field_": operator_query}

     _validate_recursive(temp_wrapper_query, temp_wrapper_schema, errors, path_prefix="", full_schema=full_schema) # path_prefix is tricky here, maybe adjust