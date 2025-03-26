import re
from collections.abc import Mapping, Sequence

# Define known MongoDB query operators (expand as needed)
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
    # Projection - Technically not query filter, but might appear
    # '$', '$elemMatch', '$meta', '$slice',
}
# Separate regex type check as isinstance doesn't work well directly
REGEX_TYPES = (re.Pattern, )
try:
    # Support bson.Regex if available (used by pymongo)
    from bson import Regex
    REGEX_TYPES = (re.Pattern, Regex)
except ImportError:
    pass


def validate_mongodb_query_syntax(query_doc):
    """
    Validates the basic syntax of a MongoDB query filter document without a schema.

    Checks for valid dictionary structure, known operators, and expected
    structural types for operator values (e.g., arrays for $in, $and; dicts for $not).
    Does NOT validate field names against a schema or data types of values.

    Args:
        query_doc: The MongoDB query filter document (should be a dictionary).

    Returns:
        list: A list of strings describing syntax errors found. An empty list means
              the syntax appears valid according to these rules.
    """
    errors = []
    if not isinstance(query_doc, Mapping):
        return ["Query root must be a dictionary."]

    _validate_syntax_recursive(query_doc, errors, path_prefix="")
    return errors

def _validate_syntax_recursive(current_part, errors, path_prefix):
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
                        _validate_syntax_recursive(sub_doc, errors, path_prefix=f"{current_path}[{i}]")

            elif key == '$not':
                # $not typically expects an operator expression block (dict) or a regex
                if not isinstance(value, Mapping) and not isinstance(value, REGEX_TYPES):
                     errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an operator expression block (dictionary) or a regex pattern.")
                elif isinstance(value, Mapping):
                    # Validate the inner expression block
                     _validate_syntax_recursive(value, errors, path_prefix=current_path)
                # If it's a regex, syntax is okay

            elif key in ('$in', '$nin', '$all'):
                 if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                    errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected an array.")
                 # Cannot validate types *within* the array without schema

            elif key == '$elemMatch':
                if not isinstance(value, Mapping):
                    errors.append(f"Invalid value type for operator '{key}' at '{current_path}': Expected a query document (dictionary).")
                else:
                     _validate_syntax_recursive(value, errors, path_prefix=current_path)

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
                    _validate_syntax_recursive(value, errors, path_prefix=current_path)
                elif has_fields:
                    # Assumed to be a nested document match, validate recursively
                     _validate_syntax_recursive(value, errors, path_prefix=current_path)
                # else: empty dictionary value, syntactically okay ({field: {}})

            # If value is a list, primitive, regex pattern etc., it's syntactically fine
            # as an implicit $eq or direct match. No further *syntax* check needed here.


# --- Example Usage ---
print("--- Validating Query Syntax ---")

valid_queries = [
    {'name': 'Alice', 'age': {'$gte': 30}},
    {'$or': [{'status': 'A'}, {'quantity': {'$lt': 10}}]},
    {'tags': {'$in': ['python', 'mongodb']}},
    {'location': {'$near': {'$geometry': {'type': 'Point', 'coordinates': [1, 1]}}}},
    {'counts': {'$elemMatch': {'value': 0, 'type': 'odd'}}},
    {'profile.email': {'$exists': True}},
    {'description': {'$regex': '^start', '$options': 'i'}},
    {'value': {'$type': ["string", "null"]}},
    {'$and': []}, # Empty $and is syntactically ok, though maybe unusual
    {'field': None}, # Matching null
    {'field': re.compile('pattern')} # Matching regex
]

invalid_queries = [
    "not a dict", # Invalid root type
    {'age': {'$gt': 30, 'name': 'Bob'}}, # Mixed operator and field name in value
    {'name': {'$greater_than': 5}}, # Unknown operator
    {'$or': {'status': 'A'}}, # $or expects an array
    {'tags': ['$in', ['a', 'b']]}, # Operator misplaced
    {'': 'value'}, # Empty field name
    {'$invalid_op': 123}, # Unknown top-level operator
    {'scores': {'$elemMatch': 10}}, # $elemMatch expects a dict
    {'type': {'$type': {'a': 1}}}, # $type expects string/int/array
    {'$and': [{'status': 'A'}, "not a dict"]}, # Item in $and is not a dict
    {'field': {'sub': 1, '$gt': 5}} # Mix of field and operator in value dict
]

print("\n-- Valid Queries --")
for i, q in enumerate(valid_queries):
    errors = validate_mongodb_query_syntax(q)
    print(f"Query {i+1}: {'VALID' if not errors else 'INVALID'}")
    if errors:
        for err in errors: print(f"  - {err}")

print("\n-- Invalid Queries --")
for i, q in enumerate(invalid_queries):
    errors = validate_mongodb_query_syntax(q)
    print(f"Query {i+1}: {'INVALID' if errors else 'VALID'}")
    if errors:
        for err in errors: print(f"  - {err}")