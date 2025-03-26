from langchain import tools
from mongodb_toolkit.get_schema import generate_db_schema
from mongodb_toolkit.validate_query_syntax import validate_mongodb_query_syntax
from mongodb_toolkit.validate_query_schema import validate_query
from mongodb_toolkit.execute_query import execute_mongodb_query

get_schema_tool = tools.Tool(
    name='get_schema',
    description='Generate a schema for a MongoDB database. This schema can be used to generate queries as per user requirements. Input to this tool is the database connection details, and the output is the schema of the database.',
    function=generate_db_schema
)

validate_query_syntax_tool = tools.Tool(
    name='validate_query_syntax',
    description='Validate the syntax of a MongoDB query. This tool checks if the query is valid or not. If the query is invalid, it will return an error message. Input to this tool is the MongoDB query string, and the output is either a success message or an error message indicating the syntax issues. If an error occurs, correct the query and try again up to 3 times.',
    function=validate_mongodb_query_syntax
)

validate_query_tool = tools.Tool(
    name='validate_query',
    description='Validate a MongoDB query against a schema. This tool ensures that the query adheres to the schema of the database. Input to this tool is the MongoDB query string and the database schema, and the output is either a success message or an error message indicating the schema validation issues. If an error occurs, correct the query and try again up to 3 times.',
    function=validate_query
)

execute_query_tool = tools.Tool(
    name='execute_query',
    description='Execute a MongoDB query. This tool runs the provided query against the MongoDB database and returns the result. Input to this tool is the MongoDB query string and the database connection details, and the output is the result of the query execution.',
    function=execute_mongodb_query
)