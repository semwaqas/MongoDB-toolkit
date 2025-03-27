
# MongoDB Toolkit for LangChain (`MongoDB-toolkit`)

[![PyPI version](https://badge.fury.io/py/MongoDB-toolkit.svg)](https://badge.fury.io/py/MongoDB-toolkit)
[![License: CC BY-ND](https://img.shields.io/badge/License-CC%20BY--ND-lightgrey.svg)](https://creativecommons.org/licenses/by-nd/4.0/)

**`MongoDB-toolkit`** provides a structured way to connect LangChain Large Language Models (LLMs) with MongoDB databases. It offers tools for schema discovery, query syntax validation, and query execution, packaged within an easy-to-configure toolkit class.

This allows LLMs, through agents using function/tool calling, to intelligently interact with your data based on natural language requests.

## Features

*   **Configuration-Focused:** Instantiate a `MongoToolkit` object with your database connection details. Tools are generated pre-configured, keeping credentials out of prompts.
*   **Schema Discovery:** Provides a tool (`get_mongodb_database_schema`) for the LLM to fetch the schema of the entire database or specific collections. Essential for understanding data structure.
*   **Query Syntax Validation:** Includes a tool (`validate_mongodb_query_syntax`) to check if a generated query dictionary adheres to MongoDB's basic syntax rules before execution.
*   **Query Execution:** Offers a tool (`execute_mongodb_find_query`) to run validated MongoDB `find` queries, returning results with a configurable default limit (10).
*   **LangChain Integration:** Designed for easy integration with LangChain agents using `StructuredTool` for robust argument handling.
*   **Resource Management:** Includes a `close()` method to properly shut down the MongoDB connection.

## Installation

Install the package using pip:

```bash
pip install MongoDB-toolkit
```

Or, install directly from the source code:

```bash
git clone https://github.com/semwaqas/MongoDB-toolkit.git
cd MongoDB-toolkit
pip install .
```

**Dependencies:** This package requires:
*   `pymongo[srv]>=4.0,<5.0`
*   `langchain>=0.1.0,<0.2.0`
*   `langchain-openai>=0.1.0`
*   `pydantic>=1.9.0,<2.0.0`

Install them if needed:
```bash
pip install "pymongo[srv]>=4.0,<5.0" "langchain>=0.1.0,<0.2.0" "langchain-openai>=0.1.0" "pydantic>=1.9.0,<2.0.0"
```

## Configuration (Important!)

This toolkit **requires configuration before use**. You must provide your MongoDB connection URI and the target database name. The recommended way is through environment variables:

```bash
# Example using export (Linux/macOS)
export MONGODB_URI="mongodb+srv://your_user:<your_password>@your_cluster.mongodb.net/?retryWrites=true&w=majority"
export MONGODB_DB_NAME="my_application_db"

# Example using set (Windows Command Prompt)
set MONGODB_URI="mongodb+srv://your_user:<your_password>@your_cluster.mongodb.net/?retryWrites=true&w=majority"
set MONGODB_DB_NAME="my_application_db"

# Example using $env: (Windows PowerShell)
$env:MONGODB_URI="mongodb+srv://your_user:<your_password>@your_cluster.mongodb.net/?retryWrites=true&w=majority"
$env:MONGODB_DB_NAME="my_application_db"
```

Alternatively, you can use a `.env` file in your project root:

```dotenv
# .env file
MONGODB_URI="mongodb+srv://your_user:<your_password>@your_cluster.mongodb.net/?retryWrites=true&w=majority"
MONGODB_DB_NAME="my_application_db"
```

And load it in your Python script using `python-dotenv`:

```bash
pip install python-dotenv
```
```python
# your_script.py
import os
from dotenv import load_dotenv

load_dotenv() # Loads variables from .env file into environment

mongo_uri = os.environ.get("MONGODB_URI")
db_name = os.environ.get("MONGODB_DB_NAME")
# ... rest of your code
```

## Usage with LangChain Agents

1.  **Load Configuration:** Ensure `MONGODB_URI` and `MONGODB_DB_NAME` are accessible (e.g., loaded from environment variables).
2.  **Import and Instantiate `MongoToolkit`:** Create an instance, passing the URI and database name.
3.  **Get Tools:** Call the `get_tools()` method on the toolkit instance.
4.  **Create Agent:** Use the obtained tools list when setting up your LangChain agent (e.g., using `create_tool_calling_agent`).
5.  **Invoke Agent:** Run the agent executor with user input.
6.  **Close Connection:** Use a `try...finally` block to ensure `toolkit.close()` is called to release the database connection.

```python
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

# Import the toolkit class
from mongodb_toolkit import MongoToolkit, MongoToolkitError
```

```python
# 1. Load Configuration
load_dotenv()
mongo_uri = os.environ.get("MONGODB_URI")
db_name = os.environ.get("MONGODB_DB_NAME")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

if not mongo_uri or not db_name:
    raise ValueError("MONGODB_URI and MONGODB_DB_NAME environment variables must be set.")
```

```python
# 2. Instantiate Toolkit
# Connection details are encapsulated here
try:
    toolkit = MongoToolkit(mongo_uri=mongo_uri, db_name=db_name)
except MongoToolkitError as e:
    print(f"Error initializing MongoToolkit: {e}")
    exit(1) # Exit if toolkit setup fails

# 3. Get Configured Tools
tools = toolkit.get_tools()
```

```python
# 4. Setup LLM and Agent
# Ensure OPENAI_API_KEY is set in environment for ChatOpenAI
llm = ChatOpenAI(model="gpt-4o-mini", temperature=1) # Or your preferred model

# Agent Prompt
prompt_template = f"""
You are an assistant interacting with the '{db_name}' MongoDB database.
You have access to these tools: {{tools}}

Follow these steps:
1.  Determine if the database schema is needed to understand the data structure based on the user request.
2.  If schema is needed, use 'get_mongodb_database_schema'. Provide 'target_collection_name' ONLY if specified or certain. Otherwise, omit it to get all schemas.
3.  Identify the correct collection name(s) from the schema.
4.  Generate a MongoDB query filter dictionary ('query_doc') based on the request and schema.
5.  Validate the query using 'validate_mongodb_query_syntax'. If errors occur, fix the query and retry validation (max 3 attempts).
6.  If syntax is valid, use 'execute_mongodb_find_query'. You MUST provide 'collection_name' and 'query_filter'. Optionally use 'projection', 'skip', 'sort'. Remember the query defaults to returning a maximum of 10 results unless you specify a different 'limit' (use 0 for unlimited results).
7.  Respond to the user with a summary of the results or indicate if no results were found. Report errors if they occurred.

User Request: {{input}}
Agent Scratchpad: {{agent_scratchpad}}
"""

prompt = ChatPromptTemplate.from_messages([
    ("system", prompt_template),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

```

```python
agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True,
    handle_parsing_errors=True # Recommended for robustness
)
```

```python
# 5. Invoke Agent & 6. Close Connection
try:
    user_input = "Find a Job for Python Developer."

    print(f"\n--- Running Agent for: '{user_input}' ---")
    response = agent_executor.invoke({"input": user_input})
    print("\n--- Final Response ---")
    print(response.get('output', 'No output found.'))

except Exception as e:
    print(f"\n--- Agent Execution Failed ---")
    # Potentially log the full traceback here
    print(f"Error: {e}")

finally:
    # Crucial: Ensure connection is closed
    print("\n--- Cleaning up ---")
    toolkit.close()
```

```
[1m> Entering new AgentExecutor chain...[0m
[32;1m[1;3m
Invoking: `get_mongodb_database_schema` with `{}`


[0mGetting schema for database: 'database'
Establishing new MongoDB connection to database 'database'...
MongoDB connection successful.
Found collections: {________, ________, ________, ________, ________} # A list of Mongodb collections
--------------------
Analyzing collection: 'mc.....k'
  Sampling up to 10 documents from 'mc.....k'...
  Analyzed 10 documents.
--------------------
Analyzing collection: 'ind.....s'
  Sampling up to 10 documents from 'ind.....s'...
  Analyzed 10 documents.
--------------------
Analyzing collection: 'em......s'
  Sampling up to 10 documents from 'em......s'...
  Analyzed 10 documents.
--------------------
Analyzing collection: 'co......s'
  Sampling up to 10 documents from 'co......s'...
  Analyzed 10 documents.
--------------------
Analyzing collection: 'ce........s'
  Sampling up to 10 documents from 'ce........s'...
  Analyzed 10 documents.
--------------------
Analyzing collection: 'de........s'
  Sampling up to 10 documents from 'de........s'...
  Analyzed 10 documents.
--------------------
Analyzing collection: 'ch........t'
  Sampling up to 10 documents from 'ch........t'...
  Analyzed 10 documents.
--------------------
Analyzing collection: 'salery_prediction_dataset'
  Sampling up to 10 documents from 'salery_prediction_dataset'...
  Analyzed 10 documents.
--------------------
Analyzing collection: 'ch......ry'
  Sampling up to 10 documents from 'ch......ry'...
  Analyzed 10 documents.
--------------------
Analyzing collection: 'ad........s'
  Sampling up to 10 documents from 'ad........s'...
  Analyzed 10 documents.
--------------------
Analyzing collection: '_....._history'
  Sampling up to 10 documents from '_....._history'...
  Analyzed 7 documents.
--------------------
Analyzing collection: 'skills'
  Sampling up to 10 documents from 'skills'...
  Analyzed 10 documents.
--------------------
Analyzing collection: 'jobs'
  Sampling up to 10 documents from 'jobs'...
  Analyzed 10 documents.
--------------------
Analyzing collection: '.........._agent_history'
  Sampling up to 10 documents from '.........._agent_history'...
  Analyzed 10 documents.


[0mExecuting find on database.jobs
  Filter: {'$or': [{'jobType': 'Python Developer'}, {'requiredSkills': 'Python'}, {'verifiedSkills': 'Python'}]}
  Limit: 10

Query executed. Found 1 documents.
[38;5;200m[1;3m[{'_id': ObjectId('67b3445618bb537cc1f6f27c'), '________': ObjectId('67adf91f44342fdc58b18790'), '________': 50000, '________': 80000, 'salaryCurrency': 'USD', '________': 'Monthly', 'jobType': 'Full-Time', '________': 'Looking for an experienced AI/ML engineer.', 'experienceFrom': 2, 'experienceTo': 5, 'noOfCandidates': 1, '________': True, 'addressId': ObjectId('67adf92044342fdc58b18791'), '________': True, 'applicantPreference': 'Nearby', 'gender': 'Any', 'jobShift': 'Day', '________': ['Python', 'Django'], '________': ['REST API', 'SQL'], '________': [0.03542608022689819, -0.47762709856033325, 0.30901196599006653, ]}][0m[32;1m[1;3m

I found a job listing for a Python Developer:

### Job Details:
- **Job Title**: AI/ML Engineer
- **Job Type**: Full-Time
- **Salary Range**: $50,000 - $80,000 (Monthly)
- **Experience Required**: 2 to 5 years
- **Description**: Looking for an experienced AI/ML engineer.
- **Required Skills**: Python, Django
- **Verified Skills**: REST API, SQL
- **Applicant Preference**: Nearby candidates
- **Gender Preference**: Any
- **Job Shift**: Day
- **Is Active**: Yes
- **Verified Candidate Needed**: Yes

If you need more information or assistance with the application process, feel free to ask![0m

[1m> Finished chain.[0m

--- Final Response ---
I found a job listing for a Python Developer:

### Job Details:
- **Job Title**: AI/ML Engineer
- **Job Type**: Full-Time
- **Salary Range**: $50,000 - $80,000 (Monthly)
- **Experience Required**: 2 to 5 years
- **Description**: Looking for an experienced AI/ML engineer.
- **Required Skills**: Python, Django
- **Verified Skills**: REST API, SQL
- **Applicant Preference**: Nearby candidates
- **Gender Preference**: Any
- **Job Shift**: Day
- **Is Active**: Yes
- **Verified Candidate Needed**: Yes

If you need more information or assistance with the application process, feel free to ask!
Closing MongoDB connection.
```


## API Reference

Check [API Reference in Documentation](https://github.com/semwaqas/MongoDB-toolkit/blob/main/documentation.md)

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs, feature requests, or improvements.

1.  Fork the repository.
2.  Create your feature branch (`git checkout -b feature/AmazingFeature`).
3.  Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4.  Push to the branch (`git push origin feature/AmazingFeature`).
5.  Open a Pull Request.

## License

Distributed under the Creative Commons Attribution-NoDerivatives (CC BY-ND) License. See `LICENSE` file for more information.