import json
import logging

import azure.functions as func

from shared_code import db_manager

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="hello_mcp",
    description="Hello world.",
    toolProperties="[]",
)
def hello_mcp(context) -> str:
    """
    A simple function that returns a greeting message.

    Args:
        context: The trigger context (not used in this function).

    Returns:
        str: A greeting message.
    """
    return "Hello I am MCPTool!"


# Constants for property names, useful for consistency
_PROPERTY_A_NAME = "num_a"
_PROPERTY_B_NAME = "num_b"


class ToolProperty:
    """
    A simple class to represent a property of an MCP tool.
    This helps in defining the expected inputs for the tool.
    """

    def __init__(
        self, property_name: str, property_type: str, description: str
    ):
        self.propertyName = property_name
        self.propertyType = property_type
        self.description = description

    def to_dict(self):
        """Converts the ToolProperty object to a dictionary for JSON serialization."""
        return {
            "propertyName": self.propertyName,
            "propertyType": self.propertyType,
            "description": self.description,
        }


tool_properties_add_integers_object = [
    ToolProperty(_PROPERTY_A_NAME, "integer", "The first integer to add."),
    ToolProperty(_PROPERTY_B_NAME, "integer", "The second integer to add."),
]

tool_properties_add_integers_json = json.dumps(
    [prop.to_dict() for prop in tool_properties_add_integers_object]
)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="add_integers",
    description="Adds two integers and returns the sum.",
    toolProperties=tool_properties_add_integers_json,
)
def add_integers_tool(context: str) -> str:
    """
    An MCP tool that adds two integers.

    The 'context' argument is expected to be a JSON string containing
    an 'arguments' dictionary, which in turn holds 'num_a' and 'num_b'.

    Example expected context (as a string):
    '{
        "arguments": {
            "num_a": 5,
            "num_b": 10
        }
    }'

    Args:
        context (str): The trigger context from MCP, expected to be a JSON string
                       containing the arguments for the tool.

    Returns:
        str: A JSON string containing the result of the addition or an error message.
    """
    try:
        logging.info(f"Add_integers_tool invoked with context: {context}")

        payload = json.loads(context)

        if "arguments" not in payload:
            logging.error("Context missing 'arguments' key.")
            return json.dumps(
                {"error": "Invalid input format: 'arguments' key missing."}
            )

        args = payload["arguments"]

        if _PROPERTY_A_NAME not in args or _PROPERTY_B_NAME not in args:
            logging.error(
                f"Context arguments missing '{_PROPERTY_A_NAME}' or '{_PROPERTY_B_NAME}'."
            )
            return json.dumps(
                {
                    "error": f"Missing required arguments: '{_PROPERTY_A_NAME}' and '{_PROPERTY_B_NAME}'."
                }
            )

        val_a_input = args[_PROPERTY_A_NAME]
        val_b_input = args[_PROPERTY_B_NAME]

        try:
            num_a = int(val_a_input)
            num_b = int(val_b_input)
        except ValueError:
            logging.error(
                f"Could not convert inputs to integers. a='{val_a_input}', b='{val_b_input}'"
            )
            return json.dumps(
                {
                    "error": "Invalid argument types. 'num_a' and 'num_b' must be integers."
                }
            )

        result = num_a + num_b
        logging.info(f"Calculation result: {num_a} + {num_b} = {result}")

        return json.dumps({"sum": result})

    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON from context: {context}")
        return json.dumps({"error": "Invalid JSON format in input context."})
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        return json.dumps({"error": f"An unexpected error occurred: {str(e)}"})


def _parse_context_args(context_str: str, expected_arg_names: list) -> dict:
    try:
        payload = json.loads(context_str)
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON format in input context: {context_str}")
        return {"error": "Invalid JSON format in input context."}

    if "arguments" not in payload:
        logging.error("Context missing 'arguments' key.")
        return {"error": "Invalid input format: 'arguments' key missing."}

    args = payload["arguments"]
    extracted_args = {}
    for arg_name in expected_arg_names:
        if arg_name not in args:
            logging.error(f"Context arguments missing '{arg_name}'.")
            return {"error": f"Missing required argument: '{arg_name}'."}
        extracted_args[arg_name] = args[arg_name]

    return extracted_args


def _execute_query(query: str) -> str:
    try:
        conn = db_manager.get_db_connection()
        if conn is None:
            return json.dumps(
                {
                    "error": "AzurePostgreSQLManager not initialized. Check environment variables and logs."
                }
            )
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        # Convert rows to list of dicts for better JSON
        result_rows = [dict(zip(colnames, row)) for row in rows]
        logging.info(f"Query returned {len(rows)} rows.")
        return json.dumps({"columns": colnames, "rows": result_rows})
    except ConnectionError as ce:
        logging.error(f"Database connection error: {ce}")
        return json.dumps({"error": f"Database connection error: {ce}"})
    finally:
        if conn:
            db_manager.release_db_connection(conn)
            logging.info("Database connection released.")


tool_properties_get_databases_json = "[]"  # No input properties


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_databases_tool",
    description="Gets the list of all databases in a configured PostgreSQL server instance.",
    toolProperties=tool_properties_get_databases_json,
)
def get_databases_tool(context: str) -> str:
    logging.info("get_databases_tool invoked.")
    query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
    return _execute_query(query)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_schemas_tool",
    description="Gets schemas of all tables in the 'public' schema of a specified database.",
    toolProperties="[]",
)
def get_schemas_tool(context: str) -> str:
    logging.info("get_schemas_tool invoked.")
    query = """
    SELECT
        table_name,
        column_name,
        ordinal_position AS position,
        data_type
    FROM
        information_schema.columns
    WHERE
        table_schema = 'public'
    ORDER BY
        table_schema,
        table_name,
        ordinal_position;
    """
    return _execute_query(query)


# QUERY DATA TOOL
_QUERY_SQL_PROPERTY = "sql_query"
tool_properties_query_data_object = [
    ToolProperty(
        _QUERY_SQL_PROPERTY, "string", "The SQL SELECT query to execute."
    )
]
tool_properties_query_data_json = json.dumps(
    [prop.to_dict() for prop in tool_properties_query_data_object]
)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="query_data_tool",
    description="Runs read-only SQL queries (SELECT statements) on a specified database.",
    toolProperties=tool_properties_query_data_json,
)
def query_data_tool(context: str) -> str:
    logging.info("query_data_tool invoked.")

    args = _parse_context_args(context, [_QUERY_SQL_PROPERTY])
    if "error" in args:
        return json.dumps(args)

    sql_query = args[_QUERY_SQL_PROPERTY]

    # Basic validation to prevent obviously non-SELECT queries for a "query_data" tool
    if not sql_query.strip().upper().startswith("SELECT"):
        logging.warning(
            f"Query data tool received non-SELECT query: {sql_query}"
        )
        return json.dumps(
            {
                "error": "This tool is for SELECT queries only. Use appropriate tools for modifications."
            }
        )

    return _execute_query(sql_query)


@app.generic_trigger(
    arg_name="context",
    type="mcpToolTrigger",
    toolName="get_all_keys_tool",
    description="Gets all keys and constraints of all tables in the 'public' schema of a specified database.",
    toolProperties="[]",
)
def get_all_keys_tool(context: str) -> str:
    logging.info("get_all_keys_tool invoked.")

    query = """
    SELECT
        tc.table_name,
        tc.constraint_name,
        CASE tc.constraint_type
            WHEN 'PRIMARY KEY' THEN 'Primary Key'
            WHEN 'FOREIGN KEY' THEN 'Foreign Key'
            WHEN 'UNIQUE' THEN 'Unique Constraint'
            ELSE tc.constraint_type
        END AS constraint_type,

        array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS columns,

        -- Only for foreign keys
        ccu.table_name AS foreign_table_name,
        array_agg(ccu.column_name ORDER BY kcu.position_in_unique_constraint) AS foreign_columns

    FROM
        information_schema.table_constraints tc
    JOIN
        information_schema.key_column_usage kcu
        ON tc.constraint_catalog = kcu.constraint_catalog
        AND tc.constraint_schema = kcu.constraint_schema
        AND tc.constraint_name = kcu.constraint_name
    LEFT JOIN
        information_schema.constraint_column_usage ccu
        ON tc.constraint_catalog = ccu.constraint_catalog
        AND tc.constraint_schema = ccu.constraint_schema
        AND tc.constraint_name = ccu.constraint_name
    WHERE
        tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
        AND tc.table_schema NOT IN ('pg_catalog', 'information_schema', 'cron')
    GROUP BY
        tc.table_name,
        tc.constraint_name,
        tc.constraint_type,
        ccu.table_name
    ORDER BY
        tc.table_name,
        tc.constraint_name;
    """
    return _execute_query(query)
