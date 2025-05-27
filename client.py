import asyncio
import json
import logging
import os
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
from langchain_core.load import dumps
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_openai import AzureChatOpenAI
from langgraph.prebuilt import create_react_agent
from mcp import ClientSession
from mcp.client.sse import sse_client

# Load environment variables
load_dotenv()


# Configure logging with security-focused setup
def setup_secure_logging():
    """Configure logging to prevent secret exposure."""

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler()],
    )

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("mcp").setLevel(logging.WARNING)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger(
        "azure.core.pipeline.policies.http_logging_policy"
    ).setLevel(logging.WARNING)

    logging.getLogger("azure").setLevel(logging.WARNING)

    return logging.getLogger(__name__)


class SensitiveDataFilter(logging.Filter):
    """Filter to redact sensitive information from log messages."""

    SENSITIVE_PATTERNS = [
        "api-key",
        "authorization",
        "bearer",
        "token",
        "password",
        "secret",
        "key=",
        "auth=",
        "code=",
        "azmcpcs=",
    ]

    def filter(self, record):
        if hasattr(record, "msg") and isinstance(record.msg, str):
            msg_lower = record.msg.lower()
            for pattern in self.SENSITIVE_PATTERNS:
                if pattern in msg_lower:
                    # Replace the entire message with a sanitized version
                    record.msg = f"[REDACTED] HTTP request containing sensitive data - {record.name}"
                    break
        return True


# Initialize secure logging
logger = setup_secure_logging()


def sanitize_url(url: str) -> str:
    """Remove sensitive parts of a URL for safe logging."""
    parsed = urlparse(url)
    # Remove auth info (username:password@)
    netloc = (
        parsed.netloc.split("@")[-1] if "@" in parsed.netloc else parsed.netloc
    )
    # Remove token or other secret query params
    query = "&".join([f"{k}=..." for k in parse_qs(parsed.query).keys()])
    return parsed._replace(netloc=netloc, query=query).geturl()


async def main():
    try:
        azure_func_uri = os.environ.get(
            "AZURE_FUNC_URI", "http://localhost:7071/runtime/webhooks/mcp/sse"
        )
        sanitized_url = sanitize_url(azure_func_uri)
        logger.info(f"Connecting to MCP server at {sanitized_url}")

        async with sse_client(azure_func_uri) as (read, write):
            async with ClientSession(read, write) as session:
                try:
                    await session.initialize()
                    logger.info("MCP session initialized successfully")

                    logger.info("Loading tools from MCP server...")
                    tools = await load_mcp_tools(session)
                    logger.info(f"Loaded {len(tools)} tools")

                    deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT")
                    api_version = os.getenv("AZURE_OPENAI_API_VERSION")

                    if not deployment_name or not api_version:
                        logger.error(
                            "Missing required Azure OpenAI environment variables."
                        )
                        return

                    logger.info(
                        f"Using Azure deployment: {deployment_name}, API version: {api_version}"
                    )

                    # Option: Add additional logging configuration for Azure OpenAI client
                    llm = AzureChatOpenAI(
                        azure_deployment=deployment_name,
                        api_version=api_version,
                        timeout=None,
                        max_retries=2,
                        # Disable detailed HTTP logging if available
                        model_kwargs={
                            "stream": False
                        },  # Reduces some logging verbosity
                    )

                    logger.info("Creating ReAct agent...")
                    agent = create_react_agent(llm, tools)

                    logger.info("Invoking agent...")
                    response = await agent.ainvoke(
                        {
                            "messages": [
                                {
                                    "role": "user",
                                    "content": "get the foreign keys from the postgres database",
                                }
                            ]
                        }
                    )

                    logger.info("Agent response received successfully")
                    logger.debug(f"Raw agent response: {response}")

                    result = dumps(response, pretty=True)
                    data = json.loads(result)

                    # Write the data to a JSON file
                    output_path = "result.json"
                    with open(output_path, "w") as json_file:
                        json.dump(data, json_file, indent=4)

                    logger.info(f"Response saved to {output_path}")

                except Exception as e:
                    logger.exception("Error during agent execution: %s", str(e))
    except Exception as e:
        logger.exception("Critical error in main function: %s", str(e))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user.")
    except Exception as e:
        logger.critical("Unhandled exception in asyncio loop: %s", str(e))
