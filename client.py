import asyncio
import json
import os

from dotenv import load_dotenv
from langchain_core.load import dumps
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_openai import AzureChatOpenAI
from langgraph.prebuilt import create_react_agent
from mcp import ClientSession
from mcp.client.sse import sse_client

load_dotenv()


async def main():
    azure_func_uri = os.environ.get(
        "AZURE_FUNC_URI", "http://localhost:7071/runtime/webhooks/mcp/sse"
    )
    async with sse_client(azure_func_uri) as (
        read,
        write,
    ):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await load_mcp_tools(session)
            deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT")
            api_version = os.getenv("AZURE_OPENAI_API_VERSION")
            llm = AzureChatOpenAI(
                azure_deployment=deployment_name,
                api_version=api_version,
                timeout=None,
                max_retries=2,
            )
            agent = create_react_agent(llm, tools)

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
            result = dumps(response, pretty=True)
            data = json.loads(result)

            # Write the data to a JSON file
            with open("result.json", "w") as json_file:
                json.dump(data, json_file, indent=4)


if __name__ == "__main__":
    asyncio.run(main())
