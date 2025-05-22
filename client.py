import asyncio
import os

from dotenv import load_dotenv
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_openai import AzureChatOpenAI
from langgraph.prebuilt import create_react_agent
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.client.sse import sse_client

load_dotenv()


async def main():
    async with sse_client("http://localhost:7071/runtime/webhooks/mcp/sse") as (
        read,
        write,
    ):
        async with ClientSession(read, write) as session:
            # セッションを初期化
            await session.initialize()

            # MCPからtools情報を取得
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
                        {"role": "user", "content": "what's (3 + 5) x 12?"}
                    ]
                }
            )
            print(response)


if __name__ == "__main__":
    asyncio.run(main())
