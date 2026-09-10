import logging
import os

from contextlib import AsyncExitStack
from typing import AsyncGenerator

import mlflow

from agents import (
    Agent,
    ModelSettings,
    Runner,
    set_default_openai_api,
    set_default_openai_client,
)
from agents.tracing import set_trace_processors
from databricks.sdk import WorkspaceClient
from databricks_openai import AsyncDatabricksOpenAI
from databricks_openai.agents import McpServer
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from agent_server.history import normalize_history_items
from agent_server.utils import (
    build_mcp_url,
    get_session_id,
    process_agent_stream_events,
)


logger = logging.getLogger(__name__)


LLM_ENDPOINT = "databricks-qwen3-next-80b-a3b-instruct"

# Catalog/schema/index that backs the AI Search MCP tool.
# Configurable via env var so the same code works across dev/staging/prod
# without editing the source.
AI_SEARCH_MCP_PATH = os.environ.get(
    "AI_SEARCH_MCP_PATH",
    "/api/2.0/mcp/ai-search/dbr_dev/yanquiel_silver/rag_documents_index",
)


set_default_openai_client(
    AsyncDatabricksOpenAI()
)

set_default_openai_api(
    "chat_completions"
)

set_trace_processors([])

mlflow.openai.autolog()

logging.getLogger(
    "mlflow.utils.autologging_utils"
).setLevel(logging.ERROR)


async def init_ai_search_mcp_server(
    workspace_client: WorkspaceClient,
) -> McpServer:
    return McpServer(
        url=build_mcp_url(
            AI_SEARCH_MCP_PATH,
            workspace_client=workspace_client,
        ),
        name="Data Platform AI Search",
        workspace_client=workspace_client,
    )


async def connect_healthy_mcp_servers(
    stack: AsyncExitStack,
    servers: list[McpServer],
) -> tuple[list[McpServer], list[str]]:
    """
    Connect to the configured MCP servers and verify
    that their tools are available.

    Servers that cannot be reached or used are skipped
    so they do not cause the entire agent request to fail.

    Returns:
        A tuple containing:
        - healthy MCP servers
        - names of unavailable MCP servers
    """

    healthy: list[McpServer] = []
    unavailable: list[str] = []

    for server in servers:
        name = getattr(
            server,
            "name",
            "MCP server",
        )

        try:
            connected = await stack.enter_async_context(
                server
            )

            await connected.list_tools()

            healthy.append(
                connected
            )

        except Exception:
            logger.warning(
                "MCP server %r unavailable; "
                "continuing without it.",
                name,
                exc_info=True,
            )

            unavailable.append(
                name
            )

    return healthy, unavailable


def create_agent(
    mcp_servers: list[McpServer] | None = None,
) -> Agent:
    return Agent(
        name="Data Platform RAG Agent",
        instructions=(
            "You are the assistant for the Data Platform project. "

            "You MUST retrieve information from the Data Platform "
            "AI Search tool before answering the user's question. "

            "Base project-related answers only on information retrieved "
            "from the project documentation. "

            "Do not answer project questions using assumptions or "
            "general knowledge when the information is not present "
            "in the retrieved documentation. "

            "Do not invent project details, table names, implementation "
            "details, technologies, values, or behavior that are not "
            "supported by the retrieved documentation. "

            "If the retrieved documentation does not contain enough "
            "information to answer the question, clearly say that the "
            "available project documentation does not provide enough "
            "information. "

            "Keep answers clear, concise, and practical. "

            "Prefer short paragraphs and bullet points when they improve "
            "readability. Avoid unnecessarily long explanations unless "
            "the user explicitly asks for more detail. "

            "When answering using information retrieved from AI Search, "
            "finish the answer with a section titled 'Sources:'. "

            "Under 'Sources:', list the document_name values of the "
            "documents actually used to produce the answer. "

            "List each source only once, even if multiple retrieved "
            "chunks came from the same document. "

            "Do not invent source names. "

            "Only include document names that appear in the AI Search "
            "results for the current question. "

            "If no source was actually used, do not create an empty "
            "'Sources:' section."
        ),
        model=LLM_ENDPOINT,
        model_settings=ModelSettings(
            tool_choice="required",
        ),
        mcp_servers=mcp_servers or [],
    )


@invoke()
async def invoke_handler(
    request: ResponsesAgentRequest,
) -> ResponsesAgentResponse:
    if session_id := get_session_id(
        request
    ):
        mlflow.update_current_trace(
            metadata={
                "mlflow.trace.session": session_id,
            }
        )

    async with AsyncExitStack() as stack:
        workspace_client = WorkspaceClient()

        ai_search_server = (
            await init_ai_search_mcp_server(
                workspace_client
            )
        )

        servers, unavailable = (
            await connect_healthy_mcp_servers(
                stack,
                [ai_search_server],
            )
        )

        if unavailable:
            logger.warning(
                "Unavailable MCP servers: %s",
                unavailable,
            )

        if not servers:
            raise RuntimeError(
                "AI Search MCP server is unavailable. "
                "The RAG agent cannot answer without retrieval."
            )

        agent = create_agent(
            mcp_servers=servers
        )

        messages = normalize_history_items(
            [
                item.model_dump()
                for item in request.input
            ]
        )

        try:
            result = await Runner.run(
                agent,
                messages,
            )

        except Exception:
            logger.exception(
                "Agent run failed."
            )
            raise

        return ResponsesAgentResponse(
            output=[
                item.to_input_item()
                for item in result.new_items
            ]
        )


@stream()
async def stream_handler(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[
    ResponsesAgentStreamEvent,
    None,
]:
    if session_id := get_session_id(
        request
    ):
        mlflow.update_current_trace(
            metadata={
                "mlflow.trace.session": session_id,
            }
        )

    async with AsyncExitStack() as stack:
        workspace_client = WorkspaceClient()

        ai_search_server = (
            await init_ai_search_mcp_server(
                workspace_client
            )
        )

        servers, unavailable = (
            await connect_healthy_mcp_servers(
                stack,
                [ai_search_server],
            )
        )

        if unavailable:
            logger.warning(
                "Unavailable MCP servers: %s",
                unavailable,
            )

        if not servers:
            raise RuntimeError(
                "AI Search MCP server is unavailable. "
                "The RAG agent cannot answer without retrieval."
            )

        agent = create_agent(
            mcp_servers=servers
        )

        messages = normalize_history_items(
            [
                item.model_dump()
                for item in request.input
            ]
        )

        try:
            result = Runner.run_streamed(
                agent,
                input=messages,
            )

            async for event in process_agent_stream_events(
                result.stream_events()
            ):
                yield event

        except Exception:
            logger.exception(
                "Agent stream failed."
            )
            raise