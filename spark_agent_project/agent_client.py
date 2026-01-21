import os
import sys
import asyncio
from typing import Literal, Annotated

from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import ToolNode

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# Load environment variables
load_dotenv()

# --- Configuration ---
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4o")
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    print("Warning: API_KEY not set in .env. LLM calls may fail.")

# --- System Prompt ---
SYSTEM_PROMPT = """You are an expert Spark Performance Tuning Agent. 
Your goal is to analyze Spark application logs and provide actionable optimization advice.

Follow this reasoning framework:
1. **Diagnosis**: First, gather facts using available tools. 
   - If the user asks "why is it slow", check `get_stage_metrics` first.
   - Check `get_driver_logs` for errors.
2. **Analysis**:
   - **Data Skew**: If a Stage's Skew Ratio (Max/Median duration) > 5, this is a strong indicator of data skew.
   - **Memory/Spill**: If `total_disk_spill_mb` > 0, the task is running out of execution memory. Suggest increasing `spark.executor.memory` or tuning `spark.memory.fraction`.
   - **Shuffle**: If shuffle read is huge, suggest broadcasting smaller tables or optimizing joins.
   - **Committer**: If tasks are fast but the job hangs at the end, check Driver logs for "V2ScanPartitioningAndOrdering" or S3 committer issues.
3. **Recommendation**: Provide specific configuration changes (e.g., `set spark.sql.shuffle.partitions = 2000`).

General Rules:
- Parallelism (partitions) should generally be 2-3x the total number of cores.
- For 1T+ datasets, suggest enabling Off-Heap memory.
- Be concise but technical.
"""

async def run_agent():
    # 1. Initialize LLM
    try:
        model = init_chat_model(MODEL_NAME, model_provider=LLM_PROVIDER)
    except Exception as e:
        print(f"Error initializing model: {e}")
        print("Please ensure you have installed the correct provider package (e.g., langchain-openai) and set the API key.")
        sys.exit(1)

    # 2. Connect to MCP Server
    # We run the server script as a subprocess
    server_params = StdioServerParameters(
        command="uv", # Or "python" if using venv directly
        args=["run", "mcp_server.py"], # Assuming 'uv run' handles deps, or just python
        env=os.environ
    )
    
    # Fallback to python if uv is not desired/available, user can adjust
    # Check if we are in a virtualenv or just use sys.executable
    server_params.command = sys.executable
    server_params.args = ["mcp_server.py"]

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            # 3. Discover Tools via MCP
            mcp_tools = await session.list_tools()
            langchain_tools = []

            for tool in mcp_tools.tools:
                # Wrap MCP tool as LangChain StructuredTool
                # Note: We need to bind the tool execution to the session
                
                async def make_tool_func(tool_name):
                     async def _impl(**kwargs):
                        return await session.call_tool(tool_name, arguments=kwargs)
                     return _impl
                
                tool_func = await make_tool_func(tool.name)
                
                lc_tool = StructuredTool.from_function(
                    func=None,
                    coroutine=tool_func,
                    name=tool.name,
                    description=tool.description
                )
                langchain_tools.append(lc_tool)
            
            # Bind tools to LLM
            model_with_tools = model.bind_tools(langchain_tools)

            # 4. Build LangGraph Agent
            
            def agent_node(state: MessagesState):
                return {"messages": [model_with_tools.invoke(state["messages"])]}

            # Prebuilt ToolNode executes the tools
            tool_node = ToolNode(langchain_tools)

            def should_continue(state: MessagesState) -> Literal["tools", END]:
                messages = state["messages"]
                last_message = messages[-1]
                if last_message.tool_calls:
                    return "tools"
                return END

            workflow = StateGraph(MessagesState)
            workflow.add_node("agent", agent_node)
            workflow.add_node("tools", tool_node)

            workflow.add_edge(START, "agent")
            workflow.add_conditional_edges("agent", should_continue)
            workflow.add_edge("tools", "agent")

            checkpointer = MemorySaver()
            app = workflow.compile(checkpointer=checkpointer)

            # 5. Interactive Loop
            config = {"configurable": {"thread_id": "1"}}
            
            print(f"Agent ready! (Model: {MODEL_NAME})")
            print("Enter a Spark App ID or a question (type 'exit' to quit).")

            # Initial greeting context
            # await app.ainvoke({"messages": [SystemMessage(content=SYSTEM_PROMPT)]}, config)
            # Actually, better to prepend system prompt to subsequent calls or handle state init
            
            # We inject system prompt by adding it to the history once or prepending it
            # For simplicity, we just assume the first turn sets it up or we pass it every time if the model supports it.
            # LangGraph's MessagesState appends. Let's add the SystemPrompt manually at the start of conversation.
            
            print("---")
            
            # Pre-seed system message
            # Note: We do this by manually managing the state update on first run if needed,
            # or just letting the user start and we insert the system prompt in the logic.
            # Here we will insert it as the first message of the conversation.
            
            first_run = True

            while True:
                try:
                    user_input = input("User: ")
                except EOFError:
                    break
                    
                if user_input.lower() in ["exit", "quit", "q"]:
                    break
                
                inputs = {"messages": [HumanMessage(content=user_input)]}
                if first_run:
                    inputs = {"messages": [SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=user_input)]}
                    first_run = False

                async for event in app.astream(inputs, config, stream_mode="values"):
                    # Stream the last message content to show progress
                    message = event["messages"][-1]
                    if isinstance(message, BaseMessage) and message.content:
                        # Only print if it's from AI and has content (not just tool call)
                        # But in 'values' mode we get the full state.
                        # Simplified print logic:
                        pass
                
                # Print final response
                final_state = app.get_state(config)
                last_msg = final_state.values["messages"][-1]
                print(f"Agent: {last_msg.content}")
                print("---")

if __name__ == "__main__":
    asyncio.run(run_agent())
