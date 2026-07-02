
import json
import logging
from typing import List, Dict, Any, Callable

from litellm import acompletion

from kernel.state import AgentWorkspaceState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EliteSovereignNodes:
    def __init__(self, model_name: str = 'gemini-1.5-pro'):
        self.model_name = model_name
        self.available_tools: Dict[str, Callable] = {}

    def register_tool(self, tool_name: str, tool_function: Callable):
        self.available_tools[tool_name] = tool_function

    async def dynamic_planner_node(self, state: AgentWorkspaceState) -> List[Dict[str, Any]]:
        system_prompt = """
        You are an AI assistant that generates a JSON-based execution plan (a list of tool calls).
        Use a ReAct-style single-step planning approach, generating only one tactical tool call at a time.
        Return raw JSON. The JSON should be a list of dictionaries, where each dictionary represents a tool call.
        Each tool call should have 'tool_name' and 'arguments' keys.
        Example: [{"tool_name": "read_file", "arguments": {"path": "/path/to/file.txt"}}]
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Current state: {state.to_dict()}"}
        ]
        try:
            response = await acompletion(
                model=self.model_name,
                messages=messages,
                response_model=List[Dict[str, Any]] # Expecting a list of tool calls
            )
            return response
        except Exception as e:
            logger.error(f"Error generating execution plan: {e}")
            return []

    async def execution_fabric_node(self, state: AgentWorkspaceState):
        if not state.execution_plan:
            return

        current_step = state.execution_plan.pop(0)
        tool_name = current_step.get("tool_name")
        arguments = current_step.get("arguments", {})

        if tool_name not in self.available_tools:
            state.last_error = f"Tool '{tool_name}' not registered."
            state.retry_count += 1
            logger.error(state.last_error)
            return

        tool_function = self.available_tools[tool_name]
        try:
            result = await tool_function(**arguments)
            state.execution_logs.append({"tool": tool_name, "arguments": arguments, "result": result})
            state.last_error = None
            state.retry_count = 0
        except Exception as e:
            import traceback
            error_message = f"Error executing tool '{tool_name}': {e}\n{traceback.format_exc()}"
            state.last_error = error_message
            state.retry_count += 1
            logger.error(error_message)
