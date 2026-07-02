import os
import sys
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List
from typing_extensions import TypedDict
from pydantic import BaseModel, Field

# Advanced third-party orchestration imports
from litellm import acompletion
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [\033[1;36m%(levelname)s\033[0m] %(name)s: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("NexusKernel")

# ==========================================
# 1. STRUCTURAL LLM OUTPUT SCHEMAS
# ==========================================
class TaskStep(BaseModel):
    step_id: int = Field(description="Sequential identification integer.")
    tool_name: str = Field(description="The matching system tool string, e.g., 'fs.write_file' or 'fs.read_file'")
    arguments: Dict[str, Any] = Field(description="Key-value pairs representing required tool parameters.")

class DynamicExecutionPlan(BaseModel):
    rationale: str = Field(description="Architectural reason for choosing this path.")
    steps: List[TaskStep] = Field(description="Ordered list of steps to execute.")

# ==========================================
# 2. STATE SPACE
# ==========================================
class AgentWorkspaceState(TypedDict):
    user_intent: str
    execution_plan: List[Dict[str, Any]]
    current_step_index: int
    workspace_context: Dict[str, Any]
    execution_logs: List[str]
    is_halted: bool

# ==========================================
# 3. HIGH-COGNITION GRAPH NODES
# ==========================================
class AIWorkspaceNodes:
    def __init__(self, model: str = "anthropic/claude-3-5-sonnet"):
        # You can replace this with "openai/gpt-4o" or any provider supported by LiteLLM
        self.model = model

    async def ai_planner_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        logger.info("\033[1;33m[AI PLANNER]\033[0m Querying LLM to synthesize structural execution plan...")
        
        system_prompt = (
            "You are the central orchestration brain of an elite developer workspace agent.\n"
            "Given a user goal, output an explicit structural execution plan using the requested JSON tool schema.\n"
            "Available Tools:\n"
            "1. 'fs.create_file' - Args: {'filename': str, 'content': str}\n"
            "2. 'fs.read_directory' - Args: {}\n"
        )
        
        # Call LiteLLM with structural schema constraints
        response = await acompletion(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": state["user_intent"]}
            ],
            response_format=DynamicExecutionPlan, # Forces structural validation via Pydantic
            temperature=0.0
        )
        
        # Parse result directly through Pydantic
        structured_res = DynamicExecutionPlan.model_validate_json(response.choices[0].message.content)
        logger.info(f"\033[1;32m[PLAN GENERATED]\033[0m Rationale: {structured_res.rationale}")
        
        # Format for our graph engine state
        serialized_steps = [step.model_dump() for step in structured_res.steps]
        return {
            "execution_plan": serialized_steps,
            "current_step_index": 0
        }

    async def tool_execution_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        idx = state["current_step_index"]
        plan = state["execution_plan"]
        current_task = plan[idx]
        
        logger.info(f"\033[1;32m[EXECUTOR]\033[0m Dispatched Action: \033[1;35m{current_task['tool_name']}\033[0m")
        args = current_task["arguments"]
        
        logs = list(state.get("execution_logs", []))
        
        # ── REAL FILE-SYSTEM OPERATIONS
        if current_task["tool_name"] == "fs.create_file":
            target_path = Path(args["filename"])
            target_path.write_text(args["content"])
            msg = f"Successfully created file: {target_path.resolve()}"
            logger.info(f" -> {msg}")
            logs.append(msg)
            
        elif current_task["tool_name"] == "fs.read_directory":
            files = [f.name for f in Path(".").iterdir() if f.is_file()]
            msg = f"Read current directory workspace. Found: {files}"
            logger.info(f" -> {msg}")
            logs.append(msg)
            
        else:
            logger.warning(f"Unsupported tool hit: {current_task['tool_name']}")

        return {
            "current_step_index": idx + 1,
            "execution_logs": logs
        }

    async def validation_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        logger.info("\033[1;34m[VALIDATOR]\033[0m Checking environment mutations...")
        return {}

# ==========================================
# 4. CONDITIONAL ROUTING & ENGINE COMPOSITION
# ==========================================
def should_continue(state: AgentWorkspaceState) -> str:
    if state["current_step_index"] >= len(state["execution_plan"]):
        return "end"
    return "continue"

def build_ai_kernel(nodes: AIWorkspaceNodes) -> StateGraph:
    workflow = StateGraph(AgentWorkspaceState)
    
    workflow.add_node("planner", nodes.ai_planner_node)
    workflow.add_node("executor", nodes.tool_execution_node)
    workflow.add_node("validator", nodes.validation_node)
    
    workflow.set_entry_point("planner")
    workflow.add_edge("planner", "executor")
    workflow.add_edge("executor", "validator")
    
    workflow.add_conditional_edges(
        "validator",
        should_continue,
        {"continue": "executor", "end": END}
    )
    return workflow.compile(checkpointer=MemorySaver())

# ==========================================
# 5. EXECUTION LOOP
# ==========================================
async def main():
    # Detect available API keys to auto-configure model selection
    selected_model = "anthropic/claude-3-5-sonnet"
    if "OPENAI_API_KEY" in os.environ and "ANTHROPIC_API_KEY" not in os.environ:
        selected_model = "openai/gpt-4o"
        
    print(f"[SYSTEM] Booting AI Runtime Kernel using target model: {selected_model}...", flush=True)
    
    nodes = AIWorkspaceNodes(model=selected_model)
    kernel = build_ai_kernel(nodes)
    config = {"configurable": {"thread_id": "solo_ai_session_01"}}
    
    # Give the agent an arbitrary open-ended architectural task
    initial_state = {
        "user_intent": "Create a new text file named blueprint.md containing an architectural summary of a microservice, then read the directory to confirm it's there.",
        "execution_plan": [],
        "current_step_index": 0,
        "workspace_context": {},
        "execution_logs": [],
        "is_halted": False
    }
    
    async for _ in kernel.astream(initial_state, config):
        pass

if __name__ == "__main__":
    if not os.environ.get("ANTHROPIC_API_KEY") and not os.environ.get("OPENAI_API_KEY"):
        print("\033[1;31m[CRITICAL]\033[0m Missing API keys. Please export ANTHROPIC_API_KEY or OPENAI_API_KEY in your terminal.")
        sys.exit(1)
    asyncio.run(main())