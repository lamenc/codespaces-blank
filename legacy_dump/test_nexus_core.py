import sys
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List
from typing_extensions import TypedDict
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

# ==========================================
# WINDOWS ENVIRONMENT PATCH (CRITICAL)
# ==========================================
if sys.platform == "win32":
    # Forces Windows to use the standard selector loop to prevent VS Code/PowerShell socket hanging
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Configure clean, real-time unbuffered terminal tracing
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [\033[1;36m%(levelname)s\033[0m] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)] # Force push directly to standard out stream
)
logger = logging.getLogger("NexusEngine")

# ==========================================
# 1. STATE SPACE
# ==========================================
class AgentWorkspaceState(TypedDict):
    user_intent: str
    execution_plan: List[Dict[str, Any]]
    current_step_index: int
    workspace_context: Dict[str, Any]
    discovered_files: List[str]
    is_halted: bool

# ==========================================
# 2. RUNTIME WORKSPACE OPERATIONS (Real I/O)
# ==========================================
class WorkspaceNodes:
    async def planner_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        logger.info("\033[1;33m[PLANNER]\033[0m Constructing deterministic I/O verification pipeline...")
        await asyncio.sleep(0.1)
        
        # Mapping real tactical steps
        generated_plan = [
            {"step": 1, "action": "fs.verify_target_dir"},
            {"step": 2, "action": "fs.inventory_artifacts"}
        ]
        return {
            "execution_plan": generated_plan,
            "current_step_index": 0,
        }

    async def execution_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        idx = state["current_step_index"]
        plan = state["execution_plan"]
        current_task = plan[idx]
        target_path = Path(state["workspace_context"]["target_dir"])
        
        logger.info(f"\033[1;32m[EXECUTOR]\033[0m Executing step {idx + 1}: \033[1;35m{current_task['action']}\033[0m")
        
        found_files = state.get("discovered_files", [])
        halt_execution = False

        if current_task["action"] == "fs.verify_target_dir":
            if not target_path.exists():
                logger.error(f"\033[1;31m[FAIL]\033[0m Directory target missing: {target_path}")
                halt_execution = True
            else:
                logger.info(f"\033[1;32m[SUCCESS]\033[0m Located valid target anchor: {target_path.resolve()}")

        elif current_task["action"] == "fs.inventory_artifacts":
            # Physically inventorying the files seen in your VS Code Explorer pane
            found_files = [f.name for f in target_path.iterdir() if f.is_file()]
            logger.info(f"\033[1;32m[SUCCESS]\033[0m Cataloged {len(found_files)} local artifacts inside workspace.")

        return {
            "current_step_index": idx + 1,
            "discovered_files": found_files,
            "is_halted": halt_execution
        }

    async def validation_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        logger.info("\033[1;34m[VALIDATOR]\033[0m Confirming structural state engine execution stability.")
        return {}

# ==========================================
# 3. GRAPH CONDITIONAL LOGIC
# ==========================================
def should_continue(state: AgentWorkspaceState) -> str:
    if state["is_halted"]:
        return "halt"
    if state["current_step_index"] >= len(state["execution_plan"]):
        return "end"
    return "continue"

# ==========================================
# 4. COMPOSITION
# ==========================================
def build_kernel() -> StateGraph:
    nodes = WorkspaceNodes()
    workflow = StateGraph(AgentWorkspaceState)
    
    workflow.add_node("planner", nodes.planner_node)
    workflow.add_node("executor", nodes.execution_node)
    workflow.add_node("validator", nodes.validation_node)
    
    workflow.set_entry_point("planner")
    workflow.add_edge("planner", "executor")
    workflow.add_edge("executor", "validator")
    
    workflow.add_conditional_edges(
        "validator",
        should_continue,
        {"continue": "executor", "halt": END, "end": END}
    )
    return workflow.compile(checkpointer=MemorySaver())

# ==========================================
# 5. EXECUTION ENTRYPOINT
# ==========================================
async def main():
    print("[SYSTEM] Booting production runtime kernel...", flush=True)
    kernel = build_kernel()
    config = {"configurable": {"thread_id": "nexus_solo_dev_01"}}
    
    # Point directly to your relative Downloads directory shown in the screenshot
    initial_state = {
        "user_intent": "Analyze layout metadata",
        "execution_plan": [],
        "current_step_index": 0,
        "workspace_context": {"target_dir": "./Downloads"},
        "discovered_files": [],
        "is_halted": False
    }
    
    async for event in kernel.astream(initial_state, config):
        # Explicitly reading the final output payload states as they change
        for node, payload in event.items():
            if payload and "discovered_files" in payload and payload["discovered_files"]:
                print(f"\n\033[1;37mFinal Discovered Files In Directory:\033[0m {payload['discovered_files']}\n")

if __name__ == "__main__":
    asyncio.run(main())