import os
import sys
import json
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List
from typing_extensions import TypedDict

# Core elite orchestration packages
from litellm import acompletion
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [\033[1;36m%(levelname)s\033[0m] %(name)s: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("NexusGroqKernel")

# ==========================================
# 1. STATE MACHINE SCHEMA
# ==========================================
class AgentWorkspaceState(TypedDict):
    user_intent: str
    execution_plan: List[Dict[str, Any]]
    current_step_index: int
    workspace_context: Dict[str, Any]
    execution_logs: List[str]

# ==========================================
# 2. HIGH-SPEED CLOUD GENERATION NODES
# ==========================================
class GroqAIWorkspaceNodes:
    def __init__(self, model_name: str = "groq/llama-3.3-70b-versatile"):
        """Leverages high-capacity 70B parameters completely free via Groq."""
        self.model = model_name

    async def cloudburst_planner_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        logger.info("\033[1;33m[GROQ LPU PLANNER]\033[0m Querying cloud microsecond matrix to build plan...")
        
        system_prompt = (
            "You are an elite autonomous programming agent workspace router.\n"
            "Analyze the user's intent and break it down into explicit structural steps.\n"
            "You must return a raw JSON object matching this schema exactly:\n"
            "{\n"
            '  "steps": [\n'
            '    {"step_id": 1, "tool_name": "fs.create_file", "arguments": {"filename": "app.py", "content": "print(1)"}}\n'
            "  ]\n"
            "}\n"
            "Available System Tools:\n"
            "- 'fs.create_file' -> Args: {'filename': str, 'content': str}\n"
            "- 'fs.read_directory' -> Args: {}\n"
            "CRITICAL: Output only valid raw JSON. Do not include conversational text or markdown code wraps."
        )
        
        # Fire off to Groq's ultra-low latency LPUs
        response = await acompletion(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": state["user_intent"]}
            ],
            temperature=0.0  # Zero out entropy for perfect structural syntax
        )
        
        raw_text = response.choices[0].message.content.strip()
        
        # Sanitize markdown formatting if the model slips up
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```json")[-1].split("```")[0].strip()
            
        try:
            plan_data = json.loads(raw_text)
            steps = plan_data.get("steps", [])
            logger.info(f"\033[1;32m[PLAN ACQUIRED]\033[0m Successfully parsed {len(steps)} steps via Groq.")
            return {"execution_plan": steps, "current_step_index": 0}
        except Exception as e:
            logger.error(f"Failed to parse model raw text token output: {raw_text}")
            # Safe defensive fallback action
            return {"execution_plan": [{"step_id": 1, "tool_name": "fs.read_directory", "arguments": {}}], "current_step_index": 0}

    async def tool_execution_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        idx = state["current_step_index"]
        plan = state["execution_plan"]
        current_task = plan[idx]
        
        logger.info(f"\033[1;32m[FABRIC EXECUTOR]\033[0m Executing tool: \033[1;35m{current_task['tool_name']}\033[0m")
        args = current_task.get("arguments", {})
        logs = list(state.get("execution_logs", []))
        
        # ── REAL WORKSPACE DISK I/O EXECUTIONS
        if current_task["tool_name"] == "fs.create_file":
            target_path = Path(args.get("filename", "output.txt"))
            target_path.write_text(args.get("content", ""))
            msg = f"File system sync clean: {target_path.resolve()}"
            logger.info(f" -> {msg}")
            logs.append(msg)
            
        elif current_task["tool_name"] == "fs.read_directory":
            files = [f.name for f in Path(".").iterdir() if f.is_file()]
            msg = f"Discovered files on local path: {files}"
            logger.info(f" -> {msg}")
            logs.append(msg)

        return {
            "current_step_index": idx + 1,
            "execution_logs": logs
        }

# ==========================================
# 3. ROUTING AND ARCHITECTURE COMPOSITION
# ==========================================
def should_continue(state: AgentWorkspaceState) -> str:
    if state["current_step_index"] >= len(state["execution_plan"]):
        return "end"
    return "continue"

def build_groq_kernel(nodes: GroqAIWorkspaceNodes) -> StateGraph:
    workflow = StateGraph(AgentWorkspaceState)
    workflow.add_node("planner", nodes.cloudburst_planner_node)
    workflow.add_node("executor", nodes.tool_execution_node)
    
    workflow.set_entry_point("planner")
    workflow.add_edge("planner", "executor")
    
    workflow.add_conditional_edges(
        "executor",
        should_continue,
        {"continue": "executor", "end": END}
    )
    return workflow.compile(checkpointer=MemorySaver())

# ==========================================
# 4. EXECUTION LOOP
# ==========================================
async def main():
    print("[SYSTEM] Booting Sovereign Free Cloudburst Kernel...", flush=True)
    
    # Using Llama 3.3 70B for deep developer engineering reasoning tasks
    nodes = GroqAIWorkspaceNodes(model_name="groq/llama-3.3-70b-versatile")
    kernel = build_groq_kernel(nodes)
    config = {"configurable": {"thread_id": "groq_session_01"}}
    
    initial_state = {
        "user_intent": "Create a file named cloudburst.txt with the content 'Computing for free at 500 tokens per second!', then scan the directory.",
        "execution_plan": [],
        "current_step_index": 0,
        "workspace_context": {},
        "execution_logs": []
    }
    
    async for _ in kernel.astream(initial_state, config):
        pass

if __name__ == "__main__":
    if "GROQ_API_KEY" not in os.environ:
        print("\033[1;31m[CRITICAL]\033[0m Missing GROQ_API_KEY. Please export it in your terminal before running.")
        sys.exit(1)
    asyncio.run(main())