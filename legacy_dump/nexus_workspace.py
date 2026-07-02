import os
import sys
import json
import asyncio
import logging
import subprocess
from pathlib import Path
from typing import Dict, Any, List
from typing_extensions import TypedDict

# Core elite orchestration packages
from litellm import acompletion
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

# Clean telemetry formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [\033[1;36m%(levelname)s\033[0m] %(name)s: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("NexusEngine")

# ==========================================
# 1. STATE MACHINE SCHEMA
# ==========================================
class AgentWorkspaceState(TypedDict):
    user_intent: str
    execution_plan: List[Dict[str, Any]]
    current_step_index: int
    execution_logs: List[str]

# ==========================================
# 2. THE HIGH-SPEED EXECUTION FABRIC
# ==========================================
class EliteWorkspaceNodes:
    def __init__(self, model_name: str = "groq/llama-3.3-70b-versatile"):
        self.model = model_name

    async def dynamic_planner_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        logger.info("\033[1;33m[PLANNER]\033[0m Architecting multi-step tactical plan via Groq...")
        
        system_prompt = (
            "You are an elite terminal-integrated developer agent engine.\n"
            "Deconstruct the user's operational request into explicit tool executions.\n"
            "You must return a raw JSON object matching this schema exactly:\n"
            "{\n"
            '  "steps": [\n'
            '    {"step_id": 1, "tool_name": "terminal.execute", "arguments": {"command": "ls -la"}}\n'
            "  ]\n"
            "}\n"
            "Available System Tools:\n"
            "- 'fs.write_file' -> Args: {'filename': str, 'content': str}\n"
            "- 'terminal.execute' -> Args: {'command': str} (Use for running scripts, git, linting, pip, etc.)\n"
            "CRITICAL: Output only valid raw JSON. Never include chat wrapper text or markdown code blocks."
        )
        
        response = await acompletion(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": state["user_intent"]}
            ],
            temperature=0.0
        )
        
        raw_text = response.choices[0].message.content.strip()
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```json")[-1].split("```")[0].strip()
            
        try:
            plan_data = json.loads(raw_text)
            return {"execution_plan": plan_data.get("steps", []), "current_step_index": 0}
        except Exception as e:
            logger.error(f"JSON parsing structural anomaly: {raw_text}")
            return {"execution_plan": [], "current_step_index": 0}

    async def execution_fabric_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        idx = state["current_step_index"]
        plan = state["execution_plan"]
        current_task = plan[idx]
        
        logger.info(f"\033[1;32m[EXECUTOR]\033[0m Invoking: \033[1;35m{current_task['tool_name']}\033[0m")
        args = current_task.get("arguments", {})
        logs = list(state.get("execution_logs", []))
        
        # Tool 1: File Writer
        if current_task["tool_name"] == "fs.write_file":
            Path(args["filename"]).write_text(args["content"])
            msg = f"Sync Completed: Written target file {args['filename']}"
            logger.info(f" -> {msg}")
            logs.append(msg)
            
        # Tool 2: Native Terminal Shell Execution (Subprocess)
        elif current_task["tool_name"] == "terminal.execute":
            cmd = args.get("command", "")
            logger.info(f" -> Executing system shell command: {cmd}")
            
            # Run the command asynchronously to prevent freezing the agent core
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            output_msg = stdout.decode().strip() or stderr.decode().strip()
            logger.info(f"\033[1;37m[SHELL OUTPUT]:\033[0m\n{output_msg}")
            logs.append(f"Command '{cmd}' exited with code {process.returncode}. Output: {output_msg}")

        return {"current_step_index": idx + 1, "execution_logs": logs}

# ==========================================
# 3. GRAPH COMPOSITION
# ==========================================
def should_continue(state: AgentWorkspaceState) -> str:
    if state["current_step_index"] >= len(state["execution_plan"]):
        return "end"
    return "continue"

def build_workspace_kernel() -> StateGraph:
    nodes = EliteWorkspaceNodes()
    workflow = StateGraph(AgentWorkspaceState)
    workflow.add_node("planner", nodes.dynamic_planner_node)
    workflow.add_node("executor", nodes.execution_fabric_node)
    
    workflow.set_entry_point("planner")
    workflow.add_edge("planner", "executor")
    
    workflow.add_conditional_edges(
        "executor",
        should_continue,
        {"continue": "executor", "end": END}
    )
    return workflow.compile(checkpointer=MemorySaver())

# ==========================================
# 4. INTERACTIVE REPL RUNTIME ENVIRONMENT
# ==========================================
async def start_interactive_shell():
    kernel = build_workspace_kernel()
    config = {"configurable": {"thread_id": "persistent_dev_session"}}
    
    print("\n" + "="*60)
    print("\033[1;32m  NEXUSKERNEL INTERACTIVE REPL INTERFACE ACTIVE\033[0m")
    print("  Engine: Llama-3.3-70B on Groq LPU | Cost: $0.00")
    print("  Type 'exit' or 'quit' to terminate the workspace session.")
    print("="*60 + "\n")

    while True:
        try:
            # Use asyncio run_in_executor to make standard terminal input non-blocking
            user_prompt = await asyncio.get_event_loop().run_in_executor(
                None, lambda: input("\033[1;36mNexusPrompt ❯\033[0m ").strip()
            )
            
            if not user_prompt:
                continue
            if user_prompt.lower() in ["exit", "quit"]:
                print("\n[SYSTEM] Powering down workspace kernel fabric. Session terminated.")
                break

            session_state = {
                "user_intent": user_prompt,
                "execution_plan": [],
                "current_step_index": 0,
                "execution_logs": []
            }

            # Run the agent graph for this prompt turn
            async for _ in kernel.astream(session_state, config):
                pass
                
            print("\n\033[1;32m✔ Operational cycle complete.\033[0m\n")

        except KeyboardInterrupt:
            print("\n[SYSTEM] Session interrupted.")
            break
        except Exception as e:
            logger.error(f"Critical turn error encountered: {str(e)}")

if __name__ == "__main__":
    asyncio.run(start_interactive_shell())