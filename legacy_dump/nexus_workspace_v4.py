import os
import sys
import json
import re
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
logger = logging.getLogger("NexusEngine")

# ==========================================
# 1. STATE MACHINE SCHEMA (Persistent)
# ==========================================
class AgentWorkspaceState(TypedDict):
    user_intent: str
    execution_plan: List[Dict[str, Any]]
    current_step_index: int
    execution_logs: List[str]          # Shared chronological execution ledger
    project_manifest: List[str]        # Keeps track of files the agent has modified/created
    last_error: str
    crashed_file_context: str
    retry_count: int

# ==========================================
# 2. PERSISTENT WORKSPACE LOGIC NODES
# ==========================================
class EliteMemoryNodes:
    def __init__(self, model_name: str = "groq/llama-3.3-70b-versatile"):
        self.model = model_name

    async def dynamic_planner_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        logs = state.get("execution_logs", [])
        manifest = state.get("project_manifest", [])
        last_error = state.get("last_error", "")
        file_ctx = state.get("crashed_file_context", "")

        # Base instructions clarifying system omniscience and memory tools
        system_instruction = (
            "You are an elite autonomous programming agent workspace. You have access to persistent memory.\n"
            "Review the project manifest containing files built in previous turns, plus execution logs.\n"
            "Deconstruct the prompt into tool execution steps.\n"
            "Return a raw JSON object matching this schema exactly:\n"
            "{\n"
            '  "steps": [\n'
            '    {"step_id": 1, "tool_name": "fs.write_file", "arguments": {"filename": "app.py", "content": "print(1)"}},\n'
            '    {"step_id": 2, "tool_name": "terminal.execute", "arguments": {"command": "python app.py"}}\n'
            '  ]\n'
            "}\n"
            "Available System Tools: 'fs.write_file', 'fs.read_file', 'terminal.execute'\n"
            "CRITICAL: Output only valid raw JSON. Never include chat wrapper text or markdown code blocks."
        )

        if last_error:
            logger.warning(f"\033[1;31m[DEBUGGER ROUTE]\033[0m Resolving stack-trace anomaly...")
            prompt = (
                f"--- CRASH TELEMETRY ---\n{last_error}\n\n"
                f"--- SOURCE CODE AFFECTED ---\n{file_ctx}\n\n"
                f"Historical Logs:\n{json.dumps(logs)}"
            )
        else:
            logger.info("\033[1;33m[PLANNER]\033[0m Ingesting memory vectors and evaluating delta variations...")
            prompt = (
                f"Current Project File Manifest: {json.dumps(manifest)}\n"
                f"Historical Execution Logs:\n{json.dumps(logs[-5:])}\n\n" # Send the last 5 logs to save token context window
                f"New User Intent: {state['user_intent']}"
            )

        response = await acompletion(model=self.model, messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt}
        ], temperature=0.0)
        
        raw_text = response.choices[0].message.content.strip()
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```json")[-1].split("```")[0].strip()
            
        try:
            plan_data = json.loads(raw_text)
            return {"execution_plan": plan_data.get("steps", []), "current_step_index": 0}
        except Exception:
            logger.error("JSON parsing structural anomaly from model stream.")
            return {"execution_plan": [], "current_step_index": 0}

    async def execution_fabric_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        idx = state["current_step_index"]
        plan = state["execution_plan"]
        current_task = plan[idx]
        
        logger.info(f"\033[1;32m[EXECUTOR]\033[0m Invoking: \033[1;35m{current_task['tool_name']}\033[0m")
        args = current_task.get("arguments", {})
        
        logs = list(state.get("execution_logs", []))
        manifest = list(state.get("project_manifest", []))
        error_signal = ""
        crashed_code_snapshot = state.get("crashed_file_context", "")

        if current_task["tool_name"] == "fs.write_file":
            filename = args["filename"]
            Path(filename).write_text(args["content"])
            msg = f"Synchronized file change to disk: {filename}"
            logger.info(f" -> {msg}")
            logs.append(msg)
            if filename not in manifest:
                manifest.append(filename)
            
        elif current_task["tool_name"] == "fs.read_file":
            filename = args["filename"]
            if Path(filename).exists():
                content = Path(filename).read_text()
                logs.append(f"Read file '{filename}' content successfully.")
            else:
                error_signal = f"File {filename} does not exist."

        elif current_task["tool_name"] == "terminal.execute":
            cmd = args.get("command", "")
            process = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            stdout_str = stdout.decode().strip()
            stderr_str = stderr.decode().strip()
            
            if process.returncode != 0:
                error_signal = f"Exit Code {process.returncode}.\nSTDERR:\n{stderr_str}\nSTDOUT:\n{stdout_str}"
                logger.error(f" -> \033[1;31mExecution runtime error encountered.\033[0m")
                
                file_match = re.search(r'File "([^"]+)", line (\d+)', stderr_str)
                if file_match:
                    target_file = file_match.group(1)
                    if Path(target_file).exists():
                        crashed_code_snapshot = Path(target_file).read_text()
            else:
                logger.info(f"\033[1;37m[SHELL OUTPUT]:\033[0m\n{stdout_str}")
                logs.append(f"Command '{cmd}' executed successfully. Output: {stdout_str}")

        return {
            "current_step_index": idx + 1,
            "execution_logs": logs,
            "project_manifest": manifest,
            "last_error": error_signal,
            "crashed_file_context": crashed_code_snapshot
        }

# ==========================================
# 3. GRAPH CONDITIONAL TRIAGE
# ==========================================
def triage_next_step(state: AgentWorkspaceState) -> str:
    if state.get("last_error"):
        if state.get("retry_count", 0) >= 3:
            return "halt"
        return "heal"
    if state["current_step_index"] >= len(state["execution_plan"]):
        return "end"
    return "continue"

def build_persistent_kernel() -> StateGraph:
    nodes = EliteMemoryNodes()
    workflow = StateGraph(AgentWorkspaceState)
    
    workflow.add_node("planner", nodes.dynamic_planner_node)
    workflow.add_node("executor", nodes.execution_fabric_node)
    
    workflow.set_entry_point("planner")
    workflow.add_edge("planner", "executor")
    
    workflow.add_conditional_edges(
        "executor",
        triage_next_step,
        {"continue": "executor", "heal": "planner", "end": END, "halt": END}
    )
    return workflow.compile(checkpointer=MemorySaver())

# ==========================================
# 4. PERSISTENT CONTEXT REPL RUNTIME
# ==========================================
async def start_workspace():
    kernel = build_persistent_kernel()
    config = {"configurable": {"thread_id": "global_sovereign_session"}}
    
    print("\n" + "="*60)
    print("\033[1;32m  NEXUSKERNEL CORE V4: MULTI-TURN MEMORY CORE ACTIVE\033[0m")
    print("  Feature: Cross-Prompt State Persistence and Manifest Tracking")
    print("="*60 + "\n")

    # Initialize the base global persistent state structure across prompt boundaries
    global_state = {
        "user_intent": "", "execution_plan": [],
        "current_step_index": 0, "execution_logs": [],
        "project_manifest": [], "last_error": "",
        "crashed_file_context": "", "retry_count": 0
    }

    while True:
        try:
            user_prompt = await asyncio.get_event_loop().run_in_executor(
                None, lambda: input("\033[1;36mNexusPrompt ❯\033[0m ").strip()
            )
            if user_prompt.lower() in ["exit", "quit"]:
                break
            if not user_prompt:
                continue

            # Load the previous session memory context state updates directly into the turn execution channel
            global_state["user_intent"] = user_prompt
            global_state["execution_plan"] = []
            global_state["current_step_index"] = 0
            global_state["last_error"] = ""
            global_state["crashed_file_context"] = ""
            global_state["retry_count"] = 0

            async for event in kernel.astream(global_state, config):
                # Continuously pipe mutations back into the persistent data containers
                for node_data in event.values():
                    if "execution_logs" in node_data:
                        global_state["execution_logs"] = node_data["execution_logs"]
                    if "project_manifest" in node_data:
                        global_state["project_manifest"] = node_data["project_manifest"]
                    if "last_error" in node_data and node_data["last_error"]:
                        global_state["retry_count"] += 1

            print("\n\033[1;32m✔ Project memory updated.\033[0m\n")

        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    asyncio.run(start_workspace())