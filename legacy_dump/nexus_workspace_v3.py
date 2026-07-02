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
# 1. STATE MACHINE SCHEMA
# ==========================================
class AgentWorkspaceState(TypedDict):
    user_intent: str
    execution_plan: List[Dict[str, Any]]
    current_step_index: int
    execution_logs: List[str]
    last_error: str
    crashed_file_context: str  # Dynamically stores the code of the file that crashed
    retry_count: int

# ==========================================
# 2. INTELLECTUAL CONTEXT NODES
# ==========================================
class EliteContextNodes:
    def __init__(self, model_name: str = "groq/llama-3.3-70b-versatile"):
        self.model = model_name

    async def dynamic_planner_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        logs = state.get("execution_logs", [])
        last_error = state.get("last_error", "")
        file_ctx = state.get("crashed_file_context", "")
        retry_count = state.get("retry_count", 0)

        if last_error:
            logger.warning(f"\033[1;31m[DEEP DEBUGGER]\033[0m Analyzing codebase fault topology (Attempt {retry_count + 1})...")
            system_prompt = (
                "You are an elite principal software engineer and automated debugger.\n"
                "A tool command just crashed. Review the error AND the source code of the file where the crash occurred.\n"
                "Generate a surgical repair plan to overwrite the file with fixed code.\n"
                "Return a raw JSON object matching this schema exactly:\n"
                "{\n"
                '  "steps": [\n'
                '    {"step_id": 1, "tool_name": "fs.write_file", "arguments": {"filename": "app.py", "content": "fixed code"}}\n'
                "  ]\n"
                "}\n"
                "Available System Tools: 'fs.write_file', 'terminal.execute'\n"
                "Output only valid raw JSON. No markdown formatting, no conversation."
            )
            prompt = (
                f"--- CRASH TELEMETRY ---\n{last_error}\n\n"
                f"--- SOURCE CODE AFFECTED ---\n{file_ctx}\n\n"
                f"Execution Log History:\n{json.dumps(logs)}"
            )
        else:
            logger.info("\033[1;33m[PLANNER]\033[0m Mapping engineering implementation steps...")
            system_prompt = (
                "You are an elite developer agent engine. Deconstruct the request into explicit tool sequences.\n"
                "Return a raw JSON object matching this schema exactly:\n"
                "{\n"
                '  "steps": [\n'
                '    {"step_id": 1, "tool_name": "fs.write_file", "arguments": {"filename": "calc.py", "content": "..."}},\n'
                '    {"step_id": 2, "tool_name": "terminal.execute", "arguments": {"command": "python calc.py"}}\n'
                '  ]\n'
                "}\n"
                "Available System Tools: 'fs.write_file', 'terminal.execute'\n"
                "Output only valid raw JSON. No markdown code wraps."
            )
            prompt = state["user_intent"]

        response = await acompletion(model=self.model, messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ], temperature=0.0)
        
        raw_text = response.choices[0].message.content.strip()
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```json")[-1].split("```")[0].strip()
            
        try:
            plan_data = json.loads(raw_text)
            return {"execution_plan": plan_data.get("steps", []), "current_step_index": 0}
        except Exception:
            logger.error("JSON parsing structural anomaly from model token stream.")
            return {"execution_plan": [], "current_step_index": 0}

    async def execution_fabric_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        idx = state["current_step_index"]
        plan = state["execution_plan"]
        current_task = plan[idx]
        
        logger.info(f"\033[1;32m[EXECUTOR]\033[0m Invoking: \033[1;35m{current_task['tool_name']}\033[0m")
        args = current_task.get("arguments", {})
        logs = list(state.get("execution_logs", []))
        
        error_signal = ""
        crashed_code_snapshot = state.get("crashed_file_context", "")

        if current_task["tool_name"] == "fs.write_file":
            Path(args["filename"]).write_text(args["content"])
            msg = f"Synchronized file change to disk: {args['filename']}"
            logger.info(f" -> {msg}")
            logs.append(msg)
            
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
                
                # ── NATIVE STACK TRACE PARSING REGEX
                # Matches Python stack traces: File "filename.py", line X
                file_match = re.search(r'File "([^"]+)", line (\d+)', stderr_str)
                if file_match:
                    target_file = file_match.group(1)
                    line_num = file_match.group(2)
                    logger.info(f" -> \033[1;36m[AUTOMATED INGESTION]\033[0m Scraped crash location: {target_file} at Line {line_num}")
                    
                    if Path(target_file).exists():
                        # Read the code of the target file to pass back to the planner node
                        crashed_code_snapshot = Path(target_file).read_text()
                        logger.info(f" -> Successfully ingested source layout for context allocation.")
            else:
                logger.info(f"\033[1;37m[SHELL OUTPUT]:\033[0m\n{stdout_str}")
                logs.append(f"Command '{cmd}' executed successfully.")

        return {
            "current_step_index": idx + 1,
            "execution_logs": logs,
            "last_error": error_signal,
            "crashed_file_context": crashed_code_snapshot
        }

# ==========================================
# 3. GRAPH CONDITIONAL EDGE TRIAGE
# ==========================================
def triage_next_step(state: AgentWorkspaceState) -> str:
    if state.get("last_error"):
        if state.get("retry_count", 0) >= 3:
            return "halt"
        return "heal"
    if state["current_step_index"] >= len(state["execution_plan"]):
        return "end"
    return "continue"

def build_workspace_kernel() -> StateGraph:
    nodes = EliteContextNodes()
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
# 4. INTERACTIVE REPL
# ==========================================
async def start_workspace():
    kernel = build_workspace_kernel()
    config = {"configurable": {"thread_id": "omniscent_session"}}
    
    print("\n" + "="*60)
    print("\033[1;32m  NEXUSKERNEL CORE V3: AUTOMATED CONTEXT INGESTION ACTIVE\033[0m")
    print("  Feature: Automated Regex Stack-Trace File Scraping Enabled")
    print("="*60 + "\n")

    while True:
        try:
            user_prompt = await asyncio.get_event_loop().run_in_executor(
                None, lambda: input("\033[1;36mNexusPrompt ❯\033[0m ").strip()
            )
            if user_prompt.lower() in ["exit", "quit"]:
                break
            if not user_prompt:
                continue

            session_state = {
                "user_intent": user_prompt, "execution_plan": [],
                "current_step_index": 0, "execution_logs": [],
                "last_error": "", "crashed_file_context": "", "retry_count": 0
            }

            async for event in kernel.astream(session_state, config):
                # Dynamically track retry increments if the state loops back
                if "executor" in event and event["executor"].get("last_error"):
                    session_state["retry_count"] += 1

        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    asyncio.run(start_workspace())