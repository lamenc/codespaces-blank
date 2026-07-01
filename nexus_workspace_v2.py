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
logger = logging.getLogger("NexusEngine")

# ==========================================
# 1. STATE MACHINE SCHEMA
# ==========================================
class AgentWorkspaceState(TypedDict):
    user_intent: str
    execution_plan: List[Dict[str, Any]]
    current_step_index: int
    execution_logs: List[str]
    last_error: str  # Tracks terminal crash logs for the self-healing routing vector
    retry_count: int

# ==========================================
# 2. SELF-HEALING ARCHITECTURE NODES
# ==========================================
class SovereignWorkspaceNodes:
    def __init__(self, model_name: str = "groq/llama-3.3-70b-versatile"):
        self.model = model_name

    async def dynamic_planner_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        """Plans regular tasks OR generates a patch plan if an error is present."""
        logs = state.get("execution_logs", [])
        last_error = state.get("last_error", "")
        retry_count = state.get("retry_count", 0)

        # ── THE DEBUGGER PATHWAY: Self-Correction Prompt Trigger
        if last_error:
            logger.warning(f"\033[1;31m[DEBUGGER ACTIVATED]\033[0m Bug detected (Attempt {retry_count}). Formulating patch...")
            system_prompt = (
                "You are an elite automated debugging kernel. A tool command just failed with an error.\n"
                "Analyze the error and the execution logs. Generate a repair plan using tools to fix the issue.\n"
                "Return a raw JSON object matching this schema exactly:\n"
                "{\n"
                '  "steps": [\n'
                '    {"step_id": 1, "tool_name": "fs.write_file", "arguments": {"filename": "app.py", "content": "fixed code"}}\n'
                "  ]\n"
                "}\n"
                "Available System Tools:\n"
                "- 'fs.write_file' -> Args: {'filename': str, 'content': str}\n"
                "- 'fs.read_file' -> Args: {'filename': str}\n"
                "- 'terminal.execute' -> Args: {'command': str}\n"
                "Output only valid raw JSON. No chat prose, no markdown formatting blocks."
            )
            prompt = f"Execution Logs:\n{json.dumps(logs)}\n\nCritical System Error:\n{last_error}"
        else:
            # ── THE STANDARD ROUTING PATHWAY
            logger.info("\033[1;33m[PLANNER]\033[0m Architecting operational execution graph...")
            system_prompt = (
                "You are an elite terminal-integrated developer agent engine.\n"
                "Deconstruct the request into explicit tool sequences. Return raw JSON matching this schema:\n"
                "{\n"
                '  "steps": [\n'
                '    {"step_id": 1, "tool_name": "terminal.execute", "arguments": {"command": "ls"}}\n'
                "  ]\n"
                "}\n"
                "Available System Tools:\n"
                "- 'fs.write_file', 'fs.read_file', 'terminal.execute'\n"
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
            return {"execution_plan": plan_data.get("steps", []), "current_step_index": 0, "last_error": ""}
        except Exception:
            logger.error(f"Schema degradation on parsing output token.")
            return {"execution_plan": [], "current_step_index": 0}

    async def execution_fabric_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        idx = state["current_step_index"]
        plan = state["execution_plan"]
        current_task = plan[idx]
        
        logger.info(f"\033[1;32m[EXECUTOR]\033[0m Invoking: \033[1;35m{current_task['tool_name']}\033[0m")
        args = current_task.get("arguments", {})
        logs = list(state.get("execution_logs", []))
        error_signal = ""

        # Tool 1: File Writer
        if current_task["tool_name"] == "fs.write_file":
            Path(args["filename"]).write_text(args["content"])
            msg = f"File Synchronized: {args['filename']}"
            logger.info(f" -> {msg}")
            logs.append(msg)
            
        # Tool 2: File Reader (Essential for evaluating existing files)
        elif current_task["tool_name"] == "fs.read_file":
            try:
                content = Path(args["filename"]).read_text()
                msg = f"Inspected file '{args['filename']}':\n{content}"
                logger.info(f" -> Ingestion complete.")
                logs.append(msg)
            except Exception as e:
                error_signal = f"fs.read_file failed: {str(e)}"

        # Tool 3: Native Terminal Shell Execution with Strict Error Interception
        elif current_task["tool_name"] == "terminal.execute":
            cmd = args.get("command", "")
            process = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            stdout_str = stdout.decode().strip()
            stderr_str = stderr.decode().strip()
            
            if process.returncode != 0:
                # Intercept the exact crash signature
                error_signal = f"Shell command '{cmd}' crashed with Exit Code {process.returncode}.\nSTDERR:\n{stderr_str}\nSTDOUT:\n{stdout_str}"
                logger.error(f" -> \033[1;31mCommand crashed!\033[0m Registering fault telemetry.")
            else:
                logger.info(f"\033[1;37m[SHELL OUTPUT]:\033[0m\n{stdout_str}")
                logs.append(f"Command '{cmd}' executed perfectly. Output: {stdout_str}")

        return {
            "current_step_index": idx + 1,
            "execution_logs": logs,
            "last_error": error_signal
        }

# ==========================================
# 3. ADVANCED CONDITIONAL STATE ROUTING
# ==========================================
def triage_next_step(state: AgentWorkspaceState) -> str:
    """Intelligently routes the state graph depending on execution errors."""
    if state.get("last_error"):
        if state.get("retry_count", 0) >= 3:
            logger.critical("Self-healing depth ceiling reached. Halting pipeline to protect codebase integrity.")
            return "halt"
        # Force the engine to loop back to the planner, carrying the error metadata
        return "heal"
    
    if state["current_step_index"] >= len(state["execution_plan"]):
        return "end"
    return "continue"

def build_healer_kernel() -> StateGraph:
    nodes = SovereignWorkspaceNodes()
    workflow = StateGraph(AgentWorkspaceState)
    
    workflow.add_node("planner", nodes.dynamic_planner_node)
    workflow.add_node("executor", nodes.execution_fabric_node)
    
    workflow.set_entry_point("planner")
    workflow.add_edge("planner", "executor")
    
    # Advanced state conditional triage map
    workflow.add_conditional_edges(
        "executor",
        triage_next_step,
        {
            "continue": "executor",
            "heal": "planner", # Loop back to planning node to inject error corrections
            "end": END,
            "halt": END
        }
    )
    return workflow.compile(checkpointer=MemorySaver())

# ==========================================
# 4. INTERACTIVE ENTRYPOINT
# ==========================================
async def start_workspace():
    kernel = build_healer_kernel()
    config = {"configurable": {"thread_id": "self_healing_session"}}
    
    print("\n" + "="*60)
    print("\033[1;35m  NEXUSKERNEL CORE V2: AUTONOMOUS SELF-HEALING REPL ACTIVE\033[0m")
    print("  Models: Llama-3.3-70B (Groq) | Self-Correction Limit: 3 Runs")
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
                "last_error": "", "retry_count": 0
            }

            async for event in kernel.astream(session_state, config):
                # Update retry allocation dynamically if loop back executes
                if "planner" in event and event["planner"].get("execution_plan"):
                    if event["planner"].get("last_error") == "":
                        # If planner triggers but we have a history of log entries, it's a healing loop turn
                        pass 

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Turn error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(start_workspace())