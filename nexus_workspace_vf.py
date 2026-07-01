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

# Configure strict unbuffered stream logging for real-time terminal diagnostics
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [\033[1;36m%(levelname)s\033[0m] %(name)s: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("NexusKernel")

# ==========================================
# 1. ADVANCED DEV TOOL SUBSYSTEMS
# ==========================================
class AdvancedDeveloperTools:
    
    @staticmethod
    def workspace_search(pattern: str, extension: str = "*.py") -> List[Dict[str, Any]]:
        """Acts like a local 'grep' engine to locate text patterns across files without blowing up context windows."""
        matches = []
        cwd = Path(".")
        
        for path in cwd.rglob(extension):
            # Safe ignores for common virtual environment and cache folders
            if any(part in path.parts for part in [".venv", "venv", "__pycache__", ".git", ".github"]):
                continue
            try:
                content = path.read_text(encoding="utf-8")
                for i, line in enumerate(content.splitlines(), 1):
                    if re.search(pattern, line, re.IGNORECASE):
                        matches.append({
                            "file": str(path),
                            "line": i,
                            "matched_text": line.strip()
                        })
            except Exception:
                continue
        return matches

    @staticmethod
    def surgical_patch(filename: str, search_block: str, replace_block: str) -> str:
        """Locates a specific block of text inside a large file and swaps it out surgically."""
        target_path = Path(filename)
        if not target_path.exists():
            raise FileNotFoundError(f"Target workspace file '{filename}' does not exist.")
            
        content = target_path.read_text(encoding="utf-8")
        
        # Normalize line endings to prevent cross-platform string matching discrepancies
        search_block_norm = search_block.strip().replace("\r\n", "\n")
        replace_block_norm = replace_block.strip().replace("\r\n", "\n")
        content_norm = content.replace("\r\n", "\n")
        
        if search_block_norm not in content_norm:
            raise ValueError("The target search_block could not be found with exact string matching inside the source file.")
            
        updated_content = content_norm.replace(search_block_norm, replace_block_norm)
        target_path.write_text(updated_content, encoding="utf-8")
        return f"Surgical patch applied successfully to {filename}."

# ==========================================
# 2. STATE MACHINE SCHEMA (Persistent)
# ==========================================
class AgentWorkspaceState(TypedDict):
    user_intent: str
    execution_plan: List[Dict[str, Any]]
    current_step_index: int
    execution_logs: List[str]          # Running ledger across the entire session lifecycle
    project_manifest: List[str]        # Keeps track of all files modified/created by the agent
    last_error: str                    # Stores stderr data for debugging iterations
    crashed_file_context: str          # Ingested target source code where terminal errors hit
    retry_count: int

# ==========================================
# 3. HIGH-SPEED ORCHESTRATION NODES
# ==========================================
class EliteSovereignNodes:
    def __init__(self, model_name: str = "groq/llama-3.3-70b-versatile"):
        self.model = model_name

    async def dynamic_planner_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        logs = state.get("execution_logs", [])
        manifest = state.get("project_manifest", [])
        last_error = state.get("last_error", "")
        file_ctx = state.get("crashed_file_context", "")

        # Unified operational layout instructions
        system_instruction = (
            "You are the central runtime planner of an elite developer agent engine with deep system access.\n"
            "Review your structural project manifest and execution logs to build non-overlapping plans.\n"
            "You must return a raw JSON object matching this schema blueprint layout exactly:\n"
            "{\n"
            '  "steps": [\n'
            '    {"step_id": 1, "tool_name": "fs.write_file", "arguments": {"filename": "main.py", "content": "print(1)"}},\n'
            '    {"step_id": 2, "tool_name": "terminal.execute", "arguments": {"command": "python main.py"}}\n'
            '  ]\n'
            "}\n\n"
            "Available Highly-Optimized System Tools:\n"
            "1. 'fs.write_file' -> Args: {'filename': str, 'content': str} (Writes/overwrites whole files)\n"
            "2. 'fs.read_file' -> Args: {'filename': str} (Reads code blocks into context)\n"
            "3. 'fs.surgical_patch' -> Args: {'filename': str, 'search_block': str, 'replace_block': str} (Surgically updates blocks inside large files)\n"
            "4. 'workspace.search' -> Args: {'pattern': str, 'extension': str} (Grep searching through active code files)\n"
            "5. 'terminal.execute' -> Args: {'command': str} (Launches non-blocking sub-processes for testing/compiling/git/pip)\n\n"
            "CRITICAL: Output valid raw JSON only. Never include conversational chatter or markdown packaging wraps."
        )

        if last_error:
            logger.warning(f"\033[1;31m[DEBUGGER ROUTE]\033[0m Intercepted execution fault. Analyzing stack-trace dependencies...")
            prompt = (
                f"--- CRASH TELEMETRY ---\n{last_error}\n\n"
                f"--- SOURCE CODE AT FAULT SITE ---\n{file_ctx}\n\n"
                f"Historical Execution Logs:\n{json.dumps(logs)}"
            )
        else:
            logger.info("\033[1;33m[PLANNER]\033[0m Ingesting memory context vectors and calculating next structural steps...")
            prompt = (
                f"Persistent Workspace Manifest: {json.dumps(manifest)}\n"
                f"Recent Execution Logs:\n{json.dumps(logs[-5:])}\n\n"
                f"Operational Request: {state['user_intent']}"
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
            logger.error("JSON schema formatting error returned from token compiler stream.")
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

        # Tool 1: Raw File Writer
        if current_task["tool_name"] == "fs.write_file":
            filename = args.get("filename")
            Path(filename).write_text(args.get("content", ""), encoding="utf-8")
            msg = f"Synchronized whole file write layout: {filename}"
            logger.info(f" -> {msg}")
            logs.append(msg)
            if filename not in manifest:
                manifest.append(filename)
            
        # Tool 2: Raw File Reader
        elif current_task["tool_name"] == "fs.read_file":
            filename = args.get("filename")
            if Path(filename).exists():
                content = Path(filename).read_text(encoding="utf-8")
                msg = f"Ingested content verification tracking for file '{filename}'"
                logger.info(f" -> {msg}")
                logs.append(f"Content of {filename}:\n{content}")
            else:
                error_signal = f"fs.read_file failure: Target path '{filename}' does not exist on disk."

        # Tool 3: Surgical Block Modifier
        elif current_task["tool_name"] == "fs.surgical_patch":
            filename = args.get("filename")
            try:
                res = AdvancedDeveloperTools.surgical_patch(
                    filename=filename,
                    search_block=args.get("search_block", ""),
                    replace_block=args.get("replace_block", "")
                )
                logger.info(f" -> {res}")
                logs.append(res)
                if filename not in manifest:
                    manifest.append(filename)
            except Exception as e:
                error_signal = f"fs.surgical_patch crash tracking signature: {str(e)}"
                logger.error(f" -> \033[1;31mSurgical block mutation failed.\033[0m")

        # Tool 4: Workspace Grep Explorer
        elif current_task["tool_name"] == "workspace.search":
            pattern = args.get("pattern", "")
            ext = args.get("extension", "*.py")
            matches = AdvancedDeveloperTools.workspace_search(pattern, ext)
            msg = f"Grep scanning completed for pattern '{pattern}'. Found {len(matches)} occurrences."
            logger.info(f" -> {msg}")
            logs.append(f"{msg} Match Manifest Matrix Data: {json.dumps(matches)}")

        # Tool 5: Async Subprocess Terminal Executor
        elif current_task["tool_name"] == "terminal.execute":
            cmd = args.get("command", "")
            process = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            stdout_str = stdout.decode(errors="ignore").strip()
            stderr_str = stderr.decode(errors="ignore").strip()
            
            if process.returncode != 0:
                error_signal = f"Exit Code {process.returncode}.\nSTDERR:\n{stderr_str}\nSTDOUT:\n{stdout_str}"
                logger.error(f" -> \033[1;31mTerminal command execution crash registered.\033[0m")
                
                # Dynamic stack trace validation scans
                file_match = re.search(r'File "([^"]+)", line (\d+)', stderr_str)
                if file_match:
                    target_file = file_match.group(1)
                    if Path(target_file).exists():
                        crashed_code_snapshot = Path(target_file).read_text(encoding="utf-8")
                        logger.info(f" -> Automated context collection successfully locked file target code: {target_file}")
            else:
                logger.info(f"\033[1;37m[SHELL OUTPUT]:\033[0m\n{stdout_str}")
                logs.append(f"Command '{cmd}' executed perfectly without warning flags.")

        return {
            "current_step_index": idx + 1,
            "execution_logs": logs,
            "project_manifest": manifest,
            "last_error": error_signal,
            "crashed_file_context": crashed_code_snapshot
        }

# ==========================================
# 4. CONDITIONAL ROOTING TRIAGE INTERFACES
# ==========================================
def triage_next_step(state: AgentWorkspaceState) -> str:
    if state.get("last_error"):
        if state.get("retry_count", 0) >= 3:
            logger.critical("Maximum automated self-correction cycles exhausted. Relinquishing loop to developer seat.")
            return "halt"
        return "heal"
    if state["current_step_index"] >= len(state["execution_plan"]):
        return "end"
    return "continue"

def build_runtime_kernel() -> StateGraph:
    nodes = EliteSovereignNodes()
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
# 5. PERSISTENT WORKSPACE ENVIRONMENT REPL
# ==========================================
async def start_workspace():
    kernel = build_runtime_kernel()
    config = {"configurable": {"thread_id": "nexus_production_session_v4"}}
    
    print("\n" + "="*70)
    print("\033[1;32m  NEXUSKERNEL WORKSPACE SYSTEM CORE INTERACTIVE REPL ACTIVE\033[0m")
    print("  Orchestration: LangGraph Stateful Engine with Checkpointer Persistence")
    print("  Tool Capabilities: Grep Search, Surgical Patches, Async Subprocesses")
    print("="*70 + "\n")

    # Global continuous memory tracking container across user turns
    global_session_context = {
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
                print("\n[SYSTEM] Disengaging memory engine modules. Kernel offline.")
                break
            if not user_prompt:
                continue

            # Load the continuous state profile for the next step execution loop
            global_session_context["user_intent"] = user_prompt
            global_session_context["execution_plan"] = []
            global_session_context["current_step_index"] = 0
            global_session_context["last_error"] = ""
            global_session_context["crashed_file_context"] = ""
            global_session_context["retry_count"] = 0

            async for event in kernel.astream(global_session_context, config):
                for node_payload in event.values():
                    # Sync modified updates back to global tracking objects
                    if "execution_logs" in node_payload:
                        global_session_context["execution_logs"] = node_payload["execution_logs"]
                    if "project_manifest" in node_payload:
                        global_session_context["project_manifest"] = node_payload["project_manifest"]
                    if "last_error" in node_payload and node_payload["last_error"]:
                        global_session_context["retry_count"] += 1

            print("\n\033[1;32m✔ Workspace memory matrix updated safely.\033[0m\n")

        except KeyboardInterrupt:
            print("\n[SYSTEM] Received keyboard execution halt sign. Interrupted processing.")
            break
        except Exception as e:
            logger.critical(f"Unhandled engineering runtime exception: {str(e)}")

if __name__ == "__main__":
    if "GROQ_API_KEY" not in os.environ:
        print("\033[1;31m[CRITICAL ERROR]\033[0m Missing GROQ_API_KEY tokens. Export environment parameters.")
        sys.exit(1)
    asyncio.run(start_workspace())