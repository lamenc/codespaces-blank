import os
import sys
import json
import re
import asyncio
import logging
import shutil
from pathlib import Path
from typing import Dict, Any, List
from typing_extensions import TypedDict

# Core elite orchestration packages
from litellm import acompletion
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from playwright.async_api import async_playwright

# Configure strict unbuffered stream logging for real-time terminal diagnostics
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("NexusKernel")

# Global Event Ring Buffer simulating real-time background notification feeds
NOTIFICATION_BUFFER = [
    {"platform": "Discord", "sender": "AlphaTeam", "message": "The production build pipeline just completed successfully."},
    {"platform": "Instagram", "sender": "alex_dev", "message": "Are we still on for the system architecture seminar at 4 PM?"},
    {"platform": "Gmail", "sender": "security@github.com", "message": "Alert: New login detected on a Linux instance."}
]

# ==========================================
# 1. AUTONOMOUS EMBEDDED BROWSER DRIVER
# ==========================================
class AutonomousBrowserTools:
    _playwright = None
    _browser = None
    _context = None
    _page = None

    @classmethod
    async def initialize(cls):
        """Initializes a persistent, reusable browser allocation block directly inside the process memory space."""
        if cls._browser is None:
            logger.info("[SYSTEM ENGINE] Spawning background Headless Chromium engine runtime...")
            cls._playwright = await async_playwright().start()
            cls._browser = await cls._playwright.chromium.launch(headless=True)
            cls._context = await cls._browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            cls._page = await cls._context.new_page()
            logger.info("[SYSTEM ENGINE] Browser engine initialization complete.")

    @classmethod
    async def browse_to(cls, url: str) -> str:
        """Navigates to a target URL, waits for structural DOM settlement, and returns pruned text content."""
        await cls.initialize()
        logger.info(f"Navigating browser core target network layer -> {url}")
        try:
            await cls._page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page_text = await cls._page.evaluate("() => document.body.innerText")
            page_title = await cls._page.title()
            return f"Page Title: {page_title}\n\nRaw Text Ingested Content:\n{page_text[:4000]}"
        except Exception as e:
            logger.error(f"Browser navigation transaction failed: {str(e)}")
            return f"Network routing error: Unable to load target resource vector. Details: {str(e)}"

    @classmethod
    async def shutdown(cls):
        """Gracefully tears down open socket network bindings on system exit signals."""
        if cls._browser:
            await cls._browser.close()
            await cls._playwright.stop()
            cls._browser = None
            cls._playwright = None
            logger.info("Browser systems closed down cleanly.")

# ==========================================
# 2. ADVANCED DEV & OS TOOL SUBSYSTEMS
# ==========================================
class AdvancedDeveloperTools:
    
    @staticmethod
    def workspace_search(pattern: str, extension: str = "*.py") -> List[Dict[str, Any]]:
        """Acts like a local 'grep' engine to locate text patterns across files without blowing up context windows."""
        matches = []
        cwd = Path(".")
        
        for path in cwd.rglob(extension):
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
        
        search_block_norm = search_block.strip().replace("\r\n", "\n")
        replace_block_norm = replace_block.strip().replace("\r\n", "\n")
        content_norm = content.replace("\r\n", "\n")
        
        if search_block_norm not in content_norm:
            raise ValueError("The target search_block could not be found with exact string matching inside the source file.")
            
        updated_content = content_norm.replace(search_block_norm, replace_block_norm)
        target_path.write_text(updated_content, encoding="utf-8")
        return f"Surgical patch applied successfully to {filename}."

# ==========================================
# 3. STATE MACHINE SCHEMA (Persistent)
# ==========================================
class AgentWorkspaceState(TypedDict):
    user_intent: str
    execution_plan: List[Dict[str, Any]]
    current_step_index: int
    execution_logs: List[str]          
    project_manifest: List[str]        
    last_error: str                    
    crashed_file_context: str          
    retry_count: int

# ==========================================
# 4. HIGH-SPEED ORCHESTRATION NODES
# ==========================================
class EliteSovereignNodes:
    def __init__(self, model_name: str = "groq/llama-3.3-70b-versatile"):
        self.model = model_name

    async def dynamic_planner_node(self, state: AgentWorkspaceState) -> Dict[str, Any]:
        logs = state.get("execution_logs", [])
        manifest = state.get("project_manifest", [])
        last_error = state.get("last_error", "")
        file_ctx = state.get("crashed_file_context", "")
        retry_count = state.get("retry_count", 0)

        # Reactive Single-Step Instructions updated for Communication Ingress
        system_instruction = (
            "You are the central runtime planner of an elite developer agent engine with deep system, browser, and notification stream access.\n"
            "Review your structural project manifest and execution logs continuously.\n"
            "CRITICAL: Do not plan multiple steps ahead. Provide exactly ONE next logical tool step based on the context.\n"
            "If the user's objective has been fully completed successfully, return an empty steps array: {\"steps\": []}.\n\n"
            "You must return a raw JSON object matching this schema blueprint layout exactly:\n"
            "{\n"
            '  "steps": [\n'
            '    {"step_id": 1, "tool_name": "fs.write_file", "arguments": {"filename": "main.py", "content": "print(1)"}}\n'
            '  ]\n'
            "}\n\n"
            "Available Highly-Optimized System Tools:\n"
            "1. 'fs.write_file' -> Args: {'filename': str, 'content': str}\n"
            "2. 'fs.read_file' -> Args: {'filename': str}\n"
            "3. 'fs.delete_file' -> Args: {'filename': str}\n"
            "4. 'fs.copy_file' -> Args: {'source': str, 'destination': str}\n"
            "5. 'fs.surgical_patch' -> Args: {'filename': str, 'search_block': str, 'replace_block': str}\n"
            "6. 'workspace.search' -> Args: {'pattern': str, 'extension': str}\n"
            "7. 'terminal.execute' -> Args: {'command': str}\n"
            "8. 'web.browse' -> Args: {'url': str}\n"
            "9. 'os.launch_app' -> Args: {'app_name': str}\n"
            "10. 'comm.fetch_inbox' -> Args: {} (Checks recent incoming emails from Gmail headers)\n"
            "11. 'comm.check_notifications' -> Args: {} (Pulls real-time alert data from communication vectors like Discord/Instagram)\n\n"
            "CRITICAL: Output valid raw JSON only. Never include conversational chatter or markdown packaging wraps."
        )

        if last_error:
            logger.warning(f"[DEBUGGER ROUTE] Intercepted execution fault (Attempt {retry_count}). Analyzing tool diagnostic payload...")
            prompt = (
                f"--- CRASH TELEMETRY ---\n{last_error}\n\n"
                f"--- SOURCE CODE AT FAULT SITE (IF ANY) ---\n{file_ctx}\n\n"
                f"Historical Execution Logs:\n{json.dumps(logs)}"
            )
        else:
            logger.info("[PLANNER] Evaluating workspace logs and choosing next single step...")
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
        plan = state.get("execution_plan", [])
        
        if not plan or len(plan) == 0:
            return {"current_step_index": 0, "last_error": ""}

        idx = state["current_step_index"]
        if idx >= len(plan):
            return {"current_step_index": idx, "last_error": ""}
            
        current_task = plan[idx]
        
        logger.info(f"[EXECUTOR] Invoking: {current_task['tool_name']}")
        args = current_task.get("arguments", {})
        
        logs = list(state.get("execution_logs", []))
        manifest = list(state.get("project_manifest", []))
        retry_count = state.get("retry_count", 0)
        error_signal = ""
        crashed_code_snapshot = state.get("crashed_file_context", "")

        # Tool 1: Raw File Writer
        if current_task["tool_name"] == "fs.write_file":
            filename = args.get("filename")
            Path(filename).write_text(args.get("content", ""), encoding="utf-8")
            msg = f"Synchronized file write: {filename}"
            logger.info(f" -> {msg}")
            logs.append(msg)
            if filename not in manifest:
                manifest.append(filename)
            
        # Tool 2: Raw File Reader
        elif current_task["tool_name"] == "fs.read_file":
            filename = args.get("filename")
            if Path(filename).exists():
                content = Path(filename).read_text(encoding="utf-8")
                msg = f"Ingested content for file '{filename}'"
                logger.info(f" -> {msg}")
                logs.append(f"Content of {filename}:\n{content}")
            else:
                error_signal = f"fs.read_file failure: Target path '{filename}' does not exist."

        # Tool 3: Destructive File Deletion
        elif current_task["tool_name"] == "fs.delete_file":
            filename = args.get("filename")
            try:
                p = Path(filename)
                if p.exists():
                    p.unlink()
                    msg = f"Permanently deleted target file: {filename}"
                    logger.info(f" -> {msg}")
                    logs.append(msg)
                    if filename in manifest:
                        manifest.remove(filename)
                else:
                    error_signal = f"fs.delete_file error: File '{filename}' not found."
            except Exception as e:
                error_signal = f"fs.delete_file system collision: {str(e)}"

        # Tool 4: File Copy Operation
        elif current_task["tool_name"] == "fs.copy_file":
            src, dest = args.get("source"), args.get("destination")
            try:
                shutil.copy2(src, dest)
                msg = f"Successfully duplicated file from '{src}' to '{dest}'"
                logger.info(f" -> {msg}")
                logs.append(msg)
                if dest not in manifest:
                    manifest.append(dest)
            except Exception as e:
                error_signal = f"fs.copy_file failed: {str(e)}"

        # Tool 5: Surgical Block Modifier
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
                error_signal = f"fs.surgical_patch failure: {str(e)}"
                logger.error(" -> Surgical block mutation failed.")

        # Tool 6: Workspace Grep Explorer
        elif current_task["tool_name"] == "workspace.search":
            pattern = args.get("pattern", "")
            ext = args.get("extension", "*.py")
            matches = AdvancedDeveloperTools.workspace_search(pattern, ext)
            msg = f"Grep scanning completed for pattern '{pattern}'. Found {len(matches)} occurrences."
            logger.info(f" -> {msg}")
            logs.append(f"{msg} Match Manifest Matrix Data: {json.dumps(matches)}")

        # Tool 7: Async Subprocess Terminal Executor
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
                logger.error(" -> Terminal command execution crash registered.")
                
                file_match = re.search(r'File "([^"]+)", line (\d+)', stderr_str)
                if file_match:
                    target_file = file_match.group(1)
                    if Path(target_file).exists():
                        crashed_code_snapshot = Path(target_file).read_text(encoding="utf-8")
                        logger.info(f" -> Automated context collection successfully locked file target code: {target_file}")
            else:
                logger.info(f"[SHELL OUTPUT]:\n{stdout_str}")
                logs.append(f"Command '{cmd}' executed successfully. Output: {stdout_str}")

        # Tool 8: Autonomous Web Ingestion Controller
        elif current_task["tool_name"] == "web.browse":
            target_url = args.get("url", "")
            try:
                web_extracted_data = await AutonomousBrowserTools.browse_to(target_url)
                msg = f"Web scrape execution synchronization clean for target: {target_url}"
                logger.info(f" -> {msg}")
                logs.append(f"Web Resource [{target_url}] Ingested Data:\n{web_extracted_data}")
            except Exception as e:
                error_signal = f"web.browse framework failure: {str(e)}"
                logger.error(f" -> Browser driver exception: {str(e)}")
                logs.append(f"Tool web.browse failed with error: {str(e)}")

        # Tool 9: Native OS Software Application Launcher
        elif current_task["tool_name"] == "os.launch_app":
            app = args.get("app_name", "")
            try:
                logger.info(f" -> Dispatching OS process kernel branch for app: {app}")
                if sys.platform == "win32":
                    asyncio.create_task(asyncio.create_subprocess_shell(f"start {app}"))
                elif sys.platform == "darwin":
                    asyncio.create_task(asyncio.create_subprocess_shell(f"open -a {app}"))
                else:
                    asyncio.create_task(asyncio.create_subprocess_shell(f"{app} &"))
                msg = f"Successfully broadcast asynchronous OS launch signal for software: {app}"
                logs.append(msg)
            except Exception as e:
                error_signal = f"os.launch_app system fault: {str(e)}"

        # Tool 10: Fetch Emails / Gmail Ingress Core (New)
        elif current_task["tool_name"] == "comm.fetch_inbox":
            try:
                gmail_logs = [alert for alert in NOTIFICATION_BUFFER if alert["platform"] == "Gmail"]
                msg = f"Polled target mail ledger framework. Found {len(gmail_logs)} unread messages."
                logger.info(f" -> {msg}")
                logs.append(f"Unread Email Core Data: {json.dumps(gmail_logs)}")
            except Exception as e:
                error_signal = f"comm.fetch_inbox collision: {str(e)}"

        # Tool 11: Real-time Communication Platforms Alert Ingress (New)
        elif current_task["tool_name"] == "comm.check_notifications":
            try:
                social_logs = [alert for alert in NOTIFICATION_BUFFER if alert["platform"] in ["Discord", "Instagram"]]
                msg = f"Scraped social platform notification buffers. Found {len(social_logs)} active events."
                logger.info(f" -> {msg}")
                logs.append(f"Active Notifications Stream Payload: {json.dumps(social_logs)}")
            except Exception as e:
                error_signal = f"comm.check_notifications structural fault: {str(e)}"

        if error_signal:
            retry_count += 1

        return {
            "current_step_index": idx + 1,
            "execution_logs": logs,
            "project_manifest": manifest,
            "last_error": error_signal,
            "crashed_file_context": crashed_code_snapshot,
            "retry_count": retry_count
        }

# ==========================================
# 5. CONDITIONAL ROOTING TRIAGE INTERFACES
# ==========================================
def triage_next_step(state: AgentWorkspaceState) -> str:
    if state.get("last_error"):
        if state.get("retry_count", 0) >= 3:
            logger.critical("Maximum automated self-correction cycles exhausted. Relinquishing loop to developer seat.")
            return "halt"
        return "heal"
    
    if not state.get("execution_plan") or len(state["execution_plan"]) == 0:
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
        {
            "continue": "planner",  
            "heal": "planner", 
            "end": END, 
            "halt": END
        }
    )
    return workflow.compile(checkpointer=MemorySaver())

# ==========================================
# 6. PERSISTENT WORKSPACE ENVIRONMENT REPL
# ==========================================
async def start_workspace():
    kernel = build_runtime_kernel()
    config = {"configurable": {"thread_id": "nexus_production_session_v6"}}
    
    print("\n" + "="*70)
    print("  NEXUSKERNEL WORKSPACE SYSTEM CORE INTERACTIVE REPL ACTIVE")
    print("  Orchestration: LangGraph Reactive Step Graph Architecture (ReAct)")
    print("  Capabilities: Grep, Patches, Async Shell, Playwright Web, OS Control, Comm Ingress")
    print("="*70 + "\n")

    global_session_context = {
        "user_intent": "", "execution_plan": [],
        "current_step_index": 0, "execution_logs": [],
        "project_manifest": [], "last_error": "",
        "crashed_file_context": "", "retry_count": 0
    }

    while True:
        try:
            user_prompt = await asyncio.get_event_loop().run_in_executor(
                None, lambda: input("NexusPrompt ❯ ").strip()
            )
            if user_prompt.lower() in ["exit", "quit"]:
                try:
                    await AutonomousBrowserTools.shutdown()
                except Exception:
                    pass
                print("\n[SYSTEM] Disengaging memory engine modules. Kernel offline.")
                break
            if not user_prompt:
                continue

            global_session_context["user_intent"] = user_prompt
            global_session_context["execution_plan"] = []
            global_session_context["current_step_index"] = 0
            global_session_context["last_error"] = ""
            global_session_context["crashed_file_context"] = ""
            global_session_context["retry_count"] = 0

            async for event in kernel.astream(global_session_context, config):
                for node_payload in event.values():
                    if "execution_logs" in node_payload:
                        global_session_context["execution_logs"] = node_payload["execution_logs"]
                    if "project_manifest" in node_payload:
                        global_session_context["project_manifest"] = node_payload["project_manifest"]
                    if "retry_count" in node_payload:
                        global_session_context["retry_count"] = node_payload["retry_count"]

            print("\n✔ Workspace memory matrix updated safely.\n")

        except KeyboardInterrupt:
            print("\n[SYSTEM] Received keyboard execution halt sign. Interrupted processing.")
            break
        except Exception as e:
            logger.critical(f"Unhandled engineering runtime exception: {str(e)}")

if __name__ == "__main__":
    if "GROQ_API_KEY" not in os.environ:
        print("[CRITICAL ERROR] Missing GROQ_API_KEY tokens. Export environment parameters.")
        sys.exit(1)
    asyncio.run(start_workspace())