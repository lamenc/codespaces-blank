from typing import Dict, Any, List
from typing_extensions import TypedDict

class AgentWorkspaceState(TypedDict):
    user_intent: str
    execution_plan: List[Dict[str, Any]]
    current_step_index: int
    execution_logs: List[str]          
    project_manifest: List[str]        
    last_error: str                    
    crashed_file_context: str          
    retry_count: int

global_session_context: Dict[str, Any] = {"user_input": None}
