
import asyncio
import json
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from kernel.engine import EliteSovereignNodes
from kernel.state import AgentWorkspaceState, global_session_context
from tools.dev_sys import AdvancedDeveloperTools
from tools.os_tools import OSController

initial_state: AgentWorkspaceState = {
    "user_intent": "",
    "execution_plan": [],
    "current_step_index": 0,
    "execution_logs": [],
    "project_manifest": [],
    "last_error": "",
    "crashed_file_context": "",
    "retry_count": 0
}

# Kernel Setup
sovereign_nodes = EliteSovereignNodes()

# Register tools
sovereign_nodes.register_tool("workspace_search", AdvancedDeveloperTools.workspace_search)
sovereign_nodes.register_tool("surgical_patch", AdvancedDeveloperTools.surgical_patch)
sovereign_nodes.register_tool("launch_app", OSController.launch_app)
sovereign_nodes.register_tool("delete_file", OSController.delete_file)
sovereign_nodes.register_tool("copy_file", OSController.copy_file)


async def call_planner(state: AgentWorkspaceState):
    print("---PLANNER---")
    plan = await sovereign_nodes.dynamic_planner_node(state)
    return {"execution_plan": plan}

async def call_executor(state: AgentWorkspaceState):
    print("---EXECUTOR---")
    await sovereign_nodes.execution_fabric_node(state)
    return {"execution_plan": state.execution_plan, "last_error": state.last_error, "retry_count": state.retry_count}


workflow = StateGraph(AgentWorkspaceState)

workflow.add_node("planner", call_planner)
workflow.add_node("executor", call_executor)

workflow.set_entry_point("planner")

workflow.add_edge("planner", "executor")

def should_continue(state: AgentWorkspaceState):
    if state.last_error:
        if state.retry_count < 3: # Simple retry mechanism
            return "planner" # Go back to planner to heal
        else:
            return "halt" # Halt if too many retries
    if state.execution_plan:
        return "executor"
    return "end"

workflow.add_conditional_edges("executor", should_continue, {"planner": "planner", "executor": "executor", "end": END, "halt": END})

# Persistence
checkpointer = MemorySaver()

app = workflow.compile(checkpointer=checkpointer)

async def start_workspace():
    print("Workspace started. Type \'exit\' to quit.")
    while True:
        try:
            user_input = await asyncio.to_thread(input, "You: ")
            if user_input.lower() == 'exit':
                break

            global_session_context["user_input"] = user_input
            # You might want to update the AgentWorkspaceState with the user input here
            # For example, by creating a new state object or updating the existing one
            # For now, let's assume the planner will pick it up from global_session_context or a more structured way
            
            config = {"configurable": {"thread_id": "1"}}
            async for event in app.astream(AgentWorkspaceState(), config=config):
                for key, value in event.items():
                    if key != "__end__":
                        print(f"Node: {key}")
                        print(f"Value: {json.dumps(value, indent=2)}")

        except KeyboardInterrupt:
            print("\nExiting workspace.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == '__main__':
    asyncio.run(start_workspace())
