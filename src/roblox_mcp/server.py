import asyncio
import logging
import json # Added for json formatting
from typing import Dict, Any, Optional, List, Union # Added Union
import re # For safe Lua string escaping
from collections import deque # Use deque for simple non-async queue
from datetime import datetime # For timestamping logs received from plugin
import uuid # For generating unique request IDs
import time # For timeouts
import threading # For locking access to shared results

# --- FastAPI Imports ---
from fastapi import FastAPI, HTTPException, Request # Added Request
from fastapi.responses import JSONResponse # For returning JSON
# --- End FastAPI Imports ---

from mcp.server.fastmcp import FastMCP, Context
from pydantic import Field, Json, BaseModel # Added BaseModel

from .config import load_config, Settings # Import config loading
from .roblox_client import RobloxClient, RobloxApiError # Import client and error
from .sse import create_sse_server # Import the SSE server creator

# --- Local Imports ---
from .config import load_config, Settings # Import config loading
from .roblox_client import RobloxClient, RobloxApiError # Import client and error
from .sse import create_sse_server # Import the SSE server creator
# --- End Local Imports ---

# --- Removed Uvicorn Import ---
# import uvicorn

# --- Define Known Services ---
KNOWN_ROBLOX_SERVICES = {
    "workspace", "lighting", "replicatedfirst", "replicatedstorage", 
    "serverstorage", "serverscriptservice", "startergui", "starterpack", 
    "starterplayer", "teams", "soundservice", "textchatservice", 
    "players", "chat", "localizationService", "testService"
    # Add more as needed
}

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("VibeBlocksMCPServer") # <<< RENAME
# <<< Set main logger level to DEBUG >>>
logger.setLevel(logging.DEBUG)
logging.getLogger("roblox_mcp.roblox_client").setLevel(logging.DEBUG)
logging.getLogger("src.roblox_mcp.sse").setLevel(logging.INFO) # Add logger for SSE module

# --- Load Config Globally (Simpler than passing everywhere) ---
try:
    global_config = load_config()
    logger.info(f"Configuration loaded for Universe ID: {global_config.roblox_universe_id}")
except Exception as e:
    logger.error(f"Failed to load configuration: {e}. Ensure .env file exists.", exc_info=True)
    global_config = None
# --- End Load Config ---

# --- Removed Global Roblox Client (will be instantiated per tool call or managed differently) ---
# global_roblox_client: Optional[RobloxClient] = None

# --- Plugin Command Queue ---
plugin_command_queue: deque = deque()

# --- Last Script Logs ---
last_script_logs: Dict[str, Any] = {"output": None, "error": None}

# --- Studio Log Buffer ---
# Store tuples: (server_timestamp, log_entry_dict_from_plugin)
studio_log_buffer: deque = deque(maxlen=200) # Limit to last 200 entries

# --- Plugin Result Handling ---
# Dictionary to store results reported back by the plugin
# Key: request_id (str), Value: Result data or None if pending
pending_plugin_results: Dict[str, Any] = {}
# Lock to ensure thread-safe access to pending_plugin_results
plugin_results_lock = threading.Lock()
# --- End Plugin Result Handling ---

# --- Pydantic Model for Incoming Logs ---
class StudioLogEntry(BaseModel):
    message: str
    log_type: str # e.g., "Print", "Info", "Warning", "Error"
    timestamp: float # Plugin timestamp (os.clock() or similar)

# --- Main FastAPI App ---
app = FastAPI(
    title="Vibe Blocks MCP Server (SSE) with Plugin Endpoint", # <<< RENAME
    description="Combines MCP Tools (via SSE) with custom endpoints for Roblox Studio Plugin communication."
)
# --- End Main FastAPI App ---

# --- Add Endpoint for Studio Plugin (DEFINED BEFORE MOUNTING SSE) ---
@app.get("/plugin_command", response_class=JSONResponse)
async def get_plugin_command():
    """Endpoint for the Roblox Studio plugin to poll for commands."""
    global plugin_command_queue # Added global access
    try:
        # Get the next command from the left side of the queue
        command = plugin_command_queue.popleft()
        logger.info(f"Dequeued command for Studio plugin: {command}")
        return command # FastAPI automatically encodes dict to JSON
    except IndexError:
        # Queue is empty
        logger.debug("Plugin command queue empty.") # Add debug log
        return {} # Return empty JSON object
    except Exception as e:
        logger.exception("Error processing plugin command request")
        # Return an error response to the plugin
        raise HTTPException(status_code=500, detail="Internal server error processing command request")
# --- End Endpoint for Studio Plugin ---

# --- Add Endpoint for Reporting Plugin Results (NEW) ---
class PluginResultPayload(BaseModel):
    request_id: str
    result: Any # Can be any JSON-serializable type

@app.post("/plugin_report_result")
async def report_plugin_result(payload: PluginResultPayload, request: Request):
    """Endpoint for the Studio plugin to report the result of an executed command."""
    global pending_plugin_results, plugin_results_lock
    client_host = request.client.host if request.client else "unknown"
    request_id = payload.request_id
    result_data = payload.result
    logger.info(f"Received result for request_id {request_id} from plugin at {client_host}")
    
    with plugin_results_lock:
        if request_id in pending_plugin_results:
            pending_plugin_results[request_id] = result_data
            logger.debug(f"Stored result for {request_id}")
        else:
            # This might happen if the server restarted or the request timed out
            logger.warning(f"Received result for unknown or expired request_id: {request_id}")
            # Optionally, could still store it for a short time in case of race conditions

    return {"status": "success", "request_id": request_id}
# --- End Endpoint for Reporting Plugin Results ---

# --- Add Endpoint for Receiving Studio Logs (NEW) ---
@app.post("/receive_studio_logs")
async def receive_studio_logs(logs: List[StudioLogEntry], request: Request):
    """Endpoint for the Roblox Studio plugin to push captured logs."""
    global studio_log_buffer
    client_host = request.client.host if request.client else "unknown"
    try:
        server_received_time = datetime.now().timestamp()
        log_count = len(logs)
        logger.info(f"Received {log_count} log entries from plugin at {client_host}")
        # Store logs with server timestamp for potential sorting/filtering later
        # Convert Pydantic model back to dict for storage if needed, or store model directly
        # Storing dicts might be simpler for the tool later
        processed_logs = [
            (server_received_time, log.model_dump()) for log in logs
        ]
        studio_log_buffer.extend(processed_logs)
        return {"status": "success", "received": log_count}
    except Exception as e:
        logger.exception(f"Error processing logs from {client_host}")
        # Don't raise HTTPException usually, just log and return error status
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})
# --- End Endpoint for Receiving Studio Logs ---

# --- MCP Server Instance (Handles Tool Definitions) ---
# Note: We still need the FastMCP instance to register tools to.
mcp_server = FastMCP(
    "VibeBlocksMCP", # <<< RENAME
    description="Roblox Studio integration via MCP (SSE Transport)",
)
logger.info("FastMCP instance created for tool registration.")
# --- End MCP Server Instance ---

# --- Mount SSE Server onto Main App (at root, AFTER defining other routes) ---
# The SSE server internally uses the mcp_server instance to run the MCP protocol
if global_config: # Only mount if config loaded, otherwise client tools fail anyway
    app.mount("/", create_sse_server(mcp_server), name="mcp_sse") # Mount back at root
    logger.info("Mounted SSE MCP transport server at /") # Log correct path
else:
    logger.error("MCP SSE Server not mounted because configuration failed to load.")
# --- End Mount SSE Server ---


# --- Tool to Queue Command for Plugin (Remains registered with MCP) ---
# This tool is called via the SSE MCP connection now
@mcp_server.tool()
async def queue_studio_command(ctx: Context,
                               command: Dict[str, Any] = Field(..., description="The command dictionary to send to the Studio plugin.")) -> str:
    """Queues a command to be picked up by the companion Studio plugin via the /plugin_command endpoint."""
    global plugin_command_queue
    try:
        plugin_command_queue.append(command)
        logger.info(f"Queued command for Studio plugin via MCP: {command}") # Keep "MCP" generic here
        return f"Successfully queued command: {command}"
    except Exception as e:
        logger.exception("Error queuing command for plugin via MCP")
        return f"Error queuing command: {e}"
# --- End Tool to Queue Command for Plugin ---

# --- Tool to Queue MULTIPLE Commands for Plugin ---
@mcp_server.tool()
async def queue_studio_command_batch(ctx: Context,
                                   command_batch: List[Dict[str, Any]] = Field(..., description="A list of command dictionaries to send sequentially to the Studio plugin.")) -> str:
    """Queues a batch of commands to be picked up sequentially by the companion Studio plugin.
       Useful for sending multi-step instructions generated by the LLM.
    """
    global plugin_command_queue
    commands_queued = 0
    try:
        if not isinstance(command_batch, list):
            return "Error: Input must be a list of command dictionaries."
        
        for command in command_batch:
            if isinstance(command, dict):
                plugin_command_queue.append(command)
                commands_queued += 1
                logger.debug(f"Queued command from batch: {command}") # Debug level might be better
            else:
                logger.warning(f"Skipping non-dictionary item in command batch: {command}")
                
        logger.info(f"Queued {commands_queued} commands for Studio plugin via MCP batch tool.") # Keep "MCP" generic here
        return f"Successfully queued {commands_queued} commands."
    except Exception as e:
        logger.exception("Error queuing command batch for plugin via MCP")
        return f"Error queuing command batch: {e}"
# --- End Tool to Queue MULTIPLE Commands for Plugin ---

# --- Tool Helper Functions ---
def escape_lua_string(value: str) -> str:
    """Safely escapes a string for embedding in Lua code."""
    # Basic escaping for quotes, backslashes, and newlines
    escaped = value.replace('\\\\', '\\\\\\\\').replace('"', '\\\\"').replace("\\n", "\\\\n") # Double escape backslashes
    return f'"{escaped}"'

def value_to_lua_string(value: Any, property_name: Optional[str] = None) -> str:
    """Converts a Python value to its Lua string representation for scripting.
       Uses property_name hint for context-specific conversions (e.g., BrickColor, Enums).
    """
    if isinstance(value, str):
        # --- CONTEXT-AWARE STRING HANDLING --- --
        prop_lower = property_name.lower() if property_name else ""

        # 1. Check for specific property types first
        if prop_lower == "brickcolor":
            # Check if it looks like a standard BrickColor name (heuristic)
            if re.match(r"^(?:[A-Z][a-zA-Z0-9 ]+|[a-z0-9 ]+)$", value):
                logger.info(f"Treating string '{value}' as BrickColor name for property '{property_name}'.")
                return f'BrickColor.new({escape_lua_string(value)})'
            else:
                 logger.warning(f"Value '{value}' for BrickColor property '{property_name}' doesn't look like a standard name. Treating as escaped string.")
                 return escape_lua_string(value)

        elif prop_lower in ["material", "parttype", "formfactor", "style", "axis", "faces", "shape"]: # Add other Enum properties here
            if value.startswith("Enum."):
                logger.info(f"Passing Enum string '{value}' directly to Lua for property '{property_name}'.")
                return value # Assume it's already valid Lua code (e.g., "Enum.Material.Plastic")
            else:
                 logger.warning(f"Value '{value}' for Enum property '{property_name}' doesn't start with 'Enum.'. Treating as escaped string.")
                 return escape_lua_string(value)

        elif prop_lower in ["position", "size", "orientation"]:
             if value.lower().startswith("vector3.new("):
                 logger.info(f"Passing Vector3 string '{value}' directly to Lua for property '{property_name}'.")
                 return value # Assume it's already valid Lua code
             else:
                 # Fallback to default string handling if not explicitly Vector3.new()
                 return escape_lua_string(value)

        elif prop_lower == "color": # For Color3 properties
            if value.lower().startswith("color3.fromrgb(") or value.lower().startswith("color3.new("):
                 logger.info(f"Passing Color3 string '{value}' directly to Lua for property '{property_name}'.")
                 return value # Assume it's already valid Lua code
            else:
                 # Fallback to default string handling
                 return escape_lua_string(value)

        # 2. Check for general formats IF property type wasn't specific
        # (Less reliable, use specific property checks above first)
        # elif value.startswith("Enum."):
        #     logger.info(f"Passing Enum string '{value}' directly to Lua (generic check).")
        #     return value
        # elif value.lower().startswith("vector3.new("):
        #      logger.info(f"Passing Vector3 string '{value}' directly to Lua (generic check).")
        #      return value
        # elif value.lower().startswith("color3.fromrgb("):
        #      logger.info(f"Passing Color3 string '{value}' directly to Lua (generic check).")
        #      return value

        # --- DEFAULT: Treat as plain escaped string --- --
        else:
            # If none of the specific property types match, treat as a simple escaped string.
            # This is crucial for properties like 'Name', 'Value' (for StringValue), etc.
            logger.debug(f"Treating string '{value}' as plain escaped string for property '{property_name or 'None'}'.")
            return escape_lua_string(value)
        # --- END CONTEXT-AWARE STRING HANDLING --- --

    elif isinstance(value, bool):
        return str(value).lower()
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, list) and len(value) == 3 and all(isinstance(v, (int, float)) for v in value):
        # Assume lists of 3 numbers are Vector3, suitable for Position/Size etc.
        logger.info(f"Converting list {value} to Vector3 for property '{property_name or 'None'}'.")
        return f"Vector3.new({value[0]}, {value[1]}, {value[2]})"
    elif isinstance(value, list):
        # Basic table conversion for simple lists
        items = ", ".join(value_to_lua_string(item, None) for item in value) # No property hint for list items
        return f"{{{items}}}"
    elif isinstance(value, dict):
        # Basic table conversion for simple dicts (string keys only for now)
        items = ", ".join(f'["{k}"] = {value_to_lua_string(v, None)}' for k, v in value.items()) # No property hint for dict values
        return f"{{{items}}}"
    elif value is None:
        return "nil"
    else:
        logger.warning(f"Unsupported type {type(value)} for Lua conversion (property: {property_name}). Treating as string.")
        return escape_lua_string(str(value)) # Fallback to string

def safe_object_path(path: str) -> str:
    """Validates and formats an object path string for use in Lua."""
    # <<< ADD LOGGING: Show the exact input path >>>
    # logger.debug(f"[safe_object_path] Received path: {path!r}") # Original f-string log
    # <<< CHANGE: Use %-formatting for safer logging >>>
    logger.debug("[safe_object_path] Received path: %r", path)

    # <<< ADD try...except around validation >>>
    try:
        is_invalid = not re.match(r"^(game|workspace|[\w.]+)$", path, re.IGNORECASE) or ".." in path
    except Exception as validation_err:
        logger.error("[safe_object_path] Error during validation check for path %r: %r", path, validation_err, exc_info=True)
        # Re-raise the specific error to see if it's the one we're hunting
        raise validation_err

    if is_invalid:
    # if not re.match(r"^(game|workspace|[\w.]+)$", path, re.IGNORECASE) or ".." in path:
        # <<< Log before raising the explicit format error >>>
        logger.warning("[safe_object_path] Path failed validation: %r", path)
        raise ValueError(f"Invalid object path format: {path}")

    # lower_path = path.lower()

    # <<< ADD Explicit str conversion and logging >>>
    try:
        logger.debug("[safe_object_path] Type before str() conversion: %s", type(path))
        path_str = str(path)
        logger.debug("[safe_object_path] Path after str() conversion: %r", path_str)
        lower_path = path_str.lower()
        logger.debug("[safe_object_path] Lowercase path: %r", lower_path)
    except Exception as convert_err:
        logger.error("[safe_object_path] Error during str() or .lower() for path %r: %r", path, convert_err, exc_info=True)
        # Re-raise to be caught by list_children
        raise ValueError(f"Error during path conversion: {convert_err}") from convert_err

    if lower_path == 'workspace': # Line C
        return 'workspace'
    if lower_path == 'game':
        return 'game'
        
    # Handle paths starting with game. or workspace.
    if lower_path.startswith('game.'):
        return 'game' + path[len('game'):] 
    if lower_path.startswith('workspace.'):
         return 'workspace' + path[len('workspace'):] 

    # --- Check if it starts with a known service name --- 
    service_match = None
    service_name_original_case = None
    remaining_path = None
    for service in KNOWN_ROBLOX_SERVICES:
        if lower_path == service:
             service_match = service
             service_name_original_case = path # Use original case if it's just the service
             break
        elif lower_path.startswith(service + '.'):
            service_match = service
            # Find the original casing of the service part
            # This assumes the input path uses consistent casing for the service part
            service_name_original_case = path[:len(service)] 
            remaining_path = path[len(service):] # Includes the leading dot
            break

    # Handle known top-level services or paths starting with them
    if service_match:
        service_ref = f'game:GetService("{service_name_original_case}")'
        if remaining_path:
             logger.debug(f"Treating path '{path}' as service '{service_name_original_case}' + path.")
             return service_ref + remaining_path # Append the rest of the path (e.g., .StarterPlayerScripts)
        else:
             logger.debug(f"Treating path '{path}' as a known service.")
             return service_ref # Just the service itself

    # --- Fallback: Assume it's relative to workspace (last resort) ---
    # (Only reach here if it doesn't start with game/workspace/known service and doesn't contain dots)
    if '.' not in path:
        logger.debug(f"Assuming path '{path}' is relative to workspace (last resort).")
        if path in ["and", "or", "not", "local", "function", "if", "then", "else", "end", "while", "do", "for", "in", "return", "break", "true", "false", "nil"]:
                raise ValueError(f"Path '{path}' conflicts with Lua keyword.")
        return f"workspace:FindFirstChild({escape_lua_string(path)}, true)" 
    
    # --- If it contains dots but didn't match any service pattern ---
    # This case is less common now but could happen for things like game.Lighting.Something
    # if Lighting wasn't in KNOWN_ROBLOX_SERVICES initially.
    # Treat as full path, let Lua handle errors.
    logger.debug(f"Treating path '{path}' as a potential full path (not starting with known service).")
    return path


# --- Tool Definitions (Registered with mcp_server) ---

# --- Refactor Tool Handlers to Initialize Client ---
async def _get_roblox_client() -> Optional[RobloxClient]:
    """Helper to get or initialize the Roblox client based on global config."""
    if not global_config:
        logger.error("Cannot initialize RobloxClient: Configuration not loaded.")
        return None
    try:
        # Consider caching this client instance if performance becomes an issue,
        # but initializing per-call is safer for now.
        # We need to properly handle session closing if we cache.
        client = RobloxClient(global_config)
        # logger.debug(f"Initialized RobloxClient instance: {id(client)}")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize RobloxClient: {e}", exc_info=True)
        return None

@mcp_server.tool()
async def execute_luau_in_cloud(ctx: Context, script_text: str = Field(..., description="The Luau code script to execute in the target place."), target_place_id: Optional[int] = Field(None, description="Optional Place ID to execute against, defaults to configured Place ID.")) -> str:
    """Executes arbitrary Luau script via the Roblox Cloud API and returns output or errors.
       Runs in a separate cloud environment, NOT the live Studio session.
    """
    global last_script_logs
    logger.info(f"Executing Luau script via Cloud API (first 100 chars): {script_text[:100]}...")
    last_script_logs = {"output": None, "error": None}
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    try:
        # call_luau now returns parsed JSON (dict/list) or raw concatenated logs (str)
        result = await client.call_luau(script=script_text, target_place_id=target_place_id)
        last_script_logs["output"] = result # Store whatever was returned

        # --- Updated Result Handling --- 
        if isinstance(result, (dict, list)):
            # If we got a dict/list, assume it was parsed JSON
            try:
                # Pretty-print the JSON result
                output_str = json.dumps(result, indent=2)
                logger.info(f"Luau execution returned parsed JSON object.")
                # Check for an error key within the JSON itself
                if isinstance(result, dict) and result.get("error"):
                     error_msg = result["error"]
                     last_script_logs["error"] = error_msg
                     return f"Script reported error (JSON): {error_msg}"
                # Check for specific script_errors key
                elif isinstance(result, dict) and result.get("script_errors"):
                     script_errs = result["script_errors"]
                     last_script_logs["error"] = script_errs
                     return f"Script reported internal errors: {json.dumps(script_errs)}"
                else:
                     return f"""Script executed successfully (JSON Output):
{output_str}"""
            except (TypeError, ValueError) as json_err:
                # Should be rare if result is already dict/list, but handle just in case
                logger.error(f"Error formatting JSON result: {json_err}")
                last_script_logs["error"] = f"JSON formatting error: {json_err}"
                return f"Script executed, but failed to format JSON result: {result}"
        elif isinstance(result, str):
            # If we got a string, assume it's concatenated logs
            logger.info(f"Luau execution returned raw string output.")
            last_script_logs["output"] = result # Already stored, but re-assign for clarity
            # Return the raw string output directly
            # Truncate long outputs for display?
            max_len = 1000
            if len(result) > max_len:
                return f"""Script executed successfully (Raw Output Truncated):
{result[:max_len]}..."""
            else:
                return f"""Script executed successfully (Raw Output):
{result}"""
        else:
            # Handle unexpected return types from call_luau
            logger.error(f"call_luau returned unexpected type: {type(result)}")
            last_script_logs["error"] = f"Unexpected return type from client: {type(result)}"
            return f"Error: Script execution returned unexpected data type: {type(result).__name__}"
        # --- End Updated Result Handling ---

    except RobloxApiError as e:
        # Handle errors raised during the API call process (e.g., connection, permissions)
        logger.error(f"Roblox API Error during Luau execution call: {e}", exc_info=True) # Log traceback
        last_script_logs["error"] = str(e)
        # Provide more context if it's a script error returned via the API error
        if "Luau script execution error" in str(e) and e.response_data:
             error_detail = e.response_data.get('message', json.dumps(e.response_data)) if isinstance(e.response_data, dict) else str(e.response_data)
             return f"Error executing Luau script (API Response): {error_detail}"
        return f"Error executing Luau script (API Call): {e}"
    except NotImplementedError as e:
        logger.error("execute_luau tool called but RobloxClient.call_luau is not fully implemented.")
        last_script_logs["error"] = "NotImplementedError in client"
        return "Error: Luau execution feature is not yet implemented in the client."
    except Exception as e:
        logger.exception("Unexpected error during execute_luau tool execution.") # Log traceback
        last_script_logs["error"] = f"Unexpected server error: {e}"
        return f"Unexpected server error: {e}"
    finally:
        # Ensure client session is closed if initialized per-call
        if client:
            await client.close_session()

@mcp_server.tool()
async def get_property(ctx: Context, object_name: str = Field(..., description="Name or path of the object (e.g., 'MyPart' or 'Workspace.Model.Part')."), property_name: str = Field(..., description="Name of the property to retrieve (e.g., 'Position', 'Name', 'BrickColor').")) -> str:
    """Retrieves the value of a specific property from an object via the Studio Plugin."""
    # <<< CHANGE: Use plugin queue AND WAIT instead of Luau execution >>>
    logger.info(f"Requesting property '{property_name}' for object '{object_name}' via plugin")

    # Basic validation
    if not re.match(r"^[\w.]+$", object_name): # Allow dots in object name path
        return f"Tool: get_property, Error: Invalid object name format: {object_name}"
    if not re.match(r"^\w+$", property_name):
        return f"Tool: get_property, Error: Invalid property name format: {property_name}"

    command = {
        "action": "get_property",
        "data": {
            "object_name": object_name,
            "property_name": property_name
        }
        # request_id will be added by queue_command_and_wait
    }

    try:
        # Use the helper to queue and wait
        result_data = await queue_command_and_wait(command, timeout=10.0)

        logger.info(f"Received result for get_property({object_name}.{property_name}): {result_data}")

        # --- Result Processing ---
        if isinstance(result_data, dict):
            if "error" in result_data:
                error_msg = result_data["error"]
                logger.error(f"Plugin reported error for get_property: {error_msg}")
                return f"Tool: get_property, Error from plugin: {error_msg}"
            elif "value" in result_data:
                value = result_data["value"]
                # Format the output nicely
                # If the value itself is a dict (e.g., serialized Vector3), pretty print it
                if isinstance(value, (dict, list)):
                    value_str = json.dumps(value, indent=2)
                else:
                    value_str = str(value)
                return f"Tool: get_property, Result: Property '{property_name}' of '{object_name}' is: {value_str}"
            else:
                # Plugin returned a dictionary but without 'error' or 'value'
                logger.warning(f"Received unexpected dictionary format from plugin for get_property: {result_data}")
                return f"Tool: get_property, Error: Received unexpected result format from plugin: {result_data}"
        else:
            # Plugin returned something other than a dictionary
            logger.warning(f"Received non-dictionary result from plugin for get_property: {result_data}")
            return f"Tool: get_property, Error: Received unexpected result type from plugin: {type(result_data).__name__}"
        # --- End Result Processing ---

    except TimeoutError as e:
        logger.error(f"Timeout waiting for get_property result: {e}")
        return f"Tool: get_property, Error: Timeout waiting for response from Studio plugin for '{object_name}.{property_name}'."
    except Exception as e:
        logger.exception(f"Error executing get_property for {object_name}.{property_name}")
        return f"Tool: get_property, Error: An unexpected server error occurred: {e}"
    # <<< END CHANGE >>>

@mcp_server.tool()
async def list_children(ctx: Context, parent_name: str = Field("Workspace", description="Name or path of the parent object (e.g., 'Workspace', 'Workspace.Model').")) -> str:
    """Retrieves children of an object via the Studio Plugin and waits for the result."""
    # <<< CHANGE: Use plugin queue AND WAIT instead of just queueing >>>
    logger.info(f"Requesting list_children via plugin for parent: '{parent_name}'")

    # Define the command to be sent to the plugin
    command = {
        "action": "list_children",
        "data": {
            "parent_name": parent_name # Send the original name for the plugin to resolve
        }
        # request_id will be added by queue_command_and_wait
    }

    try:
        logger.info(f"Attempting to list children for parent: {parent_name}")
        
        # Use the new helper to queue and wait for the result
        result_data = await queue_command_and_wait(command, timeout=15.0) # Increased timeout slightly
        
        logger.info(f"Received result for list_children({parent_name}): {result_data}")

        # --- Result Processing ---
        if isinstance(result_data, dict) and "error" in result_data:
             # Plugin reported an error
             error_msg = result_data["error"]
             logger.error(f"Plugin reported error for list_children({parent_name}): {error_msg}")
             # Return error string suitable for MCP tool output
             return f"Tool: list_children, Error from plugin: {error_msg}"
        elif isinstance(result_data, list):
             # Assume success, result is the list of children dicts
             # Format the list into a user-friendly string output
             if not result_data:
                  return f"Tool: list_children, Result: No children found for '{parent_name}'."
             else:
                  output_str = f"Tool: list_children, Result: Children of '{parent_name}':\n"
                  # <<< CHANGE: Include path in output formatting >>>
                  output_str += "\n".join([f"- {child.get('name', '?')} ({child.get('className', '?')}) Path: {child.get('path', '?')}" for child in result_data])
                  return output_str
        else:
            # Unexpected result format from plugin
            logger.warning(f"Received unexpected result format for list_children({parent_name}): {result_data} (Type: {type(result_data)})")
            return f"Tool: list_children, Error: Received unexpected result format from plugin: {result_data}"
        # --- End Result Processing ---

    except TimeoutError as e:
        logger.error(f"Timeout waiting for list_children result for parent: {parent_name}: {e}")
        return f"Tool: list_children, Error: Timeout waiting for response from Studio plugin for parent '{parent_name}'."
    except Exception as e:
        # Catch potential errors from queue_command_and_wait or other issues
        logger.exception(f"Error executing list_children for parent {parent_name}")
        return f"Tool: list_children, Error: An unexpected server error occurred: {e}"
    # <<< END CHANGE >>>

@mcp_server.tool()
async def find_instances(ctx: Context,
                     class_name: str = Field(default=None, description="ClassName to filter by (e.g., 'Part', 'Model')."),
                     name_contains: str = Field(default=None, description="Text the instance name should contain (case-insensitive)."),
                     search_root: str = Field("Workspace", description="Name or path of the object to search under (e.g., 'Workspace', 'ReplicatedStorage.Models').")
                     ) -> str:
    """Finds instances within a specified root based on class name or name containing text via the Studio Plugin."""
    # <<< CHANGE: Use plugin queue AND WAIT instead of Luau execution >>>
    logger.info(f"Requesting find_instances via plugin under '{search_root}' (class: {class_name or 'Any'}, name contains: {name_contains or 'Any'})")

    # Basic validation (can add more for search_root if needed)
    # ... (validation skipped for brevity, assume safe inputs for now)

    command = {
        "action": "find_instances",
        "data": {
            "class_name": class_name,         # Pass None if not provided
            "name_contains": name_contains,   # Pass None if not provided
            "search_root": search_root
        }
        # request_id will be added by queue_command_and_wait
    }

    try:
        # Use the helper to queue and wait
        result_data = await queue_command_and_wait(command, timeout=20.0) # Allow slightly longer timeout for search

        logger.info(f"Received result for find_instances({search_root}, {class_name}, {name_contains}): {result_data}")

        # --- Result Processing ---
        if isinstance(result_data, dict):
            if "error" in result_data:
                error_msg = result_data["error"]
                logger.error(f"Plugin reported error for find_instances: {error_msg}")
                return f"Tool: find_instances, Error from plugin: {error_msg}"
            elif "instances" in result_data:
                instances = result_data["instances"]
                if not instances:
                    return f"Tool: find_instances, Result: No instances found matching criteria under '{search_root}'."
                else:
                    # Expecting list of dicts like {name, className, path}
                    output_str = f"Tool: find_instances, Result: Found {len(instances)} instance(s) under '{search_root}':\n"
                    output_str += "\n".join([f"- {inst.get('name', '?')} ({inst.get('className', '?')}) at path: {inst.get('path', '?')}" for inst in instances])
                    return output_str
            else:
                logger.warning(f"Received unexpected dictionary format from plugin for find_instances: {result_data}")
                return f"Tool: find_instances, Error: Received unexpected result format from plugin: {result_data}"
        else:
            logger.warning(f"Received non-dictionary result from plugin for find_instances: {result_data}")
            return f"Tool: find_instances, Error: Received unexpected result type from plugin: {type(result_data).__name__}"
        # --- End Result Processing ---

    except TimeoutError as e:
        logger.error(f"Timeout waiting for find_instances result: {e}")
        return f"Tool: find_instances, Error: Timeout waiting for response from Studio plugin under '{search_root}'."
    except Exception as e:
        logger.exception(f"Error executing find_instances under {search_root}")
        return f"Tool: find_instances, Error: An unexpected server error occurred: {e}"
    # <<< END CHANGE >>>

@mcp_server.tool()
async def create_instance(ctx: Context,
                      class_name: str = Field(..., description="The ClassName of the instance to create (e.g., 'Part', 'Model', 'Script')."),
                      properties: Dict[str, Any] = None,
                      parent_name: str = Field("Workspace", description="Name or path of the parent object to create the instance under (defaults to Workspace).")) -> str:
    """Creates a new instance via Studio Plugin command queue."""
    logger.info(f"Creating instance: Class='{class_name}', Parent='{parent_name}', Props={properties}")
    
    # Basic validation
    if not re.match(r"^\w+$", class_name):
        return f"Error: Invalid ClassName format: {class_name}"
    # TODO: Consider adding validation for parent_name and property keys/values

    command = {
        "action": "create_instance",
        "data": {
            "class_name": class_name,
            "parent_name": parent_name,
            "properties": properties if properties else {}
        }
    }

    try:
        # Ensure properties is always a dict
        if properties is None:
            properties = {}
        # Queue the command AND WAIT for the result
        result = await queue_command_and_wait(command)
        
        # Process the result from the plugin
        if "error" in result:
            error_msg = result["error"]
            # Truncate long errors if necessary
            if isinstance(error_msg, str) and len(error_msg) > 250:
                error_msg = error_msg[:250] + "..."
            logger.error(f"Plugin reported error for create_instance: {error_msg}")
            return f"Error creating instance: {error_msg}"
        elif "success" in result and result["success"]:
            instance_name = result.get('name', properties.get('Name', class_name))
            instance_path = result.get('path', 'unknown path')
            logger.info(f"Plugin successfully created instance '{instance_name}' at {instance_path}")
            return f"Successfully created {class_name} '{instance_name}' at '{instance_path}'."
        else:
            logger.warning(f"Received unexpected result format from plugin for create_instance: {result}")
            return f"Error: Unexpected result format from plugin while creating instance."

    except TimeoutError:
        logger.error(f"Timeout waiting for create_instance result for {class_name}")
        return f"Error: Timeout waiting for Studio plugin to create instance '{class_name}'."
    except Exception as e:
        logger.exception("Unexpected error in create_instance tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def delete_instance(ctx: Context, object_name: str = Field(..., description="Name or path of the object to delete (e.g., 'MyPart', 'Workspace.Model').")) -> str:
    """Deletes an object from the scene by calling its :Destroy() method via the Studio Plugin."""
    # <<< CHANGE: Use plugin queue AND WAIT instead of Luau execution >>>
    logger.info(f"Requesting delete_instance via plugin for object '{object_name}'")

    # Basic validation
    if not re.match(r"^[\w.]+$", object_name): # Allow dots in object name path
        return f"Tool: delete_instance, Error: Invalid object name format: {object_name}"

    command = {
        "action": "delete_instance",
        "data": {
            "object_name": object_name
        }
        # request_id will be added by queue_command_and_wait
    }

    try:
        # Use the helper to queue and wait
        result_data = await queue_command_and_wait(command, timeout=10.0)

        logger.info(f"Received result for delete_instance({object_name}): {result_data}")

        # --- Result Processing ---
        if isinstance(result_data, dict):
            if result_data.get("success"):
                 return f"Tool: delete_instance, Result: Successfully requested deletion of '{object_name}'."
            elif "error" in result_data:
                error_msg = result_data["error"]
                logger.error(f"Plugin reported error for delete_instance: {error_msg}")
                return f"Tool: delete_instance, Error from plugin: {error_msg}"
            else:
                logger.warning(f"Received unexpected dictionary format from plugin for delete_instance: {result_data}")
                return f"Tool: delete_instance, Error: Received unexpected result format from plugin: {result_data}"
        else:
            logger.warning(f"Received non-dictionary result from plugin for delete_instance: {result_data}")
            return f"Tool: delete_instance, Error: Received unexpected result type from plugin: {type(result_data).__name__}"
        # --- End Result Processing ---

    except TimeoutError as e:
        logger.error(f"Timeout waiting for delete_instance result: {e}")
        return f"Tool: delete_instance, Error: Timeout waiting for response from Studio plugin for '{object_name}'."
    except Exception as e:
        logger.exception(f"Error executing delete_instance for {object_name}")
        return f"Tool: delete_instance, Error: An unexpected server error occurred: {e}"
    # <<< END CHANGE >>>

@mcp_server.tool()
async def set_property(ctx: Context,
                     object_name: str = Field(..., description="Name or path of the object."),
                     property_name: str = Field(..., description="Name of the property to set."),
                     # <<< CHANGE: Added example of escaped JSON string >>>
                     value: str = Field(..., description='JSON string for the value (e.g., `"hello"`, `5`, `true`, `null`, `"[1,2,3]"`, `"{\\"key\\":\\"val\\"}"`). Primitives are passed directly. Complex types (like lists or dicts) must be valid JSON *within* the string.') 
                     ) -> str:
    """Sets a specific property on an object using a JSON string input. 
-       NOTE: Requires the value parameter to be a string containing valid JSON due to framework limitations. 
-       Crucially, when providing the JSON payload for the tool call, complex types must be represented as *escaped JSON strings*.
-       Tool Call Payload Examples for 'value':
-       - String:    `"\"hello\""`
-       - Number:    `"5"`
-       - Boolean:   `"true"`
-       - Nil:       `"null"`
-       - List/Vec3: `"[0, 10, 0]"`
-       - Dict:      `"{\"name\": \"MyPart\"}"`
-       The inner Python function then parses the string content (e.g., '[0, 10, 0]').
-       Use specific tools (e.g., move_instance, set_primary_part) or create_instance (with properties) for complex types where possible.
+       The 'value' parameter should be a string containing valid JSON representing the desired value.
+       - For primitive types (string, number, boolean, nil), provide them as standard JSON strings: `"\"hello\""`, `"5"`, `"true"`, `"null"`.
+       - For complex types (like Vector3, Color3, lists, dictionaries), provide the JSON representation as a string: `"[0, 10, 0]"`, `"{\"r\": 1, \"g\": 0, \"b\": 0}"`.
+       The plugin will attempt to convert the parsed JSON value to the appropriate Roblox type based on the property name.
+       Use specific tools (e.g., move_instance, set_primary_part, create_instance with properties) for complex types where possible, as they offer better type handling.
     """
    logger.info(f"Setting property '{property_name}' on '{object_name}' from JSON string: {value}")
    
    try:
        # Manually parse the JSON string value again
        try:
            parsed_value = json.loads(value)
            logger.debug(f"Parsed value type from JSON string: {type(parsed_value).__name__}")
        except json.JSONDecodeError as json_err:
            logger.error(f"Invalid JSON string provided for value: {value} - Error: {json_err}")
            return f"Error: Invalid JSON format for value parameter. Details: {json_err}"

        if not re.match(r"^\w+$", property_name):
            return f"Error: Invalid property name format: {property_name}"

        # Create command for the plugin with the PARSED value
        command = {
            "action": "set_property",
            "data": {
                "object_name": object_name,
                "property_name": property_name,
                "value": parsed_value 
            }
        }
        
        # Queue the command and wait for result
        result = await queue_command_and_wait(command)
        
        # Process the result
        if "error" in result:
            error_msg = result["error"]
            if isinstance(error_msg, str) and len(error_msg) > 200:
                error_msg = error_msg[:200] + "..."
            return f"Error setting property '{property_name}': {error_msg}"
        elif "success" in result and result["success"]:
            return f"Successfully set property '{property_name}' on '{object_name}'."
        else:
            return f"Error: Unexpected result format from plugin while setting property."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to set property '{property_name}' on '{object_name}'."
    except ValueError as e: # Catch other potential errors like invalid property name format
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in set_property tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def set_primary_part(ctx: Context,
                          model_path: str = Field(..., description="Path to the Model object."),
                          part_path: str = Field(..., description="Path to the BasePart object to set as the PrimaryPart.")) -> str:
    """Sets the PrimaryPart property of a Model to the specified BasePart."""
    logger.info(f"Setting PrimaryPart of '{model_path}' to '{part_path}'")

    # Basic path validation (could be stricter)
    if not re.match(r"^[\w.]+$", model_path):
        return f"Error: Invalid model path format: {model_path}"
    if not re.match(r"^[\w.]+$", part_path):
        return f"Error: Invalid part path format: {part_path}"

    try:
        # Create command for the plugin
        command = {
            "action": "set_primary_part",
            "data": {
                "model_path": model_path,
                "part_path": part_path
            }
        }

        # Queue the command and wait for result
        result = await queue_command_and_wait(command)

        # Process the result
        if "error" in result:
            return f"Error setting PrimaryPart for '{model_path}': {result['error']}"
        elif "success" in result and result["success"]:
            return f"Successfully set PrimaryPart of '{model_path}' to '{part_path}'."
        else:
            return f"Error: Unexpected result format from plugin while setting PrimaryPart."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to set PrimaryPart for '{model_path}'."
    except ValueError as e: # Catch other potential errors like invalid path format
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in set_primary_part tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def move_instance(ctx: Context, object_name: str = Field(..., description="Name or path of the object to move."),
                  # <<< CHANGE: Expect standard JSON string for dictionary >>>
                  position: str = Field(..., description='JSON string for the position dictionary, e.g., `"{\"x\": 0, \"y\": 10, \"z\": 0}"`.')
                  ) -> str:
    """Moves an object to a new position using a JSON string dictionary for the position."""
    logger.info(f"Moving '{object_name}' from position JSON string: {position}")
    
    try:
        # <<< ADD: Parse JSON string to dictionary >>>
        try:
            position_dict = json.loads(position)
            logger.debug(f"Parsed position dictionary: {position_dict}")
        except json.JSONDecodeError as json_err:
            logger.error(f"Invalid JSON string provided for position: {position} - Error: {json_err}")
            return f"Error: Invalid JSON format for position parameter. Expected a dictionary string like '{{\"x\":0, \"y\":0, \"z\":0}}'. Details: {json_err}"

        # <<< CHANGE: Validate parsed dictionary >>>
        if not isinstance(position_dict, dict) or not all(k in position_dict for k in ['x', 'y', 'z']) or not all(isinstance(position_dict[k], (int, float)) for k in ['x', 'y', 'z']):
            return "Error: Invalid position dictionary structure after parsing JSON. Expected keys 'x', 'y', 'z' with number values."
    
        # Create command for the plugin, using the PARSED position dict
        command = {
            "action": "move_instance",
            "data": {
                "object_name": object_name,
                "position": position_dict # Pass the parsed dictionary
            }
        }
        
        # Queue the command and wait for result
        result = await queue_command_and_wait(command)
        
        # Process the result
        if "error" in result:
            return f"Error moving '{object_name}': {result['error']}"
        elif "success" in result and result["success"]:
            # Format output using dict values from parsed dict
            return f"Successfully moved '{object_name}' to position (x={position_dict['x']}, y={position_dict['y']}, z={position_dict['z']})."
        else:
            return f"Error: Unexpected result format from plugin while moving instance."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to move '{object_name}'."
    except ValueError as e: # Catch other potential errors
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in move_instance tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def clone_instance(ctx: Context, object_name: str = Field(..., description="Name or path of the object to clone."),
                     new_name: Optional[str] = Field(None, description="Optional new name for the cloned object."),
                     parent_name: Optional[str] = Field(None, description="Optional name or path for the parent of the clone (defaults to original parent).")) -> str:
    """Clones an existing object, optionally giving it a new name and parent."""
    logger.info(f"Cloning instance '{object_name}' (new name: {new_name}, parent: {parent_name})")
    
    try:
        # Create command for the plugin
        command = {
            "action": "clone_instance",
            "data": {
                "object_name": object_name,
                "new_name": new_name,
                "parent_name": parent_name
            }
        }
        
        # Queue the command and wait for result
        result = await queue_command_and_wait(command)
        
        # Process the result
        if "error" in result:
            return f"Error cloning instance '{object_name}': {result['error']}"
        elif "success" in result and result["success"]:
            clone_name = result.get("clone_name", "Unknown")
            clone_path = result.get("clone_path", "Unknown path")
            parent_error = result.get("parent_error")
            
            msg = f"Successfully cloned '{object_name}' to '{clone_name}' at path '{clone_path}'."
            if parent_error:
                msg += f" Warning: {parent_error}"
            return msg
        else:
            return f"Error: Unexpected result format from plugin while cloning instance."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to clone '{object_name}'."
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in clone_instance tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def create_script(ctx: Context, script_name: str = Field(..., description="The name for the new Script instance."),
                  script_code: str = Field(..., description="The Luau code content for the script."),
                  script_type: str = Field("Script", description="Type of script: 'Script' or 'LocalScript'."),
                  parent_name: str = Field("Workspace", description="Name or path of the parent object (defaults to Workspace).")) -> str:
    """Creates a new Script or LocalScript instance with the provided code under the specified parent."""
    logger.info(f"Creating {script_type} named '{script_name}' under '{parent_name}'")
    
    if script_type not in ["Script", "LocalScript"]:
        return f"Error: Invalid script_type '{script_type}'. Must be 'Script' or 'LocalScript'."
    
    try:
        # Create command for the plugin
        command = {
            "action": "create_script",
            "data": {
                "script_name": script_name,
                "script_code": script_code,
                "script_type": script_type,
                "parent_name": parent_name
            }
        }
        
        # Queue the command and wait for result
        result = await queue_command_and_wait(command)
        
        # Process the result
        if "error" in result:
            return f"Error creating script: {result['error']}"
        elif "success" in result and result["success"]:
            return f"Successfully created {script_type} '{result.get('name', script_name)}' at '{result.get('path', 'unknown path')}'."
        else:
            return f"Error: Unexpected result format from plugin while creating script."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to create script."
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in create_script tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def edit_script(ctx: Context, script_path: str = Field(..., description="Name or path of the script to edit."),
                   script_code: str = Field(..., description="The new Luau code content for the script.")) -> str:
    """Edits the source code of an existing Script or LocalScript instance."""
    logger.info(f"Editing script at '{script_path}'")
    
    try:
        # Create command for the plugin
        command = {
            "action": "edit_script",
            "data": {
                "script_path": script_path,
                "script_code": script_code
            }
        }
        
        # Queue the command and wait for result
        result = await queue_command_and_wait(command)
        
        # Process the result
        if "error" in result:
            return f"Error editing script: {result['error']}"
        elif "success" in result and result["success"]:
            return f"Successfully updated script at '{script_path}'."
        else:
            return f"Error: Unexpected result format from plugin while editing script."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to edit script."
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in edit_script tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def delete_script(ctx: Context, script_path: str = Field(..., description="Name or path of the script to delete.")) -> str:
    """Deletes an existing Script or LocalScript instance."""
    logger.info(f"Deleting script at '{script_path}'")
    
    try:
        # Create command for the plugin
        command = {
            "action": "delete_script",
            "data": {
                "script_path": script_path
            }
        }
        
        # Queue the command and wait for result
        result = await queue_command_and_wait(command)
        
        # Process the result
        if "error" in result:
            return f"Error deleting script: {result['error']}"
        elif "success" in result and result["success"]:
            return f"Successfully deleted script at '{script_path}'."
        else:
            return f"Error: Unexpected result format from plugin while deleting script."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to delete script."
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in delete_script tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def list_datastores_in_cloud(ctx: Context, prefix: Optional[str] = Field(None, description="Filter datastores with names starting with this prefix."),
                    limit: Optional[int] = Field(None, description="Maximum number of datastores to return."),
                    cursor: Optional[str] = Field(None, description="Pagination cursor from a previous response.")) -> str:
    """Lists standard datastores via the Roblox Cloud API within the configured universe."""
    logger.info(f"Listing datastores via Cloud API (prefix: {prefix}, limit: {limit})")
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    try:
        result = await client.list_datastores(prefix=prefix, limit=limit, cursor=cursor)
        stores = result.get("datastores", [])
        next_cursor = result.get("nextPageCursor")

        if not stores:
            return "No datastores found." + (f" (Prefix: {prefix})" if prefix else "")

        output = "Datastores found:\n" + "\n".join([f"- {store.get('name')}" for store in stores])
        if next_cursor:
            output += f"\n\n(Next page cursor: {next_cursor})"
        return output

    except RobloxApiError as e:
        logger.error(f"API Error listing datastores: {e}")
        return f"Error listing datastores: {e}"
    except Exception as e:
        logger.exception("Unexpected error in list_datastores tool.")
        return f"Unexpected server error: {e}"
    finally:
        if client:
            await client.close_session()

@mcp_server.tool()
async def get_datastore_value_in_cloud(ctx: Context, datastore_name: str = Field(..., description="The name of the datastore."),
                        entry_key: str = Field(..., description="The key of the entry to retrieve."),
                        scope: Optional[str] = Field("global", description="The scope of the datastore (defaults to 'global').")) -> str:
    """Gets the value of an entry from a standard datastore via the Roblox Cloud API."""
    logger.info(f"Getting datastore value via Cloud API for key '{entry_key}' from '{datastore_name}' (scope: {scope})")
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    try:
        value = await client.get_datastore_entry(datastore_name=datastore_name, entry_key=entry_key, scope=scope)

        if value is None:
            return f"No entry found for key '{entry_key}' in datastore '{datastore_name}' (scope: {scope})."

        # Format the value nicely
        if isinstance(value, (dict, list)):
            return f"Value for key '{entry_key}':\n{json.dumps(value, indent=2)}"
        else:
            return f"Value for key '{entry_key}': {value}"

    except RobloxApiError as e:
        logger.error(f"API Error getting datastore value: {e}")
        return f"Error getting value for key '{entry_key}': {e}"
    except Exception as e:
        logger.exception("Unexpected error in get_datastore_value tool.")
        return f"Unexpected server error: {e}"
    finally:
        if client:
            await client.close_session()

@mcp_server.tool()
async def set_datastore_value_in_cloud(ctx: Context, datastore_name: str = Field(..., description="The name of the datastore."),
                        entry_key: str = Field(..., description="The key of the entry to set."),
                        value: Any = Field(..., description="The JSON-serializable value to store."),
                        scope: Optional[str] = Field("global", description="The scope (defaults to 'global')."),
                       ) -> str:
    """Sets the value for an entry in a standard datastore via the Roblox Cloud API."""
    logger.info(f"Setting datastore value via Cloud API for key '{entry_key}' in '{datastore_name}' (scope: {scope})")
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    try:
        result = await client.set_datastore_entry(datastore_name=datastore_name, entry_key=entry_key, value=value, scope=scope)
        version = result.get("version")
        return f"Successfully set value for key '{entry_key}'. New version: {version}"

    except RobloxApiError as e:
        logger.error(f"API Error setting datastore value: {e}")
        return f"Error setting value for key '{entry_key}': {e}"
    except ValueError as e: # Handles non-JSON serializable value from client method
         return f"Error setting value: {e}"
    except Exception as e:
        logger.exception("Unexpected error in set_datastore_value tool.")
        return f"Unexpected server error: {e}"
    finally:
        if client:
            await client.close_session()

@mcp_server.tool()
async def delete_datastore_value_in_cloud(ctx: Context, datastore_name: str = Field(..., description="The name of the datastore."),
                           entry_key: str = Field(..., description="The key of the entry to delete."),
                           scope: Optional[str] = Field("global", description="The scope (defaults to 'global').")) -> str:
    """Deletes an entry from a standard datastore via the Roblox Cloud API."""
    logger.info(f"Deleting datastore key via Cloud API: '{entry_key}' from '{datastore_name}' (scope: {scope})")
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    try:
        await client.delete_datastore_entry(datastore_name=datastore_name, entry_key=entry_key, scope=scope)
        return f"Successfully deleted entry '{entry_key}' from datastore '{datastore_name}'."
    except RobloxApiError as e:
        # Check if it was a 404 (key didn't exist anyway)
        if e.status_code == 404:
             logger.info(f"Attempted to delete non-existent key '{entry_key}'.")
             return f"Entry '{entry_key}' did not exist in datastore '{datastore_name}'."
        logger.error(f"API Error deleting datastore value: {e}")
        return f"Error deleting value for key '{entry_key}': {e}"
    except Exception as e:
        logger.exception("Unexpected error in delete_datastore_value tool.")
        return f"Unexpected server error: {e}"
    finally:
        if client:
            await client.close_session()

@mcp_server.tool()
async def upload_asset_via_cloud(ctx: Context, file_path: str = Field(..., description="Local path to the asset file (e.g., .fbx, .png, .mp3)."),
                   asset_type: str = Field(..., description="Type of asset (e.g., 'Model', 'Image', 'Audio'). Check Roblox docs for valid types."),
                   display_name: str = Field(..., description="Name for the asset in Roblox."),
                   description: Optional[str] = Field("", description="Optional description for the asset.")) -> str:
    """Uploads a file from the local system as a new Roblox asset via the Cloud API."""
    logger.info(f"Uploading asset '{display_name}' ({asset_type}) via Cloud API from '{file_path}'")
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    # Basic validation for asset type (can be expanded based on Roblox API specifics)
    if not re.match(r"^\w+$", asset_type):
         return f"Error: Invalid asset type format: '{asset_type}'"

    try:
        result = await client.upload_asset(file_path=file_path, asset_type=asset_type, display_name=display_name, description=description)
        asset_id = result.get("assetId")
        if asset_id:
            return f"Successfully uploaded asset '{display_name}'. New Asset ID: {asset_id}"
        else:
            # This case should ideally be caught by exceptions in the client
            return f"Error: Asset upload completed but no Asset ID was returned. Result: {result.get('operationResult')}"

    except FileNotFoundError as e:
        return f"Error: File not found at path: {file_path}"
    except RobloxApiError as e:
        logger.error(f"API Error uploading asset: {e}")
        return f"Error uploading asset '{display_name}': {e}"
    except NotImplementedError as e:
        logger.error(f"Asset upload called but not fully implemented in client: {e}")
        return f"Error: Asset upload feature not implemented in client."
    except Exception as e:
        logger.exception("Unexpected error in upload_asset tool.")
        return f"Unexpected server error: {e}"
    finally:
        if client:
            await client.close_session()

@mcp_server.tool()
async def get_asset_details_via_cloud(ctx: Context, asset_id: int = Field(..., description="The ID of the asset to retrieve details for.")) -> str:
    """Gets details about a specific asset via the Roblox Cloud API using its ID."""
    logger.info(f"Getting details via Cloud API for asset ID {asset_id}")
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    try:
        details = await client.get_asset_details(asset_id=asset_id)
        return f"Asset Details for {asset_id}:\n{json.dumps(details, indent=2)}"
    except NotImplementedError:
        return "Error: Get asset details feature is not yet implemented."
    except RobloxApiError as e:
        logger.error(f"API Error getting asset details: {e}")
        return f"Error getting details for asset {asset_id}: {e}"
    except Exception as e:
        logger.exception(f"Unexpected error in get_asset_details tool.")
        return f"Unexpected server error: {e}"
    finally:
        if client:
            await client.close_session()

@mcp_server.tool()
async def list_user_assets_via_cloud(ctx: Context, asset_types: Optional[List[str]] = Field(None, description="Optional list of asset types to filter by (e.g., ['Model', 'Image'])."),
                       limit: Optional[int] = Field(None, description="Maximum number of assets to return."),
                       cursor: Optional[str] = Field(None, description="Pagination cursor.")) -> str:
    """Lists assets owned by the authenticated user via the Roblox Cloud API."""
    logger.info(f"Listing user assets via Cloud API (types: {asset_types}, limit: {limit})")
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    try:
        result = await client.list_assets(asset_types=asset_types, limit=limit, cursor=cursor)
        # Process and format the list result based on actual API response structure
        # Example assuming a structure like {"data": [...], "nextPageCursor": ...}
        assets = result.get("data", []) # Adjust key based on actual response
        next_cursor = result.get("nextPageCursor")

        if not assets:
            return "No assets found matching the criteria."

        # Adjust formatting based on details available in the list response
        output = "User Assets Found:\n" + "\n".join([f"- ID: {asset.get('assetId', 'N/A')}, Name: {asset.get('name', 'N/A')}, Type: {asset.get('type', 'N/A')}" for asset in assets])
        if next_cursor:
            output += f"\n\n(Next page cursor: {next_cursor})"
        return output

    except NotImplementedError:
        return "Error: List user assets feature is not yet implemented."
    except RobloxApiError as e:
        logger.error(f"API Error listing assets: {e}")
        return f"Error listing user assets: {e}"
    except Exception as e:
        logger.exception("Unexpected error in list_user_assets tool.")
        return f"Unexpected server error: {e}"
    finally:
        if client:
            await client.close_session()

@mcp_server.tool()
async def publish_place_via_cloud(ctx: Context, target_place_id: Optional[int] = Field(None, description="Optional Place ID to publish. Defaults to configured Place ID."),
                    version_type: str = Field("Saved", description="Version type to publish: 'Saved' or 'Published'.")) -> str:
    """Publishes the specified place via the Roblox Cloud API."""
    logger.info(f"Publishing place via Cloud API (ID: {target_place_id or 'default'}, Type: {version_type})")
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    try:
        result = await client.publish_place(target_place_id=target_place_id, version_type=version_type)
        version_number = result.get("versionNumber")
        place_id_used = target_place_id or (client.place_id if client else 'N/A') # Added client check
        if version_number:
            return f"Successfully published place {place_id_used}. New version number: {version_number}"
        else:
            return f"Place {place_id_used} published, but version number not found in response: {result}"

    except RobloxApiError as e:
        logger.error(f"API Error publishing place: {e}")
        return f"Error publishing place: {e}"
    except ValueError as e: # Catches invalid version_type from client
         return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in publish_place tool.")
        return f"Unexpected server error: {e}"
    finally:
        if client:
            await client.close_session()

@mcp_server.tool()
async def set_environment(ctx: Context,
                      properties: Dict[str, Any] = Field(..., description="Dictionary of properties to set on the Lighting service or Terrain.")) -> str:
    """Sets properties on environment services like Lighting or Terrain."""
    logger.info(f"Setting environment properties: {properties}")
    
    # Determine target service based on properties
    target = "Lighting"
    terrain_props = ["WaterColor", "WaterWaveSize", "WaterWaveSpeed", "WaterReflectance", "WaterTransparency"]
    if any(prop in properties for prop in terrain_props):
        target = "Terrain"
    
    try:
        # Check for invalid property names 
        for key in properties.keys():
            if not re.match(r"^\w+$", key):
                return f"Error: Invalid property name format: {key}"
        
        # Create command for the plugin
        command = {
            "action": "set_environment",
            "data": {
                "target": target,
                "properties": properties
            }
        }
        
        # Queue the command and wait for result
        result = await queue_command_and_wait(command)
        
        # Process the result
        if "error" in result:
            return f"Error setting environment properties on {target}: {result['error']}"
        elif "errors" in result and result["errors"]:
            error_str = json.dumps(result["errors"])
            logger.error(f"set_environment reported errors: {error_str}")
            return f"Error setting some environment properties on {target}: {error_str}"
        elif "success" in result and result["success"]:
            return f"Successfully set environment properties on {target}. (Note: Changes may not persist)"
        else:
            return f"Error: Unexpected result format from plugin while setting environment properties."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to set environment properties."
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in set_environment tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def spawn_npc(ctx: Context, model_asset_id: Optional[int] = Field(None, description="Asset ID of the NPC model to insert from Roblox library."),
                template_model_name: Optional[str] = Field(None, description="Name of an existing model in the place (e.g., in ServerStorage) to clone as the NPC."),
                position: Optional[List[float]] = Field([0,5,0], description="Position [X, Y, Z] where the NPC should be spawned."),
                parent_name: str = Field("Workspace", description="Parent object for the spawned NPC (defaults to Workspace)."),
                new_name: Optional[str] = Field(None, description="Optional name for the spawned NPC instance.")) -> str:
    """Spawns an NPC in the workspace, either by inserting a model from asset ID or cloning an existing template model."""
    logger.info(f"Spawning NPC (AssetID: {model_asset_id}, Template: {template_model_name}, Name: {new_name})")
    
    if not model_asset_id and not template_model_name:
        return "Error: Must provide either model_asset_id or template_model_name to spawn NPC."
    
    if model_asset_id and template_model_name:
        logger.warning("Both model_asset_id and template_model_name provided, using model_asset_id.")
        template_model_name = None  # Prioritize asset ID
    
    try:
        # Convert position to dictionary format expected by the plugin
        position_dict = None
        if position and len(position) == 3:
            position_dict = {
                "x": float(position[0]),
                "y": float(position[1]),
                "z": float(position[2])
            }
        
        # Create command for the plugin
        command = {
            "action": "spawn_npc",
            "data": {
                "model_asset_id": model_asset_id,
                "template_model_name": template_model_name,
                "position": position_dict,
                "parent_name": parent_name,
                "new_name": new_name
            }
        }
        
        # Queue the command and wait for result
        result = await queue_command_and_wait(command)
        
        # Process the result
        if "error" in result:
            return f"Error spawning NPC: {result['error']}"
        elif "success" in result and result["success"]:
            msg = f"Successfully spawned NPC '{result.get('name', 'unnamed')}' at '{result.get('path', 'unknown path')}'."
            if result.get('warning'):
                msg += f" Warning: {result['warning']}"
            if result.get('position_error'):
                msg += f" Position Error: {result['position_error']}"
            return msg
        else:
            return f"Error: Unexpected result format from plugin while spawning NPC."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to spawn NPC."
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in spawn_npc tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def play_animation(ctx: Context, target_name: str = Field(..., description="Name or path of the object with Humanoid or AnimationController (e.g., player character, NPC)."),
                     animation_id: int = Field(..., description="Asset ID of the Animation to play.")) -> str:
    """Loads and plays an animation on a target object's Humanoid or AnimationController."""
    logger.info(f"Playing animation {animation_id} on target '{target_name}'")
    
    try:
        # Create command for the plugin
        command = {
            "action": "play_animation",
            "data": {
                "target_name": target_name,
                "animation_id": animation_id
            }
        }
        
        # Queue the command and wait for result
        result = await queue_command_and_wait(command)
        
        # Process the result
        if "error" in result:
            return f"Error playing animation: {result['error']}"
        elif "success" in result and result["success"]:
            return f"Successfully played animation {animation_id} on '{target_name}'. {result.get('message', '')}"
        else:
            return f"Error: Unexpected result format from plugin while playing animation."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to play animation on '{target_name}'."
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in play_animation tool.")
        return f"Unexpected server error: {e}"

@mcp_server.tool()
async def send_chat_via_cloud(ctx: Context, message: str = Field(..., description="The chat message content."),
                sender_name: Optional[str] = Field(None, description="Optional name of the player or system sending the message (uses default if None).")) -> str:
    """Sends a message to the in-game chat via the Roblox Cloud API (execute_luau).
       Note: Functionality depends heavily on the game's chat setup and requires TextChatService.
    """
    logger.info(f"Sending chat message via Cloud API: '{message}' (from: {sender_name or 'System'})")
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    try:
        lua_message = escape_lua_string(message)
        # Sender logic needs refinement based on desired behavior (system vs player)
        lua_sender_logic = "nil -- Use default sender"
        if sender_name:
            # Find player or create fake sender? This part is complex.
            # For simplicity, let's assume a basic system message for now.
            lua_sender_logic = f"{{ Name = {escape_lua_string(sender_name)}, UserId = 0 }} -- Placeholder sender"
            logger.warning("send_chat sender logic is simplified.")

        script = f"""
        local httpService = game:GetService("HttpService")
        local result = {{success=false}}
        local textChatService = game:GetService("TextChatService")
        if textChatService then
            local channel = textChatService:FindFirstChild("RBXSystem") -- Or find appropriate channel
            if channel and channel:IsA("TextChannel") then
                local success_send, err_send = pcall(function()
                    -- Send message (API might change)
                    channel:SendAsync({lua_message})
                    -- Older/Alternative: channel:DisplaySystemMessage(message)
                end)
                result.success = success_send
                if not success_send then result.error = "Failed to send message: " .. tostring(err_send) end
            else
                 result.error = "Could not find suitable TextChannel (e.g., RBXSystem)."
            end
        else
            result.error = "TextChatService not found."
        end
        print(httpService:JSONEncode(result))
        """

        exec_result = await execute_luau_in_cloud(ctx, script=script)

        # Parse result
        script_output_obj = exec_result.get("output", {})
        if isinstance(script_output_obj, dict):
            if script_output_obj.get("success"):
                 return f"Chat message sent successfully."
            else:
                  return f"Error sending chat: {script_output_obj.get('error', 'Script reported failure')}"
        else:
             if exec_result.get("error"):
                  error_info = exec_result["error"]
                  error_msg = error_info.get('message', json.dumps(error_info))
                  return f"Error executing send chat script: {error_msg}"
             else:
                 raw = script_output_obj.get("raw_output", str(script_output_obj))
                 return f"Error: Could not parse status from send chat script output: {raw}"

    except RobloxApiError as e:
        logger.error(f"API Error sending chat: {e}")
        if "Luau script execution error" in str(e) and e.response_data:
             return f"Error sending chat message: {e.response_data.get('message', json.dumps(e.response_data))}"
        return f"Error sending chat message: {e}"
    except ValueError as e:
         return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in send_chat tool.")
        return f"Unexpected server error: {e}"
    finally:
        if client:
            await client.close_session()

@mcp_server.tool()
async def teleport_player_via_cloud(ctx: Context, player_name: str = Field(..., description="The exact name of the Player to teleport."),
                      destination_place_id: int = Field(..., description="The Place ID to teleport the player to."),
                      teleport_options: Optional[Dict[str, Any]] = Field(None, description="Optional TeleportOptions dictionary."),
                      custom_loading_script: Optional[str] = Field(None, description="Optional LocalScript name (in ReplicatedFirst) for custom loading screen.")) -> str:
    """Teleports a player via the Roblox Cloud API (execute_luau).
       Requires TeleportService.
    """
    logger.info(f"Teleporting player via Cloud API: '{player_name}' to place {destination_place_id}")
    client = await _get_roblox_client()
    if not client:
        return "Error: Roblox Client could not be initialized."

    try:
        lua_player_name = escape_lua_string(player_name)
        lua_options = value_to_lua_string(teleport_options) if teleport_options else "nil"
        lua_loading_script = f"game:GetService(\"ReplicatedFirst\"):FindFirstChild({escape_lua_string(custom_loading_script)})" if custom_loading_script else "nil"

        script = f"""
        local httpService = game:GetService("HttpService")
        local result = {{success=false}}
        local teleportService = game:GetService("TeleportService")
        local playersService = game:GetService("Players")
        local player = playersService:FindFirstChild({lua_player_name})

        if not teleportService then result.error = "TeleportService not found."
        elseif not player then result.error = "Player '{player_name}' not found."
        else
            local options = {lua_options}
            local loadingScript = {lua_loading_script}

            local success_tp, err_tp = pcall(function()
                teleportService:TeleportAsync({destination_place_id}, {{player}}, options, loadingScript)
            end)
            result.success = success_tp
            if not success_tp then result.error = "Teleport failed: " .. tostring(err_tp) end
        end
        print(httpService:JSONEncode(result))
        """

        exec_result = await execute_luau_in_cloud(ctx, script=script)

        # Parse result
        script_output_obj = exec_result.get("output", {})
        if isinstance(script_output_obj, dict):
            if script_output_obj.get("success"):
                 return f"Teleport initiated for player '{player_name}' to place {destination_place_id}."
            else:
                  return f"Error teleporting player: {script_output_obj.get('error', 'Script reported failure')}"
        else:
             if exec_result.get("error"):
                  error_info = exec_result["error"]
                  error_msg = error_info.get('message', json.dumps(error_info))
                  return f"Error executing teleport script: {error_msg}"
             else:
                 raw = script_output_obj.get("raw_output", str(script_output_obj))
                 return f"Error: Could not parse status from teleport script output: {raw}"

    except RobloxApiError as e:
        logger.error(f"API Error teleporting player: {e}")
        if "Luau script execution error" in str(e) and e.response_data:
             return f"Error teleporting player '{player_name}': {e.response_data.get('message', json.dumps(e.response_data))}"
        return f"Error teleporting player '{player_name}': {e}"
    except ValueError as e:
         return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in teleport_player tool.")
        return f"Unexpected server error: {e}"
    finally:
        if client:
            await client.close_session()

@mcp_server.tool()
async def get_studio_logs(ctx: Context) -> List[Dict[str, Any]]: # REMOVED random_string parameter AGAIN
    """Retrieves the most recent logs captured from the Roblox Studio Output window."""
    # The random_string parameter caused issues, removed it again. Tool schema might be inconsistent.
    global studio_log_buffer
    
    safe_limit = 200 # Default to max buffer size now
    
    current_logs = list(studio_log_buffer) # Get a snapshot
    log_slice = current_logs[-safe_limit:]
    
    formatted_logs = [log_data for server_ts, log_data in log_slice]
    
    logger.info(f"Returning last {len(formatted_logs)} studio logs (limit: {safe_limit}).") 
    return formatted_logs

# --- Helper Function to Queue Command and Prepare for Result ---
async def queue_command_and_wait(command: Dict[str, Any], timeout: float = 20.0) -> Any: # <<< CHANGE: Increased default timeout >>>
    """
    Queues a command, adds a request_id, and waits for the result via /plugin_report_result.
    Returns the result or raises TimeoutError.
    """
    global plugin_command_queue, pending_plugin_results, plugin_results_lock
    
    request_id = str(uuid.uuid4())
    command_with_id = {**command, "request_id": request_id} # Add request_id to command
    
    try:
        # Initialize pending result entry
        with plugin_results_lock:
            pending_plugin_results[request_id] = None # Mark as pending

        # Queue the command
        plugin_command_queue.append(command_with_id)
        logger.info(f"Queued command with request_id {request_id}: {command_with_id}")

        # Wait for the result
        start_time = time.monotonic()
        while time.monotonic() < start_time + timeout:
            with plugin_results_lock:
                result = pending_plugin_results.get(request_id)
            
            if result is not None:
                logger.info(f"Result received for request_id {request_id}")
                # Clean up the entry
                with plugin_results_lock:
                    del pending_plugin_results[request_id]
                return result # Return the actual result data
            
            await asyncio.sleep(0.1) # Small sleep to prevent busy-waiting

        # Timeout occurred
        logger.warning(f"Timeout waiting for result for request_id {request_id}")
        # Clean up the pending entry on timeout
        with plugin_results_lock:
            if request_id in pending_plugin_results:
                del pending_plugin_results[request_id]
        raise TimeoutError(f"Timeout waiting for plugin result for request_id {request_id}")

    except Exception as e:
        logger.exception(f"Error in queue_command_and_wait for request_id {request_id}")
        # Ensure cleanup even if other errors occur
        with plugin_results_lock:
            if request_id in pending_plugin_results:
                del pending_plugin_results[request_id]
        raise # Re-raise the exception

# --- NEW: Execute Luau in Studio via Plugin --- 
@mcp_server.tool()
async def execute_luau_in_studio(ctx: Context, script_code: str = Field(..., description="The Luau code string to execute directly in the Studio session via the plugin.")) -> str:
    """Executes arbitrary Luau script in the LIVE Studio session via the plugin.
       WARNING: Use with caution. Captures print output, return values, and errors.
    """
    logger.info(f"Executing Luau script in Studio via Plugin (first 100 chars): {script_code[:100]}...")

    command = {
        "action": "execute_script_in_studio",
        "data": {
            "script_code": script_code
        }
    }

    try:
        # Use the helper to queue and wait (use a potentially longer timeout for scripts)
        result_data = await queue_command_and_wait(command, timeout=30.0) 

        logger.info(f"Received result for execute_luau_in_studio: {result_data}")

        # --- Result Processing --- 
        if isinstance(result_data, dict):
            output_lines = result_data.get("output_lines", [])
            return_values = result_data.get("return_values") # Could be None or a list
            error_msg = result_data.get("error_message")

            output_str = "-- Execute Luau in Studio Result --\n"
            
            # Add captured output
            if output_lines:
                output_str += "\n[Output]:\n"
                output_str += "\n".join(output_lines)
                output_str += "\n"
            else:
                 output_str += "\n[No Output Captured]"
                 
            # Add return values (nicely formatted)
            if return_values is not None:
                 try:
                     return_str = json.dumps(return_values, indent=2)
                     output_str += "\n[Return Values]:\n" + return_str + "\n"
                 except TypeError:
                      output_str += f"\n[Return Values (Raw)]:\n{return_values}\n"
            
            # Add error if present
            if error_msg:
                output_str += f"\n[Error]: {error_msg}\n"
            
            return output_str.strip()
            
        else:
            # Plugin returned something unexpected
            logger.warning(f"Received non-dictionary result from plugin for execute_luau_in_studio: {result_data}")
            return f"Tool: execute_luau_in_studio, Error: Received unexpected result type from plugin: {type(result_data).__name__}"
        # --- End Result Processing --- 

    except TimeoutError as e:
        logger.error(f"Timeout waiting for execute_luau_in_studio result: {e}")
        return f"Tool: execute_luau_in_studio, Error: Timeout waiting for response from Studio plugin."
    except Exception as e:
        logger.exception("Error executing execute_luau_in_studio tool")
        return f"Tool: execute_luau_in_studio, Error: An unexpected server error occurred: {e}"
# --- END: Execute Luau in Studio via Plugin --- 

# --- NEW: Modify Children Tool --- 
@mcp_server.tool()
async def modify_children(ctx: Context,
                        parent_path: str = Field(..., description="Path to the parent object whose children will be modified."),
                        property_name: str = Field(..., description="The name of the property to set on matching children."),
                        property_value: str = Field(..., description='JSON string for the value to set (same format as set_property: primitives or escaped JSON for complex types, e.g., \'"[1,2,3]"\').'),
                        child_name_filter: Optional[str] = Field(None, description="Optional: Only modify children with this exact name."),
                        child_class_filter: Optional[str] = Field(None, description="Optional: Only modify children of this exact ClassName.")) -> str:
    """Finds direct children under a parent matching optional filters (name/class) and sets a specified property on them."""
    logger.info(f"Modifying children under '{parent_path}' (Name: {child_name_filter or 'Any'}, Class: {child_class_filter or 'Any'}) - Set '{property_name}' from JSON: {property_value}")

    # Basic validation
    if not re.match(r"^[\w.]+$", parent_path):
        return f"Error: Invalid parent path format: {parent_path}"
    if not re.match(r"^\w+$", property_name):
        return f"Error: Invalid property name format: {property_name}"
    if child_name_filter is not None and not isinstance(child_name_filter, str):
         return f"Error: Invalid child_name_filter format (must be string or null)."
    if child_class_filter is not None and not re.match(r"^\w+$", child_class_filter):
         return f"Error: Invalid child_class_filter format: {child_class_filter}"

    try:
        # Parse the JSON string value
        try:
            parsed_value = json.loads(property_value)
            logger.debug(f"Parsed property value: {parsed_value} (Type: {type(parsed_value).__name__})")
        except json.JSONDecodeError as json_err:
            logger.error(f"Invalid JSON string provided for property_value: {property_value} - Error: {json_err}")
            return f"Error: Invalid JSON format for property_value parameter. Details: {json_err}"

        # Create command for the plugin
        command = {
            "action": "modify_children",
            "data": {
                "parent_path": parent_path,
                "property_name": property_name,
                "property_value": parsed_value, # Send parsed value
                "child_name_filter": child_name_filter,
                "child_class_filter": child_class_filter
            }
        }

        # Queue the command and wait for result
        result = await queue_command_and_wait(command, timeout=60.0) # Longer timeout for potentially many children

        # Process the result
        if "error_message" in result: # Check for fatal error first
            return f"Error modifying children under '{parent_path}': {result['error_message']}"
        elif "affected_count" in result:
            affected_count = result.get("affected_count", 0)
            errors = result.get("errors", [])
            msg = f"Successfully modified {affected_count} children under '{parent_path}' matching criteria."
            if errors:
                msg += f" Encountered {len(errors)} errors during modification: {errors[:5]}..." # Show first few errors
                logger.warning(f"modify_children reported errors: {errors}")
            return msg
        else:
            return f"Error: Unexpected result format from plugin while modifying children."

    except TimeoutError:
        return f"Error: Timeout waiting for Studio plugin to modify children under '{parent_path}'."
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Unexpected error in modify_children tool.")
        return f"Unexpected server error: {e}"
# --- END: Modify Children Tool ---