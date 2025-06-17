[![MseeP.ai Security Assessment Badge](https://mseep.net/pr/majidmanzarpour-vibe-blocks-mcp-badge.png)](https://mseep.ai/app/majidmanzarpour-vibe-blocks-mcp)

# Vibe Blocks MCP for Roblox Studio

Connects Roblox Studio to AI coding editors (like Cursor, Windsurf, Claude, etc) via the Model Context Protocol (MCP), enabling AI-assisted game development within your Roblox Studio environment.

## Overview

This project consists of two main parts:

1.  **Python MCP Server:** A FastAPI server that runs locally. It exposes Roblox Studio actions as tools over MCP (using Server-Sent Events - SSE). It can optionally interact with Roblox Open Cloud APIs if configured.
2.  **Lua Companion Plugin:** A Roblox Studio plugin (`roblox_mcp_plugin/src/Plugin.server.lua`) that runs inside Studio. It polls the local Python server for commands, executes them in the Studio context (manipulating instances, reading properties, executing Luau), and sends results and Studio logs back to the server.

This allows an AI agent connected via MCP to understand and interact with your live Roblox Studio session.

## Features

*   **Live Studio Interaction:**
    *   **Scene Manipulation:** Create, delete, clone, move, scale, and set properties (including PrimaryPart) of objects (Parts, Models, Scripts, etc.) directly in the Studio scene.
    *   **Scene Inspection:** Get object properties, list children, find instances by class or name within Studio.
    *   **Scripting:** Create, edit, and delete Scripts/LocalScripts. Execute arbitrary Luau code *directly within the Studio environment* and capture output/errors.
    *   **Environment:** Set properties on Lighting or Terrain services.
    *   **Animation:** Play animations on Humanoids/AnimationControllers.
    *   **NPCs:** Spawn NPCs by cloning existing templates or inserting from Asset IDs.
    *   **Modify Children:** Apply property changes to multiple children of an object based on filters.
    *   **Studio Logs:** Retrieve recent logs from the Studio Output window.
*   **Roblox Open Cloud Integration (Optional - Requires API Key):**
    *   **Luau Execution (Cloud):** Run Luau code in a separate cloud environment (useful for tasks not requiring live Studio access).
    *   **DataStores:** List stores, get, set, and delete key-value entries in standard DataStores.
    *   **Assets:** Upload new assets (Models, Images, Audio) from local files.
    *   **Publishing:** Publish the currently saved or published version of a place.
    *   **(Planned):** Get asset details, list user assets.

## Setup

**1. Prerequisites:**

*   Python >= 3.10
*   `uv` package manager ([Install uv](https://github.com/astral-sh/uv#installation)). This is highly recommended for faster dependency management.
*   Roblox Studio
*   **(Optional)** A Roblox API Key for Open Cloud features. Get one from [Roblox Creator Dashboard > Credentials](https://create.roblox.com/credentials). You'll need permissions for the APIs you intend to use (DataStore, Asset Upload, Publishing, Luau Execution etc.).
*   **(Optional)** Your Roblox Universe ID and the target Place ID (needed for Open Cloud features).

**2. Clone the Repository:**

```bash
git clone https://github.com/majidmanzarpour/vibe-blocks-mcp 
cd vibe-blocks-mcp
```

**3. Install Dependencies:**

Using `uv` (recommended):

```bash
uv pip sync pyproject.toml
```

Alternatively, using `pip`:

```bash
pip install -r requirements.lock # Or create requirements.txt from pyproject.toml if needed
```

**4. Configure Environment (Optional - For Cloud Features):**

*   If you plan to use the Open Cloud tools (DataStores, Asset Upload, Publishing, Cloud Luau), copy the example environment file:
    ```bash
    cp .env.example .env
    ```
*   **Edit the `.env` file:**
    *   Replace `"YOUR_API_KEY_HERE"` with your Roblox API Key.
    *   Replace `0` for `ROBLOX_UNIVERSE_ID` with your Universe ID.
    *   Replace `0` for `ROBLOX_PLACE_ID` with the target Place ID.
*   **If you don't need Cloud features, you can skip creating the `.env` file.** The server will still run, but Cloud-related tools will return an error.

**5. Install the Companion Plugin in Roblox Studio:**

*   **Install Rojo:** If you don't have Rojo installed, follow the instructions on the [Rojo website](https://rojo.space/docs/install/).
*   **Build the Plugin (optional):** Navigate to the `roblox_mcp_plugin` directory in your terminal and run:
    ```bash
    rojo build default.project.json --output VibeBlocksMCP_Companion.rbxm
    ```
    This will create a `VibeBlocksMCP_Companion.rbxm` file or you can use the one provided in the repository.
*   **Install in Studio:**
    *   Find your Roblox Studio plugins folder:
        *   **Windows:** `%LOCALAPPDATA%\Roblox\Plugins`
        *   **macOS:** `~/Documents/Roblox/Plugins` (You might need to use `Cmd+Shift+G` in Finder and paste the path to navigate there, or click Plugin Folder in Roblox Studio).
    *   Move or copy the generated `VibeBlocksMCP_Companion.rbxm` file into this plugins folder.
*   **Restart Roblox Studio:** The plugin should now be loaded automatically when you open Studio.
    *   **Note:** The plugin polls `http://localhost:8000/plugin_command`. If you change the server port, you'll need to update the `SERVER_URL` variable at the top of the Lua script (`roblox_mcp_plugin/src/Plugin.server.lua`) and rebuild the plugin.

**6. Run the Python Server:**

*   Open your terminal in the project's root directory.
*   Make the server script executable (if you haven't already):
    ```bash
    chmod +x server.sh 
    ```
*   Run the server:
    ```bash
    ./server.sh
    ```
*   The server will start, check/install `uvicorn` if needed, and log that it's running on `http://localhost:8000`.
*   Keep this terminal window open while you're using the service.

**7. Connect from MCP Client (e.g., Cursor):**

*   This service works with any AI client that supports the Model Context Protocol (MCP) via Server-Sent Events (SSE), such as Cursor, Windsurf, or potentially future versions of Claude Desktop.
*   **Example using Cursor:**
    *   Go to `File > Settings > MCP` (or `Code > Settings > MCP` on Mac).
    *   Click "Add New Global MCP Server".
    *   Enter the **SSE URL:** `http://localhost:8000/sse` (make sure to include the trailing `/sse`).
    *   You may need to edit the mcp.json file
    ```
    {
    "mcpServers": {
      "Vibe Blocks MCP": {
        "url": "http://localhost:8000/sse"
        }
      }
    }
    ```
*   The client should now detect the "Vibe Blocks MCP" tool source and its available tools.

## Usage

Once the server is running, the plugin is installed in Studio, and your MCP client is connected, you can interact with your Studio session through the AI.

Address the agent (@-mentioning the tools  if your client requires it, e.g., `list_children`) and ask it to perform actions.

**Example Prompts:**

*   "Create a bright red Part named 'Floor' in Workspace. Set its size to (100, 2, 100) and position to (0, -1, 0). Anchor it."
*   "Delete the object named 'Workspace.OldPlatform'"
*   "What is the Position property of 'Workspace.SpawnLocation'?"
*   "List the children of ServerScriptService."
*   "Find all instances with className 'Script' under ServerScriptService."
*   "Execute this script in Studio: `print(game:GetService('Lighting').ClockTime)`"
*   "Set the `ClockTime` property of Lighting to 14."
*   "Clone 'ReplicatedStorage.Templates.EnemyNPC' and name the clone 'Guard1'. Parent it to Workspace."
*   "Make the model named 'Workspace.Guard1' play animation asset 123456789."
*   "Modify all children of 'Workspace.DecorationFolder' with className 'Part' to have their Material set to 'Neon'."
*   **(Cloud Example)** "Upload './assets/MyCoolModel.fbx' as a Model named 'Cool Character Model'."
*   **(Cloud Example)** "Get the value for key 'player_123_score' from the 'PlayerData' datastore."
*   **(Cloud Example)** "Publish the current place."
*   "Show me the latest logs from the Studio output."

## Available Tools

*(Tools interact either directly with the Studio Plugin or with Roblox Open Cloud APIs)*

**Studio Plugin Tools (Live Interaction):**

*   `get_property`: Retrieves the value of a specific property from an object in Studio.
*   `list_children`: Retrieves direct children of an object in Studio.
*   `find_instances`: Finds instances within a specified root based on class name or name containing text in Studio.
*   `create_instance`: Creates a new instance (Part, Model, Script, etc.) in Studio.
*   `delete_instance`: Deletes an object from the Studio scene.
*   `set_property`: Sets a specific property on an object in Studio (uses JSON string for value).
*   `set_primary_part`: Sets the PrimaryPart property of a Model.
*   `move_instance`: Moves an object (Model or BasePart) to a new position in Studio.
*   `clone_instance`: Clones an existing object in Studio.
*   `create_script`: Creates a new Script or LocalScript instance with provided code in Studio.
*   `edit_script`: Edits the source code of an existing Script or LocalScript in Studio.
*   `delete_script`: Deletes an existing Script or LocalScript instance in Studio.
*   `set_environment`: Sets properties on environment services (Lighting or Terrain) in Studio.
*   `spawn_npc`: Spawns an NPC in Studio, either by inserting a model from asset ID or cloning an existing template model.
*   `play_animation`: Loads and plays an animation on a target object's Humanoid or AnimationController in Studio.
*   `execute_luau_in_studio`: Executes arbitrary Luau script in the LIVE Studio session via the plugin and captures output/return values/errors.
*   `modify_children`: Finds direct children under a parent matching optional filters (name/class) and sets a specified property on them.
*   `get_studio_logs`: Retrieves the most recent logs captured from the Roblox Studio Output window via the plugin.

**Open Cloud API Tools (Optional - Require `.env` setup):**

*   `execute_luau_in_cloud`: Executes arbitrary Luau script via the Roblox Cloud API (runs in a separate cloud environment, not live Studio).
*   `list_datastores_in_cloud`: Lists standard datastores via the Cloud API.
*   `get_datastore_value_in_cloud`: Gets the value of an entry from a standard datastore via the Cloud API.
*   `set_datastore_value_in_cloud`: Sets the value for an entry in a standard datastore via the Cloud API.
*   `delete_datastore_value_in_cloud`: Deletes an entry from a standard datastore via the Cloud API.
*   `upload_asset_via_cloud`: Uploads a file from the local system as a new Roblox asset via the Cloud API.
*   `publish_place_via_cloud`: Publishes the specified place via the Cloud API.
*   `get_asset_details_via_cloud`: (Not Implemented) Gets details about a specific asset via the Cloud API.
*   `list_user_assets_via_cloud`: (Not Implemented) Lists assets owned by the authenticated user via the Cloud API.
*   `send_chat_via_cloud`: Sends a message to the in-game chat via the Cloud API (execute_luau).
*   `teleport_player_via_cloud`: Teleports a player via the Cloud API (execute_luau).

**Internal/Queueing Tools:**

*   `queue_studio_command`: (Lower-level) Queues a single raw command dictionary for the Studio plugin.
*   `queue_studio_command_batch`: (Lower-level) Queues a batch of raw command dictionaries for the Studio plugin.

## Troubleshooting

*   **Server Not Starting:** Ensure Python and `uv` are installed correctly. Check terminal for error messages. Make sure dependencies are installed (`uv pip sync pyproject.toml`).
*   **Plugin Not Connecting:** Verify the Python server is running. Double-check the `SERVER_URL` in the Lua plugin script matches the server address and port (default `http://localhost:8000/plugin_command`). Check Studio's Output window for errors from the plugin script.
*   **MCP Client Not Connecting:** Ensure the server is running. Verify the SSE URL (`http://localhost:8000/sse`) is entered correctly in your MCP client settings.
*   **Cloud Tools Failing:** Make sure you have created a `.env` file with a valid API Key, Universe ID, and Place ID. Ensure your API key has the necessary permissions for the specific Cloud APIs you are trying to use.
*   **Permissions:** The companion plugin requires Script Injection permissions to function correctly if you load it from a local file instead of installing it properly.
