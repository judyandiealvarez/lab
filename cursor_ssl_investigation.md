# Cursor SSL Handshake Failure Investigation

## Summary
Investigation into why some Cursor chat windows fail with SSL handshake errors while others work fine in the same workspace.

## Findings

### 1. Workspace State Database
- **Location**: `~/Library/Application Support/Cursor/User/workspaceStorage/ca9c76a214822e9547ea421aa0bbb417/`
- **State Database**: `state.vscdb` contains workspace state including chat panel configurations
- **Multiple Chat Instances**: The database shows references to many chat panel IDs:
  - `workbench.panel.aichat.8a1ba118-9ae1-444e-8f15-6513f84cda99` (active)
  - `workbench.panel.aichat.7fd4079e-3600-42c2-928d-c362fe93ead9`
  - `workbench.panel.aichat.0aa68b38-f547-41bb-8df3-7293b4101734`
  - And many more (30+ chat panel IDs found)
- **Observation**: Multiple chat instances are being created and stored in workspace state

### 2. Workspace Storage
- **Total Workspaces**: 74 workspace storage directories found
- **Current Workspace ID**: `ca9c76a214822e9547ea421aa0bbb417`
- **Workspace Path**: `/Users/shadowchamber/Projects/lab2`

### 3. Cursor Configuration Files
- **User Settings**: `~/Library/Application Support/Cursor/User/settings.json`
  - Contains cursor composer settings
  - No SSL/TLS specific configurations found
- **Workspace Settings**: `/Users/shadowchamber/Projects/lab2/.vscode/settings.json`
  - Standard Python/testing configuration
  - No network or SSL settings
- **No Cursor-specific config files** found in workspace directory

### 4. Network and SSL Configuration
- **Transport Security**: `~/Library/Application Support/Cursor/TransportSecurity`
  - Contains HSTS (HTTP Strict Transport Security) entries
  - Multiple domain entries with force-https mode
  - No obvious issues found
- **No proxy settings** found in environment variables or shell configs
- **No SSL/TLS certificates** or custom CA configurations found

### 5. Large Chat History File
- **File**: `/Users/shadowchamber/Projects/lab2/chat/cursor_why_does_cursor_1041_1055_fail.md`
- **Size**: 168,055 lines
- **Observation**: This extremely large file is indexed by Cursor's retrieval system
- **Potential Impact**: Large files may affect workspace indexing and chat initialization

### 6. Process Analysis
- Multiple Cursor processes running:
  - Main process
  - Multiple renderer processes (one per window)
  - Extension host processes
  - GPU process
- **Observation**: Each chat window appears to use a separate renderer process

### 7. Network Logs
- Network logs exist but are not easily readable (binary or encoded format)
- Logs located in: `~/Library/Application Support/Cursor/logs/20251202T041215/`
- No obvious SSL errors found in accessible log files

## Potential Root Causes

### Hypothesis 1: Workspace State Corruption
- **Theory**: The workspace state database may contain corrupted or conflicting chat instance data
- **Evidence**: Multiple chat panel IDs stored, some may be in invalid states
- **Test**: Try clearing workspace state or creating a fresh workspace

### Hypothesis 2: Large File Indexing Impact
- **Theory**: The 168K line chat history file may be causing issues during workspace initialization
- **Evidence**: File is indexed by Cursor retrieval system
- **Test**: Temporarily move/rename the large chat file and test

### Hypothesis 3: Connection Pool Exhaustion
- **Theory**: Too many chat instances may exhaust SSL connection pools
- **Evidence**: 30+ chat panel IDs in workspace state
- **Test**: Clear old chat instances from workspace state

### Hypothesis 4: Race Condition in Chat Initialization
- **Theory**: Multiple chat windows initializing simultaneously may cause SSL handshake conflicts
- **Evidence**: Some chats work, others fail - suggests timing-dependent issue
- **Test**: Open chats sequentially instead of simultaneously

### Hypothesis 5: Cursor Version/Update Issue
- **Theory**: Bug in current Cursor version affecting SSL handshake in certain conditions
- **Evidence**: Issue persists after restart, affects only some chat instances
- **Test**: Update Cursor or check for known issues in current version

## Recommended Actions

1. **Clear Workspace State** (Low Risk)
   ```bash
   # Backup first
   cp ~/Library/Application\ Support/Cursor/User/workspaceStorage/ca9c76a214822e9547ea421aa0bbb417/state.vscdb ~/state.vscdb.backup
   # Remove state database (will be recreated)
   rm ~/Library/Application\ Support/Cursor/User/workspaceStorage/ca9c76a214822e9547ea421aa0bbb417/state.vscdb
   ```

2. **Temporarily Exclude Large Chat File** (Low Risk)
   - Add `chat/cursor_why_does_cursor_1041_1055_fail.md` to `.cursorignore` or `.gitignore`
   - Restart Cursor and test

3. **Check Cursor Version** (No Risk)
   - Verify current Cursor version
   - Check for updates or known SSL issues in release notes

4. **Create Fresh Workspace** (Medium Risk - loses workspace-specific settings)
   - Close Cursor
   - Rename workspace storage directory
   - Reopen workspace (will create new storage)

5. **Clear Cursor Cache** (Low Risk)
   ```bash
   # Close Cursor first
   rm -rf ~/Library/Application\ Support/Cursor/Cache
   rm -rf ~/Library/Application\ Support/Cursor/CachedData
   ```

## Files Checked
- Workspace state database: `state.vscdb`
- User settings: `~/Library/Application Support/Cursor/User/settings.json`
- Workspace settings: `.vscode/settings.json`
- Transport security: `TransportSecurity`
- Network logs: `logs/20251202T041215/`
- Process monitor logs: `process-monitor/`

## Next Steps
1. Try clearing workspace state (action 1)
2. If that doesn't work, exclude large chat file (action 2)
3. If still failing, check for Cursor updates (action 3)
4. As last resort, create fresh workspace (action 4)

