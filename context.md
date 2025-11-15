# Context and Rules

## Output Handling

### Empty Output is NOT an Error
1. Empty output from successful commands is valid
2. Return empty string `""` for successful commands with no output, not `None`
3. `None` should only be returned for actual errors/timeouts
4. Empty string `""` with exit_code=0 = success

### Timeout Behavior
5. Timeout only triggers when process is still running AND no output received for timeout duration
6. Do NOT treat commands that produce no output as timed out

## Command Execution

### Result Analysis
7. **EACH COMMAND MUST BE ANALYSED**
8. **NO ERROR SHOULD BE UNHANDLED**
9. Verify actual state, don't just rely on command output
10. Check exit codes and parse output appropriately

### Error Handling
11. All errors must be handled explicitly
12. Log errors with proper context
13. Verify actual state when commands fail (e.g., check if container exists even if command output suggests failure)

## Testing Commands

### DO NOT USE `2>&1` IN TESTING
14. This rule applies ONLY to testing/debugging commands, NOT the codebase
15. For codebase commands: `2>&1` is acceptable
16. For testing: Do NOT use `2>&1` - user wants to see output properly

### What "retest" Means
17. When user says "retest", ALWAYS run: `lab.py status`, then `lab.py cleanup`, then `lab.py deploy` - IN THAT ORDER
18. NEVER skip cleanup - it's ALWAYS part of retest
19. NOT just unit tests like `test_config.py`
20. Run the full deployment workflow to see what fails, then fix ALL errors at once, then retest
21. Do NOT fix one error, test, fix another - fix ALL errors, then retest once
22. REMEMBER: retest = status, cleanup, deploy - ALWAYS, EVERY TIME

## Workflow

- Do NOT stop in the middle of tasks (23)
- Do NOT do things in parts unless explicitly asked (24)
- When user asks a question, answer first before doing anything (25)
- Do NOT be proactive unless explicitly asked (26)
- If something is unclear, ask for clarification (27)

## Code Quality

- Always verify actual state, not just command output (28)
- Log errors with proper context and relevant output snippets (29)
- Don't rely solely on command return codes or output (30)
