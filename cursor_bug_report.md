# Cursor Bug Report

## Describe the Bug
Some chat instances fail with SSL/TLS handshake errors while others work fine in the same workspace.

**Error Details:**
```
Request ID: 555b5388-fa66-4037-be09-b9baea90bed2
ConnectError: [internal] 1288492638272:error:10000410:SSL routines:OPENSSL_internal:SSLV3_ALERT_HANDSHAKE_FAILURE:../../third_party/boringssl/src/ssl/tls_record.cc:486:SSL alert number 40

    at UZc.$streamAiConnect (vscode-file://vscode-app/Applications/Cursor.app/Contents/Resources/app/out/vs/workbench/workbench.desktop.main.js:6331:406060)
    at async vscode-file://vscode-app/Applications/Cursor.app/Contents/Resources/app/out/vs/workbench/workbench.desktop.main.js:565:75277
    at async Object.run (vscode-file://vscode-app/Applications/Cursor.app/Contents/Resources/app/out/vs/workbench/workbench.desktop.main.js:6399:42465)
    at async o (vscode-file://vscode-app/Applications/Cursor.app/Contents/Resources/app/out/vs/workbench/workbench.desktop.main.js:2815:1269)
    at async Promise.allSettled (index 0)
    at async o7r.run (vscode-file://vscode-app/Applications/Cursor.app/Contents/Resources/app/out/vs/workbench/workbench.desktop.main.js:2815:6513)
```

**Symptoms:**
- Fresh chat windows fail immediately with "Connection Error" on first message
- Error: "Connection failed. If the problem persists, please check your internet connection or VPN"
- Some chat instances work fine while others consistently fail
- Issue persists after clearing cache, reindexing workspace, closing/reopening chats, and restarting Cursor

**Environment:**
- Subscription: Pro
- Multiple chat windows in same workspace
- Some chats work, others fail with same error

**Additional Issue:**
Rules are only processed on the first message of a chat. If a chat fails on first message, rules cannot be applied. If a working chat is continued, rules may not be applied to subsequent messages.

## Steps to Reproduce
1. Open Cursor with a workspace
2. Open a new chat window
3. Type first message in the new chat
4. Connection error appears immediately
5. Some chat windows work fine, others consistently fail with SSL handshake error
6. Issue persists after clearing cache, reindexing, closing/reopening chats, and restarting Cursor

## Expected Behavior
All chat windows in the same workspace should connect successfully. SSL handshake should complete without errors.

## Operating System
MacOS

## Current Cursor Version
[Fill in: Menu -> About Cursor -> Copy]

## For AI issues: which model did you use?
[Fill in model name if applicable]

## For AI issues: add Request ID with privacy disabled
Request ID: 555b5388-fa66-4037-be09-b9baea90bed2

## Does this stop you from using Cursor
Sometimes - I can sometimes use Cursor (some chat windows work, others fail)

## Additional Information
Please investigate why some chat instances fail with SSL handshake errors while others in the same workspace work correctly.
