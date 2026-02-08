VirtBackup (macOS)
=================

Files
-----
- virtbackup-agent                 Agent binary
- install_agent_user_service.sh    Install and start launchd user agent
- uninstall_agent_user_service.sh  Remove launchd user agent
- rotate_agent_logs.sh             Log rotation helper
- log/                             Logs will be written here

Install
-------
1) ./install_agent_user_service.sh

This creates a LaunchAgent in:
  ~/Library/LaunchAgents/com.virtbackup.agent.plist

Uninstall
---------
1) ./uninstall_agent_user_service.sh

Logs
----
- Current log: log/virtbackup-agent-<host>.log
- Rotation is triggered on install/start; adjust rotate_agent_logs.sh if needed.
