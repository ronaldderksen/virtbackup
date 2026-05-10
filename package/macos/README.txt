VirtBackup (macOS)
=================

Files
-----
- Virt Backup.app                   GUI application
- virtbackup-agent                  Background agent
- install_agent_user_service.sh     Install and start the launchd user agent
- uninstall_agent_user_service.sh   Stop and remove the launchd user agent
- rotate_agent_logs.sh              Log rotation helper used by the service
- log/                              Logs will be written here
- README.txt                        This file
- TERMS.txt                         Terms of service and liability information
- LICENSE                           Apache License 2.0

Install
-------
Run from this directory:

  ./install_agent_user_service.sh

This creates a LaunchAgent in:

  ~/Library/LaunchAgents/com.virtbackup.agent.plist

Run
---
- Start the GUI:
  open "Virt Backup.app"

- Start the agent manually:
  ./virtbackup-agent

The agent HTTP API listens on port 33551.

Terms and license
-----------------
Before using VirtBackup, read TERMS.txt and LICENSE. The software is
provided as-is and you are responsible for verifying backups and restores in
your own environment.

Always test restores before relying on backups. Keep independent backup copies
where appropriate.

Service commands
----------------
- Show status:
  launchctl print gui/$(id -u)/com.virtbackup.agent

- Restart:
  launchctl kickstart -k gui/$(id -u)/com.virtbackup.agent

- Stop:
  launchctl bootout gui/$(id -u)/com.virtbackup.agent

Logs
----
- Current log:
  log/virtbackup-agent-<host>.log

- Rotated logs:
  log/virtbackup-agent-<host>.log.1 through log.4

Log rotation is triggered when the service starts.

Uninstall
---------
Run from this directory:

  ./uninstall_agent_user_service.sh
