VirtBackup (Linux)
==================

Files
-----
- virtbackup                        GUI application
- virtbackup-agent                  Background agent
- install_agent_user_service.sh     Install and start the systemd user service
- uninstall_agent_user_service.sh   Stop and remove the systemd user service
- rotate_agent_logs.sh              Log rotation helper used by the service
- log/                              Logs will be written here
- README.txt                        This file
- TERMS.txt                         Terms of service and liability information
- LICENSE                           Apache License 2.0

Install
-------
Run from this directory:

  ./install_agent_user_service.sh

This creates and starts the systemd user service:

  ~/.config/systemd/user/virtbackup-agent.service

Run
---
- Start the GUI:
  ./virtbackup

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
  systemctl --user status virtbackup-agent.service

- Restart:
  systemctl --user restart virtbackup-agent.service

- Stop:
  systemctl --user stop virtbackup-agent.service

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
