VirtBackup

Contents
- virtbackup                      GUI application
- virtbackup-agent                Background agent
- README.txt                      This file
- install_agent_user_service.sh   Installs agent as a user systemd service
- rotate_agent_logs.sh            Log rotation helper (used by service)
- log/                            Log directory (created at runtime)

Run
- Start the GUI: ./virtbackup
- Start agent manually: ./virtbackup-agent
  (Agent HTTP API listens on port 33551)

Install agent as user service
- ./install_agent_user_service.sh

Logs
- ./log/virtbackup-agent-<host>.log

Uninstall user service (optional)
- systemctl --user stop virtbackup-agent.service
- systemctl --user disable virtbackup-agent.service
- rm ~/.config/systemd/user/virtbackup-agent.service
- systemctl --user daemon-reload
