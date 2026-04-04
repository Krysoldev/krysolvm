#!/bin/bash

# Load UI
source ./ui.sh
banner

DIR="$HOME/krysolvm"

echo -e "${CYAN}[+] Preparing to start panel...${NC}"

cd $DIR || { echo -e "${RED}Panel not found! Install first.${NC}"; exit; }

# Detect OS
OS=$(grep '^ID=' /etc/os-release | cut -d= -f2 | tr -d '"')

# Detect Python
if [[ "$OS" == "ubuntu" && -d "$DIR/venv" ]]; then
    PYTHON="$DIR/venv/bin/python"
else
    PYTHON="/usr/bin/python3"
fi

echo -e "${CYAN}[+] Using Python: $PYTHON${NC}"

# Create systemd service
echo -e "${CYAN}[+] Creating service...${NC}"

cat <<EOF > /etc/systemd/system/krysolvm.service
[Unit]
Description=KRYSOLVM Panel
After=network.target

[Service]
User=root
WorkingDirectory=$DIR
ExecStart=$PYTHON $DIR/krysolvm.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

# Reload & Start Service
(systemctl daemon-reload > /dev/null 2>&1) & spinner
(systemctl enable krysolvm > /dev/null 2>&1) & spinner
(systemctl restart krysolvm > /dev/null 2>&1) & spinner

# Get VPS IP
IP=$(curl -s ifconfig.me)

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}🚀 KRYsolVM Panel Started Successfully${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Clickable Link
echo -e "${YELLOW}KrysolVM Link :- ${GREEN}http://$IP:5000${NC}"
echo -e "${MAGENTA}👉 Open this link in your browser${NC}"
echo ""

# Login Info
echo -e "${CYAN}Login Details:${NC}"
echo -e "${GREEN}Username :- admin${NC}"
echo -e "${GREEN}Password :- admin${NC}"
echo ""

echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

read
