#!/bin/bash
source ./ui.sh
banner

DIR="$HOME/krysolvm"
cd $DIR || exit

OS=$(grep '^ID=' /etc/os-release | cut -d= -f2 | tr -d '"')

if [[ "$OS" == "ubuntu" && -d "venv" ]]; then
PYTHON="$DIR/venv/bin/python"
else
PYTHON="/usr/bin/python3"
fi

cat <<EOF > /etc/systemd/system/krysolvm.service
[Unit]
Description=KRYsolVM Panel
After=network.target

[Service]
User=root
WorkingDirectory=$DIR
ExecStart=$PYTHON $DIR/krysolvm.py
Restart=always

[Install]
WantedBy=multi-user.target
EOF

(systemctl daemon-reload > /dev/null 2>&1) & spinner
(systemctl enable krysolvm > /dev/null 2>&1) & spinner
(systemctl restart krysolvm > /dev/null 2>&1) & spinner

IP=$(curl -s ifconfig.me)

echo ""
echo -e "${GREEN}🚀 Panel Started${NC}"
echo -e "${YELLOW}KrysolVM Link :- $IP:5000${NC}"
echo -e "${CYAN}Username :- admin${NC}"
echo -e "${CYAN}Password :- admin${NC}"
read
