#!/bin/bash

source ./ui.sh
banner

DIR="$HOME/krysolvm"

echo -e "${CYAN}[+] Setting up Node (24/7 mode)...${NC}"

cd $DIR || { echo -e "${RED}Panel not found!${NC}"; exit; }

# Ask API Key
echo -e "${YELLOW}Enter Panel API Key:${NC}"
read API_KEY

# Create .env
echo -e "${CYAN}[+] Creating .env config...${NC}"

cat <<EOF > $DIR/.env
API_KEY=$API_KEY
EOF

echo -e "${GREEN}✔ .env configured${NC}"

# Install dependencies
(sudo apt update -y > /dev/null 2>&1) & spinner
(sudo apt install -y python3 python3-pip > /dev/null 2>&1) & spinner
(pip3 install flask requests python-dotenv > /dev/null 2>&1) & spinner

echo -e "${CYAN}[+] Creating systemd service...${NC}"

# Create service
cat <<EOF > /etc/systemd/system/krysol-node.service
[Unit]
Description=KRYsolVM Node
After=network.target

[Service]
User=root
WorkingDirectory=$DIR
ExecStart=/usr/bin/python3 $DIR/node.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

# Enable & start
(systemctl daemon-reload > /dev/null 2>&1) & spinner
(systemctl enable krysol-node > /dev/null 2>&1) & spinner
(systemctl restart krysol-node > /dev/null 2>&1) & spinner

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✔ Node Configured & Running 24/7${NC}"
echo -e "${GREEN}✔ API Key Connected${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

read
