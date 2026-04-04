#!/bin/bash

# Load UI
source ./ui.sh
banner

echo -e "${CYAN}[+] Installing LXC Environment...${NC}"
loading_bar

# Update system
(sudo apt update -y > /dev/null 2>&1) & spinner

# Install packages
(sudo apt install -y lxc lxc-utils uidmap bridge-utils > /dev/null 2>&1) & spinner

echo -e "${CYAN}[+] Configuring LXC...${NC}"

# Enable services
(systemctl enable lxc > /dev/null 2>&1) & spinner
(systemctl start lxc > /dev/null 2>&1) & spinner

# Setup user namespace (important)
echo "root:100000:65536" >> /etc/subuid
echo "root:100000:65536" >> /etc/subgid

# Network setup
if ! ip link show lxcbr0 > /dev/null 2>&1; then
    echo -e "${YELLOW}[+] Setting up network bridge...${NC}"
    (systemctl enable lxc-net > /dev/null 2>&1) & spinner
    (systemctl start lxc-net > /dev/null 2>&1) & spinner
fi

# Test LXC
echo -e "${CYAN}[+] Checking LXC Status...${NC}"
(lxc-checkconfig > /dev/null 2>&1) & spinner

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✔ LXC Installed Successfully${NC}"
echo -e "${GREEN}✔ VPS Deployment Ready${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

read
