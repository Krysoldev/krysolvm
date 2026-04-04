#!/bin/bash

source ./ui.sh
banner

echo -e "${RED}[!] WARNING: This will completely remove KRYsolVM Panel${NC}"
echo -e "${YELLOW}Type DELETE to confirm:${NC}"

read confirm

if [ "$confirm" != "DELETE" ]; then
    echo -e "${CYAN}Cancelled${NC}"
    exit
fi

echo -e "${CYAN}[+] Removing panel...${NC}"

# Stop service
(systemctl stop krysolvm > /dev/null 2>&1) & spinner
(systemctl disable krysolvm > /dev/null 2>&1) & spinner

# Remove service file
(rm -f /etc/systemd/system/krysolvm.service) & spinner

# Reload systemd
(systemctl daemon-reload > /dev/null 2>&1) & spinner

# Remove panel files
(rm -rf $HOME/krysolvm) & spinner

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✔ Panel Completely Removed${NC}"
echo -e "${GREEN}✔ System Cleaned${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

read
