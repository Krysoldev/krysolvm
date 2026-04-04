#!/bin/bash
source ./ui.sh
banner

(systemctl stop krysolvm) & spinner

echo -e "${RED}✔ Panel Stopped${NC}"
read
