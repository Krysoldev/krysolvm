#!/bin/bash
source ./ui.sh
banner

if systemctl is-active --quiet krysolvm; then
echo -e "${GREEN}✔ Panel Running${NC}"
else
echo -e "${RED}✖ Panel Stopped${NC}"
fi

systemctl status krysolvm --no-pager
read
