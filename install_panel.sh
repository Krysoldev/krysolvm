#!/bin/bash
source ./ui.sh
banner

echo "[+] Installing Panel..."
(git clone https://github.com/Krysoldev/krysolvm.git $HOME/krysolvm > /dev/null 2>&1) & spinner

echo -e "${GREEN}✔ Panel Installed${NC}"
read
