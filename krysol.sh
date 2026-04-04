#!/bin/bash

# COLORS
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
MAGENTA='\033[1;35m'
NC='\033[0m'

BASE_URL="https://raw.githubusercontent.com/Krysoldev/krysolvm/main"

FILES=(
"ui.sh"
"install_panel.sh"
"install_requirements.sh"
"start_panel.sh"
"stop_panel.sh"
"status_panel.sh"
"install_lxc.sh"
"uninstall_panel.sh"
)

# Download all scripts
download_files() {
    echo -e "${CYAN}[+] Fetching latest scripts...${NC}"

    for file in "${FILES[@]}"; do
        curl -s -o "$file" "$BASE_URL/$file"
        chmod +x "$file"
    done

    echo -e "${GREEN}[✓] All files loaded${NC}"
}

# Load UI
load_ui() {
    source ./ui.sh
}

# MENU
menu() {
echo -e "${CYAN}╭────────────────────────────────────╮${NC}"
echo -e "${CYAN}│ ${MAGENTA}⚡ KRYsolVM Control Panel ⚡${CYAN}   │${NC}"
echo -e "${CYAN}├────────────────────────────────────┤${NC}"

echo -e "${CYAN}│ ${GREEN}[1] Install Panel           ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[2] Install Requirements     ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[3] Start Panel              ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[4] Stop Panel               ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[5] Panel Status             ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[6] Install LXC              ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[7] Uninstall Panel          ${CYAN}│${NC}"

echo -e "${CYAN}├────────────────────────────────────┤${NC}"
echo -e "${CYAN}│ ${YELLOW}[0] Exit                   ${CYAN}│${NC}"
echo -e "${CYAN}╰────────────────────────────────────╯${NC}"
}

# INIT
download_files
load_ui

# LOOP
while true; do
    banner
    menu

    echo -ne "${MAGENTA}┌─[KRYSOL@panel]─[~]\n└──➤ ${NC}"
    read choice

    loading_bar

    case "$choice" in
        1) bash install_panel.sh ;;
        2) bash install_requirements.sh ;;
        3) bash start_panel.sh ;;
        4) bash stop_panel.sh ;;
        5) bash status_panel.sh ;;
        6) bash install_lxc.sh ;;
        7) bash uninstall_panel.sh ;;
        0)
            echo -e "${GREEN}✔ Installer Closed Safely${NC}"
            sleep 1
            clear
            exit
            ;;
        *) echo "Invalid"; sleep 1 ;;
    esac

done
