#!/bin/bash

# COLORS
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
MAGENTA='\033[1;35m'
NC='\033[0m'

BASE_URL="https://raw.githubusercontent.com/Krysoldev/krysolvm/main"
INSTALL_DIR="$HOME/.krysolvm"

FILES=(
"ui.sh"
"install_panel.sh"
"install_requirements.sh"
"start_panel.sh"
"stop_panel.sh"
"status_panel.sh"
"install_lxc.sh"
"uninstall_panel.sh"
"setup_node.sh"
)

# Create hidden directory
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

# Download scripts
download_files() {
    echo -e "${CYAN}[+] Loading system files...${NC}"

    for file in "${FILES[@]}"; do
        curl -s -o "$file" "$BASE_URL/$file"
        chmod +x "$file"
    done

    echo -e "${GREEN}[✓] System Ready${NC}"
}

# Load UI
load_ui() {
    source "$INSTALL_DIR/ui.sh"
}

# MENU
menu() {
echo -e "${CYAN}╭────────────────────────────────────╮${NC}"
echo -e "${CYAN}│ ${MAGENTA}⚡ KRYSOLVM Control Panel ⚡${CYAN}   │${NC}"
echo -e "${CYAN}├────────────────────────────────────┤${NC}"

echo -e "${CYAN}│ ${GREEN}[1] Install Panel           ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[2] Install Requirements     ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[3] Start Panel              ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[4] Stop Panel               ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[5] Panel Status             ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[6] Install LXC              ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[7] Uninstall Panel          ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[8] Setup Node               ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[9] Start Node               ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[10] Stop Node              ${CYAN}│${NC}"
echo -e "${CYAN}│ ${GREEN}[11] Node Status            ${CYAN}│${NC}"

echo -e "${CYAN}├────────────────────────────────────┤${NC}"
echo -e "${CYAN}│ ${YELLOW}[0] Exit                   ${CYAN}│${NC}"
echo -e "${CYAN}╰────────────────────────────────────╯${NC}"
}

# INIT
download_files
load_ui

# Self-delete
SCRIPT_PATH="$(realpath "$0" 2>/dev/null)"
[ -f "$SCRIPT_PATH" ] && rm -f "$SCRIPT_PATH" 2>/dev/null

# LOOP
while true; do
    banner
    menu

    echo -ne "${MAGENTA}┌─[KRYSOL@panel]─[~]\n└──➤ ${NC}"
    read choice

    loading_bar

    case "$choice" in
        1) bash "$INSTALL_DIR/install_panel.sh" ;;
        2) bash "$INSTALL_DIR/install_requirements.sh" ;;
        3) bash "$INSTALL_DIR/start_panel.sh" ;;
        4) bash "$INSTALL_DIR/stop_panel.sh" ;;
        5) bash "$INSTALL_DIR/status_panel.sh" ;;
        6) bash "$INSTALL_DIR/install_lxc.sh" ;;
        7) bash "$INSTALL_DIR/uninstall_panel.sh" ;;
        8) bash "$INSTALL_DIR/setup_node.sh" ;;
        9)
            (systemctl restart krysol-node > /dev/null 2>&1)
            echo -e "${GREEN}✔ Node Started${NC}"
            sleep 1
            ;;
        10)
            (systemctl stop krysol-node > /dev/null 2>&1)
            echo -e "${RED}✔ Node Stopped${NC}"
            sleep 1
            ;;
        11)
            systemctl status krysol-node --no-pager
            read
            ;;
        0)
            echo -e "${GREEN}✔ Installer Closed Safely${NC}"
            sleep 1
            clear
            exit
            ;;
        *) echo "Invalid"; sleep 1 ;;
    esac

done
