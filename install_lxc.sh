#!/usr/bin/env bash
set -e

#=========================================================
#   LXC + LXD AUTO INSTALLER (KRYSOL Edition)
#   Made By KrysolDev
#=========================================================

# COLORS
RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
CYAN="\e[36m"
MAGENTA="\e[35m"
RESET="\e[0m"
BOLD="\e[1m"

# TYPEWRITER
typewriter() {
    text="$1"
    for ((i=0; i<${#text}; i++)); do
        echo -ne "${text:$i:1}"
        sleep 0.01
    done
    echo ""
}

# SPINNER
spinner() {
    local pid=$!
    local spin='⠋⠙⠸⠴⠦⠇'
    local i=0
    while kill -0 $pid 2>/dev/null; do
        printf "\r${MAGENTA}[KRYSOL] %s Installing...${RESET}" "${spin:$i:1}"
        i=$(( (i+1) %6 ))
        sleep 0.08
    done
    printf "\r${GREEN}[✔] Done${RESET}\n"
}

# LOADING BAR
loading_bar() {
    echo -ne "${CYAN}[SYSTEM] Initializing ${RESET}"
    for i in {1..30}; do
        echo -ne "${GREEN}▰${RESET}"
        sleep 0.01
    done
    echo -e " ✔"
}

# BANNER
banner() {
    clear

    for i in {1..2}; do
        echo -e "${CYAN}Initializing KRYSOL Core...${RESET}"
        sleep 0.05
        clear
    done

    echo -e "${MAGENTA}"
cat << "EOF"
██╗  ██╗██████╗ ██╗   ██╗███████╗ ██████╗ ██╗     
██║ ██╔╝██╔══██╗╚██╗ ██╔╝██╔════╝██╔═══██╗██║     
█████╔╝ ██████╔╝ ╚████╔╝ ███████╗██║   ██║██║     
██╔═██╗ ██╔══██╗  ╚██╔╝  ╚════██║██║   ██║██║     
██║  ██╗██║  ██║   ██║   ███████║╚██████╔╝███████╗
╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ╚══════╝ ╚═════╝ ╚══════╝
EOF

    echo -e "${CYAN}═══════════════════════════════════════${RESET}"
    echo -e "${GREEN}     KRYSOL LXC Installer ${RESET}"
    echo -e "${CYAN}═══════════════════════════════════════${RESET}"

    typewriter "🔐 KRYSOL secure environment initializing..."
    typewriter "⚡ Loading container engine..."
    typewriter "🧠 Preparing system..."
    typewriter "🚀 Ready"

    echo -e "\n${YELLOW}[INFO] Powered by KrysolDev${RESET}\n"
}

# RUN WITH SPINNER
run() {
    ( "$@" > /dev/null 2>&1 ) & spinner
}

# ROOT CHECK
check_root() {
    if [ "$EUID" -ne 0 ]; then
        echo -e "${RED}Run as root or use sudo${RESET}"
        exit 1
    fi
}

# INSTALL FUNCTION
install_lxc() {
    banner
    loading_bar

    run apt update -y
    run apt upgrade -y

    run apt install -y lxc lxc-utils bridge-utils uidmap curl wget
    run apt install -y snapd

    systemctl enable --now snapd.socket

    run snap install lxd --channel=latest/stable

    echo -e "${CYAN}[INFO] Initializing LXD...${RESET}"
    lxd init

    usermod -aG lxd "$SUDO_USER" 2>/dev/null || true

    echo -e "\n${GREEN}${BOLD}✔ INSTALLATION COMPLETE${RESET}"

    echo -e "\n${CYAN}Next Steps:${RESET}"
    echo -e "→ reboot OR run: newgrp lxd"

    echo -e "\n${MAGENTA}KrysolDev • KRYSOL Ready 🚀${RESET}\n"
}

# MAIN
check_root
install_lxc
