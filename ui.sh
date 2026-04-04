#!/bin/bash

RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
MAGENTA='\033[1;35m'
NC='\033[0m'

typewriter() {
text="$1"
for ((i=0; i<${#text}; i++)); do
echo -ne "${text:$i:1}"
sleep 0.006
done
echo ""
}

spinner() {
local pid=$!
local spin='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
local i=0
while kill -0 $pid 2>/dev/null; do
printf "\r${CYAN}[%s] Processing...${NC}" "${spin:$i:1}"
i=$(( (i+1) %10 ))
sleep 0.07
done
printf "\r${GREEN}✔ Done!${NC}\n"
}

loading_bar() {
echo -ne "${YELLOW}Loading [${NC}"
for i in {1..20}; do
echo -ne "${GREEN}█${NC}"
sleep 0.01
done
echo -e "${YELLOW}]${NC}"
}

banner() {
clear

# glitch
for i in {1..2}; do
clear
echo -e "${CYAN}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "        ⚡ KRYSOLVM Installer ⚡"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${NC}"
sleep 0.05
done

# main logo
echo -e "${MAGENTA}"
cat << "EOF"
██╗  ██╗██████╗ ██╗   ██╗███████╗ ██████╗ ██╗     
██║ ██╔╝██╔══██╗╚██╗ ██╔╝██╔════╝██╔═══██╗██║     
█████╔╝ ██████╔╝ ╚████╔╝ ███████╗██║   ██║██║     
██╔═██╗ ██╔══██╗  ╚██╔╝  ╚════██║██║   ██║██║     
██║  ██╗██║  ██║   ██║   ███████║╚██████╔╝███████╗
╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ╚══════╝ ╚═════╝ ╚══════╝
EOF

echo -e "${CYAN}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "         🔥 VPS Panel Installer 🔥 "
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${NC}"

typewriter "🔐 Initializing system..."
typewriter "⚡ Loading modules..."
typewriter "🚀 Ready"
echo ""
}
