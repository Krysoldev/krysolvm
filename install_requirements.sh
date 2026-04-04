#!/bin/bash
source ./ui.sh
banner

cd $HOME/krysolvm || exit

OS=$(grep '^ID=' /etc/os-release | cut -d= -f2 | tr -d '"')

(apt update -y > /dev/null 2>&1) & spinner
(apt install -y python3 python3-pip > /dev/null 2>&1) & spinner

if [[ "$OS" == "ubuntu" ]]; then
(apt install -y python3-venv > /dev/null 2>&1) & spinner
python3 -m venv venv
source venv/bin/activate
(pip install -r requirements.txt > /dev/null 2>&1) & spinner
else
(pip3 install -r requirements.txt > /dev/null 2>&1) & spinner
fi

echo -e "${GREEN}✔ Requirements Installed${NC}"
read
