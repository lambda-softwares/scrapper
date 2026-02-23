#!/bin/bash
set -euo pipefail

# EC2 setup script for Kenya Law Scraper
# Usage: SSH into Ubuntu 24.04 EC2 instance, then run: bash setup_ec2.sh

# 1. System packages
sudo apt-get update
sudo apt-get install -y git tmux curl wget unzip

# 2. Google Chrome
wget -q -O /tmp/google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt-get install -y /tmp/google-chrome.deb || sudo apt-get install -f -y
rm /tmp/google-chrome.deb

# 3. uv (Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

# 4. Clone repo & install deps
cd ~
git clone <REPO_URL> scrapper
cd scrapper
uv sync

echo ""
echo "Setup complete!"
echo "Next steps:"
echo "  1. Transfer checkpoint files (if resuming):"
echo "     scp checkpoint.json processed_urls.txt ubuntu@<ip>:~/scrapper/"
echo "  2. Run the scraper in tmux:"
echo "     tmux new -s scraper"
echo "     cd ~/scrapper && uv run python parallel_coordinator.py --workers 2 --resume"
echo "     (Detach: Ctrl+B, D | Reattach: tmux attach -t scraper)"
