#!/bin/bash
# Exit immediately if a command exits with a non-zero status
set -e

# Redirect all output to a log file for easy debugging later
exec > /var/log/user-data.log 2>&1

echo "================ Starting Bootstrap Script ================"

# 1. System Updates & Dependencies
dnf update -y
dnf install -y docker git wget

# 2. Configure Docker Daemon & Permissions
systemctl start docker
systemctl enable docker
usermod -aG docker ec2-user
newgrp docker


# 3. Spin up the Public Jupyter Container with Token Disabled
echo "Launching container 'cotiviti-web' on port 80..."
docker run -d -p 80:8888 --name cotiviti-web --restart unless-stopped pauldefusco/cde_125_hol:latest jupyter lab --allow-root --ip=0.0.0.0 --ServerApp.token=''


echo "================ Bootstrap Script Completed ================"
