#!/bin/bash
set -e

# Redirect all output to a log file
exec > /var/log/user-data.log 2>&1

echo "================ Starting Bootstrap Script ================"

# --- NEW: Signal AWS on Exit ---
# This function sends the status back to AWS CloudFormation/EC2 before exiting
signal_aws() {
  STATUS=$?
  # Fetch the region and stack info dynamically
  IMDS_TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
  REGION=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/placement/region)
  
  # Send signal (0 for success, 1 for failure)
  if [ $STATUS -eq 0 ]; then
    cfn-signal --stack ${AWS::StackName} --resource EC2Instance --region $REGION --success true
  else
    cfn-signal --stack ${AWS::StackName} --resource EC2Instance --region $REGION --success false --reason "Bootstrap failed. Check /var/log/user-data.log"
  fi
}
trap signal_aws EXIT
# -------------------------------

# 1. System Updates & Dependencies
dnf update -y
dnf install -y docker git wget

# 2. Configure Docker Daemon & Permissions
systemctl start docker
systemctl enable docker
usermod -aG docker ec2-user

# 3. Setup Project Directory
WORKDIR="/home/ec2-user/CDE_125_HOL"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

# 4. Clone Git Repository
git clone https://github.com/pdefusco/CDE_125_HOL.git .
rm -f pyspark-3.5.4.tar.gz # Added -f to prevent failure if it doesn't exist yet

# 5. Download Required Dependencies
echo "Downloading PySpark archive..."
wget https://archive.apache.org/dist/spark/spark-3.5.4/pyspark-3.5.4.tar.gz

# 6. Build the Docker Image Locally
echo "Building Docker image 'cotiviti-hol'..."
docker build -t cotiviti-hol .

# 7. Spin up the Public Jupyter Container
echo "Launching container 'cotiviti-web' on port 80..."
docker run -d \
  -p 80:8888 \
  --name cotiviti-web \
  --restart unless-stopped \
  cotiviti-hol \
  jupyter lab --allow-root --ip=0.0.0.0 --ServerApp.token=''

# 8. Correct directory ownership
chown -R ec2-user:ec2-user /home/ec2-user/CDE_125_HOL

echo "================ Bootstrap Script Completed ================"
