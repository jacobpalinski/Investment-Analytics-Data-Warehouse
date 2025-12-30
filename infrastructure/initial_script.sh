#!/bin/bash
set -euo pipefail

exec > >(tee /var/log/user-data.log | logger -t user-data) 2>&1

# Set variables
DOMAIN_NAME="${domain_name}"
CERTBOT_EMAIL="${certbot_email}"

# Install Docker dependencies
sudo apt update
sudo apt install -y ca-certificates curl gnupg

# Add Dockers official GPG key
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "$${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update

# Install Docker Packages
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Install Python3, git and jq
sudo apt install -y python3 git jq

# Install AWS CLI v2
if ! command -v aws >/dev/null 2>&1; then
  echo "Installing AWS CLI..."
  sudo apt update
  sudo apt install -y unzip curl
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
  unzip awscliv2.zip
  sudo ./aws/install
fi

# Intall nginx and certbot packages
sudo apt install -y certbot python3-certbot-nginx

# Enable and start Nginx
systemctl enable nginx
systemctl start nginx

cat > /etc/nginx/sites-available/default <<EOL
server {
    listen 80;

    server_name $DOMAIN_NAME www.$DOMAIN_NAME;

    # Redirect all HTTP requests to HTTPS
    return 301 https://$$host$$request_uri;
}

server {
    listen 443 ssl;
    server_name $DOMAIN_NAME www.$DOMAIN_NAME;

    ssl_certificate /etc/letsencrypt/live/$DOMAIN_NAME/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/$DOMAIN_NAME/privkey.pem;

    # Reverse proxy to Metabase
    location /metabase/ {
        proxy_pass http://localhost:3000/;
        proxy_set_header Host $$host;
        proxy_set_header X-Real-IP $$remote_addr;
        proxy_set_header X-Forwarded-For $$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $$scheme;
    }
}
EOL

# Test nginx config and reload
sudo nginx -t && systemctl reload nginx

# Note: SSL setup is deferred. Once DNS is pointing to this EC2 instance, run:
#sudo apt install -y certbot python3-certbot-nginx
#sudo certbot --nginx -d $DOMAIN_NAME -d www.$DOMAIN_NAME --non-interactive --agree-tos -m $CERTBOT_EMAIL
#systemctl reload nginx

# Setup complete
echo "Setup complete! Nginx running with HTTPS and reverse proxy for Metabase"