#!/bin/bash
# Set variables
# DOMAIN="your-domain.com"
# EMAIL="admin@your-domain.com"

set -euxo pipefail

exec > >(tee /var/log/user-data.log | logger -t user-data) 2>&1

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
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update

# Install Docker Packages
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Install deadsnakes PPA to allow Python version specification
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update

# Install Python3, pip and git
sudo apt install -y python3.11 python3-pip python3.11-venv git

# Intall nginx and certbot packages
#sudo apt install nginx certbot python3-certbot-nginx

# Enable and start Nginx
#systemctl enable nginx
#systemctl start nginx

#cat > /etc/nginx/sites-available/default <<EOL
#server {
    #listen 80 default_server;
    #listen [::]:80 default_server;

    #server_name $DOMAIN www.$DOMAIN;

    # Redirect all HTTP requests to HTTPS
    #return 301 https://\$host\$request_uri;
#}

#server {
    #listen 443 ssl;
    #server_name $DOMAIN www.$DOMAIN;

    #ssl_certificate /etc/letsencrypt/live/$DOMAIN/fullchain.pem;
    #ssl_certificate_key /etc/letsencrypt/live/$DOMAIN/privkey.pem;

    # Reverse proxy to Airflow
    #location /airflow/ {
        #proxy_pass http://localhost:8080/;
        #proxy_set_header Host \$host;
        #proxy_set_header X-Real-IP \$remote_addr;
        #proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        #proxy_set_header X-Forwarded-Proto \$scheme;
    #}

    # Reverse proxy to Metabase
    #location /metabase/ {
        #proxy_pass http://localhost:3000/;
        #proxy_set_header Host \$host;
        #proxy_set_header X-Real-IP \$remote_addr;
        #proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        #proxy_set_header X-Forwarded-Proto \$scheme;
    #}
#}
#EOL

# Test nginx config and reload
#sudo nginx -t && systemctl reload nginx

# Obtain SSL certificate
#sudo certbot --nginx -d $DOMAIN -d www.$DOMAIN --non-interactive --agree-tos -m $EMAIL

# Enable automatic renewal
#systemctl enable certbot.timer
#systemctl start certbot.timer

# Final reload
#systemctl reload nginx

echo "Setup complete! Nginx running with HTTPS and reverse proxy for Airflow/Metabase."