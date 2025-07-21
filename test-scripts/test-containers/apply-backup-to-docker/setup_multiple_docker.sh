#!/bin/bash

# install docker
# Add Docker's official GPG key:
sudo apt update
sudo apt upgrade -y

install_docker() {
    echo "install docker"
    sudo apt-get install ca-certificates curl gnupg lsb-release
    sudo install -m 0755 -d /etc/apt/keyrings
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc

    # Add the repository to Apt sources:
    echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update

    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
}

install_docker_if_not_installed() {
  if command -v docker >/dev/null 2>&1; then
      echo "Docker is installed"
  else
      echo "Docker is NOT installed, installing now..."
      install_docker
  fi
}


# install Docker DWH
install_docker_dwh() {
  local
  local dwh_folder="aktin$dwh_num"
  local dwh_port=$((default_port + dwh_num - 1))
  echo "install docker dwh $dwh_num on port $dwh_port"
  dwh_num=$((dwh_num + 1))

  cd /
  mkdir -p "$dwh_folder"
  cd "$dwh_folder" || exit

  curl -LO https://github.com/aktin/docker-aktin-dwh/releases/latest/download/compose.yml
  echo 'mysecretpassword' > secret.txt
  echo "HTTP_PORT=$dwh_port" > .env

  docker compose up -d
}

main() {
  local default_port=80
  local dwh_num=1

  install_docker_if_not_installed

  install_docker_dwh "$PWD"
  install_docker_dwh "$PWD"
}

main
