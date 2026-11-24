#!/usr/bin/env bash
# Bootstraps an EventFlow demo host on a fresh Ubuntu 24.04 VM.
#
# Used as AWS EC2 user data and as the GCP Compute Engine startup script
# (see deploy/terraform). GCP runs startup scripts on every boot, so every
# step is idempotent.
#
# Inputs (environment variables, set by Terraform):
#   REPO_URL         git repository to deploy
#   REPO_REF         branch or tag to check out
#   WORKER_REPLICAS  number of worker containers
#   SWAP_GB          swap file size; free-tier VMs have only 1 GB RAM
#
# Secrets are generated on the VM and stored in /opt/eventflow/.env (mode
# 600). They never pass through Terraform state or cloud metadata.
set -euo pipefail

REPO_URL=${REPO_URL:-https://github.com/abhinav0107-colllab/eventflow.git}
REPO_REF=${REPO_REF:-main}
WORKER_REPLICAS=${WORKER_REPLICAS:-1}
SWAP_GB=${SWAP_GB:-2}
APP_DIR=/opt/eventflow
LOG=/var/log/eventflow-bootstrap.log

exec > >(tee -a "$LOG") 2>&1
echo "=== eventflow bootstrap started $(date -Is)"

# 1. Swap: building the Go images and running PostgreSQL, Redis, the API,
#    workers, Prometheus and Grafana does not fit in 1 GB RAM without it.
if ! swapon --show=NAME --noheadings | grep -qx /swapfile; then
  if [ ! -f /swapfile ]; then
    fallocate -l "${SWAP_GB}G" /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
  fi
  swapon /swapfile
  grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# 2. Docker Engine + Compose plugin from Docker's official apt repository.
if ! command -v docker >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y ca-certificates curl git openssl
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  # shellcheck source=/dev/null  # /etc/os-release exists only on the target VM
  codename=$(. /etc/os-release && echo "$VERSION_CODENAME")
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $codename stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
systemctl enable --now docker

# 3. Source code (cloned once; later boots reuse the checkout).
if [ ! -d "$APP_DIR/.git" ]; then
  git clone --depth 1 --branch "$REPO_REF" "$REPO_URL" "$APP_DIR"
fi
cd "$APP_DIR"

# 4. Secrets, generated once on the host.
if [ ! -f .env ]; then
  umask 077
  cat > .env <<EOF
POSTGRES_PASSWORD=$(openssl rand -hex 24)
EVENTFLOW_APP_DB_PASSWORD=$(openssl rand -hex 24)
REDIS_PASSWORD=$(openssl rand -hex 24)
EVENTFLOW_API_KEYS=ef_$(openssl rand -hex 24)
GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 12)
EVENTFLOW_LOG_LEVEL=info
EVENTFLOW_WORKER_REPLICAS=$WORKER_REPLICAS
EVENTFLOW_WORKER_CONCURRENCY=8
EOF
  echo "generated $APP_DIR/.env"
fi

# 5. Build and start. Compose restarts the containers on reboot
#    (restart: unless-stopped), so this is a no-op on later boots.
docker compose up -d --build

# 6. Wait for readiness so the log says clearly whether the deploy worked.
for _ in $(seq 1 120); do
  if curl -fsS http://127.0.0.1:8080/ready >/dev/null 2>&1; then
    echo "=== eventflow is ready $(date -Is)"
    exit 0
  fi
  sleep 5
done
echo "=== eventflow did not become ready within 10 minutes; see 'docker compose logs' in $APP_DIR"
exit 1
