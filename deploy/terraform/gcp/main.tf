# Single-VM demo deployment of EventFlow on Google Compute Engine.
#
# Same layout as the AWS module: the VM runs the Docker Compose stack, only
# SSH (22) and the API (8080) are reachable and only from var.allowed_cidr;
# Grafana and Prometheus are reached through an SSH tunnel.

locals {
  network_tag = "${var.name}-access"

  # GCP runs the startup script on every boot; bootstrap.sh is idempotent.
  startup_script = join("\n", [
    "#!/usr/bin/env bash",
    "export REPO_URL='${var.repo_url}'",
    "export REPO_REF='${var.repo_ref}'",
    "export WORKER_REPLICAS='${var.worker_replicas}'",
    file("${path.module}/../../cloud/bootstrap.sh"),
  ])
}

resource "google_compute_firewall" "ssh_api" {
  name          = "${var.name}-ssh-api"
  network       = "default"
  description   = "EventFlow demo: SSH and API from the allowed CIDR only"
  source_ranges = [var.allowed_cidr]
  target_tags   = [local.network_tag]

  allow {
    protocol = "tcp"
    ports    = ["22", "8080"]
  }
}

resource "google_compute_instance" "this" {
  name         = var.name
  machine_type = var.machine_type
  zone         = var.zone
  tags         = [local.network_tag]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
      size  = var.disk_gb
      type  = "pd-standard"
    }
  }

  network_interface {
    network = "default"
    access_config {} # ephemeral public IP
  }

  metadata = {
    ssh-keys       = "${var.ssh_user}:${trimspace(file(pathexpand(var.ssh_public_key_path)))}"
    startup-script = local.startup_script
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  labels = {
    project    = "eventflow"
    managed-by = "terraform"
  }
}
