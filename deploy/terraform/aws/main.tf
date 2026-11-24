# Single-VM demo deployment of EventFlow on AWS EC2.
#
# The VM runs the same Docker Compose stack as local development. Only SSH
# (22) and the API (8080) are reachable, and only from var.allowed_cidr;
# Grafana, Prometheus, PostgreSQL and Redis stay bound to localhost and are
# reached through an SSH tunnel.

data "aws_vpc" "default" {
  default = true
}

# Latest Ubuntu 24.04 LTS image published by Canonical.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  bootstrap = join("\n", [
    "#!/usr/bin/env bash",
    "export REPO_URL='${var.repo_url}'",
    "export REPO_REF='${var.repo_ref}'",
    "export WORKER_REPLICAS='${var.worker_replicas}'",
    file("${path.module}/../../cloud/bootstrap.sh"),
  ])
}

resource "aws_key_pair" "this" {
  key_name   = "${var.name}-key"
  public_key = file(pathexpand(var.ssh_public_key_path))
}

resource "aws_security_group" "this" {
  name        = "${var.name}-sg"
  description = "EventFlow demo: SSH and API from the allowed CIDR only"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  ingress {
    description = "EventFlow API"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    description = "Outbound (package, image and module downloads)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "this" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  key_name                    = aws_key_pair.this.key_name
  vpc_security_group_ids      = [aws_security_group.this.id]
  associate_public_ip_address = true

  user_data                   = local.bootstrap
  user_data_replace_on_change = true

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }

  # Require IMDSv2 session tokens (protects instance credentials from SSRF).
  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  tags = {
    Name = var.name
  }
}
