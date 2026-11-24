variable "project_id" {
  description = "GCP project ID to deploy into."
  type        = string
}

variable "region" {
  description = "GCP region. The Always Free e2-micro is only available in us-west1, us-central1 and us-east1."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone within the region."
  type        = string
  default     = "us-central1-a"
}

variable "name" {
  description = "Name prefix for the created resources."
  type        = string
  default     = "eventflow-demo"
}

variable "machine_type" {
  description = "Compute Engine machine type."
  type        = string
  default     = "e2-micro"
}

variable "disk_gb" {
  description = "Boot disk size in GB (pd-standard; 30 GB is within the Always Free allowance)."
  type        = number
  default     = 30
}

variable "ssh_user" {
  description = "Linux user created for the SSH key."
  type        = string
  default     = "eventflow"
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "allowed_cidr" {
  description = "CIDR allowed to reach SSH (22) and the API (8080), e.g. \"203.0.113.7/32\"."
  type        = string

  validation {
    condition     = can(cidrhost(var.allowed_cidr, 0)) && var.allowed_cidr != "0.0.0.0/0"
    error_message = "allowed_cidr must be a valid CIDR and must not be 0.0.0.0/0 (use your own IP, e.g. 203.0.113.7/32)."
  }
}

variable "repo_url" {
  description = "Git repository to deploy."
  type        = string
  default     = "https://github.com/abhinav0107-colllab/eventflow.git"
}

variable "repo_ref" {
  description = "Branch or tag to deploy."
  type        = string
  default     = "main"
}

variable "worker_replicas" {
  description = "Number of worker containers (keep 1 on e2-micro)."
  type        = number
  default     = 1
}
