variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "ap-south-1" # Mumbai
}

variable "name" {
  description = "Name prefix for the created resources."
  type        = string
  default     = "eventflow-demo"
}

variable "instance_type" {
  description = "EC2 instance type. t3.micro / t2.micro are Free Tier eligible in most regions; check your account's Free Tier terms."
  type        = string
  default     = "t3.micro"
}

variable "root_volume_gb" {
  description = "Root disk size in GB (gp3, encrypted)."
  type        = number
  default     = 20
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key allowed to log in as 'ubuntu'."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "allowed_cidr" {
  description = "CIDR allowed to reach SSH (22) and the API (8080), e.g. \"203.0.113.7/32\" for your own IP."
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
  description = "Number of worker containers (keep 1 on 1 GB instances)."
  type        = number
  default     = 1
}
