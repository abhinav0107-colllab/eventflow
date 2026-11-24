locals {
  public_ip = google_compute_instance.this.network_interface[0].access_config[0].nat_ip
}

output "public_ip" {
  description = "Public IP of the EventFlow VM."
  value       = local.public_ip
}

output "api_url" {
  description = "EventFlow API base URL (reachable from allowed_cidr only)."
  value       = "http://${local.public_ip}:8080"
}

output "ssh_command" {
  description = "SSH into the VM."
  value       = "ssh ${var.ssh_user}@${local.public_ip}"
}

output "monitoring_tunnel" {
  description = "Forward Grafana (3000) and Prometheus (9090) to localhost."
  value       = "ssh -N -L 3000:127.0.0.1:3000 -L 9090:127.0.0.1:9090 ${var.ssh_user}@${local.public_ip}"
}

output "bootstrap_log" {
  description = "Follow the first-boot setup (takes 10-15 minutes on e2-micro)."
  value       = "ssh ${var.ssh_user}@${local.public_ip} 'sudo tail -f /var/log/eventflow-bootstrap.log'"
}
