# Deploying EventFlow to AWS or GCP

EventFlow ships Terraform modules for a **single-VM demo deployment** on
either cloud:

| | AWS | GCP |
|---|---|---|
| Module | `deploy/terraform/aws` | `deploy/terraform/gcp` |
| Compute | EC2 `t3.micro`, Ubuntu 24.04 | Compute Engine `e2-micro`, Ubuntu 24.04 |
| Disk | 20 GB gp3, encrypted | 30 GB pd-standard |
| Network | Security group: SSH + API from your IP only | Firewall rule: SSH + API from your IP only |
| Hardening | IMDSv2 required | Shielded VM (secure boot, vTPM) |

Both modules run the same bootstrap script (`deploy/cloud/bootstrap.sh`). It
adds swap, installs Docker from Docker's official repository, clones this
repository, generates secrets **on the VM** (they never appear in Terraform
state), and starts the Docker Compose stack.

Only the API (port 8080) and SSH are exposed, and only to the CIDR you
configure. Grafana, Prometheus, PostgreSQL and Redis stay bound to the VM's
localhost; reach the dashboards through an SSH tunnel.

> **This is a demo topology, not production.** Everything runs on one VM.
> A production setup would use managed PostgreSQL (RDS / Cloud SQL), managed
> Redis (ElastiCache / Memorystore), container orchestration (ECS, EKS or
> GKE), TLS, and secrets in a secrets manager.

## Costs: read this first

* **Set a budget alert before you start** (AWS Billing → Budgets, or GCP
  Billing → Budgets & alerts), for example $1/₹100.
* **AWS:** Free Tier terms changed in 2025 and depend on when your account was
  created. Check *Billing → Free Tier* in the console to confirm `t3.micro` is
  covered for you.
* **GCP:** the Always Free `e2-micro` and 30 GB standard disk only apply in
  `us-west1`, `us-central1` and `us-east1`. Billing must be enabled on the
  project even for free-tier use, and an external IP address may incur a
  small charge; check the current pricing page.
* **Run `terraform destroy` when you are done.** A forgotten VM is the most
  common surprise bill.

## Prerequisites (Windows PowerShell)

Install Terraform, and create an SSH key if you don't have one (OpenSSH is
built into Windows 10/11):

```powershell
winget install Hashicorp.Terraform
```
```powershell
ssh-keygen -t ed25519 -f "$HOME\.ssh\id_ed25519"
```

Find your public IP. You'll allow only this address:

```powershell
Invoke-RestMethod https://api.ipify.org
```

On macOS/Linux, the same steps work with your package manager and
`curl https://api.ipify.org`.

---

## Option A: AWS (EC2)

1. **Credentials.** In the AWS console, create an IAM user for Terraform
   with the `AmazonEC2FullAccess` policy and an access key. Then:

   ```powershell
   winget install Amazon.AWSCLI
   ```
   ```powershell
   aws configure
   ```
   Enter the access key, the secret key and the region `ap-south-1` (Mumbai).

2. **Configure.** From the repository root:

   ```powershell
   cd deploy\terraform\aws
   ```
   ```powershell
   Copy-Item terraform.tfvars.example terraform.tfvars
   ```
   Edit `terraform.tfvars` and set `allowed_cidr` to your IP followed by
   `/32`, for example `"49.37.12.34/32"`.

3. **Deploy.**

   ```powershell
   terraform init
   ```
   ```powershell
   terraform apply
   ```
   Type `yes` when asked. Terraform prints the outputs: `api_url`,
   `ssh_command`, `monitoring_tunnel` and `bootstrap_log`.

4. **Wait for the first boot** (about 10–15 minutes on a micro instance,
   mostly building the Go images). Follow it with the `bootstrap_log` output:

   ```powershell
   ssh ubuntu@<public_ip> "sudo tail -f /var/log/eventflow-bootstrap.log"
   ```
   It ends with `=== eventflow is ready`.

Then continue with [Using the deployment](#using-the-deployment).

---

## Option B: GCP (Compute Engine)

1. **Project and credentials.** Create a project in the Google Cloud
   console, enable billing on it, then:

   ```powershell
   winget install Google.CloudSDK
   ```
   ```powershell
   gcloud auth application-default login
   ```
   ```powershell
   gcloud services enable compute.googleapis.com --project <your-project-id>
   ```

2. **Configure.**

   ```powershell
   cd deploy\terraform\gcp
   ```
   ```powershell
   Copy-Item terraform.tfvars.example terraform.tfvars
   ```
   Set `project_id` and `allowed_cidr` (your IP + `/32`) in `terraform.tfvars`.

3. **Deploy.**

   ```powershell
   terraform init
   ```
   ```powershell
   terraform apply
   ```

4. **Wait for the first boot** (about 10–15 minutes on `e2-micro`):

   ```powershell
   ssh eventflow@<public_ip> "sudo tail -f /var/log/eventflow-bootstrap.log"
   ```

The SSH user is `eventflow` on GCP and `ubuntu` on AWS.

---

## Using the deployment

The API key was generated on the VM. Read it over SSH (use `eventflow@` on
GCP):

```powershell
$KEY = ssh ubuntu@<public_ip> "sudo grep ^EVENTFLOW_API_KEYS= /opt/eventflow/.env | cut -d= -f2"
```
```powershell
$API = "http://<public_ip>:8080/api/v1"
$H = @{ "X-API-Key" = $KEY }
```

Check readiness and submit a job:

```powershell
Invoke-RestMethod "http://<public_ip>:8080/ready"
```
```powershell
$job = Invoke-RestMethod -Method Post "$API/jobs" -Headers $H -ContentType "application/json" -Body '{"type":"EMAIL_SIMULATION","payload":{"to":"user@example.com","subject":"Hello from the cloud"}}'
```
```powershell
Invoke-RestMethod "$API/jobs/$($job.id)" -Headers $H | Select-Object id, status, worker_id
```

**Grafana and Prometheus** are only reachable through an SSH tunnel. Stop
any local EventFlow stack first, because it also uses ports 3000 and 9090.
Run the `monitoring_tunnel` output and leave that window open:

```powershell
ssh -N -L 3000:127.0.0.1:3000 -L 9090:127.0.0.1:9090 ubuntu@<public_ip>
```

Then open http://localhost:3000 (user `admin`). Read the password with:

```powershell
ssh ubuntu@<public_ip> "sudo grep ^GRAFANA_ADMIN_PASSWORD= /opt/eventflow/.env | cut -d= -f2"
```

All the demo steps from the README (retries, crash recovery, idempotency)
work against the cloud API. For crash recovery, run this over SSH:

```powershell
ssh ubuntu@<public_ip> "cd /opt/eventflow && sudo docker compose kill worker && sudo docker compose up -d worker"
```

## Tear down

From the same directory you deployed from:

```powershell
terraform destroy
```

Then confirm in the cloud console that the instance is gone.

## Troubleshooting

| Problem | Fix |
|---|---|
| `terraform apply` fails with an `allowed_cidr` validation error | Use your IP followed by `/32`; `0.0.0.0/0` is rejected on purpose |
| `Error reading SSH public key` | Create the key with `ssh-keygen` (see Prerequisites) or set `ssh_public_key_path` |
| SSH or API times out | Your IP changed (common on mobile and home networks): update `allowed_cidr` and run `terraform apply` again |
| Bootstrap log stops during the image build | Normal on micro instances; the build can take 10+ minutes. If it fails with "out of memory", use `t3.small` / `e2-small` (not free) |
| AWS: `no default VPC` | Create one in the VPC console (*Actions → Create default VPC*) |
| GCP: `Compute Engine API has not been used` | Run the `gcloud services enable compute.googleapis.com` step |
| Need to start over on the VM | `terraform destroy`, then `terraform apply` |
