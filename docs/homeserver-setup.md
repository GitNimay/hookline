# Home server setup (stage 2)

The home server runs the same k3d cluster as the PC, inside WSL2, with `deploy/values/homeserver.yaml`. Images come from GitHub Container Registry; you deploy from the PC with `make deploy-homeserver`.

Reference machine: 16 GB RAM, 8 threads, SSD. WSL2 gets 10 GB, leaving 6 GB for Windows.

## 1. WSL2 and Ubuntu

In an elevated PowerShell:
```powershell
wsl --install -d Ubuntu-24.04
```
Create `%UserProfile%\.wslconfig`:
```ini
[wsl2]
memory=10GB
processors=6
swap=4GB
```
Then `wsl --shutdown` and start Ubuntu again.

Enable systemd in WSL (`/etc/wsl.conf` inside Ubuntu):
```ini
[boot]
systemd=true
```

## 2. Docker Engine inside WSL2

Inside Ubuntu (Docker Desktop also works; Engine uses less memory):
```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
sudo systemctl enable --now docker
```
Install the CLIs (pinned versions match the PC):
```bash
# k3d v5.9.0, kubectl, helm
curl -fsSL https://raw.githubusercontent.com/k3d-io/k3d/v5.9.0/install.sh | TAG=v5.9.0 bash
sudo snap install kubectl --classic
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

## 3. Cluster that starts on boot

```bash
sudo mkdir -p /srv/hookline/backups /srv/hookline/storage
git clone https://github.com/GitNimay/hookline && cd hookline
k3d cluster create --config deploy/k3d/homeserver.yaml
```
k3d's node containers use Docker's `unless-stopped` restart policy, and Docker starts with systemd, so the cluster returns after a reboot as soon as WSL starts.

Start WSL at boot with a Windows Task Scheduler task (elevated PowerShell):
```powershell
$action  = New-ScheduledTaskAction -Execute "wsl.exe" -Argument "-d Ubuntu-24.04 -u root -- systemctl start docker"
$trigger = New-ScheduledTaskTrigger -AtStartup
Register-ScheduledTask -TaskName "Hookline WSL" -Action $action -Trigger $trigger -User "SYSTEM" -RunLevel Highest
```
Also disable sleep (`powercfg /change standby-timeout-ac 0`) and give the server a static IP or a DHCP reservation on the router.

## 4. Reach the Ingress from the LAN

The k3d load balancer binds `0.0.0.0:80/443` inside WSL2. Forward the Windows host ports to WSL (elevated PowerShell; re-run if the WSL IP changes, or use WSL mirrored networking on Windows 11 23H2+ with `networkingMode=mirrored` in `.wslconfig`, which makes this unnecessary):
```powershell
$wsl = (wsl -d Ubuntu-24.04 hostname -I).Trim().Split(" ")[0]
netsh interface portproxy add v4tov4 listenport=80  listenaddress=0.0.0.0 connectport=80  connectaddress=$wsl
netsh interface portproxy add v4tov4 listenport=443 listenaddress=0.0.0.0 connectport=443 connectaddress=$wsl
New-NetFirewallRule -DisplayName "Hookline HTTP/HTTPS" -Direction Inbound -Protocol TCP -LocalPort 80,443 -Action Allow
```
Find the server's LAN IP with `ipconfig` (IPv4 of the Wi-Fi/Ethernet adapter). On each client, add to the hosts file (or create the records on the router's DNS):
```
192.168.1.50  hookline.home grafana.home
```
TLS uses the self-signed `selfsigned` ClusterIssuer; trust the certificate on clients or accept the warning. Do not expose ports 80/443 to the internet; for remote access use Tailscale or Cloudflare Tunnel (optional).

## 5. Images and secrets

Images are pushed to `ghcr.io/gitnimay/hookline-<service>` by `.github/workflows/release.yml` on every `v*` tag. Create a read-only token (GitHub → Settings → Developer settings → fine-grained token with `read:packages`) and store it as an image-pull Secret:
```bash
kubectl create namespace hookline
kubectl -n hookline create secret docker-registry ghcr-pull --docker-server=ghcr.io \
  --docker-username=<github-user> --docker-password=<token>
```
For offline use, build on the PC and `k3d image import` the images instead (set `image.prefix`/`tag` accordingly).

## 6. Deploy from the PC

Export the server's kubeconfig (inside WSL on the server) and copy it to the PC:
```bash
k3d kubeconfig get hookline | sed "s/0.0.0.0/192.168.1.50/" > hookline-home.kubeconfig
```
On the PC:
```bash
export KUBECONFIG=$PWD/hookline-home.kubeconfig
make deploy-homeserver            # k8s-secrets.sh + helmfile -e homeserver sync
helm -n hookline rollback hookline   # if a release misbehaves
```
`make deploy-homeserver` keeps existing secrets; it creates random ones only on the first run (the admin key is written to `.hookline/k8s.env`).

## 7. Backups

`deploy/helm/infra` runs `hookline-pg-backup` nightly (03:00 UTC) on the home server, writing `pg_dump -Fc` files to `/srv/hookline/backups` and keeping 7 days. Restore steps are in [runbook.md](runbook.md#restore-a-backup); do a test restore once after setup.

## 8. Check the exit criteria

1. Reboot Windows. Within 5 minutes `kubectl get pods -A` shows everything `Running`, without manual steps.
2. From another device on the LAN, open `https://grafana.home` and `https://hookline.home/v1/event-types` (401 without a key).
3. Run the load test against the server and record the numbers in [benchmarks.md](benchmarks.md):
   ```bash
   HOOKLINE_INGEST_URL=https://hookline.home k6 run loadtest/steady.js
   ```
