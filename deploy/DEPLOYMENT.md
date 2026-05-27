# vk-osint.com — VPS Deployment Guide

**Target:** Hetzner GPU VPS (Ubuntu 22.04) running Docker Compose.
**Architecture:**

```
Browser → https://api.vk-osint.com
              ↓  (Cloudflare Tunnel → Docker network)
         osint_cloudflared container
              ↓  http://api:8000
         osint_api container (FastAPI)
              ├─ GET /            → serves static/index.html (dashboard)
              ├─ GET/POST /runs   → pipeline control
              ├─ GET /entities    → entity search
              └─ ... (8 endpoints total)
         osint_worker container (ARQ)
         osint_neo4j / osint_chromadb / osint_redis / osint_ollama
```

All services run on a single Docker Compose stack.
The Cloudflare Tunnel (`cloudflared`) handles ingress — no ports exposed to the internet.

---

## Prerequisites (one-time, done from your laptop)

- [ ] Hetzner VPS provisioned (Ubuntu 22.04 LTS, GPU model with nvidia drivers)
- [ ] DNS: `api.vk-osint.com` has a CNAME → `1ed096ee-50d9-4ad1-b9c8-255aeccd28a4.cfargotunnel.com`
  (set in Cloudflare dashboard → vk-osint.com → DNS)
- [ ] Cloudflare Tunnel credentials JSON exists at `/opt/osint/.cloudflared/1ed096ee-50d9-4ad1-b9c8-255aeccd28a4.json`
  on the VPS (see Step 5 below)
- [ ] All API keys collected and ready for `.env`

---

## Step 1 — SSH into the VPS

```bash
ssh root@<your-hetzner-ip>
```

Create the deploy user and working directory:

```bash
useradd -m -s /bin/bash osint
usermod -aG docker osint 2>/dev/null || true   # may fail before Docker install — re-run after
mkdir -p /opt/osint
chown osint:osint /opt/osint
```

---

## Step 2 — Install Docker Engine

```bash
apt-get update
apt-get install -y ca-certificates curl gnupg

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

systemctl enable docker
systemctl start docker

# Add osint user to docker group (if not already done above)
usermod -aG docker osint
```

Verify:

```bash
docker --version       # e.g. Docker version 26.x.x
docker compose version # e.g. Docker Compose version v2.x.x
```

---

## Step 3 — Install NVIDIA Container Toolkit (GPU passthrough for Ollama)

> Skip this step only if running CPU-only mode. The 14B model is too slow without GPU.

```bash
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
  | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
  | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
  | tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

apt-get update
apt-get install -y nvidia-container-toolkit

nvidia-ctk runtime configure --runtime=docker
systemctl restart docker
```

Verify GPU is visible:

```bash
docker run --rm --gpus all nvidia/cuda:12.3.1-base-ubuntu22.04 nvidia-smi
```

You should see your GPU listed. If you get an error, check that the host nvidia driver is installed:

```bash
nvidia-smi   # should work on the host before testing in Docker
```

---

## Step 4 — Clone the Repository

Switch to the osint user:

```bash
su - osint
cd /opt/osint
```

Clone (use HTTPS or SSH depending on your GitHub setup):

```bash
git clone https://github.com/<your-org>/osint-system.git project
cd project
```

Or copy from your laptop using rsync if the repo is private and you haven't set up deploy keys:

```bash
# From your laptop:
rsync -avz --exclude='.git' --exclude='__pycache__' --exclude='.env' \
  /Users/ves/Documents/Claude/Projects/OSINT/project/ \
  osint@<your-hetzner-ip>:/opt/osint/project/
```

---

## Step 5 — Transfer the Cloudflare Tunnel Credentials

The credentials JSON file was created when the tunnel was first provisioned. It must live on the VPS at the path set by `CLOUDFLARED_CREDS_DIR` in `.env`.

**From your laptop:**

```bash
# Create the credentials directory on the VPS
ssh osint@<your-hetzner-ip> "mkdir -p /opt/osint/.cloudflared"

# Copy the credentials JSON
scp ~/.cloudflared/1ed096ee-50d9-4ad1-b9c8-255aeccd28a4.json \
  osint@<your-hetzner-ip>:/opt/osint/.cloudflared/1ed096ee-50d9-4ad1-b9c8-255aeccd28a4.json
```

Set restrictive permissions:

```bash
# On the VPS:
chmod 600 /opt/osint/.cloudflared/1ed096ee-50d9-4ad1-b9c8-255aeccd28a4.json
chmod 700 /opt/osint/.cloudflared
```

---

## Step 6 — Populate .env

```bash
# On the VPS, in /opt/osint/project:
cp .env.example .env
nano .env     # or: vim .env
```

Fill in every value. Mandatory fields that have no default:

| Variable | Where to get it |
|---|---|
| `SUPABASE_URL` | Supabase dashboard → Project Settings → API |
| `DATABASE_URL` | Supabase dashboard → Project Settings → Database → Connection string (Transaction mode) |
| `SUPABASE_ANON_KEY` | Supabase dashboard → Project Settings → API |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase dashboard → Project Settings → API |
| `NEO4J_PASSWORD` | Your choice — must match the password Neo4j container is initialized with |
| `SECRET_KEY` | Generate: `python3 -c "import secrets; print(secrets.token_hex(32))"` |
| `CRUNCHBASE_API_KEY` | Crunchbase → Account → API |
| `SERPAPI_API_KEY` | serpapi.com → Dashboard |
| `CLOUDFLARED_CREDS_DIR` | `/opt/osint/.cloudflared` |

```bash
chmod 600 .env   # secrets — never world-readable
```

**Critical:** the `SECRET_KEY` you set here is what the frontend sends as `X-API-Key`. It must match what you embed in `static/index.html` (see Step 9).

---

## Step 7 — Build and Start the Stack

```bash
cd /opt/osint/project

# Build the api + worker image
docker compose build --no-cache api

# Start all services (detached)
docker compose up -d
```

Watch startup logs to confirm everything comes up:

```bash
docker compose logs -f neo4j chromadb redis ollama
# Wait until all show healthy, then:
docker compose logs -f api worker cloudflared
```

Expected terminal state (all containers should show `Up` or `Up (healthy)`):

```
osint_neo4j      Up (healthy)
osint_chromadb   Up (healthy)
osint_redis      Up (healthy)
osint_ollama     Up (healthy)
osint_api        Up (healthy)
osint_worker     Up
osint_cloudflared Up
```

Check individual health:

```bash
docker compose ps
```

If a container exits immediately, check its logs:

```bash
docker compose logs <service-name> --tail=50
```

---

## Step 8 — Pull Ollama Models

Ollama must download the models before the worker can process any runs.
This step can take 10–30 minutes depending on VPS bandwidth.

```bash
# Primary extraction model (~8GB)
docker compose exec ollama ollama pull qwen3:7b

# Default reasoning model (~9GB)
docker compose exec ollama ollama pull qwen3:14b

# Escalation / resolution model (~14GB)
docker compose exec ollama ollama pull qwen3:22b

# Embedding model (~270MB)
docker compose exec ollama ollama pull nomic-embed-text
```

Verify models are loaded:

```bash
docker compose exec ollama ollama list
```

You should see all four models listed.

---

## Step 9 — Set the API Key in the Frontend

The frontend HTML has a `CONFIG` block that must have the real `SECRET_KEY`:

```bash
# On the VPS or your laptop (before deploying):
# Replace ROTATE_BEFORE_DEPLOY with your actual SECRET_KEY value
SECRET_KEY_VALUE=$(grep '^SECRET_KEY=' /opt/osint/project/.env | cut -d= -f2-)

sed -i "s/ROTATE_BEFORE_DEPLOY/${SECRET_KEY_VALUE}/" \
  /opt/osint/project/static/index.html
```

Verify the replacement happened:

```bash
grep -n "apiKey" /opt/osint/project/static/index.html
# Should show your actual key, not ROTATE_BEFORE_DEPLOY
```

> **Warning:** the API key is embedded in the page HTML. Anyone who can reach
> `api.vk-osint.com` can view-source and read it. Mitigate this with
> Cloudflare Access (Step 11) or by rotating the key after every client handoff.

---

## Step 10 — Verify End-to-End

From anywhere (your laptop, phone, etc.):

```bash
# Health check — no auth required
curl https://api.vk-osint.com/health
# Expected: {"status":"ok","db":"connected","redis":"connected","arq_queue":"connected",...}

# Dashboard — should return HTML
curl -s https://api.vk-osint.com/ | head -20
# Expected: <!DOCTYPE html> ...

# API with auth
curl -H "X-API-Key: <your-secret-key>" https://api.vk-osint.com/runs
# Expected: {"total":0,"limit":50,"offset":0,"runs":[]}
```

Open `https://api.vk-osint.com/` in a browser — the dashboard should load.

---

## Step 11 — Cloudflare Access (recommended before sharing with clients)

Cloudflare Access puts an authentication gate in front of `api.vk-osint.com`
so that only authorized users can reach the tunnel at all — before the request
ever hits your server.

1. Cloudflare dashboard → **Zero Trust** → **Access** → **Applications** → **Add an application**
2. Type: **Self-hosted**
3. Application domain: `api.vk-osint.com`
4. Policy: allow specific email addresses (your clients) or a one-time PIN flow

For machine-to-machine access (future automation), create a **Service Token**:
1. Zero Trust → **Access** → **Service Tokens** → **Create Service Token**
2. Copy the `CF-Access-Client-Id` and `CF-Access-Client-Secret` values
3. In `static/index.html` CONFIG block, set `cfAccessClientId` and `cfAccessClientSecret`

---

## Step 12 — Firewall (Hetzner VPS)

The Docker Compose stack intentionally does not expose any ports to the public
internet except via the Cloudflare Tunnel. However, apply a Hetzner Firewall as
a defense-in-depth measure:

In Hetzner Cloud → **Firewalls** → create a firewall with these inbound rules:

| Protocol | Port | Source | Purpose |
|---|---|---|---|
| TCP | 22 | Your IP only | SSH |
| TCP | 80 | Anywhere | HTTP (Cloudflare needs this for ACME) |
| TCP | 443 | Anywhere | HTTPS (Cloudflare Tunnel) |

Block everything else. Specifically:
- **Block 7474, 7687** (Neo4j) — these are exposed in `docker-compose.yml` for dev convenience but must not be public in production.
- **Block 8000, 8001, 6379, 11434** — all internal services.

After setting up the firewall, comment out or remove the public port mappings in `docker-compose.yml` for production:

```yaml
  neo4j:
    # ports:        # COMMENTED OUT IN PRODUCTION
    #   - "7474:7474"
    #   - "7687:7687"
```

---

## Operational Commands

```bash
# View all service logs (stream)
docker compose logs -f

# View specific service logs
docker compose logs -f api worker

# Restart a single service
docker compose restart api

# Rebuild and restart after a code change
cd /opt/osint/project
git pull
docker compose build --no-cache api
docker compose up -d api worker

# Stop everything
docker compose down

# Full reset (DESTROYS all data volumes)
docker compose down -v   # ← destructive, confirm before running

# Shell into a running container
docker compose exec api bash
docker compose exec ollama bash

# Check ARQ worker queue depth
docker compose exec redis redis-cli llen arq:queue:default
```

---

## Updating the Frontend

```bash
# On your laptop, edit:
/Users/ves/Documents/Claude/Projects/OSINT/project/static/index.html

# Sync to VPS:
rsync -avz \
  /Users/ves/Documents/Claude/Projects/OSINT/project/static/index.html \
  osint@<your-hetzner-ip>:/opt/osint/project/static/index.html

# Re-apply the API key (if it was replaced with ROTATE_BEFORE_DEPLOY again):
SECRET_KEY_VALUE=$(grep '^SECRET_KEY=' /opt/osint/project/.env | cut -d= -f2-)
ssh osint@<your-hetzner-ip> \
  "sed -i \"s/ROTATE_BEFORE_DEPLOY/${SECRET_KEY_VALUE}/\" /opt/osint/project/static/index.html"

# No container restart needed — FastAPI serves the file directly from disk.
# Reload the browser to see changes.
```

---

## Deployment Checklist

### Infrastructure
- [ ] Docker Engine installed, `docker compose version` works
- [ ] nvidia-container-toolkit installed, `docker run --gpus all nvidia/cuda... nvidia-smi` works
- [ ] `/opt/osint/.cloudflared/1ed096ee-50d9-4ad1-b9c8-255aeccd28a4.json` exists, perms 600
- [ ] Hetzner Firewall applied (block 7474, 7687, 8000, 8001, 6379, 11434 from public)

### Application
- [ ] `.env` populated — all mandatory fields filled, no placeholder values
- [ ] `chmod 600 .env` applied
- [ ] `docker compose build --no-cache api` completes without error
- [ ] `docker compose up -d` — all 7 containers show `Up` or `Up (healthy)`
- [ ] All 4 Ollama models pulled and listed by `ollama list`

### Connectivity
- [ ] `curl https://api.vk-osint.com/health` returns `{"status":"ok",...}`
- [ ] `curl https://api.vk-osint.com/` returns HTML (dashboard)
- [ ] Dashboard loads in browser without console errors
- [ ] `curl -H "X-API-Key: <key>" https://api.vk-osint.com/runs` returns valid JSON

### Security
- [ ] `ROTATE_BEFORE_DEPLOY` replaced with real key in `static/index.html`
- [ ] DNS CNAME `api.vk-osint.com` → tunnel UUID confirmed in Cloudflare dashboard
- [ ] Cloudflare Access policy configured on `api.vk-osint.com` (or accepted risk documented)
- [ ] Neo4j ports (7474, 7687) blocked at firewall level
