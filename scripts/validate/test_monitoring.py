"""
scripts/test_monitoring.py

Verify Level 1 (Prometheus scrape targets) and Level 2 (Pushgateway) are working.

Usage:
    python scripts/test_monitoring.py

Assumes port-forwards are already running:
    kubectl port-forward -n monitoring svc/kube-prom-kube-prometheus-prometheus 9090:9090 &
    kubectl port-forward -n monitoring svc/pushgateway 9091:9091 &
"""

import subprocess
import time
import urllib.request
import urllib.error
import urllib.parse
import json

PROMETHEUS_URL = "http://localhost:9090"
PUSHGATEWAY_URL = "http://localhost:9091"

GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
RESET  = "\033[0m"

def ok(msg):  print(f"{GREEN}[PASS]{RESET} {msg}")
def err(msg): print(f"{RED}[FAIL]{RESET} {msg}")
def info(msg): print(f"{YELLOW}[INFO]{RESET} {msg}")


def get(url) -> dict:
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


def post_raw(url: str, body: str) -> int:
    data = body.encode()
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status


def delete(url: str) -> int:
    req = urllib.request.Request(url, method="DELETE")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status


def section(title: str):
    print(f"\n{'═' * 54}")
    print(f" {title}")
    print('═' * 54)


# ── Start port-forwards ───────────────────────────────────────────────────────

section("Setup — starting port-forwards")

pf_prom = subprocess.Popen(
    ["kubectl", "port-forward", "-n", "monitoring",
     "svc/kube-prom-kube-prometheus-prometheus", "9090:9090"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)
pf_pgw = subprocess.Popen(
    ["kubectl", "port-forward", "-n", "monitoring",
     "svc/pushgateway", "9091:9091"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)

info("Waiting 5s for port-forwards to be ready...")
time.sleep(5)


# ── Level 1: Prometheus scrape targets ───────────────────────────────────────

section("Level 1 — Prometheus scrape targets")

try:
    targets_data = get(f"{PROMETHEUS_URL}/api/v1/targets?state=active")
    active = targets_data["data"]["activeTargets"]

    for job in ("airflow-statsd", "minio", "pushgateway"):
        match = next((t for t in active if t["labels"].get("job") == job), None)
        if match is None:
            err(f"{job:22s} → NOT FOUND in Prometheus targets")
        elif match["health"] == "up":
            ok(f"{job:22s} → UP  ({match['scrapeUrl']})")
        else:
            last_err = match.get("lastError", "unknown error")
            err(f"{job:22s} → {match['health'].upper()}  — {last_err}")

except urllib.error.URLError as e:
    err(f"Cannot reach Prometheus at {PROMETHEUS_URL}: {e}")
    info("Make sure port-forward is running — check kubectl get pods -n monitoring")


# ── Level 2: Pushgateway reachability + end-to-end metric push ───────────────

section("Level 2 — Pushgateway + end-to-end push")

try:
    with urllib.request.urlopen(f"{PUSHGATEWAY_URL}/-/ready", timeout=5) as r:
        if r.status == 200:
            ok("Pushgateway is reachable on :9091")
        else:
            err(f"Pushgateway /-/ready returned HTTP {r.status}")
except urllib.error.URLError as e:
    err(f"Cannot reach Pushgateway at {PUSHGATEWAY_URL}: {e}")

# Push a test metric
test_payload = (
    "# HELP test_monitoring_check Smoke test from test_monitoring.py\n"
    "# TYPE test_monitoring_check gauge\n"
    "test_monitoring_check 1.0\n"
)
try:
    status = post_raw(f"{PUSHGATEWAY_URL}/metrics/job/test_monitoring", test_payload)
    ok(f"Pushed test metric to Pushgateway (HTTP {status})")
except Exception as e:
    err(f"Failed to push metric: {e}")

# Wait for Prometheus to scrape it (up to 20s)
info("Waiting up to 20s for Prometheus to scrape the test metric...")
found = False
for _ in range(4):
    time.sleep(5)
    try:
        result = get(
            f"{PROMETHEUS_URL}/api/v1/query?"
            + urllib.parse.urlencode({"query": "test_monitoring_check"})
        )
        values = result["data"]["result"]
        if values and values[0]["value"][1] == "1":
            ok("Prometheus scraped test metric from Pushgateway ✓")
            found = True
            break
    except Exception:
        pass

if not found:
    err("Prometheus did not see test metric after 20s — check pushgateway scrape interval")

# Clean up test metric
try:
    delete(f"{PUSHGATEWAY_URL}/metrics/job/test_monitoring")
except Exception:
    pass


# ── Pods summary ─────────────────────────────────────────────────────────────

section("Pods summary — monitoring namespace")

result = subprocess.run(
    ["kubectl", "get", "pods", "-n", "monitoring", "--no-headers"],
    capture_output=True, text=True,
)
for line in result.stdout.strip().splitlines():
    parts = line.split()
    name, ready, status = parts[0], parts[1], parts[2]
    color = GREEN if status == "Running" else RED
    print(f"  {color}{status:12s}{RESET}  {ready:5s}  {name}")

print()
info("Port-forward Grafana:  kubectl port-forward -n monitoring svc/kube-prom-grafana 3000:80")
info("Open browser:          http://localhost:3000  (admin / fraud-grafana-2025)")

# ── Teardown ─────────────────────────────────────────────────────────────────
pf_prom.terminate()
pf_pgw.terminate()
