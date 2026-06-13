"""
scripts/validate/test_inference.py

Test the fraud inference endpoint (KServe V2 protocol).

Usage:
  # Port-forward the predictor POD. The `fraud-predictor` Service is an
  # ExternalName (-> knative-local-gateway), so `kubectl port-forward svc/...`
  # does NOT work — forward the pod's container port 8080 directly:
  #   POD=$(kubectl get pod -n fraud-infra -l serving.knative.dev/service=fraud-predictor -o name | head -1)
  #   kubectl port-forward -n fraud-infra "$POD" 8080:8080
  #
  # If port 8080 is already taken (e.g. an Airflow port-forward), use another
  # local port and pass --port, e.g. `... 8086:8080` then `--port 8086`.
  python scripts/validate/test_inference.py [--host HOST] [--port PORT]
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.error

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8085
MODEL_NAME   = "fraud-predictor"

TEST_CASES = [
    {
        "name": "Normal transaction (daytime, small amount)",
        "instances": [
            {"customer_id": "C0000019", "txn_amount": 150.0, "txn_hour": 14},
        ],
    },
    {
        "name": "Suspicious transaction (night, high amount, foreign, declined)",
        "instances": [
            {
                "customer_id": "C0000019",
                "txn_amount": 9999.0,
                "txn_hour": 2,
                "is_declined_txn": 1,
                "is_foreign_txn": 1,
            }
        ],
    },
    {
        "name": "Unknown customer (zero-fill fallback)",
        "instances": [
            {"customer_id": "C_UNKNOWN_999", "txn_amount": 500.0, "txn_hour": 9},
        ],
    },
    {
        "name": "Batch — 3 customers",
        "instances": [
            {"customer_id": "C0000019", "txn_amount": 50.0,   "txn_hour": 10},
            {"customer_id": "C0000020", "txn_amount": 5000.0, "txn_hour": 3, "is_foreign_txn": 1},
            {"customer_id": "C0000021", "txn_amount": 200.0,  "txn_hour": 18},
        ],
    },
]


def build_v2_payload(instances: list) -> dict:
    """Wrap instances in KServe V2 input format."""
    return {
        "inputs": [{
            "name":     "instances",
            "datatype": "BYTES",
            "shape":    [len(instances)],
            "data":     [json.dumps(inst) for inst in instances],
        }]
    }


def parse_v2_response(resp: dict) -> list:
    """Unpack KServe V2 output — each data item is a JSON-encoded prediction dict."""
    outputs = resp.get("outputs", [])
    if not outputs:
        return []
    return [json.loads(item) for item in outputs[0].get("data", [])]


def _decode_json(raw: bytes, url: str) -> dict:
    """Decode a JSON body, with a clear hint when the response isn't JSON."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        snippet = raw[:120].decode(errors="replace").replace("\n", " ").strip()
        raise RuntimeError(
            f"non-JSON response from {url} (got: {snippet!r}). "
            "Is the port-forward pointing at the fraud-predictor pod? "
            "Another service (e.g. Airflow) may be bound to this port."
        )


def http_get(url: str, timeout: int = 5) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return _decode_json(r.read(), url)


def http_post(url: str, payload: dict, timeout: int = 15) -> dict:
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return _decode_json(r.read(), url)


def check_health(base_url: str) -> bool:
    ok = True
    for path in ["/v2/health/live", "/v2/health/ready"]:
        try:
            body = http_get(base_url + path)
            print(f"  {path}: {body}")
        except Exception as e:
            print(f"  {path}: FAILED — {e}")
            ok = False
    return ok


def check_model_ready(base_url: str) -> None:
    try:
        body = http_get(f"{base_url}/v2/models/{MODEL_NAME}/ready")
        print(f"  /v2/models/{MODEL_NAME}/ready: {body}")
    except Exception as e:
        print(f"  model ready: FAILED — {e}")


def check_metrics(base_url: str) -> None:
    """Print request count and latency from /metrics."""
    try:
        with urllib.request.urlopen(f"{base_url}/metrics", timeout=5) as r:
            text = r.read().decode()
        lines = [l for l in text.splitlines()
                 if l.startswith("request_predict_seconds")]
        if lines:
            for l in lines:
                print(f"  {l}")
        else:
            print("  request_predict_seconds: no data yet (no requests made)")
    except Exception as e:
        print(f"  metrics: FAILED — {e}")


def run_inference_tests(predict_url: str) -> bool:
    all_passed = True
    for tc in TEST_CASES:
        print(f"\n[{tc['name']}]")
        payload = build_v2_payload(tc["instances"])
        t0 = time.perf_counter()
        try:
            resp        = http_post(predict_url, payload)
            latency_ms  = int((time.perf_counter() - t0) * 1000)
            predictions = parse_v2_response(resp)
            for pred in predictions:
                flag = "FRAUD" if pred.get("is_fraud") else "ok"
                print(f"  {pred['customer_id']:20s}  score={pred['fraud_score']:.4f}  [{flag}]")
            print(f"  latency: {latency_ms} ms")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"  ERROR {e.code}: {body}")
            all_passed = False
        except Exception as e:
            print(f"  ERROR: {e}")
            all_passed = False
    return all_passed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    base_url    = f"http://{args.host}:{args.port}"
    predict_url = f"{base_url}/v2/models/{MODEL_NAME}/infer"

    print(f"Target: {predict_url}\n")

    print("=== Health ===")
    if not check_health(base_url):
        sys.exit(1)
    print()

    print("=== Model ready ===")
    check_model_ready(base_url)
    print()

    print("=== Inference ===")
    passed = run_inference_tests(predict_url)
    print()

    print("=== Metrics (after tests) ===")
    check_metrics(base_url)
    print()

    print("All tests passed." if passed else "Some tests FAILED.")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
