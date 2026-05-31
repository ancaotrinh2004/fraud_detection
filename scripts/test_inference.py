"""
scripts/test_inference.py

Test the fraud inference endpoint.
Usage: python scripts/test_inference.py [--host HOST] [--port PORT]
"""

import argparse
import json
import sys
import urllib.request
import urllib.error

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8080

TEST_CASES = [
    {
        "name": "Normal transaction (daytime, small amount)",
        "payload": {
            "instances": [{
                "customer_id": "C0000019",
                "txn_amount": 150.0,
                "txn_hour": 14,
            }]
        },
    },
    {
        "name": "Suspicious transaction (night, high amount, foreign, declined)",
        "payload": {
            "instances": [{
                "customer_id": "C0000019",
                "txn_amount": 9999.0,
                "txn_hour": 2,
                "is_declined_txn": 1,
                "is_foreign_txn": 1,
            }]
        },
    },
    {
        "name": "Unknown customer (zero-fill fallback)",
        "payload": {
            "instances": [{
                "customer_id": "C_UNKNOWN_999",
                "txn_amount": 500.0,
                "txn_hour": 9,
            }]
        },
    },
    {
        "name": "Batch — 3 customers",
        "payload": {
            "instances": [
                {"customer_id": "C0000019", "txn_amount": 50.0,   "txn_hour": 10},
                {"customer_id": "C0000020", "txn_amount": 5000.0, "txn_hour": 3, "is_foreign_txn": 1},
                {"customer_id": "C0000021", "txn_amount": 200.0,  "txn_hour": 18},
            ]
        },
    },
]


def call(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def check_health(base_url: str) -> bool:
    for path in ["/v2/health/live", "/v2/health/ready"]:
        req = urllib.request.Request(base_url + path)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                body = json.loads(resp.read())
                print(f"  {path}: {body['status']}")
        except Exception as e:
            print(f"  {path}: FAILED — {e}")
            return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    base_url = f"http://{args.host}:{args.port}"
    predict_url = f"{base_url}/v1/models/fraud:predict"

    print(f"Target: {predict_url}\n")

    # Health check
    print("=== Health ===")
    if not check_health(base_url):
        sys.exit(1)
    print()

    # Model info
    try:
        req = urllib.request.Request(f"{base_url}/v1/models/fraud")
        with urllib.request.urlopen(req, timeout=5) as resp:
            info = json.loads(resp.read())
        print(f"=== Model ===")
        print(f"  version : {info['model_version']}")
        print(f"  metrics : {info['metrics']}")
        print()
    except Exception as e:
        print(f"Model info failed: {e}\n")

    # Inference tests
    print("=== Inference ===")
    all_passed = True
    for tc in TEST_CASES:
        print(f"\n[{tc['name']}]")
        try:
            result = call(predict_url, tc["payload"])
            for pred in result["predictions"]:
                flag = "FRAUD" if pred["is_fraud"] else "ok"
                print(f"  {pred['customer_id']:20s}  score={pred['fraud_score']:.4f}  [{flag}]")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"  ERROR {e.code}: {body}")
            all_passed = False
        except Exception as e:
            print(f"  ERROR: {e}")
            all_passed = False

    print()
    print("All tests passed." if all_passed else "Some tests failed.")
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
