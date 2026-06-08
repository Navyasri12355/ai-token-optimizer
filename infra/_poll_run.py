"""
Polls a Databricks run until it terminates, then checks the blob for models.
Run as: python cloud/_poll_run.py <run_id>
"""

import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


def load_dotenv(path=".env"):
    env = {}
    try:
        for line in Path(path).read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env


def az(*args):
    cmd = "az " + " ".join(args)
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"az command failed: {r.stderr.strip()}")
    return r.stdout.strip()


def get_token():
    return az(
        "account get-access-token",
        "--resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
        "--query accessToken --output tsv",
    )


def dbx_get(ws_url, token, path):
    req = urllib.request.Request(
        f"https://{ws_url}{path}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.load(resp)


def check_blobs(env):
    account = env.get("AZURE_STORAGE_ACCOUNT", "")
    key = env.get("AZURE_STORAGE_KEY", "")
    container = env.get("AZURE_CONTAINER", "pipeline-data")
    if not account or not key:
        print("  (no storage creds — skipping blob check)")
        return

    result = subprocess.run(
        f'az storage blob list --account-name "{account}" --account-key "{key}"'
        f' --container-name "{container}" --prefix models --num-results 50'
        f' --query "[].name" --output json',
        shell=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        print(f"  blob list failed: {result.stderr.strip()}")
        return
    blobs = json.loads(result.stdout) if result.stdout.strip() else []
    if blobs:
        print(f"  Models in blob ({len(blobs)} files):")
        for b in blobs[:20]:
            print(f"    {b}")
        if len(blobs) > 20:
            print(f"    ... and {len(blobs) - 20} more")
    else:
        print("  No blobs found under models/ prefix.")


def main():
    run_id = sys.argv[1] if len(sys.argv) > 1 else "173119463581759"
    ws_url = "adb-7405610179368028.8.azuredatabricks.net"
    env = load_dotenv()

    print(f"Polling run {run_id} on {ws_url}")
    print(f"Storage account from .env: {env.get('AZURE_STORAGE_ACCOUNT', '(missing)')}")
    print()

    token = get_token()
    token_fetched_at = time.time()

    for i in range(1, 120):
        # Refresh token every 45 minutes
        if time.time() - token_fetched_at > 2700:
            token = get_token()
            token_fetched_at = time.time()

        try:
            data = dbx_get(ws_url, token, f"/api/2.1/jobs/runs/get?run_id={run_id}")
        except Exception as e:
            print(f"  [{i * 15:4d}s] poll error: {e}")
            time.sleep(15)
            continue

        state = data.get("state", {})
        life = state.get("life_cycle_state", "UNKNOWN")
        result = state.get("result_state", "")
        msg = state.get("state_message", "")

        print(f"  [{i * 15:4d}s] {life:<18} {result}  {msg[:80]}")

        if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            print()
            if result == "SUCCESS":
                print("Pipeline SUCCEEDED.")
            else:
                print(f"Pipeline FAILED: {msg}")

            print()
            print("Checking Azure Blob for model artifacts...")
            check_blobs(env)
            return

        time.sleep(15)

    print("Timed out waiting for run to complete.")


if __name__ == "__main__":
    main()
