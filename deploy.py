#!/usr/bin/env python3
"""
deploy.py — Fabric Medallion Pipeline Artifact Deployment
==========================================================
Deploys code artefacts (notebooks, DAGs, dbt project) to a Fabric
workspace that has already been created manually in the Fabric UI.

Workflow:
    1. Create workspace, lakehouse, warehouse, and Managed Airflow in Fabric UI
    2. Fill in config/dev.json and config/resource_ids_dev.json
    3. python deploy.py --env dev --interactive

Requirements:
    pip install azure-identity requests pyyaml
"""

import argparse
import base64
import json
import re
import sys
import time
from pathlib import Path

import requests
import yaml
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential

# ── Constants ────────────────────────────────────────────────────────────────

FABRIC_API   = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

ROOT       = Path(__file__).parent
CONFIG_DIR = ROOT / "config"
FABRIC_DIR = ROOT / "fabric"


# ── Resource IDs ──────────────────────────────────────────────────────────────

def load_resource_ids(env: str) -> dict:
    """
    Read workspace_id and airflow_id from config/resource_ids_<env>.json.
    Create this file manually with IDs copied from the Fabric UI.

    Example config/resource_ids_dev.json:
        {
          "workspace_id": "<guid from Fabric workspace URL>",
          "airflow_id":   "<guid from Managed Airflow item settings>"
        }
    """
    path = CONFIG_DIR / f"resource_ids_{env}.json"
    if not path.exists():
        sys.exit(
            f"\nERROR: {path} not found.\n"
            f"Create it manually with your Fabric resource IDs:\n"
            f"  {{\n"
            f"    \"workspace_id\": \"<guid from Fabric workspace URL>\",\n"
            f"    \"airflow_id\":   \"<guid from Managed Airflow item settings>\"\n"
            f"  }}\n"
        )
    return json.loads(path.read_text())


# ── Auth helpers ──────────────────────────────────────────────────────────────

def get_token(interactive: bool = False) -> str:
    if interactive:
        cred = InteractiveBrowserCredential()
    else:
        try:
            cred = DefaultAzureCredential()
            return cred.get_token(FABRIC_SCOPE).token
        except Exception:
            print("  DefaultAzureCredential failed — falling back to browser login")
            cred = InteractiveBrowserCredential()
    return cred.get_token(FABRIC_SCOPE).token


def headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }


# ── Generic API wrappers ──────────────────────────────────────────────────────

def api_get(token: str, path: str) -> dict:
    r = requests.get(f"{FABRIC_API}{path}", headers=headers(token))
    r.raise_for_status()
    return r.json()


def _poll_operation(token: str, operation_url: str, interval: int = 3, timeout: int = 120) -> None:
    """Poll a Fabric LRO operation URL until it reaches a terminal state."""
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(operation_url, headers=headers(token))
        r.raise_for_status()
        status = (r.json() or {}).get("status", "")
        if status == "Succeeded":
            return
        if status in ("Failed", "Cancelled"):
            raise RuntimeError(f"Fabric operation failed: {r.text}")
        time.sleep(interval)
    raise TimeoutError(f"Fabric operation did not complete within {timeout}s: {operation_url}")


def api_post(token: str, path: str, body: dict) -> dict:
    r = requests.post(f"{FABRIC_API}{path}", headers=headers(token), json=body)
    r.raise_for_status()
    if r.status_code == 202:
        # Long-running operation — poll until done, then return empty dict.
        # Callers that need the item ID should look it up by name afterwards.
        operation_url = r.headers.get("Location") or r.headers.get("Operation-Location", "")
        if operation_url:
            _poll_operation(token, operation_url)
        return {}
    data = r.json() if r.content else {}
    return data if isinstance(data, dict) else {}


# ── Environment helpers ───────────────────────────────────────────────────────

# Packages installed in the Fabric Spark environment attached to all notebooks
# Edit config/notebook_requirements.txt to add/remove packages
NOTEBOOK_PACKAGES = [
    line.strip()
    for line in (CONFIG_DIR / "notebook_requirements.txt").read_text().splitlines()
    if line.strip() and not line.strip().startswith("#")
]


def upsert_environment(token: str, workspace_id: str, name: str) -> str:
    """Create or update a Fabric Environment with the required pip packages.
    Publishes the environment and waits for it to become active.
    Returns the environment item ID.
    """
    # Find existing environment
    existing_id = next(
        (i["id"] for i in _list_items(token, workspace_id, "Environment") if i["displayName"] == name),
        None,
    )

    if not existing_id:
        print(f"  Creating environment '{name}' …")
        result = api_post(token, f"/workspaces/{workspace_id}/items", {
            "displayName": name,
            "type": "Environment",
        })
        existing_id = result.get("id") or next(
            (i["id"] for i in _list_items(token, workspace_id, "Environment") if i["displayName"] == name),
            None,
        )
        if not existing_id:
            raise RuntimeError(f"Environment '{name}' was not found after creation")
        print(f"  Environment created: {existing_id}")
    else:
        print(f"  Environment '{name}' already exists: {existing_id}")

    # Set pip libraries via staging definition
    libraries_yaml = "dependencies:\n" + "".join(f"  - {pkg}\n" for pkg in NOTEBOOK_PACKAGES)
    encoded_yaml = base64.b64encode(libraries_yaml.encode()).decode()

    print(f"  Setting libraries: {NOTEBOOK_PACKAGES}")
    try:
        api_post(
            token,
            f"/workspaces/{workspace_id}/environments/{existing_id}/staging/libraries",
            {"customLibraries": [], "publicLibraries": {"environment.yml": encoded_yaml}},
        )
    except Exception as exc:
        print(f"  WARN: Could not set libraries via API ({exc}) — set them manually in the Environment UI.")
        return existing_id

    # Publish the environment (long-running operation)
    print("  Publishing environment …")
    try:
        api_post(token, f"/workspaces/{workspace_id}/environments/{existing_id}/staging/publish", {})
    except Exception as exc:
        print(f"  WARN: Publish API call failed ({exc}) — publish manually in the Environment UI.")
        return existing_id

    # Poll until published (timeout 10 min)
    for _ in range(60):
        time.sleep(10)
        try:
            info = api_get(token, f"/workspaces/{workspace_id}/environments/{existing_id}")
            state = info.get("properties", {}).get("publishInfo", {}).get("state", "")
            print(f"    publish state: {state}")
            if state == "Success":
                print("  Environment published.")
                break
            if state in ("Failed", "Cancelled"):
                print(f"  WARN: Environment publish ended with state '{state}' — check the Fabric UI.")
                break
        except Exception:
            pass
    else:
        print("  WARN: Timed out waiting for environment publish — notebooks may need manual env attachment.")

    return existing_id


# ── Notebook helpers ──────────────────────────────────────────────────────────

def _encode_notebook(py_path: Path, environment_id: str | None = None, workspace_id: str | None = None) -> str:
    """Base64-encode a .py file so it can be embedded in a Fabric Notebook definition.
    Optionally attaches a Fabric Environment by embedding its ID in the notebook metadata.
    """
    source = py_path.read_text(encoding="utf-8")
    metadata: dict = {
        "kernelspec": {"display_name": "PySpark", "language": "python", "name": "synapse_pyspark"},
        "language_info": {"name": "python"},
    }
    if environment_id and workspace_id:
        metadata["dependencies"] = {
            "environment": {
                "environmentId": environment_id,
                "workspaceId":   workspace_id,
            }
        }
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": metadata,
        "cells": [{
            "cell_type": "code",
            "metadata": {},
            "source": source.splitlines(keepends=True),
            "outputs": [],
            "execution_count": None,
        }],
    }
    return base64.b64encode(json.dumps(nb).encode()).decode()


def _list_items(token: str, workspace_id: str, item_type: str) -> list:
    path = f"/workspaces/{workspace_id}/items?type={item_type}"
    return api_get(token, path).get("value", [])


def upsert_notebook(
    token: str,
    workspace_id: str,
    name: str,
    py_path: Path,
    environment_id: str | None = None,
) -> str:
    """Create or update a Fabric Notebook from a .py source file.
    If environment_id is provided, the notebook is attached to that environment.
    """
    existing_id = next(
        (i["id"] for i in _list_items(token, workspace_id, "Notebook") if i["displayName"] == name),
        None,
    )
    definition = {
        "format": "ipynb",
        "parts": [{
            "path": "artifact.content.ipynb",
            "payload": _encode_notebook(py_path, environment_id=environment_id, workspace_id=workspace_id),
            "payloadType": "InlineBase64",
        }],
    }
    if existing_id:
        print(f"  Updating notebook '{name}' …")
        api_post(token, f"/workspaces/{workspace_id}/items/{existing_id}/updateDefinition",
                 {"definition": definition})
        print(f"  Notebook updated: {existing_id}")
        return existing_id

    print(f"  Creating notebook '{name}' …")
    result = api_post(token, f"/workspaces/{workspace_id}/items", {
        "displayName": name,
        "type": "Notebook",
        "definition": definition,
    })
    # api_post polls the LRO and returns {} when done — look up the real ID by name
    notebook_id = result.get("id") or next(
        (i["id"] for i in _list_items(token, workspace_id, "Notebook") if i["displayName"] == name),
        None,
    )
    if not notebook_id:
        raise RuntimeError(f"Notebook '{name}' was not found after creation")
    print(f"  Notebook created: {notebook_id}")
    return notebook_id


# ── Airflow artifact helpers ───────────────────────────────────────────────────

def upload_dag_file(token: str, workspace_id: str, airflow_id: str, local_path: Path):
    """Upload a DAG Python file to the Managed Airflow DAGs folder."""
    file_name = local_path.name
    encoded   = base64.b64encode(local_path.read_bytes()).decode()
    print(f"  Uploading DAG '{file_name}' …")
    try:
        api_post(token, f"/workspaces/{workspace_id}/managedAirflow/{airflow_id}/dags", {
            "displayName": file_name,
            "content": encoded,
        })
        print(f"  DAG uploaded: {file_name}")
    except Exception as exc:
        print(f"  WARN: DAG upload API not yet GA — upload '{file_name}' manually. ({exc})")


def upload_dbt_project(token: str, workspace_id: str, airflow_id: str):
    """Upload the dbt/ project tree alongside DAGs so the Gold BashOperator can reach it."""
    dbt_dir = FABRIC_DIR / "dags" / "dbt"
    if not dbt_dir.exists():
        print("  WARN: dbt/ directory not found — skipping dbt project upload")
        return
    for file_path in sorted(dbt_dir.rglob("*")):
        if not file_path.is_file():
            continue
        display_name = str(file_path.relative_to(ROOT)).replace("\\", "/")
        encoded = base64.b64encode(file_path.read_bytes()).decode()
        print(f"  Uploading dbt file '{display_name}' …")
        try:
            api_post(token, f"/workspaces/{workspace_id}/managedAirflow/{airflow_id}/dags", {
                "displayName": display_name,
                "content": encoded,
            })
            print(f"  dbt file uploaded: {display_name}")
        except Exception as exc:
            print(f"  WARN: Could not upload '{display_name}' — copy to Airflow storage manually. ({exc})")


def set_airflow_variable(token: str, workspace_id: str, airflow_id: str, key: str, value: str):
    print(f"  Setting Airflow variable '{key}' …")
    try:
        api_post(token, f"/workspaces/{workspace_id}/managedAirflow/{airflow_id}/variables", {
            "key": key, "value": value,
        })
    except Exception as exc:
        print(f"  WARN: Variable API not yet GA — set '{key}' manually in the Airflow UI. ({exc})")

# ── Main deploy logic ─────────────────────────────────────────────────────────

def deploy(env: str, interactive: bool = False):
    env_cfg       = json.loads((CONFIG_DIR / f"{env}.json").read_text())
    fabric_cfg    = yaml.safe_load((ROOT / "fabric.yaml").read_text())
    teams_webhook = fabric_cfg["environments"][env].get("teams_webhook_url", "")
    sql_endpoint  = env_cfg["gold_warehouse_sql_endpoint"]
    lakehouse     = env_cfg["lakehouse"]

    print(f"\n{'='*60}")
    print(f" Deploying artefacts — Fabric Medallion Pipeline  [env={env}]")
    print(f"{'='*60}\n")

    # ── 1. Load resource IDs ──────────────────────────────────────────────────
    print("[0] Loading resource IDs …")
    ids = load_resource_ids(env)
    workspace_id = ids["workspace_id"]
    airflow_id   = ids["airflow_id"]
    print(f"  workspace_id : {workspace_id}")
    print(f"  airflow_id   : {airflow_id}")

    token = get_token(interactive=interactive)

    # ── 2. Spark Environment ──────────────────────────────────────────────────
    print("\n[1/4] Spark Environment")
    env_name = fabric_cfg["environments"][env].get("spark_env_name", f"medallion-env-{env}")
    environment_id = upsert_environment(token, workspace_id, env_name)

    # ── 3. Notebooks (Bronze + Silver — Gold handled by dbt) ──────────────────
    print("\n[2/4] Notebooks")
    notebooks = {
        "shared_functions":      FABRIC_DIR / "notebooks" / "shared"  / "shared_functions.py",
        "ingest_file":           FABRIC_DIR / "notebooks" / "bronze"  / "ingest_file.py",
        "process_invoice":       FABRIC_DIR / "notebooks" / "silver1" / "process_invoice.py",
        "build_carrier_invoice": FABRIC_DIR / "notebooks" / "silver2" / "build_carrier_invoice.py",
    }
    notebook_ids: dict[str, str] = {}
    for name, path in notebooks.items():
        notebook_ids[name] = upsert_notebook(token, workspace_id, name, path, environment_id=environment_id)

    # ── 4. DAG files + dbt project ────────────────────────────────────────────
    print("\n[3/4] DAG files + dbt project")
    for dag_file in sorted((FABRIC_DIR / "dags").glob("*.py")):
        upload_dag_file(token, workspace_id, airflow_id, dag_file)
    upload_dbt_project(token, workspace_id, airflow_id)

    # ── 5. Airflow Variables ──────────────────────────────────────────────────
    print("\n[4/4] Airflow Variables")
    variables = {
        # Used by DAG: Variable.get("fabric_workspace_id")
        "fabric_workspace_id":                      workspace_id,
        # Used by DAG: notebook_id("bronze/ingest_file") → notebook_id__bronze__ingest_file
        "notebook_id__bronze__ingest_file":         notebook_ids["ingest_file"],
        # Used by DAG: notebook_id("silver1/process_invoice")
        "notebook_id__silver1__process_invoice":    notebook_ids["process_invoice"],
        # Used by DAG: notebook_id("silver2/build_carrier_invoice")
        "notebook_id__silver2__build_carrier_invoice": notebook_ids["build_carrier_invoice"],
        # Used by DAG: Variable.get("environment", default_var="dev")
        "environment":                              env,
        "TEAMS_WEBHOOK_URL":                        teams_webhook,
        "dbt_project_dir":                          "/opt/airflow/dags/dbt",
        "dbt_warehouse_server":                     sql_endpoint,
        "lakehouse":                                lakehouse,
        # azure_tenant_id / azure_client_id / azure_client_secret:
        # set these manually in Airflow or via Key Vault — never here.
    }
    for k, v in variables.items():
        set_airflow_variable(token, workspace_id, airflow_id, k, v)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(" Deployment complete")
    print(f"{'='*60}")
    print(f"  Workspace : {workspace_id}")
    print(f"  Airflow   : {airflow_id}")
    print(f"\n  Notebooks:")
    for name, nb_id in notebook_ids.items():
        print(f"    {name}: {nb_id}")
    print(f"\n  NOTE: Set azure_tenant_id / azure_client_id / azure_client_secret")
    print(f"        in Airflow Variables (or Key Vault) before triggering the DAG.")
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deploy Fabric Medallion Pipeline artefacts")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"],
                        help="Target environment (default: dev)")
    parser.add_argument("--interactive", action="store_true",
                        help="Use browser login instead of DefaultAzureCredential")
    args = parser.parse_args()
    deploy(env=args.env, interactive=args.interactive)


if __name__ == "__main__":
    main()
