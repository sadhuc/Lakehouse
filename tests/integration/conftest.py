import json
import subprocess
from dataclass import dataclass

import pytest
from databricks.sdk import WorkspaceClient

BUNDLE_DIR = "../../bundle"
TARGET = "uat"

@dataclass
class UatResourceIds:
    ingestion_pipeline_id: str
    transformation_pipeline_id: str
    orchestration_job_id: str

@pytest.fixture(scope="session")
def uat_resource_ids() -> UatResourceIds:
    result = subprocess.run(
        ["databricks", "bundle", "summary", "--target", TARGET, "--output", "json"],
        cwd = BUNDLE_DIR,
        capture_output = True,
        text = True
    )
    if result.returncode != 0:
        pytest.fail(
            f"`databricks bundle summary --target {TARGET}` failed — is the "
            f"bundle actually deployed to {TARGET}?\n{result.stderr}"
        )
    brace_index = result.stdout.find("{")
    if brace_index == -1:
        pytest.fail(
            f"`databricks bundle summary` produced no JSON at all — full stdout:\n{result.stdout}"
        )
    summary = json.loads(result.stdout[brace_index:])
    try:
        resources = summary["resources"]
        return UatResourceIds(
            ingestion_pipeline_id=resources["pipelines"]["stepright_ingestion_pipeline"]["id"],
            transformation_pipeline_id=resources["pipelines"]["stepright_transformation_pipeline"]["id"],
            orchestration_job_id=resources["jobs"]["stepright_orchestration_job"]["id"],
            warehouse_id=summary["variables"]["stepright_warehouse_id"]["value"],
        )
    except KeyError as e:
        pytest.fail(
            f"Unexpected `bundle summary` JSON shape — missing key {e}. "
            f"Full output:\n{json.dumps(summary, indent=2)}"
        )



@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
    return WorkspaceClient()

@pytest.fixture(scope="session")
def reset_uat(workspace_client=WorkspaceClient):
    catalog = TARGET
    landing_root = f"/Volumes/{catalog}/stepright/landing"
    subfolders = [
        "orders_cdc", "order_items_cdc", "customers_cdc",
        "products", "categories", "clickstream", "inventory",
    ]
    for folder in subfolders:
        dir_path = f"{landing_root}/{folder}"
        for entry in workspace_client.files.list_directory_contents(dir_path):
             workspace_client.files.delete(f"{dir_path}/{entry.name}")
    yield

        