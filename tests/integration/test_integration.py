import os
import subprocess

FIXED_SEED = 42
FIXED_AS_OF = "2026-01-01"

GENERATOR_ARGS = [
    "--n-customers", "20", "--n-orders", "50", "--n-products", "15",
    "--n-clickstream", "200", "--n-inventory-days", "2"
]

EXPECTED_LANDING_COUNTS = {
    "categories": 6,
    "customers": 20,
    "products": 15,
    "orders": 50,
    "order_items": 101,
    "clickstream": 200,
    "inventory": 90,
}

def _generate_fixed_seed_batch(output_dir: str) -> str:
    """
    Runs data_generator.py with the confirmed integration-test seed/as-of
    and scale. Returns the actual batch_0 directory path (the generator
    writes to {output_dir}/batch_0/, not output_dir directly).
    """
    result = subprocess.run(
        [
            "python3", "../../data_generator.py",
            "--batch", "0",
            "--seed", str(FIXED_SEED),
            "--as-of", FIXED_AS_OF,
            "--output-dir", output_dir,
            "--state-file", os.path.join(output_dir, "generator_state.json"),
            *GENERATOR_ARGS,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"data_generator.py failed:\n{result.stderr}"
    return os.path.join(output_dir, "batch_0")

def _upload_batch_to_landing(workspace_client, batch_dir: str, catalog: str):
    landing_root = f"/Volumes/{catalog}/stepright/landing"
    for folder in os.listdir(batch_dir):
        local_folder = os.path.join(batch_dir, folder)
        if not os.path.isdir(local_folder):
            continue
        for filename in os.listdir(local_folder):
            local_path = os.path.join(local_folder, filename)
            remote_path = f"{landing_root}/{folder}/{filename}"
            with open(local_path, "rb") as f:
                workspace_client.files.upload(remote_path, f, overwrite=True)

def _table_count(workspace_client, catalog: str, warehouse_id: str, table: str) -> int:
    result = workspace_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema="stepright",
        statement=f"SELECT COUNT(*) FROM {table}",
        wait_timeout="30s",
    )
    assert result.status.state.value == "SUCCEEDED", f"Query on {table} failed: {result.status.error}"
    return int(result.result.data_array[0][0])

def test_orchestration_job_end_to_end(workspace_client, uat_resource_ids, reset_uat, tmp_path):
    """Setup -> Trigger -> Validate, against uat, using the fixed-seed batch."""

    # --- Setup ---
    # reset_uat (fixture) has already cleared landing's contents.
    batch_dir = _generate_fixed_seed_batch(str(tmp_path / "it_batch"))
    _upload_batch_to_landing(workspace_client, batch_dir, catalog="uat")

    ingestion_update = workspace_client.pipelines.start_update(
        pipeline_id=uat_resource_ids.ingestion_pipeline_id, full_refresh=True
    )

    workspace_client.pipelines.wait_get_pipeline_idle(
        pipeline_id=uat_resource_ids.ingestion_pipeline_id
    )
 
    transformation_update = workspace_client.pipelines.start_update(
        pipeline_id=uat_resource_ids.transformation_pipeline_id, full_refresh=True
    )
    workspace_client.pipelines.wait_get_pipeline_idle(
        pipeline_id=uat_resource_ids.transformation_pipeline_id
    )
 
    run = workspace_client.jobs.run_now(
        job_id=int(uat_resource_ids.orchestration_job_id),
    ).result()
 
    assert run.state.result_state.value == "SUCCESS", (
        f"Orchestration job run did not succeed: "
        f"{run.state.result_state} — {run.state.state_message}"
    )

     # --- Validate ---
    catalog = "uat"

    conservation_checks = {
        "categories": EXPECTED_LANDING_COUNTS["categories"],
        "customers": EXPECTED_LANDING_COUNTS["customers"],
        "products": EXPECTED_LANDING_COUNTS["products"],
        "orders": EXPECTED_LANDING_COUNTS["orders"]
    }

    for source, expected_total in conservation_checks.items():
        valid = _table_count(workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_valid")
        quarantined = _table_count(workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_quarantined")
        assert valid + quarantined == expected_total, (
            f"bronze_{source}: {valid} valid + {quarantined} quarantined "
            f"= {valid + quarantined}, expected {expected_total} — a row "
            f"was lost somewhere, not just failed validation"
        )
    
    for source in conservation_checks:
        quarantined = _table_count(workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_quarantined")
        assert quarantined == 0, (
            f"bronze_{source}_quarantined has {quarantined} rows — expected 0 "
            f"given the generator's referential-integrity-by-construction. "
            f"Investigate before assuming this assertion is simply wrong."
        )
        



