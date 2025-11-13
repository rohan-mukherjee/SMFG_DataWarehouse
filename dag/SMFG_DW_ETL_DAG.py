# ======================================================
# DAG Name: SMFG_DW_ETL_DAG
# Purpose:  Extract → Transform → Load data to DW using Airflow
# Author:   Rohan
# ======================================================

from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, timezone
import os, sys, yaml, logging, traceback

# ======================================================
# Add Project Root
# ======================================================
PROJECT_ROOT = "/mnt/d/projects/SMFG_DataWarehouse"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# ======================================================
# Import Utilities
# ======================================================
from source_code.python.airflow_utility.process_logger import (
    load_config,
    get_mysql_engine,
    create_process_master,
    log_process_stage_detail,
    update_process_master,
)
from source_code.python.airflow_utility.staging_data_extraction import (
    extract_single_table,
    load_metadata_query
)

# ======================================================
# Airflow Default Arguments
# ======================================================
default_args = {
    "owner": "Rohan",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ======================================================
# Logger Setup
# ======================================================
logger = logging.getLogger("SMFG_DW_ETL")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


# ======================================================
# DAG Definition
# ======================================================
with DAG(
    dag_id="SMFG_DW_ETL_DAG",
    description="Metadata-driven ETL pipeline for SMFG Data Warehouse",
    start_date=datetime(2025, 11, 12),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    concurrency=5,  # limits max concurrent running tasks per DAG run
    tags=["SMFG", "DataWarehouse"],
) as dag:

    # --------------------------------------------------
    # 1. INIT STAGE: Config + Process Master
    # --------------------------------------------------
    with TaskGroup("init_stage", tooltip="Initialize Config and Process Master") as init_stage:

        @task()
        def update_init_stage_status(env="DEV"):
            """
            Combined INIT stage task:
                1. Load config
                2. Create process master
                3. Update success/failure status
            """

            try:
                # -----------------------------------------
                # Load config
                # -----------------------------------------
                cfg = load_config()
                # logger.info("Configuration loaded successfully.")
                engine = get_mysql_engine(cfg, env)

                # -----------------------------------------
                # Create new process master
                # -----------------------------------------
                process_id = create_process_master(
                    engine,
                    process_type="EOD",
                    current_stage="INIT_STAGE",
                    current_status="RUNNING",
                    created_by="airflow"
                )
                logger.info(f"-----: Process master created: {process_id}")

                # -----------------------------------------
                # Success update
                # -----------------------------------------
                update_process_master(
                    engine,
                    process_id,
                    status="SUCCESS",
                    remarks="Init stage completed successfully.",
                    error_message=None,
                    process_end_at=None
                )
                engine.dispose()
                logger.info(f"-----: DW_PROCESS : INIT_STAGE Process {process_id} marked SUCCESS.")

                return {"cfg": cfg, "process_id": process_id, "status": "SUCCESS"}

            except Exception as e:
                logger.error(f"-----: Error in INIT_STAGE: {e}", exc_info=True)
                engine = None

                try:
                    # Attempt to update DB with failure
                    if "cfg" in locals():
                        engine = get_mysql_engine(cfg, env)

                    if "process_id" in locals() and engine:
                        update_process_master(
                            engine,
                            process_id,
                            status="FAILED",
                            remarks="Error during INIT_STAGE execution.",
                            error_message=str(e),
                            process_end_at = datetime.now(timezone.utc)
                        )
                        logger.warning(f"-----: Process {process_id} marked FAILED.")
                    else:
                        logger.warning("-----: Could not update DB (no engine/process_id).")

                except Exception as inner_err:
                    logger.error(f"-----: Failed to update process status after exception: {inner_err}", exc_info=True)
                finally:
                    if engine:
                        engine.dispose()

                # Re-raise for Airflow to mark task failed
                raise

        init_result = update_init_stage_status()



    # --------------------------------------------------
    # 2. EXTRACTION STAGE - LOAD METADATA
    # --------------------------------------------------
    with TaskGroup("extraction_stage_load_config", tooltip="Parallel Extraction Stage") as extraction_stage_load_config:

        @task()
        def fetch_metadata(init_result, env="DEV"):
            try:
                cfg = init_result["cfg"]
                process_id = init_result["process_id"]

                
                # -----------------------------------------
                # Initial Stage Status Update
                # -----------------------------------------
                engine = get_mysql_engine(cfg, env)
                update_process_master(
                    engine,
                    process_id,
                    current_stage = "STAGING_EXTRACT",
                    status="RUNNING",
                    remarks="Metadata fetch in progress for Staging Extraction",
                    error_message=None,
                    process_end_at=None
                )
                engine.dispose()


                # -----------------------------------------
                # Loading Dynamic MetaData Query for Staging Extraction
                # -----------------------------------------
                metadata_table_query = load_metadata_query(cfg)

                
                # -----------------------------------------
                # Update Load Metadata Status
                # -----------------------------------------
                engine = get_mysql_engine(cfg, env)
                update_process_master(
                    engine,
                    process_id,
                    current_stage = "STAGING_EXTRACT",
                    status="SUCCESS",
                    remarks="Metadata fetch complete for Staging Extraction",
                    error_message=None,
                    process_end_at=None
                )
                engine.dispose()

                # logger.info(f"-----: Metadata fetched: {metadata_table_query}")
                return metadata_table_query

            except Exception as e:
                logger.error(f"-----: Failed fetching metadata: {e}", exc_info=True)
                engine = None

                try:
                    if "cfg" in locals():
                        engine = get_mysql_engine(cfg, env)

                    if "process_id" in locals() and engine:
                        update_process_master(
                            engine,
                            process_id,
                            current_stage = "STAGING_EXTRACT",
                            status="FAILED",
                            remarks='Metadata fetch fail for Staging Extraction',
                            error_message=str(e),
                            process_end_at = datetime.now(timezone.utc)
                        )
                        logger.warning(f"-----: Process {process_id} marked FAILED.")
                    else:
                        logger.warning("-----: Could not update DB (no engine/process_id).")

                except Exception as inner_err:
                    logger.error(f"-----: Failed to update process status after exception: {inner_err}", exc_info=True)
                finally:
                    if engine:
                        engine.dispose()

                # Re-raise for Airflow to mark task failed
                raise

        metadata_table_query = fetch_metadata(init_result)



    # --------------------------------------------------
    # 3. EXTRACTION STAGE - Main Staging Extraction
    # --------------------------------------------------
    with TaskGroup("extraction_stage_staging_data_extraction", tooltip="Parallel Extraction Stage") as extraction_stage_staging_data_extraction:
        pass

    # --------------------------------------------------
    # 2️⃣ EXTRACTION STAGE
    # --------------------------------------------------
    # with TaskGroup("extraction_stage", tooltip="Parallel Extraction Stage") as extraction_stage:

    #     @task()
    #     def fetch_metadata(cfg, env="DEV"):
    #         try:
    #             metadata_query = load_metadata_query(cfg)
    #             meta_list = get_table_metadata(cfg, metadata_query, env)
    #             logger.info(f"✅ Metadata fetched: {len(meta_list)} tables ready for extraction.")
    #             return meta_list
    #         except Exception as e:
    #             logger.error(f"❌ Failed fetching metadata: {e}", exc_info=True)
    #             raise

    #     @task()
    #     def extract_table(cfg, env, chunk_size, table_meta, process_id):
    #         try:
    #             result = extract_single_table(cfg, env, table_meta, chunk_size)

    #             engine = get_mysql_engine(cfg, env)
    #             log_process_stage_detail(
    #                 engine,
    #                 process_id,
    #                 stage_name="STAGING_EXTRACT",
    #                 table_name=result["table"],
    #                 status=result["status"],
    #                 output_path=result["file_path"],
    #                 created_by="airflow"
    #             )
    #             engine.dispose()
    #             logger.info(f"✅ Extraction completed for {result['table']}: {result['status']}")
    #             return result

    #         except Exception as e:
    #             tb = traceback.format_exc()
    #             logger.error(f"❌ Extraction failed for {table_meta.get('FullTableName')}: {e}\n{tb}")
    #             return {
    #                 "table": table_meta.get("FullTableName"),
    #                 "status": "FAILED",
    #                 "file_path": None,
    #                 "error": str(e)
    #             }

    #     @task()
    #     def complete_extraction(cfg, env, process_id, results):
    #         try:
    #             success = len([r for r in results if r.get("status") == "SUCCESS"])
    #             failed = len([r for r in results if r.get("status") != "SUCCESS"])
    #             remarks = f"Extraction summary → Success: {success}, Failed: {failed}"

    #             engine = get_mysql_engine(cfg, env)
    #             update_process_master(engine, process_id, status="STAGING_DONE", remarks=remarks)
    #             engine.dispose()
    #             logger.info(f"✅ Extraction stage completed: {remarks}")
    #             return remarks
    #         except Exception as e:
    #             logger.error(f"❌ Error finalizing extraction: {e}", exc_info=True)
    #             raise

    #     meta_list = fetch_metadata(cfg)
    #     extraction_results = extract_table.partial(
    #         cfg=cfg,
    #         env="DEV",
    #         chunk_size=100000,
    #         process_id=process_id
    #     ).expand(table_meta=meta_list)
    #     extraction_done = complete_extraction(cfg, "DEV", process_id, extraction_results)

    #     meta_list >> extraction_results >> extraction_done


    # # --------------------------------------------------
    # # 3️⃣ TRANSFORMATION STAGE
    # # --------------------------------------------------
    # with TaskGroup("transformation_stage", tooltip="Data Transformations") as transformation_stage:

    #     @task()
    #     def run_transformations(cfg, env, process_id):
    #         try:
    #             logger.info("🚀 Starting transformations...")
    #             # Placeholder for your transformation logic (dynamic merge/join)
    #             logger.info("✅ Transformation stage completed successfully.")
    #             return "TRANSFORMATION_DONE"
    #         except Exception as e:
    #             logger.error(f"❌ Transformation failed: {e}", exc_info=True)
    #             raise


    #     transformation_done = run_transformations(cfg, "DEV", process_id)


    # # --------------------------------------------------
    # # 4️⃣ LOAD STAGE
    # # --------------------------------------------------
    # with TaskGroup("load_stage", tooltip="Final Load into DW") as load_stage:

    #     @task()
    #     def run_load(cfg, env, process_id):
    #         try:
    #             logger.info("🚚 Starting final load into Data Warehouse...")
    #             # Placeholder for final loading logic (curated zone → DW)
    #             logger.info("✅ Load completed successfully.")
    #             return "LOAD_DONE"
    #         except Exception as e:
    #             logger.error(f"❌ Load failed: {e}", exc_info=True)
    #             raise


    #     load_done = run_load(cfg, "DEV", process_id)


    # --------------------------------------------------
    # 5️⃣ DAG FLOW
    # --------------------------------------------------
    init_stage >> extraction_stage_load_config >> extraction_stage_staging_data_extraction
    # init_stage >> extraction_stage >> transformation_stage >> load_stage
