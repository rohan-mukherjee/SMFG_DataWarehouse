# ======================================================
# Utility: Staging Data Extraction from DB 
# ======================================================

import os, sys
import yaml
import logging
import traceback
import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
PROJECT_ROOT = "/mnt/d/projects/SMFG_DataWarehouse"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    # print(f"Added project root: {PROJECT_ROOT}")

from source_code.python.airflow_utility.process_logger import (get_mysql_engine)

# ------------------------------------------------------
# Logger Setup
# ------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def consolidate_chunks_to_mainFile(table_name: str, staging_dir: str) -> str:
    """
    Consolidates all chunked parquet files for a given table into a single file.
    Filters out empty or all-NA DataFrames to avoid concat warnings.
    """
    try:
        chunk_files = sorted(
            [
                os.path.join(staging_dir, f)
                for f in os.listdir(staging_dir)
                if f.startswith(f"{table_name}_part_") and f.endswith(".parquet")
            ]
        )

        if not chunk_files:
            logger.warning(f"No chunk files found for {table_name}")
            return None

        logger.info(f"Consolidating {len(chunk_files)} chunks for table: {table_name}")

        dfs = []
        for file in chunk_files:
            df = pd.read_parquet(file)
            if not df.empty and not df.isna().all(axis=None):
                dfs.append(df)
            else:
                logger.warning(f"Skipping empty or all-NA chunk: {file}")

        if not dfs:
            logger.warning(f"All chunks empty for {table_name}. Skipping consolidation.")
            return None

        final_df = pd.concat(dfs, ignore_index=True)
        final_file_path = os.path.join(staging_dir, f"{table_name}_consolidated.parquet")

        table = pa.Table.from_pandas(final_df)
        pq.write_table(table, final_file_path, compression="snappy")

        # Cleanup intermediate parts
        for f in chunk_files:
            try:
                os.remove(f)
            except Exception as e:
                logger.warning(f"Failed to remove {f}: {e}")

        logger.info(f"Consolidated file created: {final_file_path} ({len(final_df)} rows)")
        return final_file_path

    except Exception as e:
        logger.error(f"Error consolidating {table_name}: {e}", exc_info=True)
        return None


def extract_table_data(engine, table_name: str, query: str, output_directory: str, chunk_size: int = 100000):
    """
    Extracts data from DB in chunks and writes to Parquet files.
    """
    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Extracting table: {table_name}")

        chunk_counter = 0
        for chunk in pd.read_sql(query, con=engine, chunksize=chunk_size):
            chunk_counter += 1
            output_path = os.path.join(output_directory, f"{table_name}_part_{chunk_counter}.parquet")
            chunk.to_parquet(output_path, index=False)
            logger.info(f"Saved chunk {chunk_counter}: {len(chunk)} rows → {output_path}")

        if chunk_counter == 0:
            logger.warning(f"No data extracted for {table_name}")

        logger.info(f"Extraction completed for {table_name} (Total Chunks: {chunk_counter})")
        return True

    except Exception as e:
        logger.error(f"Error extracting table {table_name}: {e}", exc_info=True)
        return False


# def extract_all_tables(cfg_d: dict, metadata_query: str, chunk_size: int, env: str = "DEV"):
#     """
#     Main function: takes config dictionary, reads metadata, extracts tables in chunks, 
#     consolidates parquet, and updates metadata with staging file paths.
#     """
#     extraction_summary = []

#     try:
#         # ---------------------------------------------------
#         # Load Config Details
#         # ---------------------------------------------------
#         env_config_data = cfg_d["ENVIRONMENT"][env]
#         env_config_paths = cfg_d["PATHS"]
#         staging_dir = env_config_paths["STAGING_ZONE"]

#         # ---------------------------------------------------
#         # Create DB Engine
#         # ---------------------------------------------------
#         engine = get_mysql_engine(cfg_d, env, database="utility_staging")

#         # ---------------------------------------------------
#         # Fetch Metadata
#         # ---------------------------------------------------
#         print("Engine type:", type(engine))
#         print("Engine URL:", getattr(engine, "url", None))
#         # print(f"---: Metadata Query - {metadata_query}")
#         # with engine.connect() as conn:
#         #     result = conn.execute(text(metadata_query))
#         #     data = result.fetchall()
#         #     columns = result.keys()
        
#         # df_meta = pd.DataFrame(data, columns=columns)

#         with engine.connect() as conn:
#             df_meta = pd.read_sql(metadata_query, con=conn)
#             logger.info(f"Metadata fetched: {len(df_meta)} rows")
#         logger.info(f"Metadata fetched: {len(df_meta)} rows")

#         # ---------------------------------------------------
#         # Process Each Table
#         # ---------------------------------------------------
#         metadata_query = load_metadata_query(cfg_d)

#         for _, row in df_meta.iterrows():
#             table_name = row["FullTableName"]
#             query = row["DataExtractQuery"].strip()

#             logger.info("=" * 80)
#             logger.info(f"Starting extraction for: {table_name}")
#             logger.info("=" * 80)

#             extraction_ok = extract_table_data(
#                 engine=engine,
#                 table_name=table_name,
#                 query=query,
#                 output_directory=staging_dir,
#                 chunk_size=chunk_size
#             )

#             staging_file_path = None
#             if extraction_ok:
#                 staging_file_path = consolidate_chunks_to_mainFile(table_name, staging_dir)

#             # ---------------------------------------------------
#             # Update Metadata Table
#             # ---------------------------------------------------
#             if staging_file_path:
#                 try:
#                     schema_name, table_only = table_name.split(".", 1)
#                     update_sql = text("""
#                         UPDATE utility_staging.DW_Table_Config 
#                         SET StagingZonePath = :path,
#                             UpdatedAt = NOW()
#                         WHERE SchemaName = :schema
#                           AND TableName = :table
#                     """)
#                     with engine.begin() as conn:
#                         conn.execute(update_sql, {
#                             "path": staging_file_path,
#                             "schema": schema_name,
#                             "table": table_only
#                         })
#                     logger.info(f"Metadata updated for {table_name}")
#                     status = "SUCCESS"
#                 except Exception as ue:
#                     logger.error(f"Failed to update metadata for {table_name}: {ue}", exc_info=True)
#                     status = "META_UPDATE_FAILED"
#             else:
#                 logger.warning(f"Skipping metadata update for {table_name}")
#                 status = "FAILED"

#             extraction_summary.append({
#                 "table": table_name,
#                 "file_path": staging_file_path,
#                 "status": status
#             })

#         # ---------------------------------------------------
#         # Cleanup
#         # ---------------------------------------------------
#         engine.dispose()
#         logger.info("All extractions completed.")

#     except Exception as e:
#         logger.error("Fatal error in extract_all_tables", exc_info=True)
#         extraction_summary.append({"error": str(e)})

#     return extraction_summary



def extract_single_table(cfg_d, env, table_meta, chunk_size):
    """
    Extracts, consolidates, and updates metadata for one table.
    Designed to run as an Airflow mapped task.
    """

    table_name = table_meta["FullTableName"]
    query = table_meta["DataExtractQuery"].strip()

    env_config_paths = cfg_d["PATHS"]
    staging_dir = env_config_paths["STAGING_ZONE"]

    engine = get_mysql_engine(cfg_d, env, database="utility_staging")

    logger.info(f"Starting extraction for: {table_name}")

    extraction_ok = extract_table_data(
        engine=engine,
        table_name=table_name,
        query=query,
        output_directory=staging_dir,
        chunk_size=chunk_size
    )

    staging_file_path = None
    status = "FAILED"

    if extraction_ok:
        staging_file_path = consolidate_chunks_to_mainFile(table_name, staging_dir)
        if staging_file_path:
            try:
                schema_name, table_only = table_name.split(".", 1)
                update_sql = text("""
                    UPDATE utility_staging.DW_Table_Config 
                    SET StagingZonePath = :path,
                        UpdatedAt = NOW()
                    WHERE SchemaName = :schema
                      AND TableName = :table
                """)
                with engine.begin() as conn:
                    conn.execute(update_sql, {
                        "path": staging_file_path,
                        "schema": schema_name,
                        "table": table_only
                    })
                status = "SUCCESS"
            except Exception as ue:
                logger.error(f"Failed to update metadata for {table_name}: {ue}", exc_info=True)
                status = "META_UPDATE_FAILED"

    engine.dispose()
    logger.info(f"✅ Completed table: {table_name} | Status: {status}")
    return {"table": table_name, "status": status, "file_path": staging_file_path}



def load_metadata_query(cfg: dict, env: str = "DEV", key: str = "STAGING_EXTRACT_METADATA_QUERY") -> list[dict]:
    """
    Loads metadata SQL query from file and returns results as JSON (list of dicts).
    """

    try:
        # ---------: 1. Get SQL file path
        query_path = cfg["PATHS"].get(key)
        if not query_path:
            raise KeyError(f"Missing '{key}' in config PATHS section.")
        if not os.path.exists(query_path):
            raise FileNotFoundError(f"Metadata SQL file not found: {query_path}")

        with open(query_path, "r", encoding="utf-8") as f:
            sql_query = f.read().strip()
        if not sql_query:
            raise ValueError(f"Metadata SQL is empty. Check file: {query_path}")

        # ---------: 2. Build MySQL engine from config
        env_cfg = cfg["ENVIRONMENT"].get(env)
        if not env_cfg:
            raise KeyError(f"Missing environment section '{env}' in config.")

        mysql_user = env_cfg["MYSQL_USER"]
        mysql_pass = env_cfg["MYSQL_PASS"]
        mysql_host = env_cfg["MYSQL_HOST"]
        mysql_port = env_cfg["MYSQL_PORT"]
        mysql_db   = env_cfg["MYSQL_DB"]

        conn_str = f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}:{mysql_port}/{mysql_db}"
        engine = create_engine(conn_str)

        # logger.info(f"MySQL engine created for {env}: {mysql_host}/{mysql_db}")

        # ---------: 3. Execute query
        raw_conn = None
        try:
            raw_conn = engine.raw_connection()
            df = pd.read_sql(sql_query, con=raw_conn)
        finally:
            if raw_conn is not None:
                raw_conn.close()

        # ---------: 4. Convert to JSON
        json_data = json.loads(df.to_json(orient="records"))
        logger.info(f"Metadata query executed successfully | Tables for Extraction : {len(json_data)}")

        return json_data

    except Exception as e:
        logger.error(f"Error executing metadata query: {e}", exc_info=True)
        raise


