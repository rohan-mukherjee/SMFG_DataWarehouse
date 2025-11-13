# ======================================================
# Utility: Process Logger
# ======================================================

import pymysql
from sqlalchemy import create_engine, text
from datetime import datetime
from urllib.parse import quote_plus
from typing import Optional
import os, sys, yaml, logging, traceback
 
# ======================================================
# Add Project Root
# ======================================================
PROJECT_ROOT = "/mnt/d/projects/SMFG_DataWarehouse"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)


logger = logging.getLogger("process_logger")


# ======================================================
# Load Config
# ======================================================
def load_config():
            try:
                config_path = os.path.join(PROJECT_ROOT, "config/config.yaml")
                with open(config_path, "r") as f:
                    cfg_d = yaml.safe_load(f)
                # logger.info("YAML Configuration loaded successfully.")
                return cfg_d
            except Exception as e:
                logger.error(f"-----: Error loading YAML config: {e}", exc_info=True)
                raise


# ======================================================
# DB Connection
# ======================================================
def get_mysql_engine(cfg, ENV: Optional[str] = None, database: Optional[str] = None):
    """
    Returns a SQLAlchemy engine for MySQL.
    
    Args:
        cfg (dict): Config dictionary with ENVIRONMENT section
        ENV (str, optional): Environment key (e.g., "DEV", "UAT", "PROD"). 
                            Defaults to "DEV" if None.
        database (str, optional): Database name to connect to.
    
    Example:
        engine = get_mysql_engine(cfg, ENV="UAT", database="financialForms")
    """
    # Default to "DEV" if ENV is None
    env_key = ENV or "DEV"
    
    # Validate environment exists
    if env_key not in cfg["ENVIRONMENT"]:
        raise KeyError(f"Environment '{env_key}' not found in config. Available: {list(cfg['ENVIRONMENT'].keys())}")
    
    env = cfg["ENVIRONMENT"][env_key]
    
    user = quote_plus(env['MYSQL_USER'])
    password = quote_plus(env['MYSQL_PASS'])
    host = env['MYSQL_HOST']
    port = env.get('MYSQL_PORT', 3306)

    base_conn = f"mysql+pymysql://{user}:{password}@{host}:{port}"
    conn_str = f"{base_conn}/{database}" if database else base_conn

    # Optional: Add connection pooling, charset, etc.
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False  # Set True for debugging
    )



# ======================================================
# Create a new process master entry
# ======================================================
def create_process_master(engine, process_type="EOD", current_stage="STAGING_EXTRACT",current_status="RUNNING", created_by="airflow"):
    process_date = datetime.now().date()
    sql = text("""
        INSERT INTO utility_staging.DW_Process_Master
        (ProcessDate, ProcessType, ProcessStartAt, CurrentStage, Status, CreatedBy)
        VALUES (:process_date, :process_type, NOW(), :current_stage, :status, :created_by)
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "process_date": process_date,
            "process_type": process_type,
            "current_stage": current_stage,
            "status": current_status,
            "created_by": created_by
        })
        process_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
    return process_id



# ======================================================
# Insert or update process stage details
# ======================================================
def log_process_stage_detail(engine, process_id, stage_name, table_id=None, table_name=None,
                     row_count=None, status="PROCESSING", error_msg=None, output_path=None, created_by="airflow"):
    """
    Inserts one record per table per stage.
    Use 'PROCESSING' at start, then update later to SUCCESS or FAILED.
    """
    insert_sql = text("""
        INSERT INTO utility_staging.DW_Process_Stage_Detail
        (StageName, ProcessID, TableID, TableName, RowCount, Status, ErrorMessage, OutputPath, CreatedBy)
        VALUES (:stage_name, :process_id, :table_id, :table_name, :row_count, :status, :error_msg, :output_path, :created_by)
    """)
    with engine.begin() as conn:
        conn.execute(insert_sql, {
            "stage_name": stage_name,
            "process_id": process_id,
            "table_id": table_id,
            "table_name": table_name,
            "row_count": row_count,
            "status": status,
            "error_msg": error_msg,
            "output_path": output_path,
            "created_by": created_by
        })
        stage_detail_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
    print(f"🟡 Stage started → {table_name} | ID: {stage_detail_id}")
    return stage_detail_id


def update_process_stage_detail(engine, stage_detail_id, status="SUCCESS", row_count=None, error_msg=None, output_path=None):
    """
    Updates the same stage record after completion or failure.
    """
    sql = text("""
        UPDATE utility_staging.DW_Process_Stage_Detail
        SET Status = :status,
            RowCount = :row_count,
            ErrorMessage = :error_msg,
            OutputPath = :output_path,
            EndTime = NOW()
        WHERE StageDetailID = :stage_detail_id
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "status": status,
            "row_count": row_count,
            "error_msg": error_msg,
            "output_path": output_path,
            "stage_detail_id": stage_detail_id
        })
    print(f"🟢 Stage updated → ID: {stage_detail_id} | {status}")


# ======================================================
# Update process master status
# ======================================================
def update_process_master(engine, process_id, status=None, current_stage = None, remarks=None, error_message=None, process_end_at=None):
    """
    Updates overall process status after stages processed.
    """
    try:
        sql = text("""
            UPDATE utility_staging.DW_Process_Master
            SET 
                Status = :status,
                CurrentStage = :current_stage,
                Remarks = :remarks,
                ProcessEndAt = :process_end_at,
                ErrorMessage = :error_message
            WHERE ProcessID = :process_id
        """)

        with engine.begin() as conn:
            conn.execute(sql, {
                "status": status,
                "current_stage":current_stage,
                "remarks": remarks,
                "process_id": process_id,
                "error_message": error_message,
                "process_end_at": process_end_at
            })

        # logger.info(f"-----: Process Master updated → ProcessID={process_id}, Status={status}")
        

    except Exception as e:
        tb = traceback.format_exc()
        logger.error(
            f"-----: Error updating process master for ProcessID={process_id}: {e}\nTraceback:\n{tb}"
        )
        raise


