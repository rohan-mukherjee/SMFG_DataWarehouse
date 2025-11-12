import time
from sqlalchemy import create_engine, text
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path
import yaml
import logging
import pandas as pd
import pymysql
import traceback
import pyarrow.parquet as pq
import pyarrow as pa


# def uat_connection_test(config_file_path:str = r"D:\projects\SMFG_DataWarehouse\config\config.yaml"):
def uat_connection_test(config_file_path:str):
    """
    UAT Connection test
    """
    try:
            with open(config_file_path, "r") as f:
                cfg_d = yaml.safe_load(f)
            
            env_config_data = cfg_d["ENVIRONMENT"]
            # paths_config_data = cfg_d["PATHS"]

            # print(env_config_data["DEV"])
            # print(paths_config_data["STAGING_EXTRACT_PATH"])

            connection = pymysql.connect(
                host=env_config_data["DEV"]["MYSQL_HOST"]
                ,port=env_config_data["DEV"]["MYSQL_PORT"]
                ,user=env_config_data["DEV"]["MYSQL_USER"]
                ,password=env_config_data["DEV"]["MYSQL_PASS"]
                # ,database=env_config_data["DEV"]["MYSQL_DB"]
            )
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] MySQL connection established!")
            print("-" * 50)
            
            # Keep the tunnel open and print status every minute
            counter = 0
            while True:
                counter += 1
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Test the connection with a simple query
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        result = cursor.fetchone()
                    
                    print(f"[{current_time}] Tunnel is ACTIVE - Check #{counter}")
                    print(f"  └─ MySQL responsive: {result[0] == 1}")
                    
                except Exception as e:
                    print(f"[{current_time}] Error querying database: {e}")
                
                # Wait for 1 minute
                time.sleep(15)
    
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: {e}")
        print("Connection failed!")



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
            print(f"⚠️ No chunk files found for {table_name}")
            return None

        print(f"🔄 Consolidating {len(chunk_files)} chunks for table: {table_name}")

        dfs = []
        for file in chunk_files:
            df = pd.read_parquet(file)
            # ✅ filter out empty or all-NA chunks
            if not df.empty and not df.isna().all(axis=None):
                dfs.append(df)
            else:
                print(f"⚠️ Skipping empty or all-NA chunk: {file}")

        if not dfs:
            print(f"⚠️ All chunks empty for {table_name}. Skipping consolidation.")
            return None

        final_df = pd.concat(dfs, ignore_index=True)

        final_file_path = os.path.join(staging_dir, f"{table_name}_consolidated.parquet")

        table = pa.Table.from_pandas(final_df)
        pq.write_table(table, final_file_path, compression="snappy")

        for f in chunk_files:
            os.remove(f)

        print(f"✅ Consolidated file created: {final_file_path} ({len(final_df)} rows)")
        return final_file_path

    except Exception as e:
        print(f"❌ ERROR consolidating {table_name}: {e}")
        traceback.print_exc()
        return None



def extract_table_data(engine, table_name, query, output_directory, chunk_size=100000):
    """
    Chunk-based extraction from a SQL table to Parquet files.
    Each chunk is saved separately for safe incremental loading.
    """
    try:
        os.makedirs(output_directory, exist_ok=True)
        print(f"Extracting Table: {table_name}")
        print("-" * 60)

        chunk_counter = 0

        for chunk in pd.read_sql(query, con=engine, chunksize=chunk_size):
            chunk_counter += 1
            print(f"Loaded {len(chunk)} rows (Chunk {chunk_counter})")

            # build chunk filename
            output_path = os.path.join(output_directory, f"{table_name}_part_{chunk_counter}.parquet")

            # write chunk to parquet
            chunk.to_parquet(output_path, index=False)
            print(f"Saved: {output_path}")

        print(f"Extraction completed for {table_name} - Total Chunks: {chunk_counter}")

    except Exception as e:
        print("❌ ERROR:", e)
        traceback.print_exc()

    


def extract_all_tables(config_file_path: str, metadata_query: str, chunk_size: int):
    """
    Reads metadata-driven extraction SQL query, sequentially extracts each table,
    consolidates chunked files, and updates StagingZonePath in DW_Table_Config.
    """
    try:
        # ---------------------------------------------------
        # 1️⃣ Load Config
        # ---------------------------------------------------
        with open(config_file_path, "r") as f:
            cfg_d = yaml.safe_load(f)
        
        env_config_data = cfg_d["ENVIRONMENT"]["DEV"]
        env_config_paths = cfg_d["PATHS"]
        staging_dir = env_config_paths["STAGING_ZONE"]

        # ---------------------------------------------------
        # 2️⃣ Create MySQL Connection Engine
        # ---------------------------------------------------
        engine = create_engine(
            f"mysql+pymysql://{env_config_data['MYSQL_USER']}:{env_config_data['MYSQL_PASS']}@"
            f"{env_config_data['MYSQL_HOST']}:{env_config_data['MYSQL_PORT']}/{env_config_data['MYSQL_DB']}",
            pool_pre_ping=True
        )

        # ---------------------------------------------------
        # 3️⃣ Get Metadata-driven Extraction Queries
        # ---------------------------------------------------
        df_metadata_dynamic_query = pd.read_sql(metadata_query, engine)

        # ---------------------------------------------------
        # 4️⃣ Sequential Extraction → Consolidation → Metadata Update
        # ---------------------------------------------------
        for _, row in df_metadata_dynamic_query.iterrows():
            table_name = row["FullTableName"]
            query = row["DataExtractQuery"].strip()

            print("\n" + "=" * 80)
            print(f"🚀 Starting extraction for: {table_name}")
            print("=" * 80)

            # Extract in chunks
            extract_table_data(
                engine=engine,
                table_name=table_name,
                query=query,
                output_directory=staging_dir,
                chunk_size=chunk_size
            )

            # Consolidate all chunked files → single parquet file
            staging_file_path = consolidate_chunks_to_mainFile(
                table_name=table_name,
                staging_dir=staging_dir
            )

            if staging_file_path:
                print(f"✅ Staging File Path For Table - {table_name} : {staging_file_path}")

                # ---------------------------------------------------
                # 5️⃣ Update Metadata Table with StagingZonePath
                # ---------------------------------------------------
                try:
                    # Extract schema/table for metadata update
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

                    print(f"📝 Metadata updated for {table_name}")

                except Exception as ue:
                    print(f"⚠️ Failed to update metadata for {table_name}: {ue}")
            else:
                print(f"⚠️ Skipping metadata update for {table_name} (no file generated)")

        # ---------------------------------------------------
        # 6️⃣ Close Engine
        # ---------------------------------------------------
        engine.dispose()
        print("\n🏁 All extractions & metadata updates completed successfully.")

    except Exception as e:
        print("❌ ERROR in extract_all_tables:", e)
        traceback.print_exc()

    

if __name__ == "__main__":
    print("=" * 50)
    
    try:
        # uat_connection_test()
        # uat_connection_test(config_file_path = r"D:\projects\SMFG_DataWarehouse\config\config.yaml")
        extract_all_tables(config_file_path = r"D:\projects\SMFG_DataWarehouse\config\config.yaml",
                            chunk_size = 10,
                           metadata_query = """
                                                WITH tableDetail AS (
                                                    SELECT 
                                                        t.TableID,
                                                        t.SchemaName,
                                                        t.TableName,
                                                        t.LoadType,
                                                        t.IncrementalFilter
                                                    FROM utility_staging.DW_Table_Config t
                                                    WHERE t.ActiveFlag = 1
                                                ),
                                                columnDetail AS (
                                                    SELECT 
                                                        t.TableID,
                                                        t.SchemaName,
                                                        t.TableName,
                                                        c.ColumnID,
                                                        c.ColumnName,
                                                        c.AliasName,
                                                        c.TransformationLogic,
                                                        CONCAT(
                                                            CASE 
                                                                WHEN c.TransformationLogic IS NULL OR TRIM(c.TransformationLogic) = '' 
                                                                    THEN c.ColumnName 
                                                                ELSE c.TransformationLogic 
                                                            END,
                                                            ' AS ', IF(c.AliasName IS NULL,c.ColumnName,c.AliasName)
                                                        ) AS modifiedColumns
                                                    FROM tableDetail t
                                                    INNER JOIN utility_staging.DW_Column_Config c
                                                        ON t.TableID = c.TableID 
                                                    WHERE c.IncludeFlag = 1
                                                ),
                                                finalColumnList AS (
                                                    SELECT 
                                                        cd.TableID,
                                                        CONCAT(cd.SchemaName, '.', cd.TableName) AS FullTableName,
                                                        GROUP_CONCAT(cd.modifiedColumns ORDER BY cd.ColumnID SEPARATOR ', ') AS FinalColumns
                                                    FROM columnDetail cd
                                                    GROUP BY cd.TableID, CONCAT(cd.SchemaName, '.', cd.TableName)
                                                ),
                                                extractionQuery AS (
                                                    SELECT 
                                                        f.TableID,
                                                        f.FullTableName,
                                                        CONCAT(
                                                            'SELECT ', f.FinalColumns, 
                                                            ' FROM ', f.FullTableName,
                                                            CASE 
                                                                WHEN td.LoadType = 'INCREMENTAL' AND td.IncrementalFilter IS NOT NULL 
                                                                    THEN CONCAT(' WHERE ', td.IncrementalFilter)
                                                                ELSE ''
                                                            END
                                                        ) AS DataExtractQuery
                                                    FROM finalColumnList f
                                                    INNER JOIN tableDetail td ON f.TableID = td.TableID
                                                )
                                                SELECT * FROM extractionQuery;
                                                """
                            )
    except KeyboardInterrupt:
        print("\n" + "=" * 50)



"""
Development Completed :-
    1. From extract_all_tables function calling extract_table_data & it's printing inputs.
    2. From extract_table_data, extracting tables and saving in chuncks as a praquet file.
    3. For large files, there will be multiple chuncks in form of paraquet files. Combine table wise all files & kept a single file table wise, then remove remainings.
    4. Once success add staging file path details in - DW_Process_Stage_Detail.OutputPath column, along with entry in same table.
    5. Add another table config details in DW_Table_Config & DW_Column_Config for sequence extraction.
    6. Develop sequentional extration parameter passing in extract_all_tables
    7. Staging Extraction Completed

Next Development :-
    1. 
    2. If failed then log error message in same table ErrorMessage column.
    2. Commit in git and develop DAG to run in airflow.(Change paths to linux for future teleport ease in config yaml)

Future Changes :- 
    1. Test same in UAT-SMHFC once Airflow added in server
    2. Metadata config in output tables
    3. Transformation using metadata config to final output path & save in CURATED_ZONE
    4. Table creation in datawarehouse schema for final output.
"""