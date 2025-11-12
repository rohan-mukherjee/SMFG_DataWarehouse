import time
import pymysql
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path
import yaml
import logging


def connect_and_monitor_uat(config_file_path:str = r"D:\projects\SMFG_DataWarehouse\config\config.yaml"):
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

if __name__ == "__main__":
    print("Starting MySQL SSH Tunnel Monitor...")
    print("Press Ctrl+C to stop")
    print("=" * 50)
    
    try:
        connect_and_monitor_uat()
    except KeyboardInterrupt:
        print("\n" + "=" * 50)
        print("Tunnel closed by user")