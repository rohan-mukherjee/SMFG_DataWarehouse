
-- =========================================================
-- STEP 1: Process master (one row per DW run)
-- =========================================================
CREATE TABLE IF NOT EXISTS utility_staging.DW_Process_Master (
  ProcessID      INT PRIMARY KEY AUTO_INCREMENT,
  ProcessDate    DATE NOT NULL,                                  -- Logical business date of the run
  ProcessType    ENUM('EOD','HOURLY') NOT NULL,                  
  ProcessStartAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,    
  ProcessEndAt   DATETIME NULL,                                  
  CurrentStage   ENUM('INIT_STAGE','STAGING_EXTRACT','DATA_TRANSFORMATION','DATA_WAREHOUSE_LOAD') NULL,
  Status         ENUM('RUNNING','SUCCESS','FAILED','PARTIAL') NOT NULL DEFAULT 'RUNNING',
  Remarks        VARCHAR(500) NULL,
  ErrorMessage   TEXT NULL,
  CreatedAt      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,   
  UpdatedAt      DATETIME NULL ON UPDATE CURRENT_TIMESTAMP,      
  CreatedBy      VARCHAR(50) NULL                                
) ENGINE=InnoDB;

-- Required indexes
CREATE INDEX idx_dpm_processdate         ON utility_staging.DW_Process_Master (ProcessDate);
CREATE INDEX idx_dpm_processtype_status  ON utility_staging.DW_Process_Master (ProcessType, Status);
CREATE INDEX idx_dpm_status              ON utility_staging.DW_Process_Master (Status);


-- =========================================================
-- STEP 2: Process stage detail (one row per table per stage)
-- =========================================================
CREATE TABLE IF NOT EXISTS utility_staging.DW_Process_Stage_Detail (
  StageDetailID  INT PRIMARY KEY AUTO_INCREMENT,
  StageName      ENUM('STAGING_EXTRACT','TRANSFORMATION','DATA_WAREHOUSE_LOAD') NOT NULL,
  ProcessID      INT NOT NULL,                                   -- FK → master
  TableID        INT NULL,                                       -- FK → table config (nullable for non-table stage ops)
  TableName      VARCHAR(100) NULL,
  StartTime      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  EndTime        DATETIME NULL,
  RowCount       BIGINT NULL,
  Status         ENUM('SUCCESS','FAILED') NOT NULL,
  ErrorMessage   TEXT NULL,                                      -- Only when failed
  OutputPath     VARCHAR(255) NULL,                              -- File path or target table
  CreatedAt      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CreatedBy      VARCHAR(50) NULL,
  CONSTRAINT fk_psd_process
    FOREIGN KEY (ProcessID) REFERENCES utility_staging.DW_Process_Master (ProcessID),
  CONSTRAINT fk_psd_tableid
    FOREIGN KEY (TableID) REFERENCES utility_staging.DW_Table_Config (TableID)
) ENGINE=InnoDB;

-- Required indexes
CREATE INDEX idx_psd_process            ON utility_staging.DW_Process_Stage_Detail (ProcessID);
CREATE INDEX idx_psd_process_stage      ON utility_staging.DW_Process_Stage_Detail (ProcessID, StageName);
CREATE INDEX idx_psd_table              ON utility_staging.DW_Process_Stage_Detail (TableID);
CREATE INDEX idx_psd_status             ON utility_staging.DW_Process_Stage_Detail (Status);



-- =========================================================
-- STEP 3: Table config (which source tables to move and how)
-- =========================================================
CREATE TABLE IF NOT EXISTS utility_staging.DW_Table_Config (
  TableID             INT PRIMARY KEY AUTO_INCREMENT,
  SchemaName          VARCHAR(100) NOT NULL,
  TableName           VARCHAR(100) NOT NULL,
  ActiveFlag          BOOLEAN NOT NULL DEFAULT 1,
  LoadType            ENUM('FULL','INCREMENTAL') NOT NULL,
  RefreshFrequency    ENUM('EOD','HOURLY','REALTIME') NOT NULL,
  IncrementalFilter   TEXT NULL,
  BatchSize           INT NOT NULL DEFAULT 0,
  Comments            VARCHAR(255) NULL,
  StagingZonePath 	  TEXT NULL,
  CuratedZonePath     TEXT NULL,
  CreatedAt           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt           DATETIME NULL ON UPDATE CURRENT_TIMESTAMP,
  CreatedBy           VARCHAR(50) NULL,
  CONSTRAINT uq_dwtc_unique_table UNIQUE (SchemaName, TableName)
) ENGINE=InnoDB;

-- Required indexes
CREATE INDEX idx_dwtc_active            ON utility_staging.DW_Table_Config (ActiveFlag);
CREATE INDEX idx_dwtc_loadtype          ON utility_staging.DW_Table_Config (LoadType);
CREATE INDEX idx_dwtc_refreshfreq       ON utility_staging.DW_Table_Config (RefreshFrequency);



-- =========================================================
-- STEP 4: Column config (source columns + per-column transform)
-- =========================================================
CREATE TABLE IF NOT EXISTS utility_staging.DW_Column_Config (
  ColumnID               INT PRIMARY KEY AUTO_INCREMENT,
  TableID                INT NOT NULL,                           -- FK → table config
  ColumnName             VARCHAR(100) NOT NULL,
  AliasName              VARCHAR(100) NULL,
  IncludeFlag            BOOLEAN NOT NULL DEFAULT 1,
  TransformationLogic    TEXT NULL,
  CreatedAt              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt              DATETIME NULL ON UPDATE CURRENT_TIMESTAMP,
  CreatedBy              VARCHAR(50) NULL,
  CONSTRAINT fk_dwcc_tableid
    FOREIGN KEY (TableID) REFERENCES utility_staging.DW_Table_Config (TableID)
) ENGINE=InnoDB;

-- Required indexes
CREATE INDEX idx_dwcc_table_include      ON utility_staging.DW_Column_Config (TableID, IncludeFlag);



-- =========================================================
-- STEP 4: Target DW table config
-- =========================================================
CREATE TABLE IF NOT EXISTS utility_staging.DW_Output_Table_Config (
  OutputTableID    INT PRIMARY KEY AUTO_INCREMENT,
  TargetSchema     VARCHAR(100) NOT NULL,
  TargetTable      VARCHAR(100) NOT NULL,
  CreatedAt        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt        DATETIME NULL ON UPDATE CURRENT_TIMESTAMP,
  CreatedBy        VARCHAR(50) NULL,
  CONSTRAINT uq_dot_unique UNIQUE (TargetSchema, TargetTable)
) ENGINE=InnoDB;


-- =========================================================
-- STEP 5: Target DW column config -> mapping from source (DW_Column_Config) to target DW table & position
-- =========================================================
CREATE TABLE IF NOT EXISTS utility_staging.DW_Output_Column_Config (
  OutputColumnID       INT PRIMARY KEY AUTO_INCREMENT,
  OutputTableID        INT NOT NULL,                             -- FK → DW_Output_Table
  SourceTableID        INT NOT NULL,                             -- FK → DW_Table_Config
  SourceColumnID       INT NOT NULL,                             -- FK → DW_Column_Config
  TargetColumnName     VARCHAR(100) NOT NULL,                    -- Final column name in DW
  TargetDataType       VARCHAR(100) NOT NULL,                    -- e.g., 'DECIMAL(18,2)', 'TIMESTAMP', 'VARCHAR(100)'
  TargetPosition       INT NOT NULL,                             -- **Controls ordering**
  AdditionalTransform  TEXT NULL,                                -- Optional extra transform at mapping stage
  IsKey                BOOLEAN NOT NULL DEFAULT 0,               -- Used for MERGE keys
  CreatedAt            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt            DATETIME NULL ON UPDATE CURRENT_TIMESTAMP,
  CreatedBy            VARCHAR(50) NULL,
  CONSTRAINT fk_dcm_outtable   FOREIGN KEY (OutputTableID)  REFERENCES utility_staging.DW_Output_Table_Config (OutputTableID),
  CONSTRAINT fk_dcm_srctbl     FOREIGN KEY (SourceTableID)  REFERENCES utility_staging.DW_Table_Config  (TableID),
  CONSTRAINT fk_dcm_srccol     FOREIGN KEY (SourceColumnID) REFERENCES utility_staging.DW_Column_Config (ColumnID),
  CONSTRAINT uq_dcm_targetcol UNIQUE (OutputTableID, TargetColumnName),
  CONSTRAINT uq_dcm_targetpos UNIQUE (OutputTableID, TargetPosition)
) ENGINE=InnoDB;

-- Required indexes
CREATE INDEX idx_dcm_outtable_pos   ON utility_staging.DW_Output_Column_Config (OutputTableID, TargetPosition);
CREATE INDEX idx_dcm_keys           ON utility_staging.DW_Output_Column_Config (OutputTableID, IsKey);



SET SESSION group_concat_max_len = 1000000; -- Change this for group_concat fix

