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