

CREATE OR ALTER   PROCEDURE [mdf_platform_orchestration].[update_load_status]
@table_id VARCHAR(200)
, @layer VARCHAR(200)
, @pipeline_id VARCHAR(200)
, @pipeline_name VARCHAR(200)
, @pipeline_run_id VARCHAR(200)
, @pipeline_trigger_id VARCHAR(200)
, @pipeline_trigger_time VARCHAR(200)
, @pipeline_trigger_type VARCHAR (200)
, @last_ingest_partition VARCHAR(200)
AS
BEGIN

	BEGIN TRY

		IF @layer = 'src2brz'
			BEGIN
				UPDATE		[mdf_platform_orchestration].[elt_table_config] 
				SET			ingest_partition = @last_ingest_partition
				WHERE		table_id = @table_id
			END
		ELSE IF @layer = 'bronze2silver'
			BEGIN
				UPDATE		[mdf_platform_orchestration].[elt_table_config] 
				SET			ingest_partition = @last_ingest_partition
							, last_loaded_dt = CAST( ( left(@last_ingest_partition,10) + ' ' + right(@last_ingest_partition,2)+ ':00:00' ) AS DATETIME2 )
				WHERE		table_id = @table_id
			END
	END TRY

	BEGIN CATCH

	IF ERROR_NUMBER() = 24556

	BEGIN

		IF @layer = 'src2brz'
			BEGIN
				UPDATE		[mdf_platform_orchestration].[elt_table_config] 
				SET			ingest_partition = @last_ingest_partition
				WHERE		table_id = @table_id
			END
		ELSE IF @layer = 'bronze2silver'
			BEGIN
				UPDATE		[mdf_platform_orchestration].[elt_table_config] 
				SET			ingest_partition = @last_ingest_partition
							, last_loaded_dt = CAST( ( left(@last_ingest_partition,10) + ' ' + right(@last_ingest_partition,2)+ ':00:00' ) AS DATETIME2 )
				WHERE		table_id = @table_id
			END

	END
	ELSE
		THROW
	END CATCH


END;