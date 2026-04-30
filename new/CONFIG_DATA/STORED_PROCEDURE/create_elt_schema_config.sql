/* ============================================================
   CREATE TABLE  mdf_platform_orchestration.elt_schema_config
   Column mapping for DB sources (sql_table / sap_table / rds_table).
   Used by src2brz_copy_from_sql_to_parquet to build the
   TabularTranslator JSON for the Copy activity, and by
   bronze2silver_transform_nbk Cell 4b to rename and cast cols.
   Fabric Warehouse (T-SQL) — idempotent
   ============================================================ */

IF OBJECT_ID(N'[mdf_platform_orchestration].[elt_schema_config]', 'U') IS NULL
BEGIN
    CREATE TABLE [mdf_platform_orchestration].[elt_schema_config] (

        [table_id]               INT              NOT NULL,   -- FK to elt_table_config.table_id (src2brz row)
        [tableschema]            VARCHAR(100)     NOT NULL,   -- source schema name
        [tablename]              VARCHAR(200)     NOT NULL,   -- source table name
        [source_column_position] INT              NOT NULL,   -- column ordinal; drives Parquet column order
        [source_column_name]     VARCHAR(200)     NOT NULL,
        [source_data_type]       VARCHAR(50)      NOT NULL,   -- SQL Server type: nvarchar | int | datetime2 | decimal …
        [target_column_name]     VARCHAR(200)     NOT NULL,
        [target_data_type]       VARCHAR(50)      NOT NULL,   -- Parquet type: STRING | INT32 | INT64 | INT96 | DECIMAL | BOOLEAN | BYTE_ARRAY

        CONSTRAINT [PK_elt_schema_config] PRIMARY KEY ([table_id], [source_column_position]),
        CONSTRAINT [FK_elt_schema_config_table] FOREIGN KEY ([table_id])
            REFERENCES [mdf_platform_orchestration].[elt_table_config] ([table_id])
    );
END;
GO
