/* ============================================================
   SEED DATA — mdf_platform_orchestration.elt_table_config
   ICCP Group — one row per unique Source System (test set)

   Source System coverage:
     1 / 101 : Sharepoint Folder  → iccp_income_statement   (excel / excel)
     2 / 102 : Sharepoint List    → pdo_csat_cdo            (sharepoint_list / csv)
     3 / 103 : SAP                → pdo_income_statement_forecast (sap_table / parquet)
     4 / 104 : AWS RDS            → sppi_service_tickets         (rds_table / parquet)

   ID ranges:
     1 –   4 : src2brz rows
   101 – 104 : bronze2silver rows (ref_table_id = src2brz row)

   bronze_file_type:
     src2brz rows  → NULL
     bronze2silver → excel | csv | parquet

   full_refresh_flag: 'Y' for full-replace; 'N' for incremental / SCD2

   To reseed: set @reseed = 1 to DELETE existing rows first.
   ============================================================ */

DECLARE @reseed BIT = 0;

IF @reseed = 1
BEGIN
    DELETE FROM [mdf_platform_orchestration].[elt_table_config]
    WHERE [table_id] IN (1,2,3,4,101,102,103,104);
END;

INSERT INTO [mdf_platform_orchestration].[elt_table_config] (
    [table_id],[datasubject],[classification],[sourcesystem],
    [sourceschema],[sourceschemaname],[sourcetablename],[tablename],
    [job_group],[ingest_channel],[layer],[container],
    [sequence_number],[cycle],[process_id],[ref_table_id],
    [bronze_file_format],[bronze_file_type],
    [file_pattern],[custom_source_path],[custom_table_name],
    [ingest_partition],[ref_ingest_partition],[last_loaded_dt],
    [criteria_columns],[full_refresh_flag]
)
VALUES

-- ══════════════════════════════════════════════════════════════
-- SRC2BRZ ROWS (IDs 1–4)
-- ══════════════════════════════════════════════════════════════

(1,'iccp-finance','restricted','sharepoint','finance',NULL,'ICCP_IS_Dimension','iccp_income_statement',
 NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,
 'excel',NULL,
 'PDO Income Statement Dimension.xlsx',
 'https://iccpgroup.sharepoint.com/sites/ICCPFinancialDataReports/Shared Documents/Financial Statements/PDO/FS Maintenance Files',
 NULL,NULL,NULL,NULL,NULL,'Y'),

-- 2 · Sharepoint List — CSAT survey list → sharepoint_list
(2,'pdo-sales-marketing','confidential','sharepoint','sales',NULL,'CDO Satisfaction Monitoring','pdo_csat_cdo',
 NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,
 'sharepoint_list',NULL,
 NULL,
 'https://iccpgroup.sharepoint.com/sites/PDOCustomerSatisfaction',
 'e58991ff-9ccc-416d-8562-a83b969ef7a0',NULL,NULL,NULL,NULL,'Y'),

-- 3 · SAP — live database table (PDO IS forecast) → sap_table
(3,'pdo-finance','restricted','sap','finance',NULL,'PDO_Income_Statement_Forecast_Final','pdo_income_statement_forecast',
 NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,
 'sap_table',NULL,
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- 4 · AWS RDS — direct table connection → rds_table
(4,'sppi-operations','internal','awsrds','operations',NULL,'Tickets Monitoring','sppi_service_tickets',
 NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,
 'rds_table',NULL,
 NULL,
 NULL,NULL,NULL,NULL,NULL,NULL,'N'),

-- ══════════════════════════════════════════════════════════════
-- brz2sil ROWS (IDs 101–104)
-- ══════════════════════════════════════════════════════════════

-- 101 · iccp_income_statement — Excel → parquet in bronze → silver warehouse table
(101,'iccp-finance','restricted','sharepoint','finance',NULL,'ICCP_IS_Dimension','iccp_income_statement',
 NULL,'ingest_channel=dfp','brz2sil','silver',10,'daily',0,1,
 'excel','excel',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- 102 · pdo_csat_cdo — SharePoint list CSV → silver warehouse table
(102,'pdo-sales-marketing','confidential','sharepoint','sales',NULL,'CDO Satisfaction Monitoring','pdo_csat_cdo',
 NULL,'ingest_channel=dfp','brz2sil','silver',10,'daily',0,2,
 'sharepoint_list','csv',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- 103 · pdo_income_statement_forecast — SAP parquet → silver warehouse table
(103,'pdo-finance','restricted','sap','finance',NULL,'PDO_Income_Statement_Forecast_Final','pdo_income_statement_forecast',
 NULL,'ingest_channel=dfp','brz2sil','silver',10,'daily',0,3,
 'sap_table','parquet',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- 104 · sppi_service_tickets — AWS RDS → silver warehouse table (incremental)
(104,'sppi-operations','internal','awsrds','operations',NULL,'Tickets Monitoring','sppi_service_tickets',
 NULL,'ingest_channel=dfp','brz2sil','silver',10,'daily',0,4,
 'rds_table','parquet',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,'N');
GO
