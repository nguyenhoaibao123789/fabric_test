/* ============================================================
   SEED DATA — mdf_platform_orchestration.elt_table_config
   Wide World Importers (WWI) SQL Server source + format examples

   Table ID ranges:
     1 – 31  : src2brz rows for 31 WWI tables (sqlvm source)
    32 – 62  : bronze2silver rows (b2s_id = src2brz_id + 31)
    70 – 72  : src2brz rows for file_as_is / sharepoint_list / sap_table
   101 – 103 : bronze2silver rows for the three format examples

   Sequence numbers (src2brz layer):
     10 = reference / lookup tables (no FK deps)
     20 = transactional tables with FK deps on seq-10 tables
     30 = tables with FK deps on seq-20 tables

   bronze_file_type:
     src2brz rows  → NULL (DB→Parquet copy; no pandas reader needed)
     bronze2silver → parquet | csv | excel | json (pandas reader format)

   full_refresh_flag: 'Y' for reference tables; 'N' for SCD2 tables

   To reseed: set @reseed = 1 to DELETE existing rows first.
   ============================================================ */

DECLARE @reseed BIT = 0;

IF @reseed = 1
BEGIN
    DELETE FROM [mdf_platform_orchestration].[elt_table_config]
    WHERE [table_id] IN (
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,
        32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,
        70,71,72,101,102,103
    );
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
-- SRC2BRZ ROWS (IDs 1–31)  — sqlvm / parquet_none_header
-- ══════════════════════════════════════════════════════════════

-- application schema (IDs 1–8, sequence 10 = load first)
(1,'application','restricted','sqlvm','application','application','cities','cities',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(2,'application','restricted','sqlvm','application','application','countries','countries',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(3,'application','restricted','sqlvm','application','application','deliverymethods','deliverymethods',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(4,'application','restricted','sqlvm','application','application','paymentmethods','paymentmethods',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(5,'application','confidential','sqlvm','application','application','people','people',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(6,'application','restricted','sqlvm','application','application','stateprovinces','stateprovinces',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(7,'application','restricted','sqlvm','application','application','systemparameters','systemparameters',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(8,'application','restricted','sqlvm','application','application','transactiontypes','transactiontypes',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- purchasing schema (ID 11 = lookup seq 10; IDs 9,10,12,13 = transactional seq 20/30)
(9,'purchasing','confidential','sqlvm','purchasing','purchasing','purchaseorderlines','purchaseorderlines',NULL,'ingest_channel=dfp','src2brz','bronze',20,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(10,'purchasing','confidential','sqlvm','purchasing','purchasing','purchaseorders','purchaseorders',NULL,'ingest_channel=dfp','src2brz','bronze',20,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(11,'purchasing','restricted','sqlvm','purchasing','purchasing','suppliercategories','suppliercategories',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(12,'purchasing','confidential','sqlvm','purchasing','purchasing','suppliers','suppliers',NULL,'ingest_channel=dfp','src2brz','bronze',20,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(13,'purchasing','confidential','sqlvm','purchasing','purchasing','suppliertransactions','suppliertransactions',NULL,'ingest_channel=dfp','src2brz','bronze',30,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- sales schema (IDs 14,15 = lookup seq 10; IDs 16–22 = transactional seq 20/30)
(14,'sales','restricted','sqlvm','sales','sales','buyinggroups','buyinggroups',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(15,'sales','restricted','sqlvm','sales','sales','customercategories','customercategories',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(16,'sales','confidential','sqlvm','sales','sales','customers','customers',NULL,'ingest_channel=dfp','src2brz','bronze',20,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(17,'sales','confidential','sqlvm','sales','sales','customertransactions','customertransactions',NULL,'ingest_channel=dfp','src2brz','bronze',30,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(18,'sales','confidential','sqlvm','sales','sales','invoicelines','invoicelines',NULL,'ingest_channel=dfp','src2brz','bronze',30,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(19,'sales','confidential','sqlvm','sales','sales','invoices','invoices',NULL,'ingest_channel=dfp','src2brz','bronze',30,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(20,'sales','restricted','sqlvm','sales','sales','orderlines','orderlines',NULL,'ingest_channel=dfp','src2brz','bronze',30,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(21,'sales','restricted','sqlvm','sales','sales','orders','orders',NULL,'ingest_channel=dfp','src2brz','bronze',20,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(22,'sales','restricted','sqlvm','sales','sales','specialdeals','specialdeals',NULL,'ingest_channel=dfp','src2brz','bronze',20,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- warehouse schema (IDs 23–26 = lookup seq 10; IDs 27–31 = transactional/time-series seq 20)
(23,'warehouse','restricted','sqlvm','warehouse','warehouse','coldroomtemperatures','coldroomtemperatures',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(24,'warehouse','restricted','sqlvm','warehouse','warehouse','colors','colors',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(25,'warehouse','restricted','sqlvm','warehouse','warehouse','packagetypes','packagetypes',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(26,'warehouse','restricted','sqlvm','warehouse','warehouse','stockgroups','stockgroups',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(27,'warehouse','restricted','sqlvm','warehouse','warehouse','stockitemholdings','stockitemholdings',NULL,'ingest_channel=dfp','src2brz','bronze',20,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(28,'warehouse','restricted','sqlvm','warehouse','warehouse','stockitems','stockitems',NULL,'ingest_channel=dfp','src2brz','bronze',20,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(29,'warehouse','restricted','sqlvm','warehouse','warehouse','stockitemstockgroups','stockitemstockgroups',NULL,'ingest_channel=dfp','src2brz','bronze',20,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(30,'warehouse','restricted','sqlvm','warehouse','warehouse','stockitemtransactions','stockitemtransactions',NULL,'ingest_channel=dfp','src2brz','bronze',20,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(31,'warehouse','restricted','sqlvm','warehouse','warehouse','vehicletemperatures','vehicletemperatures',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'parquet_none_header',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- ══════════════════════════════════════════════════════════════
-- BRONZE2SILVER ROWS (IDs 32–62, b2s_id = src2brz_id + 31)
-- ref_table_id points back to the src2brz row
-- full_refresh_flag: 'Y' for reference tables, 'N' for SCD2
-- ══════════════════════════════════════════════════════════════

-- application b2s (reference tables → full refresh, no SCD2)
(32,'application','restricted','sqlvm','application','application','cities','cities',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,1,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(33,'application','restricted','sqlvm','application','application','countries','countries',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,2,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(34,'application','restricted','sqlvm','application','application','deliverymethods','deliverymethods',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,3,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(35,'application','restricted','sqlvm','application','application','paymentmethods','paymentmethods',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,4,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(36,'application','confidential','sqlvm','application','application','people','people',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,5,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'PersonID','N'),
(37,'application','restricted','sqlvm','application','application','stateprovinces','stateprovinces',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,6,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(38,'application','restricted','sqlvm','application','application','systemparameters','systemparameters',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,7,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(39,'application','restricted','sqlvm','application','application','transactiontypes','transactiontypes',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,8,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- purchasing b2s
(40,'purchasing','confidential','sqlvm','purchasing','purchasing','purchaseorderlines','purchaseorderlines',NULL,'ingest_channel=dfp','bronze2silver','silver',20,'daily',0,9,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'PurchaseOrderLineID','N'),
(41,'purchasing','confidential','sqlvm','purchasing','purchasing','purchaseorders','purchaseorders',NULL,'ingest_channel=dfp','bronze2silver','silver',20,'daily',0,10,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'PurchaseOrderID','N'),
(42,'purchasing','restricted','sqlvm','purchasing','purchasing','suppliercategories','suppliercategories',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,11,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(43,'purchasing','confidential','sqlvm','purchasing','purchasing','suppliers','suppliers',NULL,'ingest_channel=dfp','bronze2silver','silver',20,'daily',0,12,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'SupplierID','N'),
(44,'purchasing','confidential','sqlvm','purchasing','purchasing','suppliertransactions','suppliertransactions',NULL,'ingest_channel=dfp','bronze2silver','silver',30,'daily',0,13,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'SupplierTransactionID','N'),

-- sales b2s
(45,'sales','restricted','sqlvm','sales','sales','buyinggroups','buyinggroups',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,14,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(46,'sales','restricted','sqlvm','sales','sales','customercategories','customercategories',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,15,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(47,'sales','confidential','sqlvm','sales','sales','customers','customers',NULL,'ingest_channel=dfp','bronze2silver','silver',20,'daily',0,16,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'CustomerID','N'),
(48,'sales','confidential','sqlvm','sales','sales','customertransactions','customertransactions',NULL,'ingest_channel=dfp','bronze2silver','silver',30,'daily',0,17,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'CustomerTransactionID','N'),
(49,'sales','confidential','sqlvm','sales','sales','invoicelines','invoicelines',NULL,'ingest_channel=dfp','bronze2silver','silver',30,'daily',0,18,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'InvoiceLineID','N'),
(50,'sales','confidential','sqlvm','sales','sales','invoices','invoices',NULL,'ingest_channel=dfp','bronze2silver','silver',30,'daily',0,19,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'InvoiceID','N'),
(51,'sales','restricted','sqlvm','sales','sales','orderlines','orderlines',NULL,'ingest_channel=dfp','bronze2silver','silver',30,'daily',0,20,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'OrderLineID','N'),
(52,'sales','restricted','sqlvm','sales','sales','orders','orders',NULL,'ingest_channel=dfp','bronze2silver','silver',20,'daily',0,21,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'OrderID','N'),
(53,'sales','restricted','sqlvm','sales','sales','specialdeals','specialdeals',NULL,'ingest_channel=dfp','bronze2silver','silver',20,'daily',0,22,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'SpecialDealID','N'),

-- warehouse b2s
(54,'warehouse','restricted','sqlvm','warehouse','warehouse','coldroomtemperatures','coldroomtemperatures',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,23,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(55,'warehouse','restricted','sqlvm','warehouse','warehouse','colors','colors',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,24,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(56,'warehouse','restricted','sqlvm','warehouse','warehouse','packagetypes','packagetypes',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,25,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(57,'warehouse','restricted','sqlvm','warehouse','warehouse','stockgroups','stockgroups',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,26,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(58,'warehouse','restricted','sqlvm','warehouse','warehouse','stockitemholdings','stockitemholdings',NULL,'ingest_channel=dfp','bronze2silver','silver',20,'daily',0,27,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'StockItemID','N'),
(59,'warehouse','restricted','sqlvm','warehouse','warehouse','stockitems','stockitems',NULL,'ingest_channel=dfp','bronze2silver','silver',20,'daily',0,28,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'StockItemID','N'),
(60,'warehouse','restricted','sqlvm','warehouse','warehouse','stockitemstockgroups','stockitemstockgroups',NULL,'ingest_channel=dfp','bronze2silver','silver',20,'daily',0,29,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'StockItemStockGroupID','N'),
(61,'warehouse','restricted','sqlvm','warehouse','warehouse','stockitemtransactions','stockitemtransactions',NULL,'ingest_channel=dfp','bronze2silver','silver',20,'daily',0,30,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'StockItemTransactionID','N'),
(62,'warehouse','restricted','sqlvm','warehouse','warehouse','vehicletemperatures','vehicletemperatures',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,31,'parquet_none_header','parquet',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- ══════════════════════════════════════════════════════════════
-- FORMAT EXAMPLES — one row per remaining bronze_file_format
-- ══════════════════════════════════════════════════════════════

-- file_as_is: SharePoint document library (e.g. Excel report dropped in a doc lib)
(70,'financial','confidential','sharepoint','financial',NULL,'FinancialReport*.xlsx','financial_reports',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'file_as_is',NULL,'FinancialReport*.xlsx','https://company.sharepoint.com/sites/Finance/Shared Documents',NULL,NULL,NULL,NULL,NULL,'Y'),

-- sharepoint_list: SharePoint list (exported as CSV by the copy activity)
(71,'application','restricted','sharepoint','application',NULL,'countries_list','countries_list',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'sharepoint_list',NULL,NULL,'https://company.sharepoint.com/sites/Application','{00000000-0000-0000-0000-000000000001}',NULL,NULL,NULL,NULL,'Y'),

-- sap_table: SAP BW/ECC table extracted via SAP connector
(72,'financial','confidential','sap','financial',NULL,'BKPF','bkpf',NULL,'ingest_channel=dfp','src2brz','bronze',10,'daily',0,NULL,'sap_table',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),

-- bronze2silver pairs for the format examples above
(101,'financial','confidential','sharepoint','financial',NULL,'FinancialReport*.xlsx','financial_reports',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,70,'file_as_is','excel',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(102,'application','restricted','sharepoint','application',NULL,'countries_list','countries_list',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,71,'sharepoint_list','csv',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'Y'),
(103,'financial','confidential','sap','financial',NULL,'BKPF','bkpf',NULL,'ingest_channel=dfp','bronze2silver','silver',10,'daily',0,72,'sap_table','parquet',NULL,NULL,NULL,NULL,NULL,NULL,'BKPF_PK','N');
GO
