/* ============================================================
   SEED DATA — mdf_platform_orchestration.elt_transform_config
   One example per transform_type to exercise every code branch
   in bronze2silver_transform_nbk Cell 4c.

   table_id references (bronze2silver rows from 01_seed_elt_table_config):
     52  = sales.orders
     54  = warehouse.coldroomtemperatures
     62  = warehouse.vehicletemperatures
   ============================================================ */

DECLARE @reseed BIT = 0;

IF @reseed = 1
    DELETE FROM [mdf_platform_orchestration].[elt_transform_config]
    WHERE [rule_id] IN (1,2,3,4,5,6,7);

INSERT INTO [mdf_platform_orchestration].[elt_transform_config]
    ([rule_id],[table_id],[transform_type],[source_column],[target_column],[rule_value],[sequence_number])
VALUES
-- add_literal_column: stamp every orders row with the source system name
(1, 52, 'add_literal_column', NULL, 'datasource', 'WideWorldImporters', 10),

-- remove_column: drop free-text columns that contain PII before silver load
(2, 52, 'remove_column', NULL, NULL, 'InternalComments,DeliveryInstructions', 20),

-- filter_rows: keep only valid (non-null OrderID) rows
(3, 52, 'filter_rows', NULL, NULL, 'OrderID > 0', 30),

-- dedup: remove duplicate order rows keyed on OrderID
(4, 52, 'dedup', NULL, NULL, 'OrderID', 40),

-- split_datetime: split RecordedWhen into separate date and time columns
(5, 54, 'split_datetime', 'RecordedWhen', NULL, 'RecordedDate,RecordedTime', 10),

-- union_table: append coldroomtemperatures (src2brz id=23) into vehicletemperatures
--   rule_value = the src2brz table_id of the secondary table to read from bronze
(6, 62, 'union_table', NULL, NULL, '23', 10),

-- unpivot: melt VehicleTemperatureID+Registration wide → long sensor readings
(7, 62, 'unpivot', NULL, NULL,
    'id_vars=VehicleTemperatureID,VehicleRegistration;value_vars=Temperature;var_name=SensorType;val_name=Reading',
    20);
GO
