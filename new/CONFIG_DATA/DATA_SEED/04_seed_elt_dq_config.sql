/* ============================================================
   SEED DATA — mdf_platform_orchestration.elt_dq_config
   One example per rule_type × action combination to exercise
   every code branch in bronze2silver_dq_nbk.

   table_id references (bronze2silver rows from 01_seed_elt_table_config):
     33  = application.countries
     36  = application.people
     52  = sales.orders
     54  = warehouse.coldroomtemperatures
   ============================================================ */

DECLARE @reseed BIT = 0;

IF @reseed = 1
    DELETE FROM [mdf_platform_orchestration].[elt_dq_config]
    WHERE [rule_id] IN (1,2,3,4,5,6,7,8,9);

INSERT INTO [mdf_platform_orchestration].[elt_dq_config]
    ([rule_id],[table_id],[column_name],[rule_type],[rule_value],[action],[is_active])
VALUES
-- regex + warn: ISO 3-letter country code format
(1, 33, 'IsoAlpha3Code',         'regex',          '^[A-Z]{3}$',                   'warn',   1),

-- not_null + reject: OrderID must always be present (hard rejection)
(2, 52, 'OrderID',               'not_null',        NULL,                           'reject', 1),

-- not_null + flag: CustomerID missing should flag the row but not block load
(3, 52, 'CustomerID',            'not_null',        NULL,                           'flag',   1),

-- range + warn: temperature sensor readings should be within physical range
(4, 54, 'Temperature',           'range',           'min:-30,max:30',               'warn',   1),

-- range + flag: sensor numbers are 1–100; anything outside flags the row
(5, 54, 'ColdRoomSensorNumber',  'range',           'min:1,max:100',                'flag',   1),

-- allowed_values + warn: continent must be one of the seven standard values
(6, 33, 'Continent',             'allowed_values',  'Africa,Americas,Asia,Europe,Oceania,Antarctica,Australia', 'warn', 1),

-- allowed_values + flag: boolean-like int column must only be 0 or 1
(7, 36, 'IsPermittedToLogon',    'allowed_values',  '0,1',                          'flag',   1),

-- not_null + reject: person FullName is mandatory
(8, 36, 'FullName',              'not_null',        NULL,                           'reject', 1),

-- not_null + reject: temperature sensor PK must never be null
(9, 54, 'ColdRoomTemperatureID', 'not_null',        NULL,                           'reject', 1);
GO
