-- DDL for SQL table
CREATE OR REPLACE TABLE {database}.{schema}.deposit_transactions (
    id INTEGER PRIMARY KEY,
    resident_id INTEGER,
    deposit_type_id INTEGER,
    deposit_type_name VARCHAR,
    amount NUMBER(38, 2),
    date DATE,
    refund BOOLEAN,
    community_id INTEGER,
    community_name VARCHAR,
    load_dts TIMESTAMP_TZ
);

-- COPY INTO statement to load data
COPY INTO
    {database}.{schema}.deposit_transactions (
    id,
    resident_id,
    deposit_type_id,
    deposit_type_name,
    amount,
    date,
    refund,
    community_id,
    community_name,
    load_dts
)
FROM (
    SELECT
        $1::INTEGER,                                                            -- id
        $2::INTEGER,                                                            -- resident_id
        $3::INTEGER,                                                            -- deposit_type_id
        IFF($4 = '', NULL, $4::VARCHAR),                                      -- deposit_type_name
        $5::NUMBER(38, 2),                                                      -- amount
        $6::DATE,                                                               -- date
        $7::BOOLEAN,                                                            -- refund
        $8::INTEGER,                                                            -- community_id
        $9::VARCHAR,                                                            -- community_name
        CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP())             -- load_dts
    FROM @{stage_name}/{blob_name}
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format');
