-- DDL for SQL table
CREATE OR REPLACE TABLE {database}.{schema}.activities (
    id INTEGER PRIMARY KEY,
    import_id INTEGER,
    community_id INTEGER,
    record_id INTEGER,
    record_type VARCHAR,
    activity_type_id INTEGER,
    sales_counselor_id INTEGER,
    stage_id INTEGER,
    activity_type_name VARCHAR,
    activity_result_name VARCHAR,
    scheduled_at TIMESTAMP_TZ,
    completed_at TIMESTAMP_TZ,
    discarded_at TIMESTAMP_TZ,
    created_at TIMESTAMP_TZ,
    auto_perform VARCHAR,
    direction VARCHAR,
    stage_name VARCHAR,
    notes TEXT,
    assigned_to_name VARCHAR,
    created_by_name VARCHAR,
    first_completed_of_activity_type VARCHAR,
    community_name VARCHAR,
    load_dts TIMESTAMP_TZ
);

-- COPY INTO statement to load data
COPY INTO
    {database}.{schema}.activities (
    id,
    import_id,
    community_id,
    record_id,
    record_type,
    activity_type_id,
    sales_counselor_id,
    stage_id,
    activity_type_name,
    activity_result_name,
    scheduled_at,
    completed_at,
    discarded_at,
    created_at,
    auto_perform,
    direction,
    stage_name,
    notes,
    assigned_to_name,
    created_by_name,
    first_completed_of_activity_type,
    community_name,
    load_dts
)
FROM (
    SELECT
        $1::INTEGER AS id,
        IFF($2 = '', NULL, $2::INTEGER) AS import_id,
        $3::INTEGER AS community_id,
        $4::INTEGER AS record_id,
        IFF($5 = '', NULL, $5::VARCHAR) AS record_type,
        IFF($6 = '', NULL, $6::INTEGER) AS activity_type_id,
        IFF($7 = '', NULL, $7::INTEGER) AS sales_counselor_id,
        IFF($8 = '', NULL, $8::INTEGER) AS stage_id,
        IFF($9 = '', NULL, $9::VARCHAR) AS activity_type_name,
        IFF($10 = '', NULL, $10::VARCHAR) AS activity_result_name,
        IFF($11 = '', NULL, $11::TIMESTAMP_TZ) AS scheduled_at,
        IFF($12 = '', NULL, $12::TIMESTAMP_TZ) AS completed_at,
        IFF($13 = '', NULL, $13::TIMESTAMP_TZ) AS discarded_at,
        $14::TIMESTAMP_TZ AS created_at,
        IFF($15 = '', NULL, $15::VARCHAR) AS auto_perform,
        IFF($16 = '', NULL, $16::VARCHAR) AS direction,
        IFF($17 = '', NULL, $17::VARCHAR) AS stage_name,
        IFF($18 = '', NULL, $18::TEXT) AS notes,
        IFF($19 = '', NULL, $19::VARCHAR) AS assigned_to_name,
        IFF($20 = '', NULL, $20::VARCHAR) AS created_by_name,
        IFF($21 = '', NULL, $21::VARCHAR) AS first_completed_of_activity_type,
        $22::VARCHAR AS community_name,
        CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP()) AS load_dts
    FROM @{stage_name}/{blob_name}
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format');
