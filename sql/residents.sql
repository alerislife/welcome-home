-- DDL for SQL table
CREATE OR REPLACE TABLE {database}.{schema}.residents (
    id INTEGER PRIMARY KEY,
    prospect_id INTEGER,
    person_id INTEGER,
    person_discarded_at TIMESTAMP_TZ,
    person_first_name VARCHAR,
    person_last_name VARCHAR,
    person_middle_name VARCHAR,
    person_salutation VARCHAR,
    person_position VARCHAR,
    person_cell_phone VARCHAR,
    person_home_phone VARCHAR,
    person_work_phone VARCHAR,
    person_fax_number VARCHAR,
    person_do_not_call VARCHAR,
    person_do_not_email VARCHAR,
    person_do_not_mail VARCHAR,
    person_do_not_text VARCHAR,
    person_birthdate VARCHAR,
    person_provided_age VARCHAR,
    person_email VARCHAR,
    person_gender VARCHAR,
    address_line1 VARCHAR,
    address_line2 VARCHAR,
    address_city VARCHAR,
    address_state VARCHAR,
    address_zip VARCHAR,
    care_type_name VARCHAR,
    care_type_abbreviation VARCHAR,
    marital_status VARCHAR,
    current_residence VARCHAR,
    veteran_status VARCHAR,
    community_id INTEGER,
    community_name VARCHAR,
    load_dts TIMESTAMP_TZ
);

-- COPY INTO statement to load data
COPY INTO
    {database}.{schema}.residents (
    id,
    prospect_id,
    person_id,
    person_discarded_at,
    person_first_name,
    person_last_name,
    person_middle_name,
    person_salutation,
    person_position,
    person_cell_phone,
    person_home_phone,
    person_work_phone,
    person_fax_number,
    person_do_not_call,
    person_do_not_email,
    person_do_not_mail,
    person_do_not_text,
    person_birthdate,
    person_provided_age,
    person_email,
    person_gender,
    address_line1,
    address_line2,
    address_city,
    address_state,
    address_zip,
    care_type_name,
    care_type_abbreviation,
    marital_status,
    current_residence,
    veteran_status,
    community_id,
    community_name,
    load_dts
)
FROM (
    SELECT
        $1::INTEGER,                                                            -- id
        $2::INTEGER,                                                            -- prospect_id
        $3::INTEGER,                                                            -- person_id
        IFF($4 = '', NULL, $4::TIMESTAMP_TZ),                                 -- person_discarded_at
        IFF($5 = '', NULL, $5::VARCHAR),                                      -- person_first_name
        IFF($6 = '', NULL, $6::VARCHAR),                                      -- person_last_name
        IFF($7 = '', NULL, $7::VARCHAR),                                      -- person_middle_name
        IFF($8 = '', NULL, $8::VARCHAR),                                      -- person_salutation
        IFF($9 = '', NULL, $9::VARCHAR),                                      -- person_position
        IFF($10 = '', NULL, $10::VARCHAR),                                    -- person_cell_phone
        IFF($11 = '', NULL, $11::VARCHAR),                                    -- person_home_phone
        IFF($12 = '', NULL, $12::VARCHAR),                                    -- person_work_phone
        IFF($13 = '', NULL, $13::VARCHAR),                                    -- person_fax_number
        IFF($14 = '', NULL, $14::VARCHAR),                                    -- person_do_not_call
        IFF($15 = '', NULL, $15::VARCHAR),                                    -- person_do_not_email
        IFF($16 = '', NULL, $16::VARCHAR),                                    -- person_do_not_mail
        IFF($17 = '', NULL, $17::VARCHAR),                                    -- person_do_not_text
        IFF($18 = '', NULL, $18::VARCHAR),                                    -- person_birthdate
        IFF($19 = '', NULL, $19::VARCHAR),                                    -- person_provided_age
        IFF($20 = '', NULL, $20::VARCHAR),                                    -- person_email
        IFF($21 = '', NULL, $21::VARCHAR),                                    -- person_gender
        IFF($22 = '', NULL, $22::VARCHAR),                                    -- address_line1
        IFF($23 = '', NULL, $23::VARCHAR),                                    -- address_line2
        IFF($24 = '', NULL, $24::VARCHAR),                                    -- address_city
        IFF($25 = '', NULL, $25::VARCHAR),                                    -- address_state
        IFF($26 = '', NULL, $26::VARCHAR),                                    -- address_zip
        IFF($27 = '', NULL, $27::VARCHAR),                                    -- care_type_name
        IFF($28 = '', NULL, $28::VARCHAR),                                    -- care_type_abbreviation
        IFF($29 = '', NULL, $29::VARCHAR),                                    -- marital_status
        IFF($30 = '', NULL, $30::VARCHAR),                                    -- current_residence
        IFF($31 = '', NULL, $31::VARCHAR),                                    -- veteran_status
        $32::INTEGER,                                                          -- community_id
        $33::VARCHAR,                                                          -- community_name
        CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP())             -- load_dts
    FROM @{stage_name}/{blob_name}
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format');
