SELECT
    adsh AS submission_id,
    TRY_CAST(cik AS NUMBER) AS company_id,
    name AS company_name,
    TRY_CAST(sic AS NUMBER) AS sic_code,
    countryba as business_country,
    stprba as business_state,
    cityba as business_city,
    zipba as business_zip,
    countryma as mailing_country,
    stprma as mailing_state,
    cityma as mailing_city,
    zipma as mailing_zip,
    TRY_CAST(ein AS NUMBER) as employer_id,
    TRY_TO_DATE(period, 'YYYYMMDD') AS period, 
    TRY_TO_DATE(filed, 'YYYYMMDD') AS filed,
    TRY_CAST(fy AS INTEGER) AS fiscal_year,
    fp AS fiscal_period
FROM {{ source('sec_source', 'raw_sub') }}