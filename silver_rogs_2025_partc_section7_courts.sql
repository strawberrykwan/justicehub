WITH states_data_reshaped AS (
    SELECT DISTINCT
                  *,
                  'NSW'                             AS col,
                  NSW                               AS value,
                  'NSW'                             AS state,
                  'state'                           AS court_jurisdiction
    FROM          bronze_rogs_2025_partc_section7_courts
    
    UNION
    
    SELECT DISTINCT
                  *,
                  'Vic'                             AS col,
                  Vic                               AS value,
                  'VIC'                             AS state,
                  'state'                           AS court_jurisdiction
    FROM          bronze_rogs_2025_partc_section7_courts
    
    UNION
    
    SELECT DISTINCT
                  *,
                  'Qld'                             AS col,
                  Qld                               AS value,
                  'QLD'                             AS state,
                  'state'                           AS court_jurisdiction
    FROM          bronze_rogs_2025_partc_section7_courts
    
    UNION
    
    SELECT DISTINCT
                  *,
                  'WA'                              AS col,
                  WA                                AS value,
                  'WA'                              AS state,
                  'state'                           AS court_jurisdiction
    FROM          bronze_rogs_2025_partc_section7_courts
    
    UNION
    
    SELECT DISTINCT
                  *,
                  'SA'                              AS col,
                  SA                                AS value,
                  'SA'                              AS state,
                  'state'                           AS court_jurisdiction
    FROM          bronze_rogs_2025_partc_section7_courts
    
    UNION
    
    SELECT DISTINCT
                  *,
                  'Tas'                             AS col,
                  Tas                               AS value,
                  'TAS'                             AS state,
                  'state'                           AS court_jurisdiction
    FROM          bronze_rogs_2025_partc_section7_courts
    
    UNION
    
    SELECT DISTINCT
                  *,
                  'ACT'                             AS col,
                  ACT                               AS value,
                  'ACT'                             AS state,
                  'state'                           AS court_jurisdiction
    FROM          bronze_rogs_2025_partc_section7_courts
    
    UNION
    
    SELECT DISTINCT
                  *,
                  'NT'                              AS col,
                  NT                                AS value,
                  'NT'                              AS state,
                  'state'                           AS court_jurisdiction
    FROM          bronze_rogs_2025_partc_section7_courts
    
    UNION
    
    SELECT DISTINCT
                  *,
                  'Aust'                            AS col,
                  Aust                              AS value,
                  'AUS'                             AS state,
                  'all'                             AS court_jurisdiction
    FROM          bronze_rogs_2025_partc_section7_courts
    
    UNION
    
    SELECT DISTINCT
                  *,
                  'Aust.cts'                        AS col,
                  [Aust.cts]                        AS value,
                  'AUS'                             AS state,
                  'federal'                         AS court_jurisdiction
    FROM          bronze_rogs_2025_partc_section7_courts
  ),
  
  reshaped_raw_data AS (
    SELECT DISTINCT
                  Table_Number                      AS sheet_name,
                  row,
                  col,
                  TRIM(value)                       AS value,
                  CASE
                    WHEN LOWER(Measure) LIKE '% per %' OR Unit = 'rate'
                    THEN 'rate'
                    WHEN LOWER(Measure) LIKE '%recurrent%'
                    THEN 'recurrent amount'
                    WHEN LOWER(unit_reshaped) = '%'
                    THEN 'percent'
                    ELSE 'count'
                  END                               AS measure,
                  CASE
                    WHEN LOWER(Measure) LIKE '% per %'
                    THEN unit_reshaped || ' ' || SUBSTR(Measure, INSTR(Measure, 'per'))
                    WHEN LOWER(Description1) LIKE '% per %'
                    THEN SUBSTR(Description1, INSTR(Description1, 'per'), 9999999)
                    WHEN LOWER(Description5) = 'total' OR unit_reshaped = 'number'
                    THEN 'number'
                    ELSE unit_reshaped
                  END                               AS unit,
                  CASE
                    WHEN LOWER(Unit) LIKE '%000'
                    THEN 1000
                    ELSE 1
                  END                               AS unit_multiplier,
                  CASE
                    WHEN LOWER(measure_reshaped) LIKE '% per %'
                    THEN SUBSTR(LOWER(measure_reshaped), 1, INSTR(LOWER(measure_reshaped), 'per') - 1)
                    WHEN (LOWER(Measure) LIKE '%lodgments%'
                          OR LOWER(Measure) LIKE '%finalisations%'
                          OR LOWER(Measure) LIKE '%cases%')
                         AND (LOWER(Measure) NOT LIKE '%expenditure%' OR LOWER(Measure) NOT LIKE '%fees%')
                    THEN 'cases'
                    WHEN LOWER(Measure) = 'judicial officers'
                    THEN LOWER(Measure)
                    ELSE null
                  END                               AS observation_type,
                  CASE
                    WHEN Year LIKE '%-%'
                    THEN '30/06/20' || SUBSTR(Year, INSTR(Year, '-') + 1, 4)
                    ELSE '31/12/' || Year
                  END                               AS date,
                  state,
                  Indigenous_Status                 AS indigenous_status,
                  court_jurisdiction,
                  CASE
                    WHEN Court_Type = 'All criminal courts' OR Court_Type = 'All civil courts' OR Court_Type = 'All criminal and civil courts'
                    THEN 'all levels'
                    WHEN LOWER(Court_Type) LIKE '%supreme%'
                    THEN 'supreme'
                    WHEN LOWER(Court_Type) LIKE '%district%'
                    THEN 'district'
                    WHEN LOWER(Court_Type) LIKE '%magistrate%' OR LOWER(REPLACE(Court_Type, '''', '')) = 'childrens' OR LOWER(REPLACE(Court_Type, '''', '')) = 'coroners'
                    THEN 'magistrates'
                    WHEN LOWER(Court_Type) LIKE '%fcfcoa%' OR TRIM(Court_Type) = 'Federal Court of Australia'
                    THEN 'federal'
                    ELSE 'n/a'
                  END                               AS court_level,
                  COALESCE(TRIM(LOWER(Law_Enforced)), 'n/a')         AS law_domain,
                  CASE
                    WHEN Court_Type IN ('Family/FCFCOA (Division 1) and FCFCOA (Division 2)', 'Family') OR LOWER(TRIM(Description4)) = 'family law matters'
                    THEN 'family'
                    WHEN LOWER(Court_Type) = 'supreme (probate only)'
                    THEN 'probate'
                    WHEN LOWER(Court_Type) = 'supreme (excl. probate)'
                    THEN 'non-probate'
                    WHEN LOWER(Court_Type) = 'fcfcoa (division 2)'
                    THEN 'non-family'
                    WHEN LOWER(REPLACE(Court_Type, '''', '')) = 'childrens'
                    THEN 'children'
                    WHEN LOWER(Court_Type) LIKE '%excl. children%'
                    THEN 'non-children'
                    ELSE 'general'
                  END                               AS court_function,
                  CASE
                    WHEN LOWER(Measure) = 'lodgments'
                         AND (LOWER(Measure) NOT LIKE '%expenditure%' OR LOWER(Measure) NOT LIKE '%fees%')
                    THEN 'lodged'
                    WHEN LOWER(Measure) = 'finalisations'
                         AND (LOWER(Measure) NOT LIKE '%expenditure%' OR LOWER(Measure) NOT LIKE '%fees%')
                    THEN 'finalised'
                    ELSE 'n/a'
                  END                               AS matter_status,
                  CASE
                    WHEN LOWER(Description5) = 'appeal'
                    THEN 'appeal'
                    WHEN LOWER(Description5) = 'non-appeal'
                    THEN 'non-appeal'
                    WHEN LOWER(Description5) = 'deaths reported'
                    THEN 'deaths reported'
                    WHEN LOWER(Description5) = 'fires reported'
                    THEN 'fires reported'
                    ELSE NULL
                  END                               AS matter_type_temp
                  
    FROM          (
                    SELECT DISTINCT
                                  *,
                                  CASE
                                    WHEN LOWER(Unit) = 'no.'
                                    THEN 'number'
                                    WHEN LOWER(Unit) LIKE '%000'
                                    THEN 'number'
                                    WHEN LOWER(Unit) LIKE '%$%'
                                    THEN '$'
                                    ELSE LOWER(Unit)
                                  END               AS unit_reshaped,
                                  TRIM
                                  (
                                    REPLACE
                                    (
                                        REPLACE( LOWER(Measure), 'proportion ', '' ),
                                        'average ',
                                        ''
                                    )
                                  )                 AS measure_reshaped
                    FROM          states_data_reshaped
                  )

    WHERE           Table_Number IN ('7A.17', '7A.1', '7A.2', '7A.3', '7A.4', '7A.5', '7A.6', '7A.7', '7A.8', '7A.9', '7A.10', '7A.28')
    AND             LOWER(Description1) NOT LIKE '%population%'
  )
  
    SELECT DISTINCT
                  b.sheet_name,
                  b.row,
                  b.col,
                  'courts'                          AS topic,
                  ROUND(b.value,2)                  AS value,
                  b.measure,
                  b.unit,
                  b.unit_multiplier,
                  b.observation_type,
                  'annually'                        AS frequency,
                  b.date,
                  b.state,
                  i.mapped_value                    AS indigenous_status,
                  court_jurisdiction,
                  court_level,
                  law_domain,
                  court_function,
                  matter_status,
                  CASE
                    WHEN b.observation_type = 'cases'
                    THEN COALESCE(b.matter_type_temp, 'total')
                    ELSE 'n/a'
                  END                               AS matter_type
    FROM          reshaped_raw_data AS b
    
    LEFT JOIN     mapping_table as i
    ON            LOWER(i.original_value) = LOWER(b.indigenous_status) AND i.column_name = 'indigenous_status'
    
  
    WHERE         b.value != 'na'
    AND           b.value != '..'