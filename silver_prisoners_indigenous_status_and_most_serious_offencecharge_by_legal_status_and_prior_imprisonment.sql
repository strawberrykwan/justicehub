SELECT DISTINCT
                  sheet_name,
                  row,
                  col,
                  'prisoners'                       AS topic,
                  ROUND(`.value`, 2)                AS value,
                  CASE
                    WHEN unit='no.'
                    THEN 'count'
                    WHEN unit='% prior'
                    THEN 'percent'
                    ELSE NULL
                  END                               AS measure,
                  CASE
                    WHEN unit='no.'
                    THEN 'number'
                    WHEN unit='% prior'
                    THEN '%'
                    ELSE NULL
                  END                               AS unit,
                  1                                 AS unit_multiplier,
                  'people'                          AS observation_type,
                  'annually'                        AS frequency,
                  '31/12/' || year                  AS date,
                  'AUS'                             AS state,
                  ls.mapped_value                   AS legal_status,
                  TRIM(LOWER(
                    REPLACE(
                      REPLACE(
                        REPLACE(
                          REPLACE(
                            REPLACE(
                              REPLACE(
                                REPLACE(
                                  REPLACE(
                                    REPLACE(
                                      REPLACE(offence_type, '0', ''),
                                    '1', ''),
                                  '2', ''),
                                '3', ''),
                              '4', ''),
                            '5', ''),
                          '6', ''),
                        '7', ''),
                      '8', ''),
                    '9', '')
                   ))                               AS offence_type,
                  i.mapped_value                    AS indigenous_status,
                  'all persons'                     AS sex,
                  'all persons'                     AS age,
                  CASE
                    WHEN unit='no.'
                    THEN 'total'
                    WHEN unit='% prior'
                    THEN 'prior imprisonment'
                    ELSE NULL
                  END                               AS prior_imprisonment_status
    FROM          bronze_prisoners_indigenous_status_and_most_serious_offencecharge_by_legal_status_and_prior_imprisonment AS b
    
    LEFT JOIN     mapping_table as i
    ON            LOWER(i.original_value) = LOWER(b.indigenous_status) AND i.column_name = 'indigenous_status'
    
    LEFT JOIN     mapping_table as ls
    ON            LOWER(ls.original_value) = LOWER(b.observation_type) AND ls.column_name = 'legal_status'
    
    WHERE         `.value` != 'n.p.'