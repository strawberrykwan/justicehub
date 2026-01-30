SELECT DISTINCT
                'UNKNOWN'                       AS sheet_name,
                b.row,
                b.col,
                'prisoners'                     AS topic,
                ROUND(value, 2)                 AS value,
                'count'                         AS measure,
                'number'                        AS unit,
                1                               AS unit_multiplier,
                'people'                        AS observation_type,
                'annually'                      AS frequency,
                '31/12/2024'                    AS date,
                'AUS'                           AS state,
                LOWER(legal_status)             AS legal_status,
                o.mapped_value                  AS offence_type,
                'all persons'                   AS indigenous_status,
                s.mapped_value                  AS sex,
                'all persons'                   AS age,
                'total'                         AS prior_imprisonment_status
INTO [justice_hub].[2_silver].[prisoners-most-serious-offencecharge-by-legal-status-and-sex]

FROM [justice_hub].[1_bronze].[prisoners-most-serious-offencecharge-by-legal-status-and-sex] AS b
  
LEFT JOIN     [justice_hub].[dbo].[mapping_table] as o
ON            LOWER(o.original_value) = LOWER(b.offence_type) AND o.column_name = 'offence_type'
  
LEFT JOIN     [justice_hub].[dbo].[mapping_table] as s
ON            LOWER(s.original_value) = LOWER(b.sex) AND s.column_name = 'sex'

