WITH indigenous_sex_fix AS (
    SELECT DISTINCT
                  sheet_name,
                  row,
                  col,
                  CASE
                    WHEN LOWER(indigenous_status) LIKE '%aboriginal%'
                    THEN indigenous_status
                    WHEN LOWER(indigenous_status) LIKE '%indigenous%'
                    THEN indigenous_status
                    WHEN LOWER(indigenous_status) LIKE '%unknown%'
                    THEN indigenous_status
                    ELSE 'all persons'
                  END                                             AS indigenous_status,
                  CASE
                    WHEN LOWER(sex) = 'males'
                    THEN sex
                    WHEN LOWER(sex) = 'females'
                    THEN sex
                    ELSE 'all persons'
                  END                                             AS sex
    FROM          (
                  SELECT DISTINCT
                                sheet_name,
                                row,
                                col,
                                COALESCE(`Indigenous.status`, `Median.age..years.`)   AS indigenous_status,
                                COALESCE(Sex, `Median.age..years.`)   AS sex
                  FROM          bronze_prisoners_selected_characteristics_by_stateterritory
                  )
    
  )
  
  SELECT DISTINCT
                  b.sheet_name,
                  b.row,
                  b.col,
                  'prisoners'                       AS topic,
                  ROUND(`.value`, 2)                AS value,
                  CASE
                    WHEN b.row <= 21
                    THEN 'count'
                    WHEN b.row <= 27
                    THEN 'median'
                    ELSE 'percent'
                  END                               AS measure,
                  CASE
                    WHEN b.row <= 21
                    THEN 'number'
                    WHEN b.row <= 27
                    THEN 'years'
                    ELSE '%'
                  END                               AS unit,
                  1                                 AS unit_multiplier,
                  CASE
                    WHEN b.row > 21 AND b.row <= 27
                    THEN 'age'
                    ELSE 'people'
                  END                               AS observation_type,
                  'annually'                        AS frequency,
                  '31/12/' || year                  AS date,
                  state.mapped_value                AS state,
                  COALESCE(ls.mapped_value, 'total') AS legal_status,
                  'total'                           AS offence_type,
                  i.mapped_value                    AS indigenous_status,
                  s.mapped_value                    AS sex,
                  'all persons'                     AS age,
                  COALESCE(pis.mapped_value, 'total')                  AS prior_imprisonment_status
  FROM          bronze_prisoners_selected_characteristics_by_stateterritory AS b
  
  LEFT JOIN     mapping_table as state
  ON            LOWER(state.original_value) = LOWER(b.state) AND state.column_name = 'state'
  
  LEFT JOIN     indigenous_sex_fix AS if
  ON            if.sheet_name = b.sheet_name AND if.row = b.row AND if.col = b.col
  LEFT JOIN     mapping_table as i
  ON            LOWER(i.original_value) = LOWER(if.indigenous_status) AND i.column_name = 'indigenous_status'
  
  LEFT JOIN     indigenous_sex_fix AS sf
  ON            sf.sheet_name = b.sheet_name AND sf.row = b.row AND sf.col = b.col
  
  LEFT JOIN     mapping_table as s
  ON            LOWER(s.original_value) = LOWER(sf.sex) AND s.column_name = 'sex'
  
  LEFT JOIN     mapping_table as ls
  ON            LOWER(ls.original_value) = LOWER(b.`Legal.status`) AND ls.column_name = 'legal_status'
  
  LEFT JOIN     mapping_table as pis
  ON            LOWER(pis.original_value) = LOWER(b.`Prior.imprisonment.status`) AND pis.column_name = 'prior_imprisonment_status'
    
  WHERE         `.value` != 'n.p.'