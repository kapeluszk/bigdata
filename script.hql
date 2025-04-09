
-- MapReduce job table (input_dir3)
CREATE EXTERNAL TABLE IF NOT EXISTS league_stats (
    league_id STRING,
    avg_wage DOUBLE,
    avg_age DOUBLE,
    count_players INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '${hiveconf:input_dir3}';

-- additional league info table (input_dir4)
CREATE EXTERNAL TABLE IF NOT EXISTS leagues (
    league_id STRING,
    league_name STRING,
    league_level INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '${hiveconf:input_dir4}';

-- processing data + json output
WITH joined_data AS (
    SELECT
        TRIM(ls.league_id) AS league_id,
        ls.avg_wage AS avg_wage,
        ls.avg_age AS avg_age,
        ls.count_players AS count_players,
        l.league_name AS league_name,
        l.league_level AS league_level
    FROM
        league_stats ls
    JOIN
        leagues l ON ls.league_id = l.league_id
),
    ranked_leagues AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY league_level ORDER BY avg_wage DESC) AS rank
        FROM joined_data
    )
INSERT OVERWRITE DIRECTORY '${hiveconf:output_dir}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n'
SELECT CONCAT(
    '{',
    '"league_id": "', league_id, '", ',
    '"league_name": "', league_name, '", ',
    '"league_level": ', league_level, ', ',
    '"avg_wage": ', avg_wage, ', ',
    '"avg_age": ', avg_age, ', ',
    '"count_players": ', count_players,
    '}'
) AS json_output
FROM ranked_leagues
WHERE rank <= 3;