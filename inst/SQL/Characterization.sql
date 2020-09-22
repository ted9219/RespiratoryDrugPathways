USE @resultsSchema;

IF OBJECT_ID('@studyName_targetcohort', 'U') IS NOT NULL
DROP TABLE @studyName_targetcohort;

IF OBJECT_ID('@studyName_characterization', 'U') IS NOT NULL
DROP TABLE @studyName_characterization;

-- Load target population into targetcohort table
CREATE TABLE @studyName_targetcohort
(
PERSON_ID BIGINT NOT NULL PRIMARY KEY,
INDEX_DATE date NOT NULL,
COHORT_END_DATE date NOT NULL
);

INSERT INTO @studyName_targetcohort (PERSON_ID, INDEX_DATE, COHORT_END_DATE)
SELECT
  c.subject_id,
  -- subject_id is equal to person_id
  c.cohort_start_date,
  -- cohort_start_date is equal to index_date
  c.cohort_end_date -- cohort_end_date is equal to cohort_end_date
FROM @cohortTable C
WHERE C.cohort_definition_id = @targetCohortId;

-- Do characterization
CREATE TABLE @studyName_characterization AS
select gender, count(*) as num_people, avg(age) as avg_age, avg(time_in_cohort) as avg_time_in_cohort
from (select gender_source_value as gender, date_part('year', age (p.birth_datetime::date)) as age, date_part('day', t.cohort_end_date :: TIMESTAMP - t.index_date :: TIMESTAMP) AS time_in_cohort
FROM @studyName_targetcohort as t
LEFT JOIN @cdmDatabaseSchema.person as p
ON t.person_id = p.person_id) as o
GROUP BY ROLLUP (gender);