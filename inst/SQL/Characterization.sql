
IF OBJECT_ID('@resultsSchema.@databaseName_targetcohort_@targetCohortId', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@databaseName_targetcohort_@targetCohortId;

IF OBJECT_ID('@resultsSchema.@databaseName_characterization_@targetCohortId', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@databaseName_characterization_@targetCohortId;

-- Load target population into targetcohort table
CREATE TABLE @resultsSchema.@databaseName_targetcohort_@targetCohortId
(
PERSON_ID BIGINT NOT NULL,
INDEX_DATE date NOT NULL,
COHORT_END_DATE date NOT NULL
);

INSERT INTO @resultsSchema.@databaseName_targetcohort_@targetCohortId (PERSON_ID, INDEX_DATE, COHORT_END_DATE)
SELECT
  c.subject_id,
  -- subject_id is equal to person_id
  c.cohort_start_date,
  -- cohort_start_date is equal to index_date
  c.cohort_end_date -- cohort_end_date is equal to cohort_end_date
FROM @resultsSchema.@cohortTable C
WHERE C.cohort_definition_id = @targetCohortId;

-- Do characterization
CREATE TABLE @resultsSchema.@databaseName_characterization_@targetCohortId
(
GENDER VARCHAR(55),
NUM_PEOPLE INT,
AVG_AGE FLOAT(53),
AVG_DAYS_IN_COHORT FLOAT(53)
);

INSERT INTO @resultsSchema.@databaseName_characterization_@targetCohortId
SELECT gender, count(*) as num_people, avg(age) as avg_age, avg(days_in_cohort) as avg_days_in_cohort
from (select p.gender_concept_id as gender, YEAR(t.cohort_end_date)-p.year_of_birth as age, DATEDIFF(DAY, t.index_date, t.cohort_end_date) AS days_in_cohort
FROM @resultsSchema.@databaseName_targetcohort_@targetCohortId t
LEFT JOIN @cdmDatabaseSchema.person p
ON t.person_id = p.person_id) o
GROUP BY gender;

INSERT INTO @resultsSchema.@databaseName_characterization_@targetCohortId
SELECT 'all' as gender, count(*) as num_people, avg(YEAR(t.cohort_end_date)-p.year_of_birth) as avg_age, avg(DATEDIFF(DAY,  t.index_date, t.cohort_end_date)) as avg_days_in_cohort
FROM @resultsSchema.@databaseName_targetcohort_@targetCohortId t
LEFT JOIN @cdmDatabaseSchema.person p
ON t.person_id = p.person_id;

TRUNCATE TABLE @resultsSchema.@databaseName_targetcohort_@targetCohortId;
DROP TABLE @resultsSchema.@databaseName_targetcohort_@targetCohortId;

