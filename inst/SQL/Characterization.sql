
IF OBJECT_ID('@resultsSchema.@studyName_targetcohort', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@studyName_targetcohort;

IF OBJECT_ID('@resultsSchema.@studyName_characterization', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@studyName_characterization;

-- Load target population into targetcohort table
CREATE TABLE @resultsSchema.@studyName_targetcohort
(
PERSON_ID BIGINT NOT NULL,
INDEX_DATE date NOT NULL,
COHORT_END_DATE date NOT NULL
);

INSERT INTO @resultsSchema.@studyName_targetcohort (PERSON_ID, INDEX_DATE, COHORT_END_DATE)
SELECT
  c.subject_id,
  -- subject_id is equal to person_id
  c.cohort_start_date,
  -- cohort_start_date is equal to index_date
  c.cohort_end_date -- cohort_end_date is equal to cohort_end_date
FROM @resultsSchema.@cohortTable C
WHERE C.cohort_definition_id = @targetCohortId;

-- Do characterization
CREATE TABLE @resultsSchema.@studyName_characterization
(
GENDER VARCHAR(50),
NUM_PEOPLE INT,
AVG_AGE FLOAT(53),
AVG_DAYS_IN_COHORT FLOAT(53)
);

INSERT INTO @resultsSchema.@studyName_characterization
SELECT gender, count(*) as num_people, avg(age) as avg_age, avg(days_in_cohort) as avg_days_in_cohort
from (select p.gender_source_value as gender, YEAR(t.cohort_end_date)-p.year_of_birth as age, DATEDIFF(DAY, t.index_date, t.cohort_end_date) AS days_in_cohort
FROM @resultsSchema.@studyName_targetcohort t
LEFT JOIN @cdmDatabaseSchema.person p
ON t.person_id = p.person_id) o
GROUP BY gender;

INSERT INTO @resultsSchema.@studyName_characterization
SELECT 'all' as gender, count(*) as num_people, avg(YEAR(t.cohort_end_date)-p.year_of_birth) as avg_age, avg(DATEDIFF(DAY,  t.index_date, t.cohort_end_date)) as avg_days_in_cohort
FROM @resultsSchema.@studyName_targetcohort t
LEFT JOIN @cdmDatabaseSchema.person p
ON t.person_id = p.person_id;
