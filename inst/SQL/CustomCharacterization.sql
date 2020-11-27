
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

-- Load custom cohorts into characterizationcohorts table
CREATE TABLE @resultsSchema.@databaseName_characterizationcohorts
(
COHORT_DEFINITION_ID INT,
PERSON_ID BIGINT NOT NULL,
INDEX_DATE date NOT NULL,
COHORT_END_DATE date NOT NULL
);

INSERT INTO @resultsSchema.@databaseName_characterizationcohorts (COHORT_DEFINITION_ID, PERSON_ID, INDEX_DATE, COHORT_END_DATE)
SELECT
  cohort_definition_id,
  c.subject_id,
  -- subject_id is equal to person_id
  c.cohort_start_date,
  -- cohort_start_date is equal to index_date
  c.cohort_end_date -- cohort_end_date is equal to cohort_end_date
FROM @resultsSchema.@cohortTable C
WHERE C.cohort_definition_id IN (@outcomeCohortIds));

-- Do characterization
CREATE TABLE @resultsSchema.@databaseName_characterization_@targetCohortId
(
CUSTOM_ID VARCHAR(55),
NUM_PEOPLE INT
);

INSERT INTO @resultsSchema.@databaseName_characterization_@targetCohortId (CUSTOM_ID, NUM_PEOPLE)
SELECT cohort_definition_id, count(DISTINCT c.subject_id)
FROM @resultsSchema.@databaseName_targetcohort_@targetCohortId as t
LEFT JOIN @resultsSchema.@databaseName_characterizationcohorts as c
ON t.person_id = c.person_id
WHERE c.cohort_start_date <= t.cohort_start_date
GROUP BY t.cohort_definition_id;