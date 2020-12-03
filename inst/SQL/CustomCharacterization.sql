
IF OBJECT_ID('@resultsSchema.@databaseName_characterization_@targetCohortId', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@databaseName_characterization_@targetCohortId;

IF OBJECT_ID('@resultsSchema.@databaseName_targetcohort_@targetCohortId', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@databaseName_targetcohort_@targetCohortId;

IF OBJECT_ID('@resultsSchema.@databaseName_characterizationcohorts', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@databaseName_characterizationcohorts;

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
FROM @resultsSchema.@cohortTable c
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
FROM @resultsSchema.@cohortTable c
WHERE C.cohort_definition_id IN (@characterizationCohortIds);

-- Do characterization
CREATE TABLE @resultsSchema.@databaseName_characterization_@targetCohortId
(
covariate_id VARCHAR(55),
mean NUMERIC
);

INSERT INTO @resultsSchema.@databaseName_characterization_@targetCohortId (covariate_id, mean)
SELECT c.cohort_definition_id, round(count(DISTINCT c.person_id) * 1.0 / (SELECT count(DISTINCT person_id) FROM @resultsSchema.@databaseName_targetcohort_@targetCohortId),4)
FROM @resultsSchema.@databaseName_targetcohort_@targetCohortId as t
JOIN @resultsSchema.@databaseName_characterizationcohorts as c
ON t.person_id = c.person_id
WHERE c.index_date <= t.index_date
GROUP BY c.cohort_definition_id;
