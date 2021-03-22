
IF OBJECT_ID('@resultsSchema.@databaseName_@studyName_targetcohort', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@databaseName_@studyName_targetcohort;

IF OBJECT_ID('@resultsSchema.@databaseName_@studyName_drug_seq', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@databaseName_@studyName_drug_seq;

-- Load target population into targetcohort table
CREATE TABLE @resultsSchema.@databaseName_@studyName_targetcohort
(
PERSON_ID BIGINT NOT NULL,
INDEX_DATE date NOT NULL,
COHORT_END_DATE date NOT NULL
);

INSERT INTO @resultsSchema.@databaseName_@studyName_targetcohort (PERSON_ID, INDEX_DATE, COHORT_END_DATE)
SELECT
  c.subject_id,
  -- subject_id is equal to person_id
  c.cohort_start_date,
  -- cohort_start_date is equal to index_date
  c.cohort_end_date -- cohort_end_date is equal to cohort_end_date
FROM @resultsSchema.@cohortTable C
WHERE C.cohort_definition_id = @targetCohortId;

-- Find all outcomes of the target population
CREATE TABLE @resultsSchema.@databaseName_@studyName_drug_seq
(
person_id BIGINT,
index_year INT,
drug_concept_id INT,
drug_start_date DATE,
drug_end_date   DATE,
duration_era INT,
gap_same INT
);

INSERT INTO @resultsSchema.@databaseName_@studyName_drug_seq(person_id, index_year, drug_concept_id, drug_start_date, drug_end_date, duration_era, gap_same)
SELECT
  de.subject_id,
  year(c1.index_date)                                                                   AS index_year,
  de.cohort_definition_id,
  de.cohort_start_date,
  de.cohort_end_date,
  DATEDIFF(DAY, de.cohort_start_date, de.cohort_end_date) AS duration_era,
  DATEDIFF(DAY, lag(de.cohort_end_date)
  OVER (
    PARTITION BY de.subject_id, de.cohort_definition_id
    ORDER BY de.cohort_start_date, de.cohort_end_date ), de.cohort_start_date)                  AS gap_same
FROM
  (SELECT *
   FROM @resultsSchema.@cohortTable C
         WHERE C.cohort_definition_id IN (@eventCohortIds)) de
  INNER JOIN @resultsSchema.@databaseName_@studyName_targetcohort c1
ON de.subject_id = c1.person_id
WHERE datediff(dd, de.cohort_start_date, c1.index_date) <=  @includeTreatmentsPriorToIndex AND de.cohort_start_date < c1.cohort_end_date; -- exclude events outside target cohort period


