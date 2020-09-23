
IF OBJECT_ID('@resultsSchema.@studyName_targetcohort', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@studyName_targetcohort;

IF OBJECT_ID('@resultsSchema.@studyName_drug_seq', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@studyName_drug_seq;

-- Load target population into targetcohort table
CREATE TABLE @resultsSchema.@studyName_targetcohort
(
PERSON_ID BIGINT NOT NULL PRIMARY KEY,
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

-- Find all outcomes of the target population
CREATE TABLE @resultsSchema.@studyName_drug_seq
(
person_id BIGINT,
index_year INT,
drug_concept_id INT,
drug_start_date DATE,
drug_end_date   DATE,
duration_era INT,
gap_same INT
);

INSERT INTO @resultsSchema.@studyName_drug_seq(person_id, index_year, drug_concept_id, drug_start_date, drug_end_date, duration_era, gap_same)
SELECT
  de.subject_id,
  year(c1.index_date)                                                                   AS index_year,
  de.cohort_definition_id,
  de.cohort_start_date,
  de.cohort_end_date,
  DATEDIFF(DAY, de.cohort_end_date, de.cohort_start_date) AS duration_era,
  DATEDIFF(DAY, de.cohort_start_date, lag(de.cohort_end_date)
  OVER (
    PARTITION BY de.subject_id, de.cohort_definition_id
    ORDER BY de.cohort_start_date, de.cohort_end_date ))                  AS gap_same
FROM
  (SELECT *
   FROM @resultsSchema.@cohortTable C
         WHERE C.cohort_definition_id IN (@outcomeCohortIds)) de
  INNER JOIN @resultsSchema.@studyName_targetcohort c1
ON de.subject_id = c1.person_id
WHERE c1.index_date <= de.cohort_start_date AND de.cohort_start_date < c1.cohort_end_date; -- exclude events outside target cohort period
