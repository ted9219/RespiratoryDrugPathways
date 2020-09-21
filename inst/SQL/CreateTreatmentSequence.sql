USE @resultsSchema;

DROP TABLE IF EXISTS @studyName_targetcohort;
DROP TABLE IF EXISTS @studyName_drug_seq;

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

-- Find all outcomes of the target population
CREATE TABLE @studyName_drug_seq
(
person_id BIGINT,
index_year INT,
drug_concept_id INT,
drug_start_date DATE,
drug_end_date   DATE,
duration_era INT,
gap_same INT
);

INSERT INTO @studyName_drug_seq(person_id, index_year, drug_concept_id, drug_start_date, drug_end_date, duration_era, gap_same)
SELECT
  de.subject_id,
  year(c1.index_date)                                                                   AS index_year,
  de.cohort_definition_id,
  de.cohort_start_date,
  de.cohort_end_date,
 -- row_number()
 -- OVER (
 --   PARTITION BY de.subject_id
  --  ORDER BY de.cohort_start_date, de.cohort_end_date )                                 AS drug_seq,
  date_part('day', de.cohort_end_date :: TIMESTAMP - de.cohort_start_date :: TIMESTAMP) AS duration_era,
  date_part('day', de.cohort_start_date :: TIMESTAMP - (lag(de.cohort_end_date)
  OVER (
    PARTITION BY de.subject_id, de.cohort_definition_id
    ORDER BY de.cohort_start_date, de.cohort_end_date )) :: TIMESTAMP)                  AS gap_same
FROM
  (SELECT *
   FROM @cohortTable C
         WHERE C.cohort_definition_id IN (@outcomeCohortIds)) de
  INNER JOIN @studyName_targetcohort c1
ON de.subject_id = c1.person_id
WHERE c1.index_date <= de.cohort_start_date AND de.cohort_start_date < c1.cohort_end_date; -- exclude events outside target cohort period
