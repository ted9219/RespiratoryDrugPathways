
IF OBJECT_ID('@resultsSchema.@databaseName_@studyName_drug_seq_summary', 'U') IS NOT NULL
	DROP TABLE @resultsSchema.@databaseName_@studyName_drug_seq_summary;

-- Summarize the unique treatment sequences observed
CREATE TABLE @resultsSchema.@databaseName_@studyName_drug_seq_summary
(
index_year INT,
d1_concept_id VARCHAR (40),
d2_concept_id VARCHAR (40),
d3_concept_id VARCHAR (40),
d4_concept_id VARCHAR (40),
d5_concept_id VARCHAR (40),
d1_concept_name VARCHAR (400),
d2_concept_name VARCHAR (400),
d3_concept_name VARCHAR (400),
d4_concept_name VARCHAR (400),
d5_concept_name VARCHAR (400),
num_persons INT
);

INSERT INTO @resultsSchema.@databaseName_@studyName_drug_seq_summary (index_year, d1_concept_id, d2_concept_id, d3_concept_id, d4_concept_id, d5_concept_id, d1_concept_name, d2_concept_name, d3_concept_name, d4_concept_name, d5_concept_name, num_persons)
SELECT
  d1.index_year,
  d1.drug_concept_id           AS d1_concept_id,
  d2.drug_concept_id           AS d2_concept_id,
  d3.drug_concept_id           AS d3_concept_id,
  d4.drug_concept_id           AS d4_concept_id,
  d5.drug_concept_id           AS d5_concept_id,
  d1.concept_name              AS d1_concept_name,
  d2.concept_name              AS d2_concept_name,
  d3.concept_name              AS d3_concept_name,
  d4.concept_name              AS d4_concept_name,
  d5.concept_name              AS d5_concept_name,
  count(DISTINCT d1.person_id) AS num_persons
FROM
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 1) d1
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 2) d2
    ON d1.person_id = d2.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 3) d3
    ON d1.person_id = d3.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 4) d4
    ON d1.person_id = d4.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 5) d5
    ON d1.person_id = d5.person_id
GROUP BY
  d1.index_year,
  d1.drug_concept_id,
  d2.drug_concept_id,
  d3.drug_concept_id,
  d4.drug_concept_id,
  d5.drug_concept_id,
  d1.concept_name,
  d2.concept_name,
  d3.concept_name,
  d4.concept_name,
  d5.concept_name;


-- Count total persons for attrition table
IF OBJECT_ID('@resultsSchema.@databaseName_@studyName_summary_cnt', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@databaseName_@studyName_summary_cnt;

CREATE TABLE @resultsSchema.@databaseName_@studyName_summary_cnt
(
count_type VARCHAR (500),
num_persons INT
);

INSERT INTO @resultsSchema.@databaseName_@studyName_summary_cnt (count_type, num_persons)
SELECT
  'Number of persons'         AS count_type,
  count(DISTINCT p.person_id) AS num_persons
FROM @cdmDatabaseSchema.person p;

INSERT INTO @resultsSchema.@databaseName_@studyName_summary_cnt (count_type, num_persons)
SELECT
  'Number of persons in target cohort' AS count_type,
  count(DISTINCT person_id)            AS num_persons
FROM @resultsSchema.@databaseName_@studyName_targetcohort;

INSERT INTO @resultsSchema.@databaseName_@studyName_summary_cnt (count_type, num_persons)
SELECT
  CONCAT('Number of persons in target cohort in ', index_year) AS count_type,
  num_persons            AS num_persons
FROM
  (
    SELECT
      year(index_date) as index_year,
      count(DISTINCT person_id) AS num_persons
    FROM @resultsSchema.@databaseName_@studyName_targetcohort
    GROUP BY year(index_date)
  ) t1;

INSERT INTO @resultsSchema.@databaseName_@studyName_summary_cnt (count_type, num_persons)
SELECT
  'Total number of pathways (before minCellCount)' AS count_type,
  sum(num_persons)            AS num_persons
FROM @resultsSchema.@databaseName_@studyName_drug_seq_summary;

INSERT INTO @resultsSchema.@databaseName_@studyName_summary_cnt (count_type, num_persons)
SELECT
  CONCAT('Number of pathways (before minCellCount) in ', index_year),
  num_persons
FROM
  (
    SELECT
      index_year,
      sum(num_persons) AS num_persons
    FROM @resultsSchema.@databaseName_@studyName_drug_seq_summary
    GROUP BY index_year
  ) t1;


IF OBJECT_ID('@resultsSchema.@databaseName_@studyName_targetcohort', 'U') IS NOT NULL
DROP TABLE @resultsSchema.@databaseName_@studyName_targetcohort;
