
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
d6_concept_id VARCHAR (40),
d7_concept_id VARCHAR (40),
d8_concept_id VARCHAR (40),
d9_concept_id VARCHAR (40),
d10_concept_id VARCHAR (40),
d11_concept_id VARCHAR (40),
d12_concept_id VARCHAR (40),
d13_concept_id VARCHAR (40),
d14_concept_id VARCHAR (40),
d15_concept_id VARCHAR (40),
d16_concept_id VARCHAR (40),
d17_concept_id VARCHAR (40),
d18_concept_id VARCHAR (40),
d19_concept_id VARCHAR (40),
d20_concept_id VARCHAR (40),
d1_concept_name VARCHAR (400),
d2_concept_name VARCHAR (400),
d3_concept_name VARCHAR (400),
d4_concept_name VARCHAR (400),
d5_concept_name VARCHAR (400),
d6_concept_name VARCHAR (400),
d7_concept_name VARCHAR (400),
d8_concept_name VARCHAR (400),
d9_concept_name VARCHAR (400),
d10_concept_name VARCHAR (400),
d11_concept_name VARCHAR (400),
d12_concept_name VARCHAR (400),
d13_concept_name VARCHAR (400),
d14_concept_name VARCHAR (400),
d15_concept_name VARCHAR (400),
d16_concept_name VARCHAR (400),
d17_concept_name VARCHAR (400),
d18_concept_name VARCHAR (400),
d19_concept_name VARCHAR (400),
d20_concept_name VARCHAR (400),
num_persons INT
);

INSERT INTO @resultsSchema.@databaseName_@studyName_drug_seq_summary (index_year, d1_concept_id, d2_concept_id, d3_concept_id, d4_concept_id, d5_concept_id, d6_concept_id, d7_concept_id, d8_concept_id, d9_concept_id, d10_concept_id, d11_concept_id, d12_concept_id, d13_concept_id, d14_concept_id, d15_concept_id, d16_concept_id, d17_concept_id, d18_concept_id, d19_concept_id, d20_concept_id, d1_concept_name, d2_concept_name, d3_concept_name, d4_concept_name, d5_concept_name, d6_concept_name, d7_concept_name, d8_concept_name, d9_concept_name, d10_concept_name, d11_concept_name, d12_concept_name, d13_concept_name, d14_concept_name, d15_concept_name, d16_concept_name, d17_concept_name, d18_concept_name, d19_concept_name, d20_concept_name, num_persons)
SELECT
  d1.index_year,
  d1.drug_concept_id           AS d1_concept_id,
  d2.drug_concept_id           AS d2_concept_id,
  d3.drug_concept_id           AS d3_concept_id,
  d4.drug_concept_id           AS d4_concept_id,
  d5.drug_concept_id           AS d5_concept_id,
  d6.drug_concept_id           AS d6_concept_id,
  d7.drug_concept_id           AS d7_concept_id,
  d8.drug_concept_id           AS d8_concept_id,
  d9.drug_concept_id           AS d9_concept_id,
  d10.drug_concept_id          AS d10_concept_id,
  d11.drug_concept_id          AS d11_concept_id,
  d12.drug_concept_id          AS d12_concept_id,
  d13.drug_concept_id          AS d13_concept_id,
  d14.drug_concept_id          AS d14_concept_id,
  d15.drug_concept_id          AS d15_concept_id,
  d16.drug_concept_id          AS d16_concept_id,
  d17.drug_concept_id          AS d17_concept_id,
  d18.drug_concept_id          AS d18_concept_id,
  d19.drug_concept_id          AS d19_concept_id,
  d20.drug_concept_id          AS d20_concept_id,
  d1.concept_name              AS d1_concept_name,
  d2.concept_name              AS d2_concept_name,
  d3.concept_name              AS d3_concept_name,
  d4.concept_name              AS d4_concept_name,
  d5.concept_name              AS d5_concept_name,
  d6.concept_name              AS d6_concept_name,
  d7.concept_name              AS d7_concept_name,
  d8.concept_name              AS d8_concept_name,
  d9.concept_name              AS d9_concept_name,
  d10.concept_name             AS d10_concept_name,
  d11.concept_name             AS d11_concept_name,
  d12.concept_name             AS d12_concept_name,
  d13.concept_name             AS d13_concept_name,
  d14.concept_name             AS d14_concept_name,
  d15.concept_name             AS d15_concept_name,
  d16.concept_name             AS d16_concept_name,
  d17.concept_name             AS d17_concept_name,
  d18.concept_name             AS d18_concept_name,
  d19.concept_name             AS d19_concept_name,
  d20.concept_name             AS d20_concept_name,
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
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 6) d6
    ON d1.person_id = d6.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 7) d7
    ON d1.person_id = d7.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 8) d8
    ON d1.person_id = d8.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 9) d9
    ON d1.person_id = d9.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 10) d10
    ON d1.person_id = d10.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 11) d11
    ON d1.person_id = d11.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 12) d12
    ON d1.person_id = d12.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 13) d13
    ON d1.person_id = d13.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 14) d14
    ON d1.person_id = d14.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 15) d15
    ON d1.person_id = d15.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 16) d16
    ON d1.person_id = d16.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 17) d17
    ON d1.person_id = d17.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 18) d18
    ON d1.person_id = d18.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 19) d19
    ON d1.person_id = d19.person_id
  LEFT JOIN
  (SELECT *
   FROM @resultsSchema.@databaseName_@studyName_drug_seq_processed
   WHERE drug_seq = 20) d20
    ON d1.person_id = d20.person_id
GROUP BY
  d1.index_year,
  d1.drug_concept_id,
  d2.drug_concept_id,
  d3.drug_concept_id,
  d4.drug_concept_id,
  d5.drug_concept_id,
  d6.drug_concept_id,
  d7.drug_concept_id,
  d8.drug_concept_id,
  d9.drug_concept_id,
  d10.drug_concept_id,
  d11.drug_concept_id,
  d12.drug_concept_id,
  d13.drug_concept_id,
  d14.drug_concept_id,
  d15.drug_concept_id,
  d16.drug_concept_id,
  d17.drug_concept_id,
  d18.drug_concept_id,
  d19.drug_concept_id,
  d20.drug_concept_id,
  d1.concept_name,
  d2.concept_name,
  d3.concept_name,
  d4.concept_name,
  d5.concept_name,
  d6.concept_name,
  d7.concept_name,
  d8.concept_name,
  d9.concept_name,
  d10.concept_name,
  d11.concept_name,
  d12.concept_name,
  d13.concept_name,
  d14.concept_name,
  d15.concept_name,
  d16.concept_name,
  d17.concept_name,
  d18.concept_name,
  d19.concept_name,
  d20.concept_name;


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

