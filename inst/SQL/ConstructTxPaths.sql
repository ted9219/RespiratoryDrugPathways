/*********************************************************************************
# Copyright 2014-2015 Observational Health Data Sciences and Informatics
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
********************************************************************************/


/************************
script to create treatment patterns among patients with a disease
last revised: 1 Feb 2015
author:  Jon Duke
description:
create cohort of patients with index at first treatment.
patients must have >365d prior observation and >365d of follow-up.
patients must have >1 diagnosis during their observation.
patients must have >1 treatment every 120d from index through 365d.
for each patient, we summarize the sequence of treatments (active ingredients, ordered by first date of dispensing)
we then count the number of persons with the same sequence of treatments
the results queries allow you to remove small cell counts before producing the final summary tables as needed

*************************/

--{DEFAULT @cdmSchema = 'cdmSchema'}  /*cdmSchema:  @cdmSchema*/
--{DEFAULT @resultsSchema = 'resultsSchema'}  /*resultsSchema:  @resultsSchema*/
--{DEFAULT @studyName = 'TxPath'} /*studyName:  @studyName*/
--{DEFAULT @sourceName = 'source'} /*sourceName:  @sourceName*/
--{DEFAULT @targetCohortId = '1'}
--{DEFAULT @outcomeCohortIds = '2,3'}
--{DEFAULT @cohortTable = 'resp_drug_study_cohorts'}
--{DEFAULT @txlist = '21600381,21601461,21601560,21601664,21601744,21601782'} /*txlist:  @txlist*/
--{DEFAULT @dxlist = '316866'} /*dxlist: @dxlist*/
--{DEFAULT @excludedxlist = '444094'} /*excludedxlist:  @excludedxlist*/

USE @resultsSchema;

--For Oracle: drop temp tables if they already exist
IF OBJECT_ID('@studyName_matchcohort', 'U') IS NOT NULL
DROP TABLE @studyName_matchcohort;

IF OBJECT_ID('@studyName_drug_seq_temp', 'U') IS NOT NULL
DROP TABLE @studyName_drug_seq_temp;

IF OBJECT_ID('@studyName_drug_seq', 'U') IS NOT NULL
DROP TABLE @studyName_drug_seq;

IF OBJECT_ID('@studyName_drug_seq_summary', 'U') IS NOT NULL
DROP TABLE @studyName_drug_seq_summary;

IF OBJECT_ID('@studyName_labels', 'U') IS NOT NULL
DROP TABLE @studyName_labels;

IF OBJECT_ID('@studyName_@sourceName_summary', 'U') IS NOT NULL
DROP TABLE @studyName_ @sourceName_summary;

IF OBJECT_ID('@studyName_@sourceName_person_cnt', 'U') IS NOT NULL
DROP TABLE @studyName_ @sourceName_person_cnt;

IF OBJECT_ID('@studyName_@sourceName_seq_cnt', 'U') IS NOT NULL
DROP TABLE @studyName_ @sourceName_seq_cnt;

-- Create target population
CREATE TABLE @studyName_matchcohort
(
PERSON_ID BIGINT NOT NULL PRIMARY KEY,
INDEX_DATE date NOT NULL,
COHORT_END_DATE date NOT NULL
);

-- Note: subject_id is equal to person_id, cohort_start_date is equal to index_date, cohort_end_date is equal to cohort_end_date
INSERT INTO @studyName_matchcohort (PERSON_ID, INDEX_DATE, COHORT_END_DATE)
SELECT
  c.subject_id,
  c.cohort_start_date,
  c.cohort_end_date
FROM @cohortTable C
WHERE C.cohort_definition_id = @targetCohortId;

-- Find all drugs that the matching cohort had taken

-- new code
DROP TABLE IF EXISTS results.txpath_drug_seq_temp;

CREATE TABLE results.txpath_drug_seq_temp
(
  person_id       BIGINT,
  drug_concept_id INT,
  drug_start_date DATE,
  drug_end_date   DATE,
  drug_seq        INT,
  duration_era    INT,
    gap_same             INT,
  gap             INT,
  previous_drug             INT
);

INSERT INTO results.txpath_drug_seq_temp (person_id, drug_concept_id, drug_start_date, drug_end_date, drug_seq, duration_era, gap_same, gap, previous_drug)
  SELECT
    de.subject_id,
    de.cohort_definition_id,
    de.cohort_start_date,
    de.cohort_end_date,
    row_number()
    OVER (
      PARTITION BY de.subject_id
      ORDER BY de.cohort_start_date )                                                     AS drug_seq,
    date_part('day', de.cohort_end_date :: TIMESTAMP - de.cohort_start_date :: TIMESTAMP) AS duration_era,
    date_part('day', de.cohort_start_date :: TIMESTAMP - (lag(de.cohort_end_date)
    OVER (
      PARTITION BY de.subject_id, de.cohort_definition_id
      ORDER BY de.cohort_start_date )) :: TIMESTAMP)                                      AS gap_same,
    date_part('day', de.cohort_start_date :: TIMESTAMP - (lag(de.cohort_end_date)
    OVER (
      PARTITION BY de.subject_id
      ORDER BY de.cohort_start_date )) :: TIMESTAMP)                                      AS gap,
    lag(de.cohort_definition_id)
    OVER (
      PARTITION BY de.subject_id
      ORDER BY de.cohort_start_date )                                                     AS previous_drug_concept_id
  FROM
    (SELECT *
     FROM results.resp_drug_study_cohorts c
     WHERE c.cohort_definition_id IN (5, 6, 12, 13, 16, 17, 21, 24, 25, 30, 32)) de
    INNER JOIN results.txpath_matchcohort c1
      ON de.subject_id = c1.person_id
  ORDER BY de.subject_id;

-- Apply restrictions etc.
-- Min era duration
SELECT *
FROM results.txpath_drug_seq_temp
where duration_era > 7; -- @minEraDuration

-- TODO: check era collapse in cohort definitions of outcome

-- Also add era collapse size
-- SELECT *
-- FROM results.txpath_drug_seq_temp
-- where duration_era > 7 AND (gap_same > 30 OR gap_same IS NULL); -- @minEraDuration

-- TODO: carefully test this!
SELECT t.person_id, t.drug_concept_id, t.drug_seq, min(t.drug_start_date) as drug_start_date, max(t.drug_end_date) as drug_end_date, max(duration_era) as duration_era
FROM (
SELECT
  person_id, drug_concept_id, drug_start_date,drug_end_date,
   CASE WHEN gap_same < 30 THEN --@eraCollapseSize
     date_part('day', drug_end_date :: TIMESTAMP -  lag(drug_start_date) OVER (
      PARTITION BY person_id, drug_concept_id
      ORDER BY drug_seq ) :: TIMESTAMP)
  ELSE duration_era END duration_era,
CASE WHEN gap_same < 30 THEN --@eraCollapseSize
    lag(drug_seq)OVER (
      PARTITION BY person_id, drug_concept_id
      ORDER BY drug_seq )
  ELSE drug_seq END drug_seq
FROM results.txpath_drug_seq_temp
where duration_era > 7) t -- @minEraDuration
group by t.person_id, t.drug_concept_id, t.drug_seq;

-- Also add combination window
-- TODO: add @combinationWindow


CREATE TABLE @studyName_labels --@studyName_labels
(
cohort_definition_id INT,
concept_name VARCHAR (255)
);

INSERT INTO @studyName_labels (cohort_definition_id, concept_name)
VALUES @labels;

UPDATE @studyName_labels
SET concept_name = TRIM (concept_name);

-- old code
CREATE TABLE @studyName_drug_seq AS
SELECT *
FROM @studyName_drug_seq_temp t1
LEFT JOIN @studyName_labels t2
ON t1.drug_concept_id = t2.cohort_definition_id;

-- Add column concept_name and limit to first treatments of each group
-- TODO: add back index_year, concept_name)
-- TODO: check if/else statement working
IF @firstTreatment= TRUE THEN
CREATE TABLE @studyName_drug_seq AS
SELECT t1.*, concept_name
FROM (SELECT DISTINCT
        person_id,
        drug_concept_id,
        first_value(drug_seq)
        OVER (
          PARTITION BY person_id, drug_concept_id
          ORDER BY drug_seq ASC ) AS drug_seq,
        first_value(duration_era)
        OVER (
          PARTITION BY person_id, drug_concept_id
          ORDER BY drug_seq ASC ) AS duration_era
      FROM results.txpath_drug_seq_temp
      ORDER BY person_id) t1
  LEFT JOIN @studyName_labels t2
ON t1.drug_concept_id = t2.cohort_definition_id;
ELSE
CREATE TABLE @studyName_drug_seq AS
SELECT person_id, drug_concept_id, drug_seq, duration_era, concept_name
FROM @studyName_drug_seq_temp t1
LEFT JOIN @studyName_labels t2
ON t1.drug_concept_id = t2.cohort_definition_id;
END IF;


-- old code

-- TODO: add back references, index_year
CREATE TABLE @studyName_drug_seq_temp
(
person_id BIGINT,
index_year INT,
drug_concept_id INT,
drug_seq INT
);

INSERT INTO @studyName_drug_seq_temp (person_id, index_year, drug_concept_id, drug_seq)
SELECT
  de1.subject_id,
  de1.index_year,
  de1.cohort_definition_id,
  row_number()
  OVER (
    PARTITION BY de1.subject_id
    ORDER BY de1.drug_start_date ) AS rn1
FROM
  (SELECT
     de0.subject_id,
     de0.cohort_definition_id,
     year(c1.index_date)        AS index_year,
     min(de0.cohort_start_date) AS drug_start_date
   FROM
     (SELECT *
      FROM @cohortTable C
            WHERE C.cohort_definition_id IN (@outcomeCohortIds)) de0
     INNER JOIN @studyName_matchcohort c1
                 ON de0.subject_id = c1.person_id
  GROUP BY de0.subject_id, de0.cohort_definition_id, YEAR (c1.index_date)
  ) de1;

-- Summarize the unique treatment sequences observed
CREATE TABLE @studyName_drug_seq_summary
(
index_year INT,
d1_concept_id INT,
d2_concept_id INT,
d3_concept_id INT,
d4_concept_id INT,
d5_concept_id INT,
d6_concept_id INT,
d7_concept_id INT,
d8_concept_id INT,
d9_concept_id INT,
d10_concept_id INT,
d11_concept_id INT,
d12_concept_id INT,
d13_concept_id INT,
d14_concept_id INT,
d15_concept_id INT,
d16_concept_id INT,
d17_concept_id INT,
d18_concept_id INT,
d19_concept_id INT,
d20_concept_id INT,
d1_concept_name VARCHAR (255),
d2_concept_name VARCHAR (255),
d3_concept_name VARCHAR (255),
d4_concept_name VARCHAR (255),
d5_concept_name VARCHAR (255),
d6_concept_name VARCHAR (255),
d7_concept_name VARCHAR (255),
d8_concept_name VARCHAR (255),
d9_concept_name VARCHAR (255),
d10_concept_name VARCHAR (255),
d11_concept_name VARCHAR (255),
d12_concept_name VARCHAR (255),
d13_concept_name VARCHAR (255),
d14_concept_name VARCHAR (255),
d15_concept_name VARCHAR (255),
d16_concept_name VARCHAR (255),
d17_concept_name VARCHAR (255),
d18_concept_name VARCHAR (255),
d19_concept_name VARCHAR (255),
d20_concept_name VARCHAR (255),
num_persons INT
);

INSERT INTO @studyName_drug_seq_summary (index_year, d1_concept_id, d2_concept_id, d3_concept_id, d4_concept_id, d5_concept_id, d6_concept_id, d7_concept_id, d8_concept_id, d9_concept_id, d10_concept_id, d11_concept_id, d12_concept_id, d13_concept_id, d14_concept_id, d15_concept_id, d16_concept_id, d17_concept_id, d18_concept_id, d19_concept_id, d20_concept_id, d1_concept_name, d2_concept_name, d3_concept_name, d4_concept_name, d5_concept_name, d6_concept_name, d7_concept_name, d8_concept_name, d9_concept_name, d10_concept_name, d11_concept_name, d12_concept_name, d13_concept_name, d14_concept_name, d15_concept_name, d16_concept_name, d17_concept_name, d18_concept_name, d19_concept_name, d20_concept_name, num_persons)
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
   FROM @studyName_drug_seq
   WHERE drug_seq = 1) d1
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 2) d2
    ON d1.person_id = d2.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 3) d3
    ON d1.person_id = d3.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 4) d4
    ON d1.person_id = d4.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 5) d5
    ON d1.person_id = d5.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 6) d6
    ON d1.person_id = d6.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 7) d7
    ON d1.person_id = d7.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 8) d8
    ON d1.person_id = d8.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 9) d9
    ON d1.person_id = d9.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 10) d10
    ON d1.person_id = d10.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 11) d11
    ON d1.person_id = d11.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 12) d12
    ON d1.person_id = d12.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 13) d13
    ON d1.person_id = d13.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 14) d14
    ON d1.person_id = d14.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 15) d15
    ON d1.person_id = d15.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 16) d16
    ON d1.person_id = d16.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 17) d17
    ON d1.person_id = d17.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 18) d18
    ON d1.person_id = d18.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
   WHERE drug_seq = 19) d19
    ON d1.person_id = d19.person_id
  LEFT JOIN
  (SELECT *
   FROM @studyName_drug_seq
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


{@smallcellcount != 1} ? {

CREATE TABLE @studyName_drug_seq_summary_temp
(
index_year INT,
d1_concept_id INT,
d2_concept_id INT,
d3_concept_id INT,
d4_concept_id INT,
d5_concept_id INT,
d6_concept_id INT,
d7_concept_id INT,
d8_concept_id INT,
d9_concept_id INT,
d10_concept_id INT,
d11_concept_id INT,
d12_concept_id INT,
d13_concept_id INT,
d14_concept_id INT,
d15_concept_id INT,
d16_concept_id INT,
d17_concept_id INT,
d18_concept_id INT,
d19_concept_id INT,
d20_concept_id INT,
d1_concept_name VARCHAR (255),
d2_concept_name VARCHAR (255),
d3_concept_name VARCHAR (255),
d4_concept_name VARCHAR (255),
d5_concept_name VARCHAR (255),
d6_concept_name VARCHAR (255),
d7_concept_name VARCHAR (255),
d8_concept_name VARCHAR (255),
d9_concept_name VARCHAR (255),
d10_concept_name VARCHAR (255),
d11_concept_name VARCHAR (255),
d12_concept_name VARCHAR (255),
d13_concept_name VARCHAR (255),
d14_concept_name VARCHAR (255),
d15_concept_name VARCHAR (255),
d16_concept_name VARCHAR (255),
d17_concept_name VARCHAR (255),
d18_concept_name VARCHAR (255),
d19_concept_name VARCHAR (255),
d20_concept_name VARCHAR (255),
num_persons INT
);


INSERT INTO @studyName_drug_seq_summary_temp (index_year, d1_concept_id, d2_concept_id, d3_concept_id, d4_concept_id, d5_concept_id, d6_concept_id, d7_concept_id, d8_concept_id, d9_concept_id, d10_concept_id, d11_concept_id, d12_concept_id, d13_concept_id, d14_concept_id, d15_concept_id, d16_concept_id, d17_concept_id, d18_concept_id, d19_concept_id, d20_concept_id, d1_concept_name, d2_concept_name, d3_concept_name, d4_concept_name, d5_concept_name, d6_concept_name, d7_concept_name, d8_concept_name, d9_concept_name, d10_concept_name, d11_concept_name, d12_concept_name, d13_concept_name, d14_concept_name, d15_concept_name, d16_concept_name, d17_concept_name, d18_concept_name, d19_concept_name, d20_concept_name, num_persons)
SELECT
  index_year,
  d1_concept_id,
  d2_concept_id,
  d3_concept_id,
  d4_concept_id,
  d5_concept_id,
  d6_concept_id,
  d7_concept_id,
  d8_concept_id,
  d9_concept_id,
  d10_concept_id,
  d11_concept_id,
  d12_concept_id,
  d13_concept_id,
  d14_concept_id,
  d15_concept_id,
  d16_concept_id,
  d17_concept_id,
  d18_concept_id,
  d19_concept_id,
  d20_concept_id,
  d1_concept_name,
  d2_concept_name,
  d3_concept_name,
  d4_concept_name,
  d5_concept_name,
  d6_concept_name,
  d7_concept_name,
  d8_concept_name,
  d9_concept_name,
  d10_concept_name,
  d11_concept_name,
  d12_concept_name,
  d13_concept_name,
  d14_concept_name,
  d15_concept_name,
  d16_concept_name,
  d17_concept_name,
  d18_concept_name,
  d19_concept_name,
  d20_concept_name,
  num_persons
FROM
@studyName_drug_seq_summary;

DELETE FROM @studyName_drug_seq_summary;


INSERT INTO @studyName_drug_seq_summary (index_year, d1_concept_id, d2_concept_id, d3_concept_id, d4_concept_id, d5_concept_id, d6_concept_id, d7_concept_id, d8_concept_id, d9_concept_id, d10_concept_id, d11_concept_id, d12_concept_id, d13_concept_id, d14_concept_id, d15_concept_id, d16_concept_id, d17_concept_id, d18_concept_id, d19_concept_id, d20_concept_id, d1_concept_name, d2_concept_name, d3_concept_name, d4_concept_name, d5_concept_name, d6_concept_name, d7_concept_name, d8_concept_name, d9_concept_name, d10_concept_name, d11_concept_name, d12_concept_name, d13_concept_name, d14_concept_name, d15_concept_name, d16_concept_name, d17_concept_name, d18_concept_name, d19_concept_name, d20_concept_name, num_persons)
SELECT
  index_year,
  d1_concept_id,
  d2_concept_id,
  d3_concept_id,
  d4_concept_id,
  d5_concept_id,
  d6_concept_id,
  d7_concept_id,
  d8_concept_id,
  d9_concept_id,
  d10_concept_id,
  d11_concept_id,
  d12_concept_id,
  d13_concept_id,
  d14_concept_id,
  d15_concept_id,
  d16_concept_id,
  d17_concept_id,
  d18_concept_id,
  d19_concept_id,
  d20_concept_id,
  d1_concept_name,
  d2_concept_name,
  d3_concept_name,
  d4_concept_name,
  d5_concept_name,
  d6_concept_name,
  d7_concept_name,
  d8_concept_name,
  d9_concept_name,
  d10_concept_name,
  d11_concept_name,
  d12_concept_name,
  d13_concept_name,
  d14_concept_name,
  d15_concept_name,
  d16_concept_name,
  d17_concept_name,
  d18_concept_name,
  d19_concept_name,
  d20_concept_name,
  num_persons
FROM
  (
    SELECT
      index_year,
      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      ELSE d1_concept_id END    AS d1_concept_id,
      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d1_concept_id = -1
        THEN NULL
      ELSE d2_concept_id END    AS d2_concept_id,
      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d2_concept_id = -1
        THEN NULL
      ELSE d3_concept_id END    AS d3_concept_id,
      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d3_concept_id = -1
        THEN NULL
      ELSE d4_concept_id END    AS d4_concept_id,
      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d4_concept_id = -1
        THEN NULL
      ELSE d5_concept_id END    AS d5_concept_id,
      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d5_concept_id = -1
        THEN NULL
      ELSE d6_concept_id END    AS d6_concept_id,
      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d6_concept_id = -1
        THEN NULL
      ELSE d7_concept_id END    AS d7_concept_id,
      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d7_concept_id = -1
        THEN NULL
      ELSE d8_concept_id END    AS d8_concept_id,
      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d8_concept_id = -1
        THEN NULL
      ELSE d9_concept_id END    AS d9_concept_id,
      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d9_concept_id = -1
        THEN NULL
      ELSE d10_concept_id END   AS d10_concept_id,
      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d10_concept_id = -1
        THEN NULL
      ELSE d11_concept_id END   AS d11_concept_id,
      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d11_concept_id = -1
        THEN NULL
      ELSE d12_concept_id END   AS d12_concept_id,
      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d12_concept_id = -1
        THEN NULL
      ELSE d13_concept_id END   AS d13_concept_id,
      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d13_concept_id = -1
        THEN NULL
      ELSE d14_concept_id END   AS d14_concept_id,
      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d14_concept_id = -1
        THEN NULL
      ELSE d15_concept_id END   AS d15_concept_id,
      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d15_concept_id = -1
        THEN NULL
      ELSE d16_concept_id END   AS d16_concept_id,
      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d16_concept_id = -1
        THEN NULL
      ELSE d17_concept_id END   AS d17_concept_id,
      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d17_concept_id = -1
        THEN NULL
      ELSE d18_concept_id END   AS d18_concept_id,
      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d18_concept_id = -1
        THEN NULL
      ELSE d19_concept_id END   AS d19_concept_id,
      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
        THEN -1
      WHEN d19_concept_id = -1
        THEN NULL
      ELSE d20_concept_id END   AS d20_concept_id,
      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      ELSE d1_concept_name END  AS d1_concept_name,
      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d1_concept_id = -1
        THEN NULL
      ELSE d2_concept_name END  AS d2_concept_name,
      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d2_concept_id = -1
        THEN NULL
      ELSE d3_concept_name END  AS d3_concept_name,
      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d3_concept_id = -1
        THEN NULL
      ELSE d4_concept_name END  AS d4_concept_name,
      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d4_concept_id = -1
        THEN NULL
      ELSE d5_concept_name END  AS d5_concept_name,
      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d5_concept_id = -1
        THEN NULL
      ELSE d6_concept_name END  AS d6_concept_name,
      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d6_concept_id = -1
        THEN NULL
      ELSE d7_concept_name END  AS d7_concept_name,
      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d7_concept_id = -1
        THEN NULL
      ELSE d8_concept_name END  AS d8_concept_name,
      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d8_concept_id = -1
        THEN NULL
      ELSE d9_concept_name END  AS d9_concept_name,
      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d9_concept_id = -1
        THEN NULL
      ELSE d10_concept_name END AS d10_concept_name,
      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d10_concept_id = -1
        THEN NULL
      ELSE d11_concept_name END AS d11_concept_name,
      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d11_concept_id = -1
        THEN NULL
      ELSE d12_concept_name END AS d12_concept_name,
      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d12_concept_id = -1
        THEN NULL
      ELSE d13_concept_name END AS d13_concept_name,
      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d13_concept_id = -1
        THEN NULL
      ELSE d14_concept_name END AS d14_concept_name,
      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d14_concept_id = -1
        THEN NULL
      ELSE d15_concept_name END AS d15_concept_name,
      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d15_concept_id = -1
        THEN NULL
      ELSE d16_concept_name END AS d16_concept_name,
      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d16_concept_id = -1
        THEN NULL
      ELSE d17_concept_name END AS d17_concept_name,
      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d17_concept_id = -1
        THEN NULL
      ELSE d18_concept_name END AS d18_concept_name,
      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d18_concept_id = -1
        THEN NULL
      ELSE d19_concept_name END AS d19_concept_name,
      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d19_concept_id = -1
        THEN NULL
      ELSE d20_concept_name END AS d20_concept_name,
      sum(num_persons)          AS num_persons
    FROM
      (
        SELECT
          index_year,
          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          ELSE d1_concept_id END    AS d1_concept_id,
          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d1_concept_id = -1
            THEN NULL
          ELSE d2_concept_id END    AS d2_concept_id,
          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d2_concept_id = -1
            THEN NULL
          ELSE d3_concept_id END    AS d3_concept_id,
          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d3_concept_id = -1
            THEN NULL
          ELSE d4_concept_id END    AS d4_concept_id,
          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d4_concept_id = -1
            THEN NULL
          ELSE d5_concept_id END    AS d5_concept_id,
          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d5_concept_id = -1
            THEN NULL
          ELSE d6_concept_id END    AS d6_concept_id,
          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d6_concept_id = -1
            THEN NULL
          ELSE d7_concept_id END    AS d7_concept_id,
          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d7_concept_id = -1
            THEN NULL
          ELSE d8_concept_id END    AS d8_concept_id,
          CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d8_concept_id = -1
            THEN NULL
          ELSE d9_concept_id END    AS d9_concept_id,
          CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d9_concept_id = -1
            THEN NULL
          ELSE d10_concept_id END   AS d10_concept_id,
          CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d10_concept_id = -1
            THEN NULL
          ELSE d11_concept_id END   AS d11_concept_id,
          CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d11_concept_id = -1
            THEN NULL
          ELSE d12_concept_id END   AS d12_concept_id,
          CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d12_concept_id = -1
            THEN NULL
          ELSE d13_concept_id END   AS d13_concept_id,
          CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d13_concept_id = -1
            THEN NULL
          ELSE d14_concept_id END   AS d14_concept_id,
          CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d14_concept_id = -1
            THEN NULL
          ELSE d15_concept_id END   AS d15_concept_id,
          CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d15_concept_id = -1
            THEN NULL
          ELSE d16_concept_id END   AS d16_concept_id,
          CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d16_concept_id = -1
            THEN NULL
          ELSE d17_concept_id END   AS d17_concept_id,
          CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d17_concept_id = -1
            THEN NULL
          ELSE d18_concept_id END   AS d18_concept_id,
          CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d18_concept_id = -1
            THEN NULL
          ELSE d19_concept_id END   AS d19_concept_id,
          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
            THEN -1
          WHEN d19_concept_id = -1
            THEN NULL
          ELSE d20_concept_id END   AS d20_concept_id,
          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          ELSE d1_concept_name END  AS d1_concept_name,
          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d1_concept_id = -1
            THEN NULL
          ELSE d2_concept_name END  AS d2_concept_name,
          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d2_concept_id = -1
            THEN NULL
          ELSE d3_concept_name END  AS d3_concept_name,
          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d3_concept_id = -1
            THEN NULL
          ELSE d4_concept_name END  AS d4_concept_name,
          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d4_concept_id = -1
            THEN NULL
          ELSE d5_concept_name END  AS d5_concept_name,
          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d5_concept_id = -1
            THEN NULL
          ELSE d6_concept_name END  AS d6_concept_name,
          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d6_concept_id = -1
            THEN NULL
          ELSE d7_concept_name END  AS d7_concept_name,
          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d7_concept_id = -1
            THEN NULL
          ELSE d8_concept_name END  AS d8_concept_name,
          CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d8_concept_id = -1
            THEN NULL
          ELSE d9_concept_name END  AS d9_concept_name,
          CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d9_concept_id = -1
            THEN NULL
          ELSE d10_concept_name END AS d10_concept_name,
          CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d10_concept_id = -1
            THEN NULL
          ELSE d11_concept_name END AS d11_concept_name,
          CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d11_concept_id = -1
            THEN NULL
          ELSE d12_concept_name END AS d12_concept_name,
          CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d12_concept_id = -1
            THEN NULL
          ELSE d13_concept_name END AS d13_concept_name,
          CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d13_concept_id = -1
            THEN NULL
          ELSE d14_concept_name END AS d14_concept_name,
          CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d14_concept_id = -1
            THEN NULL
          ELSE d15_concept_name END AS d15_concept_name,
          CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d15_concept_id = -1
            THEN NULL
          ELSE d16_concept_name END AS d16_concept_name,
          CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d16_concept_id = -1
            THEN NULL
          ELSE d17_concept_name END AS d17_concept_name,
          CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d17_concept_id = -1
            THEN NULL
          ELSE d18_concept_name END AS d18_concept_name,
          CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d18_concept_id = -1
            THEN NULL
          ELSE d19_concept_name END AS d19_concept_name,
          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
            THEN 'Other'
          WHEN d19_concept_id = -1
            THEN NULL
          ELSE d20_concept_name END AS d20_concept_name,
          sum(num_persons)          AS num_persons
        FROM
          (
            SELECT
              index_year,
              CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              ELSE d1_concept_id END    AS d1_concept_id,
              CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d1_concept_id = -1
                THEN NULL
              ELSE d2_concept_id END    AS d2_concept_id,
              CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d2_concept_id = -1
                THEN NULL
              ELSE d3_concept_id END    AS d3_concept_id,
              CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d3_concept_id = -1
                THEN NULL
              ELSE d4_concept_id END    AS d4_concept_id,
              CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d4_concept_id = -1
                THEN NULL
              ELSE d5_concept_id END    AS d5_concept_id,
              CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d5_concept_id = -1
                THEN NULL
              ELSE d6_concept_id END    AS d6_concept_id,
              CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d6_concept_id = -1
                THEN NULL
              ELSE d7_concept_id END    AS d7_concept_id,
              CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d7_concept_id = -1
                THEN NULL
              ELSE d8_concept_id END    AS d8_concept_id,
              CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d8_concept_id = -1
                THEN NULL
              ELSE d9_concept_id END    AS d9_concept_id,
              CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d9_concept_id = -1
                THEN NULL
              ELSE d10_concept_id END   AS d10_concept_id,
              CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d10_concept_id = -1
                THEN NULL
              ELSE d11_concept_id END   AS d11_concept_id,
              CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d11_concept_id = -1
                THEN NULL
              ELSE d12_concept_id END   AS d12_concept_id,
              CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d12_concept_id = -1
                THEN NULL
              ELSE d13_concept_id END   AS d13_concept_id,
              CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d13_concept_id = -1
                THEN NULL
              ELSE d14_concept_id END   AS d14_concept_id,
              CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d14_concept_id = -1
                THEN NULL
              ELSE d15_concept_id END   AS d15_concept_id,
              CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d15_concept_id = -1
                THEN NULL
              ELSE d16_concept_id END   AS d16_concept_id,
              CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d16_concept_id = -1
                THEN NULL
              ELSE d17_concept_id END   AS d17_concept_id,
              CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d17_concept_id = -1
                THEN NULL
              ELSE d18_concept_id END   AS d18_concept_id,
              CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d18_concept_id = -1
                THEN NULL
              ELSE d19_concept_id END   AS d19_concept_id,
              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                THEN -1
              WHEN d19_concept_id = -1
                THEN NULL
              ELSE d20_concept_id END   AS d20_concept_id,
              CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              ELSE d1_concept_name END  AS d1_concept_name,
              CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d1_concept_id = -1
                THEN NULL
              ELSE d2_concept_name END  AS d2_concept_name,
              CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d2_concept_id = -1
                THEN NULL
              ELSE d3_concept_name END  AS d3_concept_name,
              CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d3_concept_id = -1
                THEN NULL
              ELSE d4_concept_name END  AS d4_concept_name,
              CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d4_concept_id = -1
                THEN NULL
              ELSE d5_concept_name END  AS d5_concept_name,
              CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d5_concept_id = -1
                THEN NULL
              ELSE d6_concept_name END  AS d6_concept_name,
              CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d6_concept_id = -1
                THEN NULL
              ELSE d7_concept_name END  AS d7_concept_name,
              CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d7_concept_id = -1
                THEN NULL
              ELSE d8_concept_name END  AS d8_concept_name,
              CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d8_concept_id = -1
                THEN NULL
              ELSE d9_concept_name END  AS d9_concept_name,
              CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d9_concept_id = -1
                THEN NULL
              ELSE d10_concept_name END AS d10_concept_name,
              CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d10_concept_id = -1
                THEN NULL
              ELSE d11_concept_name END AS d11_concept_name,
              CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d11_concept_id = -1
                THEN NULL
              ELSE d12_concept_name END AS d12_concept_name,
              CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d12_concept_id = -1
                THEN NULL
              ELSE d13_concept_name END AS d13_concept_name,
              CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d13_concept_id = -1
                THEN NULL
              ELSE d14_concept_name END AS d14_concept_name,
              CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d14_concept_id = -1
                THEN NULL
              ELSE d15_concept_name END AS d15_concept_name,
              CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d15_concept_id = -1
                THEN NULL
              ELSE d16_concept_name END AS d16_concept_name,
              CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d16_concept_id = -1
                THEN NULL
              ELSE d17_concept_name END AS d17_concept_name,
              CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d17_concept_id = -1
                THEN NULL
              ELSE d18_concept_name END AS d18_concept_name,
              CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d18_concept_id = -1
                THEN NULL
              ELSE d19_concept_name END AS d19_concept_name,
              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                THEN 'Other'
              WHEN d19_concept_id = -1
                THEN NULL
              ELSE d20_concept_name END AS d20_concept_name,
              sum(num_persons)          AS num_persons
            FROM
              (
                SELECT
                  index_year,
                  CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  ELSE d1_concept_id END    AS d1_concept_id,
                  CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d1_concept_id = -1
                    THEN NULL
                  ELSE d2_concept_id END    AS d2_concept_id,
                  CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d2_concept_id = -1
                    THEN NULL
                  ELSE d3_concept_id END    AS d3_concept_id,
                  CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d3_concept_id = -1
                    THEN NULL
                  ELSE d4_concept_id END    AS d4_concept_id,
                  CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d4_concept_id = -1
                    THEN NULL
                  ELSE d5_concept_id END    AS d5_concept_id,
                  CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d5_concept_id = -1
                    THEN NULL
                  ELSE d6_concept_id END    AS d6_concept_id,
                  CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d6_concept_id = -1
                    THEN NULL
                  ELSE d7_concept_id END    AS d7_concept_id,
                  CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d7_concept_id = -1
                    THEN NULL
                  ELSE d8_concept_id END    AS d8_concept_id,
                  CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d8_concept_id = -1
                    THEN NULL
                  ELSE d9_concept_id END    AS d9_concept_id,
                  CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d9_concept_id = -1
                    THEN NULL
                  ELSE d10_concept_id END   AS d10_concept_id,
                  CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d10_concept_id = -1
                    THEN NULL
                  ELSE d11_concept_id END   AS d11_concept_id,
                  CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d11_concept_id = -1
                    THEN NULL
                  ELSE d12_concept_id END   AS d12_concept_id,
                  CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d12_concept_id = -1
                    THEN NULL
                  ELSE d13_concept_id END   AS d13_concept_id,
                  CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d13_concept_id = -1
                    THEN NULL
                  ELSE d14_concept_id END   AS d14_concept_id,
                  CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d14_concept_id = -1
                    THEN NULL
                  ELSE d15_concept_id END   AS d15_concept_id,
                  CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d15_concept_id = -1
                    THEN NULL
                  ELSE d16_concept_id END   AS d16_concept_id,
                  CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d16_concept_id = -1
                    THEN NULL
                  ELSE d17_concept_id END   AS d17_concept_id,
                  CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d17_concept_id = -1
                    THEN NULL
                  ELSE d18_concept_id END   AS d18_concept_id,
                  CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d18_concept_id = -1
                    THEN NULL
                  ELSE d19_concept_id END   AS d19_concept_id,
                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                    THEN -1
                  WHEN d19_concept_id = -1
                    THEN NULL
                  ELSE d20_concept_id END   AS d20_concept_id,
                  CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  ELSE d1_concept_name END  AS d1_concept_name,
                  CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d1_concept_id = -1
                    THEN NULL
                  ELSE d2_concept_name END  AS d2_concept_name,
                  CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d2_concept_id = -1
                    THEN NULL
                  ELSE d3_concept_name END  AS d3_concept_name,
                  CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d3_concept_id = -1
                    THEN NULL
                  ELSE d4_concept_name END  AS d4_concept_name,
                  CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d4_concept_id = -1
                    THEN NULL
                  ELSE d5_concept_name END  AS d5_concept_name,
                  CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d5_concept_id = -1
                    THEN NULL
                  ELSE d6_concept_name END  AS d6_concept_name,
                  CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d6_concept_id = -1
                    THEN NULL
                  ELSE d7_concept_name END  AS d7_concept_name,
                  CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d7_concept_id = -1
                    THEN NULL
                  ELSE d8_concept_name END  AS d8_concept_name,
                  CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d8_concept_id = -1
                    THEN NULL
                  ELSE d9_concept_name END  AS d9_concept_name,
                  CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d9_concept_id = -1
                    THEN NULL
                  ELSE d10_concept_name END AS d10_concept_name,
                  CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d10_concept_id = -1
                    THEN NULL
                  ELSE d11_concept_name END AS d11_concept_name,
                  CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d11_concept_id = -1
                    THEN NULL
                  ELSE d12_concept_name END AS d12_concept_name,
                  CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d12_concept_id = -1
                    THEN NULL
                  ELSE d13_concept_name END AS d13_concept_name,
                  CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d13_concept_id = -1
                    THEN NULL
                  ELSE d14_concept_name END AS d14_concept_name,
                  CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d14_concept_id = -1
                    THEN NULL
                  ELSE d15_concept_name END AS d15_concept_name,
                  CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d15_concept_id = -1
                    THEN NULL
                  ELSE d16_concept_name END AS d16_concept_name,
                  CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d16_concept_id = -1
                    THEN NULL
                  ELSE d17_concept_name END AS d17_concept_name,
                  CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d17_concept_id = -1
                    THEN NULL
                  ELSE d18_concept_name END AS d18_concept_name,
                  CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d18_concept_id = -1
                    THEN NULL
                  ELSE d19_concept_name END AS d19_concept_name,
                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d19_concept_id = -1
                    THEN NULL
                  ELSE d20_concept_name END AS d20_concept_name,
                  sum(num_persons)          AS num_persons
                FROM
                  (
                    SELECT
                      index_year,
                      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      ELSE d1_concept_id END    AS d1_concept_id,
                      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d1_concept_id = -1
                        THEN NULL
                      ELSE d2_concept_id END    AS d2_concept_id,
                      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d2_concept_id = -1
                        THEN NULL
                      ELSE d3_concept_id END    AS d3_concept_id,
                      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d3_concept_id = -1
                        THEN NULL
                      ELSE d4_concept_id END    AS d4_concept_id,
                      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d4_concept_id = -1
                        THEN NULL
                      ELSE d5_concept_id END    AS d5_concept_id,
                      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d5_concept_id = -1
                        THEN NULL
                      ELSE d6_concept_id END    AS d6_concept_id,
                      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d6_concept_id = -1
                        THEN NULL
                      ELSE d7_concept_id END    AS d7_concept_id,
                      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d7_concept_id = -1
                        THEN NULL
                      ELSE d8_concept_id END    AS d8_concept_id,
                      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d8_concept_id = -1
                        THEN NULL
                      ELSE d9_concept_id END    AS d9_concept_id,
                      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d9_concept_id = -1
                        THEN NULL
                      ELSE d10_concept_id END   AS d10_concept_id,
                      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d10_concept_id = -1
                        THEN NULL
                      ELSE d11_concept_id END   AS d11_concept_id,
                      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d11_concept_id = -1
                        THEN NULL
                      ELSE d12_concept_id END   AS d12_concept_id,
                      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d12_concept_id = -1
                        THEN NULL
                      ELSE d13_concept_id END   AS d13_concept_id,
                      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d13_concept_id = -1
                        THEN NULL
                      ELSE d14_concept_id END   AS d14_concept_id,
                      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d14_concept_id = -1
                        THEN NULL
                      ELSE d15_concept_id END   AS d15_concept_id,
                      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d15_concept_id = -1
                        THEN NULL
                      ELSE d16_concept_id END   AS d16_concept_id,
                      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d16_concept_id = -1
                        THEN NULL
                      ELSE d17_concept_id END   AS d17_concept_id,
                      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d17_concept_id = -1
                        THEN NULL
                      ELSE d18_concept_id END   AS d18_concept_id,
                      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d18_concept_id = -1
                        THEN NULL
                      ELSE d19_concept_id END   AS d19_concept_id,
                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                        THEN -1
                      WHEN d19_concept_id = -1
                        THEN NULL
                      ELSE d20_concept_id END   AS d20_concept_id,
                      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      ELSE d1_concept_name END  AS d1_concept_name,
                      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d1_concept_id = -1
                        THEN NULL
                      ELSE d2_concept_name END  AS d2_concept_name,
                      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d2_concept_id = -1
                        THEN NULL
                      ELSE d3_concept_name END  AS d3_concept_name,
                      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d3_concept_id = -1
                        THEN NULL
                      ELSE d4_concept_name END  AS d4_concept_name,
                      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d4_concept_id = -1
                        THEN NULL
                      ELSE d5_concept_name END  AS d5_concept_name,
                      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d5_concept_id = -1
                        THEN NULL
                      ELSE d6_concept_name END  AS d6_concept_name,
                      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d6_concept_id = -1
                        THEN NULL
                      ELSE d7_concept_name END  AS d7_concept_name,
                      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d7_concept_id = -1
                        THEN NULL
                      ELSE d8_concept_name END  AS d8_concept_name,
                      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d8_concept_id = -1
                        THEN NULL
                      ELSE d9_concept_name END  AS d9_concept_name,
                      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d9_concept_id = -1
                        THEN NULL
                      ELSE d10_concept_name END AS d10_concept_name,
                      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d10_concept_id = -1
                        THEN NULL
                      ELSE d11_concept_name END AS d11_concept_name,
                      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d11_concept_id = -1
                        THEN NULL
                      ELSE d12_concept_name END AS d12_concept_name,
                      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d12_concept_id = -1
                        THEN NULL
                      ELSE d13_concept_name END AS d13_concept_name,
                      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d13_concept_id = -1
                        THEN NULL
                      ELSE d14_concept_name END AS d14_concept_name,
                      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d14_concept_id = -1
                        THEN NULL
                      ELSE d15_concept_name END AS d15_concept_name,
                      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d15_concept_id = -1
                        THEN NULL
                      ELSE d16_concept_name END AS d16_concept_name,
                      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d16_concept_id = -1
                        THEN NULL
                      ELSE d17_concept_name END AS d17_concept_name,
                      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d17_concept_id = -1
                        THEN NULL
                      ELSE d18_concept_name END AS d18_concept_name,
                      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d18_concept_id = -1
                        THEN NULL
                      ELSE d19_concept_name END AS d19_concept_name,
                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d19_concept_id = -1
                        THEN NULL
                      ELSE d20_concept_name END AS d20_concept_name,
                      sum(num_persons)          AS num_persons
                    FROM
                      (
                        SELECT
                          index_year,
                          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          ELSE d1_concept_id END    AS d1_concept_id,
                          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d1_concept_id = -1
                            THEN NULL
                          ELSE d2_concept_id END    AS d2_concept_id,
                          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d2_concept_id = -1
                            THEN NULL
                          ELSE d3_concept_id END    AS d3_concept_id,
                          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d3_concept_id = -1
                            THEN NULL
                          ELSE d4_concept_id END    AS d4_concept_id,
                          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d4_concept_id = -1
                            THEN NULL
                          ELSE d5_concept_id END    AS d5_concept_id,
                          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d5_concept_id = -1
                            THEN NULL
                          ELSE d6_concept_id END    AS d6_concept_id,
                          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d6_concept_id = -1
                            THEN NULL
                          ELSE d7_concept_id END    AS d7_concept_id,
                          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d7_concept_id = -1
                            THEN NULL
                          ELSE d8_concept_id END    AS d8_concept_id,
                          CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d8_concept_id = -1
                            THEN NULL
                          ELSE d9_concept_id END    AS d9_concept_id,
                          CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d9_concept_id = -1
                            THEN NULL
                          ELSE d10_concept_id END   AS d10_concept_id,
                          CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d10_concept_id = -1
                            THEN NULL
                          ELSE d11_concept_id END   AS d11_concept_id,
                          CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d11_concept_id = -1
                            THEN NULL
                          ELSE d12_concept_id END   AS d12_concept_id,
                          CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d12_concept_id = -1
                            THEN NULL
                          ELSE d13_concept_id END   AS d13_concept_id,
                          CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d13_concept_id = -1
                            THEN NULL
                          ELSE d14_concept_id END   AS d14_concept_id,
                          CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d14_concept_id = -1
                            THEN NULL
                          ELSE d15_concept_id END   AS d15_concept_id,
                          CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d15_concept_id = -1
                            THEN NULL
                          ELSE d16_concept_id END   AS d16_concept_id,
                          CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d16_concept_id = -1
                            THEN NULL
                          ELSE d17_concept_id END   AS d17_concept_id,
                          CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d17_concept_id = -1
                            THEN NULL
                          ELSE d18_concept_id END   AS d18_concept_id,
                          CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d18_concept_id = -1
                            THEN NULL
                          ELSE d19_concept_id END   AS d19_concept_id,
                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                            THEN -1
                          WHEN d19_concept_id = -1
                            THEN NULL
                          ELSE d20_concept_id END   AS d20_concept_id,
                          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          ELSE d1_concept_name END  AS d1_concept_name,
                          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d1_concept_id = -1
                            THEN NULL
                          ELSE d2_concept_name END  AS d2_concept_name,
                          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d2_concept_id = -1
                            THEN NULL
                          ELSE d3_concept_name END  AS d3_concept_name,
                          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d3_concept_id = -1
                            THEN NULL
                          ELSE d4_concept_name END  AS d4_concept_name,
                          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d4_concept_id = -1
                            THEN NULL
                          ELSE d5_concept_name END  AS d5_concept_name,
                          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d5_concept_id = -1
                            THEN NULL
                          ELSE d6_concept_name END  AS d6_concept_name,
                          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d6_concept_id = -1
                            THEN NULL
                          ELSE d7_concept_name END  AS d7_concept_name,
                          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d7_concept_id = -1
                            THEN NULL
                          ELSE d8_concept_name END  AS d8_concept_name,
                          CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d8_concept_id = -1
                            THEN NULL
                          ELSE d9_concept_name END  AS d9_concept_name,
                          CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d9_concept_id = -1
                            THEN NULL
                          ELSE d10_concept_name END AS d10_concept_name,
                          CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d10_concept_id = -1
                            THEN NULL
                          ELSE d11_concept_name END AS d11_concept_name,
                          CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d11_concept_id = -1
                            THEN NULL
                          ELSE d12_concept_name END AS d12_concept_name,
                          CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d12_concept_id = -1
                            THEN NULL
                          ELSE d13_concept_name END AS d13_concept_name,
                          CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d13_concept_id = -1
                            THEN NULL
                          ELSE d14_concept_name END AS d14_concept_name,
                          CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d14_concept_id = -1
                            THEN NULL
                          ELSE d15_concept_name END AS d15_concept_name,
                          CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d15_concept_id = -1
                            THEN NULL
                          ELSE d16_concept_name END AS d16_concept_name,
                          CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d16_concept_id = -1
                            THEN NULL
                          ELSE d17_concept_name END AS d17_concept_name,
                          CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d17_concept_id = -1
                            THEN NULL
                          ELSE d18_concept_name END AS d18_concept_name,
                          CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d18_concept_id = -1
                            THEN NULL
                          ELSE d19_concept_name END AS d19_concept_name,
                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d19_concept_id = -1
                            THEN NULL
                          ELSE d20_concept_name END AS d20_concept_name,
                          sum(num_persons)          AS num_persons
                        FROM
                          (
                            SELECT
                              index_year,
                              CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              ELSE d1_concept_id END    AS d1_concept_id,
                              CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d1_concept_id = -1
                                THEN NULL
                              ELSE d2_concept_id END    AS d2_concept_id,
                              CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d2_concept_id = -1
                                THEN NULL
                              ELSE d3_concept_id END    AS d3_concept_id,
                              CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d3_concept_id = -1
                                THEN NULL
                              ELSE d4_concept_id END    AS d4_concept_id,
                              CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d4_concept_id = -1
                                THEN NULL
                              ELSE d5_concept_id END    AS d5_concept_id,
                              CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d5_concept_id = -1
                                THEN NULL
                              ELSE d6_concept_id END    AS d6_concept_id,
                              CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d6_concept_id = -1
                                THEN NULL
                              ELSE d7_concept_id END    AS d7_concept_id,
                              CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d7_concept_id = -1
                                THEN NULL
                              ELSE d8_concept_id END    AS d8_concept_id,
                              CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d8_concept_id = -1
                                THEN NULL
                              ELSE d9_concept_id END    AS d9_concept_id,
                              CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d9_concept_id = -1
                                THEN NULL
                              ELSE d10_concept_id END   AS d10_concept_id,
                              CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d10_concept_id = -1
                                THEN NULL
                              ELSE d11_concept_id END   AS d11_concept_id,
                              CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d11_concept_id = -1
                                THEN NULL
                              ELSE d12_concept_id END   AS d12_concept_id,
                              CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d12_concept_id = -1
                                THEN NULL
                              ELSE d13_concept_id END   AS d13_concept_id,
                              CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d13_concept_id = -1
                                THEN NULL
                              ELSE d14_concept_id END   AS d14_concept_id,
                              CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d14_concept_id = -1
                                THEN NULL
                              ELSE d15_concept_id END   AS d15_concept_id,
                              CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d15_concept_id = -1
                                THEN NULL
                              ELSE d16_concept_id END   AS d16_concept_id,
                              CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d16_concept_id = -1
                                THEN NULL
                              ELSE d17_concept_id END   AS d17_concept_id,
                              CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d17_concept_id = -1
                                THEN NULL
                              ELSE d18_concept_id END   AS d18_concept_id,
                              CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d18_concept_id = -1
                                THEN NULL
                              ELSE d19_concept_id END   AS d19_concept_id,
                              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                THEN -1
                              WHEN d19_concept_id = -1
                                THEN NULL
                              ELSE d20_concept_id END   AS d20_concept_id,
                              CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              ELSE d1_concept_name END  AS d1_concept_name,
                              CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d1_concept_id = -1
                                THEN NULL
                              ELSE d2_concept_name END  AS d2_concept_name,
                              CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d2_concept_id = -1
                                THEN NULL
                              ELSE d3_concept_name END  AS d3_concept_name,
                              CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d3_concept_id = -1
                                THEN NULL
                              ELSE d4_concept_name END  AS d4_concept_name,
                              CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d4_concept_id = -1
                                THEN NULL
                              ELSE d5_concept_name END  AS d5_concept_name,
                              CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d5_concept_id = -1
                                THEN NULL
                              ELSE d6_concept_name END  AS d6_concept_name,
                              CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d6_concept_id = -1
                                THEN NULL
                              ELSE d7_concept_name END  AS d7_concept_name,
                              CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d7_concept_id = -1
                                THEN NULL
                              ELSE d8_concept_name END  AS d8_concept_name,
                              CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d8_concept_id = -1
                                THEN NULL
                              ELSE d9_concept_name END  AS d9_concept_name,
                              CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d9_concept_id = -1
                                THEN NULL
                              ELSE d10_concept_name END AS d10_concept_name,
                              CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d10_concept_id = -1
                                THEN NULL
                              ELSE d11_concept_name END AS d11_concept_name,
                              CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d11_concept_id = -1
                                THEN NULL
                              ELSE d12_concept_name END AS d12_concept_name,
                              CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d12_concept_id = -1
                                THEN NULL
                              ELSE d13_concept_name END AS d13_concept_name,
                              CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d13_concept_id = -1
                                THEN NULL
                              ELSE d14_concept_name END AS d14_concept_name,
                              CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d14_concept_id = -1
                                THEN NULL
                              ELSE d15_concept_name END AS d15_concept_name,
                              CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d15_concept_id = -1
                                THEN NULL
                              ELSE d16_concept_name END AS d16_concept_name,
                              CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d16_concept_id = -1
                                THEN NULL
                              ELSE d17_concept_name END AS d17_concept_name,
                              CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d17_concept_id = -1
                                THEN NULL
                              ELSE d18_concept_name END AS d18_concept_name,
                              CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d18_concept_id = -1
                                THEN NULL
                              ELSE d19_concept_name END AS d19_concept_name,
                              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d19_concept_id = -1
                                THEN NULL
                              ELSE d20_concept_name END AS d20_concept_name,
                              sum(num_persons)          AS num_persons
                            FROM
                              (
                                SELECT
                                  index_year,
                                  CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  ELSE d1_concept_id END    AS d1_concept_id,
                                  CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d1_concept_id = -1
                                    THEN NULL
                                  ELSE d2_concept_id END    AS d2_concept_id,
                                  CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d2_concept_id = -1
                                    THEN NULL
                                  ELSE d3_concept_id END    AS d3_concept_id,
                                  CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d3_concept_id = -1
                                    THEN NULL
                                  ELSE d4_concept_id END    AS d4_concept_id,
                                  CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d4_concept_id = -1
                                    THEN NULL
                                  ELSE d5_concept_id END    AS d5_concept_id,
                                  CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d5_concept_id = -1
                                    THEN NULL
                                  ELSE d6_concept_id END    AS d6_concept_id,
                                  CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d6_concept_id = -1
                                    THEN NULL
                                  ELSE d7_concept_id END    AS d7_concept_id,
                                  CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d7_concept_id = -1
                                    THEN NULL
                                  ELSE d8_concept_id END    AS d8_concept_id,
                                  CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d8_concept_id = -1
                                    THEN NULL
                                  ELSE d9_concept_id END    AS d9_concept_id,
                                  CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d9_concept_id = -1
                                    THEN NULL
                                  ELSE d10_concept_id END   AS d10_concept_id,
                                  CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d10_concept_id = -1
                                    THEN NULL
                                  ELSE d11_concept_id END   AS d11_concept_id,
                                  CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d11_concept_id = -1
                                    THEN NULL
                                  ELSE d12_concept_id END   AS d12_concept_id,
                                  CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d12_concept_id = -1
                                    THEN NULL
                                  ELSE d13_concept_id END   AS d13_concept_id,
                                  CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d13_concept_id = -1
                                    THEN NULL
                                  ELSE d14_concept_id END   AS d14_concept_id,
                                  CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d14_concept_id = -1
                                    THEN NULL
                                  ELSE d15_concept_id END   AS d15_concept_id,
                                  CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d15_concept_id = -1
                                    THEN NULL
                                  ELSE d16_concept_id END   AS d16_concept_id,
                                  CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d16_concept_id = -1
                                    THEN NULL
                                  ELSE d17_concept_id END   AS d17_concept_id,
                                  CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d17_concept_id = -1
                                    THEN NULL
                                  ELSE d18_concept_id END   AS d18_concept_id,
                                  CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d18_concept_id = -1
                                    THEN NULL
                                  ELSE d19_concept_id END   AS d19_concept_id,
                                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d19_concept_id = -1
                                    THEN NULL
                                  ELSE d20_concept_id END   AS d20_concept_id,
                                  CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  ELSE d1_concept_name END  AS d1_concept_name,
                                  CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d1_concept_id = -1
                                    THEN NULL
                                  ELSE d2_concept_name END  AS d2_concept_name,
                                  CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d2_concept_id = -1
                                    THEN NULL
                                  ELSE d3_concept_name END  AS d3_concept_name,
                                  CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d3_concept_id = -1
                                    THEN NULL
                                  ELSE d4_concept_name END  AS d4_concept_name,
                                  CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d4_concept_id = -1
                                    THEN NULL
                                  ELSE d5_concept_name END  AS d5_concept_name,
                                  CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d5_concept_id = -1
                                    THEN NULL
                                  ELSE d6_concept_name END  AS d6_concept_name,
                                  CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d6_concept_id = -1
                                    THEN NULL
                                  ELSE d7_concept_name END  AS d7_concept_name,
                                  CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d7_concept_id = -1
                                    THEN NULL
                                  ELSE d8_concept_name END  AS d8_concept_name,
                                  CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d8_concept_id = -1
                                    THEN NULL
                                  ELSE d9_concept_name END  AS d9_concept_name,
                                  CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d9_concept_id = -1
                                    THEN NULL
                                  ELSE d10_concept_name END AS d10_concept_name,
                                  CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d10_concept_id = -1
                                    THEN NULL
                                  ELSE d11_concept_name END AS d11_concept_name,
                                  CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d11_concept_id = -1
                                    THEN NULL
                                  ELSE d12_concept_name END AS d12_concept_name,
                                  CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d12_concept_id = -1
                                    THEN NULL
                                  ELSE d13_concept_name END AS d13_concept_name,
                                  CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d13_concept_id = -1
                                    THEN NULL
                                  ELSE d14_concept_name END AS d14_concept_name,
                                  CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d14_concept_id = -1
                                    THEN NULL
                                  ELSE d15_concept_name END AS d15_concept_name,
                                  CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d15_concept_id = -1
                                    THEN NULL
                                  ELSE d16_concept_name END AS d16_concept_name,
                                  CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d16_concept_id = -1
                                    THEN NULL
                                  ELSE d17_concept_name END AS d17_concept_name,
                                  CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d17_concept_id = -1
                                    THEN NULL
                                  ELSE d18_concept_name END AS d18_concept_name,
                                  CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d18_concept_id = -1
                                    THEN NULL
                                  ELSE d19_concept_name END AS d19_concept_name,
                                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d19_concept_id = -1
                                    THEN NULL
                                  ELSE d20_concept_name END AS d20_concept_name,
                                  sum(num_persons)          AS num_persons
                                FROM
                                  (
                                    SELECT
                                      index_year,
                                      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      ELSE d1_concept_id END    AS d1_concept_id,
                                      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d1_concept_id = -1
                                        THEN NULL
                                      ELSE d2_concept_id END    AS d2_concept_id,
                                      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d2_concept_id = -1
                                        THEN NULL
                                      ELSE d3_concept_id END    AS d3_concept_id,
                                      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d3_concept_id = -1
                                        THEN NULL
                                      ELSE d4_concept_id END    AS d4_concept_id,
                                      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d4_concept_id = -1
                                        THEN NULL
                                      ELSE d5_concept_id END    AS d5_concept_id,
                                      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d5_concept_id = -1
                                        THEN NULL
                                      ELSE d6_concept_id END    AS d6_concept_id,
                                      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d6_concept_id = -1
                                        THEN NULL
                                      ELSE d7_concept_id END    AS d7_concept_id,
                                      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d7_concept_id = -1
                                        THEN NULL
                                      ELSE d8_concept_id END    AS d8_concept_id,
                                      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d8_concept_id = -1
                                        THEN NULL
                                      ELSE d9_concept_id END    AS d9_concept_id,
                                      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d9_concept_id = -1
                                        THEN NULL
                                      ELSE d10_concept_id END   AS d10_concept_id,
                                      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d10_concept_id = -1
                                        THEN NULL
                                      ELSE d11_concept_id END   AS d11_concept_id,
                                      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d11_concept_id = -1
                                        THEN NULL
                                      ELSE d12_concept_id END   AS d12_concept_id,
                                      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d12_concept_id = -1
                                        THEN NULL
                                      ELSE d13_concept_id END   AS d13_concept_id,
                                      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d13_concept_id = -1
                                        THEN NULL
                                      ELSE d14_concept_id END   AS d14_concept_id,
                                      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d14_concept_id = -1
                                        THEN NULL
                                      ELSE d15_concept_id END   AS d15_concept_id,
                                      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d15_concept_id = -1
                                        THEN NULL
                                      ELSE d16_concept_id END   AS d16_concept_id,
                                      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d16_concept_id = -1
                                        THEN NULL
                                      ELSE d17_concept_id END   AS d17_concept_id,
                                      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d17_concept_id = -1
                                        THEN NULL
                                      ELSE d18_concept_id END   AS d18_concept_id,
                                      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d18_concept_id = -1
                                        THEN NULL
                                      ELSE d19_concept_id END   AS d19_concept_id,
                                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d19_concept_id = -1
                                        THEN NULL
                                      ELSE d20_concept_id END   AS d20_concept_id,
                                      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      ELSE d1_concept_name END  AS d1_concept_name,
                                      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d1_concept_id = -1
                                        THEN NULL
                                      ELSE d2_concept_name END  AS d2_concept_name,
                                      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d2_concept_id = -1
                                        THEN NULL
                                      ELSE d3_concept_name END  AS d3_concept_name,
                                      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d3_concept_id = -1
                                        THEN NULL
                                      ELSE d4_concept_name END  AS d4_concept_name,
                                      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d4_concept_id = -1
                                        THEN NULL
                                      ELSE d5_concept_name END  AS d5_concept_name,
                                      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d5_concept_id = -1
                                        THEN NULL
                                      ELSE d6_concept_name END  AS d6_concept_name,
                                      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d6_concept_id = -1
                                        THEN NULL
                                      ELSE d7_concept_name END  AS d7_concept_name,
                                      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d7_concept_id = -1
                                        THEN NULL
                                      ELSE d8_concept_name END  AS d8_concept_name,
                                      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d8_concept_id = -1
                                        THEN NULL
                                      ELSE d9_concept_name END  AS d9_concept_name,
                                      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d9_concept_id = -1
                                        THEN NULL
                                      ELSE d10_concept_name END AS d10_concept_name,
                                      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d10_concept_id = -1
                                        THEN NULL
                                      ELSE d11_concept_name END AS d11_concept_name,
                                      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d11_concept_id = -1
                                        THEN NULL
                                      ELSE d12_concept_name END AS d12_concept_name,
                                      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d12_concept_id = -1
                                        THEN NULL
                                      ELSE d13_concept_name END AS d13_concept_name,
                                      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d13_concept_id = -1
                                        THEN NULL
                                      ELSE d14_concept_name END AS d14_concept_name,
                                      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d14_concept_id = -1
                                        THEN NULL
                                      ELSE d15_concept_name END AS d15_concept_name,
                                      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d15_concept_id = -1
                                        THEN NULL
                                      ELSE d16_concept_name END AS d16_concept_name,
                                      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d16_concept_id = -1
                                        THEN NULL
                                      ELSE d17_concept_name END AS d17_concept_name,
                                      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d17_concept_id = -1
                                        THEN NULL
                                      ELSE d18_concept_name END AS d18_concept_name,
                                      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d18_concept_id = -1
                                        THEN NULL
                                      ELSE d19_concept_name END AS d19_concept_name,
                                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d19_concept_id = -1
                                        THEN NULL
                                      ELSE d20_concept_name END AS d20_concept_name,
                                      sum(num_persons)          AS num_persons
                                    FROM
                                      (
                                        SELECT
                                          index_year,
                                          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          ELSE d1_concept_id END    AS d1_concept_id,
                                          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d1_concept_id = -1
                                            THEN NULL
                                          ELSE d2_concept_id END    AS d2_concept_id,
                                          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d2_concept_id = -1
                                            THEN NULL
                                          ELSE d3_concept_id END    AS d3_concept_id,
                                          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d3_concept_id = -1
                                            THEN NULL
                                          ELSE d4_concept_id END    AS d4_concept_id,
                                          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d4_concept_id = -1
                                            THEN NULL
                                          ELSE d5_concept_id END    AS d5_concept_id,
                                          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d5_concept_id = -1
                                            THEN NULL
                                          ELSE d6_concept_id END    AS d6_concept_id,
                                          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d6_concept_id = -1
                                            THEN NULL
                                          ELSE d7_concept_id END    AS d7_concept_id,
                                          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d7_concept_id = -1
                                            THEN NULL
                                          ELSE d8_concept_id END    AS d8_concept_id,
                                          CASE WHEN
                                            d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d8_concept_id = -1
                                            THEN NULL
                                          ELSE d9_concept_id END    AS d9_concept_id,
                                          CASE WHEN
                                            d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d9_concept_id = -1
                                            THEN NULL
                                          ELSE d10_concept_id END   AS d10_concept_id,
                                          CASE WHEN
                                            d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d10_concept_id = -1
                                            THEN NULL
                                          ELSE d11_concept_id END   AS d11_concept_id,
                                          CASE WHEN
                                            d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d11_concept_id = -1
                                            THEN NULL
                                          ELSE d12_concept_id END   AS d12_concept_id,
                                          CASE WHEN
                                            d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d12_concept_id = -1
                                            THEN NULL
                                          ELSE d13_concept_id END   AS d13_concept_id,
                                          CASE WHEN
                                            d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d13_concept_id = -1
                                            THEN NULL
                                          ELSE d14_concept_id END   AS d14_concept_id,
                                          CASE WHEN
                                            d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d14_concept_id = -1
                                            THEN NULL
                                          ELSE d15_concept_id END   AS d15_concept_id,
                                          CASE WHEN
                                            d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d15_concept_id = -1
                                            THEN NULL
                                          ELSE d16_concept_id END   AS d16_concept_id,
                                          CASE WHEN
                                            d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d16_concept_id = -1
                                            THEN NULL
                                          ELSE d17_concept_id END   AS d17_concept_id,
                                          CASE WHEN
                                            d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d17_concept_id = -1
                                            THEN NULL
                                          ELSE d18_concept_id END   AS d18_concept_id,
                                          CASE WHEN
                                            d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d18_concept_id = -1
                                            THEN NULL
                                          ELSE d19_concept_id END   AS d19_concept_id,
                                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d19_concept_id = -1
                                            THEN NULL
                                          ELSE d20_concept_id END   AS d20_concept_id,
                                          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          ELSE d1_concept_name END  AS d1_concept_name,
                                          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d1_concept_id = -1
                                            THEN NULL
                                          ELSE d2_concept_name END  AS d2_concept_name,
                                          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d2_concept_id = -1
                                            THEN NULL
                                          ELSE d3_concept_name END  AS d3_concept_name,
                                          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d3_concept_id = -1
                                            THEN NULL
                                          ELSE d4_concept_name END  AS d4_concept_name,
                                          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d4_concept_id = -1
                                            THEN NULL
                                          ELSE d5_concept_name END  AS d5_concept_name,
                                          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d5_concept_id = -1
                                            THEN NULL
                                          ELSE d6_concept_name END  AS d6_concept_name,
                                          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d6_concept_id = -1
                                            THEN NULL
                                          ELSE d7_concept_name END  AS d7_concept_name,
                                          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d7_concept_id = -1
                                            THEN NULL
                                          ELSE d8_concept_name END  AS d8_concept_name,
                                          CASE WHEN
                                            d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d8_concept_id = -1
                                            THEN NULL
                                          ELSE d9_concept_name END  AS d9_concept_name,
                                          CASE WHEN
                                            d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d9_concept_id = -1
                                            THEN NULL
                                          ELSE d10_concept_name END AS d10_concept_name,
                                          CASE WHEN
                                            d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d10_concept_id = -1
                                            THEN NULL
                                          ELSE d11_concept_name END AS d11_concept_name,
                                          CASE WHEN
                                            d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d11_concept_id = -1
                                            THEN NULL
                                          ELSE d12_concept_name END AS d12_concept_name,
                                          CASE WHEN
                                            d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d12_concept_id = -1
                                            THEN NULL
                                          ELSE d13_concept_name END AS d13_concept_name,
                                          CASE WHEN
                                            d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d13_concept_id = -1
                                            THEN NULL
                                          ELSE d14_concept_name END AS d14_concept_name,
                                          CASE WHEN
                                            d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d14_concept_id = -1
                                            THEN NULL
                                          ELSE d15_concept_name END AS d15_concept_name,
                                          CASE WHEN
                                            d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d15_concept_id = -1
                                            THEN NULL
                                          ELSE d16_concept_name END AS d16_concept_name,
                                          CASE WHEN
                                            d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d16_concept_id = -1
                                            THEN NULL
                                          ELSE d17_concept_name END AS d17_concept_name,
                                          CASE WHEN
                                            d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d17_concept_id = -1
                                            THEN NULL
                                          ELSE d18_concept_name END AS d18_concept_name,
                                          CASE WHEN
                                            d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d18_concept_id = -1
                                            THEN NULL
                                          ELSE d19_concept_name END AS d19_concept_name,
                                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d19_concept_id = -1
                                            THEN NULL
                                          ELSE d20_concept_name END AS d20_concept_name,
                                          sum(num_persons)          AS num_persons
                                        FROM
                                          (
                                            SELECT
                                              index_year,
                                              CASE WHEN
                                                d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              ELSE d1_concept_id END    AS d1_concept_id,
                                              CASE WHEN
                                                d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d1_concept_id = -1
                                                THEN NULL
                                              ELSE d2_concept_id END    AS d2_concept_id,
                                              CASE WHEN
                                                d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d2_concept_id = -1
                                                THEN NULL
                                              ELSE d3_concept_id END    AS d3_concept_id,
                                              CASE WHEN
                                                d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d3_concept_id = -1
                                                THEN NULL
                                              ELSE d4_concept_id END    AS d4_concept_id,
                                              CASE WHEN
                                                d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d4_concept_id = -1
                                                THEN NULL
                                              ELSE d5_concept_id END    AS d5_concept_id,
                                              CASE WHEN
                                                d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d5_concept_id = -1
                                                THEN NULL
                                              ELSE d6_concept_id END    AS d6_concept_id,
                                              CASE WHEN
                                                d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d6_concept_id = -1
                                                THEN NULL
                                              ELSE d7_concept_id END    AS d7_concept_id,
                                              CASE WHEN
                                                d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d7_concept_id = -1
                                                THEN NULL
                                              ELSE d8_concept_id END    AS d8_concept_id,
                                              CASE WHEN
                                                d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d8_concept_id = -1
                                                THEN NULL
                                              ELSE d9_concept_id END    AS d9_concept_id,
                                              CASE WHEN
                                                d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d9_concept_id = -1
                                                THEN NULL
                                              ELSE d10_concept_id END   AS d10_concept_id,
                                              CASE WHEN
                                                d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d10_concept_id = -1
                                                THEN NULL
                                              ELSE d11_concept_id END   AS d11_concept_id,
                                              CASE WHEN
                                                d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d11_concept_id = -1
                                                THEN NULL
                                              ELSE d12_concept_id END   AS d12_concept_id,
                                              CASE WHEN
                                                d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d12_concept_id = -1
                                                THEN NULL
                                              ELSE d13_concept_id END   AS d13_concept_id,
                                              CASE WHEN
                                                d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d13_concept_id = -1
                                                THEN NULL
                                              ELSE d14_concept_id END   AS d14_concept_id,
                                              CASE WHEN
                                                d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d14_concept_id = -1
                                                THEN NULL
                                              ELSE d15_concept_id END   AS d15_concept_id,
                                              CASE WHEN
                                                d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d15_concept_id = -1
                                                THEN NULL
                                              ELSE d16_concept_id END   AS d16_concept_id,
                                              CASE WHEN
                                                d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d16_concept_id = -1
                                                THEN NULL
                                              ELSE d17_concept_id END   AS d17_concept_id,
                                              CASE WHEN
                                                d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d17_concept_id = -1
                                                THEN NULL
                                              ELSE d18_concept_id END   AS d18_concept_id,
                                              CASE WHEN
                                                d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d18_concept_id = -1
                                                THEN NULL
                                              ELSE d19_concept_id END   AS d19_concept_id,
                                              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d19_concept_id = -1
                                                THEN NULL
                                              ELSE d20_concept_id END   AS d20_concept_id,
                                              CASE WHEN
                                                d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              ELSE d1_concept_name END  AS d1_concept_name,
                                              CASE WHEN
                                                d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d1_concept_id = -1
                                                THEN NULL
                                              ELSE d2_concept_name END  AS d2_concept_name,
                                              CASE WHEN
                                                d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d2_concept_id = -1
                                                THEN NULL
                                              ELSE d3_concept_name END  AS d3_concept_name,
                                              CASE WHEN
                                                d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d3_concept_id = -1
                                                THEN NULL
                                              ELSE d4_concept_name END  AS d4_concept_name,
                                              CASE WHEN
                                                d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d4_concept_id = -1
                                                THEN NULL
                                              ELSE d5_concept_name END  AS d5_concept_name,
                                              CASE WHEN
                                                d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d5_concept_id = -1
                                                THEN NULL
                                              ELSE d6_concept_name END  AS d6_concept_name,
                                              CASE WHEN
                                                d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d6_concept_id = -1
                                                THEN NULL
                                              ELSE d7_concept_name END  AS d7_concept_name,
                                              CASE WHEN
                                                d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d7_concept_id = -1
                                                THEN NULL
                                              ELSE d8_concept_name END  AS d8_concept_name,
                                              CASE WHEN
                                                d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d8_concept_id = -1
                                                THEN NULL
                                              ELSE d9_concept_name END  AS d9_concept_name,
                                              CASE WHEN
                                                d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d9_concept_id = -1
                                                THEN NULL
                                              ELSE d10_concept_name END AS d10_concept_name,
                                              CASE WHEN
                                                d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d10_concept_id = -1
                                                THEN NULL
                                              ELSE d11_concept_name END AS d11_concept_name,
                                              CASE WHEN
                                                d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d11_concept_id = -1
                                                THEN NULL
                                              ELSE d12_concept_name END AS d12_concept_name,
                                              CASE WHEN
                                                d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d12_concept_id = -1
                                                THEN NULL
                                              ELSE d13_concept_name END AS d13_concept_name,
                                              CASE WHEN
                                                d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d13_concept_id = -1
                                                THEN NULL
                                              ELSE d14_concept_name END AS d14_concept_name,
                                              CASE WHEN
                                                d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d14_concept_id = -1
                                                THEN NULL
                                              ELSE d15_concept_name END AS d15_concept_name,
                                              CASE WHEN
                                                d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d15_concept_id = -1
                                                THEN NULL
                                              ELSE d16_concept_name END AS d16_concept_name,
                                              CASE WHEN
                                                d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d16_concept_id = -1
                                                THEN NULL
                                              ELSE d17_concept_name END AS d17_concept_name,
                                              CASE WHEN
                                                d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d17_concept_id = -1
                                                THEN NULL
                                              ELSE d18_concept_name END AS d18_concept_name,
                                              CASE WHEN
                                                d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d18_concept_id = -1
                                                THEN NULL
                                              ELSE d19_concept_name END AS d19_concept_name,
                                              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d19_concept_id = -1
                                                THEN NULL
                                              ELSE d20_concept_name END AS d20_concept_name,
                                              sum(num_persons)          AS num_persons
                                            FROM
                                              (
                                                SELECT
                                                  index_year,
                                                  CASE WHEN
                                                    d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  ELSE d1_concept_id END    AS d1_concept_id,
                                                  CASE WHEN
                                                    d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d1_concept_id = -1
                                                    THEN NULL
                                                  ELSE d2_concept_id END    AS d2_concept_id,
                                                  CASE WHEN
                                                    d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d2_concept_id = -1
                                                    THEN NULL
                                                  ELSE d3_concept_id END    AS d3_concept_id,
                                                  CASE WHEN
                                                    d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d3_concept_id = -1
                                                    THEN NULL
                                                  ELSE d4_concept_id END    AS d4_concept_id,
                                                  CASE WHEN
                                                    d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d4_concept_id = -1
                                                    THEN NULL
                                                  ELSE d5_concept_id END    AS d5_concept_id,
                                                  CASE WHEN
                                                    d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d5_concept_id = -1
                                                    THEN NULL
                                                  ELSE d6_concept_id END    AS d6_concept_id,
                                                  CASE WHEN
                                                    d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d6_concept_id = -1
                                                    THEN NULL
                                                  ELSE d7_concept_id END    AS d7_concept_id,
                                                  CASE WHEN
                                                    d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d7_concept_id = -1
                                                    THEN NULL
                                                  ELSE d8_concept_id END    AS d8_concept_id,
                                                  CASE WHEN d9_concept_id > 0 AND
                                                            (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d8_concept_id = -1
                                                    THEN NULL
                                                  ELSE d9_concept_id END    AS d9_concept_id,
                                                  CASE WHEN d10_concept_id > 0 AND
                                                            (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d9_concept_id = -1
                                                    THEN NULL
                                                  ELSE d10_concept_id END   AS d10_concept_id,
                                                  CASE WHEN d11_concept_id > 0 AND
                                                            (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d10_concept_id = -1
                                                    THEN NULL
                                                  ELSE d11_concept_id END   AS d11_concept_id,
                                                  CASE WHEN d12_concept_id > 0 AND
                                                            (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d11_concept_id = -1
                                                    THEN NULL
                                                  ELSE d12_concept_id END   AS d12_concept_id,
                                                  CASE WHEN d13_concept_id > 0 AND
                                                            (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d12_concept_id = -1
                                                    THEN NULL
                                                  ELSE d13_concept_id END   AS d13_concept_id,
                                                  CASE WHEN d14_concept_id > 0 AND
                                                            (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d13_concept_id = -1
                                                    THEN NULL
                                                  ELSE d14_concept_id END   AS d14_concept_id,
                                                  CASE WHEN d15_concept_id > 0 AND
                                                            (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d14_concept_id = -1
                                                    THEN NULL
                                                  ELSE d15_concept_id END   AS d15_concept_id,
                                                  CASE WHEN d16_concept_id > 0 AND
                                                            (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d15_concept_id = -1
                                                    THEN NULL
                                                  ELSE d16_concept_id END   AS d16_concept_id,
                                                  CASE WHEN d17_concept_id > 0 AND
                                                            (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d16_concept_id = -1
                                                    THEN NULL
                                                  ELSE d17_concept_id END   AS d17_concept_id,
                                                  CASE WHEN d18_concept_id > 0 AND
                                                            (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d17_concept_id = -1
                                                    THEN NULL
                                                  ELSE d18_concept_id END   AS d18_concept_id,
                                                  CASE WHEN d19_concept_id > 0 AND
                                                            (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d18_concept_id = -1
                                                    THEN NULL
                                                  ELSE d19_concept_id END   AS d19_concept_id,
                                                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d19_concept_id = -1
                                                    THEN NULL
                                                  ELSE d20_concept_id END   AS d20_concept_id,
                                                  CASE WHEN
                                                    d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  ELSE d1_concept_name END  AS d1_concept_name,
                                                  CASE WHEN
                                                    d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d1_concept_id = -1
                                                    THEN NULL
                                                  ELSE d2_concept_name END  AS d2_concept_name,
                                                  CASE WHEN
                                                    d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d2_concept_id = -1
                                                    THEN NULL
                                                  ELSE d3_concept_name END  AS d3_concept_name,
                                                  CASE WHEN
                                                    d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d3_concept_id = -1
                                                    THEN NULL
                                                  ELSE d4_concept_name END  AS d4_concept_name,
                                                  CASE WHEN
                                                    d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d4_concept_id = -1
                                                    THEN NULL
                                                  ELSE d5_concept_name END  AS d5_concept_name,
                                                  CASE WHEN
                                                    d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d5_concept_id = -1
                                                    THEN NULL
                                                  ELSE d6_concept_name END  AS d6_concept_name,
                                                  CASE WHEN
                                                    d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d6_concept_id = -1
                                                    THEN NULL
                                                  ELSE d7_concept_name END  AS d7_concept_name,
                                                  CASE WHEN
                                                    d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d7_concept_id = -1
                                                    THEN NULL
                                                  ELSE d8_concept_name END  AS d8_concept_name,
                                                  CASE WHEN d9_concept_id > 0 AND
                                                            (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d8_concept_id = -1
                                                    THEN NULL
                                                  ELSE d9_concept_name END  AS d9_concept_name,
                                                  CASE WHEN d10_concept_id > 0 AND
                                                            (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d9_concept_id = -1
                                                    THEN NULL
                                                  ELSE d10_concept_name END AS d10_concept_name,
                                                  CASE WHEN d11_concept_id > 0 AND
                                                            (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d10_concept_id = -1
                                                    THEN NULL
                                                  ELSE d11_concept_name END AS d11_concept_name,
                                                  CASE WHEN d12_concept_id > 0 AND
                                                            (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d11_concept_id = -1
                                                    THEN NULL
                                                  ELSE d12_concept_name END AS d12_concept_name,
                                                  CASE WHEN d13_concept_id > 0 AND
                                                            (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d12_concept_id = -1
                                                    THEN NULL
                                                  ELSE d13_concept_name END AS d13_concept_name,
                                                  CASE WHEN d14_concept_id > 0 AND
                                                            (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d13_concept_id = -1
                                                    THEN NULL
                                                  ELSE d14_concept_name END AS d14_concept_name,
                                                  CASE WHEN d15_concept_id > 0 AND
                                                            (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d14_concept_id = -1
                                                    THEN NULL
                                                  ELSE d15_concept_name END AS d15_concept_name,
                                                  CASE WHEN d16_concept_id > 0 AND
                                                            (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d15_concept_id = -1
                                                    THEN NULL
                                                  ELSE d16_concept_name END AS d16_concept_name,
                                                  CASE WHEN d17_concept_id > 0 AND
                                                            (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d16_concept_id = -1
                                                    THEN NULL
                                                  ELSE d17_concept_name END AS d17_concept_name,
                                                  CASE WHEN d18_concept_id > 0 AND
                                                            (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d17_concept_id = -1
                                                    THEN NULL
                                                  ELSE d18_concept_name END AS d18_concept_name,
                                                  CASE WHEN d19_concept_id > 0 AND
                                                            (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d18_concept_id = -1
                                                    THEN NULL
                                                  ELSE d19_concept_name END AS d19_concept_name,
                                                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d19_concept_id = -1
                                                    THEN NULL
                                                  ELSE d20_concept_name END AS d20_concept_name,
                                                  sum(num_persons)          AS num_persons
                                                FROM
                                                  (
                                                    SELECT
                                                      index_year,
                                                      CASE WHEN d1_concept_id > 0 AND
                                                                (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      ELSE d1_concept_id END    AS d1_concept_id,
                                                      CASE WHEN d2_concept_id > 0 AND
                                                                (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d1_concept_id = -1
                                                        THEN NULL
                                                      ELSE d2_concept_id END    AS d2_concept_id,
                                                      CASE WHEN d3_concept_id > 0 AND
                                                                (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d2_concept_id = -1
                                                        THEN NULL
                                                      ELSE d3_concept_id END    AS d3_concept_id,
                                                      CASE WHEN d4_concept_id > 0 AND
                                                                (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d3_concept_id = -1
                                                        THEN NULL
                                                      ELSE d4_concept_id END    AS d4_concept_id,
                                                      CASE WHEN d5_concept_id > 0 AND
                                                                (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d4_concept_id = -1
                                                        THEN NULL
                                                      ELSE d5_concept_id END    AS d5_concept_id,
                                                      CASE WHEN d6_concept_id > 0 AND
                                                                (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d5_concept_id = -1
                                                        THEN NULL
                                                      ELSE d6_concept_id END    AS d6_concept_id,
                                                      CASE WHEN d7_concept_id > 0 AND
                                                                (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d6_concept_id = -1
                                                        THEN NULL
                                                      ELSE d7_concept_id END    AS d7_concept_id,
                                                      CASE WHEN d8_concept_id > 0 AND
                                                                (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d7_concept_id = -1
                                                        THEN NULL
                                                      ELSE d8_concept_id END    AS d8_concept_id,
                                                      CASE WHEN d9_concept_id > 0 AND
                                                                (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d8_concept_id = -1
                                                        THEN NULL
                                                      ELSE d9_concept_id END    AS d9_concept_id,
                                                      CASE WHEN d10_concept_id > 0 AND
                                                                (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d9_concept_id = -1
                                                        THEN NULL
                                                      ELSE d10_concept_id END   AS d10_concept_id,
                                                      CASE WHEN d11_concept_id > 0 AND
                                                                (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d10_concept_id = -1
                                                        THEN NULL
                                                      ELSE d11_concept_id END   AS d11_concept_id,
                                                      CASE WHEN d12_concept_id > 0 AND
                                                                (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d11_concept_id = -1
                                                        THEN NULL
                                                      ELSE d12_concept_id END   AS d12_concept_id,
                                                      CASE WHEN d13_concept_id > 0 AND
                                                                (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d12_concept_id = -1
                                                        THEN NULL
                                                      ELSE d13_concept_id END   AS d13_concept_id,
                                                      CASE WHEN d14_concept_id > 0 AND
                                                                (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d13_concept_id = -1
                                                        THEN NULL
                                                      ELSE d14_concept_id END   AS d14_concept_id,
                                                      CASE WHEN d15_concept_id > 0 AND
                                                                (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d14_concept_id = -1
                                                        THEN NULL
                                                      ELSE d15_concept_id END   AS d15_concept_id,
                                                      CASE WHEN d16_concept_id > 0 AND
                                                                (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d15_concept_id = -1
                                                        THEN NULL
                                                      ELSE d16_concept_id END   AS d16_concept_id,
                                                      CASE WHEN d17_concept_id > 0 AND
                                                                (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d16_concept_id = -1
                                                        THEN NULL
                                                      ELSE d17_concept_id END   AS d17_concept_id,
                                                      CASE WHEN d18_concept_id > 0 AND
                                                                (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d17_concept_id = -1
                                                        THEN NULL
                                                      ELSE d18_concept_id END   AS d18_concept_id,
                                                      CASE WHEN d19_concept_id > 0 AND
                                                                (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d18_concept_id = -1
                                                        THEN NULL
                                                      ELSE d19_concept_id END   AS d19_concept_id,
                                                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d19_concept_id = -1
                                                        THEN NULL
                                                      ELSE d20_concept_id END   AS d20_concept_id,
                                                      CASE WHEN d1_concept_id > 0 AND
                                                                (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      ELSE d1_concept_name END  AS d1_concept_name,
                                                      CASE WHEN d2_concept_id > 0 AND
                                                                (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d1_concept_id = -1
                                                        THEN NULL
                                                      ELSE d2_concept_name END  AS d2_concept_name,
                                                      CASE WHEN d3_concept_id > 0 AND
                                                                (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d2_concept_id = -1
                                                        THEN NULL
                                                      ELSE d3_concept_name END  AS d3_concept_name,
                                                      CASE WHEN d4_concept_id > 0 AND
                                                                (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d3_concept_id = -1
                                                        THEN NULL
                                                      ELSE d4_concept_name END  AS d4_concept_name,
                                                      CASE WHEN d5_concept_id > 0 AND
                                                                (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d4_concept_id = -1
                                                        THEN NULL
                                                      ELSE d5_concept_name END  AS d5_concept_name,
                                                      CASE WHEN d6_concept_id > 0 AND
                                                                (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d5_concept_id = -1
                                                        THEN NULL
                                                      ELSE d6_concept_name END  AS d6_concept_name,
                                                      CASE WHEN d7_concept_id > 0 AND
                                                                (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d6_concept_id = -1
                                                        THEN NULL
                                                      ELSE d7_concept_name END  AS d7_concept_name,
                                                      CASE WHEN d8_concept_id > 0 AND
                                                                (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d7_concept_id = -1
                                                        THEN NULL
                                                      ELSE d8_concept_name END  AS d8_concept_name,
                                                      CASE WHEN d9_concept_id > 0 AND
                                                                (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d8_concept_id = -1
                                                        THEN NULL
                                                      ELSE d9_concept_name END  AS d9_concept_name,
                                                      CASE WHEN d10_concept_id > 0 AND
                                                                (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d9_concept_id = -1
                                                        THEN NULL
                                                      ELSE d10_concept_name END AS d10_concept_name,
                                                      CASE WHEN d11_concept_id > 0 AND
                                                                (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d10_concept_id = -1
                                                        THEN NULL
                                                      ELSE d11_concept_name END AS d11_concept_name,
                                                      CASE WHEN d12_concept_id > 0 AND
                                                                (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d11_concept_id = -1
                                                        THEN NULL
                                                      ELSE d12_concept_name END AS d12_concept_name,
                                                      CASE WHEN d13_concept_id > 0 AND
                                                                (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d12_concept_id = -1
                                                        THEN NULL
                                                      ELSE d13_concept_name END AS d13_concept_name,
                                                      CASE WHEN d14_concept_id > 0 AND
                                                                (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d13_concept_id = -1
                                                        THEN NULL
                                                      ELSE d14_concept_name END AS d14_concept_name,
                                                      CASE WHEN d15_concept_id > 0 AND
                                                                (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d14_concept_id = -1
                                                        THEN NULL
                                                      ELSE d15_concept_name END AS d15_concept_name,
                                                      CASE WHEN d16_concept_id > 0 AND
                                                                (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d15_concept_id = -1
                                                        THEN NULL
                                                      ELSE d16_concept_name END AS d16_concept_name,
                                                      CASE WHEN d17_concept_id > 0 AND
                                                                (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d16_concept_id = -1
                                                        THEN NULL
                                                      ELSE d17_concept_name END AS d17_concept_name,
                                                      CASE WHEN d18_concept_id > 0 AND
                                                                (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d17_concept_id = -1
                                                        THEN NULL
                                                      ELSE d18_concept_name END AS d18_concept_name,
                                                      CASE WHEN d19_concept_id > 0 AND
                                                                (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d18_concept_id = -1
                                                        THEN NULL
                                                      ELSE d19_concept_name END AS d19_concept_name,
                                                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d19_concept_id = -1
                                                        THEN NULL
                                                      ELSE d20_concept_name END AS d20_concept_name,
                                                      sum(num_persons)          AS num_persons
                                                    FROM
                                                      (
                                                        SELECT
                                                          index_year,
                                                          CASE WHEN d1_concept_id > 0 AND
                                                                    (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          ELSE d1_concept_id END    AS d1_concept_id,
                                                          CASE WHEN d2_concept_id > 0 AND
                                                                    (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d1_concept_id = -1
                                                            THEN NULL
                                                          ELSE d2_concept_id END    AS d2_concept_id,
                                                          CASE WHEN d3_concept_id > 0 AND
                                                                    (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d2_concept_id = -1
                                                            THEN NULL
                                                          ELSE d3_concept_id END    AS d3_concept_id,
                                                          CASE WHEN d4_concept_id > 0 AND
                                                                    (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d3_concept_id = -1
                                                            THEN NULL
                                                          ELSE d4_concept_id END    AS d4_concept_id,
                                                          CASE WHEN d5_concept_id > 0 AND
                                                                    (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d4_concept_id = -1
                                                            THEN NULL
                                                          ELSE d5_concept_id END    AS d5_concept_id,
                                                          CASE WHEN d6_concept_id > 0 AND
                                                                    (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d5_concept_id = -1
                                                            THEN NULL
                                                          ELSE d6_concept_id END    AS d6_concept_id,
                                                          CASE WHEN d7_concept_id > 0 AND
                                                                    (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d6_concept_id = -1
                                                            THEN NULL
                                                          ELSE d7_concept_id END    AS d7_concept_id,
                                                          CASE WHEN d8_concept_id > 0 AND
                                                                    (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d7_concept_id = -1
                                                            THEN NULL
                                                          ELSE d8_concept_id END    AS d8_concept_id,
                                                          CASE WHEN d9_concept_id > 0 AND
                                                                    (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d8_concept_id = -1
                                                            THEN NULL
                                                          ELSE d9_concept_id END    AS d9_concept_id,
                                                          CASE WHEN d10_concept_id > 0 AND
                                                                    (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d9_concept_id = -1
                                                            THEN NULL
                                                          ELSE d10_concept_id END   AS d10_concept_id,
                                                          CASE WHEN d11_concept_id > 0 AND
                                                                    (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d10_concept_id = -1
                                                            THEN NULL
                                                          ELSE d11_concept_id END   AS d11_concept_id,
                                                          CASE WHEN d12_concept_id > 0 AND
                                                                    (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d11_concept_id = -1
                                                            THEN NULL
                                                          ELSE d12_concept_id END   AS d12_concept_id,
                                                          CASE WHEN d13_concept_id > 0 AND
                                                                    (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d12_concept_id = -1
                                                            THEN NULL
                                                          ELSE d13_concept_id END   AS d13_concept_id,
                                                          CASE WHEN d14_concept_id > 0 AND
                                                                    (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d13_concept_id = -1
                                                            THEN NULL
                                                          ELSE d14_concept_id END   AS d14_concept_id,
                                                          CASE WHEN d15_concept_id > 0 AND
                                                                    (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d14_concept_id = -1
                                                            THEN NULL
                                                          ELSE d15_concept_id END   AS d15_concept_id,
                                                          CASE WHEN d16_concept_id > 0 AND
                                                                    (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d15_concept_id = -1
                                                            THEN NULL
                                                          ELSE d16_concept_id END   AS d16_concept_id,
                                                          CASE WHEN d17_concept_id > 0 AND
                                                                    (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d16_concept_id = -1
                                                            THEN NULL
                                                          ELSE d17_concept_id END   AS d17_concept_id,
                                                          CASE WHEN d18_concept_id > 0 AND
                                                                    (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d17_concept_id = -1
                                                            THEN NULL
                                                          ELSE d18_concept_id END   AS d18_concept_id,
                                                          CASE WHEN d19_concept_id > 0 AND
                                                                    (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d18_concept_id = -1
                                                            THEN NULL
                                                          ELSE d19_concept_id END   AS d19_concept_id,
                                                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d19_concept_id = -1
                                                            THEN NULL
                                                          ELSE d20_concept_id END   AS d20_concept_id,
                                                          CASE WHEN d1_concept_id > 0 AND
                                                                    (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          ELSE d1_concept_name END  AS d1_concept_name,
                                                          CASE WHEN d2_concept_id > 0 AND
                                                                    (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d1_concept_id = -1
                                                            THEN NULL
                                                          ELSE d2_concept_name END  AS d2_concept_name,
                                                          CASE WHEN d3_concept_id > 0 AND
                                                                    (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d2_concept_id = -1
                                                            THEN NULL
                                                          ELSE d3_concept_name END  AS d3_concept_name,
                                                          CASE WHEN d4_concept_id > 0 AND
                                                                    (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d3_concept_id = -1
                                                            THEN NULL
                                                          ELSE d4_concept_name END  AS d4_concept_name,
                                                          CASE WHEN d5_concept_id > 0 AND
                                                                    (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d4_concept_id = -1
                                                            THEN NULL
                                                          ELSE d5_concept_name END  AS d5_concept_name,
                                                          CASE WHEN d6_concept_id > 0 AND
                                                                    (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d5_concept_id = -1
                                                            THEN NULL
                                                          ELSE d6_concept_name END  AS d6_concept_name,
                                                          CASE WHEN d7_concept_id > 0 AND
                                                                    (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d6_concept_id = -1
                                                            THEN NULL
                                                          ELSE d7_concept_name END  AS d7_concept_name,
                                                          CASE WHEN d8_concept_id > 0 AND
                                                                    (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d7_concept_id = -1
                                                            THEN NULL
                                                          ELSE d8_concept_name END  AS d8_concept_name,
                                                          CASE WHEN d9_concept_id > 0 AND
                                                                    (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d8_concept_id = -1
                                                            THEN NULL
                                                          ELSE d9_concept_name END  AS d9_concept_name,
                                                          CASE WHEN d10_concept_id > 0 AND
                                                                    (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d9_concept_id = -1
                                                            THEN NULL
                                                          ELSE d10_concept_name END AS d10_concept_name,
                                                          CASE WHEN d11_concept_id > 0 AND
                                                                    (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d10_concept_id = -1
                                                            THEN NULL
                                                          ELSE d11_concept_name END AS d11_concept_name,
                                                          CASE WHEN d12_concept_id > 0 AND
                                                                    (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d11_concept_id = -1
                                                            THEN NULL
                                                          ELSE d12_concept_name END AS d12_concept_name,
                                                          CASE WHEN d13_concept_id > 0 AND
                                                                    (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d12_concept_id = -1
                                                            THEN NULL
                                                          ELSE d13_concept_name END AS d13_concept_name,
                                                          CASE WHEN d14_concept_id > 0 AND
                                                                    (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d13_concept_id = -1
                                                            THEN NULL
                                                          ELSE d14_concept_name END AS d14_concept_name,
                                                          CASE WHEN d15_concept_id > 0 AND
                                                                    (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d14_concept_id = -1
                                                            THEN NULL
                                                          ELSE d15_concept_name END AS d15_concept_name,
                                                          CASE WHEN d16_concept_id > 0 AND
                                                                    (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d15_concept_id = -1
                                                            THEN NULL
                                                          ELSE d16_concept_name END AS d16_concept_name,
                                                          CASE WHEN d17_concept_id > 0 AND
                                                                    (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d16_concept_id = -1
                                                            THEN NULL
                                                          ELSE d17_concept_name END AS d17_concept_name,
                                                          CASE WHEN d18_concept_id > 0 AND
                                                                    (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d17_concept_id = -1
                                                            THEN NULL
                                                          ELSE d18_concept_name END AS d18_concept_name,
                                                          CASE WHEN d19_concept_id > 0 AND
                                                                    (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d18_concept_id = -1
                                                            THEN NULL
                                                          ELSE d19_concept_name END AS d19_concept_name,
                                                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d19_concept_id = -1
                                                            THEN NULL
                                                          ELSE d20_concept_name END AS d20_concept_name,
                                                          sum(num_persons)          AS num_persons
                                                        FROM
                                                          (
                                                            SELECT
                                                              index_year,
                                                              CASE WHEN d1_concept_id > 0 AND
                                                                        (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              ELSE d1_concept_id END    AS d1_concept_id,
                                                              CASE WHEN d2_concept_id > 0 AND
                                                                        (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d1_concept_id = -1
                                                                THEN NULL
                                                              ELSE d2_concept_id END    AS d2_concept_id,
                                                              CASE WHEN d3_concept_id > 0 AND
                                                                        (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d2_concept_id = -1
                                                                THEN NULL
                                                              ELSE d3_concept_id END    AS d3_concept_id,
                                                              CASE WHEN d4_concept_id > 0 AND
                                                                        (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d3_concept_id = -1
                                                                THEN NULL
                                                              ELSE d4_concept_id END    AS d4_concept_id,
                                                              CASE WHEN d5_concept_id > 0 AND
                                                                        (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d4_concept_id = -1
                                                                THEN NULL
                                                              ELSE d5_concept_id END    AS d5_concept_id,
                                                              CASE WHEN d6_concept_id > 0 AND
                                                                        (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d5_concept_id = -1
                                                                THEN NULL
                                                              ELSE d6_concept_id END    AS d6_concept_id,
                                                              CASE WHEN d7_concept_id > 0 AND
                                                                        (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d6_concept_id = -1
                                                                THEN NULL
                                                              ELSE d7_concept_id END    AS d7_concept_id,
                                                              CASE WHEN d8_concept_id > 0 AND
                                                                        (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d7_concept_id = -1
                                                                THEN NULL
                                                              ELSE d8_concept_id END    AS d8_concept_id,
                                                              CASE WHEN d9_concept_id > 0 AND
                                                                        (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d8_concept_id = -1
                                                                THEN NULL
                                                              ELSE d9_concept_id END    AS d9_concept_id,
                                                              CASE WHEN d10_concept_id > 0 AND
                                                                        (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d9_concept_id = -1
                                                                THEN NULL
                                                              ELSE d10_concept_id END   AS d10_concept_id,
                                                              CASE WHEN d11_concept_id > 0 AND
                                                                        (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d10_concept_id = -1
                                                                THEN NULL
                                                              ELSE d11_concept_id END   AS d11_concept_id,
                                                              CASE WHEN d12_concept_id > 0 AND
                                                                        (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d11_concept_id = -1
                                                                THEN NULL
                                                              ELSE d12_concept_id END   AS d12_concept_id,
                                                              CASE WHEN d13_concept_id > 0 AND
                                                                        (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d12_concept_id = -1
                                                                THEN NULL
                                                              ELSE d13_concept_id END   AS d13_concept_id,
                                                              CASE WHEN d14_concept_id > 0 AND
                                                                        (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d13_concept_id = -1
                                                                THEN NULL
                                                              ELSE d14_concept_id END   AS d14_concept_id,
                                                              CASE WHEN d15_concept_id > 0 AND
                                                                        (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d14_concept_id = -1
                                                                THEN NULL
                                                              ELSE d15_concept_id END   AS d15_concept_id,
                                                              CASE WHEN d16_concept_id > 0 AND
                                                                        (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d15_concept_id = -1
                                                                THEN NULL
                                                              ELSE d16_concept_id END   AS d16_concept_id,
                                                              CASE WHEN d17_concept_id > 0 AND
                                                                        (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d16_concept_id = -1
                                                                THEN NULL
                                                              ELSE d17_concept_id END   AS d17_concept_id,
                                                              CASE WHEN d18_concept_id > 0 AND
                                                                        (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d17_concept_id = -1
                                                                THEN NULL
                                                              ELSE d18_concept_id END   AS d18_concept_id,
                                                              CASE WHEN d19_concept_id > 0 AND
                                                                        (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d18_concept_id = -1
                                                                THEN NULL
                                                              ELSE d19_concept_id END   AS d19_concept_id,
                                                              CASE WHEN d20_concept_id > 0 AND
                                                                        num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d19_concept_id = -1
                                                                THEN NULL
                                                              ELSE d20_concept_id END   AS d20_concept_id,
                                                              CASE WHEN d1_concept_id > 0 AND
                                                                        (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              ELSE d1_concept_name END  AS d1_concept_name,
                                                              CASE WHEN d2_concept_id > 0 AND
                                                                        (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d1_concept_id = -1
                                                                THEN NULL
                                                              ELSE d2_concept_name END  AS d2_concept_name,
                                                              CASE WHEN d3_concept_id > 0 AND
                                                                        (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d2_concept_id = -1
                                                                THEN NULL
                                                              ELSE d3_concept_name END  AS d3_concept_name,
                                                              CASE WHEN d4_concept_id > 0 AND
                                                                        (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d3_concept_id = -1
                                                                THEN NULL
                                                              ELSE d4_concept_name END  AS d4_concept_name,
                                                              CASE WHEN d5_concept_id > 0 AND
                                                                        (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d4_concept_id = -1
                                                                THEN NULL
                                                              ELSE d5_concept_name END  AS d5_concept_name,
                                                              CASE WHEN d6_concept_id > 0 AND
                                                                        (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d5_concept_id = -1
                                                                THEN NULL
                                                              ELSE d6_concept_name END  AS d6_concept_name,
                                                              CASE WHEN d7_concept_id > 0 AND
                                                                        (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d6_concept_id = -1
                                                                THEN NULL
                                                              ELSE d7_concept_name END  AS d7_concept_name,
                                                              CASE WHEN d8_concept_id > 0 AND
                                                                        (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d7_concept_id = -1
                                                                THEN NULL
                                                              ELSE d8_concept_name END  AS d8_concept_name,
                                                              CASE WHEN d9_concept_id > 0 AND
                                                                        (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d8_concept_id = -1
                                                                THEN NULL
                                                              ELSE d9_concept_name END  AS d9_concept_name,
                                                              CASE WHEN d10_concept_id > 0 AND
                                                                        (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d9_concept_id = -1
                                                                THEN NULL
                                                              ELSE d10_concept_name END AS d10_concept_name,
                                                              CASE WHEN d11_concept_id > 0 AND
                                                                        (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d10_concept_id = -1
                                                                THEN NULL
                                                              ELSE d11_concept_name END AS d11_concept_name,
                                                              CASE WHEN d12_concept_id > 0 AND
                                                                        (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d11_concept_id = -1
                                                                THEN NULL
                                                              ELSE d12_concept_name END AS d12_concept_name,
                                                              CASE WHEN d13_concept_id > 0 AND
                                                                        (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d12_concept_id = -1
                                                                THEN NULL
                                                              ELSE d13_concept_name END AS d13_concept_name,
                                                              CASE WHEN d14_concept_id > 0 AND
                                                                        (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d13_concept_id = -1
                                                                THEN NULL
                                                              ELSE d14_concept_name END AS d14_concept_name,
                                                              CASE WHEN d15_concept_id > 0 AND
                                                                        (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d14_concept_id = -1
                                                                THEN NULL
                                                              ELSE d15_concept_name END AS d15_concept_name,
                                                              CASE WHEN d16_concept_id > 0 AND
                                                                        (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d15_concept_id = -1
                                                                THEN NULL
                                                              ELSE d16_concept_name END AS d16_concept_name,
                                                              CASE WHEN d17_concept_id > 0 AND
                                                                        (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d16_concept_id = -1
                                                                THEN NULL
                                                              ELSE d17_concept_name END AS d17_concept_name,
                                                              CASE WHEN d18_concept_id > 0 AND
                                                                        (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d17_concept_id = -1
                                                                THEN NULL
                                                              ELSE d18_concept_name END AS d18_concept_name,
                                                              CASE WHEN d19_concept_id > 0 AND
                                                                        (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d18_concept_id = -1
                                                                THEN NULL
                                                              ELSE d19_concept_name END AS d19_concept_name,
                                                              CASE WHEN d20_concept_id > 0 AND
                                                                        num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d19_concept_id = -1
                                                                THEN NULL
                                                              ELSE d20_concept_name END AS d20_concept_name,
                                                              sum(num_persons)          AS num_persons
                                                            FROM
                                                              (
                                                                SELECT
                                                                  index_year,
                                                                  CASE WHEN d1_concept_id > 0 AND
                                                                            (d2_concept_id IS NULL OR
                                                                             d2_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  ELSE d1_concept_id END    AS d1_concept_id,
                                                                  CASE WHEN d2_concept_id > 0 AND
                                                                            (d3_concept_id IS NULL OR
                                                                             d3_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d1_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d2_concept_id END    AS d2_concept_id,
                                                                  CASE WHEN d3_concept_id > 0 AND
                                                                            (d4_concept_id IS NULL OR
                                                                             d4_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d2_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d3_concept_id END    AS d3_concept_id,
                                                                  CASE WHEN d4_concept_id > 0 AND
                                                                            (d5_concept_id IS NULL OR
                                                                             d5_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d3_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d4_concept_id END    AS d4_concept_id,
                                                                  CASE WHEN d5_concept_id > 0 AND
                                                                            (d6_concept_id IS NULL OR
                                                                             d6_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d4_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d5_concept_id END    AS d5_concept_id,
                                                                  CASE WHEN d6_concept_id > 0 AND
                                                                            (d7_concept_id IS NULL OR
                                                                             d7_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d5_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d6_concept_id END    AS d6_concept_id,
                                                                  CASE WHEN d7_concept_id > 0 AND
                                                                            (d8_concept_id IS NULL OR
                                                                             d8_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d6_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d7_concept_id END    AS d7_concept_id,
                                                                  CASE WHEN d8_concept_id > 0 AND
                                                                            (d9_concept_id IS NULL OR
                                                                             d9_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d7_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d8_concept_id END    AS d8_concept_id,
                                                                  CASE WHEN d9_concept_id > 0 AND
                                                                            (d10_concept_id IS NULL OR
                                                                             d10_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d8_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d9_concept_id END    AS d9_concept_id,
                                                                  CASE WHEN d10_concept_id > 0 AND
                                                                            (d11_concept_id IS NULL OR
                                                                             d11_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d9_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d10_concept_id END   AS d10_concept_id,
                                                                  CASE WHEN d11_concept_id > 0 AND
                                                                            (d12_concept_id IS NULL OR
                                                                             d12_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d10_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d11_concept_id END   AS d11_concept_id,
                                                                  CASE WHEN d12_concept_id > 0 AND
                                                                            (d13_concept_id IS NULL OR
                                                                             d13_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d11_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d12_concept_id END   AS d12_concept_id,
                                                                  CASE WHEN d13_concept_id > 0 AND
                                                                            (d14_concept_id IS NULL OR
                                                                             d14_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d12_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d13_concept_id END   AS d13_concept_id,
                                                                  CASE WHEN d14_concept_id > 0 AND
                                                                            (d15_concept_id IS NULL OR
                                                                             d15_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d13_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d14_concept_id END   AS d14_concept_id,
                                                                  CASE WHEN d15_concept_id > 0 AND
                                                                            (d16_concept_id IS NULL OR
                                                                             d16_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d14_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d15_concept_id END   AS d15_concept_id,
                                                                  CASE WHEN d16_concept_id > 0 AND
                                                                            (d17_concept_id IS NULL OR
                                                                             d17_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d15_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d16_concept_id END   AS d16_concept_id,
                                                                  CASE WHEN d17_concept_id > 0 AND
                                                                            (d18_concept_id IS NULL OR
                                                                             d18_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d16_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d17_concept_id END   AS d17_concept_id,
                                                                  CASE WHEN d18_concept_id > 0 AND
                                                                            (d19_concept_id IS NULL OR
                                                                             d19_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d17_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d18_concept_id END   AS d18_concept_id,
                                                                  CASE WHEN d19_concept_id > 0 AND
                                                                            (d20_concept_id IS NULL OR
                                                                             d20_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d18_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d19_concept_id END   AS d19_concept_id,
                                                                  CASE WHEN d20_concept_id > 0 AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d19_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d20_concept_id END   AS d20_concept_id,
                                                                  CASE WHEN d1_concept_id > 0 AND
                                                                            (d2_concept_id IS NULL OR
                                                                             d2_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  ELSE d1_concept_name END  AS d1_concept_name,
                                                                  CASE WHEN d2_concept_id > 0 AND
                                                                            (d3_concept_id IS NULL OR
                                                                             d3_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d1_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d2_concept_name END  AS d2_concept_name,
                                                                  CASE WHEN d3_concept_id > 0 AND
                                                                            (d4_concept_id IS NULL OR
                                                                             d4_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d2_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d3_concept_name END  AS d3_concept_name,
                                                                  CASE WHEN d4_concept_id > 0 AND
                                                                            (d5_concept_id IS NULL OR
                                                                             d5_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d3_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d4_concept_name END  AS d4_concept_name,
                                                                  CASE WHEN d5_concept_id > 0 AND
                                                                            (d6_concept_id IS NULL OR
                                                                             d6_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d4_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d5_concept_name END  AS d5_concept_name,
                                                                  CASE WHEN d6_concept_id > 0 AND
                                                                            (d7_concept_id IS NULL OR
                                                                             d7_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d5_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d6_concept_name END  AS d6_concept_name,
                                                                  CASE WHEN d7_concept_id > 0 AND
                                                                            (d8_concept_id IS NULL OR
                                                                             d8_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d6_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d7_concept_name END  AS d7_concept_name,
                                                                  CASE WHEN d8_concept_id > 0 AND
                                                                            (d9_concept_id IS NULL OR
                                                                             d9_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d7_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d8_concept_name END  AS d8_concept_name,
                                                                  CASE WHEN d9_concept_id > 0 AND
                                                                            (d10_concept_id IS NULL OR
                                                                             d10_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d8_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d9_concept_name END  AS d9_concept_name,
                                                                  CASE WHEN d10_concept_id > 0 AND
                                                                            (d11_concept_id IS NULL OR
                                                                             d11_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d9_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d10_concept_name END AS d10_concept_name,
                                                                  CASE WHEN d11_concept_id > 0 AND
                                                                            (d12_concept_id IS NULL OR
                                                                             d12_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d10_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d11_concept_name END AS d11_concept_name,
                                                                  CASE WHEN d12_concept_id > 0 AND
                                                                            (d13_concept_id IS NULL OR
                                                                             d13_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d11_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d12_concept_name END AS d12_concept_name,
                                                                  CASE WHEN d13_concept_id > 0 AND
                                                                            (d14_concept_id IS NULL OR
                                                                             d14_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d12_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d13_concept_name END AS d13_concept_name,
                                                                  CASE WHEN d14_concept_id > 0 AND
                                                                            (d15_concept_id IS NULL OR
                                                                             d15_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d13_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d14_concept_name END AS d14_concept_name,
                                                                  CASE WHEN d15_concept_id > 0 AND
                                                                            (d16_concept_id IS NULL OR
                                                                             d16_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d14_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d15_concept_name END AS d15_concept_name,
                                                                  CASE WHEN d16_concept_id > 0 AND
                                                                            (d17_concept_id IS NULL OR
                                                                             d17_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d15_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d16_concept_name END AS d16_concept_name,
                                                                  CASE WHEN d17_concept_id > 0 AND
                                                                            (d18_concept_id IS NULL OR
                                                                             d18_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d16_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d17_concept_name END AS d17_concept_name,
                                                                  CASE WHEN d18_concept_id > 0 AND
                                                                            (d19_concept_id IS NULL OR
                                                                             d19_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d17_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d18_concept_name END AS d18_concept_name,
                                                                  CASE WHEN d19_concept_id > 0 AND
                                                                            (d20_concept_id IS NULL OR
                                                                             d20_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d18_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d19_concept_name END AS d19_concept_name,
                                                                  CASE WHEN d20_concept_id > 0 AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d19_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d20_concept_name END AS d20_concept_name,
                                                                  sum(num_persons)          AS num_persons
                                                                FROM
                                                                  (
                                                                    SELECT
                                                                      index_year,
                                                                      CASE WHEN d1_concept_id > 0 AND
                                                                                (d2_concept_id IS NULL OR
                                                                                 d2_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      ELSE d1_concept_id END    AS d1_concept_id,
                                                                      CASE WHEN d2_concept_id > 0 AND
                                                                                (d3_concept_id IS NULL OR
                                                                                 d3_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d1_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d2_concept_id END    AS d2_concept_id,
                                                                      CASE WHEN d3_concept_id > 0 AND
                                                                                (d4_concept_id IS NULL OR
                                                                                 d4_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d2_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d3_concept_id END    AS d3_concept_id,
                                                                      CASE WHEN d4_concept_id > 0 AND
                                                                                (d5_concept_id IS NULL OR
                                                                                 d5_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d3_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d4_concept_id END    AS d4_concept_id,
                                                                      CASE WHEN d5_concept_id > 0 AND
                                                                                (d6_concept_id IS NULL OR
                                                                                 d6_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d4_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d5_concept_id END    AS d5_concept_id,
                                                                      CASE WHEN d6_concept_id > 0 AND
                                                                                (d7_concept_id IS NULL OR
                                                                                 d7_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d5_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d6_concept_id END    AS d6_concept_id,
                                                                      CASE WHEN d7_concept_id > 0 AND
                                                                                (d8_concept_id IS NULL OR
                                                                                 d8_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d6_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d7_concept_id END    AS d7_concept_id,
                                                                      CASE WHEN d8_concept_id > 0 AND
                                                                                (d9_concept_id IS NULL OR
                                                                                 d9_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d7_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d8_concept_id END    AS d8_concept_id,
                                                                      CASE WHEN d9_concept_id > 0 AND
                                                                                (d10_concept_id IS NULL OR
                                                                                 d10_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d8_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d9_concept_id END    AS d9_concept_id,
                                                                      CASE WHEN d10_concept_id > 0 AND
                                                                                (d11_concept_id IS NULL OR
                                                                                 d11_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d9_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d10_concept_id END   AS d10_concept_id,
                                                                      CASE WHEN d11_concept_id > 0 AND
                                                                                (d12_concept_id IS NULL OR
                                                                                 d12_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d10_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d11_concept_id END   AS d11_concept_id,
                                                                      CASE WHEN d12_concept_id > 0 AND
                                                                                (d13_concept_id IS NULL OR
                                                                                 d13_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d11_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d12_concept_id END   AS d12_concept_id,
                                                                      CASE WHEN d13_concept_id > 0 AND
                                                                                (d14_concept_id IS NULL OR
                                                                                 d14_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d12_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d13_concept_id END   AS d13_concept_id,
                                                                      CASE WHEN d14_concept_id > 0 AND
                                                                                (d15_concept_id IS NULL OR
                                                                                 d15_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d13_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d14_concept_id END   AS d14_concept_id,
                                                                      CASE WHEN d15_concept_id > 0 AND
                                                                                (d16_concept_id IS NULL OR
                                                                                 d16_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d14_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d15_concept_id END   AS d15_concept_id,
                                                                      CASE WHEN d16_concept_id > 0 AND
                                                                                (d17_concept_id IS NULL OR
                                                                                 d17_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d15_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d16_concept_id END   AS d16_concept_id,
                                                                      CASE WHEN d17_concept_id > 0 AND
                                                                                (d18_concept_id IS NULL OR
                                                                                 d18_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d16_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d17_concept_id END   AS d17_concept_id,
                                                                      CASE WHEN d18_concept_id > 0 AND
                                                                                (d19_concept_id IS NULL OR
                                                                                 d19_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d17_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d18_concept_id END   AS d18_concept_id,
                                                                      CASE WHEN d19_concept_id > 0 AND
                                                                                (d20_concept_id IS NULL OR
                                                                                 d20_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d18_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d19_concept_id END   AS d19_concept_id,
                                                                      CASE WHEN d20_concept_id > 0 AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d19_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d20_concept_id END   AS d20_concept_id,
                                                                      CASE WHEN d1_concept_id > 0 AND
                                                                                (d2_concept_id IS NULL OR
                                                                                 d2_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      ELSE d1_concept_name END  AS d1_concept_name,
                                                                      CASE WHEN d2_concept_id > 0 AND
                                                                                (d3_concept_id IS NULL OR
                                                                                 d3_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d1_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d2_concept_name END  AS d2_concept_name,
                                                                      CASE WHEN d3_concept_id > 0 AND
                                                                                (d4_concept_id IS NULL OR
                                                                                 d4_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d2_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d3_concept_name END  AS d3_concept_name,
                                                                      CASE WHEN d4_concept_id > 0 AND
                                                                                (d5_concept_id IS NULL OR
                                                                                 d5_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d3_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d4_concept_name END  AS d4_concept_name,
                                                                      CASE WHEN d5_concept_id > 0 AND
                                                                                (d6_concept_id IS NULL OR
                                                                                 d6_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d4_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d5_concept_name END  AS d5_concept_name,
                                                                      CASE WHEN d6_concept_id > 0 AND
                                                                                (d7_concept_id IS NULL OR
                                                                                 d7_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d5_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d6_concept_name END  AS d6_concept_name,
                                                                      CASE WHEN d7_concept_id > 0 AND
                                                                                (d8_concept_id IS NULL OR
                                                                                 d8_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d6_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d7_concept_name END  AS d7_concept_name,
                                                                      CASE WHEN d8_concept_id > 0 AND
                                                                                (d9_concept_id IS NULL OR
                                                                                 d9_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d7_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d8_concept_name END  AS d8_concept_name,
                                                                      CASE WHEN d9_concept_id > 0 AND
                                                                                (d10_concept_id IS NULL OR
                                                                                 d10_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d8_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d9_concept_name END  AS d9_concept_name,
                                                                      CASE WHEN d10_concept_id > 0 AND
                                                                                (d11_concept_id IS NULL OR
                                                                                 d11_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d9_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d10_concept_name END AS d10_concept_name,
                                                                      CASE WHEN d11_concept_id > 0 AND
                                                                                (d12_concept_id IS NULL OR
                                                                                 d12_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d10_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d11_concept_name END AS d11_concept_name,
                                                                      CASE WHEN d12_concept_id > 0 AND
                                                                                (d13_concept_id IS NULL OR
                                                                                 d13_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d11_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d12_concept_name END AS d12_concept_name,
                                                                      CASE WHEN d13_concept_id > 0 AND
                                                                                (d14_concept_id IS NULL OR
                                                                                 d14_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d12_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d13_concept_name END AS d13_concept_name,
                                                                      CASE WHEN d14_concept_id > 0 AND
                                                                                (d15_concept_id IS NULL OR
                                                                                 d15_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d13_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d14_concept_name END AS d14_concept_name,
                                                                      CASE WHEN d15_concept_id > 0 AND
                                                                                (d16_concept_id IS NULL OR
                                                                                 d16_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d14_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d15_concept_name END AS d15_concept_name,
                                                                      CASE WHEN d16_concept_id > 0 AND
                                                                                (d17_concept_id IS NULL OR
                                                                                 d17_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d15_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d16_concept_name END AS d16_concept_name,
                                                                      CASE WHEN d17_concept_id > 0 AND
                                                                                (d18_concept_id IS NULL OR
                                                                                 d18_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d16_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d17_concept_name END AS d17_concept_name,
                                                                      CASE WHEN d18_concept_id > 0 AND
                                                                                (d19_concept_id IS NULL OR
                                                                                 d19_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d17_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d18_concept_name END AS d18_concept_name,
                                                                      CASE WHEN d19_concept_id > 0 AND
                                                                                (d20_concept_id IS NULL OR
                                                                                 d20_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d18_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d19_concept_name END AS d19_concept_name,
                                                                      CASE WHEN d20_concept_id > 0 AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d19_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d20_concept_name END AS d20_concept_name,
                                                                      sum(num_persons)          AS num_persons
                                                                    FROM
                                                                      (
                                                                        SELECT
                                                                          index_year,
                                                                          CASE WHEN d1_concept_id > 0 AND
                                                                                    (d2_concept_id IS NULL OR
                                                                                     d2_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          ELSE d1_concept_id END    AS d1_concept_id,
                                                                          CASE WHEN d2_concept_id > 0 AND
                                                                                    (d3_concept_id IS NULL OR
                                                                                     d3_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d1_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d2_concept_id END    AS d2_concept_id,
                                                                          CASE WHEN d3_concept_id > 0 AND
                                                                                    (d4_concept_id IS NULL OR
                                                                                     d4_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d2_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d3_concept_id END    AS d3_concept_id,
                                                                          CASE WHEN d4_concept_id > 0 AND
                                                                                    (d5_concept_id IS NULL OR
                                                                                     d5_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d3_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d4_concept_id END    AS d4_concept_id,
                                                                          CASE WHEN d5_concept_id > 0 AND
                                                                                    (d6_concept_id IS NULL OR
                                                                                     d6_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d4_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d5_concept_id END    AS d5_concept_id,
                                                                          CASE WHEN d6_concept_id > 0 AND
                                                                                    (d7_concept_id IS NULL OR
                                                                                     d7_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d5_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d6_concept_id END    AS d6_concept_id,
                                                                          CASE WHEN d7_concept_id > 0 AND
                                                                                    (d8_concept_id IS NULL OR
                                                                                     d8_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d6_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d7_concept_id END    AS d7_concept_id,
                                                                          CASE WHEN d8_concept_id > 0 AND
                                                                                    (d9_concept_id IS NULL OR
                                                                                     d9_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d7_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d8_concept_id END    AS d8_concept_id,
                                                                          CASE WHEN d9_concept_id > 0 AND
                                                                                    (d10_concept_id IS NULL OR
                                                                                     d10_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d8_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d9_concept_id END    AS d9_concept_id,
                                                                          CASE WHEN d10_concept_id > 0 AND
                                                                                    (d11_concept_id IS NULL OR
                                                                                     d11_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d9_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d10_concept_id END   AS d10_concept_id,
                                                                          CASE WHEN d11_concept_id > 0 AND
                                                                                    (d12_concept_id IS NULL OR
                                                                                     d12_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d10_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d11_concept_id END   AS d11_concept_id,
                                                                          CASE WHEN d12_concept_id > 0 AND
                                                                                    (d13_concept_id IS NULL OR
                                                                                     d13_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d11_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d12_concept_id END   AS d12_concept_id,
                                                                          CASE WHEN d13_concept_id > 0 AND
                                                                                    (d14_concept_id IS NULL OR
                                                                                     d14_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d12_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d13_concept_id END   AS d13_concept_id,
                                                                          CASE WHEN d14_concept_id > 0 AND
                                                                                    (d15_concept_id IS NULL OR
                                                                                     d15_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d13_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d14_concept_id END   AS d14_concept_id,
                                                                          CASE WHEN d15_concept_id > 0 AND
                                                                                    (d16_concept_id IS NULL OR
                                                                                     d16_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d14_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d15_concept_id END   AS d15_concept_id,
                                                                          CASE WHEN d16_concept_id > 0 AND
                                                                                    (d17_concept_id IS NULL OR
                                                                                     d17_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d15_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d16_concept_id END   AS d16_concept_id,
                                                                          CASE WHEN d17_concept_id > 0 AND
                                                                                    (d18_concept_id IS NULL OR
                                                                                     d18_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d16_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d17_concept_id END   AS d17_concept_id,
                                                                          CASE WHEN d18_concept_id > 0 AND
                                                                                    (d19_concept_id IS NULL OR
                                                                                     d19_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d17_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d18_concept_id END   AS d18_concept_id,
                                                                          CASE WHEN d19_concept_id > 0 AND
                                                                                    (d20_concept_id IS NULL OR
                                                                                     d20_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d18_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d19_concept_id END   AS d19_concept_id,
                                                                          CASE WHEN d20_concept_id > 0 AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d19_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d20_concept_id END   AS d20_concept_id,
                                                                          CASE WHEN d1_concept_id > 0 AND
                                                                                    (d2_concept_id IS NULL OR
                                                                                     d2_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          ELSE d1_concept_name END  AS d1_concept_name,
                                                                          CASE WHEN d2_concept_id > 0 AND
                                                                                    (d3_concept_id IS NULL OR
                                                                                     d3_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d1_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d2_concept_name END  AS d2_concept_name,
                                                                          CASE WHEN d3_concept_id > 0 AND
                                                                                    (d4_concept_id IS NULL OR
                                                                                     d4_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d2_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d3_concept_name END  AS d3_concept_name,
                                                                          CASE WHEN d4_concept_id > 0 AND
                                                                                    (d5_concept_id IS NULL OR
                                                                                     d5_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d3_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d4_concept_name END  AS d4_concept_name,
                                                                          CASE WHEN d5_concept_id > 0 AND
                                                                                    (d6_concept_id IS NULL OR
                                                                                     d6_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d4_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d5_concept_name END  AS d5_concept_name,
                                                                          CASE WHEN d6_concept_id > 0 AND
                                                                                    (d7_concept_id IS NULL OR
                                                                                     d7_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d5_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d6_concept_name END  AS d6_concept_name,
                                                                          CASE WHEN d7_concept_id > 0 AND
                                                                                    (d8_concept_id IS NULL OR
                                                                                     d8_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d6_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d7_concept_name END  AS d7_concept_name,
                                                                          CASE WHEN d8_concept_id > 0 AND
                                                                                    (d9_concept_id IS NULL OR
                                                                                     d9_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d7_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d8_concept_name END  AS d8_concept_name,
                                                                          CASE WHEN d9_concept_id > 0 AND
                                                                                    (d10_concept_id IS NULL OR
                                                                                     d10_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d8_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d9_concept_name END  AS d9_concept_name,
                                                                          CASE WHEN d10_concept_id > 0 AND
                                                                                    (d11_concept_id IS NULL OR
                                                                                     d11_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d9_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d10_concept_name END AS d10_concept_name,
                                                                          CASE WHEN d11_concept_id > 0 AND
                                                                                    (d12_concept_id IS NULL OR
                                                                                     d12_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d10_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d11_concept_name END AS d11_concept_name,
                                                                          CASE WHEN d12_concept_id > 0 AND
                                                                                    (d13_concept_id IS NULL OR
                                                                                     d13_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d11_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d12_concept_name END AS d12_concept_name,
                                                                          CASE WHEN d13_concept_id > 0 AND
                                                                                    (d14_concept_id IS NULL OR
                                                                                     d14_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d12_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d13_concept_name END AS d13_concept_name,
                                                                          CASE WHEN d14_concept_id > 0 AND
                                                                                    (d15_concept_id IS NULL OR
                                                                                     d15_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d13_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d14_concept_name END AS d14_concept_name,
                                                                          CASE WHEN d15_concept_id > 0 AND
                                                                                    (d16_concept_id IS NULL OR
                                                                                     d16_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d14_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d15_concept_name END AS d15_concept_name,
                                                                          CASE WHEN d16_concept_id > 0 AND
                                                                                    (d17_concept_id IS NULL OR
                                                                                     d17_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d15_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d16_concept_name END AS d16_concept_name,
                                                                          CASE WHEN d17_concept_id > 0 AND
                                                                                    (d18_concept_id IS NULL OR
                                                                                     d18_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d16_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d17_concept_name END AS d17_concept_name,
                                                                          CASE WHEN d18_concept_id > 0 AND
                                                                                    (d19_concept_id IS NULL OR
                                                                                     d19_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d17_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d18_concept_name END AS d18_concept_name,
                                                                          CASE WHEN d19_concept_id > 0 AND
                                                                                    (d20_concept_id IS NULL OR
                                                                                     d20_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d18_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d19_concept_name END AS d19_concept_name,
                                                                          CASE WHEN d20_concept_id > 0 AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d19_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d20_concept_name END AS d20_concept_name,
                                                                          sum(num_persons)          AS num_persons
                                                                        FROM
                                                                          (
                                                                            SELECT
                                                                              index_year,
                                                                              CASE WHEN d1_concept_id > 0 AND
                                                                                        (d2_concept_id IS NULL OR
                                                                                         d2_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              ELSE d1_concept_id END    AS d1_concept_id,
                                                                              CASE WHEN d2_concept_id > 0 AND
                                                                                        (d3_concept_id IS NULL OR
                                                                                         d3_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d1_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d2_concept_id END    AS d2_concept_id,
                                                                              CASE WHEN d3_concept_id > 0 AND
                                                                                        (d4_concept_id IS NULL OR
                                                                                         d4_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d2_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d3_concept_id END    AS d3_concept_id,
                                                                              CASE WHEN d4_concept_id > 0 AND
                                                                                        (d5_concept_id IS NULL OR
                                                                                         d5_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d3_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d4_concept_id END    AS d4_concept_id,
                                                                              CASE WHEN d5_concept_id > 0 AND
                                                                                        (d6_concept_id IS NULL OR
                                                                                         d6_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d4_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d5_concept_id END    AS d5_concept_id,
                                                                              CASE WHEN d6_concept_id > 0 AND
                                                                                        (d7_concept_id IS NULL OR
                                                                                         d7_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d5_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d6_concept_id END    AS d6_concept_id,
                                                                              CASE WHEN d7_concept_id > 0 AND
                                                                                        (d8_concept_id IS NULL OR
                                                                                         d8_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d6_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d7_concept_id END    AS d7_concept_id,
                                                                              CASE WHEN d8_concept_id > 0 AND
                                                                                        (d9_concept_id IS NULL OR
                                                                                         d9_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d7_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d8_concept_id END    AS d8_concept_id,
                                                                              CASE WHEN d9_concept_id > 0 AND
                                                                                        (d10_concept_id IS NULL OR
                                                                                         d10_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d8_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d9_concept_id END    AS d9_concept_id,
                                                                              CASE WHEN d10_concept_id > 0 AND
                                                                                        (d11_concept_id IS NULL OR
                                                                                         d11_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d9_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d10_concept_id END   AS d10_concept_id,
                                                                              CASE WHEN d11_concept_id > 0 AND
                                                                                        (d12_concept_id IS NULL OR
                                                                                         d12_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d10_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d11_concept_id END   AS d11_concept_id,
                                                                              CASE WHEN d12_concept_id > 0 AND
                                                                                        (d13_concept_id IS NULL OR
                                                                                         d13_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d11_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d12_concept_id END   AS d12_concept_id,
                                                                              CASE WHEN d13_concept_id > 0 AND
                                                                                        (d14_concept_id IS NULL OR
                                                                                         d14_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d12_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d13_concept_id END   AS d13_concept_id,
                                                                              CASE WHEN d14_concept_id > 0 AND
                                                                                        (d15_concept_id IS NULL OR
                                                                                         d15_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d13_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d14_concept_id END   AS d14_concept_id,
                                                                              CASE WHEN d15_concept_id > 0 AND
                                                                                        (d16_concept_id IS NULL OR
                                                                                         d16_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d14_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d15_concept_id END   AS d15_concept_id,
                                                                              CASE WHEN d16_concept_id > 0 AND
                                                                                        (d17_concept_id IS NULL OR
                                                                                         d17_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d15_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d16_concept_id END   AS d16_concept_id,
                                                                              CASE WHEN d17_concept_id > 0 AND
                                                                                        (d18_concept_id IS NULL OR
                                                                                         d18_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d16_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d17_concept_id END   AS d17_concept_id,
                                                                              CASE WHEN d18_concept_id > 0 AND
                                                                                        (d19_concept_id IS NULL OR
                                                                                         d19_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d17_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d18_concept_id END   AS d18_concept_id,
                                                                              CASE WHEN d19_concept_id > 0 AND
                                                                                        (d20_concept_id IS NULL OR
                                                                                         d20_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d18_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d19_concept_id END   AS d19_concept_id,
                                                                              CASE WHEN d20_concept_id > 0 AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN -1
                                                                              WHEN d19_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d20_concept_id END   AS d20_concept_id,
                                                                              CASE WHEN d1_concept_id > 0 AND
                                                                                        (d2_concept_id IS NULL OR
                                                                                         d2_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              ELSE d1_concept_name END  AS d1_concept_name,
                                                                              CASE WHEN d2_concept_id > 0 AND
                                                                                        (d3_concept_id IS NULL OR
                                                                                         d3_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d1_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d2_concept_name END  AS d2_concept_name,
                                                                              CASE WHEN d3_concept_id > 0 AND
                                                                                        (d4_concept_id IS NULL OR
                                                                                         d4_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d2_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d3_concept_name END  AS d3_concept_name,
                                                                              CASE WHEN d4_concept_id > 0 AND
                                                                                        (d5_concept_id IS NULL OR
                                                                                         d5_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d3_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d4_concept_name END  AS d4_concept_name,
                                                                              CASE WHEN d5_concept_id > 0 AND
                                                                                        (d6_concept_id IS NULL OR
                                                                                         d6_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d4_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d5_concept_name END  AS d5_concept_name,
                                                                              CASE WHEN d6_concept_id > 0 AND
                                                                                        (d7_concept_id IS NULL OR
                                                                                         d7_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d5_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d6_concept_name END  AS d6_concept_name,
                                                                              CASE WHEN d7_concept_id > 0 AND
                                                                                        (d8_concept_id IS NULL OR
                                                                                         d8_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d6_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d7_concept_name END  AS d7_concept_name,
                                                                              CASE WHEN d8_concept_id > 0 AND
                                                                                        (d9_concept_id IS NULL OR
                                                                                         d9_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d7_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d8_concept_name END  AS d8_concept_name,
                                                                              CASE WHEN d9_concept_id > 0 AND
                                                                                        (d10_concept_id IS NULL OR
                                                                                         d10_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d8_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d9_concept_name END  AS d9_concept_name,
                                                                              CASE WHEN d10_concept_id > 0 AND
                                                                                        (d11_concept_id IS NULL OR
                                                                                         d11_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d9_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d10_concept_name END AS d10_concept_name,
                                                                              CASE WHEN d11_concept_id > 0 AND
                                                                                        (d12_concept_id IS NULL OR
                                                                                         d12_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d10_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d11_concept_name END AS d11_concept_name,
                                                                              CASE WHEN d12_concept_id > 0 AND
                                                                                        (d13_concept_id IS NULL OR
                                                                                         d13_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d11_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d12_concept_name END AS d12_concept_name,
                                                                              CASE WHEN d13_concept_id > 0 AND
                                                                                        (d14_concept_id IS NULL OR
                                                                                         d14_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d12_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d13_concept_name END AS d13_concept_name,
                                                                              CASE WHEN d14_concept_id > 0 AND
                                                                                        (d15_concept_id IS NULL OR
                                                                                         d15_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d13_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d14_concept_name END AS d14_concept_name,
                                                                              CASE WHEN d15_concept_id > 0 AND
                                                                                        (d16_concept_id IS NULL OR
                                                                                         d16_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d14_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d15_concept_name END AS d15_concept_name,
                                                                              CASE WHEN d16_concept_id > 0 AND
                                                                                        (d17_concept_id IS NULL OR
                                                                                         d17_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d15_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d16_concept_name END AS d16_concept_name,
                                                                              CASE WHEN d17_concept_id > 0 AND
                                                                                        (d18_concept_id IS NULL OR
                                                                                         d18_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d16_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d17_concept_name END AS d17_concept_name,
                                                                              CASE WHEN d18_concept_id > 0 AND
                                                                                        (d19_concept_id IS NULL OR
                                                                                         d19_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d17_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d18_concept_name END AS d18_concept_name,
                                                                              CASE WHEN d19_concept_id > 0 AND
                                                                                        (d20_concept_id IS NULL OR
                                                                                         d20_concept_id = -1) AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d18_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d19_concept_name END AS d19_concept_name,
                                                                              CASE WHEN d20_concept_id > 0 AND
                                                                                        num_persons < @smallcellcount
                                                                                THEN 'Other'
                                                                              WHEN d19_concept_id = -1
                                                                                THEN NULL
                                                                              ELSE d20_concept_name END AS d20_concept_name,
                                                                              sum(num_persons)          AS num_persons
                                                                            FROM @studyName_drug_seq_summary_temp
                                                                                  GROUP BY index_year,
                                                                                  CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND num_persons < @smallcellcount THEN -1 ELSE d1_concept_id END,
                                                                                                                                                                                                                             CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d1_concept_id = -1 THEN NULL ELSE d2_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                        CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d2_concept_id = -1 THEN NULL ELSE d3_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d3_concept_id = -1 THEN NULL ELSE d4_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d4_concept_id = -1 THEN NULL ELSE d5_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d5_concept_id = -1 THEN NULL ELSE d6_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d6_concept_id = -1 THEN NULL ELSE d7_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d7_concept_id = -1 THEN NULL ELSE d8_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d8_concept_id = -1 THEN NULL ELSE d9_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d9_concept_id = -1 THEN NULL ELSE d10_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d10_concept_id = -1 THEN NULL ELSE d11_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d11_concept_id = -1 THEN NULL ELSE d12_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d12_concept_id = -1 THEN NULL ELSE d13_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d13_concept_id = -1 THEN NULL ELSE d14_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d14_concept_id = -1 THEN NULL ELSE d15_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d15_concept_id = -1 THEN NULL ELSE d16_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d16_concept_id = -1 THEN NULL ELSE d17_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d17_concept_id = -1 THEN NULL ELSE d18_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND num_persons < @smallcellcount THEN -1 WHEN d18_concept_id = -1 THEN NULL ELSE d19_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount THEN -1 WHEN d19_concept_id = -1 THEN NULL ELSE d20_concept_id END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' ELSE d1_concept_name END,
                                                                            CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d1_concept_id = -1 THEN NULL ELSE d2_concept_name END,
                                                                                                                                                                                                                              CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d2_concept_id = -1 THEN NULL ELSE d3_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d3_concept_id = -1 THEN NULL ELSE d4_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d4_concept_id = -1 THEN NULL ELSE d5_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d5_concept_id = -1 THEN NULL ELSE d6_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d6_concept_id = -1 THEN NULL ELSE d7_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d7_concept_id = -1 THEN NULL ELSE d8_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d8_concept_id = -1 THEN NULL ELSE d9_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d9_concept_id = -1 THEN NULL ELSE d10_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d10_concept_id = -1 THEN NULL ELSE d11_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d11_concept_id = -1 THEN NULL ELSE d12_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d12_concept_id = -1 THEN NULL ELSE d13_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d13_concept_id = -1 THEN NULL ELSE d14_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d14_concept_id = -1 THEN NULL ELSE d15_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d15_concept_id = -1 THEN NULL ELSE d16_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d16_concept_id = -1 THEN NULL ELSE d17_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d17_concept_id = -1 THEN NULL ELSE d18_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND num_persons < @smallcellcount THEN 'Other' WHEN d18_concept_id = -1 THEN NULL ELSE d19_concept_name END,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount THEN 'Other' WHEN d19_concept_id = -1 THEN NULL ELSE d20_concept_name END
                                                                          ) t1
                                                                        GROUP BY index_year,
                                                                          CASE WHEN d1_concept_id > 0 AND
                                                                                    (d2_concept_id IS NULL OR
                                                                                     d2_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          ELSE d1_concept_id END,
                                                                          CASE WHEN d2_concept_id > 0 AND
                                                                                    (d3_concept_id IS NULL OR
                                                                                     d3_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d1_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d2_concept_id END,
                                                                          CASE WHEN d3_concept_id > 0 AND
                                                                                    (d4_concept_id IS NULL OR
                                                                                     d4_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d2_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d3_concept_id END,
                                                                          CASE WHEN d4_concept_id > 0 AND
                                                                                    (d5_concept_id IS NULL OR
                                                                                     d5_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d3_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d4_concept_id END,
                                                                          CASE WHEN d5_concept_id > 0 AND
                                                                                    (d6_concept_id IS NULL OR
                                                                                     d6_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d4_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d5_concept_id END,
                                                                          CASE WHEN d6_concept_id > 0 AND
                                                                                    (d7_concept_id IS NULL OR
                                                                                     d7_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d5_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d6_concept_id END,
                                                                          CASE WHEN d7_concept_id > 0 AND
                                                                                    (d8_concept_id IS NULL OR
                                                                                     d8_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d6_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d7_concept_id END,
                                                                          CASE WHEN d8_concept_id > 0 AND
                                                                                    (d9_concept_id IS NULL OR
                                                                                     d9_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d7_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d8_concept_id END,
                                                                          CASE WHEN d9_concept_id > 0 AND
                                                                                    (d10_concept_id IS NULL OR
                                                                                     d10_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d8_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d9_concept_id END,
                                                                          CASE WHEN d10_concept_id > 0 AND
                                                                                    (d11_concept_id IS NULL OR
                                                                                     d11_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d9_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d10_concept_id END,
                                                                          CASE WHEN d11_concept_id > 0 AND
                                                                                    (d12_concept_id IS NULL OR
                                                                                     d12_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d10_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d11_concept_id END,
                                                                          CASE WHEN d12_concept_id > 0 AND
                                                                                    (d13_concept_id IS NULL OR
                                                                                     d13_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d11_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d12_concept_id END,
                                                                          CASE WHEN d13_concept_id > 0 AND
                                                                                    (d14_concept_id IS NULL OR
                                                                                     d14_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d12_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d13_concept_id END,
                                                                          CASE WHEN d14_concept_id > 0 AND
                                                                                    (d15_concept_id IS NULL OR
                                                                                     d15_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d13_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d14_concept_id END,
                                                                          CASE WHEN d15_concept_id > 0 AND
                                                                                    (d16_concept_id IS NULL OR
                                                                                     d16_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d14_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d15_concept_id END,
                                                                          CASE WHEN d16_concept_id > 0 AND
                                                                                    (d17_concept_id IS NULL OR
                                                                                     d17_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d15_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d16_concept_id END,
                                                                          CASE WHEN d17_concept_id > 0 AND
                                                                                    (d18_concept_id IS NULL OR
                                                                                     d18_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d16_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d17_concept_id END,
                                                                          CASE WHEN d18_concept_id > 0 AND
                                                                                    (d19_concept_id IS NULL OR
                                                                                     d19_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d17_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d18_concept_id END,
                                                                          CASE WHEN d19_concept_id > 0 AND
                                                                                    (d20_concept_id IS NULL OR
                                                                                     d20_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d18_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d19_concept_id END,
                                                                          CASE WHEN d20_concept_id > 0 AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN -1
                                                                          WHEN d19_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d20_concept_id END,
                                                                          CASE WHEN d1_concept_id > 0 AND
                                                                                    (d2_concept_id IS NULL OR
                                                                                     d2_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          ELSE d1_concept_name END,
                                                                          CASE WHEN d2_concept_id > 0 AND
                                                                                    (d3_concept_id IS NULL OR
                                                                                     d3_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d1_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d2_concept_name END,
                                                                          CASE WHEN d3_concept_id > 0 AND
                                                                                    (d4_concept_id IS NULL OR
                                                                                     d4_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d2_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d3_concept_name END,
                                                                          CASE WHEN d4_concept_id > 0 AND
                                                                                    (d5_concept_id IS NULL OR
                                                                                     d5_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d3_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d4_concept_name END,
                                                                          CASE WHEN d5_concept_id > 0 AND
                                                                                    (d6_concept_id IS NULL OR
                                                                                     d6_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d4_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d5_concept_name END,
                                                                          CASE WHEN d6_concept_id > 0 AND
                                                                                    (d7_concept_id IS NULL OR
                                                                                     d7_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d5_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d6_concept_name END,
                                                                          CASE WHEN d7_concept_id > 0 AND
                                                                                    (d8_concept_id IS NULL OR
                                                                                     d8_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d6_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d7_concept_name END,
                                                                          CASE WHEN d8_concept_id > 0 AND
                                                                                    (d9_concept_id IS NULL OR
                                                                                     d9_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d7_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d8_concept_name END,
                                                                          CASE WHEN d9_concept_id > 0 AND
                                                                                    (d10_concept_id IS NULL OR
                                                                                     d10_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d8_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d9_concept_name END,
                                                                          CASE WHEN d10_concept_id > 0 AND
                                                                                    (d11_concept_id IS NULL OR
                                                                                     d11_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d9_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d10_concept_name END,
                                                                          CASE WHEN d11_concept_id > 0 AND
                                                                                    (d12_concept_id IS NULL OR
                                                                                     d12_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d10_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d11_concept_name END,
                                                                          CASE WHEN d12_concept_id > 0 AND
                                                                                    (d13_concept_id IS NULL OR
                                                                                     d13_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d11_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d12_concept_name END,
                                                                          CASE WHEN d13_concept_id > 0 AND
                                                                                    (d14_concept_id IS NULL OR
                                                                                     d14_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d12_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d13_concept_name END,
                                                                          CASE WHEN d14_concept_id > 0 AND
                                                                                    (d15_concept_id IS NULL OR
                                                                                     d15_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d13_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d14_concept_name END,
                                                                          CASE WHEN d15_concept_id > 0 AND
                                                                                    (d16_concept_id IS NULL OR
                                                                                     d16_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d14_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d15_concept_name END,
                                                                          CASE WHEN d16_concept_id > 0 AND
                                                                                    (d17_concept_id IS NULL OR
                                                                                     d17_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d15_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d16_concept_name END,
                                                                          CASE WHEN d17_concept_id > 0 AND
                                                                                    (d18_concept_id IS NULL OR
                                                                                     d18_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d16_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d17_concept_name END,
                                                                          CASE WHEN d18_concept_id > 0 AND
                                                                                    (d19_concept_id IS NULL OR
                                                                                     d19_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d17_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d18_concept_name END,
                                                                          CASE WHEN d19_concept_id > 0 AND
                                                                                    (d20_concept_id IS NULL OR
                                                                                     d20_concept_id = -1) AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d18_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d19_concept_name END,
                                                                          CASE WHEN d20_concept_id > 0 AND
                                                                                    num_persons < @smallcellcount
                                                                            THEN 'Other'
                                                                          WHEN d19_concept_id = -1
                                                                            THEN NULL
                                                                          ELSE d20_concept_name END
                                                                      ) t2
                                                                    GROUP BY index_year,
                                                                      CASE WHEN d1_concept_id > 0 AND
                                                                                (d2_concept_id IS NULL OR
                                                                                 d2_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      ELSE d1_concept_id END,
                                                                      CASE WHEN d2_concept_id > 0 AND
                                                                                (d3_concept_id IS NULL OR
                                                                                 d3_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d1_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d2_concept_id END,
                                                                      CASE WHEN d3_concept_id > 0 AND
                                                                                (d4_concept_id IS NULL OR
                                                                                 d4_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d2_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d3_concept_id END,
                                                                      CASE WHEN d4_concept_id > 0 AND
                                                                                (d5_concept_id IS NULL OR
                                                                                 d5_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d3_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d4_concept_id END,
                                                                      CASE WHEN d5_concept_id > 0 AND
                                                                                (d6_concept_id IS NULL OR
                                                                                 d6_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d4_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d5_concept_id END,
                                                                      CASE WHEN d6_concept_id > 0 AND
                                                                                (d7_concept_id IS NULL OR
                                                                                 d7_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d5_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d6_concept_id END,
                                                                      CASE WHEN d7_concept_id > 0 AND
                                                                                (d8_concept_id IS NULL OR
                                                                                 d8_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d6_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d7_concept_id END,
                                                                      CASE WHEN d8_concept_id > 0 AND
                                                                                (d9_concept_id IS NULL OR
                                                                                 d9_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d7_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d8_concept_id END,
                                                                      CASE WHEN d9_concept_id > 0 AND
                                                                                (d10_concept_id IS NULL OR
                                                                                 d10_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d8_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d9_concept_id END,
                                                                      CASE WHEN d10_concept_id > 0 AND
                                                                                (d11_concept_id IS NULL OR
                                                                                 d11_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d9_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d10_concept_id END,
                                                                      CASE WHEN d11_concept_id > 0 AND
                                                                                (d12_concept_id IS NULL OR
                                                                                 d12_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d10_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d11_concept_id END,
                                                                      CASE WHEN d12_concept_id > 0 AND
                                                                                (d13_concept_id IS NULL OR
                                                                                 d13_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d11_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d12_concept_id END,
                                                                      CASE WHEN d13_concept_id > 0 AND
                                                                                (d14_concept_id IS NULL OR
                                                                                 d14_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d12_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d13_concept_id END,
                                                                      CASE WHEN d14_concept_id > 0 AND
                                                                                (d15_concept_id IS NULL OR
                                                                                 d15_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d13_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d14_concept_id END,
                                                                      CASE WHEN d15_concept_id > 0 AND
                                                                                (d16_concept_id IS NULL OR
                                                                                 d16_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d14_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d15_concept_id END,
                                                                      CASE WHEN d16_concept_id > 0 AND
                                                                                (d17_concept_id IS NULL OR
                                                                                 d17_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d15_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d16_concept_id END,
                                                                      CASE WHEN d17_concept_id > 0 AND
                                                                                (d18_concept_id IS NULL OR
                                                                                 d18_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d16_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d17_concept_id END,
                                                                      CASE WHEN d18_concept_id > 0 AND
                                                                                (d19_concept_id IS NULL OR
                                                                                 d19_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d17_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d18_concept_id END,
                                                                      CASE WHEN d19_concept_id > 0 AND
                                                                                (d20_concept_id IS NULL OR
                                                                                 d20_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d18_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d19_concept_id END,
                                                                      CASE WHEN d20_concept_id > 0 AND
                                                                                num_persons < @smallcellcount
                                                                        THEN -1
                                                                      WHEN d19_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d20_concept_id END,
                                                                      CASE WHEN d1_concept_id > 0 AND
                                                                                (d2_concept_id IS NULL OR
                                                                                 d2_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      ELSE d1_concept_name END,
                                                                      CASE WHEN d2_concept_id > 0 AND
                                                                                (d3_concept_id IS NULL OR
                                                                                 d3_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d1_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d2_concept_name END,
                                                                      CASE WHEN d3_concept_id > 0 AND
                                                                                (d4_concept_id IS NULL OR
                                                                                 d4_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d2_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d3_concept_name END,
                                                                      CASE WHEN d4_concept_id > 0 AND
                                                                                (d5_concept_id IS NULL OR
                                                                                 d5_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d3_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d4_concept_name END,
                                                                      CASE WHEN d5_concept_id > 0 AND
                                                                                (d6_concept_id IS NULL OR
                                                                                 d6_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d4_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d5_concept_name END,
                                                                      CASE WHEN d6_concept_id > 0 AND
                                                                                (d7_concept_id IS NULL OR
                                                                                 d7_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d5_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d6_concept_name END,
                                                                      CASE WHEN d7_concept_id > 0 AND
                                                                                (d8_concept_id IS NULL OR
                                                                                 d8_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d6_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d7_concept_name END,
                                                                      CASE WHEN d8_concept_id > 0 AND
                                                                                (d9_concept_id IS NULL OR
                                                                                 d9_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d7_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d8_concept_name END,
                                                                      CASE WHEN d9_concept_id > 0 AND
                                                                                (d10_concept_id IS NULL OR
                                                                                 d10_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d8_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d9_concept_name END,
                                                                      CASE WHEN d10_concept_id > 0 AND
                                                                                (d11_concept_id IS NULL OR
                                                                                 d11_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d9_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d10_concept_name END,
                                                                      CASE WHEN d11_concept_id > 0 AND
                                                                                (d12_concept_id IS NULL OR
                                                                                 d12_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d10_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d11_concept_name END,
                                                                      CASE WHEN d12_concept_id > 0 AND
                                                                                (d13_concept_id IS NULL OR
                                                                                 d13_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d11_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d12_concept_name END,
                                                                      CASE WHEN d13_concept_id > 0 AND
                                                                                (d14_concept_id IS NULL OR
                                                                                 d14_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d12_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d13_concept_name END,
                                                                      CASE WHEN d14_concept_id > 0 AND
                                                                                (d15_concept_id IS NULL OR
                                                                                 d15_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d13_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d14_concept_name END,
                                                                      CASE WHEN d15_concept_id > 0 AND
                                                                                (d16_concept_id IS NULL OR
                                                                                 d16_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d14_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d15_concept_name END,
                                                                      CASE WHEN d16_concept_id > 0 AND
                                                                                (d17_concept_id IS NULL OR
                                                                                 d17_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d15_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d16_concept_name END,
                                                                      CASE WHEN d17_concept_id > 0 AND
                                                                                (d18_concept_id IS NULL OR
                                                                                 d18_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d16_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d17_concept_name END,
                                                                      CASE WHEN d18_concept_id > 0 AND
                                                                                (d19_concept_id IS NULL OR
                                                                                 d19_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d17_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d18_concept_name END,
                                                                      CASE WHEN d19_concept_id > 0 AND
                                                                                (d20_concept_id IS NULL OR
                                                                                 d20_concept_id = -1) AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d18_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d19_concept_name END,
                                                                      CASE WHEN d20_concept_id > 0 AND
                                                                                num_persons < @smallcellcount
                                                                        THEN 'Other'
                                                                      WHEN d19_concept_id = -1
                                                                        THEN NULL
                                                                      ELSE d20_concept_name END
                                                                  ) t3
                                                                GROUP BY index_year,
                                                                  CASE WHEN d1_concept_id > 0 AND
                                                                            (d2_concept_id IS NULL OR
                                                                             d2_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  ELSE d1_concept_id END,
                                                                  CASE WHEN d2_concept_id > 0 AND
                                                                            (d3_concept_id IS NULL OR
                                                                             d3_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d1_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d2_concept_id END,
                                                                  CASE WHEN d3_concept_id > 0 AND
                                                                            (d4_concept_id IS NULL OR
                                                                             d4_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d2_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d3_concept_id END,
                                                                  CASE WHEN d4_concept_id > 0 AND
                                                                            (d5_concept_id IS NULL OR
                                                                             d5_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d3_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d4_concept_id END,
                                                                  CASE WHEN d5_concept_id > 0 AND
                                                                            (d6_concept_id IS NULL OR
                                                                             d6_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d4_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d5_concept_id END,
                                                                  CASE WHEN d6_concept_id > 0 AND
                                                                            (d7_concept_id IS NULL OR
                                                                             d7_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d5_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d6_concept_id END,
                                                                  CASE WHEN d7_concept_id > 0 AND
                                                                            (d8_concept_id IS NULL OR
                                                                             d8_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d6_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d7_concept_id END,
                                                                  CASE WHEN d8_concept_id > 0 AND
                                                                            (d9_concept_id IS NULL OR
                                                                             d9_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d7_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d8_concept_id END,
                                                                  CASE WHEN d9_concept_id > 0 AND
                                                                            (d10_concept_id IS NULL OR
                                                                             d10_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d8_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d9_concept_id END,
                                                                  CASE WHEN d10_concept_id > 0 AND
                                                                            (d11_concept_id IS NULL OR
                                                                             d11_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d9_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d10_concept_id END,
                                                                  CASE WHEN d11_concept_id > 0 AND
                                                                            (d12_concept_id IS NULL OR
                                                                             d12_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d10_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d11_concept_id END,
                                                                  CASE WHEN d12_concept_id > 0 AND
                                                                            (d13_concept_id IS NULL OR
                                                                             d13_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d11_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d12_concept_id END,
                                                                  CASE WHEN d13_concept_id > 0 AND
                                                                            (d14_concept_id IS NULL OR
                                                                             d14_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d12_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d13_concept_id END,
                                                                  CASE WHEN d14_concept_id > 0 AND
                                                                            (d15_concept_id IS NULL OR
                                                                             d15_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d13_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d14_concept_id END,
                                                                  CASE WHEN d15_concept_id > 0 AND
                                                                            (d16_concept_id IS NULL OR
                                                                             d16_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d14_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d15_concept_id END,
                                                                  CASE WHEN d16_concept_id > 0 AND
                                                                            (d17_concept_id IS NULL OR
                                                                             d17_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d15_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d16_concept_id END,
                                                                  CASE WHEN d17_concept_id > 0 AND
                                                                            (d18_concept_id IS NULL OR
                                                                             d18_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d16_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d17_concept_id END,
                                                                  CASE WHEN d18_concept_id > 0 AND
                                                                            (d19_concept_id IS NULL OR
                                                                             d19_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d17_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d18_concept_id END,
                                                                  CASE WHEN d19_concept_id > 0 AND
                                                                            (d20_concept_id IS NULL OR
                                                                             d20_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d18_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d19_concept_id END,
                                                                  CASE WHEN d20_concept_id > 0 AND
                                                                            num_persons < @smallcellcount
                                                                    THEN -1
                                                                  WHEN d19_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d20_concept_id END,
                                                                  CASE WHEN d1_concept_id > 0 AND
                                                                            (d2_concept_id IS NULL OR
                                                                             d2_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  ELSE d1_concept_name END,
                                                                  CASE WHEN d2_concept_id > 0 AND
                                                                            (d3_concept_id IS NULL OR
                                                                             d3_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d1_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d2_concept_name END,
                                                                  CASE WHEN d3_concept_id > 0 AND
                                                                            (d4_concept_id IS NULL OR
                                                                             d4_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d2_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d3_concept_name END,
                                                                  CASE WHEN d4_concept_id > 0 AND
                                                                            (d5_concept_id IS NULL OR
                                                                             d5_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d3_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d4_concept_name END,
                                                                  CASE WHEN d5_concept_id > 0 AND
                                                                            (d6_concept_id IS NULL OR
                                                                             d6_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d4_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d5_concept_name END,
                                                                  CASE WHEN d6_concept_id > 0 AND
                                                                            (d7_concept_id IS NULL OR
                                                                             d7_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d5_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d6_concept_name END,
                                                                  CASE WHEN d7_concept_id > 0 AND
                                                                            (d8_concept_id IS NULL OR
                                                                             d8_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d6_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d7_concept_name END,
                                                                  CASE WHEN d8_concept_id > 0 AND
                                                                            (d9_concept_id IS NULL OR
                                                                             d9_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d7_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d8_concept_name END,
                                                                  CASE WHEN d9_concept_id > 0 AND
                                                                            (d10_concept_id IS NULL OR
                                                                             d10_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d8_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d9_concept_name END,
                                                                  CASE WHEN d10_concept_id > 0 AND
                                                                            (d11_concept_id IS NULL OR
                                                                             d11_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d9_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d10_concept_name END,
                                                                  CASE WHEN d11_concept_id > 0 AND
                                                                            (d12_concept_id IS NULL OR
                                                                             d12_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d10_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d11_concept_name END,
                                                                  CASE WHEN d12_concept_id > 0 AND
                                                                            (d13_concept_id IS NULL OR
                                                                             d13_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d11_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d12_concept_name END,
                                                                  CASE WHEN d13_concept_id > 0 AND
                                                                            (d14_concept_id IS NULL OR
                                                                             d14_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d12_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d13_concept_name END,
                                                                  CASE WHEN d14_concept_id > 0 AND
                                                                            (d15_concept_id IS NULL OR
                                                                             d15_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d13_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d14_concept_name END,
                                                                  CASE WHEN d15_concept_id > 0 AND
                                                                            (d16_concept_id IS NULL OR
                                                                             d16_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d14_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d15_concept_name END,
                                                                  CASE WHEN d16_concept_id > 0 AND
                                                                            (d17_concept_id IS NULL OR
                                                                             d17_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d15_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d16_concept_name END,
                                                                  CASE WHEN d17_concept_id > 0 AND
                                                                            (d18_concept_id IS NULL OR
                                                                             d18_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d16_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d17_concept_name END,
                                                                  CASE WHEN d18_concept_id > 0 AND
                                                                            (d19_concept_id IS NULL OR
                                                                             d19_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d17_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d18_concept_name END,
                                                                  CASE WHEN d19_concept_id > 0 AND
                                                                            (d20_concept_id IS NULL OR
                                                                             d20_concept_id = -1) AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d18_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d19_concept_name END,
                                                                  CASE WHEN d20_concept_id > 0 AND
                                                                            num_persons < @smallcellcount
                                                                    THEN 'Other'
                                                                  WHEN d19_concept_id = -1
                                                                    THEN NULL
                                                                  ELSE d20_concept_name END
                                                              ) t4
                                                            GROUP BY index_year,
                                                              CASE WHEN d1_concept_id > 0 AND
                                                                        (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              ELSE d1_concept_id END,
                                                              CASE WHEN d2_concept_id > 0 AND
                                                                        (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d1_concept_id = -1
                                                                THEN NULL
                                                              ELSE d2_concept_id END,
                                                              CASE WHEN d3_concept_id > 0 AND
                                                                        (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d2_concept_id = -1
                                                                THEN NULL
                                                              ELSE d3_concept_id END,
                                                              CASE WHEN d4_concept_id > 0 AND
                                                                        (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d3_concept_id = -1
                                                                THEN NULL
                                                              ELSE d4_concept_id END,
                                                              CASE WHEN d5_concept_id > 0 AND
                                                                        (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d4_concept_id = -1
                                                                THEN NULL
                                                              ELSE d5_concept_id END,
                                                              CASE WHEN d6_concept_id > 0 AND
                                                                        (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d5_concept_id = -1
                                                                THEN NULL
                                                              ELSE d6_concept_id END,
                                                              CASE WHEN d7_concept_id > 0 AND
                                                                        (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d6_concept_id = -1
                                                                THEN NULL
                                                              ELSE d7_concept_id END,
                                                              CASE WHEN d8_concept_id > 0 AND
                                                                        (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d7_concept_id = -1
                                                                THEN NULL
                                                              ELSE d8_concept_id END,
                                                              CASE WHEN d9_concept_id > 0 AND
                                                                        (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d8_concept_id = -1
                                                                THEN NULL
                                                              ELSE d9_concept_id END,
                                                              CASE WHEN d10_concept_id > 0 AND
                                                                        (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d9_concept_id = -1
                                                                THEN NULL
                                                              ELSE d10_concept_id END,
                                                              CASE WHEN d11_concept_id > 0 AND
                                                                        (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d10_concept_id = -1
                                                                THEN NULL
                                                              ELSE d11_concept_id END,
                                                              CASE WHEN d12_concept_id > 0 AND
                                                                        (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d11_concept_id = -1
                                                                THEN NULL
                                                              ELSE d12_concept_id END,
                                                              CASE WHEN d13_concept_id > 0 AND
                                                                        (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d12_concept_id = -1
                                                                THEN NULL
                                                              ELSE d13_concept_id END,
                                                              CASE WHEN d14_concept_id > 0 AND
                                                                        (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d13_concept_id = -1
                                                                THEN NULL
                                                              ELSE d14_concept_id END,
                                                              CASE WHEN d15_concept_id > 0 AND
                                                                        (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d14_concept_id = -1
                                                                THEN NULL
                                                              ELSE d15_concept_id END,
                                                              CASE WHEN d16_concept_id > 0 AND
                                                                        (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d15_concept_id = -1
                                                                THEN NULL
                                                              ELSE d16_concept_id END,
                                                              CASE WHEN d17_concept_id > 0 AND
                                                                        (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d16_concept_id = -1
                                                                THEN NULL
                                                              ELSE d17_concept_id END,
                                                              CASE WHEN d18_concept_id > 0 AND
                                                                        (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d17_concept_id = -1
                                                                THEN NULL
                                                              ELSE d18_concept_id END,
                                                              CASE WHEN d19_concept_id > 0 AND
                                                                        (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d18_concept_id = -1
                                                                THEN NULL
                                                              ELSE d19_concept_id END,
                                                              CASE WHEN d20_concept_id > 0 AND
                                                                        num_persons < @smallcellcount
                                                                THEN -1
                                                              WHEN d19_concept_id = -1
                                                                THEN NULL
                                                              ELSE d20_concept_id END,
                                                              CASE WHEN d1_concept_id > 0 AND
                                                                        (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              ELSE d1_concept_name END,
                                                              CASE WHEN d2_concept_id > 0 AND
                                                                        (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d1_concept_id = -1
                                                                THEN NULL
                                                              ELSE d2_concept_name END,
                                                              CASE WHEN d3_concept_id > 0 AND
                                                                        (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d2_concept_id = -1
                                                                THEN NULL
                                                              ELSE d3_concept_name END,
                                                              CASE WHEN d4_concept_id > 0 AND
                                                                        (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d3_concept_id = -1
                                                                THEN NULL
                                                              ELSE d4_concept_name END,
                                                              CASE WHEN d5_concept_id > 0 AND
                                                                        (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d4_concept_id = -1
                                                                THEN NULL
                                                              ELSE d5_concept_name END,
                                                              CASE WHEN d6_concept_id > 0 AND
                                                                        (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d5_concept_id = -1
                                                                THEN NULL
                                                              ELSE d6_concept_name END,
                                                              CASE WHEN d7_concept_id > 0 AND
                                                                        (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d6_concept_id = -1
                                                                THEN NULL
                                                              ELSE d7_concept_name END,
                                                              CASE WHEN d8_concept_id > 0 AND
                                                                        (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d7_concept_id = -1
                                                                THEN NULL
                                                              ELSE d8_concept_name END,
                                                              CASE WHEN d9_concept_id > 0 AND
                                                                        (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d8_concept_id = -1
                                                                THEN NULL
                                                              ELSE d9_concept_name END,
                                                              CASE WHEN d10_concept_id > 0 AND
                                                                        (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d9_concept_id = -1
                                                                THEN NULL
                                                              ELSE d10_concept_name END,
                                                              CASE WHEN d11_concept_id > 0 AND
                                                                        (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d10_concept_id = -1
                                                                THEN NULL
                                                              ELSE d11_concept_name END,
                                                              CASE WHEN d12_concept_id > 0 AND
                                                                        (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d11_concept_id = -1
                                                                THEN NULL
                                                              ELSE d12_concept_name END,
                                                              CASE WHEN d13_concept_id > 0 AND
                                                                        (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d12_concept_id = -1
                                                                THEN NULL
                                                              ELSE d13_concept_name END,
                                                              CASE WHEN d14_concept_id > 0 AND
                                                                        (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d13_concept_id = -1
                                                                THEN NULL
                                                              ELSE d14_concept_name END,
                                                              CASE WHEN d15_concept_id > 0 AND
                                                                        (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d14_concept_id = -1
                                                                THEN NULL
                                                              ELSE d15_concept_name END,
                                                              CASE WHEN d16_concept_id > 0 AND
                                                                        (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d15_concept_id = -1
                                                                THEN NULL
                                                              ELSE d16_concept_name END,
                                                              CASE WHEN d17_concept_id > 0 AND
                                                                        (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d16_concept_id = -1
                                                                THEN NULL
                                                              ELSE d17_concept_name END,
                                                              CASE WHEN d18_concept_id > 0 AND
                                                                        (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d17_concept_id = -1
                                                                THEN NULL
                                                              ELSE d18_concept_name END,
                                                              CASE WHEN d19_concept_id > 0 AND
                                                                        (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                                        AND num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d18_concept_id = -1
                                                                THEN NULL
                                                              ELSE d19_concept_name END,
                                                              CASE WHEN d20_concept_id > 0 AND
                                                                        num_persons < @smallcellcount
                                                                THEN 'Other'
                                                              WHEN d19_concept_id = -1
                                                                THEN NULL
                                                              ELSE d20_concept_name END
                                                          ) t5
                                                        GROUP BY index_year,
                                                          CASE WHEN d1_concept_id > 0 AND
                                                                    (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          ELSE d1_concept_id END,
                                                          CASE WHEN d2_concept_id > 0 AND
                                                                    (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d1_concept_id = -1
                                                            THEN NULL
                                                          ELSE d2_concept_id END,
                                                          CASE WHEN d3_concept_id > 0 AND
                                                                    (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d2_concept_id = -1
                                                            THEN NULL
                                                          ELSE d3_concept_id END,
                                                          CASE WHEN d4_concept_id > 0 AND
                                                                    (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d3_concept_id = -1
                                                            THEN NULL
                                                          ELSE d4_concept_id END,
                                                          CASE WHEN d5_concept_id > 0 AND
                                                                    (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d4_concept_id = -1
                                                            THEN NULL
                                                          ELSE d5_concept_id END,
                                                          CASE WHEN d6_concept_id > 0 AND
                                                                    (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d5_concept_id = -1
                                                            THEN NULL
                                                          ELSE d6_concept_id END,
                                                          CASE WHEN d7_concept_id > 0 AND
                                                                    (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d6_concept_id = -1
                                                            THEN NULL
                                                          ELSE d7_concept_id END,
                                                          CASE WHEN d8_concept_id > 0 AND
                                                                    (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d7_concept_id = -1
                                                            THEN NULL
                                                          ELSE d8_concept_id END,
                                                          CASE WHEN d9_concept_id > 0 AND
                                                                    (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d8_concept_id = -1
                                                            THEN NULL
                                                          ELSE d9_concept_id END,
                                                          CASE WHEN d10_concept_id > 0 AND
                                                                    (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d9_concept_id = -1
                                                            THEN NULL
                                                          ELSE d10_concept_id END,
                                                          CASE WHEN d11_concept_id > 0 AND
                                                                    (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d10_concept_id = -1
                                                            THEN NULL
                                                          ELSE d11_concept_id END,
                                                          CASE WHEN d12_concept_id > 0 AND
                                                                    (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d11_concept_id = -1
                                                            THEN NULL
                                                          ELSE d12_concept_id END,
                                                          CASE WHEN d13_concept_id > 0 AND
                                                                    (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d12_concept_id = -1
                                                            THEN NULL
                                                          ELSE d13_concept_id END,
                                                          CASE WHEN d14_concept_id > 0 AND
                                                                    (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d13_concept_id = -1
                                                            THEN NULL
                                                          ELSE d14_concept_id END,
                                                          CASE WHEN d15_concept_id > 0 AND
                                                                    (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d14_concept_id = -1
                                                            THEN NULL
                                                          ELSE d15_concept_id END,
                                                          CASE WHEN d16_concept_id > 0 AND
                                                                    (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d15_concept_id = -1
                                                            THEN NULL
                                                          ELSE d16_concept_id END,
                                                          CASE WHEN d17_concept_id > 0 AND
                                                                    (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d16_concept_id = -1
                                                            THEN NULL
                                                          ELSE d17_concept_id END,
                                                          CASE WHEN d18_concept_id > 0 AND
                                                                    (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d17_concept_id = -1
                                                            THEN NULL
                                                          ELSE d18_concept_id END,
                                                          CASE WHEN d19_concept_id > 0 AND
                                                                    (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d18_concept_id = -1
                                                            THEN NULL
                                                          ELSE d19_concept_id END,
                                                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                            THEN -1
                                                          WHEN d19_concept_id = -1
                                                            THEN NULL
                                                          ELSE d20_concept_id END,
                                                          CASE WHEN d1_concept_id > 0 AND
                                                                    (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          ELSE d1_concept_name END,
                                                          CASE WHEN d2_concept_id > 0 AND
                                                                    (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d1_concept_id = -1
                                                            THEN NULL
                                                          ELSE d2_concept_name END,
                                                          CASE WHEN d3_concept_id > 0 AND
                                                                    (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d2_concept_id = -1
                                                            THEN NULL
                                                          ELSE d3_concept_name END,
                                                          CASE WHEN d4_concept_id > 0 AND
                                                                    (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d3_concept_id = -1
                                                            THEN NULL
                                                          ELSE d4_concept_name END,
                                                          CASE WHEN d5_concept_id > 0 AND
                                                                    (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d4_concept_id = -1
                                                            THEN NULL
                                                          ELSE d5_concept_name END,
                                                          CASE WHEN d6_concept_id > 0 AND
                                                                    (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d5_concept_id = -1
                                                            THEN NULL
                                                          ELSE d6_concept_name END,
                                                          CASE WHEN d7_concept_id > 0 AND
                                                                    (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d6_concept_id = -1
                                                            THEN NULL
                                                          ELSE d7_concept_name END,
                                                          CASE WHEN d8_concept_id > 0 AND
                                                                    (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d7_concept_id = -1
                                                            THEN NULL
                                                          ELSE d8_concept_name END,
                                                          CASE WHEN d9_concept_id > 0 AND
                                                                    (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d8_concept_id = -1
                                                            THEN NULL
                                                          ELSE d9_concept_name END,
                                                          CASE WHEN d10_concept_id > 0 AND
                                                                    (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d9_concept_id = -1
                                                            THEN NULL
                                                          ELSE d10_concept_name END,
                                                          CASE WHEN d11_concept_id > 0 AND
                                                                    (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d10_concept_id = -1
                                                            THEN NULL
                                                          ELSE d11_concept_name END,
                                                          CASE WHEN d12_concept_id > 0 AND
                                                                    (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d11_concept_id = -1
                                                            THEN NULL
                                                          ELSE d12_concept_name END,
                                                          CASE WHEN d13_concept_id > 0 AND
                                                                    (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d12_concept_id = -1
                                                            THEN NULL
                                                          ELSE d13_concept_name END,
                                                          CASE WHEN d14_concept_id > 0 AND
                                                                    (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d13_concept_id = -1
                                                            THEN NULL
                                                          ELSE d14_concept_name END,
                                                          CASE WHEN d15_concept_id > 0 AND
                                                                    (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d14_concept_id = -1
                                                            THEN NULL
                                                          ELSE d15_concept_name END,
                                                          CASE WHEN d16_concept_id > 0 AND
                                                                    (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d15_concept_id = -1
                                                            THEN NULL
                                                          ELSE d16_concept_name END,
                                                          CASE WHEN d17_concept_id > 0 AND
                                                                    (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d16_concept_id = -1
                                                            THEN NULL
                                                          ELSE d17_concept_name END,
                                                          CASE WHEN d18_concept_id > 0 AND
                                                                    (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d17_concept_id = -1
                                                            THEN NULL
                                                          ELSE d18_concept_name END,
                                                          CASE WHEN d19_concept_id > 0 AND
                                                                    (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                                    num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d18_concept_id = -1
                                                            THEN NULL
                                                          ELSE d19_concept_name END,
                                                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                            THEN 'Other'
                                                          WHEN d19_concept_id = -1
                                                            THEN NULL
                                                          ELSE d20_concept_name END
                                                      ) t6
                                                    GROUP BY index_year,
                                                      CASE WHEN d1_concept_id > 0 AND
                                                                (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      ELSE d1_concept_id END,
                                                      CASE WHEN d2_concept_id > 0 AND
                                                                (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d1_concept_id = -1
                                                        THEN NULL
                                                      ELSE d2_concept_id END,
                                                      CASE WHEN d3_concept_id > 0 AND
                                                                (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d2_concept_id = -1
                                                        THEN NULL
                                                      ELSE d3_concept_id END,
                                                      CASE WHEN d4_concept_id > 0 AND
                                                                (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d3_concept_id = -1
                                                        THEN NULL
                                                      ELSE d4_concept_id END,
                                                      CASE WHEN d5_concept_id > 0 AND
                                                                (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d4_concept_id = -1
                                                        THEN NULL
                                                      ELSE d5_concept_id END,
                                                      CASE WHEN d6_concept_id > 0 AND
                                                                (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d5_concept_id = -1
                                                        THEN NULL
                                                      ELSE d6_concept_id END,
                                                      CASE WHEN d7_concept_id > 0 AND
                                                                (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d6_concept_id = -1
                                                        THEN NULL
                                                      ELSE d7_concept_id END,
                                                      CASE WHEN d8_concept_id > 0 AND
                                                                (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d7_concept_id = -1
                                                        THEN NULL
                                                      ELSE d8_concept_id END,
                                                      CASE WHEN d9_concept_id > 0 AND
                                                                (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d8_concept_id = -1
                                                        THEN NULL
                                                      ELSE d9_concept_id END,
                                                      CASE WHEN d10_concept_id > 0 AND
                                                                (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d9_concept_id = -1
                                                        THEN NULL
                                                      ELSE d10_concept_id END,
                                                      CASE WHEN d11_concept_id > 0 AND
                                                                (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d10_concept_id = -1
                                                        THEN NULL
                                                      ELSE d11_concept_id END,
                                                      CASE WHEN d12_concept_id > 0 AND
                                                                (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d11_concept_id = -1
                                                        THEN NULL
                                                      ELSE d12_concept_id END,
                                                      CASE WHEN d13_concept_id > 0 AND
                                                                (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d12_concept_id = -1
                                                        THEN NULL
                                                      ELSE d13_concept_id END,
                                                      CASE WHEN d14_concept_id > 0 AND
                                                                (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d13_concept_id = -1
                                                        THEN NULL
                                                      ELSE d14_concept_id END,
                                                      CASE WHEN d15_concept_id > 0 AND
                                                                (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d14_concept_id = -1
                                                        THEN NULL
                                                      ELSE d15_concept_id END,
                                                      CASE WHEN d16_concept_id > 0 AND
                                                                (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d15_concept_id = -1
                                                        THEN NULL
                                                      ELSE d16_concept_id END,
                                                      CASE WHEN d17_concept_id > 0 AND
                                                                (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d16_concept_id = -1
                                                        THEN NULL
                                                      ELSE d17_concept_id END,
                                                      CASE WHEN d18_concept_id > 0 AND
                                                                (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d17_concept_id = -1
                                                        THEN NULL
                                                      ELSE d18_concept_id END,
                                                      CASE WHEN d19_concept_id > 0 AND
                                                                (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d18_concept_id = -1
                                                        THEN NULL
                                                      ELSE d19_concept_id END,
                                                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                        THEN -1
                                                      WHEN d19_concept_id = -1
                                                        THEN NULL
                                                      ELSE d20_concept_id END,
                                                      CASE WHEN d1_concept_id > 0 AND
                                                                (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      ELSE d1_concept_name END,
                                                      CASE WHEN d2_concept_id > 0 AND
                                                                (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d1_concept_id = -1
                                                        THEN NULL
                                                      ELSE d2_concept_name END,
                                                      CASE WHEN d3_concept_id > 0 AND
                                                                (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d2_concept_id = -1
                                                        THEN NULL
                                                      ELSE d3_concept_name END,
                                                      CASE WHEN d4_concept_id > 0 AND
                                                                (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d3_concept_id = -1
                                                        THEN NULL
                                                      ELSE d4_concept_name END,
                                                      CASE WHEN d5_concept_id > 0 AND
                                                                (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d4_concept_id = -1
                                                        THEN NULL
                                                      ELSE d5_concept_name END,
                                                      CASE WHEN d6_concept_id > 0 AND
                                                                (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d5_concept_id = -1
                                                        THEN NULL
                                                      ELSE d6_concept_name END,
                                                      CASE WHEN d7_concept_id > 0 AND
                                                                (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d6_concept_id = -1
                                                        THEN NULL
                                                      ELSE d7_concept_name END,
                                                      CASE WHEN d8_concept_id > 0 AND
                                                                (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d7_concept_id = -1
                                                        THEN NULL
                                                      ELSE d8_concept_name END,
                                                      CASE WHEN d9_concept_id > 0 AND
                                                                (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d8_concept_id = -1
                                                        THEN NULL
                                                      ELSE d9_concept_name END,
                                                      CASE WHEN d10_concept_id > 0 AND
                                                                (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d9_concept_id = -1
                                                        THEN NULL
                                                      ELSE d10_concept_name END,
                                                      CASE WHEN d11_concept_id > 0 AND
                                                                (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d10_concept_id = -1
                                                        THEN NULL
                                                      ELSE d11_concept_name END,
                                                      CASE WHEN d12_concept_id > 0 AND
                                                                (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d11_concept_id = -1
                                                        THEN NULL
                                                      ELSE d12_concept_name END,
                                                      CASE WHEN d13_concept_id > 0 AND
                                                                (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d12_concept_id = -1
                                                        THEN NULL
                                                      ELSE d13_concept_name END,
                                                      CASE WHEN d14_concept_id > 0 AND
                                                                (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d13_concept_id = -1
                                                        THEN NULL
                                                      ELSE d14_concept_name END,
                                                      CASE WHEN d15_concept_id > 0 AND
                                                                (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d14_concept_id = -1
                                                        THEN NULL
                                                      ELSE d15_concept_name END,
                                                      CASE WHEN d16_concept_id > 0 AND
                                                                (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d15_concept_id = -1
                                                        THEN NULL
                                                      ELSE d16_concept_name END,
                                                      CASE WHEN d17_concept_id > 0 AND
                                                                (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d16_concept_id = -1
                                                        THEN NULL
                                                      ELSE d17_concept_name END,
                                                      CASE WHEN d18_concept_id > 0 AND
                                                                (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d17_concept_id = -1
                                                        THEN NULL
                                                      ELSE d18_concept_name END,
                                                      CASE WHEN d19_concept_id > 0 AND
                                                                (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                                num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d18_concept_id = -1
                                                        THEN NULL
                                                      ELSE d19_concept_name END,
                                                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                        THEN 'Other'
                                                      WHEN d19_concept_id = -1
                                                        THEN NULL
                                                      ELSE d20_concept_name END
                                                  ) t7
                                                GROUP BY index_year,
                                                  CASE WHEN
                                                    d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  ELSE d1_concept_id END,
                                                  CASE WHEN
                                                    d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d1_concept_id = -1
                                                    THEN NULL
                                                  ELSE d2_concept_id END,
                                                  CASE WHEN
                                                    d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d2_concept_id = -1
                                                    THEN NULL
                                                  ELSE d3_concept_id END,
                                                  CASE WHEN
                                                    d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d3_concept_id = -1
                                                    THEN NULL
                                                  ELSE d4_concept_id END,
                                                  CASE WHEN
                                                    d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d4_concept_id = -1
                                                    THEN NULL
                                                  ELSE d5_concept_id END,
                                                  CASE WHEN
                                                    d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d5_concept_id = -1
                                                    THEN NULL
                                                  ELSE d6_concept_id END,
                                                  CASE WHEN
                                                    d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d6_concept_id = -1
                                                    THEN NULL
                                                  ELSE d7_concept_id END,
                                                  CASE WHEN
                                                    d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d7_concept_id = -1
                                                    THEN NULL
                                                  ELSE d8_concept_id END,
                                                  CASE WHEN d9_concept_id > 0 AND
                                                            (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d8_concept_id = -1
                                                    THEN NULL
                                                  ELSE d9_concept_id END,
                                                  CASE WHEN d10_concept_id > 0 AND
                                                            (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d9_concept_id = -1
                                                    THEN NULL
                                                  ELSE d10_concept_id END,
                                                  CASE WHEN d11_concept_id > 0 AND
                                                            (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d10_concept_id = -1
                                                    THEN NULL
                                                  ELSE d11_concept_id END,
                                                  CASE WHEN d12_concept_id > 0 AND
                                                            (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d11_concept_id = -1
                                                    THEN NULL
                                                  ELSE d12_concept_id END,
                                                  CASE WHEN d13_concept_id > 0 AND
                                                            (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d12_concept_id = -1
                                                    THEN NULL
                                                  ELSE d13_concept_id END,
                                                  CASE WHEN d14_concept_id > 0 AND
                                                            (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d13_concept_id = -1
                                                    THEN NULL
                                                  ELSE d14_concept_id END,
                                                  CASE WHEN d15_concept_id > 0 AND
                                                            (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d14_concept_id = -1
                                                    THEN NULL
                                                  ELSE d15_concept_id END,
                                                  CASE WHEN d16_concept_id > 0 AND
                                                            (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d15_concept_id = -1
                                                    THEN NULL
                                                  ELSE d16_concept_id END,
                                                  CASE WHEN d17_concept_id > 0 AND
                                                            (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d16_concept_id = -1
                                                    THEN NULL
                                                  ELSE d17_concept_id END,
                                                  CASE WHEN d18_concept_id > 0 AND
                                                            (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d17_concept_id = -1
                                                    THEN NULL
                                                  ELSE d18_concept_id END,
                                                  CASE WHEN d19_concept_id > 0 AND
                                                            (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d18_concept_id = -1
                                                    THEN NULL
                                                  ELSE d19_concept_id END,
                                                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                    THEN -1
                                                  WHEN d19_concept_id = -1
                                                    THEN NULL
                                                  ELSE d20_concept_id END,
                                                  CASE WHEN
                                                    d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  ELSE d1_concept_name END,
                                                  CASE WHEN
                                                    d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d1_concept_id = -1
                                                    THEN NULL
                                                  ELSE d2_concept_name END,
                                                  CASE WHEN
                                                    d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d2_concept_id = -1
                                                    THEN NULL
                                                  ELSE d3_concept_name END,
                                                  CASE WHEN
                                                    d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d3_concept_id = -1
                                                    THEN NULL
                                                  ELSE d4_concept_name END,
                                                  CASE WHEN
                                                    d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d4_concept_id = -1
                                                    THEN NULL
                                                  ELSE d5_concept_name END,
                                                  CASE WHEN
                                                    d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d5_concept_id = -1
                                                    THEN NULL
                                                  ELSE d6_concept_name END,
                                                  CASE WHEN
                                                    d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d6_concept_id = -1
                                                    THEN NULL
                                                  ELSE d7_concept_name END,
                                                  CASE WHEN
                                                    d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d7_concept_id = -1
                                                    THEN NULL
                                                  ELSE d8_concept_name END,
                                                  CASE WHEN d9_concept_id > 0 AND
                                                            (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d8_concept_id = -1
                                                    THEN NULL
                                                  ELSE d9_concept_name END,
                                                  CASE WHEN d10_concept_id > 0 AND
                                                            (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d9_concept_id = -1
                                                    THEN NULL
                                                  ELSE d10_concept_name END,
                                                  CASE WHEN d11_concept_id > 0 AND
                                                            (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d10_concept_id = -1
                                                    THEN NULL
                                                  ELSE d11_concept_name END,
                                                  CASE WHEN d12_concept_id > 0 AND
                                                            (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d11_concept_id = -1
                                                    THEN NULL
                                                  ELSE d12_concept_name END,
                                                  CASE WHEN d13_concept_id > 0 AND
                                                            (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d12_concept_id = -1
                                                    THEN NULL
                                                  ELSE d13_concept_name END,
                                                  CASE WHEN d14_concept_id > 0 AND
                                                            (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d13_concept_id = -1
                                                    THEN NULL
                                                  ELSE d14_concept_name END,
                                                  CASE WHEN d15_concept_id > 0 AND
                                                            (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d14_concept_id = -1
                                                    THEN NULL
                                                  ELSE d15_concept_name END,
                                                  CASE WHEN d16_concept_id > 0 AND
                                                            (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d15_concept_id = -1
                                                    THEN NULL
                                                  ELSE d16_concept_name END,
                                                  CASE WHEN d17_concept_id > 0 AND
                                                            (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d16_concept_id = -1
                                                    THEN NULL
                                                  ELSE d17_concept_name END,
                                                  CASE WHEN d18_concept_id > 0 AND
                                                            (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d17_concept_id = -1
                                                    THEN NULL
                                                  ELSE d18_concept_name END,
                                                  CASE WHEN d19_concept_id > 0 AND
                                                            (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                                            num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d18_concept_id = -1
                                                    THEN NULL
                                                  ELSE d19_concept_name END,
                                                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                    THEN 'Other'
                                                  WHEN d19_concept_id = -1
                                                    THEN NULL
                                                  ELSE d20_concept_name END
                                              ) t8
                                            GROUP BY index_year,
                                              CASE WHEN
                                                d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              ELSE d1_concept_id END,
                                              CASE WHEN
                                                d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d1_concept_id = -1
                                                THEN NULL
                                              ELSE d2_concept_id END,
                                              CASE WHEN
                                                d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d2_concept_id = -1
                                                THEN NULL
                                              ELSE d3_concept_id END,
                                              CASE WHEN
                                                d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d3_concept_id = -1
                                                THEN NULL
                                              ELSE d4_concept_id END,
                                              CASE WHEN
                                                d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d4_concept_id = -1
                                                THEN NULL
                                              ELSE d5_concept_id END,
                                              CASE WHEN
                                                d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d5_concept_id = -1
                                                THEN NULL
                                              ELSE d6_concept_id END,
                                              CASE WHEN
                                                d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d6_concept_id = -1
                                                THEN NULL
                                              ELSE d7_concept_id END,
                                              CASE WHEN
                                                d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d7_concept_id = -1
                                                THEN NULL
                                              ELSE d8_concept_id END,
                                              CASE WHEN
                                                d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d8_concept_id = -1
                                                THEN NULL
                                              ELSE d9_concept_id END,
                                              CASE WHEN
                                                d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d9_concept_id = -1
                                                THEN NULL
                                              ELSE d10_concept_id END,
                                              CASE WHEN
                                                d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d10_concept_id = -1
                                                THEN NULL
                                              ELSE d11_concept_id END,
                                              CASE WHEN
                                                d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d11_concept_id = -1
                                                THEN NULL
                                              ELSE d12_concept_id END,
                                              CASE WHEN
                                                d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d12_concept_id = -1
                                                THEN NULL
                                              ELSE d13_concept_id END,
                                              CASE WHEN
                                                d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d13_concept_id = -1
                                                THEN NULL
                                              ELSE d14_concept_id END,
                                              CASE WHEN
                                                d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d14_concept_id = -1
                                                THEN NULL
                                              ELSE d15_concept_id END,
                                              CASE WHEN
                                                d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d15_concept_id = -1
                                                THEN NULL
                                              ELSE d16_concept_id END,
                                              CASE WHEN
                                                d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d16_concept_id = -1
                                                THEN NULL
                                              ELSE d17_concept_id END,
                                              CASE WHEN
                                                d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d17_concept_id = -1
                                                THEN NULL
                                              ELSE d18_concept_id END,
                                              CASE WHEN
                                                d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d18_concept_id = -1
                                                THEN NULL
                                              ELSE d19_concept_id END,
                                              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                THEN -1
                                              WHEN d19_concept_id = -1
                                                THEN NULL
                                              ELSE d20_concept_id END,
                                              CASE WHEN
                                                d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              ELSE d1_concept_name END,
                                              CASE WHEN
                                                d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d1_concept_id = -1
                                                THEN NULL
                                              ELSE d2_concept_name END,
                                              CASE WHEN
                                                d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d2_concept_id = -1
                                                THEN NULL
                                              ELSE d3_concept_name END,
                                              CASE WHEN
                                                d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d3_concept_id = -1
                                                THEN NULL
                                              ELSE d4_concept_name END,
                                              CASE WHEN
                                                d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d4_concept_id = -1
                                                THEN NULL
                                              ELSE d5_concept_name END,
                                              CASE WHEN
                                                d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d5_concept_id = -1
                                                THEN NULL
                                              ELSE d6_concept_name END,
                                              CASE WHEN
                                                d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d6_concept_id = -1
                                                THEN NULL
                                              ELSE d7_concept_name END,
                                              CASE WHEN
                                                d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d7_concept_id = -1
                                                THEN NULL
                                              ELSE d8_concept_name END,
                                              CASE WHEN
                                                d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d8_concept_id = -1
                                                THEN NULL
                                              ELSE d9_concept_name END,
                                              CASE WHEN
                                                d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d9_concept_id = -1
                                                THEN NULL
                                              ELSE d10_concept_name END,
                                              CASE WHEN
                                                d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d10_concept_id = -1
                                                THEN NULL
                                              ELSE d11_concept_name END,
                                              CASE WHEN
                                                d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d11_concept_id = -1
                                                THEN NULL
                                              ELSE d12_concept_name END,
                                              CASE WHEN
                                                d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d12_concept_id = -1
                                                THEN NULL
                                              ELSE d13_concept_name END,
                                              CASE WHEN
                                                d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d13_concept_id = -1
                                                THEN NULL
                                              ELSE d14_concept_name END,
                                              CASE WHEN
                                                d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d14_concept_id = -1
                                                THEN NULL
                                              ELSE d15_concept_name END,
                                              CASE WHEN
                                                d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d15_concept_id = -1
                                                THEN NULL
                                              ELSE d16_concept_name END,
                                              CASE WHEN
                                                d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d16_concept_id = -1
                                                THEN NULL
                                              ELSE d17_concept_name END,
                                              CASE WHEN
                                                d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d17_concept_id = -1
                                                THEN NULL
                                              ELSE d18_concept_name END,
                                              CASE WHEN
                                                d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d18_concept_id = -1
                                                THEN NULL
                                              ELSE d19_concept_name END,
                                              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                                THEN 'Other'
                                              WHEN d19_concept_id = -1
                                                THEN NULL
                                              ELSE d20_concept_name END
                                          ) t9
                                        GROUP BY index_year,
                                          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          ELSE d1_concept_id END,
                                          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d1_concept_id = -1
                                            THEN NULL
                                          ELSE d2_concept_id END,
                                          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d2_concept_id = -1
                                            THEN NULL
                                          ELSE d3_concept_id END,
                                          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d3_concept_id = -1
                                            THEN NULL
                                          ELSE d4_concept_id END,
                                          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d4_concept_id = -1
                                            THEN NULL
                                          ELSE d5_concept_id END,
                                          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d5_concept_id = -1
                                            THEN NULL
                                          ELSE d6_concept_id END,
                                          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d6_concept_id = -1
                                            THEN NULL
                                          ELSE d7_concept_id END,
                                          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d7_concept_id = -1
                                            THEN NULL
                                          ELSE d8_concept_id END,
                                          CASE WHEN
                                            d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d8_concept_id = -1
                                            THEN NULL
                                          ELSE d9_concept_id END,
                                          CASE WHEN
                                            d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d9_concept_id = -1
                                            THEN NULL
                                          ELSE d10_concept_id END,
                                          CASE WHEN
                                            d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d10_concept_id = -1
                                            THEN NULL
                                          ELSE d11_concept_id END,
                                          CASE WHEN
                                            d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d11_concept_id = -1
                                            THEN NULL
                                          ELSE d12_concept_id END,
                                          CASE WHEN
                                            d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d12_concept_id = -1
                                            THEN NULL
                                          ELSE d13_concept_id END,
                                          CASE WHEN
                                            d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d13_concept_id = -1
                                            THEN NULL
                                          ELSE d14_concept_id END,
                                          CASE WHEN
                                            d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d14_concept_id = -1
                                            THEN NULL
                                          ELSE d15_concept_id END,
                                          CASE WHEN
                                            d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d15_concept_id = -1
                                            THEN NULL
                                          ELSE d16_concept_id END,
                                          CASE WHEN
                                            d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d16_concept_id = -1
                                            THEN NULL
                                          ELSE d17_concept_id END,
                                          CASE WHEN
                                            d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d17_concept_id = -1
                                            THEN NULL
                                          ELSE d18_concept_id END,
                                          CASE WHEN
                                            d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d18_concept_id = -1
                                            THEN NULL
                                          ELSE d19_concept_id END,
                                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                            THEN -1
                                          WHEN d19_concept_id = -1
                                            THEN NULL
                                          ELSE d20_concept_id END,
                                          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          ELSE d1_concept_name END,
                                          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d1_concept_id = -1
                                            THEN NULL
                                          ELSE d2_concept_name END,
                                          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d2_concept_id = -1
                                            THEN NULL
                                          ELSE d3_concept_name END,
                                          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d3_concept_id = -1
                                            THEN NULL
                                          ELSE d4_concept_name END,
                                          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d4_concept_id = -1
                                            THEN NULL
                                          ELSE d5_concept_name END,
                                          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d5_concept_id = -1
                                            THEN NULL
                                          ELSE d6_concept_name END,
                                          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d6_concept_id = -1
                                            THEN NULL
                                          ELSE d7_concept_name END,
                                          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1)
                                                    AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d7_concept_id = -1
                                            THEN NULL
                                          ELSE d8_concept_name END,
                                          CASE WHEN
                                            d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d8_concept_id = -1
                                            THEN NULL
                                          ELSE d9_concept_name END,
                                          CASE WHEN
                                            d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d9_concept_id = -1
                                            THEN NULL
                                          ELSE d10_concept_name END,
                                          CASE WHEN
                                            d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d10_concept_id = -1
                                            THEN NULL
                                          ELSE d11_concept_name END,
                                          CASE WHEN
                                            d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d11_concept_id = -1
                                            THEN NULL
                                          ELSE d12_concept_name END,
                                          CASE WHEN
                                            d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d12_concept_id = -1
                                            THEN NULL
                                          ELSE d13_concept_name END,
                                          CASE WHEN
                                            d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d13_concept_id = -1
                                            THEN NULL
                                          ELSE d14_concept_name END,
                                          CASE WHEN
                                            d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d14_concept_id = -1
                                            THEN NULL
                                          ELSE d15_concept_name END,
                                          CASE WHEN
                                            d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d15_concept_id = -1
                                            THEN NULL
                                          ELSE d16_concept_name END,
                                          CASE WHEN
                                            d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d16_concept_id = -1
                                            THEN NULL
                                          ELSE d17_concept_name END,
                                          CASE WHEN
                                            d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d17_concept_id = -1
                                            THEN NULL
                                          ELSE d18_concept_name END,
                                          CASE WHEN
                                            d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d18_concept_id = -1
                                            THEN NULL
                                          ELSE d19_concept_name END,
                                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                            THEN 'Other'
                                          WHEN d19_concept_id = -1
                                            THEN NULL
                                          ELSE d20_concept_name END
                                      ) t10
                                    GROUP BY index_year,
                                      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      ELSE d1_concept_id END,
                                      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d1_concept_id = -1
                                        THEN NULL
                                      ELSE d2_concept_id END,
                                      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d2_concept_id = -1
                                        THEN NULL
                                      ELSE d3_concept_id END,
                                      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d3_concept_id = -1
                                        THEN NULL
                                      ELSE d4_concept_id END,
                                      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d4_concept_id = -1
                                        THEN NULL
                                      ELSE d5_concept_id END,
                                      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d5_concept_id = -1
                                        THEN NULL
                                      ELSE d6_concept_id END,
                                      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d6_concept_id = -1
                                        THEN NULL
                                      ELSE d7_concept_id END,
                                      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d7_concept_id = -1
                                        THEN NULL
                                      ELSE d8_concept_id END,
                                      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d8_concept_id = -1
                                        THEN NULL
                                      ELSE d9_concept_id END,
                                      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d9_concept_id = -1
                                        THEN NULL
                                      ELSE d10_concept_id END,
                                      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d10_concept_id = -1
                                        THEN NULL
                                      ELSE d11_concept_id END,
                                      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d11_concept_id = -1
                                        THEN NULL
                                      ELSE d12_concept_id END,
                                      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d12_concept_id = -1
                                        THEN NULL
                                      ELSE d13_concept_id END,
                                      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d13_concept_id = -1
                                        THEN NULL
                                      ELSE d14_concept_id END,
                                      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d14_concept_id = -1
                                        THEN NULL
                                      ELSE d15_concept_id END,
                                      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d15_concept_id = -1
                                        THEN NULL
                                      ELSE d16_concept_id END,
                                      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d16_concept_id = -1
                                        THEN NULL
                                      ELSE d17_concept_id END,
                                      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d17_concept_id = -1
                                        THEN NULL
                                      ELSE d18_concept_id END,
                                      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d18_concept_id = -1
                                        THEN NULL
                                      ELSE d19_concept_id END,
                                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                        THEN -1
                                      WHEN d19_concept_id = -1
                                        THEN NULL
                                      ELSE d20_concept_id END,
                                      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      ELSE d1_concept_name END,
                                      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d1_concept_id = -1
                                        THEN NULL
                                      ELSE d2_concept_name END,
                                      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d2_concept_id = -1
                                        THEN NULL
                                      ELSE d3_concept_name END,
                                      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d3_concept_id = -1
                                        THEN NULL
                                      ELSE d4_concept_name END,
                                      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d4_concept_id = -1
                                        THEN NULL
                                      ELSE d5_concept_name END,
                                      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d5_concept_id = -1
                                        THEN NULL
                                      ELSE d6_concept_name END,
                                      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d6_concept_id = -1
                                        THEN NULL
                                      ELSE d7_concept_name END,
                                      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                                num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d7_concept_id = -1
                                        THEN NULL
                                      ELSE d8_concept_name END,
                                      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d8_concept_id = -1
                                        THEN NULL
                                      ELSE d9_concept_name END,
                                      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d9_concept_id = -1
                                        THEN NULL
                                      ELSE d10_concept_name END,
                                      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d10_concept_id = -1
                                        THEN NULL
                                      ELSE d11_concept_name END,
                                      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d11_concept_id = -1
                                        THEN NULL
                                      ELSE d12_concept_name END,
                                      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d12_concept_id = -1
                                        THEN NULL
                                      ELSE d13_concept_name END,
                                      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d13_concept_id = -1
                                        THEN NULL
                                      ELSE d14_concept_name END,
                                      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d14_concept_id = -1
                                        THEN NULL
                                      ELSE d15_concept_name END,
                                      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d15_concept_id = -1
                                        THEN NULL
                                      ELSE d16_concept_name END,
                                      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d16_concept_id = -1
                                        THEN NULL
                                      ELSE d17_concept_name END,
                                      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d17_concept_id = -1
                                        THEN NULL
                                      ELSE d18_concept_name END,
                                      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1)
                                                AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d18_concept_id = -1
                                        THEN NULL
                                      ELSE d19_concept_name END,
                                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                        THEN 'Other'
                                      WHEN d19_concept_id = -1
                                        THEN NULL
                                      ELSE d20_concept_name END
                                  ) t11
                                GROUP BY index_year,
                                  CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  ELSE d1_concept_id END,
                                  CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d1_concept_id = -1
                                    THEN NULL
                                  ELSE d2_concept_id END,
                                  CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d2_concept_id = -1
                                    THEN NULL
                                  ELSE d3_concept_id END,
                                  CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d3_concept_id = -1
                                    THEN NULL
                                  ELSE d4_concept_id END,
                                  CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d4_concept_id = -1
                                    THEN NULL
                                  ELSE d5_concept_id END,
                                  CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d5_concept_id = -1
                                    THEN NULL
                                  ELSE d6_concept_id END,
                                  CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d6_concept_id = -1
                                    THEN NULL
                                  ELSE d7_concept_id END,
                                  CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d7_concept_id = -1
                                    THEN NULL
                                  ELSE d8_concept_id END,
                                  CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d8_concept_id = -1
                                    THEN NULL
                                  ELSE d9_concept_id END,
                                  CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d9_concept_id = -1
                                    THEN NULL
                                  ELSE d10_concept_id END,
                                  CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d10_concept_id = -1
                                    THEN NULL
                                  ELSE d11_concept_id END,
                                  CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d11_concept_id = -1
                                    THEN NULL
                                  ELSE d12_concept_id END,
                                  CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d12_concept_id = -1
                                    THEN NULL
                                  ELSE d13_concept_id END,
                                  CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d13_concept_id = -1
                                    THEN NULL
                                  ELSE d14_concept_id END,
                                  CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d14_concept_id = -1
                                    THEN NULL
                                  ELSE d15_concept_id END,
                                  CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d15_concept_id = -1
                                    THEN NULL
                                  ELSE d16_concept_id END,
                                  CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d16_concept_id = -1
                                    THEN NULL
                                  ELSE d17_concept_id END,
                                  CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d17_concept_id = -1
                                    THEN NULL
                                  ELSE d18_concept_id END,
                                  CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d18_concept_id = -1
                                    THEN NULL
                                  ELSE d19_concept_id END,
                                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                    THEN -1
                                  WHEN d19_concept_id = -1
                                    THEN NULL
                                  ELSE d20_concept_id END,
                                  CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  ELSE d1_concept_name END,
                                  CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d1_concept_id = -1
                                    THEN NULL
                                  ELSE d2_concept_name END,
                                  CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d2_concept_id = -1
                                    THEN NULL
                                  ELSE d3_concept_name END,
                                  CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d3_concept_id = -1
                                    THEN NULL
                                  ELSE d4_concept_name END,
                                  CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d4_concept_id = -1
                                    THEN NULL
                                  ELSE d5_concept_name END,
                                  CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d5_concept_id = -1
                                    THEN NULL
                                  ELSE d6_concept_name END,
                                  CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d6_concept_id = -1
                                    THEN NULL
                                  ELSE d7_concept_name END,
                                  CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d7_concept_id = -1
                                    THEN NULL
                                  ELSE d8_concept_name END,
                                  CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d8_concept_id = -1
                                    THEN NULL
                                  ELSE d9_concept_name END,
                                  CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d9_concept_id = -1
                                    THEN NULL
                                  ELSE d10_concept_name END,
                                  CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d10_concept_id = -1
                                    THEN NULL
                                  ELSE d11_concept_name END,
                                  CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d11_concept_id = -1
                                    THEN NULL
                                  ELSE d12_concept_name END,
                                  CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d12_concept_id = -1
                                    THEN NULL
                                  ELSE d13_concept_name END,
                                  CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d13_concept_id = -1
                                    THEN NULL
                                  ELSE d14_concept_name END,
                                  CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d14_concept_id = -1
                                    THEN NULL
                                  ELSE d15_concept_name END,
                                  CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d15_concept_id = -1
                                    THEN NULL
                                  ELSE d16_concept_name END,
                                  CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d16_concept_id = -1
                                    THEN NULL
                                  ELSE d17_concept_name END,
                                  CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d17_concept_id = -1
                                    THEN NULL
                                  ELSE d18_concept_name END,
                                  CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                            num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d18_concept_id = -1
                                    THEN NULL
                                  ELSE d19_concept_name END,
                                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                    THEN 'Other'
                                  WHEN d19_concept_id = -1
                                    THEN NULL
                                  ELSE d20_concept_name END
                              ) t12
                            GROUP BY index_year,
                              CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              ELSE d1_concept_id END,
                              CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d1_concept_id = -1
                                THEN NULL
                              ELSE d2_concept_id END,
                              CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d2_concept_id = -1
                                THEN NULL
                              ELSE d3_concept_id END,
                              CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d3_concept_id = -1
                                THEN NULL
                              ELSE d4_concept_id END,
                              CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d4_concept_id = -1
                                THEN NULL
                              ELSE d5_concept_id END,
                              CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d5_concept_id = -1
                                THEN NULL
                              ELSE d6_concept_id END,
                              CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d6_concept_id = -1
                                THEN NULL
                              ELSE d7_concept_id END,
                              CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d7_concept_id = -1
                                THEN NULL
                              ELSE d8_concept_id END,
                              CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d8_concept_id = -1
                                THEN NULL
                              ELSE d9_concept_id END,
                              CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d9_concept_id = -1
                                THEN NULL
                              ELSE d10_concept_id END,
                              CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d10_concept_id = -1
                                THEN NULL
                              ELSE d11_concept_id END,
                              CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d11_concept_id = -1
                                THEN NULL
                              ELSE d12_concept_id END,
                              CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d12_concept_id = -1
                                THEN NULL
                              ELSE d13_concept_id END,
                              CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d13_concept_id = -1
                                THEN NULL
                              ELSE d14_concept_id END,
                              CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d14_concept_id = -1
                                THEN NULL
                              ELSE d15_concept_id END,
                              CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d15_concept_id = -1
                                THEN NULL
                              ELSE d16_concept_id END,
                              CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d16_concept_id = -1
                                THEN NULL
                              ELSE d17_concept_id END,
                              CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d17_concept_id = -1
                                THEN NULL
                              ELSE d18_concept_id END,
                              CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN -1
                              WHEN d18_concept_id = -1
                                THEN NULL
                              ELSE d19_concept_id END,
                              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                THEN -1
                              WHEN d19_concept_id = -1
                                THEN NULL
                              ELSE d20_concept_id END,
                              CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              ELSE d1_concept_name END,
                              CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d1_concept_id = -1
                                THEN NULL
                              ELSE d2_concept_name END,
                              CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d2_concept_id = -1
                                THEN NULL
                              ELSE d3_concept_name END,
                              CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d3_concept_id = -1
                                THEN NULL
                              ELSE d4_concept_name END,
                              CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d4_concept_id = -1
                                THEN NULL
                              ELSE d5_concept_name END,
                              CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d5_concept_id = -1
                                THEN NULL
                              ELSE d6_concept_name END,
                              CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d6_concept_id = -1
                                THEN NULL
                              ELSE d7_concept_name END,
                              CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d7_concept_id = -1
                                THEN NULL
                              ELSE d8_concept_name END,
                              CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d8_concept_id = -1
                                THEN NULL
                              ELSE d9_concept_name END,
                              CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d9_concept_id = -1
                                THEN NULL
                              ELSE d10_concept_name END,
                              CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d10_concept_id = -1
                                THEN NULL
                              ELSE d11_concept_name END,
                              CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d11_concept_id = -1
                                THEN NULL
                              ELSE d12_concept_name END,
                              CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d12_concept_id = -1
                                THEN NULL
                              ELSE d13_concept_name END,
                              CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d13_concept_id = -1
                                THEN NULL
                              ELSE d14_concept_name END,
                              CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d14_concept_id = -1
                                THEN NULL
                              ELSE d15_concept_name END,
                              CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d15_concept_id = -1
                                THEN NULL
                              ELSE d16_concept_name END,
                              CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d16_concept_id = -1
                                THEN NULL
                              ELSE d17_concept_name END,
                              CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d17_concept_id = -1
                                THEN NULL
                              ELSE d18_concept_name END,
                              CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                        num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d18_concept_id = -1
                                THEN NULL
                              ELSE d19_concept_name END,
                              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                                THEN 'Other'
                              WHEN d19_concept_id = -1
                                THEN NULL
                              ELSE d20_concept_name END
                          ) t13
                        GROUP BY index_year,
                          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          ELSE d1_concept_id END,
                          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d1_concept_id = -1
                            THEN NULL
                          ELSE d2_concept_id END,
                          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d2_concept_id = -1
                            THEN NULL
                          ELSE d3_concept_id END,
                          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d3_concept_id = -1
                            THEN NULL
                          ELSE d4_concept_id END,
                          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d4_concept_id = -1
                            THEN NULL
                          ELSE d5_concept_id END,
                          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d5_concept_id = -1
                            THEN NULL
                          ELSE d6_concept_id END,
                          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d6_concept_id = -1
                            THEN NULL
                          ELSE d7_concept_id END,
                          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d7_concept_id = -1
                            THEN NULL
                          ELSE d8_concept_id END,
                          CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d8_concept_id = -1
                            THEN NULL
                          ELSE d9_concept_id END,
                          CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d9_concept_id = -1
                            THEN NULL
                          ELSE d10_concept_id END,
                          CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d10_concept_id = -1
                            THEN NULL
                          ELSE d11_concept_id END,
                          CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d11_concept_id = -1
                            THEN NULL
                          ELSE d12_concept_id END,
                          CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d12_concept_id = -1
                            THEN NULL
                          ELSE d13_concept_id END,
                          CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d13_concept_id = -1
                            THEN NULL
                          ELSE d14_concept_id END,
                          CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d14_concept_id = -1
                            THEN NULL
                          ELSE d15_concept_id END,
                          CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d15_concept_id = -1
                            THEN NULL
                          ELSE d16_concept_id END,
                          CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d16_concept_id = -1
                            THEN NULL
                          ELSE d17_concept_id END,
                          CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d17_concept_id = -1
                            THEN NULL
                          ELSE d18_concept_id END,
                          CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN -1
                          WHEN d18_concept_id = -1
                            THEN NULL
                          ELSE d19_concept_id END,
                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                            THEN -1
                          WHEN d19_concept_id = -1
                            THEN NULL
                          ELSE d20_concept_id END,
                          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          ELSE d1_concept_name END,
                          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d1_concept_id = -1
                            THEN NULL
                          ELSE d2_concept_name END,
                          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d2_concept_id = -1
                            THEN NULL
                          ELSE d3_concept_name END,
                          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d3_concept_id = -1
                            THEN NULL
                          ELSE d4_concept_name END,
                          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d4_concept_id = -1
                            THEN NULL
                          ELSE d5_concept_name END,
                          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d5_concept_id = -1
                            THEN NULL
                          ELSE d6_concept_name END,
                          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d6_concept_id = -1
                            THEN NULL
                          ELSE d7_concept_name END,
                          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d7_concept_id = -1
                            THEN NULL
                          ELSE d8_concept_name END,
                          CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d8_concept_id = -1
                            THEN NULL
                          ELSE d9_concept_name END,
                          CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d9_concept_id = -1
                            THEN NULL
                          ELSE d10_concept_name END,
                          CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d10_concept_id = -1
                            THEN NULL
                          ELSE d11_concept_name END,
                          CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d11_concept_id = -1
                            THEN NULL
                          ELSE d12_concept_name END,
                          CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d12_concept_id = -1
                            THEN NULL
                          ELSE d13_concept_name END,
                          CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d13_concept_id = -1
                            THEN NULL
                          ELSE d14_concept_name END,
                          CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d14_concept_id = -1
                            THEN NULL
                          ELSE d15_concept_name END,
                          CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d15_concept_id = -1
                            THEN NULL
                          ELSE d16_concept_name END,
                          CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d16_concept_id = -1
                            THEN NULL
                          ELSE d17_concept_name END,
                          CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d17_concept_id = -1
                            THEN NULL
                          ELSE d18_concept_name END,
                          CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                    num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d18_concept_id = -1
                            THEN NULL
                          ELSE d19_concept_name END,
                          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                            THEN 'Other'
                          WHEN d19_concept_id = -1
                            THEN NULL
                          ELSE d20_concept_name END
                      ) t14
                    GROUP BY index_year,
                      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      ELSE d1_concept_id END,
                      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d1_concept_id = -1
                        THEN NULL
                      ELSE d2_concept_id END,
                      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d2_concept_id = -1
                        THEN NULL
                      ELSE d3_concept_id END,
                      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d3_concept_id = -1
                        THEN NULL
                      ELSE d4_concept_id END,
                      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d4_concept_id = -1
                        THEN NULL
                      ELSE d5_concept_id END,
                      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d5_concept_id = -1
                        THEN NULL
                      ELSE d6_concept_id END,
                      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d6_concept_id = -1
                        THEN NULL
                      ELSE d7_concept_id END,
                      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d7_concept_id = -1
                        THEN NULL
                      ELSE d8_concept_id END,
                      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d8_concept_id = -1
                        THEN NULL
                      ELSE d9_concept_id END,
                      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d9_concept_id = -1
                        THEN NULL
                      ELSE d10_concept_id END,
                      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d10_concept_id = -1
                        THEN NULL
                      ELSE d11_concept_id END,
                      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d11_concept_id = -1
                        THEN NULL
                      ELSE d12_concept_id END,
                      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d12_concept_id = -1
                        THEN NULL
                      ELSE d13_concept_id END,
                      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d13_concept_id = -1
                        THEN NULL
                      ELSE d14_concept_id END,
                      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d14_concept_id = -1
                        THEN NULL
                      ELSE d15_concept_id END,
                      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d15_concept_id = -1
                        THEN NULL
                      ELSE d16_concept_id END,
                      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d16_concept_id = -1
                        THEN NULL
                      ELSE d17_concept_id END,
                      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d17_concept_id = -1
                        THEN NULL
                      ELSE d18_concept_id END,
                      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN -1
                      WHEN d18_concept_id = -1
                        THEN NULL
                      ELSE d19_concept_id END,
                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                        THEN -1
                      WHEN d19_concept_id = -1
                        THEN NULL
                      ELSE d20_concept_id END,
                      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      ELSE d1_concept_name END,
                      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d1_concept_id = -1
                        THEN NULL
                      ELSE d2_concept_name END,
                      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d2_concept_id = -1
                        THEN NULL
                      ELSE d3_concept_name END,
                      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d3_concept_id = -1
                        THEN NULL
                      ELSE d4_concept_name END,
                      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d4_concept_id = -1
                        THEN NULL
                      ELSE d5_concept_name END,
                      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d5_concept_id = -1
                        THEN NULL
                      ELSE d6_concept_name END,
                      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d6_concept_id = -1
                        THEN NULL
                      ELSE d7_concept_name END,
                      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d7_concept_id = -1
                        THEN NULL
                      ELSE d8_concept_name END,
                      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d8_concept_id = -1
                        THEN NULL
                      ELSE d9_concept_name END,
                      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d9_concept_id = -1
                        THEN NULL
                      ELSE d10_concept_name END,
                      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d10_concept_id = -1
                        THEN NULL
                      ELSE d11_concept_name END,
                      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d11_concept_id = -1
                        THEN NULL
                      ELSE d12_concept_name END,
                      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d12_concept_id = -1
                        THEN NULL
                      ELSE d13_concept_name END,
                      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d13_concept_id = -1
                        THEN NULL
                      ELSE d14_concept_name END,
                      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d14_concept_id = -1
                        THEN NULL
                      ELSE d15_concept_name END,
                      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d15_concept_id = -1
                        THEN NULL
                      ELSE d16_concept_name END,
                      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d16_concept_id = -1
                        THEN NULL
                      ELSE d17_concept_name END,
                      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d17_concept_id = -1
                        THEN NULL
                      ELSE d18_concept_name END,
                      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                                num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d18_concept_id = -1
                        THEN NULL
                      ELSE d19_concept_name END,
                      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                        THEN 'Other'
                      WHEN d19_concept_id = -1
                        THEN NULL
                      ELSE d20_concept_name END
                  ) t15
                GROUP BY index_year,
                  CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  ELSE d1_concept_id END,
                  CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d1_concept_id = -1
                    THEN NULL
                  ELSE d2_concept_id END,
                  CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d2_concept_id = -1
                    THEN NULL
                  ELSE d3_concept_id END,
                  CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d3_concept_id = -1
                    THEN NULL
                  ELSE d4_concept_id END,
                  CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d4_concept_id = -1
                    THEN NULL
                  ELSE d5_concept_id END,
                  CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d5_concept_id = -1
                    THEN NULL
                  ELSE d6_concept_id END,
                  CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d6_concept_id = -1
                    THEN NULL
                  ELSE d7_concept_id END,
                  CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d7_concept_id = -1
                    THEN NULL
                  ELSE d8_concept_id END,
                  CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d8_concept_id = -1
                    THEN NULL
                  ELSE d9_concept_id END,
                  CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d9_concept_id = -1
                    THEN NULL
                  ELSE d10_concept_id END,
                  CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d10_concept_id = -1
                    THEN NULL
                  ELSE d11_concept_id END,
                  CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d11_concept_id = -1
                    THEN NULL
                  ELSE d12_concept_id END,
                  CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d12_concept_id = -1
                    THEN NULL
                  ELSE d13_concept_id END,
                  CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d13_concept_id = -1
                    THEN NULL
                  ELSE d14_concept_id END,
                  CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d14_concept_id = -1
                    THEN NULL
                  ELSE d15_concept_id END,
                  CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d15_concept_id = -1
                    THEN NULL
                  ELSE d16_concept_id END,
                  CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d16_concept_id = -1
                    THEN NULL
                  ELSE d17_concept_id END,
                  CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d17_concept_id = -1
                    THEN NULL
                  ELSE d18_concept_id END,
                  CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN -1
                  WHEN d18_concept_id = -1
                    THEN NULL
                  ELSE d19_concept_id END,
                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                    THEN -1
                  WHEN d19_concept_id = -1
                    THEN NULL
                  ELSE d20_concept_id END,
                  CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  ELSE d1_concept_name END,
                  CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d1_concept_id = -1
                    THEN NULL
                  ELSE d2_concept_name END,
                  CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d2_concept_id = -1
                    THEN NULL
                  ELSE d3_concept_name END,
                  CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d3_concept_id = -1
                    THEN NULL
                  ELSE d4_concept_name END,
                  CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d4_concept_id = -1
                    THEN NULL
                  ELSE d5_concept_name END,
                  CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d5_concept_id = -1
                    THEN NULL
                  ELSE d6_concept_name END,
                  CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d6_concept_id = -1
                    THEN NULL
                  ELSE d7_concept_name END,
                  CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d7_concept_id = -1
                    THEN NULL
                  ELSE d8_concept_name END,
                  CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d8_concept_id = -1
                    THEN NULL
                  ELSE d9_concept_name END,
                  CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d9_concept_id = -1
                    THEN NULL
                  ELSE d10_concept_name END,
                  CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d10_concept_id = -1
                    THEN NULL
                  ELSE d11_concept_name END,
                  CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d11_concept_id = -1
                    THEN NULL
                  ELSE d12_concept_name END,
                  CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d12_concept_id = -1
                    THEN NULL
                  ELSE d13_concept_name END,
                  CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d13_concept_id = -1
                    THEN NULL
                  ELSE d14_concept_name END,
                  CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d14_concept_id = -1
                    THEN NULL
                  ELSE d15_concept_name END,
                  CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d15_concept_id = -1
                    THEN NULL
                  ELSE d16_concept_name END,
                  CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d16_concept_id = -1
                    THEN NULL
                  ELSE d17_concept_name END,
                  CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d17_concept_id = -1
                    THEN NULL
                  ELSE d18_concept_name END,
                  CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                            num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d18_concept_id = -1
                    THEN NULL
                  ELSE d19_concept_name END,
                  CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                    THEN 'Other'
                  WHEN d19_concept_id = -1
                    THEN NULL
                  ELSE d20_concept_name END
              ) t16
            GROUP BY index_year,
              CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              ELSE d1_concept_id END,
              CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d1_concept_id = -1
                THEN NULL
              ELSE d2_concept_id END,
              CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d2_concept_id = -1
                THEN NULL
              ELSE d3_concept_id END,
              CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d3_concept_id = -1
                THEN NULL
              ELSE d4_concept_id END,
              CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d4_concept_id = -1
                THEN NULL
              ELSE d5_concept_id END,
              CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d5_concept_id = -1
                THEN NULL
              ELSE d6_concept_id END,
              CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d6_concept_id = -1
                THEN NULL
              ELSE d7_concept_id END,
              CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d7_concept_id = -1
                THEN NULL
              ELSE d8_concept_id END,
              CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d8_concept_id = -1
                THEN NULL
              ELSE d9_concept_id END,
              CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d9_concept_id = -1
                THEN NULL
              ELSE d10_concept_id END,
              CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d10_concept_id = -1
                THEN NULL
              ELSE d11_concept_id END,
              CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d11_concept_id = -1
                THEN NULL
              ELSE d12_concept_id END,
              CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d12_concept_id = -1
                THEN NULL
              ELSE d13_concept_id END,
              CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d13_concept_id = -1
                THEN NULL
              ELSE d14_concept_id END,
              CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d14_concept_id = -1
                THEN NULL
              ELSE d15_concept_id END,
              CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d15_concept_id = -1
                THEN NULL
              ELSE d16_concept_id END,
              CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d16_concept_id = -1
                THEN NULL
              ELSE d17_concept_id END,
              CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d17_concept_id = -1
                THEN NULL
              ELSE d18_concept_id END,
              CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN -1
              WHEN d18_concept_id = -1
                THEN NULL
              ELSE d19_concept_id END,
              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                THEN -1
              WHEN d19_concept_id = -1
                THEN NULL
              ELSE d20_concept_id END,
              CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              ELSE d1_concept_name END,
              CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d1_concept_id = -1
                THEN NULL
              ELSE d2_concept_name END,
              CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d2_concept_id = -1
                THEN NULL
              ELSE d3_concept_name END,
              CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d3_concept_id = -1
                THEN NULL
              ELSE d4_concept_name END,
              CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d4_concept_id = -1
                THEN NULL
              ELSE d5_concept_name END,
              CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d5_concept_id = -1
                THEN NULL
              ELSE d6_concept_name END,
              CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d6_concept_id = -1
                THEN NULL
              ELSE d7_concept_name END,
              CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d7_concept_id = -1
                THEN NULL
              ELSE d8_concept_name END,
              CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d8_concept_id = -1
                THEN NULL
              ELSE d9_concept_name END,
              CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d9_concept_id = -1
                THEN NULL
              ELSE d10_concept_name END,
              CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d10_concept_id = -1
                THEN NULL
              ELSE d11_concept_name END,
              CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d11_concept_id = -1
                THEN NULL
              ELSE d12_concept_name END,
              CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d12_concept_id = -1
                THEN NULL
              ELSE d13_concept_name END,
              CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d13_concept_id = -1
                THEN NULL
              ELSE d14_concept_name END,
              CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d14_concept_id = -1
                THEN NULL
              ELSE d15_concept_name END,
              CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d15_concept_id = -1
                THEN NULL
              ELSE d16_concept_name END,
              CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d16_concept_id = -1
                THEN NULL
              ELSE d17_concept_name END,
              CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d17_concept_id = -1
                THEN NULL
              ELSE d18_concept_name END,
              CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                        num_persons < @smallcellcount
                THEN 'Other'
              WHEN d18_concept_id = -1
                THEN NULL
              ELSE d19_concept_name END,
              CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
                THEN 'Other'
              WHEN d19_concept_id = -1
                THEN NULL
              ELSE d20_concept_name END
          ) t17
        GROUP BY index_year,
          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          ELSE d1_concept_id END,
          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d1_concept_id = -1
            THEN NULL
          ELSE d2_concept_id END,
          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d2_concept_id = -1
            THEN NULL
          ELSE d3_concept_id END,
          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d3_concept_id = -1
            THEN NULL
          ELSE d4_concept_id END,
          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d4_concept_id = -1
            THEN NULL
          ELSE d5_concept_id END,
          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d5_concept_id = -1
            THEN NULL
          ELSE d6_concept_id END,
          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d6_concept_id = -1
            THEN NULL
          ELSE d7_concept_id END,
          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d7_concept_id = -1
            THEN NULL
          ELSE d8_concept_id END,
          CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d8_concept_id = -1
            THEN NULL
          ELSE d9_concept_id END,
          CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d9_concept_id = -1
            THEN NULL
          ELSE d10_concept_id END,
          CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d10_concept_id = -1
            THEN NULL
          ELSE d11_concept_id END,
          CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d11_concept_id = -1
            THEN NULL
          ELSE d12_concept_id END,
          CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d12_concept_id = -1
            THEN NULL
          ELSE d13_concept_id END,
          CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d13_concept_id = -1
            THEN NULL
          ELSE d14_concept_id END,
          CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d14_concept_id = -1
            THEN NULL
          ELSE d15_concept_id END,
          CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d15_concept_id = -1
            THEN NULL
          ELSE d16_concept_id END,
          CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d16_concept_id = -1
            THEN NULL
          ELSE d17_concept_id END,
          CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d17_concept_id = -1
            THEN NULL
          ELSE d18_concept_id END,
          CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN -1
          WHEN d18_concept_id = -1
            THEN NULL
          ELSE d19_concept_id END,
          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
            THEN -1
          WHEN d19_concept_id = -1
            THEN NULL
          ELSE d20_concept_id END,
          CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          ELSE d1_concept_name END,
          CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d1_concept_id = -1
            THEN NULL
          ELSE d2_concept_name END,
          CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d2_concept_id = -1
            THEN NULL
          ELSE d3_concept_name END,
          CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d3_concept_id = -1
            THEN NULL
          ELSE d4_concept_name END,
          CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d4_concept_id = -1
            THEN NULL
          ELSE d5_concept_name END,
          CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d5_concept_id = -1
            THEN NULL
          ELSE d6_concept_name END,
          CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d6_concept_id = -1
            THEN NULL
          ELSE d7_concept_name END,
          CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d7_concept_id = -1
            THEN NULL
          ELSE d8_concept_name END,
          CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d8_concept_id = -1
            THEN NULL
          ELSE d9_concept_name END,
          CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d9_concept_id = -1
            THEN NULL
          ELSE d10_concept_name END,
          CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d10_concept_id = -1
            THEN NULL
          ELSE d11_concept_name END,
          CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d11_concept_id = -1
            THEN NULL
          ELSE d12_concept_name END,
          CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d12_concept_id = -1
            THEN NULL
          ELSE d13_concept_name END,
          CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d13_concept_id = -1
            THEN NULL
          ELSE d14_concept_name END,
          CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d14_concept_id = -1
            THEN NULL
          ELSE d15_concept_name END,
          CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d15_concept_id = -1
            THEN NULL
          ELSE d16_concept_name END,
          CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d16_concept_id = -1
            THEN NULL
          ELSE d17_concept_name END,
          CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d17_concept_id = -1
            THEN NULL
          ELSE d18_concept_name END,
          CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND
                    num_persons < @smallcellcount
            THEN 'Other'
          WHEN d18_concept_id = -1
            THEN NULL
          ELSE d19_concept_name END,
          CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
            THEN 'Other'
          WHEN d19_concept_id = -1
            THEN NULL
          ELSE d20_concept_name END
      ) t18
    GROUP BY index_year,
      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      ELSE d1_concept_id END,
      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d1_concept_id = -1
        THEN NULL
      ELSE d2_concept_id END,
      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d2_concept_id = -1
        THEN NULL
      ELSE d3_concept_id END,
      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d3_concept_id = -1
        THEN NULL
      ELSE d4_concept_id END,
      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d4_concept_id = -1
        THEN NULL
      ELSE d5_concept_id END,
      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d5_concept_id = -1
        THEN NULL
      ELSE d6_concept_id END,
      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d6_concept_id = -1
        THEN NULL
      ELSE d7_concept_id END,
      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d7_concept_id = -1
        THEN NULL
      ELSE d8_concept_id END,
      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d8_concept_id = -1
        THEN NULL
      ELSE d9_concept_id END,
      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d9_concept_id = -1
        THEN NULL
      ELSE d10_concept_id END,
      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d10_concept_id = -1
        THEN NULL
      ELSE d11_concept_id END,
      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d11_concept_id = -1
        THEN NULL
      ELSE d12_concept_id END,
      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d12_concept_id = -1
        THEN NULL
      ELSE d13_concept_id END,
      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d13_concept_id = -1
        THEN NULL
      ELSE d14_concept_id END,
      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d14_concept_id = -1
        THEN NULL
      ELSE d15_concept_id END,
      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d15_concept_id = -1
        THEN NULL
      ELSE d16_concept_id END,
      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d16_concept_id = -1
        THEN NULL
      ELSE d17_concept_id END,
      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d17_concept_id = -1
        THEN NULL
      ELSE d18_concept_id END,
      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND num_persons < @smallcellcount
        THEN -1
      WHEN d18_concept_id = -1
        THEN NULL
      ELSE d19_concept_id END,
      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
        THEN -1
      WHEN d19_concept_id = -1
        THEN NULL
      ELSE d20_concept_id END,
      CASE WHEN d1_concept_id > 0 AND (d2_concept_id IS NULL OR d2_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      ELSE d1_concept_name END,
      CASE WHEN d2_concept_id > 0 AND (d3_concept_id IS NULL OR d3_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d1_concept_id = -1
        THEN NULL
      ELSE d2_concept_name END,
      CASE WHEN d3_concept_id > 0 AND (d4_concept_id IS NULL OR d4_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d2_concept_id = -1
        THEN NULL
      ELSE d3_concept_name END,
      CASE WHEN d4_concept_id > 0 AND (d5_concept_id IS NULL OR d5_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d3_concept_id = -1
        THEN NULL
      ELSE d4_concept_name END,
      CASE WHEN d5_concept_id > 0 AND (d6_concept_id IS NULL OR d6_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d4_concept_id = -1
        THEN NULL
      ELSE d5_concept_name END,
      CASE WHEN d6_concept_id > 0 AND (d7_concept_id IS NULL OR d7_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d5_concept_id = -1
        THEN NULL
      ELSE d6_concept_name END,
      CASE WHEN d7_concept_id > 0 AND (d8_concept_id IS NULL OR d8_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d6_concept_id = -1
        THEN NULL
      ELSE d7_concept_name END,
      CASE WHEN d8_concept_id > 0 AND (d9_concept_id IS NULL OR d9_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d7_concept_id = -1
        THEN NULL
      ELSE d8_concept_name END,
      CASE WHEN d9_concept_id > 0 AND (d10_concept_id IS NULL OR d10_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d8_concept_id = -1
        THEN NULL
      ELSE d9_concept_name END,
      CASE WHEN d10_concept_id > 0 AND (d11_concept_id IS NULL OR d11_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d9_concept_id = -1
        THEN NULL
      ELSE d10_concept_name END,
      CASE WHEN d11_concept_id > 0 AND (d12_concept_id IS NULL OR d12_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d10_concept_id = -1
        THEN NULL
      ELSE d11_concept_name END,
      CASE WHEN d12_concept_id > 0 AND (d13_concept_id IS NULL OR d13_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d11_concept_id = -1
        THEN NULL
      ELSE d12_concept_name END,
      CASE WHEN d13_concept_id > 0 AND (d14_concept_id IS NULL OR d14_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d12_concept_id = -1
        THEN NULL
      ELSE d13_concept_name END,
      CASE WHEN d14_concept_id > 0 AND (d15_concept_id IS NULL OR d15_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d13_concept_id = -1
        THEN NULL
      ELSE d14_concept_name END,
      CASE WHEN d15_concept_id > 0 AND (d16_concept_id IS NULL OR d16_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d14_concept_id = -1
        THEN NULL
      ELSE d15_concept_name END,
      CASE WHEN d16_concept_id > 0 AND (d17_concept_id IS NULL OR d17_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d15_concept_id = -1
        THEN NULL
      ELSE d16_concept_name END,
      CASE WHEN d17_concept_id > 0 AND (d18_concept_id IS NULL OR d18_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d16_concept_id = -1
        THEN NULL
      ELSE d17_concept_name END,
      CASE WHEN d18_concept_id > 0 AND (d19_concept_id IS NULL OR d19_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d17_concept_id = -1
        THEN NULL
      ELSE d18_concept_name END,
      CASE WHEN d19_concept_id > 0 AND (d20_concept_id IS NULL OR d20_concept_id = -1) AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d18_concept_id = -1
        THEN NULL
      ELSE d19_concept_name END,
      CASE WHEN d20_concept_id > 0 AND num_persons < @smallcellcount
        THEN 'Other'
      WHEN d19_concept_id = -1
        THEN NULL
      ELSE d20_concept_name END
  ) t19;

TRUNCATE TABLE @studyName_drug_seq_summary_temp;
DROP TABLE @studyName_drug_seq_summary_temp;

}




/*****
Final tables for export:
save these results and report back with the central coordinating center
*****/


--0.  count total persons for attrition table
IF OBJECT_ID('@studyName_@sourceName_summary', 'U') IS NOT NULL
DROP TABLE @studyName_ @sourceName_summary;

CREATE TABLE @resultsSchema.dbo.@studyName_@sourceName_summary
(
count_type VARCHAR (500),
num_persons INT
);


INSERT INTO @resultsSchema.dbo.@studyName_@sourceName_summary (count_type, num_persons)
SELECT
  'Number of persons'         AS count_type,
  count(DISTINCT p.PERSON_ID) AS num_persons
FROM @cdmSchema.dbo.PERSON p;

INSERT INTO @resultsSchema.dbo.@studyName_@sourceName_summary (count_type, num_persons)
SELECT
  'Number of persons in target cohort' AS count_type,
  count(DISTINCT person_id)            AS num_persons
FROM @studyName_MatchCohort;


USE @resultsSchema;

--1.  count total persons with a treatment, by year
IF OBJECT_ID('@studyName_@sourceName_person_cnt', 'U') IS NOT NULL
DROP TABLE @studyName_ @sourceName_person_cnt;

CREATE TABLE @resultsSchema.dbo.@studyName_@sourceName_person_cnt
(
index_year INT,
num_persons INT
);

INSERT INTO @resultsSchema.dbo.@studyName_@sourceName_person_cnt (index_year, num_persons)
SELECT
  index_year,
  num_persons
FROM
  (
    SELECT
      index_year,
      sum(num_persons) AS num_persons
    FROM @studyName_drug_seq_summary
    GROUP BY index_year
  ) t1;

--2.  count total persons with a treatment, overall (29Dec2014:  now add to year summary table)

INSERT INTO @resultsSchema.dbo.@studyName_@sourceName_person_cnt (index_year, num_persons)
SELECT
  9999 AS index_year,
  num_persons
FROM
  (
    SELECT sum(num_persons) AS num_persons
    FROM @studyName_drug_seq_summary
  ) t1;

--3.  summary by year:   edit the where clause if you need to remove cell counts < minimum number
IF OBJECT_ID('@studyName_@sourceName_seq_cnt', 'U') IS NOT NULL
DROP TABLE @studyName_ @sourceName_seq_cnt;


CREATE TABLE @resultsSchema.dbo.@studyName_@sourceName_seq_cnt
(
index_year INT,
d1_concept_id INT,
d2_concept_id INT,
d3_concept_id INT,
d4_concept_id INT,
d5_concept_id INT,
d6_concept_id INT,
d7_concept_id INT,
d8_concept_id INT,
d9_concept_id INT,
d10_concept_id INT,
d11_concept_id INT,
d12_concept_id INT,
d13_concept_id INT,
d14_concept_id INT,
d15_concept_id INT,
d16_concept_id INT,
d17_concept_id INT,
d18_concept_id INT,
d19_concept_id INT,
d20_concept_id INT,
d1_concept_name VARCHAR (255),
d2_concept_name VARCHAR (255),
d3_concept_name VARCHAR (255),
d4_concept_name VARCHAR (255),
d5_concept_name VARCHAR (255),
d6_concept_name VARCHAR (255),
d7_concept_name VARCHAR (255),
d8_concept_name VARCHAR (255),
d9_concept_name VARCHAR (255),
d10_concept_name VARCHAR (255),
d11_concept_name VARCHAR (255),
d12_concept_name VARCHAR (255),
d13_concept_name VARCHAR (255),
d14_concept_name VARCHAR (255),
d15_concept_name VARCHAR (255),
d16_concept_name VARCHAR (255),
d17_concept_name VARCHAR (255),
d18_concept_name VARCHAR (255),
d19_concept_name VARCHAR (255),
d20_concept_name VARCHAR (255),
num_persons INT
);

INSERT INTO @resultsSchema.dbo.@studyName_@sourceName_seq_cnt (index_year, d1_concept_id, d2_concept_id, d3_concept_id, d4_concept_id, d5_concept_id, d6_concept_id, d7_concept_id, d8_concept_id, d9_concept_id, d10_concept_id, d11_concept_id, d12_concept_id, d13_concept_id, d14_concept_id, d15_concept_id, d16_concept_id, d17_concept_id, d18_concept_id, d19_concept_id, d20_concept_id, d1_concept_name, d2_concept_name, d3_concept_name, d4_concept_name, d5_concept_name, d6_concept_name, d7_concept_name, d8_concept_name, d9_concept_name, d10_concept_name, d11_concept_name, d12_concept_name, d13_concept_name, d14_concept_name, d15_concept_name, d16_concept_name, d17_concept_name, d18_concept_name, d19_concept_name, d20_concept_name, num_persons)
SELECT
  index_year,
  d1_concept_id,
  d2_concept_id,
  d3_concept_id,
  d4_concept_id,
  d5_concept_id,
  d6_concept_id,
  d7_concept_id,
  d8_concept_id,
  d9_concept_id,
  d10_concept_id,
  d11_concept_id,
  d12_concept_id,
  d13_concept_id,
  d14_concept_id,
  d15_concept_id,
  d16_concept_id,
  d17_concept_id,
  d18_concept_id,
  d19_concept_id,
  d20_concept_id,
  d1_concept_name,
  d2_concept_name,
  d3_concept_name,
  d4_concept_name,
  d5_concept_name,
  d6_concept_name,
  d7_concept_name,
  d8_concept_name,
  d9_concept_name,
  d10_concept_name,
  d11_concept_name,
  d12_concept_name,
  d13_concept_name,
  d14_concept_name,
  d15_concept_name,
  d16_concept_name,
  d17_concept_name,
  d18_concept_name,
  d19_concept_name,
  d20_concept_name,
  num_persons
FROM @studyName_drug_seq_summary;

--4.  overall summary (group by year):   edit the where clause if you need to remove cell counts < minimum number (here 1 as example)
--insert into @resultsSchema.dbo.@studyName_@sourceName_seq_cnt (index_year, d1_concept_id, d2_concept_id, d3_concept_id, d4_concept_id, d5_concept_id, d6_concept_id, d7_concept_id, d8_concept_id, d9_concept_id, d10_concept_id, d11_concept_id, d12_concept_id, d13_concept_id, d14_concept_id, d15_concept_id, d16_concept_id, d17_concept_id, d18_concept_id, d19_concept_id, d20_concept_id, d1_concept_name, d2_concept_name, d3_concept_name, d4_concept_name, d5_concept_name, d6_concept_name, d7_concept_name, d8_concept_name, d9_concept_name, d10_concept_name, d11_concept_name, d12_concept_name, d13_concept_name, d14_concept_name, d15_concept_name, d16_concept_name, d17_concept_name, d18_concept_name, d19_concept_name, d20_concept_name, num_persons)
--select *
--from
--(
--select 9999 as index_year, d1_concept_id, d2_concept_id, d3_concept_id, d4_concept_id, d5_concept_id, d6_concept_id, d7_concept_id, d8_concept_id, d9_concept_id, d10_concept_id, d11_concept_id, d12_concept_id, d13_concept_id, d14_concept_id, d15_concept_id, d16_concept_id, d17_concept_id, d18_concept_id, d19_concept_id, d20_concept_id, d1_concept_name, d2_concept_name, d3_concept_name, d4_concept_name, d5_concept_name, d6_concept_name, d7_concept_name, d8_concept_name, d9_concept_name, d10_concept_name, d11_concept_name, d12_concept_name, d13_concept_name, d14_concept_name, d15_concept_name, d16_concept_name, d17_concept_name, d18_concept_name, d19_concept_name, d20_concept_name,
--	sum(num_persons) as num_persons
--from @studyName_drug_seq_summary
--group by d1_concept_id, d2_concept_id, d3_concept_id, d4_concept_id, d5_concept_id, d6_concept_id, d7_concept_id, d8_concept_id, d9_concept_id, d10_concept_id, d11_concept_id, d12_concept_id, d13_concept_id, d14_concept_id, d15_concept_id, d16_concept_id, d17_concept_id, d18_concept_id, d19_concept_id, d20_concept_id, d1_concept_name, d2_concept_name, d3_concept_name, d4_concept_name, d5_concept_name, d6_concept_name, d7_concept_name, d8_concept_name, d9_concept_name, d10_concept_name, d11_concept_name, d12_concept_name, d13_concept_name, d14_concept_name, d15_concept_name, d16_concept_name, d17_concept_name, d18_concept_name, d19_concept_name, d20_concept_name
--) t1
--;

--For Oracle: cleanup temp tables:
--TRUNCATE TABLE @studyName_matchcohort;
--DROP TABLE @studyName_matchcohort;
--TRUNCATE TABLE @studyName_drug_seq_temp;
--DROP TABLE @studyName_drug_seq_temp;
---TRUNCATE TABLE @studyName_drug_seq;
--DROP TABLE @studyName_drug_seq;
--TRUNCATE TABLE @studyName_drug_seq_summary;
--DROP TABLE @studyName_drug_seq_summary;

