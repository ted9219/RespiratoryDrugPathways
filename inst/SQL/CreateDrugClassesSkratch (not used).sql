-- Defining asthma drug classes from scratch

/*
Import the ingredients
 */

-- MANUAL IMPORT 'asthma_drugs.csv' -> NO SUPERUSER ACCESS

/*
-- option 1
DROP TABLE asthma_drugs;
CREATE TABLE amarkus.asthma_drugs
				(
				atc_code TEXT,
				description TEXT,
				med_group TEXT,
				name_single_ingredient TEXT,
				concept_id INT
				)

COPY asthma_drugs(atc_code, description, med_group, name_single_ingredient, concept_id)
FROM 	'J:\amarkus\Documents\Asthma_definitions\asthma_drugs.csv'
DELIMITER ','
CSV HEADER;

-- option 2
cat 'J:\amarkus\Documents\Asthma_definitions\asthma_drugs.csv'
psql -c 'COPY asthma_drugs(atc_code, description, med_group, name_single_ingredient, concept_id) FROM STDIN CSV;'
 */


/*
Filter based on dose form

 */

-- MANUAL IMPORT 'medgroup_dosefrom.csv' -> NO SUPERUSER ACCESS

/*
DROP TABLE medgroup_doseform;
CREATE TABLE amarkus.medgroup_doseform
					(
					route TEXT,
					med_group TEXT,
					dose_form TEXT
					)
 */

-- UPDATE medgroup_doseform SET dose_form = NULL WHERE dose_form = ''; -- convert to NULL values

DROP TABLE IF EXISTS @cohort_database_schema.drug_classes;

CREATE TABLE @cohort_database_schema.drug_classes AS
SELECT
  CONCAT(asthma_drugs.med_group, ' all') AS name,
  count(DISTINCT descendants.concept_id) AS count,
  ARRAY_AGG(DISTINCT descendants.concept_id) AS concept_set
FROM asthma_drugs
  INNER JOIN cdm.concept_ancestor
  -- look for all descendants
    ON concept_ancestor.ancestor_concept_id = asthma_drugs.concept_id
  LEFT JOIN cdm.concept AS descendants -- get information descendants
    ON descendants.concept_id = concept_ancestor.descendant_concept_id
  LEFT JOIN cdm.concept_relationship AS dose_form  -- get dose form
    ON dose_form.concept_id_1 = descendants.concept_id
       AND dose_form.relationship_id = 'RxNorm has dose form'
  LEFT JOIN cdm.concept AS dose_form_info -- get information dose form (todo: check double dose forms)
    ON dose_form_info.concept_id = dose_form.concept_id_2
  INNER JOIN medgroup_doseform AS selected_dose_forms
    ON asthma_drugs.med_group = selected_dose_forms.med_group AND
       dose_form_info.concept_name IS NOT DISTINCT FROM selected_dose_forms.dose_form
GROUP BY asthma_drugs.med_group;

/*
Split in monotherapy versus fixed combinations

 */
INSERT INTO @cohort_database_schema.drug_classes
SELECT
  CONCAT(result.med_group, ' mono') AS name,
  count(DISTINCT concept_id) AS count,
  ARRAY_AGG(DISTINCT concept_id) AS concept_set
FROM (
       SELECT
         med_group,
         concept_id,
         concept_name,
         count(ingredient_concept_id)
         OVER (
           PARTITION BY med_group, concept_id ) AS num_ingredient
       FROM (SELECT DISTINCT
               asthma_drugs.med_group,
               descendants.concept_id,
               descendants.concept_name,
               ingredient_concept_id
             FROM asthma_drugs
               INNER JOIN cdm.concept_ancestor
               -- look for all descendants
                 ON concept_ancestor.ancestor_concept_id = asthma_drugs.concept_id
               LEFT JOIN cdm.concept AS descendants -- get information descendants
                 ON descendants.concept_id = concept_ancestor.descendant_concept_id
               LEFT JOIN cdm.concept_relationship AS dose_form  -- get dose form
                 ON dose_form.concept_id_1 = descendants.concept_id
                    AND dose_form.relationship_id = 'RxNorm has dose form'
               LEFT JOIN cdm.concept AS dose_form_info -- get information dose form (todo: check double dose forms)
                 ON dose_form_info.concept_id = dose_form.concept_id_2
               LEFT JOIN cdm.drug_strength AS drug_strength
                 ON drug_strength.drug_concept_id = concept_ancestor.descendant_concept_id
               INNER JOIN medgroup_doseform AS selected_dose_forms
                 ON asthma_drugs.med_group = selected_dose_forms.med_group AND
                    dose_form_info.concept_name IS NOT DISTINCT FROM
                    selected_dose_forms.dose_form) AS ingredients) AS result
WHERE num_ingredient <= 1
GROUP BY med_group;

INSERT INTO @cohort_database_schema.drug_classes
SELECT
  CONCAT(result.med_group, ' combi') AS name,
  count(DISTINCT concept_id) AS count,
  ARRAY_AGG(DISTINCT concept_id) AS concept_set
FROM (
       SELECT
         med_group,
         concept_id,
         concept_name,
         count(ingredient_concept_id)
         OVER (
           PARTITION BY med_group, concept_id ) AS num_ingredient
       FROM (SELECT DISTINCT
               asthma_drugs.med_group,
               descendants.concept_id,
               descendants.concept_name,
               ingredient_concept_id
             FROM asthma_drugs
               INNER JOIN cdm.concept_ancestor
               -- look for all descendants
                 ON concept_ancestor.ancestor_concept_id = asthma_drugs.concept_id
               LEFT JOIN cdm.concept AS descendants -- get information descendants
                 ON descendants.concept_id = concept_ancestor.descendant_concept_id
               LEFT JOIN cdm.concept_relationship AS dose_form  -- get dose form
                 ON dose_form.concept_id_1 = descendants.concept_id
                    AND dose_form.relationship_id = 'RxNorm has dose form'
               LEFT JOIN cdm.concept AS dose_form_info -- get information dose form (todo: check double dose forms)
                 ON dose_form_info.concept_id = dose_form.concept_id_2
               LEFT JOIN cdm.drug_strength AS drug_strength
                 ON drug_strength.drug_concept_id = concept_ancestor.descendant_concept_id
               INNER JOIN medgroup_doseform AS selected_dose_forms
                 ON asthma_drugs.med_group = selected_dose_forms.med_group AND
                    dose_form_info.concept_name IS NOT DISTINCT FROM
                    selected_dose_forms.dose_form) AS ingredients) AS result
WHERE num_ingredient > 1
GROUP BY med_group;


/*
Define drug strength

 */



