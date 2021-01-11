
SELECT cohort_definition_id,
	COUNT(*) AS cohort_count,
	COUNT(DISTINCT subject_id) AS person_count
FROM @resultsSchema.@cohortTable
GROUP BY cohort_definition_id;
