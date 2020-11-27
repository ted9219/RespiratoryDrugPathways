CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 9 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (317009)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (317009)
  and c.invalid_reason is null

) I
LEFT JOIN
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (46274062,45772073,42535716,4110051,4312524)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (46274062,45772073,42535716,4110051,4312524)
  and c.invalid_reason is null

) E ON I.concept_id = E.concept_id
WHERE E.concept_id is null
) C UNION ALL
SELECT 10 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (44807895,46274062,46284163,4110056,4196712,4193588,44782563,4209097,44791725)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (44807895,46274062,46284163,4110056,4196712,4193588,44782563,4209097,44791725)
  and c.invalid_reason is null

) I
LEFT JOIN
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (46269802)

) E ON I.concept_id = E.concept_id
WHERE E.concept_id is null
) C UNION ALL
SELECT 11 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (261325)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (261325)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 12 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (21603248)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (21603248)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 13 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (437082,40479837,201606,374919,4319324,80809,438688,4137275)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (437082,40479837,201606,374919,4319324,80809,438688,4137275)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 14 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (318800)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (318800)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 15 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (42537251)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (42537251)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 16 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4125022,45769441,45771045,46273452,43530693,45773005,46270082,46273487,36684328,45769442,45769438,46269767,45769443,46274124,46269777,45769352,46273462,45769351,46269785,46274059,45769350,46269771,37116845,4191479,45766728,45766727,317009,4156136,4152911,4152292,44805087,44805089,4015947,4293734,4194289,4161595,4017025,4017182,4015819,4308356,42535716,4152418,36674599,4017184,4017293,4191827,4017183,4017026,4155473,4152420,44792262,44788781,44788789,44788824,44788825,44792263,4265861,44807733,44792264,44788779,44788780,44799960,44792265,44807127,43530745,4206340,46287068,4143474,4075237,4311814,4225553,4245676,4051466,44810117,46269802,313236,4217558,4279553,42538744,36684335,257581,4138760,45768911,45772937,45768912,443801,4271333,4123254,4123253,312950,761844,4141978,46270030,46270322,46270573,4145497,252658,4225554,4119298,42536207,35609846,35609847,4155468,4146581,46269776,46274060,46269780,4143828,46269778,46269779,46270029,46269781,46269782,4022592,4110051,42536208,4155469,46269784,46269786,46269788,4142738,46269783,46273454,46269790,46269789,46269787,4309833,4155470,4212099,37109103,764677,764949,4232595,4086436,40483397,4152913,37108580,46269770,46269772,46269774,4145356,46269773,46270028,46269775,37108581,4247524,46273635,4312524,4120261,4301938,42539549,45768910,45768963,45768964,42536649,45768965,4211530)

) I
) C UNION ALL
SELECT 17 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (257004,44807895,40664816,40757953,36309893,44802479,256448,255841,40316591,40316555,255573,36684442,2101887,4110175,45877605,764579,42689663,42690682,42690688,42689665,44788723,44788724,44789649,2617703,2617704,2617705,2617702,2617701,2617700,19087349,36237014,36237261,2101897,2101895,19087350,1140449,45304853,45133801,44886066,45073366,40134492,2101890,2101891,42539771,4235401,3044760,44803712,44803714,44803925,2106233,2106220,36685451,36685452,36685453,36685454,36685455,36685456,36685457,36685458,40664872,2108545,2108544,2108541,2108542,45884823,4196712,4193588,44788819,2617553,2617554,42689543,2106218,2106217,2106213,2108546,2101892,4153356,2108543,4209097,2106190,43533300,43533319,2106212,2106191,1314354,2101868,2108525,2108526,44791725)

) I
) C UNION ALL
SELECT 18 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (46274062)

) I
) C
;

with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM
  (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
       C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.condition_start_date as sort_date
FROM
(
  SELECT co.* , row_number() over (PARTITION BY co.person_id ORDER BY co.condition_start_date, co.condition_occurrence_id) as ordinal
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 18))
) C

WHERE C.ordinal = 1
-- End Condition Occurrence Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,365,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,1095,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe

) QE

;

--- Inclusion Rule Inserts

select 0 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_0
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Demographic Criteria
SELECT 0 as index_id, e.person_id, e.event_id
FROM #qualified_events E
JOIN @cdm_database_schema.PERSON P ON P.PERSON_ID = E.PERSON_ID
WHERE YEAR(E.start_date) - P.year_of_birth >= 18
GROUP BY e.person_id, e.event_id
-- End Demographic Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 1 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_1
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
       C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.condition_start_date as sort_date
FROM
(
  SELECT co.*
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 11))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 2 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_2
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
       C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.condition_start_date as sort_date
FROM
(
  SELECT co.*
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 13))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 3 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_3
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Demographic Criteria
SELECT 0 as index_id, e.person_id, e.event_id
FROM #qualified_events E
JOIN @cdm_database_schema.PERSON P ON P.PERSON_ID = E.PERSON_ID
WHERE E.start_date > DATEFROMPARTS(2010, 12, 31) AND E.end_date < DATEFROMPARTS(2019, 12, 31)
GROUP BY e.person_id, e.event_id
-- End Demographic Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

SELECT inclusion_rule_id, person_id, event_id
INTO #inclusion_events
FROM (select inclusion_rule_id, person_id, event_id from #Inclusion_0
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_1
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_2
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_3) I;
TRUNCATE TABLE #Inclusion_0;
DROP TABLE #Inclusion_0;

TRUNCATE TABLE #Inclusion_1;
DROP TABLE #Inclusion_1;

TRUNCATE TABLE #Inclusion_2;
DROP TABLE #Inclusion_2;

TRUNCATE TABLE #Inclusion_3;
DROP TABLE #Inclusion_3;


with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),4)-1)

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows

			UNION ALL


			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date
FROM #final_cohort CO
;





TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;