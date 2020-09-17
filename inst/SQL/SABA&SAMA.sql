CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (43775457,43703675,41368154,41366705,41370066,41364753,41366481,41361998,41372527,41373305,41367955,41366704,41367754,41362581,41367753,41369314,41372743,41371216,41364752,41367025,41366258,41364751,41361454,41361456,41362228,41361801,41370818,41367211,41362784,41371797,41370278,41364958,41364959,41362786,41368725,41366482,41371798,41372528,41365706,41373306,41362785,41367956,41368936,41370065,41368508,41361455,41373100,41371571,41365138,41371534,41363270,41367723,41372512,41373290,41370784,41370031,41361277,41362960,41371979,41362199,41365137,41372304,41369272,41371533,41367722,41364239,41367940,41370259,41366218,41366671,41365456,41366990,41366670,41367393,41373054,41364240,41372305,41365457,41369505,41361166,41369273,40243202,40243203,41375222,41375428,41374814,41373600,41373590,41375016,41374552,41373755,41375476,41374551,41373908,41374880,41375787,41375182,41374062,41375330,41374550,41374061,41375475,41376059,41374060,41374722,41373622,1154660,1113078,35798540,35798511,35798512,35798548,21079585,21099202,21128524,21079586,21177703,36809343,40710448,40710447,40710446,21040282,21089280,40710390,40710389,40710386,40710388,40710387,40710918,40710917,40710914,40710916,40710915,44218074,44218083,44217944,44218001,44217982,44218000,44218082,44217999,44217943,44218104,40710439,40710438,785036,43043754,43043755,40710442,40710441,40710440,41376336,41377634,41380674,41380484,41380244,41380676,41380675,41382343,41377733,41377994,41376911,41377995,41377383,36896572,41431313,41431026,43614009,41431691,41431647,41431282,41431408,43793892,43668328,41431626,41431594,41431750,41431839,43775961,41431690,41431165,41431900,41431107,41431838,43775962,21167629,44135474,44135468,44135484,44135466,21167630,21138019,40717393,21030094,21049729,21177375,21098830,40717394,21039932,21079221,21177374,21128166,36281881,36281886,21069470,21108658,21098833,21167633,21118397,21088940,21069471,21098832,21177376,21167632,21128167,21108656,21108657,21167631,21098831,21039933,21088939,36281819,36281846,36281854,36281900,36281926,44135465,44135485,21030095,21128168,40717391,35788209,21079222,40717392,35788210,21039934,44135480,44135472,40710234,40710233,40709814,40709813,40709812,40709811,44218211,44218262,44218224,41439878,41439874,41439883,41439880,41439879,41439882,36809819,41440358,41440360,41440362,41440364,41440361,41440355,41440359,41440357,41440365,41440356,41440363,41358276,41351845,41352598,41355809,43737829,43864122,43864123,43755782,43719693,43864121,41476292,41476295,41476289,41476291,41476290,41476298,41476300,41476294,41476297,41476293,41476296,41476299,19021327,42902622,42902809,44084296,44125109,44037182,35746368,35770130,35745021,35770131,42481993,21109766,21168748,21109767,21090034,36810441,21041054,21022908,40733112,40733111,40733110,21032662,21111266,40727739,40727738,40727735,40727737,40727736,40727732,40727734,40727733,40243275,42903409,42903280,21109769,21031158,21060635,21139161,21070622,21031159,21060633,21060634,21041055,21031157,21139160,21119498,43560453,43560451,43560452,43585032,36889752,40897884,40835657,43729034,40844650,41125490,43855161,40875846,41031528,40906753,40875845,40821820,43837214,41078381,44026104,21099391,44034431,44099163,44123231,21030633,21030634,36273092,21118934,44127924,21089484,21148465,36268008,36265427,36262865,44111925,21138591,21118935,44060504,21138590,36259084,44041403,21109241,44028421,43011917,42483138,21041053,36812530,36813445,40733114,40733113,40727741,40727740,1356123,36812203,1356124,36813156,783785,36812756,40142665,40142666,41235897,41204873,41266713,41047702,41297346,41117858,41253335,41191278,41097346,41065817,41211810,40891918,41086572,41222363,41047701,41086571,41097345,40954166,41235397,41242726,40909877,41128680,41284321,41160127,41003561,40836937,41023800,41180401,41235398,40836938,40961583,40909880,41003562,40922895,41297345,41211808,41253333,41315588,41160125,41315587,40836936,41149313,40972208,40941248,41128679,40847761,41211809,40954165,41242725,41160126,41273628,41211811,41191280,41222364,40961582,40847762,41065816,41055048,40909878,41253334,40986515,41082356,41205663,41145166,41050836,40861866,41113592,40895114,40832715,43819643,43819644,43783653,43676008,43621601,43801527,43818574,43746514,43728595,40891609,40929609,41251938,41127152,41272962,40922586,41110171,41179732,41095923,40846320,41095922,40908457,40877449,41023098,41179731,40898507,40836320,41033184,40939699,41002116,41189787,41064345,40891610,40953803,41242070,41002115,41127153,41211171,41110172,41211172,41095924,40939700,41023099,44183804,44181136,44166196,44177422,40727828,40727827,783780,43034241,43145331,43167453,43200458,43145332,43200459,43200460,41047379,40960950,40970739,41002114,40867395,40727831,40727830,40727829,40922585,43167454,43211266,40877448,40939698,41220947,40908456,41304069,43156522,41148643,40929608,43200461,43189428,43156523,36811310,40891611,41189789,41220948,41158608,41064344,41064343,41033185,43638701,41265971,41148642,41282806,44160648,44187566,44162373,41117166,41158610,41189788,41158609,41272963,40824959,43134418,41019257,43167452,41113279,40727834,40727832,40727833,36811735,43034240,36811152,40953636,40891439,40953637,41108883)

) I
) C UNION ALL 
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (43705671,43867885,43795492,41365138,41371534,41363270,41367723,41372512,41373290,41370784,41370031,41361277,41362960,41371979,41362199,41365137,41372304,41369272,41371533,41367722,41364239,41367940,41370259,41366218,41366671,41365456,41366990,41366670,41367393,41373054,41364240,41372305,41365457,41369505,41361166,41369273,40243202,40243203,41375222,41375428,41374814,41373600,41373590,41375016,41374552,41373755,41375476,41374551,41373908,41374880,41375787,41375182,41374062,41375330,41374550,41374061,41375475,41376059,41374060,41374722,41373622,41375829,41375131,41374815,41375619,41374219,41374881,41375018,41375478,41375620,41374063,41373909,41374553,41375017,41374218,41375477,41376061,41375184,41376060,41375183,41375223,1154660,1113078,40710390,40710389,40710386,40710388,40710387,40710918,40710917,40710914,40710916,40710915,44218074,44218083,44217944,44218001,44217982,44218000,44218082,44217999,44217943,44218104,40710439,40710438,21177691,36283929,36778158,36778157,36283939,785036,43043754,43043755,40710442,40710441,40710440,36896572,41431313,41431026,43614009,41431691,41431647,41431282,41431408,43793892,43668328,41431626,41431594,41431750,41431839,43775961,41431690,41431165,41431900,41431107,41431838,43775962,21167629,44135474,44135468,44135484,44135466,21167630,21138019,40717393,21030094,21049729,21177375,21098830,40717394,21039932,21079221,21177374,21128166,36281881,36281886,21069470,21108658,21098833,21167633,21118397,21088940,21069471,21098832,21177376,21167632,21128167,21108656,21108657,21167631,21098831,21039933,21088939,36281819,36281846,36281854,36281900,36281926,44135465,44135485,21030095,21128168,40717391,35788209,21079222,40717392,35788210,21039934,44135480,44135472,40710234,40710233,40709814,40709813,40709812,40709811,44218215,44218209,44218196,44218275,44218274,44218248,44218208,44218247,44218207,44218273,44218258,44218198,44218206,44218239,44218197,44218223,44218189,44218238,44218222,44218221,44218211,44218262,44218224,41439878,41439874,41439883,41439880,36809819,43755782,43719693,43864121,19021327,42902622,42902809,44080150,44084296,44125109,44037182,40727739,40727738,40727735,40727737,40727736,40727732,40727734,40727733,40243275,42903409,42903280,43560453,43560451,43560452,43585032,36889752,40897884,40835657,43729034,40844650,41125490,43855161,40875846,41031528,40906753,40875845,40821820,43837214,41078381,44026104,21099391,44034431,44099163,44123231,21030633,21030634,36273092,21118934,44127924,21089484,21148465,36268008,36265427,36262865,44111925,21138591,21118935,44060504,21138590,36259084,44041403,21109241,44028421,43011917,40727741,40727740,1356123,36812203,1356124,36813156,783785,36812756,40142665,40142666,43818574,43746514,43728595,40891609,40929609,41251938,41127152,41272962,40922586,41110171,41179732,41095923,40846320,41095922,40908457,40877449,41023098,41179731,40898507,40836320,41033184,40939699,41002116,41189787,41064345,40891610,40953803,41242070,41002115,41127153,41211171,41110172,41211172,41095924,40939700,41023099,21099388,44175685,44183804,44181136,44166196,44177422,44168234,40727828,40727827,21089481,36260439,36783914,783780,43034241,43145331,43167453,43200458,43145332,43200459,43200460,40727831,40727830,40727829,40922585,43167454,43211266,40877448,40939698,41220947,40908456,41304069,43156522,41148643,40929608,43200461,43189428,43156523,36811310,43638701,44160648,44187566,44162373,41265972,41047380,41117166,41158610,41189788,41158609,41141533,36889755,41282807,40846321,40836321,41016084,41272963,40824959,43134418,41019257,43167452,41113279,40727834,40727832,40727833,36811735,43034240,36811152,36264213,43818604,43272421)

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
  select PE.person_id, PE.event_id, PE.start_date, PE.end_date, PE.target_concept_id, PE.visit_occurrence_id, PE.sort_date FROM (
-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) C


-- End Drug Exposure Criteria

) PE
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) C


-- End Drug Exposure Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id 
  and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) C


-- End Drug Exposure Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id 
  and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) P
INNER JOIN
(
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 1))
) C


-- End Drug Exposure Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,0,P.START_DATE) AND A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id and AC.event_id = pe.event_id

UNION ALL
select PE.person_id, PE.event_id, PE.start_date, PE.end_date, PE.target_concept_id, PE.visit_occurrence_id, PE.sort_date FROM (
-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 1))
) C


-- End Drug Exposure Criteria

) PE
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 1))
) C


-- End Drug Exposure Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id 
  and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 1))
) C


-- End Drug Exposure Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id 
  and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) P
INNER JOIN
(
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) C


-- End Drug Exposure Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,0,P.START_DATE) AND A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id and AC.event_id = pe.event_id

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
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

create table #inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

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