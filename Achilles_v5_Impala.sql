
/*******************************************************************
Achilles - database profiling summary statistics generation

SQL for OMOP CDM v5
*******************************************************************/

-- 0   Number of persons
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 0 as analysis_id,  '@source_name' as stratum_1, count(distinct person_id) as count_value
from PERSON;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value)
select 0 as analysis_id, '@source_name' as stratum_1, count(distinct person_id) as count_value
from PERSON;


/********************************************
ACHILLES Analyses on PERSON table
*********************************************/

-- 1   Number of persons
insert into ACHILLES_results (analysis_id, count_value)
select 1 as analysis_id,  count(distinct person_id) as count_value
from PERSON;


-- 2   Number of persons by gender
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 2 as analysis_id,  gender_concept_id as stratum_1, count(distinct person_id) as count_value
from PERSON
group by GENDER_CONCEPT_ID;


-- 3   Number of persons by year of birth
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 3 as analysis_id,  year_of_birth as stratum_1, count(distinct person_id) as count_value
from PERSON
group by YEAR_OF_BIRTH;


-- 4   Number of persons by race
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 4 as analysis_id,  RACE_CONCEPT_ID as stratum_1, count(distinct person_id) as count_value
from PERSON
group by RACE_CONCEPT_ID;


-- 5   Number of persons by ethnicity
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 5 as analysis_id,  ETHNICITY_CONCEPT_ID as stratum_1, count(distinct person_id) as count_value
from PERSON
group by ETHNICITY_CONCEPT_ID;


-- 7   Number of persons with invalid provider_id
insert into ACHILLES_results (analysis_id, count_value)
select 7 as analysis_id,  count(p1.person_id) as count_value
from PERSON p1
   left join provider pr1
   on p1.provider_id = pr1.provider_id
where p1.provider_id is not null
   and pr1.provider_id is null
;


-- 8   Number of persons with invalid location_id
insert into ACHILLES_results (analysis_id, count_value)
select 8 as analysis_id,  count(p1.person_id) as count_value
from PERSON p1
   left join location l1
   on p1.location_id = l1.location_id
where p1.location_id is not null
   and l1.location_id is null
;


-- 9   Number of persons with invalid care_site_id
insert into ACHILLES_results (analysis_id, count_value)
select 9 as analysis_id,  count(p1.person_id) as count_value
from PERSON p1
   left join care_site cs1
   on p1.care_site_id = cs1.care_site_id
where p1.care_site_id is not null
   and cs1.care_site_id is null
;


/*****************************************************************
ACHILLES Analyses on OBSERVATION_PERIOD table
******************************************************************/

-- create raw data table based on observation_period_start_date
-- for better query performance
create table OBSERVATION_PERIOD_RAW as
select p.person_id, p.gender_concept_id,
   cast(p.YEAR_OF_BIRTH as int) as year_of_birth, 
   cast(regexp_extract(observation_period_start_date, '(\\d\\d\\d\\d)', 0) as int) as year_of_start
from PERSON p join OBSERVATION_PERIOD op on p.person_id = op.person_id
;

-- 101   Number of persons by age, with age at first observation period
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 101 as analysis_id, cast(index_year - YEAR_OF_BIRTH as string) as stratum_1, count(person_id) as count_value
from (select person_id, year_of_birth, min(year_of_start) as index_year 
      from observation_period_raw 
      group by person_id, year_of_birth) t
group by index_year - year_of_birth;


-- 102   Number of persons by gender by age, with age at first observation period
insert into ACHILLES_results (analysis_id, stratum_1, count_value, stratum_2)
select 102 as analysis_id, gender_concept_id as stratum_1, count(person_id) as count_value, cast(index_year - year_of_birth as string) as stratum_2
from (select person_id, year_of_birth, gender_concept_id, min(year_of_start) as index_year 
      from observation_period_raw 
      group by person_id, year_of_birth, gender_concept_id ) t
group by gender_concept_id, index_year - year_of_birth;


-- 103   Distribution of age at first observation period
insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
with rawData as
( select person_id, min(year_of_start - year_of_birth) as age_value
  from observation_period_raw 
  group by person_id, year_of_birth
),
overallStats as
(
  select avg(1.0 * age_value) as avg_value,
  stddev(age_value) as stdev_value,
  min(age_value) as min_value,
  max(age_value) as max_value,
  count(*) as total
  from rawData
),
ageStats as
(
  select age_value, count(*) as total, row_number() over (order by age_value) as rn
  from rawData
  group by age_value
),
ageStatsPrior as
(
  select s.age_value, s.total, sum(p.total) as accumulated
  from ageStats s
  join ageStats p on p.rn <= s.rn
  group by s.age_value, s.total, s.rn
)
select 103 as analysis_id,
  o.total as count_value,
  o.min_value,
  o.max_value,
  cast(o.avg_value as float),
  cast(o.stdev_value as float),
  min(case when p.accumulated >= .50 * o.total then age_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then age_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then age_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then age_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
from ageStatsPrior p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;


-- 104   Distribution of age at first observation period by gender
insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
with rawData as
(
  select gender_concept_id, min(year_of_start) - year_of_birth as age_value
  from observation_period_raw
  group by person_id, gender_concept_id, year_of_birth
),
overallStats as
(
  select gender_concept_id,
    avg(1.0 *age_value) as avg_value,
    stddev(age_value) as stdev_value,
    min(age_value) as min_value,
    max(age_value) as max_value,
    count(*) as total
  from rawData
  group by gender_concept_id
),
ageStats as
(
  select gender_concept_id, age_value, count(*) as total, row_number() over (order by age_value) as rn
  from rawData
  group by gender_concept_id, age_value
),
ageStatsPrior as
(
  select s.gender_concept_id, s.age_value, s.total, sum(p.total) as accumulated
  from ageStats s
  join ageStats p on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
  group by s.gender_concept_id, s.age_value, s.total, s.rn
)
select 104 as analysis_id,
  o.gender_concept_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  cast(o.avg_value as float),
  cast(o.stdev_value as float),
  min(case when p.accumulated >= .50 * o.total then age_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then age_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then age_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then age_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
from ageStatsPrior p
join overallStats o on p.gender_concept_id = o.gender_concept_id
group by o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;


-- 105   Length of observation (days) of first observation period
with rawData as
(
  select count_value
  from
  (
    select DATEDIFF(dd,op.observation_period_start_date, op.observation_period_end_date) as count_value,
       ROW_NUMBER() over (PARTITION by op.person_id order by op.observation_period_start_date asc) as rn
    from OBSERVATION_PERIOD op
   ) op
   where op.rn = 1
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
  stddev(count_value) as stdev_value,
  min(count_value) as min_value,
  max(count_value) as max_value,
  count(*) as total
  from rawData
),
stats as
(
  select count_value, count(*) as total, row_number() over (order by count_value) as rn
  from
  (
    select DATEDIFF(dd,op.observation_period_start_date, op.observation_period_end_date) as count_value,
       ROW_NUMBER() over (PARTITION by op.person_id order by op.observation_period_start_date asc) as rn
    from OBSERVATION_PERIOD op
   ) op
  where op.rn = 1
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 105 as analysis_id,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


-- 106   Length of observation (days) of first observation period by gender
with rawData as
(
  select p.gender_concept_id, op.count_value
  FROM
  (
    select person_id, DATEDIFF(dd,op.observation_period_start_date, op.observation_period_end_date) as count_value,
      ROW_NUMBER() over (PARTITION by op.person_id order by op.observation_period_start_date asc) as rn
    from OBSERVATION_PERIOD op
   ) op
  JOIN PERSON p on op.person_id = p.person_id
   where op.rn = 1
),
overallStats as
(
  select gender_concept_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by gender_concept_id
),
stats as
(
  select gender_concept_id, count_value, count(*) as total, row_number() over (order by count_value) as rn
  from rawData
  group by gender_concept_id, count_value
),
priorStats as
(
  select s.gender_concept_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
  group by s.gender_concept_id, s.count_value, s.total, s.rn
)
select 106 as analysis_id,
  o.gender_concept_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value end) as p90_value
from priorStats p
join overallStats o on p.gender_concept_id = o.gender_concept_id
group by o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, gender_concept_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM #tempResults
;


-- 107   Length of observation (days) of first observation period by age decile

with rawData as
(
  select floor((year(op.OBSERVATION_PERIOD_START_DATE) - p.YEAR_OF_BIRTH)/10) as age_decile,
    DATEDIFF(dd,op.observation_period_start_date, op.observation_period_end_date) as count_value
  FROM
  (
    select person_id, 
        op.observation_period_start_date,
        op.observation_period_end_date,
      ROW_NUMBER() over (PARTITION by op.person_id order by op.observation_period_start_date asc) as rn
    from OBSERVATION_PERIOD op
  ) op
  JOIN PERSON p on op.person_id = p.person_id
  where op.rn = 1
),
overallStats as
(
  select age_decile,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by age_decile
),
stats as
(
  select age_decile,
    count_value, 
      count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by age_decile, count_value
),
priorStats as
(
  select s.age_decile, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.age_decile = p.age_decile and p.rn <= s.rn
  group by s.age_decile, s.count_value, s.total, s.rn
)
select 107 as analysis_id,
  o.age_decile,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.age_decile = o.age_decile
group by o.age_decile, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, age_decile, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM #tempResults
;


-- 108   Number of persons by length of observation period, in 30d increments
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 108 as analysis_id,  floor(DATEDIFF(dd, op1.observation_period_start_date, op1.observation_period_end_date)/30) as stratum_1, count(distinct p1.person_id) as count_value
from PERSON p1
   inner join 
   (select person_id, 
      OBSERVATION_PERIOD_START_DATE, 
      OBSERVATION_PERIOD_END_DATE, 
      ROW_NUMBER() over (PARTITION by person_id order by observation_period_start_date asc) as rn1
       from OBSERVATION_PERIOD
   ) op1
   on p1.PERSON_ID = op1.PERSON_ID
   where op1.rn1 = 1
group by floor(DATEDIFF(dd, op1.observation_period_start_date, op1.observation_period_end_date)/30)
;


-- 109   Number of persons with continuous observation in each year
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

IF OBJECT_ID('tempdb..#temp_dates', 'U') IS NOT NULL
   DROP TABLE #temp_dates;

SELECT DISTINCT 
  YEAR(observation_period_start_date) AS obs_year,
  CAST(CAST(YEAR(observation_period_start_date) AS VARCHAR(4)) +  '01' + '01' AS DATE) AS obs_year_start,   
  CAST(CAST(YEAR(observation_period_start_date) AS VARCHAR(4)) +  '12' + '31' AS DATE) AS obs_year_end
INTO
  #temp_dates
FROM observation_period
;

INSERT INTO ACHILLES_results (analysis_id, stratum_1, count_value)
SELECT 
  109 AS analysis_id,  
   obs_year AS stratum_1, 
   count(DISTINCT person_id) AS count_value
FROM observation_period,
   #temp_dates
WHERE  
      observation_period_start_date <= obs_year_start
   AND 
      observation_period_end_date >= obs_year_end
group by 
   obs_year
;

TRUNCATE TABLE #temp_dates;
DROP TABLE #temp_dates;


-- 110   Number of persons with continuous observation in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

IF OBJECT_ID('tempdb..#temp_dates', 'U') IS NOT NULL
   DROP TABLE #temp_dates;

SELECT DISTINCT 
  YEAR(observation_period_start_date)*100 + MONTH(observation_period_start_date) AS obs_month,
  CAST(CAST(YEAR(observation_period_start_date) AS VARCHAR(4)) +  RIGHT('0' + CAST(MONTH(OBSERVATION_PERIOD_START_DATE) AS VARCHAR(2)), 2) + '01' AS DATE) AS obs_month_start,  
  DATEADD(dd,-1,DATEADD(mm,1,CAST(CAST(YEAR(observation_period_start_date) AS VARCHAR(4)) +  RIGHT('0' + CAST(MONTH(OBSERVATION_PERIOD_START_DATE) AS VARCHAR(2)), 2) + '01' AS DATE))) AS obs_month_end
INTO
  #temp_dates
FROM observation_period
;


INSERT INTO ACHILLES_results (analysis_id, stratum_1, count_value)
SELECT 
  110 AS analysis_id, 
   obs_month AS stratum_1, 
   count(DISTINCT person_id) AS count_value
FROM
   observation_period,
   #temp_Dates
WHERE 
      observation_period_start_date <= obs_month_start
   AND 
      observation_period_end_date >= obs_month_end
group by 
   obs_month
;


-- 111   Number of persons by observation period start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 111 as analysis_id, 
   YEAR(observation_period_start_date)*100 + month(OBSERVATION_PERIOD_START_DATE) as stratum_1, 
   count(distinct op1.PERSON_ID) as count_value
from
   observation_period op1
group by YEAR(observation_period_start_date)*100 + month(OBSERVATION_PERIOD_START_DATE)
;


-- 112   Number of persons by observation period end month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 112 as analysis_id,  
   YEAR(observation_period_end_date)*100 + month(observation_period_end_date) as stratum_1, 
   count(distinct op1.PERSON_ID) as count_value
from
   observation_period op1
group by YEAR(observation_period_end_date)*100 + month(observation_period_end_date)
;


-- 113   Number of persons by number of observation periods
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 113 as analysis_id,  
   op1.num_periods as stratum_1, count(distinct op1.PERSON_ID) as count_value
from
   (select person_id, count(OBSERVATION_period_start_date) as num_periods from OBSERVATION_PERIOD group by PERSON_ID) op1
group by op1.num_periods
;

-- 114   Number of persons with observation period before year-of-birth
insert into ACHILLES_results (analysis_id, count_value)
select 114 as analysis_id,  
   count(distinct p1.PERSON_ID) as count_value
from
   PERSON p1
   inner join (select person_id,min(year(OBSERVATION_period_start_date)) as first_obs_year from OBSERVATION_PERIOD group by PERSON_ID) op1
   on p1.person_id = op1.person_id
where p1.year_of_birth > op1.first_obs_year
;

-- 115   Number of persons with observation period end < start
insert into ACHILLES_results (analysis_id, count_value)
select 115 as analysis_id,  
   count(op1.PERSON_ID) as count_value
from
   observation_period op1
where op1.observation_period_end_date < op1.observation_period_start_date
;



-- 116   Number of persons with at least one day of observation in each year by gender and age decile
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

IF OBJECT_ID('tempdb..#temp_dates', 'U') IS NOT NULL
   DROP TABLE #temp_dates;

select distinct 
  YEAR(observation_period_start_date) as obs_year 
INTO
  #temp_dates
from 
  OBSERVATION_PERIOD
;

insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, count_value)
select 116 as analysis_id,  
   t1.obs_year as stratum_1, 
   p1.gender_concept_id as stratum_2,
   floor((t1.obs_year - p1.year_of_birth)/10) as stratum_3,
   count(distinct p1.PERSON_ID) as count_value
from
   PERSON p1
   inner join 
  observation_period op1
   on p1.person_id = op1.person_id
   ,
   #temp_dates t1 
where year(op1.OBSERVATION_PERIOD_START_DATE) <= t1.obs_year
   and year(op1.OBSERVATION_PERIOD_END_DATE) >= t1.obs_year
group by t1.obs_year,
   p1.gender_concept_id,
   floor((t1.obs_year - p1.year_of_birth)/10)
;

TRUNCATE TABLE #temp_dates;
DROP TABLE #temp_dates;


-- 117   Number of persons with at least one day of observation in each year by gender and age decile
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

IF OBJECT_ID('tempdb..#temp_dates', 'U') IS NOT NULL
   DROP TABLE #temp_dates;

select distinct 
  YEAR(observation_period_start_date)*100 + MONTH(observation_period_start_date)  as obs_month
into 
  #temp_dates
from 
  OBSERVATION_PERIOD
;

insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 117 as analysis_id,  
   t1.obs_month as stratum_1,
   count(distinct op1.PERSON_ID) as count_value
from
   observation_period op1,
   #temp_dates t1 
where YEAR(observation_period_start_date)*100 + MONTH(observation_period_start_date) <= t1.obs_month
   and YEAR(observation_period_end_date)*100 + MONTH(observation_period_end_date) >= t1.obs_month
group by t1.obs_month
;

TRUNCATE TABLE #temp_dates;
DROP TABLE #temp_dates;


-- 118  Number of observation period records with invalid person_id
insert into ACHILLES_results (analysis_id, count_value)
select 118 as analysis_id,
  count(op1.PERSON_ID) as count_value
from
  observation_period op1
  left join PERSON p1
  on p1.person_id = op1.person_id
where p1.person_id is null
;


/********************************************
ACHILLES Analyses on VISIT_OCCURRENCE table
*********************************************/

-- 200   Number of persons with at least one visit occurrence, by visit_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 200 as analysis_id, 
   vo1.visit_concept_id as stratum_1,
   count(distinct vo1.PERSON_ID) as count_value
from
   visit_occurrence vo1
group by vo1.visit_concept_id
;

-- 201   Number of visit occurrence records, by visit_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 201 as analysis_id, 
   vo1.visit_concept_id as stratum_1,
   count(vo1.PERSON_ID) as count_value
from
   visit_occurrence vo1
group by vo1.visit_concept_id
;

-- 202   Number of persons by visit occurrence start month, by visit_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 202 as analysis_id,   
   vo1.visit_concept_id as stratum_1,
   YEAR(visit_start_date)*100 + month(visit_start_date) as stratum_2, 
   count(distinct PERSON_ID) as count_value
from
visit_occurrence vo1
group by vo1.visit_concept_id, 
   YEAR(visit_start_date)*100 + month(visit_start_date)
;

-- 203   Number of distinct visit occurrence concepts per person

with rawData as
(
    select vo1.person_id, count(distinct vo1.visit_concept_id) as count_value
      from visit_occurrence vo1
      group by vo1.person_id
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 203 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM #tempResults
;


-- 204   Number of persons with at least one visit occurrence, by visit_concept_id by calendar year by gender by age decile
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
select 204 as analysis_id,   
   vo1.visit_concept_id as stratum_1,
   YEAR(visit_start_date) as stratum_2,
   p1.gender_concept_id as stratum_3,
   floor((year(visit_start_date) - p1.year_of_birth)/10) as stratum_4, 
   count(distinct p1.PERSON_ID) as count_value
from PERSON p1
inner join
visit_occurrence vo1
on p1.person_id = vo1.person_id
group by vo1.visit_concept_id, 
   YEAR(visit_start_date),
   p1.gender_concept_id,
   floor((year(visit_start_date) - p1.year_of_birth)/10)
;

-- 206   Distribution of age by visit_concept_id

with rawData as
(
  select vo1.visit_concept_id,
     p1.gender_concept_id,
      vo1.visit_start_year - p1.year_of_birth as count_value
   from PERSON p1
   inner join 
  (
      select person_id, visit_concept_id, min(year(visit_start_date)) as visit_start_year
      from visit_occurrence
      group by person_id, visit_concept_id
   ) vo1 on p1.person_id = vo1.person_id
),
overallStats as
(
  select stratum1_id,
    stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
   group by stratum1_id, stratum2_id
),
stats as
(
  select stratum1_id, stratum2_id, count_value, count(*) as total, row_number() over (partition by stratum1_id, stratum2_id order by count_value) as rn
  from rawData
  group by stratum1_id, stratum2_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 206 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;

--207   Number of visit records with invalid person_id
insert into ACHILLES_results (analysis_id, count_value)
select 207 as analysis_id,  
   count(vo1.PERSON_ID) as count_value
from
   visit_occurrence vo1
   left join PERSON p1
   on p1.person_id = vo1.person_id
where p1.person_id is null
;


--208   Number of visit records outside valid observation period
insert into ACHILLES_results (analysis_id, count_value)
select 208 as analysis_id,  
   count(vo1.PERSON_ID) as count_value
from
   visit_occurrence vo1
   left join observation_period op1
   on op1.person_id = vo1.person_id
   and vo1.visit_start_date >= op1.observation_period_start_date
   and vo1.visit_start_date <= op1.observation_period_end_date
where op1.person_id is null
;

--209   Number of visit records with end date < start date
insert into ACHILLES_results (analysis_id, count_value)
select 209 as analysis_id,  
   count(vo1.PERSON_ID) as count_value
from
   visit_occurrence vo1
where visit_end_date < visit_start_date
;

--210   Number of visit records with invalid care_site_id
insert into ACHILLES_results (analysis_id, count_value)
select 210 as analysis_id,  
   count(vo1.PERSON_ID) as count_value
from
   visit_occurrence vo1
   left join care_site cs1
   on vo1.care_site_id = cs1.care_site_id
where vo1.care_site_id is not null
   and cs1.care_site_id is null
;


-- 211   Distribution of length of stay by visit_concept_id
with rawData as
(
  select visit_concept_id, datediff(dd,visit_start_date,visit_end_date) as count_value
  from visit_occurrence
),
overallStats as
(
  select stratum_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum_id
),
stats as
(
  select stratum_id, count_value, count(*) as total, row_number() over (order by count_value) as rn
  from rawData
  group by stratum_id, count_value
),
priorStats as
(
  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum_id = p.stratum_id and p.rn <= s.rn
  group by s.stratum_id, s.count_value, s.total, s.rn
)
select 211 as analysis_id,
  o.stratum_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum_id = o.stratum_id
group by o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


-- 220   Number of visit occurrence records by condition occurrence start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 220 as analysis_id,   
   YEAR(visit_start_date)*100 + month(visit_start_date) as stratum_1, 
   count(PERSON_ID) as count_value
from
visit_occurrence vo1
group by YEAR(visit_start_date)*100 + month(visit_start_date)
;


/********************************************
ACHILLES Analyses on PROVIDER table
*********************************************/

-- 300   Number of providers
insert into ACHILLES_results (analysis_id, count_value)
select 300 as analysis_id,  count(distinct provider_id) as count_value
from provider;


-- 301   Number of providers by specialty concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 301 as analysis_id,  specialty_concept_id as stratum_1, count(distinct provider_id) as count_value
from provider
group by specialty_CONCEPT_ID;

-- 302   Number of providers with invalid care site id
insert into ACHILLES_results (analysis_id, count_value)
select 302 as analysis_id,  count(provider_id) as count_value
from provider p1
   left join care_site cs1
   on p1.care_site_id = cs1.care_site_id
where p1.care_site_id is not null
   and cs1.care_site_id is null
;



/********************************************
ACHILLES Analyses on CONDITION_OCCURRENCE table
*********************************************/

-- 400   Number of persons with at least one condition occurrence, by condition_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 400 as analysis_id, 
   co1.condition_CONCEPT_ID as stratum_1,
   count(distinct co1.PERSON_ID) as count_value
from
   condition_occurrence co1
group by co1.condition_CONCEPT_ID
;


-- 401   Number of condition occurrence records, by condition_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 401 as analysis_id, 
   co1.condition_CONCEPT_ID as stratum_1,
   count(co1.PERSON_ID) as count_value
from
   condition_occurrence co1
group by co1.condition_CONCEPT_ID
;


-- 402   Number of persons by condition occurrence start month, by condition_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 402 as analysis_id,   
   co1.condition_concept_id as stratum_1,
   YEAR(condition_start_date)*100 + month(condition_start_date) as stratum_2, 
   count(distinct PERSON_ID) as count_value
from
condition_occurrence co1
group by co1.condition_concept_id, 
   YEAR(condition_start_date)*100 + month(condition_start_date)
;


-- 403   Number of distinct condition occurrence concepts per person
with rawData as
(
  select person_id, count(distinct condition_concept_id) as num_conditions
  from condition_occurrence
   group by person_id
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 403 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


-- 404   Number of persons with at least one condition occurrence, by condition_concept_id by calendar year by gender by age decile
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
select 404 as analysis_id,   
   co1.condition_concept_id as stratum_1,
   YEAR(condition_start_date) as stratum_2,
   p1.gender_concept_id as stratum_3,
   floor((year(condition_start_date) - p1.year_of_birth)/10) as stratum_4, 
   count(distinct p1.PERSON_ID) as count_value
from PERSON p1
inner join
condition_occurrence co1
on p1.person_id = co1.person_id
group by co1.condition_concept_id, 
   YEAR(condition_start_date),
   p1.gender_concept_id,
   floor((year(condition_start_date) - p1.year_of_birth)/10)
;

-- 405   Number of condition occurrence records, by condition_concept_id by condition_type_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 405 as analysis_id, 
   co1.condition_CONCEPT_ID as stratum_1,
   co1.condition_type_concept_id as stratum_2,
   count(co1.PERSON_ID) as count_value
from
   condition_occurrence co1
group by co1.condition_CONCEPT_ID,   
   co1.condition_type_concept_id
;

-- 406   Distribution of age by condition_concept_id
select co1.condition_concept_id as subject_id,
  p1.gender_concept_id,
   (co1.condition_start_year - p1.year_of_birth) as count_value
from PERSON p1
inner join 
(
   select person_id, condition_concept_id, min(year(condition_start_date)) as condition_start_year
   from condition_occurrence
   group by person_id, condition_concept_id
) co1 on p1.person_id = co1.person_id
;

with overallStats as
(
  select subject_id as stratum1_id,
    gender_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_406
   group by subject_id, gender_concept_id
),
stats as
(
  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
  from #rawData_406
  group by subject_id, gender_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 406 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


-- 409   Number of condition occurrence records with invalid person_id
insert into ACHILLES_results (analysis_id, count_value)
select 409 as analysis_id,  
   count(co1.PERSON_ID) as count_value
from
   condition_occurrence co1
   left join PERSON p1
   on p1.person_id = co1.person_id
where p1.person_id is null
;


-- 410   Number of condition occurrence records outside valid observation period
insert into ACHILLES_results (analysis_id, count_value)
select 410 as analysis_id,  
   count(co1.PERSON_ID) as count_value
from
   condition_occurrence co1
   left join observation_period op1
   on op1.person_id = co1.person_id
   and co1.condition_start_date >= op1.observation_period_start_date
   and co1.condition_start_date <= op1.observation_period_end_date
where op1.person_id is null
;


-- 411   Number of condition occurrence records with end date < start date
insert into ACHILLES_results (analysis_id, count_value)
select 411 as analysis_id,  
   count(co1.PERSON_ID) as count_value
from
   condition_occurrence co1
where co1.condition_end_date < co1.condition_start_date
;


-- 412   Number of condition occurrence records with invalid provider_id
insert into ACHILLES_results (analysis_id, count_value)
select 412 as analysis_id,  
   count(co1.PERSON_ID) as count_value
from
   condition_occurrence co1
   left join provider p1
   on p1.provider_id = co1.provider_id
where co1.provider_id is not null
   and p1.provider_id is null
;

-- 413   Number of condition occurrence records with invalid visit_id
insert into ACHILLES_results (analysis_id, count_value)
select 413 as analysis_id,  
   count(co1.PERSON_ID) as count_value
from
   condition_occurrence co1
   left join visit_occurrence vo1
   on co1.visit_occurrence_id = vo1.visit_occurrence_id
where co1.visit_occurrence_id is not null
   and vo1.visit_occurrence_id is null
;

-- 420   Number of condition occurrence records by condition occurrence start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 420 as analysis_id,   
   YEAR(condition_start_date)*100 + month(condition_start_date) as stratum_1, 
   count(PERSON_ID) as count_value
from
condition_occurrence co1
group by YEAR(condition_start_date)*100 + month(condition_start_date)
;


/********************************************
ACHILLES Analyses on DEATH table
*********************************************/

-- 500   Number of persons with death, by cause_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 500 as analysis_id, 
   d1.cause_concept_id as stratum_1,
   count(distinct d1.PERSON_ID) as count_value
from
   death d1
group by d1.cause_concept_id
;


-- 501   Number of records of death, by cause_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 501 as analysis_id, 
   d1.cause_concept_id as stratum_1,
   count(d1.PERSON_ID) as count_value
from
   death d1
group by d1.cause_concept_id
;


-- 502   Number of persons by condition occurrence start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 502 as analysis_id,   
   YEAR(death_date)*100 + month(death_date) as stratum_1, 
   count(distinct PERSON_ID) as count_value
from
death d1
group by YEAR(death_date)*100 + month(death_date)
;


-- 504   Number of persons with a death, by calendar year by gender by age decile
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, count_value)
select 504 as analysis_id,   
   YEAR(death_date) as stratum_1,
   p1.gender_concept_id as stratum_2,
   floor((year(death_date) - p1.year_of_birth)/10) as stratum_3, 
   count(distinct p1.PERSON_ID) as count_value
from PERSON p1
inner join
death d1
on p1.person_id = d1.person_id
group by YEAR(death_date),
   p1.gender_concept_id,
   floor((year(death_date) - p1.year_of_birth)/10)
;

-- 505   Number of death records, by death_type_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 505 as analysis_id, 
   death_type_concept_id as stratum_1,
   count(PERSON_ID) as count_value
from
   death d1
group by death_type_concept_id
;


-- 506   Distribution of age by condition_concept_id

with rawData as
(
  select p1.gender_concept_id,
    d1.death_year - p1.year_of_birth as count_value
  from PERSON p1
  inner join
  (select person_id, min(year(death_date)) as death_year
  from death
  group by person_id
  ) d1
  on p1.person_id = d1.person_id
),
overallStats as
(
  select stratum_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum_id
),
stats as
(
  select stratum_id, count_value, count(*) as total, row_number() over (order by count_value) as rn
  from rawData
  group by stratum_id, count_value
),
priorStats as
(
  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum_id = p.stratum_id and p.rn <= s.rn
  group by s.stratum_id, s.count_value, s.total, s.rn
)
select 506 as analysis_id,
  o.stratum_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum_id = o.stratum_id
group by o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


-- 509   Number of death records with invalid person_id
insert into ACHILLES_results (analysis_id, count_value)
select 509 as analysis_id, 
   count(d1.PERSON_ID) as count_value
from
   death d1
      left join person p1
      on d1.person_id = p1.person_id
where p1.person_id is null
;


-- 510   Number of death records outside valid observation period
insert into ACHILLES_results (analysis_id, count_value)
select 510 as analysis_id, 
   count(d1.PERSON_ID) as count_value
from
   death d1
      left join observation_period op1
      on d1.person_id = op1.person_id
      and d1.death_date >= op1.observation_period_start_date
      and d1.death_date <= op1.observation_period_end_date
where op1.person_id is null
;


-- 511   Distribution of time from death to last condition
insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select 511 as analysis_id,
  count(count_value) as count_value,
   min(count_value) as min_value,
   max(count_value) as max_value,
   avg(1.0*count_value) as avg_value,
   stddev(count_value) as stdev_value,
   max(case when p1<=0.50 then count_value else -9999 end) as median_value,
   max(case when p1<=0.10 then count_value else -9999 end) as p10_value,
   max(case when p1<=0.25 then count_value else -9999 end) as p25_value,
   max(case when p1<=0.75 then count_value else -9999 end) as p75_value,
   max(case when p1<=0.90 then count_value else -9999 end) as p90_value
from
(
select datediff(dd,d1.death_date, t0.max_date) as count_value,
   1.0*(row_number() over (order by datediff(dd,d1.death_date, t0.max_date)))/(count(*) over () + 1) as p1
from death d1
   inner join
   (
      select person_id, max(condition_start_date) as max_date
      from condition_occurrence
      group by person_id
   ) t0 on d1.person_id = t0.person_id
) t1
;


-- 512   Distribution of time from death to last drug
with rawData as
(
  select datediff(dd,d1.death_date, t0.max_date) as count_value
  from death d1
  inner join
   (
      select person_id, max(drug_exposure_start_date) as max_date
      from drug_exposure
      group by person_id
   ) t0
   on d1.person_id = t0.person_id
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 512 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
FROM #tempResults
;


-- 513   Distribution of time from death to last visit
with rawData as
(
  select datediff(dd,d1.death_date, t0.max_date) as count_value
  from death d1
   inner join
   (
      select person_id, max(visit_start_date) as max_date
      from visit_occurrence
      group by person_id
   ) t0
   on d1.person_id = t0.person_id
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 513 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


-- 514   Distribution of time from death to last procedure
with rawData as
(
  select datediff(dd,d1.death_date, t0.max_date) as count_value
  from death d1
   inner join
   (
      select person_id, max(procedure_date) as max_date
      from procedure_occurrence
      group by person_id
   ) t0
   on d1.person_id = t0.person_id
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 514 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


-- 515   Distribution of time from death to last observation
with rawData as
(
  select datediff(dd,d1.death_date, t0.max_date) as count_value
  from death d1
   inner join
   (
      select person_id, max(observation_date) as max_date
      from observation
      group by person_id
   ) t0
   on d1.person_id = t0.person_id
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 515 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


/********************************************
ACHILLES Analyses on PROCEDURE_OCCURRENCE table
*********************************************/

-- 600   Number of persons with at least one procedure occurrence, by procedure_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 600 as analysis_id, 
   po1.procedure_CONCEPT_ID as stratum_1,
   count(distinct po1.PERSON_ID) as count_value
from
   procedure_occurrence po1
group by po1.procedure_CONCEPT_ID
;


-- 601   Number of procedure occurrence records, by procedure_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 601 as analysis_id, 
   po1.procedure_CONCEPT_ID as stratum_1,
   count(po1.PERSON_ID) as count_value
from
   procedure_occurrence po1
group by po1.procedure_CONCEPT_ID
;


-- 602   Number of persons by procedure occurrence start month, by procedure_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 602 as analysis_id,   
   po1.procedure_concept_id as stratum_1,
   YEAR(procedure_date)*100 + month(procedure_date) as stratum_2, 
   count(distinct PERSON_ID) as count_value
from
procedure_occurrence po1
group by po1.procedure_concept_id, 
   YEAR(procedure_date)*100 + month(procedure_date)
;


-- 603   Number of distinct procedure occurrence concepts per person
with rawData as
(
  select count(distinct po.procedure_concept_id) as num_procedures
   from procedure_occurrence po
   group by po.person_id
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 603 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;






-- 604   Number of persons with at least one procedure occurrence, by procedure_concept_id by calendar year by gender by age decile
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
select 604 as analysis_id,   
   po1.procedure_concept_id as stratum_1,
   YEAR(procedure_date) as stratum_2,
   p1.gender_concept_id as stratum_3,
   floor((year(procedure_date) - p1.year_of_birth)/10) as stratum_4, 
   count(distinct p1.PERSON_ID) as count_value
from PERSON p1
inner join
procedure_occurrence po1
on p1.person_id = po1.person_id
group by po1.procedure_concept_id, 
   YEAR(procedure_date),
   p1.gender_concept_id,
   floor((year(procedure_date) - p1.year_of_birth)/10)
;

-- 605   Number of procedure occurrence records, by procedure_concept_id by procedure_type_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 605 as analysis_id, 
   po1.procedure_CONCEPT_ID as stratum_1,
   po1.procedure_type_concept_id as stratum_2,
   count(po1.PERSON_ID) as count_value
from
   procedure_occurrence po1
group by po1.procedure_CONCEPT_ID,   
   po1.procedure_type_concept_id
;



-- 606   Distribution of age by procedure_concept_id
select po1.procedure_concept_id as subject_id,
  p1.gender_concept_id,
   po1.procedure_start_year - p1.year_of_birth as count_value
from PERSON p1
inner join
(
   select person_id, procedure_concept_id, min(year(procedure_date)) as procedure_start_year
   from procedure_occurrence
   group by person_id, procedure_concept_id
) po1 on p1.person_id = po1.person_id
;

with overallStats as
(
  select subject_id as stratum1_id,
    gender_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_606
   group by subject_id, gender_concept_id
),
stats as
(
  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
  from #rawData_606
  group by subject_id, gender_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 606 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;



-- 609   Number of procedure occurrence records with invalid person_id
insert into ACHILLES_results (analysis_id, count_value)
select 609 as analysis_id,  
   count(po1.PERSON_ID) as count_value
from
   procedure_occurrence po1
   left join PERSON p1
   on p1.person_id = po1.person_id
where p1.person_id is null
;


-- 610   Number of procedure occurrence records outside valid observation period
insert into ACHILLES_results (analysis_id, count_value)
select 610 as analysis_id,  
   count(po1.PERSON_ID) as count_value
from
   procedure_occurrence po1
   left join observation_period op1
   on op1.person_id = po1.person_id
   and po1.procedure_date >= op1.observation_period_start_date
   and po1.procedure_date <= op1.observation_period_end_date
where op1.person_id is null
;



-- 612   Number of procedure occurrence records with invalid provider_id
insert into ACHILLES_results (analysis_id, count_value)
select 612 as analysis_id,  
   count(po1.PERSON_ID) as count_value
from
   procedure_occurrence po1
   left join provider p1
   on p1.provider_id = po1.provider_id
where po1.provider_id is not null
   and p1.provider_id is null
;

-- 613   Number of procedure occurrence records with invalid visit_id
insert into ACHILLES_results (analysis_id, count_value)
select 613 as analysis_id,  
   count(po1.PERSON_ID) as count_value
from
   procedure_occurrence po1
   left join visit_occurrence vo1
   on po1.visit_occurrence_id = vo1.visit_occurrence_id
where po1.visit_occurrence_id is not null
   and vo1.visit_occurrence_id is null
;


-- 620   Number of procedure occurrence records by condition occurrence start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 620 as analysis_id,   
   YEAR(procedure_date)*100 + month(procedure_date) as stratum_1, 
   count(PERSON_ID) as count_value
from
procedure_occurrence po1
group by YEAR(procedure_date)*100 + month(procedure_date)
;


/********************************************

ACHILLES Analyses on DRUG_EXPOSURE table

*********************************************/




-- 700   Number of persons with at least one drug occurrence, by drug_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 700 as analysis_id, 
   de1.drug_CONCEPT_ID as stratum_1,
   count(distinct de1.PERSON_ID) as count_value
from
   drug_exposure de1
group by de1.drug_CONCEPT_ID
;


-- 701   Number of drug occurrence records, by drug_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 701 as analysis_id, 
   de1.drug_CONCEPT_ID as stratum_1,
   count(de1.PERSON_ID) as count_value
from
   drug_exposure de1
group by de1.drug_CONCEPT_ID
;



-- 702   Number of persons by drug occurrence start month, by drug_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 702 as analysis_id,   
   de1.drug_concept_id as stratum_1,
   YEAR(drug_exposure_start_date)*100 + month(drug_exposure_start_date) as stratum_2, 
   count(distinct PERSON_ID) as count_value
from
drug_exposure de1
group by de1.drug_concept_id, 
   YEAR(drug_exposure_start_date)*100 + month(drug_exposure_start_date)
;



-- 703   Number of distinct drug exposure concepts per person
with rawData as
(
  select num_drugs as count_value
   from
   (
      select de1.person_id, count(distinct de1.drug_concept_id) as num_drugs
      from
      drug_exposure de1
      group by de1.person_id
   ) t0
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 703 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 704   Number of persons with at least one drug occurrence, by drug_concept_id by calendar year by gender by age decile
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
select 704 as analysis_id,   
   de1.drug_concept_id as stratum_1,
   YEAR(drug_exposure_start_date) as stratum_2,
   p1.gender_concept_id as stratum_3,
   floor((year(drug_exposure_start_date) - p1.year_of_birth)/10) as stratum_4, 
   count(distinct p1.PERSON_ID) as count_value
from PERSON p1
inner join
drug_exposure de1
on p1.person_id = de1.person_id
group by de1.drug_concept_id, 
   YEAR(drug_exposure_start_date),
   p1.gender_concept_id,
   floor((year(drug_exposure_start_date) - p1.year_of_birth)/10)
;

-- 705   Number of drug occurrence records, by drug_concept_id by drug_type_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 705 as analysis_id, 
   de1.drug_CONCEPT_ID as stratum_1,
   de1.drug_type_concept_id as stratum_2,
   count(de1.PERSON_ID) as count_value
from
   drug_exposure de1
group by de1.drug_CONCEPT_ID,   
   de1.drug_type_concept_id
;



-- 706   Distribution of age by drug_concept_id
select de1.drug_concept_id as subject_id,
  p1.gender_concept_id,
   de1.drug_start_year - p1.year_of_birth as count_value
from PERSON p1
inner join
(
   select person_id, drug_concept_id, min(year(drug_exposure_start_date)) as drug_start_year
   from drug_exposure
   group by person_id, drug_concept_id
) de1 on p1.person_id = de1.person_id
;

with overallStats as
(
  select subject_id as stratum1_id,
    gender_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_706
   group by subject_id, gender_concept_id
),
stats as
(
  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
  from #rawData_706
  group by subject_id, gender_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 706 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;







-- 709   Number of drug exposure records with invalid person_id
insert into ACHILLES_results (analysis_id, count_value)
select 709 as analysis_id,  
   count(de1.PERSON_ID) as count_value
from
   drug_exposure de1
   left join PERSON p1
   on p1.person_id = de1.person_id
where p1.person_id is null
;


-- 710   Number of drug exposure records outside valid observation period
insert into ACHILLES_results (analysis_id, count_value)
select 710 as analysis_id,  
   count(de1.PERSON_ID) as count_value
from
   drug_exposure de1
   left join observation_period op1
   on op1.person_id = de1.person_id
   and de1.drug_exposure_start_date >= op1.observation_period_start_date
   and de1.drug_exposure_start_date <= op1.observation_period_end_date
where op1.person_id is null
;


-- 711   Number of drug exposure records with end date < start date
insert into ACHILLES_results (analysis_id, count_value)
select 711 as analysis_id,  
   count(de1.PERSON_ID) as count_value
from
   drug_exposure de1
where de1.drug_exposure_end_date < de1.drug_exposure_start_date
;


-- 712   Number of drug exposure records with invalid provider_id
insert into ACHILLES_results (analysis_id, count_value)
select 712 as analysis_id,  
   count(de1.PERSON_ID) as count_value
from
   drug_exposure de1
   left join provider p1
   on p1.provider_id = de1.provider_id
where de1.provider_id is not null
   and p1.provider_id is null
;

-- 713   Number of drug exposure records with invalid visit_id
insert into ACHILLES_results (analysis_id, count_value)
select 713 as analysis_id,  
   count(de1.PERSON_ID) as count_value
from
   drug_exposure de1
   left join visit_occurrence vo1
   on de1.visit_occurrence_id = vo1.visit_occurrence_id
where de1.visit_occurrence_id is not null
   and vo1.visit_occurrence_id is null
;



-- 715   Distribution of days_supply by drug_concept_id
with rawData as
(
  select drug_concept_id,
      days_supply as count_value
   from drug_exposure 
   where days_supply is not null
),
overallStats as
(
  select stratum_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
   group by stratum_id
),
stats as
(
  select stratum_id, count_value, count(*) as total, row_number() over (order by count_value) as rn
  from rawData
  group by stratum_id, count_value
),
priorStats as
(
  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum_id = p.stratum_id and p.rn <= s.rn
  group by s.stratum_id, s.count_value, s.total, s.rn
)
select 715 as analysis_id,
  o.stratum_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum_id = o.stratum_id
group by o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;




-- 716   Distribution of refills by drug_concept_id
with rawData as
(
  select drug_concept_id,
    refills as count_value
   from drug_exposure 
   where refills is not null
),
overallStats as
(
  select stratum_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
   group by stratum_id
),
stats as
(
  select stratum_id, count_value, count(*) as total, row_number() over (order by count_value) as rn
  from rawData
  group by stratum_id, count_value
),
priorStats as
(
  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum_id = p.stratum_id and p.rn <= s.rn
  group by s.stratum_id, s.count_value, s.total, s.rn
)
select 716 as analysis_id,
  o.stratum_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum_id = o.stratum_id
group by o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 717   Distribution of quantity by drug_concept_id
with rawData as
(
  select drug_concept_id,
    quantity as count_value
  from drug_exposure 
   where quantity is not null
),
overallStats as
(
  select stratum_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
   group by stratum_id
),
stats as
(
  select stratum_id, count_value, count(*) as total, row_number() over (order by count_value) as rn
  from rawData
  group by stratum_id, count_value
),
priorStats as
(
  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum_id = p.stratum_id and p.rn <= s.rn
  group by s.stratum_id, s.count_value, s.total, s.rn
)
select 717 as analysis_id,
  o.stratum_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum_id = o.stratum_id
group by o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 720   Number of drug exposure records by condition occurrence start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 720 as analysis_id,   
   YEAR(drug_exposure_start_date)*100 + month(drug_exposure_start_date) as stratum_1, 
   count(PERSON_ID) as count_value
from
drug_exposure de1
group by YEAR(drug_exposure_start_date)*100 + month(drug_exposure_start_date)
;

/********************************************

ACHILLES Analyses on OBSERVATION table

*********************************************/



-- 800   Number of persons with at least one observation occurrence, by observation_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 800 as analysis_id, 
   o1.observation_CONCEPT_ID as stratum_1,
   count(distinct o1.PERSON_ID) as count_value
from
   observation o1
group by o1.observation_CONCEPT_ID
;


-- 801   Number of observation occurrence records, by observation_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 801 as analysis_id, 
   o1.observation_CONCEPT_ID as stratum_1,
   count(o1.PERSON_ID) as count_value
from
   observation o1
group by o1.observation_CONCEPT_ID
;



-- 802   Number of persons by observation occurrence start month, by observation_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 802 as analysis_id,   
   o1.observation_concept_id as stratum_1,
   YEAR(observation_date)*100 + month(observation_date) as stratum_2, 
   count(distinct PERSON_ID) as count_value
from
observation o1
group by o1.observation_concept_id, 
   YEAR(observation_date)*100 + month(observation_date)
;



-- 803   Number of distinct observation occurrence concepts per person
with rawData as
(
  select num_observations as count_value
  from
   (
     select o1.person_id, count(distinct o1.observation_concept_id) as num_observations
     from
     observation o1
     group by o1.person_id
   ) t0
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 803 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;







-- 804   Number of persons with at least one observation occurrence, by observation_concept_id by calendar year by gender by age decile
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
select 804 as analysis_id,   
   o1.observation_concept_id as stratum_1,
   YEAR(observation_date) as stratum_2,
   p1.gender_concept_id as stratum_3,
   floor((year(observation_date) - p1.year_of_birth)/10) as stratum_4, 
   count(distinct p1.PERSON_ID) as count_value
from PERSON p1
inner join
observation o1
on p1.person_id = o1.person_id
group by o1.observation_concept_id, 
   YEAR(observation_date),
   p1.gender_concept_id,
   floor((year(observation_date) - p1.year_of_birth)/10)
;

-- 805   Number of observation occurrence records, by observation_concept_id by observation_type_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 805 as analysis_id, 
   o1.observation_CONCEPT_ID as stratum_1,
   o1.observation_type_concept_id as stratum_2,
   count(o1.PERSON_ID) as count_value
from
   observation o1
group by o1.observation_CONCEPT_ID,   
   o1.observation_type_concept_id
;



-- 806   Distribution of age by observation_concept_id
select o1.observation_concept_id as subject_id,
  p1.gender_concept_id,
   o1.observation_start_year - p1.year_of_birth as count_value
from PERSON p1
inner join
(
   select person_id, observation_concept_id, min(year(observation_date)) as observation_start_year
   from observation
   group by person_id, observation_concept_id
) o1
on p1.person_id = o1.person_id
;

with overallStats as
(
  select subject_id as stratum1_id,
    gender_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_806
   group by subject_id, gender_concept_id
),
stats as
(
  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
  from #rawData_806
  group by subject_id, gender_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 806 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 807   Number of observation occurrence records, by observation_concept_id and unit_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 807 as analysis_id, 
   o1.observation_CONCEPT_ID as stratum_1,
   o1.unit_concept_id as stratum_2,
   count(o1.PERSON_ID) as count_value
from
   observation o1
group by o1.observation_CONCEPT_ID,
   o1.unit_concept_id
;





-- 809   Number of observation records with invalid person_id
insert into ACHILLES_results (analysis_id, count_value)
select 809 as analysis_id,  
   count(o1.PERSON_ID) as count_value
from
   observation o1
   left join PERSON p1
   on p1.person_id = o1.person_id
where p1.person_id is null
;


-- 810   Number of observation records outside valid observation period
insert into ACHILLES_results (analysis_id, count_value)
select 810 as analysis_id,  
   count(o1.PERSON_ID) as count_value
from
   observation o1
   left join observation_period op1
   on op1.person_id = o1.person_id
   and o1.observation_date >= op1.observation_period_start_date
   and o1.observation_date <= op1.observation_period_end_date
where op1.person_id is null
;



-- 812   Number of observation records with invalid provider_id
insert into ACHILLES_results (analysis_id, count_value)
select 812 as analysis_id,  
   count(o1.PERSON_ID) as count_value
from
   observation o1
   left join provider p1
   on p1.provider_id = o1.provider_id
where o1.provider_id is not null
   and p1.provider_id is null
;

-- 813   Number of observation records with invalid visit_id
insert into ACHILLES_results (analysis_id, count_value)
select 813 as analysis_id,  
   count(o1.PERSON_ID) as count_value
from
   observation o1
   left join visit_occurrence vo1
   on o1.visit_occurrence_id = vo1.visit_occurrence_id
where o1.visit_occurrence_id is not null
   and vo1.visit_occurrence_id is null
;


-- 814   Number of observation records with no value (numeric, string, or concept)
insert into ACHILLES_results (analysis_id, count_value)
select 814 as analysis_id,  
   count(o1.PERSON_ID) as count_value
from
   observation o1
where o1.value_as_number is null
   and o1.value_as_string is null
   and o1.value_as_concept_id is null
;


-- 815  Distribution of numeric values, by observation_concept_id and unit_concept_id
select observation_concept_id as subject_id, 
   unit_concept_id,
   value_as_number as count_value
from observation o1
where o1.unit_concept_id is not null
   and o1.value_as_number is not null
;

with overallStats as
(
  select subject_id as stratum1_id,
    unit_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_815
   group by subject_id, unit_concept_id
),
stats as
(
  select subject_id as stratum1_id, unit_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, unit_concept_id order by count_value) as rn
  from #rawData_815
  group by subject_id, unit_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 815 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 816   Distribution of low range, by observation_concept_id and unit_concept_id

--NOT APPLICABLE FOR OMOP CDM v5



-- 817   Distribution of high range, by observation_concept_id and unit_concept_id

--NOT APPLICABLE FOR OMOP CDM v5




-- 818   Number of observation records below/within/above normal range, by observation_concept_id and unit_concept_id

--NOT APPLICABLE FOR OMOP CDM v5




-- 820   Number of observation records by condition occurrence start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 820 as analysis_id,   
   YEAR(observation_date)*100 + month(observation_date) as stratum_1, 
   count(PERSON_ID) as count_value
from
observation o1
group by YEAR(observation_date)*100 + month(observation_date)
;




/********************************************

ACHILLES Analyses on DRUG_ERA table

*********************************************/


-- 900   Number of persons with at least one drug occurrence, by drug_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 900 as analysis_id, 
   de1.drug_CONCEPT_ID as stratum_1,
   count(distinct de1.PERSON_ID) as count_value
from
   drug_era de1
group by de1.drug_CONCEPT_ID
;


-- 901   Number of drug occurrence records, by drug_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 901 as analysis_id, 
   de1.drug_CONCEPT_ID as stratum_1,
   count(de1.PERSON_ID) as count_value
from
   drug_era de1
group by de1.drug_CONCEPT_ID
;



-- 902   Number of persons by drug occurrence start month, by drug_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 902 as analysis_id,   
   de1.drug_concept_id as stratum_1,
   YEAR(drug_era_start_date)*100 + month(drug_era_start_date) as stratum_2, 
   count(distinct PERSON_ID) as count_value
from
drug_era de1
group by de1.drug_concept_id, 
   YEAR(drug_era_start_date)*100 + month(drug_era_start_date)
;



-- 903   Number of distinct drug era concepts per person
with rawData as
(
  select count(distinct de1.drug_concept_id) as count_value
   from drug_era de1
   group by de1.person_id
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 903 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;






-- 904   Number of persons with at least one drug occurrence, by drug_concept_id by calendar year by gender by age decile
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
select 904 as analysis_id,   
   de1.drug_concept_id as stratum_1,
   YEAR(drug_era_start_date) as stratum_2,
   p1.gender_concept_id as stratum_3,
   floor((year(drug_era_start_date) - p1.year_of_birth)/10) as stratum_4, 
   count(distinct p1.PERSON_ID) as count_value
from PERSON p1
inner join
drug_era de1
on p1.person_id = de1.person_id
group by de1.drug_concept_id, 
   YEAR(drug_era_start_date),
   p1.gender_concept_id,
   floor((year(drug_era_start_date) - p1.year_of_birth)/10)
;




-- 906   Distribution of age by drug_concept_id
select de.drug_concept_id as subject_id,
  p1.gender_concept_id,
  de.drug_start_year - p1.year_of_birth as count_value
from PERSON p1
inner join
(
   select person_id, drug_concept_id, min(year(drug_era_start_date)) as drug_start_year
   from drug_era
   group by person_id, drug_concept_id
) de on p1.person_id =de.person_id
;

with overallStats as
(
  select subject_id as stratum1_id,
    gender_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_906
   group by subject_id, gender_concept_id
),
stats as
(
  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
  from #rawData_906
  group by subject_id, gender_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 906 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 907   Distribution of drug era length, by drug_concept_id
with rawData as
(
  select drug_concept_id,
    datediff(dd,drug_era_start_date, drug_era_end_date) as count_value
  from  drug_era de1
),
overallStats as
(
  select stratum1_id, 
     avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
      count_value, 
     count(*) as total, 
      row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 907 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 908   Number of drug eras with invalid person
insert into ACHILLES_results (analysis_id, count_value)
select 908 as analysis_id,  
   count(de1.PERSON_ID) as count_value
from
   drug_era de1
   left join PERSON p1
   on p1.person_id = de1.person_id
where p1.person_id is null
;


-- 909   Number of drug eras outside valid observation period
insert into ACHILLES_results (analysis_id, count_value)
select 909 as analysis_id,  
   count(de1.PERSON_ID) as count_value
from
   drug_era de1
   left join observation_period op1
   on op1.person_id = de1.person_id
   and de1.drug_era_start_date >= op1.observation_period_start_date
   and de1.drug_era_start_date <= op1.observation_period_end_date
where op1.person_id is null
;


-- 910   Number of drug eras with end date < start date
insert into ACHILLES_results (analysis_id, count_value)
select 910 as analysis_id,  
   count(de1.PERSON_ID) as count_value
from
   drug_era de1
where de1.drug_era_end_date < de1.drug_era_start_date
;



-- 920   Number of drug era records by drug era start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 920 as analysis_id,   
   YEAR(drug_era_start_date)*100 + month(drug_era_start_date) as stratum_1, 
   count(PERSON_ID) as count_value
from
drug_era de1
group by YEAR(drug_era_start_date)*100 + month(drug_era_start_date)
;





/********************************************

ACHILLES Analyses on CONDITION_ERA table

*********************************************/


-- 1000   Number of persons with at least one condition occurrence, by condition_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1000 as analysis_id, 
   ce1.condition_CONCEPT_ID as stratum_1,
   count(distinct ce1.PERSON_ID) as count_value
from
   condition_era ce1
group by ce1.condition_CONCEPT_ID
;


-- 1001   Number of condition occurrence records, by condition_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1001 as analysis_id, 
   ce1.condition_CONCEPT_ID as stratum_1,
   count(ce1.PERSON_ID) as count_value
from
   condition_era ce1
group by ce1.condition_CONCEPT_ID
;



-- 1002   Number of persons by condition occurrence start month, by condition_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 1002 as analysis_id,   
   ce1.condition_concept_id as stratum_1,
   YEAR(condition_era_start_date)*100 + month(condition_era_start_date) as stratum_2, 
   count(distinct PERSON_ID) as count_value
from
condition_era ce1
group by ce1.condition_concept_id, 
   YEAR(condition_era_start_date)*100 + month(condition_era_start_date)
;



-- 1003   Number of distinct condition era concepts per person
with rawData as
(
  select count(distinct ce1.condition_concept_id) as count_value
   from condition_era ce1
   group by ce1.person_id
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 1003 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 1004   Number of persons with at least one condition occurrence, by condition_concept_id by calendar year by gender by age decile
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
select 1004 as analysis_id,   
   ce1.condition_concept_id as stratum_1,
   YEAR(condition_era_start_date) as stratum_2,
   p1.gender_concept_id as stratum_3,
   floor((year(condition_era_start_date) - p1.year_of_birth)/10) as stratum_4, 
   count(distinct p1.PERSON_ID) as count_value
from PERSON p1
inner join
condition_era ce1
on p1.person_id = ce1.person_id
group by ce1.condition_concept_id, 
   YEAR(condition_era_start_date),
   p1.gender_concept_id,
   floor((year(condition_era_start_date) - p1.year_of_birth)/10)
;




-- 1006   Distribution of age by condition_concept_id
select ce.condition_concept_id as subject_id,
  p1.gender_concept_id,
  ce.condition_start_year - p1.year_of_birth as count_value
from PERSON p1
inner join
(
  select person_id, condition_concept_id, min(year(condition_era_start_date)) as condition_start_year
  from condition_era
  group by person_id, condition_concept_id
) ce on p1.person_id = ce.person_id
;

with overallStats as
(
  select subject_id as stratum1_id,
    gender_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_1006
   group by subject_id, gender_concept_id
),
stats as
(
  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
  from #rawData_1006
  group by subject_id, gender_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 1006 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;






-- 1007   Distribution of condition era length, by condition_concept_id
with rawData as
(
  select condition_concept_id as stratum1_id,
    datediff(dd,condition_era_start_date, condition_era_end_date) as count_value
  from  condition_era ce1
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
      count_value, 
     count(*) as total, 
      row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1007 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;






-- 1008   Number of condition eras with invalid person
insert into ACHILLES_results (analysis_id, count_value)
select 1008 as analysis_id,  
   count(ce1.PERSON_ID) as count_value
from
   condition_era ce1
   left join PERSON p1
   on p1.person_id = ce1.person_id
where p1.person_id is null
;


-- 1009   Number of condition eras outside valid observation period
insert into ACHILLES_results (analysis_id, count_value)
select 1009 as analysis_id,  
   count(ce1.PERSON_ID) as count_value
from
   condition_era ce1
   left join observation_period op1
   on op1.person_id = ce1.person_id
   and ce1.condition_era_start_date >= op1.observation_period_start_date
   and ce1.condition_era_start_date <= op1.observation_period_end_date
where op1.person_id is null
;


-- 1010   Number of condition eras with end date < start date
insert into ACHILLES_results (analysis_id, count_value)
select 1010 as analysis_id,  
   count(ce1.PERSON_ID) as count_value
from
   condition_era ce1
where ce1.condition_era_end_date < ce1.condition_era_start_date
;


-- 1020   Number of drug era records by drug era start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1020 as analysis_id,   
   YEAR(condition_era_start_date)*100 + month(condition_era_start_date) as stratum_1, 
   count(PERSON_ID) as count_value
from
condition_era ce1
group by YEAR(condition_era_start_date)*100 + month(condition_era_start_date)
;




/********************************************

ACHILLES Analyses on LOCATION table

*********************************************/

-- 1100   Number of persons by location 3-digit zip
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1100 as analysis_id,  
   left(l1.zip,3) as stratum_1, count(distinct person_id) as count_value
from PERSON p1
   inner join LOCATION l1
   on p1.location_id = l1.location_id
where p1.location_id is not null
   and l1.zip is not null
group by left(l1.zip,3);


-- 1101   Number of persons by location state
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1101 as analysis_id,  
   l1.state as stratum_1, count(distinct person_id) as count_value
from PERSON p1
   inner join LOCATION l1
   on p1.location_id = l1.location_id
where p1.location_id is not null
   and l1.state is not null
group by l1.state;


-- 1102   Number of care sites by location 3-digit zip
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1102 as analysis_id,  
   left(l1.zip,3) as stratum_1, count(distinct care_site_id) as count_value
from care_site cs1
   inner join LOCATION l1
   on cs1.location_id = l1.location_id
where cs1.location_id is not null
   and l1.zip is not null
group by left(l1.zip,3);


-- 1103   Number of care sites by location state
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1103 as analysis_id,  
   l1.state as stratum_1, count(distinct care_site_id) as count_value
from care_site cs1
   inner join LOCATION l1
   on cs1.location_id = l1.location_id
where cs1.location_id is not null
   and l1.state is not null
group by l1.state;


/********************************************

ACHILLES Analyses on CARE_SITE table

*********************************************/


-- 1200   Number of persons by place of service
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1200 as analysis_id,  
   cs1.place_of_service_concept_id as stratum_1, count(person_id) as count_value
from PERSON p1
   inner join care_site cs1
   on p1.care_site_id = cs1.care_site_id
where p1.care_site_id is not null
   and cs1.place_of_service_concept_id is not null
group by cs1.place_of_service_concept_id;


-- 1201   Number of visits by place of service
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1201 as analysis_id,  
   cs1.place_of_service_concept_id as stratum_1, count(visit_occurrence_id) as count_value
from visit_occurrence vo1
   inner join care_site cs1
   on vo1.care_site_id = cs1.care_site_id
where vo1.care_site_id is not null
   and cs1.place_of_service_concept_id is not null
group by cs1.place_of_service_concept_id;


-- 1202   Number of care sites by place of service
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1202 as analysis_id,  
   cs1.place_of_service_concept_id as stratum_1, 
   count(care_site_id) as count_value
from care_site cs1
where cs1.place_of_service_concept_id is not null
group by cs1.place_of_service_concept_id;


/********************************************

ACHILLES Analyses on ORGANIZATION table

*********************************************/

-- 1300   Number of organizations by place of service

--NOT APPLICABLE IN CDMv5






/********************************************

ACHILLES Analyses on PAYOR_PLAN_PERIOD table

*********************************************/


-- 1406   Length of payer plan (days) of first payer plan period by gender
with rawData as
(
  select p1.gender_concept_id as stratum1_id,
    DATEDIFF(dd,ppp1.payer_plan_period_start_date, ppp1.payer_plan_period_end_date) as count_value
  from PERSON p1
   inner join 
   (select person_id, 
      payer_plan_period_START_DATE, 
      payer_plan_period_END_DATE, 
      ROW_NUMBER() over (PARTITION by person_id order by payer_plan_period_start_date asc) as rn1
       from payer_plan_period
   ) ppp1
   on p1.PERSON_ID = ppp1.PERSON_ID
   where ppp1.rn1 = 1
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
     count_value, 
     count(*) as total, 
      row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1406 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;






-- 1407   Length of payer plan (days) of first payer plan period by age decile
with rawData as
(
  select floor((year(ppp1.payer_plan_period_START_DATE) - p1.YEAR_OF_BIRTH)/10) as stratum_id,
    DATEDIFF(dd,ppp1.payer_plan_period_start_date, ppp1.payer_plan_period_end_date) as count_value
  from PERSON p1
   inner join 
   (select person_id, 
      payer_plan_period_START_DATE, 
      payer_plan_period_END_DATE, 
      ROW_NUMBER() over (PARTITION by person_id order by payer_plan_period_start_date asc) as rn1
       from payer_plan_period
   ) ppp1
   on p1.PERSON_ID = ppp1.PERSON_ID
   where ppp1.rn1 = 1
),
overallStats as
(
  select stratum_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum_id
),
stats as
(
  select stratum_id, count_value, count(*) as total, row_number() over (order by count_value) as rn
  from rawData
  group by stratum_id, count_value
),
priorStats as
(
  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum_id = p.stratum_id and p.rn <= s.rn
  group by s.stratum_id, s.count_value, s.total, s.rn
)
select 1407 as analysis_id,
  o.stratum_id,
  o.total as count_value,
  o.min_value,
  o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum_id = o.stratum_id
group by o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;







-- 1408   Number of persons by length of payer plan period, in 30d increments
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1408 as analysis_id,  
   floor(DATEDIFF(dd, ppp1.payer_plan_period_start_date, ppp1.payer_plan_period_end_date)/30) as stratum_1, 
   count(distinct p1.person_id) as count_value
from PERSON p1
   inner join 
   (select person_id, 
      payer_plan_period_START_DATE, 
      payer_plan_period_END_DATE, 
      ROW_NUMBER() over (PARTITION by person_id order by payer_plan_period_start_date asc) as rn1
       from payer_plan_period
   ) ppp1
   on p1.PERSON_ID = ppp1.PERSON_ID
   where ppp1.rn1 = 1
group by floor(DATEDIFF(dd, ppp1.payer_plan_period_start_date, ppp1.payer_plan_period_end_date)/30)
;


-- 1409   Number of persons with continuous payer plan in each year
-- Note: using temp table instead of nested query because this gives vastly improved

IF OBJECT_ID('tempdb..#temp_dates', 'U') IS NOT NULL
   DROP TABLE #temp_dates;

select distinct 
  YEAR(payer_plan_period_start_date) as obs_year 
INTO
  #temp_dates
from 
  payer_plan_period
;

insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1409 as analysis_id,  
   t1.obs_year as stratum_1, count(distinct p1.PERSON_ID) as count_value
from
   PERSON p1
   inner join 
    payer_plan_period ppp1
   on p1.person_id = ppp1.person_id
   ,
   #temp_dates t1 
where year(ppp1.payer_plan_period_START_DATE) <= t1.obs_year
   and year(ppp1.payer_plan_period_END_DATE) >= t1.obs_year
group by t1.obs_year
;



-- 1410   Number of persons with continuous payer plan in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

IF OBJECT_ID('tempdb..#temp_dates', 'U') IS NOT NULL
   DROP TABLE #temp_dates;

SELECT DISTINCT 
  YEAR(payer_plan_period_start_date)*100 + MONTH(payer_plan_period_start_date) AS obs_month,
  CAST(CAST(YEAR(payer_plan_period_start_date) AS VARCHAR(4)) +  RIGHT('0' + CAST(MONTH(payer_plan_period_start_date) AS VARCHAR(2)), 2) + '01' AS DATE) AS obs_month_start,  
  DATEADD(dd,-1,DATEADD(mm,1,CAST(CAST(YEAR(payer_plan_period_start_date) AS VARCHAR(4)) +  RIGHT('0' + CAST(MONTH(payer_plan_period_start_date) AS VARCHAR(2)), 2) + '01' AS DATE))) AS obs_month_end
INTO
  #temp_dates
FROM 
  payer_plan_period
;

insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 
  1410 as analysis_id, 
   obs_month as stratum_1, 
   count(distinct p1.PERSON_ID) as count_value
from
   PERSON p1
   inner join 
  payer_plan_period ppp1
   on p1.person_id = ppp1.person_id
   ,
   #temp_dates
where ppp1.payer_plan_period_START_DATE <= obs_month_start
   and ppp1.payer_plan_period_END_DATE >= obs_month_end
group by obs_month
;

TRUNCATE TABLE #temp_dates;
DROP TABLE #temp_dates;



-- 1411   Number of persons by payer plan period start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1411 as analysis_id, 
   cast(cast(YEAR(payer_plan_period_start_date) as varchar(4)) +  RIGHT('0' + CAST(month(payer_plan_period_START_DATE) AS VARCHAR(2)), 2) + '01' as date) as stratum_1,
    count(distinct p1.PERSON_ID) as count_value
from
   PERSON p1
   inner join payer_plan_period ppp1
   on p1.person_id = ppp1.person_id
group by cast(cast(YEAR(payer_plan_period_start_date) as varchar(4)) +  RIGHT('0' + CAST(month(payer_plan_period_START_DATE) AS VARCHAR(2)), 2) + '01' as date)
;



-- 1412   Number of persons by payer plan period end month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1412 as analysis_id,  
   cast(cast(YEAR(payer_plan_period_end_date) as varchar(4)) +  RIGHT('0' + CAST(month(payer_plan_period_end_DATE) AS VARCHAR(2)), 2) + '01' as date) as stratum_1, 
   count(distinct p1.PERSON_ID) as count_value
from
   PERSON p1
   inner join payer_plan_period ppp1
   on p1.person_id = ppp1.person_id
group by cast(cast(YEAR(payer_plan_period_end_date) as varchar(4)) +  RIGHT('0' + CAST(month(payer_plan_period_end_DATE) AS VARCHAR(2)), 2) + '01' as date)
;


-- 1413   Number of persons by number of payer plan periods
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1413 as analysis_id,  
   ppp1.num_periods as stratum_1, 
   count(distinct p1.PERSON_ID) as count_value
from
   PERSON p1
   inner join (select person_id, count(payer_plan_period_start_date) as num_periods from payer_plan_period group by PERSON_ID) ppp1
   on p1.person_id = ppp1.person_id
group by ppp1.num_periods
;

-- 1414   Number of persons with payer plan period before year-of-birth
insert into ACHILLES_results (analysis_id, count_value)
select 1414 as analysis_id,  
   count(distinct p1.PERSON_ID) as count_value
from
   PERSON p1
   inner join (select person_id,min(year(payer_plan_period_start_date)) as first_obs_year from payer_plan_period group by PERSON_ID) ppp1
   on p1.person_id = ppp1.person_id
where p1.year_of_birth > ppp1.first_obs_year
;

-- 1415   Number of persons with payer plan period end < start
insert into ACHILLES_results (analysis_id, count_value)
select 1415 as analysis_id,  
   count(ppp1.PERSON_ID) as count_value
from
   payer_plan_period ppp1
where ppp1.payer_plan_period_end_date < ppp1.payer_plan_period_start_date
;






/********************************************

ACHILLES Analyses on DRUG_COST table

*********************************************/

-- for performance optimization, we create a table with drug costs pre-cached for the 15XX analysis

-- { 1502 in (@list_of_analysis_ids) | 1503 in (@list_of_analysis_ids) | 1504 in (@list_of_analysis_ids) | 1505 in (@list_of_analysis_ids) | 1506 in (@list_of_analysis_ids) | 1507 in (@list_of_analysis_ids) | 1508 in (@list_of_analysis_ids) | 1509 in (@list_of_analysis_ids) | 1510 in (@list_of_analysis_ids) | 1511 in (@list_of_analysis_ids)}?{

IF OBJECT_ID('ACHILLES_drug_cost_raw', 'U') IS NOT NULL
  DROP TABLE ACHILLES_drug_cost_raw;

select drug_concept_id as subject_id,
  paid_copay,
   paid_coinsurance,
   paid_toward_deductible,
   paid_by_payer,
   paid_by_coordination_benefits, 
   total_out_of_pocket,
   total_paid,
   ingredient_cost,
   dispensing_fee,
   average_wholesale_price
from drug_cost dc1
join drug_exposure de1 on de1.drug_exposure_id = dc1.drug_exposure_id and drug_concept_id <> 0
;



-- 1500   Number of drug cost records with invalid drug exposure id
insert into ACHILLES_results (analysis_id, count_value)
select 1500 as analysis_id,  
   count(dc1.drug_cost_ID) as count_value
from
   drug_cost dc1
      left join drug_exposure de1
      on dc1.drug_exposure_id = de1.drug_exposure_id
where de1.drug_exposure_id is null
;

-- 1501   Number of drug cost records with invalid payer plan period id
insert into ACHILLES_results (analysis_id, count_value)
select 1501 as analysis_id,  
   count(dc1.drug_cost_ID) as count_value
from
   drug_cost dc1
      left join payer_plan_period ppp1
      on dc1.payer_plan_period_id = ppp1.payer_plan_period_id
where dc1.payer_plan_period_id is not null
   and ppp1.payer_plan_period_id is null
;


-- 1502   Distribution of paid copay, by drug_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    paid_copay as count_value
  from ACHILLES_drug_cost_raw
  where paid_copay is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
      count_value, 
     count(*) as total, 
      row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1502 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 1503   Distribution of paid coinsurance, by drug_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    paid_coinsurance as count_value
  from ACHILLES_drug_cost_raw
  where paid_coinsurance is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
      count_value, 
     count(*) as total, 
      row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1503 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;




-- 1504   Distribution of paid toward deductible, by drug_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    paid_toward_deductible as count_value
  from ACHILLES_drug_cost_raw
  where paid_toward_deductible is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
     count_value, 
     count(*) as total, 
      row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1504 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;




-- 1505   Distribution of paid by payer, by drug_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    paid_by_payer as count_value
  from ACHILLES_drug_cost_raw
  where paid_by_payer is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
     count(*) as total, 
      row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1505 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;




-- 1506   Distribution of paid by coordination of benefit, by drug_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    paid_by_coordination_benefits as count_value
  from ACHILLES_drug_cost_raw
  where paid_by_coordination_benefits is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
      row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1506 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;



-- 1507   Distribution of total out-of-pocket, by drug_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    total_out_of_pocket as count_value
  from ACHILLES_drug_cost_raw
  where total_out_of_pocket is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
     row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1507 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;




-- 1508   Distribution of total paid, by drug_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    total_paid as count_value
  from ACHILLES_drug_cost_raw
  where total_paid is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1508 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;




-- 1509   Distribution of ingredient_cost, by drug_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    ingredient_cost as count_value
  from ACHILLES_drug_cost_raw
  where ingredient_cost is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1509 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;



-- 1510   Distribution of dispensing fee, by drug_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    dispensing_fee as count_value
  from ACHILLES_drug_cost_raw
  where dispensing_fee is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1510 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


-- 1511   Distribution of average wholesale price, by drug_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    average_wholesale_price as count_value
  from ACHILLES_drug_cost_raw
  where average_wholesale_price is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1511 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;




-- clean up cached table if exists
IF OBJECT_ID('ACHILLES_drug_cost_raw', 'U') IS NOT NULL
  DROP TABLE ACHILLES_drug_cost_raw;

/********************************************

ACHILLES Analyses on PROCEDURE_COST table

*********************************************/

-- { 1602 in (@list_of_analysis_ids) | 1603 in (@list_of_analysis_ids) | 1604 in (@list_of_analysis_ids) | 1605 in (@list_of_analysis_ids) | 1606 in (@list_of_analysis_ids) | 1607 in (@list_of_analysis_ids) | 1608 in (@list_of_analysis_ids)}?{

IF OBJECT_ID('ACHILLES_procedure_cost_raw', 'U') IS NOT NULL
  DROP TABLE ACHILLES_procedure_cost_raw;

select procedure_concept_id as subject_id,
  paid_copay,
  paid_coinsurance,
   paid_toward_deductible,
   paid_by_payer,
   paid_by_coordination_benefits, 
   total_out_of_pocket,
   total_paid
from procedure_cost pc1
join procedure_occurrence po1 on pc1.procedure_occurrence_id = po1.procedure_occurrence_id and procedure_concept_id <> 0
;

-- 1600   Number of procedure cost records with invalid procedure exposure id
insert into ACHILLES_results (analysis_id, count_value)
select 1600 as analysis_id,  
   count(pc1.procedure_cost_ID) as count_value
from
   procedure_cost pc1
      left join procedure_occurrence po1
      on pc1.procedure_occurrence_id = po1.procedure_occurrence_id
where po1.procedure_occurrence_id is null
;

-- 1601   Number of procedure cost records with invalid payer plan period id
insert into ACHILLES_results (analysis_id, count_value)
select 1601 as analysis_id,  
   count(pc1.procedure_cost_ID) as count_value
from
   procedure_cost pc1
      left join payer_plan_period ppp1
      on pc1.payer_plan_period_id = ppp1.payer_plan_period_id
where pc1.payer_plan_period_id is not null
   and ppp1.payer_plan_period_id is null
;


-- 1602   Distribution of paid copay, by procedure_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    paid_copay as count_value
  from ACHILLES_procedure_cost_raw
  where paid_copay is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1602 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
 min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;




-- 1603   Distribution of paid coinsurance, by procedure_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    paid_coinsurance as count_value
  from ACHILLES_procedure_cost_raw
  where paid_coinsurance is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1603 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
 min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
 min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;



-- 1604   Distribution of paid toward deductible, by procedure_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    paid_toward_deductible as count_value
  from ACHILLES_procedure_cost_raw
  where paid_toward_deductible is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1604 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
 min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
 min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
 min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;



-- 1605   Distribution of paid by payer, by procedure_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    paid_by_payer as count_value
  from ACHILLES_procedure_cost_raw
  where paid_by_payer is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1605 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
 min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
 min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
 min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
 min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;



-- 1606   Distribution of paid by coordination of benefit, by procedure_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    paid_by_coordination_benefits as count_value
  from ACHILLES_procedure_cost_raw
  where paid_by_coordination_benefits is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1606 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
 min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
 min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
 min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
 min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
 min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;



-- 1607   Distribution of total out-of-pocket, by procedure_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    total_out_of_pocket as count_value
  from ACHILLES_procedure_cost_raw
  where total_out_of_pocket is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1607 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
 min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
 min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
 min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
 min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
 min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;




-- 1608   Distribution of total paid, by procedure_concept_id
with rawData as
(
  select subject_id as stratum1_id,
    total_paid as count_value
  from ACHILLES_procedure_cost_raw
  where total_paid is not null
),
overallStats as
(
  select stratum1_id, 
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
  group by stratum1_id
),
stats as
(
  select stratum1_id, 
    count_value, 
    count(*) as total, 
    row_number() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)
select 1608 as analysis_id,
  p.stratum1_id as stratum_1,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
 min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
 min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
 min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
 min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
 min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id
group by p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;



-- 1609   Number of records by disease_class_concept_id

--not applicable for OMOP CDMv5



-- 1610   Number of records by revenue_code_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1610 as analysis_id, 
   revenue_code_concept_id as stratum_1, 
   count(pc1.procedure_cost_ID) as count_value
from
   procedure_cost pc1
where revenue_code_concept_id is not null
group by revenue_code_concept_id
;

-- clean up cached table if exists
IF OBJECT_ID('ACHILLES_procedure_cost_raw', 'U') IS NOT NULL
  DROP TABLE ACHILLES_procedure_cost_raw;


/********************************************

ACHILLES Analyses on COHORT table

*********************************************/

-- 1700   Number of records by cohort_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1700 as analysis_id, 
   cohort_definition_id as stratum_1, 
   count(subject_ID) as count_value
from
   cohort c1
group by cohort_definition_id
;


-- 1701   Number of records with cohort end date < cohort start date
insert into ACHILLES_results (analysis_id, count_value)
select 1701 as analysis_id, 
   count(subject_ID) as count_value
from
   cohort c1
where c1.cohort_end_date < c1.cohort_start_date
;

/********************************************

ACHILLES Analyses on MEASUREMENT table

*********************************************/



-- 1800   Number of persons with at least one measurement occurrence, by measurement_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1800 as analysis_id, 
   m.measurement_CONCEPT_ID as stratum_1,
   count(distinct m.PERSON_ID) as count_value
from
   measurement m
group by m.measurement_CONCEPT_ID
;


-- 1801   Number of measurement occurrence records, by measurement_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1801 as analysis_id, 
   m.measurement_concept_id as stratum_1,
   count(m.PERSON_ID) as count_value
from
   measurement m
group by m.measurement_CONCEPT_ID
;



-- 1802   Number of persons by measurement occurrence start month, by measurement_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 1802 as analysis_id,   
   m.measurement_concept_id as stratum_1,
   YEAR(measurement_date)*100 + month(measurement_date) as stratum_2, 
   count(distinct PERSON_ID) as count_value
from
   measurement m
group by m.measurement_concept_id, 
   YEAR(measurement_date)*100 + month(measurement_date)
;



-- 1803   Number of distinct measurement occurrence concepts per person
with rawData as
(
  select num_measurements as count_value
  from
   (
     select m.person_id, count(distinct m.measurement_concept_id) as num_measurements
     from
     measurement m
     group by m.person_id
   ) t0
),
overallStats as
(
  select avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from rawData
),
stats as
(
  select count_value, 
     count(*) as total, 
      row_number() over (order by count_value) as rn
  from rawData
  group by count_value
),
priorStats as
(
  select s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on p.rn <= s.rn
  group by s.count_value, s.total, s.rn
)
select 1803 as analysis_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
cross join overallStats o
group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;







-- 1804   Number of persons with at least one measurement occurrence, by measurement_concept_id by calendar year by gender by age decile
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
select 1804 as analysis_id,   
   m.measurement_concept_id as stratum_1,
   YEAR(measurement_date) as stratum_2,
   p1.gender_concept_id as stratum_3,
   floor((year(measurement_date) - p1.year_of_birth)/10) as stratum_4, 
   count(distinct p1.PERSON_ID) as count_value
from PERSON p1
inner join measurement m on p1.person_id = m.person_id
group by m.measurement_concept_id, 
   YEAR(measurement_date),
   p1.gender_concept_id,
   floor((year(measurement_date) - p1.year_of_birth)/10)
;

-- 1805   Number of measurement records, by measurement_concept_id by measurement_type_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 1805 as analysis_id, 
   m.measurement_concept_id as stratum_1,
   m.measurement_type_concept_id as stratum_2,
   count(m.PERSON_ID) as count_value
from measurement m
group by m.measurement_concept_id,   
   m.measurement_type_concept_id
;



-- 1806   Distribution of age by measurement_concept_id
select o1.measurement_concept_id as subject_id,
  p1.gender_concept_id,
   o1.measurement_start_year - p1.year_of_birth as count_value
from PERSON p1
inner join
(
   select person_id, measurement_concept_id, min(year(measurement_date)) as measurement_start_year
   from measurement
   group by person_id, measurement_concept_id
) o1
on p1.person_id = o1.person_id
;

with overallStats as
(
  select subject_id as stratum1_id,
    gender_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_1806
   group by subject_id, gender_concept_id
),
stats as
(
  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
  from #rawData_1806
  group by subject_id, gender_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 1806 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 1807   Number of measurement occurrence records, by measurement_concept_id and unit_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, count_value)
select 1807 as analysis_id, 
   m.measurement_concept_id as stratum_1,
   m.unit_concept_id as stratum_2,
   count(m.PERSON_ID) as count_value
from measurement m
group by m.measurement_concept_id, m.unit_concept_id
;



-- 1809   Number of measurement records with invalid person_id
insert into ACHILLES_results (analysis_id, count_value)
select 1809 as analysis_id,  
   count(m.PERSON_ID) as count_value
from measurement m
   left join PERSON p1 on p1.person_id = m.person_id
where p1.person_id is null
;


-- 1810   Number of measurement records outside valid observation period
insert into ACHILLES_results (analysis_id, count_value)
select 1810 as analysis_id,  
   count(m.PERSON_ID) as count_value
from measurement m
   left join observation_period op on op.person_id = m.person_id
   and m.measurement_date >= op.observation_period_start_date
   and m.measurement_date <= op.observation_period_end_date
where op.person_id is null
;



-- 1812   Number of measurement records with invalid provider_id
insert into ACHILLES_results (analysis_id, count_value)
select 1812 as analysis_id,  
   count(m.PERSON_ID) as count_value
from measurement m
   left join provider p on p.provider_id = m.provider_id
where m.provider_id is not null
   and p.provider_id is null
;

-- 1813   Number of observation records with invalid visit_id
insert into ACHILLES_results (analysis_id, count_value)
select 1813 as analysis_id, count(m.PERSON_ID) as count_value
from measurement m
   left join visit_occurrence vo on m.visit_occurrence_id = vo.visit_occurrence_id
where m.visit_occurrence_id is not null
   and vo.visit_occurrence_id is null
;


-- 1814   Number of measurement records with no value (numeric or concept)
insert into ACHILLES_results (analysis_id, count_value)
select 814 as analysis_id,  
   count(m.PERSON_ID) as count_value
from
   measurement m
where m.value_as_number is null
   and m.value_as_concept_id is null
;


-- 1815  Distribution of numeric values, by measurement_concept_id and unit_concept_id
select measurement_concept_id as subject_id, 
   unit_concept_id,
   value_as_number as count_value
from measurement m
where m.unit_concept_id is not null
   and m.value_as_number is not null
;

with overallStats as
(
  select subject_id as stratum1_id,
    unit_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_1815
   group by subject_id, unit_concept_id
),
stats as
(
  select subject_id as stratum1_id, unit_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, unit_concept_id order by count_value) as rn
  from #rawData_1815
  group by subject_id, unit_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 1815 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;





-- 1816   Distribution of low range, by measurement_concept_id and unit_concept_id
select measurement_concept_id as subject_id, 
   unit_concept_id,
   range_low as count_value
from measurement m
where m.unit_concept_id is not null
   and m.value_as_number is not null
   and m.range_low is not null
   and m.range_high is not null
;

with overallStats as
(
  select subject_id as stratum1_id,
    unit_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_1816
   group by subject_id, unit_concept_id
),
stats as
(
  select subject_id as stratum1_id, unit_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, unit_concept_id order by count_value) as rn
  from #rawData_1816
  group by subject_id, unit_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 1816 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
   o.max_value,
   o.avg_value,
   o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


-- 1817   Distribution of high range, by observation_concept_id and unit_concept_id
select measurement_concept_id as subject_id, 
   unit_concept_id,
   range_high as count_value
from measurement m
where m.unit_concept_id is not null
   and m.value_as_number is not null
   and m.range_low is not null
   and m.range_high is not null
;

with overallStats as
(
  select subject_id as stratum1_id,
    unit_concept_id as stratum2_id,
    avg(1.0 * count_value) as avg_value,
    stddev(count_value) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    count(*) as total
  from #rawData_1817
   group by subject_id, unit_concept_id
),
stats as
(
  select subject_id as stratum1_id, unit_concept_id as stratum2_id, count_value, count(*) as total, row_number() over (partition by subject_id, unit_concept_id order by count_value) as rn
  from #rawData_1817
  group by subject_id, unit_concept_id, count_value
),
priorStats as
(
  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
  from stats s
  join stats p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
)
select 1817 as analysis_id,
  o.stratum1_id,
  o.stratum2_id,
  o.total as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
from priorStats p
join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
group by o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

insert into ACHILLES_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
from #tempResults
;


-- 1818   Number of observation records below/within/above normal range, by observation_concept_id and unit_concept_id
insert into ACHILLES_results (analysis_id, stratum_1, stratum_2, stratum_3, count_value)
select 1818 as analysis_id,  
   m.measurement_concept_id as stratum_1,
   m.unit_concept_id as stratum_2,
   case when m.value_as_number < m.range_low then 'Below Range Low'
      when m.value_as_number >= m.range_low and m.value_as_number <= m.range_high then 'Within Range'
      when m.value_as_number > m.range_high then 'Above Range High'
      else 'Other' end as stratum_3,
   count(m.PERSON_ID) as count_value
from measurement m
where m.value_as_number is not null
   and m.unit_concept_id is not null
   and m.range_low is not null
   and m.range_high is not null
group by measurement_concept_id,
   unit_concept_id,
     case when m.value_as_number < m.range_low then 'Below Range Low'
      when m.value_as_number >= m.range_low and m.value_as_number <= m.range_high then 'Within Range'
      when m.value_as_number > m.range_high then 'Above Range High'
      else 'Other' end
;

-- 1820   Number of observation records by condition occurrence start month
insert into ACHILLES_results (analysis_id, stratum_1, count_value)
select 1820 as analysis_id,   
   YEAR(measurement_date)*100 + month(measurement_date) as stratum_1, 
   count(PERSON_ID) as count_value
from measurement m
group by YEAR(measurement_date)*100 + month(measurement_date)
;


delete from ACHILLES_results where count_value <= @smallcellcount;
delete from ACHILLES_results_dist where count_value <= @smallcellcount;

