
# ******************************************
#  person
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table person --target-dir /user/omop/person -m 1 --hive-import

create table omop.person stored as parquet as 
select * from default.person where person_id is not null;

compute stats  person;
show table stats person;


# ******************************************
#  observation_period
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table observation_period --target-dir /user/omop/observation_period -m 1 --hive-import

create table omop.observation_period stored as parquet as 
select * from default.observation_period 
where observation_period_id is not null;

compute stats observation_period;


# ******************************************
#  visit_occurrence
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table visit_occurrence --target-dir /user/omop/visit_occurrence -m 1 --hive-import

create table omop.visit_occurrence stored as parquet as 
select * from default.visit_occurrence 
where visit_occurrence_id is not null;

compute stats visit_occurrence;


# ******************************************
#  procedure_occurrence
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table procedure_occurrence --target-dir /user/omop/procedure_occurrence -m 1 --hive-import

create table omop.procedure_occurrence stored as parquet as 
select * from default.procedure_occurrence 
where procedure_occurrence_id is not null;

compute stats procedure_occurrence;


# ******************************************
#  drug_exposure
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table drug_exposure --target-dir /user/omop/drug_exposure -m 1 --hive-import

create table omop.drug_exposure stored as parquet as 
select * from default.drug_exposure 
where drug_exposure_id is not null;

compute stats drug_exposure;


# ******************************************
#  condition_occurrence
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table condition_occurrence --target-dir /user/omop/condition_occurrence -m 1 --hive-import

create table omop.condition_occurrence stored as parquet as 
select * from default.condition_occurrence 
where condition_occurrence_id is not null;

compute stats condition_occurrence;


# ******************************************
#  provider
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table provider --target-dir /user/omop/provider -m 1 --hive-import

create table omop.provider stored as parquet as 
select * from default.provider where provider_id is not null;

compute stats provider;

# ******************************************
#  payer_plan_period
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table payer_plan_period --target-dir /user/omop/payer_plan_period -m 1 --hive-import

create table omop.payer_plan_period stored as parquet as 
select * from default.payer_plan_period where payer_plan_period_id is not null;

compute stats payer_plan_period;


# ******************************************
#  visit_cost
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table visit_cost --target-dir /user/omop1/visit_cost -m 1 --hive-import

create table omop.visit_cost stored as parquet as 
select * from default.visit_cost where visit_cost_id is not null;


# ******************************************
#  observation
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table observation --target-dir /user/omop1/observation -m 1 --hive-import

create table omop.observation stored as parquet as 
select * from default.observation where observation_id is not null;


# ******************************************
#  drug_cost
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table drug_cost --target-dir /user/omop1/drug_cost -m 1 --hive-import

create table omop.drug_cost stored as parquet as 
select * from default.drug_cost where drug_cost_id is not null;


# ******************************************
#  drug_era
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table drug_era --target-dir /user/omop1/drug_era -m 1 --hive-import

create table omop.drug_era stored as parquet as 
select * from default.drug_era where drug_era_id is not null;


# ******************************************
#  procedure_cost
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table procedure_cost --target-dir /user/omop1/procedure_cost -m 1 --hive-import

create table omop.procedure_cost stored as parquet as 
select * from default.procedure_cost where procedure_cost_id is not null;


# ******************************************
#  cohort
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table cohort --target-dir /user/omop1/cohort -m 1 --hive-import

create table omop.cohort stored as parquet as 
select * from default.cohort where cohort_definition_id is not null;


# ******************************************
#  death
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table death --target-dir /user/omop1/death -m 1 --hive-import

create table omop.death stored as parquet as 
select * from default.death where person_id is not null;


# ******************************************
#  measurement
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table measurement --target-dir /user/omop1/measurement -m 1 --hive-import

create table omop.measurement stored as parquet as 
select * from default.measurement where measurement_id is not null;



# ******************************************
#  achilles_analysis
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table achilles_analysis --target-dir /user/omop/achilles_analysis -m 1 --hive-import

create table omop.achilles_analysis stored as parquet as 
select * from default.achilles_analysis where analysis_id is not null;

compute stats achilles_analysis;


# ******************************************
#  achilles_results
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table achilles_results --target-dir /user/omop1/achilles_results -m 1 --hive-import

create table omop.achilles_results stored as parquet as 
select * from default.achilles_results where 1=2;


# ******************************************
#  achilles_results_dist
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table achilles_results_dist --target-dir /user/omop1/achilles_results_dist -m 1 --hive-import

create table omop.achilles_results_dist stored as parquet as 
select * from default.achilles_results_dist where 1=2;



# ******************************************
#  condition_era
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table condition_era --target-dir /user/omop/condition_era -m 1 --hive-import

create table omop.condition_era stored as parquet as 
select * from default.condition_era where condition_era_id is not null;


# ******************************************
#  location
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table location --target-dir /user/omop/location -m 1 --hive-import

create table omop.`location` stored as parquet as 
select * from default.`location` where location_id is not null;


# ******************************************
#  care_site
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table care_site --target-dir /user/omop/care_site -m 1 --hive-import

create table omop.care_site stored as parquet as 
select * from default.care_site where care_site_id is not null;


# ******************************************
#  payer_plan_period
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table payer_plan_period --target-dir /user/omop/payer_plan_period -m 1 --hive-import

create table omop.payer_plan_period stored as parquet as 
select * from default.payer_plan_period where care_site_id is not null;


# ******************************************
# import into HDFS as text file
# ******************************************

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table person --target-dir /user/omop/person -m 1

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table observation_period --target-dir /user/omop/observation_period -m 1

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table visit_occurrence --target-dir /user/omop/visit_occurrence -m 1

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table procedure_occurrence --target-dir /user/omop/procedure_occurrence -m 1

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table drug_exposure --target-dir /user/omop/drug_exposure -m 1

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table condition_occurrence --target-dir /user/omop/condition_occurrence -m 1

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table provider --target-dir /user/omop/provider -m 1

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table payer_plan_period --target-dir /user/omop/payer_plan_period -m 1

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table visit_cost --target-dir /user/omop/visit_cost -m 1

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table drug_cost --target-dir /user/omop/drug_cost -m 1

sqoop import --connect "jdbc:sqlserver://10.120.42.12:3341;database=omop;user=NewSA;password=Login@newsa" --table person --target-dir /user/omop/person -m 1

