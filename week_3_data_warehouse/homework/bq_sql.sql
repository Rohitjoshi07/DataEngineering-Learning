
--step1
--create table
CREATE OR REPLACE EXTERNAL TABLE `data-engineering-rj.zoomcamp.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de/data/fhv_tripdata_2019-*.csv.gz']
);

LOAD DATA INTO `data-engineering-rj.zoomcamp.fhv_tripdata_internal`
 FROM FILES (
   format='CSV',
  uris = ['gs://prefect-de/data/fhv_tripdata_2019-*.csv.gz']
);

--STEP2
SELECT  count(*) FROM `data-engineering-rj.zoomcamp.fhv_tripdata`;



--STEP3
SELECT  distinct('affiliated_base_number') FROM `data-engineering-rj.zoomcamp.fhv_tripdata`;
SELECT  distinct('affiliated_base_number') FROM `data-engineering-rj.zoomcamp.fhv2019_internal`;


--STEP4

--create a partiona and cluster table
create or replace table `data-engineering-rj.zoomcamp.fhv_partitioned_clustered`
PARTITION by DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
select * from `data-engineering-rj.zoomcamp.fhv_tripdata_internal`;


--step 5
-- query to get data from partioned and clustered table
select distinct(affiliated_base_number) from 'data-engineering-rj.zoomcamp.fhv_partitioned_clustered'
 WHERE DATE(pickup_datetime) between '2019-03-01' AND '2019-03-31'; 

select distinct(affiliated_base_number) from 'data-engineering-rj.zoomcamp.fhv2019_internal'
 WHERE DATE(pickup_datetime) between '2019-03-01' AND '2019-03-31'; 
