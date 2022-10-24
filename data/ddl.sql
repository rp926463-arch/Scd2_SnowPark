CREATE OR REPLACE TABLE STAGING.STORES (store_id varchar(5), location varchar(100), value integer)
;

CREATE TABLE FINAL.STORES (store_id varchar(5), location varchar(100), value integer, start_date date, end_date date, active_flag char)
;

CREATE STAGE FINAL.CSV_STAGE
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = CSV_FORMAT
;

CREATE OR REPLACE FILE FORMAT FINAL.CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = GZIP
;

--snowsql -a bypgtvv-tw48419 -u RP926463

--PUT file://C:\Users\rosha\IdeaProjects\SCD2_SnowPark_Scala\data\stores.csv @CSV_STAGE

SELECT T.$1, T.$2, T.$3 FROM @CSV_STAGE (FILE_FORMAT => 'CSV_FORMAT') T;

INSERT INTO STAGING.STORES SELECT T.$1, T.$2, T.$3, null, null, null FROM @CSV_STAGE (FILE_FORMAT => 'CSV_FORMAT') T;

SELECT * FROM STAGING.STORES;

