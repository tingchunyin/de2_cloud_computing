DROP TABLE IF EXISTS tingchunyin_homework.bronze_views;

CREATE EXTERNAL TABLE
tingchunyin_homework.bronze_views (
    article STRING,
    views INT,
    rank INT,
    date DATE,
    retrieved_at TIMESTAMP) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://ceu-tingchunyin/datalake/views/';
