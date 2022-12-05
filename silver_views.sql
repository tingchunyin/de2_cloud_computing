DROP TABLE IF EXISTS tingchunyin_homework.silver_views;

CREATE TABLE tingchunyin_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://ceu-tingchunyin/datalake/views_silver'
    ) AS SELECT article, views, rank, date
         FROM tingchunyin_homework.bronze_views 
         where date is not null;

SELECT date, count(*) FROM silver_views GROUP BY date ORDER BY date;

select * from  tingchunyin_homework.silver_views
