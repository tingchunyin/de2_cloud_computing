DROP TABLE IF EXISTS tingchunyin_homework.gold_allviews;

CREATE TABLE tingchunyin_homework.gold_allviews 
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://ceu-tingchunyin/datalake/gold_allviews'
    )
AS SELECT article, sum(views) as total_top_views,
                min(rank) as top_rank,
                count(*) as ranked_days
                from  tingchunyin_homework.silver_views 
                group by article 
                order by -sum(views);

select * from  tingchunyin_homework.gold_allviews;
