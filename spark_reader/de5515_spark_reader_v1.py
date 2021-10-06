import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from pyspark.sql.functions import concat_ws, date_trunc, current_date, add_months, to_date, col, when, date_format


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a table that stores the health metrics of a sample of sessions.')
    parser.add_argument('--input_date', metavar='input_date', type=str, help='Enter date in format: yyyy-mm-dd')
    parser.add_argument('--output_db_name', metavar='output_db_name', type=str, help='Enter output database name')
    args = parser.parse_args()

    input_date = args.input_date
    output_dbase = args.output_db_name

    input_year = input_date[6:]
    input_month = input_date[3:5]
    input_day = input_date[:2]



    spark = (
        SparkSession.builder
            .enableHiveSupport()
            .getOrCreate()
    )


    # DATA IMPORT
    ## fact_pageview_events (format: orc) AS fpe
    df_fpe = spark.read.orc(path='s3://de-clickstream/fact_pageview_events/year=' + input_year + '/month=' + input_month + '/day=' + input_day + '/')

    ## fact_adengadinfo_events  (format: orc) AS fae
    df_fae = spark.read.orc(path='s3://de-clickstream/fact_adengadinfo_events/year=' + input_year + '/month=' + input_month + '/day=' + input_day + '/')

    #### drop beacon column
    df_fae = df_fae.drop("beacon", "skin", "event_ts", "wiki_id")

    ## rollup_wiki_beacon_pageviews (format: orc) AS bp
    df_bp = spark.read.table('statsdb.rollup_wiki_beacon_pageviews')

    ## time_on_site_session (format: parquet) AS toss
    df_toss = spark.read.parquet('s3://de-clickstream/time-on-site-sessions/year=' + input_year + '/month=' + input_month + '/day=' + input_day + '/')

    ## average_revenue_per_pageview (format: parquet) AS arpp
    df_arpp = spark.read.parquet('s3://de-clickstream/arpu/revenue_per_pageview/year=' + input_year + '/month=' + input_month + '/day=' + input_day + '/')


    # DATA FILTER
    ## fact_pageview_events where substr(session_id,1,2) in ('24','f4','a3', '03','78', '7e', '63', 'c5', 'b4')
    df_fpe_filtered = df_fpe.filter((substring("session_id", 1, 2)).isin('24','f4','a3', '03','78', '7e', '63', 'c5', 'b4'))


    # DATA CLEAR
    ## fact_pageview_events - filter out and delete records with the same pv_unique_id.
    df_fpe_filtered_unique = df_fpe_filtered.select('pv_unique_id')\
        .groupBy('pv_unique_id').count().filter('count==1').drop('count')
    df_fpe_cleared = df_fpe_filtered_unique.join(df_fpe_filtered, "pv_unique_id","left")


    ## LEFT JOINS
    ### fact_pageview_events (fpe) < time_on_site_sessions (toss)
    df_fpe_toss = df_fpe_cleared.join(df_toss, "session_id", "left")

    ### df_fpe_toss < df_fact_adengadinfo_events (fae)
    df_fpe_toss_fae = df_fpe_toss.join(df_fae, "pv_unique_id", "left")

    ### df_fpe_toss_fae < df_average_revenue_per_pageview (arpp)
    df_fpe_toss_fae_arpp = df_fpe_toss_fae.join(df_arpp, "pv_unique_id", "left")

    ## GROUP BY
    df_bp.registerTempTable("bp_db")

    query = f"""
    select 
    distinct beacon, 1 as flag, year, month, day 
    from bp_db 
    where period_id = '3' 
    and date(concat(year,'-',month,'-',day)) >= date_trunc('month',(add_months(to_date('{input_date}', 'yyyy-MM-dd'),-1))) 
    and date(concat(year,'-',month,'-',day)) < to_date('{input_date}', 'yyyy-MM-dd')"""
    df_bp_db = spark.sql(query)

    df_bp_db.registerTempTable("bp_tocase")
    df_fpe_toss_fae_arpp.registerTempTable("df")

    df_after_groupby = spark.sql("""
    select
        session_id
        ,date(concat(year,'-',month,'-',day)) as session_date
        ,min(event_ts) as session_start
        ,max(event_ts) as session_end
        ,df.beacon
        ,device_type
        ,skin
        ,country_code
        ,count(distinct pv_unique_id) as num_pvs
        ,count(distinct wiki_id) as num_wikis
        ,duration_in_seconds
        ,count(distinct event_id) as num_impressions
        ,count(distinct case when ad_status in ('success','forced_collapse') then event_id end) as num_impressions_successful
        ,sum(rev_per_pv) as session_revenue
    from df
    group by 1,2,5,6,7,8,11
    """)


    df_result = df_after_groupby.join(df_bp_db, "beacon", "left")
    df_final = df_result.withColumn('repeat_visitor', when(col("flag")==1, 1).otherwise(0))



    new_df = df_final.select('beacon', 'session_id', 'session_date', 'session_start', 'session_end', 'device_type',
                              'skin', 'country_code', 'num_pvs', 'num_wikis', 'duration_in_seconds', 'num_impressions',
                             'num_impressions_successful', 'session_revenue', 'flag', 'repeat_visitor', 'year', 'month', 'day')
    # spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
    # new_df.write.mode('overwrite').insertInto(output_dbase + '.sampleSessions')

    result_path = 's3://de-data-science-experiments/konrad/result5515/'
    new_df.write.orc(path=result_path + output_dbase, partitionBy=['year', 'month', 'day'], mode='overwrite')


