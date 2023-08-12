from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, current_date


def main():
    """
    Main function
    """
    # create a spark session
    spark = (
        SparkSession.builder.appName("Sample PySpark").master("local[*]").getOrCreate()
    )

    # spark version
    print(">>>> Spark Version: {}".format(spark.version))

    # turn off verbose logging
    spark.sparkContext.setLogLevel("ERROR")

    # create sample data
    data = [
        {"super_hero": "superman", "alternate_identity": "clark kent"},
        {"super_hero": "batman", "alternate_identity": "bruce wayne"},
        {"super_hero": "wonder woman", "alternate_identity": "princess diana"},
        {"super_hero": "flash", "alternate_identity": "barry"},
    ]

    # create a dataframe
    df = spark.createDataFrame(data)

    # print data
    df.show(n=5)

    # print schema
    df.printSchema()

    # Perform simple transformation
    final_df = df.withColumn("current_ts", current_timestamp()).withColumn(
        "current_date", current_date()
    )
    final_df.show(truncate=False)

    final_df.explain()

    ## Row File Formats - csv, json, text
    final_df.write.format("csv").mode("overwrite").save("output/final_df_csv")

    final_df.write.format("json").mode("overwrite").save("output/final_df_json")

    ## Columarn File Formats - orc, parquet
    final_df.write.format("parquet").mode("overwrite").partitionBy("current_date").save(
        "output/final_df_parquet"
    )

    final_df.write.format("orc").mode("overwrite").partitionBy("current_date").save(
        "output/final_df_orc"
    )

    ## Community Contributed File Formats - avro, delta, hudi, iceberg, jdbc
    ## In order to read or write in above file formats
    ## their datasources packaged as jars need to be included in spark runtime
    ## class path. This can be done in 3 ways
    ## Option 1 - add required jars to $SPARK_HOME/jars
    ## Option 2 - pass jars as parameter to spark-submit using --jars parameter
    ## Option 3 - Setup jars in spark config

    # stop spark
    spark.stop()


if __name__ == "__main__":
    main()
