from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    current_date,
    monotonically_increasing_id,
)


def main():
    """
    Main function
    """
    # create a spark session
    spark = (
        SparkSession.builder.config(
            "spark.jars",
            "/opt/pyspark_app/jars/spark-avro_2.12-3.3.0.jar, \
             /opt/pyspark_app/jars/delta-core_2.12-2.2.0.jar, \
             /opt/pyspark_app/jars/delta-storage-2.2.0.jar, \
             /opt/pyspark_app/jars/postgresql-42.6.0.jar",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .appName("Sample PySpark")
        .master("local[*]")
        .getOrCreate()
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
    print(">>>> Writing final_df - csv")
    final_df.write.format("csv").mode("overwrite").save("output/final_df_csv")
    print(">>>> Writing final_df -  json")
    final_df.write.format("json").mode("overwrite").save("output/final_df_json")
    print(">>>> Writing final_df - parquet")
    ## Columarn File Formats - orc, parquet
    final_df.write.format("parquet").mode("overwrite").partitionBy("current_date").save(
        "output/final_df_parquet"
    )
    print(">>>> Writing final_df - orc")
    final_df.write.format("orc").mode("overwrite").partitionBy("current_date").save(
        "output/final_df_orc"
    )

    ## Community Contributed File Formats - avro, delta, jdbc
    ## In order to read or write in above file formats
    ## their datasources packaged as jars need to be included in spark runtime
    ## class path. This can be done in 3 ways
    ## Option 1 - add required jars to $SPARK_HOME/jars
    ## Option 2 - pass jars as parameter to spark-submit using --jars parameter
    ## Option 3 - Setup jars in spark config
    print(">>>> Writing final_df - avro")
    final_df.write.format("avro").mode("overwrite").partitionBy("current_date").save(
        "output/final_df_avro"
    )
    print(">>>> Writing final_df - delta")
    final_df.write.format("delta").mode("overwrite").partitionBy("current_date").save(
        "output/final_df_delta"
    )

    # Write to Postgres database using JDBC
    final_df.withColumn("id", monotonically_increasing_id()).withColumn(
        "name", lit("dummy")
    ).withColumn("updated_ts", current_timestamp()).withColumnRenamed(
        "current_ts", "created_ts"
    ).select(
        "id", "name", "created_ts", "updated_ts"
    ).write.format(
        "jdbc"
    ).option(
        "url", "jdbc:postgresql://postgres_db:5432/postgres"
    ).option(
        "dbtable", "public.item"
    ).option(
        "user", "postgres"
    ).option(
        "password", "XXXXXX"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()

    # stop spark
    spark.stop()


if __name__ == "__main__":
    main()
