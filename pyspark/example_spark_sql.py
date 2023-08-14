from session.spark_session import get_session


def main(spark_session):
    # spark session
    spark = spark_session

    # create sample data
    data = [
        {"super_hero": "superman", "alternate_identity": "clark kent"},
        {"super_hero": "batman", "alternate_identity": "bruce wayne"},
        {"super_hero": "wonder woman", "alternate_identity": "princess diana"},
        {"super_hero": "flash", "alternate_identity": "barry"},
    ]

    # create a dataframe
    df = spark.createDataFrame(data)

    # register dataframe as a SQL Temporary view
    df.createOrReplaceTempView("tbl_superhero")

    # Example 1 - simple spark sql on Temp View
    spark.sql("select * from tbl_superhero").show()

    # Example 2 - spark sql on Global View
    df.createGlobalTempView("tbl_global_superhero")

    spark.sql("select * from global_temp.tbl_global_superhero").show()
    print(spark)

    print("GlobalTempView persists across Spark Sessions within a Spark Job!")
    print("GlobalTempView is tied to a system preserved database `global_temp`")
    new_spark = spark.newSession()

    print(new_spark)
    new_spark.sql("select * from global_temp.tbl_global_superhero").show()

    return None


if __name__ == "__main__":
    spark = get_session()
    main(spark)
