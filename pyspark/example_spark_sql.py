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
    df.createOrReplaceTempView("superhero")

    spark.sql("select * from superhero").show()

    return None


if __name__ == "__main__":
    spark = get_session()
    main(spark)
