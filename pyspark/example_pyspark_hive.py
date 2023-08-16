"""
Apache Spark & Apache Hive - Match made in heaven!

Apache Hive is a distributed, fault-tolerant data warehouse system that enables analytics on 
distributed data storage using SQL Apache Hive allows you to run SQL on big data!

What is Hive Metastore (HMS)? 
    Hive Metastore (HMS) is a central repository of metadata for Hive Tables and partitions in a relational
    database and provides clients (Hive, Impala and Spark) access to this information using the metastore
    service API. This is building block for data lakes

What is the purpose of a metastore? 
    Data abstraction and data discovery.Data abstraction decouples data formats, extractors 
    and loaders along the query. With Hive table, this information is provided once during Hive Table creation and reused every
    time table is referenced. Data Discovery enables user to discover and explore data

What are metadata objects?
    Metadata Objects are Database, Table, Partition 
    Database - namespace for tables. `default` database is used for tables with no user supplied
    database name
    Table - metadata for table contains columns, owner, storage and SerDe information. It also
    contains location of the underlying data, output formats and bucketing information. 
    Partition - Each Partition can have its own columns and SerDe and storage information

What is metastore architecture?
    Metastore is an object store backed by a database or object store. Database allow querying
    of metadata. The metastore can be configured either as remote or embedded data store. In
    remote mode, metastore is a thrift service

How does Apache Spark play a role with HiveTables?
    Apache Spark is a client that create HiveTables and query them. By using saveAsTable
    command the dataframe can be stored as a Hive Table. Spark will create a default local Hive
    metastore using Derby. Persistent tables will still exist even after your Spark Program
    has restarted, as long as you maintain connection to your metastore. 

What are the benefits of HiveTables?
    * Data Abstraction - access data as Tables with no knowledge of underlying location, format or partitions
    * Data Discoverability - explore what data is avialable
    * Hive metastore uses only necessary paritions for a query, discovering all partitions
        on the first query is no longer needed
    * Partition information is not gathered by default when creating external datasource tables
    * To sync the partition information in the metastore, use `MSCK REPAIR TABLE`

How does Spark SQL support Apache Hive?
    Spark SQL supports reading and writing data in Apache Hive. Configuration in Hive is 
    done by hive-site.xml, core-site.xml and hdfs-site.xml. Instantiate SparkSession with 
    Hive support, including connectivity to a persistent Hive metastore, support for Hive 
    serdes, and Hive user-defined functions.

What are Hive metastore (HMS)?
    AWS GLUE Data Catalog is a metastore provided by AWS

"""
from os.path import abspath
from pyspark.sql import SparkSession


def main(spark_session):
    """
    Spark Hive Table example
    """
    # spark version
    print(">>>> Spark Version: {}".format(spark.version))

    # turn off verbose logging
    spark_session.sparkContext.setLogLevel("ERROR")

    # create sample data
    data = [
        {"super_hero": "superman", "alternate_identity": "clark kent"},
        {"super_hero": "batman", "alternate_identity": "bruce wayne"},
        {"super_hero": "wonder woman", "alternate_identity": "princess diana"},
        {"super_hero": "flash", "alternate_identity": "barry"},
    ]

    # create a dataframe
    df = spark_session.createDataFrame(data)

    # create a Hive Table
    df.write.mode("overwrite").saveAsTable("super_hero_tbl")

    # querying a hive table
    spark_session.sql("select * from super_hero_tbl").show()

    spark_session.stop()


if __name__ == "__main__":
    warehouse_location = abspath("spark-warehouse")
    spark = (
        SparkSession.builder.appName("Spark Hive Example")
        .config("spark.sql.warehouse.dir", warehouse_location)
        .enableHiveSupport()
        .getOrCreate()
    )
    main(spark)
