#Spark Basic
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id,lit,col
from pyspark.sql.types import IntegerType, StructType, StructField, StringType


def basic():
    spark=SparkSession.builder.appName("Basics of Spark").getOrCreate()

    df_csv=spark.read.option("header",True).option("InferSchema",True).format("csv").load("file:///C:/spark3/python/data/")

    df_csv.show()     # action # display dataframe. by default 20 lines

#to add a new column:
    # df_csv_col_add=df_csv.withColumn("City", col('ID'))
# when then
#     df_csv_col_add.show()
#     df_csv_col_rname= df_csv_col_add.withColumnRenamed("ID", "Emp_ID")
#     df_csv_col_rname.show()
#     df_csv_col_rname.printSchema()

    print("No. of records in df_csv: ", df_csv.count())
    # df_csv.show(80)

    print("no.of partitions of df_csv: ", df_csv.rdd.getNumPartitions())
    # df_csv.printSchema()
    #
    df_part_id = df_csv.withColumn('pid', spark_partition_id())
    #
    df_part_id.show(10)

    df_data_dist=df_part_id.select('ID','pid').groupBy('pid').count()
    df_data_dist.show()

    df_csv_new=df_csv.coalesce(2)        #df_csv.repartition(2) 
    df_csv_new.show()
    df_part_id_new = df_csv_new.withColumn('pid', spark_partition_id())
    print("no.of partitions of df_csv: ", df_csv_new.rdd.getNumPartitions())
    df_data_dist_new=df_part_id_new.select('ID','pid').groupBy('pid').count()
    df_data_dist_new.show()

    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    # df_sel=df_csv.select('ID','Name')
    # df_sel.show()

    # schema = StructType([StructField('ID', IntegerType()),
    #                      StructField('Name', StringType()),
    #                      StructField('Dept', StringType()),
    #                      StructField('Address', StringType())
    #                      ])

    # df_csv_cdt=spark.createDataFrame(df_csv.rdd, schema)
    # df_csv_cdt.printSchema()

#using Spark SQL way:
    # df_csv.createOrReplaceTempView("emptable")
    #
    # abcd=spark.sql("select Address, count(id) from emptable group by address")
    # abcd.show()
    #
    df_sel = df_csv.select('ID', 'Dept').groupBy('Dept').count()
    df_sel.show()
    # print(df_csv.rdd.getNumPartitions())
    #
    # print(type(df_sel))
    # df_sel.explain

    #repartitioning and coalesce   // it can increase or decrease the number of partitions.

    print("-----End of read--------")

    #Spark dataframe Reader API.
    #Default File format -  Parquet
    # df1=spark.read.csv("file path")

    # list1=range(1,20)
    # for i in list1:
    #     print(i)

    # df = spark.range(100000)
    # df.createOrReplaceTempView('myView')
    # spark.sql('explain cost select * from myView').show(truncate=False)

    # df_csv=spark.read.option("header",True).format("csv").load("file:///C:/spark3/python/data/emp1.csv")
    # df_csv.show()
    # print(df_csv.rdd.getNumPartitions())
    # df_csv.printSchema()
    # df_csv.select('ID','Name').show()      #select some columns.


    # print("------------DF2 info-----------------")
    # df_dir_csv = spark.read.option("header", True).csv("file:///C:/spark3/python/data/")
#to print the schema of the DataFrame
    # df_dir_csv.printSchema()
#to get the number of partitions
    # print(df_dir_csv.rdd.getNumPartitions())
#add a new column
    # df_dir_csv.withColumn('pid', spark_partition_id()).show(100)
#count the no. of records
    # df_dir_csv.count()
#filter records
    # df_dir_csv.filter(df_dir_csv.ID=='3').show()

#create a new DF:
    # df_part_id=df_dir_csv.withColumn('pid', spark_partition_id())
#Count the number of records in each partition
    # df_part_id.groupby('pid').count().show()


    # df_dir_csv.select(count(df_dir_csv.ID), spark_partition_id).groupby(spark_partition_id).show()
    # df_dir_csv.createOrReplaceTempView('myView1')
    # spark.sql('explain cost select * from myView1').show(truncate=False)

if __name__=='__main__':
    basic()
