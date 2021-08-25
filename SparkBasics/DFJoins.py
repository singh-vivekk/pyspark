#Spark Joins
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id,lit,col
from pyspark.sql.types import IntegerType, StructType, StructField, StringType


def basicJoin():
    spark=SparkSession.builder.appName("Basics of Spark").getOrCreate()

    df_csv1=spark.read.option("header",True).option("InferSchema",True).format("csv").load("file:///C:/spark3/python/data/emp1.csv")

    df_csv2=spark.read.option("header",True).option("InferSchema",True).format("csv").load("file:///C:/spark3/python/data/emp3.csv")

    df_csv1.show()
    df_csv2.show()

    print("select with filter............")
    df_new=df_csv1.select(df_csv1.ID.between(2,4))
    df_new.filter(df_new).show()

    df_join=df_csv1.join(df_csv2, 'Name','Leftanti')    #Deafult is Inner. We can use any of the joins
    # df_join = df_csv1.join(df_csv2, df_csv1.ID==df_csv2.ID).drop(df_csv2.ID)
    df_join.show()

#physical plan
    # print(df_join.explain())

if __name__=='__main__':
    basicJoin()
