'''
    Basic RDD operations

    To execute the code using Spark submit, place the file at /home/cloudera/SparkRDD.py
    Execute it on cluster with following command - spark-submit --master yarn  SparkRDD.py
'''

# ----------------------------------------count.py---------------------------------------
from pyspark import SparkContext
import SparkModuleCall
sc = SparkContext("local", "count app")
words = sc.parallelize (
  ["scala",
  "java",
  "hadoop",
  "spark",
  "akka",
  "spark vs hadoop",
  "pyspark",
  "pyspark and spark"]
)
counts = words.count()
print ("Number of elements in RDD -> %i" % (counts))
SparkModuleCall.printName("Test")
# ----------------------------------------count.py---------------------------------------