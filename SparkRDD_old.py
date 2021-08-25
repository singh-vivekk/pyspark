'''
    Basic RDD operations

    To execute the code using Spark submit, place the file at /home/cloudera/SparkRDD.py
    Execute it on cluster with following command - spark-submit --master yarn  SparkRDD.py
    C:\spark3>spark-submit --master local[*] examples/src/main/python/pi.py 10
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

rdd1=sc.textFile("GitSetup.txt")
print(rdd1.take(10))

rdd1.foreach(print)

rdd2=rdd1.map(lambda x: (1,x))
rdd2.foreach(print)

rdd3=rdd1.flatMap(lambda x: x.split(" "))
rdd3.foreach(print)