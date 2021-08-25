from pyspark import SparkContext

def BasicRDD():
    sc = SparkContext("local", "First App")
    print(sc.appName)

# Scenario 1: create rdd from in memory data:

    print("*********** Creating RDD from in-memory object *************")
    in_mem_rdd = sc.parallelize(range(1,10))
    print(in_mem_rdd.collect())
    print("*********** End of create RDD from in-memory object **********\n\n")

# Scenario 2: create rdd from a file:

    print("***********  Creating RDD from file  *************")
    file_rdd=sc.textFile("for_RDD.txt")
    print(file_rdd.collect())
    print("*********** End of create RDD from file **********\n\n")

# Scenario 3: create rdd from a another RDD:

    print("***********  Creating RDD from another RDD  *************\n")
    new_rdd = file_rdd.take(3)
    print(new_rdd)
    print("*********** End of create RDD from another RDD **********\n\n")

# Basic operation on RDD:

    print("\n***** To count the number of records *********\n")
    print(" Total no. of records in in_mem_rdd RDD :", in_mem_rdd.count())
    print(" Total no. of records in file_rdd RDD :", file_rdd.count())


    print("\n***** number of partitions of RDD *********")
    print(" No. of partitions of in_mem_rdd RDD :", in_mem_rdd.getNumPartitions())
    print(" No. of partitions of file_rdd RDD :", file_rdd.getNumPartitions())
BasicRDD()

