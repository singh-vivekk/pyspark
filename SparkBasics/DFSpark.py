from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


def testSpark():
    print("Spark setup")
    spark=SparkSession.builder.appName("test").getOrCreate()
    print(spark)

    print('''\t###################################
    #### READ operation in spark #####
    ###################################''')

# Scenario 1: to read dataframe from csv file format:

    print("*********** Create a DF using CSV file *************")
    df_csv= spark.read.option("header",True).format ("csv").load("business.csv")
    df_csv.show(5)
    df_csv.printSchema()    #to show the datatypes of the columns
    print("*********** End of create a DF using CSV file **********\n\n")

 # Scenario 2: to read a dataframe from text file format:

    print("*********** Create a DF using txt file *************")
    df_txt= spark.read.text("for_DF.txt")
    df_txt.show(5,truncate=False)
    print("*********** End of create a DF using txt file **********\n\n")

# Scenario 3: to read  a dataframe from json file format:

    print("*********** Create a DF using JSON file *************")
    df_json= spark.read.option("multiline","true").format("json").load("for_DF.json")
    df_json.show()
    print("*********** End of create a DF using JSON file **********\n\n")

#Read JSON file from multiline
#Sometimes you may want to read records from JSON file that scattered multiple lines,
# In order to read such files, use-value true to multiline option, by default multiline option, is set to false.


    print('''\t###################################
    #### WRITE operation in spark #####
    ###################################''')

    df_wr_csv=df_csv.select("id", "dept")
    df_wr_csv.show(5)

    print("*********** Writing DF into CSV format *************\n\n")
    df_wr_csv.write.mode("overwrite").option("header",True).csv("df_csv_save")

    print("*********** Writing DF into Text format *************\n\n")
    df_wr_txt=df_csv.select("name")
    df_wr_txt.write.mode("overwrite").format("text").save("df_txt_save")


if __name__=='__main__':
    testSpark()