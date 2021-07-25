'''

    This is a sample pyspark project
    Press Shift+F10 to execute it or replace it with your code
    Press Double Shift to search everywhere for classes, files, tool windows, actions and settings.

'''

import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession


def print_hi(name):
    #use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')    #Press Ctrl+F8 to toggle the breakpoint.
    print(f'Hello, {name}')  #Press Ctrl+F8 to goggle the breakpoint.

    my_list =[1,2,3]
    spark=SparkSession.builder.appName("My first Spark app").getOrCreate()

    df = spark.createDataFrame(my_list, IntegerType())
    df.show()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('Python Pycharm')