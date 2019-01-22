from pyspark.sql.functions import *
from pyspark.sql.types import *

class App1:
    def application_main():
        spark = SparkSession.builder.master('local[2]').appName('App1').getOrCreate()
        
        logic_main(spark)

    def logic_main(spark):
        # get data
        df = get_data(spark)

        # unpivot
        df = unpivot(df)

        # persist result
        persist(df)

    def 