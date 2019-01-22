from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql import SparkSession

class App1:
    def application_main(self):
        spark = SparkSession.builder.master('local[2]').appName('App1').getOrCreate()
        self.logic_main(spark)


    def logic_main(self, spark):
        # get data
        df = self.get_data(spark)
        # df.show()

        # create new timestamps
        df_newts = self.create_new_timestamps(df)

        # union and fill
        df_unioned = self.union_and_fill(df, df_newts)


    def union_and_fill(self, df, df2):
        w = Window.partitionBy("tagid").orderBy('ts')
        w2 = Window.partitionBy(col('tagid'), col('session')).orderBy(col('ts'))

        return df.unionAll(df2)\
                 .withColumn('new_session', expr("case when value=-999 then 0 else 1 end"))\
                 .withColumn("session", sum(col('new_session')).over(w))\
                 .withColumn("nvalue", when(col('value') != -999, col('value')).otherwise(first(col('value')).over(w2)))\
          

    def create_new_timestamps(self, df):
        minmax = df.agg(min(col('ts')), max(col('ts'))).collect()[0]
        min_ts, max_ts = minmax[0], minmax[1]
        # print(min_ts, max_ts)
        return df.groupBy('tagid').agg(collect_list('ts').alias('ts_list'))\
                .withColumn('new_ts_list', 
                    expr("sequence(to_timestamp('{0}'), to_timestamp('{1}'), interval 5 minutes)".format(min_ts, max_ts))
                )\
                .withColumn('new_ts_list_f', array_except('new_ts_list', 'ts_list'))\
                .select('tagid', explode('new_ts_list_f').alias('new_ts'), lit(-999).alias('value'))\

    def get_data(self, spark):
        df = spark.createDataFrame([
            ("tag1", "2017-01-01 09:00:00", "1.1"),
            ("tag1", "2017-01-01 11:33:11", "1.2"),
            ("tag1", "2017-01-01 11:37:00", "1.3"),
            ("tag2", "2017-01-01 09:00:00", "2.1"),
            ("tag2", "2017-01-01 09:10:00", "2.2"),
            ("tag2", "2017-01-01 09:20:00", "2.3"),
            ("tag2", "2017-01-01 09:30:00", "2.4"),
            ("tag2", "2017-01-01 09:40:00", "2.5"),
            ("tag2", "2017-01-01 09:50:00", "2.6"),
            ("tag2", "2017-01-01 10:00:00", "2.7"),
            ("tag2", "2017-01-01 10:10:00", "2.8"),
            ("tag2", "2017-01-01 10:20:00", "2.9"),
            ("tag2", "2017-01-01 10:30:00", "2.10"),
            ("tag2", "2017-01-01 10:40:00", "2.11"),
            ("tag2", "2017-01-01 10:50:00", "2.12"),
            ("tag2", "2017-01-01 11:00:00", "2.13"),
            ("tag2", "2017-01-01 11:10:00", "2.14"),
            ("tag2", "2017-01-01 11:20:00", "2.15"),
            ("tag2", "2017-01-01 11:30:00", "2.16"),
            ("tag2", "2017-01-01 11:40:00", "2.17"),
            ("tag2", "2017-01-01 11:50:00", "2.18"),
            ("tag2", "2017-01-01 12:00:00", "2.19"),
            ("tag3", "2017-01-01 09:39:59", "2.1"),
            ("tag3", "2017-01-01 09:59:59", "2.2"),
            ("tag3", "2017-01-01 10:00:00", "2.3"),
            ("tag3", "2017-01-01 10:00:01", "2.4"),
            ("tag3", "2017-01-01 10:59:59", "2.5"),
            ("tag3", "2017-01-01 11:00:00", "2.6"),
            ("tag3", "2017-01-01 11:00:01", "2.7"),
            ("tag3", "2017-01-01 11:59:59", "2.8"),
            ("tag3", "2017-01-01 12:00:00", "2.9")
        ], ['tagid', 'ts', 'value'])

        return df.withColumn('ts', to_timestamp('ts'))\
            .withColumn('value', expr("cast(value as double)"))

if __name__ == '__main__':
    app = App1()
    app.application_main()