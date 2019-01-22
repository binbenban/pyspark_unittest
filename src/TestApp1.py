from pyspark.sql.functions import *
from pyspark.sql.types import *
from PySparkTest import PySparkTest
from App1 import App1
import unittest
import pandas

class TestApp1(PySparkTest):
    def test_basic(self):
        df = self.spark.createDataFrame([
            (1, "a"),
            (2, "b"),
            (3, "c")
        ], ['id', 'name'])

        df2 = df.agg(
            sum(col('id'))
        )

        self.assertEquals(df2.collect()[0][0], 6)

    def test_get_data(self):
        app = App1()
        df = app.get_data(self.spark).toPandas()
        self.assertEquals(df.shape[0], 31)
        self.assertEquals(df.shape[1], 3)
        

if __name__ == '__main__':
    unittest.main()