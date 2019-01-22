from pyspark.sql.functions import *
from pyspark.sql.types import *
from PySparkTest import PySparkTest
import unittest

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

if __name__ == '__main__':
    unittest.main()