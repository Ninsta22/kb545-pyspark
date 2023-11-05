from mylib.spark_db import create_session, csv_to_spark, execute_query
import unittest


class TestMyLib(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_session()
        cls.df = csv_to_spark(cls.spark, "2021-2022 NBA Player Stats - Regular.csv")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # Tests that the CSV File was Read Properly
    def test_read_csv(self):
        assert len(self.df.colums) == 30

    def test_sql_query(self):
        try:
            execute_query(self.spark, "SELECT * FROM nba_data WHERE Tm LIKE 'MIA'")
            assert True
        except:
            assert False
