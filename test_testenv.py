import unittest
import pandas as pd
from testenv import df

class TestDataFrame(unittest.TestCase):
    def setUp(self):
        self.df = df

    def test_dataframe_creation(self):
        self.assertEqual(list(self.df.columns), ['Name', 'Date', 'DateNew', 'Dates_Equal'])
        self.assertEqual(len(self.df), 3)

    def test_date_conversion(self):
        self.assertTrue(pd.isnull(self.df.loc[1, 'Date']))
        self.assertFalse(pd.isnull(self.df.loc[0, 'Date']))
        self.assertFalse(pd.isnull(self.df.loc[2, 'Date']))

    def test_dates_comparison(self):
        self.assertTrue(self.df.loc[0, 'Dates_Equal'])
        self.assertTrue(self.df.loc[1, 'Dates_Equal'])
        self.assertFalse(self.df.loc[2, 'Dates_Equal'])

if __name__ == '__main__':
    unittest.main()