"""
Unit tests for add_dirt.
"""

import unittest
import pandas as pd
import add_dirt as test_me
import movie_cleaner as movieCleaner



class MyTestCase(unittest.TestCase):

    def test_add_misspells(self):
        test_dirty_df = pd.read_csv("data_test/test_data_dirty_step_01.csv")
        dirty_df = test_me.add_misspelling_to_column(test_dirty_df,movieCleaner.COLN_COUNTRY,"Spain","Spein",0.5)
        dirty_df = dirty_df[dirty_df[movieCleaner.COLN_COUNTRY].notna()]
        self.assertEqual(len(dirty_df[dirty_df[movieCleaner.COLN_COUNTRY].str.contains("Spein")]),2, msg="should be 2")


    def test_add_reduced_year_format(self):
        test_dirty_df = pd.read_csv("data_test/test_data_dirty_step_01.csv")
        dirty_df = test_me.add_reduced_year_format(test_dirty_df,ratio=0.4)
        dirty_df = dirty_df[dirty_df[movieCleaner.COLN_YEAR].notna()]
        dirty_df = dirty_df[dirty_df[movieCleaner.COLN_YEAR] <= 100]
        self.assertEqual(len(dirty_df), 8, msg="short year format")


    def test_add_duplicate_rows(self):
        test_dirty_df = pd.read_csv("data_test/test_data_dirty_step_01.csv")
        dirty_df = test_me.add_duplicate_rows(test_dirty_df,ratio=0.4)
        self.assertEqual(len(dirty_df), 36, msg="short year format")


    def test_change_to_hr_min(self):

        test_dirty_df = pd.read_csv("data_test/test_data_dirty_step_01.csv")
        dirty_df = test_me.change_runtime_to_hour_min(test_dirty_df, ratio=0.4)
        dirty_df = dirty_df[dirty_df[movieCleaner.COLN_RUNTIME].notna()]
        dirty_df = dirty_df[dirty_df[movieCleaner.COLN_RUNTIME].str.contains("hr")]
        self.assertGreater(9, len(dirty_df), msg="hr min format")
        self.assertLess(5, len(dirty_df), msg="hr min format")

    def test_add_runtime_outlier(self):
        test_dirty_df = pd.read_csv("data_test/test_data_dirty_step_01.csv")
        dirty_df = test_me.add_runtime_outliers(test_dirty_df, ratio=0.20)
        dirty_df = dirty_df[dirty_df[movieCleaner.COLN_RUNTIME].notna()]

        dirty_df = dirty_df[~dirty_df[movieCleaner.COLN_RUNTIME].str.contains(" ")]
        print(dirty_df[movieCleaner.COLN_RUNTIME])
        self.assertGreater( 6, len(dirty_df), msg="outliers 1")
        self.assertLess( 3, len(dirty_df), msg="outliers 2")



if __name__ == '__main__':

    unittest.main()
