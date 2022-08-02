"""
unit tests for movie_cleaner
"""
import unittest
import pandas as pd


# what is being tested
import movie_cleaner as test_me

class MyTestCase(unittest.TestCase):

    def test_drop_duplicates(self):
        test_dirty_df = pd.read_csv("data_test/test_data_dirty_01.csv")
        cleaned_df = test_me.drop_duplicates(test_dirty_df, test_me.COLN_LINK)
        self.assertEqual(len(cleaned_df), len(test_dirty_df) - 2 , msg="should have dropped two duplicate rows")

    def test_drop_duplicates_two_cols(self):
        test_dirty_df = pd.read_csv("data_test/test_data_dirty_01.csv")
        cleaned_df = test_me.drop_duplicates(test_dirty_df, test_me.COLN_LINK, test_me.COLN_FESTIVAL_YEAR)
        #should only drop one duplicate as festival year is different in one of the years
        self.assertEqual(len(cleaned_df), len(test_dirty_df) - 1 , msg="should have dropped one duplicate rows"+
                                                                       " as festival year is different.")

    def test_remove_leading_trailing_blanks(self):
        test_dirty_df = pd.read_csv("data_test/test_data_dirty_01.csv")
        cleaned_df = test_me.remove_leading_trailing_blanks(test_dirty_df)
        self.assertEqual(cleaned_df.loc[0,test_me.COLN_GENRE],"Drama",msg="cell value should have no "+
                                                                           "leading or trailing spaces.")
        self.assertEqual(cleaned_df.loc[0, test_me.COLN_RUNTIME], "93 Min", msg="runtime should have no blanks")
        self.assertEqual(cleaned_df.loc[0, test_me.COLN_FESTIVAL_YEAR],2010, msg="festival year should have no blanks")


    def test_remove_leading_comma(self):
        test_dirty_df = pd.read_csv("data_test/test_data_dirty_01.csv")
        cleaned_df = test_me.remove_leading_comma(test_dirty_df, test_me.COLN_COUNTRY)
        self.assertEqual(cleaned_df.loc[7, test_me.COLN_COUNTRY],"Australia,USA",msg="cell value should have no "+
                                                                           "leading comma.")

    def test_add_country_count(self):

        test_dirty_df = pd.read_csv("data_test/test_data_dirty_01.csv")
        cleaned_df = test_me.remove_leading_comma(test_dirty_df, test_me.COLN_COUNTRY)
        cleaned_df = test_me.add_country_count(cleaned_df)
        self.assertEqual(cleaned_df.loc[14, test_me.COLN_COUNTRY_COUNT], 4, msg="number of countries for this row unexpected")
        self.assertEqual(cleaned_df.loc[12, test_me.COLN_COUNTRY_COUNT], 0, msg="number of countries for this row unexpected")


    def test_drop_min_from_runtime(self):
        test_dirty_df = pd.read_csv("data_test/test_data_dirty_01.csv")
        cleaned_df = test_me.remove_leading_trailing_blanks(test_dirty_df)
        cleaned_df = test_me.clean_runtime_column(cleaned_df)

        self.assertEqual(cleaned_df.loc[0, test_me.COLN_RUNTIME], 93, msg="no min ")
        self.assertTrue(pd.isna(cleaned_df.loc[15, test_me.COLN_RUNTIME]), msg="Nan value check ")
        self.assertTrue(pd.isna(cleaned_df.loc[18, test_me.COLN_RUNTIME]), msg="Not a correct runtime value - return NAN ")


    def test_generate_director_value(self):
        test_dirty_df = pd.read_csv("data_test/test_data_dirty_01.csv")
        cleaned_df = test_me.remove_leading_trailing_blanks(test_dirty_df)
        cleaned_df = test_me.generate_director_value(cleaned_df)

        #check known variations
        self.assertEqual(cleaned_df.loc[3, test_me.COLN_DIRECTOR], "Gudrun F. Widlok, Rouven Rech", msg="no 'director' listed")
        self.assertEqual(cleaned_df.loc[2, test_me.COLN_DIRECTOR], "Hans Van Nuffel", msg="just a 'director' listed no 'directors'")
        self.assertEqual(cleaned_df.loc[15, test_me.COLN_DIRECTOR], "Rachel Perkins,John Black", msg="'director' and 'directors' listed")

    def test_convert_hr_to_min(self):

        value_in_min = test_me.convert_hr_to_min(" 2 HRs 10 ")
        self.assertEqual(value_in_min,"130 Min",msg="hrs_to_min 1")

        value_in_min = test_me.convert_hr_to_min("2HRs10")
        self.assertEqual(value_in_min, "130 Min", msg="hrs_to_min 2")

        value_in_min = test_me.convert_hr_to_min("HRs")
        self.assertEqual(value_in_min, "", msg="hrs_to_min 3")

    def test_convert_year_to_four(self):
        value_in_min = test_me.convert_year_to_four("2000")
        self.assertEqual(value_in_min, 2000, msg="convert year to four 1")

        value_in_min = test_me.convert_year_to_four("84")
        self.assertEqual(value_in_min, 1984, msg="convert year to four 2")

        value_in_min = test_me.convert_year_to_four("12")
        self.assertEqual(value_in_min, 2012, msg="convert year to four 3")

        value_in_min = test_me.convert_year_to_four("BB")
        self.assertTrue(pd.isna(value_in_min), msg="convert year to four - weird input ")

    def test_replace_value(self):

        value_returned = test_me.replace_value("USAA")
        self.assertEqual(value_returned, "USA" , msg="convert year to four 1")

        value_returned = test_me.replace_value("Spain,Ireland")
        self.assertEqual(value_returned, "Spain,Ireland", msg="replace known value 2")

        value_returned = test_me.replace_value("Mexico,Spein,Ireland")
        self.assertEqual(value_returned, "Mexico,Spain,Ireland", msg="replace known value 3")


if __name__ == '__main__':
    unittest.main()
