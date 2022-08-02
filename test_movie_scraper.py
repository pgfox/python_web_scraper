"""
Unit tests for movie scraper
BEWARE: it does make some calles out to ZFF website

"""

import unittest
from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager

import movie_cleaner
import movie_scraper as test_me

class MyTestCase(unittest.TestCase):

    def test_create_movie_list(self):
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install())

        try:
            url = 'https://zff.com/en/archive/?page=1&language=en&festival_year=2010'
            driver.get(url)
            movie_list = test_me.create_movie_url_list(driver.page_source)

            self.assertEqual(36,len(movie_list),msg="number of movie returned")
        finally:
            driver.close()

    def test_scrape_individual_page(self):
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install())
        try:
            url = "https://zff.com/en/archive/16702/"
            driver.get(url)
            page_data = test_me.scrape_movie_page(driver.page_source)
            self.assertEqual(page_data.get(movie_cleaner.COLN_FILM_TITLE), "Grounding – Die letzten Tage der Swissair",
                             msg="movie title returned")
            self.assertEqual(page_data.get(movie_cleaner.COLN_COUNTRY), "Switzerland", msg="movie country returned")
            self.assertEqual(page_data.get(movie_cleaner.COLN_YEAR), "2006", msg="movie year returned")

        finally:
            driver.close()

    def test_scrape_individual_page_two(self):
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install())
        try:
            url = "https://zff.com/en/archive/26552/"
            driver.get(url)
            page_data = test_me.scrape_movie_page(driver.page_source)
            self.assertEqual(page_data.get(movie_cleaner.COLN_FILM_TITLE), "Anonymous", msg="movie title (two) returned")
            self.assertEqual(page_data.get(movie_cleaner.COLN_COUNTRY), ",USA,UK,Germany", msg="movie country(two) returned")
            self.assertEqual(page_data.get(movie_cleaner.COLN_YEAR), "2011", msg="movie year (two) returned")
            self.assertEqual(page_data.get(movie_cleaner.COLN_DIRECTOR), " Roland Emmerich", msg="director (two) returned")

        finally:
            driver.close()

    def test_scrape_individual_page_three(self):
        driver = webdriver.Firefox(executable_path=GeckoDriverManager().install())
        try:
            url = "https://zff.com/en/archive/10765/"
            driver.get(url)
            page_data = test_me.scrape_movie_page(driver.page_source)
            self.assertEqual(page_data.get(movie_cleaner.COLN_FILM_TITLE), "The End of the Tour",
                             msg="movie title (three) returned")
            self.assertEqual(page_data.get(movie_cleaner.COLN_COUNTRY), "USA", msg="movie country (three) returned")
            self.assertEqual(page_data.get(movie_cleaner.COLN_YEAR), "2015", msg="movie year (three) returned")


        finally:
            driver.close()


    def test_check_configured_years(self):
        movie_years = [2012, 2014]
        # raised expection if wrong
        test_me.check_configured_years(movie_years)

        movie_wrong_years = [2012, 1601]
        try:
            test_me.check_configured_years(movie_wrong_years)
            self.assertTrue(False, msg= "should not have got here - expected expection")
        except test_me.ScraperException as ex:
            pass


if __name__ == '__main__':
    unittest.main()
