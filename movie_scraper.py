"""
This module scrapes the Zurich Film Festival (ZFF) archive. The years it scrapes is controlled by the
FESTIVAL_YEAR_SCRAPE list.

The ZFF have an archive page for each year; this page contains a dynamic list of movies that is pageinated.

The primary working function is scrape_zff_year_page(). It :
    * navigates through the paginated movie list ( navigate_to_next_page() ) for that year
    * assembles a list of movies urls
    * scrapes each movie page ( scrape_movie_page() )
    * writes each festival year to a './data/year/data_dirty_zff_YEAR.csv'

Once all the archive pages have been scraped, the individual .csv files in './data/years/' are merged to a single file
'./data/data_zff_src.csv'.

"""

import sys

import pandas as pd
from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager

import logging
import movie_cleaner as mc

#FESTIVAL_YEAR_SCRAPE = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
FESTIVAL_YEAR_SCRAPE = [2010]

# years available on zff website
FIRST_FESTIVAL_YEAR = 2005
LAST_FESTIVAL_YEAR = 2021

BASE_URL = "https://zff.com"
ARCHIVE_BASE_URL = "https://zff.com/en/archive/?page=1&language=en&festival_year="


# initialise a logger for the module.
def init_logger():
    my_logger = logging.getLogger("movie_scraper")
    my_logger.setLevel(logging.INFO)

    # log to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(name)s — %(message)s"))

    # log to file
    file_handler = logging.FileHandler('./logs/movie_scraper.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(name)s — %(message)s"))

    my_logger.addHandler(console_handler)
    my_logger.addHandler(file_handler)
    return my_logger


LOG = init_logger()


class ScraperException(Exception):
    """
    Signifies the scraper had an unexpected issue - see msg for info
    """
    pass


def create_key_value(item: str) -> tuple:
    # remove EOL chars from within string
    words = item
    words = words.replace('\n', '')
    words = words.replace('\r', '')

    # split into key:value tuples - by FIRST occurrence of delimiter
    words = words.strip().split(sep=":", maxsplit=1)

    if len(words) == 2:
        return words[0].lower(), words[1]
    else:
        return "", ""


def create_country_year_entries(key: str, value: str) -> dict:
    """
    sample 'Country, Year: USA,UK 2015' line item. Need to split into key/value for both country and year
    :param key: 'Country, Year:' part
    :param value: ' USA,UK 2015' - can contain many countries but only one year
    :return: dict with both key/value pairs
    """
    page_data = dict()

    if ("," in key):
        keys = key.split(sep=",")
        values = value.split(sep=",")

        # mutiple "countries" listed and then the 'year'
        if (len(values) > 2):
            the_value = " "
            for country_vale in values[:-1]:
                the_value = the_value + "," + country_vale.strip()

            page_data[keys[0].strip().lower()] = the_value.strip()
            page_data[keys[1].strip().lower()] = values[-1].strip()
        else:
            page_data[keys[0].strip().lower()] = values[0].strip()
            page_data[keys[1].strip().lower()] = values[1].strip()

    return page_data


def scrape_movie_page(page_source: str) -> dict:
    """
    Scrapes the movie details from the page dedicated to that movie. The format of the details are usually
    similar but the order of details can change.

    :param page_source - full page source from selenium driver
    :return: dict with key/value pairs of information we are interested in.

    """
    soup = BeautifulSoup(page_source, "html.parser")
    heading_element = soup.find(name="div", attrs={"class": "small-12 large-11 columns"})
    movie_title = heading_element.find(name="h1").text

    page_data = {}
    page_data[mc.COLN_FILM_TITLE] = movie_title.strip()

    info_list = soup.find_all("ul", attrs={"class": "info-list"})

    # iterate threw the list of movie details
    for item in info_list[0].find_all("li"):

        key, value = create_key_value(item.text)

        # item is not key/value pair so we skip to next item
        if len(key) == 0:
            continue

        #  "country,year" entry is special - need to parse separately.
        if mc.COLN_COUNTRY in key:
            entries = create_country_year_entries(key, value)
            page_data.update(entries)
        else:
            # just add key/value
            page_data[key] = value

    return page_data


def create_movie_url_list(page_source: str) -> list:
    """
    Scrapes the list of movies from that page

    :param page_source: html source from selenium driver
    :return: list of movies from that page.
    """
    soup = BeautifulSoup(page_source, "html.parser")
    movies = soup.find_all("li", attrs={"class": "movie"})
    movie_list = []

    for movie in movies:
        movie_link = movie.find(name="a")
        # create a full url
        full_movie_link = BASE_URL + movie_link["href"]
        LOG.info(f"retrieved url {full_movie_link}")
        movie_list.append(full_movie_link)

    return movie_list


def navigate_to_next_page(driver: webdriver, page_number: int, festival_year: str) -> bool:
    """
    method navigates between the pages of the paginated list of movies for a specific
    festival year. Sometimes clicking on the 'next' button does not work - so I have included retry
    logic (function tries 3 times).

    :param driver: selenium web driver
    :param page_number: which (paginated) page number.
    :param festival_year: which archived festival year is being scrapped.
    :return: boolean - indicates Selenium driver has another page to scrape
    """
    success = False
    try:

        # try clicking the button three times (this is usually where rendering issues occur)
        for x in range(3):
            try:
                link = driver.find_element(By.XPATH, "//a[@class='next right']")
                # scroll webdriver to the end of page (next may be hidden).
                driver.execute_script("arguments[0].scrollIntoView();", link)
                link.click()
                success = True
                break

            except StaleElementReferenceException as ex:
                LOG.warning(f" RETRY - issue locating/clicking the 'next right' {ex.msg}")

        # if it did not successfully navigate after three times - give up
        if not success:
            raise ScraperException(f"Failed to Navigate to next page on year {festival_year} page {page_number}")
            return False
        else:
            return True
    # no 'next' button on page - pagination is finished
    except NoSuchElementException as ex:
        LOG.info(f"No next button found on page {page_number} of archive list - assume it is last page in pageination.")
        return False


def scrape_zff_year_page(selm_driver: webdriver, archive_url: str, festival_year: str) -> None:
    """
    Scrapes the archive page for a given festival year:
        * walks the paginated movie list.
        * assembles the movie list (urls)
        * scrapes each movie page
        * writes /data/year/data_dirty_zff_YEAR.csv

    :param selm_driver: selenium driver
    :param archive_url: url of the archived festival year
    :param festival_year:  festival year that is being scraped
    :return: None
    """

    # wait 10 seconds if you cannot find element.
    selm_driver.implicitly_wait(10)

    selm_driver.get(archive_url)

    LOG.debug(selm_driver.page_source)

    movie_list = []
    page_number = 0
    next_page = True

    while next_page:
        page_number = page_number + 1
        LOG.debug(f"Processing PAGE number {page_number}")

        # ensure page is rendered - should force a wait if it not there.
        selm_driver.find_element(By.XPATH, "//li[@class='movie']")
        # create a list of movies from this page
        page_movie_list = create_movie_url_list(selm_driver.page_source)
        # add to "full" list
        movie_list = movie_list + page_movie_list
        next_page = navigate_to_next_page(driver=selm_driver, page_number=page_number, festival_year=festival_year)

    LOG.info(f"###### {festival_year} all pages scraped - number of films returned {len(movie_list)}")
    LOG.debug(f"###### {festival_year} Film list returned  \n{movie_list}")

    movie_df = pd.DataFrame(columns=[mc.COLN_LINK, mc.COLN_FILM_TITLE,
                                     mc.COLN_GENRE, mc.COLN_COUNTRY, mc.COLN_RUNTIME, mc.COLN_LANGUAGES,
                                     mc.COLN_SUBTITLES, mc.COLN_DIRECTOR])

    # walk through list of movie and actually scrape each movie detail
    for movie_url in movie_list:
        LOG.info(f"movie url scraping {movie_url}")
        selm_driver.get(movie_url)

        # check page is rendered (will force a wait of up to 10 seconds)
        selm_driver.find_element(By.XPATH, "//h1")

        # then scrape it
        page_data = scrape_movie_page(selm_driver.page_source)
        page_data["link"] = movie_url
        movie_df = movie_df.append(page_data, ignore_index=True)

    file_name = mc.DIRTY_YEAR_FILENAME_PREFIX + str(festival_year) + ".csv"
    movie_df.to_csv(file_name, index=False)


def check_configured_years(FILM_FESTIVAL_YEAR: list) -> None:
    """
    check scraper is configured for appropriate years.

    :param FILM_FESTIVAL_YEAR: list of years that are configured for scraping
    :return: None
    """
    all_available_years = [x for x in range(FIRST_FESTIVAL_YEAR, LAST_FESTIVAL_YEAR)]

    # check if input in correct range- otherwise throw an exception
    for festival_year in FILM_FESTIVAL_YEAR:
        if festival_year not in all_available_years:
            raise ScraperException(f"selected festival year {festival_year}" +
                                   f" is not in range - years must be between" +
                                   f" {FIRST_FESTIVAL_YEAR} and {LAST_FESTIVAL_YEAR} ")


def scrape_zff_archive() -> None:
    """
    scrapes all configured festival years
    :return:
    """
    selm_driver = webdriver.Firefox(executable_path=GeckoDriverManager().install())

    try:

        # ensure selected range is correct.
        check_configured_years(FESTIVAL_YEAR_SCRAPE)

        # log what year are being scraped
        festival_year_strings = [str(i) for i in FESTIVAL_YEAR_SCRAPE]
        LOG.info(f"Scraping ZFF for years {','.join(festival_year_strings)}")

        # do the scraping
        for year in FESTIVAL_YEAR_SCRAPE:
            archive_page_url = ARCHIVE_BASE_URL + str(year)
            scrape_zff_year_page(selm_driver=selm_driver, archive_url=archive_page_url, festival_year=year)

    finally:
        # try to close the driver anyway - regardless of exceptions
        selm_driver.close()


def merge_files() -> None:
    """
    Merge all the individual year file into one _src.csv file
    :return: None
    """
    # load file
    full_data_set = pd.DataFrame()

    for festival_year in FESTIVAL_YEAR_SCRAPE:
        file_name = mc.DIRTY_YEAR_FILENAME_PREFIX + str(festival_year) + ".csv"
        year_df = pd.read_csv(file_name)
        year_df[mc.COLN_FESTIVAL_YEAR] = festival_year
        full_data_set = full_data_set.append(year_df)

    full_data_set.to_csv(mc.SRC_FILENAME, index=False)


if __name__ == '__main__':
    scrape_zff_archive()
    merge_files()
