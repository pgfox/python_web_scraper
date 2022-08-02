"""
Module used to clean the ZFF _src_dirty.csv file.

Some of the 'dirt' is introduced BUT some 'dirt' comes from the ZFF website. There is a function created for each
type of cleaning.

##### General cleaning functions

* remove_leading_trailing_blanks() - removes all leading/trailing spaces in df

* drop_duplicates() - drop duplicates in dataframe.

* replace_known_value() - catches known incorrect values (eg. 'misspellings')

* remove_leading_comma() - removes leading commas from a column entries

##### Specific column cleaning functions

* remove_list_of_movies() - removes a known list of "movies" that should not be included in analysis - these are mainly
    ZFF presentations (not real movies)

* generate_director_value() - combines 'director' and 'directors' columns

* clean_year_column() - make sure all years in full format 2010 (not short format '10' ), valid year used

* split_title() - movie on ZFF can have a 'real' and 'alternate' title in one column

* clean_runtime_column() - normalise format, convert to int, check for outliers

* add_country_count() - add numeric count of countries associated with movie

* add_festival_name_columnn() - add a column to identify this info came from Zurich Film Festival - needed in
    later steps


"""

import numpy as np
import pandas as pd
import logging
import sys

# file name constances
DIRTY_YEAR_FILENAME_PREFIX = "./data/years/data_dirty_zff_"
SRC_FILENAME = "./data/data_zff_src.csv"
SRC_DIRTY_FILENAME = "./data/data_zff_src_dirty.csv"
STAGE_FILENAME = "./data/data_zff_stage.csv"
# identifier used to identify Zurich Film Festival
FESTIVAL_IDENTIFIER = "ZFF"

# constants for the column names
COLN_FESTIVAL_YEAR = "festival_year"
COLN_LINK = "link"
COLN_FILM_TITLE = "film_title"
COLN_FESTIVAL_NAME = "festival_name"
COLN_ALT_FILM_TITLE = "alt_title"
COLN_GENRE = "genre"
COLN_COUNTRY = "country"
COLN_COUNTRY_COUNT = "country_count"
COLN_DIRECTOR = "director"
COLN_YEAR = "year"
COLN_LANGUAGES = "languages"
COLN_RUNTIME = "runtime"
COLN_SUBTITLES = "subtitles"

# list of columns to use
COLN_NAMES_TO_INCLUDE = [COLN_LINK, COLN_FILM_TITLE, COLN_ALT_FILM_TITLE, COLN_FESTIVAL_NAME,
                         COLN_FESTIVAL_YEAR, COLN_DIRECTOR, COLN_COUNTRY, COLN_COUNTRY_COUNT, COLN_YEAR,
                         COLN_LANGUAGES, COLN_RUNTIME]

MAX_YEAR = 2020
MIN_YEAR = 1930

RUNTIME_MIN = 2
RUNTIME_MAX = 400

#used for correcting known misspellings in data
KNOWN_TERMS = {"USAA": "USA", "Spein": "Spain"}


def init_logger():
    my_logger = logging.getLogger("movie_cleaner")
    my_logger.setLevel(logging.INFO)

    # log to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(name)s — %(message)s"))

    # log to file
    file_handler = logging.FileHandler('./logs/movie_cleaner.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(name)s — %(message)s"))

    my_logger.addHandler(console_handler)
    my_logger.addHandler(file_handler)
    return my_logger


LOG = init_logger()


def split_title(original_df: pd.DataFrame) -> pd.DataFrame:
    """
    movie on ZFF can have a 'real'/'alternate' title in one column. If two titles present, split and place second title
     in 'alt_title'

    :param original_df: input data frame
    :return: modified data frame
    """
    LOG.debug(f"shape before spilt {original_df.shape}")

    original_df[[COLN_FILM_TITLE, COLN_ALT_FILM_TITLE]] = original_df[COLN_FILM_TITLE].str.split("/", n=1, expand=True)

    LOG.debug(f"shape after spilt {original_df.shape}")
    return original_df


def add_festival_name_columnn(original_df: pd.DataFrame) -> pd.DataFrame:
    original_df[COLN_FESTIVAL_NAME] = FESTIVAL_IDENTIFIER
    return original_df


def concat_director_values(row: pd.Series) -> str:
    """
    Used to combine 'director' and 'directors' column

    :param row: single row from dataframe
    :return: string value of 'director' column
    """
    if (pd.isna(row["directors"])):
        return row["director"]

    #  "directors" has data and director entry is NaN
    if (not pd.isna(row["directors"]) and pd.isna(row["director"])):
        return row["directors"]

    if (not pd.isna(row["directors"]) and not pd.isna(row["director"])):
        # already got a director so need to append
        return row["director"] + "," + row["directors"]

    return ""


def generate_director_value(original_df: pd.DataFrame) -> pd.DataFrame:
    original_df[COLN_DIRECTOR] = original_df.apply(concat_director_values, axis=1)
    return original_df


def remove_list_of_movies(original_df: pd.DataFrame) -> pd.DataFrame:
    """
    removes a known list of "movies" that should not be included in analysis - these are mainly
    ZFF presentations (not real movies)

    :param original_df: input dataframe
    :return: modified dataframe
    """
    films_to_drop = ["ZFF Masters:", "ZFF Talks: ", "A Conversation with...", "A Conversation With ...",
                     "Filmmusikkonzert", "craftwork", "Public Zurich Master Class:",
                     "Internationaler Filmmusikwettbewerb",
                     "Filmmusikwettbewerb", "Kurzfilme", "Int. Filmmusikwettbewerb", "Kurzfilme aus Mexiko",
                     "Kurzfilme aus Ungarn", "Kurzfilmprogramm:"
                                             "Short Cuts vol.", "Zürcher Filmpreis:", "onedotzero_ch", "wavelength 1",
                     "wow+flutter 1",
                     "Kurzfilmblock", "Kurzfilmprogramm", "Kurzes aus Italien", "Kurzes aus Kolumbien",
                     "Kurzes aus der Türkei"]

    LOG.debug(f"shape before drop film titles {original_df.shape}")

    for key_word in films_to_drop:
        original_df = original_df[~original_df[COLN_FILM_TITLE].str.startswith(key_word)]

    LOG.debug(f"shape after drop film titles {original_df.shape}")

    return original_df


def convert_hr_to_min(cell_value: str) -> str:
    """
    convert 'X hrs Y' to 'ZZ Min'.
    e.g. '1 hrs 20' to '80 Min'

    :param cell_value: string to convert
    :return: converted string
    """

    # if it is not a string "hrs" is not there
    if not isinstance(cell_value, str):
        return cell_value

    cell_value_lower = cell_value.lower().strip()
    try:
        if "hrs" in cell_value_lower:
            values = cell_value_lower.split("hrs")

            # first value hours and second value mins
            hours_in_minutes = int(values[0].strip()) * 60
            minutes = int(values[1].strip())
            total_minutes = hours_in_minutes + minutes
            return str(total_minutes) + " Min"
        else:
            return cell_value
    except Exception:
        # do not understand format - return blank
        return ""


def convert_min_string_to_int(cell_value: any) -> int:
    """
    convert '80 Min' or '80' (string) to '80' (int)
    :param cell_value:
    :return:
    """
    # NaN -unknown so set to blank
    if (pd.isna(cell_value)):
        return np.nan

    # has an "min" end
    if (isinstance(cell_value, str) and cell_value.lower().strip()[-3:] == "min"):
        # using double strip to remove space between number and min - eg. "93 Min "  => "93"
        min_number = cell_value.strip()[:-3].strip()
        return int(min_number)

    # maybe it is just a number
    try:
        return int(cell_value)
    except Exception as ex:
        #just log it
        LOG.debug(f"exception reported during int conversion {ex}")

    # if you got to here - runtime is not an int value - return NA
    return np.nan


def check_runtime_outliers(cell_value: int) -> float:
    """
    check if runtime is within a sensible range otherwise return NaN

    :param cell_value: int value of runtime
    :return: verified runtime value
    """
    # if na do nothing
    if pd.isna(cell_value):
        return cell_value

    if cell_value < RUNTIME_MIN or cell_value > RUNTIME_MAX:
        # return a blank if outside range
        LOG.info(f"Outlier '{cell_value}' in '{COLN_RUNTIME}' found - setting it to 'NAN'")
        return np.nan
    else:
        # otherwise it should be fine
        return cell_value


def clean_runtime_column(original_df: pd.DataFrame) -> pd.DataFrame:
    """
    normalise format, convert to int, check for outliers

    :param original_df: input dataframe
    :return: modified data frame
    """
    # first check for "hr" format
    original_df[COLN_RUNTIME] = original_df[COLN_RUNTIME].apply(convert_hr_to_min)

    # convert "min" to int value
    original_df[COLN_RUNTIME] = original_df[COLN_RUNTIME].apply(convert_min_string_to_int)

    # check for outliers
    original_df[COLN_RUNTIME] = original_df[COLN_RUNTIME].apply(check_runtime_outliers)

    original_df[COLN_RUNTIME] = original_df[COLN_RUNTIME].astype('Int64')

    return original_df


def drop_duplicates(original_df: pd.DataFrame, *colnames) -> pd.DataFrame:
    """
    check for duplicates based on a (variable) list of column names

    :param original_df: input dataframe
    :param colnames: variable list of column names to use in duplicate detection
    :return: modified dataframe
    """
    LOG.debug(f"before duplicates dropped {original_df.shape}")
    list_colnames = [x for x in colnames]
    cleaned_df = original_df.drop_duplicates(subset=list_colnames)
    LOG.debug(f"after duplicates dropped {cleaned_df.shape}")
    return cleaned_df


def remove_leading_trailing_blanks(original_df: pd.DataFrame) -> pd.DataFrame:
    """
    remove any leading/trailing blanks in dataframe values

    :param original_df: input dataframe
    :return: modified dataframe
    """
    # apply transformation to every cell in dataframe
    original_df = original_df.applymap(
        lambda cell_value: cell_value.strip() if not pd.isna(cell_value) and isinstance(cell_value, str) else cell_value)

    return original_df


def remove_leading_comma(original_df: pd.DataFrame, colname: str) -> pd.DataFrame:
    """
    remove leading comma from a specified column

    :param original_df: input dataframe
    :param colname: modified dataframe
    :return:
    """
    original_df[colname] = original_df[colname].apply(
        lambda cell_value: cell_value[1:] if not pd.isna(cell_value) and cell_value[0] == "," else cell_value)
    return original_df


def count_countries(country_list: str) -> int:
    """
    :param string containing comma seperated list
    :return: number of countries
    """

    # no countries listed
    if (pd.isna(country_list)):
        return 0

    split_list = country_list.split(",")
    return len(split_list)


def validate_year(year: int) -> float:
    if year > MAX_YEAR and year < MIN_YEAR:
        return np.nan
    try:
        return int(year)
    except Exception:
        return np.nan


def convert_year_to_four(cell_value: any) -> float:
    """
    Convert short format year to long format year .e.g. 10 - 2010

    Assumes: anything from 00-20 is in 2000. anything from 20 - 99 is in 1900

    :param cell_value: year is str or numeric format
    :return: 4 digit numberic or NaN if it cannot be interpreted
    """

    # do nothing
    if pd.isna(cell_value):
        return cell_value

    try:
        if isinstance(cell_value, str):
            cell_value = int(cell_value)

        # if not in 10's then we do nothing
        if cell_value > 99:
            return cell_value

        # try to convert based on value of 2 digits
        # anything above 20 is assumed to be 19XX, anything less than 20 is assumed 20XX

        if cell_value >= 20:
            return 1900 + cell_value
        else:
            return 2000 + cell_value

    except Exception as ex:
        # could not convert - return na
        LOG.debug(ex)
        return np.nan


def clean_year_column(original_df: pd.DataFrame) -> pd.DataFrame:
    """
   make sure all years in full format (2010 not short format '10' ), valid year used

    :param original_df: input dataFrame
    :return: modified dataframe
    """
    original_df[COLN_YEAR] = original_df[COLN_YEAR].apply(convert_year_to_four)

    original_df[COLN_YEAR] = original_df[COLN_YEAR].apply(validate_year)

    # int64 as there are nulls in there.
    original_df[COLN_YEAR] = original_df[COLN_YEAR].astype('Int64')

    return original_df


def add_country_count(original_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add additional country_count column.
    :param original_df: input dataframe
    :return: modified dataframe
    """
    original_df[COLN_COUNTRY_COUNT] = original_df[COLN_COUNTRY].apply(count_countries)
    return original_df


def replace_value(cell_value: str) -> str:
    if (pd.isna(cell_value)):
        return cell_value
    # any terms found in cell_value, replace them
    for term in KNOWN_TERMS.keys():
        if term in cell_value:
            cell_value = cell_value.replace(term, KNOWN_TERMS.get(term))

    return cell_value


def replace_known_value(original_df: pd.DataFrame, col_name: str) -> str:
    """
    remove any known values with 'correct' version. Uses 'KNOWN_TERMS' dict.

    :param original_df: input dataframe
    :param col_name: modified dataframe
    :return:
    """
    original_df[col_name] = original_df[col_name].apply(replace_value)

    return original_df


def clean_data() -> None:
    """
    Read _src_dirty.csv
    Do the data cleaning and
    write _stage.csv

    :return: None
    """

    dirty_df = pd.read_csv(SRC_DIRTY_FILENAME)

    LOG.info(f" DIRTY dataframe shape {dirty_df.shape}")

    cleaned_df = split_title(dirty_df)
    cleaned_df = remove_leading_trailing_blanks(original_df=cleaned_df)
    cleaned_df = generate_director_value(cleaned_df)
    cleaned_df = add_festival_name_columnn(cleaned_df)
    cleaned_df = remove_list_of_movies(cleaned_df)
    cleaned_df = drop_duplicates(cleaned_df, COLN_LINK, COLN_FESTIVAL_YEAR)

    cleaned_df = replace_known_value(cleaned_df, COLN_COUNTRY)

    # deal with year column
    cleaned_df = clean_year_column(cleaned_df)

    # deal with country column
    cleaned_df = remove_leading_comma(cleaned_df, COLN_COUNTRY)
    cleaned_df = add_country_count(cleaned_df)

    # deal with runtime column
    cleaned_df = clean_runtime_column(cleaned_df)

    # write a subset of values to the file
    cleaned_df = cleaned_df[COLN_NAMES_TO_INCLUDE]
    LOG.debug(cleaned_df.head())

    LOG.info(f" CLEAN dataframe shape {cleaned_df.shape}")
    cleaned_df.to_csv(STAGE_FILENAME, index=False)


if __name__ == '__main__':
    clean_data()
