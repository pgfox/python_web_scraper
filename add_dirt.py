"""
The module adds additional dirt to the _src.csv file and writes it to src_dirty.csv. There are a set of
functions contained that introduce dirt (one function for each of the following):

    * modify proportion of country names to be misspelled (random selected)
    * modify proportion of "runtime" column so that it will be list in "1 hr 30" instead
        of  "90 Min" (randomly selected)
    * modify proportion of "year" column so that they will be listed as 2 numbers
        rather than 4 numbers - "14" instead of "2014" (randomly selected)
    * add proportion of duplicate rows ((randomly selected)
    * modify proportion of "runtime" to contain outlier values (randomly selected)


NOTE: The scrapped data already contains:
    * column that sometimes need to split into two - "film_title" column sometimes contains "real title/alternative title"
    * column in wrong format - "year" column is "float" instead of "int"
    * sometimes rows have "director" and "directors" - these need to be joined
    * some columns have "leading" commas in there list -  "country" code example ",Spain,Mexico"
    * most columns have addition leading and trailing spaces that need to be removed.



"""

import movie_cleaner
import pandas as pd
from random import randint
import logging
import sys


# initialise a logger for the module.
def init_logger():
    my_logger = logging.getLogger("add_dirt")
    my_logger.setLevel(logging.DEBUG)

    # log to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(name)s — %(message)s"))

    # log to file
    file_handler = logging.FileHandler('./logs/add_dirt.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(name)s — %(message)s"))

    my_logger.addHandler(console_handler)
    my_logger.addHandler(file_handler)
    return my_logger


LOG = init_logger()


def add_misspelling_to_column(original_df: pd.DataFrame, column_name: str, real_value: str, misspelled_value: str,
                              ratio: float) -> pd.DataFrame:
    """
     modify a proportion values to be misspelled (random selected)

    :param original_df: dataframe to work on
    :param column_name: column where misspelling is placed
    :param real_value: value to misspell
    :param misspelled_value:  actual misspelled value
    :param ratio: what % of real_values will be misspelled
    :return DataFrame: containing misspelled values
    """
    logical_mask = original_df[column_name].apply(lambda cell_value: True if pd.notna(cell_value) and
                                                                             real_value in cell_value else False)

    new_df = original_df[logical_mask].sample(frac=ratio)[column_name].apply(
        lambda cell_value: cell_value.replace(real_value, misspelled_value) if real_value in cell_value else cell_value)

    original_df.update(new_df)

    LOG.info(f"add misspellings for {real_value} in {len(new_df)} rows.")

    return original_df


def convert_to_hr(cell_value: str) -> str:
    """
    convert min to 'x hr y' -   "90 Min" to "1 hr 30" instead

    :param cell_value: input string
    :return: string with converted value
    """
    try:
        # rmove the "Min" ending
        minutes_only = cell_value.strip()[:-3].strip()
        minutes_only = int(minutes_only)

        hours = minutes_only // 60
        min_remainder = minutes_only % 60

        hour_mins = str(hours) + " hrs " + str(min_remainder)
        return hour_mins
    except Exception:
        # seem quite dirty already - reuse original cell value
        return cell_value


def change_runtime_to_hour_min(original_df: pd.DataFrame, ratio: float) -> pd.DataFrame:
    """
    modify proportion of "runtime" column to "X hr Y" format e.g '1 hr 30'

    :param original_df: input dataframe
    :param ratio: % of values to change
    :return: modified dataframe
    """
    original_df = original_df[original_df[movie_cleaner.COLN_RUNTIME].notna()]

    new_df = original_df.sample(frac=ratio)[movie_cleaner.COLN_RUNTIME].apply(
        lambda cell_value: convert_to_hr(cell_value))

    original_df.update(new_df)

    LOG.info(f"for '{movie_cleaner.COLN_RUNTIME}' column changed {len(new_df)} rows to 'hr' ")

    return original_df


def add_reduced_year_format(original_df: pd.DataFrame, ratio: float) -> pd.DataFrame:
    """
    modify proportion of "year" column so that they will be listed as 2 numbers
        rather than 4 numbers - "14" instead of "2014" (randomly selected)

    :param original_df: input dataframe
    :param ratio: % of year value to change
    :return: modified dataframe
    """

    logical_mask = original_df[movie_cleaner.COLN_YEAR].apply(
        lambda cell_value: True if pd.notna(cell_value) else False)

    # remove the 100's and leave the remainer
    new_df = original_df[logical_mask].sample(frac=ratio)[movie_cleaner.COLN_YEAR].apply(
        lambda cell_value: cell_value % 100)

    original_df.update(new_df)

    LOG.info(f"for '{movie_cleaner.COLN_YEAR}' column changed {len(new_df)} rows to 'short form' ")

    return original_df


def add_duplicate_rows(original_df: pd.DataFrame, ratio: float) -> pd.DataFrame:
    """
    add proportion of duplicate rows (randomly selected)

    :param original_df: input dataframe
    :param ratio: % of rows in dataframe to duplicate
    :return: modified dataframe
    """
    new_df = original_df.sample(frac=ratio)
    new_df = pd.concat([new_df] * 2, ignore_index=True)
    original_df = pd.concat([original_df, new_df])

    LOG.info(f"add {len(new_df)} 'duplicate' rows ")

    return original_df


def generate_outlier(cell_value: str) -> str:
    # if it already in "weird" format - do nothing
    if ("hr" in cell_value):
        return cell_value

    outlier_value = randint(800, 10000)
    return str(outlier_value) + "Min"


def add_runtime_outliers(original_df: pd.DataFrame, ratio: float) -> pd.DataFrame:
    """
    modify proportion of "runtime" to contain outlier values (randomly selected - both the entries selected and the
    outlier values). The outliers are between 800 and 10,000 minutes

    :param original_df: input dataframe
    :param ratio: % of entries to change
    :return: modified dataframe
    """
    logical_mask = original_df[movie_cleaner.COLN_RUNTIME].apply(
        lambda cell_value: True if pd.notna(cell_value) else False)
    new_df = original_df[logical_mask].sample(frac=ratio)[movie_cleaner.COLN_RUNTIME].apply(
        lambda cell_value: generate_outlier(cell_value))

    original_df.update(new_df)

    LOG.info(f"for '{movie_cleaner.COLN_RUNTIME}' column changed {len(new_df)} rows to 'outliers' ")
    return original_df


def add_dirt() -> None:
    """

    The primary method in the module. Reads the _src.csv file in, adds dirt with each function call and writes the file
    to dirty.csv

    :return:
    """
    dirty_df = pd.read_csv(movie_cleaner.SRC_FILENAME)

    extra_dirty_df = add_misspelling_to_column(dirty_df, movie_cleaner.COLN_COUNTRY, "Spain", "Spein", 0.5)
    extra_dirty_df = add_misspelling_to_column(extra_dirty_df, movie_cleaner.COLN_COUNTRY, "USA", "USAA", 0.1)
    extra_dirty_df = change_runtime_to_hour_min(extra_dirty_df, ratio=0.10)
    extra_dirty_df = add_reduced_year_format(extra_dirty_df, ratio=0.20)
    extra_dirty_df = add_duplicate_rows(extra_dirty_df, ratio=0.10)
    extra_dirty_df = add_runtime_outliers(extra_dirty_df, ratio=0.01)

    extra_dirty_df.to_csv(movie_cleaner.SRC_DIRTY_FILENAME, index=False)


if __name__ == '__main__':
    add_dirt()
