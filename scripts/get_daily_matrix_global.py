#!/usr/bin/python
# -*- coding: utf-8 -*-

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Created by: Anderson Brito
# Email: andersonfbrito@gmail.com
# Python: 3
#
#  get_daily_matrix_global.py.py -> This code converts Johns Hopkins (Global)
#                                   dashboard raw data in CSV into a TSV,
#                                   with reformatted dates and columns.
#
#
# Release date: 2020-09-16
# Last update: 2021-01-13
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import os
import pandas as pd
import pycountry_convert as pyCountry
import pycountry
import argparse
import time
from datetime import datetime, timedelta
import numpy as np

DATA_SOURCE = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'
COUNTRIES_TO_REMOVE = []
COUNTRY_NAMES_TO_FIX = {'Kosovo[1]': 'Kosovo'}
COUNTRIES_TO_MERGE = {'Bonaire, Sint Eustatius and Saba': ['Bonaire', 'Sint Eustatius', 'Saba']}
COUNTRY_CODES_TO_FORCE = {'Kosovo': 'XKX'}

def check_date_format(date: str) -> str:
    """
    Check if date is in YYYY-MM-DD format
    """
    try:
        datetime.strptime(date, '%Y-%m-%d')
        return date
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Download and reformat epidemiological data from World Health Organization",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--type", required=False, nargs=1, type=str, default='cases',
                        choices=['cases', 'deaths'], help="Should values in target column be summed up?")
    parser.add_argument("--start-date", required=False, type=check_date_format,  help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=False, type=check_date_format,  help="End date in YYYY-MM-DD format")
    parser.add_argument("--output", required=True, help="TSV with daily counts")
    args = parser.parse_args()

    # Get arguments
    data_type = args.type[0]
    start_date = args.start_date
    end_date = args.end_date
    output = args.output
    path = os.getcwd()

    if start_date and end_date:
        assert start_date <= end_date, 'start-date must be before end-date'

    # Download raw data from World Health Organization and save it as a pandas dataframe
    covid_df = pd.read_csv(DATA_SOURCE, keep_default_na=False)
    
    # Some adjustments to the dataframe
    covid_df['Date_reported'] = pd.to_datetime(covid_df['Date_reported'], format='%Y-%m-%d')
    covid_df['Country_code'].replace(' ', np.NaN, inplace=True)
    covid_df.drop(columns=['WHO_region', 'Cumulative_cases', 'Cumulative_deaths'], inplace=True)
    covid_df.rename(columns={'Country_code': 'code', 'Country': 'country'}, inplace=True)

    # Check data quality
    assert covid_df['Date_reported'].dt.year.min() == 2020, 'Date before 2020 found'
    assert covid_df['Date_reported'].dt.year.max() <= datetime.now().year, 'Date in the future found'
    assert covid_df.query('country != "Other"')['code'].isnull().sum() == 0, 'Country with no country code found'
    assert covid_df.query('country == "Other"')['code'].notna().sum() == 0, 'Country "Other" has country code'
    assert covid_df.groupby(['code'])['country'].unique().apply(lambda x: len(x)).max() == 1, 'Country code has multiple countries'
    assert covid_df['country'].isnull().sum() == 0, 'country is null'
    assert covid_df.groupby(['country'])['code'].unique().apply(lambda x: len(x)).max() == 1, 'Country has multiple country codes'

    # Filter by date
    if start_date is not None:
        covid_df = covid_df[covid_df['Date_reported'] >= start_date]
    if end_date is not None:
        covid_df = covid_df[covid_df['Date_reported'] <= end_date]

    # Drop unnecessary columns
    metric_column = 'New_cases' if data_type == 'cases' else 'New_deaths'
    covid_df = covid_df[['Date_reported', 'code', 'country', metric_column]]

    # Create matrix with countries as rows and dates as columns
    covid_df = covid_df.set_index(['code', 'country', 'Date_reported']).unstack('Date_reported')
    covid_df.columns = covid_df.columns.droplevel(0)
    covid_df.columns = covid_df.columns.strftime('%Y-%m-%d')
    covid_df = covid_df.reset_index().rename_axis(None, axis=1)

    # Drop rows with unwanted countries
    if COUNTRIES_TO_REMOVE:
        assert isinstance(COUNTRIES_TO_REMOVE, list), 'COUNTRIES_TO_REMOVE must be a list'
        covid_df = covid_df[~covid_df['country'].isin(COUNTRIES_TO_REMOVE)]

    # Rename unusual country and territory names
    if COUNTRY_NAMES_TO_FIX:
        assert isinstance(COUNTRY_NAMES_TO_FIX, dict), 'COUNTRY_NAMES_TO_FIX must be a dictionary'
        covid_df['country'].replace(COUNTRY_NAMES_TO_FIX, inplace=True)

    # Merge countries
    if COUNTRIES_TO_MERGE:
        assert isinstance(COUNTRIES_TO_MERGE, dict), 'COUNTRIES_TO_MERGE must be a dictionary'
        for country, countries_to_merge in COUNTRIES_TO_MERGE.items():
            assert isinstance(countries_to_merge, list), 'COUNTRIES_TO_MERGE must be a dictionary of lists'
            assert all([c in covid_df['country'].unique() for c in countries_to_merge]), f'One of {countries_to_merge} not found in dataframe'

            # Change country names
            for c in countries_to_merge:
                covid_df.loc[covid_df['country'] == c, 'code'] = np.NaN
                covid_df.loc[covid_df['country'] == c, 'country'] = country

            # Sum values
            covid_df = covid_df.groupby(['country']).sum().reset_index()

    # Map ISO alpha2 country codes to ISO alpha3 country codes
    # First try: use pycountry library
    iso2_to_iso3_map = {country.alpha_2: country.alpha_3 for country in pycountry.countries}
    covid_df['code'] = covid_df['code'].map(iso2_to_iso3_map)

    # Second try: use pycountry_convert library searching by country name
    countries_with_no_code = covid_df.query('country != "Other" and code.isnull()', engine='python')['country'].unique()
    for country in countries_with_no_code:
        try:
            code = pyCountry.country_name_to_country_alpha3(country)
            covid_df.loc[covid_df['country'] == country, 'code'] = code
        except Exception as e:
            pass
    
    # Third try: use COUNTRY_CODES_TO_FORCE dictionary to force country codes
    if COUNTRY_CODES_TO_FORCE:
        assert isinstance(COUNTRY_CODES_TO_FORCE, dict), 'COUNTRY_CODES_TO_FORCE must be a dictionary'
        for country, code in COUNTRY_CODES_TO_FORCE.items():
            covid_df.loc[covid_df['country'] == country, 'code'] = code

    # Check final dataframe data quality
    assert covid_df.query('country != "Other"')['code'].isnull().sum() == 0, 'Final dataframe has null codes'
    assert covid_df['code'].duplicated().sum() == 0, 'Final dataframe has duplicated codes'
    assert covid_df.query('country != "Other"')['country'].isnull().sum() == 0, 'Final dataframe has null countries'
    assert covid_df['country'].duplicated().sum() == 0, 'Final dataframe has duplicated countries'
    assert covid_df.drop(columns=['code', 'country']).isnull().sum().sum() == 0, 'Final dataframe has null values'

    # Save dataframe as TSV file
    assert output.endswith('.tsv'), 'Output file must end with .tsv extension'
    covid_df.to_csv(output, sep='\t', index=False)
    print('\nOutput successfully exported: ' + output + '\n')
