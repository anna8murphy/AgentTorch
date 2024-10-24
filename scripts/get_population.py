import json
import os
import pickle
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from constants import STATE_DICT, API_KEY, AGE_PATTERNS, CENSUS_API_URL, CENSUS_VARIABLES_URL

def fetch_url(url):
    """
    Fetches the content of a webpage given its URL.

    Args:
        url (str): The URL of the webpage to fetch.

    Returns:
        str: The HTML content of the webpage.

    Raises:
        HTTPError: If the request to the URL fails.
    """
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def get_age_gender_labels(html_content):
    """
    Extracts age and gender-related variables and labels from an HTML table.

    Args:
        html_content (str): The HTML content containing a table with variable names, labels, and concepts.

    Returns:
        tuple: A list of variable names (ex. B01001_004E) under the concept "Sex by Age" and a dictionary mapping variable names to labels (ex. Male 5 to 9 years).
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')
    if not table:
        print("No table found in the HTML content")
        return [], {}
    
    tbody = table.find('tbody')
    
    variables = []
    variable_labels = {}
    for row in tbody.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) >= 3:
            variable_name = cells[0].text.strip()
            label = cells[1].text.strip()
            concept = cells[2].text.strip()

            if concept == "Sex by Age":
                variables.append(variable_name)
                variable_labels[variable_name] = label
    
    return variables, variable_labels

def get_ethnicity_labels():
    """
    Returns variable names and labels for ethnicity categories used in census data.

    Returns:
        tuple: A list of variable names and a dictionary mapping variable names to ethnicity labels.
    """
    variable_labels = {
        "B02001_002E": "White",
        "B02001_003E": "Black",
        "B02001_004E": "American Indian and Alaska Native",
        "B02001_005E": "Asian",
        "B02001_006E": "Native Hawaiian and Other Pacific Islander",
        "B02001_007E": "Some Other Race",
        "B02001_008E": "Two or More Races"
    }
    return list(variable_labels.keys()), variable_labels

def fetch_census_data(variables, state, county):
    """
    Fetches county-level census data for a specific state and county from 2022 5-Year ACS.

    Args:
        variables (list): A list of variable names to fetch.
        state (str): The state FIPS code.
        county (str): The county FIPS code.

    Returns:
        list: A JSON response containing the census data for the specified variables, state, and county.

    Raises:
        HTTPError: If the request to the API fails.
    """    
    get_variables = "NAME," + ",".join(variables)
    
    params = {
        "get": get_variables,
        "for": f"county:{county}",
        "in": f"state:{state}",
        "key": API_KEY
    }

    response = requests.get(CENSUS_API_URL, params=params)
    response.raise_for_status()
    return response.json()

def parse_label(label):
    """
    Parses a label to determine the gender and age group.

    Args:
        label (str): The label describing gender and age.

    Returns:
        tuple: A string representing gender ('male' or 'female') and a string representing the age group.
    """
    gender = 'male' if 'Male' in label else 'female'
    
    for pattern, age_group in AGE_PATTERNS:
        if re.search(pattern, label):
            return gender, age_group
    
    return gender, 'unknown'

def process_age_gender_data(data, variable_labels, state_abbr):
    """
    Processes age and gender census data and transforms it into a structured DataFrame.

    Args:
        data (list): Census data in a list format.
        variable_labels (dict): A dictionary mapping variable names to labels.
        state_abbr (str): The state abbreviation.

    Returns:
        pd.DataFrame: A DataFrame containing processed age and gender data.
    """
    df = pd.DataFrame(data[1:], columns=data[0])
    
    region = state_abbr
    area = region + '0001'
    
    transformed_data = {}
    
    for variable, value in df.iloc[0].items():
        if variable in variable_labels:
            label = variable_labels[variable]
            gender, age = parse_label(label)
            count = int(value)
            
            key = (area, gender, age, region)
            if key[2] != 'unknown':
                if (key[2] == '20' or key[2] == '21'):
                    key = (area, gender, '20t21', region)
                if key not in transformed_data:
                    transformed_data[key] = 0
                transformed_data[key] += count
    
    result = [
        {
            'area': area,
            'gender': gender,
            'age': age,
            'count': count,
            'region': region
        }
        for (area, gender, age, region), count in transformed_data.items()
    ]
    return pd.DataFrame(result)

def get_counties(state_fips):
    """
    Fetches all counties for a specific state based on the state FIPS code.

    Args:
        state_fips (str): The FIPS code for the state.

    Returns:
        list: A list of county FIPS codes for the state.

    Raises:
        HTTPError: If the request to the API fails.
    """
    params = {
        "get": "NAME",
        "for": "county:*", 
        "in": f"state:{state_fips}", 
        "key": API_KEY
    }

    response = requests.get(CENSUS_API_URL, params=params)
    response.raise_for_status()

    counties_data = response.json()

    counties = [county[2] for county in counties_data[1:]]  # Skip the header row

    return counties

def get_zctas(state_fips):
    """
    Fetches Zip Code Tabulation Areas (ZCTAs) for a specified state using the Census API.

    Args:
        state_fips (str): The Federal Information Processing Standards (FIPS) code for the state 
                          whose ZCTAs are to be retrieved.

    Returns:
        list: A list of ZCTAs (as strings) corresponding to the specified state.
    """
    zcta_url = "https://api.census.gov/data/2017/acs/acs5?get=NAME,group(B19013)&for=zip%20code%20tabulation%20area:*" # get all zctas
    zcta_response = fetch_url(zcta_url)
    split_response = zcta_response.split('],')

    state_zctas = [line.split(',')[-1][1:-1] for line in split_response[1:] if line.split(',')[-2] == f'"{state}"']
    return state_zctas
    
def process_ethnicity_data(data, labels, state_abbr):
    """
    Processes ethnicity census data and transforms it into a structured DataFrame.

    Args:
        data (list): Census data in a list format.
        labels (dict): A dictionary mapping variable names to ethnicity labels.
        state_abbr (str): The state abbreviation.

    Returns:
        pd.DataFrame: A DataFrame containing processed ethnicity data.
    """
    df = pd.DataFrame(data[1:], columns=data[0])
    id_vars = ['NAME', 'county']
    value_vars = list(labels.keys())
    melted_df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='variable', value_name='count')
    
    melted_df['ethnicity'] = melted_df['variable'].map(labels)
    melted_df['area'] = state_abbr + '0001'
    melted_df['region'] = state_abbr
    
    result_df = melted_df[['area', 'ethnicity', 'count', 'region']]
    
    result_df['count'] = result_df['count'].astype(int)
    
    return result_df

def create_state_dirs(path):
    """
    Creates directories for each state based on the state name from the STATE_DICT.

    Args:
        path (str): The base directory where state directories should be created.

    Returns:
        None
    """
    for name in STATE_DICT:

        name = STATE_DICT[name][1]
        try:
            a = f'data/{path}/' + name
            os.mkdir(a)

            print(f"Directory {name} created successfully")

        except FileExistsError:
            print(f"Directory {name} already exists")


def main():

    loc_id = "county"

    # get all census variables
    html_content = fetch_url(CENSUS_VARIABLES_URL)
    ag_vars, ag_labels = get_age_gender_labels(html_content)
    e_vars, e_labels = get_ethnicity_labels()

    # for each state
    for state_fips in STATE_DICT:
        
        counties = get_counties(state_fips)
        state_abbr = STATE_DICT[state_fips][1]

        for county in counties:

            try:
                # fetch data using api
                ag_data = fetch_census_data(ag_vars, state_fips, county)
                e_data = fetch_census_data(e_vars, state_fips, county)

                # create dataframes
                ag_df = process_age_gender_data(ag_data, ag_labels, state_abbr)
                e_df = process_ethnicity_data(e_data, e_labels, state_abbr)

                # create dict of dataframes
                pop_data = {
                'age_gender': ag_df,
                'ethnicity': e_df
                }

                # store age and gender data, used in household.py
                ag_filename = f"data/age_gender/{state_abbr}/{county}_age_gender.csv"
                ag_df.to_csv(ag_filename, index=False)

                # write combined population data to pickle   
                pop_filename = f"data/population/{state_abbr}/{county}_population.pkl"

                with open(pop_filename, 'wb') as file:
                    pickle.dump(pop_data, file)

                print("saved data for county", county)

            except:
                print("error processing county", county)

        print("state", state_abbr, "finished")

if __name__ == "__main__":
    main()