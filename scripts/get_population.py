import json
import os
import pickle
import re
import argparse
import math

from enum import Enum
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from constants import STATE_DICT, API_KEY, AGE_PATTERNS, CENSUS_API_URL, CENSUS_VARIABLES_URL

class GeographyType(Enum):
    COUNTY = "county"
    ZCTA = "zip code tabulation area"

@dataclass
class GeographyConfig:
    type: GeographyType
    fips: str
    state_abbr: str

class CensusGeographyHandler:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
    
    def fetch_geography_units(self, config: GeographyConfig) -> List[str]:
        """Fetch all geographic units (counties or ZCTAs) for a state"""
        if config.type == GeographyType.ZCTA:
            return get_zctas(config.fips)
        
        params = {
            "get": "NAME",
            "for": f"{config.type.value}:*",
            "in": f"state:{config.fips}",
            "key": self.api_key
        }
        
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        return [row[2] for row in data[1:]]
    
    def fetch_census_data(self, 
                         variables: List[str], 
                         config: GeographyConfig, 
                         geo_unit: str) -> Dict:
        """Fetch census data for a specific geographic unit"""
        params = {
            "get": "NAME," + ",".join(variables),
            "for": f"{config.type.value}:{geo_unit}",
            "key": self.api_key
        }

        if config.type == GeographyType.COUNTY:
            params["in"] = f"state:{config.fips}"
        
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        return response.json()
    
    def save_data(self, 
                  data: Dict, 
                  config: GeographyConfig, 
                  geo_path: str, 
                  geo_unit: str,
                  data_type: str):

        """Save processed data to appropriate location"""
        base_path = f"{geo_path}/{data_type}/{config.state_abbr}" # ex. county_data/population/AL
        os.makedirs(base_path, exist_ok=True)
        
        filename = f"{base_path}/{geo_unit}_{data_type}" # ex. county_data/population/AL/013_population.pkl

        if isinstance(data, pd.DataFrame):
            data.to_csv(f"{filename}.csv", index=False)
        else:
            with open(f"{filename}.pkl", 'wb') as f:
                pickle.dump(data, f)

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

    state_zctas = [line.split(',')[-1][1:-1] for line in split_response[1:] if line.split(',')[-2] == f'"{state_fips}"']
    return state_zctas


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
    
def process_ethnicity_data(data, labels, state_abbr, geography_type):
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
    id_vars = ['NAME', geography_type]
    value_vars = list(labels.keys())
    melted_df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='variable', value_name='count')
    
    melted_df['ethnicity'] = melted_df['variable'].map(labels)
    melted_df['area'] = state_abbr + '0001'
    melted_df['region'] = state_abbr
    
    result_df = melted_df[['area', 'ethnicity', 'count', 'region']]
    
    result_df['count'] = result_df['count'].astype(int)
    
    return result_df

def get_state_batch(batch_number, batch_size=10):
    """
    Get the states for a specific batch number.
    States are sorted alphabetically by abbreviation before batching.
    """
    # Sort states by abbreviation
    sorted_states = sorted(STATE_DICT.items(), key=lambda x: x[1][1])  # Sort by state abbreviation
    
    # Calculate batch indices
    start_idx = (batch_number - 1) * batch_size
    end_idx = start_idx + batch_size
    
    # Get states for this batch
    batch_states = dict(sorted_states[start_idx:end_idx])
    
    if not batch_states:
        total_batches = math.ceil(len(STATE_DICT) / batch_size)
        raise ValueError(f"Invalid batch number. Please choose a batch between 1 and {total_batches}")
    
    return batch_states

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Census Geography Data Handler')
    
    # Add mutually exclusive group for batch or state
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--batch',
        type=int,
        help='Batch number to process (1-6, each batch contains 10 states)'
    )
    group.add_argument(
        '--state',
        type=str,
        help='Process specific state (use state abbreviation, e.g., CA)'
    )
    
    return parser.parse_args()

def main():
    handler = CensusGeographyHandler(API_KEY, CENSUS_API_URL)
    
    geography_type = GeographyType.ZCTA
    
    html_content = fetch_url(CENSUS_VARIABLES_URL)
    ag_vars, ag_labels = get_age_gender_labels(html_content)
    e_vars, e_labels = get_ethnicity_labels()
    
    args = parse_arguments()
    
    # Determine which states to process
    if args.batch:
        states_to_process = get_state_batch(args.batch)
        print(f"Processing batch {args.batch} states: {', '.join(state_abbr for _, (_, state_abbr) in states_to_process.items())}")
    else:  # args.state
        states_to_process = {
            fips: (name, abbr) for fips, (name, abbr) in STATE_DICT.items()
            if abbr == args.state.upper()
        }
        if not states_to_process:
            raise ValueError(f"Invalid state abbreviation: {args.state}")
    
    for state_fips, (state_name, state_abbr) in states_to_process.items():
        # Create config
        config = GeographyConfig(
            type=geography_type,
            fips=state_fips,
            state_abbr=state_abbr
        )
        
        # Get all geographic units for the state
        geo_units = handler.fetch_geography_units(config)
        
        for geo_unit in geo_units:
            try:
                # Fetch data
                ag_data = handler.fetch_census_data(ag_vars, config, geo_unit)
                e_data = handler.fetch_census_data(e_vars, config, geo_unit)
                
                # Process data
                ag_df = process_age_gender_data(ag_data, ag_labels, state_abbr)
                e_df = process_ethnicity_data(e_data, e_labels, state_abbr, geography_type.value)
                
                # Combine data
                pop_data = {
                    'age_gender': ag_df,
                    'ethnicity': e_df
                }
                
                # Set output directory
                if (geography_type == GeographyType.ZCTA):
                    geo_path = "zcta_data"
                else:
                    geo_path = "county_data"

                handler.save_data(ag_df, config, geo_path, geo_unit, 'age_gender')
                handler.save_data(pop_data, config, geo_path, geo_unit, 'population')
                
                print(f"Saved data for {config.type.value} {geo_unit}")
                
            except Exception as e:
                print(f"Error processing {config.type.value} {geo_unit}: {str(e)}")
        
        print(f"State {state_abbr} finished")

if __name__ == "__main__":
    main()