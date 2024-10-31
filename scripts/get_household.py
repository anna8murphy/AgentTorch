import pickle
import pandas as pd
import requests
from multiprocessing import Pool
from itertools import islice
import os

from constants import API_KEY, STATE_DICT, AGE_GROUP_MAPPING, CENSUS_API_URL
from get_population import get_counties, get_zctas
from get_population import CensusGeographyHandler, GeographyType, GeographyConfig

def get_household_labels():
    """
    Returns the variable keys and labels for household-related census data.
    
    Returns:
        tuple: A list of household variable keys and a dictionary mapping variables to descriptive labels.
    """
    variable_labels = {
        "B11001_001E": "Households",
        "B11001_002E": "Family Households",
        "B11001_007E": "Nonfamily Households",
        "B25010_001E": "Average Household Size"
    }

    return list(variable_labels.keys()), variable_labels

def process_household_data(data, labels, state_abbr, zcta):
    """
    Processes household census data and combines it with demographic (age/gender) data.

    Args:
        data (list): The household data fetched from the census API.
        labels (dict): A dictionary mapping variable names to their labels.
        state_abbr (str): The state abbreviation.
        zcta (str): The zcta code.
    
    Returns:
        pd.DataFrame: A DataFrame containing household statistics and the number of children and adults.
    """
    df = pd.DataFrame(data[1:], columns=data[0])
    area = state_abbr + '0001'
    
    h_data = {
        'area': area,
    }
    
    for variable, label in labels.items():
        value = float(df.loc[0, variable])
        if label == 'Households':
            h_data['household_num'] = int(value)
        elif label == 'Family Households':
            h_data['family_households'] = int(value)
        elif label == 'Nonfamily Households':
            h_data['nonfamily_households'] = int(value)
        elif label == 'Average Household Size':
            h_data['average_household_size'] = value
    
    age_gender_df = pd.read_csv(f'zcta_data/age_gender/{state_abbr}/{zcta}_age_gender.csv')
   
    h_data['children_num'] = age_gender_df[age_gender_df['age'].isin(AGE_GROUP_MAPPING['children_list'])]['count'].sum()
    h_data['people_num'] = age_gender_df[age_gender_df['age'].isin(AGE_GROUP_MAPPING['adult_list'])]['count'].sum()
    
    return pd.DataFrame([h_data])

def process_state(state_info):
    """
    Process a single state's data.
    
    Args:
        state_info (tuple): Contains state_fips and variables for processing
    
    Returns:
        str: State abbreviation of processed state
    """

    state_fips, variables, variable_labels = state_info
    state_abbr = STATE_DICT[state_fips][1]

    geography_handler = CensusGeographyHandler(api_key=API_KEY, base_url=CENSUS_API_URL)
    geography_type = GeographyType.ZCTA
    
    config = GeographyConfig(
            type=geography_type,
            fips=state_fips,
            state_abbr=state_abbr
        )
    
    # Ensure the output directory exists
    os.makedirs(f'zcta_data/household/{state_abbr}', exist_ok=True)
    
    zctas = get_zctas(state_fips)
    
    for zcta in zctas:
        try:
            data = geography_handler.fetch_census_data(variables, config, zcta)
            h_df = process_household_data(data, variable_labels, state_abbr, zcta)
            print(h_df)
            h_df.to_pickle(f'zcta_data/household/{state_abbr}/{zcta}_household.pkl')
        except Exception as e:
            print(f"Failed processing {state_abbr} - {zcta}: {str(e)}")
            continue
    
    return state_abbr

def batch(iterable, size):
    """
    Helper function to create batches from an iterable.
    
    Args:
        iterable: The iterable to batch
        size (int): The size of each batch
    
    Yields:
        list: A batch of items from the iterable
    """
    iterator = iter(iterable)
    while True:
        batch = list(islice(iterator, size))
        if not batch:
            break
        yield batch

def main():

    # Get household labels and data
    variables, variable_labels = get_household_labels()
    
    # Create list of state processing information
    state_process_info = [(state_fips, variables, variable_labels) 
                         for state_fips in STATE_DICT]
    
    # Process states in batches of 10
    with Pool() as pool:
        for state_batch in batch(state_process_info, 10):
            # Process batch of states in parallel
            completed_states = pool.map(process_state, state_batch)
            print(f"Completed processing states: {', '.join(completed_states)}")

if __name__ == "__main__":
    main()