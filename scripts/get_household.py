import pickle

import pandas as pd
import requests

from constants import API_KEY, STATE_DICT, AGE_GROUP_MAPPING
from get_population import fetch_census_data, get_counties, create_state_dirs

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

def process_household_data(data, labels, state_abbr, county):
    """
    Processes household census data and combines it with demographic (age/gender) data.

    Args:
        data (list): The household data fetched from the census API.
        labels (dict): A dictionary mapping variable names to their labels.
        state_abbr (str): The state abbreviation.
        county (str): The county code.
    
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
    
    age_gender_df = pd.read_csv(f'data/age_gender/{state_abbr}/{county}_age_gender.csv')
   
    h_data['children_num'] = age_gender_df[age_gender_df['age'].isin(AGE_GROUP_MAPPING['children_list'])]['count'].sum()
    h_data['people_num'] = age_gender_df[age_gender_df['age'].isin(AGE_GROUP_MAPPING['adult_list'])]['count'].sum()
    
    result_df = pd.DataFrame([h_data])
    
    return result_df

def main():

    # create_state_dirs("household")

    # get household labels and data
    variables, variable_labels = get_household_labels()

    for state_fips in STATE_DICT:
        
        counties = get_counties(state_fips)
        state_abbr = STATE_DICT[state_fips][1]

        for county in counties:
            try:
                data = fetch_census_data(variables, state_fips, county)

                # get df
                h_df = process_household_data(data, variable_labels, state_abbr, county)

                # save
                h_df.to_pickle('data/household/' + state_abbr + "/" + county + '_household.pkl')

            except:
                print("failed")
                continue
        
        print("state", state_abbr, "finished")
    
if __name__ == "__main__":
    main()

